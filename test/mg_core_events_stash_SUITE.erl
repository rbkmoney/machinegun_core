-module(mg_core_events_stash_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% Tests
-export([stash_enlarge_test/1]).
-export([stash_shrink_test/1]).

%% mg_core_events_machine handler
-behaviour(mg_core_events_machine).
-export([process_signal/4, process_call/4, process_repair/4]).

%% mg_core_events_sink handler
-behaviour(mg_core_events_sink).
-export([add_events/6]).

%% Pulse
-export([handle_beat/2]).

%% Internal types

-type action() :: mg_core_events_machine:complex_action().
-type aux_state() :: term().
-type call() :: term().
-type call_result() :: mg_core_events_machine:call_result().
-type repair_result() :: mg_core_events_machine:repair_result().
-type deadline() :: mg_core_deadline:deadline().
-type event() :: term().
-type machine() :: mg_core_events_machine:machine().
-type req_ctx() :: mg_core:request_context().
-type signal() :: mg_core_events_machine:signal().
-type signal_result() :: mg_core_events_machine:signal_result().


-type handlers() :: #{
    signal_handler => fun((signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}),
    call_handler => fun((call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}),
    sink_handler => fun(([event()]) -> ok)
}.
-type processor() :: {module(), handlers()}.
-type options() :: #{
    processor := processor(),
    namespace := mg_core:ns(),
    event_stash_size := non_neg_integer()
}.

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%% Common test handlers

-spec all() ->
    [test_name()].
all() ->
    [
       stash_enlarge_test,
       stash_shrink_test
    ].

-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    {ok, StoragePid} = mg_core_storage_memory:start_link(#{name => ?MODULE}),
    true = erlang:unlink(StoragePid),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

%% Tests

-spec stash_enlarge_test(config()) ->
    any().
stash_enlarge_test(_C) ->
    MachineID = <<"machine_1">>,
    Options0 = #{
        processor => {?MODULE, #{
            signal_handler => fun
                ({init, NewEvents}, AuxState, []) -> {AuxState, NewEvents, #{}}
            end,
            call_handler => fun
                ({emit, NewEvents}, AuxState, _) -> {ok, AuxState, NewEvents, #{}}
            end
        }},
        namespace => <<"event_stash_test">>,
        event_stash_size => 3
    },
    {Pid0, OptionsRef0, Refs0} = start_automaton(Options0),
    ok = start(OptionsRef0, MachineID, [1, 2]),
    ok = call(OptionsRef0, MachineID, {emit, [3, 4]}),
    ?assertEqual([1, 2, 3, 4], get_history(OptionsRef0, MachineID)),
    ?assertEqual([4, 3, 2, 1], get_history(OptionsRef0, MachineID, {undefined, undefined, backward})),
    ok = stop_automaton(Refs0, Pid0),
    Options1 = Options0#{event_stash_size => 5},
    {Pid1, OptionsRef1, Refs1} = start_automaton(Options1),
    ok = call(OptionsRef1, MachineID, {emit, [5, 6]}),
    ?assertEqual([1, 2, 3, 4, 5, 6], get_history(OptionsRef1, MachineID)),
    ?assertEqual([6, 5, 4, 3, 2, 1], get_history(OptionsRef1, MachineID, {undefined, undefined, backward})),
    ok = stop_automaton(Refs1, Pid1).

-spec stash_shrink_test(config()) ->
    any().
stash_shrink_test(_C) ->
    MachineID = <<"machine_2">>,
    Options0 = #{
        processor => {?MODULE, #{
            signal_handler => fun
                ({init, NewEvents}, AuxState, []) -> {AuxState, NewEvents, #{}}
            end,
            call_handler => fun
                ({emit, NewEvents}, AuxState, _) -> {ok, AuxState, NewEvents, #{}}
            end
        }},
        namespace => <<"event_stash_test">>,
        event_stash_size => 5
    },
    {Pid0, OptionsRef0, Refs0} = start_automaton(Options0),
    ok = start(OptionsRef0, MachineID, [1, 2]),
    ok = call(OptionsRef0, MachineID, {emit, [3, 4, 5]}),
    ?assertEqual([1, 2, 3, 4, 5], get_history(OptionsRef0, MachineID)),
    ?assertEqual([5, 4, 3, 2, 1], get_history(OptionsRef0, MachineID, {undefined, undefined, backward})),
    ok = stop_automaton(Refs0, Pid0),
    Options1 = Options0#{event_stash_size => 3},
    {Pid1, OptionsRef1, Refs1} = start_automaton(Options1),
    ok = call(OptionsRef1, MachineID, {emit, [6, 7]}),
    ?assertEqual([1, 2, 3, 4, 5, 6, 7], get_history(OptionsRef1, MachineID)),
    ?assertEqual([7, 6, 5, 4, 3, 2, 1], get_history(OptionsRef1, MachineID, {undefined, undefined, backward})),
    ok = stop_automaton(Refs1, Pid1).

%% Processor handlers

-spec process_signal(handlers(), req_ctx(), deadline(), mg_core_events_machine:signal_args()) -> signal_result().
process_signal(Handlers, _ReqCtx, _Deadline, {EncodedSignal, Machine}) ->
    Handler = maps:get(signal_handler, Handlers, fun dummy_signal_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Signal = decode_signal(EncodedSignal),
    ct:pal("call signal handler ~p with [~p, ~p, ~p]", [Handler, Signal, AuxState, History]),
    {NewAuxState, NewEvents, ComplexAction} = Handler(Signal, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {StateChange, ComplexAction}.

-spec process_call(handlers(), req_ctx(), deadline(), mg_core_events_machine:call_args()) -> call_result().
process_call(Handlers, _ReqCtx, _Deadline, {EncodedCall, Machine}) ->
    Handler = maps:get(call_handler, Handlers, fun dummy_call_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Call = decode(EncodedCall),
    ct:pal("call call handler ~p with [~p, ~p, ~p]", [Handler, Call, AuxState, History]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Call, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {encode(Result), StateChange, ComplexAction}.

-spec process_repair(options(), req_ctx(), deadline(), mg_core_events_machine:repair_args()) -> repair_result().
process_repair(Options, _ReqCtx, _Deadline, {EncodedArgs, Machine}) ->
    Handler = maps:get(repair_handler, Options, fun dummy_repair_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Args = decode(EncodedArgs),
    ct:pal("call repair handler ~p with [~p, ~p, ~p]", [Handler, Args, AuxState, History]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Args, AuxState, History),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    Events = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, Events},
    {ok, {encode(Result), StateChange, ComplexAction}}.

-spec add_events(handlers(), mg_core:ns(), mg_core:id(), [event()], req_ctx(), deadline()) ->
    ok.
add_events(Handlers, _NS, _MachineID, Events, _ReqCtx, _Deadline) ->
    Handler = maps:get(sink_handler, Handlers, fun dummy_sink_handler/1),
    ct:pal("call sink handler ~p with [~p]", [Handler, Events]),
    ok = Handler(decode_events(Events)).

-spec dummy_signal_handler(signal(), aux_state(), [event()]) ->
    {aux_state(), [event()], action()}.
dummy_signal_handler(_Signal, AuxState, _Events) ->
    {AuxState, [], #{}}.

-spec dummy_call_handler(signal(), aux_state(), [event()]) ->
    {aux_state(), [event()], action()}.
dummy_call_handler(_Signal, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_repair_handler(term(), aux_state(), [event()]) ->
    {ok, aux_state(), [event()], action()}.
dummy_repair_handler(_Args, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_sink_handler([event()]) ->
    ok.
dummy_sink_handler(_Events) ->
    ok.

%% Pulse handler

-spec handle_beat(_, mg_core_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

%% Internal functions

-spec start_automaton(options()) ->
    {pid(), mg_core_namespace:options_ref(), [mg_core_namespace:options_ref()]}.
start_automaton(Options) ->
    {OptionsRef, Refs} = save_namespace_options(Options),
    {mg_core_utils:throw_if_error(mg_core_namespace:start_link(OptionsRef)), OptionsRef, Refs}.

-spec stop_automaton([mg_core_namespace:options_ref()], pid()) ->
    ok.
stop_automaton(OptionsRefs, Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    [persistent_term:erase(Ref) || Ref <- OptionsRefs],
    ok.

-spec save_namespace_options(options()) ->
    mg_core_namespace:options_ref().
save_namespace_options(Options) ->
    NS = maps:get(namespace, Options),
    Processor = maps:get(processor, Options),
    StashSize = maps:get(event_stash_size, Options),
    Scheduler = #{min_scan_delay => 100},
    Storage = {mg_core_storage_memory, #{
        existing_storage_name => ?MODULE
    }},
    TagsRef = mg_core_namespace:make_options_ref(<<NS/binary, "_tags">>),
    TagsNSOptions = mg_core_machine_tags:make_namespace_options(#{
        namespace => <<NS/binary, "_tags">>,
        storage => mg_core_ct_helper:build_storage(NS, Storage),
        registry => mg_core_procreg_gproc,
        pulse => ?MODULE
    }),
    OptionsRef = mg_core_namespace:make_options_ref(NS),
    NSOptions = #{
        namespace => NS,
        processor => {mg_core_events_machine, #{
            namespace => NS,
            processor => Processor,
            pulse => ?MODULE,
            events_storage => Storage,
            tagging => #{
                namespace_options_ref => TagsRef
            },
            event_sinks => [
                {?MODULE, Options}
            ],
            event_stash_size => StashSize
        }},
        storage => Storage,
        registry => mg_core_procreg_gproc,
        pulse => ?MODULE,
        schedulers => #{
            timers => Scheduler,
            timers_retries => Scheduler,
            overseer => Scheduler
        }
    },
    ok = mg_core_namespace:save_options(NSOptions, OptionsRef),
    ok = mg_core_namespace:save_options(TagsNSOptions, TagsRef),
    {OptionsRef, [OptionsRef, TagsRef]}.

%%

-spec start(mg_core_namespace:options_ref(), mg_core:id(), term()) ->
    ok.
start(OptionsRef, MachineID, Args) ->
    Deadline = mg_core_deadline:from_timeout(3000),
    mg_core_events_machine:start(OptionsRef, MachineID, encode(Args), <<>>, Deadline).

-spec call(mg_core_namespace:options_ref(), mg_core:id(), term()) ->
    term().
call(OptionsRef, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_core_deadline:from_timeout(3000),
    Result = mg_core_events_machine:call(OptionsRef, {id, MachineID}, encode(Args), HRange, <<>>, Deadline),
    decode(Result).

-spec get_history(mg_core_namespace:options_ref(), mg_core:id()) ->
    ok.
get_history(OptionsRef, MachineID) ->
    get_history(OptionsRef, MachineID, {undefined, undefined, forward}).

-spec get_history(mg_core_namespace:options_ref(), mg_core:id(), mg_core_events:history_range()) ->
    ok.
get_history(OptionsRef, MachineID, HRange) ->
    Machine = mg_core_events_machine:get_machine(OptionsRef, {id, MachineID}, HRange),
    {_AuxState, History} = decode_machine(Machine),
    History.

%% Codecs

-spec decode_machine(machine()) ->
    {aux_state(), History :: [event()]}.
decode_machine(#{aux_state := EncodedAuxState, history := EncodedHistory}) ->
    {decode_aux_state(EncodedAuxState), decode_events(EncodedHistory)}.

-spec decode_aux_state(mg_core_events_machine:aux_state()) ->
    aux_state().
decode_aux_state({#{format_version := 1}, EncodedAuxState}) ->
    decode(EncodedAuxState);
decode_aux_state({#{}, <<>>}) ->
    <<>>.

-spec decode_events([mg_core_events:event()]) ->
    [event()].
decode_events(Events) ->
    [
        decode(EncodedEvent)
        || #{body := {#{}, EncodedEvent}} <- Events
    ].

-spec decode_signal(signal()) ->
    signal().
decode_signal(timeout) ->
    timeout;
decode_signal({init, Args}) ->
    {init, decode(Args)};
decode_signal({repair, Args}) ->
    {repair, decode(Args)}.

-spec encode(term()) ->
    binary().
encode(Value) ->
    erlang:term_to_binary(Value).

-spec decode(binary()) ->
    term().
decode(Value) ->
    erlang:binary_to_term(Value, [safe]).
