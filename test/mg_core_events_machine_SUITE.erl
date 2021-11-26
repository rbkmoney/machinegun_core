%%%
%%% Copyright 2019 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(mg_core_events_machine_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("ct_helper.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([get_events_test/1]).
-export([continuation_repair_test/1]).
-export([double_tag_repair_test/1]).
-export([get_corrupted_machine_fails/1]).
-export([ed209_migration_simple_succeeds/1]).
-export([ed209_migration_repair_succeeds/1]).

%% mg_core_events_machine handler
-behaviour(mg_core_events_machine).
-export_type([options/0]).
-export([process_signal/4, process_call/4, process_repair/4]).

%% mg_core_events_sink handler
-behaviour(mg_core_events_sink).
-export([add_events/6]).

%% mg_core_storage callbacks
-behaviour(mg_core_storage).
-export([child_spec/2, do_request/2]).

%% Pulse
-export([handle_beat/2]).

%% Internal types

-type call() :: term().
-type signal() :: mg_core_events_machine:signal().
-type machine() :: mg_core_events_machine:machine().
-type signal_result() :: mg_core_events_machine:signal_result().
-type call_result() :: mg_core_events_machine:call_result().
-type repair_result() :: mg_core_events_machine:repair_result().
-type action() :: mg_core_events_machine:complex_action().
-type event() :: term().
-type history() :: [{mg_core_events:id(), event()}].
-type aux_state() :: term().
-type req_ctx() :: mg_core:request_context().
-type deadline() :: mg_core_deadline:deadline().

-type options() :: #{
    signal_handler => fun((signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}),
    call_handler => fun(
        (call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}
    ),
    sink_handler => fun((history()) -> ok)
}.

-type test_name() :: atom().
-type config() :: [{atom(), _}].

%% Common test handlers

-spec all() -> [test_name()].
all() ->
    [
        get_events_test,
        continuation_repair_test,
        double_tag_repair_test,
        get_corrupted_machine_fails,
        ed209_migration_simple_succeeds,
        ed209_migration_repair_succeeds
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

%% Tests

-spec get_events_test(config()) -> any().
get_events_test(_C) ->
    NS = <<"events">>,
    ProcessorOpts = #{
        signal_handler => fun({init, <<>>}, AuxState, []) ->
            {AuxState, [], #{}}
        end,
        call_handler => fun({emit, N}, AuxState, _) ->
            {ok, AuxState, [{} || _ <- lists:seq(1, N)], #{}}
        end,
        sink_handler => fun(_Events) ->
            % NOTE
            % Inducing random delay so that readers would hit delayed
            % events reading logic more frequently.
            Spread = 100,
            Baseline = 20,
            timer:sleep(rand:uniform(Spread) + Baseline)
        end
    },
    N = 10,
    ok = lists:foreach(
        fun(I) ->
            ct:pal("Complexity = ~p", [I]),
            BaseOpts = #{
                pulse => {?MODULE, quiet},
                event_stash_size => rand:uniform(2 * I)
            },
            StorageOpts = #{
                batching => #{concurrency_limit => rand:uniform(5 * I)},
                random_transient_fail => #{put => 0.1 / N}
            },
            Options = events_machine_options(BaseOpts, StorageOpts, ProcessorOpts, NS),
            ct:pal("Options = ~p", [Options]),
            {Pid, Options} = start_automaton(Options),
            MachineID = genlib:to_binary(I),
            ok = start(Options, MachineID, <<>>),
            NEmits = rand:uniform(2 * I),
            NGets = rand:uniform(10 * I),
            Delay = rand:uniform(400) + 100,
            Size = rand:uniform(5 * I),
            THRange = t_history_range(),
            _ = genlib_pmap:map(
                fun
                    (emit) ->
                        ok = timer:sleep(rand:uniform(Delay)),
                        ok = call(Options, MachineID, {emit, rand:uniform(2 * I)});
                    (get) ->
                        ok = timer:sleep(rand:uniform(Delay)),
                        {ok, HRange} = proper_gen:pick(THRange, Size),
                        History = get_history(Options, MachineID, HRange),
                        assert_history_consistent(History, HRange)
                end,
                lists:duplicate(NEmits, emit) ++
                    lists:duplicate(NGets, get)
            ),
            ok = stop_automaton(Pid)
        end,
        lists:seq(1, N)
    ).

-spec assert_history_consistent
    (history(), mg_core_events:history_range()) -> ok;
    (history(), _Assertion :: {from | limit | direction, _}) -> ok.
assert_history_consistent(History, HRange = {From, Limit, Direction}) ->
    Result = lists:all(fun(Assert) -> assert_history_consistent(History, Assert) end, [
        {from, {From, Direction}},
        {limit, Limit},
        {direction, Direction}
    ]),
    ?assertMatch({true, _, _}, {Result, History, HRange});
assert_history_consistent([{ID, _} | _], {from, {From, forward}}) when From /= undefined ->
    From < ID;
assert_history_consistent([{ID, _} | _], {from, {From, backward}}) when From /= undefined ->
    From > ID;
assert_history_consistent(History, {limit, Limit}) ->
    length(History) =< Limit;
assert_history_consistent(History = [{ID, _} | _], {direction, Direction}) ->
    Step =
        case Direction of
            forward -> +1;
            backward -> -1
        end,
    Expected = lists:seq(ID, ID + (length(History) - 1) * Step, Step),
    Expected =:= [ID1 || {ID1, _} <- History];
assert_history_consistent(_History, _Assertion) ->
    true.

-spec t_history_range() -> proper_types:raw_type().
t_history_range() ->
    {t_from_event(), t_limit(), t_direction()}.

-spec t_from_event() -> proper_types:raw_type().
t_from_event() ->
    proper_types:frequency([
        {4, proper_types:non_neg_integer()},
        {1, proper_types:exactly(undefined)}
    ]).

-spec t_limit() -> proper_types:raw_type().
t_limit() ->
    proper_types:frequency([
        {4, proper_types:pos_integer()},
        {1, proper_types:exactly(undefined)}
    ]).

-spec t_direction() -> proper_types:raw_type().
t_direction() ->
    proper_types:oneof([
        proper_types:exactly(forward),
        proper_types:exactly(backward)
    ]).

-spec continuation_repair_test(config()) -> any().
continuation_repair_test(_C) ->
    NS = <<"test">>,
    MachineID = <<"machine">>,
    TestRunner = self(),
    ProcessorOptions = #{
        signal_handler => fun({init, <<>>}, AuxState, []) -> {AuxState, [1], #{}} end,
        call_handler => fun(raise, AuxState, [1]) -> {ok, AuxState, [2], #{}} end,
        repair_handler => fun(<<>>, AuxState, [1, 2]) -> {ok, AuxState, [3], #{}} end,
        sink_handler => fun
            ([2]) ->
                erlang:error(test_error);
            (Events) ->
                TestRunner ! {sink_events, Events},
                ok
        end
    },
    {Pid, Options} = start_automaton(ProcessorOptions, NS),
    ok = start(Options, MachineID, <<>>),
    ?assertReceive({sink_events, [1]}),
    ?assertException(throw, {logic, machine_failed}, call(Options, MachineID, raise)),
    ok = repair(Options, MachineID, <<>>),
    ?assertReceive({sink_events, [2, 3]}),
    ?assertEqual([{1, 1}, {2, 2}, {3, 3}], get_history(Options, MachineID)),
    ok = stop_automaton(Pid).

-spec double_tag_repair_test(config()) -> any().
double_tag_repair_test(_C) ->
    NS = <<"test">>,
    MachineID1 = <<"machine1">>,
    MachineID2 = <<"machine2">>,
    Tag = <<"haha got you">>,
    ProcessorOptions = #{
        signal_handler => fun({init, <<>>}, AuxState, []) -> {AuxState, [1], #{}} end,
        call_handler => fun(tag, AuxState, [1]) -> {ok, AuxState, [2], #{tag => Tag}} end,
        repair_handler => fun(<<>>, AuxState, [1, 2]) -> {ok, AuxState, [3], #{}} end
    },
    {Pid, Options} = start_automaton(ProcessorOptions, NS),
    ok = start(Options, MachineID1, <<>>),
    ok = start(Options, MachineID2, <<>>),
    ?assertEqual(ok, call(Options, MachineID1, tag)),
    ?assertException(throw, {logic, machine_failed}, call(Options, MachineID2, tag)),
    ok = repair(Options, MachineID2, <<>>),
    ?assertEqual([{1, 1}, {2, 2}], get_history(Options, MachineID1)),
    ?assertEqual([{1, 1}, {2, 2}, {3, 3}], get_history(Options, MachineID2)),
    ok = stop_automaton(Pid).

-spec get_corrupted_machine_fails(config()) -> any().
get_corrupted_machine_fails(_C) ->
    NS = <<"corruption">>,
    MachineID = genlib:to_binary(?FUNCTION_NAME),
    LoseEvery = 4,
    ProcessorOpts = #{
        signal_handler => fun
            ({init, <<>>}, _AuxState, []) ->
                {0, [], #{}};
            (timeout, N, _) ->
                {0, [I || I <- lists:seq(1, N)], #{}}
        end,
        call_handler => fun({emit, N}, _AuxState, _) ->
            {ok, N, [], #{timer => {set_timer, {timeout, 0}, undefined, undefined}}}
        end
    },
    BaseOptions = events_machine_options(
        #{event_stash_size => 0},
        #{},
        ProcessorOpts,
        NS
    ),
    LossyStorage =
        {?MODULE, #{
            lossfun => fun(I) -> (I rem LoseEvery) == 0 end,
            storage => {mg_core_storage_memory, #{}}
        }},
    EventsStorage = mg_core_ct_helper:build_storage(<<NS/binary, "_events">>, LossyStorage),
    {Pid, Options} = start_automaton(BaseOptions#{events_storage => EventsStorage}),
    ok = start(Options, MachineID, <<>>),
    _ = ?assertEqual([], get_history(Options, MachineID)),
    ok = call(Options, MachineID, {emit, LoseEvery * 2}),
    ok = timer:sleep(1000),
    _ = ?assertError(_, get_history(Options, MachineID)),
    ok = stop_automaton(Pid).

-spec ed209_migration_simple_succeeds(config()) -> any().
ed209_migration_simple_succeeds(C) ->
    ed209_migration_scenario_succeeds(simple, C).

-spec ed209_migration_repair_succeeds(config()) -> any().
ed209_migration_repair_succeeds(C) ->
    ed209_migration_scenario_succeeds(repair, C).

-define(ED_209_STASH_SIZE, 5).
-define(ED_209_INITIAL_EVENTS, 8).
-define(ED_209_EMITTED_EVENTS, 4).

-spec ed209_migration_scenario_succeeds(simple | repair, config()) -> any().
ed209_migration_scenario_succeeds(Scenario, C) ->
    NS = <<"ED-209">>,
    {ok, StoragePid} = mg_core_storage_memory:start_link(#{name => ?MODULE}),
    StorageOpts = #{existing_storage_name => ?MODULE},
    RunnerPid = self(),
    SignalHandler = fun({init, N}, _AuxState, []) ->
        {0, lists:seq(1, N), #{}}
    end,
    RepairHandler = fun({HSize, AuxStateIn, {emit, N}}, AuxState, History) ->
        AuxState = AuxStateIn,
        History = lists:seq(1, HSize),
        {ok, AuxState + 1, lists:seq(HSize + 1, HSize + N), #{}}
    end,
    CallHandler = fun({emit, N}, AuxState, History) ->
        HSize = length(History),
        {ok, AuxState + 1, lists:seq(HSize + 1, HSize + N), #{}}
    end,
    SinkHandler = fun(Events) ->
        FailOn = persistent_term:get({?FUNCTION_NAME, fail_event_sink_on}, []),
        case Events -- FailOn of
            Events ->
                _ = [RunnerPid ! Event || Event <- Events],
                ok;
            _FailThen ->
                erlang:error({failed_event_sink_on, FailOn})
        end
    end,
    ProcessorOpts = #{
        signal_handler => SignalHandler,
        call_handler => CallHandler,
        repair_handler => RepairHandler,
        sink_handler => SinkHandler
    },
    BaseOpts = #{event_stash_size => ?ED_209_STASH_SIZE},
    Options = events_machine_options(BaseOpts, StorageOpts, ProcessorOpts, NS),
    MachineID = genlib:to_binary(Scenario),

    % Fire up pre-ED-290 automaton
    {ok, Pid1} = mg_core_events_machine_pre_ed290:start_link(Options),
    ?assertEqual(ok, start(Options, MachineID, ?ED_209_INITIAL_EVENTS)),
    % Fail machine mid-continuation, yet with handful of additional events in state
    ok = persistent_term:put({?FUNCTION_NAME, fail_event_sink_on}, [?ED_209_INITIAL_EVENTS + 1]),
    ?assertThrow({logic, machine_failed}, call(Options, MachineID, {emit, ?ED_209_EMITTED_EVENTS})),
    ok = stop_automaton(Pid1),

    % Ensure sink handler fails no more
    true = persistent_term:erase({?FUNCTION_NAME, fail_event_sink_on}),

    % Fire up current automaton, with existing machine state in storage
    {ok, Pid2} = mg_core_events_machine:start_link(Options),
    % Verify migration scenario
    _ = ed209_migration_scenario_succeeds(Options, MachineID, Scenario, C),
    ok = stop_automaton(Pid2),

    ok = proc_lib:stop(StoragePid, normal, 5000).

-spec ed209_migration_scenario_succeeds(options(), mg_core:id(), simple | repair, config()) ->
    any().
ed209_migration_scenario_succeeds(Options, MachineID, simple, _C) ->
    NumEvents1 = ?ED_209_INITIAL_EVENTS + ?ED_209_EMITTED_EVENTS,
    ?assertEqual({_AuxState1 = 1, mk_seq_history(NumEvents1)}, get_machine(Options, MachineID)),
    ?assertEqual(ok, simple_repair(Options, MachineID)),
    ?assertEqual({_AuxState2 = 1, mk_seq_history(NumEvents1)}, get_machine(Options, MachineID)),
    NumFreshEvents = 7,
    NumEvents2 = NumEvents1 + NumFreshEvents,
    ?assertEqual(ok, call(Options, MachineID, {emit, NumFreshEvents})),
    ?assertEqual({_AuxState3 = 2, mk_seq_history(NumEvents2)}, get_machine(Options, MachineID)),
    ?assertEqual(lists:seq(1, NumEvents2), _SinkEvents = ?flushMailbox());
ed209_migration_scenario_succeeds(Options, MachineID, repair, _C) ->
    NumEvents1 = ?ED_209_INITIAL_EVENTS + ?ED_209_EMITTED_EVENTS,
    ?assertEqual({_AuxState1 = 1, mk_seq_history(NumEvents1)}, get_machine(Options, MachineID)),
    NumRepairEvents = 1,
    ?assertEqual(ok, repair(Options, MachineID, {NumEvents1, 1, {emit, NumRepairEvents}})),
    NumEvents2 = NumEvents1 + NumRepairEvents,
    ?assertEqual({_AuxState2 = 2, mk_seq_history(NumEvents2)}, get_machine(Options, MachineID)),
    NumFreshEvents = 7,
    NumEvents3 = NumEvents2 + NumFreshEvents,
    ?assertEqual(ok, call(Options, MachineID, {emit, NumFreshEvents})),
    ?assertEqual({_AuxState3 = 3, mk_seq_history(NumEvents3)}, get_machine(Options, MachineID)),
    ?assertEqual(lists:seq(1, NumEvents3), _SinkEvents = ?flushMailbox()).

-spec mk_seq_history(pos_integer()) -> history().
mk_seq_history(Size) ->
    [{N, N} || N <- lists:seq(1, Size)].

%% Processor handlers

-spec process_signal(options(), req_ctx(), deadline(), mg_core_events_machine:signal_args()) ->
    signal_result().
process_signal(Options, _ReqCtx, _Deadline, {EncodedSignal, Machine}) ->
    Handler = maps:get(signal_handler, Options, fun dummy_signal_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Signal = decode_signal(EncodedSignal),
    Events = extract_events(History),
    ct:pal("call signal handler ~p with [~p, ~p, ~p]", [Handler, Signal, AuxState, Events]),
    {NewAuxState, NewEvents, ComplexAction} = Handler(Signal, AuxState, Events),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    NewEvents1 = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, NewEvents1},
    {StateChange, ComplexAction}.

-spec process_call(options(), req_ctx(), deadline(), mg_core_events_machine:call_args()) ->
    call_result().
process_call(Options, _ReqCtx, _Deadline, {EncodedCall, Machine}) ->
    Handler = maps:get(call_handler, Options, fun dummy_call_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Call = decode(EncodedCall),
    Events = extract_events(History),
    ct:pal("call call handler ~p with [~p, ~p, ~p]", [Handler, Call, AuxState, Events]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Call, AuxState, Events),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    NewEvents1 = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, NewEvents1},
    {encode(Result), StateChange, ComplexAction}.

-spec process_repair(options(), req_ctx(), deadline(), mg_core_events_machine:repair_args()) ->
    repair_result().
process_repair(Options, _ReqCtx, _Deadline, {EncodedArgs, Machine}) ->
    Handler = maps:get(repair_handler, Options, fun dummy_repair_handler/3),
    {AuxState, History} = decode_machine(Machine),
    Args = decode(EncodedArgs),
    Events = extract_events(History),
    ct:pal("call repair handler ~p with [~p, ~p, ~p]", [Handler, Args, AuxState, Events]),
    {Result, NewAuxState, NewEvents, ComplexAction} = Handler(Args, AuxState, Events),
    AuxStateContent = {#{format_version => 1}, encode(NewAuxState)},
    NewEvents1 = [{#{format_version => 1}, encode(E)} || E <- NewEvents],
    StateChange = {AuxStateContent, NewEvents1},
    {ok, {encode(Result), StateChange, ComplexAction}}.

-spec add_events(options(), mg_core:ns(), mg_core:id(), [event()], req_ctx(), deadline()) -> ok.
add_events(Options, _NS, _MachineID, Events, _ReqCtx, _Deadline) ->
    Handler = maps:get(sink_handler, Options, fun dummy_sink_handler/1),
    ct:pal("call sink handler ~p with [~p]", [Handler, Events]),
    ok = Handler(extract_events(decode_history(Events))).

-spec dummy_signal_handler(signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}.
dummy_signal_handler(_Signal, AuxState, _Events) ->
    {AuxState, [], #{}}.

-spec dummy_call_handler(call(), aux_state(), [event()]) -> {ok, aux_state(), [event()], action()}.
dummy_call_handler(_Call, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_repair_handler(term(), aux_state(), [event()]) ->
    {ok, aux_state(), [event()], action()}.
dummy_repair_handler(_Args, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_sink_handler([event()]) -> ok.
dummy_sink_handler(_Events) ->
    ok.

%% Lossy storage

-spec child_spec(map(), atom()) -> supervisor:child_spec() | undefined.
child_spec(#{name := Name, storage := {Module, Options}}, ChildID) ->
    mg_core_storage:child_spec({Module, Options#{name => Name}}, ChildID).

-spec do_request(map(), mg_core_storage:request()) -> mg_core_storage:response().
do_request(Options = #{lossfun := LossFun}, Req = {put, _Key, Context, BodyOpaque, _}) ->
    % Yeah, no easy way to know MachineID here, we're left with Body only
    #{body := {_MD, Data}} = mg_core_events:kv_to_event({<<"42">>, BodyOpaque}),
    case LossFun(decode(Data)) of
        % should be indistinguishable from write loss
        true -> Context;
        false -> delegate_request(Options, Req)
    end;
do_request(Options, Req) ->
    delegate_request(Options, Req).

-spec delegate_request(map(), mg_core_storage:request()) -> mg_core_storage:response().
delegate_request(#{name := Name, pulse := Pulse, storage := {Module, Options}}, Req) ->
    mg_core_storage:do_request({Module, Options#{name => Name, pulse => Pulse}}, Req).

%% Utils

-spec start_automaton(options(), mg_core:ns()) -> pid().
start_automaton(ProcessorOptions, NS) ->
    start_automaton(events_machine_options(ProcessorOptions, NS)).

-spec start_automaton(mg_core_events_machine:options()) ->
    {pid(), mg_core_events_machine:options()}.
start_automaton(Options) ->
    {mg_core_utils:throw_if_error(mg_core_events_machine:start_link(Options)), Options}.

-spec stop_automaton(pid()) -> ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec events_machine_options(options(), mg_core:ns()) -> mg_core_events_machine:options().
events_machine_options(Options, NS) ->
    events_machine_options(#{}, #{}, Options, NS).

-spec events_machine_options(BaseOptions, StorageOptions, options(), mg_core:ns()) ->
    mg_core_events_machine:options()
when
    BaseOptions :: mg_core_events_machine:options(),
    StorageOptions :: mg_core_storage:storage_options().
events_machine_options(Base, StorageOptions, ProcessorOptions, NS) ->
    Scheduler = #{
        min_scan_delay => 1000,
        target_cutoff => 15
    },
    Options = maps:merge(
        #{
            pulse => ?MODULE,
            default_processing_timeout => timer:seconds(10),
            event_stash_size => 5,
            event_sinks => [
                {?MODULE, ProcessorOptions}
            ]
        },
        Base
    ),
    Pulse = maps:get(pulse, Options),
    Storage = {mg_core_storage_memory, StorageOptions},
    Options#{
        namespace => NS,
        processor => {?MODULE, ProcessorOptions},
        tagging => #{
            namespace => <<NS/binary, "_tags">>,
            storage => Storage,
            worker => #{
                registry => mg_core_procreg_gproc
            },
            pulse => Pulse,
            retries => #{}
        },
        machines => #{
            namespace => NS,
            storage => mg_core_ct_helper:build_storage(NS, Storage),
            worker => #{
                registry => mg_core_procreg_gproc
            },
            pulse => Pulse,
            schedulers => #{
                timers => Scheduler,
                timers_retries => Scheduler,
                overseer => #{}
            }
        },
        events_storage => mg_core_ct_helper:build_storage(<<NS/binary, "_events">>, Storage)
    }.

-spec start(mg_core_events_machine:options(), mg_core:id(), term()) -> ok.
start(Options, MachineID, Args) ->
    Deadline = mg_core_deadline:from_timeout(3000),
    mg_core_events_machine:start(Options, MachineID, encode(Args), <<>>, Deadline).

-spec call(mg_core_events_machine:options(), mg_core:id(), term()) -> term().
call(Options, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_core_deadline:from_timeout(3000),
    Response = mg_core_events_machine:call(
        Options,
        {id, MachineID},
        encode(Args),
        HRange,
        <<>>,
        Deadline
    ),
    decode(Response).

-spec repair(mg_core_events_machine:options(), mg_core:id(), term()) -> ok.
repair(Options, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_core_deadline:from_timeout(3000),
    {ok, Response} = mg_core_events_machine:repair(
        Options,
        {id, MachineID},
        encode(Args),
        HRange,
        <<>>,
        Deadline
    ),
    decode(Response).

-spec simple_repair(mg_core_events_machine:options(), mg_core:id()) -> ok.
simple_repair(Options, MachineID) ->
    Deadline = mg_core_deadline:from_timeout(3000),
    mg_core_events_machine:simple_repair(
        Options,
        {id, MachineID},
        <<>>,
        Deadline
    ).

-spec get_history(mg_core_events_machine:options(), mg_core:id()) -> history().
get_history(Options, MachineID) ->
    {_AuxState, History} = get_machine(Options, MachineID),
    History.

-spec get_history(mg_core_events_machine:options(), mg_core:id(), mg_core_events:history_range()) ->
    history().
get_history(Options, MachineID, HRange) ->
    {_AuxState, History} = get_machine(Options, MachineID, HRange),
    History.

-spec get_machine(mg_core_events_machine:options(), mg_core:id()) -> {aux_state(), history()}.
get_machine(Options, MachineID) ->
    HRange = {undefined, undefined, forward},
    get_machine(Options, MachineID, HRange).

-spec get_machine(mg_core_events_machine:options(), mg_core:id(), mg_core_events:history_range()) ->
    {aux_state(), history()}.
get_machine(Options, MachineID, HRange) ->
    Machine = mg_core_events_machine:get_machine(Options, {id, MachineID}, HRange),
    decode_machine(Machine).

-spec extract_events(history()) -> [event()].
extract_events(History) ->
    [Event || {_ID, Event} <- History].

%% Codecs

-spec decode_machine(machine()) -> {aux_state(), History :: [event()]}.
decode_machine(#{aux_state := EncodedAuxState, history := EncodedHistory}) ->
    {decode_aux_state(EncodedAuxState), decode_history(EncodedHistory)}.

-spec decode_aux_state(mg_core_events_machine:aux_state()) -> aux_state().
decode_aux_state({#{format_version := 1}, EncodedAuxState}) ->
    decode(EncodedAuxState);
decode_aux_state({#{}, <<>>}) ->
    <<>>.

-spec decode_history([mg_core_events:event()]) -> [event()].
decode_history(Events) ->
    [
        {ID, decode(EncodedEvent)}
     || #{id := ID, body := {#{}, EncodedEvent}} <- Events
    ].

-spec decode_signal(signal()) -> signal().
decode_signal(timeout) ->
    timeout;
decode_signal({init, Args}) ->
    {init, decode(Args)};
decode_signal({repair, Args}) ->
    {repair, decode(Args)}.

-spec encode(term()) -> binary().
encode(Value) ->
    erlang:term_to_binary(Value).

-spec decode(binary()) -> term().
decode(Value) ->
    erlang:binary_to_term(Value, [safe]).

%% Pulse handler

-include("pulse.hrl").

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, Beat = #mg_core_machine_lifecycle_failed{}) ->
    ct:pal("~p", [Beat]);
handle_beat(_, Beat = #mg_core_machine_lifecycle_transient_error{}) ->
    ct:pal("~p", [Beat]);
handle_beat(quiet, _Beat) ->
    ok;
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
