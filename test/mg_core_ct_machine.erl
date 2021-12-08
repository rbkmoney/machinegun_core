-module(mg_core_ct_machine).

-export([start/3]).
-export([call/3]).
-export([history/2]).
-export([history/3]).

%% mg_core_events_machine handler
-behaviour(mg_core_events_machine).
-export([process_signal/4, process_call/4, process_repair/4]).

%% mg_core_events_sink handler
-behaviour(mg_core_events_sink).
-export([add_events/6]).

-type options() :: mg_core_events_machine:options().
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
    call_handler => fun(
        (call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}
    ),
    sink_handler => fun(([event()]) -> ok)
}.

%%

-spec start(options(), mg_core:id(), term()) -> ok.
start(Options, MachineID, Args) ->
    Deadline = mg_core_deadline:from_timeout(3000),
    mg_core_events_machine:start(Options, MachineID, encode(Args), <<>>, Deadline).

-spec call(options(), mg_core:id(), term()) -> term().
call(Options, MachineID, Args) ->
    HRange = {undefined, undefined, forward},
    Deadline = mg_core_deadline:from_timeout(3000),
    Result = mg_core_events_machine:call(
        Options,
        {id, MachineID},
        encode(Args),
        HRange,
        <<>>,
        Deadline
    ),
    decode(Result).

-spec history(options(), mg_core:id()) -> ok.
history(Options, MachineID) ->
    history(Options, MachineID, {undefined, undefined, forward}).

-spec history(options(), mg_core:id(), mg_core_events:history_range()) -> ok.
history(Options, MachineID, HRange) ->
    Machine = mg_core_events_machine:get_machine(Options, {id, MachineID}, HRange),
    {_AuxState, History} = decode_machine(Machine),
    History.

%% Processor

-spec process_signal(handlers(), req_ctx(), deadline(), mg_core_events_machine:signal_args()) ->
    signal_result().
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

-spec process_call(handlers(), req_ctx(), deadline(), mg_core_events_machine:call_args()) ->
    call_result().
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

-spec process_repair(options(), req_ctx(), deadline(), mg_core_events_machine:repair_args()) ->
    repair_result().
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

-spec add_events(handlers(), mg_core:ns(), mg_core:id(), [event()], req_ctx(), deadline()) -> ok.
add_events(Handlers, _NS, _MachineID, Events, _ReqCtx, _Deadline) ->
    Handler = maps:get(sink_handler, Handlers, fun dummy_sink_handler/1),
    ct:pal("call sink handler ~p with [~p]", [Handler, Events]),
    ok = Handler(decode_events(Events)).

-spec dummy_signal_handler(signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}.
dummy_signal_handler(_Signal, AuxState, _Events) ->
    {AuxState, [], #{}}.

-spec dummy_call_handler(signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}.
dummy_call_handler(_Signal, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_repair_handler(term(), aux_state(), [event()]) ->
    {ok, aux_state(), [event()], action()}.
dummy_repair_handler(_Args, AuxState, _Events) ->
    {ok, AuxState, [], #{}}.

-spec dummy_sink_handler([event()]) -> ok.
dummy_sink_handler(_Events) ->
    ok.

%% Codecs

-spec decode_machine(machine()) -> {aux_state(), History :: [event()]}.
decode_machine(#{aux_state := EncodedAuxState, history := EncodedHistory}) ->
    {decode_aux_state(EncodedAuxState), decode_events(EncodedHistory)}.

-spec decode_aux_state(mg_core_events_machine:aux_state()) -> aux_state().
decode_aux_state({#{format_version := 1}, EncodedAuxState}) ->
    decode(EncodedAuxState);
decode_aux_state({#{}, <<>>}) ->
    <<>>.

-spec decode_events([mg_core_events:event()]) -> [event()].
decode_events(Events) ->
    [
        decode(EncodedEvent)
     || #{body := {#{}, EncodedEvent}} <- Events
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
