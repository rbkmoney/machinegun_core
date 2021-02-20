%%%
%%% Copyright 2020 RBKmoney
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

-module(mg_core_events_modernizer_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("ct_helper.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests

-export([modernizer_test/1]).

%% mg_core_events_machine handler
-behaviour(mg_core_events_machine).
-export_type([options/0]).
-export([process_signal/4, process_call/4, process_repair/4]).

%% mg_core_events_modernizer handler
-behaviour(mg_core_events_modernizer).
-export([modernize_event/3]).

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
        modernizer_test
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

%% Tests

-spec modernizer_test(config()) -> any().
modernizer_test(_C) ->
    NS = <<"test">>,
    MachineID = genlib:unique(),
    ProcessorOptions = #{
        signal_handler => fun({init, <<>>}, AuxState, []) -> {AuxState, [test], #{}} end
    },
    ModernizerOptons = #{
        current_format_version => 2,
        handler => {?MODULE, #{}}
    },
    {Pid, Options} = start_automaton(ProcessorOptions, NS),
    ok = start(Options, MachineID, <<>>),
    _ = ?assertEqual([{1, #{format_version => 1}, test}], get_history(Options, MachineID)),
    ok = modernize(ModernizerOptons, Options, MachineID),
    _ = ?assertEqual([{1, #{format_version => 2}, test}], get_history(Options, MachineID)),
    ok = stop_automaton(Pid).

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

%% Modernizer handler

-spec modernize_event(_, _, mg_core_events_modernizer:machine_event()) ->
    mg_core_events_modernizer:modernized_event_body().
modernize_event(_, _, #{event := #{body := {#{format_version := FV}, Data}}}) ->
    {#{format_version => FV + 1}, Data}.

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
events_machine_options(ProcessorOptions, NS) ->
    Pulse = ?MODULE,
    Storage = {mg_core_storage_memory, #{}},
    #{
        pulse => Pulse,
        event_stash_size => 0,
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
            pulse => Pulse
        },
        events_storage => mg_core_ct_helper:build_storage(<<NS/binary, "_events">>, Storage)
    }.

-spec start(mg_core_events_machine:options(), mg_core:id(), term()) -> ok.
start(Options, MachineID, Args) ->
    Deadline = mg_core_deadline:from_timeout(3000),
    mg_core_events_machine:start(Options, MachineID, encode(Args), <<>>, Deadline).

-spec modernize(
    mg_core_events_modernizer:options(),
    mg_core_events_machine:options(),
    mg_core:id()
) -> ok.
modernize(Options, EventsMachineOptions, MachineID) ->
    mg_core_events_modernizer:modernize_machine(
        Options,
        EventsMachineOptions,
        <<>>,
        {id, MachineID},
        {undefined, undefined, forward}
    ).

-spec get_history(mg_core_events_machine:options(), mg_core:id()) -> history().
get_history(Options, MachineID) ->
    HRange = {undefined, undefined, forward},
    get_history(Options, MachineID, HRange).

-spec get_history(mg_core_events_machine:options(), mg_core:id(), mg_core_events:history_range()) ->
    history().
get_history(Options, MachineID, HRange) ->
    Machine = mg_core_events_machine:get_machine(Options, {id, MachineID}, HRange),
    {_AuxState, History} = decode_machine(Machine),
    History.

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
        {ID, Metadata, decode(EncodedEvent)}
        || #{id := ID, body := {Metadata, EncodedEvent}} <- Events
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
