%%%
%%% Copyright 2017 RBKmoney
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

-module(mg_core_events_sink_machine).

%% API
-export_type([event_body/0]).
-export_type([options/0]).
-export_type([namespace_options/0]).
-export_type([storage_options/0]).

-export([child_spec/2]).
-export([make_namespace_options/1]).

-export([get_history/2]).
-export([repair/3]).

%% mg_core_events_sink handler
-behaviour(mg_core_events_sink).
-export([add_events/6]).

%% mg_core_machine handler
-behaviour(mg_core_machine).
-export([get_machine/4]).
-export([process_machine/7]).
-export([processor_child_spec/2]).

%%
%% API
%%
-type event_body() :: #{
    source_ns => mg_core:ns(),
    source_id => mg_core:id(),
    event     => mg_core_events:event()
}.
-type event() :: mg_core_events:event(event_body()).

-type namespace_options() :: #{
    name := atom(),
    namespace := mg_core:ns(),
    registry := mg_core_procreg:options(),
    pulse := mg_core_pulse:handler(),
    storage := mg_core_namespace:storage_options(),
    events_storage := storage_options(),
    machine => mg_core_namespace:machine_options(),
    workers_manager => mg_core_namespace:workers_manager_options()
}.

-type options() :: #{
    name := atom(),
    namespace_options_ref := mg_core_namespace:options_ref(),
    machine_id := mg_core:id()
}.

-type storage_options() :: mg_core_utils:mod_opts(map()).  % like mg_core_storage:options() except `name`

%% Internal types

-type processor_options() :: #{
    name := atom(),
    namespace := mg_core:ns(),
    pulse := mg_core_pulse:handler(),
    events_storage := storage_options()
}.

%% API

-spec child_spec(options(), term()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_core_namespace:child_spec(namespace_options_ref(Options), ChildID).

-spec make_namespace_options(namespace_options()) ->
    mg_core_namespace:options().
make_namespace_options(Options) ->
    Namespace = maps:get(namespace, Options),
    NSOptions = maps:with([registry, pulse, storage, machine, workers_manager], Options),
    NSOptions#{
        namespace => mg_core_utils:concatenate_namespaces(Namespace, <<"machines">>),
        processor => {?MODULE, #{
            name => maps:get(name, Options),
            namespace => maps:get(namespace, Options),
            events_storage => events_storage_options(Options),
            pulse => maps:get(pulse, Options)
        }}
    }.

-spec add_events(Options, SourceNS, SourceMachineID, Events, ReqCtx, Deadline) -> ok when
    Options :: options(),
    SourceNS :: mg_core:ns(),
    SourceMachineID :: mg_core:id(),
    Events :: [mg_core_events:event()],
    ReqCtx :: mg_core:request_context(),
    Deadline :: mg_core_deadline:deadline().
add_events(Options, SourceNS, SourceMachineID, Events, ReqCtx, Deadline) ->
    #{machine_id := EventSinkID, namespace_options_ref := NSOptionsRef} = Options,
    ok = mg_core_namespace:call_with_lazy_start(
            NSOptionsRef,
            EventSinkID,
            {add_events, SourceNS, SourceMachineID, Events},
            ReqCtx,
            Deadline,
            undefined
        ).

-spec get_history(options(), mg_core_events:history_range()) ->
    [event()].
get_history(#{machine_id := EventSinkID} = Options, HistoryRange) ->
    #{events_range := EventsRange} = get_state(Options, EventSinkID),
    StorageOptions = load_storage_options(Options),
    Batch = mg_core_dirange:fold(
        fun (EventID, Batch) ->
            Key = mg_core_events:add_machine_id(EventSinkID, mg_core_events:event_id_to_key(EventID)),
            mg_core_storage:add_batch_request({get, Key}, Batch)
        end,
        mg_core_storage:new_batch(),
        mg_core_events:intersect_range(EventsRange, HistoryRange)
    ),
    BatchResults = mg_core_storage:run_batch(StorageOptions, Batch),
    lists:map(
        fun ({{get, Key}, {_Context, Value}}) ->
            kv_to_sink_event(EventSinkID, {Key, Value})
        end,
        BatchResults
    ).

-spec repair(options(), mg_core:request_context(), mg_core_deadline:deadline()) ->
    ok.
repair(#{machine_id := EventSinkID} = Options, ReqCtx, Deadline) ->
    mg_core_namespace:repair(namespace_options_ref(Options), EventSinkID, undefined, ReqCtx, Deadline).

%%
%% mg_core_processor handler
%%
-type state() :: #{
    events_range => mg_core_events:events_range()
}.

-spec processor_child_spec(processor_options(), term()) ->
    supervisor:child_spec() | undefined.
processor_child_spec(Options, ChildID) ->
    mg_core_storage:child_spec(events_storage_options(Options), ChildID).

-spec get_machine(Options, ID, Args, PackedState) -> Result when
    Options :: processor_options(),
    ID :: mg_core:id(),
    Args :: undefined,
    PackedState :: mg_core_machine:machine_state(),
    Result :: state().
get_machine(_Options, _ID, undefined, State) ->
    opaque_to_state(State).

-spec process_machine(Options, EventSinkID, Impact, PCtx, ReqCtx, Deadline, PackedState) -> Result when
    Options :: processor_options(),
    EventSinkID :: mg_core:id(),
    Impact :: mg_core_machine:processor_impact(),
    PCtx :: mg_core_machine:processing_context(),
    ReqCtx :: mg_core:request_context(),
    Deadline :: mg_core_deadline:deadline(),
    PackedState :: mg_core_machine:machine_state(),
    Result :: mg_core_machine:processor_result().
process_machine(Options, EventSinkID, Impact, _PCtx, _ReqCtx, _Deadline, PackedState) ->
    State =
        case {Impact, PackedState} of
            {{init, _}, null} -> new_state();
            {_        , _   } -> opaque_to_state(PackedState)
        end,
    NewState = process_machine_(Options, EventSinkID, Impact, State),
    {{reply, ok}, sleep, state_to_opaque(NewState)}.

-spec process_machine_(processor_options(), mg_core:id(), mg_core_machine:processor_impact(), state()) ->
    state().
process_machine_(_, _, {init, undefined}, State) ->
    State;
process_machine_(_, _, {repair, undefined}, State) ->
    State;
process_machine_(Options, EventSinkID, {call, {add_events, SourceNS, SourceMachineID, Events}}, State) ->
    {SinkEvents, NewState} = generate_sink_events(SourceNS, SourceMachineID, Events, State),
    ok = store_sink_events(Options, EventSinkID, SinkEvents),
    NewState.

%%

-spec store_sink_events(processor_options(), mg_core:id(), [event()]) ->
    ok.
store_sink_events(Options, EventSinkID, SinkEvents) ->
    lists:foreach(
        fun(SinkEvent) ->
            store_event(Options, EventSinkID, SinkEvent)
        end,
        SinkEvents
    ).

-spec store_event(processor_options(), mg_core:id(), event()) ->
    ok.
store_event(#{events_storage := Storage}, EventSinkID, SinkEvent) ->
    {Key, Value} = sink_event_to_kv(EventSinkID, SinkEvent),
    _ = mg_core_storage:put(Storage, Key, undefined, Value, []),
    ok.

-spec get_state(options(), mg_core:id()) ->
    state().
get_state(Options, EventSinkID) ->
    #{namespace_options_ref := NSOptionsRef} = Options,
    try
        mg_core_namespace:get_machine(NSOptionsRef, EventSinkID, undefined)
    catch throw:{logic, machine_not_found} ->
        new_state()
    end.

-spec new_state() ->
    state().
new_state() ->
    #{events_range => undefined}.

-spec events_storage_options(namespace_options() | processor_options()) ->
    mg_core_storage:options().
events_storage_options(#{namespace := NS, events_storage := StorageOptions, pulse := Handler}) ->
    {Mod, Options} = mg_core_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, ?MODULE, events}, pulse => Handler}}.

-spec load_storage_options(options()) ->
    mg_core_storage:options().
load_storage_options(#{namespace_options_ref := NSOptionsRef}) ->
    {?MODULE, ProcessorOptions} = mg_core_namespace:load_processor_options(NSOptionsRef),
    events_storage_options(ProcessorOptions).

-spec namespace_options_ref(options()) ->
    mg_core_namespace:options_ref().
namespace_options_ref(Options) ->
    maps:get(namespace_options_ref, Options).

%%

-spec generate_sink_events(mg_core:ns(), mg_core:id(), [mg_core_events:event()], state()) ->
    {[event()], state()}.
generate_sink_events(SourceNS, SourceMachineID, Events, State=#{events_range:=EventsRange}) ->
    Bodies = [generate_sink_event_body(SourceNS, SourceMachineID, Event) || Event <- Events],
    {SinkEvents, NewEventsRange} = mg_core_events:generate_events_with_range(Bodies, EventsRange),
    {SinkEvents, State#{events_range := NewEventsRange}}.

-spec generate_sink_event_body(mg_core:ns(), mg_core:id(), mg_core_events:event()) ->
    event_body().
generate_sink_event_body(SourceNS, SourceMachineID, Event) ->
    #{
        source_ns => SourceNS,
        source_id => SourceMachineID,
        event     => Event
    }.


%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_core_storage:opaque().
state_to_opaque(#{events_range := EventsRange}) ->
    [1, mg_core_events:events_range_to_opaque(EventsRange)].

-spec opaque_to_state(mg_core_storage:opaque()) ->
    state().
opaque_to_state([1, EventsRange]) ->
    #{
        events_range => mg_core_events:opaque_to_events_range(EventsRange)
    }.

-spec sink_event_body_to_opaque(Vsn :: integer(), event_body()) ->
    mg_core_storage:opaque().
sink_event_body_to_opaque(_Vsn, #{source_ns := SourceNS, source_id := SourceMachineID, event := Event}) ->
    [1, SourceNS, SourceMachineID, mg_core_events:event_to_opaque(Event)].

-spec opaque_to_sink_event_body(Vsn :: integer(), mg_core_storage:opaque()) ->
    event_body().
opaque_to_sink_event_body(_Vsn, [1, SourceNS, SourceMachineID, Event]) ->
    #{
        source_ns => SourceNS,
        source_id => SourceMachineID,
        event     => mg_core_events:opaque_to_event(Event)
    }.

-spec sink_event_to_kv(mg_core:id(), event()) ->
    mg_core_storage:kv().
sink_event_to_kv(EventSinkID, Event) ->
    mg_core_events:add_machine_id(EventSinkID, mg_core_events:event_to_kv(Event, fun sink_event_body_to_opaque/2)).

-spec kv_to_sink_event(mg_core:id(), mg_core_storage:kv()) ->
    event().
kv_to_sink_event(EventSinkID, Kvs) ->
    mg_core_events:kv_to_event(mg_core_events:remove_machine_id(EventSinkID, Kvs), fun opaque_to_sink_event_body/2).
