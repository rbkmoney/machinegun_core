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

%%%
%%% Storage utility functions for use with mg_core_events_machine
%%%

-module(mg_core_events_storage_kvs).

%%

-behaviour(mg_core_events_storage).
-export([child_spec/2]).
-export([store_events/4]).
-export([get_events/4]).

-type options() :: #{
    % Base
    name := mg_core_machine_storage:name(),
    pulse := mg_core_pulse:handler(),
    % KV Storage
    kvs := mg_core_storage:options()
}.

-type machine_id() :: mg_core:id().
-type machine_ns() :: mg_core:ns().
-type event() :: mg_core_events:event().
-type events_range() :: mg_core_events:events_range().

%%

-spec child_spec(options(), _ChildID) -> supervisor:child_spec() | undefined.
child_spec(Options, ChildID) ->
    mg_core_storage:child_spec(kvs_options(Options), ChildID).

-spec store_events(options(), machine_ns(), machine_id(), [event()]) -> ok.
store_events(Options, _NS, ID, Events) ->
    lists:foreach(
        fun({Key, Value}) ->
            _ = mg_core_storage:put(kvs_options(Options), Key, undefined, Value, [])
        end,
        events_to_kvs(ID, Events)
    ).

-spec get_events(options(), machine_ns(), machine_id(), events_range()) -> [event()].
get_events(Options, _NS, ID, Range) ->
    Batch = mg_core_dirange:fold(
        fun(EventID, Acc) ->
            Key = mg_core_events:add_machine_id(ID, mg_core_events:event_id_to_key(EventID)),
            mg_core_storage:add_batch_request({get, Key}, Acc)
        end,
        mg_core_storage:new_batch(),
        Range
    ),
    BatchResults = mg_core_storage:run_batch(kvs_options(Options), Batch),
    lists:map(
        fun({{get, Key}, {_Context, Value}}) ->
            kv_to_event(ID, {Key, Value})
        end,
        BatchResults
    ).

%%

-spec events_to_kvs(mg_core:id(), [mg_core_events:event()]) -> [mg_core_storage:kv()].
events_to_kvs(MachineID, Events) ->
    mg_core_events:add_machine_id(MachineID, mg_core_events:events_to_kvs(Events)).

-spec kv_to_event(mg_core:id(), mg_core_storage:kv()) -> mg_core_events:event().
kv_to_event(MachineID, Kv) ->
    mg_core_events:kv_to_event(mg_core_events:remove_machine_id(MachineID, Kv)).

kvs_options(#{name := Name, pulse := Handler, kvs := KVSOptions}) ->
    {Mod, Options} = mg_core_utils:separate_mod_opts(KVSOptions, #{}),
    {Mod, Options#{name => Name, pulse => Handler}}.
