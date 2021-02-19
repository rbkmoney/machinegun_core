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

-module(mg_core_events_storage).

-define(STORAGE_NS, mg_core_events_machine).

%%

-export([child_spec/1]).
-export([store_event/3]).
-export([store_events/3]).
-export([get_events/3]).

%%

-spec child_spec(mg_core_events_machine:options()) -> supervisor:child_spec().
child_spec(Options) ->
    mg_core_storage:child_spec(events_storage_options(Options), events_storage).

-spec store_event(mg_core_events_machine:options(), mg_core:id(), mg_core_events:event()) -> ok.
store_event(Options, ID, Event) ->
    store_events(Options, ID, [Event]).

-spec store_events(mg_core_events_machine:options(), mg_core:id(), [mg_core_events:event()]) -> ok.
store_events(Options, ID, Events) ->
    lists:foreach(
        fun({Key, Value}) ->
            _ = mg_core_storage:put(events_storage_options(Options), Key, undefined, Value, [])
        end,
        events_to_kvs(ID, Events)
    ).

-spec get_events(mg_core_events_machine:options(), mg_core:id(), mg_core_events:events_range()) ->
    [mg_core_events:event()].
get_events(Options, ID, Range) ->
    Batch = mg_core_dirange:fold(
        fun(EventID, Acc) ->
            Key = mg_core_events:add_machine_id(ID, mg_core_events:event_id_to_key(EventID)),
            mg_core_storage:add_batch_request({get, Key}, Acc)
        end,
        mg_core_storage:new_batch(),
        Range
    ),
    BatchResults = mg_core_storage:run_batch(events_storage_options(Options), Batch),
    lists:map(
        fun({{get, Key}, {_Context, Value}}) ->
            kv_to_event(ID, {Key, Value})
        end,
        BatchResults
    ).

%%

-spec events_storage_options(mg_core_events_machine:options()) -> mg_core_storage:options().
events_storage_options(#{namespace := NS, events_storage := StorageOptions, pulse := Handler}) ->
    {Mod, Options} = mg_core_utils:separate_mod_opts(StorageOptions, #{}),
    {Mod, Options#{name => {NS, ?STORAGE_NS, events}, pulse => Handler}}.

-spec events_to_kvs(mg_core:id(), [mg_core_events:event()]) -> [mg_core_storage:kv()].
events_to_kvs(MachineID, Events) ->
    mg_core_events:add_machine_id(MachineID, mg_core_events:events_to_kvs(Events)).

-spec kv_to_event(mg_core:id(), mg_core_storage:kv()) -> mg_core_events:event().
kv_to_event(MachineID, Kv) ->
    mg_core_events:kv_to_event(mg_core_events:remove_machine_id(MachineID, Kv)).
