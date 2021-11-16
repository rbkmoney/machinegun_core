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

-module(mg_core_events_storage).

-export([child_spec/2]).
-export([get_events/4]).
-export([store_event/4]).
-export([store_events/4]).

-export_type([name/0]).
-export_type([options/0]).

-type name() :: term().
-type options() :: mg_core_utils:mod_opts(storage_options()).
-type storage_options() :: #{
    name := name(),
    processor := module(),
    pulse := mg_core_pulse:handler(),
    atom() => _
}.

-type machine_id() :: mg_core:id().
-type machine_ns() :: mg_core:ns().
-type event() :: mg_core_events:event().
-type events_range() :: mg_core_events:events_range().

-callback get_events(storage_options(), machine_ns(), machine_id(), events_range()) -> [event()].
-callback store_events(storage_options(), machine_ns(), machine_id(), [event()]) -> ok.

%%

-spec child_spec(options(), term()) -> supervisor:child_spec() | undefined.
child_spec(Options, ChildID) ->
    mg_core_utils:apply_mod_opts_if_defined(Options, child_spec, undefined, [ChildID]).

-spec get_events(options(), machine_ns(), machine_id(), events_range()) -> [event()].
get_events(Options, NS, MachineID, Range) ->
    mg_core_utils:apply_mod_opts(Options, get_events, [NS, MachineID, Range]).

-spec store_event(options(), machine_ns(), machine_id(), event()) -> ok.
store_event(Options, NS, MachineID, Event) ->
    store_events(Options, NS, MachineID, [Event]).

-spec store_events(options(), machine_ns(), machine_id(), [event()]) -> ok.
store_events(_Options, _NS, _MachineID, []) ->
    ok;
store_events(Options, NS, MachineID, Events) ->
    mg_core_utils:apply_mod_opts(Options, store_events, [NS, MachineID, Events]).
