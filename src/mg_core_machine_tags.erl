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

-module(mg_core_machine_tags).

%% API
-export_type([start_options/0]).
-export_type([call_options/0]).
-export_type([tag/0]).

-export([child_spec/2]).
-export([add/5]).
-export([replace/5]).
-export([resolve/2]).
-export([is_target_exist/2]).

%% mg_core_machine handler
-behaviour(mg_core_machine).
-export([get_machine/4]).
-export([process_machine/7]).

-type start_options() :: #{
    namespace := mg_core:ns(),
    registry := mg_core_procreg:options(),
    pulse := mg_core_pulse:handler(),
    storage := mg_core_namespace:storage_options(),
    target := mg_core_namespace:call_options(),
    worker => mg_core_namespace:worker_options(),
    machine => mg_core_namespace:machine_options(),
    workers_manager => mg_core_namespace:workers_manager_start_options()
}.

-type call_options() :: #{
    namespace := mg_core:ns(),
    registry := mg_core_procreg:options(),
    pulse := mg_core_pulse:handler(),
    storage := mg_core_namespace:storage_options(),
    target := mg_core_namespace:call_options(),
    workers_manager => mg_core_namespace:workers_manager_call_options()
}.

-type tag() :: binary().

%% Internal types

-type deadline() :: mg_core_deadline:deadline().
-type req_ctx() :: mg_core:request_context().

-type processor_start_options() :: #{
    target := mg_core_namespace:start_options()
}.

-type processor_call_options() :: #{
    target := mg_core_namespace:call_options()
}.

%% API

-spec child_spec(start_options(), term()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    mg_core_namespace:child_spec(namespace_options(Options), ChildID).

-spec add(call_options(), tag(), mg_core:id(), req_ctx(), deadline()) ->
    ok | {already_exists, mg_core:id()} | no_return().
add(Options, Tag, ID, ReqCtx, Deadline) ->
    mg_core_namespace:call_with_lazy_start(
        namespace_options(Options),
        Tag,
        {add, ID},
        ReqCtx,
        Deadline,
        undefined
    ).

-spec replace(call_options(), tag(), mg_core:id(), req_ctx(), deadline()) ->
    ok | no_return().
replace(Options, Tag, ID, ReqCtx, Deadline) ->
    mg_core_namespace:call_with_lazy_start(
        namespace_options(Options),
        Tag,
        {replace, ID},
        ReqCtx,
        Deadline,
        undefined
    ).

-spec is_target_exist(call_options(), mg_core:id()) ->
    boolean().
is_target_exist(Options, ID) ->
    mg_core_namespace:is_exist(target_options(Options), ID).

-spec resolve(call_options(), tag()) ->
    mg_core:id() | undefined | no_return().
resolve(Options, Tag) ->
    try
        mg_core_namespace:get_machine(namespace_options(Options), Tag, undefined)
    catch throw:{logic, machine_not_found} ->
        undefined
    end.

%%
%% mg_core_machine handler
%%
-type state() :: mg_core:id() | undefined.

-spec get_machine(Options, ID, Args, PackedState) -> Result when
    Options :: processor_call_options(),
    ID :: mg_core:id(),
    Args :: undefined,
    PackedState :: mg_core_machine:machine_state(),
    Result :: state().
get_machine(_Options, _ID, undefined, State) ->
    opaque_to_state(State).

-spec process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, PackedState) -> Result when
    Options :: processor_start_options(),
    ID :: mg_core:id(),
    Impact :: mg_core_machine:processor_impact(),
    PCtx :: mg_core_machine:processing_context(),
    ReqCtx :: req_ctx(),
    Deadline :: deadline(),
    PackedState :: mg_core_machine:machine_state(),
    Result :: mg_core_machine:processor_result().
process_machine(_Options, _ID, {init, undefined}, _PCtx, _ReqCtx, _Deadline, _PackedState) ->
    {{reply, ok}, sleep, state_to_opaque(undefined)};
process_machine(_Options, _ID, {repair, undefined}, _PCtx, _ReqCtx, _Deadline, PackedState) ->
    {{reply, ok}, sleep, PackedState};
process_machine(_Options, _ID, {call, {add, ID}}, _PCtx, _ReqCtx, _Deadline, PackedState) ->
    case opaque_to_state(PackedState) of
        undefined ->
            {{reply, ok}, sleep, state_to_opaque(ID)};
        ID ->
            {{reply, ok}, sleep, PackedState};
        OtherID ->
            {{reply, {already_exists, OtherID}}, sleep, PackedState}
    end;
process_machine(_Options, _ID, {call, {replace, ID}}, _PCtx, _ReqCtx, _Deadline, _PackedState) ->
    {{reply, ok}, sleep, state_to_opaque(ID)}.

%%
%% local
%%
-spec namespace_options(start_options() | call_options()) ->
    mg_core_namespace:start_options() | mg_core_namespace:call_options().
namespace_options(Options) ->
    NSOptions = maps:with([namespace, registry, pulse, storage, worker, machine, workers_manager], Options),
    NSOptions#{
        processor => {?MODULE, #{
            target => maps:get(target, Options)
        }}
    }.

-spec target_options(start_options() | call_options()) ->
    mg_core_namespace:start_options() | mg_core_namespace:call_options().
target_options(Options) ->
    maps:get(target, Options).

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) ->
    mg_core_storage:opaque().
state_to_opaque(undefined) ->
    [1, null];
state_to_opaque(ID) ->
    [1, ID].

-spec opaque_to_state(mg_core_storage:opaque()) ->
    state().
opaque_to_state([1, null]) ->
    undefined;
opaque_to_state([1, ID]) ->
    ID.
