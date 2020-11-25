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

-module(mg_core_queue_interrupted).


-behaviour(mg_core_queue_scanner).
-export([init/1]).
-export([search_tasks/3]).

-behaviour(mg_core_scheduler_worker).
-export([execute_task/2]).

%% Types

-type milliseconds() :: non_neg_integer().
-type options() :: #{
    namespace := mg_core:ns(),
    scheduler_name := mg_core_scheduler:name(),
    pulse := mg_core_pulse:handler(),
    namespace_options_ref := mg_core_namespace:options_ref(),
    min_scan_delay => milliseconds(),
    rescan_delay => milliseconds(),
    processing_timeout => timeout()
}.
-record(state, {
    continuation :: mg_core_storage:continuation()
}).
-opaque state() :: #state{}.

-export_type([state/0]).
-export_type([options/0]).

%% Internal types

-type task_id() :: mg_core:id().
-type task_payload() :: #{}.
-type task() :: mg_core_queue_task:task(task_id(), task_payload()).
-type scan_delay() :: mg_core_queue_scanner:scan_delay().

-define(DEFAULT_PROCESSING_TIMEOUT, 60000).  % 1 minute

%%
%% API
%%

-spec init(options()) ->
    {ok, state()}.
init(_Options) ->
    {ok, #state{continuation = undefined}}.

-spec search_tasks(options(), _Limit :: non_neg_integer(), state()) ->
    {{scan_delay(), [task()]}, state()}.
search_tasks(Options, Limit, #state{continuation = Continuation} = State) ->
    CurrentTime = mg_core_queue_task:current_time(),
    NSOptions = namespace_options_ref(Options),
    Query = processing,
    {IDs, NewContinuation} = mg_core_namespace:search(NSOptions, Query, Limit, Continuation),
    Tasks = [#{id => ID, machine_id => ID, target_time => CurrentTime} || ID <- IDs],
    Delay = get_delay(NewContinuation, Options),
    NewState = State#state{continuation = NewContinuation},
    {{Delay, Tasks}, NewState}.

-spec execute_task(options(), task()) ->
    ok.
execute_task(Options, #{machine_id := MachineID}) ->
    Timeout = maps:get(processing_timeout, Options, ?DEFAULT_PROCESSING_TIMEOUT),
    Deadline = mg_core_deadline:from_timeout(Timeout),
    ok = mg_core_namespace:resume_interrupted(namespace_options_ref(Options), MachineID, Deadline).

%% Internals

-spec namespace_options_ref(options()) ->
    mg_core_namespace:options_ref().
namespace_options_ref(#{namespace_options_ref := NSOptions}) ->
    NSOptions.

-spec get_delay(mg_core_storage:continuation(), options()) ->
    scan_delay().
get_delay(undefined, Options) ->
    maps:get(rescan_delay, Options, 10 * 60 * 1000);
get_delay(_Other, Options) ->
    maps:get(min_scan_delay, Options, 1000).
