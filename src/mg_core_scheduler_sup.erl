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

-module(mg_core_scheduler_sup).

-type id() :: mg_core_scheduler:id().

-type options() :: #{
    % manager
    start_interval => non_neg_integer(),
    capacity := non_neg_integer(),
    quota_name := mg_core_quota_worker:name(),
    quota_share => mg_core_quota:share(),
    % scanner
    queue_handler := mg_core_queue_scanner:queue_handler(),
    max_scan_limit => mg_core_queue_scanner:scan_limit() | unlimited,
    scan_ahead => mg_core_queue_scanner:scan_ahead(),
    retry_scan_delay => mg_core_queue_scanner:scan_delay(),
    squad_opts => mg_core_gen_squad:opts(),
    % workers
    task_handler := mg_core_utils:mod_opts(),
    % common
    pulse => mg_core_pulse:handler()
}.

-export_type([options/0]).

-export([child_spec/3]).
-export([start_link/2]).

%%

-spec child_spec(id(), options(), _ChildID) -> supervisor:child_spec().
child_spec(ID, Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [ID, Options]},
        restart => permanent,
        type => supervisor
    }.

-spec start_link(id(), options()) -> mg_core_utils:gen_start_ret().
start_link(SchedulerID, Options) ->
    ManagerOptions = maps:with(
        [start_interval, capacity, quota_name, quota_share, pulse],
        Options
    ),
    ScannerOptions = maps:with(
        [queue_handler, max_scan_limit, scan_ahead, retry_scan_delay, squad_opts, pulse],
        Options
    ),
    WorkerOptions = maps:with(
        [task_handler, pulse],
        Options
    ),
    mg_core_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        mg_core_utils:lists_compact([
            mg_core_queue_scanner:child_spec(SchedulerID, ScannerOptions, queue),
            mg_core_scheduler_worker:child_spec(SchedulerID, WorkerOptions, tasks),
            mg_core_scheduler:child_spec(SchedulerID, ManagerOptions, manager)
        ])
    ).
