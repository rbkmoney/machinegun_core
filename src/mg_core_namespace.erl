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
%%% Namespace process tree and functions for working with it
%%%
-module(mg_core_namespace).

-export_type([options_ref/0]).
-export_type([options/0]).
-export_type([storage_options/0]).
-export_type([machine_options/0]).
-export_type([schedulers_options/0]).
-export_type([workers_manager_options/0]).

-export([child_spec/2]).
-export([start_link/1]).

-export([make_options_ref/1]).
-export([save_options/2]).
-export([load_options/1]).
-export([load_processor_options/1]).

-export([start/5]).
-export([simple_repair/4]).
-export([repair/5]).
-export([call/5]).
-export([send_timeout/4]).
-export([resume_interrupted/3]).
-export([fail/4]).
-export([fail/5]).
-export([get_machine/3]).
-export([get_status/2]).
-export([is_exist/2]).
-export([search/2]).
-export([search/3]).
-export([search/4]).
-export([call_with_lazy_start/6]).

%% Types

-type options() :: #{
    namespace := mg_core:ns(),
    registry := mg_core_procreg:options(),
    pulse := mg_core_pulse:handler(),
    storage := storage_options(),
    processor := mg_core_machine:processor_options(),
    machine => machine_options(),
    schedulers => schedulers_options(),
    workers_manager => workers_manager_options()
}.

-opaque options_ref() :: {atom(), mg_core:ns()}.

%% Internal types

-type id() :: mg_core:id().
-type call() :: mg_core_machine:call().
-type req_ctx() :: mg_core:request_context().
-type maybe(T) :: T | undefined.
-type deadline() :: mg_core_deadline:deadline().
-type seconds() :: non_neg_integer().

-type storage_options() :: mg_core_utils:mod_opts(#{atom() => any()}).  % like mg_core_storage:options() except `name`
-type scheduler_type() :: overseer | timers | timers_retries.
-type scheduler_options() :: disable | #{
    % how much tasks in total scheduler is ready to enqueue for processing
    capacity => non_neg_integer(),
    % wait at least this delay before subsequent scanning of persistent store for queued tasks
    min_scan_delay => mg_core_queue_scanner:scan_delay(),
    % wait at most this delay before subsequent scanning attempts when queue appears to be empty
    rescan_delay => mg_core_queue_scanner:scan_delay(),
    % how many tasks to fetch at most
    max_scan_limit => mg_core_queue_scanner:scan_limit(),
    % by how much to adjust limit to account for possibly duplicated tasks
    scan_ahead => mg_core_queue_scanner:scan_ahead(),
    % how many seconds in future a task can be for it to be sent to the local scheduler
    target_cutoff => seconds(),
    % name of quota limiting number of active tasks
    task_quota => mg_core_quota_worker:name(),
    % share of quota limit
    task_share => mg_core_quota:share()
}.
-type schedulers_options() :: #{scheduler_type() => scheduler_options()}.
-type workers_manager_options() :: #{
    sidecar => mg_core_utils:mod_opts(),
    message_queue_len_limit => mg_core_workers_manager:queue_limit(),
    shutdown => mg_core_workers_manager:shutdown()
}.
-type machine_options() :: #{
    retries => mg_core_machine:retry_opt(),
    suicide_probability => mg_core_machine:suicide_probability(),
    timer_processing_timeout => timeout(),
    unload_timeout => timeout(),
    hibernate_timeout => timeout()
}.

%% Constants

-define(DEFAULT_SCHEDULER_CAPACITY, 1000).

%% API

-spec child_spec(options_ref(), term()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options_ref()) ->
    mg_core_utils:gen_start_ret().
start_link(OptionsRef) ->
    Options = load_options(OptionsRef),
    mg_core_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_one},
        [
            mg_core_storage:child_spec(storage_options(Options), storage),
            machine_sup_child_spec(Options, machine_sup),
            scheduler_sup_child_spec(Options, scheduler_sup)
        ]
    ).

-spec make_options_ref(mg_core:ns()) ->
    options_ref().
make_options_ref(NS) ->
    {?MODULE, NS}.

-spec save_options(options(), options_ref()) ->
    ok | {error, {exists, options()}}.
save_options(Options, Ref) ->
    case persistent_term:get(Ref, undefined) of
        undefined ->
            % Races are possible in this place.
            % However, I assume that this code will be called on
            % the application initialization, so no one would
            % think of calling it concurrently.
            persistent_term:put(Ref, Options);
        Other ->
            {error, {exists, Other}}
    end.

-spec load_options(options_ref()) ->
    options().
load_options(Ref) ->
    persistent_term:get(Ref).

-spec load_processor_options(options_ref()) ->
    mg_core_machine:processor_options().
load_processor_options(OptionsRef) ->
    Options = load_options(OptionsRef),
    maps:get(processor, Options).

%% Machine API

-spec start(options_ref(), mg_core:id(), term(), req_ctx(), deadline()) ->
    _Resp | no_return().
start(OptionsRef, ID, Args, ReqCtx, Deadline) ->
    call_(OptionsRef, ID, {start, Args}, ReqCtx, Deadline).

-spec simple_repair(options_ref(), mg_core:id(), req_ctx(), deadline()) ->
    _Resp | no_return().
simple_repair(OptionsRef, ID, ReqCtx, Deadline) ->
    call_(OptionsRef, ID, simple_repair, ReqCtx, Deadline).

-spec repair(options_ref(), mg_core:id(), term(), req_ctx(), deadline()) ->
    _Resp | no_return().
repair(OptionsRef, ID, Args, ReqCtx, Deadline) ->
    call_(OptionsRef, ID, {repair, Args}, ReqCtx, Deadline).

-spec call(options_ref(), mg_core:id(), term(), req_ctx(), deadline()) ->
    _Resp | no_return().
call(OptionsRef, ID, Call, ReqCtx, Deadline) ->
    call_(OptionsRef, ID, {call, Call}, ReqCtx, Deadline).

-spec send_timeout(options_ref(), mg_core:id(), genlib_time:ts(), deadline()) ->
    _Resp | no_return().
send_timeout(OptionsRef, ID, Timestamp, Deadline) ->
    call_(OptionsRef, ID, {timeout, Timestamp}, undefined, Deadline).

-spec resume_interrupted(options_ref(), mg_core:id(), deadline()) ->
    _Resp | no_return().
resume_interrupted(OptionsRef, ID, Deadline) ->
    call_(OptionsRef, ID, resume_interrupted_one, undefined, Deadline).

-spec fail(options_ref(), mg_core:id(), req_ctx(), deadline()) ->
    ok.
fail(OptionsRef, ID, ReqCtx, Deadline) ->
    fail(OptionsRef, ID, {error, explicit_fail, []}, ReqCtx, Deadline).

-spec fail(options_ref(), mg_core:id(), mg_core_utils:exception(), req_ctx(), deadline()) ->
    ok.
fail(OptionsRef, ID, Exception, ReqCtx, Deadline) ->
    call_(OptionsRef, ID, {fail, Exception}, ReqCtx, Deadline).

-spec get_machine(options_ref(), mg_core:id(), mg_core_machine:get_machine_args()) ->
    mg_core_machine:processor_machine_state() | no_return().
get_machine(OptionsRef, ID, Args) ->
    mg_core_machine:get_machine(load_machine_options(OptionsRef), ID, Args).

-spec get_status(options_ref(), mg_core:id()) ->
    mg_core_machine:machine_status() | no_return().
get_status(OptionsRef, ID) ->
    mg_core_machine:get_status(load_machine_options(OptionsRef), ID).

-spec is_exist(options_ref(), mg_core:id()) ->
    boolean() | no_return().
is_exist(OptionsRef, ID) ->
    mg_core_machine:is_exist(load_machine_options(OptionsRef), ID).

-spec search(OptionsRef, Query, Limit, Continuation) -> Result when
    OptionsRef :: options_ref(),
    Query :: mg_core_machine:search_query(),
    Limit :: mg_core_storage:index_limit(),
    Continuation :: mg_core_storage:continuation(),
    Result :: mg_core_storage:search_result() | no_return().
search(OptionsRef, Query, Limit, Continuation) ->
    mg_core_machine:search(load_machine_options(OptionsRef), Query, Limit, Continuation).

-spec search(options_ref(), mg_core_machine:search_query(), mg_core_storage:index_limit()) ->
    mg_core_storage:search_result() | no_return().
search(OptionsRef, Query, Limit) ->
    mg_core_machine:search(load_machine_options(OptionsRef), Query, Limit).

-spec search(options_ref(), mg_core_machine:search_query()) ->
    mg_core_storage:search_result() | no_return().
search(OptionsRef, Query) ->
    mg_core_machine:search(load_machine_options(OptionsRef), Query).

-spec call_with_lazy_start(options_ref(), mg_core:id(), term(), req_ctx(), deadline(), term()) ->
    _Resp | no_return().
call_with_lazy_start(Options, ID, Call, ReqCtx, Deadline, StartArgs) ->
    try
        call(Options, ID, Call, ReqCtx, Deadline)
    catch throw:{logic, machine_not_found} ->
        try
            _ = start(Options, ID, StartArgs, ReqCtx, Deadline)
        catch throw:{logic, machine_already_exist} ->
            % вдруг кто-то ещё делает аналогичный процесс
            ok
        end,
        % если к этому моменту машина не создалась, значит она уже не создастся
        % и исключение будет оправданным
        call(Options, ID, Call, ReqCtx, Deadline)
    end.

%% Internals

-spec load_machine_options(options_ref()) ->
    mg_core_machine:options().
load_machine_options(OptionsRef) ->
    Options = load_options(OptionsRef),
    machine_options(Options).

-spec load_manager_options(options_ref()) ->
    mg_core_workers_manager:options().
load_manager_options(OptionsRef) ->
    Options = load_options(OptionsRef),
    manager_options(Options).

-spec call_(options_ref(), id(), call(), maybe(req_ctx()), deadline()) ->
    _Reply | {error, _}.
call_(OptionsRef, ID, Call, ReqCtx, Deadline) ->
    mg_core_utils:throw_if_error(
        mg_core_workers_manager:call(load_manager_options(OptionsRef), ID, Call, ReqCtx, Deadline)
    ).

-spec machine_sup_child_spec(options(), term()) ->
    supervisor:child_spec().
machine_sup_child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {mg_core_utils_supervisor_wrapper, start_link, [
            #{strategy => rest_for_one},
            mg_core_utils:lists_compact([
                mg_core_machine:processor_child_spec(machine_options(Options), machine),
                mg_core_workers_manager:child_spec(manager_options(Options), manager)
            ])
        ]},
        restart  => permanent,
        type     => supervisor
    }.

-spec scheduler_sup_child_spec(options(), term()) ->
    supervisor:child_spec().
scheduler_sup_child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {mg_core_utils_supervisor_wrapper, start_link, [
            #{
                strategy  => one_for_one,
                intensity => 10,
                period    => 30
            },
            mg_core_utils:lists_compact([
                scheduler_child_spec(timers        , Options),
                scheduler_child_spec(timers_retries, Options),
                scheduler_child_spec(overseer      , Options)
            ])
        ]},
        restart  => permanent,
        type     => supervisor
    }.


-spec scheduler_child_spec(scheduler_type(), options()) ->
    supervisor:child_spec() | undefined.
scheduler_child_spec(SchedulerType, Options) ->
    case scheduler_options(SchedulerType, Options) of
        disable ->
            undefined;
        Config ->
            SchedulerID = scheduler_id(SchedulerType, Options),
            SchedulerOptions = scheduler_sup_options(SchedulerType, Options, Config),
            mg_core_scheduler_sup:child_spec(SchedulerID, SchedulerOptions, SchedulerType)
    end.

-spec scheduler_options(scheduler_type(), options()) ->
    scheduler_options().
scheduler_options(SchedulerType, Options) ->
    maps:get(SchedulerType, maps:get(schedulers, Options, #{}), disable).

-spec scheduler_id(scheduler_type(), options()) ->
    mg_core_scheduler:id() | undefined.
scheduler_id(SchedulerType, #{namespace := NS}) ->
    {SchedulerType, NS}.

-spec scheduler_sup_options(scheduler_type(), options(), scheduler_options()) ->
    mg_core_scheduler_sup:options().
scheduler_sup_options(SchedulerType, Options, Config) when
    SchedulerType == timers;
    SchedulerType == timers_retries
->
    TimerQueue = case SchedulerType of
        timers         -> waiting;
        timers_retries -> retrying
    end,
    MachineOptions = maps:get(machine, Options, #{}),
    HandlerOptions = #{
        processing_timeout => maps:get(timer_processing_timeout, MachineOptions, undefined),
        timer_queue        => TimerQueue,
        min_scan_delay     => maps:get(min_scan_delay, Config, undefined),
        lookahead          => scheduler_cutoff(Config)
    },
    scheduler_sup_options(mg_core_queue_timer, Options, HandlerOptions, Config);
scheduler_sup_options(overseer, Options, Config) ->
    HandlerOptions = #{
        min_scan_delay => maps:get(min_scan_delay, Config, undefined),
        rescan_delay   => maps:get(rescan_delay, Config, undefined)
    },
    scheduler_sup_options(mg_core_queue_interrupted, Options, HandlerOptions, Config).

-spec scheduler_sup_options(module(), options(), map(), scheduler_options()) ->
    mg_core_scheduler_sup:options().
scheduler_sup_options(HandlerMod, Options, HandlerOptions, Config) ->
    #{
        namespace := NS,
        pulse := Pulse
    } = Options,
    FullHandlerOptions = genlib_map:compact(maps:merge(
        #{
            pulse => Pulse,
            namespace_options_ref => make_options_ref(NS)
        },
        HandlerOptions
    )),
    Handler = {HandlerMod, FullHandlerOptions},
    genlib_map:compact(#{
        capacity => maps:get(capacity, Config, ?DEFAULT_SCHEDULER_CAPACITY),
        quota_name => maps:get(task_quota, Config, unlimited),
        quota_share => maps:get(task_share, Config, 1),
        queue_handler => Handler,
        max_scan_limit => maps:get(max_scan_limit, Config, undefined),
        scan_ahead => maps:get(scan_ahead, Config, undefined),
        task_handler => Handler,
        pulse => Pulse
    }).

-spec scheduler_cutoff(scheduler_options()) ->
    seconds().
scheduler_cutoff(#{target_cutoff := Cutoff}) ->
    Cutoff;
scheduler_cutoff(#{min_scan_delay := MinScanDelay}) ->
    erlang:convert_time_unit(MinScanDelay, millisecond, second);
scheduler_cutoff(disable) ->
    undefined;
scheduler_cutoff(#{}) ->
    undefined.

-spec storage_options(options()) ->
    mg_core_storage:options().
storage_options(#{namespace := NS, storage := Storage, pulse := Pulse}) ->
    {Mod, Options} = mg_core_utils:separate_mod_opts(Storage, #{}),
    {Mod, Options#{name => {NS, ?MODULE, machines}, pulse => Pulse}}.

-spec manager_options(options()) ->
    mg_core_workers_manager:options().
manager_options(Options) ->
    #{
        pulse := Pulse,
        registry := Registry,
        namespace := NS
    } = Options,
    ManagerOptions = maps:get(workers_manager, Options, #{}),
    ManagerOptions#{
        pulse => Pulse,
        worker => {mg_core_machine, machine_options(Options)},
        registry => Registry,
        namespace => NS
    }.

-spec machine_options(options()) ->
    mg_core_machine:options().
machine_options(Options) ->
    #{
        namespace := NS,
        processor := Processor,
        pulse := Pulse
    } = Options,
    MachineOptions = maps:get(machine, Options, #{}),
    MachineOptions#{
        namespace => NS,
        processor => Processor,
        pulse => Pulse,
        storage => storage_options(Options),
        schedulers => machine_schedulers_opt(Options)
    }.

-spec machine_schedulers_opt(options()) ->
    mg_core_machine:schedulers_opt().
machine_schedulers_opt(Options) ->
    Types = [timers, timers_retries],
    maps:from_list([{T, machine_scheduler_opt(T, Options)} || T <- Types]).

-spec machine_scheduler_opt(scheduler_type(), options()) ->
    mg_core_machine:scheduler_opt().
machine_scheduler_opt(SchedulerType, Options) ->
    SchedulerOptions = scheduler_options(SchedulerType, Options),
    genlib_map:compact(#{
        id => scheduler_id(SchedulerType, Options),
        target_cutoff => scheduler_cutoff(SchedulerOptions)
    }).