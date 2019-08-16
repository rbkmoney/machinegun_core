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

-module(mg_scheduler).

-include_lib("mg/include/pulse.hrl").

-behaviour(gen_server).

-export([child_spec/3]).
-export([start_link/2]).

-export([add_task/2]).

%% gen_server callbacks
-export([init/1]).
-export([handle_info/2]).
-export([handle_cast/2]).
-export([handle_call/3]).
-export([code_change/3]).
-export([terminate/2]).

-callback child_spec(queue_options(), atom()) -> supervisor:child_spec() | undefined.
-callback init(queue_options()) -> {ok, queue_state()}.
-callback search_new_tasks(Options, Limit, State) -> {ok, Status, Result, State} when
    Options :: queue_options(),
    Limit :: non_neg_integer(),
    Result :: [task_info()],
    Status :: search_status(),
    State :: queue_state().

-optional_callbacks([child_spec/2]).

%% Types
-type options() :: #{
    namespace := mg:ns(),
    name := name(),
    registry := process_registry(),
    queue_handler := queue_handler(),
    task_handler := mg_utils:mod_opts(),
    pulse := mg_pulse:handler(),
    quota_name := mg_quota_worker:name(),
    quota_share => mg_quota:share(),
    no_task_wait => timeout(),
    search_interval => timeout()
}.
-type task_info(TaskID, TaskPayload) :: #{
    id := TaskID,
    payload := TaskPayload,
    created_at := integer(),  % erlang monotonic time
    target_time => genlib_time:ts(),  % unix timestamp in seconds
    machine_id => mg:id()
}.
-type ref_options() :: #{
    namespace := mg:ns(),
    name      := name(),
    registry  := process_registry()
}.
-type task_info() :: task_info(task_id(), task_payload()).
-type search_status() :: continue | completed.
-type process_registry() :: gproc | consuela.
-type name() :: atom().

-export_type([name/0]).
-export_type([options/0]).
-export_type([ref_options/0]).
-export_type([process_registry/0]).
-export_type([task_info/0]).
-export_type([task_info/2]).
-export_type([search_status/0]).

%% Internal types
-record(state, {
    ns :: mg:ns(),
    name :: name(),
    queue_handler :: queue_handler(),
    queue_state :: queue_state(),
    pulse :: mg_pulse:handler(),
    options :: options(),
    quota_name :: mg_quota_worker:name(),
    quota_share :: mg_quota:share(),
    quota_reserved :: mg_quota:resource() | undefined,
    timer :: reference(),
    search_interval :: timeout(),
    no_task_wait :: timeout(),
    active_tasks :: #{task_id() => pid()},
    waiting_tasks :: queue:queue(task_id()),
    tasks_info :: #{task_id() => task_info()},
    task_monitors :: #{monitor() => task_id()}
}).
-type state() :: #state{}.
-type task_id() :: any().
-type monitor() :: reference().
-type queue_state() :: any().
-type task_payload() :: any().
-type queue_options() :: any().
-type queue_handler() :: mg_utils:mod_opts(queue_options()).

-define(DEFAULT_SEARCH_INTERVAL, 1000).  % 1 second
-define(DEFAULT_NO_TASK_WAIT, 1000).  % 1 second
-define(SEARCH_MESSAGE, search_new_tasks).
-define(SEARCH_NUMBER, 10).

%%
%% API
%%

-spec child_spec(_RegName, options(), _ChildID) ->
    supervisor:child_spec().
child_spec(RegName, Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [RegName, Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(_RegName, options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, #{queue_handler := Handler} = Options) ->
    do_start_link(
        RegName,
        #{strategy => one_for_all},
        mg_utils:lists_compact([
            mg_scheduler_worker:child_spec(Options, tasks),
            handler_child_spec(Handler, queue_handler),
            manager_child_spec(Options, manager)
        ]),
        Options
    ).

-spec do_start_link(_RegName, supervisor:sup_flags(), [supervisor:child_spec()], options()) ->
    mg_utils:gen_start_ret().
do_start_link(RegName, Flags, ChildSpecs, #{registry := consuela, pulse := Pulse}) ->
    LeaderOpts = #{pulse => mg_consuela_pulse_adapter:pulse(leader, Pulse)},
    consuela_leader_supervisor:start_link(RegName, mg_utils_supervisor_wrapper, {Flags, ChildSpecs}, LeaderOpts);
do_start_link(_RegName, Flags, ChildSpecs, #{registry := gproc}) ->
    mg_utils_supervisor_wrapper:start_link(Flags, ChildSpecs).

-spec add_task(ref_options(), task_info()) ->
    ok.
add_task(Options, TaskInfo) ->
    try
        gen_server:call(self_ref(Options), {add_task, TaskInfo})
    catch
        exit:Reason ->
            erlang:throw({transient, {scheduler_unavailable, Reason}})
    end.

%% gen_server callbacks

-spec init(options()) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    SearchInterval = maps:get(search_interval, Options, ?DEFAULT_SEARCH_INTERVAL),
    NoTaskWait = maps:get(no_task_wait, Options, ?DEFAULT_NO_TASK_WAIT),
    Name = maps:get(name, Options),
    NS = maps:get(namespace, Options),
    QueueHandler = maps:get(queue_handler, Options),
    {ok, QueueState} = handler_init(QueueHandler),
    {ok, #state{
        ns = NS,
        name = Name,
        queue_handler = QueueHandler,
        queue_state = QueueState,
        pulse = maps:get(pulse, Options),
        options = Options,
        quota_name = maps:get(quota_name, Options),
        quota_share = maps:get(quota_share, Options, 1),
        quota_reserved = undefined,
        search_interval = SearchInterval,
        no_task_wait = NoTaskWait,
        active_tasks = #{},
        task_monitors = #{},
        tasks_info = #{},
        waiting_tasks = queue:new(),
        timer = erlang:send_after(SearchInterval, self(), ?SEARCH_MESSAGE)
    }}.

-spec handle_call(Call :: any(), mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({add_task, TaskInfo}, _From, State0) ->
    State1 = add_tasks([TaskInfo], State0),
    State2 = maybe_update_reserved(State1),
    State3 = start_new_tasks(State2),
    {reply, ok, State3};
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-spec handle_cast(Cast :: any(), state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State}.

-spec handle_info(Info :: any(), state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(?SEARCH_MESSAGE, State0) ->
    {SearchStatus, State1} = search_new_tasks(State0),
    Timeout = get_timer_timeout(SearchStatus, State1),
    State2 = restart_timer(?SEARCH_MESSAGE, Timeout, State1),
    State3 = update_reserved(State2),
    State4 = start_new_tasks(State3),
    {noreply, State4};
handle_info({'DOWN', Monitor, process, _Object, _Info}, State0) ->
    State1 = forget_about_task(Monitor, State0),
    State2 = start_new_tasks(State1),
    {noreply, State2};
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info received: ~p", [Info]),
    {noreply, State}.

-spec code_change(OldVsn :: any(), state(), Extra :: any()) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec terminate(Reason :: any(), state()) ->
    ok.
terminate(_Reason, _State) ->
    ok.

%% Internlas

-spec manager_child_spec(options(), atom()) ->
    supervisor:child_spec().
manager_child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {gen_server, start_link, [self_reg_name(Options), ?MODULE, Options, []]},
        restart  => permanent,
        shutdown => 5000
    }.

% Process registration

-spec self_ref(ref_options()) ->
    mg_utils:gen_ref().
self_ref(ID) ->
    self_reg_name(ID).

-spec self_reg_name(options() | ref_options()) ->
    mg_utils:gen_reg_name().
self_reg_name(#{registry := consuela} = Options) ->
    {via, consuela, wrap_id(Options)};
self_reg_name(#{registry := gproc} = Options) ->
    {via, consuela, {n, l, wrap_id(Options)}}.

-spec wrap_id(options() | ref_options()) ->
    term().
wrap_id(#{name := Name, namespace := NS}) ->
    {?MODULE, {Name, NS}}.

% Callback helpers

-spec handler_init(queue_handler()) ->
    {ok, queue_state()}.
handler_init(Handler) ->
    mg_utils:apply_mod_opts(Handler, init).

-spec handler_search(Handler, Limit, State) -> {ok, Status, Result, State} when
    Handler :: queue_handler(),
    Limit :: non_neg_integer(),
    Result :: [task_info()],
    Status :: search_status(),
    State :: queue_state().
handler_search(Handler, Limit, State) ->
    mg_utils:apply_mod_opts(Handler, search_new_tasks, [Limit, State]).

-spec handler_child_spec(queue_options(), atom()) ->
    supervisor:child_spec() | undefined.
handler_child_spec(Handler, ChildID) ->
    mg_utils:apply_mod_opts_if_defined(Handler, child_spec, undefined, [ChildID]).

% Timer

-spec restart_timer(any(), timeout(), state()) -> state().
restart_timer(Message, Timeout, #state{timer = TimerRef} = State) ->
    _ = erlang:cancel_timer(TimerRef),
    State#state{timer = erlang:send_after(Timeout, self(), Message)}.

-spec get_timer_timeout(search_status(), state()) ->
    timeout().
get_timer_timeout(continue, State) ->
    State#state.search_interval;
get_timer_timeout(completed, State) ->
    State#state.no_task_wait.

% Helpers

-spec forget_about_task(monitor(), state()) ->
    state().
forget_about_task(Monitor, State) ->
    #state{active_tasks = Tasks, tasks_info = TaskInfo, task_monitors = Monitors} = State,
    case maps:find(Monitor, Monitors) of
        {ok, TaskID} ->
            State#state{
                active_tasks = maps:remove(TaskID, Tasks),
                task_monitors = maps:remove(Monitor, Monitors),
                tasks_info = maps:remove(TaskID, TaskInfo)
            };
        error ->
            State
    end.

-spec add_tasks([task_info()], state()) ->
    state().
add_tasks([], State) ->
    State;
add_tasks(NewTasks, State) ->
    #state{tasks_info = TasksInfo, waiting_tasks = WaitingTasks} = State,
    UnknownTasks = [Task || #{id := ID} = Task <- NewTasks, maps:is_key(ID, TasksInfo) =:= false],
    NewWaitingTasks = queue:join(WaitingTasks, queue:from_list([ID || #{id := ID} <- UnknownTasks])),
    NewTasksInfo = maps:merge(TasksInfo, maps:from_list([{ID, Info} || #{id := ID} = Info <- UnknownTasks])),
    ok = emit_new_tasks_beat(UnknownTasks, State),
    State#state{tasks_info = NewTasksInfo, waiting_tasks = NewWaitingTasks}.

-spec search_new_tasks(state()) ->
    {search_status(), state()}.
search_new_tasks(#state{tasks_info = TaskInfo} = State) ->
    TasksNeeded = get_search_number(State),
    case maps:size(TaskInfo) of
        TotalKnownTasks when TotalKnownTasks < TasksNeeded ->
            {ok, Status, NewTasks, NewState} = try_search_tasks(TasksNeeded, State),
            {Status, add_tasks(NewTasks, NewState)};
        TotalKnownTasks when TotalKnownTasks >= TasksNeeded ->
            {continue, State}
    end.

-spec get_search_number(state()) ->
    non_neg_integer().
get_search_number(#state{quota_reserved = undefined}) ->
    ?SEARCH_NUMBER;
get_search_number(#state{quota_reserved = Reserved}) ->
    erlang:max(Reserved * 2, ?SEARCH_NUMBER).

-spec try_search_tasks(non_neg_integer(), state()) ->
    {ok, search_status(), [task_info()], state()}.
try_search_tasks(SearchLimit, State) ->
    #state{
        queue_state = HandlerState,
        queue_handler = Handler
    } = State,
    {ok, Status, NewTasks, NewHandlerState} = try
        handler_search(Handler, SearchLimit, HandlerState)
    catch
        throw:({ErrorType, _Details} = Reason):ST when
            ErrorType =:= transient orelse
            ErrorType =:= timeout
        ->
            Exception = {throw, Reason, ST},
            ok = emit_search_error_beat(Exception, State),
            {ok, continue, [], HandlerState}
    end,
    {ok, Status, NewTasks, State#state{queue_state = NewHandlerState}}.

-spec start_new_tasks(state()) ->
    state().
start_new_tasks(State) ->
    #state{
        quota_reserved = Reserved,
        active_tasks = ActiveTasks
    } = State,
    TotalActiveTasks = maps:size(ActiveTasks),
    NewTasksNumber = erlang:max(Reserved - TotalActiveTasks, 0),
    start_multiple_tasks(NewTasksNumber, State).

-spec start_multiple_tasks(non_neg_integer(), state()) ->
    state().
start_multiple_tasks(0, State) ->
    State;
start_multiple_tasks(N, State) when N > 0 ->
    #state{
        ns = NS,
        name = Name,
        tasks_info = TasksInfo,
        waiting_tasks = WaitingTasks,
        active_tasks = ActiveTasks,
        task_monitors = Monitors
    } = State,
    case queue:out(WaitingTasks) of
        {{value, TaskID}, NewWaitingTasks} ->
            TaskInfo = maps:get(TaskID, TasksInfo),
            {ok, Pid} = mg_scheduler_worker:start_task(NS, Name, TaskInfo),
            Monitor = erlang:monitor(process, Pid),
            NewState = State#state{
                waiting_tasks = NewWaitingTasks,
                active_tasks = ActiveTasks#{TaskID => Pid},
                task_monitors = Monitors#{Monitor => TaskID}
            },
            start_multiple_tasks(N - 1, NewState);
        {empty, WaitingTasks} ->
            State
    end.

-spec update_reserved(state()) ->
    state().
update_reserved(State) ->
    #state{
        ns = NS,
        name = Name,
        tasks_info = TaskInfo,
        quota_name = Quota,
        quota_share = QuotaShare,
        active_tasks = ActiveTasks
    } = State,
    TotalKnownTasks = maps:size(TaskInfo),
    TotalActiveTasks = maps:size(ActiveTasks),
    ClientOptions = #{
        client_id => {NS, Name},
        share => QuotaShare
    },
    Reserved = mg_quota_worker:reserve(ClientOptions, TotalActiveTasks, TotalKnownTasks, Quota),
    NewState = State#state{quota_reserved = Reserved},
    ok = emit_reserved_beat(TotalActiveTasks, TotalKnownTasks, Reserved, NewState),
    NewState.

%% logging

-spec emit_beat(mg_pulse:handler(), mg_pulse:beat()) -> ok.
emit_beat(Handler, Beat) ->
    ok = mg_pulse:handle_beat(Handler, Beat).

-spec emit_new_tasks_beat([task_info()], state()) ->
    ok.
emit_new_tasks_beat(NewTasks, #state{pulse = Pulse, ns = NS, name = Name}) ->
    emit_beat(Pulse, #mg_scheduler_new_tasks{
        namespace = NS,
        scheduler_name = Name,
        new_tasks_count = erlang:length(NewTasks)
    }).

-spec emit_search_error_beat(mg_utils:exception(), state()) ->
    ok.
emit_search_error_beat(Exception, #state{pulse = Pulse, ns = NS, name = Name}) ->
    emit_beat(Pulse, #mg_scheduler_search_error{
        namespace = NS,
        scheduler_name = Name,
        exception = Exception
    }).

-spec emit_reserved_beat(non_neg_integer(), non_neg_integer(), mg_quota:resource(), state()) ->
    ok.
emit_reserved_beat(Active, Total, Reserved, State) ->
    #state{pulse = Pulse, ns = NS, name = Name, quota_name = Quota} = State,
    emit_beat(Pulse, #mg_scheduler_quota_reserved{
        namespace = NS,
        scheduler_name = Name,
        active_tasks = Active,
        waiting_tasks = Total - Active,
        quota_name = Quota,
        quota_reserved = Reserved
    }).

-spec(maybe_update_reserved(state()) -> state()).
maybe_update_reserved(#state{quota_reserved = undefined} = State) ->
    update_reserved(State);
maybe_update_reserved(State) ->
    State.