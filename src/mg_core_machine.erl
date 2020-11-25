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

%%%
%%% Примитивная "машина".
%%%
%%% Имеет идентификатор.
%%% При падении хендлера переводит машину в error состояние.
%%% Оперирует стейтом и набором атрибутов.
%%%
-module(mg_core_machine).

-behaviour(mg_core_workers_manager).
-behaviour(gen_server).

%%
%% Логика работы с ошибками.
%%
%% Возможные ошибки:
%%  - ожиданные -- throw()
%%   - бизнес-логические -- logic
%%    - машина не найдена -- machine_not_found
%%    - машина уже существует -- machine_already_exist
%%    - машина находится в упавшем состоянии -- machine_failed
%%    - некорректная попытка повторно запланировать обработку события -- invalid_reschedule_request
%%    - что-то ещё?
%%   - временные -- transient
%%    - сервис перегружен —- overload
%%    - хранилище недоступно -- {storage_unavailable, Details}
%%    - процессор недоступен -- {processor_unavailable, Details}
%%   - таймауты -- {timeout, Details}
%%   - окончательные -- permanent
%%    - исчерпаны попытки повтора обработки таймера -- timer_retries_exhausted
%%  - неожиданные
%%   - что-то пошло не так -- падение с любой другой ошибкой
%%
%% Например: throw:{logic, machine_not_found}, throw:{transient, {storage_unavailable, ...}}, error:badarg
%%
%% Если в процессе обработки внешнего запроса происходит ожидаемая ошибка, она мапится в код ответа,
%%  если неожидаемая ошибка, то запрос падает с internal_error, ошибка пишется в лог.
%%
%% Если в процессе обработки запроса машиной происходит ожидаемая ошибка, то она прокидывается вызывающему коду,
%%  если неожидаемая, то машина переходит в error состояние и в ответ возникает ошибка machine_failed.
%%
%% Хранилище и процессор кидают либо ошибку о недоступности, либо падают.
%%

-include_lib("machinegun_core/include/pulse.hrl").

%% API
-export_type([retry_opt/0]).
-export_type([suicide_probability/0]).
-export_type([scheduler_opt/0]).
-export_type([schedulers_opt/0]).
-export_type([options/0]).
-export_type([storage_options/0]).
-export_type([processor_options/0]).
-export_type([thrown_error/0]).
-export_type([logic_error/0]).
-export_type([transient_error/0]).
-export_type([call/0]).
-export_type([machine_state/0]).
-export_type([processor_machine_state/0]).
-export_type([get_machine_args/0]).
-export_type([machine_status/0]).
-export_type([processor_impact/0]).
-export_type([processing_context/0]).
-export_type([processor_result/0]).
-export_type([processor_reply_action/0]).
-export_type([processor_flow_action/0]).
-export_type([search_query/0]).
-export_type([machine_regular_status/0]).

-export([processor_child_spec/2]).

-export([get_machine/3]).
-export([get_status/2]).
-export([is_exist/2]).
-export([search/2]).
-export([search/3]).
-export([search/4]).
-export([reply/2]).

%% Internal API
-export([all_statuses/0]).
-export([get_storage_machine/2]).

%% mg_core_workers_manager callbacks
-export([start_link/5, call/5]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type seconds() :: non_neg_integer().
-type scheduler_type() :: timers | timers_retries.
-type scheduler_opt() :: #{
    id := mg_core_scheduler:id(),
    % how many seconds in future a task can be for it to be sent to the local scheduler
    target_cutoff => seconds()
}.
-type retry_subj() :: storage | processor | timers | continuation.
-type retry_opt() :: #{
    retry_subj()   => mg_core_retry:policy()
}.
-type schedulers_opt() :: #{scheduler_type() => scheduler_opt()}.
-type suicide_probability() :: float() | integer() | undefined. % [0, 1]

-type options() :: #{
    namespace := mg_core:ns(),
    pulse := mg_core_pulse:handler(),
    storage := storage_options(),
    processor := processor_options(),
    schedulers := schedulers_opt(),
    retries => retry_opt(),
    unload_timeout => timeout(),
    hibernate_timeout => timeout(),
    suicide_probability => suicide_probability(),
    timer_processing_timeout => timeout()
}.

-type call() ::
      {start, Args::term()}
    | {fail, Exception::term()}
    | {call, Payload::term()}
    | {repair, Args::term()}
    | {timeout, seconds()}
    |  simple_repair
    |  resume_interrupted_one
.

-type storage_options() :: mg_core_storage:options().
-type processor_options() :: mg_core_utils:mod_opts().
-type thrown_error() :: {logic, logic_error()} | {transient, transient_error()} | {timeout, _Reason}.
-type logic_error() :: machine_already_exist | machine_not_found | machine_failed | machine_already_working.
-type transient_error() :: overload | {storage_unavailable, _Reason} | {processor_unavailable, _Reason} | unavailable.

-type throws() :: no_return().
-type machine_state() :: mg_core_storage:opaque().
-type processor_machine_state() :: term().
-type get_machine_args() :: term().

-type machine_regular_status() ::
       sleeping
    | {waiting, genlib_time:ts(), request_context(), HandlingTimeout::pos_integer()}
    | {retrying, Target::genlib_time:ts(), Start::genlib_time:ts(), Attempt::non_neg_integer(), request_context()}
    | {processing, request_context()}
.
-type machine_status() :: machine_regular_status() | {error, Reason::term(), machine_regular_status()}.

%%

-type call_context() :: term(). % в OTP он не описан, а нужно бы :(
-type processing_state() :: term().
% контест обработки, сбрасывается при выгрузке машины
-type processing_context() :: #{
    call_context => call_context(),
    state        => processing_state()
} | undefined.
-type processor_impact() ::
      {init   , term()}
    | {repair , term()}
    | {call   , term()}
    |  timeout
    |  continuation
.
-type processor_reply_action() :: noreply  | {reply, _}.
-type processor_flow_action() ::
       sleep
    | {wait, genlib_time:ts(), request_context(), HandlingTimeout::pos_integer()}
    | {continue, processing_state()}
    |  keep
    |  remove
.
-type processor_result() :: {processor_reply_action(), processor_flow_action(), machine_state()}.
-type request_context() :: mg_core:request_context().

-type processor_retry() :: mg_core_retry:strategy() | undefined.

-type deadline() :: mg_core_deadline:deadline().
-type reg_name() :: mg_core_utils:gen_reg_name().
-type maybe(T) :: T | undefined.

-callback processor_child_spec(_Options, term()) ->
    supervisor:child_spec() | undefined.
-callback process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg_core:id(),
    Impact :: processor_impact(),
    PCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    MachineState :: machine_state(),
    Result :: processor_result().
-callback get_machine(Options, ID, Args, MachineState) -> Result when
    Options :: any(),
    ID :: mg_core:id(),
    Args :: get_machine_args(),
    MachineState :: machine_state(),
    Result :: processor_machine_state().
-optional_callbacks([processor_child_spec/2]).

-type search_query() ::
       sleeping
    |  waiting
    | {waiting, From::genlib_time:ts(), To::genlib_time:ts()}
    |  retrying
    | {retrying, From::genlib_time:ts(), To::genlib_time:ts()}
    |  processing
    |  failed
.

%%
%% API
%%
-spec processor_child_spec(options(), term()) ->
    supervisor:child_spec() | undefined.
processor_child_spec(Options, ChildID) ->
    ProcessorOptions = processor_options(Options),
    mg_core_utils:apply_mod_opts_if_defined(ProcessorOptions, processor_child_spec, undefined, [ChildID]).

-spec call(options(), mg_core_utils:gen_ref(), call(), request_context(), deadline()) ->
    _Resp | throws().
call(_Options, Ref, Call, ReqCtx, Deadline) ->
    gen_server:call(Ref, {call, Deadline, Call, ReqCtx}, mg_core_deadline:to_timeout(Deadline)).

-spec get_machine(options(), mg_core:id(), get_machine_args()) ->
    processor_machine_state() | throws().
get_machine(Options, ID, Args) ->
    {_, #{state := State}} =
        mg_core_utils:throw_if_undefined(get_storage_machine(Options, ID), {logic, machine_not_found}),
    mg_core_utils:apply_mod_opts(
        processor_options(Options),
        get_machine,
        [ID, Args, State]
    ).

-spec get_status(options(), mg_core:id()) ->
    machine_status() | throws().
get_status(Options, ID) ->
    {_, #{status := Status}} =
        mg_core_utils:throw_if_undefined(get_storage_machine(Options, ID), {logic, machine_not_found}),
    Status.

-spec is_exist(options(), mg_core:id()) ->
    boolean() | throws().
is_exist(Options, ID) ->
    get_storage_machine(Options, ID) =/= undefined.

-spec search(options(), search_query(), mg_core_storage:index_limit(), mg_core_storage:continuation()) ->
    mg_core_storage:search_result() | throws().
search(Options, Query, Limit, Continuation) ->
    StorageQuery = storage_search_query(Query, Limit, Continuation),
    mg_core_storage:search(storage_options(Options), StorageQuery).

-spec search(options(), search_query(), mg_core_storage:index_limit()) ->
    mg_core_storage:search_result() | throws().
search(Options, Query, Limit) ->
    mg_core_storage:search(storage_options(Options), storage_search_query(Query, Limit)).

-spec search(options(), search_query()) ->
    mg_core_storage:search_result() | throws().
search(Options, Query) ->
    % TODO deadline
    mg_core_storage:search(storage_options(Options), storage_search_query(Query)).

-spec reply(processing_context(), _) ->
    ok.
reply(undefined, _) ->
    % отвечать уже некому
    ok;
reply(#{call_context := CallContext}, Reply) ->
    _ = gen_server:reply(CallContext, Reply),
    ok.

%%
%% mg_core_workers_manager callbacks
%%
-spec start_link(options(), reg_name(), mg_core:id(), request_context(), deadline()) ->
    mg_core_utils:gen_start_ret().
start_link(Options, RegName, ID, ReqCtx, Deadline) ->
    Opts = [{timeout, mg_core_deadline:to_timeout(Deadline)}],
    gen_server:start_link(RegName, ?MODULE, {ID, Options, ReqCtx}, Opts).

%%
%% gen_server callbacks
%%
-type worker_state() :: #{
    id                := mg_core:id(),
    status            := {loading, options(), request_context()} | {working, MachineState :: state()},
    unload_tref       := reference() | undefined,
    hibernate_timeout := timeout(),
    unload_timeout    := timeout()
}.

-spec init({mg_core:id(), options(), request_context()}) ->
    mg_core_utils:gen_server_init_ret(worker_state()).
init({ID, Options, ReqCtx}) ->
    HibernateTimeout = maps:get(hibernate_timeout, Options, 5 * 1000),
    UnloadTimeout = maps:get(unload_timeout, Options, 60 * 1000),
    State = #{
        id                => ID,
        status            => {loading, Options, ReqCtx},
        unload_tref       => undefined,
        hibernate_timeout => HibernateTimeout,
        unload_timeout    => UnloadTimeout
    },
    {ok, schedule_unload_timer(State)}.

-spec handle_call(_Call, mg_core_utils:gen_server_from(), worker_state()) ->
    mg_core_utils:gen_server_handle_call_ret(worker_state()).
% загрузка делается отдельно и лениво, чтобы не блокировать этим супервизор,
% т.к. у него легко может начать расти очередь
handle_call(Call={call, _, _, _}, From, State=#{id:=ID, status:={loading, Options, ReqCtx}}) ->
    case handle_load(ID, Options, ReqCtx) of
        {ok, ModState} ->
            handle_call(Call, From, State#{status:={working, ModState}});
        Error={error, _} ->
            {stop, normal, Error, State}
    end;
handle_call({call, Deadline, Call, ReqCtx}, From, State=#{status:={working, ModState}}) ->
    case mg_core_deadline:is_reached(Deadline) of
        false ->
            {ReplyAction, NewModState} = handle_call(Call, From, ReqCtx, Deadline, ModState),
            NewState = State#{status:={working, NewModState}},
            case ReplyAction of
                {reply, Reply} -> {reply, Reply, schedule_unload_timer(NewState), hibernate_timeout(NewState)};
                noreply        -> {noreply, schedule_unload_timer(NewState), hibernate_timeout(NewState)}
            end;
        true ->
            ok = logger:warning(
                "rancid worker call received: ~p from: ~p deadline: ~s reqctx: ~p",
                [Call, From, mg_core_deadline:format(Deadline), ReqCtx]
            ),
            {noreply, schedule_unload_timer(State), hibernate_timeout(State)}
    end;
handle_call(Call, From, State) ->
    ok = logger:error("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State, hibernate_timeout(State)}.

-spec handle_cast(_Cast, worker_state()) ->
    mg_core_utils:gen_server_handle_cast_ret(worker_state()).
handle_cast(Cast, State) ->
    ok = logger:error("unexpected gen_server cast received: ~p", [Cast]),
    {noreply, State, hibernate_timeout(State)}.

-spec handle_info(_Info, worker_state()) ->
    mg_core_utils:gen_server_handle_info_ret(worker_state()).
handle_info(timeout, State) ->
    {noreply, State, hibernate};
handle_info({timeout, TRef, unload}, State=#{unload_tref:=TRef, status:=Status}) ->
    case Status of
        {working, ModState} ->
            _ = handle_unload(ModState);
        {loading, _, _} ->
            ok
    end,
    {stop, normal, State};
handle_info({timeout, _, unload}, State=#{}) ->
    % А кто-то опаздал!
    {noreply, schedule_unload_timer(State), hibernate_timeout(State)};
handle_info(Info, State) ->
    ok = logger:error("unexpected gen_server info ~p", [Info]),
    {noreply, State, hibernate_timeout(State)}.

-spec code_change(_, worker_state(), _) ->
    mg_core_utils:gen_server_code_change_ret(worker_state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, worker_state()) ->
    ok.
terminate(_, _) ->
    ok.

%%
%% Internal API
%%
-define(DEFAULT_RETRY_POLICY       , {exponential, infinity, 2, 10, 60 * 1000}).

-define(can_be_retried(ErrorType), ErrorType =:= transient orelse ErrorType =:= timeout).

%%

-spec all_statuses() ->
    [atom()].
all_statuses() ->
    [sleeping, waiting, retrying, processing, failed].

%%
%% worker internals
%%
-type state() :: #{
    id              => mg_core:id(),
    namespace       => mg_core:ns(),
    options         => options(),
    schedulers      => #{scheduler_type() => scheduler_ref()},
    storage_machine => storage_machine() | nonexistent | unknown,
    storage_context => mg_core_storage:context() | undefined
}.

-type storage_machine() :: #{
    status => machine_status(),
    state  => machine_state()
}.

-type scheduler_ref() ::
    {mg_core_scheduler:id(), _TargetCutoff :: seconds()}.

-spec handle_load(mg_core:id(), options(), request_context()) ->
    {ok, state()} | {error, Reason :: any()}.
handle_load(ID, Options, ReqCtx) ->
    Namespace = maps:get(namespace, Options),
    Schedulers = maps:get(schedulers, Options),
    State = #{
        id              => ID,
        namespace       => Namespace,
        options         => Options,
        schedulers      => Schedulers,
        storage_machine => unknown,
        storage_context => undefined
    },
    load_storage_machine(ReqCtx, State).

-spec handle_call(_Call, call_context(), maybe(request_context()), deadline(), state()) ->
    {{reply, _Resp} | noreply, state()}.
handle_call(Call, CallContext, ReqCtx, Deadline, S=#{storage_machine:=StorageMachine}) ->
    PCtx = new_processing_context(CallContext),

    % довольно сложное место, тут определяется приоритет реакции на внешние раздражители, нужно быть аккуратнее
    case {Call, StorageMachine} of
        % восстановление после ошибок обращения в хранилище
        {Call         , unknown     } ->
            {ok, S1} = load_storage_machine(ReqCtx, S),
            handle_call(Call, CallContext, ReqCtx, Deadline, S1);

        % start
        {{start, Args}, nonexistent } -> {noreply, process({init, Args}, PCtx, ReqCtx, Deadline, S)};
        { _           , nonexistent } -> {{reply, {error, {logic, machine_not_found    }}}, S};
        {{start, _   }, #{status:=_}} -> {{reply, {error, {logic, machine_already_exist}}}, S};

        % fail
        {{fail, Exception}, _} -> {{reply, ok}, handle_exception(Exception, ReqCtx, Deadline, S)};

        % сюда мы не должны попадать если машина не падала во время обработки запроса
        % (когда мы переходили в стейт processing)
        {_, #{status := {processing, ProcessingReqCtx}}} ->
            % обработка машин в стейте processing идёт без дедлайна
            % машина должна либо упасть, либо перейти в другое состояние
            S1 = process(continuation, undefined, ProcessingReqCtx, undefined, S),
            handle_call(Call, CallContext, ReqCtx, Deadline, S1);

        % ничего не просходит, просто убеждаемся, что машина загружена
        {resume_interrupted_one, _} -> {{reply, {ok, ok}}, S};

        % call
        {{call  , SubCall}, #{status:= sleeping         }} ->
            {noreply, process({call, SubCall}, PCtx, ReqCtx, Deadline, S)};
        {{call  , SubCall}, #{status:={waiting, _, _, _}}} ->
            {noreply, process({call, SubCall}, PCtx, ReqCtx, Deadline, S)};
        {{call  , SubCall}, #{status:={retrying, _, _, _, _}}} ->
            {noreply, process({call, SubCall}, PCtx, ReqCtx, Deadline, S)};
        {{call  , _      }, #{status:={error  , _, _   }}} -> {{reply, {error, {logic, machine_failed}}}, S};

        % repair
        {{repair, Args}, #{status:={error  , _, _}}} -> {noreply, process({repair, Args}, PCtx, ReqCtx, Deadline, S)};
        {{repair, _   }, #{status:=_              }} -> {{reply, {error, {logic, machine_already_working}}}, S};

        % simple_repair
        {simple_repair, #{status:={error  , _, _}}} -> {{reply, ok}, process_simple_repair(ReqCtx, Deadline, S)};
        {simple_repair, #{status:=_              }} -> {{reply, {error, {logic, machine_already_working}}}, S};

        % timers
        {{timeout, Ts0}, #{status:={waiting, Ts1, InitialReqCtx, _}}}
            when Ts0 =:= Ts1 ->
            {noreply, process(timeout, PCtx, InitialReqCtx, Deadline, S)};
        {{timeout, Ts0}, #{status:={retrying, Ts1, _, _, InitialReqCtx}}}
            when Ts0 =:= Ts1 ->
            {noreply, process(timeout, PCtx, InitialReqCtx, Deadline, S)};
        {{timeout, _}, #{status:=_}} ->
            {{reply, {ok, ok}}, S}

    end.

-spec handle_unload(state()) ->
    ok.
handle_unload(State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_lifecycle_unloaded{
        namespace = NS,
        machine_id = ID
    }).

%%
%% processing context
%%
-spec new_processing_context(call_context()) ->
    processing_context().
new_processing_context(CallContext) ->
    #{
        call_context => CallContext,
        state        => undefined
    }.

%%
%% storage machine
%%
-spec new_storage_machine() ->
    storage_machine().
new_storage_machine() ->
    #{
        status => sleeping,
        state  => null
    }.

-spec get_storage_machine(options(), mg_core:id()) ->
    {mg_core_storage:context(), storage_machine()} | undefined.
get_storage_machine(Options, ID) ->
    try mg_core_storage:get(storage_options(Options), ID) of
        undefined ->
            undefined;
        {Context, PackedMachine} ->
            {Context, opaque_to_storage_machine(PackedMachine)}
    catch
        throw:{logic, {invalid_key, _StorageDetails} = Details} ->
            throw({logic, {invalid_machine_id, Details}})
    end.

-spec load_storage_machine(request_context(), state()) ->
    {ok, state()} | {error, Reason :: any()}.
load_storage_machine(ReqCtx, State) ->
    #{options := Options, id := ID, namespace := Namespace} = State,
    try
        {StorageContext, StorageMachine} =
            case get_storage_machine(Options, ID) of
                undefined -> {undefined, nonexistent};
                V         -> V
            end,

        NewState = State#{
            storage_machine => StorageMachine,
            storage_context => StorageContext
        },
        ok = emit_machine_load_beat(Options, Namespace, ID, ReqCtx, StorageMachine),
        {ok, NewState}
    catch throw:Reason:ST ->
        Exception = {throw, Reason, ST},
        ok = emit_beat(Options, #mg_core_machine_lifecycle_loading_error{
            namespace = Namespace,
            machine_id = ID,
            request_context = ReqCtx,
            exception = Exception
        }),
        {error, Reason}
    end.

%%
%% packer to opaque
%%
-spec storage_machine_to_opaque(storage_machine()) ->
    mg_core_storage:opaque().
storage_machine_to_opaque(#{status := Status, state := State }) ->
    [1, machine_status_to_opaque(Status), State].

-spec opaque_to_storage_machine(mg_core_storage:opaque()) ->
    storage_machine().
opaque_to_storage_machine([1, Status, State]) ->
    #{status => opaque_to_machine_status(Status), state => State}.

-spec machine_status_to_opaque(machine_status()) ->
    mg_core_storage:opaque().
machine_status_to_opaque(Status) ->
    Opaque =
        case Status of
             sleeping                    ->  1;
            {waiting, TS, ReqCtx, HdlTo} -> [2, TS, ReqCtx, HdlTo];
            {processing, ReqCtx}         -> [3, ReqCtx];
            % TODO подумать как упаковывать reason
            {error, Reason, OldStatus}   -> [4, erlang:term_to_binary(Reason), machine_status_to_opaque(OldStatus)];
            {retrying, TS, StartTS, Attempt, ReqCtx} -> [5, TS, StartTS, Attempt, ReqCtx]
        end,
    Opaque.

-spec opaque_to_machine_status(mg_core_storage:opaque()) ->
    machine_status().
opaque_to_machine_status(Opaque) ->
    case Opaque of
         1                     ->  sleeping;
        [2, TS               ] -> {waiting, TS, null, 30000}; % совместимость со старой версией
        [2, TS, ReqCtx, HdlTo] -> {waiting, TS, ReqCtx, HdlTo};
         3                     -> {processing, null}; % совместимость со старой версией
        [3, ReqCtx           ] -> {processing, ReqCtx};
        [4, Reason, OldStatus] -> {error, erlang:binary_to_term(Reason), opaque_to_machine_status(OldStatus)};
        % устаревшее
        [4, Reason           ] -> {error, erlang:binary_to_term(Reason), sleeping};
        [5, TS, StartTS, Attempt, ReqCtx] -> {retrying, TS, StartTS, Attempt, ReqCtx}
    end.


%%
%% indexes
%%
-define(status_idx , {integer, <<"status"      >>}).
-define(waiting_idx, {integer, <<"waiting_date">>}).
-define(retrying_idx, {integer, <<"retrying_date">>}).

-spec storage_search_query(search_query(), mg_core_storage:index_limit(), mg_core_storage:continuation()) ->
    mg_core_storage:index_query().
storage_search_query(Query, Limit, Continuation) ->
    erlang:append_element(storage_search_query(Query, Limit), Continuation).

-spec storage_search_query(search_query(), mg_core_storage:index_limit()) ->
    mg_core_storage:index_query().
storage_search_query(Query, Limit) ->
    erlang:append_element(storage_search_query(Query), Limit).

-spec storage_search_query(search_query()) ->
    mg_core_storage:index_query().
storage_search_query(sleeping) ->
    {?status_idx, 1};
storage_search_query(waiting) ->
    {?status_idx, 2};
storage_search_query({waiting, FromTs, ToTs}) ->
    {?waiting_idx, {FromTs, ToTs}};
storage_search_query(processing) ->
    {?status_idx, 3};
storage_search_query(failed) ->
    {?status_idx, 4};
storage_search_query(retrying) ->
    {?status_idx, 5};
storage_search_query({retrying, FromTs, ToTs}) ->
    {?retrying_idx, {FromTs, ToTs}}.

-spec storage_machine_to_indexes(storage_machine()) ->
    [mg_core_storage:index_update()].
storage_machine_to_indexes(#{status := Status}) ->
    status_index(Status) ++ status_range_index(Status).

-spec status_index(machine_status()) ->
    [mg_core_storage:index_update()].
status_index(Status) ->
    StatusInt =
        case Status of
             sleeping                -> 1;
            {waiting   , _, _, _   } -> 2;
            {processing, _         } -> 3;
            {error     , _, _      } -> 4;
            {retrying  , _, _, _, _} -> 5
        end,
    [{?status_idx, StatusInt}].

-spec status_range_index(machine_status()) ->
    [mg_core_storage:index_update()].
status_range_index({waiting, Timestamp, _, _}) ->
    [{?waiting_idx, Timestamp}];
status_range_index({retrying, Timestamp, _, _, _}) ->
    [{?retrying_idx, Timestamp}];
status_range_index(_) ->
    [].

%%
%% processing
%%

-spec process_simple_repair(request_context(), deadline(), state()) ->
    state().
process_simple_repair(ReqCtx, Deadline, State) ->
    #{storage_machine := StorageMachine = #{status := {error, _, OldStatus}}} = State,
    transit_state(
        ReqCtx,
        Deadline,
        StorageMachine#{status => OldStatus},
        State
    ).

-spec process(processor_impact(), processing_context(), request_context(), deadline(), state()) ->
    state().
process(Impact, ProcessingCtx, ReqCtx, Deadline, State) ->
    RetryStrategy = get_impact_retry_strategy(Impact, Deadline, State),
    try
        process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, RetryStrategy)
    catch
        Class:Reason:ST ->
            ok = do_reply_action({reply, {error, {logic, machine_failed}}}, ProcessingCtx),
            handle_exception({Class, Reason, ST}, ReqCtx, Deadline, State)
    end.

-spec process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, Retry) -> State when
    Impact :: processor_impact(),
    ProcessingCtx :: processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    State :: state(),
    Retry :: processor_retry().
process_with_retry(_, _, _, _, #{storage_machine := unknown} = State, _) ->
    % После попыток обработать вызов пришли в неопредленное состояние.
    % На этом уровне больше ничего не сделать, пусть разбираются выше.
    State;
process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, State, RetryStrategy) ->
    #{id := ID, namespace := NS, options := Opts} = State,
    try
        process_unsafe(Impact, ProcessingCtx, ReqCtx, Deadline, try_init_state(Impact, State))
    catch
        throw:(Reason=({ErrorType, _Details})):ST when ?can_be_retried(ErrorType) ->
            ok = emit_beat(Opts, #mg_core_machine_process_transient_error{
                namespace = NS,
                machine_id = ID,
                exception = {throw, Reason, ST},
                request_context = ReqCtx
            }),
            ok = do_reply_action({reply, {error, Reason}}, ProcessingCtx),
            NewState = handle_transient_exception(Reason, State),
            case process_retry_next_step(RetryStrategy) of
                ignore when Impact == timeout ->
                    reschedule(ReqCtx, undefined, NewState);
                ignore ->
                    NewState;
                finish ->
                    erlang:throw({permanent, {retries_exhausted, Reason}});
                {wait, Timeout, NewRetryStrategy} ->
                    ok = timer:sleep(Timeout),
                    process_with_retry(Impact, ProcessingCtx, ReqCtx, Deadline, NewState, NewRetryStrategy)
            end
    end.

-spec process_retry_next_step(processor_retry()) ->
    {wait, timeout(), mg_core_retry:strategy()} | finish | ignore.
process_retry_next_step(undefined) ->
    ignore;
process_retry_next_step(RetryStrategy) ->
    genlib_retry:next_step(RetryStrategy).

-spec get_impact_retry_strategy(processor_impact(), deadline(), state()) ->
    processor_retry().
get_impact_retry_strategy(continuation, Deadline, #{options := Options}) ->
    retry_strategy(continuation, Options, Deadline);
get_impact_retry_strategy(_Impact, _Deadline, _State) ->
    undefined.

-spec try_init_state(processor_impact(), state()) ->
    state().
try_init_state({init, _}, State) ->
    State#{storage_machine := new_storage_machine()};
try_init_state(_Impact, State) ->
    State.

-spec handle_transient_exception(transient_error(), state()) -> state().
handle_transient_exception({storage_unavailable, _Details}, State) ->
    State#{storage_machine := unknown};
handle_transient_exception(_Reason, State) ->
    State.

-spec handle_exception(Exception, ReqCtx, Deadline, state()) -> state() when
    Exception :: mg_core_utils:exception(),
    ReqCtx :: request_context(),
    Deadline :: deadline().
handle_exception(Exception, ReqCtx, Deadline, State) ->
    #{options := Options, id := ID, namespace := NS, storage_machine := StorageMachine} = State,
    ok = emit_beat(Options, #mg_core_machine_lifecycle_failed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline,
        exception = Exception
    }),
    case StorageMachine of
        nonexistent ->
            State;
        #{status := {error, _, _}} ->
            State;
        #{status := OldStatus} ->
            NewStorageMachine = StorageMachine#{status => {error, Exception, OldStatus}},
            transit_state(ReqCtx, Deadline, NewStorageMachine, State)
    end.

-spec process_unsafe(processor_impact(), processing_context(), request_context(), deadline(), state()) ->
    state().
process_unsafe(Impact, ProcessingCtx, ReqCtx, Deadline, State = #{storage_machine := StorageMachine}) ->
    ok = emit_pre_process_beats(Impact, ReqCtx, Deadline, State),
    ProcessStart = erlang:monotonic_time(),
    {ReplyAction, Action, NewMachineState} =
        call_processor(Impact, ProcessingCtx, ReqCtx, Deadline, State),
    ProcessDuration = erlang:monotonic_time() - ProcessStart,
    ok = emit_post_process_beats(Impact, ReqCtx, Deadline, ProcessDuration, State),
    ok = try_suicide(State, ReqCtx),
    NewStorageMachine0 = StorageMachine#{state := NewMachineState},
    NewState =
        case Action of
            {continue, _} ->
                NewStorageMachine = NewStorageMachine0#{status := {processing, ReqCtx}},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            sleep ->
                NewStorageMachine = NewStorageMachine0#{status := sleeping},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            {wait, Timestamp, HdlReqCtx, HdlTo} ->
                Status = {waiting, Timestamp, HdlReqCtx, HdlTo},
                NewStorageMachine = NewStorageMachine0#{status := Status},
                transit_state(ReqCtx, Deadline, NewStorageMachine, State);
            keep ->
                State;
            remove ->
                remove_from_storage(ReqCtx, Deadline, State)
        end,
    ok = do_reply_action(wrap_reply_action(ok, ReplyAction), ProcessingCtx),
    case Action of
        {continue, NewProcessingSubState} ->
            % продолжение обработки машины делается без дедлайна
            % предполагается, что машина должна рано или поздно завершить свои дела или упасть
            process(continuation, ProcessingCtx#{state:=NewProcessingSubState}, ReqCtx, undefined, NewState);
        _ ->
            NewState
    end.

-spec call_processor(processor_impact(), processing_context(), request_context(), deadline(), state()) ->
    processor_result().
call_processor(Impact, ProcessingCtx, ReqCtx, Deadline, State) ->
    #{options := Options, id := ID, storage_machine := #{state := MachineState}} = State,
    mg_core_utils:apply_mod_opts(
        processor_options(Options),
        process_machine,
        [ID, Impact, ProcessingCtx, ReqCtx, Deadline, MachineState]
    ).

-spec reschedule(ReqCtx, Deadline, state()) -> state() when
    ReqCtx :: request_context(),
    Deadline :: deadline().
reschedule(_, _, #{storage_machine := unknown} = State) ->
    % После попыток обработать вызов пришли в неопредленное состояние.
    % На этом уровне больше ничего не сделать, пусть разбираются выше.
    State;
reschedule(ReqCtx, Deadline, State) ->
    #{id:= ID, options := Options, namespace := NS} = State,
    try
        {ok, NewState, Target, Attempt} = reschedule_unsafe(ReqCtx, Deadline, State),
        ok = emit_beat(Options, #mg_core_timer_lifecycle_rescheduled{
            namespace = NS,
            machine_id = ID,
            request_context = ReqCtx,
            deadline = Deadline,
            target_timestamp = Target,
            attempt = Attempt
        }),
        NewState
    catch
        throw:(Reason=({ErrorType, _Details})):ST when ?can_be_retried(ErrorType) ->
            Exception = {throw, Reason, ST},
            ok = emit_beat(Options, #mg_core_timer_lifecycle_rescheduling_error{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                deadline = Deadline,
                exception = Exception
            }),
            handle_transient_exception(Reason, State)
    end.

-spec reschedule_unsafe(ReqCtx, Deadline, state()) -> Result when
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    Result :: {ok, state(), genlib_time:ts(), non_neg_integer()}.
reschedule_unsafe(ReqCtx, Deadline, State = #{
    storage_machine := StorageMachine = #{status := Status},
    options         := Options
}) ->
    {Start, Attempt} = case Status of
        {waiting, _, _, _}     -> {genlib_time:unow(), 0};
        {retrying, _, S, A, _} -> {S, A + 1}
    end,
    RetryStrategy = retry_strategy(timers, Options, undefined, Start, Attempt),
    case genlib_retry:next_step(RetryStrategy) of
        {wait, Timeout, _NewRetryStrategy} ->
            Target = get_schedule_target(Timeout),
            NewStatus = {retrying, Target, Start, Attempt, ReqCtx},
            NewStorageMachine = StorageMachine#{status => NewStatus},
            {ok, transit_state(ReqCtx, Deadline, NewStorageMachine, State), Target, Attempt};
        finish ->
            throw({permanent, timer_retries_exhausted})
    end.

-spec get_schedule_target(timeout()) ->
    genlib_time:ts().
get_schedule_target(TimeoutMS) ->
    Now = genlib_time:unow(),
    Now + (TimeoutMS div 1000).

-spec do_reply_action(processor_reply_action(), undefined | processing_context()) ->
    ok.
do_reply_action(noreply, _) ->
    ok;
do_reply_action({reply, Reply}, ProcessingCtx) ->
    ok = reply(ProcessingCtx, Reply),
    ok.

-spec wrap_reply_action(_, processor_reply_action()) ->
    processor_reply_action().
wrap_reply_action(_, noreply) ->
    noreply;
wrap_reply_action(Wrapper, {reply, R}) ->
    {reply, {Wrapper, R}}.


-spec transit_state(request_context(), deadline(), storage_machine(), state()) ->
    state().
transit_state(_ReqCtx, _Deadline, NewStorageMachine, State=#{storage_machine := OldStorageMachine})
    when NewStorageMachine =:= OldStorageMachine
->
    State;
transit_state(ReqCtx, Deadline, NewStorageMachine = #{status := Status}, State) ->
    #{
        id              := ID,
        options         := Options,
        storage_machine := #{status := StatusWas},
        storage_context := StorageContext
    } = State,
    _ = case Status of
        StatusWas  -> ok;
        _Different -> handle_status_transition(Status, State)
    end,
    F = fun() ->
            mg_core_storage:put(
                storage_options(Options),
                ID,
                StorageContext,
                storage_machine_to_opaque(NewStorageMachine),
                storage_machine_to_indexes(NewStorageMachine)
            )
        end,
    RS = retry_strategy(storage, Options, Deadline),
    NewStorageContext = do_with_retry(Options, ID, F, RS, ReqCtx, transit),
    State#{
        storage_machine := NewStorageMachine,
        storage_context := NewStorageContext
    }.

-spec handle_status_transition(machine_status(), state()) ->
    _.
handle_status_transition({waiting, TargetTimestamp, _, _}, State) ->
    try_send_timer_task(timers, TargetTimestamp, State);
handle_status_transition({retrying, TargetTimestamp, _, _, _}, State) ->
    try_send_timer_task(timers_retries, TargetTimestamp, State);
handle_status_transition(_Status, _State) ->
    ok.

-spec try_send_timer_task(scheduler_type(), mg_core_queue_task:target_time(), state()) ->
    ok.
try_send_timer_task(SchedulerType, TargetTime, #{id := ID, schedulers := Schedulers}) ->
    case maps:find(SchedulerType, Schedulers) of
        {ok, #{id := SchedulerID, target_cutoff := Cutoff}} when is_integer(Cutoff) ->
            % Ok let's send if it's not too far in the future.
            CurrentTime = mg_core_queue_task:current_time(),
            case TargetTime =< CurrentTime + Cutoff of
                true ->
                    Task = mg_core_queue_timer:build_task(ID, TargetTime),
                    mg_core_scheduler:send_task(SchedulerID, Task);
                false ->
                    ok
            end;
        {ok, ShedulerOpts} when not is_map_key(target_cutoff, ShedulerOpts) ->
            % No defined cutoff, can't make decisions.
            ok;
        error ->
            % No scheduler to send task to.
            ok
    end.

-spec remove_from_storage(request_context(), deadline(), state()) ->
    state().
remove_from_storage(ReqCtx, Deadline, State) ->
    #{namespace := NS, id := ID, options := Options, storage_context := StorageContext} = State,
    F = fun() ->
            mg_core_storage:delete(
                storage_options(Options),
                ID,
                StorageContext
            )
        end,
    RS = retry_strategy(storage, Options, Deadline),
    ok = do_with_retry(Options, ID, F, RS, ReqCtx, remove),
    ok = emit_beat(Options, #mg_core_machine_lifecycle_removed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx
    }),
    State#{storage_machine := nonexistent, storage_context := undefined}.

-spec retry_strategy(retry_subj(), options(), deadline()) ->
    mg_core_retry:strategy().
retry_strategy(Subj, Options, Deadline) ->
    retry_strategy(Subj, Options, Deadline, undefined, undefined).

-spec retry_strategy(Subj, Options, Deadline, InitialTs, Attempt) -> mg_core_retry:strategy() when
    Subj :: retry_subj(),
    Options :: options(),
    Deadline :: deadline(),
    InitialTs :: genlib_time:ts() | undefined,
    Attempt :: non_neg_integer() | undefined.
retry_strategy(Subj, Options, Deadline, InitialTs, Attempt) ->
    Retries = maps:get(retries, Options, #{}),
    Policy = maps:get(Subj, Retries, ?DEFAULT_RETRY_POLICY),
    constrain_retry_strategy(Policy, Deadline, InitialTs, Attempt).

-spec constrain_retry_strategy(Policy, Deadline, InitialTs, Attempt) -> mg_core_retry:strategy() when
    Policy :: mg_core_retry:policy(),
    Deadline :: deadline(),
    InitialTs :: genlib_time:ts() | undefined,
    Attempt :: non_neg_integer() | undefined.
constrain_retry_strategy(Policy, undefined, InitialTs, Attempt) ->
    mg_core_retry:new_strategy(Policy, InitialTs, Attempt);
constrain_retry_strategy(Policy, Deadline, InitialTs, Attempt) ->
    Timeout = mg_core_deadline:to_timeout(Deadline),
    mg_core_retry:new_strategy({timecap, Timeout, Policy}, InitialTs, Attempt).

-spec emit_pre_process_beats(processor_impact(), request_context(), deadline(), state()) ->
    ok.
emit_pre_process_beats(Impact, ReqCtx, Deadline, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_process_started{
        processor_impact = Impact,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline
    }),
    emit_pre_process_timer_beats(Impact, ReqCtx, Deadline, State).

-spec emit_pre_process_timer_beats(processor_impact(), request_context(), deadline(), state()) ->
    ok.
emit_pre_process_timer_beats(timeout, ReqCtx, Deadline, State) ->
    #{
        id := ID,
        options := #{namespace := NS} = Options,
        storage_machine := #{status := Status}
    } = State,
    {ok, QueueName, TargetTimestamp} = extract_timer_queue_info(Status),
    emit_beat(Options, #mg_core_timer_process_started{
        queue = QueueName,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = TargetTimestamp,
        deadline = Deadline
    });
emit_pre_process_timer_beats(_Impact, _ReqCtx, _Deadline, _State) ->
    ok.

-spec emit_post_process_beats(processor_impact(), request_context(), deadline(), integer(), state()) ->
    ok.
emit_post_process_beats(Impact, ReqCtx, Deadline, Duration, State) ->
    #{id := ID, options := #{namespace := NS} = Options} = State,
    ok = emit_beat(Options, #mg_core_machine_process_finished{
        processor_impact = Impact,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        deadline = Deadline,
        duration = Duration
    }),
    emit_post_process_timer_beats(Impact, ReqCtx, Deadline, Duration, State).

-spec emit_post_process_timer_beats(processor_impact(), request_context(), deadline(), integer(), state()) ->
    ok.
emit_post_process_timer_beats(timeout, ReqCtx, Deadline, Duration, State) ->
    #{
        id := ID,
        options := #{namespace := NS} = Options,
        storage_machine := #{status := Status}
    } = State,
    {ok, QueueName, TargetTimestamp} = extract_timer_queue_info(Status),
    emit_beat(Options, #mg_core_timer_process_finished{
        queue = QueueName,
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = TargetTimestamp,
        deadline = Deadline,
        duration = Duration
    });
emit_post_process_timer_beats(_Impact, _ReqCtx, _Deadline, _Duration, _State) ->
    ok.

-spec extract_timer_queue_info(machine_status()) ->
    {ok, normal | retries, genlib_time:ts()} | {error, not_timer}.
extract_timer_queue_info({waiting, Timestamp, _, _}) ->
    {ok, normal, Timestamp};
extract_timer_queue_info({retrying, Timestamp, _, _, _}) ->
    {ok, retries, Timestamp};
extract_timer_queue_info(_Other) ->
    {error, not_timer}.

-spec emit_machine_load_beat(options(), mg_core:ns(), mg_core:id(), request_context(), StorageMachine) -> ok when
    StorageMachine :: storage_machine() | unknown | nonexistent.
emit_machine_load_beat(Options, Namespace, ID, ReqCtx, nonexistent) ->
    ok = emit_beat(Options, #mg_core_machine_lifecycle_created{
        namespace = Namespace,
        machine_id = ID,
        request_context = ReqCtx
    });
emit_machine_load_beat(Options, Namespace, ID, ReqCtx, _StorageMachine) ->
    ok = emit_beat(Options, #mg_core_machine_lifecycle_loaded{
        namespace = Namespace,
        machine_id = ID,
        request_context = ReqCtx
    }).

%%

-spec storage_options(options()) ->
    storage_options().
storage_options(#{storage := Storage}) ->
    Storage.

-spec processor_options(options()) ->
    processor_options().
processor_options(#{processor := Processor}) ->
    Processor.

-spec try_suicide(state(), request_context()) ->
    ok | no_return().
try_suicide(#{options := Options = #{suicide_probability := Prob}, id := ID, namespace := NS}, ReqCtx) ->
    case (Prob =/= undefined) andalso (rand:uniform() < Prob) of
        true ->
            ok = emit_beat(Options, #mg_core_machine_lifecycle_committed_suicide{
                namespace = NS,
                machine_id = ID,
                request_context = ReqCtx,
                suicide_probability = Prob
            }),
            erlang:exit(self(), kill);
        false ->
            ok
    end;
try_suicide(#{}, _) ->
    ok.

%%
%% retrying
%%
-spec do_with_retry(Options, ID, Fun, mg_core_retry:strategy(), request_context(), atom()) -> Result when
    Options :: options(),
    ID :: mg_core:id(),
    Fun :: fun(() -> Result),
    Result :: any().
do_with_retry(Options = #{namespace := NS}, ID, Fun, RetryStrategy, ReqCtx, BeatCtx) ->
    try
        Fun()
    catch throw:(Reason={transient, _}):ST ->
        NextStep = genlib_retry:next_step(RetryStrategy),
        ok = emit_beat(Options, #mg_core_machine_lifecycle_transient_error{
            context = BeatCtx,
            namespace = NS,
            machine_id = ID,
            exception = {throw, Reason, ST},
            request_context = ReqCtx,
            retry_strategy = RetryStrategy,
            retry_action = NextStep
        }),
        case NextStep of
            {wait, Timeout, NewRetryStrategy} ->
                ok = timer:sleep(Timeout),
                do_with_retry(Options, ID, Fun, NewRetryStrategy, ReqCtx, BeatCtx);
            finish ->
                throw(Reason)
        end
    end.

%%
%% logging
%%
-spec emit_beat(options(), mg_core_pulse:beat()) -> ok.
emit_beat(#{pulse := Handler}, Beat) ->
    ok = mg_core_pulse:handle_beat(Handler, Beat).

%%
%% worker helpers
%%
-spec hibernate_timeout(worker_state()) ->
    timeout().
hibernate_timeout(#{hibernate_timeout:=Timeout}) ->
    Timeout.

-spec unload_timeout(worker_state()) ->
    timeout().
unload_timeout(#{unload_timeout:=Timeout}) ->
    Timeout.

-spec schedule_unload_timer(worker_state()) ->
    worker_state().
schedule_unload_timer(#{unload_tref := UnloadTRef} = State) ->
    _ = case UnloadTRef of
        undefined ->
            ok;
        TRef ->
            erlang:cancel_timer(TRef)
    end,
    State#{unload_tref := start_timer(State)}.

-spec start_timer(worker_state()) ->
    reference().
start_timer(State) ->
    erlang:start_timer(unload_timeout(State), erlang:self(), unload).
