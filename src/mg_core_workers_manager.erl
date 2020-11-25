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
%%% Запускаемые по требованию именнованные процессы.
%%% Могут обращаться друг к другу.
%%% Регистрируются по идентификатору.
%%%
-module(mg_core_workers_manager).

-include_lib("machinegun_core/include/pulse.hrl").

%% API
-export_type([options/0]).
-export_type([queue_limit/0]).
-export_type([shutdown/0]).

-export([child_spec    /2]).
-export([start_link    /1]).
-export([call          /5]).
-export([brutal_kill   /2]).
-export([is_alive      /2]).
-export([list          /2]).

%% Callbacks

-callback start_link(worker_options(), gen_reg_name(), mg_core:id(), req_ctx(), deadline()) ->
    mg_core_utils:gen_start_ret().

-callback call(worker_options(), ref(), call(), req_ctx(), deadline()) ->
    call_reply().

%% Types
-type options() :: #{
    namespace               := mg_core:ns(),
    registry                := mg_core_procreg:options(),
    worker                  := mg_core_utils:mod_opts(worker_options()),
    pulse                   := mg_core_pulse:handler(),
    message_queue_len_limit => queue_limit(),
    sidecar                 => mg_core_utils:mod_opts(),
    shutdown                => shutdown()
}.

-type queue_limit() :: non_neg_integer().
-type shutdown() :: brutal_kill | timeout().  % like supervisor:shutdown()

%% Internal types
-type id() :: mg_core:id().
-type req_ctx() :: mg_core:request_context().
-type gen_ref() :: mg_core_utils:gen_ref().
-type gen_reg_name() :: mg_core_utils:gen_reg_name().
-type maybe(T) :: T | undefined.
-type deadline() :: mg_core_deadline:deadline().
-type worker_options() :: term().
-type ref() :: mg_core_utils:gen_ref().
-type call() :: term().
-type call_reply() :: term().

%% Constants
-define(default_message_queue_len_limit, 50).
-define(worker_id(NS, ID), {?MODULE, {worker, NS, ID}}).

%%
%% API
%%

-spec child_spec(options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(options()) ->
    mg_core_utils:gen_start_ret().
start_link(Options) ->
    mg_core_utils_supervisor_wrapper:start_link(
        #{strategy => rest_for_one},
        mg_core_utils:lists_compact([
            manager_child_spec(Options),
            sidecar_child_spec(Options)
        ])
    ).

-spec manager_child_spec(options()) ->
    supervisor:child_spec().
manager_child_spec(Options) ->
    Args = [
        self_reg_name(Options),
        #{strategy => simple_one_for_one},
        [worker_child_spec(Options, worker)]
    ],
    #{
        id    => manager,
        start => {mg_core_utils_supervisor_wrapper, start_link, Args},
        type  => supervisor
    }.

-spec sidecar_child_spec(options()) ->
    supervisor:child_spec() | undefined.
sidecar_child_spec(#{sidecar := Sidecar} = Options) ->
    mg_core_utils:apply_mod_opts(Sidecar, child_spec, [Options, sidecar]);
sidecar_child_spec(#{}) ->
    undefined.

% sync
-spec call(options(), id(), call(), maybe(req_ctx()), deadline()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline) ->
    case mg_core_deadline:is_reached(Deadline) of
        false ->
            call(Options, ID, Call, ReqCtx, Deadline, true);
        true ->
            {error, {transient, worker_call_deadline_reached}}
    end.

-spec call(options(), id(), call(), maybe(req_ctx()), deadline(), boolean()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline, CanRetry) ->
    Ref = worker_ref(Options, ID),
    {WorkerModule, WorkerOptions} = worker_and_options(Options),
    MFArgs = {WorkerModule, call, [WorkerOptions, Ref, Call, ReqCtx, Deadline]},
    try mg_core_procreg:call(registry_options(Options), MFArgs) catch
        exit:Reason ->
            handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason, CanRetry)
    end.

-spec handle_worker_exit(options(), id(), call(), maybe(req_ctx()), deadline(), _Reason, boolean()) ->
    _Reply | {error, _}.
handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason, CanRetry) ->
    MaybeRetry = case CanRetry of
        true ->
            fun (_Details) -> start_and_retry_call(Options, ID, Call, ReqCtx, Deadline) end;
        false ->
            fun (Details) -> {error, {transient, Details}} end
    end,
    case Reason of
        % We have to take into account that `gen_server:call/2` wraps exception details in a
        % tuple with original call MFA attached.
        % > https://github.com/erlang/otp/blob/OTP-21.3/lib/stdlib/src/gen_server.erl#L215
        noproc                 -> MaybeRetry(noproc);
        {noproc    , _MFA}     -> MaybeRetry(noproc);
        {normal    , _MFA}     -> MaybeRetry(normal);
        {shutdown  , _MFA}     -> MaybeRetry(shutdown);
        {timeout   , _MFA}     -> {error, Reason};
        {killed    , _MFA}     -> {error, {timeout, killed}};
        {transient , _Details} -> {error, Reason};
        Unknown                -> {error, {unexpected_exit, Unknown}}
    end.

-spec start_and_retry_call(options(), id(), call(), maybe(req_ctx()), deadline()) ->
    _Reply | {error, _}.
start_and_retry_call(Options, ID, Call, ReqCtx, Deadline) ->
    %
    % NOTE возможно тут будут проблемы и это место надо очень хорошо отсмотреть
    %  чтобы потом не ловить неожиданных проблем
    %
    % идея в том, что если нет процесса, то мы его запускаем
    %
    case start_child(Options, ID, ReqCtx, Deadline) of
        {ok, _} ->
            call(Options, ID, Call, ReqCtx, Deadline, false);
        {error, {already_started, _}} ->
            call(Options, ID, Call, ReqCtx, Deadline, false);
        {error, Reason} ->
            {error, Reason}
    end.

-spec brutal_kill(options(), id()) ->
    ok.
brutal_kill(Options, ID) ->
    case mg_core_utils:gen_reg_name_to_pid(worker_ref(Options, ID)) of
        undefined ->
            ok;
        Pid ->
            try
                true = erlang:exit(Pid, kill),
                ok
            catch exit:noproc ->
                ok
            end
    end.


-spec is_alive(options(), id()) ->
    boolean().
is_alive(Options, ID) ->
    Pid = mg_core_utils:gen_reg_name_to_pid(worker_ref(Options, ID)),
    Pid =/= undefined andalso erlang:is_process_alive(Pid).

-spec worker_and_options(options()) ->
    {module(), worker_options()}.
worker_and_options(#{worker := WorkerOptions}) ->
    mg_core_utils:separate_mod_opts(WorkerOptions).

-spec list(mg_core_procreg:options(), mg_core:ns()) -> % TODO nonuniform interface
    [{mg_core:ns(), mg_core:id(), pid()}].
list(Procreg, NS) ->
    [
        {NS, ID, Pid} ||
            {?worker_id(_, ID), Pid} <- mg_core_procreg:select(Procreg, ?worker_id(NS, '$1'))
    ].

%%
%% local
%%

-spec worker_child_spec(options(), term()) ->
    supervisor:child_spec().
worker_child_spec(Options, ChildID) ->
    Shutdown = maps:get(shutdown, Options, 5000),
    {WorkerModule, WorkerOptions} = worker_and_options(Options),
    Spec = #{
        id       => ChildID,
        start    => {WorkerModule, start_link, [WorkerOptions]},
        restart  => temporary,
        shutdown => Shutdown
    },
    mg_core_procreg:wrap_child_spec(
        registry_options(Options),
        Spec
    ).

-spec start_child(options(), id(), maybe(req_ctx()), deadline()) ->
    {ok, pid()} | {error, term()}.
start_child(Options, ID, ReqCtx, Deadline) ->
    SelfRef = self_ref(Options),
    WorkerName = worker_reg_name(Options, ID),
    #{namespace := NS, pulse := Pulse} = Options,
    MsgQueueLimit = message_queue_len_limit(Options),
    MsgQueueLen = mg_core_utils:msg_queue_len(SelfRef),
    ok = mg_core_pulse:handle_beat(Pulse, #mg_core_worker_start_attempt{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        msg_queue_len = MsgQueueLen,
        msg_queue_limit = MsgQueueLimit
    }),
    case MsgQueueLen < MsgQueueLimit of
        true ->
            do_start_child(SelfRef, WorkerName, ID, ReqCtx, Deadline);
        false ->
            {error, {transient, overload}}
    end.

-spec do_start_child(gen_ref(), mg_core_procreg:reg_name(), id(), maybe(req_ctx()), deadline()) ->
    {ok, pid()} | {error, term()}.
do_start_child(SelfRef, WorkerName, ID, ReqCtx, Deadline) ->
    try
        supervisor:start_child(SelfRef, [[WorkerName, ID, ReqCtx, Deadline]])
    catch
        exit:{timeout, Reason} ->
            {error, {timeout, Reason}}
    end.

-spec message_queue_len_limit(options()) ->
    queue_limit().
message_queue_len_limit(Options) ->
    maps:get(message_queue_len_limit, Options, ?default_message_queue_len_limit).

-spec self_ref(options()) ->
    gen_ref().
self_ref(Options) ->
    {via, gproc, gproc_self_key(Options)}.

-spec self_reg_name(options()) ->
    mg_core_utils:gen_reg_name().
self_reg_name(Options) ->
    {via, gproc, gproc_self_key(Options)}.

-spec gproc_self_key(options()) ->
    gproc:key().
gproc_self_key(Options) ->
    {n, l, self_wrap(maps:get(namespace, Options))}.

-spec self_wrap(mg_core:ns()) ->
    term().
self_wrap(NS) ->
    {?MODULE, {manager, NS}}.

-spec worker_ref(options(), mg_core:id()) ->
    mg_core_procreg:ref().
worker_ref(Options, ID) ->
    #{namespace := NS} = Options,
    mg_core_procreg:ref(registry_options(Options), ?worker_id(NS, ID)).

-spec worker_reg_name(options(), mg_core:id()) ->
    mg_core_procreg:reg_name().
worker_reg_name(Options, ID) ->
    #{namespace := NS} = Options,
    mg_core_procreg:reg_name(registry_options(Options), ?worker_id(NS, ID)).

-spec registry_options(options()) ->
    mg_core_procreg:options().
registry_options(#{registry := RegistryOptions}) ->
    RegistryOptions.
