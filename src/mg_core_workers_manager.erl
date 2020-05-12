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
-export_type([start_options/0]).
-export_type([call_options/0]).
-export_type([queue_limit/0]).

-export([child_spec    /2]).
-export([start_link    /1]).
-export([call          /5]).
-export([brutal_kill   /2]).
-export([is_alive      /2]).
-export([list          /2]).

%% Types
-type start_options() :: #{
    namespace               := mg_core:ns(),
    registry                := mg_core_procreg:options(),
    worker                  := mg_core_worker:start_options(),
    pulse                   := mg_core_pulse:handler(),
    message_queue_len_limit => queue_limit(),
    sidecar                 => mg_core_utils:mod_opts()
}.
-type call_options() :: #{
    namespace               := mg_core:ns(),
    registry                := mg_core_procreg:options(),
    worker                  := mg_core_worker:call_options(),
    pulse                   := mg_core_pulse:handler(),
    message_queue_len_limit => queue_limit()
}.
-type queue_limit() :: non_neg_integer().

%% Internal types
-type id() :: mg_core:id().
-type call() :: mg_core_worker:call_payload().
-type req_ctx() :: mg_core:request_context().
-type gen_ref() :: mg_core_utils:gen_ref().
-type maybe(T) :: T | undefined.
-type deadline() :: mg_core_deadline:deadline().

%% Constants
-define(default_message_queue_len_limit, 50).
-define(worker_wrap(NS, ID), {?MODULE, {worker, NS, ID}}).

%%
%% API
%%

-spec child_spec(start_options(), atom()) ->
    supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => permanent,
        type     => supervisor
    }.

-spec start_link(start_options()) ->
    mg_core_utils:gen_start_ret().
start_link(Options) ->
    mg_core_utils_supervisor_wrapper:start_link(
        #{strategy => rest_for_one},
        mg_core_utils:lists_compact([
            manager_child_spec(Options),
            sidecar_child_spec(Options)
        ])
    ).

-spec manager_child_spec(start_options()) ->
    supervisor:child_spec().
manager_child_spec(Options) ->
    Args = [
        self_reg_name(Options),
        #{strategy => simple_one_for_one},
        [
            mg_core_procreg:wrap_child_spec(
                registry_options(Options),
                mg_core_worker:child_spec(worker, worker_options(Options))
            )
        ]
    ],
    #{
        id    => manager,
        start => {mg_core_utils_supervisor_wrapper, start_link, Args},
        type  => supervisor
    }.

-spec sidecar_child_spec(start_options()) ->
    supervisor:child_spec() | undefined.
sidecar_child_spec(#{sidecar := Sidecar} = Options) ->
    mg_core_utils:apply_mod_opts(Sidecar, child_spec, [Options, sidecar]);
sidecar_child_spec(#{}) ->
    undefined.

% sync
-spec call(call_options(), id(), call(), maybe(req_ctx()), deadline()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline) ->
    case mg_core_deadline:is_reached(Deadline) of
        false ->
            call(Options, ID, Call, ReqCtx, Deadline, true);
        true ->
            {error, {transient, worker_call_deadline_reached}}
    end.

-spec call(call_options(), id(), call(), maybe(req_ctx()), deadline(), boolean()) ->
    _Reply | {error, _}.
call(Options, ID, Call, ReqCtx, Deadline, CanRetry) ->
    Ref = worker_ref(Options, ID),
    MFArgs = {mg_core_worker, call, [worker_options(Options), Ref, Call, ReqCtx, Deadline]},
    try mg_core_procreg:call(registry_options(Options), MFArgs) catch
        exit:Reason ->
            handle_worker_exit(Options, ID, Call, ReqCtx, Deadline, Reason, CanRetry)
    end.

-spec handle_worker_exit(call_options(), id(), call(), maybe(req_ctx()), deadline(), _Reason, boolean()) ->
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

-spec start_and_retry_call(call_options(), id(), call(), maybe(req_ctx()), deadline()) ->
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

-spec brutal_kill(call_options(), id()) ->
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


-spec is_alive(call_options(), id()) ->
    boolean().
is_alive(Options, ID) ->
    Pid = mg_core_utils:gen_reg_name_to_pid(worker_ref(Options, ID)),
    Pid =/= undefined andalso erlang:is_process_alive(Pid).

-spec worker_options
    (start_options()) -> mg_core_worker:start_options();
    (call_options()) -> mg_core_worker:call_options().
worker_options(#{worker := WorkerOptions}) ->
    WorkerOptions.

-spec list(mg_core_procreg:options(), mg_core:ns()) -> % TODO nonuniform interface
    [{mg_core:ns(), mg_core:id(), pid()}].
list(Procreg, NS) ->
    [
        {NS, ID, Pid} ||
            {?worker_wrap(_, ID), Pid} <- mg_core_procreg:select(Procreg, ?worker_wrap(NS, '$1'))
    ].

%%
%% local
%%
-spec start_child(call_options(), id(), maybe(req_ctx()), deadline()) ->
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

-spec message_queue_len_limit(start_options() | call_options()) ->
    queue_limit().
message_queue_len_limit(Options) ->
    maps:get(message_queue_len_limit, Options, ?default_message_queue_len_limit).

-spec self_ref(call_options()) ->
    gen_ref().
self_ref(Options) ->
    {via, gproc, gproc_self_key(Options)}.

-spec self_reg_name(start_options()) ->
    mg_core_utils:gen_reg_name().
self_reg_name(Options) ->
    {via, gproc, gproc_self_key(Options)}.

-spec gproc_self_key(start_options() | call_options()) ->
    gproc:key().
gproc_self_key(Options) ->
    {n, l, self_wrap(maps:get(namespace, Options))}.

-spec self_wrap(mg_core:ns()) ->
    term().
self_wrap(NS) ->
    {?MODULE, {manager, NS}}.

-spec worker_ref(call_options(), mg_core:id()) ->
    mg_core_procreg:ref().
worker_ref(Options, ID) ->
    #{namespace := NS} = Options,
    mg_core_procreg:ref(registry_options(Options), ?worker_wrap(NS, ID)).

-spec worker_reg_name(call_options(), mg_core:id()) ->
    mg_core_procreg:reg_name().
worker_reg_name(Options, ID) ->
    #{namespace := NS} = Options,
    mg_core_procreg:reg_name(registry_options(Options), ?worker_wrap(NS, ID)).

-spec registry_options(start_options() | call_options()) ->
    mg_core_procreg:options().
registry_options(#{registry := RegistryOptions}) ->
    RegistryOptions.
