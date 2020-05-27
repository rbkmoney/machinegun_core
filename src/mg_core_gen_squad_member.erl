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

%%% Squad behaviour
%%%
%%% A squad is a group of processes (usually distributed over a cluster of
%%% nodes) with dynamic membership which choose a leader among themselves
%%% in a simple yet only eventually consistent manner. Implementation is
%%% is simple: everyone knows everyone else and sends them heartbeats,
%%% so it's not well suited for large clusters (more than 10 nodes).
%%%
-module(mg_core_gen_squad_member).

%%

-type rank()  :: mg_core_gen_squad:rank().
-type squad() :: mg_core_gen_squad:squad().
-type from()  :: {pid(), _}.
-type vsn()   :: _Vsn | {down, _Vsn}.

-type reason() ::
    normal | shutdown | {shutdown, _} | _.

-type reply(Reply, St) ::
    {reply, Reply, St} |
    {reply, Reply, St, _Timeout :: pos_integer() | hibernate} |
    noreply(St).

-type noreply(St) ::
    {noreply, St} |
    {noreply, St, _Timeout :: pos_integer() | hibernate} |
    stop(St).

-type stop(St) ::
    {stop, _Reason, St}.

-callback init(_Args) ->
    {ok, _State} | ignore | {stop, _Reason}.

-callback discover(State) ->
    {ok, [pid()], State} | stop(State).

-callback handle_rank_change(rank(), squad(), State) ->
    noreply(State).

-callback handle_call(_Call, from(), rank(), squad(), State) ->
    reply(_Reply, State).

-callback handle_cast(_Cast, rank(), squad(), State) ->
    noreply(State).

-callback handle_info(_Info, rank(), squad(), State) ->
    noreply(State).

-callback terminate(reason(), _State) ->
    _.

-callback code_change(vsn(), State, _Extra) ->
    {ok, State} | {error, _Reason}.

-optional_callbacks([
    terminate/2,
    code_change/3
]).

-export([start_link/3]).
-export([start_link/4]).

%%

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%%

-type milliseconds() :: pos_integer().

-type discovery_opts() :: #{
    initial_interval => milliseconds(), %  1000 by default
    refresh_interval => milliseconds()  % 60000 by default
}.

-type opts() :: #{
    discovery => discovery_opts(),
    heartbeat => mg_core_gen_squad:heartbeat_opts(),
    promotion => mg_core_gen_squad:promotion_opts(),
    pulse     => mg_core_gen_squad_pulse:handler()
}.

-spec start_link(module(), _Args, opts()) ->
    {ok, pid()} | ignore | {error, _}.
start_link(Module, Args, Opts) ->
    gen_server:start_link(?MODULE, {Module, Args, Opts}, []).

-spec start_link(mg_core_procreg:reg_name(), module(), _Args, opts()) ->
    {ok, pid()} | ignore | {error, _}.
start_link(RegName, Module, Args, Opts) ->
    gen_server:start_link(RegName, ?MODULE, {Module, Args, Opts}, []).

%%

-record(st, {
    squad    :: pid(),
    modstate :: {module(), _ArgsOrState},
    timers   :: #{atom() => reference()},
    opts     :: discovery_opts(),
    pulse    :: mg_core_gen_squad_pulse:handler()
}).

-type st() :: #st{}.

-spec init({module(), _Args, opts()}) ->
    {ok, st()} | ignore | {stop, _Reason}.
init({Module, Args, Opts}) ->
    Pulse = maps:get(pulse, Opts, undefined),
    St0 = #st{
        modstate = {Module, Args},
        pulse = Pulse
    },
    case invoke_callback(init, [], St0) of
        {ok, St} ->
            DOpts = maps:get(discovery, Opts, #{}),
            HOpts = maps:get(heartbeat, Opts, #{}),
            POpts = maps:get(promotion, Opts, #{}),
            {ok, SquadPid} = mg_core_gen_squad:start_link(self(), HOpts, POpts, Pulse),
            {ok, defer_discovery(St#st{squad = SquadPid, opts = set_defaults(DOpts)})};
        Ret ->
            Ret
    end.

-spec set_defaults(discovery_opts()) ->
    discovery_opts().
set_defaults(DOpts) ->
    maps:merge(
        #{
            initial_interval =>  1000,
            refresh_interval => 60000
        },
        DOpts
    ).

-spec handle_call(_Call, from(), st()) ->
    reply(_, st()).
handle_call(Call, From, St = #st{squad = SquadPid}) ->
    {Rank, Squad} = mg_core_gen_squad:get_state(SquadPid),
    invoke_callback(handle_call, [Call, From, Rank, Squad], try_cancel_timer(user, St)).

-spec handle_cast(_Cast, st()) ->
    noreply(st()).
handle_cast(Cast, St = #st{squad = SquadPid}) ->
    {Rank, Squad} = mg_core_gen_squad:get_state(SquadPid),
    invoke_callback(handle_cast, [Cast, Rank, Squad], try_cancel_timer(user, St)).

-type timer() :: discover | user.

-type info() ::
    {timeout, reference(), timer()}.

-spec handle_info(info(), st()) ->
    noreply(st()).
handle_info({timeout, TRef, Msg}, St) ->
    _ = beat({{timer, TRef}, {fired, Msg}}, St),
    handle_timeout(Msg, TRef, St);
handle_info(Info, St = #st{squad = SquadPid}) ->
    {Rank, Squad} = mg_core_gen_squad:get_state(SquadPid),
    invoke_callback(handle_info, [Info, Rank, Squad], try_cancel_timer(user, St)).

-spec handle_timeout(timer(), reference(), st()) ->
    st().
handle_timeout(discovery, TRef, St = #st{timers = Timers0}) ->
    {TRef, Timers} = maps:take(discovery, Timers0),
    try_discover(St#st{timers = Timers});
handle_timeout(user, TRef, St = #st{squad = SquadPid, timers = Timers0}) ->
    {TRef, Timers} = maps:take(user, Timers0),
    {Rank, Squad} = mg_core_gen_squad:get_state(SquadPid),
    invoke_callback(handle_info, [timeout, Rank, Squad], St#st{timers = Timers}).

-spec restart_timer(atom(), pos_integer(), st()) ->
    st().
restart_timer(Type, Timeout, St) ->
    start_timer(Type, Timeout, try_cancel_timer(Type, St)).

-spec try_cancel_timer(atom(), st()) ->
    st().
try_cancel_timer(Type, St = #st{timers = Timers}) ->
    case Timers of
        #{Type := TRef} ->
            case erlang:cancel_timer(TRef) of
                false -> receive {timeout, TRef, _} -> ok after 0 -> ok end;
                _Time -> ok
            end,
            _ = beat({{timer, TRef}, cancelled}, St),
            St#st{timers = maps:remove(Type, Timers)};
        #{} ->
            St
    end.

-spec start_timer(atom(), pos_integer(), st()) ->
    st().
start_timer(Type, Timeout, St = #st{timers = Timers}) ->
    TRef = erlang:start_timer(Timeout, self(), Type),
    _ = beat({{timer, TRef}, {started, Timeout, Type}}, St),
    St#st{timers = Timers#{Type => TRef}}.

-spec terminate(reason(), st()) ->
    _.
terminate(Reason, St) ->
    try_invoke_callback(terminate, [Reason], ok, St).

-spec code_change(_, st(), _) ->
    {ok, st()} | {error, _Reason}.
code_change(OldVsn, St, Extra) ->
    try_invoke_callback(code_change, [OldVsn], [Extra], {ok, St}, St).

%% Core logic

-spec try_discover(st()) ->
    st().
try_discover(St0 = #st{squad = SquadPid}) ->
    case invoke_callback(discover, [], St0) of
        {ok, Members, St} ->
            ok = mg_core_gen_squad:handle_discovery(SquadPid, Members),
            defer_discovery(St);
        Ret ->
            Ret
    end.

-spec defer_discovery(st()) ->
    st().
defer_discovery(St = #st{squad = Squad, opts = #{discovery := DOpts}}) ->
    Timeout = case maps:size(Squad) of
        S when S < 2 -> maps:get(initial_interval, DOpts);
        _            -> maps:get(refresh_interval, DOpts)
    end,
    restart_timer(discovery, Timeout, St).

%% Utilities

-spec invoke_callback(_Name :: atom(), _Args :: list(), st()) ->
    _Result.
invoke_callback(Name, Args, St = #st{modstate = {Module, ModState}}) ->
    handle_callback_ret(erlang:apply(Module, Name, Args ++ [ModState]), St).

-spec try_invoke_callback(_Name :: atom(), _Args :: list(), _Default, st()) ->
    _Result.
try_invoke_callback(Name, Args, Default, St) ->
    try_invoke_callback(Name, Args, [], Default, St).

-spec try_invoke_callback(_Name :: atom(), _Args :: list(), _LastArgs :: list(), _Default, st()) ->
    _Result.
try_invoke_callback(Name, Args, LastArgs, Default, St = #st{modstate = {Module, ModState}}) ->
    handle_callback_ret(
        try erlang:apply(Module, Name, Args ++ [ModState] ++ LastArgs) catch
            error:undef -> Default
        end,
        St
    ).

-spec handle_callback_ret(_Result, st()) ->
    _Result.
handle_callback_ret({ok, ModSt}, St) ->
    {ok, update_modstate(ModSt, St)};
handle_callback_ret({ok, Result, ModSt}, St) ->
    {ok, Result, update_modstate(ModSt, St)};
handle_callback_ret({reply, Reply, ModSt}, St) ->
    {reply, Reply, update_modstate(ModSt, St)};
handle_callback_ret({reply, Reply, ModSt, hibernate}, St) ->
    {reply, Reply, update_modstate(ModSt, St), hibernate};
handle_callback_ret({reply, Reply, ModSt, Timeout}, St) ->
    {reply, Reply, start_timer(user, Timeout, update_modstate(ModSt, St))};
handle_callback_ret({noreply, ModSt}, St) ->
    {noreply, update_modstate(ModSt, St)};
handle_callback_ret({noreply, ModSt, hibernate}, St) ->
    {noreply, update_modstate(ModSt, St), hibernate};
handle_callback_ret({noreply, ModSt, Timeout}, St) ->
    {noreply, start_timer(user, Timeout, update_modstate(ModSt, St))};
handle_callback_ret({stop, Reason, ModSt}, St) ->
    {stop, Reason, update_modstate(ModSt, St)};
handle_callback_ret(Ret, _St) ->
    Ret.

-spec update_modstate(_ModState, st()) ->
    st().
update_modstate(ModSt, St = #st{modstate = {Module, _}}) ->
    St#st{modstate = {Module, ModSt}}.

%%

-spec beat(mg_core_gen_squad_pulse:beat(), st()) ->
    _.
beat(Beat, #st{pulse = Handler}) when Handler /= undefined ->
    mg_core_gen_squad_pulse:handle_beat(Handler, Beat);
beat(_Beat, _St) ->
    ok.
