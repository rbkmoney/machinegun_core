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

%%% TODO
%%%  - Streamline event interface
%%%  - More tests
%%%

%%% ROADMAP
%%%  - Seems that squad owning member server will be far easier to
%%%    implement, not the other way around. We may probably leak
%%%    member server pids when proxying calls, this is probably ok.
%%%  - We don't need member timers. It's simpler to just cull squad
%%%    view on every heartbeat tick and incoming broadcast.
%%%  - We would need to broadcast whole view not just some pids, this
%%%    way consistent squad view is simple to achieve. Delays and clock
%%%    skews will become our primary concern though.
%%%  - Member should probably cull itself if it lags behind too much?
%%%    How one could tell if he lags behind or some other member is too
%%%    far in the future?
%%%
-module(mg_core_gen_squad).

%%

-export([leader/1]).
-export([members/1]).

-type rank()      :: leader | follower.
-type age()       :: pos_integer().
-type timestamp() :: integer().

-opaque squad() :: #{ % presumably _always_ nonempty
    pid() => member()
}.

-type member() :: #{
    age             => age(),
    last_contact    => timestamp(),
    loss_timer      => reference()
}.

-export_type([rank/0]).
-export_type([squad/0]).
-export_type([member/0]).

%%

-export([start_link/4]).
-export([get_state/1]).
-export([handle_discovery/2]).

%%

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

%%

-type milliseconds() :: pos_integer().

-type heartbeat_opts() :: #{
    broadcast_interval => milliseconds(), %  200 by default
    loss_timeout       => milliseconds()  % 1000 by default
}.

-type promotion_opts() :: #{
    min_squad_age => age() % 3 by default
}.

-type pulse() ::
    mg_core_gen_squad_pulse:handler() | undefined.

-export_type([heartbeat_opts/0]).
-export_type([promotion_opts/0]).

-spec start_link(pid(), heartbeat_opts(), promotion_opts(), pulse()) ->
    {ok, pid()} | ignore | {error, _}.
start_link(Parent, HOpts, POpts, Pulse) ->
    St = mk_state(Parent, set_heartbeat_defaults(HOpts), set_promotion_defaults(POpts), Pulse),
    gen_server:start_link(?MODULE, St, []).

-spec set_heartbeat_defaults(heartbeat_opts()) ->
    heartbeat_opts().
set_heartbeat_defaults(HOpts) ->
    maps:merge(#{
        broadcast_interval =>  400,
        loss_timeout       => 1000
    }, HOpts).

-spec set_promotion_defaults(promotion_opts()) ->
    promotion_opts().
set_promotion_defaults(POpts) ->
    maps:merge(#{
        min_squad_age => 3
    }, POpts).

-spec get_state(pid()) ->
    {rank(), squad()}.
get_state(Pid) ->
    gen_server:call(Pid, get_state, infinity).

%%

-spec leader(squad()) ->
    pid().
leader(Squad) ->
    % NOTE
    % Adding some controlled randomness here.
    % So that `node id` term would not dominate in ordering in different squads on some set of nodes
    % which could happen to have "same" pids in them, e.g pid 85 @ node 1, pid 85 @ node 2 and so
    % forth. This may affect squads started under a supervisor on different nodes, since startup
    % order of whole supervision trees in a release is fixed and determinate.
    Members = lists:sort(members(Squad)),
    Size = length(Members),
    Seed = erlang:phash2(Members),
    State = rand:seed_s(exrop, {Size, Seed, Seed}),
    {N, _} = rand:uniform_s(Size, State),
    lists:nth(N, Members).

-spec rank(pid(), squad()) ->
    rank().
rank(Pid, Squad) ->
    case leader(Squad) of
        Pid -> leader;
        _   -> follower
    end.

-spec members(squad()) ->
    [pid()].
members(Squad) ->
    maps:keys(Squad).

%%

-record(st, {
    parent    :: pid(),
    squad     :: squad(),
    rank      :: rank() | undefined,
    heartbeat :: heartbeat_opts(),
    promotion :: promotion_opts(),
    pulse     :: pulse()
}).

-type st() :: #st{}.

-spec mk_state(pid(), heartbeat_opts(), promotion_opts(), pulse()) ->
    st().
mk_state(Parent, HOpts, POpts, Pulse) ->
    #st{
        parent    = Parent,
        squad     = #{},
        heartbeat = HOpts,
        promotion = POpts,
        pulse     = Pulse
    }.

-spec init(st()) ->
    {ok, st()} | ignore | {stop, _Reason}.
init(St) ->
    {ok, add_member(self(), St)}.

-type from() :: {pid(), _}.

-spec handle_call(get_state, from(), st()) ->
    {reply, {rank(), squad()}, st()}.
handle_call(get_state, _From, St = #st{squad = Squad}) ->
    {reply, {get_rank(St), Squad}, St};
handle_call(Call, From, St) ->
    _ = beat({unexpected, {{call, From}, Call}}, St),
    {noreply, St}.

-spec get_rank(st()) ->
    rank().
get_rank(#st{rank = Rank}) when Rank /= undefined ->
    Rank;
get_rank(#st{rank = undefined}) ->
    follower.

-type cast() ::
    mg_core_gen_squad_heart:envelope() |
    heartbeat.

-spec handle_cast(cast(), st()) ->
    {noreply, st()}.
handle_cast({'$squad', Payload = #{vsn := 1, msg := _, from := _}}, St) ->
    _ = beat({{broadcast, Payload}, received}, St),
    handle_broadcast(Payload, St);
handle_cast(heartbeat, St) ->
    handle_heartbeat_feedback(St);
handle_cast(Cast, St) ->
    _ = beat({unexpected, {cast, Cast}}, St),
    {noreply, St}.

-spec handle_broadcast(mg_core_gen_squad_heart:payload(), st()) ->
    {noreply, st()}.
handle_broadcast(#{msg := howdy, from := Pid, members := Pids}, St = #st{squad = SquadWas}) ->
    St1 = refresh_member(Pid, add_members([Pid | Pids], St)),
    % NOTE
    % Simple approach: retransmit another broadcast to those members we see for the first time.
    % It's possible to reduce message rate here at the expense of higher squad convergence time,
    % for example taking just half or some m << size(Squad) of new squad members randomly. Would
    % be better to consider some gossip protocol scheme instead though.
    ok = broadcast(howdy, newbies(SquadWas), broadcast, St1),
    try_update_squad(St1).

-type timer() :: {lost, pid()}.
-type info() ::
    {timeout, reference(), timer()}.

-spec handle_info(info(), st()) ->
    {noreply, st()}.
handle_info({timeout, TRef, {lost, Pid} = Msg}, St) ->
    _ = beat({{timer, TRef}, {fired, Msg}}, St),
    try_update_squad(handle_loss_timeout(TRef, Pid, St));
handle_info(Info, St) ->
    _ = beat({unexpected, {info, Info}}, St),
    {noreply, St}.

%% Core logic

-spec handle_heartbeat_feedback(st()) ->
    st().
handle_heartbeat_feedback(St) ->
    try_update_squad(refresh_member(self(), St)).

-spec try_update_squad(st()) ->
    st().
try_update_squad(St = #st{opts = Opts}) ->
    Rank = try_promote(St),
    case St of
        #st{} when Rank == undefined ->
            {noreply, St};
        #st{rank = Rank} ->
            {noreply, St};
        #st{} ->
            St1 = St#st{rank = Rank},
            _ = beat({rank, {changed, Rank}}, St2),
            invoke_callback(handle_rank_change, [Rank, Squad], try_cancel_st_timer(user, St2))
    end.

-spec try_promote(st()) ->
    rank() | undefined.
try_promote(#st{squad = Squad, promotion = #{min_squad_age := MinAge}}) ->
    % NOTE
    % Inequality `number()` < `atom()` always holds.
    SquadAge = maps:fold(fun (_, Member, Age) -> min(Age, maps:get(age, Member, 0)) end, undefined, Squad),
    case SquadAge of
        N when is_integer(N), N >= MinAge ->
            rank(self(), Squad);
        _ ->
            undefined
    end.

%%

-spec add_members([pid()], st()) ->
    squad().
add_members(Members, St) ->
    lists:foldl(fun add_member/2, St, Members).

-spec add_member(pid(), st()) ->
    squad().
add_member(Pid, St = #st{squad = Squad}) when not is_map_key(Pid, Squad) ->
    Member = watch_member(Pid, St),
    _ = beat({{member, Pid}, added}, St),
    St#st{squad = Squad#{Pid => Member}};
add_member(_Pid, St) ->
    St.

-spec refresh_member(pid(), st()) ->
    squad().
refresh_member(Pid, St = #st{squad = Squad}) when is_map_key(Pid, Squad) ->
    Member = account_heartbeat(rewatch_member(Pid, maps:get(Pid, Squad), St)),
    _ = beat({{member, Pid}, {refreshed, Member}}, St),
    St#st{squad = Squad#{Pid := Member}};
refresh_member(_Pid, St) ->
    St.

-spec remove_member(pid(), member(), _Reason :: lost | {down, _}, st()) ->
    st().
remove_member(Pid, Member, Reason, St = #st{squad = Squad}) when is_map_key(Pid, Squad) ->
    _ = unwatch_member(Member, St),
    _ = beat({{member, Pid}, {removed, Member, Reason}}, St),
    St#st{squad = maps:remove(Pid, Squad)};
remove_member(_Pid, _Member, _Reason, St) ->
    St.

-spec watch_member(pid(), st()) ->
    member().
watch_member(Pid, St) when Pid /= self() ->
    defer_loss(Pid, #{}, St);
watch_member(Pid, _St) when Pid == self() ->
    #{}.

-spec rewatch_member(pid(), member(), st()) ->
    member().
rewatch_member(Pid, Member0, St) when Pid /= self() ->
    {TRef, Member} = maps:take(loss_timer, Member0),
    ok = cancel_timer(TRef, St),
    defer_loss(Pid, Member, St);
rewatch_member(Pid, Member, _St) when Pid == self() ->
    Member.

-spec unwatch_member(member(), st()) ->
    member().
unwatch_member(Member = #{loss_timer := TRef}, St) ->
    ok = cancel_timer(TRef, St),
    maps:remove(loss_timer, Member);
unwatch_member(Member = #{}, _St) ->
    Member.

-spec defer_loss(pid(), member(), st()) ->
    member().
defer_loss(Pid, Member, St = #st{heartbeat = #{loss_timeout := Timeout}}) ->
    false = maps:is_key(loss_timer, Member),
    Member#{loss_timer => start_timer({lost, Pid}, Timeout, St)}.

-spec handle_loss_timeout(reference(), pid(), st()) ->
    st().
handle_loss_timeout(TRef, Pid, St = #st{squad = Squad}) ->
    {TRef, Member} = maps:take(loss_timer, maps:get(Pid, Squad)),
    remove_member(Pid, Member, lost, St).

-spec account_heartbeat(member()) ->
    member().
account_heartbeat(Member) ->
    Member#{
        age          => maps:get(age, Member, 0) + 1,
        last_contact => erlang:system_time(millisecond)
    }.

%%

-type recepient_filter() :: fun((pid()) -> boolean()).

-spec broadcast(mg_core_gen_squad_heart:message(), recepient_filter(), _Ctx, st()) ->
    ok.
broadcast(Message, RecepientFilter, Ctx, St = #st{squad = Squad}) ->
    Self = self(),
    Members = members(maps:remove(Self, Squad)),
    Recepients = lists:filter(RecepientFilter, Members),
    Pulse = maps:get(pulse, Opts, undefined),
    mg_core_gen_squad_heart:broadcast(Message, Self, Members, Recepients, Ctx, Pulse).

-spec newbies(squad()) ->
    recepient_filter().
newbies(Squad) ->
    fun (Pid) -> not maps:is_key(Pid, Squad) end.

%%

-spec start_timer(_Msg, timeout(), st()) ->
    reference().
start_timer(Msg, Timeout, St) ->
    TRef = erlang:start_timer(Timeout, self(), Msg),
    _ = beat({{timer, TRef}, {started, Timeout, Msg}}, St),
    TRef.

-spec cancel_timer(reference(), st()) ->
    ok.
cancel_timer(TRef, St) ->
    _ = beat({{timer, TRef}, cancelled}, St),
    case erlang:cancel_timer(TRef) of
        false -> receive {timeout, TRef, _} -> ok after 0 -> ok end;
        _Time -> ok
    end.

%%

-spec beat(mg_core_gen_squad_pulse:beat(), st()) ->
    _.
beat(Beat, #st{pulse = Handler}) when Handler /= undefined ->
    mg_core_gen_squad_pulse:handle_beat(Handler, Beat);
beat(_Beat, _St) ->
    ok.
