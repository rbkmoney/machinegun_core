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

-module(mg_core_instant_timer_task_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([instant_start_test/1]).
-export([without_shedulers_test/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([process_machine/7]).
-export([get_machine/4]).


%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()] | {group, atom()}.
all() ->
    [
       instant_start_test,
       without_shedulers_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_machine, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

%%
%% tests
%%
-define(req_ctx, <<"req_ctx">>).

-spec instant_start_test(config()) ->
    _.
instant_start_test(_C) ->
    NS = <<"test">>,
    ID = genlib:to_binary(?FUNCTION_NAME),
    {Pid, OptionsRef} = start_automaton(namespace_options(NS)),

    ok = mg_core_namespace:start(OptionsRef, ID, 0, ?req_ctx, mg_core_deadline:default()),
     0 = mg_core_namespace:call(OptionsRef, ID, get, ?req_ctx, mg_core_deadline:default()),
    ok = mg_core_namespace:call(OptionsRef, ID, force_timeout, ?req_ctx, mg_core_deadline:default()),
    F = fun() ->
            mg_core_namespace:call(OptionsRef, ID, get, ?req_ctx, mg_core_deadline:default())
        end,
    mg_core_ct_helper:assert_wait_expected(1, F, mg_core_retry:new_strategy({linear, _Retries = 10, _Timeout = 100})),

    ok = stop_automaton(OptionsRef, Pid).

-spec without_shedulers_test(config()) ->
    _.
without_shedulers_test(_C) ->
    NS = <<"test">>,
    ID = genlib:to_binary(?FUNCTION_NAME),
    {Pid, OptionsRef} = start_automaton(namespace_options_wo_shedulers(NS)),

    ok = mg_core_namespace:start(OptionsRef, ID, 0, ?req_ctx, mg_core_deadline:default()),
     0 = mg_core_namespace:call(OptionsRef, ID, get, ?req_ctx, mg_core_deadline:default()),
    ok = mg_core_namespace:call(OptionsRef, ID, force_timeout, ?req_ctx, mg_core_deadline:default()),
    % machine is still alive
    _  = mg_core_namespace:call(OptionsRef, ID, get, ?req_ctx, mg_core_deadline:default()),

    ok = stop_automaton(OptionsRef, Pid).

%%
%% processor
%%
-record(machine_state, {
    counter = 0 :: integer(),
    timer = undefined :: {genlib_time:ts(), mg_core_machine:request_context()} | undefined
}).
-type machine_state() :: #machine_state{}.

-spec get_machine(Options, ID, Args, PackedState) -> Result when
    Options :: any(),
    ID :: mg_core:id(),
    Args :: undefined,
    PackedState :: mg_core_machine:machine_state(),
    Result :: machine_state().
get_machine(_Options, _ID, undefined, State) ->
    decode_state(State).

-spec process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, MachineState) -> Result when
    Options :: any(),
    ID :: mg_core:id(),
    Impact :: mg_core_machine:processor_impact(),
    PCtx :: mg_core_machine:processing_context(),
    ReqCtx :: mg_core_machine:request_context(),
    Deadline :: mg_core_deadline:deadline(),
    MachineState :: mg_core_machine:machine_state(),
    Result :: mg_core_machine:processor_result().
process_machine(_, _, Impact, _, ReqCtx, _, EncodedState) ->
    State = decode_state(EncodedState),
    {Reply, Action, NewState} = do_process_machine(Impact, ReqCtx, State),
    {Reply, try_set_timer(NewState, Action), encode_state(NewState)}.

-spec do_process_machine(mg_core_machine:processor_impact(), mg_core_machine:request_context(), machine_state()) ->
    mg_core_machine:processor_result().
do_process_machine({init, Counter}, ?req_ctx, State) ->
    {{reply, ok}, sleep, State#machine_state{counter = Counter}};
do_process_machine({call, get}, ?req_ctx, #machine_state{counter = Counter} = State) ->
    ct:pal("Counter is ~p", [Counter]),
    {{reply, Counter}, sleep, State};
do_process_machine({call, force_timeout}, ?req_ctx = ReqCtx, State) ->
    TimerTarget = genlib_time:unow(),
    {{reply, ok}, sleep, State#machine_state{timer = {TimerTarget, ReqCtx}}};
do_process_machine(timeout, ?req_ctx, #machine_state{counter = Counter} = State) ->
    ct:pal("Counter updated to ~p", [Counter + 1]),
    {{reply, ok}, sleep, State#machine_state{counter = Counter + 1, timer = undefined}}.

-spec encode_state(machine_state()) -> mg_core_machine:machine_state().
encode_state(#machine_state{counter = Counter, timer = {TimerTarget, ReqCtx}}) ->
    [Counter, TimerTarget, ReqCtx];
encode_state(#machine_state{counter = Counter, timer = undefined}) ->
    [Counter].

-spec decode_state(mg_core_machine:machine_state()) -> machine_state().
decode_state(null) ->
    #machine_state{};
decode_state([Counter, TimerTarget, ReqCtx]) ->
    #machine_state{counter = Counter, timer = {TimerTarget, ReqCtx}};
decode_state([Counter]) ->
    #machine_state{counter = Counter}.

-spec try_set_timer(machine_state(), mg_core_machine:processor_flow_action()) ->
    mg_core_machine:processor_flow_action().
try_set_timer(#machine_state{timer = {TimerTarget, ReqCtx}}, sleep) ->
    {wait, TimerTarget, ReqCtx, 5000};
try_set_timer(#machine_state{timer = undefined}, Action) ->
    Action.

%%
%% utils
%%
-spec start_automaton(mg_core_namespace:options()) ->
    {pid(), mg_core_namespace:options_ref()}.
start_automaton(Options) ->
    OptionsRef = mg_core_namespace:make_options_ref(maps:get(namespace, Options)),
    ok = mg_core_namespace:save_options(Options, OptionsRef),
    {mg_core_utils:throw_if_error(mg_core_namespace:start_link(OptionsRef)), OptionsRef}.

-spec stop_automaton(mg_core_namespace:options_ref(), pid()) ->
    ok.
stop_automaton(OptionsRef, Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    _ = persistent_term:erase(OptionsRef),
    ok.

-spec namespace_options(mg_core:ns()) ->
    mg_core_namespace:options().
namespace_options(NS) ->
    Scheduler = #{
        min_scan_delay => timer:hours(1)
    },
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_core_ct_helper:build_storage(NS, mg_core_storage_memory),
        registry  => mg_core_procreg_gproc,
        pulse     => ?MODULE,
        schedulers => #{
            timers         => Scheduler,
            timers_retries => Scheduler,
            overseer       => Scheduler
        }
    }.

-spec namespace_options_wo_shedulers(mg_core:ns()) ->
    mg_core_namespace:options().
namespace_options_wo_shedulers(NS) ->
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_core_ct_helper:build_storage(NS, mg_core_storage_memory),
        registry  => mg_core_procreg_gproc,
        pulse     => ?MODULE,
        schedulers => #{
            % none
        }
    }.

-spec handle_beat(_, mg_core_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
