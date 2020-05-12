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

-module(mg_core_machine_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% tests
-export([simple_test/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([process_machine/7]).
-export([get_machine/4]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        {group, with_gproc},
        {group, with_consuela}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {with_gproc    , [], [{group, base}]},
        {with_consuela , [], [{group, base}]},
        {base          , [], [
            simple_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_machine, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([consuela, machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(with_gproc, C) ->
    [{registry, mg_core_procreg_gproc} | C];
init_per_group(with_consuela, C) ->
    [{registry, {mg_core_procreg_consuela, #{pulse => ?MODULE}}} | C];
init_per_group(base, C) ->
    C.

-spec end_per_group(group_name(), config()) ->
    _.
end_per_group(_, _C) ->
    ok.

%%
%% tests
%%
-define(req_ctx, <<"req_ctx">>).

-spec simple_test(config()) ->
    _.
simple_test(C) ->
    Options = automaton_options(C),
    TestKey = <<"test_key">>,
    ID = <<"42">>,
    Pid = start_automaton(Options),

    {logic, machine_not_found} =
        (catch mg_core_namespace:call(Options, ID, get, ?req_ctx, mg_core_deadline:default())),

    ok = mg_core_namespace:start(Options, ID, {TestKey, 0}, ?req_ctx, mg_core_deadline:default()),
    {logic, machine_already_exist} =
        (catch mg_core_namespace:start(Options, ID, {TestKey, 0}, ?req_ctx, mg_core_deadline:default())),

    0  = mg_core_namespace:call (Options, ID, get              , ?req_ctx, mg_core_deadline:default()),
    ok = mg_core_namespace:call (Options, ID, increment        , ?req_ctx, mg_core_deadline:default()),
    1  = mg_core_namespace:call (Options, ID, get              , ?req_ctx, mg_core_deadline:default()),
    ok = mg_core_namespace:call (Options, ID, delayed_increment, ?req_ctx, mg_core_deadline:default()),
    ok = timer:sleep(2000),
    2  = mg_core_namespace:call (Options, ID, get              , ?req_ctx, mg_core_deadline:default()),

    % call fail/simple_repair
    {logic, machine_failed} =
        (catch mg_core_namespace:call   (Options, ID, fail, ?req_ctx, mg_core_deadline:default())),
    ok = mg_core_namespace:simple_repair(Options, ID,       ?req_ctx, mg_core_deadline:default()),
    2  = mg_core_namespace:call         (Options, ID, get , ?req_ctx, mg_core_deadline:default()),

    % call fail/repair
    {logic, machine_failed} = (catch mg_core_namespace:call  (Options, ID, fail      , ?req_ctx, mg_core_deadline:default())),
    repaired                =        mg_core_namespace:repair(Options, ID, repair_arg, ?req_ctx, mg_core_deadline:default()),
    2                       =        mg_core_namespace:call  (Options, ID, get       , ?req_ctx, mg_core_deadline:default()),

    % call fail/repair fail/repair
    {logic, machine_failed} = (catch mg_core_namespace:call(Options, ID, fail, ?req_ctx, mg_core_deadline:default())),
    {logic, machine_failed} = (catch mg_core_namespace:repair(Options, ID, fail, ?req_ctx, mg_core_deadline:default())),
    repaired = mg_core_namespace:repair(Options, ID, repair_arg, ?req_ctx, mg_core_deadline:default()),
    {logic, machine_already_working} =
        (catch mg_core_namespace:repair(Options, ID, repair_arg, ?req_ctx, mg_core_deadline:default())),
    2  = mg_core_namespace:call(Options, ID, get, ?req_ctx, mg_core_deadline:default()),

    ok  = mg_core_namespace:call(Options, ID, remove, ?req_ctx, mg_core_deadline:default()),

    {logic, machine_not_found} =
        (catch mg_core_namespace:call(Options, ID, get, ?req_ctx, mg_core_deadline:default())),

    ok = stop_automaton(Pid).

%%
%% processor
%%
-spec get_machine(_Options, mg_core:id(), _Args, mg_core_machine:machine_state()) ->
    mg_core_machine:processor_result() | no_return().
get_machine(_, _, _, State) ->
    State.

-spec process_machine(_Options, mg_core:id(), mg_core_machine:processor_impact(), _, _, _, mg_core_machine:machine_state()) ->
    mg_core_machine:processor_result() | no_return().
process_machine(_, _, {_, fail}, _, ?req_ctx, _, _) ->
    _ = exit(1),
    {noreply, sleep, []};
process_machine(_, _, {init, {TestKey, TestValue}}, _, ?req_ctx, _, null) ->
    {{reply, ok}, sleep, [TestKey, TestValue]};
process_machine(_, _, {call, get}, _, ?req_ctx, _, [TestKey, TestValue]) ->
    {{reply, TestValue}, sleep, [TestKey, TestValue]};
process_machine(_, _, {call, increment}, _, ?req_ctx, _, [TestKey, TestValue]) ->
    {{reply, ok}, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {call, delayed_increment}, _, ?req_ctx, _, State) ->
    {{reply, ok}, {wait, genlib_time:unow() + 1, ?req_ctx, 5000}, State};
process_machine(_, _, {call, remove}, _, ?req_ctx, _, State) ->
    {{reply, ok}, remove, State};
process_machine(_, _, timeout, _, ?req_ctx, _, [TestKey, TestValue]) ->
    {noreply, sleep, [TestKey, TestValue + 1]};
process_machine(_, _, {repair, repair_arg}, _, ?req_ctx, _, [TestKey, TestValue]) ->
    {{reply, repaired}, sleep, [TestKey, TestValue]}.

%%
%% utils
%%
-spec start_automaton(mg_core_namespace:start_options()) ->
    pid().
start_automaton(Options) ->
    mg_core_utils:throw_if_error(mg_core_namespace:start_link(Options)).

-spec stop_automaton(pid()) ->
    ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec automaton_options(config()) ->
    mg_core_namespace:start_options().
automaton_options(C) ->
    Scheduler = #{},
    #{
        namespace => <<"test">>,
        processor => ?MODULE,
        storage   => mg_core_storage_memory,
        registry  => ?config(registry, C),
        pulse     => ?MODULE,
        schedulers => #{
            timers         => Scheduler,
            timers_retries => Scheduler,
            overseer       => Scheduler
        }
    }.

-spec handle_beat(_, mg_core_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
