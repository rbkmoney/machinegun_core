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

-define(NAMESPACE, <<"test">>).

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

%% tests
-export([simple_test/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([pool_child_spec/2, process_machine/7]).

%% mg_core_machine_storage_cql
-export([prepare_get_query/2]).
-export([prepare_update_query/4]).
-export([read_machine_state/2]).
-export([bootstrap/3]).

-export([start/0]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, with_gproc}
        % {group, with_consuela}
    ].

-spec groups() -> [{group_name(), list(_), test_name()}].
groups() ->
    [
        {with_gproc, [], [{group, base}]},
        {with_consuela, [], [{group, base}]},
        {base, [], [
            simple_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_machine, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([consuela, machinegun_core]),
    _ = (mg_core_machine_storage_cql:bootstrap(storage_options(), ?NAMESPACE)),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(with_gproc, C) ->
    [{registry, mg_core_procreg_gproc} | C];
init_per_group(with_consuela, C) ->
    [{registry, {mg_core_procreg_consuela, #{pulse => ?MODULE}}} | C];
init_per_group(base, C) ->
    C.

-spec end_per_group(group_name(), config()) -> _.
end_per_group(_, _C) ->
    ok.

%%
%% tests
%%
-define(REQ_CTX, <<"req_ctx">>).

-spec simple_test(config()) -> _.
simple_test(C) ->
    Options = automaton_options(C),
    TestKey = <<"test_key">>,
    ID = <<"42">>,
    Pid = start_automaton(Options),

    {logic, machine_not_found} =
        (catch mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())),

    ok = mg_core_machine:start(Options, ID, {TestKey, 0}, ?REQ_CTX, mg_core_deadline:default()),
    {logic, machine_already_exist} =
        (catch mg_core_machine:start(
            Options,
            ID,
            {TestKey, 0},
            ?REQ_CTX,
            mg_core_deadline:default()
        )),

    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(Options, ID, increment, ?REQ_CTX, mg_core_deadline:default()),
    1 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(Options, ID, delayed_increment, ?REQ_CTX, mg_core_deadline:default()),
    ok = timer:sleep(2000),
    2 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),

    % call fail/simple_repair
    {logic, machine_failed} =
        (catch mg_core_machine:call(Options, ID, fail, ?REQ_CTX, mg_core_deadline:default())),
    ok = mg_core_machine:simple_repair(Options, ID, ?REQ_CTX, mg_core_deadline:default()),
    2 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),

    % call fail/repair
    {logic, machine_failed} =
        (catch mg_core_machine:call(Options, ID, fail, ?REQ_CTX, mg_core_deadline:default())),
    repaired = mg_core_machine:repair(
        Options,
        ID,
        repair_arg,
        ?REQ_CTX,
        mg_core_deadline:default()
    ),
    2 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),

    % call fail/repair fail/repair
    {logic, machine_failed} =
        (catch mg_core_machine:call(Options, ID, fail, ?REQ_CTX, mg_core_deadline:default())),
    {logic, machine_failed} =
        (catch mg_core_machine:repair(Options, ID, fail, ?REQ_CTX, mg_core_deadline:default())),
    repaired = mg_core_machine:repair(
        Options,
        ID,
        repair_arg,
        ?REQ_CTX,
        mg_core_deadline:default()
    ),
    {logic, machine_already_working} =
        (catch mg_core_machine:repair(
            Options,
            ID,
            repair_arg,
            ?REQ_CTX,
            mg_core_deadline:default()
        )),
    2 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),

    ok = mg_core_machine:call(Options, ID, remove, ?REQ_CTX, mg_core_deadline:default()),

    {logic, machine_not_found} =
        (catch mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())),

    ok = stop_automaton(Pid).

%%
%% processor
%%

-type machine_state() :: {mg_core_storage:opaque(), integer()}.

-spec pool_child_spec(_Options, atom()) -> supervisor:child_spec().
pool_child_spec(_Options, Name) ->
    #{
        id => Name,
        start => {?MODULE, start, []}
    }.

-spec process_machine(
    _Options,
    mg_core:id(),
    mg_core_machine:processor_impact(),
    _,
    _,
    _,
    machine_state()
) -> mg_core_machine:processor_result() | no_return().
process_machine(_, _, {_, fail}, _, ?REQ_CTX, _, _) ->
    _ = exit(1),
    {noreply, sleep, []};
process_machine(_, _, {init, {TestKey, TestValue}}, _, ?REQ_CTX, _, undefined) ->
    {{reply, ok}, sleep, {TestKey, TestValue}};
process_machine(_, _, {call, get}, _, ?REQ_CTX, _, {TestKey, TestValue}) ->
    {{reply, TestValue}, sleep, {TestKey, TestValue}};
process_machine(_, _, {call, increment}, _, ?REQ_CTX, _, {TestKey, TestValue}) ->
    {{reply, ok}, sleep, {TestKey, TestValue + 1}};
process_machine(_, _, {call, delayed_increment}, _, ?REQ_CTX, _, State) ->
    {{reply, ok}, {wait, genlib_time:unow() + 1, ?REQ_CTX, 5000}, State};
process_machine(_, _, {call, remove}, _, ?REQ_CTX, _, State) ->
    {{reply, ok}, remove, State};
process_machine(_, _, timeout, _, ?REQ_CTX, _, {TestKey, TestValue}) ->
    {noreply, sleep, {TestKey, TestValue + 1}};
process_machine(_, _, {repair, repair_arg}, _, ?REQ_CTX, _, {TestKey, TestValue}) ->
    {{reply, repaired}, sleep, {TestKey, TestValue}}.

%%
%% storage
%%

-define(COLUMNS, [
    test_key,
    test_counter
]).

%%

-type query_get() :: mg_core_machine_storage_cql:query_get().
-type query_update() :: mg_core_machine_storage_cql:query_update().

-spec prepare_get_query(_, query_get()) -> query_get().
prepare_get_query(_, Query) ->
    ?COLUMNS ++ Query.

-spec prepare_update_query(_, machine_state(), machine_state() | undefined, query_update()) ->
    query_update().
prepare_update_query(_, {TestKey, TestValue}, _Was, Query) ->
    Query#{
        test_key => mg_core_machine_storage_cql:write_opaque(TestKey),
        test_counter => TestValue
    }.

-spec read_machine_state(_, mg_core_machine_storage_cql:record()) -> machine_state().
read_machine_state(_, #{test_key := Key, test_counter := Value}) ->
    {Key, Value}.

-spec bootstrap(_, mg_core:ns(), mg_core_machine_storage_cql:client()) -> ok.
bootstrap(_, NS, Client) ->
    {ok, _} = cqerl_client:run_query(
        Client,
        mg_core_string_utils:join([
            "ALTER TABLE",
            mg_core_machine_storage_cql:mk_table_name(NS),
            "ADD (test_key BLOB, test_counter INT)"
        ])
    ),
    ok.

%%
%% utils
%%
-spec start() -> ignore.
start() ->
    ignore.

-spec start_automaton(mg_core_machine:options()) -> pid().
start_automaton(Options) ->
    mg_core_utils:throw_if_error(mg_core_machine:start_link(Options)).

-spec stop_automaton(pid()) -> ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec automaton_options(config()) -> mg_core_machine:options().
automaton_options(C) ->
    Scheduler = #{},
    #{
        namespace => ?NAMESPACE,
        processor => ?MODULE,
        % storage => mg_core_storage_memory,
        storage => {mg_core_machine_storage_cql, storage_options()},
        worker => #{registry => ?config(registry, C)},
        pulse => ?MODULE,
        schedulers => #{
            timers => Scheduler,
            timers_retries => Scheduler,
            overseer => Scheduler
        }
    }.

-spec storage_options() -> mg_core_machine_storage_cql:options().
storage_options() ->
    #{
        node => {"scylla0", 9042},
        keyspace => mg,
        schema => ?MODULE
    }.

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
