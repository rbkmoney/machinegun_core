%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_core_timer_retry_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([transient_fail/1]).
-export([permanent_fail/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([pool_child_spec/2, process_machine/7]).

%% mg_core_machine_storage_kvs
-behaviour(mg_core_machine_storage_kvs).
-export([state_to_opaque/1]).
-export([opaque_to_state/1]).

%% mg_core_machine_storage_cql schema
% TODO
% -behaviour(mg_core_machine_storage_cql).
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
-type test_name() :: atom().
-type group_name() :: atom().
-type group() :: {Name :: atom(), Opts :: list(), [test_name()]}.
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, storage_memory},
        {group, storage_cql}
    ].

-spec groups() -> [group()].
groups() ->
    [
        {storage_cql, [], [{group, all}]},
        {storage_memory, [], [{group, all}]},
        {all, [parallel, shuffle], [
            transient_fail,
            permanent_fail
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_machine, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(storage_memory, C) ->
    [{storage, memory} | C];
init_per_group(storage_cql, C) ->
    [{storage, cql} | C];
init_per_group(all, C) ->
    C.

-spec end_per_group(GroupName :: atom(), config()) -> ok.
end_per_group(_GroupName, C) ->
    C.

%%
%% tests
%%
-define(REQ_CTX, <<"req_ctx">>).

-spec transient_fail(config()) -> _.
transient_fail(C) ->
    BinTestName = genlib:to_binary(?FUNCTION_NAME),
    NS = BinTestName,
    ID = BinTestName,
    Storage = mg_core_ct_helper:bootstrap_machine_storage(?config(storage, C), NS, ?MODULE),
    Options = automaton_options(
        NS,
        Storage,
        {intervals, [1000, 1000, 1000, 1000, 1000, 1000, 1000]}
    ),
    Pid = start_automaton(Options),

    ok = mg_core_machine:start(Options, ID, normal, ?REQ_CTX, mg_core_deadline:default()),
    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(
        Options,
        ID,
        {set_mode, failing},
        ?REQ_CTX,
        mg_core_deadline:default()
    ),
    ok = timer:sleep(3000),
    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(
        Options,
        ID,
        {set_mode, counting},
        ?REQ_CTX,
        mg_core_deadline:default()
    ),
    ok = timer:sleep(3000),
    I = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    true = I > 0,

    ok = stop_automaton(Pid).

-spec permanent_fail(config()) -> _.
permanent_fail(C) ->
    BinTestName = genlib:to_binary(?FUNCTION_NAME),
    NS = BinTestName,
    ID = BinTestName,
    Storage = mg_core_ct_helper:bootstrap_machine_storage(?config(storage, C), NS, ?MODULE),
    Options = automaton_options(NS, Storage, {intervals, [1000]}),
    Pid = start_automaton(Options),

    ok = mg_core_machine:start(Options, ID, normal, ?REQ_CTX, mg_core_deadline:default()),
    0 = mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default()),
    ok = mg_core_machine:call(
        Options,
        ID,
        {set_mode, failing},
        ?REQ_CTX,
        mg_core_deadline:default()
    ),
    ok = timer:sleep(4000),
    {logic, machine_failed} =
        (catch mg_core_machine:call(Options, ID, get, ?REQ_CTX, mg_core_deadline:default())),

    ok = stop_automaton(Pid).

%%
%% processor
%%
-type mode() :: normal | counting | failing.
-type state() :: {mode(), _Counter :: non_neg_integer()}.

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
    state()
) -> mg_core_machine:processor_result() | no_return().
process_machine(_, _, {init, Mode}, _, ?REQ_CTX, _, undefined) ->
    {{reply, ok}, build_timer(), {Mode, 0}};
process_machine(_, _, {call, get}, _, ?REQ_CTX, _, {_Mode, Counter} = State) ->
    {{reply, Counter}, build_timer(), State};
process_machine(_, _, {call, {set_mode, NewMode}}, _, ?REQ_CTX, _, {_Mode, Counter}) ->
    {{reply, ok}, build_timer(), {NewMode, Counter}};
process_machine(_, _, timeout, _, ?REQ_CTX, _, {normal, _Counter} = State) ->
    {{reply, ok}, build_timer(), State};
process_machine(_, _, timeout, _, ?REQ_CTX, _, {counting = Mode, Counter}) ->
    {{reply, ok}, build_timer(), {Mode, Counter + 1}};
process_machine(_, _, timeout, _, ?REQ_CTX, _, {failing, _Counter}) ->
    erlang:throw({transient, oops}).
%%
%% mg_core_machine_storage_kvs
%%
-spec state_to_opaque(state()) -> mg_core_storage:opaque().
state_to_opaque({Mode, Counter}) ->
    [mode_to_binary(Mode), Counter].

-spec mode_to_binary(mode()) -> binary().
mode_to_binary(normal) -> <<"normal">>;
mode_to_binary(counting) -> <<"counting">>;
mode_to_binary(failing) -> <<"failing">>.

-spec opaque_to_state(mg_core_storage:opaque()) -> state().
opaque_to_state([Mode, Counter]) ->
    {binary_to_mode(Mode), Counter}.

-spec binary_to_mode(binary()) -> mode().
binary_to_mode(<<"normal">>) -> normal;
binary_to_mode(<<"counting">>) -> counting;
binary_to_mode(<<"failing">>) -> failing.

%%
%% mg_core_machine_storage_cql
%%
-type query_get() :: mg_core_machine_storage_cql:query_get().
-type query_update() :: mg_core_machine_storage_cql:query_update().

-spec prepare_get_query(_, query_get()) -> query_get().
prepare_get_query(_, Query) ->
    [test_mode, test_counter] ++ Query.

-spec prepare_update_query(_, state(), state() | undefined, query_update()) ->
    query_update().
prepare_update_query(_, {Mode, Counter}, _Prev, Query) ->
    Query#{
        test_mode => mode_to_binary(Mode),
        test_counter => Counter
    }.

-spec read_machine_state(_, mg_core_machine_storage_cql:record()) -> state().
read_machine_state(_, #{test_mode := Mode, test_counter := Counter}) ->
    {binary_to_mode(Mode), Counter}.

-spec bootstrap(_, mg_core:ns(), mg_core_machine_storage_cql:client()) -> ok.
bootstrap(_, NS, Client) ->
    {ok, _} = cqerl_client:run_query(
        Client,
        mg_core_string_utils:join([
            "ALTER TABLE",
            mg_core_machine_storage_cql:mk_table_name(NS),
            "ADD (test_mode TEXT, test_counter INT)"
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

-spec automaton_options(mg_core:ns(), mg_core_machine_storage:options(), mg_core_retry:policy()) ->
    mg_core_machine:options().
automaton_options(NS, Storage, RetryPolicy) ->
    Scheduler = #{
        min_scan_delay => 1000,
        target_cutoff => 15
    },
    #{
        namespace => NS,
        processor => ?MODULE,
        storage => Storage,
        worker => #{
            registry => mg_core_procreg_gproc
        },
        pulse => ?MODULE,
        retries => #{
            timers => RetryPolicy
        },
        schedulers => #{
            timers => Scheduler,
            timers_retries => Scheduler,
            overseer => Scheduler
        }
    }.

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

-spec build_timer() -> mg_core_machine:processor_flow_action().
build_timer() ->
    {wait, genlib_time:unow() + 1, ?REQ_CTX, 5000}.
