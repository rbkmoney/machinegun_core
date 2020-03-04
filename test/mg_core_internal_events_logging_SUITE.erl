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

-module(mg_core_internal_events_logging_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([robust_handling/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([pool_child_spec/2, process_machine/7]).

-export([start/0]).

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
       robust_handling
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

%% Errors in event hadler don't affect machine
-spec robust_handling(config()) ->
    _.
robust_handling(_C) ->
    BinTestName = genlib:to_binary(robust_handling),
    NS = BinTestName,
    ID = BinTestName,
    Options = automaton_options(NS),
    Pid = start_automaton(Options),

    ok = mg_core_machine:start(Options, ID, undefined, ?req_ctx, mg_core_deadline:default()),
    ok = timer:sleep(2000),
    {retrying, _, _, _, _} = mg_core_machine:get_status(Options, ID),

    ok = stop_automaton(Pid).

%%
%% processor
%%
-spec pool_child_spec(_Options, atom()) ->
    supervisor:child_spec().
pool_child_spec(_Options, Name) ->
    #{
        id    => Name,
        start => {?MODULE, start, []}
    }.

-spec process_machine(_Options, machinegun_core:id(), mg_core_machine:processor_impact(), _, _, _, mg_core_machine:machine_state()) ->
    mg_core_machine:processor_result() | no_return().
process_machine(_, _, {init, _}, _, ?req_ctx, _, null) ->
    {{reply, ok}, build_timer(), []};
process_machine(_, _, timeout, _, ?req_ctx, _, _State) ->
    erlang:throw({transient, timeout_oops}).

%%
%% utils
%%
-spec start()->
    ignore.
start() ->
    ignore.

-spec start_automaton(mg_core_machine:options()) ->
    pid().
start_automaton(Options) ->
    mg_core_utils:throw_if_error(mg_core_machine:start_link(Options)).

-spec stop_automaton(pid()) ->
    ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec automaton_options(machinegun_core:ns()) ->
    mg_core_machine:options().
automaton_options(NS) ->
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_core_ct_helper:build_storage(NS, mg_core_storage_memory),
        worker    => #{registry => mg_core_procreg_gproc},
        pulse     => ?MODULE,
        retries   => #{
            timers         => {intervals, [1000, 1000, 1000, 1000, 1000]},
            processor      => {intervals, [1]}
        },
        schedulers => #{
            timers         => #{min_scan_delay => 1000},
            timers_retries => #{min_scan_delay => 1000}
        }
    }.

-spec handle_beat(_, mg_core_pulse:beat()) ->
    ok.
handle_beat(_, _Event) ->
    erlang:error(logging_oops).

-spec build_timer() ->
    mg_core_machine:processor_flow_action().
build_timer() ->
    {wait, genlib_time:unow() + 1, ?req_ctx, 5000}.
