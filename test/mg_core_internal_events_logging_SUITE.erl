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
-export([get_machine/4, process_machine/7]).

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
    {Pid, OptionsRef} = start_automaton(namespace_options(NS)),

    ok = mg_core_namespace:start(OptionsRef, ID, undefined, ?req_ctx, mg_core_deadline:default()),
    ok = timer:sleep(2000),
    {retrying, _, _, _, _} = mg_core_namespace:get_status(OptionsRef, ID),

    ok = stop_automaton(OptionsRef, Pid).

%%
%% processor
%%
-spec get_machine(_Options, mg_core:id(), _Args, mg_core_machine:machine_state()) ->
    mg_core_machine:processor_result() | no_return().
get_machine(_, _, _, State) ->
    State.

-spec process_machine(_Options, mg_core:id(), mg_core_machine:processor_impact(), _, _, _, mg_core_machine:machine_state()) ->
    mg_core_machine:processor_result() | no_return().
process_machine(_, _, {init, _}, _, ?req_ctx, _, null) ->
    {{reply, ok}, build_timer(), []};
process_machine(_, _, timeout, _, ?req_ctx, _, _State) ->
    erlang:throw({transient, timeout_oops}).

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
    #{
        namespace => NS,
        processor => ?MODULE,
        storage   => mg_core_ct_helper:build_storage(NS, mg_core_storage_memory),
        registry  => mg_core_procreg_gproc,
        pulse     => ?MODULE,
        machine   => #{
            retries   => #{
                timers         => {intervals, [1000, 1000, 1000, 1000, 1000]},
                processor      => {intervals, [1]}
            }
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
