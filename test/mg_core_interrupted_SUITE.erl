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

-module(mg_core_interrupted_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([interrupted_machines_resumed/1]).

%% mg_core_machine
-behaviour(mg_core_machine).
-export([process_machine/7]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name()] | {group, atom()}.
all() ->
    [
        interrupted_machines_resumed
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

%%
%% tests
%%
-define(INIT_ARGS, <<"normies">>).
-define(REQ_CTX, <<"req_ctx">>).

-spec interrupted_machines_resumed(config()) -> _.
interrupted_machines_resumed(_C) ->
    NS = genlib:to_binary(?FUNCTION_NAME),
    {ok, StoragePid} = mg_core_storage_memory:start(#{name => ?MODULE}),
    Options = automaton_options(NS, ?MODULE),

    N = 8,
    Runtime = 1000,
    Answer = 42,

    Pid1 = start_automaton(Options),
    IDs = [genlib:to_binary(I) || I <- lists:seq(1, N)],
    _ = [
        begin
            ok = mg_core_machine:start(
                Options,
                ID,
                ?INIT_ARGS,
                ?REQ_CTX,
                mg_core_deadline:default()
            ),
            ?assertEqual(
                undefined,
                mg_core_machine:call(Options, ID, answer, ?REQ_CTX, mg_core_deadline:default())
            ),
            ?assertEqual(
                ok,
                mg_core_machine:call(
                    Options,
                    ID,
                    {run, Runtime, Answer},
                    ?REQ_CTX,
                    mg_core_deadline:default()
                )
            )
        end
     || ID <- IDs
    ],
    ok = stop_automaton(Pid1),

    Pid2 = start_automaton(Options),
    ok = timer:sleep(Runtime * 2),
    _ = [
        ?assertEqual(
            Answer,
            mg_core_machine:call(Options, ID, answer, ?REQ_CTX, mg_core_deadline:default())
        )
     || ID <- IDs
    ],
    ok = stop_automaton(Pid2),

    ok = proc_lib:stop(StoragePid).

%%
%% processor
%%
-spec process_machine(
    _Options,
    mg_core:id(),
    mg_core_machine:processor_impact(),
    _,
    _,
    _,
    mg_core_machine:machine_state()
) -> mg_core_machine:processor_result() | no_return().
process_machine(_, _, {init, ?INIT_ARGS}, _, ?REQ_CTX, _, null) ->
    {{reply, ok}, sleep, #{}};
process_machine(_, _, {call, {run, Runtime, Answer}}, _, ?REQ_CTX, _, State) ->
    {{reply, ok}, {continue, #{}}, State#{<<"run">> => [Runtime, Answer]}};
process_machine(_, _, continuation, _, ?REQ_CTX, _, #{<<"run">> := [Runtime, Answer]}) ->
    ok = timer:sleep(Runtime),
    {noreply, sleep, #{<<"answer">> => Answer}};
process_machine(_, _, {call, answer}, _, ?REQ_CTX, _, State) ->
    {{reply, maps:get(<<"answer">>, State, undefined)}, sleep, State}.

%%
%% utils
%%
-spec start_automaton(mg_core_machine:options()) -> pid().
start_automaton(Options) ->
    mg_core_utils:throw_if_error(mg_core_machine:start_link(Options)).

-spec stop_automaton(pid()) -> ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid).

-spec automaton_options(mg_core:ns(), mg_core_storage:name()) -> mg_core_machine:options().
automaton_options(NS, StorageName) ->
    Scheduler = #{
        min_scan_delay => 1000,
        target_cutoff => 15
    },
    #{
        namespace => NS,
        processor => ?MODULE,
        storage => mg_core_ct_helper:build_storage(
            NS,
            {mg_core_storage_memory, #{
                existing_storage_name => StorageName
            }}
        ),
        worker => #{
            registry => mg_core_procreg_gproc
        },
        pulse => ?MODULE,
        schedulers => #{
            overseer => Scheduler
        }
    }.

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, {squad, _}) ->
    ok;
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
