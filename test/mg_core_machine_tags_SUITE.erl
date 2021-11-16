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

-module(mg_core_machine_tags_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

%% tests
-export([tag/1]).
-export([idempotent_tag/1]).
-export([double_tag/1]).
-export([replace/1]).
-export([resolve/1]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [{group, group_name()}].
all() ->
    [
        {group, storage_memory},
        {group, storage_cql}
    ].

-spec groups() -> [{group_name(), list(_), [test_name() | {group, group_name()}]}].
groups() ->
    [
        {storage_memory, [], [{group, main}]},
        {storage_cql, [], [{group, main}]},
        {main, [sequence], [
            tag,
            idempotent_tag,
            double_tag,
            replace,
            resolve
        ]}
    ].

%%
%% starting/stopping
%%
-define(NS, <<"test_tags">>).

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_storage, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(storage_memory, C) ->
    MachineStorage = mg_core_ct_helper:bootstrap_machine_storage(memory, ?NS, mg_core_machine_tags),
    [{storage, MachineStorage} | C];
init_per_group(storage_cql, C) ->
    MachineStorage = mg_core_ct_helper:bootstrap_machine_storage(cql, ?NS, mg_core_machine_tags),
    [{storage, MachineStorage} | C];
init_per_group(main, C) ->
    Options = automaton_options(?config(storage, C)),
    Pid = start_automaton(Options),
    true = erlang:unlink(Pid),
    [{options, Options}, {automaton, Pid} | C].

-spec end_per_group(group_name(), config()) -> _.
end_per_group(main, C) ->
    Pid = ?config(automaton, C),
    ok = proc_lib:stop(Pid, shutdown, 5000);
end_per_group(_Name, _C) ->
    ok.

%%
%% tests
%%
-define(ID, <<"tagged_id">>).
-define(OTHER_ID, <<"other_id">>).
-define(TAG, <<"tag">>).

-spec tag(config()) -> _.
tag(C) ->
    ok = mg_core_machine_tags:add(?config(options, C), ?TAG, ?ID, null, mg_core_deadline:default()).

-spec idempotent_tag(config()) -> _.
idempotent_tag(C) ->
    tag(C).

-spec double_tag(config()) -> _.
double_tag(C) ->
    {already_exists, ?ID} =
        mg_core_machine_tags:add(
            ?config(options, C),
            ?TAG,
            ?OTHER_ID,
            null,
            mg_core_deadline:default()
        ).

-spec replace(config()) -> _.
replace(C) ->
    ok = mg_core_machine_tags:replace(
        ?config(options, C),
        ?TAG,
        ?ID,
        null,
        mg_core_deadline:default()
    ).

-spec resolve(config()) -> _.
resolve(C) ->
    ?ID = mg_core_machine_tags:resolve(?config(options, C), ?TAG).

%%
%% utils
%%
-spec start_automaton(mg_core_machine_tags:options()) -> pid().
start_automaton(Options) ->
    mg_core_utils:throw_if_error(
        mg_core_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_core_machine_tags:child_spec(Options, tags)]
        )
    ).

-spec automaton_options(mg_core_machine_storage:options()) -> mg_core_machine_tags:options().
automaton_options(Storage) ->
    #{
        namespace => ?NS,
        storage => Storage,
        worker => #{registry => mg_core_procreg_gproc},
        pulse => ?MODULE,
        retries => #{}
    }.

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
