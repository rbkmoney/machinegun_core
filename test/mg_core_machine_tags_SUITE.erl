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
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

%% tests
-export([tag/1]).
-export([idempotent_tag/1]).
-export([double_tag/1]).
-export([replace/1]).
-export([invalid_replace/1]).
-export([resolve/1]).

%% Pulse
-export([handle_beat/2]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
        {group, main}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {main, [sequence], [
            tag,
            idempotent_tag,
            double_tag,
            replace,
            invalid_replace,
            resolve
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_storage, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    {Pid, OptionsRef} = start_automaton(),
    true = erlang:unlink(Pid),
    [{apps, Apps}, {pid, Pid}, {options_ref, OptionsRef} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    ok = proc_lib:stop(?config(pid, C)),
    _ = persistent_term:erase(?config(options_ref, C)),
    mg_core_ct_helper:stop_applications(?config(apps, C)).


%%
%% tests
%%
-define(ID       , <<"tagged_id">>).
-define(OTHER_ID , <<"other_id" >>).
-define(TAG      , <<"tag"      >>).

-spec tag(config()) ->
    _.
tag(_C) ->
    ?assertEqual(ok, mg_core_machine_tags:add(tag_options(), ?TAG, ?ID, null, mg_core_deadline:default())).

-spec idempotent_tag(config()) ->
    _.
idempotent_tag(C) ->
    tag(C).

-spec double_tag(config()) ->
    _.
double_tag(_C) ->
    ?assertEqual(
        {already_exists, ?ID},
        mg_core_machine_tags:add(tag_options(), ?TAG, ?OTHER_ID, null, mg_core_deadline:default())
    ).

-spec replace(config()) ->
    _.
replace(_C) ->
    ?assertEqual(
        ok,
        mg_core_machine_tags:replace(tag_options(), ?TAG, ?ID, ?OTHER_ID, null, mg_core_deadline:default())
    ).

-spec invalid_replace(config()) ->
    _.
invalid_replace(_C) ->
    ?assertEqual(
        {invalid_old_id, ?OTHER_ID},
        mg_core_machine_tags:replace(
            tag_options(), ?TAG, ?ID, ?OTHER_ID, null, mg_core_deadline:default()
        )
    ).

-spec resolve(config()) ->
    _.
resolve(_C) ->
    ?assertEqual(?OTHER_ID, mg_core_machine_tags:resolve(tag_options(), ?TAG)).

%%
%% utils
%%
-spec start_automaton() ->
    {pid(), mg_core_namespace:options_ref()}.
start_automaton() ->
    OptionsRef = save_tag_namespace_options(),
    Pid = mg_core_utils:throw_if_error(
        mg_core_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_core_machine_tags:child_spec(tag_options(), tags)]
        )
    ),
    {Pid, OptionsRef}.

-spec tag_options() ->
    mg_core_machine_tags:options().
tag_options() ->
    #{
        namespace_options_ref => mg_core_namespace:make_options_ref(<<"test_tags">>)
    }.

-spec save_tag_namespace_options() ->
    mg_core_namespace:options_ref().
save_tag_namespace_options() ->
    NS = <<"test_tags">>,
    Options = mg_core_machine_tags:make_namespace_options(#{
        namespace => <<"test_tags">>,
        storage   => mg_core_storage_memory,
        registry  => mg_core_procreg_gproc,
        pulse     => ?MODULE
    }),
    OptionsRef = mg_core_namespace:make_options_ref(NS),
    ok = mg_core_namespace:save_options(Options, OptionsRef),
    OptionsRef.

-spec handle_beat(_, mg_core_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
