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

-module(mg_core_procreg).

% Any term sans ephemeral ones, like `reference()`s / `pid()`s / `fun()`s.
-type name() :: term().
-type name_pattern() :: ets:match_pattern().

-type ref() :: mg_core_utils:gen_ref().
-type reg_name() :: mg_core_utils:gen_reg_name().

-type procreg_options() :: term().
-type options() :: mg_core_utils:mod_opts(procreg_options()).

-type mfargs() :: {module(), Function :: atom(), Args :: [term()]}.

-export_type([name/0]).
-export_type([name_pattern/0]).
-export_type([ref/0]).
-export_type([reg_name/0]).
-export_type([options/0]).
-export_type([mfargs/0]).

-export_type([start_link_ret/0]).

-export([ref/2]).
-export([reg_name/2]).
-export([select/2]).

-export([start_link/3]).
-export([wrap_child_spec/2]).
-export([call/2]).

%% Names and references

-callback ref(procreg_options(), name()) ->
    ref().

-callback reg_name(procreg_options(), name()) ->
    reg_name().

-callback select(procreg_options(), name_pattern()) ->
    [{name(), pid()}].

-callback call(procreg_options(), mfargs()) ->
    _Reply.

-type start_link_ret() ::
    {ok, pid()} | {error, term()}.

-callback start_link(procreg_options(), mfargs()) ->
    start_link_ret().

%% API

-spec ref(options(), name()) ->
    ref().
ref(Options, Name) ->
    mg_core_utils:apply_mod_opts(Options, ref, [Name]).

-spec reg_name(options(), name()) ->
    reg_name().
reg_name(Options, Name) ->
    mg_core_utils:apply_mod_opts(Options, reg_name, [Name]).

-spec select(options(), name_pattern()) ->
    [{name(), pid()}].
select(Options, NamePattern) ->
    mg_core_utils:apply_mod_opts(Options, select, [NamePattern]).

-spec call(options(), mfargs()) ->
    _Reply.
call(Options, CallMFA) ->
    mg_core_utils:apply_mod_opts(Options, call, [CallMFA]).

-spec start_link(options(), mfargs(), list()) ->
    start_link_ret().
start_link(Options, StartMFA, Args) ->
    mg_core_utils:apply_mod_opts(Options, start_link, [add_args(StartMFA, Args)]).

-spec wrap_child_spec(options(), supervisor:child_spec()) ->
    supervisor:child_spec().
wrap_child_spec(Options, Spec) ->
    #{start := StartMFAargs} = Spec,
    Spec#{start => {?MODULE, start_link, [Options, StartMFAargs]}}.

%% Internals

-spec add_args(mfargs(), Agrs :: list()) ->
    mfargs().
add_args({M, F, A}, Args) ->
    {M, F, A ++ Args}.
