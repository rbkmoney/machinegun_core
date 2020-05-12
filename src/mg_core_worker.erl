%%%
%%% Copyright 2020 RBKmoney
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

-module(mg_core_worker).

%% API
-export_type([start_options/0]).
-export_type([call_options/0]).
-export_type([call_payload/0]).
-export_type([call_reply/0]).
-export_type([call_context/0]).
-export_type([shutdown/0]).

-export([child_spec/2]).
-export([start_link/5]).
-export([call/5]).

%% Callbacks

-callback start_link(handler_start_options(), reg_name(), mg_core:id(), req_ctx(), deadline()) ->
    mg_core_utils:gen_start_ret().

-callback call(handler_call_options(), ref(), call_payload(), req_ctx(), deadline()) ->
    call_reply().

%% Types

-type start_options() :: #{
    namespace := mg_core:ns(),
    handler := mg_core_utils:mod_opts(handler_start_options()),
    shutdown => shutdown()
}.
-type call_options() :: #{
    handler := mg_core_utils:mod_opts(handler_call_options())
}.
-type call_payload() :: term().
-type call_reply() :: term().
-type call_context() :: term(). % в OTP он не описан, а нужно бы :(
-type shutdown() :: brutal_kill | timeout().  % like supervisor:shutdown()

%% Internal types

-type reg_name() :: mg_core_utils:gen_reg_name().
-type ref() :: mg_core_utils:gen_ref().
-type req_ctx() :: mg_core:request_context().
-type deadline() :: mg_core_deadline:deadline().
-type handler_start_options() :: term().
-type handler_call_options() :: term().

%% API

-spec child_spec(atom(), start_options()) ->
    supervisor:child_spec().
child_spec(ChildID, Options) ->
    Shutdown = maps:get(shutdown, Options, 5000),
    #{
        id       => ChildID,
        start    => {?MODULE, start_link, [Options]},
        restart  => temporary,
        shutdown => Shutdown
    }.

-spec start_link(start_options(), reg_name(), mg_core:id(), req_ctx(), deadline()) ->
    mg_core_utils:gen_start_ret().
start_link(Options, RegName, ID, ReqCtx, Deadline) ->
    {HandlerModule, HandlerOptions} = mg_core_utils:separate_mod_opts(maps:get(handler, Options)),
    HandlerModule:start_link(HandlerOptions, RegName, ID, ReqCtx, Deadline).

-spec call(call_options(), ref(), call_payload(), req_ctx(), deadline()) ->
    call_reply().
call(Options, Ref, Payload, ReqCtx, Deadline) ->
    {HandlerModule, HandlerOptions} = mg_core_utils:separate_mod_opts(maps:get(handler, Options)),
    HandlerModule:call(HandlerOptions, Ref, Payload, ReqCtx, Deadline).
