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

-module(mg_core_events_sink).

-export([add_events/6]).

-callback add_events(
    handler_options(),
    mg_core:ns(),
    mg_core:id(),
    [event()],
    req_ctx(),
    deadline()
) -> ok.

%% Types

-type handler(Options) :: mg_core_utils:mod_opts(Options).
-type handler() :: handler(handler_options()).

-export_type([handler/1]).
-export_type([handler/0]).

%% Internal types

-type event() :: mg_core_events:event().
-type req_ctx() :: mg_core:request_context().
-type deadline() :: mg_core_deadline:deadline().
-type handler_options() :: any().

%% API

-spec add_events(handler(), mg_core:ns(), mg_core:id(), [event()], req_ctx(), deadline()) -> ok.
add_events(_Handler, _NS, _ID, [], _ReqCtx, _Deadline) ->
    ok;
add_events(Handler, NS, ID, Events, ReqCtx, Deadline) ->
    ok = mg_core_utils:apply_mod_opts(Handler, add_events, [NS, ID, Events, ReqCtx, Deadline]).
