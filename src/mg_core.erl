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
-module(mg_core).

%% API
-export_type([ns/0]).
-export_type([id/0]).
-export_type([request_context/0]).
-export_type([unix_timestamp_s/0]).
-export_type([unix_timestamp_ns/0]).
-export_type([timeout_ms/0]).

-type ns() :: binary().
-type id() :: binary().
-type request_context() :: mg_core_storage:opaque().
-type unix_timestamp_s() :: non_neg_integer().
-type unix_timestamp_ns() :: non_neg_integer().
-type timeout_ms() :: pos_integer().
