-module(mg_core_machine_tags_cql_schema).

%% Schema
-export([prepare_get_query/2]).
-export([prepare_update_query/4]).
-export([read_machine_state/2]).

%% Bootstrapping
-export([bootstrap/3]).
-export([teardown/3]).

-type options() :: undefined.
-type machine_state() :: mg_core_machine_tags:state().

-type query_get() :: mg_core_machine_storage_cql:query_get().
-type query_update() :: mg_core_machine_storage_cql:query_update().
-type record() :: mg_core_machine_storage_cql:record().

-define(COLUMN, tagged_machine_id).

%%

-spec prepare_get_query(options(), query_get()) -> query_get().
prepare_get_query(_Options, Query) ->
    [?COLUMN] ++ Query.

-spec prepare_update_query(options(), machine_state(), machine_state() | undefined, query_update()) ->
    query_update().
prepare_update_query(_Options, State, State, Query) ->
    Query;
prepare_update_query(_Options, ID, _, Query) ->
    Query#{?COLUMN => ID}.

-spec read_machine_state(options(), record()) -> machine_state().
read_machine_state(_Options, #{?COLUMN := ID}) ->
    read_maybe(ID).

-spec read_maybe(T | null) -> T | undefined.
read_maybe(V) when V /= null ->
    V;
read_maybe(null) ->
    undefined.

%%

-spec bootstrap(options(), mg_core:ns(), mg_core_storage_cql:client()) -> ok.
bootstrap(_Options, NS, Client) ->
    mg_core_storage_cql:execute_query(
        Client,
        erlang:iolist_to_binary(
            mg_core_string_utils:join([
                "ALTER TABLE",
                mg_core_machine_storage_cql:mk_table_name(NS),
                "ADD (tagged_machine_id TEXT)"
            ])
        )
    ),
    ok.

-spec teardown(options(), mg_core:ns(), mg_core_storage_cql:client()) -> ok.
teardown(_Options, _NS, _Client) ->
    ok.
