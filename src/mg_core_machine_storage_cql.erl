%% Notes
%%
%% This storage interface implementation manages machine state in Cassandra /
%% ScyllaDB cluster. We use `cqerl` client to speak to DB with CQLv4.
%%
%% Machines are stored on a table-per-namespace basis, no more, no less.
%% This module oversees that tables and indexes are there and up-to-date.
%% Yet the job of creating and configuring a keyspace is **out of scope**.
%%
%% TODOs
%%
%%  * We need better client.
%%    Currently employed `cqerl` lacks a way to start independent pools, one per
%%    namespace. Also we can't control pool size through code, only through
%%    envvar. Also we can't specify timeout per request. Also the way it works
%%    with maps isn't great, looks like we condemn ourselves to a performance
%%    penalty.
%%
%%  * Schema migrations. 😓
%%    Also note that records are not versioned (yet). Which means that each
%%    record has version 1, implicitly. Given the nature of schema this shoudn't
%%    incur ambiguity in the future when we'll need to introduce the notion of
%%    record version, this (hopefully) would amount to just adding a column.
%%
%%  * Allow to specify LWW timestamp w/ writes?
%%    If everything works correctly this shouldn't be needed: mg enforces
%%    one-process-per-machine guarantee which means concurrent writes aren't
%%    possible. However, can we somehow guard ourselves against some runaway
%%    (or those affected by suspend) processes, at least. Though to battle them
%%    we probably want some form of fencing usually available with consensus
%%    primitives (e.g. use session renewal timestamp or lock sequence number
%%    as LWW timestamp).
%%
%%  * External blob storage concept.
%%    We'd need to lift single table restriction here for this.

-module(mg_core_machine_storage_cql).

-include_lib("cqerl/include/cqerl.hrl").

-behaviour(mg_core_machine_storage).
-export([get/3]).
-export([update/6]).
-export([remove/4]).
-export([search/5]).

%% Bootstrapping
-export([bootstrap/2]).
-export([teardown/2]).

%% Utilities
-export([mk_table_name/1]).

-export_type([options/0]).
-export_type([context/0]).

-export_type([column/0]).
-export_type([record/0]).
-export_type([query_get/0]).
-export_type([query_update/0]).
-export_type([client/0]).

-type options() :: #{
    % Base
    name := mg_core_machine_storage:name(),
    processor := module(),
    pulse := mg_core_pulse:handler(),
    % Network
    % TODO
    % Bootstrap multiple cluster endpoints from DNS somehow.
    node := {inet:ip_address() | string(), Port :: integer()},
    ssl => [ssl:tls_client_option()],
    % Data
    keyspace := atom(),
    schema => mg_core_utils:mod_opts(),
    consistency => #{
        read => consistency_level(),
        write => consistency_level()
    }
}.

-type ns() :: mg_core:ns().
-type id() :: mg_core:id().
-type machine() :: mg_core_machine_storage:machine().
-type machine_status() :: mg_core_machine_storage:machine_status().
-type machine_status_class() :: mg_core_machine_storage:machine_status_class().
-type machine_state() :: mg_core_machine_storage:machine_state().

-type search_status_query() :: mg_core_machine_storage:search_status_query().
-type search_target_query() :: mg_core_machine_storage:search_target_query().
-type search_status_result() :: mg_core_machine_storage:search_status_result().
-type search_target_result() :: mg_core_machine_storage:search_target_result().
-type search_limit() :: mg_core_machine_storage:search_limit().
-type continuation() :: #cql_result{}.

%% TODO LWW timestamp?
-type context() :: [].

-type column() :: atom().
-type record() :: #{column() => mg_core_storage_cql:cql_datum()}.
-type query_get() :: [column()].
-type query_update() :: record().
-type client() :: {pid(), reference()}.

-define(COLUMNS, [
    id,
    status,
    target,
    retry,
    reqctx,
    handling_timeout,
    error_reason,
    original_status,
    original_target
]).

-define(STATUS_SLEEPING, 1).
-define(STATUS_WAITING, 2).
-define(STATUS_PROCESSING, 3).
-define(STATUS_FAILED, 4).
-define(STATUS_RETRYING, 5).

%%

-spec get(options(), ns(), id()) -> undefined | {context(), machine()}.
get(Options, NS, ID) ->
    Query = mk_get_query(Options, NS, ID),
    mg_core_storage_cql:execute_query(Options, Query, fun(Result) ->
        case cqerl:head(Result) of
            Record when is_list(Record) ->
                {[], read_machine(Options, maps:from_list(Record))};
            empty_dataset ->
                undefined
        end
    end).

-spec mk_get_query(options(), ns(), id()) -> #cql_query{}.
mk_get_query(Options, NS, ID) ->
    % TODO
    % This is a bit wasteful, maybe we can somehow prepare statements beforehand
    % or memoize them.
    Query = mg_core_utils:apply_mod_opts(get_schema(Options), prepare_get_query, [?COLUMNS]),
    mg_core_storage_cql:mk_query(
        Options,
        select,
        mk_statement([
            "SELECT",
            mg_core_string_utils:join_with(Query, ","),
            "FROM",
            mk_table_name(NS),
            "WHERE id = ?"
        ]),
        [{id, ID}]
    ).

-spec update(options(), ns(), id(), machine(), machine() | undefined, context()) -> context().
update(Options, NS, ID, Machine, MachineWas, _Context) ->
    Query = mk_update_query(Options, NS, ID, Machine, genlib:define(MachineWas, #{})),
    mg_core_storage_cql:execute_query(Options, Query, fun(void) -> [] end).

-spec mk_update_query(options(), ns(), id(), machine(), machine()) -> #cql_query{}.
mk_update_query(Options, NS, ID, Machine, MachineWas) ->
    % TODO
    % This is harder to memoize: set of affected columns is non-constant. This
    % should be taken into account.
    Query = write_machine(Options, Machine, MachineWas, #{}),
    mg_core_storage_cql:mk_query(
        Options,
        update,
        mk_statement([
            "UPDATE",
            mk_table_name(NS),
            "SET",
            mg_core_string_utils:join_with(
                [mg_core_string_utils:join(Column, "= ?") || Column <- maps:keys(Query)],
                ","
            ),
            "WHERE id = ?"
        ]),
        Query#{id => ID}
    ).

-spec remove(options(), ns(), id(), context()) -> ok.
remove(Options, NS, ID, _Context) ->
    Query = mk_remove_query(Options, NS, ID),
    mg_core_storage_cql:execute_query(Options, Query, fun(void) -> ok end).

-spec mk_remove_query(options(), ns(), id()) -> #cql_query{}.
mk_remove_query(Options, NS, ID) ->
    mg_core_storage_cql:mk_query(
        Options,
        delete,
        mk_statement(["DELETE FROM", mk_table_name(NS), "WHERE id = ?"]),
        [{id, ID}]
    ).

-spec search
    (options(), ns(), search_status_query(), search_limit(), continuation() | undefined) ->
        mg_core_machine_storage:search_page(search_status_result());
    (options(), ns(), search_target_query(), search_limit(), continuation() | undefined) ->
        mg_core_machine_storage:search_page(search_target_result()).
search(Options, NS, Search, Limit, undefined) ->
    Query = mk_search_query(Options, NS, Search, Limit),
    mg_core_storage_cql:execute_query(Options, Query, fun mk_search_page/1);
search(_Options, _NS, _Search, _Limit, Continuation) ->
    mg_core_storage_cql:execute_continuation(Continuation, fun mk_search_page/1).

mk_search_query(Options, NS, Status, Limit) when is_atom(Status) ->
    Query = mg_core_storage_cql:mk_query(
        Options,
        select,
        mk_statement([
            "SELECT id FROM",
            mk_table_name(NS),
            "WHERE status = :status"
        ]),
        [{status, write_status_class(Status)}]
    ),
    Query#cql_query{page_size = Limit};
mk_search_query(Options, NS, {Status, From, To}, Limit) ->
    Query = mg_core_storage_cql:mk_query(
        Options,
        select,
        mk_statement([
            "SELECT target, id FROM",
            mk_matview_name(NS, target),
            "WHERE target > :target_from AND target <= :target_to AND status = :status",
            "ALLOW FILTERING"
        ]),
        [
            {target_from, mg_core_storage_cql:write_timestamp_s(From)},
            {target_to, mg_core_storage_cql:write_timestamp_s(To)},
            {status, write_status_class(Status)}
        ]
    ),
    Query#cql_query{page_size = Limit}.

mk_search_page(Result) ->
    Page = accumulate_page(Result, []),
    case cqerl:has_more_pages(Result) of
        true -> {Page, Result};
        false -> {Page, undefined}
    end.

-spec accumulate_page
    (#cql_result{}, [search_target_result()]) -> [search_target_result()];
    (#cql_result{}, [search_status_result()]) -> [search_status_result()].
accumulate_page(Result, Acc) ->
    case cqerl:head(Result) of
        [{target, TS}, {id, ID}] ->
            SR = {mg_core_storage_cql:read_timestamp_s(TS), ID},
            accumulate_page(cqerl:tail(Result), [SR | Acc]);
        [{id, ID}] ->
            accumulate_page(cqerl:tail(Result), [ID | Acc]);
        empty_dataset ->
            Acc
    end.

-spec get_schema(options()) -> mg_core_utils:mod_opts().
get_schema(#{schema := Schema}) ->
    Schema;
get_schema(#{processor := mg_core_events_machine}) ->
    mg_core_events_machine_cql_schema.

%%

-spec read_machine(options(), record()) -> mg_core_machine_storage:machine().
read_machine(Options, Record) ->
    #{
        status => read_status(Record),
        state => read_machine_state(Options, Record)
    }.

-spec read_machine_state(options(), record()) -> mg_core_machine_storage:machine_state().
read_machine_state(Options, Record) ->
    mg_core_utils:apply_mod_opts(get_schema(Options), read_machine_state, [Record]).

-spec read_status(record()) -> mg_core_machine_storage:machine_status().
read_status(#{status := ?STATUS_SLEEPING}) ->
    sleeping;
read_status(#{status := ?STATUS_WAITING, target := TS, reqctx := ReqCtx, handling_timeout := TO}) ->
    {
        waiting,
        mg_core_storage_cql:read_timestamp_s(TS),
        mg_core_storage_cql:read_opaque(ReqCtx),
        TO
    };
read_status(#{status := ?STATUS_PROCESSING, reqctx := ReqCtx}) ->
    {processing, mg_core_storage_cql:read_opaque(ReqCtx)};
read_status(#{status := ?STATUS_FAILED, error_reason := Reason, original_status := OS} = Record) ->
    {error, read_term(Reason), read_status(Record#{status := OS})};
read_status(#{status := ?STATUS_RETRYING, target := TS, reqctx := ReqCtx, retry := [Start, Attempt]}) ->
    {
        retrying,
        mg_core_storage_cql:read_timestamp_s(TS),
        mg_core_storage_cql:read_timestamp_s(Start),
        Attempt,
        mg_core_storage_cql:read_opaque(ReqCtx)
    }.

%%

-spec write_machine(options(), machine(), machine(), query_update()) -> query_update().
write_machine(Options, #{status := Status, state := State}, MachineWas, Query) ->
    % TODO
    % We need few proptests here to verify that what we write and what we read
    % back are same thing. Attempt to optimize out unwanted column updates made
    % this logic a bit convoluted.
    StateWas = maps:get(state, MachineWas, undefined),
    StatusWas = maps:get(status, MachineWas, undefined),
    Query1 = clean_machine_status(StatusWas, Query),
    Query2 = write_machine_status(Status, StatusWas, Query1),
    write_machine_state(Options, State, StateWas, Query2).

-spec clean_machine_status(_StatusWas :: machine_status(), query_update()) -> query_update().
clean_machine_status({error, _, _}, Query) ->
    Query#{
        error_reason => null,
        original_status => null,
        original_target => null
    };
clean_machine_status(_, Query) ->
    Query.

-spec write_machine_state(options(), machine_state(), machine_state() | undefined, query_update()) ->
    query_update().
write_machine_state(_Options, State, State, Query) ->
    Query;
write_machine_state(Options, State, StateWas, Query) ->
    mg_core_utils:apply_mod_opts(get_schema(Options), prepare_update_query, [State, StateWas, Query]).

-spec write_machine_status(machine_status(), _Was :: machine_status(), query_update()) ->
    query_update().
write_machine_status(Status, Status, Query) ->
    Query;
write_machine_status(Status = sleeping, StatusWas, Query) ->
    Query1 = write_changes([class], Status, StatusWas, Query),
    Query1#{
        target => null
    };
write_machine_status(Status = {waiting, _, _, TO}, StatusWas, Query) ->
    Query1 = write_changes([class, target, reqctx], Status, StatusWas, Query),
    Query1#{
        handling_timeout => TO
    };
write_machine_status(Status = {processing, _}, StatusWas, Query) ->
    Query1 = write_changes([class, reqctx], Status, StatusWas, Query),
    Query1#{
        target => null
    };
write_machine_status(Status = {error, _, OS}, StatusWas, Query) ->
    Query1 = write_changes([class, error_reason], Status, StatusWas, Query),
    Query2 = write_defined(original_target, get_status_detail(target, OS), Query1),
    Query2#{
        target => null,
        original_status => write_status_class(get_status_detail(class, OS))
    };
write_machine_status(Status = {retrying, _, Start, Attempt, _}, StatusWas, Query) ->
    Query1 = write_changes([class, target, reqctx], Status, StatusWas, Query),
    Query1#{
        retry => [mg_core_storage_cql:write_timestamp_s(Start), Attempt]
    }.

-spec write_defined(atom(), _Value | undefined, query_update()) -> query_update().
write_defined(_Detail, undefined, Query) ->
    Query;
write_defined(Detail, V, Query) ->
    write_status_detail(Detail, V, Query).

-spec write_changes([atom()], machine_status(), machine_status(), query_update()) -> query_update().
write_changes(Details, Status, StatusWas, Query) ->
    lists:foldl(
        fun(Detail, QAcc) ->
            write_changed(
                Detail,
                get_status_detail(Detail, Status),
                get_status_detail(Detail, StatusWas),
                QAcc
            )
        end,
        Query,
        Details
    ).

-spec write_changed(atom(), T, T, query_update()) -> query_update().
write_changed(_Detail, V, V, Query) ->
    Query;
write_changed(Detail, V, _, Query) ->
    write_status_detail(Detail, V, Query).

-spec write_status_detail(atom(), _Value, query_update()) -> query_update().
write_status_detail(class, C, Query) ->
    Query#{status => write_status_class(C)};
write_status_detail(target, TS, Query) ->
    Query#{target => mg_core_storage_cql:write_timestamp_s(TS)};
write_status_detail(reqctx, ReqCtx, Query) ->
    Query#{reqctx => mg_core_storage_cql:write_opaque(ReqCtx)};
write_status_detail(error_reason, Reason, Query) ->
    Query#{error_reason => write_term(Reason)};
write_status_detail(original_target, Reason, Query) ->
    Query#{original_target => mg_core_storage_cql:write_timestamp_s(Reason)}.

-spec write_status_class(machine_status_class()) -> mg_core_storage_cql:cql_smallint().
write_status_class(sleeping) -> ?STATUS_SLEEPING;
write_status_class(waiting) -> ?STATUS_WAITING;
write_status_class(processing) -> ?STATUS_PROCESSING;
write_status_class(error) -> ?STATUS_FAILED;
write_status_class(retrying) -> ?STATUS_RETRYING.

get_status_detail(class, sleeping) -> sleeping;
get_status_detail(class, {waiting, _, _, _}) -> waiting;
get_status_detail(class, {retrying, _, _, _, _}) -> retrying;
get_status_detail(class, {processing, _}) -> processing;
get_status_detail(class, {error, _, _}) -> error;
get_status_detail(reqctx, {waiting, _, ReqCtx, _}) -> ReqCtx;
get_status_detail(reqctx, {retrying, _, _, _, ReqCtx}) -> ReqCtx;
get_status_detail(reqctx, {processing, ReqCtx}) -> ReqCtx;
get_status_detail(target, {waiting, TS, _, _}) -> TS;
get_status_detail(target, {retrying, TS, _, _, _}) -> TS;
get_status_detail(error_reason, {error, Reason, _}) -> Reason;
get_status_detail(_, _) -> undefined.

%%

-spec read_term(mg_core_storage_cql:cql_blob()) -> term().
read_term(Bytes) ->
    erlang:binary_to_term(Bytes).

-spec write_term(term()) -> mg_core_storage_cql:cql_blob().
write_term(Term) ->
    erlang:term_to_binary(Term).

%%

-spec bootstrap(options(), ns()) -> ok.
bootstrap(Options, NS) ->
    % TODO
    % Need to think what to do if this fails midway.
    Client = mg_core_storage_cql:get_client(Options),
    TableName = mk_table_name(NS),
    ok = mg_core_storage_cql:execute_query(
        Client,
        mk_statement([
            mg_core_string_utils:join(["CREATE TABLE", TableName, "("]),
            % NOTE
            % Keep in sync with `?COLUMNS`.
            "id TEXT,"
            "status SMALLINT,"
            "target TIMESTAMP,"
            "retry TUPLE<TIMESTAMP, SMALLINT>,"
            "reqctx BLOB,"
            "handling_timeout INT,"
            "error_reason BLOB,"
            "original_status SMALLINT,"
            "original_target TIMESTAMP,"
            "PRIMARY KEY (id)"
            ")"
        ])
    ),
    ok = mg_core_storage_cql:execute_query(
        Client,
        % TODO
        % Low cardinality index. This will break storage eventually when total
        % number of machines grows past some watermark.
        mk_statement([
            "CREATE INDEX",
            mg_core_string_utils:join_(TableName, "status"),
            "ON",
            TableName,
            "(status)"
        ])
    ),
    ok = mg_core_storage_cql:execute_query(
        Client,
        % TODO
        % High churn index. This strongly advised against. We can probably
        % alleviate this churn by using coarse timestamp instead of target, for
        % example, by breaking timeline into 100-seconds-long buckets.
        % TODO
        % Also, intuitively this gives no significant benefits when compared
        % with plain index. Maybe our best bet is two indexes: one on waiting
        % target and one on retrying target, however this would burden write path.
        mk_statement([
            "CREATE MATERIALIZED VIEW",
            mk_matview_name(NS, target),
            "AS SELECT id, status, target FROM",
            TableName,
            "WHERE target IS NOT NULL",
            "PRIMARY KEY (target, id)"
        ])
    ),
    mg_core_utils:apply_mod_opts(get_schema(Options), bootstrap, [NS, Client]).

-spec teardown(options(), ns()) -> ok.
teardown(Options, NS) ->
    Client = mg_core_storage_cql:get_client(Options),
    ok = mg_core_utils:apply_mod_opts(get_schema(Options), teardown, [NS, Client]),
    ok = mg_core_storage_cql:execute_query(
        Client,
        mk_statement([
            "DROP MATERIALIZED VIEW IF EXISTS", mk_matview_name(NS, target)
        ])
    ),
    ok = mg_core_storage_cql:execute_query(
        Client,
        mk_statement([
            "DROP TABLE IF EXISTS", mk_table_name(NS)
        ])
    ).

-spec mk_table_name(ns()) -> iodata().
mk_table_name(NS) ->
    mg_core_string_utils:join_(NS, "machines").

-spec mk_matview_name(ns(), column()) -> iodata().
mk_matview_name(NS, Column) ->
    mg_core_string_utils:join_(mk_table_name(NS), Column).

-spec mk_statement(list()) -> binary().
mk_statement(List) ->
    erlang:iolist_to_binary(mg_core_string_utils:join(List)).
