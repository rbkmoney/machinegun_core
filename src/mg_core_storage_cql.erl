-module(mg_core_storage_cql).

-include_lib("cqerl/include/cqerl.hrl").

-export([get_client/1]).
-export([mk_query/4]).
-export([execute_query/2]).
-export([execute_query/3]).
-export([execute_batch/3]).
-export([execute_continuation/2]).

-export([read_opaque/1]).
-export([write_opaque/1]).
-export([read_timestamp_s/1]).
-export([write_timestamp_s/1]).
-export([read_timestamp_ns/1]).
-export([write_timestamp_ns/1]).

-export_type([client/0]).
-export_type([cql_blob/0]).
-export_type([cql_smallint/0]).
-export_type([cql_timestamp/0]).
-export_type([cql_datum/0]).

-type options() :: #{
    % Base
    pulse := mg_core_pulse:handler(),
    % Network
    node := {inet:ip_address() | string(), Port :: integer()},
    ssl => [ssl:tls_client_option()],
    % Data
    keyspace := atom(),
    consistency => #{
        read => consistency_level(),
        write => consistency_level()
    },
    % NOTE
    % Anything put here on higher levels.
    atom() => _
}.

-type client() :: {pid(), reference()}.
-type verb() :: select | update | delete.
-type values() :: #{_Name :: atom() => cql_datum()} | [{_Name :: atom(), cql_datum()}].
-type cql_smallint() :: -32786..32767.
-type cql_timestamp() :: pos_integer().
-type cql_blob() :: binary().
-type cql_datum() :: integer() | binary() | cql_blob() | boolean() | list(cql_datum()) | null.
-type cql_query() :: iodata() | #cql_query{}.
-type cql_error() :: {_Code :: integer(), _Description :: binary(), _Details}.

% NOTE
% This should ensure that reads either see recent writes or fail.
% This way we prevent lost writes when, say, some machines fail over quickly to
% another node and resume processing.
-define(DEFAULT_READ_CONSISTENCY, quorum).
-define(DEFAULT_WRITE_CONSISTENCY, quorum).

% -define(CQL_DATE_EPOCH, 16#7FFFFFFF).
-define(NS_PER_SEC, 1000000000).
-define(NS_PER_DAY, (24 * 3600 * ?NS_PER_SEC)).

%%

-spec get_client(options()) -> client().
get_client(#{node := Node, keyspace := Keyspace} = Options) ->
    CqerlOpts = maps:to_list(maps:with([ssl], Options)),
    % NOTE
    % We resort to private API here because public one breaks dialyzer. 😠
    case cqerl_hash:get_client(Node, [{keyspace, Keyspace} | CqerlOpts]) of
        {ok, Client} ->
            Client;
        {error, Reason} ->
            erlang:throw({transient, {storage_unavailable, Reason}})
    end.

-spec mk_query(options(), verb(), iodata(), values()) -> #cql_query{}.
mk_query(Options, Verb, Statement, Values) ->
    #cql_query{
        statement = Statement,
        values = Values,
        consistency = get_query_consistency(Verb, Options),
        % NOTE
        % Avoids questionable regex analysis in cqerl.
        % https://github.com/cqerl/cqerl/blob/9d3c5f1a/src/cqerl_cache.erl#L55-L55
        reusable = true
    }.

-spec execute_query(options() | client(), cql_query(), fun((#cql_result{}) -> R)) -> R.
execute_query(Options = #{}, Query, Then) ->
    execute_query(get_client(Options), Query, Then);
execute_query(Client, Query, Then) ->
    ct:pal(" > QUERY: ~p", [Query]),
    try cqerl_client:run_query(Client, Query) of
        {ok, Result} ->
            ct:pal(" < RESULT: ~p", [Result]),
            Then(Result);
        {error, Error} ->
            handle_error(Error)
    catch
        % TODO noproc?
        exit:{timeout, _} ->
            handle_error(timeout)
    end.

-spec execute_query(options() | client(), cql_query()) -> ok.
execute_query(OptionsOrClient, Query) ->
    execute_query(OptionsOrClient, Query, fun(_) -> ok end).

-spec execute_batch(options(), cql_query(), [values()]) -> ok.
execute_batch(Options = #{}, Query, Batch) ->
    Client = get_client(Options),
    QueryBatch = #cql_query_batch{
        consistency = get_query_consistency(update, Options),
        queries = [Query#cql_query{values = Values} || Values <- Batch]
    },
    ct:pal(" > BATCH: ~p + ~p", [Query, Batch]),
    try cqerl_client:run_query(Client, QueryBatch) of
        {ok, Result} ->
            ct:pal(" < RESULT: ~p", [Result]),
            ok;
        {error, Error} ->
            handle_error(Error)
    catch
        % TODO noproc?
        exit:{timeout, _} ->
            handle_error(timeout)
    end.

-spec execute_continuation(#cql_result{}, fun((#cql_result{}) -> R)) -> R.
execute_continuation(Continuation, Then) ->
    ct:pal(" > CONTINUATION: ~p", [Continuation]),
    try cqerl_client:fetch_more(Continuation) of
        {ok, Result} ->
            ct:pal(" < RESULT: ~p", [Result]),
            Then(Result);
        {error, Error} ->
            handle_error(Error)
    catch
        % TODO noproc?
        exit:{timeout, _} ->
            handle_error(timeout)
    end.

-spec handle_error(cql_error() | connection_closed | timeout) -> no_return().
handle_error({Code, Description, Details}) when
    % https://github.com/apache/cassandra/blob/2e2db4dc40/doc/native_protocol_v4.spec#L1046
    Code >= 16#1000 andalso Code =< 16#1003;
    Code == 16#1100;
    Code == 16#1200;
    Code == 16#1300
->
    erlang:throw({transient, {storage_unavailable, {Description, Details}}});
handle_error(Reason) when
    Reason == connection_closed;
    Reason == timeout
->
    erlang:throw({transient, {storage_unavailable, Reason}}).

-spec get_query_consistency(verb(), options()) -> consistency_level().
get_query_consistency(select, #{consistency := #{read := Consistency}}) ->
    Consistency;
get_query_consistency(update, #{consistency := #{write := Consistency}}) ->
    Consistency;
get_query_consistency(delete, #{consistency := #{write := Consistency}}) ->
    Consistency;
get_query_consistency(select, _) ->
    ?DEFAULT_READ_CONSISTENCY;
get_query_consistency(update, _) ->
    ?DEFAULT_WRITE_CONSISTENCY;
get_query_consistency(delete, _) ->
    ?DEFAULT_WRITE_CONSISTENCY.

%%

-spec read_opaque(cql_blob()) -> mg_core_storage:opaque().
read_opaque(Bytes) ->
    mg_core_storage:binary_to_opaque(Bytes).

-spec write_opaque(mg_core_storage:opaque()) -> cql_blob().
write_opaque(Opaque) ->
    mg_core_storage:opaque_to_binary(Opaque).

-spec read_timestamp_s(cql_timestamp()) -> mg_core:unix_timestamp_s().
read_timestamp_s(TS) ->
    TS div 1000.

-spec write_timestamp_s(mg_core:unix_timestamp_s()) -> cql_timestamp().
write_timestamp_s(TS) ->
    % https://github.com/apache/cassandra/blob/2e2db4dc/doc/native_protocol_v4.spec#L946
    TS * 1000.

-spec read_timestamp_ns([integer()]) -> mg_core:unix_timestamp_ns().
read_timestamp_ns([Date, Time]) ->
    Seconds = genlib_time:daytime_to_unixtime({Date, {0, 0, 0}}),
    Seconds * ?NS_PER_SEC + Time.

-spec write_timestamp_ns(mg_core:unix_timestamp_ns()) -> [integer()].
write_timestamp_ns(TS) ->
    % NOTE
    % This is rather pointless. Client will happily convert it back to an integer-based
    % representation, while being more wasteful at that:
    % https://github.com/cqerl/cqerl/blob/8ae26d76/src/cqerl_datatypes.erl#L361-L363
    % It would be much easier to just give the CQL backend a date in integer
    % representation as outlined here:
    % https://github.com/apache/cassandra/blob/2e2db4dc/doc/native_protocol_v4.spec#L885
    % [?CQL_DATE_EPOCH + (TS div ?NS_PER_DAY), Time].
    Seconds = TS div ?NS_PER_SEC,
    {Date, _} = genlib_time:unixtime_to_daytime(Seconds),
    % https://github.com/apache/cassandra/blob/2e2db4dc/doc/native_protocol_v4.spec#L941
    Time = TS rem ?NS_PER_DAY,
    [Date, Time].
