-module(mg_core_events_storage_cql).

-include_lib("cqerl/include/cqerl.hrl").

-behaviour(mg_core_events_storage).
-export([get_events/4]).
-export([store_events/4]).

%% Bootstrapping
-export([bootstrap/2]).
-export([teardown/2]).

-type options() :: #{
    % Base
    name := mg_core_events_storage:name(),
    pulse := mg_core_pulse:handler(),
    % Network
    node := {inet:ip_address() | string(), Port :: integer()},
    ssl => [ssl:tls_client_option()],
    % Data
    keyspace := atom(),
    consistency => #{
        read => consistency_level(),
        write => consistency_level()
    }
}.

-type machine_ns() :: mg_core:ns().
-type machine_id() :: mg_core:id().
-type event() :: mg_core_events:event().
-type events_range() :: mg_core_events:events_range().

-define(COLUMNS, [
    machine_id,
    page_offset
    | ?EVENT_COLUMNS
]).

-define(EVENT_COLUMNS, [
    event_id,
    created_at,
    body,
    body_format_vsn
]).

% NOTE
% We need to spread events across partitions because of the prospect of a single
% partition becoming too large.
% * https://cassandra.apache.org/doc/latest/cassandra/data_modeling/intro.html#data-model-analysis
% * https://docs.scylladb.com/troubleshooting/large-partition-table/#configure
% A value of 50 seems good enough: numerous short-lived machines have way less
% events on average than that, uncommon infinitely updating machines with regular
% snapshots should not consume too much space per partition.
% Making this configurable while being useful promises to be really cumbersome so
% it's out of scope right now.
-define(EVENTS_PER_PAGE, 50).

%%

-spec get_events(options(), machine_ns(), machine_id(), events_range()) -> [event()].
get_events(Options, NS, MachineID, Range) ->
    get_events(Options, NS, MachineID, Range, []).

-spec get_events(options(), machine_ns(), machine_id(), events_range(), Acc :: [event()]) ->
    [event()].
get_events(Options, NS, MachineID, Range, Acc) ->
    case mg_core_dirange:direction(Range) of
        Direction when Direction /= 0 ->
            PageCutoff = compute_event_page_cutoff(Range),
            PageRanges = mg_core_dirange:dissect(Range, PageCutoff),
            get_events(Options, NS, MachineID, Direction, PageRanges, Acc);
        0 ->
            % Range is empty
            Acc
    end.

get_events(Options, NS, MachineID, +1, {PageRange, Rest}, Acc) ->
    % Range is forward: accumulate rest of events...
    AccTail = get_events(Options, NS, MachineID, Rest, Acc),
    % ...then append current page.
    get_events_page(Options, NS, MachineID, PageRange, AccTail);
get_events(Options, NS, MachineID, -1, {PageRange, Rest}, Acc) ->
    % Range is backward: fetch current page...
    AccTail = get_events_page(Options, NS, MachineID, PageRange, Acc),
    % ...then append rest of events.
    get_events(Options, NS, MachineID, Rest, AccTail).

get_events_page(Options, NS, MachineID, PageRange, Acc) ->
    Query = mk_get_query(Options, NS, MachineID, PageRange),
    mg_core_storage_cql:execute_query(Options, Query, fun(Result) ->
        accumulate_events(Result, Acc)
    end).

accumulate_events(Result, Acc) ->
    case cqerl:head(Result) of
        Record when is_list(Record) ->
            [read_event(maps:from_list(Record)) | accumulate_events(cqerl:tail(Result), Acc)];
        empty_dataset ->
            Acc
    end.

read_event(#{event_id := ID, created_at := TS} = Record) ->
    #{
        id => ID,
        created_at => mg_core_storage_cql:read_timestamp_ns(TS),
        body => read_event_body(Record)
    }.

read_event_body(#{body := Body} = Record) ->
    {read_event_body_metadata(Record), mg_core_storage_cql:read_opaque(Body)}.

read_event_body_metadata(#{body_format_vsn := null}) ->
    #{};
read_event_body_metadata(#{body_format_vsn := FV}) ->
    #{format_version => FV}.

mk_get_query(Options, NS, MachineID, Range) ->
    {From, To, Ordering} =
        case mg_core_dirange:bounds(Range) of
            {F, T} when F =< T -> {F, T, "ASC"};
            {T, F} when T > F -> {F, T, "DESC"}
        end,
    mg_core_storage_cql:mk_query(
        Options,
        select,
        mk_statement([
            "SELECT",
            mg_core_string_utils:join_with(?EVENT_COLUMNS, ","),
            "FROM",
            mk_table_name(NS),
            "WHERE machine_id = :machine_id",
            "AND page_offset = :page_offset",
            "AND event_id >= :event_from",
            "AND event_id <= :event_to",
            "ORDER BY event_id",
            Ordering
        ]),
        [
            {machine_id, MachineID},
            {page_offset, compute_event_page_offset(From)},
            {event_from, From},
            {event_to, To}
        ]
    ).

-spec store_events(options(), machine_ns(), machine_id(), [event()]) -> ok.
store_events(Options, NS, MachineID, Events) ->
    Query = mg_core_storage_cql:mk_query(
        Options,
        update,
        mk_statement([
            "INSERT INTO",
            mk_table_name(NS),
            "(",
            mg_core_string_utils:join_with(?COLUMNS, ","),
            ")",
            "VALUES (",
            mg_core_string_utils:join_with(["?" || _ <- ?COLUMNS], ","),
            ")"
        ]),
        []
    ),
    Values = [{machine_id, MachineID}],
    Batch = [write_event(Event, Values) || Event <- Events],
    mg_core_storage_cql:execute_batch(Options, Query, Batch).

write_event(#{id := ID, created_at := TS, body := Body}, Values) ->
    write_event_body(Body, [
        {event_id, ID},
        {page_offset, compute_event_page_offset(ID)},
        {created_at, mg_core_storage_cql:write_timestamp_ns(TS)}
        | Values
    ]).

write_event_body({MD, Content}, Values) ->
    write_event_body_metadata(MD, [
        {body, mg_core_storage_cql:write_opaque(Content)}
        | Values
    ]).

write_event_body_metadata(#{format_version := FV}, Values) ->
    [{body_format_vsn, FV} | Values];
write_event_body_metadata(#{}, Values) ->
    Values.

compute_event_page_offset(EventID) ->
    % What page this event belongs to?
    EventID - EventID rem ?EVENTS_PER_PAGE.

compute_event_page_cutoff(Range) ->
    % Where does "current" page of the event range ends?
    compute_event_page_offset(mg_core_dirange:from(Range)) +
        case mg_core_dirange:direction(Range) of
            +1 -> ?EVENTS_PER_PAGE - 1;
            -1 -> 0
        end.

%%

-spec bootstrap(options(), machine_ns()) -> ok.
bootstrap(Options, NS) ->
    TableName = mk_table_name(NS),
    ok = mg_core_storage_cql:execute_query(
        Options,
        mk_statement([
            mg_core_string_utils:join(["CREATE TABLE", TableName, "("]),
            % NOTE
            % Keep in sync with `?COLUMNS`.
            "machine_id TEXT,"
            "page_offset INT,"
            "event_id INT,"
            "created_at TUPLE<DATE, TIME>,"
            "body BLOB,"
            "body_format_vsn SMALLINT,"
            "PRIMARY KEY ((machine_id, page_offset), event_id)"
            ")"
        ])
    ).

-spec teardown(options(), machine_ns()) -> ok.
teardown(Options, NS) ->
    ok = mg_core_storage_cql:execute_query(
        Options,
        mk_statement([
            "DROP TABLE IF EXISTS", mk_table_name(NS)
        ])
    ).

-spec mk_table_name(machine_ns()) -> iodata().
mk_table_name(NS) ->
    mg_core_string_utils:join_(NS, "events").

-spec mk_statement(list()) -> binary().
mk_statement(List) ->
    erlang:iolist_to_binary(mg_core_string_utils:join(List)).
