-module(mg_core_events_machine_cql_schema).

%% Schema
-export([prepare_get_query/2]).
-export([prepare_update_query/4]).
-export([read_machine_state/2]).

%% Bootstrapping
-export([bootstrap/3]).
-export([teardown/3]).

-type options() :: undefined.
-type machine_state() :: mg_core_events_machine:state().

-type query_get() :: mg_core_machine_storage_cql:query_get().
-type query_update() :: mg_core_machine_storage_cql:query_update().
-type record() :: mg_core_machine_storage_cql:record().

%% TODO
%% Event stash? Is it abstractable over kvs only?
-define(COLUMNS, [
    events_range,
    events_stash,
    aux_state,
    aux_state_format_vsn,
    % TODO drop?
    timer_target,
    timer_handling_event_range,
    timer_handling_timeout,
    new_events_range,
    delayed_add_events,
    delayed_add_tag,
    delayed_remove
]).

%%

-spec prepare_get_query(options(), query_get()) -> query_get().
prepare_get_query(_Options, Query) ->
    ?COLUMNS ++ Query.

-spec prepare_update_query(options(), machine_state(), _Was :: machine_state(), query_update()) ->
    query_update().
prepare_update_query(_Options, State, StateWas, Query) ->
    write_changes(
        fun write_state/4,
        % NOTE
        % Order is important because we reuse timer- and aux-state-related
        % columns for some bits of delayed actions.
        [events, events_range, aux_state, timer, delayed_actions],
        State,
        genlib:define(StateWas, #{}),
        Query
    ).

write_changes(Fun, Things, V, Was, Query) ->
    lists:foldl(
        fun(Thing, QAcc) ->
            write_changed(
                Fun,
                Thing,
                maps:get(Thing, V, undefined),
                maps:get(Thing, Was, undefined),
                QAcc
            )
        end,
        Query,
        Things
    ).

write_changed(_, _, V1, V2, Query) when V1 =:= V2 ->
    Query;
write_changed(Fun, Thing, V, Was, Query) ->
    Fun(Thing, V, Was, Query).

write_state(events, [], _, Query) ->
    Query;
write_state(events, ES, _, Query) ->
    Query#{events_stash => write_events(ES)};
write_state(events_range, ER, _, Query) ->
    Query#{events_range => write_events_range(ER)};
write_state(aux_state, AS, _, Query) ->
    write_aux_state(AS, Query);
write_state(delayed_actions, DA, DAWas, Query) ->
    write_delayed_actions(DA, DAWas, Query);
write_state(timer, T, _, Query) ->
    write_timer(T, Query).

-spec write_aux_state(mg_core_events_machine:aux_state(), query_update()) -> query_update().
write_aux_state({MD, Content}, Query) ->
    write_aux_state_metadata(MD, Query#{
        aux_state => mg_core_storage_cql:write_opaque(Content)
    }).

-spec write_aux_state_metadata(mg_core_events:metadata(), query_update()) -> query_update().
write_aux_state_metadata(#{format_version := FV}, Query) ->
    Query#{
        aux_state_format_vsn => FV
    };
write_aux_state_metadata(#{}, Query) ->
    Query.

-spec write_delayed_actions(DA, Was, query_update()) -> query_update() when
    DA :: mg_core_events_machine:delayed_actions(),
    Was :: mg_core_events_machine:delayed_actions().
write_delayed_actions(DA = #{}, DAWas, Query) ->
    write_changes(
        fun write_delayed_action/4,
        [
            new_events_range,
            add_tag,
            new_timer,
            remove,
            add_events,
            new_aux_state
        ],
        DA,
        genlib:define(DAWas, #{}),
        Query
    );
write_delayed_actions(undefined, _, Query) ->
    Query#{
        new_events_range => null,
        delayed_add_events => null,
        delayed_remove => null,
        delayed_add_tag => null
    }.

write_delayed_action(new_events_range, ER, _, Query) ->
    Query#{new_events_range => write_events_range(ER)};
write_delayed_action(add_tag, T, _, Query) ->
    Query#{delayed_add_tag => T};
write_delayed_action(new_timer, unchanged, _, Query) ->
    Query;
write_delayed_action(new_timer, T, _, Query) ->
    write_timer(T, Query);
write_delayed_action(remove, R, _, Query) ->
    Query#{delayed_remove => write_remove_action(R)};
write_delayed_action(add_events, Es, _, Query) ->
    Query#{delayed_add_events => write_events(Es)};
write_delayed_action(new_aux_state, AS, _, Query) ->
    % TODO ED-290
    write_aux_state(AS, Query).

-spec write_timer(mg_core_events_machine:timer_state() | undefined, query_update()) ->
    query_update().
write_timer(undefined, Query) ->
    Query#{timer_target => null};
write_timer({TS, _ReqCtx, TO, ER}, Query) ->
    Query#{
        timer_target => mg_core_storage_cql:write_timestamp_s(TS),
        timer_handling_timeout => TO,
        timer_handling_event_range => write_events_range(ER)
    }.

%%

-spec read_machine_state(options(), record()) -> machine_state().
read_machine_state(_Options, Record) ->
    State0 = #{
        events => read_machine_events_stash(Record),
        events_range => read_machine_events_range(Record),
        aux_state => read_aux_state(Record),
        timer => read_timer(Record)
    },
    State0#{
        delayed_actions => read_delayed_actions(Record, State0)
    }.

-spec read_machine_events_stash(record()) -> [mg_core_events:event()].
read_machine_events_stash(#{events_stash := ES}) ->
    read_events(ES);
read_machine_events_stash(#{}) ->
    [].

-spec read_machine_events_range(record()) -> mg_core_events:events_range().
read_machine_events_range(#{events_range := ER}) ->
    read_events_range(ER).

-spec read_aux_state(record()) -> mg_core_events_machine:aux_state().
read_aux_state(Record = #{aux_state := AS}) ->
    {read_aux_state_metadata(Record), mg_core_storage_cql:read_opaque(AS)}.

-spec read_aux_state_metadata(record()) -> mg_core_events:metadata().
read_aux_state_metadata(#{aux_state_format_vsn := null}) ->
    #{};
read_aux_state_metadata(#{aux_state_format_vsn := FV}) when is_integer(FV) ->
    #{format_version => FV}.

-spec read_timer(record()) -> mg_core_events_machine:timer_state() | undefined.
read_timer(#{timer_target := null}) ->
    undefined;
read_timer(#{
    timer_target := TS,
    reqctx := ReqCtx,
    timer_handling_timeout := TO,
    timer_handling_event_range := ER
}) ->
    {
        mg_core_storage_cql:read_timestamp_s(TS),
        mg_core_storage_cql:read_opaque(ReqCtx),
        TO,
        read_events_range(ER)
    }.

-spec read_delayed_actions(record(), machine_state()) -> mg_core_events_machine:delayed_actions().
read_delayed_actions(#{new_events_range := null}, _) ->
    undefined;
read_delayed_actions(
    #{
        new_events_range := ER,
        delayed_add_events := AE,
        delayed_add_tag := T,
        delayed_remove := R
    },
    State
) ->
    #{
        add_tag => read_maybe(T),
        new_timer => maps:get(timer, State),
        remove => read_remove_action(R),
        add_events => read_events(AE),
        % TODO ED-290
        new_aux_state => maps:get(aux_state, State),
        new_events_range => read_events_range(ER)
    }.

%%

-spec read_events(binary()) -> [mg_core_events:event()].
read_events(Opaque) ->
    mg_core_events:opaques_to_events(mg_core_storage_cql:read_opaque(Opaque)).

-spec write_events([mg_core_events:event()]) -> binary().
write_events(Es) ->
    mg_core_storage_cql:write_opaque(mg_core_events:events_to_opaques(Es)).

-spec read_events_range(list(mg_core_events:id())) -> mg_core_events:events_range().
read_events_range([From, To]) ->
    mg_core_dirange:forward(From, To).

-spec write_events_range(mg_core_events:events_range()) -> list(mg_core_events:id()).
write_events_range(ER) ->
    {From, To} = mg_core_dirange:bounds(ER),
    [From, To].

-spec read_remove_action(boolean() | null) -> remove | undefined.
read_remove_action(true) ->
    remove;
read_remove_action(null) ->
    undefined.

-spec write_remove_action(remove | undefined) -> boolean() | null.
write_remove_action(remove) ->
    true;
write_remove_action(undefined) ->
    null.

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
                "ADD (",
                "events_range TUPLE<INT, INT>,"
                % TODO lowest `format_version` of events here?
                "events_stash BLOB,"
                "aux_state BLOB,"
                "aux_state_format_vsn SMALLINT,"
                "timer_target TIMESTAMP,"
                "timer_handling_event_range TUPLE<INT, INT>,"
                "timer_handling_timeout INT,"
                "new_events_range TUPLE<INT, INT>,"
                % TODO ED-290
                "delayed_add_events BLOB,"
                "delayed_add_tag TEXT,"
                "delayed_remove BOOLEAN",
                ")"
            ])
        )
    ),
    ok.

-spec teardown(options(), mg_core:ns(), mg_core_storage_cql:client()) -> ok.
teardown(_Options, _NS, _Client) ->
    ok.
