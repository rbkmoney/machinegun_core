-module(mg_core_machine_storage_kvs).

-behaviour(mg_core_machine_storage).
-export([child_spec/2]).
-export([get/3]).
-export([update/6]).
-export([remove/4]).
-export([search/5]).

-type ns() :: mg_core:ns().
-type id() :: mg_core:id().
-type options() :: #{
    % Base
    name := mg_core_machine_storage:name(),
    processor := module(),
    pulse := mg_core_pulse:handler(),
    % KV Storage
    kvs := mg_core_storage:options()
}.

-type machine() :: mg_core_machine_storage:machine().
-type machine_status() :: mg_core_machine_storage:machine_status().
-type machine_state() :: mg_core_machine_storage:machine_state().
-type context() :: mg_core_storage:context().

-type search_status_query() :: mg_core_machine_storage:search_status_query().
-type search_status_result() :: mg_core_machine_storage:search_status_result().
-type search_target_query() :: mg_core_machine_storage:search_target_query().
-type search_target_result() :: mg_core_machine_storage:search_target_result().
-type search_limit() :: mg_core_machine_storage:search_limit().
-type continuation() :: mg_core_storage:continuation().

-callback opaque_to_state(mg_core_storage:opaque()) -> machine_state().
-callback state_to_opaque(machine_state()) -> mg_core_storage:opaque().

%%

-spec child_spec(options(), _ChildID) -> supervisor:child_spec() | undefined.
child_spec(Options, ChildID) ->
    mg_core_storage:child_spec(kvs_options(Options), ChildID).

-spec get(options(), ns(), id()) -> undefined | {context(), machine()}.
get(Options, _NS, ID) ->
    case mg_core_storage:get(kvs_options(Options), ID) of
        undefined ->
            undefined;
        {Context, PackedMachine} ->
            {Context, opaque_to_storage_machine(Options, PackedMachine)}
    end.

-spec update(options(), ns(), id(), machine(), machine() | undefined, context()) -> context().
update(Options, _NS, ID, Machine, _MachineWas, Context) ->
    mg_core_storage:put(
        kvs_options(Options),
        ID,
        Context,
        storage_machine_to_opaque(Options, Machine),
        storage_machine_to_indexes(Machine)
    ).

-spec remove(options(), ns(), id(), context()) -> ok.
remove(Options, _NS, ID, Context) ->
    mg_core_storage:delete(kvs_options(Options), ID, Context).

-spec search
    (options(), ns(), search_status_query(), search_limit(), continuation() | undefined) ->
        mg_core_machine_storage:search_page(search_status_result());
    (options(), ns(), search_target_query(), search_limit(), continuation() | undefined) ->
        mg_core_machine_storage:search_page(search_target_result()).
search(Options, _NS, Query, Limit, Continuation) ->
    mg_core_storage:search(kvs_options(Options), storage_search_query(Query, Limit, Continuation)).

kvs_options(#{name := Name, pulse := Handler, kvs := KVSOptions}) ->
    {Mod, Options} = mg_core_utils:separate_mod_opts(KVSOptions, #{}),
    {Mod, Options#{name => Name, pulse => Handler}}.

%%
%% packer to opaque
%%
-spec storage_machine_to_opaque(options(), machine()) -> mg_core_storage:opaque().
storage_machine_to_opaque(#{processor := Processor}, #{status := Status, state := State}) ->
    OpaqueState = erlang:apply(Processor, state_to_opaque, [State]),
    [1, machine_status_to_opaque(Status), OpaqueState].

-spec opaque_to_storage_machine(options(), mg_core_storage:opaque()) -> machine().
opaque_to_storage_machine(#{processor := Processor}, [1, Status, OpaqueState]) ->
    State = erlang:apply(Processor, opaque_to_state, [OpaqueState]),
    #{status => opaque_to_machine_status(Status), state => State}.

-spec machine_status_to_opaque(machine_status()) -> mg_core_storage:opaque().
machine_status_to_opaque(Status) ->
    Opaque =
        case Status of
            sleeping ->
                1;
            {waiting, TS, ReqCtx, HdlTo} ->
                [2, TS, ReqCtx, HdlTo];
            {processing, ReqCtx} ->
                [3, ReqCtx];
            % TODO подумать как упаковывать reason
            {error, Reason, OldStatus} ->
                [4, erlang:term_to_binary(Reason), machine_status_to_opaque(OldStatus)];
            {retrying, TS, StartTS, Attempt, ReqCtx} ->
                [5, TS, StartTS, Attempt, ReqCtx]
        end,
    Opaque.

-spec opaque_to_machine_status(mg_core_storage:opaque()) -> machine_status().
opaque_to_machine_status(Opaque) ->
    case Opaque of
        1 ->
            sleeping;
        % совместимость со старой версией
        [2, TS] ->
            {waiting, TS, null, 30000};
        [2, TS, ReqCtx, HdlTo] ->
            {waiting, TS, ReqCtx, HdlTo};
        % совместимость со старой версией
        3 ->
            {processing, null};
        [3, ReqCtx] ->
            {processing, ReqCtx};
        [4, Reason, OldStatus] ->
            {error, erlang:binary_to_term(Reason), opaque_to_machine_status(OldStatus)};
        % устаревшее
        [4, Reason] ->
            {error, erlang:binary_to_term(Reason), sleeping};
        [5, TS, StartTS, Attempt, ReqCtx] ->
            {retrying, TS, StartTS, Attempt, ReqCtx}
    end.

%%
%% indexes
%%
-define(STATUS_IDX, {integer, <<"status">>}).
-define(WAITING_IDX, {integer, <<"waiting_date">>}).
-define(RETRYING_IDX, {integer, <<"retrying_date">>}).

-spec storage_search_query(
    search_status_query() | search_target_query(),
    mg_core_storage:index_limit(),
    mg_core_storage:continuation()
) -> mg_core_storage:index_query().
storage_search_query(Query, Limit, Continuation) ->
    erlang:append_element(storage_search_query(Query, Limit), Continuation).

-spec storage_search_query(
    search_status_query() | search_target_query(), mg_core_storage:index_limit()
) ->
    mg_core_storage:index_query().
storage_search_query(Query, Limit) ->
    erlang:append_element(storage_search_query(Query), Limit).

-spec storage_search_query(search_status_query() | search_target_query()) ->
    mg_core_storage:index_query().
storage_search_query(sleeping) ->
    {?STATUS_IDX, 1};
storage_search_query(waiting) ->
    {?STATUS_IDX, 2};
storage_search_query(processing) ->
    {?STATUS_IDX, 3};
storage_search_query(failed) ->
    {?STATUS_IDX, 4};
storage_search_query(retrying) ->
    {?STATUS_IDX, 5};
storage_search_query({waiting, FromTs, ToTs}) ->
    {?WAITING_IDX, {FromTs, ToTs}};
storage_search_query({retrying, FromTs, ToTs}) ->
    {?RETRYING_IDX, {FromTs, ToTs}}.

-spec storage_machine_to_indexes(machine()) -> [mg_core_storage:index_update()].
storage_machine_to_indexes(#{status := Status}) ->
    status_index(Status) ++ status_range_index(Status).

-spec status_index(machine_status()) -> [mg_core_storage:index_update()].
status_index(Status) ->
    StatusInt =
        case Status of
            sleeping -> 1;
            {waiting, _, _, _} -> 2;
            {processing, _} -> 3;
            {error, _, _} -> 4;
            {retrying, _, _, _, _} -> 5
        end,
    [{?STATUS_IDX, StatusInt}].

-spec status_range_index(machine_status()) -> [mg_core_storage:index_update()].
status_range_index({waiting, Timestamp, _, _}) ->
    [{?WAITING_IDX, Timestamp}];
status_range_index({retrying, Timestamp, _, _, _}) ->
    [{?RETRYING_IDX, Timestamp}];
status_range_index(_) ->
    [].
