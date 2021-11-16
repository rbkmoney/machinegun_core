%% TODOs
%% * what's up with `HandlingTimeout`? It's not used anywhere AFAICS.

-module(mg_core_machine_storage).

-export([child_spec/2]).

-export([get/3]).
-export([update/6]).
-export([remove/4]).
-export([search/4]).
-export([search/5]).

-export_type([name/0]).
-export_type([options/0]).
-export_type([storage_options/0]).
-export_type([context/0]).
-export_type([machine/0]).
-export_type([machine_status/0]).
-export_type([machine_status_class/0]).
-export_type([machine_state/0]).

-export_type([search_status_query/0]).
-export_type([search_target_query/0]).
-export_type([search_status_result/0]).
-export_type([search_target_result/0]).
-export_type([search_page/1]).
-export_type([search_limit/0]).
-export_type([continuation/0]).

-type name() :: term().
-type options() :: mg_core_utils:mod_opts(storage_options()).
-type storage_options() :: #{
    name := name(),
    processor := module(),
    pulse := mg_core_pulse:handler(),
    atom() => _
}.

-type context() :: term().
-type ns() :: mg_core:ns().
-type id() :: mg_core:id().
-type request_context() :: mg_core:request_context().
-type unix_timestamp_s() :: mg_core:unix_timestamp_s().

-type machine() :: #{
    status => machine_status(),
    state => machine_state()
}.

-type machine_regular_status() ::
    sleeping
    | {waiting, unix_timestamp_s(), request_context(), HandlingTimeout :: mg_core:timeout_ms()}
    | {retrying, Target :: unix_timestamp_s(), Start :: unix_timestamp_s(),
        Attempt :: non_neg_integer(), request_context()}
    | {processing, request_context()}.

-type machine_status() ::
    machine_regular_status() | {error, Reason :: term(), machine_regular_status()}.

-type machine_status_class() ::
    sleeping | waiting | retrying | processing | error.

-type machine_state() :: term().

-type search_status_query() :: machine_status_class().
-type search_target_query() ::
    {waiting, From :: unix_timestamp_s(), To :: unix_timestamp_s()}
    | {retrying, From :: unix_timestamp_s(), To :: unix_timestamp_s()}.

-type search_status_result() :: id().
-type search_target_result() :: {unix_timestamp_s(), id()}.
-type search_page(T) :: {[T], continuation() | undefined}.
-type search_limit() :: pos_integer().
-type continuation() :: term().

-callback child_spec(storage_options(), atom()) ->
    supervisor:child_spec() | undefined.

-callback get(storage_options(), ns(), id()) ->
    {context(), machine()} | undefined.
-callback update(storage_options(), ns(), id(), machine(), machine() | undefined, context()) ->
    context().
-callback remove(storage_options(), ns(), id(), context()) ->
    ok.

-optional_callbacks([child_spec/2]).

-define(ID_SIZE_LOWER_BOUND, 1).
-define(ID_SIZE_UPPER_BOUND, 1024).

%%

-spec child_spec(options(), term()) -> supervisor:child_spec() | undefined.
child_spec(Options, ChildID) ->
    mg_core_utils:apply_mod_opts_if_defined(Options, child_spec, undefined, [ChildID]).

-spec get(options(), ns(), id()) -> undefined | {context(), machine()}.
get(Options, NS, ID) ->
    _ = validate_machine_id(ID),
    mg_core_utils:apply_mod_opts(Options, get, [NS, ID]).

-spec update(options(), ns(), id(), machine(), machine() | undefined, context()) -> context().
update(Options, NS, ID, Machine, MachineWas, Context) ->
    _ = validate_machine_id(ID),
    mg_core_utils:apply_mod_opts(Options, update, [NS, ID, Machine, MachineWas, Context]).

-spec remove(options(), ns(), id(), context()) -> ok.
remove(Options, NS, ID, Context) ->
    _ = validate_machine_id(ID),
    mg_core_utils:apply_mod_opts(Options, remove, [NS, ID, Context]).

search(Options, NS, SearchQuery, Limit) ->
    mg_core_utils:apply_mod_opts(Options, search, [NS, SearchQuery, Limit, undefined]).

search(Options, NS, SearchQuery, Limit, Continuation) ->
    mg_core_utils:apply_mod_opts(Options, search, [NS, SearchQuery, Limit, Continuation]).

-spec validate_machine_id(id()) -> true.
validate_machine_id(ID) when byte_size(ID) < ?ID_SIZE_LOWER_BOUND ->
    throw({logic, {invalid_machine_id, {too_small, ID}}});
validate_machine_id(ID) when byte_size(ID) > ?ID_SIZE_UPPER_BOUND ->
    throw({logic, {invalid_machine_id, {too_big, ID}}});
validate_machine_id(ID) ->
    mg_core_string_utils:is_printable(ID) orelse
        throw({logic, {invalid_machine_id, {nonprintable, ID}}}).
