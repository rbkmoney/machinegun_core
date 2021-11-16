-module(mg_core_events_stash_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

%% Tests
-export([stash_enlarge_test/1]).
-export([stash_shrink_test/1]).

%% Pulse
-export([handle_beat/2]).

%% Internal types

-type action() :: mg_core_events_machine:complex_action().
-type aux_state() :: term().
-type call() :: term().
-type event() :: term().
-type signal() :: mg_core_events_machine:signal().

-type handlers() :: #{
    signal_handler => fun((signal(), aux_state(), [event()]) -> {aux_state(), [event()], action()}),
    call_handler => fun(
        (call(), aux_state(), [event()]) -> {term(), aux_state(), [event()], action()}
    ),
    sink_handler => fun(([event()]) -> ok)
}.
-type processor() :: {module(), handlers()}.
-type options() :: #{
    processor := processor(),
    event_stash_size := non_neg_integer()
}.

-type test_name() :: atom().
-type group_name() :: atom().
-type config() :: [{atom(), _}].

-define(NS, <<"event_stash_test">>).

%% Common test handlers

-spec all() -> [test_name()].
all() ->
    [
        {group, storage_memory},
        {group, storage_cql}
    ].

-spec groups() -> [{group_name(), list(_Option), test_name()}].
groups() ->
    [
        {storage_memory, [], [{group, base}]},
        {storage_cql, [], [{group, base}]},
        {base, [], [
            stash_enlarge_test,
            stash_shrink_test
        ]}
    ].

-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_core_events_storage_cql, '_', '_'}, x),
    Apps = mg_core_ct_helper:start_applications([machinegun_core]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    mg_core_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) -> config().
init_per_group(storage_memory, C) ->
    {ok, StoragePid} = mg_core_storage_memory:start_link(#{name => ?MODULE}),
    true = erlang:unlink(StoragePid),
    KVSOptions = {mg_core_storage_memory, #{existing_storage_name => ?MODULE}},
    MachineStorage = {mg_core_machine_storage_kvs, #{kvs => KVSOptions}},
    EventsStorage = {mg_core_events_storage_kvs, #{kvs => KVSOptions}},
    [
        {machine_storage, MachineStorage},
        {events_storage, EventsStorage},
        {memory_storage_pid, StoragePid}
        | C
    ];
init_per_group(storage_cql, C) ->
    Options = #{
        node => {"scylla0", 9042},
        keyspace => mg,
        schema => mg_core_events_machine_cql_schema
    },
    MachineStorage = {mg_core_machine_storage_cql, Options},
    EventsStorage = {mg_core_events_storage_cql, Options},
    ok = mg_core_ct_helper:bootstrap_storage(MachineStorage, ?NS),
    ok = mg_core_ct_helper:bootstrap_storage(EventsStorage, ?NS),
    [
        {machine_storage, MachineStorage},
        {events_storage, EventsStorage}
        | C
    ];
init_per_group(base, C) ->
    C.

-spec end_per_group(group_name(), config()) -> ok.
end_per_group(storage_memory, C) ->
    proc_lib:stop(?config(memory_storage_pid, C), normal, 5000);
end_per_group(storage_cql, _C) ->
    ok;
end_per_group(base, _C) ->
    ok.

%% Tests

-spec stash_enlarge_test(config()) -> any().
stash_enlarge_test(C) ->
    MachineID = <<"machine_1">>,
    Options0 = #{
        processor =>
            {mg_core_ct_machine, #{
                signal_handler => fun({init, NewEvents}, AuxState, []) ->
                    {AuxState, NewEvents, #{}}
                end,
                call_handler => fun({emit, NewEvents}, AuxState, _) ->
                    {ok, AuxState, NewEvents, #{}}
                end
            }},
        event_stash_size => 3
    },
    {Pid0, EMOpts0} = start_automaton(Options0, C),
    ok = mg_core_ct_machine:start(EMOpts0, MachineID, [1, 2]),
    ok = mg_core_ct_machine:call(EMOpts0, MachineID, {emit, [3, 4]}),
    ?assertEqual([1, 2, 3, 4], mg_core_ct_machine:history(EMOpts0, MachineID)),
    ?assertEqual(
        [4, 3, 2, 1],
        mg_core_ct_machine:history(EMOpts0, MachineID, {undefined, undefined, backward})
    ),
    ok = stop_automaton(Pid0),
    Options1 = Options0#{event_stash_size => 5},
    {Pid1, EMOpts1} = start_automaton(Options1, C),
    ok = mg_core_ct_machine:call(EMOpts1, MachineID, {emit, [5, 6]}),
    ?assertEqual([1, 2, 3, 4, 5, 6], mg_core_ct_machine:history(EMOpts1, MachineID)),
    ?assertEqual(
        [6, 5, 4, 3, 2, 1],
        mg_core_ct_machine:history(EMOpts1, MachineID, {undefined, undefined, backward})
    ),
    ok = stop_automaton(Pid1).

-spec stash_shrink_test(config()) -> any().
stash_shrink_test(C) ->
    MachineID = <<"machine_2">>,
    Options0 = #{
        processor =>
            {mg_core_ct_machine, #{
                signal_handler => fun({init, NewEvents}, AuxState, []) ->
                    {AuxState, NewEvents, #{}}
                end,
                call_handler => fun({emit, NewEvents}, AuxState, _) ->
                    {ok, AuxState, NewEvents, #{}}
                end
            }},
        event_stash_size => 5
    },
    {Pid0, EMOpts0} = start_automaton(Options0, C),
    ok = mg_core_ct_machine:start(EMOpts0, MachineID, [1, 2]),
    ok = mg_core_ct_machine:call(EMOpts0, MachineID, {emit, [3, 4, 5]}),
    ?assertEqual([1, 2, 3, 4, 5], mg_core_ct_machine:history(EMOpts0, MachineID)),
    ?assertEqual(
        [5, 4, 3, 2, 1],
        mg_core_ct_machine:history(EMOpts0, MachineID, {undefined, undefined, backward})
    ),
    ok = stop_automaton(Pid0),
    Options1 = Options0#{event_stash_size => 3},
    {Pid1, EMOpts1} = start_automaton(Options1, C),
    ok = mg_core_ct_machine:call(EMOpts1, MachineID, {emit, [6, 7]}),
    ?assertEqual([1, 2, 3, 4, 5, 6, 7], mg_core_ct_machine:history(EMOpts1, MachineID)),
    ?assertEqual(
        [7, 6, 5, 4, 3, 2, 1],
        mg_core_ct_machine:history(EMOpts1, MachineID, {undefined, undefined, backward})
    ),
    ok = stop_automaton(Pid1).

%% Pulse handler

-spec handle_beat(_, mg_core_pulse:beat()) -> ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).

%% Internal functions

-spec start_automaton(options(), config()) -> {pid(), mg_core_events_machine:options()}.
start_automaton(Options, C) ->
    EMOpts = events_machine_options(Options, C),
    {mg_core_utils:throw_if_error(mg_core_events_machine:start_link(EMOpts)), EMOpts}.

-spec stop_automaton(pid()) -> ok.
stop_automaton(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec events_machine_options(options(), config()) -> mg_core_events_machine:options().
events_machine_options(Options, C) ->
    Scheduler = #{registry => mg_core_procreg_gproc, interval => 100},
    #{
        namespace => ?NS,
        processor => maps:get(processor, Options),
        tagging => #{
            namespace => <<?NS/binary, "_tags">>,
            storage => {mg_core_machine_storage_kvs, #{kvs => mg_core_storage_memory}},
            worker => #{
                registry => mg_core_procreg_gproc
            },
            pulse => ?MODULE,
            retries => #{}
        },
        machines => #{
            namespace => ?NS,
            storage => ?config(machine_storage, C),
            worker => #{
                registry => mg_core_procreg_gproc
            },
            pulse => ?MODULE,
            schedulers => #{
                timers => Scheduler,
                timers_retries => Scheduler,
                overseer => Scheduler
            }
        },
        events_storage => ?config(events_storage, C),
        event_sinks => [
            {mg_core_ct_machine, Options}
        ],
        pulse => ?MODULE,
        default_processing_timeout => timer:seconds(10),
        event_stash_size => maps:get(event_stash_size, Options)
    }.
