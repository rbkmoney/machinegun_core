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

%%%
%%% Оперирующая эвентами машина.
%%% Добавляет понятие эвента, тэга и ссылки(ref).
%%% Отсылает эвенты в event sink (если он указан).
%%%
%%% Эвенты в машине всегда идут в таком порядке, что слева самые старые.
%%%
-module(mg_core_events_machine).

-include_lib("machinegun_core/include/pulse.hrl").

%% API
-export_type([options/0]).
-export_type([storage_options/0]).
-export_type([ref/0]).
-export_type([machine/0]).
-export_type([tag_action/0]).
-export_type([timer_action/0]).
-export_type([complex_action/0]).
-export_type([state_change/0]).
-export_type([signal/0]).
-export_type([signal_args/0]).
-export_type([call_args/0]).
-export_type([repair_args/0]).
-export_type([signal_result/0]).
-export_type([call_result/0]).
-export_type([repair_result/0]).
-export_type([request_context/0]).

-export([child_spec/2]).
-export([start_link/1]).
-export([start/5]).
-export([repair/6]).
-export([simple_repair/4]).
-export([call/6]).
-export([get_machine/3]).
-export([remove/4]).

%% mg_core_machine handler
-behaviour(mg_core_machine).
-export([processor_child_spec/1, process_machine/7]).

-define(DEFAULT_RETRY_POLICY, {exponential, infinity, 2, 10, 60 * 1000}).

%%
%% API
%%
-callback processor_child_spec(_Options) -> supervisor:child_spec() | undefined.
-callback process_signal(_Options, request_context(), deadline(), signal_args()) -> signal_result().
-callback process_call(_Options, request_context(), deadline(), call_args()) -> call_result().
-callback process_repair(_Options, request_context(), deadline(), repair_args()) ->
    repair_result() | no_return().
-optional_callbacks([processor_child_spec/1]).

-type id() :: mg_core:id().
-type tag() :: mg_core_machine_tags:tag().
-type event() :: mg_core_events:event().
-type events_range() :: mg_core_events:events_range().

%% calls, signals, get_gistory
-type signal_args() :: {signal(), machine()}.
-type call_args() :: {term(), machine()}.
-type repair_args() :: {term(), machine()}.
-type signal_result() :: {state_change(), complex_action()}.
-type call_result() :: {term(), state_change(), complex_action()}.
-type repair_result() ::
    {ok, {term(), state_change(), complex_action()}}
    | {error, repair_error()}.
-type repair_error() :: {failed, term()}.
-type state_change() :: {aux_state(), [mg_core_events:body()]}.
-type signal() :: {init, term()} | timeout | {repair, term()}.
-type aux_state() :: mg_core_events:content().
-type request_context() :: mg_core:request_context().
-type reply_action() :: mg_core_machine:processor_reply_action().
-type flow_action() :: mg_core_machine:processor_flow_action().
-type process_result() :: {reply_action(), flow_action(), state()}.

-type machine() :: #{
    ns => mg_core:ns(),
    id => mg_core:id(),
    history => [mg_core_events:event()],
    history_range => mg_core_events:history_range(),
    aux_state => aux_state(),
    timer => int_timer()
}.

%% TODO сделать более симпатично
-type int_timer() ::
    {genlib_time:ts(), request_context(), pos_integer(), mg_core_events:history_range()}.

%% actions
-type complex_action() :: #{
    timer => timer_action() | undefined,
    tag => tag_action() | undefined,
    remove => remove | undefined
}.
-type tag_action() :: tag().
-type timer_action() ::
    {set_timer, timer(), mg_core_events:history_range() | undefined,
        Timeout :: pos_integer() | undefined}
    | unset_timer.
-type timer() :: {timeout, timeout_()} | {deadline, calendar:datetime()}.
-type timeout_() :: non_neg_integer().
-type deadline() :: mg_core_deadline:deadline().

-type ref() :: {id, id()} | {tag, tag()}.
-type options() :: #{
    namespace => mg_core:ns(),
    events_storage => storage_options(),
    processor => mg_core_utils:mod_opts(),
    tagging => mg_core_machine_tags:options(),
    machines => mg_core_machine:options(),
    retries => #{_Subject => mg_core_retry:policy()},
    pulse => mg_core_pulse:handler(),
    event_sinks => [mg_core_events_sink:handler()],
    default_processing_timeout => timeout(),
    event_stash_size => non_neg_integer()
}.
% like mg_core_storage:options() except `name`
-type storage_options() :: mg_core_utils:mod_opts(map()).

-spec child_spec(options(), atom()) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options]},
        restart => permanent,
        type => supervisor
    }.

-spec start_link(options()) -> mg_core_utils:gen_start_ret().
start_link(Options) ->
    mg_core_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        mg_core_utils:lists_compact([
            mg_core_events_storage:child_spec(Options),
            mg_core_machine_tags:child_spec(tags_machine_options(Options), tags),
            mg_core_machine:child_spec(machine_options(Options), automaton)
        ])
    ).

-spec start(options(), id(), term(), request_context(), deadline()) -> ok.
start(Options, ID, Args, ReqCtx, Deadline) ->
    HRange = {undefined, undefined, forward},
    ok = mg_core_machine:start(
        machine_options(Options),
        ID,
        {Args, HRange},
        ReqCtx,
        Deadline
    ).

-spec repair(
    options(),
    ref(),
    term(),
    mg_core_events:history_range(),
    request_context(),
    deadline()
) -> {ok, _Resp} | {error, repair_error()}.
repair(Options, Ref, Args, HRange, ReqCtx, Deadline) ->
    mg_core_machine:repair(
        machine_options(Options),
        ref2id(Options, Ref),
        {Args, HRange},
        ReqCtx,
        Deadline
    ).

-spec simple_repair(options(), ref(), request_context(), deadline()) -> ok.
simple_repair(Options, Ref, ReqCtx, Deadline) ->
    ok = mg_core_machine:simple_repair(
        machine_options(Options),
        ref2id(Options, Ref),
        ReqCtx,
        Deadline
    ).

-spec call(
    options(),
    ref(),
    term(),
    mg_core_events:history_range(),
    request_context(),
    deadline()
) -> _Resp.
call(Options, Ref, Args, HRange, ReqCtx, Deadline) ->
    mg_core_machine:call(
        machine_options(Options),
        ref2id(Options, Ref),
        {Args, HRange},
        ReqCtx,
        Deadline
    ).

-spec get_machine(options(), ref(), mg_core_events:history_range()) -> machine().
get_machine(Options, Ref, HRange) ->
    % нужно понимать, что эти операции разнесены по времени, и тут могут быть рэйсы
    ID = ref2id(Options, Ref),
    InitialState = opaque_to_state(mg_core_machine:get(machine_options(Options), ID)),
    EffectiveState = maybe_apply_delayed_actions(InitialState),
    _ = mg_core_utils:throw_if_undefined(EffectiveState, {logic, machine_not_found}),
    machine(Options, ID, EffectiveState, HRange).

-spec remove(options(), id(), request_context(), deadline()) -> ok.
remove(Options, ID, ReqCtx, Deadline) ->
    mg_core_machine:call(machine_options(Options), ID, remove, ReqCtx, Deadline).

%%

-spec ref2id(options(), ref()) -> id() | no_return().
ref2id(_, {id, ID}) ->
    ID;
ref2id(Options, {tag, Tag}) ->
    case mg_core_machine_tags:resolve(tags_machine_options(Options), Tag) of
        undefined -> throw({logic, machine_not_found});
        ID -> ID
    end.

%%
%% mg_core_processor handler
%%
-type state() :: #{
    events => [mg_core_events:event()],
    events_range => events_range(),
    aux_state => aux_state(),
    delayed_actions => delayed_actions(),
    timer => int_timer() | undefined
}.
-type delayed_actions() ::
    #{
        add_tag => mg_core_machine_tags:tag() | undefined,
        remove => remove | undefined,
        new_events_range => events_range(),
        % ED-290
        % Deprecated, should be removed after initial rollout.
        new_timer => int_timer() | undefined | unchanged,
        add_events => [mg_core_events:event()],
        new_aux_state => aux_state()
    }
    | undefined.

%%

-spec processor_child_spec(options()) -> supervisor:child_spec() | undefined.
processor_child_spec(Options) ->
    mg_core_utils:apply_mod_opts_if_defined(
        processor_options(Options),
        processor_child_spec,
        undefined
    ).

-spec process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, PackedState) -> Result when
    Options :: options(),
    ID :: id(),
    Impact :: mg_core_machine:processor_impact(),
    PCtx :: mg_core_machine:processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    PackedState :: mg_core_machine:machine_state(),
    Result :: mg_core_machine:processor_result().
process_machine(Options, ID, Impact, PCtx, ReqCtx, Deadline, PackedState) ->
    {ReplyAction, ProcessingFlowAction, NewState} =
        try
            process_machine_(
                Options,
                ID,
                Impact,
                PCtx,
                ReqCtx,
                Deadline,
                opaque_to_state(PackedState)
            )
        catch
            throw:{transient, Reason}:ST ->
                erlang:raise(throw, {transient, Reason}, ST);
            throw:Reason ->
                erlang:throw({transient, {processor_unavailable, Reason}})
        end,
    {ReplyAction, ProcessingFlowAction, state_to_opaque(NewState)}.

%%

-spec process_machine_(Options, ID, Impact, PCtx, ReqCtx, Deadline, State) -> Result when
    Options :: options(),
    ID :: id(),
    Impact :: mg_core_machine:processor_impact() | {'timeout', _},
    PCtx :: mg_core_machine:processing_context(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    State :: state(),
    Result :: process_result() | no_return().
process_machine_(
    Options,
    ID,
    Subj = timeout,
    PCtx,
    ReqCtx,
    Deadline,
    State = #{timer := {_, _, _, HRange}}
) ->
    NewState = State#{timer := undefined},
    process_machine_(Options, ID, {Subj, {undefined, HRange}}, PCtx, ReqCtx, Deadline, NewState);
process_machine_(_, _, {call, remove}, _, _, _, State) ->
    % TODO удалить эвенты (?)
    {{reply, ok}, remove, State};
process_machine_(Options, ID, {Subj, {Args, HRange}}, _, ReqCtx, Deadline, State) ->
    % обработка стандартных запросов
    % NOTE
    % We won't handle `undefined` here though it's possible, yet extremely unlikely.
    % Imagine a machine ordered to be removed failed during continuation, and then someone tries
    % to repair it.
    EffectiveState = maybe_apply_delayed_actions(State),
    Machine = machine(Options, ID, EffectiveState, HRange),
    % TODO ED-290
    % Keeping original state intact shouldn't be needed after initial rollout?
    process_machine_std(Options, ReqCtx, Deadline, Subj, Args, Machine, EffectiveState);
process_machine_(
    Options,
    ID,
    continuation,
    PCtx,
    ReqCtx,
    Deadline,
    State0 = #{delayed_actions := DelayedActions}
) ->
    % отложенные действия (эвент синк, тэг)
    %
    % надо понимать, что:
    %  - эвенты добавляются в event sink
    %  - создатся тэг
    %  - отсылается ответ
    %  - если есть удаление, то удаляется
    % надо быть аккуратнее, мест чтобы накосячить тут вагон и маленькая тележка  :-\
    %
    % действия должны обязательно произойти в конце концов (таймаута нет), либо машина должна упасть

    ok = update_event_sinks(Options, ID, ReqCtx, Deadline, State0),
    ok = add_tag(Options, ID, ReqCtx, Deadline, maps:get(add_tag, DelayedActions)),

    % TODO ED-290
    % Should be safe to drop after initial rollout, given there are no machines in production which
    % became failed during continuation processing.
    State1 =
        case DelayedActions of
            #{add_events := Events} ->
                {State, ExternalEvents} = split_events(Options, State0, Events),
                ok = store_events(Options, ID, ExternalEvents),
                % NOTE
                % Doing this so these `Events` won't duplicate in state later on, in
                % `try_apply_delayed_actions/1`, since it hijacks event stash.
                State#{delayed_actions := DelayedActions#{add_events := []}};
            #{} ->
                State0
        end,

    ReplyAction =
        case PCtx of
            #{state := Reply} ->
                {reply, Reply};
            undefined ->
                noreply
        end,
    {FlowAction, State2} =
        case maybe_apply_delayed_actions(State1) of
            undefined ->
                {remove, State1};
            StateNext ->
                {state_to_flow_action(StateNext), StateNext}
        end,
    {ReplyAction, FlowAction, reset_delayed_actions(State2)}.

-spec process_machine_std(Options, ReqCtx, Deadline, Subject, Args, Machine, State) ->
    process_result() | no_return()
when
    Options :: options(),
    ReqCtx :: request_context(),
    Deadline :: deadline(),
    Subject :: init | repair | call | timeout,
    Args :: term(),
    Machine :: machine(),
    State :: state().
process_machine_std(Options, ReqCtx, Deadline, repair, Args, Machine, State) ->
    case process_repair(Options, ReqCtx, Deadline, Args, Machine, State) of
        {ok, {Reply, NewState}} ->
            {noreply, {continue, {ok, Reply}}, NewState};
        {error, _} = Error ->
            {{reply, Error}, keep, State}
    end;
process_machine_std(Options, ReqCtx, Deadline, Subject, Args, Machine, State) ->
    {Reply, NewState} =
        case Subject of
            init -> process_signal(Options, ReqCtx, Deadline, {init, Args}, Machine, State);
            timeout -> process_signal(Options, ReqCtx, Deadline, timeout, Machine, State);
            call -> process_call(Options, ReqCtx, Deadline, Args, Machine, State)
        end,
    {noreply, {continue, Reply}, NewState}.

-spec add_tag(options(), id(), request_context(), deadline(), undefined | tag()) -> ok.
add_tag(_, _, _, _, undefined) ->
    ok;
add_tag(Options, ID, ReqCtx, Deadline, Tag) ->
    case mg_core_machine_tags:add(tags_machine_options(Options), Tag, ID, ReqCtx, Deadline) of
        ok ->
            ok;
        {already_exists, OtherMachineID} ->
            case mg_core_machine:is_exist(machine_options(Options), OtherMachineID) of
                true ->
                    % была договорённость, что при двойном тэгировании роняем машину
                    exit({double_tagging, OtherMachineID});
                false ->
                    % это забытый после удаления тэг
                    ok = mg_core_machine_tags:replace(
                        tags_machine_options(Options),
                        Tag,
                        ID,
                        ReqCtx,
                        Deadline
                    )
            end
    end.

-spec split_events(options(), state(), [event()]) ->
    {state(), [mg_core_events:event()]}.
split_events(#{event_stash_size := Max}, State = #{events := EventStash}, NewEvents) ->
    Events = EventStash ++ NewEvents,
    NumEvents = erlang:length(Events),
    case NumEvents > Max of
        true ->
            {External, Internal} = lists:split(NumEvents - Max, Events),
            {State#{events => Internal}, External};
        false ->
            {State#{events => Events}, []}
    end.

-spec retry_store_events(options(), id(), deadline(), [event()]) -> ok.
retry_store_events(Options, ID, Deadline, Events) ->
    % TODO ED-324
    % We won't notice transient errors here, guess we need them right at the storage level.
    ok = mg_core_retry:do(
        get_retry_strategy(Options, storage, Deadline),
        fun() -> store_events(Options, ID, Events) end
    ).

-spec store_events(options(), id(), [event()]) -> ok.
store_events(Options, ID, Events) ->
    mg_core_events_storage:store_events(Options, ID, Events).

-spec update_event_sinks(options(), id(), request_context(), deadline(), state()) -> ok.
%% TODO ED-290
%% Should be safe to drop this clause after initial rollout, given there are no machines in
%% production which became failed during continuation processing.
update_event_sinks(
    Options,
    ID,
    ReqCtx,
    Deadline,
    #{delayed_actions := #{add_events := Events}}
) ->
    push_events_to_event_sinks(Options, ID, ReqCtx, Deadline, Events);
update_event_sinks(
    Options,
    ID,
    ReqCtx,
    Deadline,
    State = #{delayed_actions := #{new_events_range := NewEventsRange}}
) ->
    Events = get_events(Options, ID, State, NewEventsRange),
    push_events_to_event_sinks(Options, ID, ReqCtx, Deadline, Events).

-spec push_events_to_event_sinks(options(), id(), request_context(), deadline(), [event()]) -> ok.
push_events_to_event_sinks(Options, ID, ReqCtx, Deadline, Events) ->
    Namespace = get_option(namespace, Options),
    EventSinks = maps:get(event_sinks, Options, []),
    lists:foreach(
        fun(EventSinkHandler) ->
            ok = mg_core_events_sink:add_events(
                EventSinkHandler,
                Namespace,
                ID,
                Events,
                ReqCtx,
                Deadline
            )
        end,
        EventSinks
    ).

-spec state_to_flow_action(state()) -> mg_core_machine:processor_flow_action().
state_to_flow_action(#{timer := undefined}) ->
    sleep;
state_to_flow_action(#{timer := {Timestamp, ReqCtx, HandlingTimeout, _}}) ->
    {wait, Timestamp, ReqCtx, HandlingTimeout}.

-spec apply_delayed_actions_to_state(delayed_actions(), state()) -> state() | remove.
apply_delayed_actions_to_state(#{remove := remove}, _) ->
    remove;
% TODO ED-290
% Should be safe to drop this clause after initial rollout, given there are no machines
% in production which became failed during continuation processing.
apply_delayed_actions_to_state(
    DA = #{
        add_events := NewEvents,
        new_aux_state := NewAuxState,
        new_events_range := NewEventsRange
    },
    State = #{events := Events}
) ->
    apply_delayed_timer_actions_to_state(
        DA,
        State#{
            % NOTE
            % Hijacking event stash stuff here, in the name of code simplicity.
            events := Events ++ NewEvents,
            events_range := NewEventsRange,
            aux_state := NewAuxState
        }
    );
apply_delayed_actions_to_state(DA, State) ->
    apply_delayed_timer_actions_to_state(DA, State).

-spec apply_delayed_timer_actions_to_state(delayed_actions(), state()) -> state().
apply_delayed_timer_actions_to_state(#{new_timer := unchanged}, State) ->
    State;
apply_delayed_timer_actions_to_state(#{new_timer := Timer}, State) ->
    State#{timer := Timer};
apply_delayed_timer_actions_to_state(#{}, State) ->
    State.

-spec emit_action_beats(options(), mg_core:id(), request_context(), complex_action()) -> ok.
emit_action_beats(Options, ID, ReqCtx, ComplexAction) ->
    ok = emit_timer_action_beats(Options, ID, ReqCtx, ComplexAction),
    ok.

-spec emit_timer_action_beats(options(), mg_core:id(), request_context(), complex_action()) -> ok.
emit_timer_action_beats(Options, ID, ReqCtx, #{timer := unset_timer}) ->
    #{namespace := NS, pulse := Pulse} = Options,
    mg_core_pulse:handle_beat(Pulse, #mg_core_timer_lifecycle_removed{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx
    });
emit_timer_action_beats(Options, ID, ReqCtx, #{timer := {set_timer, Timer, _, _}}) ->
    #{namespace := NS, pulse := Pulse} = Options,
    mg_core_pulse:handle_beat(Pulse, #mg_core_timer_lifecycle_created{
        namespace = NS,
        machine_id = ID,
        request_context = ReqCtx,
        target_timestamp = timer_to_timestamp(Timer)
    });
emit_timer_action_beats(_Options, _ID, _ReqCtx, #{}) ->
    ok.

%%

-spec process_signal(options(), request_context(), deadline(), signal(), machine(), state()) ->
    {ok, state()}.
process_signal(Options = #{processor := Processor}, ReqCtx, Deadline, Signal, Machine, State) ->
    SignalArgs = [ReqCtx, Deadline, {Signal, Machine}],
    {StateChange, ComplexAction} = mg_core_utils:apply_mod_opts(
        Processor,
        process_signal,
        SignalArgs
    ),
    #{id := ID} = Machine,
    NewState = handle_processing_result(
        Options,
        ID,
        StateChange,
        ComplexAction,
        ReqCtx,
        Deadline,
        State
    ),
    {ok, NewState}.

-spec process_call(options(), request_context(), deadline(), term(), machine(), state()) ->
    {_Resp, state()}.
process_call(Options = #{processor := Processor}, ReqCtx, Deadline, Args, Machine, State) ->
    CallArgs = [ReqCtx, Deadline, {Args, Machine}],
    {Resp, StateChange, ComplexAction} = mg_core_utils:apply_mod_opts(
        Processor,
        process_call,
        CallArgs
    ),
    #{id := ID} = Machine,
    NewState = handle_processing_result(
        Options,
        ID,
        StateChange,
        ComplexAction,
        ReqCtx,
        Deadline,
        State
    ),
    {Resp, NewState}.

-spec process_repair(options(), request_context(), deadline(), term(), machine(), state()) ->
    {ok, {_Resp, state()}} | {error, repair_error()}.
process_repair(Options = #{processor := Processor}, ReqCtx, Deadline, Args, Machine, State) ->
    RepairArgs = [ReqCtx, Deadline, {Args, Machine}],
    case mg_core_utils:apply_mod_opts(Processor, process_repair, RepairArgs) of
        {ok, {Resp, StateChange, ComplexAction}} ->
            #{id := ID} = Machine,
            NewState = handle_processing_result(
                Options,
                ID,
                StateChange,
                ComplexAction,
                ReqCtx,
                Deadline,
                State
            ),
            {ok, {Resp, NewState}};
        {error, _} = Error ->
            Error
    end.

-spec handle_processing_result(
    options(),
    id(),
    state_change(),
    complex_action(),
    request_context(),
    deadline(),
    state()
) ->
    state().
handle_processing_result(Options, ID, StateChange, ComplexAction, ReqCtx, Deadline, StateWas) ->
    {State, Events} = handle_state_change(
        Options,
        StateChange,
        handle_complex_action(ComplexAction, ReqCtx, StateWas)
    ),
    ok = retry_store_events(Options, ID, Deadline, Events),
    ok = emit_action_beats(Options, ID, ReqCtx, ComplexAction),
    State.

-spec handle_state_change(options(), state_change(), state()) ->
    {state(), [event()]}.
handle_state_change(
    Options,
    {AuxState, EventsBodies},
    StateWas = #{events_range := EventsRangeWas}
) ->
    {Events, EventsRange} = mg_core_events:generate_events_with_range(EventsBodies, EventsRangeWas),
    DelayedActions = #{
        % NOTE
        % This is a range of events which are not yet pushed to event sinks
        new_events_range => diff_event_ranges(EventsRange, EventsRangeWas)
    },
    State = add_delayed_actions(
        DelayedActions,
        StateWas#{
            events_range := EventsRange,
            aux_state := AuxState
        }
    ),
    split_events(Options, State, Events).

-spec diff_event_ranges(events_range(), events_range()) -> events_range().
diff_event_ranges(LHS, undefined) ->
    LHS;
diff_event_ranges(LHS, RHS) ->
    {_, Diff} = mg_core_dirange:dissect(LHS, mg_core_dirange:to(RHS)),
    Diff.

-spec handle_complex_action(complex_action(), request_context(), state()) ->
    state().
handle_complex_action(ComplexAction, ReqCtx, StateWas) ->
    TimerAction = maps:get(timer, ComplexAction, undefined),
    State = handle_timer_action(TimerAction, ReqCtx, StateWas),
    DelayedActions = #{
        add_tag => maps:get(tag, ComplexAction, undefined),
        remove => maps:get(remove, ComplexAction, undefined)
    },
    add_delayed_actions(DelayedActions, State).

-spec handle_timer_action(undefined | timer_action(), request_context(), state()) ->
    state().
handle_timer_action(undefined, _, State) ->
    State;
handle_timer_action(unset_timer, _, State) ->
    State#{timer := undefined};
handle_timer_action({set_timer, Timer, undefined, HandlingTimeout}, ReqCtx, State) ->
    HRange = {undefined, undefined, forward},
    handle_timer_action({set_timer, Timer, HRange, HandlingTimeout}, ReqCtx, State);
handle_timer_action({set_timer, Timer, HRange, undefined}, ReqCtx, State) ->
    handle_timer_action({set_timer, Timer, HRange, 30}, ReqCtx, State);
handle_timer_action({set_timer, Timer, HRange, HandlingTimeout}, ReqCtx, State) ->
    Timestamp = timer_to_timestamp(Timer),
    State#{timer := {Timestamp, ReqCtx, HandlingTimeout * 1000, HRange}}.

-spec timer_to_timestamp(timer()) -> genlib_time:ts().
timer_to_timestamp({timeout, Timeout}) ->
    erlang:system_time(second) + Timeout;
timer_to_timestamp({deadline, Deadline}) ->
    genlib_time:daytime_to_unixtime(Deadline).

%%

-spec processor_options(options()) -> mg_core_utils:mod_opts().
processor_options(Options) ->
    maps:get(processor, Options).

-spec machine_options(options()) -> mg_core_machine:options().
machine_options(Options = #{machines := MachinesOptions}) ->
    (maps:without([processor], MachinesOptions))#{
        processor => {?MODULE, Options}
    }.

-spec tags_machine_options(options()) -> mg_core_machine_tags:options().
tags_machine_options(#{tagging := Options}) ->
    Options.

-spec get_option(atom(), options()) -> _.
get_option(Subj, Options) ->
    maps:get(Subj, Options).

%%

-spec machine(options(), id(), state(), mg_core_events:history_range()) -> machine().
machine(Options = #{namespace := Namespace}, ID, State, HRange) ->
    #{
        events_range := EventsRange,
        aux_state := AuxState,
        timer := Timer
    } = State,
    QueryRange = mg_core_events:intersect_range(EventsRange, HRange),
    #{
        ns => Namespace,
        id => ID,
        history => get_events(Options, ID, State, QueryRange),
        history_range => HRange,
        aux_state => AuxState,
        timer => Timer
    }.

-type event_getter() :: fun((events_range()) -> [mg_core_events:event()]).
-type event_sources() :: [{events_range(), event_getter()}, ...].

-spec get_events(options(), id(), state(), events_range()) ->
    [event()].
get_events(_Options, _ID, _State, undefined) ->
    [];
get_events(Options, ID, #{events_range := EventsRange, events := EventStash}, FromRange) ->
    StorageSource = {EventsRange, storage_event_getter(Options, ID)},
    EventStashSource = {compute_events_range(EventStash), event_list_getter(EventStash)},
    Sources = [
        Source
     || Source = {Range, _Getter} <- [EventStashSource, StorageSource],
        Range /= undefined
    ],
    get_events(Sources, FromRange).

-spec get_events(event_sources(), events_range()) -> [event()].
get_events(Sources, EventsRange) ->
    lists:flatten(gather_events(Sources, EventsRange)).

-spec gather_events(event_sources(), events_range()) -> [event() | [event()]].
gather_events([{AvailRange, Getter} | Sources], EvRange) ->
    % NOTE
    % We find out which part of `EvRange` is covered by current source (which is `Range`)
    % and which parts are covered by other sources. In the most complex case there are three
    % parts. For example:
    % ```
    % EvRange    = {1, 42}
    % AvailRange = {35, 40}
    % intersect(EvRange, AvailRange) = {
    %     { 1, 34} = RL,
    %     {35, 40} = Range,
    %     {41, 42} = RR
    % }
    % ```
    {RL, Range, RR} = mg_core_dirange:intersect(EvRange, AvailRange),
    Events1 =
        case mg_core_dirange:size(RR) of
            0 -> [];
            _ -> gather_events(Sources, RR)
        end,
    Events2 =
        case mg_core_dirange:size(Range) of
            0 -> Events1;
            _ -> concat_events(Getter(Range), Events1)
        end,
    case mg_core_dirange:size(RL) of
        0 -> Events2;
        _ -> concat_events(gather_events(Sources, RL), Events2)
    end;
gather_events([], _EvRange) ->
    [].

-spec concat_events([event()], [event()]) -> [event() | [event()]].
concat_events(Events, []) ->
    Events;
concat_events(Events, Acc) ->
    [Events | Acc].

-spec storage_event_getter(options(), mg_core:id()) -> event_getter().
storage_event_getter(Options, ID) ->
    fun(Range) ->
        mg_core_events_storage:get_events(Options, ID, Range)
    end.

-spec event_list_getter([mg_core_events:event()]) -> event_getter().
event_list_getter(Events) ->
    fun(Range) ->
        mg_core_events:slice_events(Events, Range)
    end.

-spec maybe_apply_delayed_actions(state()) -> state() | undefined.
try_apply_delayed_actions(#{delayed_actions := undefined} = State) ->
    State;
maybe_apply_delayed_actions(#{delayed_actions := DA} = State) ->
    case apply_delayed_actions_to_state(DA, State) of
        NewState = #{} ->
            NewState;
        remove ->
            undefined
    end.

-spec reset_delayed_actions(state()) -> state().
reset_delayed_actions(#{delayed_actions := DA} = State) when DA /= undefined ->
    State#{delayed_actions := undefined}.

-spec add_delayed_actions(delayed_actions(), state()) -> state().
add_delayed_actions(DelayedActions, #{delayed_actions := undefined} = State) ->
    State#{delayed_actions => DelayedActions};
add_delayed_actions(NewDelayedActions, #{delayed_actions := OldDelayedActions} = State) ->
    MergedActions = maps:fold(fun add_delayed_action/3, OldDelayedActions, NewDelayedActions),
    State#{delayed_actions => MergedActions}.

-spec add_delayed_action(Field :: atom(), Value :: term(), delayed_actions()) -> delayed_actions().
%% Tag
add_delayed_action(add_tag, Tag, DelayedActions) ->
    % NOTE
    % Deliberately discarding existing tag action here, even if, say, ongoing repair does not tag.
    % This is kind of a hack, yet this way repairs are more useful in practice.
    DelayedActions#{add_tag => Tag};
%% Removing
add_delayed_action(remove, undefined, DelayedActions) ->
    DelayedActions;
add_delayed_action(remove, Remove, DelayedActions) ->
    DelayedActions#{remove => Remove};
%% TODO ED-290
%% Should be safe to drop after initial rollout, given there are no machines in production which
%% became failed during continuation processing.
add_delayed_action(new_events_range, Range, DelayedActions = #{add_events := NewEvents}) ->
    % NOTE
    % Preserve yet "unsinked" events in `new_events_range` so they'll get in event sinks next
    % continuation.
    NewEventsRange = compute_events_range(NewEvents),
    DelayedActions#{new_events_range => mg_core_dirange:unify(Range, NewEventsRange)};
add_delayed_action(new_events_range, Range, DelayedActions) ->
    % NOTE
    % Preserve yet "unsinked" events in `new_events_range` so they'll get in event sinks next
    % continuation.
    EventsRangeWas = maps:get(new_events_range, DelayedActions, mg_core_dirange:empty()),
    DelayedActions#{new_events_range => mg_core_dirange:unify(Range, EventsRangeWas)}.

-spec compute_events_range([mg_core_events:event()]) -> mg_core_events:events_range().
compute_events_range([]) ->
    mg_core_dirange:empty();
compute_events_range([#{id := ID} | _] = Events) ->
    mg_core_dirange:forward(ID, ID + erlang:length(Events) - 1).

%%
%% packer to opaque
%%
-spec state_to_opaque(state()) -> mg_core_storage:opaque().
state_to_opaque(State) ->
    #{
        events := Events,
        events_range := EventsRange,
        aux_state := AuxState,
        delayed_actions := DelayedActions,
        timer := Timer
    } = State,
    [
        4,
        mg_core_events:events_range_to_opaque(EventsRange),
        mg_core_events:content_to_opaque(AuxState),
        mg_core_events:maybe_to_opaque(DelayedActions, fun delayed_actions_to_opaque/1),
        mg_core_events:maybe_to_opaque(Timer, fun int_timer_to_opaque/1),
        mg_core_events:events_to_opaques(Events)
    ].

-spec opaque_to_state(mg_core_storage:opaque()) -> state().
%% при создании есть момент (continuation) когда ещё нет стейта
opaque_to_state(null) ->
    #{
        events => [],
        events_range => undefined,
        aux_state => {#{}, <<>>},
        delayed_actions => undefined,
        timer => undefined
    };
opaque_to_state([1, EventsRange, AuxState, DelayedActions]) ->
    #{
        events => [],
        events_range => mg_core_events:opaque_to_events_range(EventsRange),
        aux_state => {#{}, AuxState},
        delayed_actions => mg_core_events:maybe_from_opaque(
            DelayedActions,
            fun opaque_to_delayed_actions/1
        ),
        timer => undefined
    };
opaque_to_state([2, EventsRange, AuxState, DelayedActions, Timer]) ->
    State = opaque_to_state([1, EventsRange, AuxState, DelayedActions]),
    State#{
        timer := mg_core_events:maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
    };
opaque_to_state([3, EventsRange, AuxState, DelayedActions, Timer]) ->
    #{
        events => [],
        events_range => mg_core_events:opaque_to_events_range(EventsRange),
        aux_state => mg_core_events:opaque_to_content(AuxState),
        delayed_actions => mg_core_events:maybe_from_opaque(
            DelayedActions,
            fun opaque_to_delayed_actions/1
        ),
        timer => mg_core_events:maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
    };
opaque_to_state([4, EventsRange, AuxState, DelayedActions, Timer, Events]) ->
    #{
        events => mg_core_events:opaques_to_events(Events),
        events_range => mg_core_events:opaque_to_events_range(EventsRange),
        aux_state => mg_core_events:opaque_to_content(AuxState),
        delayed_actions => mg_core_events:maybe_from_opaque(
            DelayedActions,
            fun opaque_to_delayed_actions/1
        ),
        timer => mg_core_events:maybe_from_opaque(Timer, fun opaque_to_int_timer/1)
    }.

-spec delayed_actions_to_opaque(delayed_actions()) -> mg_core_storage:opaque().
delayed_actions_to_opaque(undefined) ->
    null;
delayed_actions_to_opaque(
    #{add_tag := Tag, remove := Remove, new_events_range := NewEventsRange}
) ->
    [
        4,
        mg_core_events:maybe_to_opaque(Tag, fun mg_core_events:identity/1),
        mg_core_events:maybe_to_opaque(Remove, fun remove_to_opaque/1),
        mg_core_events:events_range_to_opaque(NewEventsRange)
    ].

-spec opaque_to_delayed_actions(mg_core_storage:opaque()) -> delayed_actions().
opaque_to_delayed_actions(null) ->
    undefined;
% TODO ED-290
% Should be safe to drop legacy unmarshalling after initial rollout, given there are no machines
% in production which became failed during continuation processing.
opaque_to_delayed_actions([1, Tag, Timer, Events, AuxState, EventsRange]) ->
    #{
        add_tag => mg_core_events:maybe_from_opaque(Tag, fun mg_core_events:identity/1),
        new_timer => mg_core_events:maybe_from_opaque(Timer, fun opaque_to_delayed_timer_actions/1),
        remove => undefined,
        add_events => mg_core_events:opaques_to_events(Events),
        new_aux_state => {#{}, AuxState},
        new_events_range => mg_core_events:opaque_to_events_range(EventsRange)
    };
opaque_to_delayed_actions([2, Tag, Timer, Remove, Events, AuxState, EventsRange]) ->
    DelayedActions = opaque_to_delayed_actions([1, Tag, Timer, Events, AuxState, EventsRange]),
    DelayedActions#{
        remove := mg_core_events:maybe_from_opaque(Remove, fun opaque_to_remove/1),
        new_aux_state := {#{}, AuxState}
    };
opaque_to_delayed_actions([3, Tag, Timer, Remove, Events, AuxState, EventsRange]) ->
    DelayedActions = opaque_to_delayed_actions([
        2,
        Tag,
        Timer,
        Remove,
        Events,
        AuxState,
        EventsRange
    ]),
    DelayedActions#{
        new_aux_state := mg_core_events:opaque_to_content(AuxState)
    };
opaque_to_delayed_actions([4, Tag, Remove, EventsRange]) ->
    #{
        add_tag => mg_core_events:maybe_from_opaque(Tag, fun mg_core_events:identity/1),
        remove => mg_core_events:maybe_from_opaque(Remove, fun opaque_to_remove/1),
        new_events_range => mg_core_events:opaque_to_events_range(EventsRange)
    }.

-spec opaque_to_delayed_timer_actions(mg_core_storage:opaque()) ->
    genlib_time:ts() | undefined | unchanged.
opaque_to_delayed_timer_actions(<<"unchanged">>) ->
    unchanged;
opaque_to_delayed_timer_actions(Timer) ->
    opaque_to_int_timer(Timer).

-spec remove_to_opaque(remove) -> mg_core_storage:opaque().
remove_to_opaque(Value) ->
    enum_to_int(Value, [remove]).

-spec opaque_to_remove(mg_core_storage:opaque()) -> remove.
opaque_to_remove(Value) ->
    int_to_enum(Value, [remove]).

-spec enum_to_int(T, [T]) -> pos_integer().
enum_to_int(Value, Enum) ->
    lists_at(Value, Enum).

-spec int_to_enum(pos_integer(), [T]) -> T.
int_to_enum(Value, Enum) ->
    lists:nth(Value, Enum).

-spec int_timer_to_opaque(int_timer()) -> mg_core_storage:opaque().
int_timer_to_opaque({Timestamp, ReqCtx, HandlingTimeout, HRange}) ->
    [1, Timestamp, ReqCtx, HandlingTimeout, mg_core_events:history_range_to_opaque(HRange)].

-spec opaque_to_int_timer(mg_core_storage:opaque()) -> int_timer().
opaque_to_int_timer([1, Timestamp, ReqCtx, HandlingTimeout, HRange]) ->
    {Timestamp, ReqCtx, HandlingTimeout, mg_core_events:opaque_to_history_range(HRange)}.

%%

-spec get_retry_strategy(options(), _Subject :: storage, deadline()) -> mg_core_retry:strategy().
get_retry_strategy(Options, Subject, Deadline) ->
    Retries = maps:get(retries, Options, #{}),
    Policy = maps:get(Subject, Retries, ?DEFAULT_RETRY_POLICY),
    mg_core_retry:constrain(mg_core_retry:new_strategy(Policy), Deadline).

%%

-spec lists_at(E, [E]) -> pos_integer() | undefined.
lists_at(E, L) ->
    lists_at(E, L, 1).

-spec lists_at(E, [E], pos_integer()) -> pos_integer() | undefined.
lists_at(_, [], _) ->
    undefined;
lists_at(E, [H | _], N) when E =:= H ->
    N;
lists_at(E, [_ | T], N) ->
    lists_at(E, T, N + 1).
