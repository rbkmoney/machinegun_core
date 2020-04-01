
%% Timer operations

-record(mg_core_timer_lifecycle_created, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    target_timestamp :: genlib_time:ts()
}).

-record(mg_core_timer_lifecycle_removed, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_timer_lifecycle_rescheduled, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    target_timestamp :: genlib_time:ts(),
    attempt :: non_neg_integer()
}).

-record(mg_core_timer_lifecycle_rescheduling_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    exception :: mg_core_utils:exception()
}).

%% Timer processing

-record(mg_core_timer_process_started, {
    queue :: normal | retries,
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    target_timestamp :: genlib_time:ts(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_timer_process_finished, {
    queue :: normal | retries,
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    target_timestamp :: genlib_time:ts(),
    deadline :: mg_core_deadline:deadline(),
    duration :: non_neg_integer()  % in native units
}).

%% Scheduler

-record(mg_core_scheduler_search_success, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    delay :: mg_core_queue_scanner:scan_delay(),
    tasks :: [mg_core_queue_task:task()],
    limit :: mg_core_queue_scanner:scan_limit(),
    duration :: non_neg_integer()  % in native units
}).

-record(mg_core_scheduler_search_error, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    exception :: mg_core_utils:exception()
}).

-record(mg_core_scheduler_task_error, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    exception :: mg_core_utils:exception(),
    machine_id :: mg_core:id() | undefined
}).

-record(mg_core_scheduler_task_add_error, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    exception :: mg_core_utils:exception(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_scheduler_new_tasks, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    new_tasks_count :: non_neg_integer()
}).

-record(mg_core_scheduler_task_started, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    machine_id :: mg_core:id() | undefined,
    task_delay :: timeout()
}).

-record(mg_core_scheduler_task_finished, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    machine_id :: mg_core:id() | undefined,
    task_delay :: timeout(),
    process_duration :: non_neg_integer()  % in native units
}).

-record(mg_core_scheduler_quota_reserved, {
    namespace :: mg_core:ns(),
    scheduler_name :: mg_core_scheduler:name(),
    active_tasks :: non_neg_integer() ,
    waiting_tasks :: non_neg_integer(),
    quota_name :: mg_core_quota_worker:name(),
    quota_reserved :: mg_core_quota:resource()
}).

%% Machine

-record(mg_core_machine_process_transient_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    exception :: mg_core_utils:exception(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_process_started, {
    processor_impact :: mg_core_machine:processor_impact(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_machine_process_finished, {
    processor_impact :: mg_core_machine:processor_impact(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    duration :: non_neg_integer()  % in native units
}).

%% Machines state

-record(mg_core_machine_lifecycle_loaded, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_lifecycle_created, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_lifecycle_removed, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context()
}).

-record(mg_core_machine_lifecycle_unloaded, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id()
}).

-record(mg_core_machine_lifecycle_committed_suicide, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    suicide_probability :: mg_core_machine:suicide_probability()
}).

-record(mg_core_machine_lifecycle_failed, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    exception :: mg_core_utils:exception()
}).

-record(mg_core_machine_lifecycle_loading_error, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    exception :: mg_core_utils:exception()
}).

-record(mg_core_machine_lifecycle_transient_error, {
    context :: atom(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    exception :: mg_core_utils:exception(),
    request_context :: mg_core:request_context(),
    retry_strategy :: mg_core_retry:strategy(),
    retry_action :: {wait, timeout(), mg_core_retry:strategy()} | finish
}).

%% Storage operations
%% Duration is in native units

-record(mg_core_storage_get_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_get_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_core_storage_put_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_put_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_core_storage_search_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_search_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

-record(mg_core_storage_delete_start, {
    name :: mg_core_storage:name()
}).

-record(mg_core_storage_delete_finish, {
    name :: mg_core_storage:name(),
    duration :: non_neg_integer()
}).

%% Workers management

-record(mg_core_worker_call_attempt, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline()
}).

-record(mg_core_worker_start_attempt, {
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    msg_queue_len :: non_neg_integer(),
    msg_queue_limit :: mg_core_workers_manager:queue_limit()
}).

%% Events sink operations

-record(mg_core_events_sink_kafka_sent, {
    name :: atom(),
    namespace :: mg_core:ns(),
    machine_id :: mg_core:id(),
    request_context :: mg_core:request_context(),
    deadline :: mg_core_deadline:deadline(),
    encode_duration :: non_neg_integer(),  % in native units
    send_duration :: non_neg_integer(),  % in native units
    data_size :: non_neg_integer(),  % in bytes
    partition :: brod:partition(),
    offset :: brod:offset()
}).
