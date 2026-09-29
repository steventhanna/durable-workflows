use super::{
    ActivityStatus, ApprovalStatus, AttemptOutcome, ScheduleRunStatus, WaitKind, WorkflowStatus,
};
use diesel::{AsChangeset, Insertable, Queryable, Selectable};

use crate::{ActivityId, ApprovalId, DbMillis, ScheduleRunId, WorkflowId};

use crate::schema::{
    durable_activity, durable_activity_attempt, durable_approval, durable_progress_event,
    durable_schedule_run, durable_schedule_state, durable_topic_lock, durable_workflow,
    durable_workflow_event,
};

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_workflow)]
#[diesel(check_for_backend(crate::Db))]
pub struct WorkflowRow {
    pub id: WorkflowId,
    pub kind: String,
    pub version: i32,
    pub input_json: String,
    pub state_json: String,
    pub state_version: i32,
    pub status: WorkflowStatus,
    pub result_json: Option<String>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub wait_kind: Option<WaitKind>,
    pub wait_reference_id: Option<i64>,
    pub available_at: DbMillis,
    pub activation_attempts: i32,
    pub max_activation_attempts: i32,
    pub consecutive_continuations: i32,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DbMillis>,
    pub deduplication_key: Option<String>,
    pub schedule_run_id: Option<ScheduleRunId>,
    pub root_workflow_id: Option<WorkflowId>,
    pub restarted_from_workflow_id: Option<WorkflowId>,
    pub parent_workflow_id: Option<WorkflowId>,
    pub parent_command_sequence: Option<i32>,
    pub command_sequence: i32,
    pub delivered_event_sequence: i32,
    pub created_at: DbMillis,
    pub updated_at: DbMillis,
    pub completed_at: Option<DbMillis>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_workflow_event)]
#[diesel(check_for_backend(crate::Db))]
pub struct WorkflowEventRow {
    pub id: i64,
    pub workflow_id: WorkflowId,
    pub sequence: i32,
    pub delivery_sequence: Option<i32>,
    pub event_type: String,
    pub metadata_json: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: DbMillis,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_activity)]
#[diesel(check_for_backend(crate::Db))]
pub struct ActivityRow {
    pub id: ActivityId,
    pub workflow_id: WorkflowId,
    pub command_sequence: i32,
    pub replacement_number: i32,
    pub kind: String,
    pub version: i32,
    pub topic: String,
    pub payload_json: String,
    pub status: ActivityStatus,
    pub available_at: DbMillis,
    pub max_attempts: i32,
    pub attempt_count: i32,
    pub timeout_millis: i64,
    pub lease_duration_millis: i64,
    pub retry_policy_json: String,
    pub operation_key: Option<String>,
    pub provider_result_json: Option<String>,
    pub last_error_category: Option<String>,
    pub last_error_message: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DbMillis>,
    pub root_activity_id: Option<ActivityId>,
    pub replaces_activity_id: Option<ActivityId>,
    pub created_at: DbMillis,
    pub updated_at: DbMillis,
    pub completed_at: Option<DbMillis>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_activity_attempt)]
#[diesel(check_for_backend(crate::Db))]
pub struct ActivityAttemptRow {
    pub activity_id: ActivityId,
    pub attempt_number: i32,
    pub worker_id: String,
    pub lease_token: String,
    pub started_at: DbMillis,
    pub heartbeat_at: DbMillis,
    pub finished_at: Option<DbMillis>,
    pub outcome: Option<AttemptOutcome>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub provider_result_json: Option<String>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_progress_event)]
#[diesel(check_for_backend(crate::Db))]
pub struct ProgressEventRow {
    pub activity_id: ActivityId,
    pub attempt_number: i32,
    pub sequence: i32,
    pub code: String,
    pub description: String,
    pub description_bytes: i32,
    pub completed_units: Option<i64>,
    pub total_units: Option<i64>,
    pub severity: String,
    pub metadata_json: Option<String>,
    pub created_at: DbMillis,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_approval)]
#[diesel(check_for_backend(crate::Db))]
pub struct ApprovalRow {
    pub id: ApprovalId,
    pub workflow_id: WorkflowId,
    pub command_sequence: i32,
    pub kind: String,
    pub version: i32,
    pub prompt_metadata_json: String,
    pub validation_schema_json: String,
    pub validation_version: i32,
    pub status: ApprovalStatus,
    pub requested_at: DbMillis,
    pub expires_at: Option<DbMillis>,
    pub decision_payload_json: Option<String>,
    pub decided_by: Option<i32>,
    pub operator_reason: Option<String>,
    pub resolved_at: Option<DbMillis>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_schedule_state)]
#[diesel(check_for_backend(crate::Db))]
pub struct ScheduleStateRow {
    pub schedule_key: String,
    pub definition_fingerprint: String,
    pub definition_version: i32,
    pub next_local_occurrence: String,
    pub next_occurrence_at: DbMillis,
    pub last_materialized_at: Option<DbMillis>,
    pub paused_at: Option<DbMillis>,
    pub paused_by: Option<i32>,
    pub pause_reason: Option<String>,
    pub created_at: DbMillis,
    pub updated_at: DbMillis,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_schedule_run)]
#[diesel(check_for_backend(crate::Db))]
pub struct ScheduleRunRow {
    pub id: ScheduleRunId,
    pub schedule_key: String,
    pub local_occurrence: String,
    pub scheduled_for: DbMillis,
    pub materialized_at: DbMillis,
    pub status: ScheduleRunStatus,
    pub reason: Option<String>,
    pub actor_id: Option<i32>,
    pub workflow_id: Option<WorkflowId>,
    pub created_at: DbMillis,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_topic_lock)]
#[diesel(check_for_backend(crate::Db))]
pub struct TopicLockRow {
    pub topic: String,
    pub max_concurrency: i32,
    pub updated_at: DbMillis,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_workflow)]
pub struct NewWorkflowRow {
    pub kind: String,
    pub version: i32,
    pub input_json: String,
    pub state_json: String,
    pub state_version: i32,
    pub status: WorkflowStatus,
    pub result_json: Option<String>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub wait_kind: Option<WaitKind>,
    pub wait_reference_id: Option<i64>,
    pub available_at: DbMillis,
    pub activation_attempts: i32,
    pub max_activation_attempts: i32,
    pub consecutive_continuations: i32,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DbMillis>,
    pub deduplication_key: Option<String>,
    pub schedule_run_id: Option<ScheduleRunId>,
    pub root_workflow_id: Option<WorkflowId>,
    pub restarted_from_workflow_id: Option<WorkflowId>,
    pub parent_workflow_id: Option<WorkflowId>,
    pub parent_command_sequence: Option<i32>,
    pub command_sequence: i32,
    pub delivered_event_sequence: i32,
    pub created_at: DbMillis,
    pub updated_at: DbMillis,
    pub completed_at: Option<DbMillis>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_workflow_event)]
pub struct NewWorkflowEventRow {
    pub workflow_id: WorkflowId,
    pub sequence: i32,
    pub delivery_sequence: Option<i32>,
    pub event_type: String,
    pub metadata_json: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: DbMillis,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_activity)]
pub struct NewActivityRow {
    pub workflow_id: WorkflowId,
    pub command_sequence: i32,
    pub replacement_number: i32,
    pub kind: String,
    pub version: i32,
    pub topic: String,
    pub payload_json: String,
    pub status: ActivityStatus,
    pub available_at: DbMillis,
    pub max_attempts: i32,
    pub attempt_count: i32,
    pub timeout_millis: i64,
    pub lease_duration_millis: i64,
    pub retry_policy_json: String,
    pub operation_key: Option<String>,
    pub provider_result_json: Option<String>,
    pub last_error_category: Option<String>,
    pub last_error_message: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DbMillis>,
    pub root_activity_id: Option<ActivityId>,
    pub replaces_activity_id: Option<ActivityId>,
    pub created_at: DbMillis,
    pub updated_at: DbMillis,
    pub completed_at: Option<DbMillis>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_activity_attempt)]
pub struct NewActivityAttemptRow {
    pub activity_id: ActivityId,
    pub attempt_number: i32,
    pub worker_id: String,
    pub lease_token: String,
    pub started_at: DbMillis,
    pub heartbeat_at: DbMillis,
    pub finished_at: Option<DbMillis>,
    pub outcome: Option<AttemptOutcome>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub provider_result_json: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_progress_event)]
pub struct NewProgressEventRow {
    pub activity_id: ActivityId,
    pub attempt_number: i32,
    pub sequence: i32,
    pub code: String,
    pub description: String,
    pub description_bytes: i32,
    pub completed_units: Option<i64>,
    pub total_units: Option<i64>,
    pub severity: String,
    pub metadata_json: Option<String>,
    pub created_at: DbMillis,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_approval)]
pub struct NewApprovalRow {
    pub workflow_id: WorkflowId,
    pub command_sequence: i32,
    pub kind: String,
    pub version: i32,
    pub prompt_metadata_json: String,
    pub validation_schema_json: String,
    pub validation_version: i32,
    pub status: ApprovalStatus,
    pub requested_at: DbMillis,
    pub expires_at: Option<DbMillis>,
    pub decision_payload_json: Option<String>,
    pub decided_by: Option<i32>,
    pub operator_reason: Option<String>,
    pub resolved_at: Option<DbMillis>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_schedule_state)]
pub struct NewScheduleStateRow {
    pub schedule_key: String,
    pub definition_fingerprint: String,
    pub definition_version: i32,
    pub next_local_occurrence: String,
    pub next_occurrence_at: DbMillis,
    pub last_materialized_at: Option<DbMillis>,
    pub paused_at: Option<DbMillis>,
    pub paused_by: Option<i32>,
    pub pause_reason: Option<String>,
    pub created_at: DbMillis,
    pub updated_at: DbMillis,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_schedule_run)]
pub struct NewScheduleRunRow {
    pub schedule_key: String,
    pub local_occurrence: String,
    pub scheduled_for: DbMillis,
    pub materialized_at: DbMillis,
    pub status: ScheduleRunStatus,
    pub reason: Option<String>,
    pub actor_id: Option<i32>,
    pub workflow_id: Option<WorkflowId>,
    pub created_at: DbMillis,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_topic_lock)]
pub struct NewTopicLockRow {
    pub topic: String,
    pub max_concurrency: i32,
    pub updated_at: DbMillis,
}

/// Changeset fragment that clears an activity's lease (S1, S9). Every
/// activity update that leaves `running` includes it, so a new exit path
/// cannot forget one of the three columns.
#[derive(Debug, Clone, Copy, AsChangeset)]
#[diesel(table_name = durable_activity)]
#[diesel(treat_none_as_null = true)]
pub(crate) struct LeaseCleared {
    lease_owner: Option<&'static str>,
    lease_token: Option<&'static str>,
    lease_expires_at: Option<DbMillis>,
}

impl LeaseCleared {
    pub(crate) const fn new() -> Self {
        Self {
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
        }
    }
}
