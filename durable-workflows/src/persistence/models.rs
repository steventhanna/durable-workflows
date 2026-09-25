use super::{ActivityStatus, WorkflowStatus};
use diesel::{Insertable, Queryable, Selectable};

use crate::schema::{
    durable_activity, durable_activity_attempt, durable_approval, durable_progress_event,
    durable_schedule_run, durable_schedule_state, durable_topic_lock, durable_workflow,
    durable_workflow_event,
};

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_workflow)]
#[diesel(check_for_backend(crate::Db))]
pub struct WorkflowRow {
    pub id: i64,
    pub kind: String,
    pub version: i32,
    pub input_json: String,
    pub state_json: String,
    pub state_version: i32,
    pub status: WorkflowStatus,
    pub result_json: Option<String>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub wait_kind: Option<String>,
    pub wait_reference_id: Option<i64>,
    pub available_at: i64,
    pub activation_attempts: i32,
    pub max_activation_attempts: i32,
    pub consecutive_continuations: i32,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub deduplication_key: Option<String>,
    pub schedule_run_id: Option<i64>,
    pub root_workflow_id: Option<i64>,
    pub restarted_from_workflow_id: Option<i64>,
    pub parent_workflow_id: Option<i64>,
    pub parent_command_sequence: Option<i32>,
    pub command_sequence: i32,
    pub delivered_event_sequence: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_workflow_event)]
#[diesel(check_for_backend(crate::Db))]
pub struct WorkflowEventRow {
    pub id: i64,
    pub workflow_id: i64,
    pub sequence: i32,
    pub delivery_sequence: Option<i32>,
    pub event_type: String,
    pub metadata_json: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_activity)]
#[diesel(check_for_backend(crate::Db))]
pub struct ActivityRow {
    pub id: i64,
    pub workflow_id: i64,
    pub command_sequence: i32,
    pub replacement_number: i32,
    pub kind: String,
    pub version: i32,
    pub topic: String,
    pub payload_json: String,
    pub status: ActivityStatus,
    pub available_at: i64,
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
    pub lease_expires_at: Option<i64>,
    pub root_activity_id: Option<i64>,
    pub replaces_activity_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_activity_attempt)]
#[diesel(check_for_backend(crate::Db))]
pub struct ActivityAttemptRow {
    pub activity_id: i64,
    pub attempt_number: i32,
    pub worker_id: String,
    pub lease_token: String,
    pub started_at: i64,
    pub heartbeat_at: i64,
    pub finished_at: Option<i64>,
    pub outcome: Option<String>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub provider_result_json: Option<String>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_progress_event)]
#[diesel(check_for_backend(crate::Db))]
pub struct ProgressEventRow {
    pub activity_id: i64,
    pub attempt_number: i32,
    pub sequence: i32,
    pub code: String,
    pub description: String,
    pub description_bytes: i32,
    pub completed_units: Option<i64>,
    pub total_units: Option<i64>,
    pub severity: String,
    pub metadata_json: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_approval)]
#[diesel(check_for_backend(crate::Db))]
pub struct ApprovalRow {
    pub id: i64,
    pub workflow_id: i64,
    pub command_sequence: i32,
    pub kind: String,
    pub version: i32,
    pub prompt_metadata_json: String,
    pub validation_schema_json: String,
    pub validation_version: i32,
    pub status: String,
    pub requested_at: i64,
    pub expires_at: Option<i64>,
    pub decision_payload_json: Option<String>,
    pub decided_by: Option<i32>,
    pub operator_reason: Option<String>,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_schedule_state)]
#[diesel(check_for_backend(crate::Db))]
pub struct ScheduleStateRow {
    pub schedule_key: String,
    pub definition_fingerprint: String,
    pub definition_version: i32,
    pub next_local_occurrence: String,
    pub next_occurrence_at: i64,
    pub last_materialized_at: Option<i64>,
    pub paused_at: Option<i64>,
    pub paused_by: Option<i32>,
    pub pause_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_schedule_run)]
#[diesel(check_for_backend(crate::Db))]
pub struct ScheduleRunRow {
    pub id: i64,
    pub schedule_key: String,
    pub local_occurrence: String,
    pub scheduled_for: i64,
    pub materialized_at: i64,
    pub status: String,
    pub reason: Option<String>,
    pub actor_id: Option<i32>,
    pub workflow_id: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = durable_topic_lock)]
#[diesel(check_for_backend(crate::Db))]
pub struct TopicLockRow {
    pub topic: String,
    pub max_concurrency: i32,
    pub updated_at: i64,
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
    pub wait_kind: Option<String>,
    pub wait_reference_id: Option<i64>,
    pub available_at: i64,
    pub activation_attempts: i32,
    pub max_activation_attempts: i32,
    pub consecutive_continuations: i32,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub deduplication_key: Option<String>,
    pub schedule_run_id: Option<i64>,
    pub root_workflow_id: Option<i64>,
    pub restarted_from_workflow_id: Option<i64>,
    pub parent_workflow_id: Option<i64>,
    pub parent_command_sequence: Option<i32>,
    pub command_sequence: i32,
    pub delivered_event_sequence: i32,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_workflow_event)]
pub struct NewWorkflowEventRow {
    pub workflow_id: i64,
    pub sequence: i32,
    pub delivery_sequence: Option<i32>,
    pub event_type: String,
    pub metadata_json: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_activity)]
pub struct NewActivityRow {
    pub workflow_id: i64,
    pub command_sequence: i32,
    pub replacement_number: i32,
    pub kind: String,
    pub version: i32,
    pub topic: String,
    pub payload_json: String,
    pub status: ActivityStatus,
    pub available_at: i64,
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
    pub lease_expires_at: Option<i64>,
    pub root_activity_id: Option<i64>,
    pub replaces_activity_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_activity_attempt)]
pub struct NewActivityAttemptRow {
    pub activity_id: i64,
    pub attempt_number: i32,
    pub worker_id: String,
    pub lease_token: String,
    pub started_at: i64,
    pub heartbeat_at: i64,
    pub finished_at: Option<i64>,
    pub outcome: Option<String>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub provider_result_json: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_progress_event)]
pub struct NewProgressEventRow {
    pub activity_id: i64,
    pub attempt_number: i32,
    pub sequence: i32,
    pub code: String,
    pub description: String,
    pub description_bytes: i32,
    pub completed_units: Option<i64>,
    pub total_units: Option<i64>,
    pub severity: String,
    pub metadata_json: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_approval)]
pub struct NewApprovalRow {
    pub workflow_id: i64,
    pub command_sequence: i32,
    pub kind: String,
    pub version: i32,
    pub prompt_metadata_json: String,
    pub validation_schema_json: String,
    pub validation_version: i32,
    pub status: String,
    pub requested_at: i64,
    pub expires_at: Option<i64>,
    pub decision_payload_json: Option<String>,
    pub decided_by: Option<i32>,
    pub operator_reason: Option<String>,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_schedule_state)]
pub struct NewScheduleStateRow {
    pub schedule_key: String,
    pub definition_fingerprint: String,
    pub definition_version: i32,
    pub next_local_occurrence: String,
    pub next_occurrence_at: i64,
    pub last_materialized_at: Option<i64>,
    pub paused_at: Option<i64>,
    pub paused_by: Option<i32>,
    pub pause_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_schedule_run)]
pub struct NewScheduleRunRow {
    pub schedule_key: String,
    pub local_occurrence: String,
    pub scheduled_for: i64,
    pub materialized_at: i64,
    pub status: String,
    pub reason: Option<String>,
    pub actor_id: Option<i32>,
    pub workflow_id: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = durable_topic_lock)]
pub struct NewTopicLockRow {
    pub topic: String,
    pub max_concurrency: i32,
    pub updated_at: i64,
}
