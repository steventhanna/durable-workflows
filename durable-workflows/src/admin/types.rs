use serde::{Deserialize, Serialize};

use crate::{
    ActivityId, ApprovalId, DurableError, MisfirePolicy, OverlapPolicy, ScheduleRunId, WorkflowId,
};

pub const MAX_ADMIN_PAGE_SIZE: u32 = 100;
pub const MAX_TIMELINE_PAGE_SIZE: u32 = 200;
const DEFAULT_ADMIN_PAGE_SIZE: u32 = 50;
const MAX_ACTOR_ID_BYTES: usize = 191;

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageRequest {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl PageRequest {
    pub fn bounded_limit(&self, maximum: u32) -> Result<u32, DurableError> {
        let limit = self.limit.unwrap_or(DEFAULT_ADMIN_PAGE_SIZE);
        if limit == 0 || limit > maximum {
            return Err(DurableError::InvalidDefinition(format!(
                "page limit must contain 1 to {maximum} entries"
            )));
        }
        Ok(limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operator {
    actor_id: String,
    reason: String,
}

impl Operator {
    pub fn new(
        actor_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self, DurableError> {
        let actor_id = actor_id.into();
        let reason = reason.into().trim().to_string();
        if actor_id.is_empty() || actor_id.len() > MAX_ACTOR_ID_BYTES {
            return Err(DurableError::InvalidDefinition(
                "operator actor ID must contain 1 to 191 bytes".to_string(),
            ));
        }
        if reason.is_empty() || reason.len() > crate::MAX_ERROR_REASON_BYTES {
            return Err(DurableError::InvalidDefinition(format!(
                "operator reason must contain 1 to {} bytes",
                crate::MAX_ERROR_REASON_BYTES
            )));
        }
        Ok(Self { actor_id, reason })
    }

    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct JsonFieldSummary {
    pub present: bool,
    pub bytes: usize,
    /// Parsed JSON body. Present on admin detail GETs so operators can inspect
    /// and correct payloads; omitted from list/timeline responses. May be any
    /// JSON value (object, array, string, number, boolean, or null).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<serde_json::Value>)]
    pub value: Option<serde_json::Value>,
}

impl JsonFieldSummary {
    pub fn from_required(value: &str) -> Self {
        Self {
            present: true,
            bytes: value.len(),
            value: None,
        }
    }

    pub fn from_optional(value: Option<&str>) -> Self {
        Self {
            present: value.is_some(),
            bytes: value.map_or(0, str::len),
            value: None,
        }
    }

    pub fn from_required_json(raw: &str) -> Result<Self, DurableError> {
        let value = serde_json::from_str(raw).map_err(|error| {
            DurableError::InvalidState(format!("persisted JSON is invalid: {error}"))
        })?;
        Ok(Self {
            present: true,
            bytes: raw.len(),
            value: Some(value),
        })
    }

    pub fn from_optional_json(raw: Option<&str>) -> Result<Self, DurableError> {
        match raw {
            Some(raw) => Self::from_required_json(raw),
            None => Ok(Self {
                present: false,
                bytes: 0,
                value: None,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

// utoipa 5 composes a derived generic schema by inlining `<T as PartialSchema>::schema()` into the
// `items` position, which drops the item type from the component graph. Building the page schema by
// hand keeps `items` a `$ref` to the named item component.
impl<T: utoipa::ToSchema> utoipa::__dev::ComposeSchema for AdminPage<T> {
    fn compose(
        _generics: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>,
    ) -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, Ref, SchemaType, Type};

        ObjectBuilder::new()
            .schema_type(Type::Object)
            .property(
                "items",
                ArrayBuilder::new().items(utoipa::openapi::RefOr::Ref(Ref::from_schema_name(
                    <T as utoipa::ToSchema>::name(),
                ))),
            )
            .required("items")
            .property(
                "nextCursor",
                ObjectBuilder::new().schema_type(SchemaType::Array(vec![Type::String, Type::Null])),
            )
            .into()
    }
}

impl<T: utoipa::ToSchema> utoipa::ToSchema for AdminPage<T> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!("AdminPage_{}", <T as utoipa::ToSchema>::name()))
    }

    fn schemas(
        schemas: &mut Vec<(
            String,
            utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        )>,
    ) {
        schemas.push((
            <T as utoipa::ToSchema>::name().to_string(),
            <T as utoipa::PartialSchema>::schema(),
        ));
        <T as utoipa::ToSchema>::schemas(schemas);
    }
}

pub type WorkflowSummaryPage = AdminPage<WorkflowSummary>;
pub type TimelineEntryPage = AdminPage<TimelineEntry>;
pub type ActivitySummaryPage = AdminPage<ActivitySummary>;
pub type ActivityAttemptSummaryPage = AdminPage<ActivityAttemptSummary>;
pub type ProgressSummaryPage = AdminPage<ProgressSummary>;
pub type TopicMetricsPage = AdminPage<TopicMetrics>;
pub type ScheduleSummaryPage = AdminPage<ScheduleSummary>;
pub type ScheduleRunSummaryPage = AdminPage<ScheduleRunSummary>;
pub type ApprovalSummaryPage = AdminPage<ApprovalSummary>;

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowListFilter {
    #[serde(flatten)]
    pub page: PageRequest,
    pub kind: Option<String>,
    pub version: Option<i32>,
    pub status: Option<String>,
    pub schedule_run_id: Option<ScheduleRunId>,
    pub root_workflow_id: Option<WorkflowId>,
    pub created_after: Option<i64>,
    pub created_before: Option<i64>,
    pub waiting_approval: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityListFilter {
    #[serde(flatten)]
    pub page: PageRequest,
    pub workflow_id: Option<WorkflowId>,
    pub kind: Option<String>,
    pub version: Option<i32>,
    pub topic: Option<String>,
    pub status: Option<String>,
    pub created_after: Option<i64>,
    pub created_before: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityDetailRequest {
    pub attempt_cursor: Option<String>,
    pub attempt_limit: Option<u32>,
    pub progress_cursor: Option<String>,
    pub progress_limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalListFilter {
    #[serde(flatten)]
    pub page: PageRequest,
    pub workflow_id: Option<WorkflowId>,
    pub kind: Option<String>,
    pub version: Option<i32>,
    pub status: Option<String>,
    pub requested_after: Option<i64>,
    pub requested_before: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HourlyThroughputBucket {
    pub starts_at: i64,
    pub completed_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetrics {
    pub topic: String,
    pub max_concurrency: u32,
    pub configured_max_concurrency: u32,
    pub configuration_mismatch: bool,
    pub active_count: u64,
    pub available_capacity: u32,
    pub ready_count: u64,
    pub retry_scheduled_count: u64,
    pub dead_letter_count: u64,
    pub oldest_ready_age_millis: Option<u64>,
    pub oldest_active_age_millis: Option<u64>,
    pub hourly_throughput: Vec<HourlyThroughputBucket>,
    pub captured_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleStateSummary {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ScheduleHealthIssue {
    MissingState,
    UnregisteredState,
    DefinitionMismatch {
        registered_version: i32,
        persisted_version: i32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRunSummary {
    pub id: ScheduleRunId,
    pub schedule_key: String,
    pub local_occurrence: String,
    pub scheduled_for: i64,
    pub materialized_at: i64,
    pub status: String,
    pub reason: Option<String>,
    pub actor_id: Option<i32>,
    pub workflow_id: Option<WorkflowId>,
    pub workflow_status: Option<String>,
    pub workflow_completed_at: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleSummary {
    pub key: String,
    pub registered_version: Option<i32>,
    pub registered_fingerprint: Option<String>,
    pub cron: Option<String>,
    pub timezone: Option<String>,
    pub misfire: Option<MisfirePolicy>,
    pub overlap: Option<OverlapPolicy>,
    pub misfire_grace_millis: Option<i64>,
    pub state: Option<ScheduleStateSummary>,
    pub active_overlap_count: u64,
    pub skipped_count: u64,
    pub coalesced_count: u64,
    pub last_run: Option<ScheduleRunSummary>,
    pub recent_runs: Vec<ScheduleRunSummary>,
    pub health_issues: Vec<ScheduleHealthIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowControlOutcome {
    pub workflow_id: WorkflowId,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRestartOutcome {
    pub source_workflow_id: WorkflowId,
    pub workflow_id: WorkflowId,
    pub kind: String,
    pub version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityRetryOutcome {
    pub source_activity_id: ActivityId,
    pub activity_id: ActivityId,
    pub kind: String,
    pub version: i32,
    pub operation_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalResolutionOutcome {
    pub approval_id: ApprovalId,
    pub workflow_id: WorkflowId,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleControlOutcome {
    pub schedule_key: String,
    pub paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRunNowOutcome {
    pub schedule_run_id: ScheduleRunId,
    pub workflow_id: WorkflowId,
    pub scheduled_for: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowSummary {
    pub id: WorkflowId,
    pub kind: String,
    pub version: i32,
    pub status: String,
    pub wait_kind: Option<String>,
    pub schedule_run_id: Option<ScheduleRunId>,
    pub root_workflow_id: Option<WorkflowId>,
    pub restarted_from_workflow_id: Option<WorkflowId>,
    pub input: JsonFieldSummary,
    pub state: JsonFieldSummary,
    pub result: JsonFieldSummary,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivitySummary {
    pub id: ActivityId,
    pub workflow_id: WorkflowId,
    pub kind: String,
    pub version: i32,
    pub topic: String,
    pub status: String,
    pub replacement_number: i32,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub operation_key: Option<String>,
    pub root_activity_id: Option<ActivityId>,
    pub replaces_activity_id: Option<ActivityId>,
    pub payload: JsonFieldSummary,
    pub provider_result: JsonFieldSummary,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub available_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityAttemptSummary {
    pub activity_id: ActivityId,
    pub attempt_number: i32,
    pub worker_id: String,
    pub started_at: i64,
    pub heartbeat_at: i64,
    pub finished_at: Option<i64>,
    pub outcome: Option<String>,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub provider_result: JsonFieldSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityDetail {
    pub activity: ActivitySummary,
    #[schema(value_type = ActivityAttemptSummaryPage)]
    pub attempts: AdminPage<ActivityAttemptSummary>,
    #[schema(value_type = ProgressSummaryPage)]
    pub progress: AdminPage<ProgressSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProgressSummary {
    pub activity_id: ActivityId,
    pub attempt_number: i32,
    pub sequence: i32,
    pub code: String,
    pub description: String,
    pub completed_units: Option<i64>,
    pub total_units: Option<i64>,
    pub severity: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalSummary {
    pub id: ApprovalId,
    pub workflow_id: WorkflowId,
    pub kind: String,
    pub version: i32,
    pub status: String,
    pub decision: JsonFieldSummary,
    pub decided_by: Option<i32>,
    pub operator_reason: Option<String>,
    pub requested_at: i64,
    pub expires_at: Option<i64>,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(tag = "type", content = "data", rename_all = "camelCase")]
pub enum TimelineEntry {
    WorkflowEvent {
        event_type: String,
        actor_type: Option<String>,
        actor_id: Option<String>,
        reason: Option<String>,
        occurred_at: i64,
    },
    Activity(ActivitySummary),
    ActivityAttempt(ActivityAttemptSummary),
    Progress(ProgressSummary),
    Approval(ApprovalSummary),
}
