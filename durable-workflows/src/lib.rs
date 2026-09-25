extern crate self as durable_workflows;

pub mod admin;
mod definition;
mod dialect;
mod error;
mod flow;
mod ids;
pub mod observability;
mod policy;
mod progress;
mod readiness;
mod registry;
mod runtime;
mod schedule;
mod store;
#[cfg(feature = "trace-model")]
#[doc(hidden)]
pub mod trace;
#[cfg(not(feature = "trace-model"))]
#[path = "trace/noop.rs"]
mod trace;
mod transition;

pub mod migrations;

#[doc(hidden)]
pub mod persistence;
pub mod schema;

#[cfg(feature = "mysql")]
pub type DurableConnection = diesel_async::AsyncMysqlConnection;
#[cfg(feature = "postgres")]
pub type DurableConnection = diesel_async::AsyncPgConnection;
pub type DurablePool = diesel_async::pooled_connection::bb8::Pool<DurableConnection>;

#[cfg(feature = "mysql")]
pub(crate) type Db = diesel::mysql::Mysql;
#[cfg(feature = "postgres")]
pub(crate) type Db = diesel::pg::Pg;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Mysql,
    Postgres,
}

#[cfg(feature = "mysql")]
pub const BACKEND: BackendKind = BackendKind::Mysql;
#[cfg(feature = "postgres")]
pub const BACKEND: BackendKind = BackendKind::Postgres;

pub use definition::{
    ActivityContext, ActivityHandler, ActivityTopic, DefinitionKey, DurableActivity,
    DurableWorkflow, WorkflowContext, WorkflowHandler,
};
pub use durable_workflows_macros::{
    durable_flow, DurableActivity, DurableSchedule, DurableWorkflow,
};

// Generated `#[durable_flow]` code references these re-exports so user crates
// do not need matching direct dependencies.
#[doc(hidden)]
pub use async_trait;
pub use error::{ActivityError, DurableError, WorkflowError};
pub use flow::{DurableFlow, FlowJournal, JournalEntry, WfCtx, WfError};
pub use ids::{ActivityId, ApprovalId, ScheduleRunId, WorkflowId};
pub use policy::{deterministic_jitter_percentile, BackoffPolicy, RetryPolicy};
pub use progress::{
    ProgressEvent, ProgressReportOutcome, ProgressReporter, ProgressSeverity,
    MAX_PROGRESS_DESCRIPTION_BYTES, MAX_PROGRESS_EVENTS_PER_ATTEMPT,
};
pub use readiness::ReadinessReport;
pub use registry::{
    ActivityDispatchError, ActivityRegistry, PreparedWorkflowStart, StoredTransition,
    TopicDefinition, TopicRegistry, WorkflowDispatchError, WorkflowRegistry,
};
pub use runtime::{
    ActivityClaim, ActivityWorker, ApprovalExpiryMaterializer, CoordinatorConfig, DurableRuntime,
    HealthAlertSink, RuntimeConfig, RuntimeHandle, RuntimeShutdownError, RuntimeTaskError,
    ScheduleMaterializationReport, ScheduleMaterializer, TimerMaterializer, WorkerConfig,
    WorkflowClaim, WorkflowCoordinator,
};
pub use schedule::{
    DurableSchedule, LocalTimeDisposition, MisfirePolicy, OverlapPolicy, ScheduleCalendar,
    ScheduleDefinitionMetadata, ScheduleHandler, ScheduleOccurrence, ScheduleRegistry,
    ScheduleStateReconcileOutcome,
};
#[doc(hidden)]
pub use serde;
pub use store::{DurableStore, StartOptions, StartOutcome};
pub use transition::{
    ActivityCommand, ActivityResult, ApprovalRequest, ApprovalResult, ChildResult,
    ChildWorkflowCommand, WorkflowEvent, WorkflowTransition,
};

pub const MAX_INPUT_STATE_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_EVENT_METADATA_BYTES: usize = 16 * 1024;
pub const MAX_ERROR_REASON_BYTES: usize = 2 * 1024;
