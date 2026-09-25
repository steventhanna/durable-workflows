use std::time::Duration;

use async_trait::async_trait;
use durable_workflows::{
    admin::ScheduleRegistry, ActivityContext, ActivityError, ActivityHandler, ActivityRegistry,
    ActivityTopic, DurableActivity, DurableError, DurableSchedule, DurableWorkflow, MisfirePolicy,
    OverlapPolicy, RetryPolicy, ScheduleHandler, ScheduleRunId, WorkflowContext, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowRegistry, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct VersionOneWorkflow {
    value: i32,
}

impl DurableWorkflow for VersionOneWorkflow {
    const KIND: &'static str = "registry_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for VersionOneWorkflow {
    type Context = ();
    type State = i32;
    type Approval = bool;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError> {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct VersionTwoWorkflow {
    value: i32,
    label: String,
}

impl DurableWorkflow for VersionTwoWorkflow {
    const KIND: &'static str = "registry_workflow";
    const VERSION: i32 = 2;
}

#[async_trait]
impl WorkflowHandler for VersionTwoWorkflow {
    type Context = ();
    type State = String;
    type Approval = String;
    type Output = String;

    fn initial_state(&self) -> Self::State {
        format!("{}:{}", self.label, self.value)
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError> {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

#[derive(Clone, Copy)]
enum TestTopic {
    External,
}

impl ActivityTopic for TestTopic {
    fn key(self) -> &'static str {
        "external"
    }

    fn max_concurrency(self) -> u32 {
        2
    }
}

macro_rules! activity_version {
    ($name:ident, $version:expr, $attempts:expr) => {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct $name {
            value: i32,
        }

        impl DurableActivity for $name {
            type Topic = TestTopic;

            const KIND: &'static str = "registry_activity";
            const VERSION: i32 = $version;
            const MAX_ATTEMPTS: u32 = $attempts;
            const TIMEOUT: Duration = Duration::from_secs(5);
            const LEASE_DURATION: Duration = Duration::from_secs(10);

            fn topic() -> Self::Topic {
                TestTopic::External
            }

            fn retry_policy() -> RetryPolicy {
                RetryPolicy::fixed(3).expect("test policy")
            }
        }

        #[async_trait]
        impl ActivityHandler for $name {
            type Context = ();
            type Output = i32;

            async fn execute(
                &self,
                _context: ActivityContext<'_, Self::Context>,
            ) -> Result<Self::Output, ActivityError> {
                Ok(self.value)
            }
        }
    };
}

activity_version!(VersionOneActivity, 1, 2);
activity_version!(VersionTwoActivity, 2, 4);

struct TestSchedule;

impl DurableSchedule for TestSchedule {
    const KEY: &'static str = "test_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "0 0 0 * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::Skip;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: Duration = Duration::from_secs(60);
}

#[async_trait]
impl ScheduleHandler for TestSchedule {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        _connection: &mut durable_workflows::DurableConnection,
        _schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        WorkflowId::new(1)
    }
}

#[test]
fn workflow_preparation_preserves_exact_versions_and_selects_current_explicitly() {
    let mut registry = WorkflowRegistry::new();
    registry
        .register::<VersionTwoWorkflow>()
        .expect("register v2");
    registry
        .register::<VersionOneWorkflow>()
        .expect("register v1");

    let historical = registry
        .prepare_start_exact("registry_workflow", 1, r#"{"value":7}"#)
        .expect("prepare historical input");
    assert_eq!(historical.version(), 1);
    assert_eq!(historical.input_json(), r#"{"value":7}"#);
    assert_eq!(historical.state_json(), "7");

    let current = registry
        .prepare_start_current("registry_workflow", r#"{"value":8,"label":"corrected"}"#)
        .expect("prepare current input");
    assert_eq!(current.version(), 2);
    assert_eq!(current.state_json(), r#""corrected:8""#);
    assert!(registry
        .prepare_start_current("registry_workflow", historical.input_json())
        .is_err());
}

#[test]
fn approval_validation_is_pinned_to_the_workflow_version() {
    let mut registry = WorkflowRegistry::new();
    registry
        .register::<VersionOneWorkflow>()
        .expect("register v1");
    registry
        .register::<VersionTwoWorkflow>()
        .expect("register v2");
    assert_eq!(
        registry
            .validate_approval_exact("registry_workflow", 1, "true")
            .expect("v1 approval"),
        "true"
    );
    assert!(registry
        .validate_approval_exact("registry_workflow", 2, "true")
        .is_err());
}

#[test]
fn activity_preparation_preserves_historical_policy_and_uses_current_policy_for_correction() {
    let mut registry = ActivityRegistry::new();
    registry
        .register::<VersionTwoActivity>()
        .expect("register v2");
    registry
        .register::<VersionOneActivity>()
        .expect("register v1");

    let historical = registry
        .prepare_command_exact(
            "registry_activity",
            1,
            r#"{"value":11}"#,
            Some("same-operation".to_string()),
        )
        .expect("historical command");
    assert_eq!(historical.version(), 1);
    assert_eq!(historical.max_attempts(), 2);
    assert_eq!(historical.operation_key(), Some("same-operation"));

    let current = registry
        .prepare_command_current(
            "registry_activity",
            r#"{"value":12}"#,
            Some("replacement-operation".to_string()),
        )
        .expect("current command");
    assert_eq!(current.version(), 2);
    assert_eq!(current.max_attempts(), 4);
    assert_eq!(current.topic(), "external");
    assert_eq!(current.payload_json(), r#"{"value":12}"#);
}

#[test]
fn schedule_registry_is_explicit_sorted_and_rejects_duplicates() {
    let mut registry = ScheduleRegistry::new();
    registry.register::<TestSchedule>().expect("schedule");
    let definitions = registry.definitions();
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].key, "test_schedule");
    assert!(matches!(
        registry.register::<TestSchedule>(),
        Err(DurableError::DuplicateDefinition { .. })
    ));
}
