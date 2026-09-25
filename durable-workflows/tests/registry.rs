mod support;

use std::time::Duration;

use async_trait::async_trait;
use durable_workflows::{
    ActivityContext, ActivityHandler, ActivityTopic, DurableActivity, DurableWorkflow, RetryPolicy,
    WorkflowContext, WorkflowEvent, WorkflowHandler, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WorkflowV1 {
    value: i32,
}

impl DurableWorkflow for WorkflowV1 {
    const KIND: &'static str = "versioned_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for WorkflowV1 {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete {
            output: state + self.value,
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WorkflowV2 {
    value: i32,
}

impl DurableWorkflow for WorkflowV2 {
    const KIND: &'static str = "versioned_workflow";
    const VERSION: i32 = 2;
}

#[async_trait]
impl WorkflowHandler for WorkflowV2 {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value * 2
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete {
            output: state + self.value,
        })
    }
}

#[derive(Clone, Copy)]
enum Topics {
    External,
    ExternalConflict,
}

impl ActivityTopic for Topics {
    fn key(self) -> &'static str {
        match self {
            Self::External | Self::ExternalConflict => "external",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::External => 4,
            Self::ExternalConflict => 8,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ActivityV1 {
    value: i32,
}

impl DurableActivity for ActivityV1 {
    type Topic = Topics;

    const KIND: &'static str = "versioned_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(30);
    const LEASE_DURATION: Duration = Duration::from_secs(60);

    fn topic() -> Self::Topic {
        Topics::External
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(5).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for ActivityV1 {
    type Context = ();
    type Output = i32;

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, durable_workflows::ActivityError> {
        Ok(self.value * 2)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ActivityV2 {
    value: i32,
}

impl DurableActivity for ActivityV2 {
    type Topic = Topics;

    const KIND: &'static str = "versioned_activity";
    const VERSION: i32 = 2;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(30);
    const LEASE_DURATION: Duration = Duration::from_secs(60);

    fn topic() -> Self::Topic {
        Topics::External
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(5).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for ActivityV2 {
    type Context = ();
    type Output = i32;

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, durable_workflows::ActivityError> {
        Ok(self.value * 3)
    }
}

#[test]
fn exact_workflow_versions_coexist_and_duplicates_are_rejected() {
    let mut registry = durable_workflows::WorkflowRegistry::<()>::new();
    registry.register::<WorkflowV1>().expect("v1 registers");
    registry.register::<WorkflowV2>().expect("v2 registers");

    assert!(registry.contains("versioned_workflow", 1));
    assert!(registry.contains("versioned_workflow", 2));
    assert!(registry.register::<WorkflowV1>().is_err());
}

#[test]
fn exact_activity_versions_coexist_and_duplicates_are_rejected() {
    let mut registry = durable_workflows::ActivityRegistry::<()>::new();
    registry.register::<ActivityV1>().expect("v1 registers");
    registry.register::<ActivityV2>().expect("v2 registers");

    assert!(registry.contains("versioned_activity", 1));
    assert!(registry.contains("versioned_activity", 2));
    assert!(registry.register::<ActivityV1>().is_err());
}

#[test]
fn explicit_registration_macros_build_typed_registries() {
    let workflows = durable_workflows::register_durable_workflows!(
        ();
        WorkflowV1,
        WorkflowV2
    )
    .expect("workflow registry is valid");
    let activities = durable_workflows::register_durable_activities!(() ; ActivityV1)
        .expect("activity registry is valid");
    let topics = durable_workflows::register_durable_topics!(Topics::External)
        .expect("topic registry is valid");

    assert_eq!(workflows.len(), 2);
    assert!(activities.contains("versioned_activity", 1));
    assert_eq!(topics.get("external").unwrap().max_concurrency, 4);
}

#[test]
fn conflicting_topic_limits_are_rejected() {
    let mut topics = durable_workflows::TopicRegistry::new();
    topics
        .register(Topics::External)
        .expect("first topic registers");
    assert!(topics.register(Topics::External).is_ok());

    let error = topics
        .register(Topics::ExternalConflict)
        .expect_err("one topic key cannot have two global limits");
    assert!(error.to_string().contains("external"));
}

#[derive(Clone, Copy)]
struct StaticTopic(&'static str);

impl ActivityTopic for StaticTopic {
    fn key(self) -> &'static str {
        self.0
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

#[test]
fn topic_keys_must_be_canonical_ascii() {
    let mut topics = durable_workflows::TopicRegistry::new();
    topics
        .register(StaticTopic("cafe"))
        .expect("canonical topic registers");
    for key in ["café", "straße", "PROVIDER", "Strasse"] {
        let error = topics
            .register(StaticTopic(key))
            .expect_err("non-canonical keys cannot be registered");
        assert!(
            error.to_string().contains(key),
            "error should name the rejected key {key}: {error}"
        );
        assert!(
            error.to_string().contains("[a-z0-9][a-z0-9._-]*"),
            "error should describe the canonical alphabet: {error}"
        );
    }
}

#[test]
fn readiness_reports_every_missing_exact_version_deterministically() {
    let workflows =
        durable_workflows::register_durable_workflows!(() ; WorkflowV2).expect("registry is valid");
    let activities = durable_workflows::ActivityRegistry::<()>::new();

    let report = durable_workflows::ReadinessReport::compare(
        &workflows,
        &activities,
        vec![
            ("versioned_workflow".to_string(), 1),
            ("versioned_workflow".to_string(), 2),
        ],
        vec![("versioned_activity".to_string(), 1)],
    );

    assert_eq!(
        report.missing_workflows(),
        &[("versioned_workflow".to_string(), 1)]
    );
    assert_eq!(
        report.missing_activities(),
        &[("versioned_activity".to_string(), 1)]
    );
    assert!(report.ensure_ready().is_err());
}

#[tokio::test]
async fn type_erased_workflow_dispatch_rejects_invalid_stored_json() {
    let workflows =
        durable_workflows::register_durable_workflows!(() ; WorkflowV1).expect("registry is valid");

    let error = workflows
        .step_stored(
            "versioned_workflow",
            1,
            &(),
            None,
            "not-json",
            "1",
            WorkflowEvent::Started,
        )
        .await
        .expect_err("invalid immutable input must not reach workflow code");

    assert!(error.to_string().contains("serialization"));
}

#[tokio::test]
async fn type_erased_activity_dispatch_uses_the_exact_version() {
    let activities = durable_workflows::register_durable_activities!(
        ();
        ActivityV1,
        ActivityV2
    )
    .expect("registry is valid");

    let v1 = activities
        .execute_stored("versioned_activity", 1, &(), r#"{"value":4}"#)
        .await
        .expect("v1 executes");
    let v2 = activities
        .execute_stored("versioned_activity", 2, &(), r#"{"value":4}"#)
        .await
        .expect("v2 executes");

    assert_eq!(v1, "8");
    assert_eq!(v2, "12");
}

#[tokio::test]
async fn mysql_readiness_excludes_terminal_definitions() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{
        persistence::{NewActivityRow, NewWorkflowRow},
        schema::{durable_activity, durable_workflow},
    };

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let now = durable_workflows::persistence::now_millis();

    for (version, status, key) in [(1, "ready", "active"), (99, "succeeded", "terminal")] {
        diesel::insert_into(durable_workflow::table)
            .values(NewWorkflowRow {
                kind: "versioned_workflow".to_string(),
                version,
                input_json: r#"{"value":1}"#.to_string(),
                state_json: "1".to_string(),
                state_version: 1,
                status: durable_workflows::persistence::WorkflowStatus::try_from(status)
                    .expect("valid fixture status"),
                result_json: None,
                error_category: None,
                error_message: None,
                wait_kind: None,
                wait_reference_id: None,
                available_at: now,
                activation_attempts: 0,
                max_activation_attempts: 3,
                consecutive_continuations: 0,
                lease_owner: None,
                lease_token: None,
                lease_expires_at: None,
                deduplication_key: Some(key.to_string()),
                schedule_run_id: None,
                root_workflow_id: None,
                restarted_from_workflow_id: None,
                parent_workflow_id: None,
                parent_command_sequence: None,
                command_sequence: 0,
                delivered_event_sequence: 0,
                created_at: now,
                updated_at: now,
                completed_at: (status == "succeeded").then_some(now),
            })
            .execute(&mut connection)
            .await
            .expect("workflow fixture inserts");
    }

    let workflow_id = durable_workflow::table
        .filter(durable_workflow::deduplication_key.eq("active"))
        .select(durable_workflow::id)
        .first::<i64>(&mut connection)
        .await
        .expect("active workflow exists");

    for (version, status, command_sequence) in [(1, "pending", 1), (99, "succeeded", 2)] {
        diesel::insert_into(durable_activity::table)
            .values(NewActivityRow {
                workflow_id,
                command_sequence,
                replacement_number: 0,
                kind: "versioned_activity".to_string(),
                version,
                topic: "external".to_string(),
                payload_json: r#"{"value":1}"#.to_string(),
                status: durable_workflows::persistence::ActivityStatus::try_from(status)
                    .expect("valid fixture status"),
                available_at: now,
                max_attempts: 3,
                attempt_count: 0,
                timeout_millis: 30_000,
                lease_duration_millis: 60_000,
                retry_policy_json: r#"{"backoff":{"Fixed":{"delay_secs":5}}}"#.to_string(),
                operation_key: None,
                provider_result_json: None,
                last_error_category: None,
                last_error_message: None,
                lease_owner: None,
                lease_token: None,
                lease_expires_at: None,
                root_activity_id: None,
                replaces_activity_id: None,
                created_at: now,
                updated_at: now,
                completed_at: (status == "succeeded").then_some(now),
            })
            .execute(&mut connection)
            .await
            .expect("activity fixture inserts");
    }

    let workflows =
        durable_workflows::register_durable_workflows!(() ; WorkflowV1).expect("registry is valid");
    let activities = durable_workflows::register_durable_activities!(() ; ActivityV1)
        .expect("registry is valid");
    let topics = durable_workflows::register_durable_topics!(Topics::External)
        .expect("topic registry is valid");
    let ready = durable_workflows::ReadinessReport::query(
        &mut connection,
        &workflows,
        &activities,
        &topics,
    )
    .await
    .expect("readiness query succeeds");
    assert!(ready.ensure_ready().is_ok());

    let historical_activity_id = durable_activity::table
        .filter(durable_activity::version.eq(99))
        .select(durable_activity::id)
        .first::<i64>(&mut connection)
        .await
        .expect("historical activity");
    diesel::update(durable_activity::table.find(historical_activity_id))
        .set(durable_activity::status.eq("dead_lettered"))
        .execute(&mut connection)
        .await
        .expect("dead-letter historical activity");
    diesel::update(durable_workflow::table.find(workflow_id))
        .set((
            durable_workflow::status.eq("blocked"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(historical_activity_id)),
        ))
        .execute(&mut connection)
        .await
        .expect("block on historical activity");
    let blocked = durable_workflows::ReadinessReport::query(
        &mut connection,
        &workflows,
        &activities,
        &topics,
    )
    .await
    .expect("blocked readiness query succeeds");
    assert_eq!(
        blocked.missing_activities(),
        &[("versioned_activity".to_string(), 99)]
    );

    diesel::update(durable_workflow::table.find(workflow_id))
        .set(durable_workflow::status.eq("paused"))
        .execute(&mut connection)
        .await
        .expect("pause blocked workflow");
    let paused = durable_workflows::ReadinessReport::query(
        &mut connection,
        &workflows,
        &activities,
        &topics,
    )
    .await
    .expect("paused readiness query succeeds");
    assert_eq!(
        paused.missing_activities(),
        &[("versioned_activity".to_string(), 99)]
    );

    let empty_topics = durable_workflows::TopicRegistry::new();
    let missing = durable_workflows::ReadinessReport::query(
        &mut connection,
        &durable_workflows::WorkflowRegistry::<()>::new(),
        &durable_workflows::ActivityRegistry::<()>::new(),
        &empty_topics,
    )
    .await
    .expect("readiness query succeeds");
    assert_eq!(
        missing.missing_workflows(),
        &[("versioned_workflow".to_string(), 1)]
    );
    assert_eq!(
        missing.missing_activities(),
        &[
            ("versioned_activity".to_string(), 1),
            ("versioned_activity".to_string(), 99),
        ]
    );
    assert_eq!(missing.missing_topics(), &["external".to_string()]);

    support::drop_durable_tables(&mut connection).await;
}
