mod support;

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    observability::{
        emit_schedule_materialization_alert, lease_fingerprint, HealthAlert, HealthScanner,
        HealthScannerConfig,
    },
    persistence::NewActivityRow,
    schema::{durable_activity, durable_workflow},
    ActivityContext, ActivityError, ActivityHandler, ActivityRegistry, ActivityTopic,
    ActivityWorker, CoordinatorConfig, DurableActivity, DurableError, DurableStore,
    DurableWorkflow, MisfirePolicy, OverlapPolicy, RetryPolicy, ScheduleDefinitionMetadata,
    StartOptions, TopicRegistry, WorkerConfig, WorkflowContext, WorkflowCoordinator, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowRegistry, WorkflowTransition,
};
use tracing_subscriber::fmt::{format::FmtSpan, MakeWriter};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SecretWorkflow {
    customer_secret: String,
}

impl DurableWorkflow for SecretWorkflow {
    const KIND: &'static str = "missing_observability_workflow";
    const VERSION: i32 = 7;
}

#[async_trait]
impl WorkflowHandler for SecretWorkflow {
    type Context = ();
    type State = String;
    type Approval = bool;
    type Output = String;

    fn initial_state(&self) -> Self::State {
        self.customer_secret.clone()
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
enum ObservabilityTopic {
    External,
}

impl ActivityTopic for ObservabilityTopic {
    fn key(self) -> &'static str {
        "observability_external"
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SecretActivity {
    customer_secret: String,
}

impl DurableActivity for SecretActivity {
    type Topic = ObservabilityTopic;

    const KIND: &'static str = "observability_activity";
    const VERSION: i32 = 4;
    const MAX_ATTEMPTS: u32 = 2;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        ObservabilityTopic::External
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("retry policy")
    }
}

#[async_trait]
impl ActivityHandler for SecretActivity {
    type Context = ();
    type Output = ();

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self.0.clone())
    }
}

impl io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LogBuffer {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().expect("log buffer").clone()).expect("UTF-8 logs")
    }
}

#[tokio::test]
async fn health_scan_classifies_bounded_identifier_only_alerts_and_redacts_payloads() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let stale_workflow = store
        .start(
            &SecretWorkflow {
                customer_secret: "customer-secret-workflow".to_string(),
            },
            StartOptions::default(),
        )
        .await
        .expect("stale workflow")
        .workflow_id;
    let exhausted_workflow = store
        .start(
            &SecretWorkflow {
                customer_secret: "customer-secret-exhausted".to_string(),
            },
            StartOptions::default(),
        )
        .await
        .expect("exhausted workflow")
        .workflow_id;
    let now = durable_workflows::persistence::now_millis();
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(stale_workflow.get()))
        .set((
            durable_workflow::status.eq("running"),
            durable_workflow::lease_owner.eq(Some("worker-secret".to_string())),
            durable_workflow::lease_token
                .eq(Some("00000000-0000-0000-0000-000000000999".to_string())),
            durable_workflow::lease_expires_at.eq(Some(now - 120_000)),
        ))
        .execute(&mut connection)
        .await
        .expect("stale workflow state");
    diesel::update(durable_workflow::table.find(exhausted_workflow.get()))
        .set((
            durable_workflow::status.eq("failed"),
            durable_workflow::error_category.eq(Some("activation".to_string())),
            durable_workflow::error_message.eq(Some("customer-secret-error".to_string())),
            durable_workflow::completed_at.eq(Some(now - 1)),
        ))
        .execute(&mut connection)
        .await
        .expect("exhausted workflow state");
    diesel::insert_into(durable_activity::table)
        .values([
            activity_row(stale_workflow.get(), 1, "running", now - 120_000),
            activity_row(exhausted_workflow.get(), 2, "dead_lettered", now - 1),
        ])
        .execute(&mut connection)
        .await
        .expect("activities");
    drop(connection);

    let scanner = HealthScanner::new(
        pool,
        Arc::new(WorkflowRegistry::<()>::new()),
        Arc::new(ActivityRegistry::<()>::new()),
        Arc::new(TopicRegistry::new()),
        HealthScannerConfig {
            stale_after: Duration::from_secs(60),
            max_alerts_per_kind: 10,
        },
    )
    .expect("scanner");
    let report = scanner.scan_once(now).await.expect("health scan");
    assert!(report.alerts.iter().any(|alert| matches!(
        alert,
        HealthAlert::MissingWorkflowDefinition { kind, version }
            if kind == SecretWorkflow::KIND && *version == SecretWorkflow::VERSION
    )));
    assert!(report.alerts.iter().any(|alert| matches!(
        alert,
        HealthAlert::ActivationExhausted { workflow_id, .. }
            if *workflow_id == exhausted_workflow
    )));
    assert!(report
        .alerts
        .iter()
        .any(|alert| matches!(alert, HealthAlert::ActivityDeadLettered { .. })));
    assert!(report.alerts.iter().any(|alert| matches!(
        alert,
        HealthAlert::StaleWorkflow { workflow_id, .. } if *workflow_id == stale_workflow
    )));
    assert!(report
        .alerts
        .iter()
        .any(|alert| matches!(alert, HealthAlert::StaleActivity { .. })));

    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_writer(buffer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    report.emit();
    let logs = buffer.contents();
    assert!(logs.contains("durable workflow health alert"));
    assert!(logs.contains(&stale_workflow.to_string()));
    for prohibited in [
        "customer-secret-workflow",
        "customer-secret-exhausted",
        "customer-secret-activity",
        "customer-secret-provider-result",
        "customer-secret-error",
        "00000000-0000-0000-0000-000000000999",
        "worker-secret",
    ] {
        assert!(!logs.contains(prohibited), "logs exposed {prohibited}");
    }
}

#[test]
fn lease_fingerprint_is_stable_bounded_and_does_not_expose_the_token() {
    let token = "00000000-0000-0000-0000-000000000999";
    let first = lease_fingerprint(token);
    assert_eq!(first, lease_fingerprint(token));
    assert_eq!(first.len(), 16);
    assert!(!first.is_empty());
    assert!(!token.contains(&first));
    assert!(!first.contains(token));
}

#[test]
fn schedule_alerts_classify_failures_without_logging_error_details() {
    let definition = ScheduleDefinitionMetadata {
        key: "safe_schedule_key".to_string(),
        version: 3,
        fingerprint: "fingerprint".to_string(),
        cron: "* * * * * *".to_string(),
        timezone: "UTC".to_string(),
        misfire: MisfirePolicy::RunLatest,
        overlap: OverlapPolicy::Allow,
        misfire_grace_millis: 1_000,
    };
    let errors = [
        DurableError::Conflict("customer-secret-drift".to_string()),
        DurableError::InvalidState(
            "schedule exceeds the per-tick occurrence scan bound: customer-secret-backlog"
                .to_string(),
        ),
        DurableError::InvalidState("customer-secret-failure".to_string()),
    ];
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_writer(buffer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    for error in &errors {
        emit_schedule_materialization_alert(&definition, error, 123);
    }
    let logs = buffer.contents();
    for expected in [
        "schedule_definition_drift",
        "schedule_backlog_bound_exhausted",
        "schedule_materialization_failed",
        "safe_schedule_key",
    ] {
        assert!(logs.contains(expected));
    }
    assert!(!logs.contains("customer-secret"));
    assert!(!logs.contains("fingerprint"));
}

#[tokio::test]
async fn activation_span_has_safe_correlation_fields_without_token_or_payload() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = DurableStore::new(pool.clone())
        .start(
            &SecretWorkflow {
                customer_secret: "customer-secret-span".to_string(),
            },
            StartOptions::default(),
        )
        .await
        .expect("workflow")
        .workflow_id;
    let mut workflows = WorkflowRegistry::new();
    workflows
        .register::<SecretWorkflow>()
        .expect("workflow definition");
    let coordinator = WorkflowCoordinator::new(
        pool,
        Arc::new(()),
        Arc::new(workflows),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "coordinator-safe",
        CoordinatorConfig::default(),
    )
    .expect("coordinator");
    let claim = coordinator
        .claim_one()
        .await
        .expect("claim")
        .expect("workflow claim");
    let raw_token = claim.lease_token().to_string();
    let fingerprint = lease_fingerprint(&raw_token);
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(buffer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    coordinator.activate_claim(claim).await.expect("activation");
    let logs = buffer.contents();
    assert!(logs.contains("durable.workflow.activation"));
    assert!(logs.contains(&workflow_id.to_string()));
    assert!(logs.contains(SecretWorkflow::KIND));
    assert!(logs.contains(&fingerprint));
    assert!(!logs.contains(&raw_token));
    assert!(!logs.contains("customer-secret-span"));
}

#[tokio::test]
async fn activity_span_has_safe_correlation_fields_without_token_or_payload() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = DurableStore::new(pool.clone())
        .start(
            &SecretWorkflow {
                customer_secret: "customer-secret-host".to_string(),
            },
            StartOptions::default(),
        )
        .await
        .expect("workflow")
        .workflow_id;
    let now = durable_workflows::persistence::now_millis();
    let mut row = activity_row(workflow_id.get(), 1, "pending", now);
    row.kind = SecretActivity::KIND.to_string();
    row.version = SecretActivity::VERSION;
    row.topic = ObservabilityTopic::External.key().to_string();
    row.payload_json = serde_json::to_string(&SecretActivity {
        customer_secret: "customer-secret-activity-span".to_string(),
    })
    .expect("activity JSON");
    row.max_attempts = 2;
    row.attempt_count = 0;
    row.timeout_millis = 5_000;
    row.lease_duration_millis = 10_000;
    row.lease_owner = None;
    row.lease_token = None;
    row.lease_expires_at = None;
    row.completed_at = None;
    let mut connection = pool.get().await.expect("connection");
    diesel::insert_into(durable_activity::table)
        .values(row)
        .execute(&mut connection)
        .await
        .expect("activity");
    let activity_id = durable_activity::table
        .select(durable_activity::id)
        .order(durable_activity::id.desc())
        .first::<i64>(&mut connection)
        .await
        .expect("activity ID");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(activity_id)),
            durable_workflow::command_sequence.eq(1),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut connection)
        .await
        .expect("workflow wait");
    drop(connection);

    let activities = durable_workflows::register_durable_activities!(() ; SecretActivity)
        .expect("activity registry");
    let topics = durable_workflows::register_durable_topics!(ObservabilityTopic::External)
        .expect("topic registry");
    let worker = ActivityWorker::new(
        pool,
        Arc::new(()),
        Arc::new(activities),
        Arc::new(topics),
        "observability-worker",
        WorkerConfig::default(),
    )
    .expect("worker");
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(buffer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    assert_eq!(
        worker
            .run_one(ObservabilityTopic::External.key())
            .await
            .expect("activity execution")
            .expect("activity claim")
            .get(),
        activity_id
    );
    let logs = buffer.contents();
    assert!(logs.contains("durable.activity.attempt"));
    assert!(logs.contains(&workflow_id.to_string()));
    assert!(logs.contains(&activity_id.to_string()));
    assert!(logs.contains(SecretActivity::KIND));
    assert!(logs.contains(ObservabilityTopic::External.key()));
    assert!(logs.contains("schedule_run_id="));
    assert!(logs.contains("lease_fingerprint="));
    assert!(!logs.contains("lease_token="));
    assert!(!logs.contains("customer-secret-activity-span"));
    assert!(!logs.contains("customer-secret-host"));
}

fn activity_row(
    workflow_id: i64,
    command_sequence: i32,
    status: &str,
    lease_expires_at: i64,
) -> NewActivityRow {
    NewActivityRow {
        workflow_id,
        command_sequence,
        replacement_number: 0,
        kind: "missing_observability_activity".to_string(),
        version: 3,
        topic: "missing_observability_topic".to_string(),
        payload_json: r#"{"customer":"customer-secret-activity"}"#.to_string(),
        status: durable_workflows::persistence::ActivityStatus::try_from(status)
            .expect("valid fixture status"),
        available_at: 0,
        max_attempts: 1,
        attempt_count: 1,
        timeout_millis: 1_000,
        lease_duration_millis: 2_000,
        retry_policy_json: serde_json::to_string(&RetryPolicy::fixed(1).expect("retry policy"))
            .expect("retry JSON"),
        operation_key: Some(format!("operation-{command_sequence}")),
        provider_result_json: (status == "dead_lettered")
            .then(|| r#"{"secret":"customer-secret-provider-result"}"#.to_string()),
        last_error_category: (status == "dead_lettered").then(|| "provider".to_string()),
        last_error_message: (status == "dead_lettered")
            .then(|| "customer-secret-error".to_string()),
        lease_owner: (status == "running").then(|| "worker-secret".to_string()),
        lease_token: (status == "running")
            .then(|| "00000000-0000-0000-0000-000000000998".to_string()),
        lease_expires_at: (status == "running").then_some(lease_expires_at),
        root_activity_id: None,
        replaces_activity_id: None,
        created_at: 1,
        updated_at: 1,
        completed_at: (status == "dead_lettered").then_some(lease_expires_at),
    }
}
