mod support;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    persistence::{ActivityAttemptRow, ActivityRow, NewActivityRow, ProgressEventRow, WorkflowRow},
    schema::{
        durable_activity, durable_activity_attempt, durable_progress_event, durable_workflow,
        durable_workflow_event,
    },
    ActivityContext, ActivityError, ActivityHandler, ActivityTopic, DurableActivity, DurableError,
    DurableStore, DurableWorkflow, ProgressEvent, ProgressReportOutcome, RetryPolicy, StartOptions,
    WorkerConfig, WorkflowContext, WorkflowError, WorkflowEvent, WorkflowHandler,
    WorkflowTransition,
};

#[derive(Clone, Copy)]
enum Topics {
    External,
    Other,
}

#[derive(Clone, Copy)]
struct LowerOtherCap;

impl ActivityTopic for LowerOtherCap {
    fn key(self) -> &'static str {
        "other"
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

impl ActivityTopic for Topics {
    fn key(self) -> &'static str {
        match self {
            Self::External => "external",
            Self::Other => "other",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::External => 1,
            Self::Other => 2,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TestActivity {
    value: i32,
}

impl DurableActivity for TestActivity {
    type Topic = Topics;

    const KIND: &'static str = "test_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        Topics::External
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[derive(Default)]
struct TestContext {
    mode: AtomicU8,
    cancellation_observed: AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    stale_progress_fenced: AtomicBool,
}

#[async_trait]
impl ActivityHandler for TestActivity {
    type Context = TestContext;
    type Output = serde_json::Value;

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        match AtomicU8::load(&context.application().mode, Ordering::SeqCst) {
            0 => Ok(serde_json::json!(self.value * 2)),
            1 => Err(ActivityError::retryable("provider_busy", "try again")),
            2 => Err(ActivityError::permanent("invalid_request", "do not retry")),
            3 => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(serde_json::json!(self.value))
            }
            4 => {
                let reporter = context.progress_reporter().expect("execution reporter");
                let mut persisted = 0;
                for index in 0..101 {
                    match reporter
                        .report(ProgressEvent::new("batch", format!("item {index}")))
                        .await
                        .expect("progress report")
                    {
                        ProgressReportOutcome::Persisted { .. } => persisted += 1,
                        ProgressReportOutcome::LimitReached => {}
                    }
                }
                Ok(serde_json::json!(persisted))
            }
            5 => {
                context.application().started.notify_one();
                let cancellation = context.cancellation_token().ok_or_else(|| {
                    ActivityError::permanent("context", "missing cancellation token")
                })?;
                cancellation.cancelled().await;
                context
                    .application()
                    .cancellation_observed
                    .store(true, Ordering::SeqCst);
                Err(ActivityError::retryable(
                    "cancelled",
                    "cooperatively stopped",
                ))
            }
            6 => {
                let reporter = context.progress_reporter().expect("execution reporter");
                let exact = "é".repeat(1_024);
                assert!(matches!(
                    reporter
                        .report(ProgressEvent::new("unicode", exact))
                        .await
                        .expect("bounded Unicode progress"),
                    ProgressReportOutcome::Persisted { sequence: 1 }
                ));
                let oversized = "é".repeat(1_025);
                assert!(matches!(
                    reporter
                        .report(ProgressEvent::new("unicode", oversized))
                        .await,
                    Err(DurableError::PayloadTooLarge {
                        actual_bytes: 2_050,
                        max_bytes: 2_048,
                        ..
                    })
                ));
                Ok(serde_json::json!(1))
            }
            7 => {
                context.application().started.notify_one();
                context.application().resume.notified().await;
                let result = context
                    .progress_reporter()
                    .expect("execution reporter")
                    .report(ProgressEvent::new("stale", "must be fenced"))
                    .await;
                context.application().stale_progress_fenced.store(
                    matches!(result, Err(DurableError::FencedWrite)),
                    Ordering::SeqCst,
                );
                Ok(serde_json::json!(1))
            }
            8 => Ok(serde_json::Value::String("\"".repeat(32_000))),
            _ => unreachable!("unsupported test mode"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TestActivityV2 {
    value: i32,
}

impl DurableActivity for TestActivityV2 {
    type Topic = Topics;

    const KIND: &'static str = TestActivity::KIND;
    const VERSION: i32 = 2;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        Topics::External
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for TestActivityV2 {
    type Context = TestContext;
    type Output = i32;

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        Ok(self.value * 3)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct HostWorkflow;

impl DurableWorkflow for HostWorkflow {
    const KIND: &'static str = "activity_host";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for HostWorkflow {
    type Context = ();
    type State = ();
    type Approval = ();
    type Output = ();

    fn initial_state(&self) -> Self::State {}

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        _state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError> {
        Ok(WorkflowTransition::Complete { output: () })
    }
}

fn worker(
    pool: durable_workflows::DurablePool,
    context: Arc<TestContext>,
    worker_id: &str,
) -> durable_workflows::ActivityWorker<TestContext> {
    worker_with_config(
        pool,
        context,
        worker_id,
        WorkerConfig {
            heartbeat_interval: Duration::from_millis(20),
            shutdown_grace: Duration::from_secs(1),
        },
    )
}

fn worker_with_config(
    pool: durable_workflows::DurablePool,
    context: Arc<TestContext>,
    worker_id: &str,
    config: WorkerConfig,
) -> durable_workflows::ActivityWorker<TestContext> {
    let activities = durable_workflows::register_durable_activities!(
        TestContext;
        TestActivity,
        TestActivityV2
    )
    .expect("activity registry is valid");
    let topics = durable_workflows::register_durable_topics!(Topics::External, Topics::Other)
        .expect("topic registry is valid");
    durable_workflows::ActivityWorker::new(
        pool,
        context,
        Arc::new(activities),
        Arc::new(topics),
        worker_id,
        config,
    )
    .expect("worker is valid")
}

async fn schedule_activity(
    pool: &durable_workflows::DurablePool,
    topic: &str,
    max_attempts: i32,
    timeout_millis: i64,
    lease_duration_millis: i64,
) -> (i64, i64) {
    let workflow_id = DurableStore::new(pool.clone())
        .start(&HostWorkflow, StartOptions::default())
        .await
        .expect("workflow start")
        .workflow_id
        .get();
    let now = durable_workflows::persistence::now_millis();
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_activity::table)
        .values(NewActivityRow {
            workflow_id,
            command_sequence: 1,
            replacement_number: 0,
            kind: TestActivity::KIND.to_string(),
            version: TestActivity::VERSION,
            topic: topic.to_string(),
            payload_json: serde_json::to_string(&TestActivity { value: 21 }).expect("payload"),
            status: durable_workflows::persistence::ActivityStatus::try_from("pending")
                .expect("valid fixture status"),
            available_at: now,
            max_attempts,
            attempt_count: 0,
            timeout_millis,
            lease_duration_millis,
            retry_policy_json: serde_json::to_string(&TestActivity::retry_policy())
                .expect("retry policy"),
            operation_key: Some(format!("activity-{workflow_id}")),
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
            completed_at: None,
        })
        .execute(&mut connection)
        .await
        .expect("activity insert");
    let activity_id = durable_activity::table
        .filter(durable_activity::workflow_id.eq(workflow_id))
        .select(durable_activity::id)
        .first::<i64>(&mut connection)
        .await
        .expect("activity id");
    diesel::update(durable_workflow::table.find(workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(activity_id)),
            durable_workflow::command_sequence.eq(1),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut connection)
        .await
        .expect("workflow wait update");
    (workflow_id, activity_id)
}

#[test]
fn worker_defaults_bound_heartbeats_and_shutdown() {
    let config = WorkerConfig::default();
    assert!(config.heartbeat_interval > Duration::ZERO);
    assert!(config.shutdown_grace > Duration::ZERO);
}

#[tokio::test]
async fn global_sweep_claims_a_bounded_batch_across_topics() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    schedule_activity(&pool, "other", 3, 5_000, 10_000).await;
    schedule_activity(&pool, "other", 3, 5_000, 10_000).await;
    let worker = worker(pool.clone(), Arc::new(TestContext::default()), "dispatcher");

    let capacities = [("external".to_string(), 1), ("other".to_string(), 2)]
        .into_iter()
        .collect();
    let claims = worker
        .claim_batch(3, &capacities)
        .await
        .expect("global sweep");
    assert_eq!(claims.len(), 3);
    assert_eq!(
        claims
            .iter()
            .filter(|claim| claim.topic() == "external")
            .count(),
        1
    );
    assert_eq!(
        claims
            .iter()
            .filter(|claim| claim.topic() == "other")
            .count(),
        2
    );

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn topic_limit_is_global_and_other_topics_remain_independent() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    schedule_activity(&pool, "other", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    let first = worker(pool.clone(), context.clone(), "worker-one");
    let second = worker(pool.clone(), context, "worker-two");

    let (external, other) = tokio::join!(first.claim_one("external"), second.claim_one("other"));
    let external = external.expect("first claim").expect("external work");
    assert!(other.expect("other claim").is_some());
    assert_eq!(external.attempt_number().expect("attempt"), 1);
    assert!(second
        .claim_one("external")
        .await
        .expect("second claim")
        .is_none());

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn two_workers_claim_distinct_rows_up_to_the_global_topic_limit() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    schedule_activity(&pool, "other", 3, 5_000, 10_000).await;
    schedule_activity(&pool, "other", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    let first = worker(pool.clone(), context.clone(), "fleet-one");
    let second = worker(pool.clone(), context.clone(), "fleet-two");
    let third = worker(pool.clone(), context, "fleet-three");

    let (left, right) = tokio::join!(first.claim_one("other"), second.claim_one("other"));
    let left = left.expect("first claim").expect("first activity");
    let right = right.expect("second claim").expect("second activity");
    assert_ne!(
        left.activity_id().expect("first id"),
        right.activity_id().expect("second id")
    );
    assert!(third
        .claim_one("other")
        .await
        .expect("third claim")
        .is_none());

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn old_worker_skips_activity_versions_it_cannot_execute() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, v2_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let (_, v1_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_activity::table.find(v2_id))
        .set(durable_activity::version.eq(2))
        .execute(&mut connection)
        .await
        .expect("upgrade first activity");
    drop(connection);

    let activities = durable_workflows::register_durable_activities!(TestContext; TestActivity)
        .expect("old activity registry");
    let topics = durable_workflows::register_durable_topics!(Topics::External, Topics::Other)
        .expect("topic registry");
    let old_worker = durable_workflows::ActivityWorker::new(
        pool.clone(),
        Arc::new(TestContext::default()),
        Arc::new(activities),
        Arc::new(topics),
        "old-worker",
        WorkerConfig::default(),
    )
    .expect("old worker");

    let claim = old_worker
        .claim_one("external")
        .await
        .expect("old claim")
        .expect("locally executable activity");
    assert_eq!(claim.activity_id().expect("claimed activity").get(), v1_id);

    let mut connection = pool.get().await.expect("connection");
    let untouched = durable_activity::table
        .find(v2_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("v2 activity");
    assert_eq!(untouched.status.as_str(), "pending");
    assert_eq!(untouched.attempt_count, 0);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn persisted_topic_cap_rejects_incompatible_worker_definition() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registered =
        durable_workflows::register_durable_topics!(Topics::Other).expect("registered topic");
    let mut connection = pool.get().await.expect("connection");
    registered
        .seed_locks(&mut connection)
        .await
        .expect("initial topic cap");

    let incompatible = durable_workflows::register_durable_topics!(LowerOtherCap)
        .expect("incompatible local topic registry");
    assert!(matches!(
        incompatible.seed_locks(&mut connection).await,
        Err(DurableError::InvalidDefinition(message))
            if message.contains("persisted concurrency limit 2")
                && message.contains("registered limit 1")
    ));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn activity_claim_eligibility_uses_database_time_when_process_clock_differs() {
    let Some(pool) = support::fresh_pool_with_max_size(1).await else {
        return;
    };
    let process_now = durable_workflows::persistence::now_millis();
    let database_seconds = process_now / 1_000 + 86_400;
    let database_now = database_seconds * 1_000;
    let mut connection = pool.get().await.expect("clock connection");
    support::freeze_database_clock(&mut connection, database_now).await;
    drop(connection);

    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let available_at = database_now - 1_000;
    assert!(available_at > durable_workflows::persistence::now_millis());
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::available_at.eq(available_at))
        .execute(&mut connection)
        .await
        .expect("schedule by database time");
    drop(connection);

    let claimed = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "database-clock-worker",
    )
    .claim_one("external")
    .await
    .expect("claim with database clock")
    .expect("database-due activity");
    assert_eq!(
        claimed.activity_id().expect("activity id").get(),
        activity_id
    );

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn success_finishes_attempt_and_wakes_workflow_with_typed_event() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let worker = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "success-worker",
    );

    assert_eq!(
        worker
            .run_one("external")
            .await
            .expect("execution")
            .map(|id| id.get()),
        Some(activity_id)
    );

    let mut connection = pool.get().await.expect("test connection");
    let activity = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("activity");
    let workflow = durable_workflow::table
        .find(workflow_id)
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow");
    let attempt = durable_activity_attempt::table
        .find((activity_id, 1))
        .select(ActivityAttemptRow::as_select())
        .first::<ActivityAttemptRow>(&mut connection)
        .await
        .expect("attempt");
    let event_json = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id))
        .filter(durable_workflow_event::event_type.eq("activity_succeeded"))
        .select(durable_workflow_event::metadata_json)
        .first::<Option<String>>(&mut connection)
        .await
        .expect("success event")
        .expect("metadata");
    let event: WorkflowEvent = serde_json::from_str(&event_json).expect("typed event");

    assert_eq!(activity.status.as_str(), "succeeded");
    assert_eq!(activity.provider_result_json.as_deref(), Some("42"));
    assert_eq!(attempt.outcome.as_deref(), Some("succeeded"));
    assert_eq!(workflow.status.as_str(), "ready");
    assert_eq!(workflow.wait_reference_id, None);
    assert!(matches!(
        event,
        WorkflowEvent::ActivitySucceeded {
            command_sequence: 1,
            ..
        }
    ));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn maximum_activity_output_fits_the_durable_workflow_event() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(8, Ordering::SeqCst);
    let worker = worker(pool.clone(), context, "large-output-worker");

    assert_eq!(
        worker
            .run_one("external")
            .await
            .expect("maximum bounded output commits")
            .map(|id| id.get()),
        Some(activity_id)
    );

    let mut connection = pool.get().await.expect("test connection");
    let metadata_bytes = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id))
        .filter(durable_workflow_event::event_type.eq("activity_succeeded"))
        .select(durable_workflow_event::metadata_json)
        .first::<Option<String>>(&mut connection)
        .await
        .expect("success event")
        .expect("event metadata")
        .len();
    assert!(metadata_bytes > 65_535);

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn retryable_failure_reschedules_with_backoff_and_consumes_attempt() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(1, Ordering::SeqCst);
    let worker = worker(pool.clone(), context, "retry-worker");
    let before = durable_workflows::persistence::now_millis();

    worker.run_one("external").await.expect("retry execution");

    let mut connection = pool.get().await.expect("test connection");
    let row = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("activity");
    assert_eq!(row.status.as_str(), "pending");
    assert_eq!(row.attempt_count, 1);
    assert!(row.available_at >= before + 900);
    assert_eq!(row.last_error_category.as_deref(), Some("provider_busy"));
    let retry_history = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id))
        .filter(durable_workflow_event::event_type.eq("activity_retry_scheduled"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("retry history");
    assert_eq!(retry_history, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn permanent_failure_dead_letters_and_blocks_the_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(2, Ordering::SeqCst);
    let worker = worker(pool.clone(), context, "permanent-worker");

    worker
        .run_one("external")
        .await
        .expect("permanent execution");

    let mut connection = pool.get().await.expect("test connection");
    let activity_status = durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("activity status");
    let workflow_status = durable_workflow::table
        .find(workflow_id)
        .select(durable_workflow::status)
        .first::<String>(&mut connection)
        .await
        .expect("workflow status");
    assert_eq!(activity_status, "dead_lettered");
    assert_eq!(workflow_status, "blocked");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn retry_exhaustion_dead_letters_and_blocks_the_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 1, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(1, Ordering::SeqCst);
    let worker = worker(pool.clone(), context, "exhaustion-worker");

    worker
        .run_one("external")
        .await
        .expect("exhaustion execution");

    let mut connection = pool.get().await.expect("test connection");
    let activity = durable_activity::table
        .find(activity_id)
        .select((durable_activity::status, durable_activity::attempt_count))
        .first::<(String, i32)>(&mut connection)
        .await
        .expect("activity");
    let workflow_status = durable_workflow::table
        .find(workflow_id)
        .select(durable_workflow::status)
        .first::<String>(&mut connection)
        .await
        .expect("workflow");
    assert_eq!(activity, ("dead_lettered".to_string(), 1));
    assert_eq!(workflow_status, "blocked");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn expired_lease_is_reconciled_and_reclaimed_as_the_next_attempt() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    let first = worker(pool.clone(), context.clone(), "expired-one");
    let second = worker(pool.clone(), context, "expired-two");
    first
        .claim_one("external")
        .await
        .expect("claim")
        .expect("work");
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_expires_at.eq(Some(0_i64)))
        .execute(&mut connection)
        .await
        .expect("expire lease");
    drop(connection);

    let recovery_started = durable_workflows::persistence::now_millis();
    assert!(second
        .claim_one("external")
        .await
        .expect("reconcile")
        .is_none());
    let mut connection = pool.get().await.expect("test connection");
    let (status, available_at) = durable_activity::table
        .find(activity_id)
        .select((durable_activity::status, durable_activity::available_at))
        .first::<(String, i64)>(&mut connection)
        .await
        .expect("reconciled activity");
    assert_eq!(status, "pending");
    assert!(available_at >= recovery_started + 900);
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::available_at.eq(durable_workflows::persistence::now_millis()))
        .execute(&mut connection)
        .await
        .expect("make recovered activity due");
    drop(connection);

    let reclaimed = second
        .claim_one("external")
        .await
        .expect("reclaim")
        .expect("work");
    assert_eq!(reclaimed.activity_id().expect("id").get(), activity_id);
    assert_eq!(reclaimed.attempt_number().expect("attempt"), 2);

    let mut connection = pool.get().await.expect("test connection");
    let first_outcome = durable_activity_attempt::table
        .find((activity_id, 1))
        .select(durable_activity_attempt::outcome)
        .first::<Option<String>>(&mut connection)
        .await
        .expect("first attempt");
    assert_eq!(first_outcome.as_deref(), Some("lease_expired"));
    let recovery_history = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id))
        .filter(durable_workflow_event::event_type.eq("activity_lease_expired"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("recovery history");
    assert_eq!(recovery_history, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn heartbeat_and_completion_are_fenced_by_attempt_and_token() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let worker = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "fenced-worker",
    );
    let claim = worker
        .claim_one("external")
        .await
        .expect("claim")
        .expect("work");
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_token.eq(Some("replacement-token".to_string())))
        .execute(&mut connection)
        .await
        .expect("replace token");
    drop(connection);

    assert!(matches!(
        worker.heartbeat(&claim).await,
        Err(DurableError::FencedWrite)
    ));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn timeout_is_retryable() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, timeout_id) = schedule_activity(&pool, "external", 3, 40, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(3, Ordering::SeqCst);
    let worker = worker(pool.clone(), context.clone(), "timeout-worker");
    worker.run_one("external").await.expect("timeout execution");
    let mut connection = pool.get().await.expect("test connection");
    let timeout_row = durable_activity::table
        .find(timeout_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("timeout activity");
    assert_eq!(timeout_row.status.as_str(), "pending");
    assert_eq!(timeout_row.last_error_category.as_deref(), Some("timeout"));

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn shutdown_cancels_cooperatively_within_the_grace_period() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(5, Ordering::SeqCst);
    let worker = Arc::new(worker(pool.clone(), context.clone(), "cancellation-worker"));
    let executing = worker.clone();
    let run = tokio::spawn(async move { executing.run_one("external").await });
    context.started.notified().await;

    worker.shutdown();
    run.await
        .expect("worker task")
        .expect("cancelled execution");

    assert!(AtomicBool::load(
        &context.cancellation_observed,
        Ordering::SeqCst
    ));
    let mut connection = pool.get().await.expect("test connection");
    let row = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("activity");
    assert_eq!(row.status.as_str(), "pending");
    assert_eq!(row.last_error_category.as_deref(), Some("cancelled"));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn progress_is_bounded_to_one_hundred_events() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(TestContext::default());
    let worker = worker(pool.clone(), context.clone(), "progress-worker");
    let (_, progress_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    context.mode.store(4, Ordering::SeqCst);
    worker
        .run_one("external")
        .await
        .expect("progress execution");

    let mut connection = pool.get().await.expect("test connection");
    let progress = durable_progress_event::table
        .filter(durable_progress_event::activity_id.eq(progress_id))
        .order(durable_progress_event::sequence.asc())
        .select(ProgressEventRow::as_select())
        .load::<ProgressEventRow>(&mut connection)
        .await
        .expect("progress events");
    assert_eq!(progress.len(), 100);
    assert_eq!(progress.as_slice().first().map(|row| row.sequence), Some(1));
    assert_eq!(progress.last().map(|row| row.sequence), Some(100));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn progress_description_bounds_are_utf8_safe_and_reject_before_insert() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(TestContext::default());
    context.mode.store(6, Ordering::SeqCst);
    let worker = worker(pool.clone(), context, "unicode-progress-worker");
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;

    worker
        .run_one("external")
        .await
        .expect("progress execution");

    let mut connection = pool.get().await.expect("test connection");
    let descriptions = durable_progress_event::table
        .filter(durable_progress_event::activity_id.eq(activity_id))
        .select((
            durable_progress_event::description,
            durable_progress_event::description_bytes,
        ))
        .load::<(String, i32)>(&mut connection)
        .await
        .expect("progress events");
    assert_eq!(descriptions.len(), 1);
    assert_eq!(descriptions[0].0.chars().count(), 1_024);
    assert_eq!(descriptions[0].1, 2_048);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn stale_lease_cannot_emit_progress_or_commit_a_result() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(TestContext::default());
    context.mode.store(7, Ordering::SeqCst);
    let worker = Arc::new(worker_with_config(
        pool.clone(),
        context.clone(),
        "stale-progress-worker",
        WorkerConfig {
            heartbeat_interval: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(1),
        },
    ));
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let executing = worker.clone();
    let run = tokio::spawn(async move { executing.run_one("external").await });
    context.started.notified().await;
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_token.eq(Some("replacement-token".to_string())))
        .execute(&mut connection)
        .await
        .expect("replace lease token");
    drop(connection);
    context.resume.notify_one();

    assert!(matches!(
        run.await.expect("worker task"),
        Err(DurableError::FencedWrite)
    ));
    assert!(AtomicBool::load(
        &context.stale_progress_fenced,
        Ordering::SeqCst
    ));
    let mut connection = pool.get().await.expect("test connection");
    let progress_count = durable_progress_event::table
        .filter(durable_progress_event::activity_id.eq(activity_id))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("progress count");
    assert_eq!(progress_count, 0);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn missing_exact_activity_version_is_skipped_without_consuming_attempt() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::version.eq(99))
        .execute(&mut connection)
        .await
        .expect("change version");
    drop(connection);
    let worker = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "missing-worker",
    );

    assert!(worker
        .claim_one("external")
        .await
        .expect("unknown versions are skipped")
        .is_none());

    let mut connection = pool.get().await.expect("test connection");
    let attempt_count = durable_activity::table
        .find(activity_id)
        .select(durable_activity::attempt_count)
        .first::<i32>(&mut connection)
        .await
        .expect("attempt count");
    assert_eq!(attempt_count, 0);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn retry_exhaustion_dead_letters_and_blocks_without_advancing_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "external", 1, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(1, Ordering::SeqCst);
    let worker = worker(pool.clone(), context, "exhaustion-worker");

    worker.run_one("external").await.expect("execution");

    let mut connection = pool.get().await.expect("test connection");
    let status = durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("activity status");
    let workflow = durable_workflow::table
        .find(workflow_id)
        .select((
            durable_workflow::status,
            durable_workflow::delivered_event_sequence,
        ))
        .first::<(String, i32)>(&mut connection)
        .await
        .expect("workflow status");
    assert_eq!(status, "dead_lettered");
    assert_eq!(workflow, ("blocked".to_string(), 1));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn exact_activity_version_selects_the_matching_handler() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set((
            durable_activity::version.eq(TestActivityV2::VERSION),
            durable_activity::payload_json
                .eq(serde_json::to_string(&TestActivityV2 { value: 21 }).expect("v2 payload")),
        ))
        .execute(&mut connection)
        .await
        .expect("persist v2 definition");
    drop(connection);
    let worker = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "version-worker",
    );

    worker.run_one("external").await.expect("v2 execution");

    let mut connection = pool.get().await.expect("test connection");
    let result = durable_activity::table
        .find(activity_id)
        .select(durable_activity::provider_result_json)
        .first::<Option<String>>(&mut connection)
        .await
        .expect("activity result");
    assert_eq!(result.as_deref(), Some("63"));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn progress_rejects_oversized_descriptions_and_stale_leases_before_insert() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let worker = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "progress-fence-worker",
    );
    let claim = worker
        .claim_one("external")
        .await
        .expect("claim")
        .expect("work");
    let reporter = worker.progress_reporter(&claim).expect("reporter");

    let oversized = reporter
        .report(ProgressEvent::new("oversized", "é".repeat(1_025)))
        .await;
    assert!(matches!(
        oversized,
        Err(DurableError::PayloadTooLarge {
            actual_bytes: 2_050,
            max_bytes: 2_048,
            ..
        })
    ));
    assert_eq!(
        reporter
            .report(ProgressEvent::new("accepted", "one checkpoint"))
            .await
            .expect("valid progress"),
        ProgressReportOutcome::Persisted { sequence: 1 }
    );

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_token.eq(Some("replacement-token".to_string())))
        .execute(&mut connection)
        .await
        .expect("replace lease");
    drop(connection);
    assert!(matches!(
        reporter
            .report(ProgressEvent::new("late", "must not persist"))
            .await,
        Err(DurableError::FencedWrite)
    ));

    let mut connection = pool.get().await.expect("test connection");
    let count = durable_progress_event::table
        .filter(durable_progress_event::activity_id.eq(activity_id))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("progress count");
    assert_eq!(count, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn heartbeat_updates_are_atomic_when_the_attempt_fence_is_stale() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let worker = worker(
        pool.clone(),
        Arc::new(TestContext::default()),
        "heartbeat-atomic-worker",
    );
    let claim = worker
        .claim_one("external")
        .await
        .expect("claim")
        .expect("work");
    let mut connection = pool.get().await.expect("test connection");
    let original_expiry = durable_activity::table
        .find(activity_id)
        .select(durable_activity::lease_expires_at)
        .first::<Option<i64>>(&mut connection)
        .await
        .expect("lease expiry");
    diesel::update(durable_activity_attempt::table.find((activity_id, 1)))
        .set(durable_activity_attempt::lease_token.eq("replacement-token"))
        .execute(&mut connection)
        .await
        .expect("replace attempt token");
    drop(connection);

    assert!(matches!(
        worker.heartbeat(&claim).await,
        Err(DurableError::FencedWrite)
    ));

    let mut connection = pool.get().await.expect("test connection");
    let final_expiry = durable_activity::table
        .find(activity_id)
        .select(durable_activity::lease_expires_at)
        .first::<Option<i64>>(&mut connection)
        .await
        .expect("lease expiry after failed heartbeat");
    assert_eq!(final_expiry, original_expiry);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn stale_completion_is_rejected_after_the_lease_changes() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(3, Ordering::SeqCst);
    let worker = Arc::new(worker_with_config(
        pool.clone(),
        context,
        "stale-result-worker",
        WorkerConfig {
            heartbeat_interval: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(1),
        },
    ));
    let running_worker = worker.clone();
    let execution = tokio::spawn(async move { running_worker.run_one("external").await });

    let mut running = false;
    for _ in 0..100 {
        let mut connection = pool.get().await.expect("test connection");
        running = durable_activity::table
            .find(activity_id)
            .select(durable_activity::status)
            .first::<String>(&mut connection)
            .await
            .expect("activity status")
            == "running";
        if running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(running, "activity became running");
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_token.eq(Some("replacement-token".to_string())))
        .execute(&mut connection)
        .await
        .expect("replace lease");
    drop(connection);

    let result = execution.await.expect("worker task");
    assert!(matches!(result, Err(DurableError::FencedWrite)));
    let mut connection = pool.get().await.expect("test connection");
    let status = durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("activity status");
    assert_eq!(status, "running");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn shutdown_cancellation_is_observed_before_the_attempt_is_rescheduled() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "external", 3, 5_000, 10_000).await;
    let context = Arc::new(TestContext::default());
    context.mode.store(5, Ordering::SeqCst);
    let worker = Arc::new(worker(pool.clone(), context.clone(), "cancellation-worker"));
    let running_worker = worker.clone();
    let execution = tokio::spawn(async move { running_worker.run_one("external").await });
    context.started.notified().await;

    worker.shutdown();
    execution
        .await
        .expect("worker task")
        .expect("cancelled execution settles");

    assert!(AtomicBool::load(
        &context.cancellation_observed,
        Ordering::SeqCst
    ));
    let mut connection = pool.get().await.expect("test connection");
    let status = durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("activity status");
    assert_eq!(status, "pending");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn single_and_batch_claims_share_the_attempt_and_lease_contract() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    for batch in [false, true] {
        let (_, activity_id) = schedule_activity(&pool, "other", 3, 5_000, 120_000).await;
        let worker = worker(
            pool.clone(),
            Arc::new(TestContext::default()),
            "contract-worker",
        );
        let claim = if batch {
            worker
                .claim_batch(1, &[("other".to_string(), 1)].into_iter().collect())
                .await
                .expect("batch claim")
                .pop()
                .expect("activity")
        } else {
            worker
                .claim_one("other")
                .await
                .expect("single claim")
                .expect("activity")
        };
        assert_eq!(claim.activity_id().expect("id").get(), activity_id);
        assert_eq!(claim.attempt_number().expect("attempt number"), 1);
        let mut connection = pool.get().await.expect("connection");
        let row = durable_activity::table
            .find(activity_id)
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(&mut connection)
            .await
            .expect("activity row");
        let attempt = durable_activity_attempt::table
            .filter(durable_activity_attempt::activity_id.eq(activity_id))
            .select(ActivityAttemptRow::as_select())
            .first::<ActivityAttemptRow>(&mut connection)
            .await
            .expect("attempt row");
        assert_eq!(row.status.as_str(), "running");
        assert_eq!(row.attempt_count, 1);
        assert_eq!(row.lease_owner.as_deref(), Some("contract-worker"));
        assert_eq!(row.lease_token.as_deref(), Some(claim.lease_token()));
        assert_eq!(attempt.lease_token, claim.lease_token());
        assert_eq!(attempt.worker_id, "contract-worker");
        assert_eq!(attempt.attempt_number, 1);
        assert_eq!(row.lease_expires_at, Some(attempt.started_at + 120_000));
        assert_eq!(attempt.heartbeat_at, attempt.started_at);
        assert!(attempt.finished_at.is_none());
    }
}
