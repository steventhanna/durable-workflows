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
use diesel_async::{AsyncConnection, RunQueryDsl};
use durable_workflows::{
    persistence::{ActivityRow, NewActivityRow},
    schema::{durable_activity, durable_workflow, durable_workflow_event},
    ActivityContext, ActivityError, ActivityHandler, ActivityRegistry, ActivityTopic,
    DurableActivity, DurableError, DurableRuntime, DurableStore, DurableWorkflow, RetryPolicy,
    RuntimeConfig, StartOptions, TopicRegistry, WorkflowContext, WorkflowError, WorkflowEvent,
    WorkflowHandler, WorkflowRegistry, WorkflowTransition,
};

#[derive(Clone, Copy)]
enum RuntimeTopic {
    Test,
    Continuation,
}

impl ActivityTopic for RuntimeTopic {
    fn key(self) -> &'static str {
        match self {
            Self::Test => "runtime_test",
            Self::Continuation => "runtime_continuation",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::Test => 1,
            Self::Continuation => 2,
        }
    }
}

#[derive(Default)]
struct RuntimeTestContext {
    mode: AtomicU8,
    started: tokio::sync::Notify,
    row_lock_acquired: tokio::sync::Notify,
    cancellation_observed: AtomicBool,
    blocked_started: tokio::sync::Notify,
    release_blocked: tokio::sync::Notify,
    blocked_completed: AtomicBool,
    panic_started: tokio::sync::Notify,
    probe_started: tokio::sync::Notify,
    pool: std::sync::Mutex<Option<durable_workflows::DurablePool>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RuntimeActivity;

impl DurableActivity for RuntimeActivity {
    type Topic = RuntimeTopic;

    const KIND: &'static str = "runtime_test_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(1);
    const LEASE_DURATION: Duration = Duration::from_secs(2);

    fn topic() -> Self::Topic {
        RuntimeTopic::Test
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test retry policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for RuntimeActivity {
    type Context = RuntimeTestContext;
    type Output = i32;

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        match AtomicU8::load(&context.application().mode, Ordering::SeqCst) {
            0 => Ok(1),
            1 => {
                context.application().started.notify_one();
                context
                    .cancellation_token()
                    .ok_or_else(|| ActivityError::permanent("context", "missing cancellation"))?
                    .cancelled()
                    .await;
                context
                    .application()
                    .cancellation_observed
                    .store(true, Ordering::SeqCst);
                Err(ActivityError::retryable("cancelled", "shutdown"))
            }
            2 => {
                context.application().started.notify_one();
                std::future::pending::<()>().await;
                Ok(0)
            }
            3 => {
                context.application().started.notify_one();
                panic!("intentional runtime supervision panic")
            }
            4 => {
                let pool = context
                    .application()
                    .pool
                    .lock()
                    .expect("test context pool mutex")
                    .clone()
                    .ok_or_else(|| ActivityError::permanent("test", "missing pool"))?;
                let activity_id = context
                    .activity_id()
                    .ok_or_else(|| ActivityError::permanent("test", "missing activity id"))?
                    .get();
                let application = context.application();
                let mut connection = pool
                    .get()
                    .await
                    .map_err(|error| ActivityError::retryable("database", error.to_string()))?;
                let result: Result<(), DurableError> = connection
                    .transaction(async move |connection| {
                        diesel::update(durable_activity::table.find(activity_id))
                            .set(
                                durable_activity::updated_at
                                    .eq(durable_workflows::persistence::now_millis()),
                            )
                            .execute(connection)
                            .await?;
                        application.row_lock_acquired.notify_one();
                        tokio::time::sleep(Duration::from_millis(75)).await;
                        Ok(())
                    })
                    .await;
                result.map_err(|error| ActivityError::retryable("database", error.to_string()))?;
                Ok(1)
            }
            5 => {
                context.application().blocked_started.notify_one();
                context.application().release_blocked.notified().await;
                context
                    .application()
                    .blocked_completed
                    .store(true, Ordering::SeqCst);
                Ok(1)
            }
            _ => Err(ActivityError::permanent("test", "unknown mode")),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum ContinuationBehavior {
    Panic,
    Probe,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RuntimeContinuationActivity {
    behavior: ContinuationBehavior,
}

impl DurableActivity for RuntimeContinuationActivity {
    type Topic = RuntimeTopic;

    const KIND: &'static str = "runtime_continuation_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 1;
    const TIMEOUT: Duration = Duration::from_secs(10);
    const LEASE_DURATION: Duration = Duration::from_secs(11);

    fn topic() -> Self::Topic {
        RuntimeTopic::Continuation
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test retry policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for RuntimeContinuationActivity {
    type Context = RuntimeTestContext;
    type Output = i32;

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        match self.behavior {
            ContinuationBehavior::Panic => {
                context.application().panic_started.notify_one();
                panic!("intentional continuation panic")
            }
            ContinuationBehavior::Probe => {
                context.application().probe_started.notify_one();
                Ok(1)
            }
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RuntimeWorkflow;

impl DurableWorkflow for RuntimeWorkflow {
    const KIND: &'static str = "runtime_test_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for RuntimeWorkflow {
    type Context = RuntimeTestContext;
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

fn registries() -> (
    Arc<WorkflowRegistry<RuntimeTestContext>>,
    Arc<ActivityRegistry<RuntimeTestContext>>,
    Arc<TopicRegistry>,
) {
    let workflows = durable_workflows::register_durable_workflows!(
        RuntimeTestContext;
        RuntimeWorkflow
    )
    .expect("workflow registry");
    let activities = durable_workflows::register_durable_activities!(
        RuntimeTestContext;
        RuntimeActivity,
        RuntimeContinuationActivity
    )
    .expect("activity registry");
    let topics =
        durable_workflows::register_durable_topics!(RuntimeTopic::Test, RuntimeTopic::Continuation)
            .expect("topic registry");
    (Arc::new(workflows), Arc::new(activities), Arc::new(topics))
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        idle_delay: Duration::from_millis(5),
        restart_backoff: Duration::from_millis(10),
        max_task_restarts: 2,
        max_workers_per_topic: 1,
        worker: durable_workflows::WorkerConfig {
            heartbeat_interval: Duration::from_millis(10),
            shutdown_grace: Duration::from_millis(100),
        },
        ..RuntimeConfig::default()
    }
}

fn runtime(
    pool: durable_workflows::DurablePool,
    context: Arc<RuntimeTestContext>,
    id: &str,
    config: RuntimeConfig,
) -> DurableRuntime<RuntimeTestContext> {
    let (workflows, activities, topics) = registries();
    DurableRuntime::new(pool, context, workflows, activities, topics, id, config)
        .expect("runtime definition")
}

async fn schedule_activity(
    pool: &durable_workflows::DurablePool,
    max_attempts: i32,
    timeout_millis: i64,
    lease_duration_millis: i64,
) -> (i64, i64) {
    schedule_activity_payload(
        pool,
        RuntimeActivity::KIND,
        RuntimeActivity::VERSION,
        serde_json::to_string(&RuntimeActivity).expect("payload"),
        RuntimeTopic::Test,
        max_attempts,
        timeout_millis,
        lease_duration_millis,
    )
    .await
}

async fn schedule_continuation_activity(
    pool: &durable_workflows::DurablePool,
    behavior: ContinuationBehavior,
) -> (i64, i64) {
    let activity = RuntimeContinuationActivity { behavior };
    schedule_activity_payload(
        pool,
        RuntimeContinuationActivity::KIND,
        RuntimeContinuationActivity::VERSION,
        serde_json::to_string(&activity).expect("payload"),
        RuntimeTopic::Continuation,
        RuntimeContinuationActivity::MAX_ATTEMPTS as i32,
        RuntimeContinuationActivity::TIMEOUT.as_millis() as i64,
        RuntimeContinuationActivity::LEASE_DURATION.as_millis() as i64,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn schedule_activity_payload(
    pool: &durable_workflows::DurablePool,
    kind: &str,
    version: i32,
    payload_json: String,
    topic: RuntimeTopic,
    max_attempts: i32,
    timeout_millis: i64,
    lease_duration_millis: i64,
) -> (i64, i64) {
    let workflow_id = DurableStore::new(pool.clone())
        .start(&RuntimeWorkflow, StartOptions::default())
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
            kind: kind.to_string(),
            version,
            topic: topic.key().to_string(),
            payload_json,
            status: durable_workflows::persistence::ActivityStatus::try_from("pending")
                .expect("valid fixture status"),
            available_at: now,
            max_attempts,
            attempt_count: 0,
            timeout_millis,
            lease_duration_millis,
            retry_policy_json: serde_json::to_string(&RuntimeActivity::retry_policy())
                .expect("retry policy"),
            operation_key: Some(format!("runtime-{workflow_id}")),
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

async fn wait_for_activity_status(
    pool: &durable_workflows::DurablePool,
    activity_id: i64,
    expected: &str,
) -> ActivityRow {
    for _ in 0..400 {
        let mut connection = pool.get().await.expect("test connection");
        let row = durable_activity::table
            .find(activity_id)
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(&mut connection)
            .await
            .expect("activity row");
        if row.status.as_str() == expected {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("activity {activity_id} did not reach {expected}")
}

#[test]
fn runtime_defaults_bound_polling_restarts_and_local_workers() {
    let config = RuntimeConfig::default();

    assert_eq!(config.idle_delay, Duration::from_secs(2));
    assert_eq!(config.timer_poll_interval, Duration::from_secs(10));
    assert!(config.restart_backoff > Duration::ZERO);
    assert!(config.forced_shutdown_timeout > Duration::ZERO);
    assert!(config.max_task_restarts > 0);
    assert!(config.max_workers_per_topic > 0);
    assert!(config.health_scan_interval > Duration::ZERO);
    assert!(config.health_stale_after > Duration::ZERO);
    assert!(config.max_health_alerts_per_kind > 0);
    assert_eq!(
        config.approval_expiry_poll_interval,
        Duration::from_secs(10)
    );
    assert_eq!(config.schedule_poll_interval, Duration::from_secs(10));
}

#[tokio::test]
async fn empty_runtime_starts_ready_and_shuts_down_without_detached_tasks() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(RuntimeTestContext::default());
    assert!(runtime(
        pool.clone(),
        context.clone(),
        "invalid-topic",
        runtime_config()
    )
    .with_topic_worker_limit("not-registered", 2)
    .is_err());
    assert!(
        runtime(pool.clone(), context, "invalid-limit", runtime_config())
            .with_topic_worker_limit(RuntimeTopic::Test.key(), 0)
            .is_err()
    );
    let invalid_config = RuntimeConfig {
        timer_poll_interval: Duration::from_secs(61),
        ..RuntimeConfig::default()
    };
    assert!(matches!(
        DurableRuntime::new(
            pool.clone(),
            Arc::new(()),
            Arc::new(WorkflowRegistry::new()),
            Arc::new(ActivityRegistry::new()),
            Arc::new(TopicRegistry::new()),
            "invalid-runtime",
            invalid_config,
        ),
        Err(DurableError::InvalidDefinition(_))
    ));
    let runtime = DurableRuntime::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(WorkflowRegistry::new()),
        Arc::new(ActivityRegistry::new()),
        Arc::new(TopicRegistry::new()),
        "runtime-test",
        RuntimeConfig::default(),
    )
    .expect("runtime definition");

    let handle = runtime.spawn().await.expect("ready runtime");
    handle
        .shutdown(Duration::from_secs(2))
        .await
        .expect("clean shutdown");

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn runtime_readiness_rejects_registered_activity_with_unregistered_topic() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflows = durable_workflows::register_durable_workflows!(
        RuntimeTestContext;
        RuntimeWorkflow
    )
    .expect("workflow registry");
    let activities = durable_workflows::register_durable_activities!(
        RuntimeTestContext;
        RuntimeActivity
    )
    .expect("activity registry");
    let runtime = DurableRuntime::new(
        pool.clone(),
        Arc::new(RuntimeTestContext::default()),
        Arc::new(workflows),
        Arc::new(activities),
        Arc::new(TopicRegistry::new()),
        "missing-topic-runtime",
        runtime_config(),
    )
    .expect("runtime definition");

    match runtime.spawn().await {
        Ok(handle) => {
            let _ = handle.shutdown(Duration::from_secs(1)).await;
            panic!("runtime must reject a registered activity whose topic has no worker");
        }
        Err(error) => assert!(error.to_string().contains("runtime_test")),
    }

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn readiness_fails_before_any_claim_when_a_live_version_is_unregistered() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let outcome = DurableStore::new(pool.clone())
        .start(&RuntimeWorkflow, StartOptions::default())
        .await
        .expect("workflow start");
    let runtime = DurableRuntime::new(
        pool.clone(),
        Arc::new(RuntimeTestContext::default()),
        Arc::new(WorkflowRegistry::new()),
        Arc::new(ActivityRegistry::new()),
        Arc::new(TopicRegistry::new()),
        "unready-runtime",
        runtime_config(),
    )
    .expect("runtime definition");

    assert!(matches!(
        runtime.spawn().await,
        Err(DurableError::MissingDefinitions { .. })
    ));

    let mut connection = pool.get().await.expect("test connection");
    let lease = durable_workflow::table
        .find(outcome.workflow_id.get())
        .select((durable_workflow::status, durable_workflow::lease_token))
        .first::<(String, Option<String>)>(&mut connection)
        .await
        .expect("workflow");
    assert_eq!(lease, ("ready".to_string(), None));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn shutdown_stops_claims_and_gives_active_handlers_a_cooperative_grace() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(RuntimeTestContext::default());
    context.mode.store(1, Ordering::SeqCst);
    let (_, activity_id) = schedule_activity(&pool, 3, 1_000, 2_000).await;
    let handle = runtime(
        pool.clone(),
        context.clone(),
        "cooperative",
        runtime_config(),
    )
    .spawn()
    .await
    .expect("runtime ready");
    context.started.notified().await;

    handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect("clean cooperative shutdown");

    assert!(AtomicBool::load(
        &context.cancellation_observed,
        Ordering::SeqCst
    ));
    let row = wait_for_activity_status(&pool, activity_id, "pending").await;
    assert_eq!(row.attempt_count, 1);
    assert_eq!(row.lease_token, None);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut connection = pool.get().await.expect("test connection");
    let attempts = durable_activity::table
        .find(activity_id)
        .select(durable_activity::attempt_count)
        .first::<i32>(&mut connection)
        .await
        .expect("attempt count");
    assert_eq!(attempts, 1, "shutdown runtime made no new claim");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn bounded_shutdown_leaves_a_lease_for_a_second_runtime_to_recover() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let first_context = Arc::new(RuntimeTestContext::default());
    first_context.mode.store(2, Ordering::SeqCst);
    let (_, activity_id) = schedule_activity(&pool, 3, 1_000, 2_000).await;
    let first = runtime(
        pool.clone(),
        first_context.clone(),
        "first",
        runtime_config(),
    )
    .spawn()
    .await
    .expect("first runtime ready");
    first_context.started.notified().await;

    let shutdown = first.shutdown(Duration::from_millis(20)).await;
    assert!(
        shutdown.is_err(),
        "non-cooperative handler exceeded drain deadline"
    );
    let running = wait_for_activity_status(&pool, activity_id, "running").await;
    assert_eq!(running.attempt_count, 1);
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_expires_at.eq(Some(0_i64)))
        .execute(&mut connection)
        .await
        .expect("expire abandoned lease");
    drop(connection);

    let second_context = Arc::new(RuntimeTestContext::default());
    let second = runtime(pool.clone(), second_context, "second", runtime_config())
        .spawn()
        .await
        .expect("second runtime ready");
    let pending = wait_for_activity_status(&pool, activity_id, "pending").await;
    assert_eq!(pending.attempt_count, 1);
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::available_at.eq(durable_workflows::persistence::now_millis()))
        .execute(&mut connection)
        .await
        .expect("make recovered work due");
    drop(connection);
    let succeeded = wait_for_activity_status(&pool, activity_id, "succeeded").await;
    assert_eq!(succeeded.attempt_count, 2);
    second
        .shutdown(Duration::from_secs(1))
        .await
        .expect("second runtime shutdown");

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn worker_panics_are_retained_restarted_and_reported_on_shutdown() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(RuntimeTestContext::default());
    context.mode.store(3, Ordering::SeqCst);
    let (_, activity_id) = schedule_activity(&pool, 3, 1_000, 2_000).await;
    let handle = runtime(pool.clone(), context.clone(), "panic", runtime_config())
        .spawn()
        .await
        .expect("runtime ready");
    context.started.notified().await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    let error = handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect_err("panic remains visible");
    assert!(error
        .errors
        .iter()
        .any(|error| error.panicked && error.task == "activity-dispatcher"));
    let row = wait_for_activity_status(&pool, activity_id, "running").await;
    assert_eq!(row.attempt_count, 1);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn activity_dispatcher_restarts_without_waiting_for_blocked_sibling() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(RuntimeTestContext::default());
    context.mode.store(5, Ordering::SeqCst);
    let (_, blocked_id) = schedule_activity(&pool, 1, 10_000, 11_000).await;
    let mut config = runtime_config();
    config.max_workers_per_topic = 2;
    let handle = runtime(pool.clone(), context.clone(), "continuation", config)
        .spawn()
        .await
        .expect("runtime ready");
    context.blocked_started.notified().await;

    let (_, panic_id) = schedule_continuation_activity(&pool, ContinuationBehavior::Panic).await;
    context.panic_started.notified().await;
    let (_, probe_id) = schedule_continuation_activity(&pool, ContinuationBehavior::Probe).await;

    // A restart may sweep before the probe is inserted, then use the normal
    // one-second dispatcher backoff; leave time for the following database claim.
    tokio::time::timeout(Duration::from_secs(5), context.probe_started.notified())
        .await
        .expect("restarted dispatcher claims before sibling completes");
    assert!(!AtomicBool::load(
        &context.blocked_completed,
        Ordering::SeqCst
    ));

    context.release_blocked.notify_one();
    wait_for_activity_status(&pool, blocked_id, "succeeded").await;
    wait_for_activity_status(&pool, probe_id, "succeeded").await;

    let error = handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect_err("panic remains visible");
    assert!(error
        .errors
        .iter()
        .any(|error| error.panicked && error.task == "activity-dispatcher"));
    let panic_row = wait_for_activity_status(&pool, panic_id, "running").await;
    assert_eq!(panic_row.attempt_count, 1);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn readiness_failure_happens_before_any_workflow_claim() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let started = DurableStore::new(pool.clone())
        .start(&RuntimeWorkflow, StartOptions::default())
        .await
        .expect("workflow start");
    let runtime = DurableRuntime::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(WorkflowRegistry::new()),
        Arc::new(ActivityRegistry::new()),
        Arc::new(TopicRegistry::new()),
        "missing-runtime",
        RuntimeConfig::default(),
    )
    .expect("runtime definition");

    assert!(matches!(
        runtime.spawn().await,
        Err(DurableError::MissingDefinitions { .. })
    ));
    let mut connection = pool.get().await.expect("test connection");
    let status = durable_workflow::table
        .find(started.workflow_id.get())
        .select(durable_workflow::status)
        .first::<String>(&mut connection)
        .await
        .expect("workflow status");
    assert_eq!(status, "ready");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn cancellation_prevents_new_claims() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(RuntimeTestContext::default());
    let handle = runtime(pool.clone(), context, "claim-stop", runtime_config())
        .spawn()
        .await
        .expect("runtime starts");
    handle.cancellation_token().cancel();
    let started = DurableStore::new(pool.clone())
        .start(&RuntimeWorkflow, StartOptions::default())
        .await
        .expect("workflow start");
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut connection = pool.get().await.expect("test connection");
    let status = durable_workflow::table
        .find(started.workflow_id.get())
        .select(durable_workflow::status)
        .first::<String>(&mut connection)
        .await
        .expect("workflow status");
    assert_eq!(status, "ready");
    drop(connection);
    handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect("runtime shutdown");
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn shutdown_requests_cooperative_cancellation_and_drains_the_attempt() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, 3, 1_000, 2_000).await;
    let context = Arc::new(RuntimeTestContext::default());
    context.mode.store(1, Ordering::SeqCst);
    let handle = runtime(
        pool.clone(),
        context.clone(),
        "cooperative",
        runtime_config(),
    )
    .spawn()
    .await
    .expect("runtime starts");
    context.started.notified().await;

    handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect("runtime drains");

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
        .expect("activity row");
    assert_eq!(row.status.as_str(), "pending");
    assert_eq!(row.last_error_category.as_deref(), Some("cancelled"));
    assert_eq!(row.lease_token, None);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn bounded_shutdown_stops_heartbeats_and_a_second_runtime_recovers() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, 3, 1_000, 2_000).await;
    let first_context = Arc::new(RuntimeTestContext::default());
    first_context.mode.store(2, Ordering::SeqCst);
    let mut first_config = runtime_config();
    first_config.worker.shutdown_grace = Duration::from_secs(5);
    let first = runtime(
        pool.clone(),
        first_context.clone(),
        "uncooperative",
        first_config,
    )
    .spawn()
    .await
    .expect("first runtime starts");
    first_context.started.notified().await;

    let shutdown = first.shutdown(Duration::from_millis(20)).await;
    assert!(shutdown.is_err());
    let stopped = wait_for_activity_status(&pool, activity_id, "running").await;
    let stopped_expiry = stopped.lease_expires_at;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let still_stopped = wait_for_activity_status(&pool, activity_id, "running").await;
    assert_eq!(still_stopped.lease_expires_at, stopped_expiry);

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_expires_at.eq(Some(0_i64)))
        .execute(&mut connection)
        .await
        .expect("expire abandoned lease");
    drop(connection);

    let second_context = Arc::new(RuntimeTestContext::default());
    let second = runtime(pool.clone(), second_context, "recovery", runtime_config())
        .spawn()
        .await
        .expect("second runtime starts");
    let pending = wait_for_activity_status(&pool, activity_id, "pending").await;
    assert_eq!(pending.attempt_count, 1);
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::available_at.eq(durable_workflows::persistence::now_millis()))
        .execute(&mut connection)
        .await
        .expect("make retry due");
    drop(connection);
    let succeeded = wait_for_activity_status(&pool, activity_id, "succeeded").await;
    assert_eq!(succeeded.attempt_count, 2);
    second
        .shutdown(Duration::from_secs(1))
        .await
        .expect("second runtime drains");

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn heartbeat_does_not_starve_a_handler_holding_the_activity_row() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, 3, 1_000, 2_000).await;
    let context = Arc::new(RuntimeTestContext::default());
    context.mode.store(4, Ordering::SeqCst);
    *context.pool.lock().expect("test context pool mutex") = Some(pool.clone());
    let mut config = runtime_config();
    config.worker.heartbeat_interval = Duration::from_millis(5);
    let handle = runtime(pool.clone(), context.clone(), "lock-overlap", config)
        .spawn()
        .await
        .expect("runtime starts");
    context.row_lock_acquired.notified().await;

    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_activity_status(&pool, activity_id, "succeeded"),
    )
    .await
    .expect("handler transaction and heartbeat must both make progress");
    handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect("runtime drains");

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn panicking_worker_is_reported_restarted_and_recovered_to_dead_letter() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, 2, 40, 100).await;
    let panic_context = Arc::new(RuntimeTestContext::default());
    panic_context.mode.store(3, Ordering::SeqCst);
    let mut config = runtime_config();
    config.max_task_restarts = 1;
    let handle = runtime(pool.clone(), panic_context, "panic-runtime", config)
        .spawn()
        .await
        .expect("runtime starts");
    let cancellation = handle.cancellation_token();
    let completion = handle.completion_token();
    tokio::time::timeout(Duration::from_secs(5), cancellation.cancelled())
        .await
        .expect("restart budget is exhausted");
    tokio::time::timeout(Duration::from_secs(5), completion.cancelled())
        .await
        .expect("supervisor completion is externally observable");
    let shutdown = handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect_err("panics are surfaced");
    assert!(shutdown.errors.iter().any(|error| error.panicked));

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::lease_expires_at.eq(Some(0_i64)))
        .execute(&mut connection)
        .await
        .expect("expire panicked lease");
    drop(connection);
    let recovery = runtime(
        pool.clone(),
        Arc::new(RuntimeTestContext::default()),
        "panic-recovery",
        runtime_config(),
    )
    .spawn()
    .await
    .expect("recovery runtime starts");
    let dead_lettered = wait_for_activity_status(&pool, activity_id, "dead_lettered").await;
    assert_eq!(dead_lettered.attempt_count, 2);
    recovery
        .shutdown(Duration::from_secs(1))
        .await
        .expect("recovery runtime drains");

    let mut connection = pool.get().await.expect("test connection");
    let blocked = durable_workflow::table
        .find(workflow_id)
        .select(durable_workflow::status)
        .first::<String>(&mut connection)
        .await
        .expect("workflow status");
    let panic_recovery_events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id))
        .filter(durable_workflow_event::event_type.eq("activity_lease_expired"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("panic recovery history");
    assert_eq!(blocked, "blocked");
    assert!(panic_recovery_events >= 1);
    support::drop_durable_tables(&mut connection).await;
}
