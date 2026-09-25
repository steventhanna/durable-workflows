//! Directed runs recorded for trace checking against the Quint model
//! (`docs/design/trace-checking.md`). Each test leaves its database in place
//! so `durable-trace dump` can read `durable_trace`.
#![cfg(feature = "trace-model")]

mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel::{QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    persistence::WorkflowRow, schema::durable_workflow, ActivityCommand, ActivityContext,
    ActivityError, ActivityHandler, ActivityTopic, CoordinatorConfig, DurableActivity,
    DurableStore, DurableWorkflow, RetryPolicy, StartOptions, WorkerConfig, WorkflowContext,
    WorkflowEvent, WorkflowHandler, WorkflowTransition,
};

#[derive(Clone, Copy)]
enum Topics {
    Emails,
}

impl ActivityTopic for Topics {
    fn key(self) -> &'static str {
        "emails"
    }

    fn max_concurrency(self) -> u32 {
        4
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SendEmail;

impl DurableActivity for SendEmail {
    type Topic = Topics;

    const KIND: &'static str = "send_email";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(30);
    const LEASE_DURATION: Duration = Duration::from_secs(60);

    fn topic() -> Self::Topic {
        Topics::Emails
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(5).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for SendEmail {
    type Context = ();
    type Output = i32;

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        Ok(1)
    }
}

/// Runs one activity, then completes.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TraceFlow;

impl DurableWorkflow for TraceFlow {
    const KIND: &'static str = "trace_flow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for TraceFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        match event {
            WorkflowEvent::Started => {
                let activity = ActivityCommand::new(&SendEmail, None).map_err(|error| {
                    durable_workflows::WorkflowError::new("definition", error.to_string())
                })?;
                Ok(WorkflowTransition::RunActivity {
                    state: state + 1,
                    activity,
                })
            }
            WorkflowEvent::ActivitySucceeded { .. } => {
                Ok(WorkflowTransition::Complete { output: () })
            }
            _ => Err(durable_workflows::WorkflowError::new(
                "unexpected_event",
                "trace flow received an unexpected event",
            )),
        }
    }
}

fn activities() -> Arc<durable_workflows::ActivityRegistry<()>> {
    Arc::new(
        durable_workflows::register_durable_activities!(
            () ; SendEmail, Flaky, HoldOnce, HoldTwice, HoldLong
        )
        .expect("activities"),
    )
}

#[tokio::test]
async fn activity_happy_path() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = DurableStore::new(pool.clone())
        .start(&TraceFlow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;

    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(
            durable_workflows::register_durable_workflows!(() ; TraceFlow).expect("workflows"),
        ),
        activities(),
        "rt1:coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator");
    // The heartbeat interval outlasts the handler, so no heartbeat runs.
    let worker = durable_workflows::ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        activities(),
        Arc::new(durable_workflows::register_durable_topics!(Topics::Emails).expect("topics")),
        "rt1:dispatcher",
        WorkerConfig {
            heartbeat_interval: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(1),
        },
    )
    .expect("worker");

    assert_eq!(
        coordinator.activate_one().await.expect("first activation"),
        Some(workflow_id)
    );
    assert!(worker
        .run_one("emails")
        .await
        .expect("activity execution")
        .is_some());
    assert_eq!(
        coordinator.activate_one().await.expect("second activation"),
        Some(workflow_id)
    );

    let mut connection = pool.get().await.expect("connection");
    let workflow = durable_workflow::table
        .find(workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow row");
    assert_eq!(workflow.status.as_str(), "succeeded");
}

type Step =
    Result<WorkflowTransition<i32, serde_json::Value, ()>, durable_workflows::WorkflowError>;

fn unexpected() -> durable_workflows::WorkflowError {
    durable_workflows::WorkflowError::new("unexpected_event", "unexpected event")
}

/// Continues once, then completes.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ContinueFlow;

impl DurableWorkflow for ContinueFlow {
    const KIND: &'static str = "trace_continue";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ContinueFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Step {
        match event {
            WorkflowEvent::Started => Ok(WorkflowTransition::Continue { state: state + 1 }),
            WorkflowEvent::Continued => Ok(WorkflowTransition::Complete { output: () }),
            _ => Err(unexpected()),
        }
    }
}

/// Runs `TraceFlow` as a child, then completes.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ParentFlow;

impl DurableWorkflow for ParentFlow {
    const KIND: &'static str = "trace_parent";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ParentFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Step {
        match event {
            WorkflowEvent::Started => Ok(WorkflowTransition::RunChild {
                state: state + 1,
                child: durable_workflows::ChildWorkflowCommand::new(&TraceFlow).map_err(
                    |error| durable_workflows::WorkflowError::new("definition", error.to_string()),
                )?,
            }),
            WorkflowEvent::ChildSucceeded { .. } => Ok(WorkflowTransition::Complete { output: () }),
            _ => Err(unexpected()),
        }
    }
}

/// Every activation fails.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FailingFlow;

impl DurableWorkflow for FailingFlow {
    const KIND: &'static str = "trace_failing";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for FailingFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        _state: Self::State,
        _event: WorkflowEvent,
    ) -> Step {
        Err(durable_workflows::WorkflowError::new(
            "boom",
            "trace failing flow always fails",
        ))
    }
}

/// Completes on its first activation.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct QuickFlow;

impl DurableWorkflow for QuickFlow {
    const KIND: &'static str = "trace_quick";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for QuickFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        _state: Self::State,
        _event: WorkflowEvent,
    ) -> Step {
        Ok(WorkflowTransition::Complete { output: () })
    }
}

fn coordinator(
    pool: &durable_workflows::DurablePool,
    worker_id: &str,
    config: CoordinatorConfig,
) -> durable_workflows::WorkflowCoordinator<()> {
    durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(
            durable_workflows::register_durable_workflows!(
                () ; TraceFlow, ContinueFlow, ParentFlow, FailingFlow, QuickFlow, FlakyFlow,
                HoldFlow
            )
            .expect("workflows"),
        ),
        activities(),
        worker_id,
        config,
    )
    .expect("coordinator")
}

fn worker(pool: &durable_workflows::DurablePool) -> durable_workflows::ActivityWorker<()> {
    durable_workflows::ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        activities(),
        Arc::new(durable_workflows::register_durable_topics!(Topics::Emails).expect("topics")),
        "rt1:dispatcher",
        WorkerConfig {
            heartbeat_interval: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(1),
        },
    )
    .expect("worker")
}

async fn status(
    pool: &durable_workflows::DurablePool,
    id: durable_workflows::WorkflowId,
) -> String {
    let mut connection = pool.get().await.expect("connection");
    durable_workflow::table
        .find(id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow row")
        .status
        .as_str()
        .to_string()
}

#[tokio::test]
async fn continue_then_complete() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let id = DurableStore::new(pool.clone())
        .start(&ContinueFlow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let coordinator = coordinator(&pool, "rt1:coordinator", CoordinatorConfig::default());
    assert_eq!(
        coordinator.activate_one().await.expect("continue"),
        Some(id)
    );
    assert_eq!(
        coordinator.activate_one().await.expect("complete"),
        Some(id)
    );
    assert_eq!(status(&pool, id).await, "succeeded");
}

#[tokio::test]
async fn child_happy_path() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let parent = DurableStore::new(pool.clone())
        .start(&ParentFlow, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let coordinator = coordinator(&pool, "rt1:coordinator", CoordinatorConfig::default());
    let worker = worker(&pool);
    assert_eq!(
        coordinator.activate_one().await.expect("run child"),
        Some(parent)
    );
    let child = coordinator
        .activate_one()
        .await
        .expect("child runs its activity")
        .expect("child claimed");
    assert!(worker.run_one("emails").await.expect("activity").is_some());
    assert_eq!(
        coordinator.activate_one().await.expect("child completes"),
        Some(child)
    );
    assert_eq!(
        coordinator.activate_one().await.expect("parent completes"),
        Some(parent)
    );
    assert_eq!(status(&pool, parent).await, "succeeded");
}

#[tokio::test]
async fn activation_failure_retries_then_fails() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let id = DurableStore::new(pool.clone())
        .start(&FailingFlow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let coordinator = coordinator(
        &pool,
        "rt1:coordinator",
        CoordinatorConfig {
            max_activation_attempts: 3,
            activation_retry_policy: RetryPolicy::fixed(1).expect("policy"),
            ..CoordinatorConfig::default()
        },
    );
    let mut failures = 0;
    while failures < 3 {
        if coordinator
            .activate_one()
            .await
            .expect("activation")
            .is_some()
        {
            failures += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(status(&pool, id).await, "failed");
}

#[tokio::test]
async fn stale_coordinator_fence_miss() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let id = DurableStore::new(pool.clone())
        .start(&QuickFlow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let short = CoordinatorConfig {
        lease_duration: Duration::from_millis(500),
        ..CoordinatorConfig::default()
    };
    let stale = coordinator(&pool, "rt1:coordinator", short);
    let fresh = coordinator(&pool, "rt2:coordinator", short);
    let claim = stale.claim_one().await.expect("claim").expect("claimed");
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        fresh.activate_one().await.expect("recover and complete"),
        Some(id)
    );
    assert!(matches!(
        stale.activate_claim(claim).await,
        Err(durable_workflows::DurableError::FencedWrite)
    ));
    assert_eq!(status(&pool, id).await, "succeeded");
}

/// G11: cancelling a parent leaves its child live (`inv_G11_cancelReachesChildren`).
#[tokio::test]
async fn cancel_parent_with_running_child() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let parent = DurableStore::new(pool.clone())
        .start(&ParentFlow, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let coordinator = coordinator(&pool, "rt1:coordinator", CoordinatorConfig::default());
    assert_eq!(
        coordinator.activate_one().await.expect("run child"),
        Some(parent)
    );
    let child = coordinator
        .activate_one()
        .await
        .expect("child runs its activity")
        .expect("child claimed");
    let mut connection = pool.get().await.expect("connection");
    DurableStore::cancel_with_conn(&mut connection, parent, "trace cancel")
        .await
        .expect("parent cancels");
    drop(connection);
    assert_eq!(status(&pool, parent).await, "cancelled");
    assert_eq!(status(&pool, child).await, "waiting_activity");
}

#[tokio::test]
async fn cancel_waiting_activity() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let id = DurableStore::new(pool.clone())
        .start(&TraceFlow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let coordinator = coordinator(&pool, "rt1:coordinator", CoordinatorConfig::default());
    assert_eq!(
        coordinator.activate_one().await.expect("run activity"),
        Some(id)
    );
    let mut connection = pool.get().await.expect("connection");
    DurableStore::cancel_with_conn(&mut connection, id, "trace cancel")
        .await
        .expect("cancels");
    // A second cancel of a terminal workflow is a no-op and records nothing.
    DurableStore::cancel_with_conn(&mut connection, id, "trace cancel")
        .await
        .expect("no-op");
    drop(connection);
    assert_eq!(status(&pool, id).await, "cancelled");
}

/// T-X2 branches: StartNew, supersede a failed row, ReturnLatest, and the
/// restart-key `Conflict` of both T-X2 and T-X1 after a public successor (N3).
#[tokio::test]
async fn recoverable_start_after_failure() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let keyed = || StartOptions::default().with_deduplication_key("k");
    let coordinator = coordinator(
        &pool,
        "rt1:coordinator",
        CoordinatorConfig {
            max_activation_attempts: 1,
            ..CoordinatorConfig::default()
        },
    );
    let first = store
        .start_or_restart_recoverable(&FailingFlow, keyed())
        .await
        .expect("start new");
    assert!(first.inserted);
    assert_eq!(
        coordinator.activate_one().await.expect("fails"),
        Some(first.workflow_id)
    );
    let second = store
        .start_or_restart_recoverable(&FailingFlow, keyed())
        .await
        .expect("supersede");
    assert!(second.inserted);
    let latest = store
        .start_or_restart_recoverable(&FailingFlow, keyed())
        .await
        .expect("return latest");
    assert_eq!(latest.workflow_id, second.workflow_id);
    assert!(!latest.inserted);
    assert_eq!(
        coordinator.activate_one().await.expect("fails"),
        Some(second.workflow_id)
    );
    let from_second = || StartOptions {
        restarted_from_workflow_id: Some(second.workflow_id),
        ..StartOptions::default()
    };
    store
        .start(&FailingFlow, from_second())
        .await
        .expect("public successor of a failed row");
    assert!(matches!(
        store
            .start_or_restart_recoverable(&FailingFlow, keyed())
            .await,
        Err(durable_workflows::DurableError::Conflict(_))
    ));
    assert!(matches!(
        store.start(&FailingFlow, from_second()).await,
        Err(durable_workflows::DurableError::Conflict(_))
    ));
}

// ---------------------------------------------------------------------------
// Activity worker (phase 2b): retries, dead letters, heartbeats, reconcile.
// ---------------------------------------------------------------------------

/// Fails `fail_times` attempts (retryable), or every attempt (permanent).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Flaky {
    fail_times: u32,
    permanent: bool,
}

impl DurableActivity for Flaky {
    type Topic = Topics;

    const KIND: &'static str = "flaky";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 2;
    const TIMEOUT: Duration = Duration::from_secs(30);
    const LEASE_DURATION: Duration = Duration::from_secs(60);

    fn topic() -> Self::Topic {
        Topics::Emails
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for Flaky {
    type Context = ();
    type Output = i32;

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        if self.permanent {
            return Err(ActivityError::permanent("trace", "permanent failure"));
        }
        if context.attempt_number().unwrap_or(0) <= self.fail_times {
            return Err(ActivityError::retryable("trace", "retryable failure"));
        }
        Ok(1)
    }
}

/// Per-test gates: `started` gets a permit when a handler starts; the handler
/// returns once `release` has a permit.
struct Gate {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

fn gate(name: &str) -> Arc<Gate> {
    static GATES: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<String, Arc<Gate>>>,
    > = std::sync::LazyLock::new(Default::default);
    GATES
        .lock()
        .expect("gates")
        .entry(name.to_string())
        .or_insert_with(|| {
            Arc::new(Gate {
                started: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
            })
        })
        .clone()
}

/// An activity held on a gate; short leases so heartbeats and expiry happen fast.
macro_rules! hold_activity {
    ($name:ident, $kind:literal, $attempts:literal, $timeout_ms:literal, $lease_ms:literal) => {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct $name {
            gate: String,
        }

        impl DurableActivity for $name {
            type Topic = Topics;

            const KIND: &'static str = $kind;
            const VERSION: i32 = 1;
            const MAX_ATTEMPTS: u32 = $attempts;
            const TIMEOUT: Duration = Duration::from_millis($timeout_ms);
            const LEASE_DURATION: Duration = Duration::from_millis($lease_ms);

            fn topic() -> Self::Topic {
                Topics::Emails
            }

            fn retry_policy() -> RetryPolicy {
                RetryPolicy::fixed(1).expect("test policy is valid")
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
                let gate = gate(&self.gate);
                gate.started.add_permits(1);
                gate.release.acquire().await.expect("gate").forget();
                Ok(1)
            }
        }
    };
}

hold_activity!(HoldOnce, "hold_once", 1, 1200, 1500);
hold_activity!(HoldTwice, "hold_twice", 2, 1200, 1500);
hold_activity!(HoldLong, "hold_long", 1, 30000, 60000);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FlakyFlow {
    fail_times: u32,
    permanent: bool,
}

impl DurableWorkflow for FlakyFlow {
    const KIND: &'static str = "trace_flaky";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for FlakyFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Step {
        match event {
            WorkflowEvent::Started => Ok(WorkflowTransition::RunActivity {
                state,
                activity: ActivityCommand::new(
                    &Flaky {
                        fail_times: self.fail_times,
                        permanent: self.permanent,
                    },
                    None,
                )
                .map_err(|error| {
                    durable_workflows::WorkflowError::new("definition", error.to_string())
                })?,
            }),
            WorkflowEvent::ActivitySucceeded { .. } => {
                Ok(WorkflowTransition::Complete { output: () })
            }
            _ => Err(unexpected()),
        }
    }
}

/// `activity`: `once`, `twice` or `long` (the hold activity to run).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct HoldFlow {
    gate: String,
    activity: String,
}

impl DurableWorkflow for HoldFlow {
    const KIND: &'static str = "trace_hold";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for HoldFlow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Step {
        let definition = |error: durable_workflows::DurableError| {
            durable_workflows::WorkflowError::new("definition", error.to_string())
        };
        match event {
            WorkflowEvent::Started => {
                let gate = self.gate.clone();
                let activity = match self.activity.as_str() {
                    "once" => ActivityCommand::new(&HoldOnce { gate }, None),
                    "twice" => ActivityCommand::new(&HoldTwice { gate }, None),
                    _ => ActivityCommand::new(&HoldLong { gate }, None),
                }
                .map_err(definition)?;
                Ok(WorkflowTransition::RunActivity { state, activity })
            }
            WorkflowEvent::ActivitySucceeded { .. } => {
                Ok(WorkflowTransition::Complete { output: () })
            }
            _ => Err(unexpected()),
        }
    }
}

fn named_worker(
    pool: &durable_workflows::DurablePool,
    worker_id: &str,
    heartbeat_interval: Duration,
) -> durable_workflows::ActivityWorker<()> {
    durable_workflows::ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        activities(),
        Arc::new(durable_workflows::register_durable_topics!(Topics::Emails).expect("topics")),
        worker_id,
        WorkerConfig {
            heartbeat_interval,
            shutdown_grace: Duration::from_secs(1),
        },
    )
    .expect("worker")
}

/// Starts `flow` and lets the coordinator schedule its activity; returns the
/// workflow and activity ids.
async fn start_activity<W>(
    pool: &durable_workflows::DurablePool,
    flow: &W,
) -> (durable_workflows::WorkflowId, i64)
where
    W: durable_workflows::WorkflowHandler,
{
    let id = DurableStore::new(pool.clone())
        .start(flow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let coordinator = coordinator(pool, "rt1:coordinator", CoordinatorConfig::default());
    assert_eq!(
        coordinator.activate_one().await.expect("run activity"),
        Some(id)
    );
    let mut connection = pool.get().await.expect("connection");
    let activity = durable_workflow::table
        .find(id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow row")
        .wait_reference_id
        .expect("waits on the activity");
    (id, activity)
}

async fn complete(pool: &durable_workflows::DurablePool, id: durable_workflows::WorkflowId) {
    let coordinator = coordinator(pool, "rt1:coordinator", CoordinatorConfig::default());
    assert_eq!(
        coordinator.activate_one().await.expect("completes"),
        Some(id)
    );
    assert_eq!(status(pool, id).await, "succeeded");
}

/// Retry delays are whole seconds (`RetryPolicy::fixed(1)`).
async fn wait_for_retry_delay() {
    tokio::time::sleep(Duration::from_millis(1_200)).await;
}

#[tokio::test]
async fn retry_then_succeed() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (id, _) = start_activity(
        &pool,
        &FlakyFlow {
            fail_times: 1,
            permanent: false,
        },
    )
    .await;
    let worker = worker(&pool);
    assert!(worker.run_one("emails").await.expect("retryable").is_some());
    wait_for_retry_delay().await;
    assert!(worker.run_one("emails").await.expect("succeeds").is_some());
    complete(&pool, id).await;
}

#[tokio::test]
async fn dead_letter_blocks_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (id, _) = start_activity(
        &pool,
        &FlakyFlow {
            fail_times: 0,
            permanent: true,
        },
    )
    .await;
    assert!(worker(&pool)
        .run_one("emails")
        .await
        .expect("permanent")
        .is_some());
    assert_eq!(status(&pool, id).await, "blocked");
}

#[tokio::test]
async fn attempts_exhausted_dead_letters() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (id, _) = start_activity(
        &pool,
        &FlakyFlow {
            fail_times: 2,
            permanent: false,
        },
    )
    .await;
    let worker = worker(&pool);
    assert!(worker.run_one("emails").await.expect("retry").is_some());
    wait_for_retry_delay().await;
    assert!(worker.run_one("emails").await.expect("exhausted").is_some());
    assert_eq!(status(&pool, id).await, "blocked");
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn trace_rows(pool: &durable_workflows::DurablePool, action: &str) -> i64 {
    let mut connection = pool.get().await.expect("connection");
    diesel::sql_query(format!(
        "SELECT COUNT(*) AS n FROM durable_trace WHERE action = '{action}'"
    ))
    .get_result::<Count>(&mut connection)
    .await
    .expect("trace count")
    .n
}

#[tokio::test]
async fn heartbeat_extends_lease() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let gate_name = "heartbeat_extends_lease";
    let (id, _) = start_activity(
        &pool,
        &HoldFlow {
            gate: gate_name.to_string(),
            activity: "once".to_string(),
        },
    )
    .await;
    let worker = named_worker(&pool, "rt1:dispatcher", Duration::from_millis(100));
    let run = tokio::spawn(async move { worker.run_one("emails").await });
    let gate = gate(gate_name);
    gate.started.acquire().await.expect("started").forget();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while trace_rows(&pool, "TW2_Commit").await < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no heartbeat committed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gate.release.add_permits(1);
    assert!(run.await.expect("joins").expect("succeeds").is_some());
    complete(&pool, id).await;
}

/// Worker A's heartbeat blocks on a row lock until A's local lease deadline
/// drops it (`TW2_Drop`); worker B's claim then reconciles the expired lease.
async fn expire_while_heartbeat_blocked(
    pool: &durable_workflows::DurablePool,
    gate_name: &str,
    activity: i64,
) {
    use diesel_async::SimpleAsyncConnection;
    let worker = named_worker(pool, "rtA:dispatcher", Duration::from_millis(100));
    let run = tokio::spawn(async move { worker.run_one("emails").await });
    gate(gate_name)
        .started
        .acquire()
        .await
        .expect("started")
        .forget();
    let mut blocker = pool.get().await.expect("blocker connection");
    for statement in [
        "START TRANSACTION".to_string(),
        format!("SELECT id FROM durable_activity WHERE id = {activity} FOR UPDATE"),
    ] {
        blocker
            .batch_execute(&statement)
            .await
            .expect("lock the activity row");
    }
    assert!(matches!(
        run.await.expect("joins"),
        Err(durable_workflows::DurableError::FencedWrite)
    ));
    blocker.batch_execute("ROLLBACK").await.expect("release");
    drop(blocker);
    // DB time passes the lease expiry, which the local deadline undercuts.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn expired_lease_exhausted_blocks_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let gate_name = "expired_lease_exhausted_blocks_workflow";
    let (id, activity) = start_activity(
        &pool,
        &HoldFlow {
            gate: gate_name.to_string(),
            activity: "once".to_string(),
        },
    )
    .await;
    expire_while_heartbeat_blocked(&pool, gate_name, activity).await;
    let second = named_worker(&pool, "rtB:dispatcher", Duration::from_millis(100));
    assert!(second
        .claim_one("emails")
        .await
        .expect("reconcile")
        .is_none());
    assert_eq!(status(&pool, id).await, "blocked");
}

#[tokio::test]
async fn expired_lease_reconciled_and_reclaimed() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let gate_name = "expired_lease_reconciled_and_reclaimed";
    let (id, activity) = start_activity(
        &pool,
        &HoldFlow {
            gate: gate_name.to_string(),
            activity: "twice".to_string(),
        },
    )
    .await;
    expire_while_heartbeat_blocked(&pool, gate_name, activity).await;
    let second = named_worker(&pool, "rtB:dispatcher", Duration::from_millis(100));
    assert!(second
        .claim_one("emails")
        .await
        .expect("reconcile")
        .is_none());
    wait_for_retry_delay().await;
    gate(gate_name).release.add_permits(1);
    assert!(second.run_one("emails").await.expect("reclaim").is_some());
    complete(&pool, id).await;
}

#[tokio::test]
async fn crash_then_recovery() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (id, _) = start_activity(
        &pool,
        &HoldFlow {
            gate: "crash_then_recovery".to_string(),
            activity: "once".to_string(),
        },
    )
    .await;
    let first = named_worker(&pool, "rtA:dispatcher", Duration::from_millis(100));
    assert!(first.claim_one("emails").await.expect("claim").is_some());
    drop(first);
    durable_workflows::trace::record_local(
        &pool,
        "rtA:dispatcher",
        durable_workflows::trace::Action::new("Crash", serde_json::json!({})),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_700)).await;
    let second = named_worker(&pool, "rtB:dispatcher", Duration::from_millis(100));
    assert!(second
        .claim_one("emails")
        .await
        .expect("reconcile")
        .is_none());
    assert_eq!(status(&pool, id).await, "blocked");
}

#[tokio::test]
async fn cancel_while_running_fence_miss() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let gate_name = "cancel_while_running_fence_miss";
    let (id, _) = start_activity(
        &pool,
        &HoldFlow {
            gate: gate_name.to_string(),
            activity: "long".to_string(),
        },
    )
    .await;
    let worker = worker(&pool);
    let run = tokio::spawn(async move { worker.run_one("emails").await });
    let gate = gate(gate_name);
    gate.started.acquire().await.expect("started").forget();
    let mut connection = pool.get().await.expect("connection");
    DurableStore::cancel_with_conn(&mut connection, id, "trace cancel")
        .await
        .expect("cancels");
    drop(connection);
    gate.release.add_permits(1);
    assert!(matches!(
        run.await.expect("joins"),
        Err(durable_workflows::DurableError::FencedWrite)
    ));
    assert_eq!(status(&pool, id).await, "cancelled");
}
