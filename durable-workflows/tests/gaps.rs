//! Reproduction tests for the suspected gaps in `docs/INVARIANTS.md` section 6.
//!
//! Each test asserts the behavior the invariant calls correct. A test that
//! fails against the current engine confirms its gap and is `#[ignore]`d with
//! the reason, so the suite stays green; a passing test refutes its gap and
//! stays as a regression test.

mod support;

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel::{sql_types::BigInt, QueryableByName};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use durable_workflows::{
    admin::{AdminControlService, Operator},
    persistence::{database_now_millis, find_workflow_by_id, WorkflowRow},
    schema::{durable_activity, durable_workflow},
    ActivityContext, ActivityError, ActivityHandler, ActivityRegistry, ActivityTopic,
    ActivityWorker, CoordinatorConfig, DurableActivity, DurableFlow, DurablePool, DurableRuntime,
    DurableStore, DurableWorkflow, RetryPolicy, RuntimeConfig, StartOptions, TopicRegistry, WfCtx,
    WfError, WorkerConfig, WorkflowCoordinator, WorkflowId, WorkflowRegistry,
};
use tokio::sync::Semaphore;

const CONDITION_TIMEOUT: Duration = Duration::from_secs(15);

/// Lets a test hold a flow inside its `step` so an operator action can land
/// between the coordinator's claim and its commit.
struct GapContext {
    entered: Semaphore,
    release: Semaphore,
}

impl Default for GapContext {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}

#[derive(Clone, Copy)]
enum GapTopic {
    G2,
    G10A,
    G10B,
    G11,
}

impl ActivityTopic for GapTopic {
    fn key(self) -> &'static str {
        match self {
            GapTopic::G2 => "gap_g2",
            GapTopic::G10A => "gap_g10_a",
            GapTopic::G10B => "gap_g10_b",
            GapTopic::G11 => "gap_g11",
        }
    }

    fn max_concurrency(self) -> u32 {
        4
    }
}

macro_rules! gap_activity {
    ($name:ident, $kind:literal, $topic:expr, $max_attempts:expr, $body:expr) => {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct $name;

        impl DurableActivity for $name {
            type Topic = GapTopic;

            const KIND: &'static str = $kind;
            const VERSION: i32 = 1;
            const MAX_ATTEMPTS: u32 = $max_attempts;
            const TIMEOUT: Duration = Duration::from_secs(5);
            const LEASE_DURATION: Duration = Duration::from_secs(10);

            fn topic() -> Self::Topic {
                $topic
            }

            fn retry_policy() -> RetryPolicy {
                RetryPolicy::fixed(1).expect("test policy is valid")
            }
        }

        #[async_trait]
        impl ActivityHandler for $name {
            type Context = GapContext;
            type Output = ();

            async fn execute(
                &self,
                _context: ActivityContext<'_, GapContext>,
            ) -> Result<(), ActivityError> {
                $body
            }
        }
    };
}

gap_activity!(
    G2FailingActivity,
    "gap_g2_failing",
    GapTopic::G2,
    1,
    Err(ActivityError::permanent("provider", "rejected"))
);
gap_activity!(G10ActivityA, "gap_g10_a", GapTopic::G10A, 3, Ok(()));
gap_activity!(G10ActivityB, "gap_g10_b", GapTopic::G10B, 3, Ok(()));
gap_activity!(
    G11UnservedActivity,
    "gap_g11_unserved",
    GapTopic::G11,
    3,
    Ok(())
);

macro_rules! gap_flow {
    ($name:ident { $($field:ident : $ty:ty),* }, $kind:literal, $version:literal, |$self_:ident, $ctx:ident| $body:expr) => {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct $name {
            $($field: $ty),*
        }

        impl DurableWorkflow for $name {
            const KIND: &'static str = $kind;
            const VERSION: i32 = $version;
        }

        #[async_trait]
        impl DurableFlow for $name {
            type Context = GapContext;
            type Output = ();

            async fn run(&self, $ctx: &mut WfCtx<'_, GapContext>) -> Result<(), WfError> {
                let $self_ = self;
                $body
            }
        }
    };
}

gap_flow!(G1Gated { gated: bool }, "gap_g1_gated", 1, |this, ctx| {
    if this.gated {
        ctx.application().entered.add_permits(1);
        ctx.application()
            .release
            .acquire()
            .await
            .expect("release semaphore is open")
            .forget();
    }
    Ok(())
});

gap_flow!(G2Child {}, "gap_g2_child", 1, |_this, ctx| {
    ctx.run(&G2FailingActivity).await
});

gap_flow!(G2Parent {}, "gap_g2_parent", 1, |_this, ctx| {
    ctx.child_with_key(&G2Child {}, "g2-key").await
});

gap_flow!(G3Panic {}, "gap_g3_panic", 1, |_this, _ctx| {
    panic!("poison-pill step");
});

gap_flow!(G4Child {}, "gap_g4_child", 1, |_this, _ctx| Ok(()));

gap_flow!(G4Parent {}, "gap_g4_parent", 1, |_this, ctx| {
    ctx.child(&G4Child {}).await
});

gap_flow!(G6ChildV1 {}, "gap_g6_child", 1, |_this, _ctx| Ok(()));
gap_flow!(G6ChildV2 {}, "gap_g6_child", 2, |_this, _ctx| Ok(()));

gap_flow!(G6ParentOfV1 {}, "gap_g6_parent_v1", 1, |_this, ctx| {
    ctx.child_with_key(&G6ChildV1 {}, "g6-key").await
});

gap_flow!(G6ParentOfV2 {}, "gap_g6_parent_v2", 1, |_this, ctx| {
    ctx.child_with_key(&G6ChildV2 {}, "g6-key").await
});

gap_flow!(G8SelfKeyed {}, "gap_g8_self", 1, |_this, ctx| {
    ctx.child_with_key(&G8SelfKeyed {}, "g8-self").await
});

gap_flow!(G10Flow { topic_b: bool }, "gap_g10_flow", 1, |this, ctx| {
    if this.topic_b {
        ctx.run(&G10ActivityB).await
    } else {
        ctx.run(&G10ActivityA).await
    }
});

gap_flow!(G11Child {}, "gap_g11_child", 1, |_this, ctx| {
    ctx.run(&G11UnservedActivity).await
});

gap_flow!(G11Parent {}, "gap_g11_parent", 1, |_this, ctx| {
    ctx.child(&G11Child {}).await
});

fn workflows() -> Arc<WorkflowRegistry<GapContext>> {
    Arc::new(
        durable_workflows::register_durable_workflows!(
            GapContext;
            G1Gated,
            G2Child,
            G2Parent,
            G3Panic,
            G4Child,
            G4Parent,
            G6ChildV1,
            G6ChildV2,
            G6ParentOfV1,
            G6ParentOfV2,
            G8SelfKeyed,
            G10Flow,
            G11Child,
            G11Parent
        )
        .expect("workflow registry is valid"),
    )
}

fn activities() -> Arc<ActivityRegistry<GapContext>> {
    Arc::new(
        durable_workflows::register_durable_activities!(
            GapContext;
            G2FailingActivity,
            G10ActivityA,
            G10ActivityB,
            G11UnservedActivity
        )
        .expect("activity registry is valid"),
    )
}

fn topics() -> Arc<TopicRegistry> {
    Arc::new(
        durable_workflows::register_durable_topics!(
            GapTopic::G2,
            GapTopic::G10A,
            GapTopic::G10B,
            GapTopic::G11
        )
        .expect("topic registry is valid"),
    )
}

fn coordinator(
    pool: &DurablePool,
    context: Arc<GapContext>,
    config: CoordinatorConfig,
) -> WorkflowCoordinator<GapContext> {
    WorkflowCoordinator::new(
        pool.clone(),
        context,
        workflows(),
        activities(),
        "gap-coordinator",
        config,
    )
    .expect("coordinator is valid")
}

fn worker(pool: &DurablePool, context: Arc<GapContext>) -> ActivityWorker<GapContext> {
    ActivityWorker::new(
        pool.clone(),
        context,
        activities(),
        topics(),
        "gap-worker",
        WorkerConfig {
            heartbeat_interval: Duration::from_millis(50),
            shutdown_grace: Duration::from_secs(1),
        },
    )
    .expect("worker is valid")
}

fn control(pool: &DurablePool) -> AdminControlService<GapContext> {
    AdminControlService::new(pool.clone(), workflows(), activities())
}

fn operator(reason: &str) -> Operator {
    Operator::new("7", reason).expect("operator is valid")
}

async fn load(pool: &DurablePool, workflow_id: WorkflowId) -> WorkflowRow {
    let mut connection = pool.get().await.expect("test connection");
    find_workflow_by_id(&mut connection, workflow_id)
        .await
        .expect("workflow loads")
}

fn id(raw: i64) -> WorkflowId {
    WorkflowId::new(raw).expect("valid workflow id")
}

async fn running_workflow(pool: &DurablePool) -> Option<WorkflowId> {
    let mut connection = pool.get().await.expect("test connection");
    durable_workflow::table
        .filter(durable_workflow::status.eq("running"))
        .select(durable_workflow::id)
        .first::<i64>(&mut connection)
        .await
        .ok()
        .map(id)
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

/// Number of InnoDB transactions in lock wait that have already modified rows.
#[cfg(feature = "mysql")]
async fn lock_waiters(pool: &DurablePool, min_rows_modified: i64) -> i64 {
    let mut connection = pool.get().await.expect("test connection");
    diesel::sql_query(format!(
        "SELECT COUNT(*) AS count FROM information_schema.innodb_trx \
         WHERE trx_state = 'LOCK WAIT' AND trx_rows_modified >= {min_rows_modified}"
    ))
    .get_result::<Count>(&mut connection)
    .await
    .expect("innodb_trx query")
    .count
}

/// Number of backends in this database waiting on a heavyweight lock. With
/// `min_rows_modified > 0`, only transactions that already hold a write xid.
#[cfg(feature = "postgres")]
async fn lock_waiters(pool: &DurablePool, min_rows_modified: i64) -> i64 {
    let mut connection = pool.get().await.expect("test connection");
    diesel::sql_query(format!(
        "SELECT COUNT(*) AS count FROM pg_stat_activity \
         WHERE datname = current_database() AND wait_event_type = 'Lock' \
         AND ({min_rows_modified} = 0 OR backend_xid IS NOT NULL)"
    ))
    .get_result::<Count>(&mut connection)
    .await
    .expect("pg_stat_activity query")
    .count
}

/// Polls a database condition. The timeout only bounds a failing test; the
/// ordering the test relies on comes from the condition itself.
async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        // information_schema.innodb_trx refreshes its cache only after 100 ms
        // without reads, so polling faster would never observe new state.
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn expire_lease_by_database_clock(pool: &DurablePool, workflow_id: WorkflowId) {
    let mut connection = pool.get().await.expect("test connection");
    let now = database_now_millis(&mut connection)
        .await
        .expect("database clock");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set(durable_workflow::lease_expires_at.eq(Some(now - 1)))
        .execute(&mut connection)
        .await
        .expect("lease expiry update");
}

// ---------------------------------------------------------------------------
// G2
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G2: recoverable start cancels the blocked keyed child but its parent stays waiting_child on it"]
async fn g2_recoverable_start_wakes_parent_of_superseded_blocked_child() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let store = DurableStore::new(pool.clone());
    let parent_id = store
        .start(&G2Parent {}, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    coordinator
        .activate_one()
        .await
        .expect("parent activates")
        .expect("parent claim");
    let parent = load(&pool, parent_id).await;
    assert_eq!(parent.status.as_str(), "waiting_child");
    let child_id = id(parent.wait_reference_id.expect("child reference"));
    coordinator
        .activate_one()
        .await
        .expect("child activates")
        .expect("child claim");
    worker(&pool, context.clone())
        .run_one("gap_g2")
        .await
        .expect("activity runs")
        .expect("activity claim");
    let child = load(&pool, child_id).await;
    assert_eq!(child.status.as_str(), "blocked");
    assert_eq!(child.deduplication_key.as_deref(), Some("g2-key"));

    let successor = store
        .start_or_restart_recoverable(
            &G2Child {},
            StartOptions::default().with_deduplication_key("g2-key"),
        )
        .await
        .expect("recoverable start");
    assert!(successor.inserted);
    assert_eq!(load(&pool, child_id).await.status.as_str(), "cancelled");

    let parent = load(&pool, parent_id).await;
    assert!(
        !(parent.status.as_str() == "waiting_child"
            && parent.wait_reference_id == Some(child_id.get())),
        "parent {} is stranded: status {} still waits on cancelled child {} (successor {})",
        parent.id,
        parent.status.as_str(),
        child_id.get(),
        successor.workflow_id.get(),
    );
}

// ---------------------------------------------------------------------------
// G11
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G11: cancelling a parent leaves its child waiting_activity with a pending activity"]
async fn g11_parent_cancellation_cancels_child_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let parent_id = DurableStore::new(pool.clone())
        .start(&G11Parent {}, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let coordinator = coordinator(&pool, context, CoordinatorConfig::default());
    coordinator
        .activate_one()
        .await
        .expect("parent activates")
        .expect("parent claim");
    let child_id = id(load(&pool, parent_id)
        .await
        .wait_reference_id
        .expect("child reference"));
    coordinator
        .activate_one()
        .await
        .expect("child activates")
        .expect("child claim");
    let child = load(&pool, child_id).await;
    assert_eq!(child.status.as_str(), "waiting_activity");
    let activity_id = child.wait_reference_id.expect("activity reference");

    control(&pool)
        .cancel_workflow(parent_id, &operator("cancel the parent"))
        .await
        .expect("parent cancels");
    assert_eq!(load(&pool, parent_id).await.status.as_str(), "cancelled");

    let child = load(&pool, child_id).await;
    let mut connection = pool.get().await.expect("test connection");
    let activity_status = durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("activity status");
    assert_eq!(
        (child.status.as_str(), activity_status.as_str()),
        ("cancelled", "cancelled"),
        "child workflow and its activity outlive the cancelled parent"
    );
}

// ---------------------------------------------------------------------------
// G1
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G1: activate_one returns Err(FencedWrite) when an operator pauses the workflow during its step"]
async fn g1_pause_during_step_is_not_a_coordinator_error() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let workflow_id = DurableStore::new(pool.clone())
        .start(&G1Gated { gated: true }, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let coordinator = Arc::new(coordinator(
        &pool,
        context.clone(),
        CoordinatorConfig::default(),
    ));
    let activation = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.activate_one().await }
    });
    context
        .entered
        .acquire()
        .await
        .expect("entered semaphore")
        .forget();
    assert_eq!(load(&pool, workflow_id).await.status.as_str(), "running");
    control(&pool)
        .pause_workflow(workflow_id, &operator("pause during step"))
        .await
        .expect("pause accepts a running workflow");
    context.release.add_permits(1);
    let outcome = activation.await.expect("activation task joins");
    assert_eq!(load(&pool, workflow_id).await.status.as_str(), "paused");
    assert!(
        outcome.is_ok(),
        "a fenced commit after an operator pause escalated to a coordinator error: {outcome:?}"
    );
}

#[tokio::test]
#[ignore = "confirms G1: two operator pauses during steps exhaust max_task_restarts=1 and the runtime stops processing"]
async fn g1_repeated_operator_pauses_do_not_stop_the_runtime() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let store = DurableStore::new(pool.clone());
    for _ in 0..2 {
        store
            .start(&G1Gated { gated: true }, StartOptions::default())
            .await
            .expect("gated workflow starts");
    }
    let runtime = DurableRuntime::new(
        pool.clone(),
        context.clone(),
        workflows(),
        activities(),
        topics(),
        "gap-runtime",
        RuntimeConfig {
            idle_delay: Duration::from_millis(5),
            restart_backoff: Duration::from_millis(10),
            max_task_restarts: 1,
            ..RuntimeConfig::default()
        },
    )
    .expect("runtime definition");
    let handle = runtime.spawn().await.expect("runtime spawns");
    let completion = handle.completion_token();

    for round in 0..2 {
        tokio::time::timeout(CONDITION_TIMEOUT, context.entered.acquire())
            .await
            .unwrap_or_else(|_| panic!("round {round}: no gated step started"))
            .expect("entered semaphore")
            .forget();
        let running = running_workflow(&pool)
            .await
            .expect("the gated workflow is running");
        control(&pool)
            .pause_workflow(running, &operator("pause during step"))
            .await
            .expect("pause accepts a running workflow");
        context.release.add_permits(1);
    }

    // Liveness probe: the runtime must still activate new work.
    let probe = store
        .start(&G1Gated { gated: false }, StartOptions::default())
        .await
        .expect("probe starts")
        .workflow_id;
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    let mut probe_status = load(&pool, probe).await.status.as_str().to_string();
    while probe_status != "succeeded"
        && !completion.is_cancelled()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
        probe_status = load(&pool, probe).await.status.as_str().to_string();
    }
    let stopped = completion.is_cancelled();
    let errors = match handle.shutdown(Duration::from_secs(5)).await {
        Ok(()) => Vec::new(),
        Err(error) => error.errors,
    };
    assert!(
        probe_status == "succeeded" && !stopped,
        "runtime stopped after benign operator pauses: probe status {probe_status}, \
         runtime completed {stopped}, task errors {errors:?}"
    );
}

// ---------------------------------------------------------------------------
// G4
// ---------------------------------------------------------------------------

#[tokio::test]
async fn g4_child_completion_sees_a_parent_pause_committed_after_its_first_read() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let parent = DurableStore::new(pool.clone())
        .start(&G4Parent {}, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let coordinator = Arc::new(coordinator(&pool, context, CoordinatorConfig::default()));
    coordinator
        .activate_one()
        .await
        .expect("parent schedules its child");
    let parent_row = load(&pool, parent).await;
    assert_eq!(parent_row.status.as_str(), "waiting_child");
    let child = id(parent_row
        .wait_reference_id
        .expect("parent waits on a child"));
    let child_claim = coordinator
        .claim_one()
        .await
        .expect("claim")
        .expect("child claim");
    assert_eq!(child_claim.workflow_id().expect("id"), child);

    // Block the child's next history insert. The child's completion takes its
    // first read (`next_event_sequence` on the child), then waits to insert its
    // own history event, before it locks the parent.
    let mut connection = pool.get().await.expect("test connection");
    let last_child_sequence = durable_workflows::schema::durable_workflow_event::table
        .filter(durable_workflows::schema::durable_workflow_event::workflow_id.eq(child.get()))
        .select(diesel::dsl::max(
            durable_workflows::schema::durable_workflow_event::sequence,
        ))
        .get_result::<Option<i32>>(&mut connection)
        .await
        .expect("child history")
        .expect("child has history");
    drop(connection);
    let mut blocker = pool.get().await.expect("blocker connection");
    // MySQL: an InnoDB REPEATABLE READ gap lock on every later sequence.
    #[cfg(feature = "mysql")]
    let block = [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".to_string(),
        "START TRANSACTION".to_string(),
        format!(
            "SELECT sequence FROM durable_workflow_event \
             FORCE INDEX (uq_durable_workflow_event_sequence) \
             WHERE workflow_id = {} AND sequence > {last_child_sequence} FOR SHARE",
            child.get()
        ),
    ];
    // Postgres has no gap locks: an uncommitted row on the next sequence makes
    // the completion's insert wait on the unique index until ROLLBACK.
    #[cfg(feature = "postgres")]
    let block = [
        "START TRANSACTION".to_string(),
        format!(
            "INSERT INTO durable_workflow_event (workflow_id, sequence, event_type, created_at) \
             VALUES ({}, {}, 'gap_g4_blocker', 0)",
            child.get(),
            last_child_sequence + 1
        ),
    ];
    for statement in block {
        blocker
            .batch_execute(&statement)
            .await
            .expect("block the child's next event");
    }

    let completion = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.activate_claim(child_claim).await }
    });
    wait_until(
        "child completion to block on its history insert",
        || async { lock_waiters(&pool, 1).await >= 1 },
    )
    .await;

    tokio::time::timeout(
        CONDITION_TIMEOUT,
        control(&pool).pause_workflow(parent, &operator("pause while child completes")),
    )
    .await
    .expect("pause is not blocked by the child's completion")
    .expect("pause accepts a waiting parent");
    blocker.batch_execute("ROLLBACK").await.expect("release");
    drop(blocker);

    let outcome = tokio::time::timeout(CONDITION_TIMEOUT, completion)
        .await
        .expect("child completion finishes")
        .expect("child completion task joins");
    assert!(
        outcome.is_ok(),
        "child completion collided with the parent's pause event: {outcome:?}"
    );
    assert_eq!(load(&pool, child).await.status.as_str(), "succeeded");
    let parent_row = load(&pool, parent).await;
    assert_eq!(parent_row.status.as_str(), "paused");
    assert_eq!(parent_row.wait_reference_id, None);
    let mut connection = pool.get().await.expect("test connection");
    let parent_history = durable_workflows::schema::durable_workflow_event::table
        .filter(durable_workflows::schema::durable_workflow_event::workflow_id.eq(parent.get()))
        .order(durable_workflows::schema::durable_workflow_event::sequence.asc())
        .select(durable_workflows::schema::durable_workflow_event::event_type)
        .load::<String>(&mut connection)
        .await
        .expect("parent history");
    assert_eq!(
        parent_history[parent_history.len() - 2..],
        ["workflow_paused".to_string(), "child_succeeded".to_string()]
    );
}

// ---------------------------------------------------------------------------
// G6
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G6: under a dedup race the v2 parent commits waiting_child on the v1 child without a version check"]
async fn g6_child_dedup_race_rejects_version_mismatch() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let store = DurableStore::new(pool.clone());
    let parent_v1 = store
        .start(&G6ParentOfV1 {}, StartOptions::default())
        .await
        .expect("v1 parent starts")
        .workflow_id;
    let parent_v2 = store
        .start(&G6ParentOfV2 {}, StartOptions::default())
        .await
        .expect("v2 parent starts")
        .workflow_id;
    let coordinator = Arc::new(coordinator(&pool, context, CoordinatorConfig::default()));
    let claim_v1 = coordinator
        .claim_one()
        .await
        .expect("claim")
        .expect("v1 parent claim");
    let claim_v2 = coordinator
        .claim_one()
        .await
        .expect("claim")
        .expect("v2 parent claim");
    assert_eq!(claim_v1.workflow_id().expect("id"), parent_v1);
    assert_eq!(claim_v2.workflow_id().expect("id"), parent_v2);

    // Share-lock the v1 parent's row: the child insert's foreign-key check
    // still passes, but the fenced parent update that follows insert_child in
    // commit_child waits, so the v1 child row stays inserted and uncommitted.
    let mut blocker = pool.get().await.expect("blocker connection");
    blocker
        .batch_execute("START TRANSACTION")
        .await
        .expect("begin");
    blocker
        .batch_execute(&format!(
            "SELECT id FROM durable_workflow WHERE id = {} FOR SHARE",
            parent_v1.get()
        ))
        .await
        .expect("share-lock v1 parent");

    let v1_commit = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.activate_claim(claim_v1).await }
    });
    wait_until("v1 commit to insert its child and block", || async {
        lock_waiters(&pool, 1).await >= 1
    })
    .await;

    // The v2 parent's consistent-read pre-check cannot see the uncommitted v1
    // child; its insert then waits on the v1 child's unique key.
    let v2_commit = tokio::spawn({
        let coordinator = coordinator.clone();
        async move { coordinator.activate_claim(claim_v2).await }
    });
    wait_until("v2 commit to block on the dedup key", || async {
        lock_waiters(&pool, 0).await >= 2
    })
    .await;
    blocker.batch_execute("ROLLBACK").await.expect("release");
    drop(blocker);

    let v1_outcome = tokio::time::timeout(CONDITION_TIMEOUT, v1_commit)
        .await
        .expect("v1 commit finishes")
        .expect("v1 task joins");
    let v2_outcome = tokio::time::timeout(CONDITION_TIMEOUT, v2_commit)
        .await
        .expect("v2 commit finishes")
        .expect("v2 task joins");
    v1_outcome.expect("v1 parent commits");

    let mut connection = pool.get().await.expect("test connection");
    let (child_id, child_version) = durable_workflow::table
        .filter(durable_workflow::kind.eq("gap_g6_child"))
        .filter(durable_workflow::deduplication_key.eq("g6-key"))
        .select((durable_workflow::id, durable_workflow::version))
        .first::<(i64, i32)>(&mut connection)
        .await
        .expect("keyed child");
    assert_eq!(child_version, 1);
    let v1 = load(&pool, parent_v1).await;
    assert_eq!(v1.wait_reference_id, Some(child_id));
    let v2 = load(&pool, parent_v2).await;
    assert!(
        !(v2.status.as_str() == "waiting_child" && v2.wait_reference_id == Some(child_id)),
        "v2 parent waits on v1 child {child_id}: status {}, activation outcome {v2_outcome:?}",
        v2.status.as_str()
    );
}

// ---------------------------------------------------------------------------
// G10
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G10: one row with leaseDuration <= timeout makes claim_batch return Err and claim nothing on any topic"]
async fn g10_invalid_activity_row_does_not_stop_other_claims() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let store = DurableStore::new(pool.clone());
    let bad_workflow = store
        .start(&G10Flow { topic_b: false }, StartOptions::default())
        .await
        .expect("topic A workflow starts")
        .workflow_id;
    let good_workflow = store
        .start(&G10Flow { topic_b: true }, StartOptions::default())
        .await
        .expect("topic B workflow starts")
        .workflow_id;
    let coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    for _ in 0..2 {
        coordinator
            .activate_one()
            .await
            .expect("activation")
            .expect("claim");
    }
    let bad_activity = load(&pool, bad_workflow)
        .await
        .wait_reference_id
        .expect("topic A activity");
    let good_activity = load(&pool, good_workflow)
        .await
        .wait_reference_id
        .expect("topic B activity");

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(bad_activity))
        .set(durable_activity::lease_duration_millis.eq(durable_activity::timeout_millis))
        .execute(&mut connection)
        .await
        .expect("corrupt the topic A row");
    drop(connection);

    let capacity = HashMap::from([("gap_g10_a".to_string(), 1), ("gap_g10_b".to_string(), 1)]);
    let outcome = worker(&pool, context).claim_batch(4, &capacity).await;
    let claimed: Vec<i64> = match &outcome {
        Ok(claims) => claims
            .iter()
            .map(|claim| claim.activity_id().expect("id").get())
            .collect(),
        Err(_) => Vec::new(),
    };
    assert!(
        outcome.is_ok() && claimed == vec![good_activity],
        "valid topic B activity {good_activity} was not claimed: {:?}",
        outcome.as_ref().map(|_| &claimed)
    );
}

// ---------------------------------------------------------------------------
// G3
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G3: a panicking step never counts an activation attempt; after lease recovery it panics again forever"]
async fn g3_panicking_step_is_bounded_by_activation_attempts() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let workflow_id = DurableStore::new(pool.clone())
        .start(&G3Panic {}, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let coordinator = Arc::new(coordinator(
        &pool,
        context,
        CoordinatorConfig {
            max_activation_attempts: 2,
            ..CoordinatorConfig::default()
        },
    ));
    let mut panics = 0;
    for _ in 0..5 {
        let row = load(&pool, workflow_id).await;
        if row.status.as_str() == "failed" {
            break;
        }
        if row.status.as_str() == "running" {
            expire_lease_by_database_clock(&pool, workflow_id).await;
        }
        let joined = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.activate_one().await }
        })
        .await;
        if joined.as_ref().is_err_and(|error| error.is_panic()) {
            panics += 1;
        }
    }
    let row = load(&pool, workflow_id).await;
    assert_eq!(
        row.status.as_str(),
        "failed",
        "poison-pill step is unbounded: {panics} coordinator panics, status {}, \
         activation_attempts {}",
        row.status.as_str(),
        row.activation_attempts
    );
}

// ---------------------------------------------------------------------------
// G8
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "confirms G8: child_with_key resolving to the caller commits a self-wait (waiting_child on itself)"]
async fn g8_child_key_resolving_to_self_does_not_wait_on_itself() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let workflow_id = DurableStore::new(pool.clone())
        .start(
            &G8SelfKeyed {},
            StartOptions::default().with_deduplication_key("g8-self"),
        )
        .await
        .expect("workflow starts")
        .workflow_id;
    coordinator(&pool, context, CoordinatorConfig::default())
        .activate_one()
        .await
        .expect("activation")
        .expect("claim");
    let row = load(&pool, workflow_id).await;
    assert!(
        !(row.status.as_str() == "waiting_child" && row.wait_reference_id == Some(row.id)),
        "workflow {} waits on itself: status {}",
        row.id,
        row.status.as_str()
    );
}

// ---------------------------------------------------------------------------
// N1
// ---------------------------------------------------------------------------

gap_flow!(N1Child { fail: bool }, "gap_n1_child", 1, |this, ctx| {
    if this.fail {
        ctx.run(&G2FailingActivity).await
    } else {
        Ok(())
    }
});

gap_flow!(N1Parent {}, "gap_n1_parent", 1, |_this, ctx| {
    ctx.child_with_key(&N1Child { fail: false }, "n1-key")
        .await?;
    ctx.child(&N1Child { fail: true }).await
});

fn n1_workflows() -> Arc<WorkflowRegistry<GapContext>> {
    Arc::new(
        durable_workflows::register_durable_workflows!(GapContext; N1Child, N1Parent)
            .expect("workflow registry is valid"),
    )
}

struct N1Tree {
    pool: DurablePool,
    parent: WorkflowId,
    keyed_child: WorkflowId,
    sibling: WorkflowId,
}

/// P runs keyed child w2 ("n1-key") to success, then an auto-keyed child w3 of
/// the same kind. With `block_sibling`, w3's activity dead-letters and w3 blocks;
/// otherwise w3 stays `waiting_activity`.
async fn n1_tree(block_sibling: bool) -> Option<N1Tree> {
    let pool = support::fresh_pool().await?;
    let context = Arc::new(GapContext::default());
    let parent = DurableStore::new(pool.clone())
        .start(&N1Parent {}, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let coordinator = WorkflowCoordinator::new(
        pool.clone(),
        context.clone(),
        n1_workflows(),
        activities(),
        "n1-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    let activate = || async {
        coordinator
            .activate_one()
            .await
            .expect("activation")
            .expect("claim");
    };

    activate().await;
    let keyed_child = id(load(&pool, parent)
        .await
        .wait_reference_id
        .expect("keyed child reference"));
    activate().await;
    let row = load(&pool, keyed_child).await;
    assert_eq!(row.status.as_str(), "succeeded");
    assert_eq!(row.deduplication_key.as_deref(), Some("n1-key"));

    activate().await;
    let parent_row = load(&pool, parent).await;
    assert_eq!(parent_row.status.as_str(), "waiting_child");
    let sibling = id(parent_row.wait_reference_id.expect("sibling reference"));
    assert_ne!(sibling, keyed_child);
    activate().await;
    let row = load(&pool, sibling).await;
    assert_eq!(row.kind, "gap_n1_child");
    assert_eq!(row.root_workflow_id, Some(parent.get()));
    assert_ne!(row.deduplication_key.as_deref(), Some("n1-key"));
    assert_eq!(row.status.as_str(), "waiting_activity");

    if block_sibling {
        worker(&pool, context)
            .run_one("gap_g2")
            .await
            .expect("activity runs")
            .expect("activity claim");
        assert_eq!(load(&pool, sibling).await.status.as_str(), "blocked");
    }
    Some(N1Tree {
        pool,
        parent,
        keyed_child,
        sibling,
    })
}

#[tokio::test]
#[ignore = "confirms N1: recoverable start on child key k cancels the blocked auto-keyed sibling and restarts it"]
async fn n1_recoverable_start_on_child_key_leaves_blocked_sibling_alone() {
    let Some(tree) = n1_tree(true).await else {
        return;
    };
    let outcome = DurableStore::new(tree.pool.clone())
        .start_or_restart_recoverable(
            &N1Child { fail: false },
            StartOptions::default().with_deduplication_key("n1-key"),
        )
        .await
        .expect("recoverable start");
    let returned = load(&tree.pool, outcome.workflow_id).await;
    let keyed = load(&tree.pool, tree.keyed_child).await;
    let sibling = load(&tree.pool, tree.sibling).await;
    let parent = load(&tree.pool, tree.parent).await;
    assert!(
        outcome.workflow_id == tree.keyed_child
            && !outcome.inserted
            && sibling.status.as_str() == "blocked",
        "recovering key n1-key (keyed child {} status {}) touched sibling {}: sibling status {}; \
         returned {} (inserted {}, restarted_from {:?}, root {:?}); parent {} status {} waits on {:?}",
        keyed.id,
        keyed.status.as_str(),
        sibling.id,
        sibling.status.as_str(),
        returned.id,
        outcome.inserted,
        returned.restarted_from_workflow_id,
        returned.root_workflow_id,
        parent.id,
        parent.status.as_str(),
        parent.wait_reference_id,
    );
}

#[tokio::test]
#[ignore = "confirms N1: recoverable start on child key k returns the id of an unrelated running sibling"]
async fn n1_recoverable_start_on_child_key_returns_its_own_row() {
    let Some(tree) = n1_tree(false).await else {
        return;
    };
    let outcome = DurableStore::new(tree.pool.clone())
        .start_or_restart_recoverable(
            &N1Child { fail: false },
            StartOptions::default().with_deduplication_key("n1-key"),
        )
        .await
        .expect("recoverable start");
    assert!(
        outcome.workflow_id == tree.keyed_child && !outcome.inserted,
        "recovering key n1-key returned {} (inserted {}), not keyed child {}; sibling {} status {}",
        outcome.workflow_id.get(),
        outcome.inserted,
        tree.keyed_child.get(),
        tree.sibling.get(),
        load(&tree.pool, tree.sibling).await.status.as_str(),
    );
}

// ---------------------------------------------------------------------------
// N2
// ---------------------------------------------------------------------------

/// Holds an activity handler inside `execute` and counts handlers that are
/// still executing (the count drops when the handler future is dropped).
struct N2Context {
    entered: Semaphore,
    release: Semaphore,
    executing: std::sync::atomic::AtomicUsize,
}

impl Default for N2Context {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            executing: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl N2Context {
    fn executing(&self) -> usize {
        std::sync::atomic::AtomicUsize::load(&self.executing, std::sync::atomic::Ordering::SeqCst)
    }
}

struct ExecutingGuard<'a>(&'a std::sync::atomic::AtomicUsize);

impl<'a> ExecutingGuard<'a> {
    fn enter(counter: &'a std::sync::atomic::AtomicUsize) -> Self {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for ExecutingGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
struct N2Topic;

impl ActivityTopic for N2Topic {
    fn key(self) -> &'static str {
        "gap_n2"
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct N2HeldActivity;

impl DurableActivity for N2HeldActivity {
    type Topic = N2Topic;

    const KIND: &'static str = "gap_n2_held";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(20);
    const LEASE_DURATION: Duration = Duration::from_secs(30);

    fn topic() -> Self::Topic {
        N2Topic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for N2HeldActivity {
    type Context = N2Context;
    type Output = ();

    // Deliberately ignores the cancellation token: the worst case for the cap.
    async fn execute(&self, context: ActivityContext<'_, N2Context>) -> Result<(), ActivityError> {
        let application = context.application();
        let _executing = ExecutingGuard::enter(&application.executing);
        application.entered.add_permits(1);
        application
            .release
            .acquire()
            .await
            .expect("release semaphore is open")
            .forget();
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct N2Flow {}

impl DurableWorkflow for N2Flow {
    const KIND: &'static str = "gap_n2_flow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for N2Flow {
    type Context = N2Context;
    type Output = ();

    async fn run(&self, ctx: &mut WfCtx<'_, N2Context>) -> Result<(), WfError> {
        ctx.run(&N2HeldActivity).await
    }
}

fn n2_workflows() -> Arc<WorkflowRegistry<N2Context>> {
    Arc::new(
        durable_workflows::register_durable_workflows!(N2Context; N2Flow)
            .expect("workflow registry is valid"),
    )
}

fn n2_activities() -> Arc<ActivityRegistry<N2Context>> {
    Arc::new(
        durable_workflows::register_durable_activities!(N2Context; N2HeldActivity)
            .expect("activity registry is valid"),
    )
}

// The first heartbeat fires 5 s after the claim; every step between r1's claim
// and r2's claim finishes well inside that, so r1's fence miss cannot end the
// overlap before r2 claims.
const N2_HEARTBEAT: Duration = Duration::from_secs(5);
const N2_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

fn n2_worker(
    pool: &DurablePool,
    context: Arc<N2Context>,
    worker_id: &str,
) -> ActivityWorker<N2Context> {
    ActivityWorker::new(
        pool.clone(),
        context,
        n2_activities(),
        Arc::new(
            durable_workflows::register_durable_topics!(N2Topic).expect("topic registry is valid"),
        ),
        worker_id,
        WorkerConfig {
            heartbeat_interval: N2_HEARTBEAT,
            shutdown_grace: N2_SHUTDOWN_GRACE,
        },
    )
    .expect("worker is valid")
}

#[derive(Clone, Copy, Debug)]
enum N2Revoke {
    ApplicationCancel,
    OperatorPause,
}

async fn n2_cap_holds_after_revoke(revoke: N2Revoke) {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(N2Context::default());
    let store = DurableStore::new(pool.clone());
    let coordinator = WorkflowCoordinator::new(
        pool.clone(),
        context.clone(),
        n2_workflows(),
        n2_activities(),
        "n2-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");

    let w1 = store
        .start(&N2Flow {}, StartOptions::default())
        .await
        .expect("w1 starts")
        .workflow_id;
    coordinator
        .activate_one()
        .await
        .expect("w1 activates")
        .expect("w1 claim");
    let a1 = load(&pool, w1).await.wait_reference_id.expect("a1");

    let r1 = n2_worker(&pool, context.clone(), "n2-r1");
    let r1_task = tokio::spawn(async move {
        let outcome = r1.run_one("gap_n2").await;
        (outcome, tokio::time::Instant::now())
    });
    context
        .entered
        .acquire()
        .await
        .expect("entered semaphore")
        .forget();
    assert_eq!(context.executing(), 1);

    let w2 = store
        .start(&N2Flow {}, StartOptions::default())
        .await
        .expect("w2 starts")
        .workflow_id;
    coordinator
        .activate_one()
        .await
        .expect("w2 activates")
        .expect("w2 claim");
    let a2 = load(&pool, w2).await.wait_reference_id.expect("a2");

    let r2 = n2_worker(&pool, context.clone(), "n2-r2");
    assert!(
        r2.claim_one("gap_n2").await.expect("claim").is_none(),
        "control: the cap admits a2 while a1 is running"
    );

    match revoke {
        N2Revoke::ApplicationCancel => {
            let mut connection = pool.get().await.expect("test connection");
            DurableStore::cancel_with_conn(&mut connection, w1, "application cancels w1")
                .await
                .expect("w1 cancels");
        }
        N2Revoke::OperatorPause => {
            AdminControlService::new(pool.clone(), n2_workflows(), n2_activities())
                .pause_workflow(w1, &operator("operator pauses w1"))
                .await
                .expect("w1 pauses");
        }
    }

    let second = r2.claim_one("gap_n2").await.expect("claim");
    let second_claimed_at = tokio::time::Instant::now();
    let executing_at_second_claim = context.executing();
    let second_id = second
        .as_ref()
        .map(|claim| claim.activity_id().expect("id").get());

    let (r1_outcome, r1_returned_at) = tokio::time::timeout(CONDITION_TIMEOUT, r1_task)
        .await
        .expect("r1 returns")
        .expect("r1 task joins");
    let overlap = r1_returned_at.saturating_duration_since(second_claimed_at);
    assert_eq!(context.executing(), 0, "a1's handler outlived run_one");
    assert!(
        second.is_none(),
        "{revoke:?}: r2 claimed activity {second_id:?} (a2 = {a2}) on a cap-1 topic while a1 {a1}'s \
         handler was executing ({executing_at_second_claim} handler(s) executing); a1's handler \
         ran on for {overlap:?} after that claim, until r1.run_one returned {r1_outcome:?}"
    );
}

#[tokio::test]
#[ignore = "confirms N2: application cancel frees the cap-1 slot while the cancelled handler still executes"]
async fn n2_application_cancel_keeps_topic_slot_until_handler_stops() {
    n2_cap_holds_after_revoke(N2Revoke::ApplicationCancel).await;
}

#[tokio::test]
#[ignore = "confirms N2: operator pause frees the cap-1 slot while the paused handler still executes"]
async fn n2_operator_pause_keeps_topic_slot_until_handler_stops() {
    n2_cap_holds_after_revoke(N2Revoke::OperatorPause).await;
}
