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
    persistence::{database_now_millis, find_activity_by_id, find_workflow_by_id, WorkflowRow},
    schema::{
        durable_activity, durable_activity_attempt, durable_workflow, durable_workflow_event,
    },
    ActivityContext, ActivityError, ActivityHandler, ActivityId, ActivityRegistry, ActivityTopic,
    ActivityWorker, CoordinatorConfig, DurableActivity, DurableError, DurableFlow, DurablePool,
    DurableRuntime, DurableStore, DurableWorkflow, RetryPolicy, RuntimeConfig, StartOptions,
    TopicRegistry, WfCtx, WfError, WorkerConfig, WorkflowCoordinator, WorkflowId, WorkflowRegistry,
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

gap_flow!(G1GatedPanic {}, "gap_g1_gated_panic", 1, |_this, ctx| {
    ctx.application().entered.add_permits(1);
    ctx.application()
        .release
        .acquire()
        .await
        .expect("release semaphore is open")
        .forget();
    panic!("step fails after the operator cancel");
});

gap_flow!(G2Child {}, "gap_g2_child", 1, |_this, ctx| {
    ctx.run(&G2FailingActivity).await
});

gap_flow!(G2Parent {}, "gap_g2_parent", 1, |_this, ctx| {
    ctx.child_with_key(&G2Child {}, "g2-key").await
});

/// Blocks on its first generation (`fail`); a recovery generation started
/// with `fail: false` returns `value`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct G2RecoveringChild {
    fail: bool,
    value: i32,
}

impl DurableWorkflow for G2RecoveringChild {
    const KIND: &'static str = "gap_g2_recovering_child";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for G2RecoveringChild {
    type Context = GapContext;
    type Output = i32;

    async fn run(&self, ctx: &mut WfCtx<'_, GapContext>) -> Result<i32, WfError> {
        if self.fail {
            ctx.run(&G2FailingActivity).await?;
        }
        Ok(self.value)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct G2ReattachParent {}

impl DurableWorkflow for G2ReattachParent {
    const KIND: &'static str = "gap_g2_reattach_parent";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for G2ReattachParent {
    type Context = GapContext;
    type Output = i32;

    async fn run(&self, ctx: &mut WfCtx<'_, GapContext>) -> Result<i32, WfError> {
        ctx.child_with_key(
            &G2RecoveringChild {
                fail: true,
                value: 1,
            },
            "g2-reattach",
        )
        .await
    }
}

gap_flow!(G3Panic {}, "gap_g3_panic", 1, |_this, _ctx| {
    panic!("poison-pill step");
});

gap_flow!(G3Hang {}, "gap_g3_hang", 1, |_this, _ctx| {
    tokio::time::sleep(Duration::from_secs(120)).await;
    Ok(())
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

// depth 0 is started with key "g8-ancestor"; depth 2, its grandchild, asks for
// a child with that key.
gap_flow!(
    G8Ancestor { depth: u8 },
    "gap_g8_ancestor",
    1,
    |this, ctx| {
        match this.depth {
            0 => ctx.child(&G8Ancestor { depth: 1 }).await,
            1 => ctx.child(&G8Ancestor { depth: 2 }).await,
            _ => {
                ctx.child_with_key(&G8Ancestor { depth: 0 }, "g8-ancestor")
                    .await
            }
        }
    }
);

gap_flow!(G10Flow { topic_b: bool }, "gap_g10_flow", 1, |this, ctx| {
    if this.topic_b {
        ctx.run(&G10ActivityB).await
    } else {
        ctx.run(&G10ActivityA).await
    }
});

gap_flow!(N4Gated {}, "gap_n4_gated", 1, |_this, ctx| {
    ctx.application().entered.add_permits(1);
    ctx.application()
        .release
        .acquire()
        .await
        .expect("release semaphore is open")
        .forget();
    ctx.run(&G10ActivityA).await
});

gap_flow!(N4GatedChild {}, "gap_n4_gated_child", 1, |_this, ctx| {
    ctx.application().entered.add_permits(1);
    ctx.application()
        .release
        .acquire()
        .await
        .expect("release semaphore is open")
        .forget();
    ctx.child(&G4Child {}).await
});

gap_flow!(G11Child {}, "gap_g11_child", 1, |_this, ctx| {
    ctx.run(&G11UnservedActivity).await
});

gap_flow!(G11Parent {}, "gap_g11_parent", 1, |_this, ctx| {
    ctx.child(&G11Child {}).await
});

/// Starts an owned (auto-keyed) child that blocks, for the G11 restart
/// generation test.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct G11RecoveringParent {}

impl DurableWorkflow for G11RecoveringParent {
    const KIND: &'static str = "gap_g11_recovering_parent";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for G11RecoveringParent {
    type Context = GapContext;
    type Output = i32;

    async fn run(&self, ctx: &mut WfCtx<'_, GapContext>) -> Result<i32, WfError> {
        ctx.child(&G2RecoveringChild {
            fail: true,
            value: 1,
        })
        .await
    }
}

gap_flow!(G11Grandparent {}, "gap_g11_grandparent", 1, |_this, ctx| {
    ctx.child(&G11Parent {}).await
});

gap_flow!(
    G11KeyedParent {},
    "gap_g11_keyed_parent",
    1,
    |_this, ctx| {
        let key = "g11-shared-child";
        ctx.child_with_key(&G11Child {}, key).await
    }
);

/// Signals `entered` when it starts and again when its cancellation token
/// fires, then waits for `release`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct G11HeldActivity;

impl DurableActivity for G11HeldActivity {
    type Topic = GapTopic;

    const KIND: &'static str = "gap_g11_held";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(20);
    const LEASE_DURATION: Duration = Duration::from_secs(30);

    fn topic() -> Self::Topic {
        GapTopic::G11
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for G11HeldActivity {
    type Context = GapContext;
    type Output = ();

    async fn execute(&self, context: ActivityContext<'_, GapContext>) -> Result<(), ActivityError> {
        let application = context.application();
        application.entered.add_permits(1);
        context
            .cancellation_token()
            .expect("executions carry a cancellation token")
            .cancelled()
            .await;
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

gap_flow!(
    G11RunningChild {},
    "gap_g11_running_child",
    1,
    |_this, ctx| {
        let activity = G11HeldActivity;
        ctx.run(&activity).await
    }
);

gap_flow!(
    G11RunningParent {},
    "gap_g11_running_parent",
    1,
    |_this, ctx| {
        let child = G11RunningChild {};
        ctx.child(&child).await
    }
);

fn workflows() -> Arc<WorkflowRegistry<GapContext>> {
    Arc::new(
        durable_workflows::register_durable_workflows!(
            GapContext;
            G1Gated,
            G1GatedPanic,
            G2Child,
            G2Parent,
            G2RecoveringChild,
            G2ReattachParent,
            G3Panic,
            G3Hang,
            G4Child,
            G4Parent,
            G6ChildV1,
            G6ChildV2,
            G6ParentOfV1,
            G6ParentOfV2,
            G8SelfKeyed,
            G8Ancestor,
            G10Flow,
            N4Gated,
            N4GatedChild,
            G11Child,
            G11Parent,
            G11Grandparent,
            G11KeyedParent,
            G11RunningChild,
            G11RunningParent,
            G11RecoveringParent
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
            G11UnservedActivity,
            G11HeldActivity
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

/// Claims one workflow on a task that owns `coordinator` and returns its id.
/// The task activates the claim once the returned sender fires; the handle
/// yields the activation's outcome.
async fn claim_on_task(
    mut coordinator: WorkflowCoordinator<GapContext>,
) -> (
    WorkflowId,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<WorkflowId, DurableError>>,
) {
    let (claimed_sender, claimed) = tokio::sync::oneshot::channel();
    let (activate, activate_signal) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let claim = coordinator
            .claim_one()
            .await
            .expect("claim")
            .expect("workflow claim");
        claimed_sender
            .send(claim.workflow_id().expect("id"))
            .expect("claim receiver");
        activate_signal.await.expect("activate signal");
        claim.activate().await
    });
    let workflow_id = claimed.await.expect("claim task reports its claim");
    (workflow_id, activate, task)
}

fn worker(pool: &DurablePool, context: Arc<GapContext>) -> ActivityWorker<GapContext> {
    ActivityWorker::new(
        pool.clone(),
        context,
        activities(),
        topics(),
        "gap-worker",
        WorkerConfig::default()
            .with_heartbeat_interval(Duration::from_millis(50))
            .with_shutdown_grace(Duration::from_secs(1)),
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

// ---------------------------------------------------------------------------
// G2
// ---------------------------------------------------------------------------

#[tokio::test]
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
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
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

/// Starts `parent`, runs it to its child wait and runs that child until its
/// only activity dead-letters and blocks it. Returns the blocked child.
async fn block_keyed_child<W>(
    pool: &DurablePool,
    context: &Arc<GapContext>,
    coordinator: &mut WorkflowCoordinator<GapContext>,
    parent: &W,
) -> (WorkflowId, WorkflowId)
where
    W: durable_workflows::WorkflowHandler,
{
    let parent_id = DurableStore::new(pool.clone())
        .start(parent, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    assert_eq!(
        coordinator.activate_one().await.expect("parent activates"),
        Some(parent_id)
    );
    let child_id = id(load(pool, parent_id)
        .await
        .wait_reference_id
        .expect("child reference"));
    block_child(pool, context, coordinator, child_id).await;
    (parent_id, child_id)
}

/// Runs a ready `child` to its activity and fails that activity, so the child blocks.
async fn block_child(
    pool: &DurablePool,
    context: &Arc<GapContext>,
    coordinator: &mut WorkflowCoordinator<GapContext>,
    child_id: WorkflowId,
) {
    assert_eq!(
        coordinator.activate_one().await.expect("child activates"),
        Some(child_id)
    );
    worker(pool, context.clone())
        .run_one("gap_g2")
        .await
        .expect("activity runs")
        .expect("activity claim");
    assert_eq!(load(pool, child_id).await.status.as_str(), "blocked");
}

async fn reattached_events(pool: &DurablePool, workflow_id: WorkflowId) -> Vec<Option<String>> {
    let mut connection = pool.get().await.expect("test connection");
    durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::event_type.eq("child_wait_reattached"))
        .order(durable_workflow_event::sequence.asc())
        .select(durable_workflow_event::metadata_json)
        .load::<Option<String>>(&mut connection)
        .await
        .expect("history loads")
}

#[tokio::test]
async fn g2_reattached_parent_completes_with_the_successor_output() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let (parent_id, child_id) =
        block_keyed_child(&pool, &context, &mut coordinator, &G2ReattachParent {}).await;

    let successor = DurableStore::new(pool.clone())
        .start_or_restart_recoverable(
            &G2RecoveringChild {
                fail: false,
                value: 42,
            },
            StartOptions::default().with_deduplication_key("g2-reattach"),
        )
        .await
        .expect("recoverable start");
    assert!(successor.inserted);
    assert_eq!(load(&pool, child_id).await.status.as_str(), "cancelled");
    let parent = load(&pool, parent_id).await;
    assert_eq!(parent.status.as_str(), "waiting_child");
    assert_eq!(parent.wait_reference_id, Some(successor.workflow_id.get()));
    let events = reattached_events(&pool, parent_id).await;
    assert_eq!(events.len(), 1);
    let metadata: serde_json::Value =
        serde_json::from_str(events[0].as_deref().expect("reattach metadata")).expect("JSON");
    assert_eq!(
        metadata,
        serde_json::json!({ "from": child_id.get(), "to": successor.workflow_id.get() })
    );

    assert_eq!(
        coordinator.activate_one().await.expect("successor runs"),
        Some(successor.workflow_id)
    );
    assert_eq!(
        load(&pool, successor.workflow_id).await.status.as_str(),
        "succeeded"
    );
    assert_eq!(
        coordinator.activate_one().await.expect("parent resumes"),
        Some(parent_id)
    );
    let parent = load(&pool, parent_id).await;
    assert_eq!(parent.status.as_str(), "succeeded");
    assert_eq!(parent.result_json.as_deref(), Some("42"));
}

#[tokio::test]
async fn g2_keyed_child_lineage_after_two_recoveries() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let store = DurableStore::new(pool.clone());
    let recover = || {
        store.start_or_restart_recoverable(
            &G2Child {},
            StartOptions::default().with_deduplication_key("g2-key"),
        )
    };
    let (parent_id, keyed_id) =
        block_keyed_child(&pool, &context, &mut coordinator, &G2Parent {}).await;

    let first = recover().await.expect("first recovery");
    assert!(first.inserted);
    block_child(&pool, &context, &mut coordinator, first.workflow_id).await;
    let second = recover().await.expect("second recovery");
    assert!(second.inserted);
    let again = recover().await.expect("live generation is returned");
    assert_eq!(again.workflow_id, second.workflow_id);
    assert!(!again.inserted);

    let keyed = load(&pool, keyed_id).await;
    let first_row = load(&pool, first.workflow_id).await;
    let second_row = load(&pool, second.workflow_id).await;
    assert_eq!(keyed.status.as_str(), "cancelled");
    assert_eq!(first_row.status.as_str(), "cancelled");
    assert_eq!(second_row.status.as_str(), "ready");
    assert_eq!(first_row.restarted_from_workflow_id, Some(keyed_id.get()));
    assert_eq!(
        second_row.restarted_from_workflow_id,
        Some(first.workflow_id.get())
    );
    assert_eq!(keyed.root_workflow_id, Some(parent_id.get()));
    assert_eq!(first_row.root_workflow_id, Some(parent_id.get()));
    assert_eq!(second_row.root_workflow_id, Some(parent_id.get()));
    assert_eq!(first_row.deduplication_key, None);
    assert_eq!(second_row.deduplication_key, None);

    let parent = load(&pool, parent_id).await;
    assert_eq!(parent.status.as_str(), "waiting_child");
    assert_eq!(parent.wait_reference_id, Some(second.workflow_id.get()));
    assert_eq!(reattached_events(&pool, parent_id).await.len(), 2);
}

#[tokio::test]
async fn d4_child_with_key_after_recovery_attaches_to_the_newest_generation() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let store = DurableStore::new(pool.clone());
    let (_, keyed_id) = block_keyed_child(&pool, &context, &mut coordinator, &G2Parent {}).await;
    let successor = store
        .start_or_restart_recoverable(
            &G2Child {},
            StartOptions::default().with_deduplication_key("g2-key"),
        )
        .await
        .expect("recoverable start");
    assert!(successor.inserted);
    assert_eq!(
        coordinator.activate_one().await.expect("successor runs"),
        Some(successor.workflow_id)
    );
    assert_eq!(
        load(&pool, successor.workflow_id).await.status.as_str(),
        "waiting_activity"
    );

    let late_parent = store
        .start(&G2Parent {}, StartOptions::default())
        .await
        .expect("late parent starts")
        .workflow_id;
    assert_eq!(
        coordinator.activate_one().await.expect("late parent runs"),
        Some(late_parent)
    );
    let late = load(&pool, late_parent).await;
    assert_eq!(late.status.as_str(), "waiting_child");
    assert_eq!(
        late.wait_reference_id,
        Some(successor.workflow_id.get()),
        "child_with_key attached to {:?}, not the newest generation {} of keyed row {}",
        late.wait_reference_id,
        successor.workflow_id.get(),
        keyed_id.get(),
    );
}

// ---------------------------------------------------------------------------
// G11
// ---------------------------------------------------------------------------

#[tokio::test]
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
    let mut coordinator = coordinator(&pool, context, CoordinatorConfig::default());
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

async fn activity_status(pool: &DurablePool, activity_id: i64) -> String {
    let mut connection = pool.get().await.expect("test connection");
    durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("activity status")
}

async fn cancelled_reason(pool: &DurablePool, workflow_id: WorkflowId) -> Option<String> {
    let mut connection = pool.get().await.expect("test connection");
    durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::event_type.eq("workflow_cancelled"))
        .select(durable_workflow_event::reason)
        .first::<Option<String>>(&mut connection)
        .await
        .expect("workflow_cancelled event")
}

/// Activates the ready workflow `parent` to its child wait and returns the child.
async fn activate_to_child(
    pool: &DurablePool,
    coordinator: &mut WorkflowCoordinator<GapContext>,
    parent: WorkflowId,
) -> WorkflowId {
    assert_eq!(
        coordinator.activate_one().await.expect("parent activates"),
        Some(parent)
    );
    let row = load(pool, parent).await;
    assert_eq!(row.status.as_str(), "waiting_child");
    id(row.wait_reference_id.expect("child reference"))
}

#[tokio::test]
async fn g11_cancel_reaches_grandchildren() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let top = DurableStore::new(pool.clone())
        .start(&G11Grandparent {}, StartOptions::default())
        .await
        .expect("grandparent starts")
        .workflow_id;
    let mut coordinator = coordinator(&pool, context, CoordinatorConfig::default());
    let middle = activate_to_child(&pool, &mut coordinator, top).await;
    let leaf = activate_to_child(&pool, &mut coordinator, middle).await;
    assert_eq!(
        coordinator.activate_one().await.expect("leaf activates"),
        Some(leaf)
    );
    let activity_id = load(&pool, leaf)
        .await
        .wait_reference_id
        .expect("activity reference");

    let mut connection = pool.get().await.expect("test connection");
    DurableStore::cancel_with_conn(&mut connection, top, "cancel the grandparent")
        .await
        .expect("grandparent cancels");

    for workflow_id in [top, middle, leaf] {
        assert_eq!(load(&pool, workflow_id).await.status.as_str(), "cancelled");
    }
    assert_eq!(activity_status(&pool, activity_id).await, "cancelled");
    assert_eq!(
        cancelled_reason(&pool, middle).await.as_deref(),
        Some(format!("parent workflow {top} cancelled: cancel the grandparent").as_str())
    );
    assert_eq!(
        cancelled_reason(&pool, leaf).await.as_deref(),
        Some(
            format!(
                "parent workflow {middle} cancelled: parent workflow {top} cancelled: cancel the grandparent"
            )
            .as_str()
        )
    );
}

/// A parent's cancel reaches every generation of its owned children: the
/// T-X2 successor of a blocked auto-keyed child, to which G2 re-attached the
/// parent, is cancelled with the cascade reason.
#[tokio::test]
async fn g11_cancel_reaches_the_restart_successor_of_an_owned_child() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let (parent_id, child_id) =
        block_keyed_child(&pool, &context, &mut coordinator, &G11RecoveringParent {}).await;
    let key = load(&pool, child_id)
        .await
        .deduplication_key
        .expect("owned child has the generated key");
    assert!(key.starts_with(&format!("child:{parent_id}:")), "{key}");

    let successor = DurableStore::new(pool.clone())
        .start_or_restart_recoverable(
            &G2RecoveringChild {
                fail: false,
                value: 7,
            },
            StartOptions::default().with_deduplication_key(key),
        )
        .await
        .expect("recoverable start");
    assert!(successor.inserted);
    assert_eq!(load(&pool, child_id).await.status.as_str(), "cancelled");
    assert_eq!(
        load(&pool, parent_id).await.wait_reference_id,
        Some(successor.workflow_id.get())
    );

    control(&pool)
        .cancel_workflow(parent_id, &operator("cancel the parent"))
        .await
        .expect("parent cancels");

    assert_eq!(load(&pool, parent_id).await.status.as_str(), "cancelled");
    assert_eq!(
        load(&pool, successor.workflow_id).await.status.as_str(),
        "cancelled"
    );
    assert_eq!(
        cancelled_reason(&pool, successor.workflow_id)
            .await
            .as_deref(),
        Some(format!("parent workflow {parent_id} cancelled: cancel the parent").as_str())
    );
}

#[tokio::test]
async fn g11_domain_keyed_child_survives_parent_cancellation() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let parent = DurableStore::new(pool.clone())
        .start(&G11KeyedParent {}, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let mut coordinator = coordinator(&pool, context, CoordinatorConfig::default());
    let child = activate_to_child(&pool, &mut coordinator, parent).await;
    assert_eq!(
        coordinator.activate_one().await.expect("child activates"),
        Some(child)
    );
    let activity_id = load(&pool, child)
        .await
        .wait_reference_id
        .expect("activity reference");

    control(&pool)
        .cancel_workflow(parent, &operator("cancel the parent"))
        .await
        .expect("parent cancels");

    assert_eq!(load(&pool, parent).await.status.as_str(), "cancelled");
    assert_eq!(load(&pool, child).await.status.as_str(), "waiting_activity");
    assert_eq!(activity_status(&pool, activity_id).await, "pending");
}

#[tokio::test]
async fn g11_cascade_revokes_a_running_child_activity() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let parent = DurableStore::new(pool.clone())
        .start(&G11RunningParent {}, StartOptions::default())
        .await
        .expect("parent starts")
        .workflow_id;
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let child = activate_to_child(&pool, &mut coordinator, parent).await;
    assert_eq!(
        coordinator.activate_one().await.expect("child activates"),
        Some(child)
    );
    let activity_id = load(&pool, child)
        .await
        .wait_reference_id
        .expect("activity reference");
    let runner = worker(&pool, context.clone());
    let run = tokio::spawn(async move { runner.run_one("gap_g11").await });
    let entered = || async {
        tokio::time::timeout(CONDITION_TIMEOUT, context.entered.acquire())
            .await
            .expect("handler signals")
            .expect("entered semaphore")
            .forget();
    };
    entered().await;

    control(&pool)
        .cancel_workflow(parent, &operator("cancel the parent"))
        .await
        .expect("parent cancels");
    assert_eq!(load(&pool, child).await.status.as_str(), "cancelled");
    // The heartbeat learns of the revoke and cancels the handler's token; the
    // row keeps its lease and open attempt until the handler returns.
    entered().await;
    assert_eq!(activity_status(&pool, activity_id).await, "cancelling");

    context.release.add_permits(1);
    tokio::time::timeout(CONDITION_TIMEOUT, run)
        .await
        .expect("worker returns")
        .expect("worker task joins")
        .expect("worker settles the revoked attempt");
    assert_eq!(activity_status(&pool, activity_id).await, "cancelled");
    let mut connection = pool.get().await.expect("test connection");
    let (outcome, finished_at) = durable_activity_attempt::table
        .find((activity_id, 1))
        .select((
            durable_activity_attempt::outcome,
            durable_activity_attempt::finished_at,
        ))
        .first::<(Option<String>, Option<i64>)>(&mut connection)
        .await
        .expect("attempt row");
    assert_eq!(outcome.as_deref(), Some("operator_cancelled"));
    assert!(finished_at.is_some());
}

// ---------------------------------------------------------------------------
// G1
// ---------------------------------------------------------------------------

#[tokio::test]
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
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let activation = tokio::spawn(async move { coordinator.activate_one().await });
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
        RuntimeConfig::default()
            .with_idle_delay(Duration::from_millis(5))
            .with_restart_backoff(Duration::from_millis(10))
            .with_max_task_restarts(1),
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

/// T-C3's fence miss: the step fails (here, panics) after an operator cancel.
#[tokio::test]
async fn g1_activation_failure_after_operator_cancel_is_not_a_coordinator_error() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let workflow_id = DurableStore::new(pool.clone())
        .start(&G1GatedPanic {}, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
    let activation = tokio::spawn(async move { coordinator.activate_one().await });
    context
        .entered
        .acquire()
        .await
        .expect("entered semaphore")
        .forget();
    control(&pool)
        .cancel_workflow(workflow_id, &operator("cancel during step"))
        .await
        .expect("cancel accepts a running workflow");
    context.release.add_permits(1);
    let outcome = activation.await.expect("activation task joins");
    let row = load(&pool, workflow_id).await;
    assert_eq!(row.status.as_str(), "cancelled");
    assert_eq!(row.activation_attempts, 0);
    assert_eq!(
        outcome.expect("a fenced T-C3 after an operator cancel is not a coordinator error"),
        Some(workflow_id)
    );
}

// ---------------------------------------------------------------------------
// N4
// ---------------------------------------------------------------------------

/// A stale coordinator's workflow is recovered and advanced by another
/// coordinator (`wait` is the wait kind the command leaves), then the stale
/// one commits the same command. Returns the stale commit's outcome after
/// checking the recovering commit is intact and was not duplicated.
async fn n4_stale_commit<W>(flow: &W, wait: &str) -> Option<Result<WorkflowId, DurableError>>
where
    W: durable_workflows::WorkflowHandler,
{
    let pool = support::fresh_pool().await?;
    let workflow_id = DurableStore::new(pool.clone())
        .start(flow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let stale_context = Arc::new(GapContext::default());
    let mut stale = WorkflowCoordinator::new(
        pool.clone(),
        stale_context.clone(),
        workflows(),
        activities(),
        "n4-stale-coordinator",
        CoordinatorConfig::default().with_lease_duration(Duration::from_millis(300)),
    )
    .expect("coordinator is valid");
    let activation = tokio::spawn(async move {
        let claim = stale
            .claim_one()
            .await
            .expect("claim succeeds")
            .expect("workflow is claimed");
        claim.activate().await
    });
    stale_context
        .entered
        .acquire()
        .await
        .expect("entered semaphore")
        .forget();

    wait_until("the stale claim's lease to expire", || async {
        let row = load(&pool, workflow_id).await;
        let mut connection = pool.get().await.expect("test connection");
        let now = database_now_millis(&mut connection)
            .await
            .expect("database clock");
        row.lease_expires_at.is_some_and(|expiry| expiry < now)
    })
    .await;
    let recovering_context = Arc::new(GapContext::default());
    recovering_context.release.add_permits(1);
    let mut recovering = WorkflowCoordinator::new(
        pool.clone(),
        recovering_context,
        workflows(),
        activities(),
        "n4-recovering-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    assert_eq!(
        recovering
            .activate_one()
            .await
            .expect("recovering coordinator commits"),
        Some(workflow_id)
    );
    let recovered = load(&pool, workflow_id).await;
    assert_eq!(recovered.wait_kind.map(|kind| kind.as_str()), Some(wait));

    stale_context.release.add_permits(1);
    let outcome = activation.await.expect("activation task joins");
    let row = load(&pool, workflow_id).await;
    assert_eq!(row.status, recovered.status);
    assert_eq!(row.wait_reference_id, recovered.wait_reference_id);
    let mut connection = pool.get().await.expect("test connection");
    let commands = if wait == "activity" {
        durable_activity::table
            .filter(durable_activity::workflow_id.eq(workflow_id.get()))
            .count()
            .get_result::<i64>(&mut connection)
            .await
    } else {
        durable_workflow::table
            .filter(durable_workflow::parent_workflow_id.eq(workflow_id.get()))
            .count()
            .get_result::<i64>(&mut connection)
            .await
    }
    .expect("command count");
    assert_eq!(
        commands, 1,
        "the stale commit duplicated the {wait} command"
    );
    Some(outcome)
}

/// N4: before the fix the stale RunActivity insert hit the recovering
/// commit's `uq_durable_activity_command` (a database error, no fence miss).
#[tokio::test]
async fn n4_stale_run_activity_commit_is_a_fence_miss() {
    let Some(outcome) = n4_stale_commit(&N4Gated {}, "activity").await else {
        return;
    };
    assert!(
        matches!(outcome, Err(DurableError::FencedWrite)),
        "a stale RunActivity commit must miss its fence: {outcome:?}"
    );
}

/// The RunChild path: `insert_child` resolves the auto key to the recovering
/// commit's child, so the stale commit reaches its fence.
#[tokio::test]
async fn n4_stale_run_child_commit_is_a_fence_miss() {
    let Some(outcome) = n4_stale_commit(&N4GatedChild {}, "child").await else {
        return;
    };
    assert!(
        matches!(outcome, Err(DurableError::FencedWrite)),
        "a stale RunChild commit must miss its fence: {outcome:?}"
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
    let mut coordinator = coordinator(&pool, context, CoordinatorConfig::default());
    coordinator
        .activate_one()
        .await
        .expect("parent schedules its child");
    let parent_row = load(&pool, parent).await;
    assert_eq!(parent_row.status.as_str(), "waiting_child");
    let child = id(parent_row
        .wait_reference_id
        .expect("parent waits on a child"));
    let (child_claim, activate_child, completion) = claim_on_task(coordinator).await;
    assert_eq!(child_claim, child);

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

    activate_child.send(()).expect("child claim task waits");
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
    // One coordinator per claim: a coordinator holds one outstanding claim.
    let coordinator_v1 = WorkflowCoordinator::new(
        pool.clone(),
        context.clone(),
        workflows(),
        activities(),
        "g6-coordinator-v1",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    let coordinator_v2 = WorkflowCoordinator::new(
        pool.clone(),
        context,
        workflows(),
        activities(),
        "g6-coordinator-v2",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    let (claim_v1, activate_v1, v1_commit) = claim_on_task(coordinator_v1).await;
    let (claim_v2, activate_v2, v2_commit) = claim_on_task(coordinator_v2).await;
    assert_eq!(claim_v1, parent_v1);
    assert_eq!(claim_v2, parent_v2);

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

    activate_v1.send(()).expect("v1 claim task waits");
    wait_until("v1 commit to insert its child and block", || async {
        lock_waiters(&pool, 1).await >= 1
    })
    .await;

    // The v2 parent's consistent-read pre-check cannot see the uncommitted v1
    // child; its insert then waits on the v1 child's unique key.
    activate_v2.send(()).expect("v2 claim task waits");
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
    let mut coordinator = coordinator(&pool, context.clone(), CoordinatorConfig::default());
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

/// G10 (fixed): the claim quarantines a row with invalid bounds (dead-letters
/// it and blocks its workflow) instead of claiming it. The operator retry that
/// recovers it is in `admin_controls.rs` (the trace model does not model
/// `retry_activity`).
#[tokio::test]
async fn g10_quarantined_row_is_dead_lettered_and_blocks_its_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let store = DurableStore::new(pool.clone());
    let workflow = store
        .start(&G10Flow { topic_b: false }, StartOptions::default())
        .await
        .expect("topic A workflow starts")
        .workflow_id;
    coordinator(&pool, context.clone(), CoordinatorConfig::default())
        .activate_one()
        .await
        .expect("activation")
        .expect("claim");
    let bad_activity = load(&pool, workflow)
        .await
        .wait_reference_id
        .expect("topic A activity");

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_activity::table.find(bad_activity))
        .set(durable_activity::lease_duration_millis.eq(durable_activity::timeout_millis))
        .execute(&mut connection)
        .await
        .expect("corrupt the topic A row");
    drop(connection);

    let capacity = HashMap::from([("gap_g10_a".to_string(), 1), ("gap_g10_b".to_string(), 1)]);
    let claims = worker(&pool, context)
        .claim_batch(4, &capacity)
        .await
        .expect("claim_batch quarantines the row instead of failing");
    assert!(claims.is_empty(), "the invalid row was claimed");

    let mut connection = pool.get().await.expect("test connection");
    let activity = find_activity_by_id(
        &mut connection,
        ActivityId::new(bad_activity).expect("activity id"),
    )
    .await
    .expect("activity row");
    assert_eq!(activity.status.as_str(), "dead_lettered");
    assert_eq!(activity.attempt_count, 0);
    assert_eq!(activity.last_error_category.as_deref(), Some("invalid_row"));
    assert!(activity
        .last_error_message
        .as_deref()
        .is_some_and(|message| message.starts_with("invalid_bounds")));
    assert!(activity.completed_at.is_some());
    let history = durable_workflows::schema::durable_workflow_event::table
        .filter(durable_workflows::schema::durable_workflow_event::workflow_id.eq(workflow.get()))
        .order(durable_workflows::schema::durable_workflow_event::sequence.asc())
        .select(durable_workflows::schema::durable_workflow_event::event_type)
        .load::<String>(&mut connection)
        .await
        .expect("history");
    drop(connection);
    assert!(
        history.iter().any(|event| event == "activity_quarantined"),
        "history: {history:?}"
    );
    let blocked = load(&pool, workflow).await;
    assert_eq!(blocked.status.as_str(), "blocked");
    assert_eq!(blocked.error_category.as_deref(), Some("invalid_row"));
    assert_eq!(blocked.wait_reference_id, Some(bad_activity));
}

/// Activates `workflow_id` until it fails, waiting out the activation retry
/// backoff (1 s) between attempts. Each activation must return promptly.
async fn activate_until_failed(
    pool: &DurablePool,
    coordinator: &mut WorkflowCoordinator<GapContext>,
    workflow_id: WorkflowId,
) -> (usize, WorkflowRow) {
    let started = tokio::time::Instant::now();
    let mut activations = 0;
    loop {
        let row = load(pool, workflow_id).await;
        if row.status.as_str() == "failed" {
            return (activations, row);
        }
        assert!(
            started.elapsed() < CONDITION_TIMEOUT,
            "the step never failed its activations: status {}, activation_attempts {}",
            row.status.as_str(),
            row.activation_attempts
        );
        let activation = tokio::time::timeout(Duration::from_secs(5), coordinator.activate_one())
            .await
            .expect("the coordinator is not blocked by the step")
            .expect("activation is not a coordinator error");
        if activation.is_some() {
            activations += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test]
async fn g3_panicking_step_fails_at_the_activation_cap() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = DurableStore::new(pool.clone())
        .start(&G3Panic {}, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let mut coordinator = coordinator(
        &pool,
        Arc::new(GapContext::default()),
        CoordinatorConfig::default().with_max_activation_attempts(2),
    );
    let (activations, row) = activate_until_failed(&pool, &mut coordinator, workflow_id).await;
    assert_eq!(activations, 2);
    assert_eq!(row.activation_attempts, 2);
    assert_eq!(
        row.error_message.as_deref(),
        Some("step panicked: poison-pill step")
    );
}

#[tokio::test]
async fn g3_step_exceeding_step_timeout_is_bounded_by_activation_attempts() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = DurableStore::new(pool.clone())
        .start(&G3Hang {}, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    let mut coordinator = coordinator(
        &pool,
        Arc::new(GapContext::default()),
        CoordinatorConfig::default()
            .with_max_activation_attempts(2)
            .with_step_timeout(Duration::from_millis(100)),
    );
    let (activations, row) = activate_until_failed(&pool, &mut coordinator, workflow_id).await;
    assert_eq!(activations, 2);
    assert_eq!(row.activation_attempts, 2);
    assert_eq!(
        row.error_message.as_deref(),
        Some("step exceeded step_timeout")
    );
}

// ---------------------------------------------------------------------------
// G8
// ---------------------------------------------------------------------------

#[tokio::test]
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

#[tokio::test]
async fn g8_child_key_resolving_to_a_grandparent_is_an_activation_failure() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GapContext::default());
    let grandparent = DurableStore::new(pool.clone())
        .start(
            &G8Ancestor { depth: 0 },
            StartOptions::default().with_deduplication_key("g8-ancestor"),
        )
        .await
        .expect("grandparent starts")
        .workflow_id;
    let mut coordinator = coordinator(&pool, context, CoordinatorConfig::default());
    for _ in 0..3 {
        coordinator
            .activate_one()
            .await
            .expect("activation")
            .expect("claim");
    }
    let parent = load(&pool, grandparent)
        .await
        .wait_reference_id
        .expect("parent");
    let caller = load(&pool, WorkflowId::new(parent).expect("id"))
        .await
        .wait_reference_id
        .expect("caller");
    let row = load(&pool, WorkflowId::new(caller).expect("id")).await;
    assert_eq!(row.parent_workflow_id, Some(parent));
    assert!(
        row.status.as_str() != "waiting_child",
        "workflow {caller} waits on {:?}",
        row.wait_reference_id
    );
    assert_eq!(row.activation_attempts, 1);
    let message = row.error_message.expect("activation failure message");
    assert!(
        message.contains(&format!(
            "child key g8-ancestor resolves to workflow {}, which is the caller or an ancestor",
            grandparent.get()
        )),
        "{message}"
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
    let mut coordinator = WorkflowCoordinator::new(
        pool.clone(),
        context.clone(),
        n1_workflows(),
        activities(),
        "n1-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    let mut activate = async || {
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
        WorkerConfig::default()
            .with_heartbeat_interval(N2_HEARTBEAT)
            .with_shutdown_grace(N2_SHUTDOWN_GRACE),
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
    let mut coordinator = WorkflowCoordinator::new(
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
async fn n2_application_cancel_keeps_topic_slot_until_handler_stops() {
    n2_cap_holds_after_revoke(N2Revoke::ApplicationCancel).await;
}

#[tokio::test]
async fn n2_operator_pause_keeps_topic_slot_until_handler_stops() {
    n2_cap_holds_after_revoke(N2Revoke::OperatorPause).await;
}

/// Starts one `N2Flow` and activates it, returning the workflow and its activity.
async fn n2_start(
    store: &DurableStore,
    coordinator: &mut WorkflowCoordinator<N2Context>,
    pool: &DurablePool,
) -> (WorkflowId, i64) {
    let workflow_id = store
        .start(&N2Flow {}, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id;
    coordinator
        .activate_one()
        .await
        .expect("workflow activates")
        .expect("workflow claim");
    let activity_id = load(pool, workflow_id)
        .await
        .wait_reference_id
        .expect("activity wait");
    (workflow_id, activity_id)
}

fn n2_coordinator(pool: &DurablePool, context: Arc<N2Context>) -> WorkflowCoordinator<N2Context> {
    WorkflowCoordinator::new(
        pool.clone(),
        context,
        n2_workflows(),
        n2_activities(),
        "n2-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid")
}

async fn n2_activity(pool: &DurablePool, activity_id: i64) -> (String, i32, i32, Option<String>) {
    let mut connection = pool.get().await.expect("test connection");
    durable_activity::table
        .find(activity_id)
        .select((
            durable_activity::status,
            durable_activity::attempt_count,
            durable_activity::max_attempts,
            durable_activity::lease_token,
        ))
        .first(&mut connection)
        .await
        .expect("activity row")
}

async fn n2_attempt(pool: &DurablePool, activity_id: i64, attempt: i32) -> (Option<String>, bool) {
    let mut connection = pool.get().await.expect("test connection");
    let (outcome, finished_at) = durable_activity_attempt::table
        .find((activity_id, attempt))
        .select((
            durable_activity_attempt::outcome,
            durable_activity_attempt::finished_at,
        ))
        .first::<(Option<String>, Option<i64>)>(&mut connection)
        .await
        .expect("attempt row");
    (outcome, finished_at.is_some())
}

#[tokio::test]
async fn n2_pause_then_resume_does_not_claim_the_next_attempt_until_the_old_one_settles() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(N2Context::default());
    let store = DurableStore::new(pool.clone());
    let mut coordinator = n2_coordinator(&pool, context.clone());
    let (w1, a1) = n2_start(&store, &mut coordinator, &pool).await;
    let r1 = n2_worker(&pool, context.clone(), "n2-r1");
    let r1_task = tokio::spawn(async move { r1.run_one("gap_n2").await });
    context
        .entered
        .acquire()
        .await
        .expect("entered semaphore")
        .forget();

    let admin = AdminControlService::new(pool.clone(), n2_workflows(), n2_activities());
    admin
        .pause_workflow(w1, &operator("operator pauses w1"))
        .await
        .expect("w1 pauses");
    let resumed = admin
        .resume_workflow(w1, &operator("operator resumes w1"))
        .await
        .expect("w1 resumes");
    assert_eq!(resumed.status, "waiting_activity");
    assert_eq!(n2_activity(&pool, a1).await.0, "cancelling");

    let r2 = n2_worker(&pool, context.clone(), "n2-r2");
    assert!(
        r2.claim_one("gap_n2").await.expect("claim").is_none(),
        "attempt 2 was claimed while attempt 1's handler was executing"
    );
    assert_eq!(context.executing(), 1);

    tokio::time::timeout(CONDITION_TIMEOUT, r1_task)
        .await
        .expect("r1 returns")
        .expect("r1 task joins")
        .expect("r1 settles the revoked attempt");
    assert_eq!(context.executing(), 0);
    assert_eq!(
        n2_attempt(&pool, a1, 1).await,
        (Some("operator_paused".to_string()), true)
    );
    let second = r2
        .claim_one("gap_n2")
        .await
        .expect("claim")
        .expect("attempt 2 is claimable once attempt 1 settled");
    assert_eq!(second.activity_id().expect("id").get(), a1);
    assert_eq!(second.attempt_number().expect("attempt"), 2);
}

#[tokio::test]
async fn n2_settled_paused_activity_is_pending_with_one_more_attempt() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(N2Context::default());
    let store = DurableStore::new(pool.clone());
    let mut coordinator = n2_coordinator(&pool, context.clone());
    let (w1, a1) = n2_start(&store, &mut coordinator, &pool).await;
    let r1 = n2_worker(&pool, context.clone(), "n2-r1");
    let r1_task = tokio::spawn(async move { r1.run_one("gap_n2").await });
    context
        .entered
        .acquire()
        .await
        .expect("entered semaphore")
        .forget();
    AdminControlService::new(pool.clone(), n2_workflows(), n2_activities())
        .pause_workflow(w1, &operator("operator pauses w1"))
        .await
        .expect("w1 pauses");
    let (status, _, max_attempts, token) = n2_activity(&pool, a1).await;
    assert_eq!(status, "cancelling");
    assert!(token.is_some(), "the revoked attempt keeps its lease");
    assert_eq!(max_attempts, 4);

    tokio::time::timeout(CONDITION_TIMEOUT, r1_task)
        .await
        .expect("r1 returns")
        .expect("r1 task joins")
        .expect("r1 settles the revoked attempt");
    let (status, attempt_count, max_attempts, token) = n2_activity(&pool, a1).await;
    assert_eq!(
        (status.as_str(), attempt_count, max_attempts),
        ("pending", 1, N2HeldActivity::MAX_ATTEMPTS as i32 + 1)
    );
    assert!(token.is_none());
    assert_eq!(
        n2_attempt(&pool, a1, 1).await,
        (Some("operator_paused".to_string()), true)
    );
    assert_eq!(load(&pool, w1).await.status.as_str(), "paused");
    let mut connection = pool.get().await.expect("test connection");
    let settled = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(w1.get()))
        .filter(durable_workflow_event::event_type.eq("activity_revoke_settled"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("history");
    assert_eq!(settled, 1);
}

#[tokio::test]
async fn n2_crash_while_cancelling_settles_by_lease_reconciliation() {
    let Some(pool) = support::fresh_pool_with_max_size(8).await else {
        return;
    };
    let context = Arc::new(N2Context::default());
    let store = DurableStore::new(pool.clone());
    let mut coordinator = n2_coordinator(&pool, context.clone());
    let (w1, a1) = n2_start(&store, &mut coordinator, &pool).await;
    let (_w2, a2) = n2_start(&store, &mut coordinator, &pool).await;
    // A claim with no executor: its worker crashed.
    let crashed = n2_worker(&pool, context.clone(), "n2-crashed");
    let claim = crashed
        .claim_one("gap_n2")
        .await
        .expect("claim")
        .expect("a1 claim");
    assert_eq!(claim.activity_id().expect("id").get(), a1);
    let mut connection = pool.get().await.expect("test connection");
    DurableStore::cancel_with_conn(&mut connection, w1, "application cancels w1")
        .await
        .expect("w1 cancels");
    assert_eq!(n2_activity(&pool, a1).await.0, "cancelling");
    let r2 = n2_worker(&pool, context.clone(), "n2-r2");
    assert!(r2.claim_one("gap_n2").await.expect("claim").is_none());

    // The trace records that no handler holds the claim (the model's `Crash`).
    #[cfg(feature = "trace-model")]
    durable_workflows::trace::record_local(
        &pool,
        "n2-crashed",
        durable_workflows::trace::Action::new("Crash", serde_json::json!({})),
    )
    .await;
    diesel::update(durable_activity::table.find(a1))
        .set(durable_activity::lease_expires_at.eq(Some(1_i64)))
        .execute(&mut connection)
        .await
        .expect("expire a1's lease");
    drop(connection);
    let second = r2
        .claim_one("gap_n2")
        .await
        .expect("claim")
        .expect("reconciliation frees the slot");
    assert_eq!(second.activity_id().expect("id").get(), a2);
    let (status, _, _, token) = n2_activity(&pool, a1).await;
    assert_eq!(status, "cancelled");
    assert!(token.is_none());
    assert_eq!(
        n2_attempt(&pool, a1, 1).await,
        (Some("lease_expired".to_string()), true)
    );
}
