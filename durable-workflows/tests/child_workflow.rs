mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    admin::{AdminControlService, Operator},
    durable_flow,
    persistence::WorkflowRow,
    schema::{durable_workflow, durable_workflow_event},
    ActivityContext, ActivityError, ActivityHandler, ActivityTopic, CoordinatorConfig,
    DurableActivity, DurableFlow, DurableStore, DurableWorkflow, RetryPolicy, StartOptions, WfCtx,
    WfError, WorkerConfig,
};

#[derive(Clone, Copy)]
struct MathTopic;

impl ActivityTopic for MathTopic {
    fn key(self) -> &'static str {
        "child_math"
    }

    fn max_concurrency(self) -> u32 {
        4
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct DoubleActivity {
    value: i64,
}

impl DurableActivity for DoubleActivity {
    type Topic = MathTopic;

    const KIND: &'static str = "double_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        MathTopic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for DoubleActivity {
    type Context = ();
    type Output = i64;

    async fn execute(&self, _context: ActivityContext<'_, ()>) -> Result<i64, ActivityError> {
        Ok(self.value * 2)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ChildDouble {
    value: i64,
}

impl DurableWorkflow for ChildDouble {
    const KIND: &'static str = "child_double_flow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for ChildDouble {
    type Context = ();
    type Output = i64;

    async fn run(&self, ctx: &mut WfCtx<'_, ()>) -> Result<i64, WfError> {
        ctx.run(&DoubleActivity { value: self.value }).await
    }
}

/// A child that parks on an activity no worker in these tests ever claims, so
/// the parent stays in `waiting_child` until an operator intervenes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StuckChild;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct UnservedActivity;

#[derive(Clone, Copy)]
struct UnservedTopic;

impl ActivityTopic for UnservedTopic {
    fn key(self) -> &'static str {
        "unserved_topic"
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

impl DurableActivity for UnservedActivity {
    type Topic = UnservedTopic;

    const KIND: &'static str = "unserved_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        UnservedTopic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("test policy is valid")
    }
}

#[async_trait]
impl ActivityHandler for UnservedActivity {
    type Context = ();
    type Output = ();

    async fn execute(&self, _context: ActivityContext<'_, ()>) -> Result<(), ActivityError> {
        Ok(())
    }
}

impl DurableWorkflow for StuckChild {
    const KIND: &'static str = "stuck_child_flow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for StuckChild {
    type Context = ();
    type Output = ();

    async fn run(&self, ctx: &mut WfCtx<'_, ()>) -> Result<(), WfError> {
        ctx.run(&UnservedActivity).await
    }
}

#[durable_flow(kind = "parent_pipeline_flow", version = 1)]
async fn parent_pipeline_flow(ctx: &mut WfCtx<'_, ()>, value: i64) -> Result<i64, WfError> {
    let doubled = ctx.child(&ChildDouble { value }).await?;
    let redoubled = ctx.run(&DoubleActivity { value: doubled }).await?;
    Ok(redoubled)
}

#[durable_flow(kind = "keyed_parent_flow", version = 1)]
async fn keyed_parent_flow(ctx: &mut WfCtx<'_, ()>, value: i64) -> Result<i64, WfError> {
    ctx.child_with_key(&ChildDouble { value }, "shared-child")
        .await
}

#[durable_flow(kind = "stuck_parent_flow", version = 1)]
async fn stuck_parent_flow(ctx: &mut WfCtx<'_, ()>) -> Result<(), WfError> {
    ctx.child(&StuckChild).await
}

#[durable_flow(kind = "keyed_stuck_parent_flow", version = 1)]
async fn keyed_stuck_parent_flow(ctx: &mut WfCtx<'_, ()>) -> Result<(), WfError> {
    ctx.child_with_key(&StuckChild, "shared-stuck-child").await
}

fn coordinator(
    pool: &durable_workflows::DurablePool,
    worker_id: &str,
    config: CoordinatorConfig,
) -> durable_workflows::WorkflowCoordinator<()> {
    let workflows = durable_workflows::register_durable_workflows!(
        ();
        ParentPipelineFlow,
        KeyedParentFlow,
        KeyedStuckParentFlow,
        StuckParentFlow,
        ChildDouble,
        StuckChild
    )
    .expect("workflow registry is valid");
    let activities = durable_workflows::register_durable_activities!(
        ();
        DoubleActivity,
        UnservedActivity
    )
    .expect("activity registry is valid");
    durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(workflows),
        Arc::new(activities),
        worker_id,
        config,
    )
    .expect("coordinator is valid")
}

fn activity_worker(
    pool: &durable_workflows::DurablePool,
    worker_id: &str,
) -> durable_workflows::ActivityWorker<()> {
    let activities = durable_workflows::register_durable_activities!(
        ();
        DoubleActivity,
        UnservedActivity
    )
    .expect("activity registry is valid");
    let topics = durable_workflows::register_durable_topics!(MathTopic, UnservedTopic)
        .expect("topic registry is valid");
    durable_workflows::ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(activities),
        Arc::new(topics),
        worker_id,
        WorkerConfig {
            heartbeat_interval: Duration::from_millis(20),
            shutdown_grace: Duration::from_secs(1),
        },
    )
    .expect("worker is valid")
}

async fn drain(
    coordinator: &durable_workflows::WorkflowCoordinator<()>,
    worker: &durable_workflows::ActivityWorker<()>,
) {
    for _ in 0..32 {
        let activated = coordinator
            .activate_one()
            .await
            .expect("activation succeeds")
            .is_some();
        let executed = worker
            .run_one("child_math")
            .await
            .expect("activity run succeeds")
            .is_some();
        if !activated && !executed {
            return;
        }
    }
    panic!("workflows did not settle within the drain budget");
}

async fn load_workflow(
    pool: &durable_workflows::DurablePool,
    workflow_id: durable_workflows::WorkflowId,
) -> WorkflowRow {
    let mut connection = pool.get().await.expect("test connection");
    durable_workflows::persistence::find_workflow_by_id(&mut connection, workflow_id)
        .await
        .expect("workflow loads")
}

#[tokio::test]
async fn parent_awaits_child_flow_and_completes_with_its_output() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let started = store
        .start(&ParentPipelineFlow { value: 5 }, StartOptions::default())
        .await
        .expect("parent starts");
    let coordinator = coordinator(&pool, "parent-coordinator", CoordinatorConfig::default());

    // First activation: the parent suspends at the child step.
    coordinator
        .activate_one()
        .await
        .expect("parent activates")
        .expect("parent claim");
    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "waiting_child");
    assert_eq!(parent.wait_kind.as_deref(), Some("child"));
    let child_id = parent.wait_reference_id.expect("child reference");

    let mut connection = pool.get().await.expect("test connection");
    let child = durable_workflow::table
        .find(child_id)
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("child row");
    drop(connection);
    assert_eq!(child.kind, ChildDouble::KIND);
    assert_eq!(child.parent_workflow_id, Some(parent.id));
    assert_eq!(child.parent_command_sequence, Some(1));
    assert_eq!(child.root_workflow_id, Some(parent.id));
    assert_eq!(
        child.deduplication_key,
        Some(format!("child:{}:1", parent.id))
    );

    let worker = activity_worker(&pool, "child-worker");
    drain(&coordinator, &worker).await;

    let child = load_workflow(
        &pool,
        durable_workflows::WorkflowId::new(child_id).expect("child id"),
    )
    .await;
    assert_eq!(child.status.as_str(), "succeeded");
    assert_eq!(child.result_json.as_deref(), Some("10"));
    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "succeeded");
    assert_eq!(parent.result_json.as_deref(), Some("20"));

    let mut connection = pool.get().await.expect("test connection");
    let wake_events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(parent.id))
        .filter(durable_workflow_event::event_type.eq("child_succeeded"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("wake event count");
    assert_eq!(wake_events, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn cancelled_child_delivers_child_failed_and_fails_the_parent() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let started = store
        .start(&StuckParentFlow {}, StartOptions::default())
        .await
        .expect("parent starts");
    let coordinator = coordinator(
        &pool,
        "cancel-coordinator",
        CoordinatorConfig {
            max_activation_attempts: 1,
            ..CoordinatorConfig::default()
        },
    );

    // Parent suspends at the child; the child parks on its unserved activity.
    coordinator
        .activate_one()
        .await
        .expect("parent activates")
        .expect("parent claim");
    coordinator
        .activate_one()
        .await
        .expect("child activates")
        .expect("child claim");
    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "waiting_child");
    let child_id = parent.wait_reference_id.expect("child reference");

    let workflows = Arc::new(
        durable_workflows::register_durable_workflows!(
            ();
            ParentPipelineFlow,
            KeyedParentFlow,
            StuckParentFlow,
            ChildDouble,
            StuckChild
        )
        .expect("workflow registry"),
    );
    let activities = Arc::new(
        durable_workflows::register_durable_activities!(
            ();
            DoubleActivity,
            UnservedActivity
        )
        .expect("activity registry"),
    );
    let control = AdminControlService::new(pool.clone(), workflows, activities);
    let operator = Operator::new("7", "cancel the stuck child").expect("operator");
    control
        .cancel_workflow(
            durable_workflows::WorkflowId::new(child_id).expect("child id"),
            &operator,
        )
        .await
        .expect("child cancels");

    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "ready");
    assert!(parent.wait_kind.is_none());

    coordinator
        .activate_one()
        .await
        .expect("parent activates with the failure")
        .expect("parent claim");
    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "failed");
    assert!(parent
        .error_message
        .as_deref()
        .is_some_and(|message| message.contains("child_cancelled")));

    let mut connection = pool.get().await.expect("test connection");
    let failure_events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(parent.id))
        .filter(durable_workflow_event::event_type.eq("child_failed"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("failure event count");
    assert_eq!(failure_events, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn in_flight_keyed_child_wakes_every_waiting_parent() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let coordinator = coordinator(
        &pool,
        "shared-waiter-coordinator",
        CoordinatorConfig::default(),
    );

    let first = store
        .start(&KeyedStuckParentFlow {}, StartOptions::default())
        .await
        .expect("first parent starts");
    coordinator
        .activate_one()
        .await
        .expect("first parent activates")
        .expect("first parent claim");
    let first_parent = load_workflow(&pool, first.workflow_id).await;
    assert_eq!(first_parent.status.as_str(), "waiting_child");
    let child_id = first_parent.wait_reference_id.expect("shared child");

    let second = store
        .start(&KeyedStuckParentFlow {}, StartOptions::default())
        .await
        .expect("second parent starts");
    // The shared child is also ready, so drain activations until the second
    // parent has attached to it rather than assuming the next claim is that
    // parent.
    for _ in 0..8 {
        let second_parent = load_workflow(&pool, second.workflow_id).await;
        if second_parent.status == durable_workflows::persistence::WorkflowStatus::WaitingChild {
            assert_eq!(second_parent.wait_reference_id, Some(child_id));
            break;
        }
        coordinator
            .activate_one()
            .await
            .expect("activation succeeds")
            .expect("ready workflow claim");
    }
    let second_parent = load_workflow(&pool, second.workflow_id).await;
    assert_eq!(second_parent.status.as_str(), "waiting_child");
    assert_eq!(second_parent.wait_reference_id, Some(child_id));

    let mut connection = pool.get().await.expect("test connection");
    let children = durable_workflow::table
        .filter(durable_workflow::kind.eq(StuckChild::KIND))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("child count");
    drop(connection);
    assert_eq!(children, 1);

    let workflows = Arc::new(
        durable_workflows::register_durable_workflows!(
            ();
            ParentPipelineFlow,
            KeyedParentFlow,
            KeyedStuckParentFlow,
            StuckParentFlow,
            ChildDouble,
            StuckChild
        )
        .expect("workflow registry"),
    );
    let activities = Arc::new(
        durable_workflows::register_durable_activities!(
            ();
            DoubleActivity,
            UnservedActivity
        )
        .expect("activity registry"),
    );
    let control = AdminControlService::new(pool.clone(), workflows, activities);
    let operator = Operator::new("7", "cancel the shared stuck child").expect("operator");
    control
        .cancel_workflow(
            durable_workflows::WorkflowId::new(child_id).expect("child id"),
            &operator,
        )
        .await
        .expect("shared child cancels");

    let first_parent = load_workflow(&pool, first.workflow_id).await;
    let second_parent = load_workflow(&pool, second.workflow_id).await;
    assert_eq!(first_parent.status.as_str(), "ready");
    assert_eq!(second_parent.status.as_str(), "ready");
    assert!(first_parent.wait_kind.is_none());
    assert!(second_parent.wait_kind.is_none());

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn domain_keyed_child_deduplicates_and_terminal_hits_wake_immediately() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let coordinator = coordinator(&pool, "dedup-coordinator", CoordinatorConfig::default());
    let worker = activity_worker(&pool, "dedup-worker");

    let first = store
        .start(&KeyedParentFlow { value: 4 }, StartOptions::default())
        .await
        .expect("first parent starts");
    drain(&coordinator, &worker).await;
    let first_parent = load_workflow(&pool, first.workflow_id).await;
    assert_eq!(first_parent.status.as_str(), "succeeded");
    assert_eq!(first_parent.result_json.as_deref(), Some("8"));

    // The second parent reuses the completed child through the shared key and
    // must complete without creating another child instance.
    let second = store
        .start(&KeyedParentFlow { value: 900 }, StartOptions::default())
        .await
        .expect("second parent starts");
    drain(&coordinator, &worker).await;
    let second_parent = load_workflow(&pool, second.workflow_id).await;
    assert_eq!(second_parent.status.as_str(), "succeeded");
    assert_eq!(second_parent.result_json.as_deref(), Some("8"));

    let mut connection = pool.get().await.expect("test connection");
    let children = durable_workflow::table
        .filter(durable_workflow::kind.eq(ChildDouble::KIND))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("child count");
    assert_eq!(children, 1);
    support::drop_durable_tables(&mut connection).await;
}

/// Same kind + domain key as [`ChildDouble`], but a bumped definition version.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ChildDoubleV2 {
    value: i64,
}

impl DurableWorkflow for ChildDoubleV2 {
    const KIND: &'static str = ChildDouble::KIND;
    const VERSION: i32 = 2;
}

#[async_trait]
impl DurableFlow for ChildDoubleV2 {
    type Context = ();
    type Output = i64;

    async fn run(&self, ctx: &mut WfCtx<'_, ()>) -> Result<i64, WfError> {
        ctx.run(&DoubleActivity { value: self.value }).await
    }
}

#[durable_flow(kind = "keyed_parent_v2_flow", version = 1)]
async fn keyed_parent_v2_flow(ctx: &mut WfCtx<'_, ()>, value: i64) -> Result<i64, WfError> {
    ctx.child_with_key(&ChildDoubleV2 { value }, "shared-child")
        .await
}

#[tokio::test]
async fn keyed_child_rejects_dedup_hit_with_different_version() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let v1_coordinator = coordinator(&pool, "version-dedup-v1", CoordinatorConfig::default());
    let worker = activity_worker(&pool, "version-dedup-worker");

    store
        .start(&KeyedParentFlow { value: 4 }, StartOptions::default())
        .await
        .expect("v1 parent starts");
    drain(&v1_coordinator, &worker).await;

    let workflows = durable_workflows::register_durable_workflows!(
        ();
        KeyedParentV2Flow,
        ChildDoubleV2
    )
    .expect("v2 registry");
    let activities =
        durable_workflows::register_durable_activities!((); DoubleActivity).expect("v2 activities");
    let v2_coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(workflows),
        Arc::new(activities),
        "version-dedup-v2",
        CoordinatorConfig::default(),
    )
    .expect("v2 coordinator");

    let second = store
        .start(&KeyedParentV2Flow { value: 9 }, StartOptions::default())
        .await
        .expect("v2 parent starts");
    v2_coordinator
        .activate_one()
        .await
        .expect("activation records the definition mismatch")
        .expect("v2 parent was claimed");

    let parent = load_workflow(&pool, second.workflow_id).await;
    assert_eq!(parent.status.as_str(), "ready");
    assert_eq!(parent.error_category.as_deref(), Some("activation"));
    assert!(
        parent
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("does not match expected")),
        "unexpected parent error: {:?}",
        parent.error_message
    );

    let mut connection = pool.get().await.expect("test connection");
    let children = durable_workflow::table
        .filter(durable_workflow::kind.eq(ChildDouble::KIND))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("child count");
    assert_eq!(children, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn paused_parent_resumes_into_waiting_child() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = DurableStore::new(pool.clone());
    let started = store
        .start(&StuckParentFlow {}, StartOptions::default())
        .await
        .expect("parent starts");
    let coordinator = coordinator(&pool, "pause-coordinator", CoordinatorConfig::default());
    coordinator
        .activate_one()
        .await
        .expect("parent activates")
        .expect("parent claim");

    let workflows = Arc::new(
        durable_workflows::register_durable_workflows!(
            ();
            ParentPipelineFlow,
            KeyedParentFlow,
            StuckParentFlow,
            ChildDouble,
            StuckChild
        )
        .expect("workflow registry"),
    );
    let activities = Arc::new(
        durable_workflows::register_durable_activities!(
            ();
            DoubleActivity,
            UnservedActivity
        )
        .expect("activity registry"),
    );
    let control = AdminControlService::new(pool.clone(), workflows, activities);
    let operator = Operator::new("7", "pause for inspection").expect("operator");
    control
        .pause_workflow(started.workflow_id, &operator)
        .await
        .expect("parent pauses");
    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "paused");
    assert_eq!(parent.wait_kind.as_deref(), Some("child"));

    control
        .resume_workflow(started.workflow_id, &operator)
        .await
        .expect("parent resumes");
    let parent = load_workflow(&pool, started.workflow_id).await;
    assert_eq!(parent.status.as_str(), "waiting_child");

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}
