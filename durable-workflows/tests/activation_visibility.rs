mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    observability::BenignActivationKind, schema::durable_workflow, ActivityRegistry,
    CoordinatorConfig, DurableError, DurableRuntime, DurableStore, DurableWorkflow, RuntimeConfig,
    StartOptions, TopicRegistry, WorkflowContext, WorkflowCoordinator, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowRegistry, WorkflowTransition,
};
use tokio::sync::Notify;

#[derive(Default)]
struct GateContext {
    entered: Notify,
    release: Notify,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GatedWorkflow;

impl DurableWorkflow for GatedWorkflow {
    const KIND: &'static str = "activation_visibility_gated";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for GatedWorkflow {
    type Context = GateContext;
    type State = ();
    type Approval = ();
    type Output = ();
    fn initial_state(&self) {}

    async fn step(
        &self,
        context: WorkflowContext<'_, GateContext>,
        _: (),
        _: WorkflowEvent,
    ) -> Result<WorkflowTransition<(), (), ()>, WorkflowError> {
        context.application().entered.notify_one();
        context.application().release.notified().await;
        Ok(WorkflowTransition::Complete { output: () })
    }
}

#[test]
fn runtime_defaults_bound_transient_activation_alerts() {
    let config = RuntimeConfig::default();
    assert_eq!(config.max_transient_activation_errors, 10);
    assert_eq!(
        config.transient_activation_error_window,
        Duration::from_secs(5 * 60)
    );
}

#[tokio::test]
async fn runtime_rejects_zero_transient_activation_alert_bounds() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let invalid = [
        RuntimeConfig::default().with_max_transient_activation_errors(0),
        RuntimeConfig::default().with_transient_activation_error_window(Duration::ZERO),
    ];
    for config in invalid {
        assert!(matches!(
            DurableRuntime::new(
                pool.clone(),
                Arc::new(()),
                Arc::new(WorkflowRegistry::new()),
                Arc::new(ActivityRegistry::new()),
                Arc::new(TopicRegistry::new()),
                "invalid-transient-bounds",
                config,
            ),
            Err(DurableError::InvalidDefinition(_))
        ));
    }
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn activate_one_counts_a_lost_fence_as_a_fence_miss() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let context = Arc::new(GateContext::default());
    let workflows = durable_workflows::register_durable_workflows!(GateContext; GatedWorkflow)
        .expect("workflow registry");
    let mut coordinator = WorkflowCoordinator::new(
        pool.clone(),
        context.clone(),
        Arc::new(workflows),
        Arc::new(ActivityRegistry::new()),
        "visibility-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator");
    let counters = coordinator.activation_counters();
    let workflow_id = DurableStore::new(pool.clone())
        .start(&GatedWorkflow, StartOptions::default())
        .await
        .expect("workflow started")
        .workflow_id;

    let activation = tokio::spawn(async move { coordinator.activate_one().await });
    tokio::time::timeout(Duration::from_secs(10), context.entered.notified())
        .await
        .expect("step entered");
    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_workflow::table.find(workflow_id))
        .set(durable_workflow::lease_token.eq(Some("recovered-by-another-claim")))
        .execute(&mut connection)
        .await
        .expect("lease moved on");
    context.release.notify_one();

    let activated = activation
        .await
        .expect("activation task joined")
        .expect("a lost fence is not a coordinator error");
    assert_eq!(activated, Some(workflow_id));
    assert_eq!(counters.get(BenignActivationKind::FenceMiss), 1);
    assert_eq!(counters.get(BenignActivationKind::Transient), 0);

    support::drop_durable_tables(&mut connection).await;
}
