mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use durable_workflows::{
    ActivityCommand, ActivityContext, ActivityError, ActivityHandler, ActivityTopic,
    CoordinatorConfig, DurableActivity, DurableWorkflow, RetryPolicy, WorkflowContext,
    WorkflowEvent, WorkflowHandler, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ContinueWorkflow;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ContinueWorkflowV2;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FailingWorkflow;

impl DurableWorkflow for FailingWorkflow {
    const KIND: &'static str = "failing_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for FailingWorkflow {
    type Context = ();
    type State = ();
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {}

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        _state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Err(durable_workflows::WorkflowError::new(
            "intentional",
            "activation test failure",
        ))
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ManyContinuesWorkflow;

impl DurableWorkflow for ManyContinuesWorkflow {
    const KIND: &'static str = "many_continues_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ManyContinuesWorkflow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        0
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
        if state < 17 {
            Ok(WorkflowTransition::Continue { state: state + 1 })
        } else {
            Ok(WorkflowTransition::Complete { output: state })
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OneShotWorkflow;

impl DurableWorkflow for OneShotWorkflow {
    const KIND: &'static str = "one_shot_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for OneShotWorkflow {
    type Context = ();
    type State = ();
    type Approval = serde_json::Value;
    type Output = String;

    fn initial_state(&self) -> Self::State {}

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        _state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete {
            output: "done".to_string(),
        })
    }
}

#[derive(Clone, Copy)]
enum Topics {
    External,
}

impl ActivityTopic for Topics {
    fn key(self) -> &'static str {
        "external"
    }

    fn max_concurrency(self) -> u32 {
        2
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
impl ActivityHandler for TestActivity {
    type Context = ();
    type Output = i32;

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<Self::Output, ActivityError> {
        Ok(self.value)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ActivityWorkflow;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SleepWorkflow;

impl DurableWorkflow for SleepWorkflow {
    const KIND: &'static str = "sleep_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for SleepWorkflow {
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
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::SleepUntil {
            state: state + 1,
            wake_at_millis: durable_workflows::persistence::now_millis() + 60_000,
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ApprovalWorkflow;

impl DurableWorkflow for ApprovalWorkflow {
    const KIND: &'static str = "approval_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ApprovalWorkflow {
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
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::WaitForApproval {
            state: state + 1,
            approval: serde_json::json!({"prompt": "Approve this test?"}),
            expires_at_millis: None,
        })
    }
}

impl DurableWorkflow for ActivityWorkflow {
    const KIND: &'static str = "activity_workflow";
    const VERSION: i32 = 1;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum WaitWorkflow {
    Sleep,
    NeedsApproval,
}

impl DurableWorkflow for WaitWorkflow {
    const KIND: &'static str = "wait_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for WaitWorkflow {
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
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        match self {
            Self::Sleep => Ok(WorkflowTransition::SleepUntil {
                state: state + 1,
                wake_at_millis: durable_workflows::persistence::now_millis() + 60_000,
            }),
            Self::NeedsApproval => Ok(WorkflowTransition::WaitForApproval {
                state: state + 1,
                approval: serde_json::json!({ "prompt": "Proceed?" }),
                expires_at_millis: None,
            }),
        }
    }
}

#[async_trait]
impl WorkflowHandler for ActivityWorkflow {
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
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        let activity = ActivityCommand::new(&TestActivity { value: 7 }, Some("op-7".to_string()))
            .map_err(|error| {
            durable_workflows::WorkflowError::new("definition", error.to_string())
        })?;
        Ok(WorkflowTransition::RunActivity {
            state: state + 1,
            activity,
        })
    }
}

impl DurableWorkflow for ContinueWorkflow {
    const KIND: &'static str = "continue_workflow";
    const VERSION: i32 = 1;
}

impl DurableWorkflow for ContinueWorkflowV2 {
    const KIND: &'static str = ContinueWorkflow::KIND;
    const VERSION: i32 = 2;
}

#[async_trait]
impl WorkflowHandler for ContinueWorkflowV2 {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

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
            WorkflowEvent::Started => Ok(WorkflowTransition::Continue { state: state + 2 }),
            WorkflowEvent::Continued => Ok(WorkflowTransition::Complete { output: state }),
            _ => Err(durable_workflows::WorkflowError::new(
                "unexpected_event",
                "test workflow received the wrong event",
            )),
        }
    }
}

#[async_trait]
impl WorkflowHandler for ContinueWorkflow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

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
            WorkflowEvent::Started => Ok(WorkflowTransition::Continue { state: state + 1 }),
            WorkflowEvent::Continued => Ok(WorkflowTransition::Complete { output: state }),
            _ => Err(durable_workflows::WorkflowError::new(
                "unexpected_event",
                "test workflow received the wrong event",
            )),
        }
    }
}

#[test]
fn coordinator_defaults_bound_leases_retries_and_continuations() {
    let config = CoordinatorConfig::default();

    assert!(config.lease_duration > Duration::ZERO);
    assert!(config.max_activation_attempts > 0);
    assert_eq!(config.max_consecutive_continuations, 16);
    assert!(config.continuation_delay > Duration::ZERO);
}

#[tokio::test]
async fn activation_persists_continue_then_completion() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    let started = store
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let registry = durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
        .expect("registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "test-worker",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");

    assert!(coordinator
        .activate_one()
        .await
        .expect("first activation")
        .is_some());
    assert!(coordinator
        .activate_one()
        .await
        .expect("second activation")
        .is_some());

    let mut connection = pool.get().await.expect("test connection");
    let workflow =
        durable_workflows::persistence::find_workflow_by_id(&mut connection, started.workflow_id)
            .await
            .expect("workflow loads");
    assert_eq!(workflow.status.as_str(), "succeeded");
    assert_eq!(workflow.result_json.as_deref(), Some("1"));

    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{persistence::WorkflowEventRow, schema::durable_workflow_event};
    let events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(started.workflow_id.get()))
        .order(durable_workflow_event::sequence.asc())
        .select(WorkflowEventRow::as_select())
        .load::<WorkflowEventRow>(&mut connection)
        .await
        .expect("history loads");
    assert_eq!(
        events
            .iter()
            .map(|event| (
                event.sequence,
                event.delivery_sequence,
                event.event_type.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            (1, Some(1), "started"),
            (2, Some(2), "continued"),
            (3, None, "workflow_succeeded"),
        ]
    );

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn two_coordinators_cannot_claim_the_same_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    durable_workflows::DurableStore::new(pool.clone())
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let registry = Arc::new(
        durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
            .expect("registry is valid"),
    );
    let first = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry.clone(),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "worker-one",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    let second = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry,
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "worker-two",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");

    let (first_claim, second_claim) = tokio::join!(first.claim_one(), second.claim_one());
    let claimed = [first_claim.unwrap(), second_claim.unwrap()]
        .into_iter()
        .flatten()
        .count();
    assert_eq!(claimed, 1);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn two_coordinators_claim_distinct_ready_workflows() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    store
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("first workflow starts");
    store
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("second workflow starts");
    let registry = Arc::new(
        durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
            .expect("registry is valid"),
    );
    let first = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry.clone(),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "fleet-coordinator-one",
        CoordinatorConfig::default(),
    )
    .expect("first coordinator is valid");
    let second = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry.clone(),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "fleet-coordinator-two",
        CoordinatorConfig::default(),
    )
    .expect("second coordinator is valid");
    let third = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry,
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "fleet-coordinator-three",
        CoordinatorConfig::default(),
    )
    .expect("third coordinator is valid");

    let (left, right) = tokio::join!(first.claim_one(), second.claim_one());
    let left = left.expect("first claim").expect("first workflow");
    let right = right.expect("second claim").expect("second workflow");
    assert_ne!(
        left.workflow_id().expect("first id"),
        right.workflow_id().expect("second id")
    );
    assert!(third.claim_one().await.expect("third claim").is_none());

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn old_coordinator_skips_workflow_versions_it_cannot_execute() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    let v2 = store
        .start(
            &ContinueWorkflowV2,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("v2 workflow starts")
        .workflow_id;
    let v1 = store
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("v1 workflow starts")
        .workflow_id;
    let registry = Arc::new(
        durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
            .expect("old registry is valid"),
    );
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry,
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "old-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("old coordinator");

    let claim = coordinator
        .claim_one()
        .await
        .expect("old claim")
        .expect("locally executable workflow");
    assert_eq!(claim.workflow_id().expect("claimed workflow"), v1);

    use diesel::{QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    let mut connection = pool.get().await.expect("connection");
    let untouched = durable_workflows::schema::durable_workflow::table
        .find(v2.get())
        .select(durable_workflows::persistence::WorkflowRow::as_select())
        .first::<durable_workflows::persistence::WorkflowRow>(&mut connection)
        .await
        .expect("v2 workflow");
    assert_eq!(untouched.status.as_str(), "ready");
    assert_eq!(untouched.activation_attempts, 0);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn stale_lease_cannot_commit_a_transition() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::durable_workflow;

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    durable_workflows::DurableStore::new(pool.clone())
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let registry = Arc::new(
        durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
            .expect("registry is valid"),
    );
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry,
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "stale-worker",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    let claim = coordinator
        .claim_one()
        .await
        .expect("claim succeeds")
        .expect("workflow is claimed");
    let workflow_id = claim.workflow_id().expect("workflow id is valid");

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set(durable_workflow::lease_token.eq(Some("replacement-token".to_string())))
        .execute(&mut connection)
        .await
        .expect("lease is replaced");
    drop(connection);

    let error = coordinator
        .activate_claim(claim)
        .await
        .expect_err("stale claim must be fenced");
    assert!(matches!(
        error,
        durable_workflows::DurableError::FencedWrite
    ));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn expired_claim_is_recovered_by_another_coordinator() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    durable_workflows::DurableStore::new(pool.clone())
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let registry = Arc::new(
        durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
            .expect("registry is valid"),
    );
    let config = CoordinatorConfig {
        lease_duration: Duration::from_millis(2),
        ..CoordinatorConfig::default()
    };
    let first = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry.clone(),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "crashed-worker",
        config,
    )
    .expect("coordinator is valid");
    let second = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        registry,
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "recovery-worker",
        config,
    )
    .expect("coordinator is valid");
    let expired = first
        .claim_one()
        .await
        .expect("claim succeeds")
        .expect("workflow is claimed");
    tokio::time::sleep(Duration::from_millis(8)).await;
    let recovered = second
        .claim_one()
        .await
        .expect("recovery claim succeeds")
        .expect("expired workflow is recovered");

    assert_eq!(
        expired.workflow_id().unwrap(),
        recovered.workflow_id().unwrap()
    );
    assert_ne!(expired.lease_token(), recovered.lease_token());
    assert!(matches!(
        first.activate_claim(expired).await,
        Err(durable_workflows::DurableError::FencedWrite)
    ));

    let mut connection = pool.get().await.expect("test connection");
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::durable_workflow_event;
    let recovery_events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(recovered.workflow_id().unwrap().get()))
        .filter(durable_workflow_event::event_type.eq("lease_recovered"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("recovery history is queryable");
    assert_eq!(recovery_events, 1);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn timer_and_approval_transitions_commit_their_wait_state_atomically() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::{durable_approval, durable_workflow};

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    let sleeping = store
        .start(
            &WaitWorkflow::Sleep,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("sleep workflow starts");
    let approval = store
        .start(
            &WaitWorkflow::NeedsApproval,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("approval workflow starts");
    let registry = durable_workflows::register_durable_workflows!(() ; WaitWorkflow)
        .expect("registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "wait-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");

    coordinator.activate_one().await.expect("sleep activates");
    coordinator
        .activate_one()
        .await
        .expect("approval activates");

    let mut connection = pool.get().await.expect("test connection");
    let sleep_state = durable_workflow::table
        .find(sleeping.workflow_id.get())
        .select((
            durable_workflow::status,
            durable_workflow::wait_kind,
            durable_workflow::command_sequence,
            durable_workflow::state_json,
        ))
        .first::<(String, Option<String>, i32, String)>(&mut connection)
        .await
        .expect("sleep workflow loads");
    assert_eq!(
        sleep_state,
        (
            "sleeping".to_string(),
            Some("timer".to_string()),
            1,
            "1".to_string()
        )
    );

    let approval_state = durable_workflow::table
        .find(approval.workflow_id.get())
        .select((
            durable_workflow::status,
            durable_workflow::wait_kind,
            durable_workflow::command_sequence,
            durable_workflow::state_json,
        ))
        .first::<(String, Option<String>, i32, String)>(&mut connection)
        .await
        .expect("approval workflow loads");
    assert_eq!(
        approval_state,
        (
            "waiting_approval".to_string(),
            Some("approval".to_string()),
            1,
            "1".to_string()
        )
    );
    let approval_count = durable_approval::table
        .filter(durable_approval::workflow_id.eq(approval.workflow_id.get()))
        .filter(durable_approval::status.eq("pending"))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("approval request is queryable");
    assert_eq!(approval_count, 1);

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn activity_transition_commits_state_command_and_wait_atomically() {
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{
        persistence::{ActivityRow, WorkflowRow},
        schema::{durable_activity, durable_workflow},
    };

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let started = durable_workflows::DurableStore::new(pool.clone())
        .start(
            &ActivityWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let registry = durable_workflows::register_durable_workflows!(() ; ActivityWorkflow)
        .expect("registry is valid");
    let activities = durable_workflows::register_durable_activities!(() ; TestActivity)
        .expect("activity registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(activities),
        "activity-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    coordinator
        .activate_one()
        .await
        .expect("activation succeeds");

    let mut connection = pool.get().await.expect("test connection");
    let workflow = durable_workflow::table
        .find(started.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow loads");
    let activity = durable_activity::table
        .filter(durable_activity::workflow_id.eq(started.workflow_id.get()))
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("activity loads");
    assert_eq!(workflow.status.as_str(), "waiting_activity");
    assert_eq!(workflow.state_json, "1");
    assert_eq!(workflow.command_sequence, 1);
    assert_eq!(workflow.wait_reference_id, Some(activity.id));
    assert_eq!(activity.kind, "test_activity");
    assert_eq!(activity.operation_key.as_deref(), Some("op-7"));

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn timer_and_approval_transitions_persist_their_wait_records() {
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{
        persistence::{ApprovalRow, WorkflowRow},
        schema::{durable_approval, durable_workflow},
    };

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    let sleeping = store
        .start(&SleepWorkflow, durable_workflows::StartOptions::default())
        .await
        .expect("sleep workflow starts");
    let approval = store
        .start(
            &ApprovalWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("approval workflow starts");
    let registry = durable_workflows::register_durable_workflows!(
        ();
        SleepWorkflow,
        ApprovalWorkflow
    )
    .expect("registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "wait-worker",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    coordinator.activate_one().await.expect("timer commits");
    coordinator.activate_one().await.expect("approval commits");

    let mut connection = pool.get().await.expect("test connection");
    let sleeping_row = durable_workflow::table
        .find(sleeping.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("sleep workflow loads");
    let approval_row = durable_workflow::table
        .find(approval.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("approval workflow loads");
    let request = durable_approval::table
        .filter(durable_approval::workflow_id.eq(approval.workflow_id.get()))
        .select(ApprovalRow::as_select())
        .first::<ApprovalRow>(&mut connection)
        .await
        .expect("approval request loads");
    assert_eq!(sleeping_row.status.as_str(), "sleeping");
    assert_eq!(sleeping_row.wait_kind.as_deref(), Some("timer"));
    assert_eq!(approval_row.status.as_str(), "waiting_approval");
    assert_eq!(approval_row.wait_reference_id, Some(request.id));
    assert!(request.prompt_metadata_json.contains("Approve this test"));

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn non_deliverable_history_never_reaches_workflow_code() {
    use diesel_async::RunQueryDsl;
    use durable_workflows::{persistence::NewWorkflowEventRow, schema::durable_workflow_event};

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let started = durable_workflows::DurableStore::new(pool.clone())
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_workflow_event::table)
        .values(NewWorkflowEventRow {
            workflow_id: started.workflow_id.get(),
            sequence: 2,
            delivery_sequence: None,
            event_type: "operator_note".to_string(),
            metadata_json: None,
            actor_type: Some("operator".to_string()),
            actor_id: Some("test".to_string()),
            reason: Some("history only".to_string()),
            created_at: durable_workflows::persistence::now_millis(),
        })
        .execute(&mut connection)
        .await
        .expect("history event inserts");
    drop(connection);
    let registry = durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
        .expect("registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "history-worker",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    coordinator
        .activate_one()
        .await
        .expect("started event is delivered");

    let mut connection = pool.get().await.expect("test connection");
    let row =
        durable_workflows::persistence::find_workflow_by_id(&mut connection, started.workflow_id)
            .await
            .expect("workflow loads");
    assert_eq!(row.state_json, "1");
    assert_eq!(row.activation_attempts, 0);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn missing_definition_is_skipped_without_advancing_state_or_attempts() {
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::durable_workflow;

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    let missing = store
        .start(
            &ContinueWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(durable_workflows::WorkflowRegistry::new()),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "missing-definition",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    assert!(coordinator
        .activate_one()
        .await
        .expect("unknown definition is skipped")
        .is_none());

    let mut connection = pool.get().await.expect("test connection");
    let (status, state, cursor, attempts) = durable_workflow::table
        .find(missing.workflow_id.get())
        .select((
            durable_workflow::status,
            durable_workflow::state_json,
            durable_workflow::delivered_event_sequence,
            durable_workflow::activation_attempts,
        ))
        .first::<(String, String, i32, i32)>(&mut connection)
        .await
        .expect("workflow loads");
    assert_eq!(status, "ready");
    assert_eq!(state, "0");
    assert_eq!(cursor, 0);
    assert_eq!(attempts, 0);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn activation_errors_back_off_then_fail_at_the_configured_cap() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::{durable_workflow, durable_workflow_event};

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let started = durable_workflows::DurableStore::new(pool.clone())
        .start(&FailingWorkflow, durable_workflows::StartOptions::default())
        .await
        .expect("workflow starts");
    let registry = durable_workflows::register_durable_workflows!(() ; FailingWorkflow)
        .expect("registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "failure-worker",
        CoordinatorConfig {
            max_activation_attempts: 2,
            ..CoordinatorConfig::default()
        },
    )
    .expect("coordinator is valid");
    coordinator
        .activate_one()
        .await
        .expect("first failure is recorded");

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_workflow::table.find(started.workflow_id.get()))
        .set(durable_workflow::available_at.eq(durable_workflows::persistence::now_millis()))
        .execute(&mut connection)
        .await
        .expect("retry is made due");
    drop(connection);
    coordinator
        .activate_one()
        .await
        .expect("second failure is recorded");

    let mut connection = pool.get().await.expect("test connection");
    let (status, attempts, cursor) = durable_workflow::table
        .find(started.workflow_id.get())
        .select((
            durable_workflow::status,
            durable_workflow::activation_attempts,
            durable_workflow::delivered_event_sequence,
        ))
        .first::<(String, i32, i32)>(&mut connection)
        .await
        .expect("workflow loads");
    assert_eq!(status, "failed");
    assert_eq!(attempts, 2);
    assert_eq!(cursor, 0);
    assert_eq!(
        durable_workflow_event::table
            .filter(durable_workflow_event::workflow_id.eq(started.workflow_id.get()))
            .filter(durable_workflow_event::event_type.eq("activation_exhausted"))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .expect("terminal history count succeeds"),
        1
    );
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn sixteenth_continue_yields_to_another_ready_workflow() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    let long = store
        .start(
            &ManyContinuesWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("long workflow starts");
    let registry = durable_workflows::register_durable_workflows!(
        ();
        ManyContinuesWorkflow,
        OneShotWorkflow
    )
    .expect("registry is valid");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(registry),
        Arc::new(durable_workflows::ActivityRegistry::new()),
        "fairness-worker",
        CoordinatorConfig::default(),
    )
    .expect("coordinator is valid");
    for _ in 0..15 {
        coordinator
            .activate_one()
            .await
            .expect("continuation commits");
    }
    let short = store
        .start(&OneShotWorkflow, durable_workflows::StartOptions::default())
        .await
        .expect("short workflow starts");
    coordinator
        .activate_one()
        .await
        .expect("sixteenth continuation commits");
    coordinator
        .activate_one()
        .await
        .expect("short workflow runs next");

    let mut connection = pool.get().await.expect("test connection");
    let long_row =
        durable_workflows::persistence::find_workflow_by_id(&mut connection, long.workflow_id)
            .await
            .expect("long workflow loads");
    let short_row =
        durable_workflows::persistence::find_workflow_by_id(&mut connection, short.workflow_id)
            .await
            .expect("short workflow loads");
    assert_eq!(long_row.status.as_str(), "ready");
    assert_eq!(long_row.state_json, "16");
    assert_eq!(long_row.consecutive_continuations, 0);
    assert!(long_row.available_at > durable_workflows::persistence::now_millis() - 100);
    assert_eq!(short_row.status.as_str(), "succeeded");

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_expired_lease_scans_do_not_deadlock_distinct_ready_claims() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let store = durable_workflows::DurableStore::new(pool.clone());
    for _ in 0..2 {
        store
            .start(
                &ContinueWorkflow,
                durable_workflows::StartOptions::default(),
            )
            .await
            .expect("workflow starts");
    }

    let (claim_pool, mut arrivals, released) = paused_expiry_probe_pool().await;
    let registry = Arc::new(
        durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
            .expect("workflow registry"),
    );
    let make_coordinator = |worker| {
        durable_workflows::WorkflowCoordinator::new(
            claim_pool.clone(),
            Arc::new(()),
            registry.clone(),
            Arc::new(durable_workflows::ActivityRegistry::new()),
            worker,
            CoordinatorConfig::default(),
        )
        .expect("coordinator")
    };
    let first = make_coordinator("expiry-probe-one");
    let second = make_coordinator("expiry-probe-two");
    let left = claim_with_blocking_probe(first);
    let right = claim_with_blocking_probe(second);
    let probes = tokio::time::timeout(Duration::from_secs(5), async {
        arrivals.recv().await.expect("first completed expiry probe");
        arrivals
            .recv()
            .await
            .expect("second completed expiry probe");
    })
    .await;
    *released.0.lock().expect("release probe gate") = true;
    released.1.notify_all();
    probes.expect("both transactions reach the empty expiry probe before either claims");
    let left = left
        .await
        .expect("first task")
        .expect("first claim")
        .expect("first workflow");
    let right = right
        .await
        .expect("second task")
        .expect("second claim")
        .expect("second workflow");
    assert_ne!(
        left.workflow_id().expect("first id"),
        right.workflow_id().expect("second id")
    );
}

async fn paused_expiry_probe_pool() -> (
    durable_workflows::DurablePool,
    tokio::sync::mpsc::UnboundedReceiver<()>,
    Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
) {
    use diesel::connection::InstrumentationEvent;
    use diesel_async::{
        pooled_connection::{AsyncDieselConnectionManager, ManagerConfig},
        AsyncConnection,
    };
    use std::sync::{Condvar, Mutex};

    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let (arrived, arrivals) = tokio::sync::mpsc::unbounded_channel();
    let mut config = ManagerConfig::<durable_workflows::DurableConnection>::default();
    config.custom_setup = Box::new({
        let released = released.clone();
        move |url| {
            let released = released.clone();
            let arrived = arrived.clone();
            Box::pin(async move {
                let mut connection = durable_workflows::DurableConnection::establish(url).await?;
                let mut paused = false;
                connection.set_instrumentation(move |event: InstrumentationEvent<'_>| {
                    if let InstrumentationEvent::FinishQuery {
                        query, error: None, ..
                    } = event
                    {
                        let query = query.to_string().replace(['`', '"'], "");
                        if !paused && query.contains("ORDER BY durable_workflow.lease_expires_at") {
                            paused = true;
                            arrived.send(()).expect("test coordinator receives probe");
                            let (lock, condition) = &*released;
                            let (_guard, timeout) = condition
                                .wait_timeout_while(
                                    lock.lock().expect("probe gate"),
                                    Duration::from_secs(10),
                                    |released| !*released,
                                )
                                .expect("probe gate wait");
                            assert!(!timeout.timed_out(), "test releases both expiry probes");
                        }
                    }
                });
                Ok(connection)
            })
        }
    });
    let manager = AsyncDieselConnectionManager::new_with_config(
        support::durable_database_url().expect("dedicated test URL"),
        config,
    );
    let claim_pool = diesel_async::pooled_connection::bb8::Pool::builder()
        .max_size(2)
        .build(manager)
        .await
        .expect("instrumented claim pool");
    (claim_pool, arrivals, released)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_candidates_are_rechecked_after_renewal_or_completion() {
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{
        persistence::{WorkflowRow, WorkflowStatus},
        schema::{durable_workflow, durable_workflow_event},
    };

    for current_status in [WorkflowStatus::Running, WorkflowStatus::Succeeded] {
        let Some(pool) = support::fresh_pool().await else {
            return;
        };
        let workflow_id = durable_workflows::DurableStore::new(pool.clone())
            .start(
                &ContinueWorkflow,
                durable_workflows::StartOptions::default(),
            )
            .await
            .expect("workflow starts")
            .workflow_id;
        let mut connection = pool.get().await.expect("fixture connection");
        diesel::update(durable_workflow::table.find(workflow_id.get()))
            .set((
                durable_workflow::status.eq(WorkflowStatus::Running),
                durable_workflow::lease_token.eq(Some("original-lease")),
                durable_workflow::lease_expires_at.eq(Some(0_i64)),
            ))
            .execute(&mut connection)
            .await
            .expect("expired workflow fixture");
        let (claim_pool, mut arrivals, released) = paused_expiry_probe_pool().await;
        let coordinator = durable_workflows::WorkflowCoordinator::new(
            claim_pool,
            Arc::new(()),
            Arc::new(
                durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
                    .expect("registry"),
            ),
            Arc::new(durable_workflows::ActivityRegistry::new()),
            "stale-expiry-probe",
            CoordinatorConfig::default(),
        )
        .expect("coordinator");
        let claim = claim_with_blocking_probe(coordinator);
        let probe = tokio::time::timeout(Duration::from_secs(5), arrivals.recv()).await;
        let future_expiry = durable_workflows::persistence::now_millis() + 60_000;
        let lease = (current_status == WorkflowStatus::Running).then_some("renewed-lease");
        let expiry = (current_status == WorkflowStatus::Running).then_some(future_expiry);
        let update = tokio::time::timeout(
            Duration::from_secs(3),
            diesel::update(durable_workflow::table.find(workflow_id.get()))
                .set((
                    durable_workflow::status.eq(current_status),
                    durable_workflow::lease_token.eq(lease),
                    durable_workflow::lease_expires_at.eq(expiry),
                ))
                .execute(&mut connection),
        )
        .await;
        *released.0.lock().expect("release probe gate") = true;
        released.1.notify_all();
        probe
            .expect("expiry discovery completes")
            .expect("expiry probe observed");
        update
            .expect("discovery must not lock the candidate")
            .expect("concurrent lease/state update");
        assert!(claim
            .await
            .expect("claim task")
            .expect("claim result")
            .is_none());
        let row = durable_workflow::table
            .find(workflow_id.get())
            .select(WorkflowRow::as_select())
            .first::<WorkflowRow>(&mut connection)
            .await
            .expect("current workflow");
        assert_eq!(row.status, current_status);
        assert_eq!(row.lease_token.as_deref(), lease);
        assert_eq!(row.lease_expires_at, expiry);
        let recoveries = durable_workflow_event::table
            .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
            .filter(durable_workflow_event::event_type.eq("lease_recovered"))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .expect("recovery history");
        assert_eq!(
            recoveries, 0,
            "stale discovery must not recover a renewed or completed workflow"
        );
    }
}

fn claim_with_blocking_probe(
    coordinator: durable_workflows::WorkflowCoordinator<()>,
) -> tokio::task::JoinHandle<
    Result<Option<durable_workflows::WorkflowClaim>, durable_workflows::DurableError>,
> {
    let runtime = tokio::runtime::Handle::current();
    // Synchronous instrumentation must not block Tokio's local task queues.
    tokio::task::spawn_blocking(move || runtime.block_on(coordinator.claim_one()))
}

#[tokio::test]
async fn expired_lease_recovery_advances_past_locked_candidate_pages() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::{AsyncConnection, RunQueryDsl};
    use durable_workflows::{
        persistence::WorkflowStatus,
        schema::{durable_workflow, durable_workflow_event},
    };

    for unlocked_candidates in [0, 1] {
        let Some(pool) = support::fresh_pool().await else {
            return;
        };
        let store = durable_workflows::DurableStore::new(pool.clone());
        let mut workflow_ids = Vec::new();
        for _ in 0..(32 + unlocked_candidates) {
            workflow_ids.push(
                store
                    .start(
                        &ContinueWorkflow,
                        durable_workflows::StartOptions::default(),
                    )
                    .await
                    .expect("workflow starts")
                    .workflow_id
                    .get(),
            );
        }
        let mut connection = pool.get().await.expect("fixture connection");
        diesel::update(durable_workflow::table)
            .set((
                durable_workflow::status.eq(WorkflowStatus::Running),
                durable_workflow::lease_token.eq(Some("expired-lease")),
                durable_workflow::lease_expires_at.eq(Some(0_i64)),
            ))
            .execute(&mut connection)
            .await
            .expect("expired workflows with tied lease timestamps");
        let coordinator = durable_workflows::WorkflowCoordinator::new(
            pool.clone(),
            Arc::new(()),
            Arc::new(
                durable_workflows::register_durable_workflows!(() ; ContinueWorkflow)
                    .expect("registry"),
            ),
            Arc::new(durable_workflows::ActivityRegistry::new()),
            "locked-expiry-page",
            CoordinatorConfig::default(),
        )
        .expect("coordinator");
        let claim = connection
            .transaction(async |connection| {
                for id in &workflow_ids[..32] {
                    durable_workflow::table
                        .find(*id)
                        .for_update()
                        .select(durable_workflow::id)
                        .first::<i64>(connection)
                        .await?;
                }
                let claim = tokio::time::timeout(Duration::from_secs(5), coordinator.claim_one())
                    .await
                    .expect("claim finishes while the oldest page stays locked")
                    .expect("claim succeeds");
                Ok::<_, diesel::result::Error>(claim)
            })
            .await
            .expect("blocker transaction");
        assert_eq!(
            claim.map(|claim| claim.workflow_id().expect("claimed id").get()),
            workflow_ids.get(32).copied(),
            "recovery must inspect the unlocked candidate beyond the first page"
        );
        let unchanged = durable_workflow::table
            .filter(durable_workflow::id.eq_any(&workflow_ids[..32]))
            .filter(durable_workflow::status.eq(WorkflowStatus::Running))
            .filter(durable_workflow::lease_token.eq(Some("expired-lease")))
            .filter(durable_workflow::lease_expires_at.eq(Some(0_i64)))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .expect("locked workflows remain unchanged");
        assert_eq!(unchanged, 32);
        let recovered_ids = durable_workflow_event::table
            .filter(durable_workflow_event::event_type.eq("lease_recovered"))
            .select(durable_workflow_event::workflow_id)
            .load::<i64>(&mut connection)
            .await
            .expect("recovery history");
        assert_eq!(
            recovered_ids,
            workflow_ids
                .get(32)
                .copied()
                .into_iter()
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn library_transactions_pin_read_committed_before_begin() {
    use diesel::connection::InstrumentationEvent;
    use diesel_async::{
        pooled_connection::{AsyncDieselConnectionManager, ManagerConfig},
        AsyncConnection,
    };
    use std::sync::Mutex;

    #[cfg(feature = "mysql")]
    const SET_READ_COMMITTED: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
    #[cfg(feature = "postgres")]
    const BEGIN_READ_COMMITTED: &str = "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED";
    const TOP_LEVEL_BEGIN: &str = "<begin depth 1>";

    let Some(_pool) = support::fresh_pool().await else {
        return;
    };
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut config = ManagerConfig::<durable_workflows::DurableConnection>::default();
    config.custom_setup = Box::new({
        let log = log.clone();
        move |url| {
            let log = log.clone();
            Box::pin(async move {
                let mut connection = durable_workflows::DurableConnection::establish(url).await?;
                connection.set_instrumentation(move |event: InstrumentationEvent<'_>| {
                    let entry = match event {
                        InstrumentationEvent::StartQuery { query, .. } => query.to_string(),
                        InstrumentationEvent::BeginTransaction { depth, .. } => {
                            format!("<begin depth {depth}>")
                        }
                        _ => return,
                    };
                    log.lock().expect("sql log").push(entry);
                });
                Ok(connection)
            })
        }
    });
    let manager = AsyncDieselConnectionManager::new_with_config(
        support::durable_database_url().expect("dedicated test URL"),
        config,
    );
    let pool = diesel_async::pooled_connection::bb8::Pool::builder()
        .max_size(2)
        .build(manager)
        .await
        .expect("recording pool");

    durable_workflows::DurableStore::new(pool.clone())
        .start(
            &ActivityWorkflow,
            durable_workflows::StartOptions::default(),
        )
        .await
        .expect("workflow starts");
    let coordinator = durable_workflows::WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(
            durable_workflows::register_durable_workflows!(() ; ActivityWorkflow)
                .expect("registry"),
        ),
        Arc::new(
            durable_workflows::register_durable_activities!(() ; TestActivity)
                .expect("activity registry"),
        ),
        "isolation-probe-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator");
    let claim = coordinator
        .claim_one()
        .await
        .expect("workflow claim")
        .expect("workflow is claimable");
    coordinator
        .activate_claim(claim)
        .await
        .expect("activation commits");
    let worker = durable_workflows::ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(
            durable_workflows::register_durable_activities!(() ; TestActivity)
                .expect("activity registry"),
        ),
        Arc::new(durable_workflows::register_durable_topics!(Topics::External).expect("topics")),
        "isolation-probe-worker",
        durable_workflows::WorkerConfig::default(),
    )
    .expect("worker");
    worker
        .claim_one("external")
        .await
        .expect("activity claim")
        .expect("activity is claimable");

    let log = log.lock().expect("sql log").clone();
    let top_level_begins = log.iter().filter(|entry| *entry == TOP_LEVEL_BEGIN).count();
    // Start, workflow claim, transition commit, activity claim.
    assert!(
        top_level_begins >= 4,
        "expected at least four library transactions: {log:#?}"
    );
    // MySQL: `SET TRANSACTION ISOLATION LEVEL ...` must immediately precede
    // each top-level BEGIN.
    #[cfg(feature = "mysql")]
    for (index, entry) in log.iter().enumerate() {
        if entry == TOP_LEVEL_BEGIN {
            assert_eq!(
                index.checked_sub(1).map(|previous| log[previous].as_str()),
                Some(SET_READ_COMMITTED),
                "transaction at {index} is not pinned to READ COMMITTED: {log:#?}"
            );
        }
        if entry == SET_READ_COMMITTED {
            assert_eq!(
                log.get(index + 1).map(String::as_str),
                Some(TOP_LEVEL_BEGIN),
                "isolation statement at {index} does not open a top-level transaction: {log:#?}"
            );
        }
    }
    // Postgres: diesel-async records the begin event, then runs the BEGIN
    // statement that carries the isolation level.
    #[cfg(feature = "postgres")]
    for (index, entry) in log.iter().enumerate() {
        if entry == TOP_LEVEL_BEGIN {
            assert_eq!(
                log.get(index + 1).map(String::as_str),
                Some(BEGIN_READ_COMMITTED),
                "transaction at {index} is not pinned to READ COMMITTED: {log:#?}"
            );
        }
    }
}
