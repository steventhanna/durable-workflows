mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use durable_workflows::{
    admin::{AdminControlService, Operator},
    persistence::{ActivityAttemptRow, ActivityRow, NewActivityRow, WorkflowEventRow, WorkflowRow},
    schema::{
        durable_activity, durable_activity_attempt, durable_workflow, durable_workflow_event,
    },
    ActivityContext, ActivityError, ActivityHandler, ActivityRegistry, ActivityTopic,
    ActivityWorker, CoordinatorConfig, DurableActivity, DurableError, DurableStore,
    DurableWorkflow, RetryPolicy, StartOptions, TopicRegistry, WorkerConfig, WorkflowContext,
    WorkflowCoordinator, WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowId,
    WorkflowRegistry, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ControlWorkflowV1 {
    value: i32,
}

impl DurableWorkflow for ControlWorkflowV1 {
    const KIND: &'static str = "control_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ControlWorkflowV1 {
    type Context = ();
    type State = i32;
    type Approval = bool;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ControlWorkflowV2 {
    value: i32,
    label: String,
}

impl DurableWorkflow for ControlWorkflowV2 {
    const KIND: &'static str = "control_workflow";
    const VERSION: i32 = 2;
}

#[async_trait]
impl WorkflowHandler for ControlWorkflowV2 {
    type Context = ();
    type State = String;
    type Approval = String;
    type Output = String;

    fn initial_state(&self) -> Self::State {
        format!("{}:{}", self.label, self.value)
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

#[derive(Debug, Clone, Copy)]
enum ControlTopic {
    External,
}

impl ActivityTopic for ControlTopic {
    fn key(self) -> &'static str {
        "control_external"
    }

    fn max_concurrency(self) -> u32 {
        2
    }
}

macro_rules! control_activity {
    ($name:ident, $version:expr, $attempts:expr) => {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct $name {
            value: i32,
        }

        impl DurableActivity for $name {
            type Topic = ControlTopic;

            const KIND: &'static str = "control_activity";
            const VERSION: i32 = $version;
            const MAX_ATTEMPTS: u32 = $attempts;
            const TIMEOUT: Duration = Duration::from_secs(5);
            const LEASE_DURATION: Duration = Duration::from_secs(10);

            fn topic() -> Self::Topic {
                ControlTopic::External
            }

            fn retry_policy() -> RetryPolicy {
                RetryPolicy::fixed(10).expect("retry policy")
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
                Ok(self.value)
            }
        }
    };
}

control_activity!(ControlActivityV1, 1, 2);
control_activity!(ControlActivityV2, 2, 4);

fn operator(reason: &str) -> Operator {
    Operator::new("42", reason).expect("operator")
}

fn registries() -> (WorkflowRegistry<()>, ActivityRegistry<()>, TopicRegistry) {
    let mut workflows = WorkflowRegistry::new();
    workflows
        .register::<ControlWorkflowV1>()
        .expect("workflow v1");
    workflows
        .register::<ControlWorkflowV2>()
        .expect("workflow v2");
    let mut activities = ActivityRegistry::new();
    activities
        .register::<ControlActivityV1>()
        .expect("activity v1");
    activities
        .register::<ControlActivityV2>()
        .expect("activity v2");
    let mut topics = TopicRegistry::new();
    topics
        .register(ControlTopic::External)
        .expect("control topic");
    (workflows, activities, topics)
}

async fn start_workflow(pool: &durable_workflows::DurablePool) -> WorkflowId {
    DurableStore::new(pool.clone())
        .start(
            &ControlWorkflowV1 { value: 7 },
            StartOptions::default().with_deduplication_key("source-dedup"),
        )
        .await
        .expect("workflow start")
        .workflow_id
}

async fn insert_activity(
    pool: &durable_workflows::DurablePool,
    workflow_id: WorkflowId,
    status: &str,
) -> i64 {
    let now = 10_000;
    let mut connection = pool.get().await.expect("connection");
    diesel::insert_into(durable_activity::table)
        .values(NewActivityRow {
            workflow_id: workflow_id.get(),
            command_sequence: 1,
            replacement_number: 0,
            kind: "control_activity".to_string(),
            version: 1,
            topic: "control_external".to_string(),
            payload_json: r#"{"value":11}"#.to_string(),
            status: durable_workflows::persistence::ActivityStatus::try_from(status)
                .expect("valid fixture status"),
            available_at: 0,
            max_attempts: 2,
            attempt_count: 0,
            timeout_millis: 5_000,
            lease_duration_millis: 10_000,
            retry_policy_json: serde_json::to_string(
                &RetryPolicy::fixed(10).expect("retry policy"),
            )
            .expect("retry JSON"),
            operation_key: Some("provider-operation-11".to_string()),
            provider_result_json: None,
            last_error_category: (status == "dead_lettered").then(|| "provider".to_string()),
            last_error_message: (status == "dead_lettered").then(|| "failed".to_string()),
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
            root_activity_id: None,
            replaces_activity_id: None,
            created_at: now,
            updated_at: now,
            completed_at: (status == "dead_lettered").then_some(now),
        })
        .execute(&mut connection)
        .await
        .expect("activity insert");
    durable_activity::table
        .select(durable_activity::id)
        .order(durable_activity::id.desc())
        .first(&mut connection)
        .await
        .expect("activity ID")
}

async fn set_waiting_activity(
    pool: &durable_workflows::DurablePool,
    workflow_id: WorkflowId,
    activity_id: i64,
    status: &str,
) {
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq(status),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(activity_id)),
            durable_workflow::command_sequence.eq(1),
        ))
        .execute(&mut connection)
        .await
        .expect("workflow wait");
}

#[tokio::test]
async fn activity_claim_requires_the_parent_to_wait_for_that_exact_activity() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, workflow_id, "pending").await;
    set_waiting_activity(&pool, workflow_id, activity_id, "paused").await;
    let (_, activities, topics) = registries();
    let worker = ActivityWorker::new(
        pool,
        Arc::new(()),
        Arc::new(activities),
        Arc::new(topics),
        "worker-1",
        WorkerConfig::default(),
    )
    .expect("worker");
    assert!(worker
        .claim_one("control_external")
        .await
        .expect("claim")
        .is_none());
}

#[tokio::test]
async fn activity_claim_skips_a_locked_workflow_without_locking_the_activity() {
    assert_activity_claim_lock_order(false).await;
}

#[tokio::test]
async fn batch_activity_claim_skips_a_locked_workflow_without_locking_the_activity() {
    assert_activity_claim_lock_order(true).await;
}

async fn assert_activity_claim_lock_order(batch: bool) {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, workflow_id, "pending").await;
    set_waiting_activity(&pool, workflow_id, activity_id, "waiting_activity").await;
    let next_workflow_id = DurableStore::new(pool.clone())
        .start(
            &ControlWorkflowV1 { value: 8 },
            StartOptions::default().with_deduplication_key("next-source-dedup"),
        )
        .await
        .expect("next workflow start")
        .workflow_id;
    let next_activity_id = insert_activity(&pool, next_workflow_id, "pending").await;
    set_waiting_activity(
        &pool,
        next_workflow_id,
        next_activity_id,
        "waiting_activity",
    )
    .await;
    let (_, activities, topics) = registries();
    let worker = ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(activities),
        Arc::new(topics),
        "lock-order-worker",
        WorkerConfig::default(),
    )
    .expect("worker");

    let mut operator_connection = pool.get().await.expect("operator connection");
    operator_connection
        .batch_execute("BEGIN")
        .await
        .expect("operator transaction");
    durable_workflow::table
        .find(workflow_id.get())
        .for_update()
        .select(durable_workflow::id)
        .first::<i64>(&mut operator_connection)
        .await
        .expect("workflow lock");

    let claim = tokio::spawn(async move {
        if batch {
            worker
                .claim_batch(
                    1,
                    &[("control_external".to_string(), 1)].into_iter().collect(),
                )
                .await
                .map(|mut claims| claims.pop())
        } else {
            worker.claim_one("control_external").await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let activity_lock = tokio::time::timeout(
        Duration::from_secs(1),
        durable_activity::table
            .find(activity_id)
            .for_update()
            .select(durable_activity::id)
            .first::<i64>(&mut operator_connection),
    )
    .await;
    operator_connection
        .batch_execute("ROLLBACK")
        .await
        .expect("operator rollback");

    assert!(
        matches!(activity_lock, Ok(Ok(id)) if id == activity_id),
        "an operator holding the workflow lock must be able to lock the activity without deadlocking against a claimant"
    );
    let claim = claim
        .await
        .expect("claim task")
        .expect("claim")
        .expect("claimant must continue past the locked workflow");
    assert_eq!(
        claim.activity_id().expect("claimed activity").get(),
        next_activity_id
    );
}

#[tokio::test]
async fn pause_fences_a_workflow_transition_claimed_before_the_operator_action() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let (workflows, activities, _) = registries();
    let workflows = Arc::new(workflows);
    let activities = Arc::new(activities);
    let coordinator = WorkflowCoordinator::new(
        pool.clone(),
        Arc::new(()),
        workflows.clone(),
        activities.clone(),
        "control-coordinator",
        CoordinatorConfig::default(),
    )
    .expect("coordinator");
    let claim = coordinator
        .claim_one()
        .await
        .expect("claim")
        .expect("workflow claim");
    let service = AdminControlService::new(pool.clone(), workflows, activities);
    service
        .pause_workflow(workflow_id, &operator("pause stale workflow claim"))
        .await
        .expect("pause");
    assert!(matches!(
        coordinator.activate_claim(claim).await,
        Err(DurableError::FencedWrite)
    ));
}

#[tokio::test]
async fn pause_fences_a_running_activity_and_resume_reopens_the_wait() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, workflow_id, "pending").await;
    set_waiting_activity(&pool, workflow_id, activity_id, "waiting_activity").await;
    let (workflows, activities, topics) = registries();
    let activities = Arc::new(activities);
    let worker = ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        activities.clone(),
        Arc::new(topics),
        "worker-1",
        WorkerConfig::default(),
    )
    .expect("worker");
    let claim = worker
        .claim_one("control_external")
        .await
        .expect("claim")
        .expect("activity claim");
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), activities);
    let outcome = service
        .pause_workflow(workflow_id, &operator("provider maintenance"))
        .await
        .expect("pause");
    assert_eq!(outcome.status, "paused");
    assert!(matches!(
        worker.heartbeat(&claim).await,
        Err(DurableError::FencedWrite)
    ));

    let mut connection = pool.get().await.expect("connection");
    let activity = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("activity");
    assert_eq!(activity.status.as_str(), "pending");
    assert!(activity.lease_token.is_none());
    let attempt = durable_activity_attempt::table
        .find((activity_id, 1))
        .select(ActivityAttemptRow::as_select())
        .first::<ActivityAttemptRow>(&mut connection)
        .await
        .expect("attempt");
    assert_eq!(attempt.outcome.as_deref(), Some("operator_paused"));
    assert!(attempt.finished_at.is_some());
    drop(connection);

    let resumed = service
        .resume_workflow(workflow_id, &operator("maintenance complete"))
        .await
        .expect("resume");
    assert_eq!(resumed.status, "waiting_activity");
}

#[tokio::test]
async fn pausing_the_final_activity_attempt_preserves_one_execution_attempt() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, workflow_id, "pending").await;
    set_waiting_activity(&pool, workflow_id, activity_id, "waiting_activity").await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::max_attempts.eq(1))
        .execute(&mut connection)
        .await
        .expect("single attempt cap");
    drop(connection);
    let (workflows, activities, topics) = registries();
    let activities = Arc::new(activities);
    let worker = ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        activities.clone(),
        Arc::new(topics),
        "single-attempt-worker",
        WorkerConfig::default(),
    )
    .expect("worker");
    worker
        .claim_one("control_external")
        .await
        .expect("claim")
        .expect("final allowed attempt");
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), activities);
    service
        .pause_workflow(workflow_id, &operator("pause final attempt"))
        .await
        .expect("pause");
    service
        .resume_workflow(workflow_id, &operator("resume final attempt"))
        .await
        .expect("resume");

    let replacement_claim = worker
        .claim_one("control_external")
        .await
        .expect("replacement claim")
        .expect("pause must preserve execution capacity");
    assert_eq!(replacement_claim.attempt_number().expect("attempt"), 2);
}

#[tokio::test]
async fn restart_supersedes_paused_source_and_transfers_schedule_origin() {
    use durable_workflows::persistence::{NewScheduleRunRow, NewScheduleStateRow, ScheduleRunRow};
    use durable_workflows::schema::{durable_schedule_run, durable_schedule_state};

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::insert_into(durable_schedule_state::table)
        .values(NewScheduleStateRow {
            schedule_key: "restart_schedule".to_string(),
            definition_fingerprint: "0".repeat(64),
            definition_version: 1,
            next_local_occurrence: "2026-01-01T00:01:00".to_string(),
            next_occurrence_at: 60_000,
            last_materialized_at: Some(0),
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            created_at: 0,
            updated_at: 0,
        })
        .execute(&mut connection)
        .await
        .expect("schedule state");
    diesel::insert_into(durable_schedule_run::table)
        .values(NewScheduleRunRow {
            schedule_key: "restart_schedule".to_string(),
            local_occurrence: "2026-01-01T00:00:00".to_string(),
            scheduled_for: 0,
            materialized_at: 0,
            status: "started".to_string(),
            reason: None,
            actor_id: None,
            workflow_id: Some(workflow_id.get()),
            created_at: 0,
        })
        .execute(&mut connection)
        .await
        .expect("schedule run");
    let schedule_run_id = durable_schedule_run::table
        .select(durable_schedule_run::id)
        .first::<i64>(&mut connection)
        .await
        .expect("schedule run id");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq("paused"),
            durable_workflow::schedule_run_id.eq(Some(schedule_run_id)),
        ))
        .execute(&mut connection)
        .await
        .expect("scheduled paused workflow");
    drop(connection);

    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), Arc::new(activities));
    let restarted = service
        .restart_workflow(workflow_id, &operator("replace paused scheduled workflow"))
        .await
        .expect("restart");

    let mut connection = pool.get().await.expect("connection");
    let source = durable_workflow::table
        .find(workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("source");
    assert_eq!(source.status.as_str(), "cancelled");
    let replacement = durable_workflow::table
        .find(restarted.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("replacement");
    assert_eq!(replacement.schedule_run_id, Some(schedule_run_id));
    let run = durable_schedule_run::table
        .find(schedule_run_id)
        .select(ScheduleRunRow::as_select())
        .first::<ScheduleRunRow>(&mut connection)
        .await
        .expect("schedule run");
    assert_eq!(run.workflow_id, Some(restarted.workflow_id.get()));
    drop(connection);
    assert!(matches!(
        service
            .resume_workflow(workflow_id, &operator("must not revive source"))
            .await,
        Err(DurableError::Conflict(_))
    ));
}

#[tokio::test]
async fn two_simultaneous_pauses_have_one_winner_and_attributed_history() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), Arc::new(activities));
    let first_operator = operator("first operator");
    let second_operator = operator("second operator");
    let (first, second) = tokio::join!(
        service.pause_workflow(workflow_id, &first_operator),
        service.pause_workflow(workflow_id, &second_operator),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let error = first.err().or_else(|| second.err()).expect("one conflict");
    assert!(matches!(error, DurableError::Conflict(_)));

    let mut connection = pool.get().await.expect("connection");
    let events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::event_type.eq("workflow_paused"))
        .select(WorkflowEventRow::as_select())
        .load::<WorkflowEventRow>(&mut connection)
        .await
        .expect("events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor_type.as_deref(), Some("operator"));
    assert_eq!(events[0].actor_id.as_deref(), Some("42"));
}

#[tokio::test]
async fn resuming_an_overdue_timer_does_not_run_without_a_timer_event() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq("sleeping"),
            durable_workflow::wait_kind.eq(Some("timer".to_string())),
            durable_workflow::wait_reference_id.eq(Some(1_i64)),
            durable_workflow::available_at.eq(0_i64),
        ))
        .execute(&mut connection)
        .await
        .expect("timer wait");
    drop(connection);
    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool, Arc::new(workflows), Arc::new(activities));
    service
        .pause_workflow(workflow_id, &operator("pause timer"))
        .await
        .expect("pause");
    let resumed = service
        .resume_workflow(workflow_id, &operator("resume timer"))
        .await
        .expect("resume");
    assert_eq!(resumed.status, "sleeping");
}

#[tokio::test]
async fn cancel_fences_late_activity_work_and_is_not_idempotent() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, workflow_id, "pending").await;
    set_waiting_activity(&pool, workflow_id, activity_id, "waiting_activity").await;
    let (workflows, activities, topics) = registries();
    let activities = Arc::new(activities);
    let worker = ActivityWorker::new(
        pool.clone(),
        Arc::new(()),
        activities.clone(),
        Arc::new(topics),
        "worker-1",
        WorkerConfig::default(),
    )
    .expect("worker");
    let claim = worker
        .claim_one("control_external")
        .await
        .expect("claim")
        .expect("activity claim");
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), activities);
    let cancelled = service
        .cancel_workflow(workflow_id, &operator("customer requested cancellation"))
        .await
        .expect("cancel");
    assert_eq!(cancelled.status, "cancelled");
    assert!(matches!(
        worker.heartbeat(&claim).await,
        Err(DurableError::FencedWrite)
    ));
    assert!(matches!(
        service
            .cancel_workflow(workflow_id, &operator("duplicate cancellation"))
            .await,
        Err(DurableError::Conflict(_))
    ));
}

#[tokio::test]
async fn restart_is_version_pinned_while_correction_selects_current_and_rolls_back_invalid_json() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq("failed"),
            durable_workflow::completed_at.eq(Some(20_000_i64)),
        ))
        .execute(&mut connection)
        .await
        .expect("terminal source");
    drop(connection);
    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), Arc::new(activities));
    let restarted = service
        .restart_workflow(workflow_id, &operator("replay exact version"))
        .await
        .expect("restart");
    assert_eq!(restarted.version, 1);
    assert_ne!(restarted.workflow_id, workflow_id);

    let correction_source = DurableStore::new(pool.clone())
        .start(
            &ControlWorkflowV1 { value: 9 },
            StartOptions::default().with_deduplication_key("correction-source"),
        )
        .await
        .expect("correction source")
        .workflow_id;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(correction_source.get()))
        .set((
            durable_workflow::status.eq("failed"),
            durable_workflow::completed_at.eq(Some(20_001_i64)),
        ))
        .execute(&mut connection)
        .await
        .expect("terminal correction source");
    drop(connection);

    let invalid = service
        .correct_and_restart_workflow(
            correction_source,
            r#"{"value":8}"#,
            &operator("invalid correction"),
        )
        .await;
    assert!(matches!(
        invalid,
        Err(DurableError::InvalidPayload {
            field: "workflow input",
            ..
        })
    ));
    let corrected = service
        .correct_and_restart_workflow(
            correction_source,
            r#"{"value":8,"label":"corrected"}"#,
            &operator("corrected input"),
        )
        .await
        .expect("corrected restart");
    assert_eq!(corrected.version, 2);

    let mut connection = pool.get().await.expect("connection");
    let corrected_row = durable_workflow::table
        .find(corrected.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("corrected workflow");
    assert_eq!(
        corrected_row.root_workflow_id,
        Some(correction_source.get())
    );
    assert_eq!(
        corrected_row.restarted_from_workflow_id,
        Some(correction_source.get())
    );
    assert!(corrected_row.deduplication_key.is_none());
    assert_eq!(corrected_row.state_json, r#""corrected:8""#);
}

#[tokio::test]
async fn restart_is_one_shot_for_an_unscheduled_source() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set(durable_workflow::status.eq("paused"))
        .execute(&mut connection)
        .await
        .expect("pause source");
    drop(connection);

    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), Arc::new(activities));
    let first_operator = operator("first concurrent restart");
    let second_operator = operator("second concurrent restart");
    let (first, second) = tokio::join!(
        service.restart_workflow(workflow_id, &first_operator),
        service.restart_workflow(workflow_id, &second_operator),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.as_ref().err().or_else(|| second.as_ref().err()),
        Some(DurableError::Conflict(_))
    ));
    assert!(matches!(
        service
            .restart_workflow(workflow_id, &operator("sequential duplicate restart"))
            .await,
        Err(DurableError::Conflict(_))
    ));

    let mut connection = pool.get().await.expect("connection");
    let child_count = durable_workflow::table
        .filter(durable_workflow::restarted_from_workflow_id.eq(Some(workflow_id.get())))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("restart child count");
    assert_eq!(child_count, 1);
}

#[tokio::test]
async fn retry_creates_immutable_lineage_and_correction_changes_version_and_operation_key() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, workflow_id, "dead_lettered").await;
    set_waiting_activity(&pool, workflow_id, activity_id, "blocked").await;
    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), Arc::new(activities));
    let retried = service
        .retry_activity(
            durable_workflows::ActivityId::new(activity_id).expect("activity ID"),
            &operator("provider recovered"),
        )
        .await
        .expect("retry");
    assert_eq!(retried.version, 1);
    assert_eq!(
        retried.operation_key.as_deref(),
        Some("provider-operation-11")
    );

    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_activity::table.find(retried.activity_id.get()))
        .set((
            durable_activity::status.eq("dead_lettered"),
            durable_activity::last_error_category.eq(Some("provider".to_string())),
            durable_activity::last_error_message.eq(Some("failed again".to_string())),
            durable_activity::completed_at.eq(Some(30_000_i64)),
        ))
        .execute(&mut connection)
        .await
        .expect("replacement failure");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set(durable_workflow::status.eq("blocked"))
        .execute(&mut connection)
        .await
        .expect("re-block workflow");
    drop(connection);

    let invalid = service
        .correct_and_retry_activity(
            retried.activity_id,
            r#"{"value":"invalid"}"#,
            &operator("invalid corrected payload"),
        )
        .await;
    assert!(matches!(
        invalid,
        Err(DurableError::InvalidPayload {
            field: "activity payload",
            ..
        })
    ));

    let corrected = service
        .correct_and_retry_activity(
            retried.activity_id,
            r#"{"value":99}"#,
            &operator("corrected activity payload"),
        )
        .await
        .expect("corrected retry");
    assert_eq!(corrected.version, 2);
    assert_ne!(corrected.operation_key, retried.operation_key);

    let mut connection = pool.get().await.expect("connection");
    let row = durable_activity::table
        .find(corrected.activity_id.get())
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("replacement");
    assert_eq!(row.root_activity_id, Some(activity_id));
    assert_eq!(row.replaces_activity_id, Some(retried.activity_id.get()));
    assert_eq!(row.replacement_number, 2);
    let workflow = durable_workflow::table
        .find(workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow");
    assert_eq!(workflow.status.as_str(), "waiting_activity");
    assert_eq!(
        workflow.wait_reference_id,
        Some(corrected.activity_id.get())
    );
}

#[tokio::test]
async fn recoverable_start_fences_dead_letter_retry_and_races_to_one_live_generation() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let source = start_workflow(&pool).await;
    let activity_id = insert_activity(&pool, source, "dead_lettered").await;
    set_waiting_activity(&pool, source, activity_id, "blocked").await;

    let recovery = DurableStore::new(pool.clone())
        .start_or_restart_recoverable(
            &ControlWorkflowV1 { value: 7 },
            StartOptions::default().with_deduplication_key("source-dedup"),
        )
        .await
        .expect("blocked source recovers");
    assert!(recovery.inserted);

    let (workflows, activities, _) = registries();
    let service = AdminControlService::new(pool.clone(), Arc::new(workflows), Arc::new(activities));
    assert!(matches!(
        service
            .retry_activity(
                durable_workflows::ActivityId::new(activity_id).expect("activity ID"),
                &operator("late dead-letter retry"),
            )
            .await,
        Err(DurableError::Conflict(_))
    ));

    let race_source = DurableStore::new(pool.clone())
        .start(
            &ControlWorkflowV1 { value: 8 },
            StartOptions::default().with_deduplication_key("source-race"),
        )
        .await
        .expect("race source starts")
        .workflow_id;
    let race_activity_id = insert_activity(&pool, race_source, "dead_lettered").await;
    set_waiting_activity(&pool, race_source, race_activity_id, "blocked").await;
    let store = DurableStore::new(pool.clone());
    let retry_operator = operator("concurrent dead-letter retry");
    let (race_recovery, race_retry) = tokio::join!(
        store.start_or_restart_recoverable(
            &ControlWorkflowV1 { value: 8 },
            StartOptions::default().with_deduplication_key("source-race"),
        ),
        service.retry_activity(
            durable_workflows::ActivityId::new(race_activity_id).expect("race activity ID"),
            &retry_operator,
        ),
    );
    let race_recovery = race_recovery.expect("race recovery resolves");
    assert_eq!(
        usize::from(race_recovery.inserted) + usize::from(race_retry.is_ok()),
        1
    );

    let mut connection = pool.get().await.expect("connection");
    let source_status = durable_workflow::table
        .find(source.get())
        .select(durable_workflow::status)
        .first::<String>(&mut connection)
        .await
        .expect("source status");
    let source_activity_status = durable_activity::table
        .find(activity_id)
        .select(durable_activity::status)
        .first::<String>(&mut connection)
        .await
        .expect("source activity status");
    assert_eq!(source_status, "cancelled");
    assert_eq!(source_activity_status, "cancelled");
    let race_live = durable_workflow::table
        .filter(
            durable_workflow::id
                .eq(race_source.get())
                .or(durable_workflow::root_workflow_id.eq(Some(race_source.get()))),
        )
        .filter(durable_workflow::status.ne_all(["succeeded", "failed", "cancelled"]))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("race live generations");
    assert_eq!(race_live, 1);
}
