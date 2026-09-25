mod support;

use std::sync::Arc;

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    admin::{AdminControlService, Operator},
    persistence::{ApprovalRow, NewApprovalRow, WorkflowEventRow, WorkflowRow},
    schema::{durable_approval, durable_workflow, durable_workflow_event},
    ActivityRegistry, ApprovalExpiryMaterializer, ApprovalId, DurableError, DurableStore,
    DurableWorkflow, StartOptions, TimerMaterializer, WorkflowContext, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowRegistry, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TemporalWorkflow;

impl DurableWorkflow for TemporalWorkflow {
    const KIND: &'static str = "temporal_test_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for TemporalWorkflow {
    type Context = ();
    type State = ();
    type Approval = bool;
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

async fn start(pool: &durable_workflows::DurablePool) -> WorkflowId {
    DurableStore::new(pool.clone())
        .start(&TemporalWorkflow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id
}

async fn workflow(pool: &durable_workflows::DurablePool, id: WorkflowId) -> WorkflowRow {
    let mut connection = pool.get().await.expect("connection");
    durable_workflow::table
        .find(id.get())
        .select(WorkflowRow::as_select())
        .first(&mut connection)
        .await
        .expect("workflow")
}

async fn deliverable_events(
    pool: &durable_workflows::DurablePool,
    id: WorkflowId,
    event_type: &str,
) -> Vec<WorkflowEventRow> {
    let mut connection = pool.get().await.expect("connection");
    durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(id.get()))
        .filter(durable_workflow_event::event_type.eq(event_type))
        .order(durable_workflow_event::sequence.asc())
        .select(WorkflowEventRow::as_select())
        .load(&mut connection)
        .await
        .expect("events")
}

#[tokio::test]
async fn timers_wake_once_at_the_exact_command_and_preserve_pause() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = 1_800_000_000_000_i64;
    let due = start(&pool).await;
    let future = start(&pool).await;
    let paused = start(&pool).await;
    let mut connection = pool.get().await.expect("connection");
    for (id, status, available_at, command) in [
        (due, "sleeping", now, 7_i32),
        (future, "sleeping", now + 60_000, 8_i32),
        (paused, "paused", now + 1, 9_i32),
    ] {
        diesel::update(durable_workflow::table.find(id.get()))
            .set((
                durable_workflow::status.eq(status),
                durable_workflow::wait_kind.eq(Some("timer".to_string())),
                durable_workflow::wait_reference_id.eq(Some(i64::from(command))),
                durable_workflow::available_at.eq(available_at),
                durable_workflow::command_sequence.eq(command),
                durable_workflow::delivered_event_sequence.eq(1),
            ))
            .execute(&mut connection)
            .await
            .expect("timer state");
    }
    drop(connection);

    let first = Arc::new(TimerMaterializer::new(pool.clone()));
    let second = Arc::new(TimerMaterializer::new(pool.clone()));
    let (left, right) = tokio::join!(first.wake_one(now), second.wake_one(now));
    let outcomes = [left.expect("left"), right.expect("right")];
    assert_eq!(outcomes.iter().filter(|id| **id == Some(due)).count(), 1);
    assert!(outcomes.contains(&None));

    let due_row = workflow(&pool, due).await;
    assert_eq!(due_row.status.as_str(), "ready");
    assert!(due_row.wait_kind.is_none());
    assert!(due_row.wait_reference_id.is_none());
    assert_eq!(workflow(&pool, future).await.status.as_str(), "sleeping");
    assert_eq!(workflow(&pool, paused).await.status.as_str(), "paused");
    let events = deliverable_events(&pool, due, "timer_fired").await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].delivery_sequence, Some(2));
    assert_eq!(
        serde_json::from_str::<WorkflowEvent>(
            events[0].metadata_json.as_deref().expect("typed metadata")
        )
        .expect("event"),
        WorkflowEvent::TimerFired {
            command_sequence: 7
        }
    );
    assert_eq!(first.wake_one(now + 1).await.expect("paused timer"), None);
    let paused_row = workflow(&pool, paused).await;
    assert_eq!(paused_row.status.as_str(), "paused");
    assert_eq!(paused_row.wait_kind.as_deref(), Some("timer"));
    assert!(deliverable_events(&pool, paused, "timer_fired")
        .await
        .is_empty());
    diesel::update(durable_workflow::table.find(paused.get()))
        .set(durable_workflow::status.eq("sleeping"))
        .execute(&mut pool.get().await.expect("resume connection"))
        .await
        .expect("resume timer");
    assert_eq!(
        first.wake_one(now + 1).await.expect("resumed timer"),
        Some(paused)
    );
    assert_eq!(
        deliverable_events(&pool, paused, "timer_fired").await.len(),
        1
    );
}

#[tokio::test]
async fn an_inconsistent_due_timer_rolls_back_without_clearing_the_wait() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = 1_800_000_000_000_i64;
    let id = start(&pool).await;
    diesel::update(durable_workflow::table.find(id.get()))
        .set((
            durable_workflow::status.eq("sleeping"),
            durable_workflow::wait_kind.eq(Some("timer".to_string())),
            durable_workflow::wait_reference_id.eq(Some(8_i64)),
            durable_workflow::available_at.eq(now),
            durable_workflow::command_sequence.eq(7),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut pool.get().await.expect("connection"))
        .await
        .expect("timer state");

    assert!(matches!(
        TimerMaterializer::new(pool.clone()).wake_one(now).await,
        Err(DurableError::InvalidState(_))
    ));
    assert_eq!(workflow(&pool, id).await.status.as_str(), "sleeping");
    assert!(deliverable_events(&pool, id, "timer_fired")
        .await
        .is_empty());
}

async fn seed_approval(
    pool: &durable_workflows::DurablePool,
    workflow_id: WorkflowId,
    status: &str,
    command: i32,
    expires_at: i64,
) -> ApprovalId {
    let mut connection = pool.get().await.expect("connection");
    diesel::insert_into(durable_approval::table)
        .values(NewApprovalRow {
            workflow_id: workflow_id.get(),
            command_sequence: command,
            kind: TemporalWorkflow::KIND.to_string(),
            version: TemporalWorkflow::VERSION,
            prompt_metadata_json: r#"{"prompt":"safe"}"#.to_string(),
            validation_schema_json: "{}".to_string(),
            validation_version: 1,
            status: "pending".to_string(),
            requested_at: expires_at - 1_000,
            expires_at: Some(expires_at),
            decision_payload_json: None,
            decided_by: None,
            operator_reason: None,
            resolved_at: None,
        })
        .execute(&mut connection)
        .await
        .expect("approval");
    let approval_id = durable_approval::table
        .filter(durable_approval::workflow_id.eq(workflow_id.get()))
        .order(durable_approval::id.desc())
        .select(durable_approval::id)
        .first::<i64>(&mut connection)
        .await
        .expect("approval id");
    let approval_id = ApprovalId::new(approval_id).expect("typed approval ID");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq(status),
            durable_workflow::wait_kind.eq(Some("approval".to_string())),
            durable_workflow::wait_reference_id.eq(Some(approval_id.get())),
            durable_workflow::command_sequence.eq(command),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut connection)
        .await
        .expect("approval wait");
    approval_id
}

#[tokio::test]
async fn approval_expiry_is_typed_atomic_race_safe_and_preserves_pause() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = 1_800_000_000_000_i64;
    let waiting = start(&pool).await;
    let waiting_approval = seed_approval(&pool, waiting, "waiting_approval", 4, now).await;
    let first = Arc::new(ApprovalExpiryMaterializer::new(pool.clone()));
    let second = Arc::new(ApprovalExpiryMaterializer::new(pool.clone()));
    let (left, right) = tokio::join!(first.expire_one(now), second.expire_one(now));
    let outcomes = [left.expect("left"), right.expect("right")];
    assert_eq!(
        outcomes
            .iter()
            .filter(|id| **id == Some(waiting_approval))
            .count(),
        1
    );
    assert!(outcomes.contains(&None));
    assert_eq!(workflow(&pool, waiting).await.status.as_str(), "ready");
    let events = deliverable_events(&pool, waiting, "approval_expired").await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        serde_json::from_str::<WorkflowEvent>(
            events[0].metadata_json.as_deref().expect("typed metadata")
        )
        .expect("event"),
        WorkflowEvent::ApprovalExpired {
            command_sequence: 4
        }
    );

    let paused = start(&pool).await;
    let paused_approval = seed_approval(&pool, paused, "paused", 5, now).await;
    assert_eq!(
        ApprovalExpiryMaterializer::new(pool.clone())
            .expire_one(now)
            .await
            .expect("paused expiry"),
        Some(paused_approval)
    );
    let paused_row = workflow(&pool, paused).await;
    assert_eq!(paused_row.status.as_str(), "paused");
    assert!(paused_row.wait_kind.is_none());
    assert_eq!(
        deliverable_events(&pool, paused, "approval_expired")
            .await
            .len(),
        1
    );

    let mut connection = pool.get().await.expect("connection");
    let approval = durable_approval::table
        .find(paused_approval.get())
        .select(ApprovalRow::as_select())
        .first::<ApprovalRow>(&mut connection)
        .await
        .expect("approval");
    assert_eq!(approval.status, "expired");
    assert_eq!(approval.resolved_at, Some(now));
}

#[tokio::test]
async fn approval_resolution_and_expiry_have_one_transactional_winner() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = durable_workflows::persistence::now_millis();
    let workflow_id = start(&pool).await;
    let approval_id = seed_approval(&pool, workflow_id, "waiting_approval", 6, now + 60_000).await;
    let mut workflows = WorkflowRegistry::<()>::new();
    workflows
        .register::<TemporalWorkflow>()
        .expect("workflow registry");
    let controls = AdminControlService::new(
        pool.clone(),
        Arc::new(workflows),
        Arc::new(ActivityRegistry::new()),
    );
    let expiry = ApprovalExpiryMaterializer::new(pool.clone());
    let operator = Operator::new("42", "resolve versus expiry race").expect("operator");

    let (expired, resolved) = tokio::join!(
        expiry.materialize_one(now + 120_000),
        controls.resolve_approval(approval_id, "true", &operator),
    );
    let expiry_won = matches!(expired, Ok(Some(id)) if id == approval_id);
    let resolution_won = resolved.is_ok();
    assert_ne!(expiry_won, resolution_won);

    let mut connection = pool.get().await.expect("connection");
    let approval = durable_approval::table
        .find(approval_id.get())
        .select(ApprovalRow::as_select())
        .first::<ApprovalRow>(&mut connection)
        .await
        .expect("approval");
    assert!(matches!(approval.status.as_str(), "expired" | "resolved"));
    let temporal_events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(
            durable_workflow_event::event_type.eq_any(["approval_expired", "approval_resolved"]),
        )
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("temporal event count");
    assert_eq!(temporal_events, 1);
}
