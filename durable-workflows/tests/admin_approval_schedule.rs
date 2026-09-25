mod support;

use std::sync::Arc;

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    admin::{AdminControlService, Operator, ScheduleRegistry},
    persistence::{
        ApprovalRow, NewApprovalRow, NewScheduleStateRow, ScheduleRunRow, WorkflowEventRow,
        WorkflowRow,
    },
    schema::{
        durable_approval, durable_schedule_run, durable_schedule_state, durable_workflow,
        durable_workflow_event,
    },
    ActivityRegistry, DurableError, DurableSchedule, DurableStore, DurableWorkflow, MisfirePolicy,
    OverlapPolicy, ScheduleHandler, ScheduleRunId, StartOptions, WorkflowContext, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowRegistry, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ApprovalWorkflow {
    value: i32,
}

impl DurableWorkflow for ApprovalWorkflow {
    const KIND: &'static str = "admin_approval_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ApprovalWorkflow {
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

struct ManualSchedule;

impl DurableSchedule for ManualSchedule {
    const KEY: &'static str = "manual_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "0 0 0 * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::Skip;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(60);
}

#[async_trait]
impl ScheduleHandler for ManualSchedule {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        Ok(DurableStore::start_with_conn(
            connection,
            &ApprovalWorkflow { value: 19 },
            StartOptions {
                schedule_run_id: Some(schedule_run_id),
                ..StartOptions::default()
            },
        )
        .await?
        .workflow_id)
    }
}

struct FailingSchedule;

impl DurableSchedule for FailingSchedule {
    const KEY: &'static str = "failing_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "0 0 0 * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::Skip;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(60);
}

#[async_trait]
impl ScheduleHandler for FailingSchedule {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        _connection: &mut DurableConnection,
        _schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        Err(DurableError::InvalidState(
            "test schedule failed to start".to_string(),
        ))
    }
}

fn operator(reason: &str) -> Operator {
    Operator::new("42", reason).expect("operator")
}

fn workflow_registry() -> Arc<WorkflowRegistry<()>> {
    let mut registry = WorkflowRegistry::new();
    registry
        .register::<ApprovalWorkflow>()
        .expect("workflow registration");
    Arc::new(registry)
}

fn schedule_fingerprint<S>() -> String
where
    S: ScheduleHandler<Context = ()>,
{
    let mut schedules = ScheduleRegistry::<()>::new();
    schedules.register::<S>().expect("schedule definition");
    schedules
        .get(S::KEY)
        .expect("registered schedule metadata")
        .fingerprint
        .clone()
}

async fn start_workflow(pool: &durable_workflows::DurablePool) -> WorkflowId {
    DurableStore::new(pool.clone())
        .start(&ApprovalWorkflow { value: 7 }, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id
}

async fn insert_approval(
    pool: &durable_workflows::DurablePool,
    workflow_id: WorkflowId,
    expires_at: Option<i64>,
) -> durable_workflows::ApprovalId {
    let now = durable_workflows::persistence::now_millis();
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_approval::table)
        .values(NewApprovalRow {
            workflow_id: workflow_id.get(),
            command_sequence: 1,
            kind: ApprovalWorkflow::KIND.to_string(),
            version: ApprovalWorkflow::VERSION,
            prompt_metadata_json: r#"{"prompt":"safe"}"#.to_string(),
            validation_schema_json: r#"{"type":"boolean"}"#.to_string(),
            validation_version: 1,
            status: "pending".to_string(),
            requested_at: now,
            expires_at,
            decision_payload_json: None,
            decided_by: None,
            operator_reason: None,
            resolved_at: None,
        })
        .execute(&mut connection)
        .await
        .expect("approval insert");
    let approval_id = durable_approval::table
        .select(durable_approval::id)
        .order(durable_approval::id.desc())
        .first::<i64>(&mut connection)
        .await
        .expect("approval ID");
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq("waiting_approval"),
            durable_workflow::wait_kind.eq(Some("approval".to_string())),
            durable_workflow::wait_reference_id.eq(Some(approval_id)),
            durable_workflow::command_sequence.eq(1),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut connection)
        .await
        .expect("workflow approval wait");
    durable_workflows::ApprovalId::new(approval_id).expect("approval")
}

#[tokio::test]
async fn approval_resolution_validates_exact_version_and_wakes_with_a_typed_event() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let approval_id = insert_approval(
        &pool,
        workflow_id,
        Some(durable_workflows::persistence::now_millis() + 60_000),
    )
    .await;
    let service = AdminControlService::new(
        pool.clone(),
        workflow_registry(),
        Arc::new(ActivityRegistry::new()),
    );
    let outcome = service
        .resolve_approval(approval_id, "true", &operator("Manual approval recorded"))
        .await
        .expect("approval resolution");
    assert_eq!(outcome.status, "resolved");
    assert!(matches!(
        service
            .resolve_approval(approval_id, "false", &operator("Duplicate decision"))
            .await,
        Err(DurableError::Conflict(_))
    ));

    let mut connection = pool.get().await.expect("test connection");
    let approval = durable_approval::table
        .find(approval_id.get())
        .select(ApprovalRow::as_select())
        .first::<ApprovalRow>(&mut connection)
        .await
        .expect("approval");
    assert_eq!(approval.status, "resolved");
    assert_eq!(approval.decision_payload_json.as_deref(), Some("true"));
    assert_eq!(approval.decided_by, Some(42));
    let workflow = durable_workflow::table
        .find(workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow");
    assert_eq!(workflow.status.as_str(), "ready");
    assert!(workflow.wait_reference_id.is_none());
    let approval_event = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::delivery_sequence.gt(workflow.delivered_event_sequence))
        .order(durable_workflow_event::delivery_sequence.asc())
        .select(WorkflowEventRow::as_select())
        .first::<WorkflowEventRow>(&mut connection)
        .await
        .expect("approval event");
    assert_eq!(approval_event.actor_type.as_deref(), Some("operator"));
    assert_eq!(approval_event.actor_id.as_deref(), Some("42"));
    assert_eq!(
        approval_event.reason.as_deref(),
        Some("Manual approval recorded")
    );
    let event_json = approval_event.metadata_json.expect("event metadata");
    let event: WorkflowEvent = serde_json::from_str(&event_json).expect("typed event");
    assert!(matches!(event, WorkflowEvent::ApprovalResolved { .. }));
}

#[tokio::test]
async fn expired_or_invalid_approval_decisions_roll_back_without_waking() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let approval_id = insert_approval(
        &pool,
        workflow_id,
        Some(durable_workflows::persistence::now_millis() - 1),
    )
    .await;
    let service = AdminControlService::new(
        pool.clone(),
        workflow_registry(),
        Arc::new(ActivityRegistry::new()),
    );
    assert!(matches!(
        service
            .resolve_approval(approval_id, "true", &operator("Late decision"))
            .await,
        Err(DurableError::Conflict(_))
    ));

    let mut connection = pool.get().await.expect("test connection");
    diesel::update(durable_approval::table.find(approval_id.get()))
        .set(durable_approval::expires_at.eq(None::<i64>))
        .execute(&mut connection)
        .await
        .expect("remove expiry");
    drop(connection);
    assert!(matches!(
        service
            .resolve_approval(
                approval_id,
                r#""not-a-boolean""#,
                &operator("Invalid decision")
            )
            .await,
        Err(DurableError::InvalidPayload {
            field: "approval decision",
            ..
        })
    ));
    let mut connection = pool.get().await.expect("test connection");
    let approval = durable_approval::table
        .find(approval_id.get())
        .select(ApprovalRow::as_select())
        .first::<ApprovalRow>(&mut connection)
        .await
        .expect("approval");
    assert_eq!(approval.status, "pending");
    assert!(approval.decision_payload_json.is_none());
}

#[tokio::test]
async fn schedule_controls_are_code_owned_and_run_now_is_atomic_without_cadence_drift() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let next_occurrence = durable_workflows::persistence::now_millis() + 86_400_000;
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_schedule_state::table)
        .values(NewScheduleStateRow {
            schedule_key: ManualSchedule::KEY.to_string(),
            definition_fingerprint: schedule_fingerprint::<ManualSchedule>(),
            definition_version: ManualSchedule::VERSION,
            next_local_occurrence: "2099-01-01T00:00:00".to_string(),
            next_occurrence_at: next_occurrence,
            last_materialized_at: None,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            created_at: next_occurrence - 1_000,
            updated_at: next_occurrence - 1_000,
        })
        .execute(&mut connection)
        .await
        .expect("schedule state");
    drop(connection);
    let mut schedules = ScheduleRegistry::new();
    schedules
        .register::<ManualSchedule>()
        .expect("schedule definition");
    let service = AdminControlService::new(
        pool.clone(),
        workflow_registry(),
        Arc::new(ActivityRegistry::new()),
    )
    .with_schedules(Arc::new(()), Arc::new(schedules));
    assert!(
        service
            .pause_schedule(ManualSchedule::KEY, &operator("Pause scheduled work"))
            .await
            .expect("pause")
            .paused
    );
    assert!(matches!(
        service
            .pause_schedule(ManualSchedule::KEY, &operator("Duplicate pause"))
            .await,
        Err(DurableError::Conflict(_))
    ));
    assert!(
        !service
            .resume_schedule(ManualSchedule::KEY, &operator("Resume scheduled work"))
            .await
            .expect("resume")
            .paused
    );
    assert!(matches!(
        service
            .pause_schedule("unregistered_schedule", &operator("Invalid schedule"))
            .await,
        Err(DurableError::NotFound { .. })
    ));
    let run = service
        .run_schedule_now(ManualSchedule::KEY, &operator("Operational backfill"))
        .await
        .expect("run now");

    let mut connection = pool.get().await.expect("test connection");
    let state_next = durable_schedule_state::table
        .find(ManualSchedule::KEY)
        .select(durable_schedule_state::next_occurrence_at)
        .first::<i64>(&mut connection)
        .await
        .expect("next occurrence");
    assert_eq!(state_next, next_occurrence);
    let schedule_run = durable_schedule_run::table
        .find(run.schedule_run_id.get())
        .select(ScheduleRunRow::as_select())
        .first::<ScheduleRunRow>(&mut connection)
        .await
        .expect("schedule run");
    assert_eq!(schedule_run.workflow_id, Some(run.workflow_id.get()));
    assert_eq!(schedule_run.actor_id, Some(42));
    let workflow = durable_workflow::table
        .find(run.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("manual workflow");
    assert_eq!(workflow.schedule_run_id, Some(run.schedule_run_id.get()));
    let operator_event = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(run.workflow_id.get()))
        .filter(durable_workflow_event::event_type.eq("schedule_run_now"))
        .select(WorkflowEventRow::as_select())
        .first::<WorkflowEventRow>(&mut connection)
        .await
        .expect("run-now operator event");
    assert_eq!(operator_event.actor_id.as_deref(), Some("42"));
    assert_eq!(
        operator_event.reason.as_deref(),
        Some("Operational backfill")
    );
}

#[tokio::test]
async fn schedule_controls_reject_unregistered_and_mismatched_definitions() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = durable_workflows::persistence::now_millis();
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_schedule_state::table)
        .values(NewScheduleStateRow {
            schedule_key: ManualSchedule::KEY.to_string(),
            definition_fingerprint:
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
            definition_version: ManualSchedule::VERSION,
            next_local_occurrence: "2099-01-01T00:00:00".to_string(),
            next_occurrence_at: now + 60_000,
            last_materialized_at: None,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            created_at: now,
            updated_at: now,
        })
        .execute(&mut connection)
        .await
        .expect("schedule state");
    drop(connection);

    let unregistered = AdminControlService::new(
        pool.clone(),
        workflow_registry(),
        Arc::new(ActivityRegistry::new()),
    )
    .with_schedules(Arc::new(()), Arc::new(ScheduleRegistry::new()));
    assert!(matches!(
        unregistered
            .pause_schedule(ManualSchedule::KEY, &operator("Pause unknown schedule"))
            .await,
        Err(DurableError::NotFound { .. })
    ));

    let mut schedules = ScheduleRegistry::new();
    schedules
        .register::<ManualSchedule>()
        .expect("schedule definition");
    let mismatched =
        AdminControlService::new(pool, workflow_registry(), Arc::new(ActivityRegistry::new()))
            .with_schedules(Arc::new(()), Arc::new(schedules));
    assert!(matches!(
        mismatched
            .pause_schedule(ManualSchedule::KEY, &operator("Pause mismatched schedule"))
            .await,
        Err(DurableError::Conflict(_))
    ));
}

#[tokio::test]
async fn failed_manual_schedule_start_rolls_back_the_run_row() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = durable_workflows::persistence::now_millis();
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_schedule_state::table)
        .values(NewScheduleStateRow {
            schedule_key: FailingSchedule::KEY.to_string(),
            definition_fingerprint: schedule_fingerprint::<FailingSchedule>(),
            definition_version: FailingSchedule::VERSION,
            next_local_occurrence: "2099-01-01T00:00:00".to_string(),
            next_occurrence_at: now + 60_000,
            last_materialized_at: None,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            created_at: now,
            updated_at: now,
        })
        .execute(&mut connection)
        .await
        .expect("schedule state");
    drop(connection);

    let mut schedules = ScheduleRegistry::new();
    schedules
        .register::<FailingSchedule>()
        .expect("schedule definition");
    let service = AdminControlService::new(
        pool.clone(),
        workflow_registry(),
        Arc::new(ActivityRegistry::new()),
    )
    .with_schedules(Arc::new(()), Arc::new(schedules));
    assert!(matches!(
        service
            .run_schedule_now(FailingSchedule::KEY, &operator("Test atomic rollback"))
            .await,
        Err(DurableError::InvalidState(_))
    ));

    let mut connection = pool.get().await.expect("test connection");
    let run_count = durable_schedule_run::table
        .filter(durable_schedule_run::schedule_key.eq(FailingSchedule::KEY))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .expect("schedule run count");
    assert_eq!(run_count, 0);
}
