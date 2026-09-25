mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    persistence::NewApprovalRow,
    schema::{durable_approval, durable_schedule_run, durable_workflow, durable_workflow_event},
    ActivityRegistry, DurableError, DurableRuntime, DurableSchedule, DurableStore, DurableWorkflow,
    MisfirePolicy, OverlapPolicy, RuntimeConfig, ScheduleHandler, ScheduleRegistry, ScheduleRunId,
    StartOptions, TopicRegistry, WorkflowContext, WorkflowError, WorkflowEvent, WorkflowHandler,
    WorkflowId, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TemporalRuntimeWorkflow;

impl DurableWorkflow for TemporalRuntimeWorkflow {
    const KIND: &'static str = "temporal_runtime_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for TemporalRuntimeWorkflow {
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

struct HealthySchedule;

impl DurableSchedule for HealthySchedule {
    const KEY: &'static str = "runtime_healthy_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "* * * * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: Duration = Duration::from_secs(30);
}

#[async_trait]
impl ScheduleHandler for HealthySchedule {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        Ok(DurableStore::start_with_conn(
            connection,
            &TemporalRuntimeWorkflow,
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
    const KEY: &'static str = "runtime_failing_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "* * * * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: Duration = Duration::from_secs(30);
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
            "intentional schedule failure".to_string(),
        ))
    }
}

async fn start_workflow(pool: &durable_workflows::DurablePool) -> WorkflowId {
    DurableStore::new(pool.clone())
        .start(&TemporalRuntimeWorkflow, StartOptions::default())
        .await
        .expect("workflow start")
        .workflow_id
}

async fn seed_timer(pool: &durable_workflows::DurablePool, now: i64) -> WorkflowId {
    let workflow_id = start_workflow(pool).await;
    diesel::update(durable_workflow::table.find(workflow_id.get()))
        .set((
            durable_workflow::status.eq("sleeping"),
            durable_workflow::wait_kind.eq(Some("timer".to_string())),
            durable_workflow::wait_reference_id.eq(Some(1_i64)),
            durable_workflow::available_at.eq(now.saturating_sub(1)),
            durable_workflow::command_sequence.eq(1),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut pool.get().await.expect("timer connection"))
        .await
        .expect("timer state");
    workflow_id
}

async fn seed_expired_approval(pool: &durable_workflows::DurablePool, now: i64) -> WorkflowId {
    let workflow_id = start_workflow(pool).await;
    let mut connection = pool.get().await.expect("approval connection");
    diesel::insert_into(durable_approval::table)
        .values(NewApprovalRow {
            workflow_id: workflow_id.get(),
            command_sequence: 1,
            kind: TemporalRuntimeWorkflow::KIND.to_string(),
            version: TemporalRuntimeWorkflow::VERSION,
            prompt_metadata_json: r#"{"prompt":"safe"}"#.to_string(),
            validation_schema_json: "{}".to_string(),
            validation_version: 1,
            status: "pending".to_string(),
            requested_at: now.saturating_sub(1_000),
            expires_at: Some(now.saturating_sub(1)),
            decision_payload_json: None,
            decided_by: None,
            operator_reason: None,
            resolved_at: None,
        })
        .execute(&mut connection)
        .await
        .expect("approval insert");
    let approval_id = durable_approval::table
        .filter(durable_approval::workflow_id.eq(workflow_id.get()))
        .select(durable_approval::id)
        .first::<i64>(&mut connection)
        .await
        .expect("approval id");
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
        .expect("approval state");
    workflow_id
}

async fn event_count(
    pool: &durable_workflows::DurablePool,
    workflow_id: WorkflowId,
    event_type: &str,
) -> i64 {
    durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::event_type.eq(event_type))
        .count()
        .get_result(&mut pool.get().await.expect("event connection"))
        .await
        .expect("event count")
}

async fn schedule_run_count(pool: &durable_workflows::DurablePool, key: &str) -> i64 {
    durable_schedule_run::table
        .filter(durable_schedule_run::schedule_key.eq(key))
        .count()
        .get_result(&mut pool.get().await.expect("schedule connection"))
        .await
        .expect("schedule run count")
}

#[tokio::test]
async fn runtime_supervises_temporal_sources_isolates_schedules_and_stops_cleanly() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let now = durable_workflows::persistence::now_millis();
    let timer = seed_timer(&pool, now).await;
    let approval = seed_expired_approval(&pool, now).await;

    let mut schedules = ScheduleRegistry::new();
    schedules
        .register::<HealthySchedule>()
        .expect("healthy schedule");
    schedules
        .register::<FailingSchedule>()
        .expect("failing schedule");
    let schedules = Arc::new(schedules);
    for key in [HealthySchedule::KEY, FailingSchedule::KEY] {
        schedules
            .reconcile_state(key, &pool, now.saturating_sub(3_000))
            .await
            .expect("schedule state");
    }

    let workflows = durable_workflows::register_durable_workflows!(
        ();
        TemporalRuntimeWorkflow
    )
    .expect("workflow registry");
    let runtime = DurableRuntime::new(
        pool.clone(),
        Arc::new(()),
        Arc::new(workflows),
        Arc::new(ActivityRegistry::new()),
        Arc::new(TopicRegistry::new()),
        "temporal-runtime-test",
        RuntimeConfig {
            idle_delay: Duration::from_millis(5),
            timer_poll_interval: Duration::from_millis(5),
            approval_expiry_poll_interval: Duration::from_millis(5),
            schedule_poll_interval: Duration::from_millis(10),
            ..RuntimeConfig::default()
        },
    )
    .expect("runtime definition")
    .with_schedules(schedules);
    let handle = runtime.spawn().await.expect("runtime ready");

    let completed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if event_count(&pool, timer, "timer_fired").await == 1
                && event_count(&pool, approval, "approval_expired").await == 1
                && schedule_run_count(&pool, HealthySchedule::KEY).await >= 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "runtime did not materialize all due work"
    );
    assert_eq!(schedule_run_count(&pool, FailingSchedule::KEY).await, 0);

    handle
        .shutdown(Duration::from_secs(2))
        .await
        .expect("clean runtime shutdown");
    let runs_after_shutdown = schedule_run_count(&pool, HealthySchedule::KEY).await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        schedule_run_count(&pool, HealthySchedule::KEY).await,
        runs_after_shutdown,
        "shutdown left a schedule poller running"
    );

    let mut connection = pool.get().await.expect("cleanup connection");
    support::drop_durable_tables(&mut connection).await;
}
