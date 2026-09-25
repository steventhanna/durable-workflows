mod support;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    admin::{AdminControlService, Operator},
    persistence::{ScheduleRunRow, ScheduleStateRow},
    schema::{durable_schedule_run, durable_schedule_state, durable_workflow},
    ActivityRegistry, DurableError, DurableSchedule, DurableStore, DurableWorkflow, MisfirePolicy,
    OverlapPolicy, ScheduleHandler, ScheduleMaterializer, ScheduleRegistry, ScheduleRunId,
    StartOptions, WorkflowContext, WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowId,
    WorkflowRegistry, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ScheduledWorkflow;

impl DurableWorkflow for ScheduledWorkflow {
    const KIND: &'static str = "scheduled_test_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ScheduledWorkflow {
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

macro_rules! schedule {
    ($name:ident, $key:literal, $misfire:expr, $overlap:expr, $fails:expr) => {
        struct $name;

        impl DurableSchedule for $name {
            const KEY: &'static str = $key;
            const VERSION: i32 = 1;
            const CRON: &'static str = "0 0 8 * * *";
            const TIMEZONE: &'static str = "UTC";
            const MISFIRE: MisfirePolicy = $misfire;
            const OVERLAP: OverlapPolicy = $overlap;
            const MISFIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(300);
        }

        #[async_trait]
        impl ScheduleHandler for $name {
            type Context = ();

            async fn start_occurrence(
                _context: &Self::Context,
                connection: &mut DurableConnection,
                schedule_run_id: ScheduleRunId,
                _scheduled_for: i64,
            ) -> Result<WorkflowId, DurableError> {
                if $fails {
                    return Err(DurableError::InvalidState("scheduled start failed".into()));
                }
                Ok(DurableStore::start_with_conn(
                    connection,
                    &ScheduledWorkflow,
                    StartOptions {
                        schedule_run_id: Some(schedule_run_id),
                        ..StartOptions::default()
                    },
                )
                .await?
                .workflow_id)
            }
        }
    };
}

schedule!(
    LatestSchedule,
    "latest_schedule",
    MisfirePolicy::RunLatest,
    OverlapPolicy::Allow,
    false
);
schedule!(
    FailingSchedule,
    "failing_schedule",
    MisfirePolicy::RunLatest,
    OverlapPolicy::Allow,
    true
);
schedule!(
    SkipSchedule,
    "skip_schedule",
    MisfirePolicy::Skip,
    OverlapPolicy::Allow,
    false
);
schedule!(
    CatchUpSchedule,
    "catch_up_schedule",
    MisfirePolicy::CatchUp { max_occurrences: 2 },
    OverlapPolicy::Allow,
    false
);

struct GapSchedule;

impl DurableSchedule for GapSchedule {
    const KEY: &'static str = "gap_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "0 30 2 * * *";
    const TIMEZONE: &'static str = "America/Denver";
    const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(300);
}

struct ScanBoundSchedule;

impl DurableSchedule for ScanBoundSchedule {
    const KEY: &'static str = "scan_bound_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = "0 * * * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(300);
}

struct FailingRecoverySchedule;

impl DurableSchedule for FailingRecoverySchedule {
    const KEY: &'static str = "failing_recovery_schedule";
    const VERSION: i32 = 1;
    const CRON: &'static str = ScanBoundSchedule::CRON;
    const TIMEZONE: &'static str = ScanBoundSchedule::TIMEZONE;
    const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: std::time::Duration = ScanBoundSchedule::MISFIRE_GRACE;
}

#[async_trait]
impl ScheduleHandler for FailingRecoverySchedule {
    type Context = ();

    async fn start_occurrence(
        context: &Self::Context,
        connection: &mut DurableConnection,
        schedule_run_id: ScheduleRunId,
        scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        FailingSchedule::start_occurrence(context, connection, schedule_run_id, scheduled_for).await
    }
}

macro_rules! successful_handler {
    ($name:ident) => {
        #[async_trait]
        impl ScheduleHandler for $name {
            type Context = ();

            async fn start_occurrence(
                _context: &Self::Context,
                connection: &mut DurableConnection,
                schedule_run_id: ScheduleRunId,
                _scheduled_for: i64,
            ) -> Result<WorkflowId, DurableError> {
                Ok(DurableStore::start_with_conn(
                    connection,
                    &ScheduledWorkflow,
                    StartOptions {
                        schedule_run_id: Some(schedule_run_id),
                        ..StartOptions::default()
                    },
                )
                .await?
                .workflow_id)
            }
        }
    };
}

successful_handler!(GapSchedule);
successful_handler!(ScanBoundSchedule);

fn registry() -> Arc<ScheduleRegistry<()>> {
    let mut registry = ScheduleRegistry::new();
    registry.register::<LatestSchedule>().expect("latest");
    registry.register::<FailingSchedule>().expect("failing");
    registry.register::<SkipSchedule>().expect("skip");
    registry.register::<CatchUpSchedule>().expect("catch up");
    registry.register::<GapSchedule>().expect("gap");
    registry
        .register::<FailingRecoverySchedule>()
        .expect("failing recovery");
    registry
        .register::<ScanBoundSchedule>()
        .expect("scan bound");
    Arc::new(registry)
}

fn at(year: i32, month: u32, day: u32, hour: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
        .single()
        .expect("UTC instant")
        .timestamp_millis()
}

async fn state(pool: &durable_workflows::DurablePool, key: &str) -> ScheduleStateRow {
    let mut connection = pool.get().await.expect("connection");
    durable_schedule_state::table
        .find(key)
        .select(ScheduleStateRow::as_select())
        .first(&mut connection)
        .await
        .expect("state")
}

async fn runs(pool: &durable_workflows::DurablePool, key: &str) -> Vec<ScheduleRunRow> {
    let mut connection = pool.get().await.expect("connection");
    durable_schedule_run::table
        .filter(durable_schedule_run::schedule_key.eq(key))
        .order(durable_schedule_run::scheduled_for.asc())
        .select(ScheduleRunRow::as_select())
        .load(&mut connection)
        .await
        .expect("runs")
}

#[tokio::test]
async fn run_latest_records_backlog_starts_one_and_is_multi_instance_exactly_once() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    let deployed_at = at(2026, 1, 1, 0);
    registry
        .reconcile_state(LatestSchedule::KEY, &pool, deployed_at)
        .await
        .expect("state");
    let first = Arc::new(ScheduleMaterializer::new(
        pool.clone(),
        Arc::new(()),
        registry.clone(),
    ));
    let second = Arc::new(ScheduleMaterializer::new(
        pool.clone(),
        Arc::new(()),
        registry,
    ));
    let now = at(2026, 1, 3, 9);
    let (left, right) = tokio::join!(
        first.materialize_schedule(LatestSchedule::KEY, now),
        second.materialize_schedule(LatestSchedule::KEY, now),
    );
    let outcomes = [left.expect("left"), right.expect("right")];
    assert_eq!(
        outcomes.iter().map(|outcome| outcome.started).sum::<u32>(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome.coalesced)
            .sum::<u32>(),
        2
    );

    let rows = runs(&pool, LatestSchedule::KEY).await;
    assert_eq!(
        rows.iter()
            .map(|row| row.status.as_str())
            .collect::<Vec<_>>(),
        vec!["coalesced", "coalesced", "started"]
    );
    let started = rows.last().expect("started run");
    let workflow_schedule_run = durable_workflow::table
        .filter(durable_workflow::schedule_run_id.eq(Some(started.id)))
        .select(durable_workflow::schedule_run_id)
        .first::<Option<i64>>(&mut pool.get().await.expect("connection"))
        .await
        .expect("scheduled workflow");
    assert_eq!(workflow_schedule_run, Some(started.id));
    let schedule_state = state(&pool, LatestSchedule::KEY).await;
    assert_eq!(schedule_state.next_local_occurrence, "2026-01-04T08:00:00");
    assert!(schedule_state.next_occurrence_at > now);
}

#[tokio::test]
async fn failed_start_rolls_back_run_history_and_cadence_for_retry() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    let deployed_at = at(2026, 1, 1, 0);
    registry
        .reconcile_state(FailingSchedule::KEY, &pool, deployed_at)
        .await
        .expect("state");
    let before = state(&pool, FailingSchedule::KEY).await;
    let error = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry)
        .materialize_schedule(FailingSchedule::KEY, at(2026, 1, 1, 9))
        .await
        .expect_err("start fails");
    assert!(matches!(error, DurableError::InvalidState(_)));
    assert!(runs(&pool, FailingSchedule::KEY).await.is_empty());
    let after = state(&pool, FailingSchedule::KEY).await;
    assert_eq!(after.next_local_occurrence, before.next_local_occurrence);
    assert_eq!(after.next_occurrence_at, before.next_occurrence_at);
}

#[tokio::test]
async fn skip_and_catch_up_apply_grace_and_bounded_chronological_starts() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    let deployed_at = at(2026, 1, 1, 0);
    for key in [SkipSchedule::KEY, CatchUpSchedule::KEY] {
        registry
            .reconcile_state(key, &pool, deployed_at)
            .await
            .expect("state");
    }
    let materializer = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry);
    let now = at(2026, 1, 3, 8) + 60_000;
    let skip = materializer
        .materialize_schedule(SkipSchedule::KEY, now)
        .await
        .expect("skip policy");
    assert_eq!((skip.started, skip.skipped, skip.coalesced), (1, 2, 0));
    assert_eq!(
        runs(&pool, SkipSchedule::KEY)
            .await
            .iter()
            .map(|run| run.status.as_str())
            .collect::<Vec<_>>(),
        vec!["skipped", "skipped", "started"]
    );

    let catch_up = materializer
        .materialize_schedule(CatchUpSchedule::KEY, now)
        .await
        .expect("catch-up policy");
    assert_eq!(
        (catch_up.started, catch_up.skipped, catch_up.coalesced),
        (2, 1, 0)
    );
    assert_eq!(
        runs(&pool, CatchUpSchedule::KEY)
            .await
            .iter()
            .map(|run| run.status.as_str())
            .collect::<Vec<_>>(),
        vec!["skipped", "started", "started"]
    );
}

#[tokio::test]
async fn dst_gap_is_recorded_and_over_bound_backlog_recovers_atomically() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    registry
        .reconcile_state(GapSchedule::KEY, &pool, at(2026, 3, 8, 0))
        .await
        .expect("gap state");
    let materializer = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry.clone());
    let gap = materializer
        .materialize_schedule(GapSchedule::KEY, at(2026, 3, 8, 10))
        .await
        .expect("gap history");
    assert_eq!((gap.started, gap.skipped), (0, 1));
    let gap_runs = runs(&pool, GapSchedule::KEY).await;
    assert_eq!(gap_runs[0].status, "skipped");
    assert_eq!(gap_runs[0].reason.as_deref(), Some("dst_gap"));

    let scan_start = at(2026, 1, 1, 0);
    registry
        .reconcile_state(ScanBoundSchedule::KEY, &pool, scan_start)
        .await
        .expect("scan state");
    let before = state(&pool, ScanBoundSchedule::KEY).await;
    let beyond_bound = scan_start + 10_001 * 60_000;
    let controls = AdminControlService::new(
        pool.clone(),
        Arc::new(WorkflowRegistry::new()),
        Arc::new(ActivityRegistry::new()),
    )
    .with_schedules(Arc::new(()), registry.clone());
    let operator = Operator::new("42", "Exercise paused backlog recovery").expect("operator");
    controls
        .pause_schedule(ScanBoundSchedule::KEY, &operator)
        .await
        .expect("pause");
    assert!(
        materializer
            .materialize_schedule(ScanBoundSchedule::KEY, beyond_bound)
            .await
            .expect("paused tick")
            .paused
    );
    assert_eq!(
        state(&pool, ScanBoundSchedule::KEY)
            .await
            .next_occurrence_at,
        before.next_occurrence_at
    );
    controls
        .resume_schedule(ScanBoundSchedule::KEY, &operator)
        .await
        .expect("resume");

    let first = materializer
        .materialize_schedule(ScanBoundSchedule::KEY, beyond_bound)
        .await
        .expect("first recovery chunk");
    assert_eq!(
        (first.inspected, first.started, first.coalesced),
        (10_000, 0, 10_000)
    );
    let after = state(&pool, ScanBoundSchedule::KEY).await;
    assert!(after.next_local_occurrence > before.next_local_occurrence);
    assert_eq!(after.next_occurrence_at, beyond_bound);
    assert_eq!(runs(&pool, ScanBoundSchedule::KEY).await.len(), 10_000);
    let second = materializer
        .materialize_schedule(ScanBoundSchedule::KEY, beyond_bound)
        .await
        .expect("finish recovery");
    assert_eq!((second.inspected, second.started), (1, 1));
    assert!(
        state(&pool, ScanBoundSchedule::KEY)
            .await
            .next_occurrence_at
            > beyond_bound
    );
    let repeated = materializer
        .materialize_schedule(ScanBoundSchedule::KEY, beyond_bound)
        .await
        .expect("idempotent retry");
    assert_eq!((repeated.inspected, repeated.started), (0, 0));
}

#[tokio::test]
async fn failed_recovery_chunk_keeps_prior_progress_and_rolls_back_its_own_cursor() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    let start = at(2026, 1, 1, 0);
    registry
        .reconcile_state(FailingRecoverySchedule::KEY, &pool, start)
        .await
        .expect("state");
    let materializer = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry);
    let now = start + 10_001 * 60_000;
    let first = materializer
        .materialize_schedule(FailingRecoverySchedule::KEY, now)
        .await
        .expect("safe prefix");
    assert_eq!(first.coalesced, 10_000);
    let before_failure = state(&pool, FailingRecoverySchedule::KEY).await;
    for _ in 0..2 {
        assert!(matches!(
            materializer
                .materialize_schedule(FailingRecoverySchedule::KEY, now)
                .await,
            Err(DurableError::InvalidState(_))
        ));
        let after = state(&pool, FailingRecoverySchedule::KEY).await;
        assert_eq!(
            after.next_local_occurrence,
            before_failure.next_local_occurrence
        );
        assert_eq!(after.next_occurrence_at, before_failure.next_occurrence_at);
        assert_eq!(
            runs(&pool, FailingRecoverySchedule::KEY).await.len(),
            10_000
        );
    }
}
