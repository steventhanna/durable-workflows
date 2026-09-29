mod support;

use durable_workflows::DbMillis;
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use diesel::{QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    persistence::{ScheduleRunRow, ScheduleStateRow},
    schema::{durable_schedule_run, durable_schedule_state},
    DurableError, DurableSchedule, DurableStore, DurableWorkflow, MisfirePolicy, OverlapPolicy,
    ScheduleHandler, ScheduleMaterializer, ScheduleRegistry, ScheduleRunId,
    ScheduleStateReconcileOutcome, StartOptions, WorkflowContext, WorkflowError, WorkflowEvent,
    WorkflowHandler, WorkflowId, WorkflowTransition,
};

macro_rules! schedule_definition {
    ($name:ident, $version:expr, $cron:expr) => {
        struct $name;

        impl DurableSchedule for $name {
            const KEY: &'static str = "versioned_schedule";
            const VERSION: i32 = $version;
            const CRON: &'static str = $cron;
            const TIMEZONE: &'static str = "America/Denver";
            const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
            const OVERLAP: OverlapPolicy = OverlapPolicy::QueueOne;
            const MISFIRE_GRACE: Duration = Duration::from_secs(300);
        }

        #[async_trait]
        impl ScheduleHandler for $name {
            type Context = ();

            async fn start_occurrence(
                _context: &Self::Context,
                _connection: &mut DurableConnection,
                _schedule_run_id: ScheduleRunId,
                _scheduled_for: i64,
            ) -> Result<WorkflowId, DurableError> {
                WorkflowId::new(1)
            }
        }
    };
}

schedule_definition!(ScheduleV1, 1, "0 0 8 * * *");
schedule_definition!(ScheduleV2, 2, "0 30 9 * * *");
schedule_definition!(ScheduleV2Drift, 2, "0 45 9 * * *");

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct FoldWorkflow;

impl DurableWorkflow for FoldWorkflow {
    const KIND: &'static str = "fold_schedule_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for FoldWorkflow {
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

/// A RunLatest schedule that starts a real workflow (runs reference it).
macro_rules! started_schedule {
    ($name:ident, $version:expr, $cron:expr, $zone:expr) => {
        struct $name;

        impl DurableSchedule for $name {
            const KEY: &'static str = "started_schedule";
            const VERSION: i32 = $version;
            const CRON: &'static str = $cron;
            const TIMEZONE: &'static str = $zone;
            const MISFIRE: MisfirePolicy = MisfirePolicy::RunLatest;
            const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
            const MISFIRE_GRACE: Duration = Duration::from_secs(300);
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
                Ok(DurableStore::start_with_conn(
                    connection,
                    &FoldWorkflow,
                    StartOptions::default().with_schedule_run_id(schedule_run_id),
                )
                .await?
                .workflow_id)
            }
        }
    };
}

started_schedule!(FoldV1, 1, "0 30 1 * * *", "America/Denver");
started_schedule!(FoldV2, 2, "0 30 1 * * *", "America/Denver");
started_schedule!(DailyV1, 1, "0 0 8 * * *", "America/Denver");
started_schedule!(EarlierV2, 2, "0 0 7 * * *", "America/Denver");
started_schedule!(TokyoV1, 1, "0 0 * * * *", "Asia/Tokyo");
started_schedule!(UtcV2, 2, "0 0 * * * *", "UTC");

async fn run_occurrences(pool: &durable_workflows::DurablePool) -> Vec<String> {
    let mut connection = pool.get().await.expect("test connection");
    durable_schedule_run::table
        .order(durable_schedule_run::id)
        .select(ScheduleRunRow::as_select())
        .load::<ScheduleRunRow>(&mut connection)
        .await
        .expect("runs")
        .into_iter()
        .map(|run| run.local_occurrence)
        .collect()
}

fn utc(month: u32, day: u32, hour: u32, minute: u32) -> i64 {
    Utc.with_ymd_and_hms(2026, month, day, hour, minute, 0)
        .single()
        .expect("UTC instant")
        .timestamp_millis()
}

fn registry<S>() -> ScheduleRegistry<()>
where
    S: ScheduleHandler<Context = ()>,
{
    let mut registry = ScheduleRegistry::new();
    registry.register::<S>().expect("valid schedule");
    registry
}

async fn persisted_state(pool: &durable_workflows::DurablePool) -> ScheduleStateRow {
    let mut connection = pool.get().await.expect("test connection");
    durable_schedule_state::table
        .select(ScheduleStateRow::as_select())
        .first(&mut connection)
        .await
        .expect("schedule state")
}

#[tokio::test]
async fn concurrent_initialization_inserts_one_state_and_preserves_its_cadence() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = Arc::new(registry::<ScheduleV1>());
    let now = Utc
        .with_ymd_and_hms(2026, 1, 10, 16, 0, 0)
        .single()
        .expect("UTC instant")
        .timestamp_millis();

    let (left, right) = tokio::join!(
        registry.reconcile_state(ScheduleV1::KEY, &pool, DbMillis::from_database_millis(now)),
        registry.reconcile_state(ScheduleV1::KEY, &pool, DbMillis::from_database_millis(now)),
    );
    let outcomes = [
        left.expect("left reconcile"),
        right.expect("right reconcile"),
    ];
    assert!(outcomes.contains(&ScheduleStateReconcileOutcome::Inserted));
    assert!(outcomes.contains(&ScheduleStateReconcileOutcome::Preserved));

    let initial = persisted_state(&pool).await;
    assert_eq!(initial.schedule_key, ScheduleV1::KEY);
    assert_eq!(initial.definition_version, 1);
    assert_eq!(initial.next_local_occurrence, "2026-01-11T08:00:00");

    let later = now + Duration::from_secs(86_400).as_millis() as i64;
    assert_eq!(
        registry
            .reconcile_state(
                ScheduleV1::KEY,
                &pool,
                DbMillis::from_database_millis(later)
            )
            .await
            .expect("idempotent reconcile"),
        ScheduleStateReconcileOutcome::Preserved
    );
    let unchanged = persisted_state(&pool).await;
    assert_eq!(unchanged.next_occurrence_at, initial.next_occurrence_at);
    assert_eq!(
        unchanged.next_local_occurrence,
        initial.next_local_occurrence
    );

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn version_reconciliation_upgrades_once_never_downgrades_and_rejects_drift() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let v1 = registry::<ScheduleV1>();
    let initial_now = Utc
        .with_ymd_and_hms(2026, 1, 10, 16, 0, 0)
        .single()
        .expect("UTC instant")
        .timestamp_millis();
    assert_eq!(
        v1.reconcile_state(
            ScheduleV1::KEY,
            &pool,
            DbMillis::from_database_millis(initial_now)
        )
        .await
        .expect("initial state"),
        ScheduleStateReconcileOutcome::Inserted
    );

    let deployment_now = Utc
        .with_ymd_and_hms(2026, 1, 12, 18, 0, 0)
        .single()
        .expect("UTC instant")
        .timestamp_millis();
    let v2 = registry::<ScheduleV2>();
    assert_eq!(
        v2.reconcile_state(
            ScheduleV2::KEY,
            &pool,
            DbMillis::from_database_millis(deployment_now)
        )
        .await
        .expect("upgrade"),
        ScheduleStateReconcileOutcome::Upgraded
    );
    let upgraded = persisted_state(&pool).await;
    assert_eq!(upgraded.definition_version, 2);
    assert_eq!(upgraded.next_local_occurrence, "2026-01-13T09:30:00");

    assert_eq!(
        v1.reconcile_state(
            ScheduleV1::KEY,
            &pool,
            DbMillis::from_database_millis(deployment_now + 1)
        )
        .await
        .expect("older process observes newer state"),
        ScheduleStateReconcileOutcome::NewerPersisted
    );
    assert_eq!(persisted_state(&pool).await.definition_version, 2);

    let drift = registry::<ScheduleV2Drift>()
        .reconcile_state(
            ScheduleV2Drift::KEY,
            &pool,
            DbMillis::from_database_millis(deployment_now + 1),
        )
        .await
        .expect_err("same-version drift must fail closed");
    assert!(matches!(drift, DurableError::Conflict(_)));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

/// G5: in America/Denver, 2026-11-01 01:00-02:00 repeats (07:00-08:00Z MDT,
/// then 08:00-09:00Z MST). 01:30 fires once, at the earlier pass (07:30Z).
#[tokio::test]
async fn reconcile_in_the_second_pass_of_a_fall_back_hour_yields_a_cursor_after_now() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let second_pass = utc(11, 1, 8, 20);
    assert_eq!(
        registry::<FoldV1>()
            .reconcile_state(
                FoldV1::KEY,
                &pool,
                DbMillis::from_database_millis(second_pass)
            )
            .await
            .expect("initial state"),
        ScheduleStateReconcileOutcome::Inserted
    );
    let state = persisted_state(&pool).await;
    assert_eq!(state.next_local_occurrence, "2026-11-02T01:30:00");
    assert_eq!(state.next_occurrence_at.get(), utc(11, 2, 8, 30));
    assert!(state.next_occurrence_at.get() > second_pass);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

/// G5: an upgrade in the second pass must not re-target the occurrence the
/// first pass materialized; the next tick then succeeds.
#[tokio::test]
async fn upgrade_in_the_second_pass_does_not_retarget_the_materialized_occurrence() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let v1 = Arc::new(registry::<FoldV1>());
    v1.reconcile_state(
        FoldV1::KEY,
        &pool,
        DbMillis::from_database_millis(utc(10, 31, 12, 0)),
    )
    .await
    .expect("initial state");
    assert_eq!(
        persisted_state(&pool).await.next_local_occurrence,
        "2026-11-01T01:30:00"
    );
    let first_pass = ScheduleMaterializer::new(pool.clone(), Arc::new(()), v1)
        .materialize_schedule(
            FoldV1::KEY,
            DbMillis::from_database_millis(utc(11, 1, 7, 35)),
        )
        .await
        .expect("first-pass tick");
    assert_eq!(first_pass.started, 1);

    let second_pass = utc(11, 1, 8, 20);
    let v2 = Arc::new(registry::<FoldV2>());
    assert_eq!(
        v2.reconcile_state(
            FoldV2::KEY,
            &pool,
            DbMillis::from_database_millis(second_pass)
        )
        .await
        .expect("upgrade"),
        ScheduleStateReconcileOutcome::Upgraded
    );
    let upgraded = persisted_state(&pool).await;
    assert_eq!(upgraded.definition_version, 2);
    assert_eq!(upgraded.next_local_occurrence, "2026-11-02T01:30:00");
    assert!(upgraded.next_occurrence_at.get() > second_pass);

    let next_day = ScheduleMaterializer::new(pool.clone(), Arc::new(()), v2)
        .materialize_schedule(
            FoldV2::KEY,
            DbMillis::from_database_millis(utc(11, 2, 8, 31)),
        )
        .await
        .expect("tick after the upgrade");
    assert_eq!(next_day.started, 1);
    assert_eq!(
        run_occurrences(&pool).await,
        vec!["2026-11-01T01:30:00", "2026-11-02T01:30:00"]
    );
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

/// The upgrade cursor is the first new-calendar occurrence after `now` that
/// is strictly after the last materialized occurrence (S27), so it may be
/// before the persisted cursor: daily 08:00 -> 07:00 at 05:00 local still runs
/// today at 07:00.
#[tokio::test]
async fn upgrade_to_an_earlier_slot_runs_it_today() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let v1 = Arc::new(registry::<DailyV1>());
    v1.reconcile_state(
        DailyV1::KEY,
        &pool,
        DbMillis::from_database_millis(utc(1, 9, 16, 0)),
    )
    .await
    .expect("initial state");
    let yesterday = ScheduleMaterializer::new(pool.clone(), Arc::new(()), v1)
        .materialize_schedule(
            DailyV1::KEY,
            DbMillis::from_database_millis(utc(1, 10, 15, 5)),
        )
        .await
        .expect("yesterday's tick");
    assert_eq!(yesterday.started, 1);
    assert_eq!(
        persisted_state(&pool).await.next_local_occurrence,
        "2026-01-11T08:00:00"
    );

    // 05:00 MST.
    let v2 = Arc::new(registry::<EarlierV2>());
    assert_eq!(
        v2.reconcile_state(
            EarlierV2::KEY,
            &pool,
            DbMillis::from_database_millis(utc(1, 11, 12, 0))
        )
        .await
        .expect("upgrade"),
        ScheduleStateReconcileOutcome::Upgraded
    );
    let upgraded = persisted_state(&pool).await;
    assert_eq!(upgraded.next_local_occurrence, "2026-01-11T07:00:00");
    assert_eq!(upgraded.next_occurrence_at.get(), utc(1, 11, 14, 0));

    let today = ScheduleMaterializer::new(pool.clone(), Arc::new(()), v2)
        .materialize_schedule(
            EarlierV2::KEY,
            DbMillis::from_database_millis(utc(1, 11, 14, 1)),
        )
        .await
        .expect("today's tick");
    assert_eq!(today.started, 1);
    assert_eq!(
        run_occurrences(&pool).await,
        vec!["2026-01-10T08:00:00", "2026-01-11T07:00:00"]
    );
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

/// G5 class across a timezone change west (Asia/Tokyo -> UTC): UTC's next
/// key after `now` was already materialized under Tokyo, so the upgrade
/// cursor skips past the last materialized occurrence.
#[tokio::test]
async fn upgrade_to_a_western_timezone_does_not_retarget_a_materialized_occurrence() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let v1 = Arc::new(registry::<TokyoV1>());
    // 2026-01-11T00:00 in Tokyo.
    v1.reconcile_state(
        TokyoV1::KEY,
        &pool,
        DbMillis::from_database_millis(utc(1, 10, 15, 0)),
    )
    .await
    .expect("initial state");
    // 03:30 in Tokyo: 01:00, 02:00 and 03:00 get run rows.
    let tokyo = ScheduleMaterializer::new(pool.clone(), Arc::new(()), v1)
        .materialize_schedule(
            TokyoV1::KEY,
            DbMillis::from_database_millis(utc(1, 10, 18, 30)),
        )
        .await
        .expect("Tokyo tick");
    assert_eq!((tokyo.inspected, tokyo.started), (3, 1));

    let v2 = Arc::new(registry::<UtcV2>());
    let now = utc(1, 10, 18, 30);
    assert_eq!(
        v2.reconcile_state(UtcV2::KEY, &pool, DbMillis::from_database_millis(now))
            .await
            .expect("upgrade"),
        ScheduleStateReconcileOutcome::Upgraded
    );
    let upgraded = persisted_state(&pool).await;
    assert_eq!(upgraded.next_local_occurrence, "2026-01-11T04:00:00");
    assert!(upgraded.next_occurrence_at.get() > now);

    let utc_tick = ScheduleMaterializer::new(pool.clone(), Arc::new(()), v2)
        .materialize_schedule(UtcV2::KEY, DbMillis::from_database_millis(utc(1, 11, 4, 1)))
        .await
        .expect("tick after the upgrade");
    assert_eq!(utc_tick.started, 1);
    assert_eq!(
        run_occurrences(&pool).await,
        vec![
            "2026-01-11T01:00:00",
            "2026-01-11T02:00:00",
            "2026-01-11T03:00:00",
            "2026-01-11T04:00:00",
        ]
    );
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}
