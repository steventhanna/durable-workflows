mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use diesel::{QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    persistence::ScheduleStateRow, schema::durable_schedule_state, DurableError, DurableSchedule,
    MisfirePolicy, OverlapPolicy, ScheduleHandler, ScheduleRegistry, ScheduleRunId,
    ScheduleStateReconcileOutcome, WorkflowId,
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
        registry.reconcile_state(ScheduleV1::KEY, &pool, now),
        registry.reconcile_state(ScheduleV1::KEY, &pool, now),
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
            .reconcile_state(ScheduleV1::KEY, &pool, later)
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
        v1.reconcile_state(ScheduleV1::KEY, &pool, initial_now)
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
        v2.reconcile_state(ScheduleV2::KEY, &pool, deployment_now)
            .await
            .expect("upgrade"),
        ScheduleStateReconcileOutcome::Upgraded
    );
    let upgraded = persisted_state(&pool).await;
    assert_eq!(upgraded.definition_version, 2);
    assert_eq!(upgraded.next_local_occurrence, "2026-01-13T09:30:00");

    assert_eq!(
        v1.reconcile_state(ScheduleV1::KEY, &pool, deployment_now + 1)
            .await
            .expect("older process observes newer state"),
        ScheduleStateReconcileOutcome::NewerPersisted
    );
    assert_eq!(persisted_state(&pool).await.definition_version, 2);

    let drift = registry::<ScheduleV2Drift>()
        .reconcile_state(ScheduleV2Drift::KEY, &pool, deployment_now + 1)
        .await
        .expect_err("same-version drift must fail closed");
    assert!(matches!(drift, DurableError::Conflict(_)));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}
