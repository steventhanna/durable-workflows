mod support;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    persistence::ScheduleRunRow,
    schema::{durable_schedule_run, durable_workflow},
    DurableError, DurableSchedule, DurableStore, DurableWorkflow, MisfirePolicy, OverlapPolicy,
    ScheduleHandler, ScheduleMaterializer, ScheduleRegistry, ScheduleRunId, StartOptions,
    WorkflowContext, WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OverlapWorkflow;

impl DurableWorkflow for OverlapWorkflow {
    const KIND: &'static str = "overlap_test_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for OverlapWorkflow {
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

macro_rules! overlap_schedule {
    ($name:ident, $key:literal, $overlap:expr) => {
        struct $name;

        impl DurableSchedule for $name {
            const KEY: &'static str = $key;
            const VERSION: i32 = 1;
            const CRON: &'static str = "0 * * * * *";
            const TIMEZONE: &'static str = "UTC";
            const MISFIRE: MisfirePolicy = MisfirePolicy::CatchUp { max_occurrences: 3 };
            const OVERLAP: OverlapPolicy = $overlap;
            const MISFIRE_GRACE: Duration = Duration::from_secs(10);
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
                    &OverlapWorkflow,
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

overlap_schedule!(AllowSchedule, "allow_overlap", OverlapPolicy::Allow);
overlap_schedule!(
    SkipActiveSchedule,
    "skip_active",
    OverlapPolicy::SkipIfActive
);
overlap_schedule!(QueueOneSchedule, "queue_one", OverlapPolicy::QueueOne);

fn registry() -> Arc<ScheduleRegistry<()>> {
    let mut registry = ScheduleRegistry::new();
    registry.register::<AllowSchedule>().expect("allow");
    registry
        .register::<SkipActiveSchedule>()
        .expect("skip active");
    registry.register::<QueueOneSchedule>().expect("queue one");
    Arc::new(registry)
}

fn at_minute(minute: i64) -> i64 {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("instant")
        .timestamp_millis()
        + minute * 60_000
}

async fn runs(pool: &durable_workflows::DurablePool, key: &str) -> Vec<ScheduleRunRow> {
    let mut connection = pool.get().await.expect("connection");
    durable_schedule_run::table
        .filter(durable_schedule_run::schedule_key.eq(key))
        .order((
            durable_schedule_run::scheduled_for.asc(),
            durable_schedule_run::id.asc(),
        ))
        .select(ScheduleRunRow::as_select())
        .load(&mut connection)
        .await
        .expect("runs")
}

#[tokio::test]
async fn overlap_policies_apply_globally_to_each_selected_occurrence() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    for key in [
        AllowSchedule::KEY,
        SkipActiveSchedule::KEY,
        QueueOneSchedule::KEY,
    ] {
        registry
            .reconcile_state(key, &pool, at_minute(0))
            .await
            .expect("state");
    }
    let materializer = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry);
    let allow = materializer
        .materialize_schedule(AllowSchedule::KEY, at_minute(3))
        .await
        .expect("allow");
    assert_eq!((allow.started, allow.queued, allow.skipped), (3, 0, 0));

    let skip = materializer
        .materialize_schedule(SkipActiveSchedule::KEY, at_minute(3))
        .await
        .expect("skip active");
    assert_eq!((skip.started, skip.queued, skip.skipped), (1, 0, 2));
    assert_eq!(
        runs(&pool, SkipActiveSchedule::KEY)
            .await
            .iter()
            .map(|run| run.status.as_str())
            .collect::<Vec<_>>(),
        vec!["started", "skipped", "skipped"]
    );
    let skipped_runs = runs(&pool, SkipActiveSchedule::KEY).await;
    let blocked_workflow_id = skipped_runs[0].workflow_id.expect("started workflow");
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(blocked_workflow_id))
        .set((
            durable_workflow::status.eq("blocked"),
            durable_workflow::error_category.eq(Some("activity_retry_exhausted")),
        ))
        .execute(&mut connection)
        .await
        .expect("block exhausted workflow");
    drop(connection);
    let after_block = materializer
        .materialize_schedule(SkipActiveSchedule::KEY, at_minute(4))
        .await
        .expect("materialize after blocked occurrence");
    assert_eq!((after_block.started, after_block.skipped), (0, 1));

    let queue = materializer
        .materialize_schedule(QueueOneSchedule::KEY, at_minute(3))
        .await
        .expect("queue one");
    assert_eq!((queue.started, queue.queued, queue.skipped), (1, 1, 1));
    assert_eq!(
        runs(&pool, QueueOneSchedule::KEY)
            .await
            .iter()
            .map(|run| run.status.as_str())
            .collect::<Vec<_>>(),
        vec!["started", "queued", "skipped"]
    );
}

#[tokio::test]
async fn queue_one_promotes_once_after_the_active_workflow_finishes() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let registry = registry();
    registry
        .reconcile_state(QueueOneSchedule::KEY, &pool, at_minute(0))
        .await
        .expect("state");
    let initial = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry.clone());
    initial
        .materialize_schedule(QueueOneSchedule::KEY, at_minute(3))
        .await
        .expect("initial queue");
    let initial_runs = runs(&pool, QueueOneSchedule::KEY).await;
    let active_workflow_id = initial_runs[0].workflow_id.expect("active workflow");
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_workflow::table.find(active_workflow_id))
        .set((
            durable_workflow::status.eq("succeeded"),
            durable_workflow::completed_at.eq(Some(at_minute(3))),
        ))
        .execute(&mut connection)
        .await
        .expect("complete active workflow");
    drop(connection);

    let left = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry.clone());
    let right = ScheduleMaterializer::new(pool.clone(), Arc::new(()), registry);
    let (left, right) = tokio::join!(
        left.materialize_schedule(QueueOneSchedule::KEY, at_minute(3) + 1),
        right.materialize_schedule(QueueOneSchedule::KEY, at_minute(3) + 1),
    );
    assert_eq!(
        left.expect("left").started + right.expect("right").started,
        1
    );
    let promoted = runs(&pool, QueueOneSchedule::KEY).await;
    assert_eq!(
        promoted
            .iter()
            .map(|run| run.status.as_str())
            .collect::<Vec<_>>(),
        vec!["started", "started", "skipped"]
    );
    assert_eq!(promoted[1].reason.as_deref(), Some("queue_one_promoted"));
    assert!(promoted[1].workflow_id.is_some());
}
