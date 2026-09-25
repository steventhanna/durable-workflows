mod support;

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use durable_workflows::DurableConnection;
use durable_workflows::{
    admin::{
        AdminQueryService, ApprovalListFilter, PageRequest, ScheduleHealthIssue, ScheduleRegistry,
    },
    persistence::{
        NewActivityAttemptRow, NewActivityRow, NewApprovalRow, NewScheduleRunRow,
        NewScheduleStateRow,
    },
    schema::{
        durable_activity, durable_activity_attempt, durable_approval, durable_schedule_run,
        durable_schedule_state, durable_topic_lock, durable_workflow,
    },
    ActivityTopic, DurableError, DurableSchedule, DurableStore, DurableWorkflow, MisfirePolicy,
    OverlapPolicy, RetryPolicy, ScheduleHandler, ScheduleRunId, StartOptions, TopicRegistry,
    WorkflowContext, WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowTransition,
};

const NOW: i64 = 100_000_000;
const HOUR_MILLIS: i64 = 3_600_000;

#[derive(Clone, Copy)]
enum MetricsTopic {
    Provider,
    Fax,
}

#[tokio::test]
async fn topic_metrics_report_the_persisted_cap_and_registry_mismatch() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_topic_lock::table)
        .values((
            durable_topic_lock::topic.eq("provider"),
            durable_topic_lock::max_concurrency.eq(1_i32),
            durable_topic_lock::updated_at.eq(NOW),
        ))
        .execute(&mut connection)
        .await
        .expect("persisted topic cap");
    drop(connection);

    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("topic");
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .expect("topic metrics remain visible during a rolling mismatch");
    let metrics = page.items.as_slice().first().expect("provider metrics");

    assert_eq!(metrics.max_concurrency, 1);
    assert_eq!(metrics.configured_max_concurrency, 2);
    assert!(metrics.configuration_mismatch);
    assert_eq!(metrics.available_capacity, 1);
}

impl ActivityTopic for MetricsTopic {
    fn key(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Fax => "fax",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::Provider => 2,
            Self::Fax => 3,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MetricsWorkflow;

impl DurableWorkflow for MetricsWorkflow {
    const KIND: &'static str = "admin_metrics_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for MetricsWorkflow {
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

struct RegisteredSchedule;

impl DurableSchedule for RegisteredSchedule {
    const KEY: &'static str = "registered_schedule";
    const VERSION: i32 = 3;
    const CRON: &'static str = "0 0 0 * * *";
    const TIMEZONE: &'static str = "UTC";
    const MISFIRE: MisfirePolicy = MisfirePolicy::Skip;
    const OVERLAP: OverlapPolicy = OverlapPolicy::Allow;
    const MISFIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(60);
}

#[async_trait]
impl ScheduleHandler for RegisteredSchedule {
    type Context = ();

    async fn start_occurrence(
        _context: &Self::Context,
        _connection: &mut DurableConnection,
        _schedule_run_id: ScheduleRunId,
        _scheduled_for: i64,
    ) -> Result<WorkflowId, DurableError> {
        Err(DurableError::InvalidState(
            "manual start is not used by visibility tests".to_string(),
        ))
    }
}

async fn start_workflow(pool: &durable_workflows::DurablePool) -> i64 {
    DurableStore::new(pool.clone())
        .start(&MetricsWorkflow, StartOptions::default())
        .await
        .expect("workflow starts")
        .workflow_id
        .get()
}

struct ActivitySeed<'a> {
    sequence: i32,
    status: &'a str,
    available_at: i64,
    attempt_count: i32,
    lease_expires_at: Option<i64>,
    completed_at: Option<i64>,
}

async fn insert_activity(
    connection: &mut DurableConnection,
    workflow_id: i64,
    seed: ActivitySeed<'_>,
) -> i64 {
    insert_activity_on_topic(connection, workflow_id, "provider", seed).await
}

async fn insert_activity_on_topic(
    connection: &mut DurableConnection,
    workflow_id: i64,
    topic: &str,
    seed: ActivitySeed<'_>,
) -> i64 {
    diesel::insert_into(durable_activity::table)
        .values(NewActivityRow {
            workflow_id,
            command_sequence: seed.sequence,
            replacement_number: 0,
            kind: "metrics_activity".to_string(),
            version: 1,
            topic: topic.to_string(),
            payload_json: "{}".to_string(),
            status: durable_workflows::persistence::ActivityStatus::try_from(seed.status)
                .expect("valid fixture status"),
            available_at: seed.available_at,
            max_attempts: 3,
            attempt_count: seed.attempt_count,
            timeout_millis: 10_000,
            lease_duration_millis: 5_000,
            retry_policy_json: serde_json::to_string(&RetryPolicy::fixed(1).expect("retry policy"))
                .expect("retry JSON"),
            operation_key: None,
            provider_result_json: None,
            last_error_category: None,
            last_error_message: None,
            lease_owner: seed.lease_expires_at.map(|_| "metrics-worker".to_string()),
            lease_token: seed
                .lease_expires_at
                .map(|_| "00000000-0000-0000-0000-000000000111".to_string()),
            lease_expires_at: seed.lease_expires_at,
            root_activity_id: None,
            replaces_activity_id: None,
            created_at: seed.available_at - 1_000,
            updated_at: seed.available_at,
            completed_at: seed.completed_at,
        })
        .execute(connection)
        .await
        .expect("activity insert");
    durable_activity::table
        .select(durable_activity::id)
        .order(durable_activity::id.desc())
        .first(connection)
        .await
        .expect("activity ID")
}

#[tokio::test]
async fn topic_metrics_use_one_captured_clock_and_exact_lease_fences() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let ready_workflow_id = start_workflow(&pool).await;
    let retry_workflow_id = start_workflow(&pool).await;
    let paused_workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");
    let active_id = insert_activity(
        &mut connection,
        workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "running",
            available_at: NOW - 10_000,
            attempt_count: 1,
            lease_expires_at: Some(NOW + 1_000),
            completed_at: None,
        },
    )
    .await;
    diesel::insert_into(durable_activity_attempt::table)
        .values(NewActivityAttemptRow {
            activity_id: active_id,
            attempt_number: 1,
            worker_id: "metrics-worker".to_string(),
            lease_token: "00000000-0000-0000-0000-000000000111".to_string(),
            started_at: NOW - 8_000,
            heartbeat_at: NOW - 100,
            finished_at: None,
            outcome: None,
            error_category: None,
            error_message: None,
            provider_result_json: None,
        })
        .execute(&mut connection)
        .await
        .expect("active attempt");
    insert_activity(
        &mut connection,
        workflow_id,
        ActivitySeed {
            sequence: 2,
            status: "running",
            available_at: NOW - 20_000,
            attempt_count: 1,
            lease_expires_at: Some(NOW - 1),
            completed_at: None,
        },
    )
    .await;
    let ready_id = insert_activity(
        &mut connection,
        ready_workflow_id,
        ActivitySeed {
            sequence: 3,
            status: "pending",
            available_at: NOW - 5_000,
            attempt_count: 0,
            lease_expires_at: None,
            completed_at: None,
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(ready_workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(ready_id)),
        ))
        .execute(&mut connection)
        .await
        .expect("ready parent wait");
    let retry_id = insert_activity(
        &mut connection,
        retry_workflow_id,
        ActivitySeed {
            sequence: 4,
            status: "pending",
            available_at: NOW + 5_000,
            attempt_count: 1,
            lease_expires_at: None,
            completed_at: None,
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(retry_workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(retry_id)),
        ))
        .execute(&mut connection)
        .await
        .expect("retry parent wait");
    // Pending under a paused parent must not inflate ready_count.
    insert_activity(
        &mut connection,
        paused_workflow_id,
        ActivitySeed {
            sequence: 7,
            status: "pending",
            available_at: NOW - 15_000,
            attempt_count: 0,
            lease_expires_at: None,
            completed_at: None,
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(paused_workflow_id))
        .set((
            durable_workflow::status.eq("paused"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
        ))
        .execute(&mut connection)
        .await
        .expect("paused parent");
    insert_activity(
        &mut connection,
        workflow_id,
        ActivitySeed {
            sequence: 5,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - HOUR_MILLIS),
        },
    )
    .await;
    insert_activity(
        &mut connection,
        workflow_id,
        ActivitySeed {
            sequence: 6,
            status: "succeeded",
            available_at: NOW - 2 * HOUR_MILLIS,
            attempt_count: 1,
            lease_expires_at: None,
            completed_at: Some(NOW - 2 * HOUR_MILLIS),
        },
    )
    .await;
    drop(connection);

    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("topic");
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .expect("topic metrics");
    assert_eq!(page.items.len(), 1);
    let metrics = &page.items[0];
    assert_eq!(metrics.max_concurrency, 2);
    assert_eq!(metrics.active_count, 1);
    assert_eq!(metrics.available_capacity, 1);
    assert_eq!(metrics.ready_count, 1);
    assert_eq!(metrics.retry_scheduled_count, 1);
    assert_eq!(metrics.dead_letter_count, 1);
    assert_eq!(metrics.oldest_ready_age_millis, Some(5_000));
    assert_eq!(metrics.oldest_active_age_millis, Some(8_000));
    assert_eq!(metrics.hourly_throughput.len(), 24);
    assert_eq!(
        metrics
            .hourly_throughput
            .iter()
            .map(|bucket| bucket.completed_count)
            .sum::<u64>(),
        2
    );
    let current_hour = NOW / HOUR_MILLIS * HOUR_MILLIS;
    let first_hour = current_hour - 23 * HOUR_MILLIS;
    assert_eq!(metrics.hourly_throughput[0].starts_at, first_hour);
    assert_eq!(metrics.hourly_throughput[23].starts_at, current_hour);
    assert_eq!(
        metrics
            .hourly_throughput
            .iter()
            .find(|bucket| bucket.starts_at == (NOW - HOUR_MILLIS) / HOUR_MILLIS * HOUR_MILLIS)
            .map(|bucket| bucket.completed_count),
        Some(1)
    );
    assert_eq!(
        metrics
            .hourly_throughput
            .iter()
            .find(|bucket| bucket.starts_at == (NOW - 2 * HOUR_MILLIS) / HOUR_MILLIS * HOUR_MILLIS)
            .map(|bucket| bucket.completed_count),
        Some(1)
    );
    assert_eq!(metrics.captured_at, NOW);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn topic_metrics_dead_letter_count_excludes_resolved_workflows() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let resolved_workflow_id = start_workflow(&pool).await;
    let cancelled_workflow_id = start_workflow(&pool).await;
    let blocked_workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");

    // A best-effort activity whose workflow moved on and succeeded without it.
    insert_activity(
        &mut connection,
        resolved_workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - HOUR_MILLIS),
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(resolved_workflow_id))
        .set(durable_workflow::status.eq("succeeded"))
        .execute(&mut connection)
        .await
        .expect("resolved workflow succeeds");

    insert_activity(
        &mut connection,
        cancelled_workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - HOUR_MILLIS),
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(cancelled_workflow_id))
        .set(durable_workflow::status.eq("cancelled"))
        .execute(&mut connection)
        .await
        .expect("cancelled workflow");

    // Still genuinely blocking its workflow: this one should count.
    insert_activity(
        &mut connection,
        blocked_workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - HOUR_MILLIS),
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(blocked_workflow_id))
        .set(durable_workflow::status.eq("blocked"))
        .execute(&mut connection)
        .await
        .expect("blocked workflow");
    drop(connection);

    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("topic");
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .expect("topic metrics");
    let metrics = &page.items[0];
    assert_eq!(metrics.dead_letter_count, 1);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn topic_metrics_dead_letter_count_excludes_retried_chains_on_open_workflows() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");

    // Original dead-lettered activity; an operator retried it.
    let dead_letter_id = insert_activity(
        &mut connection,
        workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - HOUR_MILLIS),
        },
    )
    .await;

    // The replacement in the same root_activity_id chain succeeded.
    diesel::insert_into(durable_activity::table)
        .values(NewActivityRow {
            workflow_id,
            command_sequence: 2,
            replacement_number: 1,
            kind: "metrics_activity".to_string(),
            version: 1,
            topic: "provider".to_string(),
            payload_json: "{}".to_string(),
            status: durable_workflows::persistence::ActivityStatus::Succeeded,
            available_at: NOW - HOUR_MILLIS + 500,
            max_attempts: 3,
            attempt_count: 1,
            timeout_millis: 10_000,
            lease_duration_millis: 5_000,
            retry_policy_json: serde_json::to_string(&RetryPolicy::fixed(1).expect("retry policy"))
                .expect("retry JSON"),
            operation_key: None,
            provider_result_json: None,
            last_error_category: None,
            last_error_message: None,
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
            root_activity_id: Some(dead_letter_id),
            replaces_activity_id: Some(dead_letter_id),
            created_at: NOW - HOUR_MILLIS,
            updated_at: NOW - HOUR_MILLIS + 500,
            completed_at: Some(NOW - HOUR_MILLIS + 500),
        })
        .execute(&mut connection)
        .await
        .expect("replacement activity insert");

    // The workflow moved on to other, still-open work instead of terminating.
    diesel::update(durable_workflow::table.find(workflow_id))
        .set(durable_workflow::status.eq("sleeping"))
        .execute(&mut connection)
        .await
        .expect("workflow keeps running");
    drop(connection);

    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("topic");
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .expect("topic metrics");
    let metrics = &page.items[0];
    assert_eq!(metrics.dead_letter_count, 0);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn topic_metrics_batch_counts_stay_isolated_per_topic() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let provider_workflow_id = start_workflow(&pool).await;
    let fax_workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");
    insert_activity(
        &mut connection,
        provider_workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - 500),
        },
    )
    .await;
    insert_activity_on_topic(
        &mut connection,
        fax_workflow_id,
        "fax",
        ActivitySeed {
            sequence: 1,
            status: "succeeded",
            available_at: NOW - 2 * HOUR_MILLIS,
            attempt_count: 1,
            lease_expires_at: None,
            completed_at: Some(NOW - 2 * HOUR_MILLIS),
        },
    )
    .await;
    let fax_ready_id = insert_activity_on_topic(
        &mut connection,
        fax_workflow_id,
        "fax",
        ActivitySeed {
            sequence: 2,
            status: "pending",
            available_at: NOW - 1_000,
            attempt_count: 0,
            lease_expires_at: None,
            completed_at: None,
        },
    )
    .await;
    diesel::update(durable_workflow::table.find(fax_workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(fax_ready_id)),
        ))
        .execute(&mut connection)
        .await
        .expect("fax parent wait");
    drop(connection);

    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("provider");
    topics.register(MetricsTopic::Fax).expect("fax");
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .expect("batched topic metrics");
    assert_eq!(
        page.items
            .iter()
            .map(|metrics| metrics.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["fax", "provider"]
    );

    let fax = &page.items[0];
    assert_eq!(fax.max_concurrency, 3);
    assert_eq!(fax.active_count, 0);
    assert_eq!(fax.ready_count, 1);
    assert_eq!(fax.retry_scheduled_count, 0);
    assert_eq!(fax.dead_letter_count, 0);
    assert_eq!(fax.oldest_ready_age_millis, Some(1_000));
    assert_eq!(
        fax.hourly_throughput
            .iter()
            .map(|bucket| bucket.completed_count)
            .sum::<u64>(),
        1
    );

    let provider = &page.items[1];
    assert_eq!(provider.max_concurrency, 2);
    assert_eq!(provider.active_count, 0);
    assert_eq!(provider.ready_count, 0);
    assert_eq!(provider.retry_scheduled_count, 0);
    assert_eq!(provider.dead_letter_count, 1);
    assert_eq!(provider.oldest_ready_age_millis, None);
    assert_eq!(
        provider
            .hourly_throughput
            .iter()
            .map(|bucket| bucket.completed_count)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        provider
            .hourly_throughput
            .iter()
            .find(|bucket| bucket.starts_at == NOW / HOUR_MILLIS * HOUR_MILLIS)
            .map(|bucket| bucket.completed_count),
        Some(1)
    );

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn topic_metrics_treat_spellings_as_distinct_topics() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_topic_lock::table)
        .values((
            durable_topic_lock::topic.eq("PROVIDER"),
            durable_topic_lock::max_concurrency.eq(1_i32),
            durable_topic_lock::updated_at.eq(NOW),
        ))
        .execute(&mut connection)
        .await
        .expect("persisted topic cap for a distinct spelling");
    insert_activity_on_topic(
        &mut connection,
        workflow_id,
        "provider",
        ActivitySeed {
            sequence: 1,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - 500),
        },
    )
    .await;
    insert_activity_on_topic(
        &mut connection,
        workflow_id,
        "PROVIDER",
        ActivitySeed {
            sequence: 2,
            status: "dead_lettered",
            available_at: NOW - HOUR_MILLIS,
            attempt_count: 3,
            lease_expires_at: None,
            completed_at: Some(NOW - 1_000),
        },
    )
    .await;
    drop(connection);

    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("topic");
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .expect("exact-spelling topic metrics");
    assert_eq!(
        page.items
            .iter()
            .map(|metrics| metrics.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["provider"]
    );
    let provider = &page.items[0];
    assert_eq!(provider.dead_letter_count, 1);
    assert_eq!(provider.max_concurrency, 2);
    assert_eq!(provider.configured_max_concurrency, 2);
    assert_eq!(
        provider
            .hourly_throughput
            .iter()
            .map(|bucket| bucket.completed_count)
            .sum::<u64>(),
        1
    );

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn current_topic_metrics_use_database_time() {
    let Some(pool) = support::fresh_pool_with_max_size(1).await else {
        return;
    };
    let database_seconds = 1_800_000_000_i64;
    let mut connection = pool.get().await.expect("clock connection");
    support::freeze_database_clock(&mut connection, database_seconds * 1_000).await;
    drop(connection);
    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).expect("topic");

    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics(&topics, PageRequest::default())
        .await
        .expect("current topic metrics");
    assert_eq!(page.items[0].captured_at, database_seconds * 1_000);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn schedule_health_includes_unregistered_state_and_bounded_recent_runs() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let mut schedules = ScheduleRegistry::<()>::new();
    schedules
        .register::<RegisteredSchedule>()
        .expect("registered schedule");
    let registered_fingerprint = schedules
        .get(RegisteredSchedule::KEY)
        .expect("registered schedule metadata")
        .fingerprint
        .clone();
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");
    for (key, version, fingerprint) in [
        ("registered_schedule".to_string(), 3, registered_fingerprint),
        (
            "orphaned_schedule".to_string(),
            1,
            "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
        ),
    ] {
        diesel::insert_into(durable_schedule_state::table)
            .values(NewScheduleStateRow {
                schedule_key: key,
                definition_fingerprint: fingerprint,
                definition_version: version,
                next_local_occurrence: "2099-01-01T00:00:00".to_string(),
                next_occurrence_at: NOW + HOUR_MILLIS,
                last_materialized_at: Some(NOW - HOUR_MILLIS),
                paused_at: None,
                paused_by: None,
                pause_reason: None,
                created_at: NOW - 10_000,
                updated_at: NOW - 5_000,
            })
            .execute(&mut connection)
            .await
            .expect("schedule state");
    }
    for (offset, status) in [(2_i64, "skipped"), (1_i64, "coalesced"), (0_i64, "started")] {
        diesel::insert_into(durable_schedule_run::table)
            .values(NewScheduleRunRow {
                schedule_key: "registered_schedule".to_string(),
                local_occurrence: format!("occurrence-{offset}"),
                scheduled_for: NOW - offset * HOUR_MILLIS,
                materialized_at: NOW - offset * HOUR_MILLIS,
                status: status.to_string(),
                reason: Some(format!("bounded {status}")),
                actor_id: None,
                workflow_id: None,
                created_at: NOW - offset * HOUR_MILLIS,
            })
            .execute(&mut connection)
            .await
            .expect("schedule run");
    }
    let active_run_id = durable_schedule_run::table
        .filter(durable_schedule_run::status.eq("started"))
        .select(durable_schedule_run::id)
        .first::<i64>(&mut connection)
        .await
        .expect("active run ID");
    diesel::update(durable_workflow::table.find(workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::schedule_run_id.eq(Some(active_run_id)),
        ))
        .execute(&mut connection)
        .await
        .expect("link workflow to run");
    diesel::update(durable_schedule_run::table.find(active_run_id))
        .set(durable_schedule_run::workflow_id.eq(Some(workflow_id)))
        .execute(&mut connection)
        .await
        .expect("link run to workflow");
    drop(connection);

    let service = AdminQueryService::new(pool.clone());
    let page = service
        .list_schedules(&schedules, PageRequest::default(), 2)
        .await
        .expect("schedule summaries");
    assert_eq!(
        page.items
            .iter()
            .map(|schedule| schedule.key.as_str())
            .collect::<Vec<_>>(),
        vec!["orphaned_schedule", "registered_schedule"]
    );
    assert_eq!(
        page.items[0].health_issues,
        vec![ScheduleHealthIssue::UnregisteredState]
    );
    let registered = &page.items[1];
    assert!(registered.health_issues.is_empty());
    assert_eq!(registered.cron.as_deref(), Some(RegisteredSchedule::CRON));
    assert_eq!(
        registered.timezone.as_deref(),
        Some(RegisteredSchedule::TIMEZONE)
    );
    assert_eq!(registered.misfire, Some(RegisteredSchedule::MISFIRE));
    assert_eq!(registered.overlap, Some(RegisteredSchedule::OVERLAP));
    assert_eq!(registered.misfire_grace_millis, Some(60_000));
    assert_eq!(
        registered
            .state
            .as_ref()
            .map(|state| state.next_local_occurrence.as_str()),
        Some("2099-01-01T00:00:00")
    );
    assert_eq!(registered.active_overlap_count, 1);
    assert_eq!(registered.skipped_count, 1);
    assert_eq!(registered.coalesced_count, 1);
    assert_eq!(registered.recent_runs.len(), 2);
    assert_eq!(
        registered.last_run.as_ref().map(|run| run.id.get()),
        Some(active_run_id)
    );

    let runs = service
        .list_schedule_runs(
            "registered_schedule",
            PageRequest {
                cursor: None,
                limit: Some(2),
            },
        )
        .await
        .expect("schedule runs");
    assert_eq!(runs.items.len(), 2);
    assert!(runs.next_cursor.is_some());
    assert_eq!(
        runs.items[0].workflow_status.as_deref(),
        Some("waiting_activity")
    );

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn schedule_run_cursors_support_max_length_schedule_keys() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let schedule_key = "s".repeat(191);
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_schedule_state::table)
        .values(NewScheduleStateRow {
            schedule_key: schedule_key.clone(),
            definition_fingerprint:
                "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            definition_version: 1,
            next_local_occurrence: "2099-01-01T00:00:00".to_string(),
            next_occurrence_at: NOW + HOUR_MILLIS,
            last_materialized_at: Some(NOW),
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            created_at: NOW,
            updated_at: NOW,
        })
        .execute(&mut connection)
        .await
        .expect("schedule state");
    for offset in 0..2_i64 {
        diesel::insert_into(durable_schedule_run::table)
            .values(NewScheduleRunRow {
                schedule_key: schedule_key.clone(),
                local_occurrence: format!("occurrence-{offset}"),
                scheduled_for: NOW - offset,
                materialized_at: NOW - offset,
                status: "skipped".to_string(),
                reason: None,
                actor_id: None,
                workflow_id: None,
                created_at: NOW - offset,
            })
            .execute(&mut connection)
            .await
            .expect("schedule run");
    }
    drop(connection);

    let service = AdminQueryService::new(pool.clone());
    let first = service
        .list_schedule_runs(
            &schedule_key,
            PageRequest {
                cursor: None,
                limit: Some(1),
            },
        )
        .await
        .expect("first page");
    assert_eq!(first.items.len(), 1);
    let second = service
        .list_schedule_runs(
            &schedule_key,
            PageRequest {
                cursor: first.next_cursor,
                limit: Some(1),
            },
        )
        .await
        .expect("second page");
    assert_eq!(second.items.len(), 1);

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn approval_listing_classifies_expiry_without_loading_decisions() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");
    for (sequence, status, expires_at, decision) in [
        (1, "pending", Some(NOW - 1), None),
        (
            2,
            "approved",
            None,
            Some(r#"{"secret":"approval-decision"}"#.to_string()),
        ),
    ] {
        diesel::insert_into(durable_approval::table)
            .values(NewApprovalRow {
                workflow_id,
                command_sequence: sequence,
                kind: "medical_review".to_string(),
                version: 1,
                prompt_metadata_json: r#"{"secret":"approval-prompt"}"#.to_string(),
                validation_schema_json: "{}".to_string(),
                validation_version: 1,
                status: status.to_string(),
                requested_at: NOW - i64::from(sequence) * 1_000,
                expires_at,
                decision_payload_json: decision,
                decided_by: (status == "approved").then_some(7),
                operator_reason: (status == "approved").then(|| "Reviewed".to_string()),
                resolved_at: (status == "approved").then_some(NOW - 100),
            })
            .execute(&mut connection)
            .await
            .expect("approval");
    }
    drop(connection);

    let page = AdminQueryService::new(pool.clone())
        .list_approvals_at(
            ApprovalListFilter {
                status: Some("expired".to_string()),
                ..ApprovalListFilter::default()
            },
            NOW,
        )
        .await
        .expect("expired approvals");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].status, "expired");
    let json = serde_json::to_string(&page).expect("approval JSON");
    assert!(!json.contains("approval-decision"));
    assert!(!json.contains("approval-prompt"));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn prioritized_continuation_ready_age_uses_creation_time() {
    let pool = support::fresh_pool()
        .await
        .expect("owned durable fixture is required");
    let workflow_id = start_workflow(&pool).await;
    let mut connection = pool.get().await.expect("test connection");
    let activity_id = insert_activity(
        &mut connection,
        workflow_id,
        ActivitySeed {
            sequence: 1,
            status: "pending",
            available_at: 0,
            attempt_count: 0,
            lease_expires_at: None,
            completed_at: None,
        },
    )
    .await;
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::created_at.eq(NOW - 5_000))
        .execute(&mut connection)
        .await
        .unwrap();
    diesel::update(durable_workflow::table.find(workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(activity_id)),
        ))
        .execute(&mut connection)
        .await
        .unwrap();
    drop(connection);
    let mut topics = TopicRegistry::new();
    topics.register(MetricsTopic::Provider).unwrap();
    let page = AdminQueryService::new(pool.clone())
        .list_topic_metrics_at(&topics, PageRequest::default(), NOW)
        .await
        .unwrap();
    assert_eq!(page.items[0].ready_count, 1);
    assert_eq!(page.items[0].oldest_ready_age_millis, Some(5_000));
    let query = AdminQueryService::new(pool.clone());
    let activities = query
        .list_activities(durable_workflows::admin::ActivityListFilter::default())
        .await
        .unwrap();
    assert_eq!(activities.items.len(), 1);
    assert_eq!(activities.items[0].available_at, NOW - 5_000);
    let detail = query
        .get_activity(
            durable_workflows::ActivityId::new(activity_id).unwrap(),
            durable_workflows::admin::ActivityDetailRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(detail.activity.available_at, NOW - 5_000);
    let mut connection = pool.get().await.unwrap();
    support::drop_durable_tables(&mut connection).await;
}
