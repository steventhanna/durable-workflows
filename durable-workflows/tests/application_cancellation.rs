mod support;

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    persistence::{ActivityRow, NewActivityRow},
    schema::{durable_activity, durable_workflow},
    ActivityContext, ActivityError, ActivityHandler, ActivityTopic, DurableActivity, DurableError,
    DurableStore, DurableWorkflow, RetryPolicy, StartOptions, WorkerConfig, WorkflowContext,
    WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowTransition,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Clone, Copy)]
struct CaptureTopic;
impl ActivityTopic for CaptureTopic {
    fn key(self) -> &'static str {
        "capture"
    }
    fn max_concurrency(self) -> u32 {
        1
    }
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CleanupActivity {
    linger: bool,
}
impl DurableActivity for CleanupActivity {
    type Topic = CaptureTopic;
    const KIND: &'static str = "cleanup_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);
    fn topic() -> Self::Topic {
        CaptureTopic
    }
    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("policy")
    }
}
#[derive(Default)]
struct CleanupContext {
    started: tokio::sync::Notify,
    cleaned: AtomicBool,
}
#[async_trait]
impl ActivityHandler for CleanupActivity {
    type Context = CleanupContext;
    type Output = ();
    async fn execute(
        &self,
        context: ActivityContext<'_, CleanupContext>,
    ) -> Result<(), ActivityError> {
        context.application().started.notify_one();
        context
            .cancellation_token()
            .expect("cancellation")
            .cancelled()
            .await;
        if self.linger {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
        context.application().cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}
fn worker(
    pool: durable_workflows::DurablePool,
    context: Arc<CleanupContext>,
) -> durable_workflows::ActivityWorker<CleanupContext> {
    durable_workflows::ActivityWorker::new(
        pool,
        context,
        Arc::new(
            durable_workflows::register_durable_activities!(CleanupContext; CleanupActivity)
                .expect("activities"),
        ),
        Arc::new(durable_workflows::register_durable_topics!(CaptureTopic).expect("topics")),
        "cleanup-worker",
        WorkerConfig {
            heartbeat_interval: Duration::from_millis(20),
            shutdown_grace: Duration::from_millis(150),
        },
    )
    .expect("worker")
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct HostWorkflow;

impl DurableWorkflow for HostWorkflow {
    const KIND: &'static str = "activity_host";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for HostWorkflow {
    type Context = ();
    type State = ();
    type Approval = ();
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

async fn schedule_activity(
    pool: &durable_workflows::DurablePool,
    topic: &str,
    max_attempts: i32,
    timeout_millis: i64,
    lease_duration_millis: i64,
) -> (i64, i64) {
    let workflow_id = DurableStore::new(pool.clone())
        .start(&HostWorkflow, StartOptions::default())
        .await
        .expect("workflow start")
        .workflow_id
        .get();
    let now = durable_workflows::persistence::now_millis();
    let mut connection = pool.get().await.expect("test connection");
    diesel::insert_into(durable_activity::table)
        .values(NewActivityRow {
            workflow_id,
            command_sequence: 1,
            replacement_number: 0,
            kind: CleanupActivity::KIND.to_string(),
            version: CleanupActivity::VERSION,
            topic: topic.to_string(),
            payload_json: serde_json::to_string(&CleanupActivity { linger: false })
                .expect("payload"),
            status: durable_workflows::persistence::ActivityStatus::try_from("pending")
                .expect("valid fixture status"),
            available_at: now,
            max_attempts,
            attempt_count: 0,
            timeout_millis,
            lease_duration_millis,
            retry_policy_json: serde_json::to_string(&CleanupActivity::retry_policy())
                .expect("retry policy"),
            operation_key: Some(format!("activity-{workflow_id}")),
            provider_result_json: None,
            last_error_category: None,
            last_error_message: None,
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
            root_activity_id: None,
            replaces_activity_id: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        })
        .execute(&mut connection)
        .await
        .expect("activity insert");
    let activity_id = durable_activity::table
        .filter(durable_activity::workflow_id.eq(workflow_id))
        .select(durable_activity::id)
        .first::<i64>(&mut connection)
        .await
        .expect("activity id");
    diesel::update(durable_workflow::table.find(workflow_id))
        .set((
            durable_workflow::status.eq("waiting_activity"),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(activity_id)),
            durable_workflow::command_sequence.eq(1),
            durable_workflow::delivered_event_sequence.eq(1),
        ))
        .execute(&mut connection)
        .await
        .expect("workflow wait update");
    (workflow_id, activity_id)
}

#[tokio::test]
async fn revoked_heartbeat_awaits_cleanup_without_persisting_its_result() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    for linger in [false, true] {
        let (_, activity_id) = schedule_activity(&pool, "capture", 3, 5_000, 10_000).await;
        let mut connection = pool.get().await.expect("connection");
        diesel::update(durable_activity::table.find(activity_id))
            .set(
                durable_activity::payload_json
                    .eq(serde_json::to_string(&CleanupActivity { linger }).expect("payload")),
            )
            .execute(&mut connection)
            .await
            .expect("payload update");
        drop(connection);
        let context = Arc::new(CleanupContext::default());
        let worker = worker(pool.clone(), context.clone());
        let run = tokio::spawn(async move { worker.run_one("capture").await });
        context.started.notified().await;
        let mut connection = pool.get().await.expect("connection");
        diesel::update(durable_activity::table.find(activity_id))
            .set((
                durable_activity::status.eq("cancelled"),
                durable_activity::lease_token.eq(None::<String>),
            ))
            .execute(&mut connection)
            .await
            .expect("revoke");
        drop(connection);
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("bounded cleanup")
            .expect("worker join");
        assert!(matches!(result, Err(DurableError::FencedWrite)));
        assert_eq!(
            AtomicBool::load(&context.cleaned, Ordering::SeqCst),
            !linger,
            "revocation must await cooperative cleanup"
        );
        let mut connection = pool.get().await.expect("connection");
        let row = durable_activity::table
            .find(activity_id)
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(&mut connection)
            .await
            .expect("activity");
        assert_eq!(row.status.as_str(), "cancelled");
        assert!(row.provider_result_json.is_none());
    }
    application_cancellation_is_atomic_idempotent_and_releases_capacity().await;
}

async fn application_cancellation_is_atomic_idempotent_and_releases_capacity() {
    use diesel_async::AsyncConnection;
    use durable_workflows::{
        persistence::ActivityAttemptRow,
        schema::{durable_activity_attempt, durable_workflow_event},
        WorkflowId,
    };
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (workflow_id, activity_id) = schedule_activity(&pool, "capture", 3, 5_000, 10_000).await;
    let workflow_id = WorkflowId::new(workflow_id).expect("workflow id");
    let (_, next_activity_id) = schedule_activity(&pool, "capture", 3, 5_000, 10_000).await;
    let worker = worker(pool.clone(), Arc::new(CleanupContext::default()));
    let claim = worker
        .claim_one("capture")
        .await
        .expect("claim")
        .expect("old claim");
    assert_eq!(claim.activity_id().expect("activity id").get(), activity_id);
    assert!(worker
        .claim_one("capture")
        .await
        .expect("capacity")
        .is_none());

    let mut connection = pool.get().await.expect("connection");
    let rollback: Result<(), DurableError> = connection
        .transaction(async |connection| {
            DurableStore::cancel_with_conn(connection, workflow_id, "replacement").await?;
            DurableStore::start_with_conn(
                connection,
                &HostWorkflow,
                StartOptions::default().with_deduplication_key("rolled-back-replacement"),
            )
            .await?;
            Err(DurableError::Conflict(
                "rollback both transitions".to_string(),
            ))
        })
        .await;
    assert!(rollback.is_err());
    assert!(
        DurableStore::find_by_deduplication_key_with_conn::<HostWorkflow>(
            &mut connection,
            "rolled-back-replacement"
        )
        .await
        .expect("lookup")
        .is_none()
    );
    drop(connection);
    worker
        .heartbeat(&claim)
        .await
        .expect("rollback retains old authority");

    let mut connection = pool.get().await.expect("connection");
    DurableStore::cancel_with_conn(&mut connection, workflow_id, "replacement")
        .await
        .expect("cancel");
    DurableStore::cancel_with_conn(&mut connection, workflow_id, "duplicate")
        .await
        .expect("idempotent cancel");
    let attempt = durable_activity_attempt::table
        .find((activity_id, 1))
        .select(ActivityAttemptRow::as_select())
        .first::<ActivityAttemptRow>(&mut connection)
        .await
        .expect("attempt");
    assert!(attempt.finished_at.is_some());
    assert_eq!(attempt.outcome.as_deref(), Some("application_cancelled"));
    let events = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(workflow_id.get()))
        .filter(durable_workflow_event::event_type.eq("workflow_cancelled"))
        .select((
            durable_workflow_event::actor_type,
            durable_workflow_event::actor_id,
            durable_workflow_event::reason,
        ))
        .load::<(Option<String>, Option<String>, Option<String>)>(&mut connection)
        .await
        .expect("events");
    assert_eq!(
        events,
        vec![(
            Some("system".to_string()),
            None,
            Some("replacement".to_string())
        )]
    );
    let status = durable_workflow::table
        .find(workflow_id.get())
        .select(durable_workflow::status)
        .first::<durable_workflows::persistence::WorkflowStatus>(&mut connection)
        .await
        .expect("workflow");
    assert_eq!(status.as_str(), "cancelled");
    drop(connection);
    assert!(matches!(
        worker.heartbeat(&claim).await,
        Err(DurableError::FencedWrite)
    ));
    let next_claim = worker
        .claim_one("capture")
        .await
        .expect("next claim")
        .expect("released capacity");
    assert_eq!(
        next_claim.activity_id().expect("next id").get(),
        next_activity_id
    );

    let (pending_workflow, pending_activity) =
        schedule_activity(&pool, "capture", 3, 5_000, 10_000).await;
    let mut connection = pool.get().await.expect("connection");
    DurableStore::cancel_with_conn(
        &mut connection,
        WorkflowId::new(pending_workflow).expect("pending workflow"),
        "stopped before dispatch",
    )
    .await
    .expect("cancel pending");
    let pending = durable_activity::table
        .find(pending_activity)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("pending activity");
    assert_eq!(pending.status.as_str(), "cancelled");
    assert_eq!(pending.attempt_count, 0);
    assert!(pending.lease_token.is_none());
    drop(connection);

    for status in ["ready", "succeeded", "failed", "cancelled"] {
        let id = DurableStore::new(pool.clone())
            .start(&HostWorkflow, StartOptions::default())
            .await
            .expect("start")
            .workflow_id;
        let mut connection = pool.get().await.expect("connection");
        diesel::update(durable_workflow::table.find(id.get()))
            .set(durable_workflow::status.eq(status))
            .execute(&mut connection)
            .await
            .expect("status");
        assert!(DurableStore::cancel_with_conn(&mut connection, id, " ")
            .await
            .is_err());
        DurableStore::cancel_with_conn(&mut connection, id, "no longer needed")
            .await
            .expect("cancel status");
        let actual = durable_workflow::table
            .find(id.get())
            .select(durable_workflow::status)
            .first::<durable_workflows::persistence::WorkflowStatus>(&mut connection)
            .await
            .expect("status");
        assert_eq!(
            actual.as_str(),
            if status == "ready" {
                "cancelled"
            } else {
                status
            }
        );
    }
}

#[path = "application_cancellation/timeout_cleanup.rs"]
mod timeout_cleanup;
