mod support;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    persistence::{ActivityRow, ActivityStatus, WorkflowStatus},
    schema::{durable_activity, durable_workflow},
    ActivityCommand, ActivityContext, ActivityError, ActivityHandler, ActivityTopic,
    ActivityWorker, CoordinatorConfig, DurableActivity, DurableStore, DurableWorkflow, RetryPolicy,
    StartOptions, WorkerConfig, WorkflowContext, WorkflowCoordinator, WorkflowError, WorkflowEvent,
    WorkflowHandler, WorkflowTransition,
};

#[derive(Clone, Copy)]
struct ScanTopic;

impl ActivityTopic for ScanTopic {
    fn key(self) -> &'static str {
        "continuation_scan"
    }
    fn max_concurrency(self) -> u32 {
        2
    }
}

struct ScanContext {
    clock: AtomicI64,
    restarts: AtomicUsize,
    reject_next: AtomicBool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Cursor {
    issued_at: i64,
    page: u8,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ScanPage {
    cursor: Option<Cursor>,
}

impl DurableActivity for ScanPage {
    type Topic = ScanTopic;
    const KIND: &'static str = "continuation_scan_page";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(30);
    const LEASE_DURATION: Duration = Duration::from_secs(60);
    fn topic() -> Self::Topic {
        ScanTopic
    }
    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(5).unwrap()
    }
}

#[async_trait]
impl ActivityHandler for ScanPage {
    type Context = ScanContext;
    type Output = Option<Cursor>;

    async fn execute(
        &self,
        context: ActivityContext<'_, ScanContext>,
    ) -> Result<Self::Output, ActivityError> {
        if context
            .application()
            .reject_next
            .swap(false, Ordering::SeqCst)
        {
            return Err(ActivityError::retryable("provider_busy", "retry later"));
        }
        // Advance provider time without sleeping: a large FIFO backlog consumes the cursor budget.
        let now = context
            .application()
            .clock
            .fetch_add(10_000, Ordering::SeqCst);
        let page = match &self.cursor {
            Some(cursor) if now - cursor.issued_at < 240_000 => cursor.page,
            Some(_) => {
                context
                    .application()
                    .restarts
                    .fetch_add(1, Ordering::SeqCst);
                0
            }
            None => 0,
        };
        Ok((page < 1).then_some(Cursor {
            issued_at: now,
            page: page + 1,
        }))
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ScanWorkflow {
    initial_cursor: Option<Cursor>,
}

impl DurableWorkflow for ScanWorkflow {
    const KIND: &'static str = "continuation_scan_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for ScanWorkflow {
    type Context = ScanContext;
    type State = ();
    type Approval = ();
    type Output = ();
    fn initial_state(&self) {}

    async fn step(
        &self,
        _: WorkflowContext<'_, ScanContext>,
        _: (),
        event: WorkflowEvent,
    ) -> Result<WorkflowTransition<(), (), ()>, WorkflowError> {
        let cursor = match event {
            WorkflowEvent::Started => self.initial_cursor.clone(),
            WorkflowEvent::ActivitySucceeded { result, .. } => {
                let cursor: Option<Cursor> = result
                    .decode_for::<ScanPage, _>()
                    .map_err(|error| WorkflowError::new("result", error.to_string()))?;
                let Some(cursor) = cursor else {
                    return Ok(WorkflowTransition::Complete { output: () });
                };
                Some(cursor)
            }
            _ => return Err(WorkflowError::new("event", "unexpected scan event")),
        };
        let continuation = cursor.is_some();
        let activity = ActivityCommand::new(&ScanPage { cursor }, None)
            .map_err(|error| WorkflowError::new("command", error.to_string()))?
            .with_continuation_priority(continuation);
        Ok(WorkflowTransition::RunActivity {
            state: (),
            activity,
        })
    }
}

fn runners(
    pool: durable_workflows::DurablePool,
    context: Arc<ScanContext>,
) -> (
    WorkflowCoordinator<ScanContext>,
    ActivityWorker<ScanContext>,
) {
    let workflows =
        durable_workflows::register_durable_workflows!(ScanContext; ScanWorkflow).unwrap();
    let activities =
        Arc::new(durable_workflows::register_durable_activities!(ScanContext; ScanPage).unwrap());
    let topics = Arc::new(durable_workflows::register_durable_topics!(ScanTopic).unwrap());
    (
        WorkflowCoordinator::new(
            pool.clone(),
            context.clone(),
            Arc::new(workflows),
            activities.clone(),
            "scan-coordinator",
            CoordinatorConfig::default(),
        )
        .unwrap(),
        ActivityWorker::new(
            pool,
            context,
            activities,
            topics,
            "scan-worker",
            WorkerConfig::default(),
        )
        .unwrap(),
    )
}

#[test]
fn continuation_priority_is_opt_in_and_legacy_commands_decode() {
    let ordinary = ActivityCommand::new(&ScanPage { cursor: None }, None).unwrap();
    assert!(!ordinary.has_continuation_priority());
    let mut old = serde_json::to_value(&ordinary).unwrap();
    old.as_object_mut().unwrap().remove("continuationPriority");
    let decoded: ActivityCommand = serde_json::from_value(old).unwrap();
    assert!(!decoded.has_continuation_priority());
    let prioritized = ordinary.with_continuation_priority(true);
    let decoded: ActivityCommand =
        serde_json::from_str(&serde_json::to_string(&prioritized).unwrap()).unwrap();
    assert!(decoded.has_continuation_priority());
}

#[tokio::test]
async fn oversized_expired_cursor_backlog_finishes_each_scan_instead_of_restarting_forever() {
    let pool = support::fresh_pool()
        .await
        .expect("owned durable fixture is required");
    let context = Arc::new(ScanContext {
        clock: AtomicI64::new(600_000),
        restarts: AtomicUsize::new(0),
        reject_next: AtomicBool::new(false),
    });
    let (coordinator, worker) = runners(pool.clone(), context.clone());
    let store = DurableStore::new(pool.clone());
    for _ in 0..50 {
        store
            .start(
                &ScanWorkflow {
                    initial_cursor: Some(Cursor {
                        issued_at: 0,
                        page: 1,
                    }),
                },
                StartOptions::default(),
            )
            .await
            .unwrap();
        coordinator
            .activate_one()
            .await
            .unwrap()
            .expect("initial activity scheduled");
    }
    for _ in 0..100 {
        worker
            .run_one(ScanTopic.key())
            .await
            .unwrap()
            .expect("queued scan page");
        coordinator
            .activate_one()
            .await
            .unwrap()
            .expect("page checkpointed");
    }
    let mut conn = pool.get().await.unwrap();
    let completed = durable_workflow::table
        .filter(durable_workflow::status.eq(WorkflowStatus::Succeeded))
        .count()
        .get_result::<i64>(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        completed, 50,
        "each stale scan must restart once and finish before cycling through the backlog"
    );
    assert_eq!(
        AtomicUsize::load(&context.restarts, Ordering::SeqCst),
        50,
        "queueing must not repeatedly expire the fresh continuation"
    );
    support::drop_durable_tables(&mut conn).await;
}

#[tokio::test]
async fn continuation_batch_keeps_stable_workflow_order_and_respects_cap_and_retry_delay() {
    let pool = support::fresh_pool()
        .await
        .expect("owned durable fixture is required");
    let context = Arc::new(ScanContext {
        clock: AtomicI64::new(600_000),
        restarts: AtomicUsize::new(0),
        reject_next: AtomicBool::new(false),
    });
    let (coordinator, worker) = runners(pool.clone(), context);
    let store = DurableStore::new(pool.clone());
    for _ in 0..2 {
        store
            .start(
                &ScanWorkflow {
                    initial_cursor: None,
                },
                StartOptions::default(),
            )
            .await
            .unwrap();
        coordinator.activate_one().await.unwrap();
    }
    let mut ids = Vec::new();
    for _ in 0..50 {
        let id = store
            .start(
                &ScanWorkflow {
                    initial_cursor: Some(Cursor {
                        issued_at: 0,
                        page: 1,
                    }),
                },
                StartOptions::default(),
            )
            .await
            .unwrap()
            .workflow_id
            .get();
        ids.push(id);
        coordinator.activate_one().await.unwrap();
    }
    worker.run_one(ScanTopic.key()).await.unwrap();
    coordinator.activate_one().await.unwrap();
    let mut conn = pool.get().await.unwrap();
    let current = durable_activity::table
        .filter(durable_activity::workflow_id.eq(ids[0]))
        .filter(durable_activity::status.eq(ActivityStatus::Pending))
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        current.available_at, 0,
        "coordinator must persist continuation priority"
    );
    let capacity = HashMap::from([(ScanTopic.key().to_owned(), 2)]);
    let claims = worker.claim_batch(10, &capacity).await.unwrap();
    assert_eq!(
        claims.len(),
        2,
        "the topic's two-provider-call cap still applies"
    );
    assert_eq!(
        claims[0].activity_id().unwrap().get(),
        current.id,
        "a newly inserted next page must precede older continuation activity IDs"
    );
    assert!(worker.claim_batch(10, &capacity).await.unwrap().is_empty());
    let claimed_ids = claims
        .iter()
        .map(|claim| claim.activity_id().unwrap().get())
        .collect::<Vec<_>>();
    let now = durable_workflows::persistence::database_now_millis(&mut conn)
        .await
        .unwrap();
    diesel::update(durable_activity::table.filter(durable_activity::id.eq_any(&claimed_ids)))
        .set((
            durable_activity::status.eq(ActivityStatus::Pending),
            durable_activity::available_at.eq(now + 60_000),
            durable_activity::lease_owner.eq(None::<String>),
            durable_activity::lease_token.eq(None::<String>),
            durable_activity::lease_expires_at.eq(None::<i64>),
        ))
        .execute(&mut conn)
        .await
        .unwrap();
    let next = worker.claim_batch(2, &capacity).await.unwrap();
    assert_eq!(next.len(), 2);
    assert!(
        next.iter()
            .all(|claim| !claimed_ids.contains(&claim.activity_id().unwrap().get())),
        "future retry eligibility must override continuation priority"
    );
    support::drop_durable_tables(&mut conn).await;
}

#[tokio::test]
async fn failed_continuation_waits_for_backoff_then_reacquires_priority() {
    let pool = support::fresh_pool()
        .await
        .expect("owned durable fixture is required");
    let context = Arc::new(ScanContext {
        clock: AtomicI64::new(600_000),
        restarts: AtomicUsize::new(0),
        reject_next: AtomicBool::new(true),
    });
    let (coordinator, worker) = runners(pool.clone(), context);
    DurableStore::new(pool.clone())
        .start(
            &ScanWorkflow {
                initial_cursor: Some(Cursor {
                    issued_at: 0,
                    page: 1,
                }),
            },
            StartOptions::default(),
        )
        .await
        .unwrap();
    coordinator.activate_one().await.unwrap();
    let activity_id = worker
        .run_one(ScanTopic.key())
        .await
        .unwrap()
        .unwrap()
        .get();
    let mut conn = pool.get().await.unwrap();
    let failed = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut conn)
        .await
        .unwrap();
    assert_eq!(failed.status, ActivityStatus::Pending);
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(
        failed.available_at - failed.updated_at,
        5_000,
        "priority must not remove provider retry backoff"
    );
    assert!(worker.claim_one(ScanTopic.key()).await.unwrap().is_none());
    assert!(worker
        .claim_batch(2, &HashMap::from([(ScanTopic.key().to_owned(), 2)]))
        .await
        .unwrap()
        .is_empty());
    let now = durable_workflows::persistence::database_now_millis(&mut conn)
        .await
        .unwrap();
    diesel::update(durable_activity::table.find(activity_id))
        .set(durable_activity::available_at.eq(now - 1))
        .execute(&mut conn)
        .await
        .unwrap();
    worker.run_one(ScanTopic.key()).await.unwrap();
    coordinator.activate_one().await.unwrap();
    let continuation = durable_activity::table
        .filter(durable_activity::status.eq(ActivityStatus::Pending))
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut conn)
        .await
        .unwrap();
    assert_ne!(continuation.id, activity_id);
    assert_eq!(continuation.available_at, 0);
    support::drop_durable_tables(&mut conn).await;
}
