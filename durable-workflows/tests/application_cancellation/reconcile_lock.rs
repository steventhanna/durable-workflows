use super::*;
use diesel_async::SimpleAsyncConnection;
use std::collections::HashMap;

#[derive(Clone, Copy)]
struct OtherTopic;
impl ActivityTopic for OtherTopic {
    fn key(self) -> &'static str {
        "other"
    }
    fn max_concurrency(self) -> u32 {
        1
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OtherActivity;

impl DurableActivity for OtherActivity {
    type Topic = OtherTopic;
    const KIND: &'static str = "other_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        OtherTopic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("retry policy")
    }
}

#[async_trait]
impl ActivityHandler for OtherActivity {
    type Context = CleanupContext;
    type Output = ();

    async fn execute(
        &self,
        _context: ActivityContext<'_, Self::Context>,
    ) -> Result<(), ActivityError> {
        Ok(())
    }
}

fn two_topic_worker(
    pool: durable_workflows::DurablePool,
) -> durable_workflows::ActivityWorker<CleanupContext> {
    durable_workflows::ActivityWorker::new(
        pool,
        Arc::new(CleanupContext::default()),
        Arc::new(
            durable_workflows::register_durable_activities!(
                CleanupContext; CleanupActivity, OtherActivity
            )
            .expect("activities"),
        ),
        Arc::new(
            durable_workflows::register_durable_topics!(CaptureTopic, OtherTopic).expect("topics"),
        ),
        "reconcile-lock-worker",
        WorkerConfig::default(),
    )
    .expect("worker")
}

async fn activity_row(
    pool: &durable_workflows::DurablePool,
    activity_id: durable_workflows::ActivityId,
) -> ActivityRow {
    let mut connection = pool.get().await.expect("connection");
    durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first(&mut connection)
        .await
        .expect("activity row")
}

// An application transaction that holds a workflow row (a cancel or start in the
// caller's transaction, then slow work) must not stall claims: the claim sweep holds
// every topic lock row while it reconciles, so it skips the locked workflow's expired
// attempt, counts it against its topic's cap, and reconciles it in a later sweep.
#[tokio::test]
async fn reconcile_skips_a_workflow_an_application_transaction_holds() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let worker = two_topic_worker(pool.clone());
    let (held_workflow, expired) = schedule_activity(&pool, "capture", 3, 100, 300).await;
    let claim = worker
        .claim_one("capture")
        .await
        .expect("claim")
        .expect("claimed activity");
    assert_eq!(claim.activity_id(), expired);
    drop(claim);
    let (_, queued) = schedule_activity(&pool, "capture", 3, 5_000, 10_000).await;
    let (_, other) = schedule_activity(&pool, "other", 3, 5_000, 10_000).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_activity::table.find(other))
        .set((
            durable_activity::kind.eq(OtherActivity::KIND),
            durable_activity::payload_json
                .eq(serde_json::to_string(&OtherActivity).expect("payload")),
        ))
        .execute(&mut connection)
        .await
        .expect("other activity kind");
    drop(connection);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut application = pool.get().await.expect("application connection");
    application.batch_execute("BEGIN").await.expect("begin");
    durable_workflow::table
        .find(held_workflow)
        .for_update()
        .select(durable_workflow::id)
        .first::<durable_workflows::WorkflowId>(&mut application)
        .await
        .expect("application holds the workflow row");

    let capacity = HashMap::from([("capture".to_string(), 1), ("other".to_string(), 1)]);
    let claims = tokio::time::timeout(Duration::from_secs(5), worker.claim_batch(2, &capacity))
        .await
        .expect("the claim sweep does not wait for the application transaction")
        .expect("claim batch");
    let claimed: Vec<_> = claims.iter().map(|claim| claim.activity_id()).collect();
    assert_eq!(
        claimed,
        vec![other],
        "the other topic still claims; the unsettled attempt keeps its capture slot"
    );
    drop(claims);
    let row = activity_row(&pool, expired).await;
    assert_eq!(row.status.as_str(), "running", "the locked attempt waits");
    assert!(row.lease_token.is_some());

    application.batch_execute("COMMIT").await.expect("commit");
    drop(application);
    let claims = tokio::time::timeout(Duration::from_secs(5), worker.claim_batch(2, &capacity))
        .await
        .expect("bounded claim sweep")
        .expect("claim batch");
    let claimed: Vec<_> = claims.iter().map(|claim| claim.activity_id()).collect();
    assert_eq!(claimed, vec![queued], "the reconciled slot is free again");
    drop(claims);
    let row = activity_row(&pool, expired).await;
    assert_ne!(
        row.status.as_str(),
        "running",
        "a later sweep reconciles it"
    );
    assert_eq!(row.last_error_category.as_deref(), Some("lease_expired"));
    assert!(row.lease_token.is_none());
}
