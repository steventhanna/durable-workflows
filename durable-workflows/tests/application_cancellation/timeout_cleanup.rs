use super::*;

// Budgets: a 60ms timeout, a 150ms shutdown grace and a 40ms cleanup, so the
// cleanup finishes well inside the grace and the lingering handler is dropped
// after >= 200ms. Only a host too loaded to run a 40ms sleep within 150ms fails
// this (seen only alongside check.sh; not reproduced under a parallel full
// suite run); a larger grace would weaken the linger bound.
#[tokio::test]
async fn timeout_awaits_bounded_cleanup_and_never_commits_handler_success() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    for linger in [false, true] {
        let (_, activity_id) = schedule_activity(&pool, "capture", 1, 60, 5_000).await;
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
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(2), worker.run_one("capture"))
            .await
            .expect("timeout cleanup is bounded")
            .expect("timeout outcome persisted");
        assert_eq!(result.expect("claimed activity").get(), activity_id);
        assert_eq!(
            AtomicBool::load(&context.cleaned, Ordering::SeqCst),
            !linger,
            "timeout must await accepted-data cleanup before dropping the handler"
        );
        if linger {
            assert!(started.elapsed() >= Duration::from_millis(200));
        }
        let mut connection = pool.get().await.expect("connection");
        let row = durable_activity::table
            .find(activity_id)
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(&mut connection)
            .await
            .expect("activity");
        assert_eq!(row.status.as_str(), "dead_lettered");
        assert_eq!(row.last_error_category.as_deref(), Some("timeout"));
        assert!(row.provider_result_json.is_none());
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LeasedCleanupActivity;

impl DurableActivity for LeasedCleanupActivity {
    type Topic = CaptureTopic;
    const KIND: &'static str = "leased_cleanup_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 3;
    const TIMEOUT: Duration = Duration::from_millis(60);
    const LEASE_DURATION: Duration = Duration::from_millis(180);

    fn topic() -> Self::Topic {
        CaptureTopic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("retry policy")
    }
}

#[derive(Default)]
struct LeasedCleanupContext {
    cleanup_started: tokio::sync::Notify,
    release_cleanup: tokio::sync::Notify,
    cleaned: AtomicBool,
}

#[async_trait]
impl ActivityHandler for LeasedCleanupActivity {
    type Context = LeasedCleanupContext;
    type Output = ();

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<(), ActivityError> {
        context
            .cancellation_token()
            .expect("cancellation")
            .cancelled()
            .await;
        context.application().cleanup_started.notify_one();
        context.application().release_cleanup.notified().await;
        context.application().cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}

fn leased_cleanup_worker(
    pool: durable_workflows::DurablePool,
    context: Arc<LeasedCleanupContext>,
    id: &str,
) -> durable_workflows::ActivityWorker<LeasedCleanupContext> {
    durable_workflows::ActivityWorker::new(
        pool,
        context,
        Arc::new(
            durable_workflows::register_durable_activities!(
                LeasedCleanupContext; LeasedCleanupActivity
            )
            .expect("activities"),
        ),
        Arc::new(durable_workflows::register_durable_topics!(CaptureTopic).expect("topics")),
        id,
        WorkerConfig::default()
            .with_heartbeat_interval(Duration::from_millis(20))
            .with_shutdown_grace(Duration::from_secs(2)),
    )
    .expect("worker")
}

// Budgets: a 180ms lease renewed every 20ms, and a 250ms wait that outlasts
// one lease, so a reclaim is refused only because renewals continue. Only a
// host too loaded to run a 20ms heartbeat within 180ms fails this (seen only
// alongside check.sh; not reproduced under a parallel full suite run).
#[tokio::test]
async fn timeout_cleanup_renews_lease_until_handler_finishes_before_allowing_retry() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "capture", 3, 60, 180).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set((
            durable_activity::kind.eq(LeasedCleanupActivity::KIND),
            durable_activity::payload_json
                .eq(serde_json::to_string(&LeasedCleanupActivity).expect("payload")),
        ))
        .execute(&mut connection)
        .await
        .expect("leased activity payload");
    drop(connection);

    let context = Arc::new(LeasedCleanupContext::default());
    let source = leased_cleanup_worker(pool.clone(), context.clone(), "source");
    let run = tokio::spawn(async move { source.run_one("capture").await });
    tokio::time::timeout(Duration::from_secs(2), context.cleanup_started.notified())
        .await
        .expect("timeout starts cleanup");
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!AtomicBool::load(&context.cleaned, Ordering::SeqCst));

    let replacement = leased_cleanup_worker(
        pool.clone(),
        Arc::new(LeasedCleanupContext::default()),
        "replacement",
    );
    let first_claim = replacement.claim_one("capture").await.expect("first probe");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second_claim = replacement
        .claim_one("capture")
        .await
        .expect("second probe");
    context.release_cleanup.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("bounded completion")
        .expect("source join");

    assert!(
        first_claim.is_none() && second_claim.is_none(),
        "another worker must not reclaim the attempt while its timed-out handler is still cleaning up"
    );
    assert_eq!(
        result
            .expect("timeout persisted")
            .expect("claimed activity")
            .get(),
        activity_id
    );
    assert!(AtomicBool::load(&context.cleaned, Ordering::SeqCst));
    let mut connection = pool.get().await.expect("connection");
    let row = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("finished attempt");
    assert_eq!(row.status.as_str(), "pending");
    assert_eq!(row.attempt_count, 1);
    assert_eq!(row.last_error_category.as_deref(), Some("timeout"));
    assert!(row.provider_result_json.is_none());
    drop(connection);
    let retry = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(claim) = replacement
                .claim_one("capture")
                .await
                .expect("retry lookup")
            {
                break claim;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("retry becomes available after cleanup and its configured delay");
    assert_eq!(retry.activity_id().expect("retry id").get(), activity_id);
    assert_eq!(retry.attempt_number().expect("retry attempt"), 2);
}

#[tokio::test]
async fn heartbeat_failure_limits_cleanup_to_the_last_confirmed_lease() {
    // Every renewal and the test's own checkout must finish well inside these budgets;
    // a 180ms lease with a 40ms checkout lapsed on slow CI runners before cleanup began.
    const LEASE_MILLIS: i64 = 720;
    for (healthy_millis, checkout_millis, release_millis, should_clean, block_release) in [
        (0, 160, 1_400, false, false),
        (0, 1_000, 1_400, false, false),
        (0, 4_000, 1_400, false, false),
        (1_000, 100, 300, true, false),
        (0, 4_000, 1_400, false, true),
    ] {
        let Some(pool) = support::fresh_pool().await else {
            return;
        };
        let (_, activity_id) = schedule_activity(&pool, "capture", 3, 60, LEASE_MILLIS).await;
        let mut connection = pool.get().await.expect("connection");
        diesel::update(durable_activity::table.find(activity_id))
            .set((
                durable_activity::kind.eq(LeasedCleanupActivity::KIND),
                durable_activity::payload_json
                    .eq(serde_json::to_string(&LeasedCleanupActivity).expect("payload")),
            ))
            .execute(&mut connection)
            .await
            .expect("leased activity payload");
        drop(connection);
        let url = support::durable_database_url().expect("fixture URL");
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            durable_workflows::DurableConnection,
        >::new(url);
        let limited_pool = diesel_async::pooled_connection::bb8::Pool::builder()
            .max_size(1)
            // Connect before the worker starts: the short checkout timeout bounds lease
            // renewal, not connection setup (Postgres SCRAM auth can exceed 40ms).
            .min_idle(Some(1))
            .connection_timeout(Duration::from_millis(checkout_millis))
            .build(manager)
            .await
            .expect("short-checkout worker pool");
        let context = Arc::new(LeasedCleanupContext::default());
        let source = leased_cleanup_worker(limited_pool.clone(), context.clone(), "source");
        let mut run = tokio::spawn(async move { source.run_one("capture").await });
        tokio::select! {
            () = context.cleanup_started.notified() => {}
            finished = &mut run => panic!(
                "run_one finished before cleanup started (checkout={checkout_millis}): {finished:?}"
            ),
            () = tokio::time::sleep(Duration::from_secs(2)) => panic!(
                "timeout starts cleanup (checkout={checkout_millis})"
            ),
        }
        tokio::time::sleep(Duration::from_millis(healthy_millis)).await;
        // A renewal in flight can outlast one short checkout; the lease is still healthy then.
        let held = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(held) = limited_pool.get().await {
                    break held;
                }
            }
        })
        .await
        .expect("hold the only worker connection");
        if block_release {
            // Make the handler and expired lease ready together on this current-thread runtime.
            std::thread::sleep(Duration::from_millis(release_millis));
        } else {
            tokio::time::sleep(Duration::from_millis(release_millis)).await;
        }
        context.release_cleanup.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("bounded failed-heartbeat cleanup")
            .expect("source join");
        drop(held);
        {
            use diesel::IntoSql;
            let mut recycled = limited_pool
                .get()
                .await
                .expect("worker pool remains usable");
            let value = diesel::select(1_i32.into_sql::<diesel::sql_types::Integer>())
                .get_result::<i32>(&mut *recycled)
                .await
                .expect("query after interrupted renewal");
            assert_eq!(value, 1);
        }
        assert!(
            result.is_err(),
            "heartbeat failure must remain the activity result"
        );
        assert_eq!(
            AtomicBool::load(&context.cleaned, Ordering::SeqCst),
            should_clean,
            "cleanup must stay inside the confirmed lease: healthy={healthy_millis}, checkout={checkout_millis}, release={release_millis}, blocking={block_release}"
        );
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LockingProgressActivity;

impl DurableActivity for LockingProgressActivity {
    type Topic = CaptureTopic;
    const KIND: &'static str = "locking_progress_activity";
    const VERSION: i32 = 1;
    const MAX_ATTEMPTS: u32 = 1;
    const TIMEOUT: Duration = Duration::from_millis(60);
    const LEASE_DURATION: Duration = Duration::from_secs(30);

    fn topic() -> Self::Topic {
        CaptureTopic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("retry policy")
    }
}

#[derive(Default)]
struct LockingProgressContext {
    started: tokio::sync::Notify,
    report: tokio::sync::Notify,
}

#[async_trait]
impl ActivityHandler for LockingProgressActivity {
    type Context = LockingProgressContext;
    type Output = ();

    async fn execute(
        &self,
        context: ActivityContext<'_, Self::Context>,
    ) -> Result<(), ActivityError> {
        context.application().started.notify_one();
        context.application().report.notified().await;
        // The report locks the activity row, then waits on the test's
        // uncommitted event with the same sequence. The handler ignores
        // cancellation, so the runtime gives up on it mid-transaction.
        let _ = context
            .progress_reporter()
            .expect("progress reporter")
            .report(durable_workflows::ProgressEvent::new("blocked", "held"))
            .await;
        std::future::pending::<()>().await;
        Ok(())
    }
}

// N5: finishing an attempt must not wait on a row lock held by the handler
// it gave up on. Budgets: a 60ms timeout and a 150ms grace end the attempt
// at ~210ms; the blocking event is rolled back at 600ms (on MySQL the finish
// waits for it: the event's foreign-key check holds a shared lock on the
// attempt row). Without the fix the finish waits on the abandoned progress
// transaction forever (Postgres) or for innodb_lock_wait_timeout (MySQL).
// Heartbeats are 10s apart so none runs while the row is locked.
#[tokio::test]
async fn timed_out_handler_holding_the_activity_row_lock_does_not_block_its_finish() {
    use diesel_async::SimpleAsyncConnection;
    use durable_workflows::{persistence::NewProgressEventRow, schema::durable_progress_event};

    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let (_, activity_id) = schedule_activity(&pool, "capture", 1, 60, 30_000).await;
    let mut connection = pool.get().await.expect("connection");
    diesel::update(durable_activity::table.find(activity_id))
        .set((
            durable_activity::kind.eq(LockingProgressActivity::KIND),
            durable_activity::payload_json
                .eq(serde_json::to_string(&LockingProgressActivity).expect("payload")),
        ))
        .execute(&mut connection)
        .await
        .expect("retarget activity");
    drop(connection);

    let context = Arc::new(LockingProgressContext::default());
    let worker = durable_workflows::ActivityWorker::new(
        pool.clone(),
        context.clone(),
        Arc::new(
            durable_workflows::register_durable_activities!(
                LockingProgressContext; LockingProgressActivity
            )
            .expect("activities"),
        ),
        Arc::new(durable_workflows::register_durable_topics!(CaptureTopic).expect("topics")),
        "locking-progress-worker",
        WorkerConfig::default()
            .with_heartbeat_interval(Duration::from_secs(10))
            .with_shutdown_grace(Duration::from_millis(150)),
    )
    .expect("worker");
    let run = tokio::spawn(async move { worker.run_one("capture").await });
    context.started.notified().await;

    let now = support::db_now(&pool).await;
    let mut blocker = pool.get().await.expect("blocker connection");
    blocker.batch_execute("BEGIN").await.expect("begin blocker");
    diesel::insert_into(durable_progress_event::table)
        .values(NewProgressEventRow {
            activity_id,
            attempt_number: 1,
            sequence: 1,
            code: "blocker".to_string(),
            description: String::new(),
            description_bytes: 0,
            completed_units: None,
            total_units: None,
            severity: "info".to_string(),
            metadata_json: None,
            created_at: now,
        })
        .execute(&mut blocker)
        .await
        .expect("blocking progress event");
    context.report.notify_one();
    tokio::time::sleep(Duration::from_millis(600)).await;
    blocker
        .batch_execute("ROLLBACK")
        .await
        .expect("release blocker");
    drop(blocker);

    let result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("finish must not wait on the abandoned handler's row lock")
        .expect("worker join")
        .expect("timeout outcome persisted");
    assert_eq!(result.expect("claimed activity").get(), activity_id);
    let mut connection = pool.get().await.expect("connection");
    let row = durable_activity::table
        .find(activity_id)
        .select(ActivityRow::as_select())
        .first::<ActivityRow>(&mut connection)
        .await
        .expect("activity");
    assert_eq!(row.status.as_str(), "dead_lettered");
    assert_eq!(row.last_error_category.as_deref(), Some("timeout"));
}
