//! The supervisor's restart policy against a real database (INVARIANTS
//! §2.1): a database outage restarts tasks with backoff without spending the
//! restart budget, an outage longer than `max_transient_outage` stops the
//! runtime, and every stop of the runtime is logged once with its cause.
//!
//! The outage is a TCP proxy in front of the test database that refuses
//! connections and drops the open ones, as a restarting database host does
//! (issue #4: a PlanetScale tablet restart, "connection refused").

mod support;

use std::{
    io::Write,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use diesel::QueryDsl;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use durable_workflows::{
    schema::durable_workflow, ActivityRegistry, DurableConnection, DurablePool, DurableRuntime,
    DurableStore, DurableWorkflow, RuntimeConfig, StartOptions, TopicRegistry, WorkflowContext,
    WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowTransition,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Probe;

impl DurableWorkflow for Probe {
    const KIND: &'static str = "runtime_restart_probe";
    const VERSION: i32 = 1;
}

impl WorkflowHandler for Probe {
    type Context = ();
    type State = ();
    type Approval = ();
    type Output = ();
    fn initial_state(&self) {}

    async fn step(
        &self,
        _: WorkflowContext<'_, ()>,
        _: (),
        _: WorkflowEvent,
    ) -> Result<WorkflowTransition<(), (), ()>, WorkflowError> {
        Ok(WorkflowTransition::Complete { output: () })
    }
}

/// A TCP proxy to the test database that can go down: it then refuses new
/// connections (its listener is closed) and drops the open ones.
struct FlakyProxy {
    address: SocketAddr,
    upstream: String,
    up: Mutex<CancellationToken>,
}

impl FlakyProxy {
    async fn start(upstream: String) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let proxy = Arc::new(Self {
            address: listener.local_addr().expect("proxy address"),
            upstream,
            up: Mutex::new(CancellationToken::new()),
        });
        proxy.serve(listener);
        proxy
    }

    fn serve(self: &Arc<Self>, listener: TcpListener) {
        let up = self.up.lock().unwrap().clone();
        let upstream = self.upstream.clone();
        tokio::spawn(async move {
            loop {
                let (mut client, _) = tokio::select! {
                    _ = up.cancelled() => return,
                    accepted = listener.accept() => match accepted {
                        Ok(accepted) => accepted,
                        Err(_) => continue,
                    },
                };
                let up = up.clone();
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    tokio::select! {
                        _ = up.cancelled() => {}
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                    }
                });
            }
        });
    }

    fn go_down(&self) {
        self.up.lock().unwrap().cancel();
    }

    async fn come_up(self: &Arc<Self>) {
        // The closed listener's port is free again; nothing else binds a
        // fixed port in the suite.
        let listener = loop {
            match TcpListener::bind(self.address).await {
                Ok(listener) => break listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        *self.up.lock().unwrap() = CancellationToken::new();
        self.serve(listener);
    }

    /// `url` with its host and port replaced by the proxy's.
    fn url(&self, url: &str) -> String {
        let (before, after) = url.split_once('@').expect("database URL has credentials");
        let path = after.find('/').expect("database URL has a database name");
        format!("{before}@{}{}", self.address, &after[path..])
    }
}

fn upstream_address(url: &str) -> String {
    let (_, after) = url.split_once('@').expect("database URL has credentials");
    after[..after.find('/').expect("database URL has a database name")].to_string()
}

async fn pool_for(url: String) -> DurablePool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<DurableConnection>::new(
            url,
        );
    diesel_async::pooled_connection::bb8::Pool::builder()
        .max_size(4)
        .connection_timeout(Duration::from_millis(250))
        .build(manager)
        .await
        .expect("durable pool")
}

/// Log lines written on this test's thread. `#[tokio::test]` runs the runtime's
/// tasks on the test thread, so a thread-local subscriber sees them.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let logs = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(move || logs.clone())
                .finish(),
        )
    }

    fn lines(&self, message: &str) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|line| line.contains(message))
            .map(str::to_string)
            .collect()
    }
}

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

const TASK_STOPPED: &str = "durable runtime task stopped";
const RUNTIME_STOPPED: &str = "durable runtime stopped";

/// Short delays so a task hits the database often; a budget of one restart.
fn config() -> RuntimeConfig {
    RuntimeConfig::default()
        .with_idle_delay(Duration::from_millis(20))
        .with_restart_backoff(Duration::from_millis(10))
        .with_max_restart_backoff(Duration::from_millis(80))
        .with_max_task_restarts(1)
        .with_timer_poll_interval(Duration::from_millis(20))
        .with_approval_expiry_poll_interval(Duration::from_millis(20))
        .with_health_scan_interval(Duration::from_millis(50))
}

fn runtime(pool: DurablePool, id: &str, config: RuntimeConfig) -> DurableRuntime<()> {
    let workflows =
        durable_workflows::register_durable_workflows!((); Probe).expect("workflow registry");
    DurableRuntime::new(
        pool,
        Arc::new(()),
        Arc::new(workflows),
        Arc::new(ActivityRegistry::new()),
        Arc::new(TopicRegistry::new()),
        id,
        config,
    )
    .expect("runtime definition")
}

#[test]
fn runtime_defaults_back_off_exponentially_and_bound_transient_outages() {
    let config = RuntimeConfig::default();
    assert_eq!(config.restart_backoff, Duration::from_secs(1));
    assert_eq!(config.max_restart_backoff, Duration::from_secs(60));
    assert_eq!(config.task_healthy_after, Duration::from_secs(2 * 60));
    assert_eq!(config.max_transient_outage, Duration::from_secs(15 * 60));
    assert_eq!(config.max_task_restarts, 8);
}

#[tokio::test]
async fn runtime_rejects_zero_restart_bounds() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    for config in [
        RuntimeConfig::default().with_task_healthy_after(Duration::ZERO),
        RuntimeConfig::default().with_max_transient_outage(Duration::ZERO),
    ] {
        assert!(matches!(
            DurableRuntime::new(
                pool.clone(),
                Arc::new(()),
                Arc::new(durable_workflows::WorkflowRegistry::<()>::new()),
                Arc::new(ActivityRegistry::new()),
                Arc::new(TopicRegistry::new()),
                "invalid-restart-bounds",
                config,
            ),
            Err(durable_workflows::DurableError::InvalidDefinition(_))
        ));
    }
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn a_database_outage_does_not_spend_the_restart_budget() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let url = support::durable_database_url().expect("test database URL");
    let proxy = FlakyProxy::start(upstream_address(&url)).await;
    let logs = CapturedLogs::default();
    let _logging = logs.install();
    let handle = runtime(pool_for(proxy.url(&url)).await, "outage-runtime", config())
        .spawn()
        .await
        .expect("runtime starts");
    let completion = handle.completion_token();

    proxy.go_down();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let transient_stops = logs
        .lines(TASK_STOPPED)
        .into_iter()
        .filter(|line| line.contains("failure_kind=\"transient_unavailable\""))
        .count();
    assert!(
        transient_stops > 1,
        "the outage failed tasks more often than the budget of 1 allows; got {transient_stops}:\n{}",
        logs.lines(TASK_STOPPED).join("\n")
    );
    assert!(
        !completion.is_cancelled(),
        "a database outage stopped the runtime: {:?}",
        logs.lines(RUNTIME_STOPPED)
    );
    let faults: Vec<_> = logs
        .lines(TASK_STOPPED)
        .into_iter()
        .filter(|line| !line.contains("failure_kind=\"transient_"))
        .collect();
    assert!(
        faults.is_empty(),
        "outage errors classified as faults: {faults:#?}"
    );

    // The runtime recovers on its own once the database is back.
    proxy.come_up().await;
    let workflow_id = DurableStore::new(pool.clone())
        .start(&Probe, StartOptions::default())
        .await
        .expect("probe starts")
        .workflow_id;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut connection = pool.get().await.expect("test connection");
        let status: String = durable_workflow::table
            .find(workflow_id)
            .select(durable_workflow::status)
            .first(&mut connection)
            .await
            .expect("probe status");
        if status == "succeeded" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline && !completion.is_cancelled(),
            "the runtime did not resume after the outage; probe is {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(logs.lines(RUNTIME_STOPPED).is_empty());

    let shutdown = handle
        .shutdown(Duration::from_secs(5))
        .await
        .expect_err("the outage's task errors are reported at shutdown");
    assert!(shutdown.errors.iter().all(|error| !error.panicked));
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn an_outage_longer_than_the_limit_stops_the_runtime_and_logs_why() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let url = support::durable_database_url().expect("test database URL");
    let proxy = FlakyProxy::start(upstream_address(&url)).await;
    let logs = CapturedLogs::default();
    let _logging = logs.install();
    let handle = runtime(
        pool_for(proxy.url(&url)).await,
        "outage-limit-runtime",
        config().with_max_transient_outage(Duration::from_millis(300)),
    )
    .spawn()
    .await
    .expect("runtime starts");
    let completion = handle.completion_token();

    proxy.go_down();
    tokio::time::timeout(Duration::from_secs(20), completion.cancelled())
        .await
        .expect("an outage past max_transient_outage stops the runtime");

    let stops = logs.lines(RUNTIME_STOPPED);
    assert_eq!(
        stops.len(),
        1,
        "one runtime stop is logged once: {stops:#?}"
    );
    let stop = &stops[0];
    assert!(
        stop.contains("reason=\"transient_outage_exceeded\""),
        "{stop}"
    );
    assert!(
        stop.contains("failure_kind=\"transient_unavailable\""),
        "{stop}"
    );
    assert!(stop.contains("task="), "{stop}");
    assert!(stop.contains("last_error="), "{stop}");
    handle
        .shutdown(Duration::from_secs(5))
        .await
        .expect_err("the stop is reported at shutdown");
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn an_exhausted_restart_budget_stops_the_runtime_and_logs_the_task_and_error() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    let logs = CapturedLogs::default();
    let _logging = logs.install();
    let handle = runtime(pool.clone(), "budget-runtime", config())
        .spawn()
        .await
        .expect("runtime starts");
    let completion = handle.completion_token();

    // A schema fault, not an outage: every approval-expiry poll fails the
    // same way, and each failure spends the budget.
    let mut connection = pool.get().await.expect("test connection");
    connection
        .batch_execute("DROP TABLE durable_approval")
        .await
        .expect("approval table dropped");
    drop(connection);
    tokio::time::timeout(Duration::from_secs(20), completion.cancelled())
        .await
        .expect("a fault past max_task_restarts stops the runtime");

    let stops = logs.lines(RUNTIME_STOPPED);
    assert_eq!(
        stops.len(),
        1,
        "one runtime stop is logged once: {stops:#?}"
    );
    let stop = &stops[0];
    assert!(
        stop.contains("reason=\"restart_budget_exhausted\""),
        "{stop}"
    );
    assert!(stop.contains("failure_kind=\"fault\""), "{stop}");
    assert!(
        stop.contains("task=approval-expiry") || stop.contains("task=health"),
        "{stop}"
    );
    assert!(
        stop.contains("durable_approval"),
        "the last error is named: {stop}"
    );
    handle
        .shutdown(Duration::from_secs(5))
        .await
        .expect_err("the faults are reported at shutdown");
    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}
