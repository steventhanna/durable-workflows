//! Concurrent, seeded workload recorded for trace checking against the Quint
//! model (`docs/design/trace-checking.md` §9(b), phase 4).
//!
//! Each seed runs 2 or 3 real `DurableRuntime`s on one database with short
//! leases and two topics (caps 1 and 2) for `DURABLE_TRACE_WORKLOAD_SECS` seconds
//! (default 20). A seeded driver starts workflows (some with a deduplication
//! key), cancels, pauses and resumes them, and crashes runtimes (the handle is
//! dropped without a graceful shutdown, `Crash` is recorded, and a replacement
//! runtime with a new id starts). Each runtime has its own connection pool, so
//! a crash closes its connections as a process exit does: in a shared pool an
//! aborted query's half-read connection goes back to the next user. Workflow steps and activity outcomes are
//! drawn from the seed keyed by (workflow id, command sequence) and (activity
//! id, attempt), so a replay after a crash takes the same step (the model's
//! assumption that `step` is deterministic). The database clock is real time.
//!
//! Every seed asserts that its own trace holds a `Crash`, a `TC1_Claim` that
//! recovered an expired workflow lease, a `TW1_Claim` that reconciled an
//! expired activity lease, and a fence miss: the driver aims its crashes at
//! runtimes that hold leases and its cancels and pauses at workflows whose
//! activity is running, which makes each of them reliable per seed.
//!
//! `DURABLE_TRACE_WORKLOAD_SEEDS=N` (N > 4) also runs seeds 5..=N in
//! `workload_extra_seeds`, four at a time; each gets its own database and
//! trace (named by its thread, `workload_seed_<n>`). The extra seeds assert
//! only that a crash happened.
#![cfg(feature = "trace-model")]

mod support;

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use diesel::{
    sql_types::{BigInt, Nullable, Text},
    QueryDsl, QueryableByName, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use durable_workflows::{
    admin::{AdminControlService, Operator},
    persistence::{ActivityRow, WorkflowRow},
    schema::{durable_activity, durable_workflow},
    trace::{record_local, Action},
    ActivityCommand, ActivityContext, ActivityError, ActivityHandler, ActivityRegistry,
    ActivityTopic, BackendKind, ChildWorkflowCommand, CoordinatorConfig, DurableActivity,
    DurablePool, DurableRuntime, DurableStore, DurableWorkflow, RetryPolicy, RuntimeConfig,
    RuntimeHandle, StartOptions, TopicRegistry, WorkerConfig, WorkflowContext, WorkflowError,
    WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowRegistry, WorkflowTransition, BACKEND,
};

/// Stop starting work once the trace holds this many records ...
const SOFT_STEP_CAP: i64 = 320;
/// ... and end the run at this many.
const HARD_STEP_CAP: i64 = 380;
/// Commands a workflow issues before it completes.
const MAX_COMMANDS: u32 = 5;
/// Children nest at most this deep.
const MAX_DEPTH: u32 = 2;
const MAX_CRASHES: u32 = 4;
/// The driver's pool and each runtime's.
const POOL_SIZE: u32 = 6;

// ---------------------------------------------------------------------------
// Seeded choices

fn mix(seed: u64, a: u64, b: u64, salt: u64) -> u64 {
    let mut z = seed
        ^ a.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ b.wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
        ^ salt.wrapping_mul(0x1656_67B1_9E37_79F9);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const SALT_FLOW: u64 = 1;
const SALT_PACE: u64 = 2;
const SALT_ACTIVITY: u64 = 3;
const SALT_DETAIL: u64 = 4;

/// The driver's own sequence of choices.
struct DriverRng(u64);

impl DriverRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0, 0, 0, 0)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> Option<T> {
        if items.is_empty() {
            None
        } else {
            let index = usize::try_from(self.below(items.len() as u64)).unwrap_or(0);
            items.get(index).copied()
        }
    }
}

/// Shared by every runtime of one seed.
struct Workload {
    seed: u64,
    /// (workflow, command sequence) pairs whose one-time slow step already ran.
    /// Timing only: a step's result never depends on it.
    slowed: Mutex<HashSet<(i64, u32)>>,
}

impl Workload {
    /// Some steps take a while; a few outlast the coordinator lease once, so
    /// another runtime recovers the workflow and the first one misses its fence.
    /// Only steps that insert no row (`stale_ok`: Continue, Complete) outlast
    /// it: a stale RunActivity / RunChild commit hits the recovering commit's
    /// unique key before its fence (a known engine issue, reported with the
    /// workload) and ends without a `CoordFenceMiss`.
    async fn pace_step(&self, workflow: i64, sequence: u32, stale_ok: bool) {
        let roll = mix(self.seed, workflow as u64, u64::from(sequence), SALT_PACE) % 100;
        let delay = if roll < 10 && stale_ok {
            let first = self
                .slowed
                .lock()
                .map(|mut slowed| slowed.insert((workflow, sequence)))
                .unwrap_or(false);
            if first {
                2_600
            } else {
                0
            }
        } else if roll < 30 {
            50 + roll * 15
        } else {
            0
        };
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Topics, activities, flow

#[derive(Clone, Copy)]
enum Topics {
    Serial,
    Pair,
}

impl ActivityTopic for Topics {
    fn key(self) -> &'static str {
        match self {
            Self::Serial => "serial",
            Self::Pair => "pair",
        }
    }

    fn max_concurrency(self) -> u32 {
        match self {
            Self::Serial => 1,
            Self::Pair => 2,
        }
    }
}

/// Success, retryable or permanent failure, a hang past the timeout, or a
/// slow success that heartbeats, drawn from (activity id, attempt).
async fn behave(context: &ActivityContext<'_, Workload>) -> Result<i32, ActivityError> {
    let workload = context.application();
    let activity = context.activity_id().map_or(0, |id| id.get());
    let attempt = context.attempt_number().unwrap_or(0);
    let roll = mix(
        workload.seed,
        activity as u64,
        u64::from(attempt),
        SALT_ACTIVITY,
    ) % 100;
    let detail = mix(
        workload.seed,
        activity as u64,
        u64::from(attempt),
        SALT_DETAIL,
    );
    match roll {
        0..=29 => Ok(1),
        30..=44 => Err(ActivityError::retryable(
            "workload",
            "seeded retryable failure",
        )),
        45..=49 => Err(ActivityError::permanent(
            "workload",
            "seeded permanent failure",
        )),
        50..=61 => {
            // Ignores cancellation: the worker's timeout and grace decide.
            tokio::time::sleep(Duration::from_millis(3_000 + detail % 1_500)).await;
            Ok(1)
        }
        _ => {
            tokio::time::sleep(Duration::from_millis(550 + detail % 400)).await;
            Ok(1)
        }
    }
}

macro_rules! workload_activity {
    ($name:ident, $kind:literal, $topic:expr) => {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct $name;

        impl DurableActivity for $name {
            type Topic = Topics;

            const KIND: &'static str = $kind;
            const VERSION: i32 = 1;
            const MAX_ATTEMPTS: u32 = 3;
            const TIMEOUT: Duration = Duration::from_secs(1);
            const LEASE_DURATION: Duration = Duration::from_secs(3);

            fn topic() -> Self::Topic {
                $topic
            }

            fn retry_policy() -> RetryPolicy {
                RetryPolicy::fixed(1).expect("workload retry policy is valid")
            }
        }

        #[async_trait]
        impl ActivityHandler for $name {
            type Context = Workload;
            type Output = i32;

            async fn execute(
                &self,
                context: ActivityContext<'_, Self::Context>,
            ) -> Result<Self::Output, ActivityError> {
                behave(&context).await
            }
        }
    };
}

workload_activity!(SerialTask, "serial_task", Topics::Serial);
workload_activity!(PairTask, "pair_task", Topics::Pair);

/// Continue / RunActivity / RunChild / Complete, drawn from (workflow id,
/// command sequence). The state is the command sequence.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WorkloadFlow {
    depth: u32,
}

impl DurableWorkflow for WorkloadFlow {
    const KIND: &'static str = "workload_flow";
    const VERSION: i32 = 1;
}

fn definition(error: durable_workflows::DurableError) -> WorkflowError {
    WorkflowError::new("definition", error.to_string())
}

#[async_trait]
impl WorkflowHandler for WorkloadFlow {
    type Context = Workload;
    type State = u32;
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn step(
        &self,
        context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError> {
        match event {
            WorkflowEvent::Started
            | WorkflowEvent::Continued
            | WorkflowEvent::ActivitySucceeded { .. }
            | WorkflowEvent::ChildSucceeded { .. }
            | WorkflowEvent::ChildFailed { .. } => {}
            _ => {
                return Err(WorkflowError::new(
                    "unexpected_event",
                    "workload flow received an unexpected event",
                ))
            }
        }
        let workload = context.application();
        let workflow = context.workflow_id().map_or(0, WorkflowId::get);
        let roll = mix(workload.seed, workflow as u64, u64::from(state), SALT_FLOW);
        let stale_ok = state >= MAX_COMMANDS || roll % 100 < 18 || roll % 100 > 77;
        workload.pace_step(workflow, state, stale_ok).await;
        if state >= MAX_COMMANDS {
            return Ok(WorkflowTransition::Complete { output: () });
        }
        let next = state + 1;
        match roll % 100 {
            0..=17 => Ok(WorkflowTransition::Continue { state: next }),
            choice @ 18..=77 if choice < 63 || self.depth >= MAX_DEPTH => {
                let activity = if (roll >> 8).is_multiple_of(2) {
                    ActivityCommand::new(&SerialTask, None)
                } else {
                    ActivityCommand::new(&PairTask, None)
                }
                .map_err(definition)?;
                Ok(WorkflowTransition::RunActivity {
                    state: next,
                    activity,
                })
            }
            18..=77 => {
                let child = ChildWorkflowCommand::new(&WorkloadFlow {
                    depth: self.depth + 1,
                })
                .map_err(definition)?;
                let child = match (roll >> 8) % 3 {
                    0 => child,
                    1 => child.with_deduplication_key("child-a"),
                    _ => child.with_deduplication_key("child-b"),
                };
                Ok(WorkflowTransition::RunChild { state: next, child })
            }
            _ => Ok(WorkflowTransition::Complete { output: () }),
        }
    }
}

// ---------------------------------------------------------------------------
// Runtimes

type Registries = (
    Arc<WorkflowRegistry<Workload>>,
    Arc<ActivityRegistry<Workload>>,
    Arc<TopicRegistry>,
);

fn registries() -> Registries {
    (
        Arc::new(
            durable_workflows::register_durable_workflows!(Workload; WorkloadFlow)
                .expect("workflow registry"),
        ),
        Arc::new(
            durable_workflows::register_durable_activities!(Workload; SerialTask, PairTask)
                .expect("activity registry"),
        ),
        Arc::new(
            durable_workflows::register_durable_topics!(Topics::Serial, Topics::Pair)
                .expect("topic registry"),
        ),
    )
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        coordinator: CoordinatorConfig {
            lease_duration: Duration::from_secs(2),
            ..CoordinatorConfig::default()
        },
        worker: WorkerConfig {
            heartbeat_interval: Duration::from_millis(500),
            shutdown_grace: Duration::from_millis(500),
        },
        idle_delay: Duration::from_millis(150),
        restart_backoff: Duration::from_millis(100),
        ..RuntimeConfig::default()
    }
}

struct LiveRuntime {
    id: String,
    handle: RuntimeHandle,
    pool: DurablePool,
}

async fn pool_for(url: &str) -> DurablePool {
    diesel_async::pooled_connection::bb8::Pool::builder()
        .max_size(POOL_SIZE)
        .build(
            diesel_async::pooled_connection::AsyncDieselConnectionManager::<
                durable_workflows::DurableConnection,
            >::new(url),
        )
        .await
        .expect("runtime pool")
}

async fn spawn_runtime(url: &str, workload: &Arc<Workload>, id: String) -> LiveRuntime {
    let (workflows, activities, topics) = registries();
    let pool = pool_for(url).await;
    let handle = DurableRuntime::new(
        pool.clone(),
        workload.clone(),
        workflows,
        activities,
        topics,
        id.clone(),
        runtime_config(),
    )
    .expect("runtime definition")
    .spawn()
    .await
    .expect("runtime spawns");
    LiveRuntime { id, handle, pool }
}

/// Drops the handle (no graceful shutdown) and the runtime's pool, waits for
/// its tasks to stop, then records `Crash` for it.
async fn crash(pool: &DurablePool, runtime: LiveRuntime) {
    let completion = runtime.handle.completion_token();
    drop(runtime.handle);
    drop(runtime.pool);
    let _ = tokio::time::timeout(Duration::from_secs(5), completion.cancelled()).await;
    // Aborted child tasks stop at their next poll; let them and any detached
    // local record land before the crash.
    tokio::time::sleep(Duration::from_millis(200)).await;
    record_local(
        pool,
        &format!("{}:dispatcher", runtime.id),
        Action::new("Crash", serde_json::json!({})),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Database probes

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct TraceRecord {
    #[diesel(sql_type = Text)]
    action: String,
    #[diesel(sql_type = Nullable<Text>)]
    params: Option<String>,
}

async fn trace_len(pool: &DurablePool) -> i64 {
    let mut connection = pool.get().await.expect("connection");
    diesel::sql_query("SELECT COUNT(*) AS n FROM durable_trace")
        .get_result::<Count>(&mut connection)
        .await
        .expect("trace count")
        .n
}

async fn trace_records(pool: &DurablePool) -> Vec<(String, serde_json::Value)> {
    let cast = match BACKEND {
        BackendKind::Mysql => "CAST(params_json AS CHAR)",
        BackendKind::Postgres => "CAST(params_json AS TEXT)",
    };
    let mut connection = pool.get().await.expect("connection");
    diesel::sql_query(format!(
        "SELECT action, {cast} AS params FROM durable_trace ORDER BY seq"
    ))
    .load::<TraceRecord>(&mut connection)
    .await
    .expect("trace records")
    .into_iter()
    .map(|record| {
        let params = record
            .params
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or(serde_json::Value::Null);
        (record.action, params)
    })
    .collect()
}

struct Snapshot {
    workflows: Vec<WorkflowRow>,
    activities: Vec<ActivityRow>,
}

async fn snapshot(pool: &DurablePool) -> Snapshot {
    let mut connection = pool.get().await.expect("connection");
    let workflows = durable_workflow::table
        .select(WorkflowRow::as_select())
        .order(durable_workflow::id)
        .load::<WorkflowRow>(&mut connection)
        .await
        .expect("workflow rows");
    let activities = durable_activity::table
        .select(ActivityRow::as_select())
        .order(durable_activity::id)
        .load::<ActivityRow>(&mut connection)
        .await
        .expect("activity rows");
    Snapshot {
        workflows,
        activities,
    }
}

fn wid(id: i64) -> WorkflowId {
    WorkflowId::new(id).expect("row ids are positive")
}

fn terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "cancelled")
}

impl Snapshot {
    /// Workflows whose current activity is running on a live runtime (a
    /// cancel or pause then revokes a live lease), else every open workflow in
    /// `allowed`. A workflow whose activity a crashed runtime held is left
    /// alone, so its expired lease is reconciled.
    fn targets(&self, allowed: &[&str], crashed: &HashSet<String>) -> Vec<i64> {
        let owner_crashed = |row: &ActivityRow| {
            row.lease_owner.as_deref().is_some_and(|owner| {
                crashed.contains(owner.split_once(':').map_or(owner, |(runtime, _)| runtime))
            })
        };
        let running: HashSet<i64> = self
            .activities
            .iter()
            .filter(|row| row.status.as_str() == "running" && !owner_crashed(row))
            .map(|row| row.id)
            .collect();
        let orphaned: HashSet<i64> = self
            .activities
            .iter()
            .filter(|row| row.status.as_str() == "running" && owner_crashed(row))
            .map(|row| row.id)
            .collect();
        let waits_on = |row: &WorkflowRow, set: &HashSet<i64>| {
            row.status.as_str() == "waiting_activity"
                && row
                    .wait_reference_id
                    .is_some_and(|activity| set.contains(&activity))
        };
        let hot: Vec<i64> = self
            .workflows
            .iter()
            .filter(|row| waits_on(row, &running))
            .map(|row| row.id)
            .collect();
        if !hot.is_empty() {
            return hot;
        }
        self.workflows
            .iter()
            .filter(|row| allowed.contains(&row.status.as_str()) && !waits_on(row, &orphaned))
            .map(|row| row.id)
            .collect()
    }

    fn paused(&self) -> Vec<i64> {
        self.workflows
            .iter()
            .filter(|row| row.status.as_str() == "paused")
            .map(|row| row.id)
            .collect()
    }

    /// Runtimes that hold an activity lease (`activities`) or a workflow
    /// lease right now.
    fn lease_holders(&self, activities: bool) -> Vec<String> {
        let owners: Vec<String> = if activities {
            self.activities
                .iter()
                .filter(|row| row.status.as_str() == "running")
                .filter_map(|row| row.lease_owner.clone())
                .collect()
        } else {
            self.workflows
                .iter()
                .filter(|row| row.status.as_str() == "running")
                .filter_map(|row| row.lease_owner.clone())
                .collect()
        };
        owners
            .into_iter()
            .map(|owner| {
                owner
                    .split_once(':')
                    .map_or(owner.clone(), |(runtime, _)| runtime.to_string())
            })
            .collect()
    }

    /// Workflows that still make progress on their own (not paused, blocked
    /// or finished).
    fn active(&self) -> usize {
        self.workflows
            .iter()
            .filter(|row| {
                let status = row.status.as_str();
                !terminal(status) && status != "paused" && status != "blocked"
            })
            .count()
    }
}

// ---------------------------------------------------------------------------
// Driver

#[derive(Debug, Default)]
struct Stats {
    records: usize,
    actions: BTreeMap<String, usize>,
    crashes: usize,
    recoveries: usize,
    reconciles: usize,
    fence_misses: usize,
}

impl Stats {
    fn from_records(records: &[(String, serde_json::Value)]) -> Self {
        let mut stats = Stats {
            records: records.len(),
            ..Stats::default()
        };
        for (action, params) in records {
            *stats.actions.entry(action.clone()).or_default() += 1;
            match action.as_str() {
                "Crash" => stats.crashes += 1,
                "TC1_Claim" if !params["recovered"].is_null() => stats.recoveries += 1,
                "TW1_Claim"
                    if params["reconciled"]
                        .as_array()
                        .is_some_and(|reconciled| !reconciled.is_empty()) =>
                {
                    stats.reconciles += 1;
                }
                "TW3_FenceMiss" | "TW2_FenceMiss" | "CoordFenceMiss" => stats.fence_misses += 1,
                _ => {}
            }
        }
        stats
    }
}

fn workload_secs() -> u64 {
    std::env::var("DURABLE_TRACE_WORKLOAD_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20)
}

async fn run_seed(seed: u64) -> Option<Stats> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::ERROR)
        .with_test_writer()
        .try_init();
    let pool = support::fresh_pool_with_max_size(POOL_SIZE).await?;
    let url = support::durable_database_url().expect("the seed's database url");
    let workload = Arc::new(Workload {
        seed,
        slowed: Mutex::new(HashSet::new()),
    });
    let mut rng = DriverRng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D));
    let runtime_count = 2 + rng.below(2);
    let mut next_runtime = 1;
    let mut runtimes = Vec::new();
    for _ in 0..runtime_count {
        runtimes.push(spawn_runtime(&url, &workload, format!("rt{next_runtime}")).await);
        next_runtime += 1;
    }
    let (workflows, activities, _) = registries();
    let admin = AdminControlService::new(pool.clone(), workflows, activities);
    let operator = Operator::new("workload", "trace workload").expect("operator");
    let store = DurableStore::new(pool.clone());

    let started = tokio::time::Instant::now();
    let total = Duration::from_secs(workload_secs());
    let mut crashes = 0;
    let mut crashed = HashSet::new();
    let mut starting = true;
    while started.elapsed() < total {
        tokio::time::sleep(Duration::from_millis(200 + rng.below(600))).await;
        let length = trace_len(&pool).await;
        if length >= HARD_STEP_CAP {
            break;
        }
        if length >= SOFT_STEP_CAP {
            starting = false;
        }
        if !starting {
            continue;
        }
        let state = snapshot(&pool).await;
        // At least two crashes (one per lease kind): forced from 40% and 70%
        // of the run on.
        let late = started.elapsed() * 5 > total * 2;
        let later = started.elapsed() * 10 > total * 7;
        let roll = if (crashes == 0 && late) || (crashes == 1 && later) {
            95
        } else {
            rng.below(100)
        };
        match roll {
            0..=54 => {
                // Keep the open set small so steps stay within the cap.
                if state.active() >= 16 {
                    continue;
                }
                for _ in 0..1 + rng.below(2) {
                    let options = if rng.below(5) == 0 {
                        StartOptions::default()
                            .with_deduplication_key(format!("start-{}", rng.below(3)))
                    } else {
                        StartOptions::default()
                    };
                    store
                        .start(&WorkloadFlow { depth: 0 }, options)
                        .await
                        .expect("workflow starts");
                }
            }
            55..=64 => {
                let open = state.targets(
                    &[
                        "ready",
                        "running",
                        "waiting_activity",
                        "waiting_child",
                        "paused",
                        "blocked",
                    ],
                    &crashed,
                );
                if let Some(target) = rng.pick(&open) {
                    let mut connection = pool.get().await.expect("connection");
                    // A workflow that finished since the snapshot refuses the cancel.
                    let _ = DurableStore::cancel_with_conn(
                        &mut connection,
                        wid(target),
                        "workload cancel",
                    )
                    .await;
                }
            }
            65..=77 => {
                let paused = state.paused();
                if !paused.is_empty() && rng.below(3) != 0 {
                    if let Some(target) = rng.pick(&paused) {
                        let _ = admin.resume_workflow(wid(target), &operator).await;
                    }
                } else {
                    let open =
                        state.targets(&["ready", "waiting_activity", "waiting_child"], &crashed);
                    if let Some(target) = rng.pick(&open) {
                        let _ = admin.pause_workflow(wid(target), &operator).await;
                    }
                }
            }
            78..=99 if crashes < MAX_CRASHES => {
                // Alternate between activity and workflow lease holders, so a
                // reconcile and a recovery follow: wait up to 2 s for one.
                let activities = crashes.is_multiple_of(2);
                let mut victim = None;
                let polling = tokio::time::Instant::now();
                while victim.is_none() && polling.elapsed() < Duration::from_secs(2) {
                    let holders: Vec<usize> = snapshot(&pool)
                        .await
                        .lease_holders(activities)
                        .iter()
                        .filter_map(|holder| {
                            runtimes.iter().position(|runtime| &runtime.id == holder)
                        })
                        .collect();
                    victim = rng.pick(&holders);
                    if victim.is_none() {
                        tokio::time::sleep(Duration::from_millis(40)).await;
                    }
                }
                let victim = victim.unwrap_or_else(|| {
                    usize::try_from(rng.below(runtimes.len() as u64)).unwrap_or(0)
                });
                let runtime = runtimes.remove(victim);
                crashed.insert(runtime.id.clone());
                crash(&pool, runtime).await;
                crashes += 1;
                runtimes.push(spawn_runtime(&url, &workload, format!("rt{next_runtime}")).await);
                next_runtime += 1;
            }
            _ => {}
        }
    }

    for runtime in &runtimes {
        runtime.handle.cancellation_token().cancel();
    }
    for runtime in runtimes {
        let _ = runtime.handle.shutdown(Duration::from_secs(5)).await;
    }
    let stats = Stats::from_records(&trace_records(&pool).await);
    eprintln!(
        "trace_workload seed {seed}: {} records, {} runtimes started, crashes {}, recoveries {}, reconciles {}, fence misses {}, actions {:?}",
        stats.records,
        next_runtime - 1,
        stats.crashes,
        stats.recoveries,
        stats.reconciles,
        stats.fence_misses,
        stats.actions
    );
    Some(stats)
}

fn assert_interesting(seed: u64, stats: &Stats) {
    assert!(stats.crashes > 0, "seed {seed}: no Crash in the trace");
    assert!(
        stats.recoveries > 0,
        "seed {seed}: no TC1_Claim recovered a workflow lease"
    );
    assert!(
        stats.reconciles > 0,
        "seed {seed}: no TW1_Claim reconciled an activity lease"
    );
    assert!(
        stats.fence_misses > 0,
        "seed {seed}: no fence miss (TW3_FenceMiss, TW2_FenceMiss, CoordFenceMiss)"
    );
}

macro_rules! seed_test {
    ($name:ident, $seed:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            if let Some(stats) = run_seed($seed).await {
                assert_interesting($seed, &stats);
            }
        }
    };
}

seed_test!(workload_seed_1, 1);
seed_test!(workload_seed_2, 2);
seed_test!(workload_seed_3, 3);
seed_test!(workload_seed_4, 4);

/// Seeds 5..=`DURABLE_TRACE_WORKLOAD_SEEDS`, four at a time, each on a thread
/// named after its seed so its trace gets its own name.
#[test]
fn workload_extra_seeds() {
    let seeds = std::env::var("DURABLE_TRACE_WORKLOAD_SEEDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(4);
    let extra: Vec<u64> = (5..=seeds).collect();
    for batch in extra.chunks(4) {
        let threads: Vec<_> = batch
            .iter()
            .map(|&seed| {
                std::thread::Builder::new()
                    .name(format!("workload_seed_{seed}"))
                    .spawn(move || {
                        let runtime = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(4)
                            .enable_all()
                            .build()
                            .expect("tokio runtime");
                        runtime.block_on(run_seed(seed)).map(|stats| stats.crashes)
                    })
                    .expect("seed thread")
            })
            .collect();
        for (thread, seed) in threads.into_iter().zip(batch) {
            if let Some(crashes) = thread.join().expect("seed thread joins") {
                assert!(crashes > 0, "seed {seed}: no Crash in the trace");
            }
        }
    }
}
