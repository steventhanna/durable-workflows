use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{Id, JoinError, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    observability::{
        emit_readiness_alerts, emit_schedule_materialization_alert, HealthScanReport,
        HealthScanner, HealthScannerConfig,
    },
    persistence, ActivityRegistry, ActivityWorker, ApprovalExpiryMaterializer, CoordinatorConfig,
    DurableError, DurablePool, ReadinessReport, ScheduleMaterializer, ScheduleRegistry,
    TimerMaterializer, TopicRegistry, WorkerConfig, WorkflowCoordinator, WorkflowRegistry,
};

const MAX_TEMPORAL_POLL_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
pub struct RuntimeConfig {
    pub coordinator: CoordinatorConfig,
    pub worker: WorkerConfig,
    pub idle_delay: Duration,
    pub restart_backoff: Duration,
    pub forced_shutdown_timeout: Duration,
    pub max_task_restarts: u32,
    pub max_workers_per_topic: u32,
    pub health_scan_interval: Duration,
    pub health_stale_after: Duration,
    pub max_health_alerts_per_kind: u32,
    pub timer_poll_interval: Duration,
    pub approval_expiry_poll_interval: Duration,
    pub schedule_poll_interval: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            coordinator: CoordinatorConfig::default(),
            worker: WorkerConfig::default(),
            idle_delay: Duration::from_secs(2),
            restart_backoff: Duration::from_secs(1),
            forced_shutdown_timeout: Duration::from_secs(5),
            max_task_restarts: 8,
            max_workers_per_topic: 4,
            health_scan_interval: Duration::from_secs(60),
            health_stale_after: Duration::from_secs(5 * 60),
            max_health_alerts_per_kind: 25,
            timer_poll_interval: Duration::from_secs(10),
            approval_expiry_poll_interval: Duration::from_secs(10),
            schedule_poll_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTaskError {
    pub task: String,
    pub message: String,
    pub panicked: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("durable runtime stopped with {count} task error(s)", count = .errors.len())]
pub struct RuntimeShutdownError {
    pub errors: Vec<RuntimeTaskError>,
}

/// Callback invoked with every health scan report, after the report is logged.
/// Runs on the health task; implementations must not block.
pub type HealthAlertSink = Arc<dyn Fn(&HealthScanReport) + Send + Sync>;

pub struct DurableRuntime<C> {
    pool: DurablePool,
    context: Arc<C>,
    workflows: Arc<WorkflowRegistry<C>>,
    activities: Arc<ActivityRegistry<C>>,
    topics: Arc<TopicRegistry>,
    schedules: Arc<ScheduleRegistry<C>>,
    runtime_id: String,
    config: RuntimeConfig,
    health_alert_sink: Option<HealthAlertSink>,
    topic_worker_limits: HashMap<String, u32>,
}

impl<C> DurableRuntime<C>
where
    C: Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: DurablePool,
        context: Arc<C>,
        workflows: Arc<WorkflowRegistry<C>>,
        activities: Arc<ActivityRegistry<C>>,
        topics: Arc<TopicRegistry>,
        runtime_id: impl Into<String>,
        config: RuntimeConfig,
    ) -> Result<Self, DurableError> {
        let runtime_id = runtime_id.into();
        if runtime_id.is_empty()
            || config.idle_delay.is_zero()
            || config.restart_backoff.is_zero()
            || config.forced_shutdown_timeout.is_zero()
            || config.max_task_restarts == 0
            || config.max_workers_per_topic == 0
            || config.health_scan_interval.is_zero()
            || config.health_stale_after.is_zero()
            || config.max_health_alerts_per_kind == 0
            || config.max_health_alerts_per_kind > 100
            || !valid_temporal_poll_interval(config.timer_poll_interval)
            || !valid_temporal_poll_interval(config.approval_expiry_poll_interval)
            || !valid_temporal_poll_interval(config.schedule_poll_interval)
        {
            return Err(DurableError::InvalidDefinition(
                "durable runtime identity and bounds must be non-zero".to_string(),
            ));
        }
        HealthScannerConfig {
            stale_after: config.health_stale_after,
            max_alerts_per_kind: config.max_health_alerts_per_kind,
        }
        .validate()?;
        Ok(Self {
            pool,
            context,
            workflows,
            activities,
            topics,
            schedules: Arc::new(ScheduleRegistry::new()),
            runtime_id,
            config,
            health_alert_sink: None,
            topic_worker_limits: HashMap::new(),
        })
    }

    pub fn with_schedules(mut self, schedules: Arc<ScheduleRegistry<C>>) -> Self {
        self.schedules = schedules;
        self
    }

    pub fn with_health_alert_sink(mut self, sink: HealthAlertSink) -> Self {
        self.health_alert_sink = Some(sink);
        self
    }

    /// Overrides local capacity for one registered topic; the global limit still applies.
    pub fn with_topic_worker_limit(
        mut self,
        topic: impl Into<String>,
        limit: u32,
    ) -> Result<Self, DurableError> {
        let topic = topic.into();
        if limit == 0 || self.topics.get(&topic).is_none() {
            return Err(DurableError::InvalidDefinition(
                "local worker limit requires a registered topic and non-zero capacity".to_string(),
            ));
        }
        self.topic_worker_limits.insert(topic, limit);
        Ok(self)
    }

    pub async fn spawn(self) -> Result<RuntimeHandle, DurableError> {
        let deployed_at = database_now(&self.pool).await?;
        for definition in self.schedules.definitions() {
            if let Err(error) = self
                .schedules
                .reconcile_state(&definition.key, &self.pool, deployed_at)
                .await
            {
                emit_schedule_materialization_alert(&definition, &error, deployed_at);
                return Err(error);
            }
        }
        let mut connection = self.pool.get().await?;
        self.topics.seed_locks(&mut connection).await?;
        let readiness = ReadinessReport::query(
            &mut connection,
            &self.workflows,
            &self.activities,
            &self.topics,
        )
        .await?;
        let readiness_checked_at = persistence::database_now_millis(&mut connection).await?;
        emit_readiness_alerts(&readiness, readiness_checked_at);
        readiness.ensure_ready()?;
        drop(connection);

        let cancellation = CancellationToken::new();
        let forced_cancellation = CancellationToken::new();
        let completion = CancellationToken::new();
        let (activity_executions, activity_execution_collector) = ActivityExecutionManager::new();
        let parts = Arc::new(RuntimeParts {
            pool: self.pool,
            context: self.context,
            workflows: self.workflows,
            activities: self.activities,
            topics: self.topics,
            schedules: self.schedules,
            runtime_id: self.runtime_id,
            config: self.config,
            cancellation: cancellation.clone(),
            forced_cancellation: forced_cancellation.clone(),
            health_alert_sink: self.health_alert_sink,
            activity_executions,
            topic_worker_limits: self.topic_worker_limits,
        });
        let completion_guard = CompletionGuard(completion.clone());
        let supervisor = tokio::spawn(async move {
            let _completion_guard = completion_guard;
            supervise(parts, activity_execution_collector).await
        });
        Ok(RuntimeHandle {
            cancellation,
            forced_cancellation,
            completion,
            supervisor,
            forced_shutdown_timeout: self.config.forced_shutdown_timeout,
        })
    }
}

pub struct RuntimeHandle {
    cancellation: CancellationToken,
    forced_cancellation: CancellationToken,
    completion: CancellationToken,
    supervisor: JoinHandle<Vec<RuntimeTaskError>>,
    forced_shutdown_timeout: Duration,
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.forced_cancellation.cancel();
        self.supervisor.abort();
    }
}

impl RuntimeHandle {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn completion_token(&self) -> CancellationToken {
        self.completion.clone()
    }

    pub async fn shutdown(mut self, deadline: Duration) -> Result<(), RuntimeShutdownError> {
        self.cancellation.cancel();
        let errors = match tokio::time::timeout(deadline, &mut self.supervisor).await {
            Ok(Ok(errors)) => errors,
            Ok(Err(error)) => vec![join_error("supervisor", error)],
            Err(_) => {
                self.forced_cancellation.cancel();
                let deadline_error = RuntimeTaskError {
                    task: "supervisor".to_string(),
                    message: format!("shutdown deadline elapsed after {deadline:?}"),
                    panicked: false,
                };
                match tokio::time::timeout(self.forced_shutdown_timeout, &mut self.supervisor).await
                {
                    Ok(Ok(mut errors)) => {
                        errors.push(deadline_error);
                        errors
                    }
                    Ok(Err(error)) => vec![deadline_error, join_error("supervisor", error)],
                    Err(_) => {
                        self.supervisor.abort();
                        let _ = (&mut self.supervisor).await;
                        vec![
                            deadline_error,
                            RuntimeTaskError {
                                task: "supervisor".to_string(),
                                message: format!(
                                    "forced shutdown did not reach a safe point after {:?}",
                                    self.forced_shutdown_timeout
                                ),
                                panicked: false,
                            },
                        ]
                    }
                }
            }
        };
        if errors.is_empty() {
            Ok(())
        } else {
            Err(RuntimeShutdownError { errors })
        }
    }
}

struct RuntimeParts<C> {
    pool: DurablePool,
    context: Arc<C>,
    workflows: Arc<WorkflowRegistry<C>>,
    activities: Arc<ActivityRegistry<C>>,
    topics: Arc<TopicRegistry>,
    schedules: Arc<ScheduleRegistry<C>>,
    runtime_id: String,
    config: RuntimeConfig,
    cancellation: CancellationToken,
    forced_cancellation: CancellationToken,
    health_alert_sink: Option<HealthAlertSink>,
    activity_executions: ActivityExecutionManager,
    topic_worker_limits: HashMap<String, u32>,
}

type ActivityExecution = Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send>>;
type ActivityExecutionCollector =
    Pin<Box<dyn Future<Output = Result<(), RuntimeTaskFailure>> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivityExecutionSnapshot {
    total: usize,
    active_by_topic: HashMap<String, usize>,
}

enum ActivityExecutionCommand {
    Spawn {
        topic: String,
        dispatcher_generation: u64,
        execution: ActivityExecution,
        response: oneshot::Sender<Result<(), RuntimeTaskFailure>>,
    },
    Snapshot {
        response: oneshot::Sender<ActivityExecutionSnapshot>,
    },
    TakeFailure {
        response: oneshot::Sender<Option<RuntimeTaskFailure>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

struct ActivityExecutionManager {
    commands: mpsc::Sender<ActivityExecutionCommand>,
    changes: watch::Receiver<u64>,
    next_dispatcher_generation: Arc<AtomicU64>,
    dispatcher_generation: Option<u64>,
}

impl Clone for ActivityExecutionManager {
    fn clone(&self) -> Self {
        let mut changes = self.changes.clone();
        changes.borrow_and_update();
        Self {
            commands: self.commands.clone(),
            changes,
            next_dispatcher_generation: self.next_dispatcher_generation.clone(),
            dispatcher_generation: self.dispatcher_generation,
        }
    }
}

impl ActivityExecutionManager {
    fn new() -> (Self, ActivityExecutionCollector) {
        let (commands, receiver) = mpsc::channel(64);
        let (change_sender, changes) = watch::channel(0);
        let collector = Box::pin(collect_activity_executions(receiver, change_sender));
        (
            Self {
                commands,
                changes,
                next_dispatcher_generation: Arc::new(AtomicU64::new(1)),
                dispatcher_generation: None,
            },
            collector,
        )
    }

    fn for_dispatcher(&self) -> Self {
        let mut manager = self.clone();
        manager.dispatcher_generation = Some(
            manager
                .next_dispatcher_generation
                .fetch_add(1, Ordering::Relaxed),
        );
        manager
    }

    async fn spawn<F>(&mut self, topic: String, execution: F) -> Result<(), RuntimeTaskFailure>
    where
        F: Future<Output = Result<(), DurableError>> + Send + 'static,
    {
        let (response, result) = oneshot::channel();
        self.commands
            .send(ActivityExecutionCommand::Spawn {
                topic,
                dispatcher_generation: self.dispatcher_generation.ok_or_else(|| {
                    RuntimeTaskFailure {
                        message: "activity execution manager is not bound to a dispatcher"
                            .to_string(),
                        panicked: false,
                    }
                })?,
                execution: Box::pin(execution),
                response,
            })
            .await
            .map_err(|_| activity_execution_manager_unavailable())?;
        let result = result
            .await
            .map_err(|_| activity_execution_manager_unavailable())?;
        self.changes.borrow_and_update();
        result
    }

    async fn snapshot(&self) -> Result<ActivityExecutionSnapshot, RuntimeTaskFailure> {
        let (response, snapshot) = oneshot::channel();
        self.commands
            .send(ActivityExecutionCommand::Snapshot { response })
            .await
            .map_err(|_| activity_execution_manager_unavailable())?;
        snapshot
            .await
            .map_err(|_| activity_execution_manager_unavailable())
    }

    async fn take_failure(&self) -> Result<Option<RuntimeTaskFailure>, RuntimeTaskFailure> {
        let (response, failure) = oneshot::channel();
        self.commands
            .send(ActivityExecutionCommand::TakeFailure { response })
            .await
            .map_err(|_| activity_execution_manager_unavailable())?;
        failure
            .await
            .map_err(|_| activity_execution_manager_unavailable())
    }

    async fn changed(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), RuntimeTaskFailure> {
        tokio::select! {
            _ = cancellation.cancelled() => Ok(()),
            changed = self.changes.changed() => {
                changed.map_err(|_| activity_execution_manager_unavailable())
            }
        }
    }

    async fn shutdown(&self) -> Result<(), RuntimeTaskFailure> {
        let (response, shutdown) = oneshot::channel();
        self.commands
            .send(ActivityExecutionCommand::Shutdown { response })
            .await
            .map_err(|_| activity_execution_manager_unavailable())?;
        shutdown
            .await
            .map_err(|_| activity_execution_manager_unavailable())
    }
}

fn activity_execution_manager_unavailable() -> RuntimeTaskFailure {
    RuntimeTaskFailure {
        message: "activity execution manager is unavailable".to_string(),
        panicked: false,
    }
}

async fn collect_activity_executions(
    mut commands: mpsc::Receiver<ActivityExecutionCommand>,
    changes: watch::Sender<u64>,
) -> Result<(), RuntimeTaskFailure> {
    let mut executions = JoinSet::new();
    let mut tasks_by_id = HashMap::<Id, ActivityExecutionTask>::new();
    let mut active_by_dispatcher = HashMap::<u64, usize>::new();
    let mut failed_dispatchers = HashSet::<u64>::new();
    let mut snapshot = ActivityExecutionSnapshot {
        total: 0,
        active_by_topic: HashMap::new(),
    };
    let mut failures = VecDeque::new();
    let mut supplemental_failures = VecDeque::new();
    let mut shutting_down = false;
    let mut shutdown_response = None::<oneshot::Sender<()>>;

    loop {
        if shutting_down && executions.is_empty() {
            if let Some(response) = shutdown_response.take() {
                let _ = response.send(());
            }
            failures.append(&mut supplemental_failures);
            return finish_activity_execution_collection(failures);
        }
        tokio::select! {
            biased;
            result = executions.join_next_with_id(), if !executions.is_empty() => {
                if let Some(result) = result {
                    collect_activity_execution_result(
                        &mut tasks_by_id,
                        &mut active_by_dispatcher,
                        &mut failed_dispatchers,
                        &mut snapshot,
                        &mut failures,
                        &mut supplemental_failures,
                        result,
                    )?;
                    changes.send_modify(|generation| {
                        *generation = generation.wrapping_add(1);
                    });
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return Err(RuntimeTaskFailure {
                        message: "activity execution manager command channel closed".to_string(),
                        panicked: false,
                    });
                };
                match command {
                    ActivityExecutionCommand::Spawn {
                        topic,
                        dispatcher_generation,
                        execution,
                        response,
                    } => {
                        if shutting_down {
                            let _ = response.send(Err(RuntimeTaskFailure {
                                message: "activity execution manager is shutting down".to_string(),
                                panicked: false,
                            }));
                            continue;
                        }
                        let task = executions.spawn(execution);
                        tasks_by_id.insert(
                            task.id(),
                            ActivityExecutionTask {
                                topic: topic.clone(),
                                dispatcher_generation,
                            },
                        );
                        *active_by_dispatcher
                            .entry(dispatcher_generation)
                            .or_default() += 1;
                        snapshot.total += 1;
                        *snapshot.active_by_topic.entry(topic).or_default() += 1;
                        changes.send_modify(|generation| {
                            *generation = generation.wrapping_add(1);
                        });
                        let _ = response.send(Ok(()));
                    }
                    ActivityExecutionCommand::Snapshot { response } => {
                        let _ = response.send(snapshot.clone());
                    }
                    ActivityExecutionCommand::TakeFailure { response } => {
                        let failure = take_activity_execution_failure(&mut failures);
                        if let Err(Some(failure)) = response.send(failure) {
                            failures.push_front(failure);
                        }
                    }
                    ActivityExecutionCommand::Shutdown { response } => {
                        shutting_down = true;
                        shutdown_response = Some(response);
                    }
                }
            }
        }
    }
}

struct ActivityExecutionTask {
    topic: String,
    dispatcher_generation: u64,
}

fn take_activity_execution_failure(
    failures: &mut VecDeque<RuntimeTaskFailure>,
) -> Option<RuntimeTaskFailure> {
    let mut failure = failures.pop_front()?;
    let additional = failures.len();
    while let Some(next) = failures.pop_front() {
        failure.panicked |= next.panicked;
    }
    if additional > 0 {
        let suffix = if additional == 1 { "task" } else { "tasks" };
        failure.message = format!(
            "{}; {additional} additional activity execution {suffix} failed",
            failure.message
        );
    }
    Some(failure)
}

fn collect_activity_execution_result(
    tasks_by_id: &mut HashMap<Id, ActivityExecutionTask>,
    active_by_dispatcher: &mut HashMap<u64, usize>,
    failed_dispatchers: &mut HashSet<u64>,
    snapshot: &mut ActivityExecutionSnapshot,
    failures: &mut VecDeque<RuntimeTaskFailure>,
    supplemental_failures: &mut VecDeque<RuntimeTaskFailure>,
    result: Result<(Id, Result<(), DurableError>), JoinError>,
) -> Result<(), RuntimeTaskFailure> {
    let task_id = match &result {
        Ok((task_id, _)) => *task_id,
        Err(error) => error.id(),
    };
    let task = tasks_by_id
        .remove(&task_id)
        .ok_or_else(|| RuntimeTaskFailure {
            message: format!("activity execution task {task_id} had no topic record"),
            panicked: false,
        })?;
    let topic = task.topic;
    snapshot.total = snapshot
        .total
        .checked_sub(1)
        .ok_or_else(activity_execution_manager_unavailable)?;
    let active = snapshot
        .active_by_topic
        .get_mut(&topic)
        .ok_or_else(|| RuntimeTaskFailure {
            message: format!("activity execution topic {topic} had no active count"),
            panicked: false,
        })?;
    *active = active
        .checked_sub(1)
        .ok_or_else(activity_execution_manager_unavailable)?;

    match result {
        Ok((_, Ok(()))) => {}
        Ok((_, Err(error))) => {
            tracing::error!(
                topic = %topic,
                error = %error,
                "durable activity execution failed"
            );
        }
        Err(error) => {
            let failure = RuntimeTaskFailure {
                message: format!("activity execution task failed: {error}"),
                panicked: error.is_panic(),
            };
            if failed_dispatchers.insert(task.dispatcher_generation) {
                failures.push_back(failure);
            } else {
                tracing::error!(
                    dispatcher_generation = task.dispatcher_generation,
                    panicked = failure.panicked,
                    error = %failure.message,
                    "additional durable activity execution task failed in the same dispatcher incident"
                );
                supplemental_failures.push_back(failure);
            }
        }
    }
    let group_active = active_by_dispatcher
        .get_mut(&task.dispatcher_generation)
        .ok_or_else(activity_execution_manager_unavailable)?;
    *group_active = group_active
        .checked_sub(1)
        .ok_or_else(activity_execution_manager_unavailable)?;
    if *group_active == 0 {
        active_by_dispatcher.remove(&task.dispatcher_generation);
    }
    Ok(())
}

fn finish_activity_execution_collection(
    mut failures: VecDeque<RuntimeTaskFailure>,
) -> Result<(), RuntimeTaskFailure> {
    let Some(first_failure) = failures.pop_front() else {
        return Ok(());
    };
    for failure in failures {
        tracing::error!(
            panicked = failure.panicked,
            error = %failure.message,
            "additional durable activity execution task failed during shutdown"
        );
    }
    Err(first_failure)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TaskSpec {
    ActivityExecutionManager,
    Coordinator,
    Health,
    Timer,
    ApprovalExpiry,
    Schedule { key: String },
    ActivityDispatcher,
}

#[derive(Debug)]
struct RuntimeTaskFailure {
    message: String,
    panicked: bool,
}

impl From<DurableError> for RuntimeTaskFailure {
    fn from(error: DurableError) -> Self {
        Self {
            message: error.to_string(),
            panicked: false,
        }
    }
}

impl TaskSpec {
    fn name(&self) -> String {
        match self {
            Self::ActivityExecutionManager => "activity-execution-manager".to_string(),
            Self::Coordinator => "coordinator".to_string(),
            Self::Health => "health".to_string(),
            Self::Timer => "timer".to_string(),
            Self::ApprovalExpiry => "approval-expiry".to_string(),
            Self::Schedule { key } => format!("schedule:{key}"),
            Self::ActivityDispatcher => "activity-dispatcher".to_string(),
        }
    }
}

async fn supervise<C>(
    parts: Arc<RuntimeParts<C>>,
    activity_execution_collector: ActivityExecutionCollector,
) -> Vec<RuntimeTaskError>
where
    C: Send + Sync + 'static,
{
    let mut tasks = JoinSet::<Result<(), RuntimeTaskFailure>>::new();
    let mut task_specs = HashMap::<Id, TaskSpec>::new();
    let collector_name = TaskSpec::ActivityExecutionManager.name();
    let collector = tasks.spawn(activity_execution_collector.instrument(tracing::info_span!(
        "durable.runtime.task",
        runtime_id = %parts.runtime_id,
        task = %collector_name,
    )));
    task_specs.insert(collector.id(), TaskSpec::ActivityExecutionManager);
    spawn_child(
        &mut tasks,
        &mut task_specs,
        parts.clone(),
        TaskSpec::Coordinator,
        false,
    );
    spawn_child(
        &mut tasks,
        &mut task_specs,
        parts.clone(),
        TaskSpec::Health,
        false,
    );
    spawn_child(
        &mut tasks,
        &mut task_specs,
        parts.clone(),
        TaskSpec::Timer,
        false,
    );
    spawn_child(
        &mut tasks,
        &mut task_specs,
        parts.clone(),
        TaskSpec::ApprovalExpiry,
        false,
    );
    for definition in parts.schedules.definitions() {
        spawn_child(
            &mut tasks,
            &mut task_specs,
            parts.clone(),
            TaskSpec::Schedule {
                key: definition.key,
            },
            false,
        );
    }
    spawn_child(
        &mut tasks,
        &mut task_specs,
        parts.clone(),
        TaskSpec::ActivityDispatcher,
        false,
    );

    let mut errors = Vec::new();
    let mut restarts = HashMap::<TaskSpec, u32>::new();
    while let Some(joined) = tasks.join_next_with_id().await {
        let (task_id, result) = match joined {
            Ok((task_id, result)) => (task_id, Ok(result)),
            Err(error) => (error.id(), Err(error)),
        };
        let Some(spec) = task_specs.remove(&task_id) else {
            errors.push(RuntimeTaskError {
                task: "supervisor".to_string(),
                message: format!("completed task {task_id} had no supervision record"),
                panicked: false,
            });
            parts.cancellation.cancel();
            continue;
        };
        if parts.cancellation.is_cancelled() {
            if let Err(error) = result {
                errors.push(join_error(&spec.name(), error));
            } else if let Ok(Err(error)) = result {
                errors.push(RuntimeTaskError {
                    task: spec.name(),
                    message: error.message,
                    panicked: error.panicked,
                });
            }
            if spec == TaskSpec::ActivityDispatcher {
                if let Err(error) = parts.activity_executions.shutdown().await {
                    errors.push(RuntimeTaskError {
                        task: TaskSpec::ActivityExecutionManager.name(),
                        message: error.message,
                        panicked: error.panicked,
                    });
                }
            }
            continue;
        }

        let task_error = match result {
            Ok(Ok(())) => RuntimeTaskError {
                task: spec.name(),
                message: "task stopped unexpectedly".to_string(),
                panicked: false,
            },
            Ok(Err(error)) => RuntimeTaskError {
                task: spec.name(),
                message: error.message,
                panicked: error.panicked,
            },
            Err(error) => join_error(&spec.name(), error),
        };
        tracing::error!(
            task = %task_error.task,
            panicked = task_error.panicked,
            error = %task_error.message,
            "durable runtime task stopped"
        );
        errors.push(task_error);
        if spec == TaskSpec::ActivityExecutionManager {
            parts.cancellation.cancel();
            continue;
        }
        let restart_count = restarts.entry(spec.clone()).or_default();
        *restart_count = restart_count.saturating_add(1);
        if *restart_count > parts.config.max_task_restarts {
            parts.cancellation.cancel();
            if spec == TaskSpec::ActivityDispatcher {
                if let Err(error) = parts.activity_executions.shutdown().await {
                    errors.push(RuntimeTaskError {
                        task: TaskSpec::ActivityExecutionManager.name(),
                        message: error.message,
                        panicked: error.panicked,
                    });
                }
            }
        } else {
            spawn_child(&mut tasks, &mut task_specs, parts.clone(), spec, true);
        }
    }
    errors
}

struct CompletionGuard(CancellationToken);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn spawn_child<C>(
    tasks: &mut JoinSet<Result<(), RuntimeTaskFailure>>,
    task_specs: &mut HashMap<Id, TaskSpec>,
    parts: Arc<RuntimeParts<C>>,
    spec: TaskSpec,
    delayed: bool,
) where
    C: Send + Sync + 'static,
{
    let task_spec = spec.clone();
    let task_name = spec.name();
    let runtime_id = parts.runtime_id.clone();
    let task = tasks.spawn(
        async move {
            if delayed {
                tokio::select! {
                    _ = parts.cancellation.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(parts.config.restart_backoff) => {}
                }
            }
            run_task(parts, spec).await
        }
        .instrument(tracing::info_span!(
            "durable.runtime.task",
            runtime_id = %runtime_id,
            task = %task_name,
        )),
    );
    task_specs.insert(task.id(), task_spec);
}

async fn run_task<C>(parts: Arc<RuntimeParts<C>>, spec: TaskSpec) -> Result<(), RuntimeTaskFailure>
where
    C: Send + Sync + 'static,
{
    match spec {
        TaskSpec::ActivityExecutionManager => Err(RuntimeTaskFailure {
            message: "activity execution manager cannot be started as a restartable task"
                .to_string(),
            panicked: false,
        }),
        TaskSpec::Coordinator => {
            let coordinator = WorkflowCoordinator::new(
                parts.pool.clone(),
                parts.context.clone(),
                parts.workflows.clone(),
                parts.activities.clone(),
                format!("{}:coordinator", parts.runtime_id),
                parts.config.coordinator,
            )?;
            loop {
                if parts.cancellation.is_cancelled() {
                    return Ok(());
                }
                if coordinator.activate_one().await?.is_none() {
                    wait_for_work(&parts.cancellation, parts.config.idle_delay).await;
                }
            }
        }
        TaskSpec::Health => {
            let scanner = HealthScanner::new(
                parts.pool.clone(),
                parts.workflows.clone(),
                parts.activities.clone(),
                parts.topics.clone(),
                HealthScannerConfig {
                    stale_after: parts.config.health_stale_after,
                    max_alerts_per_kind: parts.config.max_health_alerts_per_kind,
                },
            )?;
            loop {
                if parts.cancellation.is_cancelled() {
                    return Ok(());
                }
                let report = scanner.scan_once(database_now(&parts.pool).await?).await?;
                report.emit();
                if let Some(sink) = &parts.health_alert_sink {
                    // A faulty sink must not take down workflow execution: an
                    // uncaught panic here would count against max_task_restarts
                    // and eventually cancel the whole runtime.
                    let call =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(&report)));
                    if call.is_err() {
                        tracing::error!("health alert sink panicked; alerts were logged above");
                    }
                }
                wait_for_work(&parts.cancellation, parts.config.health_scan_interval).await;
            }
        }
        TaskSpec::Timer => {
            let materializer = TimerMaterializer::new(parts.pool.clone());
            loop {
                if parts.cancellation.is_cancelled() {
                    return Ok(());
                }
                if materializer.materialize_next().await?.is_none() {
                    wait_for_work(&parts.cancellation, parts.config.timer_poll_interval).await;
                }
            }
        }
        TaskSpec::ApprovalExpiry => {
            let materializer = ApprovalExpiryMaterializer::new(parts.pool.clone());
            loop {
                if parts.cancellation.is_cancelled() {
                    return Ok(());
                }
                if materializer.expire_next().await?.is_none() {
                    wait_for_work(
                        &parts.cancellation,
                        parts.config.approval_expiry_poll_interval,
                    )
                    .await;
                }
            }
        }
        TaskSpec::Schedule { key } => {
            let materializer = ScheduleMaterializer::new(
                parts.pool.clone(),
                parts.context.clone(),
                parts.schedules.clone(),
            );
            loop {
                if parts.cancellation.is_cancelled() {
                    return Ok(());
                }
                match materializer.materialize_schedule_now(&key).await {
                    Ok(report) => {
                        if report.inspected == 0 && report.started == 0 && report.queued == 0 {
                            wait_for_work(&parts.cancellation, parts.config.schedule_poll_interval)
                                .await;
                        }
                    }
                    Err(error) => {
                        let captured_at = database_now(&parts.pool).await?;
                        let definition =
                            parts
                                .schedules
                                .get(&key)
                                .ok_or_else(|| DurableError::NotFound {
                                    resource: "schedule definition",
                                    identifier: key.clone(),
                                })?;
                        emit_schedule_materialization_alert(definition, &error, captured_at);
                        if schedule_error_is_retryable(&error) {
                            return Err(error.into());
                        }
                        wait_for_work(&parts.cancellation, parts.config.schedule_poll_interval)
                            .await;
                    }
                }
            }
        }
        TaskSpec::ActivityDispatcher => {
            let activity_worker = Arc::new(
                ActivityWorker::new(
                    parts.pool.clone(),
                    parts.context.clone(),
                    parts.activities.clone(),
                    parts.topics.clone(),
                    format!("{}:dispatcher", parts.runtime_id),
                    parts.config.worker,
                )?
                .with_cancellation_token(parts.cancellation.clone())
                .with_forced_cancellation_token(parts.forced_cancellation.clone()),
            );
            let capacity: usize = parts
                .topics
                .definitions()
                .into_iter()
                .map(|topic| {
                    local_topic_limit(
                        &topic.key,
                        topic.max_concurrency,
                        parts.config.max_workers_per_topic,
                        &parts.topic_worker_limits,
                    )
                })
                .sum();
            let mut executions = parts.activity_executions.for_dispatcher();
            let idle_delays = [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(5),
                Duration::from_secs(10),
            ];
            let mut empty_sweeps = 0_usize;
            loop {
                if parts.cancellation.is_cancelled() {
                    return Ok(());
                }
                let pending_failure = executions.take_failure().await?;
                if let Some(failure) = pending_failure {
                    return Err(failure);
                }
                let snapshot = tokio::select! {
                    biased;
                    _ = parts.cancellation.cancelled() => return Ok(()),
                    snapshot = executions.snapshot() => snapshot?,
                };
                let available = capacity.saturating_sub(snapshot.total);
                if available == 0 {
                    executions.changed(&parts.cancellation).await?;
                    continue;
                }
                let local_topic_capacity: HashMap<_, _> = parts
                    .topics
                    .definitions()
                    .into_iter()
                    .map(|topic| {
                        let configured = local_topic_limit(
                            &topic.key,
                            topic.max_concurrency,
                            parts.config.max_workers_per_topic,
                            &parts.topic_worker_limits,
                        );
                        let active = snapshot
                            .active_by_topic
                            .get(&topic.key)
                            .copied()
                            .unwrap_or_default();
                        (topic.key, configured.saturating_sub(active))
                    })
                    .collect();
                let claims = activity_worker
                    .claim_batch(available, &local_topic_capacity)
                    .await?;
                if claims.is_empty() {
                    let delay = jittered_idle_delay(
                        idle_delays[empty_sweeps.min(idle_delays.len() - 1)],
                        &parts.runtime_id,
                    );
                    empty_sweeps = empty_sweeps.saturating_add(1);
                    tokio::select! {
                        _ = parts.cancellation.cancelled() => {},
                        result = executions.changed(&parts.cancellation) => result?,
                        _ = tokio::time::sleep(delay) => {}
                    }
                } else {
                    empty_sweeps = 0;
                    for claim in claims {
                        let topic = claim.topic().to_string();
                        let worker = activity_worker.clone();
                        executions
                            .spawn(
                                topic,
                                async move { worker.execute_claim_traced(claim).await },
                            )
                            .await?;
                    }
                }
            }
        }
    }
}

async fn wait_for_work(cancellation: &CancellationToken, idle_delay: Duration) {
    tokio::select! {
        _ = cancellation.cancelled() => {},
        _ = tokio::time::sleep(idle_delay) => {},
    }
}

async fn database_now(pool: &DurablePool) -> Result<i64, DurableError> {
    let mut connection = pool.get().await?;
    persistence::database_now_millis(&mut connection).await
}

fn valid_temporal_poll_interval(interval: Duration) -> bool {
    !interval.is_zero() && interval <= MAX_TEMPORAL_POLL_INTERVAL
}

fn local_topic_limit(
    topic: &str,
    global_limit: u32,
    default_local_limit: u32,
    overrides: &HashMap<String, u32>,
) -> usize {
    global_limit.min(overrides.get(topic).copied().unwrap_or(default_local_limit)) as usize
}

fn jittered_idle_delay(base: Duration, runtime_id: &str) -> Duration {
    let hash = runtime_id.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u64::from(byte))
    });
    // Keep the documented upper bound while spreading concurrent runtimes
    // across the final ten percent of each backoff interval.
    base.mul_f64(0.9 + (hash % 101) as f64 / 1_000.0)
}

fn schedule_error_is_retryable(error: &DurableError) -> bool {
    matches!(
        error,
        DurableError::Database(_)
            | DurableError::Pool(_)
            | DurableError::PoolCheckout(_)
            | DurableError::Serialization(_)
    )
}

fn join_error(task: &str, error: JoinError) -> RuntimeTaskError {
    RuntimeTaskError {
        task: task.to_string(),
        message: error.to_string(),
        panicked: error.is_panic(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    async fn test_activity_execution_manager() -> (
        ActivityExecutionManager,
        CancellationToken,
        JoinHandle<Result<(), RuntimeTaskFailure>>,
    ) {
        let cancellation = CancellationToken::new();
        let (manager, collector) = ActivityExecutionManager::new();
        let manager = manager.for_dispatcher();
        let collector = tokio::spawn(collector);
        (manager, cancellation, collector)
    }

    async fn wait_for_manager_idle(manager: &mut ActivityExecutionManager) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if manager.snapshot().await.expect("manager snapshot").total == 0 {
                    return;
                }
                manager
                    .changed(&CancellationToken::new())
                    .await
                    .expect("manager change");
            }
        })
        .await
        .expect("manager became idle");
    }

    #[tokio::test]
    async fn activity_execution_error_releases_capacity_without_failing_dispatcher() {
        let (mut manager, cancellation, collector) = test_activity_execution_manager().await;
        manager
            .spawn("external".to_string(), async {
                Err(DurableError::InvalidState("transient failure".to_string()))
            })
            .await
            .expect("execution submitted");

        wait_for_manager_idle(&mut manager).await;

        assert!(manager
            .take_failure()
            .await
            .expect("failure query")
            .is_none());
        assert_eq!(
            manager
                .snapshot()
                .await
                .expect("manager snapshot")
                .active_by_topic
                .get("external"),
            Some(&0)
        );
        manager.shutdown().await.expect("manager shutdown");
        cancellation.cancel();
        collector
            .await
            .expect("collector joined")
            .expect("collector exit");
    }

    #[tokio::test]
    async fn activity_execution_panic_is_reported_without_waiting_for_sibling() {
        let (mut manager, cancellation, collector) = test_activity_execution_manager().await;
        let release_sibling = Arc::new(tokio::sync::Notify::new());
        let sibling_completed = Arc::new(AtomicBool::new(false));
        let sibling_release = release_sibling.clone();
        let sibling_completion = sibling_completed.clone();
        manager
            .spawn("other".to_string(), async move {
                sibling_release.notified().await;
                sibling_completion.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .expect("sibling submitted");
        manager
            .spawn("external".to_string(), async {
                panic!("activity handler panic");
            })
            .await
            .expect("panic submitted");

        let failure = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(failure) = manager.take_failure().await.expect("failure query") {
                    return failure;
                }
                manager
                    .changed(&CancellationToken::new())
                    .await
                    .expect("manager change");
            }
        })
        .await
        .expect("panic reported before sibling release");

        assert!(failure.panicked);
        assert!(!sibling_completed.load(Ordering::SeqCst));
        assert_eq!(manager.snapshot().await.expect("manager snapshot").total, 1);

        let mut restarted_dispatcher = manager.clone();
        let snapshot = restarted_dispatcher
            .snapshot()
            .await
            .expect("restarted dispatcher snapshot");
        assert_eq!(snapshot.total, 1);
        assert_eq!(snapshot.active_by_topic.get("other"), Some(&1));

        release_sibling.notify_one();
        wait_for_manager_idle(&mut restarted_dispatcher).await;
        assert!(sibling_completed.load(Ordering::SeqCst));
        assert_eq!(
            restarted_dispatcher
                .snapshot()
                .await
                .expect("manager snapshot")
                .active_by_topic
                .get("other"),
            Some(&0)
        );

        manager.shutdown().await.expect("manager shutdown");
        cancellation.cancel();
        collector
            .await
            .expect("collector joined")
            .expect("collector exit");
    }

    #[tokio::test]
    async fn activity_execution_panic_discovered_during_shutdown_is_retained() {
        let (mut manager, _, collector) = test_activity_execution_manager().await;
        manager
            .spawn("external".to_string(), async {
                panic!("activity handler panic during shutdown");
            })
            .await
            .expect("panic submitted");
        wait_for_manager_idle(&mut manager).await;

        manager.shutdown().await.expect("manager shutdown");
        let failure = collector
            .await
            .expect("collector joined")
            .expect_err("shutdown must retain the unconsumed panic");

        assert!(failure.panicked);
    }

    #[tokio::test]
    async fn abandoned_failure_request_returns_panic_to_the_queue() {
        let (mut manager, _, collector) = test_activity_execution_manager().await;
        manager
            .spawn("external".to_string(), async {
                panic!("activity handler panic during cancellation");
            })
            .await
            .expect("panic submitted");
        wait_for_manager_idle(&mut manager).await;

        let (response, abandoned) = oneshot::channel();
        drop(abandoned);
        manager
            .commands
            .send(ActivityExecutionCommand::TakeFailure { response })
            .await
            .expect("failure request submitted");
        tokio::task::yield_now().await;

        let failure = manager
            .take_failure()
            .await
            .expect("failure query")
            .expect("abandoned panic was requeued");
        assert!(failure.panicked);

        manager.shutdown().await.expect("manager shutdown");
        collector
            .await
            .expect("collector joined")
            .expect("collector exit");
    }

    #[tokio::test]
    async fn activity_execution_panics_from_one_dispatcher_are_one_incident() {
        let (mut manager, _, collector) = test_activity_execution_manager().await;
        let release_late_panic = Arc::new(tokio::sync::Notify::new());
        let late_release = release_late_panic.clone();
        manager
            .spawn("external".to_string(), async {
                panic!("first activity handler panic");
            })
            .await
            .expect("first panic submitted");
        manager
            .spawn("external".to_string(), async move {
                late_release.notified().await;
                panic!("late sibling activity handler panic");
            })
            .await
            .expect("late panic submitted");

        let failure = manager
            .take_failure()
            .await
            .expect("failure query")
            .expect("panic burst reported");

        assert!(failure.panicked);
        release_late_panic.notify_one();
        wait_for_manager_idle(&mut manager).await;
        assert!(manager
            .take_failure()
            .await
            .expect("failure query")
            .is_none());

        manager.shutdown().await.expect("manager shutdown");
        let late_failure = collector
            .await
            .expect("collector joined")
            .expect_err("late sibling panic is retained for shutdown");
        assert!(late_failure.panicked);
        assert!(late_failure
            .message
            .contains("late sibling activity handler panic"));
    }

    #[test]
    fn topic_override_only_changes_selected_local_capacity_and_obeys_global_limit() {
        let limits = HashMap::from([("rtms".to_string(), 24)]);
        assert_eq!(local_topic_limit("rtms", 40, 4, &limits), 24);
        assert_eq!(local_topic_limit("billing", 40, 4, &limits), 4);
        assert_eq!(local_topic_limit("rtms", 12, 4, &limits), 12);
    }
}
