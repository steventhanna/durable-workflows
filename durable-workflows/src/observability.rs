use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError,
    },
    time::Duration,
};

use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;

use crate::{
    persistence::{ActivityStatus, WorkflowStatus},
    schema::{durable_activity, durable_workflow},
    ActivityId, ActivityRegistry, DurableError, DurablePool, ReadinessReport,
    ScheduleDefinitionMetadata, TopicRegistry, WorkflowId, WorkflowRegistry,
};

const MAX_HEALTH_ALERTS_PER_KIND: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct HealthScannerConfig {
    pub stale_after: Duration,
    pub max_alerts_per_kind: u32,
}

impl Default for HealthScannerConfig {
    fn default() -> Self {
        Self {
            stale_after: Duration::from_secs(5 * 60),
            max_alerts_per_kind: 25,
        }
    }
}

/// Builder-style setters. The struct is `#[non_exhaustive]`: start from
/// [`HealthScannerConfig::default`] and override fields with these. Bounds (non-zero
/// durations and counts) are checked where the config is used, not here.
impl HealthScannerConfig {
    #[must_use]
    pub const fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    #[must_use]
    pub const fn with_max_alerts_per_kind(mut self, max_alerts_per_kind: u32) -> Self {
        self.max_alerts_per_kind = max_alerts_per_kind;
        self
    }
}

impl HealthScannerConfig {
    pub(crate) fn validate(&self) -> Result<(), DurableError> {
        if self.stale_after.is_zero()
            || self.max_alerts_per_kind == 0
            || self.max_alerts_per_kind > MAX_HEALTH_ALERTS_PER_KIND
        {
            return Err(DurableError::InvalidDefinition(format!(
                "health scanner bounds require a non-zero stale duration and 1 to {MAX_HEALTH_ALERTS_PER_KIND} alerts per kind"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HealthAlert {
    MissingWorkflowDefinition {
        kind: String,
        version: i32,
    },
    MissingActivityDefinition {
        kind: String,
        version: i32,
    },
    MissingTopic {
        topic: String,
    },
    ActivationExhausted {
        workflow_id: WorkflowId,
        kind: String,
        version: i32,
    },
    ActivityDeadLettered {
        activity_id: ActivityId,
        workflow_id: WorkflowId,
        kind: String,
        version: i32,
        topic: String,
    },
    StaleWorkflow {
        workflow_id: WorkflowId,
        kind: String,
        version: i32,
    },
    StaleActivity {
        activity_id: ActivityId,
        workflow_id: WorkflowId,
        kind: String,
        version: i32,
        topic: String,
    },
    /// More than `max_errors` workflow activations of this process rolled back
    /// on a transient database error within one `window`. `errors` is the
    /// highest count one window reached since the previous health report.
    TransientActivationErrors {
        errors: u32,
        max_errors: u32,
        window: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthScanReport {
    pub captured_at: i64,
    pub alerts: Vec<HealthAlert>,
}

impl HealthScanReport {
    pub fn emit(&self) {
        for alert in &self.alerts {
            match alert {
                HealthAlert::MissingWorkflowDefinition { kind, version } => tracing::error!(
                    alert_kind = "missing_workflow_definition",
                    definition_kind = %kind,
                    definition_version = *version,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::MissingActivityDefinition { kind, version } => tracing::error!(
                    alert_kind = "missing_activity_definition",
                    definition_kind = %kind,
                    definition_version = *version,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::MissingTopic { topic } => tracing::error!(
                    alert_kind = "missing_topic",
                    topic = %topic,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::ActivationExhausted {
                    workflow_id,
                    kind,
                    version,
                } => tracing::error!(
                    alert_kind = "activation_exhausted",
                    workflow_id = workflow_id.get(),
                    definition_kind = %kind,
                    definition_version = *version,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::ActivityDeadLettered {
                    activity_id,
                    workflow_id,
                    kind,
                    version,
                    topic,
                } => tracing::error!(
                    alert_kind = "activity_dead_lettered",
                    workflow_id = workflow_id.get(),
                    activity_id = activity_id.get(),
                    definition_kind = %kind,
                    definition_version = *version,
                    topic = %topic,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::StaleWorkflow {
                    workflow_id,
                    kind,
                    version,
                } => tracing::error!(
                    alert_kind = "stale_workflow",
                    workflow_id = workflow_id.get(),
                    definition_kind = %kind,
                    definition_version = *version,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::StaleActivity {
                    activity_id,
                    workflow_id,
                    kind,
                    version,
                    topic,
                } => tracing::error!(
                    alert_kind = "stale_activity",
                    workflow_id = workflow_id.get(),
                    activity_id = activity_id.get(),
                    definition_kind = %kind,
                    definition_version = *version,
                    topic = %topic,
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
                HealthAlert::TransientActivationErrors {
                    errors,
                    max_errors,
                    window,
                } => tracing::error!(
                    alert_kind = "transient_activation_errors",
                    errors = *errors,
                    max_errors = *max_errors,
                    window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX),
                    captured_at = self.captured_at,
                    "durable workflow health alert"
                ),
            }
        }
    }
}

pub(crate) const DEFAULT_MAX_TRANSIENT_ACTIVATION_ERRORS: u32 = 10;
pub(crate) const DEFAULT_TRANSIENT_ACTIVATION_WINDOW: Duration = Duration::from_secs(5 * 60);

/// Why an activation that `WorkflowCoordinator::activate_one` skipped rolled
/// back (G1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BenignActivationKind {
    /// The claim lost its fence (`DurableError::FencedWrite`): an operator
    /// action or lease recovery moved the row on.
    FenceMiss,
    /// The database aborted the transaction with a deadlock, serialization
    /// failure or lock wait timeout (`DurableError::is_transient`).
    Transient,
}

impl BenignActivationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FenceMiss => "fence_miss",
            Self::Transient => "transient",
        }
    }
}

/// Process-local counts of benign activation outcomes (G1), shared by every
/// coordinator of one runtime. Read them with [`ActivationCounters::get`];
/// `RuntimeHandle::activation_counters` returns the runtime's instance.
#[derive(Debug)]
pub struct ActivationCounters {
    fence_misses: AtomicU64,
    transient_errors: AtomicU64,
    transient_window: Mutex<TransientErrorWindow>,
}

impl Default for ActivationCounters {
    fn default() -> Self {
        Self::with_window(TransientErrorWindow {
            max_errors: DEFAULT_MAX_TRANSIENT_ACTIVATION_ERRORS,
            window: DEFAULT_TRANSIENT_ACTIVATION_WINDOW,
            count: 0,
            window_started: None,
            peak_over_limit: None,
        })
    }
}

impl ActivationCounters {
    pub(crate) fn new(max_transient_errors: u32, window: Duration) -> Result<Self, DurableError> {
        Ok(Self::with_window(TransientErrorWindow::new(
            max_transient_errors,
            window,
        )?))
    }

    fn with_window(window: TransientErrorWindow) -> Self {
        Self {
            fence_misses: AtomicU64::new(0),
            transient_errors: AtomicU64::new(0),
            transient_window: Mutex::new(window),
        }
    }

    /// Benign activations of `kind` recorded since the counters were created.
    pub fn get(&self, kind: BenignActivationKind) -> u64 {
        self.counter(kind).load(Ordering::Relaxed)
    }

    fn counter(&self, kind: BenignActivationKind) -> &AtomicU64 {
        match kind {
            BenignActivationKind::FenceMiss => &self.fence_misses,
            BenignActivationKind::Transient => &self.transient_errors,
        }
    }

    pub(crate) fn record(&self, kind: BenignActivationKind, now: tokio::time::Instant) {
        self.counter(kind).fetch_add(1, Ordering::Relaxed);
        match kind {
            BenignActivationKind::FenceMiss => {}
            BenignActivationKind::Transient => self.window().record(now),
        }
    }

    /// The transient-error alert latched since the previous call, if any.
    pub(crate) fn take_transient_alert(&self) -> Option<HealthAlert> {
        self.window().take_alert()
    }

    fn window(&self) -> std::sync::MutexGuard<'_, TransientErrorWindow> {
        self.transient_window
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Transient activation errors counted in fixed windows, like the runtime's
/// restart budget: a window starts at the first error after the previous one
/// ended. A window that holds more than `max_errors` latches an alert until
/// [`TransientErrorWindow::take_alert`], so a burst between two health scans
/// is still reported.
#[derive(Debug)]
pub(crate) struct TransientErrorWindow {
    max_errors: u32,
    window: Duration,
    count: u32,
    window_started: Option<tokio::time::Instant>,
    peak_over_limit: Option<u32>,
}

impl TransientErrorWindow {
    pub(crate) fn new(max_errors: u32, window: Duration) -> Result<Self, DurableError> {
        if max_errors == 0 || window.is_zero() {
            return Err(DurableError::InvalidDefinition(
                "transient activation error alerts require a non-zero error limit and window"
                    .to_string(),
            ));
        }
        Ok(Self {
            max_errors,
            window,
            count: 0,
            window_started: None,
            peak_over_limit: None,
        })
    }

    pub(crate) fn record(&mut self, now: tokio::time::Instant) {
        let expired = self
            .window_started
            .is_none_or(|started| now.saturating_duration_since(started) > self.window);
        if expired {
            self.count = 0;
            self.window_started = Some(now);
        }
        self.count = self.count.saturating_add(1);
        if self.count > self.max_errors {
            self.peak_over_limit = Some(
                self.peak_over_limit
                    .map_or(self.count, |peak| peak.max(self.count)),
            );
        }
    }

    pub(crate) fn take_alert(&mut self) -> Option<HealthAlert> {
        self.peak_over_limit
            .take()
            .map(|errors| HealthAlert::TransientActivationErrors {
                errors,
                max_errors: self.max_errors,
                window: self.window,
            })
    }
}

pub struct HealthScanner<C> {
    pool: DurablePool,
    workflows: Arc<WorkflowRegistry<C>>,
    activities: Arc<ActivityRegistry<C>>,
    topics: Arc<TopicRegistry>,
    config: HealthScannerConfig,
}

impl<C> HealthScanner<C>
where
    C: Send + Sync + 'static,
{
    pub fn new(
        pool: DurablePool,
        workflows: Arc<WorkflowRegistry<C>>,
        activities: Arc<ActivityRegistry<C>>,
        topics: Arc<TopicRegistry>,
        config: HealthScannerConfig,
    ) -> Result<Self, DurableError> {
        config.validate()?;
        Ok(Self {
            pool,
            workflows,
            activities,
            topics,
            config,
        })
    }

    pub async fn scan_once(&self, captured_at: i64) -> Result<HealthScanReport, DurableError> {
        let mut connection = self.pool.get().await?;
        let readiness = ReadinessReport::query(
            &mut connection,
            &self.workflows,
            &self.activities,
            &self.topics,
        )
        .await?;
        let mut alerts = readiness_alerts(&readiness, self.config.max_alerts_per_kind as usize);
        let limit = i64::from(self.config.max_alerts_per_kind);
        let stale_millis = i64::try_from(self.config.stale_after.as_millis()).map_err(|_| {
            DurableError::InvalidDefinition(
                "health scanner stale duration exceeds the database range".to_string(),
            )
        })?;
        let stale_before = captured_at.saturating_sub(stale_millis);

        let exhausted = durable_workflow::table
            .filter(durable_workflow::status.eq(WorkflowStatus::Failed))
            .filter(durable_workflow::error_category.eq("activation"))
            .order(durable_workflow::id.asc())
            .limit(limit)
            .select((
                durable_workflow::id,
                durable_workflow::kind,
                durable_workflow::version,
            ))
            .load::<(i64, String, i32)>(&mut connection)
            .await?;
        for (id, kind, version) in exhausted {
            alerts.push(HealthAlert::ActivationExhausted {
                workflow_id: WorkflowId::new(id)?,
                kind,
                version,
            });
        }

        let dead_letters = durable_activity::table
            .filter(durable_activity::status.eq(ActivityStatus::DeadLettered))
            .order(durable_activity::id.asc())
            .select((
                durable_activity::id,
                durable_activity::root_activity_id,
                durable_activity::workflow_id,
                durable_activity::kind,
                durable_activity::version,
                durable_activity::topic,
            ))
            .load::<(i64, Option<i64>, i64, String, i32, String)>(&mut connection)
            .await?;

        // A dead-lettered activity may already have a resolved successor via the
        // replacement chain (see `replaces_activity_id`); every activity in a chain
        // shares the same `root_activity_id` (or is the root itself). The activity's
        // own `status` column never changes once dead-lettered, so we must check the
        // chain here rather than trust `status` alone, or every scan re-alerts on
        // issues that were already fixed by a successful retry.
        let candidate_roots: Vec<i64> = dead_letters
            .iter()
            .map(|(id, root_activity_id, ..)| root_activity_id.unwrap_or(*id))
            .collect();
        let resolved_roots: std::collections::HashSet<i64> = if candidate_roots.is_empty() {
            std::collections::HashSet::new()
        } else {
            durable_activity::table
                .filter(durable_activity::status.eq(ActivityStatus::Succeeded))
                .filter(
                    durable_activity::root_activity_id
                        .eq_any(&candidate_roots)
                        .or(durable_activity::id.eq_any(&candidate_roots)),
                )
                .select((durable_activity::id, durable_activity::root_activity_id))
                .load::<(i64, Option<i64>)>(&mut connection)
                .await?
                .into_iter()
                .map(|(id, root_activity_id)| root_activity_id.unwrap_or(id))
                .collect()
        };

        let mut unresolved = 0usize;
        for (id, root_activity_id, workflow_id, kind, version, topic) in dead_letters {
            let effective_root = root_activity_id.unwrap_or(id);
            if resolved_roots.contains(&effective_root) {
                continue;
            }
            if unresolved >= limit as usize {
                break;
            }
            unresolved += 1;
            alerts.push(HealthAlert::ActivityDeadLettered {
                activity_id: ActivityId::new(id)?,
                workflow_id: WorkflowId::new(workflow_id)?,
                kind,
                version,
                topic,
            });
        }

        let stale_workflows = durable_workflow::table
            .filter(durable_workflow::status.eq(WorkflowStatus::Running))
            .filter(
                durable_workflow::lease_expires_at
                    .le(stale_before)
                    .or(durable_workflow::lease_expires_at.is_null()),
            )
            .order(durable_workflow::id.asc())
            .limit(limit)
            .select((
                durable_workflow::id,
                durable_workflow::kind,
                durable_workflow::version,
            ))
            .load::<(i64, String, i32)>(&mut connection)
            .await?;
        for (id, kind, version) in stale_workflows {
            alerts.push(HealthAlert::StaleWorkflow {
                workflow_id: WorkflowId::new(id)?,
                kind,
                version,
            });
        }

        let stale_activities = durable_activity::table
            .filter(durable_activity::status.eq_any(ActivityStatus::LEASE_HOLDERS))
            .filter(
                durable_activity::lease_expires_at
                    .le(stale_before)
                    .or(durable_activity::lease_expires_at.is_null()),
            )
            .order(durable_activity::id.asc())
            .limit(limit)
            .select((
                durable_activity::id,
                durable_activity::workflow_id,
                durable_activity::kind,
                durable_activity::version,
                durable_activity::topic,
            ))
            .load::<(i64, i64, String, i32, String)>(&mut connection)
            .await?;
        for (id, workflow_id, kind, version, topic) in stale_activities {
            alerts.push(HealthAlert::StaleActivity {
                activity_id: ActivityId::new(id)?,
                workflow_id: WorkflowId::new(workflow_id)?,
                kind,
                version,
                topic,
            });
        }
        Ok(HealthScanReport {
            captured_at,
            alerts,
        })
    }
}

fn readiness_alerts(readiness: &ReadinessReport, limit: usize) -> Vec<HealthAlert> {
    let mut alerts = Vec::new();
    alerts.extend(
        readiness
            .missing_workflows()
            .iter()
            .take(limit)
            .map(|(kind, version)| HealthAlert::MissingWorkflowDefinition {
                kind: kind.clone(),
                version: *version,
            }),
    );
    alerts.extend(
        readiness
            .missing_activities()
            .iter()
            .take(limit)
            .map(|(kind, version)| HealthAlert::MissingActivityDefinition {
                kind: kind.clone(),
                version: *version,
            }),
    );
    alerts.extend(readiness.missing_topics().iter().take(limit).map(|topic| {
        HealthAlert::MissingTopic {
            topic: topic.clone(),
        }
    }));
    alerts
}

pub(crate) fn emit_readiness_alerts(readiness: &ReadinessReport, captured_at: i64) {
    HealthScanReport {
        captured_at,
        alerts: readiness_alerts(readiness, MAX_HEALTH_ALERTS_PER_KIND as usize),
    }
    .emit();
}

pub fn emit_schedule_materialization_alert(
    definition: &ScheduleDefinitionMetadata,
    error: &DurableError,
    captured_at: i64,
) {
    let alert_kind = match error {
        DurableError::Conflict(_) => "schedule_definition_drift",
        DurableError::InvalidState(message) if message.contains("occurrence scan bound") => {
            "schedule_backlog_bound_exhausted"
        }
        _ => "schedule_materialization_failed",
    };
    tracing::error!(
        alert_kind,
        schedule_key = %definition.key,
        definition_version = definition.version,
        captured_at,
        "durable workflow schedule alert"
    );
}

pub fn lease_fingerprint(token: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in token.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(300);

    fn alert(errors: u32) -> Option<HealthAlert> {
        Some(HealthAlert::TransientActivationErrors {
            errors,
            max_errors: 2,
            window: WINDOW,
        })
    }

    #[test]
    fn transient_window_rejects_zero_limit_or_window() {
        assert!(matches!(
            TransientErrorWindow::new(0, WINDOW),
            Err(DurableError::InvalidDefinition(_))
        ));
        assert!(matches!(
            TransientErrorWindow::new(2, Duration::ZERO),
            Err(DurableError::InvalidDefinition(_))
        ));
        assert!(TransientErrorWindow::new(1, Duration::from_millis(1)).is_ok());
        assert!(matches!(
            ActivationCounters::new(0, WINDOW),
            Err(DurableError::InvalidDefinition(_))
        ));
    }

    #[test]
    fn transient_window_alerts_only_above_the_limit_within_one_window() {
        let start = tokio::time::Instant::now();
        let mut window = TransientErrorWindow::new(2, WINDOW).unwrap();
        window.record(start);
        window.record(start + Duration::from_secs(100));
        assert_eq!(
            window.take_alert(),
            None,
            "two errors do not exceed a limit of 2"
        );

        window.record(start + WINDOW);
        assert_eq!(window.take_alert(), alert(3));
        assert_eq!(window.take_alert(), None, "taking the alert clears it");
    }

    #[test]
    fn transient_window_starts_a_new_count_after_the_window() {
        let start = tokio::time::Instant::now();
        let mut window = TransientErrorWindow::new(2, WINDOW).unwrap();
        window.record(start);
        window.record(start + Duration::from_secs(200));
        let later = start + WINDOW + Duration::from_secs(1);
        window.record(later);
        window.record(later + Duration::from_secs(1));
        assert_eq!(
            window.take_alert(),
            None,
            "errors in two windows are not added together"
        );
        window.record(later + Duration::from_secs(2));
        assert_eq!(window.take_alert(), alert(3));
    }

    #[test]
    fn transient_window_latches_the_peak_until_taken() {
        let start = tokio::time::Instant::now();
        let mut window = TransientErrorWindow::new(2, WINDOW).unwrap();
        for second in 0..5 {
            window.record(start + Duration::from_secs(second));
        }
        let next = start + WINDOW + Duration::from_secs(10);
        for second in 0..3 {
            window.record(next + Duration::from_secs(second));
        }
        assert_eq!(
            window.take_alert(),
            alert(5),
            "a burst that ended before the scan is still reported at its peak"
        );
    }

    #[test]
    fn activation_counters_count_each_kind_and_feed_only_transient_errors_to_the_window() {
        let counters = ActivationCounters::new(1, WINDOW).unwrap();
        let now = tokio::time::Instant::now();
        for _ in 0..3 {
            counters.record(BenignActivationKind::FenceMiss, now);
        }
        assert_eq!(counters.get(BenignActivationKind::FenceMiss), 3);
        assert_eq!(counters.get(BenignActivationKind::Transient), 0);
        assert_eq!(counters.take_transient_alert(), None);

        counters.record(BenignActivationKind::Transient, now);
        counters.record(BenignActivationKind::Transient, now);
        assert_eq!(counters.get(BenignActivationKind::Transient), 2);
        assert_eq!(counters.get(BenignActivationKind::FenceMiss), 3);
        assert_eq!(
            counters.take_transient_alert(),
            Some(HealthAlert::TransientActivationErrors {
                errors: 2,
                max_errors: 1,
                window: WINDOW,
            })
        );
    }

    #[test]
    fn default_counters_match_the_runtime_defaults() {
        let config = crate::RuntimeConfig::default();
        assert_eq!(
            config.max_transient_activation_errors,
            DEFAULT_MAX_TRANSIENT_ACTIVATION_ERRORS
        );
        assert_eq!(
            config.transient_activation_error_window,
            DEFAULT_TRANSIENT_ACTIVATION_WINDOW
        );
        assert_eq!(BenignActivationKind::FenceMiss.as_str(), "fence_miss");
        assert_eq!(BenignActivationKind::Transient.as_str(), "transient");
    }
}
