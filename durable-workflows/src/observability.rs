use std::{sync::Arc, time::Duration};

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
            }
        }
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
            .filter(durable_activity::status.eq(ActivityStatus::Running))
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
