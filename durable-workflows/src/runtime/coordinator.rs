use std::{sync::Arc, time::Duration};

use diesel::{
    BoolExpressionMethods, ExpressionMethods, NullableExpressionMethods, OptionalExtension,
    QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use tracing::Instrument;

use crate::{
    deterministic_jitter_percentile,
    observability::lease_fingerprint,
    persistence::{self, NewWorkflowEventRow, WorkflowEventRow, WorkflowRow, WorkflowStatus},
    schema::durable_workflow,
    ActivityRegistry, BackoffPolicy, DurableError, DurablePool, RetryPolicy, StoredTransition,
    WorkflowEvent, WorkflowId, WorkflowRegistry,
};

macro_rules! fenced_workflow {
    ($claim:expr) => {
        durable_workflow::table
            .find($claim.row.id)
            .filter(durable_workflow::status.eq(WorkflowStatus::Running))
            .filter(durable_workflow::lease_token.eq(&$claim.lease_token))
    };
}

#[derive(Debug, Clone, Copy)]
pub struct CoordinatorConfig {
    pub lease_duration: Duration,
    pub max_activation_attempts: u32,
    pub activation_retry_policy: RetryPolicy,
    pub max_consecutive_continuations: u32,
    pub continuation_delay: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(30),
            max_activation_attempts: 8,
            activation_retry_policy: RetryPolicy::from_validated(BackoffPolicy::Exponential {
                initial_secs: 1,
                max_secs: 60,
                jitter_percent: 20,
            }),
            max_consecutive_continuations: 16,
            continuation_delay: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowClaim {
    row: WorkflowRow,
    lease_token: String,
}

impl WorkflowClaim {
    pub fn workflow_id(&self) -> Result<WorkflowId, DurableError> {
        WorkflowId::new(self.row.id)
    }

    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }
}

pub struct WorkflowCoordinator<C> {
    pool: DurablePool,
    context: Arc<C>,
    registry: Arc<WorkflowRegistry<C>>,
    activities: Arc<ActivityRegistry<C>>,
    worker_id: String,
    config: CoordinatorConfig,
}

impl<C> WorkflowCoordinator<C>
where
    C: Send + Sync + 'static,
{
    pub fn new(
        pool: DurablePool,
        context: Arc<C>,
        registry: Arc<WorkflowRegistry<C>>,
        activities: Arc<ActivityRegistry<C>>,
        worker_id: impl Into<String>,
        config: CoordinatorConfig,
    ) -> Result<Self, DurableError> {
        let worker_id = worker_id.into();
        if worker_id.is_empty()
            || config.lease_duration.is_zero()
            || config.max_activation_attempts == 0
            || config.max_consecutive_continuations == 0
            || config.continuation_delay.is_zero()
        {
            return Err(DurableError::InvalidDefinition(
                "coordinator identity and bounds must be non-zero".to_string(),
            ));
        }
        Ok(Self {
            pool,
            context,
            registry,
            activities,
            worker_id,
            config,
        })
    }

    pub async fn activate_one(&self) -> Result<Option<WorkflowId>, DurableError> {
        let Some(claim) = self.claim_one().await? else {
            return Ok(None);
        };
        let workflow_id = claim.workflow_id()?;
        self.activate_claim(claim).await?;
        Ok(Some(workflow_id))
    }

    pub async fn activate_claim(&self, claim: WorkflowClaim) -> Result<WorkflowId, DurableError> {
        let workflow_id = claim.workflow_id()?;
        let lease_fingerprint = lease_fingerprint(&claim.lease_token);
        let span = tracing::info_span!(
            "durable.workflow.activation",
            otel.kind = "consumer",
            workflow_id = workflow_id.get(),
            schedule_run_id = ?claim.row.schedule_run_id,
            kind = %claim.row.kind,
            version = claim.row.version,
            lease_fingerprint = %lease_fingerprint,
            coordinator_id = %self.worker_id,
        );
        self.activate_claim_inner(claim).instrument(span).await
    }

    async fn activate_claim_inner(&self, claim: WorkflowClaim) -> Result<WorkflowId, DurableError> {
        let workflow_id = claim.workflow_id()?;
        let mut connection = self.pool.get().await?;
        let event = persistence::next_delivery_event(
            &mut connection,
            workflow_id,
            claim.row.delivered_event_sequence,
        )
        .await?;
        drop(connection);
        let Some(event) = event else {
            crate::trace::record_local(
                &self.pool,
                &self.worker_id,
                crate::trace::Action::new(
                    "LC1_NoEvent",
                    serde_json::json!({
                        "workflow_id": claim.row.id,
                        "token": claim.lease_token,
                    }),
                ),
            )
            .await;
            return Err(DurableError::InvalidState(format!(
                "claimed workflow {workflow_id} has no deliverable event"
            )));
        };

        match self
            .registry
            .step_stored(
                &claim.row.kind,
                claim.row.version,
                self.context.as_ref(),
                Some(workflow_id),
                &claim.row.input_json,
                &claim.row.state_json,
                decode_event(&event)?,
            )
            .await
        {
            Ok(transition) => {
                if let StoredTransition::RunChild { child, .. } = &transition {
                    if !self.registry.contains(child.kind(), child.version()) {
                        let message = format!(
                            "child workflow {} v{} is not registered",
                            child.kind(),
                            child.version()
                        );
                        self.record_activation_failure(&claim, &message).await?;
                        return Ok(workflow_id);
                    }
                }
                if let StoredTransition::RunActivity { activity, .. } = &transition {
                    if !self
                        .activities
                        .contains(activity.kind(), activity.version())
                    {
                        let message = format!(
                            "activity {} v{} is not registered",
                            activity.kind(),
                            activity.version()
                        );
                        self.record_activation_failure(&claim, &message).await?;
                        return Ok(workflow_id);
                    }
                    let registered_topic = self
                        .activities
                        .topic_for(activity.kind(), activity.version());
                    if registered_topic != Some(activity.topic()) {
                        let message = format!(
                            "activity {} v{} topic mismatch: command has {:?}, registry has {:?}",
                            activity.kind(),
                            activity.version(),
                            activity.topic(),
                            registered_topic
                        );
                        self.record_activation_failure(&claim, &message).await?;
                        return Ok(workflow_id);
                    }
                }
                match self
                    .commit_transition(claim.clone(), event, transition)
                    .await
                {
                    Ok(()) => {}
                    // Closed definition / command-validation errors are bounded
                    // activation failures — never escalate to supervisor restarts.
                    Err(error @ DurableError::DefinitionMismatch { .. })
                    | Err(error @ DurableError::InvalidDefinition(_)) => {
                        self.record_activation_failure(&claim, &error.to_string())
                            .await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => {
                self.record_activation_failure(&claim, &error.to_string())
                    .await?
            }
        }
        Ok(workflow_id)
    }

    pub async fn claim_one(&self) -> Result<Option<WorkflowClaim>, DurableError> {
        let lease_duration_millis = duration_millis(self.config.lease_duration)?;
        let worker_id = self.worker_id.clone();
        let local_definitions = self.registry.definition_keys();
        let configured_max_activation =
            i32::try_from(self.config.max_activation_attempts).unwrap_or(i32::MAX);
        let actor = self.worker_id.as_str();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::actor(actor);
            let now = persistence::database_now_millis(connection).await?;
            let lease_expires_at = now.checked_add(lease_duration_millis).ok_or_else(|| {
                DurableError::InvalidDefinition("workflow lease timestamp overflow".to_string())
            })?;
            // The scan stays unlocked with a per-row relock. Under REPEATABLE READ an empty
            // locking range scan would retain gap locks, and concurrent ready-to-running
            // updates would deadlock inserting into that range; READ COMMITTED takes none.
            let mut expired_cursor = None;
            let expired = 'expired: loop {
                let mut candidates = durable_workflow::table
                    .filter(durable_workflow::status.eq(WorkflowStatus::Running))
                    .filter(durable_workflow::lease_expires_at.le(now))
                    .into_boxed::<crate::Db>();
                if let Some((expiry, id)) = expired_cursor {
                    candidates = candidates.filter(
                        durable_workflow::lease_expires_at.gt(expiry).or(
                            durable_workflow::lease_expires_at
                                .eq(expiry)
                                .and(durable_workflow::id.gt(id)),
                        ),
                    );
                }
                let page = candidates
                    .order((
                        durable_workflow::lease_expires_at.asc(),
                        durable_workflow::id.asc(),
                    ))
                    .limit(32)
                    .select((
                        durable_workflow::lease_expires_at.assume_not_null(),
                        durable_workflow::id,
                    ))
                    .load::<(i64, i64)>(connection)
                    .await?;
                let Some(last_candidate) = page.last().copied() else {
                    break None;
                };
                let page_len = page.len();
                for (_, expired_id) in page {
                    let candidate = durable_workflow::table
                        .find(expired_id)
                        .for_update()
                        .skip_locked()
                        .select(WorkflowRow::as_select())
                        .first::<WorkflowRow>(connection)
                        .await
                        .optional()?;
                    if let Some(candidate) = candidate {
                        if candidate.status == WorkflowStatus::Running
                            && candidate
                                .lease_expires_at
                                .is_some_and(|expiry| expiry <= now)
                        {
                            break 'expired Some(candidate);
                        }
                    }
                }
                if page_len < 32 {
                    break None;
                }
                expired_cursor = Some(last_candidate);
            };
            let recovered = expired.as_ref().map(|expired| expired.id);
            if let Some(expired) = expired {
                let changed = diesel::update(
                    durable_workflow::table
                        .find(expired.id)
                        .filter(durable_workflow::status.eq(WorkflowStatus::Running))
                        .filter(durable_workflow::lease_token.eq(expired.lease_token.clone())),
                )
                .set((
                    durable_workflow::status.eq(WorkflowStatus::Ready),
                    durable_workflow::available_at.eq(now),
                    durable_workflow::lease_owner.eq(None::<String>),
                    durable_workflow::lease_token.eq(None::<String>),
                    durable_workflow::lease_expires_at.eq(None::<i64>),
                    durable_workflow::updated_at.eq(now),
                ))
                .execute(connection)
                .await?;
                ensure_fenced(changed)?;
                append_history(
                    connection,
                    WorkflowId::new(expired.id)?,
                    "lease_recovered",
                    now,
                )
                .await?;
                crate::trace::touch_wf(expired.id);
            }

            let mut ready = durable_workflow::table.into_boxed::<crate::Db>();
            let mut definitions = local_definitions.into_iter();
            let Some((kind, version)) = definitions.next() else {
                declare_workflow_claim(recovered, None, 0);
                return Ok(None);
            };
            ready = ready.filter(
                durable_workflow::kind
                    .eq(kind)
                    .and(durable_workflow::version.eq(version)),
            );
            for (kind, version) in definitions {
                ready = ready.or_filter(
                    durable_workflow::kind
                        .eq(kind)
                        .and(durable_workflow::version.eq(version)),
                );
            }
            let candidate_ids = ready
                .filter(durable_workflow::status.eq(WorkflowStatus::Ready))
                .filter(durable_workflow::available_at.le(now))
                .order((
                    durable_workflow::available_at.asc(),
                    durable_workflow::id.asc(),
                ))
                .limit(32)
                .select(durable_workflow::id)
                .load::<i64>(connection)
                .await?;
            let mut claimed_row = None;
            for candidate_id in candidate_ids {
                claimed_row = durable_workflow::table
                    .find(candidate_id)
                    .filter(durable_workflow::status.eq(WorkflowStatus::Ready))
                    .filter(durable_workflow::available_at.le(now))
                    .for_update()
                    .skip_locked()
                    .select(WorkflowRow::as_select())
                    .first::<WorkflowRow>(connection)
                    .await
                    .optional()?;
                if claimed_row.is_some() {
                    break;
                }
            }
            let Some(mut row) = claimed_row else {
                declare_workflow_claim(recovered, None, 0);
                return Ok(None);
            };

            let lease_token = uuid::Uuid::new_v4().to_string();
            let changed = diesel::update(
                durable_workflow::table
                    .find(row.id)
                    .filter(durable_workflow::status.eq(WorkflowStatus::Ready)),
            )
            .set((
                durable_workflow::status.eq(WorkflowStatus::Running),
                durable_workflow::lease_owner.eq(Some(worker_id)),
                durable_workflow::lease_token.eq(Some(lease_token.clone())),
                durable_workflow::lease_expires_at.eq(Some(lease_expires_at)),
                durable_workflow::updated_at.eq(now),
            ))
            .execute(connection)
            .await?;
            ensure_fenced(changed)?;
            crate::trace::touch_wf(row.id);
            declare_workflow_claim(
                recovered,
                Some((row.id, &lease_token, lease_expires_at)),
                row.max_activation_attempts.min(configured_max_activation),
            );
            row.status = WorkflowStatus::Running;
            row.lease_token = Some(lease_token.clone());
            row.lease_expires_at = Some(lease_expires_at);
            Ok(Some(WorkflowClaim { row, lease_token }))
        })
        .await
    }

    async fn commit_transition(
        &self,
        claim: WorkflowClaim,
        event: WorkflowEventRow,
        transition: StoredTransition,
    ) -> Result<(), DurableError> {
        let mut connection = self.pool.get().await?;
        let config = self.config;
        let actor = self.worker_id.as_str();
        let fence = (claim.row.id, claim.lease_token.clone());
        let result = crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::actor(actor);
            commit_on_connection(connection, &claim, &event, transition, config).await
        })
        .await;
        drop(connection);
        self.record_fence_miss(fence, &result).await;
        result
    }

    /// `CoordFenceMiss`: T-C2 or T-C3 lost its fence and rolled back.
    async fn record_fence_miss(
        &self,
        (workflow_id, token): (i64, String),
        result: &Result<(), DurableError>,
    ) {
        if crate::trace::ENABLED && matches!(result, Err(DurableError::FencedWrite)) {
            crate::trace::record_local(
                &self.pool,
                &self.worker_id,
                crate::trace::Action::new(
                    "CoordFenceMiss",
                    serde_json::json!({ "workflow_id": workflow_id, "token": token }),
                ),
            )
            .await;
        }
    }

    async fn record_activation_failure(
        &self,
        claim: &WorkflowClaim,
        message: &str,
    ) -> Result<(), DurableError> {
        let mut connection = self.pool.get().await?;
        let claim = claim.clone();
        let fence = (claim.row.id, claim.lease_token.clone());
        let message = message.to_string();
        let config = self.config;
        let actor = self.worker_id.as_str();
        let result = crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::actor(actor);
            let current = durable_workflow::table
                .find(claim.row.id)
                .filter(durable_workflow::status.eq(WorkflowStatus::Running))
                .filter(durable_workflow::lease_token.eq(&claim.lease_token))
                .for_update()
                .select(WorkflowRow::as_select())
                .first::<WorkflowRow>(connection)
                .await
                .optional()?
                .ok_or(DurableError::FencedWrite)?;
            let attempt = current.activation_attempts.saturating_add(1);
            let configured_max = i32::try_from(config.max_activation_attempts).unwrap_or(i32::MAX);
            let max_activation = current.max_activation_attempts.min(configured_max);
            let exhausted = attempt >= max_activation;
            let now = persistence::database_now_millis(connection).await?;
            let attempt_number = u32::try_from(attempt).unwrap_or(u32::MAX);
            let jitter_percentile = deterministic_jitter_percentile(format!(
                "workflow:{}:activation:{}",
                current.id, attempt_number
            ));
            let delay = config
                .activation_retry_policy
                .delay_for_attempt(attempt_number, jitter_percentile)?;
            let available_at = now.saturating_add(duration_millis(delay)?);
            let error = crate::WorkflowError::new("activation", message);
            let changed = diesel::update(
                durable_workflow::table
                    .find(current.id)
                    .filter(durable_workflow::status.eq(WorkflowStatus::Running))
                    .filter(durable_workflow::lease_token.eq(&claim.lease_token)),
            )
            .set((
                durable_workflow::status.eq(if exhausted {
                    WorkflowStatus::Failed
                } else {
                    WorkflowStatus::Ready
                }),
                durable_workflow::activation_attempts.eq(attempt),
                durable_workflow::available_at.eq(available_at),
                durable_workflow::error_category.eq(Some(error.category.clone())),
                durable_workflow::error_message.eq(Some(error.message.clone())),
                durable_workflow::lease_owner.eq(None::<String>),
                durable_workflow::lease_token.eq(None::<String>),
                durable_workflow::lease_expires_at.eq(None::<i64>),
                durable_workflow::updated_at.eq(now),
                durable_workflow::completed_at.eq(exhausted.then_some(now)),
            ))
            .execute(connection)
            .await?;
            ensure_fenced(changed)?;
            crate::trace::touch_wf(current.id);
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "TC3_ActivationFailure",
                    serde_json::json!({
                        "workflow_id": current.id,
                        "token": claim.lease_token,
                        "attempt": attempt,
                        "max_activation": max_activation,
                        "exhausted": exhausted,
                        "available_at": available_at,
                    }),
                )
            });
            append_history(
                connection,
                WorkflowId::new(current.id)?,
                if exhausted {
                    "activation_exhausted"
                } else {
                    "activation_failed"
                },
                now,
            )
            .await?;
            if exhausted {
                persistence::wake_waiting_parents_on_child_terminal(
                    connection,
                    current.id,
                    &current.kind,
                    current.version,
                    Err((error.category, error.message)),
                    now,
                )
                .await?;
            }
            Ok(())
        })
        .await;
        drop(connection);
        self.record_fence_miss(fence, &result).await;
        result
    }
}

async fn commit_on_connection(
    connection: &mut crate::DurableConnection,
    claim: &WorkflowClaim,
    event: &WorkflowEventRow,
    transition: StoredTransition,
    config: CoordinatorConfig,
) -> Result<(), DurableError> {
    let workflow_id = claim.workflow_id()?;
    let delivered = event.delivery_sequence.ok_or_else(|| {
        DurableError::InvalidState("workflow received non-deliverable history".to_string())
    })?;
    let now = persistence::database_now_millis(connection).await?;
    crate::trace::touch_wf(claim.row.id);
    match transition {
        StoredTransition::Continue { state_json } => {
            let streak = claim.row.consecutive_continuations.saturating_add(1);
            let fairness =
                streak >= i32::try_from(config.max_consecutive_continuations).unwrap_or(i32::MAX);
            let available_at = if fairness {
                now.saturating_add(duration_millis(config.continuation_delay)?)
            } else {
                now
            };
            let changed = diesel::update(fenced_workflow!(claim))
                .set((
                    durable_workflow::state_json.eq(state_json),
                    durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
                    durable_workflow::status.eq(WorkflowStatus::Ready),
                    durable_workflow::available_at.eq(available_at),
                    durable_workflow::delivered_event_sequence.eq(delivered),
                    durable_workflow::consecutive_continuations.eq(if fairness {
                        0
                    } else {
                        streak
                    }),
                    durable_workflow::activation_attempts.eq(0),
                    durable_workflow::error_category.eq(None::<String>),
                    durable_workflow::error_message.eq(None::<String>),
                    durable_workflow::lease_owner.eq(None::<String>),
                    durable_workflow::lease_token.eq(None::<String>),
                    durable_workflow::lease_expires_at.eq(None::<i64>),
                    durable_workflow::updated_at.eq(now),
                ))
                .execute(connection)
                .await?;
            ensure_fenced(changed)?;
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "TC2_Commit",
                    serde_json::json!({
                        "workflow_id": claim.row.id,
                        "token": claim.lease_token,
                        "transition": "continue",
                        "consumed": delivered,
                        "available_at": available_at,
                    }),
                )
            });
            let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
            persistence::append_event(
                connection,
                NewWorkflowEventRow {
                    workflow_id: claim.row.id,
                    sequence,
                    delivery_sequence: Some(delivered.saturating_add(1)),
                    event_type: "continued".to_string(),
                    metadata_json: None,
                    actor_type: Some("system".to_string()),
                    actor_id: None,
                    reason: None,
                    created_at: now,
                },
            )
            .await
        }
        StoredTransition::Complete { output_json } => {
            let changed = diesel::update(fenced_workflow!(claim))
                .set((
                    durable_workflow::status.eq(WorkflowStatus::Succeeded),
                    durable_workflow::result_json.eq(Some(output_json.clone())),
                    durable_workflow::delivered_event_sequence.eq(delivered),
                    durable_workflow::consecutive_continuations.eq(0),
                    durable_workflow::activation_attempts.eq(0),
                    durable_workflow::error_category.eq(None::<String>),
                    durable_workflow::error_message.eq(None::<String>),
                    durable_workflow::lease_owner.eq(None::<String>),
                    durable_workflow::lease_token.eq(None::<String>),
                    durable_workflow::lease_expires_at.eq(None::<i64>),
                    durable_workflow::updated_at.eq(now),
                    durable_workflow::completed_at.eq(Some(now)),
                ))
                .execute(connection)
                .await?;
            ensure_fenced(changed)?;
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "TC2_Commit",
                    serde_json::json!({
                        "workflow_id": claim.row.id,
                        "token": claim.lease_token,
                        "transition": "complete",
                        "consumed": delivered,
                    }),
                )
            });
            append_history(connection, workflow_id, "workflow_succeeded", now).await?;
            persistence::wake_waiting_parents_on_child_terminal(
                connection,
                claim.row.id,
                &claim.row.kind,
                claim.row.version,
                Ok(output_json),
                now,
            )
            .await?;
            Ok(())
        }
        transition => commit_wait_transition(connection, claim, delivered, transition, now).await,
    }
}

async fn commit_wait_transition(
    connection: &mut crate::DurableConnection,
    claim: &WorkflowClaim,
    delivered: i32,
    transition: StoredTransition,
    now: i64,
) -> Result<(), DurableError> {
    use crate::persistence::NewApprovalRow;

    let workflow_id = claim.workflow_id()?;
    let command = claim.row.command_sequence.saturating_add(1);
    match transition {
        StoredTransition::SleepUntil {
            state_json,
            wake_at_millis,
        } => {
            let changed = diesel::update(fenced_workflow!(claim))
                .set((
                    durable_workflow::state_json.eq(state_json),
                    durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
                    durable_workflow::status.eq(WorkflowStatus::Sleeping),
                    durable_workflow::wait_kind.eq(Some("timer".to_string())),
                    durable_workflow::wait_reference_id.eq(Some(i64::from(command))),
                    durable_workflow::available_at.eq(wake_at_millis),
                    durable_workflow::command_sequence.eq(command),
                    durable_workflow::delivered_event_sequence.eq(delivered),
                    durable_workflow::consecutive_continuations.eq(0),
                    durable_workflow::activation_attempts.eq(0),
                    durable_workflow::error_category.eq(None::<String>),
                    durable_workflow::error_message.eq(None::<String>),
                    durable_workflow::lease_owner.eq(None::<String>),
                    durable_workflow::lease_token.eq(None::<String>),
                    durable_workflow::lease_expires_at.eq(None::<i64>),
                    durable_workflow::updated_at.eq(now),
                ))
                .execute(connection)
                .await?;
            ensure_fenced(changed)?;
            declare_unmodeled("timer");
            append_history(connection, workflow_id, "timer_scheduled", now).await
        }
        StoredTransition::WaitForApproval {
            state_json,
            approval_json,
            expires_at_millis,
        } => {
            let approval_id = crate::dialect::insert_approval(
                connection,
                NewApprovalRow {
                    workflow_id: claim.row.id,
                    command_sequence: command,
                    kind: claim.row.kind.clone(),
                    version: claim.row.version,
                    prompt_metadata_json: approval_json,
                    validation_schema_json: "{}".to_string(),
                    validation_version: 1,
                    status: "pending".to_string(),
                    requested_at: now,
                    expires_at: expires_at_millis,
                    decision_payload_json: None,
                    decided_by: None,
                    operator_reason: None,
                    resolved_at: None,
                },
            )
            .await?;
            let changed = diesel::update(fenced_workflow!(claim))
                .set((
                    durable_workflow::state_json.eq(state_json),
                    durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
                    durable_workflow::status.eq(WorkflowStatus::WaitingApproval),
                    durable_workflow::wait_kind.eq(Some("approval".to_string())),
                    durable_workflow::wait_reference_id.eq(Some(approval_id)),
                    durable_workflow::command_sequence.eq(command),
                    durable_workflow::delivered_event_sequence.eq(delivered),
                    durable_workflow::consecutive_continuations.eq(0),
                    durable_workflow::activation_attempts.eq(0),
                    durable_workflow::error_category.eq(None::<String>),
                    durable_workflow::error_message.eq(None::<String>),
                    durable_workflow::lease_owner.eq(None::<String>),
                    durable_workflow::lease_token.eq(None::<String>),
                    durable_workflow::lease_expires_at.eq(None::<i64>),
                    durable_workflow::updated_at.eq(now),
                ))
                .execute(connection)
                .await?;
            ensure_fenced(changed)?;
            declare_unmodeled("approval");
            append_history(connection, workflow_id, "approval_requested", now).await
        }
        StoredTransition::RunActivity {
            state_json,
            activity,
        } => {
            commit_activity(
                connection, claim, delivered, command, state_json, activity, now,
            )
            .await
        }
        StoredTransition::RunChild { state_json, child } => {
            commit_child(
                connection, claim, delivered, command, state_json, child, now,
            )
            .await
        }
        StoredTransition::Continue { .. } | StoredTransition::Complete { .. } => Err(
            DurableError::InvalidState("transition routed to the wrong commit path".to_string()),
        ),
    }
}

async fn commit_activity(
    connection: &mut crate::DurableConnection,
    claim: &WorkflowClaim,
    delivered: i32,
    command: i32,
    state_json: String,
    activity: crate::ActivityCommand,
    now: i64,
) -> Result<(), DurableError> {
    use crate::persistence::{ActivityStatus, NewActivityRow};

    let available_at = if activity.has_continuation_priority() {
        crate::transition::CONTINUATION_READY_AT_MILLIS
    } else {
        now
    };
    let activity_id = crate::dialect::insert_activity(
        connection,
        NewActivityRow {
            workflow_id: claim.row.id,
            command_sequence: command,
            replacement_number: 0,
            kind: activity.kind().to_string(),
            version: activity.version(),
            topic: activity.topic().to_string(),
            payload_json: activity.payload_json().to_string(),
            status: ActivityStatus::Pending,
            available_at,
            max_attempts: i32::try_from(activity.max_attempts()).map_err(|_| {
                DurableError::InvalidDefinition(
                    "activity attempts exceed the database integer range".to_string(),
                )
            })?,
            attempt_count: 0,
            timeout_millis: duration_millis(activity.timeout())?,
            lease_duration_millis: duration_millis(activity.lease_duration())?,
            retry_policy_json: serde_json::to_string(&activity.retry_policy())?,
            operation_key: activity.operation_key().map(str::to_owned),
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
        },
    )
    .await?;
    let changed = diesel::update(fenced_workflow!(claim))
        .set((
            durable_workflow::state_json.eq(state_json),
            durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
            durable_workflow::status.eq(WorkflowStatus::WaitingActivity),
            durable_workflow::wait_kind.eq(Some("activity".to_string())),
            durable_workflow::wait_reference_id.eq(Some(activity_id)),
            durable_workflow::command_sequence.eq(command),
            durable_workflow::delivered_event_sequence.eq(delivered),
            durable_workflow::consecutive_continuations.eq(0),
            durable_workflow::activation_attempts.eq(0),
            durable_workflow::error_category.eq(None::<String>),
            durable_workflow::error_message.eq(None::<String>),
            durable_workflow::lease_owner.eq(None::<String>),
            durable_workflow::lease_token.eq(None::<String>),
            durable_workflow::lease_expires_at.eq(None::<i64>),
            durable_workflow::updated_at.eq(now),
        ))
        .execute(connection)
        .await?;
    ensure_fenced(changed)?;
    crate::trace::touch_act(activity_id);
    crate::trace::declare(|| {
        crate::trace::Action::new(
            "TC2_Commit",
            serde_json::json!({
                "workflow_id": claim.row.id,
                "token": claim.lease_token,
                "transition": "run_activity",
                "consumed": delivered,
                "activity_id": activity_id,
                "topic": activity.topic(),
                "max_attempts": activity.max_attempts(),
                "available_at": available_at,
                "continuation_priority": activity.has_continuation_priority(),
            }),
        )
    });
    append_history(connection, claim.workflow_id()?, "activity_scheduled", now).await
}

async fn commit_child(
    connection: &mut crate::DurableConnection,
    claim: &WorkflowClaim,
    delivered: i32,
    command: i32,
    state_json: String,
    child: crate::ChildWorkflowCommand,
    now: i64,
) -> Result<(), DurableError> {
    let parent_id = claim.row.id;
    let deduplication_key = child
        .deduplication_key()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("child:{parent_id}:{command}"));
    let root_workflow_id = claim.row.root_workflow_id.unwrap_or(parent_id);
    let trace_key = crate::trace::ENABLED.then(|| deduplication_key.clone());
    let outcome = crate::DurableStore::insert_child(
        connection,
        &child,
        deduplication_key,
        parent_id,
        command,
        root_workflow_id,
    )
    .await?;
    let changed = diesel::update(fenced_workflow!(claim))
        .set((
            durable_workflow::state_json.eq(state_json),
            durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
            durable_workflow::status.eq(WorkflowStatus::WaitingChild),
            durable_workflow::wait_kind.eq(Some("child".to_string())),
            durable_workflow::wait_reference_id.eq(Some(outcome.workflow_id.get())),
            durable_workflow::command_sequence.eq(command),
            durable_workflow::delivered_event_sequence.eq(delivered),
            durable_workflow::consecutive_continuations.eq(0),
            durable_workflow::activation_attempts.eq(0),
            durable_workflow::error_category.eq(None::<String>),
            durable_workflow::error_message.eq(None::<String>),
            durable_workflow::lease_owner.eq(None::<String>),
            durable_workflow::lease_token.eq(None::<String>),
            durable_workflow::lease_expires_at.eq(None::<i64>),
            durable_workflow::updated_at.eq(now),
        ))
        .execute(connection)
        .await?;
    ensure_fenced(changed)?;
    crate::trace::declare(|| {
        crate::trace::Action::new(
            "TC2_Commit",
            serde_json::json!({
                "workflow_id": parent_id,
                "token": claim.lease_token,
                "transition": "run_child",
                "consumed": delivered,
                "child_kind": child.kind(),
                "child_key": trace_key,
                "child_id": outcome.workflow_id.get(),
                "inserted": outcome.inserted,
            }),
        )
    });
    append_history(
        connection,
        claim.workflow_id()?,
        "child_workflow_scheduled",
        now,
    )
    .await?;
    if !outcome.inserted {
        // Lock + current-read the child after establishing the parent wait so a
        // concurrent terminal commit cannot wake with no waiters while an
        // earlier unlocked read still looks non-terminal.
        let existing =
            persistence::find_workflow_by_id_for_update(connection, outcome.workflow_id).await?;
        let terminal_outcome = match existing.status {
            WorkflowStatus::Succeeded => Some(Ok(existing
                .result_json
                .clone()
                .unwrap_or_else(|| "null".to_string()))),
            WorkflowStatus::Failed => Some(Err((
                existing
                    .error_category
                    .clone()
                    .unwrap_or_else(|| "failed".to_string()),
                existing
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "child workflow failed".to_string()),
            ))),
            WorkflowStatus::Cancelled => Some(Err((
                "child_cancelled".to_string(),
                "child workflow was cancelled".to_string(),
            ))),
            _ => None,
        };
        if let Some(terminal_outcome) = terminal_outcome {
            persistence::wake_waiting_parents_on_child_terminal(
                connection,
                existing.id,
                &existing.kind,
                existing.version,
                terminal_outcome,
                now,
            )
            .await?;
        }
    }
    Ok(())
}

async fn append_history(
    connection: &mut crate::DurableConnection,
    workflow_id: WorkflowId,
    event_type: &str,
    created_at: i64,
) -> Result<(), DurableError> {
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: workflow_id.get(),
            sequence,
            delivery_sequence: None,
            event_type: event_type.to_string(),
            metadata_json: None,
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason: None,
            created_at,
        },
    )
    .await
}

fn decode_event(event: &WorkflowEventRow) -> Result<WorkflowEvent, DurableError> {
    match event.event_type.as_str() {
        "started" => Ok(WorkflowEvent::Started),
        "continued" => Ok(WorkflowEvent::Continued),
        _ => {
            let json = event.metadata_json.as_deref().ok_or_else(|| {
                DurableError::InvalidState(format!(
                    "deliverable event {} has no metadata",
                    event.event_type
                ))
            })?;
            Ok(serde_json::from_str(json)?)
        }
    }
}

/// A committed transition the model does not cover (`name`: timer, approval).
fn declare_unmodeled(name: &'static str) {
    crate::trace::declare_unmodeled(name, true);
}

/// Declares `TC1_Claim` when the claim recovered or claimed a workflow.
/// `max_activation` is the activation cap T-C3 applies to the claimed row.
fn declare_workflow_claim(
    recovered: Option<i64>,
    claimed: Option<(i64, &str, i64)>,
    max_activation: i32,
) {
    if recovered.is_none() && claimed.is_none() {
        return;
    }
    crate::trace::declare(|| {
        crate::trace::Action::new(
            "TC1_Claim",
            serde_json::json!({
                "recovered": recovered,
                "claimed": claimed.map(|(id, _, _)| id),
                "token": claimed.map(|(_, token, _)| token),
                "lease_expires_at": claimed.map(|(_, _, lease_expires_at)| lease_expires_at),
                "max_activation": claimed.map(|_| max_activation),
            }),
        )
    });
}

fn ensure_fenced(changed: usize) -> Result<(), DurableError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}

fn duration_millis(duration: Duration) -> Result<i64, DurableError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        DurableError::InvalidDefinition("duration exceeds the database range".to_string())
    })
}
