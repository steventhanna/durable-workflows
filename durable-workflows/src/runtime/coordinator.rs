use std::{
    any::Any,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crate::DbMillis;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, NullableExpressionMethods, OptionalExtension,
    QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use tracing::Instrument;

use crate::{
    deterministic_jitter_percentile,
    observability::{lease_fingerprint, ActivationCounters, BenignActivationKind},
    persistence::{
        self, ApprovalStatus, NewWorkflowEventRow, Wait, WorkflowEventRow, WorkflowRow,
        WorkflowStatus,
    },
    schema::durable_workflow,
    ActivityRegistry, BackoffPolicy, DurableError, DurablePool, RetryPolicy, StoredTransition,
    WorkflowEvent, WorkflowId, WorkflowRegistry,
};
use crate::{
    store::ChildStart,
    tx::{self, ClaimFence, Committed, Declared, Locked, Trace, Tx, TxScope, Undeclared},
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
#[non_exhaustive]
pub struct CoordinatorConfig {
    pub lease_duration: Duration,
    pub max_activation_attempts: u32,
    pub activation_retry_policy: RetryPolicy,
    pub max_consecutive_continuations: u32,
    pub continuation_delay: Duration,
    /// Longest a workflow `step` may run. A step that exceeds it, or panics,
    /// is a bounded activation failure (T-C3). Default 20 s, a third below
    /// the default `lease_duration` (30 s). Keep it below `lease_duration`
    /// with a margin for the claim-to-step delay and the T-C3 commit: the
    /// lease is not renewed during a step, so a step that times out at or
    /// after lease expiry races lease recovery by another runtime, and a
    /// T-C3 that loses that race records no activation attempt (G3, S8).
    pub step_timeout: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(30),
            max_activation_attempts: 8,
            activation_retry_policy: RetryPolicy::from_checked(BackoffPolicy::Exponential {
                initial_secs: 1,
                max_secs: 60,
                jitter_percent: 20,
            }),
            max_consecutive_continuations: 16,
            continuation_delay: Duration::from_millis(100),
            step_timeout: Duration::from_secs(20),
        }
    }
}

/// Builder-style setters. The struct is `#[non_exhaustive]`: start from
/// [`CoordinatorConfig::default`] and override fields with these. Bounds (non-zero
/// durations and counts) are checked where the config is used, not here.
impl CoordinatorConfig {
    #[must_use]
    pub const fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    #[must_use]
    pub const fn with_max_activation_attempts(mut self, max_activation_attempts: u32) -> Self {
        self.max_activation_attempts = max_activation_attempts;
        self
    }

    #[must_use]
    pub const fn with_activation_retry_policy(
        mut self,
        activation_retry_policy: RetryPolicy,
    ) -> Self {
        self.activation_retry_policy = activation_retry_policy;
        self
    }

    #[must_use]
    pub const fn with_max_consecutive_continuations(
        mut self,
        max_consecutive_continuations: u32,
    ) -> Self {
        self.max_consecutive_continuations = max_consecutive_continuations;
        self
    }

    #[must_use]
    pub const fn with_continuation_delay(mut self, continuation_delay: Duration) -> Self {
        self.continuation_delay = continuation_delay;
        self
    }

    #[must_use]
    pub const fn with_step_timeout(mut self, step_timeout: Duration) -> Self {
        self.step_timeout = step_timeout;
        self
    }
}

#[derive(Debug, Clone)]
struct ClaimedRow {
    row: WorkflowRow,
    lease_token: String,
}

impl ClaimedRow {
    fn workflow_id(&self) -> WorkflowId {
        self.row.id
    }
}

/// One workflow leased by a [`WorkflowCoordinator`] (T-C1), not yet
/// activated.
///
/// The claim mutably borrows its coordinator, so a coordinator holds at most
/// one outstanding workflow claim: a second [`WorkflowCoordinator::claim_one`]
/// while this claim is alive does not compile. This is the model's
/// one-claim-per-runtime rule (`TC1_Claim`). Consume the claim with
/// [`Self::activate`]. Dropping it releases nothing: the row stays `running`
/// until its lease expires and lease recovery reclaims it.
#[must_use = "an unused claim holds its lease until it expires"]
pub struct WorkflowClaim<'a, C> {
    coordinator: &'a mut WorkflowCoordinator<C>,
    claimed: ClaimedRow,
}

impl<C> std::fmt::Debug for WorkflowClaim<'_, C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowClaim")
            .field("worker_id", &self.coordinator.worker_id)
            .field("row", &self.claimed.row)
            .field("lease_token", &self.claimed.lease_token)
            .finish()
    }
}

impl<C> WorkflowClaim<'_, C> {
    pub fn workflow_id(&self) -> WorkflowId {
        self.claimed.workflow_id()
    }

    pub fn lease_token(&self) -> &str {
        &self.claimed.lease_token
    }
}

impl<C> WorkflowClaim<'_, C>
where
    C: Send + Sync + 'static,
{
    /// Runs one step for this claim and commits its transition (T-C2), or
    /// records an activation failure (T-C3) when the step fails, panics or
    /// exceeds `step_timeout`. Returns `FencedWrite` when the claim lost its
    /// fence: an operator paused or cancelled the workflow, or its lease
    /// expired and another coordinator recovered it.
    pub async fn activate(self) -> Result<WorkflowId, DurableError> {
        self.coordinator.activate_claimed(self.claimed).await
    }
}

/// Claims and activates workflows (T-C1..T-C3) under one worker id.
///
/// A coordinator holds at most one outstanding claim: [`Self::claim_one`]
/// takes `&mut self` and the returned [`WorkflowClaim`] borrows the
/// coordinator until it is activated or dropped. Map one worker id to one
/// coordinator. Extra coordinators under the same id stay safe, because every
/// write is fenced by the claim's own lease token, but the trace checker maps
/// a worker id to one model runtime, and a model runtime holds one claim.
pub struct WorkflowCoordinator<C> {
    pool: DurablePool,
    context: Arc<C>,
    registry: Arc<WorkflowRegistry<C>>,
    activities: Arc<ActivityRegistry<C>>,
    worker_id: String,
    config: CoordinatorConfig,
    activation_counters: Arc<ActivationCounters>,
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
            || config.step_timeout.is_zero()
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
            activation_counters: Arc::new(ActivationCounters::default()),
        })
    }

    pub(crate) fn with_activation_counters(mut self, counters: Arc<ActivationCounters>) -> Self {
        self.activation_counters = counters;
        self
    }

    /// The counts of benign activations that [`Self::activate_one`] skipped.
    pub fn activation_counters(&self) -> Arc<ActivationCounters> {
        self.activation_counters.clone()
    }

    /// Claims one workflow and activates it. A claim that lost its fence, or
    /// a transient database error, is not an error here (see
    /// [`WorkflowClaim::activate`]): the activation is logged, counted in
    /// [`Self::activation_counters`] and skipped.
    pub async fn activate_one(&mut self) -> Result<Option<WorkflowId>, DurableError> {
        let Some(claimed) = self.claim_row().await? else {
            return Ok(None);
        };
        let workflow_id = claimed.workflow_id();
        if let Err(error) = self.activate_claimed(claimed).await {
            let Some(kind) = benign_activation_kind(&error) else {
                return Err(error);
            };
            self.activation_counters
                .record(kind, tokio::time::Instant::now());
            tracing::warn!(
                workflow_id = workflow_id.get(),
                benign_kind = kind.as_str(),
                error = %error,
                "workflow activation rolled back; the row is left to its new owner or to lease recovery"
            );
        }
        Ok(Some(workflow_id))
    }

    async fn activate_claimed(&self, claim: ClaimedRow) -> Result<WorkflowId, DurableError> {
        let workflow_id = claim.workflow_id();
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

    async fn activate_claim_inner(&self, claim: ClaimedRow) -> Result<WorkflowId, DurableError> {
        let workflow_id = claim.workflow_id();
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

        let event_input = decode_event(&event)?;
        let step = CatchUnwind(Box::pin(self.registry.step_stored(
            &claim.row.kind,
            claim.row.version,
            self.context.as_ref(),
            Some(workflow_id),
            &claim.row.input_json,
            &claim.row.state_json,
            event_input,
        )));
        let stepped = match tokio::time::timeout(self.config.step_timeout, step).await {
            Ok(Ok(stepped)) => stepped,
            Ok(Err(payload)) => {
                let message = format!("step panicked: {}", panic_message(&*payload));
                self.record_activation_failure(&claim, &message).await?;
                return Ok(workflow_id);
            }
            Err(_) => {
                self.record_activation_failure(&claim, "step exceeded step_timeout")
                    .await?;
                return Ok(workflow_id);
            }
        };
        match stepped {
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

    /// Leases one ready workflow, or recovers one whose lease expired (T-C1).
    /// The returned claim borrows this coordinator until it is activated or
    /// dropped, so a coordinator holds at most one outstanding claim.
    pub async fn claim_one(&mut self) -> Result<Option<WorkflowClaim<'_, C>>, DurableError> {
        let claimed = self.claim_row().await?;
        Ok(claimed.map(|claimed| WorkflowClaim {
            coordinator: self,
            claimed,
        }))
    }

    async fn claim_row(&self) -> Result<Option<ClaimedRow>, DurableError> {
        let lease_duration_millis = duration_millis(self.config.lease_duration)?;
        let worker_id = self.worker_id.clone();
        let local_definitions = self.registry.definition_keys();
        let configured_max_activation =
            i32::try_from(self.config.max_activation_attempts).unwrap_or(i32::MAX);
        let actor = self.worker_id.as_str();
        let mut connection = self.pool.get().await?;
        crate::dialect::transaction(
            &mut connection,
            async move |Tx {
                            connection, trace, ..
                        }| {
                crate::trace::actor(actor);
                let now = persistence::database_now_millis(connection).await?;
                let lease_expires_at =
                    now.checked_plus_millis(lease_duration_millis)
                        .ok_or_else(|| {
                            DurableError::InvalidDefinition(
                                "workflow lease timestamp overflow".to_string(),
                            )
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
                        .load::<(i64, WorkflowId)>(connection)
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
                                    .is_some_and(|expiry| expiry <= now.get())
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
                    append_history(connection, expired.id, "lease_recovered", now).await?;
                    crate::trace::touch_wf(expired.id);
                }

                let mut ready = durable_workflow::table.into_boxed::<crate::Db>();
                let mut definitions = local_definitions.into_iter();
                let Some((kind, version)) = definitions.next() else {
                    return Ok(commit_workflow_claim(trace, recovered, None, 0, None));
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
                    .load::<WorkflowId>(connection)
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
                    return Ok(commit_workflow_claim(trace, recovered, None, 0, None));
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
                let claimed = (row.id, lease_token.clone(), lease_expires_at.get());
                let max_activation = row.max_activation_attempts.min(configured_max_activation);
                row.status = WorkflowStatus::Running;
                row.lease_token = Some(lease_token.clone());
                row.lease_expires_at = Some(lease_expires_at.get());
                Ok(commit_workflow_claim(
                    trace,
                    recovered,
                    Some((claimed.0, &claimed.1, claimed.2)),
                    max_activation,
                    Some(ClaimedRow { row, lease_token }),
                ))
            },
        )
        .await
    }

    async fn commit_transition(
        &self,
        claim: ClaimedRow,
        event: WorkflowEventRow,
        transition: StoredTransition,
    ) -> Result<(), DurableError> {
        let mut connection = self.pool.get().await?;
        let config = self.config;
        let actor = self.worker_id.as_str();
        let fence = (claim.row.id, claim.lease_token.clone());
        let result = crate::dialect::transaction(
            &mut connection,
            async move |Tx {
                            connection,
                            scope,
                            trace,
                        }| {
                crate::trace::actor(actor);
                let trace = commit_on_connection(
                    connection, scope, trace, &claim, &event, transition, config,
                )
                .await?;
                Ok(trace.commit(()))
            },
        )
        .await;
        drop(connection);
        self.record_fence_miss(fence, &result).await;
        result
    }

    /// `CoordFenceMiss`: T-C2 or T-C3 lost its fence and rolled back.
    async fn record_fence_miss(
        &self,
        (workflow_id, token): (WorkflowId, String),
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
        claim: &ClaimedRow,
        message: &str,
    ) -> Result<(), DurableError> {
        let mut connection = self.pool.get().await?;
        let claim = claim.clone();
        let fence = (claim.row.id, claim.lease_token.clone());
        let message = message.to_string();
        let config = self.config;
        let actor = self.worker_id.as_str();
        let result = crate::dialect::transaction(
            &mut connection,
            async move |Tx {
                            connection,
                            scope,
                            trace,
                        }| {
                crate::trace::actor(actor);
                let current = tx::lock_optional(
                    connection,
                    scope,
                    durable_workflow::table
                        .find(claim.row.id)
                        .filter(durable_workflow::status.eq(WorkflowStatus::Running))
                        .filter(durable_workflow::lease_token.eq(&claim.lease_token))
                        .for_update()
                        .select(WorkflowRow::as_select()),
                )
                .await?
                .ok_or(DurableError::FencedWrite)?;
                let attempt = current.activation_attempts.saturating_add(1);
                let configured_max =
                    i32::try_from(config.max_activation_attempts).unwrap_or(i32::MAX);
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
                let available_at = now.saturating_plus_millis(duration_millis(delay)?);
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
                let trace = trace.declare(|| {
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
                    current.id,
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
                        current.as_ref(),
                        Err((error.category, error.message)),
                        now,
                    )
                    .await?;
                }
                Ok(trace.commit(()))
            },
        )
        .await;
        drop(connection);
        self.record_fence_miss(fence, &result).await;
        result
    }
}

async fn commit_on_connection<'tx>(
    connection: &mut crate::DurableConnection,
    scope: TxScope<'tx>,
    trace: Trace<'tx, Undeclared>,
    claim: &ClaimedRow,
    event: &WorkflowEventRow,
    transition: StoredTransition,
    config: CoordinatorConfig,
) -> Result<Trace<'tx, Declared>, DurableError> {
    let workflow_id = claim.workflow_id();
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
                now.saturating_plus_millis(duration_millis(config.continuation_delay)?)
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
            let trace = trace.declare(|| {
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
                    created_at: now.get(),
                },
            )
            .await?;
            Ok(trace)
        }
        StoredTransition::Complete { output_json } => {
            let completed = tx::lock_by_update(
                connection,
                scope,
                &claim.row,
                &claim.lease_token,
                (
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
                ),
            )
            .await?;
            let trace = trace.declare(|| {
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
                completed,
                Ok(output_json),
                now,
            )
            .await?;
            Ok(trace)
        }
        StoredTransition::SleepUntil {
            state_json,
            wake_at_millis,
        } => {
            let wait = WaitTransition::SleepUntil {
                state_json,
                wake_at_millis,
            };
            commit_wait_transition(connection, scope, trace, claim, delivered, wait, now).await
        }
        StoredTransition::WaitForApproval {
            state_json,
            approval_json,
            expires_at_millis,
        } => {
            let wait = WaitTransition::WaitForApproval {
                state_json,
                approval_json,
                expires_at_millis,
            };
            commit_wait_transition(connection, scope, trace, claim, delivered, wait, now).await
        }
        StoredTransition::RunActivity {
            state_json,
            activity,
        } => {
            let wait = WaitTransition::RunActivity {
                state_json,
                activity,
            };
            commit_wait_transition(connection, scope, trace, claim, delivered, wait, now).await
        }
        StoredTransition::RunChild { state_json, child } => {
            let wait = WaitTransition::RunChild { state_json, child };
            commit_wait_transition(connection, scope, trace, claim, delivered, wait, now).await
        }
    }
}

/// The [`StoredTransition`]s that leave the workflow waiting, which
/// `commit_wait_transition` commits. A separate enum, so that function has no
/// arm for `Continue` or `Complete` to reject at run time.
enum WaitTransition {
    SleepUntil {
        state_json: String,
        wake_at_millis: i64,
    },
    WaitForApproval {
        state_json: String,
        approval_json: String,
        expires_at_millis: Option<i64>,
    },
    RunActivity {
        state_json: String,
        activity: crate::ActivityCommand,
    },
    RunChild {
        state_json: String,
        child: crate::ChildWorkflowCommand,
    },
}

async fn commit_wait_transition<'tx>(
    connection: &mut crate::DurableConnection,
    scope: TxScope<'tx>,
    trace: Trace<'tx, Undeclared>,
    claim: &ClaimedRow,
    delivered: i32,
    transition: WaitTransition,
    now: DbMillis,
) -> Result<Trace<'tx, Declared>, DurableError> {
    use crate::persistence::NewApprovalRow;

    let workflow_id = claim.workflow_id();
    let command = claim.row.command_sequence.saturating_add(1);
    match transition {
        WaitTransition::SleepUntil {
            state_json,
            wake_at_millis,
        } => {
            let changed = diesel::update(fenced_workflow!(claim))
                .set((
                    durable_workflow::state_json.eq(state_json),
                    durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
                    durable_workflow::status.eq(WorkflowStatus::Sleeping),
                    persistence::WaitColumns::on(Wait::Timer {
                        command_sequence: u32::try_from(command).map_err(|_| {
                            DurableError::InvalidState(
                                "negative workflow command sequence".to_string(),
                            )
                        })?,
                    }),
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
            let trace = declare_unmodeled(trace, "timer");
            append_history(connection, workflow_id, "timer_scheduled", now).await?;
            Ok(trace)
        }
        WaitTransition::WaitForApproval {
            state_json,
            approval_json,
            expires_at_millis,
        } => {
            let fence = lock_fence(connection, scope, claim).await?;
            let approval_id = crate::dialect::insert_approval(
                connection,
                fence.as_ref(),
                NewApprovalRow {
                    workflow_id: claim.row.id,
                    command_sequence: command,
                    kind: claim.row.kind.clone(),
                    version: claim.row.version,
                    prompt_metadata_json: approval_json,
                    validation_schema_json: "{}".to_string(),
                    validation_version: 1,
                    status: ApprovalStatus::Pending,
                    requested_at: now.get(),
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
                    persistence::WaitColumns::on(Wait::Approval(approval_id)),
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
            let trace = declare_unmodeled(trace, "approval");
            append_history(connection, workflow_id, "approval_requested", now).await?;
            Ok(trace)
        }
        WaitTransition::RunActivity {
            state_json,
            activity,
        } => {
            commit_activity(
                connection, scope, trace, claim, delivered, command, state_json, activity, now,
            )
            .await
        }
        WaitTransition::RunChild { state_json, child } => {
            commit_child(
                connection, scope, trace, claim, delivered, command, state_json, child, now,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_activity<'tx>(
    connection: &mut crate::DurableConnection,
    scope: TxScope<'tx>,
    trace: Trace<'tx, Undeclared>,
    claim: &ClaimedRow,
    delivered: i32,
    command: i32,
    state_json: String,
    activity: crate::ActivityCommand,
    now: DbMillis,
) -> Result<Trace<'tx, Declared>, DurableError> {
    use crate::persistence::{ActivityStatus, NewActivityRow};

    let available_at = if activity.has_continuation_priority() {
        crate::transition::CONTINUATION_READY_AT_MILLIS
    } else {
        now.get()
    };
    let fence = lock_fence(connection, scope, claim).await?;
    let activity_id = crate::dialect::insert_activity(
        connection,
        fence.as_ref(),
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
            created_at: now.get(),
            updated_at: now.get(),
            completed_at: None,
        },
    )
    .await?;
    let changed = diesel::update(fenced_workflow!(claim))
        .set((
            durable_workflow::state_json.eq(state_json),
            durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
            durable_workflow::status.eq(WorkflowStatus::WaitingActivity),
            persistence::WaitColumns::on(Wait::Activity(activity_id)),
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
    let trace = trace.declare(|| {
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
    append_history(connection, claim.workflow_id(), "activity_scheduled", now).await?;
    Ok(trace)
}

#[allow(clippy::too_many_arguments)]
async fn commit_child<'tx>(
    connection: &mut crate::DurableConnection,
    scope: TxScope<'tx>,
    trace: Trace<'tx, Undeclared>,
    claim: &ClaimedRow,
    delivered: i32,
    command: i32,
    state_json: String,
    child: crate::ChildWorkflowCommand,
    now: DbMillis,
) -> Result<Trace<'tx, Declared>, DurableError> {
    let parent_id = claim.row.id;
    let deduplication_key = child
        .deduplication_key()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("child:{parent_id}:{command}"));
    let root_workflow_id = claim.row.root_workflow_id.unwrap_or(parent_id);
    let trace_key = crate::trace::ENABLED.then(|| deduplication_key.clone());
    let outcome = crate::DurableStore::insert_child(
        connection,
        scope,
        &child,
        deduplication_key.clone(),
        parent_id,
        command,
        root_workflow_id,
    )
    .await?;
    // G8: waiting on the caller or an ancestor never ends. This check runs
    // before `commit_child` takes a lock of its own. For the caller's own key, the keyed row that
    // `insert_child` locked is the caller's row, so the transaction holds one
    // row lock when it rolls back: no second lock, no wait. For an ancestor's
    // key, only the ancestor's generations are locked; the caller's row is not
    // locked until the fenced update below, which this path never reaches.
    let child_id = outcome.workflow_id();
    if !outcome.inserted() && is_caller_or_ancestor(connection, claim, child_id).await? {
        return Err(DurableError::InvalidDefinition(format!(
            "child key {deduplication_key} resolves to workflow {}, which is the caller or an ancestor",
            child_id.get()
        )));
    }
    // Lock an existing child before the parent: its terminal transaction locks
    // the child and then its waiting parents, so the reverse order deadlocks
    // (G9). The lock is held to commit, so the status read here stays current
    // until the parent's wait is visible. A new child needs no lock: no other
    // transaction sees it yet. The parent's fence is therefore not taken
    // first here; `insert_child` never fails on the auto key's duplicate, so
    // a stale claim still ends in `FencedWrite` below.
    set_child_wait(
        connection, claim, &outcome, state_json, command, delivered, now,
    )
    .await?;
    let trace = trace.declare(|| {
        crate::trace::Action::new(
            "TC2_Commit",
            serde_json::json!({
                "workflow_id": parent_id,
                "token": claim.lease_token,
                "transition": "run_child",
                "consumed": delivered,
                "child_kind": child.kind(),
                "child_key": trace_key,
                "child_id": child_id.get(),
                "inserted": outcome.inserted(),
            }),
        )
    });
    append_history(
        connection,
        claim.workflow_id(),
        "child_workflow_scheduled",
        now,
    )
    .await?;
    if let ChildStart::Existing(existing) = &outcome {
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
            WorkflowStatus::Ready
            | WorkflowStatus::Running
            | WorkflowStatus::WaitingActivity
            | WorkflowStatus::WaitingChild
            | WorkflowStatus::Sleeping
            | WorkflowStatus::WaitingApproval
            | WorkflowStatus::Paused
            | WorkflowStatus::Blocked => None,
        };
        if let Some(terminal_outcome) = terminal_outcome {
            persistence::wake_waiting_parents_on_child_terminal(
                connection,
                Locked::as_ref(existing),
                terminal_outcome,
                now,
            )
            .await?;
        }
    }
    Ok(trace)
}

/// The parent's fenced move to `waiting_child`. Takes the [`ChildStart`]: an
/// existing child arrives locked (G9), a new one needs no lock.
async fn set_child_wait(
    connection: &mut crate::DurableConnection,
    claim: &ClaimedRow,
    child: &ChildStart<'_>,
    state_json: String,
    command: i32,
    delivered: i32,
    now: DbMillis,
) -> Result<(), DurableError> {
    let changed = diesel::update(fenced_workflow!(claim))
        .set((
            durable_workflow::state_json.eq(state_json),
            durable_workflow::state_version.eq(claim.row.state_version.saturating_add(1)),
            durable_workflow::status.eq(WorkflowStatus::WaitingChild),
            persistence::WaitColumns::on(Wait::Child(child.workflow_id())),
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
    ensure_fenced(changed)
}

/// Whether `target` is the claimed workflow or one of its ancestors (G8). The
/// walk follows `parent_workflow_id`, and from a row without a parent its
/// `restarted_from_workflow_id`: a T-X2 or T-A5 successor carries no parent,
/// but it stands in for the generation it restarted, whose parent G2 may have
/// re-attached to it. Both links point at a row inserted earlier, so the
/// walk ends.
async fn is_caller_or_ancestor(
    connection: &mut crate::DurableConnection,
    claim: &ClaimedRow,
    target: WorkflowId,
) -> Result<bool, DurableError> {
    let mut current = Some(claim.row.id);
    while let Some(id) = current {
        if id == target {
            return Ok(true);
        }
        let (parent, restarted_from) = if id == claim.row.id {
            (
                claim.row.parent_workflow_id,
                claim.row.restarted_from_workflow_id,
            )
        } else {
            durable_workflow::table
                .find(id)
                .select((
                    durable_workflow::parent_workflow_id,
                    durable_workflow::restarted_from_workflow_id,
                ))
                .first::<(Option<WorkflowId>, Option<WorkflowId>)>(connection)
                .await?
        };
        current = parent.or(restarted_from);
    }
    Ok(false)
}

async fn append_history(
    connection: &mut crate::DurableConnection,
    workflow_id: WorkflowId,
    event_type: &str,
    created_at: DbMillis,
) -> Result<(), DurableError> {
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id,
            sequence,
            delivery_sequence: None,
            event_type: event_type.to_string(),
            metadata_json: None,
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason: None,
            created_at: created_at.get(),
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
fn declare_unmodeled<'tx>(
    trace: Trace<'tx, Undeclared>,
    name: &'static str,
) -> Trace<'tx, Declared> {
    trace.declare_unmodeled(name, true)
}

/// Commits T-C1 with `value`: declared as `TC1_Claim` when the claim
/// recovered or claimed a workflow, unchanged otherwise. `max_activation` is
/// the activation cap T-C3 applies to the claimed row.
fn commit_workflow_claim<R>(
    trace: Trace<'_, Undeclared>,
    recovered: Option<WorkflowId>,
    claimed: Option<(WorkflowId, &str, i64)>,
    max_activation: i32,
    value: R,
) -> Committed<R> {
    if recovered.is_none() && claimed.is_none() {
        return trace.unchanged(value);
    }
    trace
        .declare(|| {
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
        })
        .commit(value)
}

/// Locks the claimed row under its fence before a T-C2 path inserts a command
/// row. A stale claim then gets `FencedWrite`, not a unique-key error from the
/// command the recovering claim already inserted (N4).
async fn lock_fence<'tx>(
    connection: &mut crate::DurableConnection,
    scope: TxScope<'tx>,
    claim: &ClaimedRow,
) -> Result<Locked<'tx, ClaimFence>, DurableError> {
    tx::lock_optional(
        connection,
        scope,
        fenced_workflow!(claim)
            .for_update()
            .select((durable_workflow::id,)),
    )
    .await?
    .ok_or(DurableError::FencedWrite)
}

/// A T-C2/T-C3 outcome that is not a coordinator error: the fence was lost
/// (an operator action or lease recovery moved the row on), or a transient
/// database error rolled the transaction back.
fn benign_activation_kind(error: &DurableError) -> Option<BenignActivationKind> {
    match error {
        DurableError::FencedWrite => Some(BenignActivationKind::FenceMiss),
        error if crate::dialect::is_transient_error(error) => Some(BenignActivationKind::Transient),
        _ => None,
    }
}

/// Resolves to `Err(payload)` when polling the inner future panics.
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
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

#[cfg(test)]
mod tests {
    use super::*;

    /// G3/S8: with the defaults, a timed-out step finishes (T-C3) before its
    /// workflow lease expires, leaving a third of the lease as margin.
    #[test]
    fn default_step_timeout_leaves_a_margin_below_the_lease() {
        let config = CoordinatorConfig::default();
        assert!(config.step_timeout < config.lease_duration);
        assert!(config.lease_duration - config.step_timeout >= config.lease_duration / 3);
    }
}
