use std::{collections::HashMap, sync::Arc, time::Duration};

use diesel::{
    BoolExpressionMethods, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    observability::lease_fingerprint,
    persistence::{
        self, ActivityRow, ActivityStatus, NewActivityAttemptRow, NewWorkflowEventRow,
        WorkflowStatus,
    },
    schema::{durable_activity, durable_activity_attempt, durable_topic_lock, durable_workflow},
    ActivityContext, ActivityDispatchError, ActivityError, ActivityId, ActivityRegistry,
    ActivityResult, DurableError, DurablePool, ProgressReporter, RetryPolicy, TopicRegistry,
    WorkflowEvent, WorkflowId,
};

macro_rules! fenced_activity {
    ($claim:expr) => {
        durable_activity::table
            .find($claim.row.id)
            .filter(durable_activity::status.eq(ActivityStatus::Running))
            .filter(durable_activity::attempt_count.eq($claim.attempt_number))
            .filter(durable_activity::lease_token.eq(&$claim.lease_token))
    };
}

const CLAIM_CANDIDATE_SCAN_LIMIT: i64 = 32;

#[derive(Debug, Clone, Copy)]
pub struct WorkerConfig {
    pub heartbeat_interval: Duration,
    pub shutdown_grace: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityClaim {
    row: ActivityRow,
    schedule_run_id: Option<i64>,
    attempt_number: i32,
    lease_token: String,
    lease_deadline: tokio::time::Instant,
}

impl ActivityClaim {
    pub fn activity_id(&self) -> Result<ActivityId, DurableError> {
        ActivityId::new(self.row.id)
    }

    pub fn attempt_number(&self) -> Result<u32, DurableError> {
        u32::try_from(self.attempt_number)
            .map_err(|_| DurableError::InvalidState("negative activity attempt".to_string()))
    }

    pub fn lease_token(&self) -> &str {
        &self.lease_token
    }

    pub fn topic(&self) -> &str {
        &self.row.topic
    }
}

pub struct ActivityWorker<C> {
    pool: DurablePool,
    context: Arc<C>,
    activities: Arc<ActivityRegistry<C>>,
    topics: Arc<TopicRegistry>,
    worker_id: String,
    config: WorkerConfig,
    cancellation: CancellationToken,
    forced_cancellation: CancellationToken,
}

impl<C> ActivityWorker<C>
where
    C: Send + Sync + 'static,
{
    pub fn new(
        pool: DurablePool,
        context: Arc<C>,
        activities: Arc<ActivityRegistry<C>>,
        topics: Arc<TopicRegistry>,
        worker_id: impl Into<String>,
        config: WorkerConfig,
    ) -> Result<Self, DurableError> {
        let worker_id = worker_id.into();
        if worker_id.is_empty()
            || config.heartbeat_interval.is_zero()
            || config.shutdown_grace.is_zero()
        {
            return Err(DurableError::InvalidDefinition(
                "activity worker identity and bounds must be non-zero".to_string(),
            ));
        }
        Ok(Self {
            pool,
            context,
            activities,
            topics,
            worker_id,
            config,
            cancellation: CancellationToken::new(),
            forced_cancellation: CancellationToken::new(),
        })
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn with_cancellation_token(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub(crate) fn with_forced_cancellation_token(
        mut self,
        forced_cancellation: CancellationToken,
    ) -> Self {
        self.forced_cancellation = forced_cancellation;
        self
    }

    pub fn shutdown(&self) {
        self.cancellation.cancel();
    }

    pub fn progress_reporter(
        &self,
        claim: &ActivityClaim,
    ) -> Result<ProgressReporter, DurableError> {
        Ok(ProgressReporter::new(
            self.pool.clone(),
            claim.activity_id()?,
            claim.attempt_number,
            claim.lease_token.clone(),
        ))
    }

    pub async fn run_one(&self, topic: &str) -> Result<Option<ActivityId>, DurableError> {
        let Some(claim) = self.claim_one(topic).await? else {
            return Ok(None);
        };
        let activity_id = claim.activity_id()?;
        let lease_fingerprint = lease_fingerprint(&claim.lease_token);
        let span = tracing::info_span!(
            "durable.activity.attempt",
            otel.kind = "consumer",
            workflow_id = claim.row.workflow_id,
            schedule_run_id = ?claim.schedule_run_id,
            activity_id = activity_id.get(),
            attempt_number = claim.attempt_number,
            kind = %claim.row.kind,
            version = claim.row.version,
            topic = %claim.row.topic,
            lease_fingerprint = %lease_fingerprint,
            worker_id = %self.worker_id,
        );
        self.execute_claim(claim).instrument(span).await?;
        Ok(Some(activity_id))
    }

    pub async fn claim_one(&self, topic: &str) -> Result<Option<ActivityClaim>, DurableError> {
        self.topics.get(topic).ok_or_else(|| {
            DurableError::InvalidDefinition(format!("unregistered activity topic {topic}"))
        })?;
        let local_definitions = self.activities.definition_keys();
        let topic = topic.to_string();
        let worker_id = self.worker_id.clone();
        let mut connection = self.pool.get().await?;
        self.topics.seed_locks(&mut connection).await?;
        let (result, rolled_back) = crate::trace::capture_rollback(crate::dialect::transaction(
            &mut connection,
            async move |connection| {
                crate::trace::actor(&worker_id);
                let topics = std::slice::from_ref(&topic);
                let lease_sample_started = tokio::time::Instant::now();
                let now = persistence::database_now_millis(connection).await?;
                let max_concurrency = durable_topic_lock::table
                    .find(&topic)
                    .for_update()
                    .select(durable_topic_lock::max_concurrency)
                    .first::<i32>(connection)
                    .await?;
                let max_concurrency = i64::from(max_concurrency);

                let reconciled = reconcile_expired(connection, &topic, now).await?;

                let in_flight = durable_activity::table
                    .filter(durable_activity::topic.eq(&topic))
                    .filter(durable_activity::status.eq(ActivityStatus::Running))
                    .filter(durable_activity::lease_expires_at.gt(now))
                    .count()
                    .get_result::<i64>(connection)
                    .await?;
                crate::trace::note("in_flight_seen", || topic_value(&topic, in_flight.into()));
                crate::trace::note("local_avail", || topic_value(&topic, 1.into()));
                if in_flight >= max_concurrency {
                    declare_activity_claims(topics, &reconciled, &[]);
                    return Ok(None);
                }

                let mut pending = durable_activity::table
                    .inner_join(
                        durable_workflow::table
                            .on(durable_workflow::id.eq(durable_activity::workflow_id)),
                    )
                    .into_boxed::<crate::Db>();
                let mut definitions = local_definitions.into_iter();
                let Some((kind, version)) = definitions.next() else {
                    declare_activity_claims(topics, &reconciled, &[]);
                    return Ok(None);
                };
                pending = pending.filter(
                    durable_activity::kind
                        .eq(kind)
                        .and(durable_activity::version.eq(version)),
                );
                for (kind, version) in definitions {
                    pending = pending.or_filter(
                        durable_activity::kind
                            .eq(kind)
                            .and(durable_activity::version.eq(version)),
                    );
                }
                // A stable workflow tie-break lets finite scans finish; FIFO among pages can
                // repeatedly expire every cursor when the continuation backlog is large.
                let candidates = pending
                    .filter(durable_activity::topic.eq(&topic))
                    .filter(durable_activity::status.eq(ActivityStatus::Pending))
                    .filter(durable_activity::available_at.le(now))
                    .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
                    .filter(durable_workflow::wait_reference_id.eq(durable_activity::id.nullable()))
                    .order((
                        durable_activity::available_at.asc(),
                        diesel::dsl::case_when(
                            durable_activity::available_at
                                .eq(crate::transition::CONTINUATION_READY_AT_MILLIS),
                            durable_activity::workflow_id,
                        )
                        .otherwise(durable_activity::id)
                        .asc(),
                        durable_activity::id.asc(),
                    ))
                    .select((durable_activity::id, durable_activity::workflow_id))
                    .limit(CLAIM_CANDIDATE_SCAN_LIMIT)
                    .load::<(i64, i64)>(connection)
                    .await?;
                for (candidate_id, workflow_id) in candidates {
                    if let Some(claim) = self
                        .claim_locked_candidate(
                            connection,
                            candidate_id,
                            workflow_id,
                            now,
                            lease_sample_started,
                            &worker_id,
                        )
                        .await?
                    {
                        declare_activity_claims(topics, &reconciled, std::slice::from_ref(&claim));
                        return Ok(Some(claim));
                    }
                }
                declare_activity_claims(topics, &reconciled, &[]);
                Ok(None)
            },
        ))
        .await;
        drop(connection);
        self.record_claim_error(rolled_back).await;
        result
    }

    /// Claims a bounded cross-topic batch under one short global dispatch
    /// critical section. Acquiring every topic row at once also interoperates
    /// with the legacy single-topic claim protocol during rolling deployments.
    pub async fn claim_batch(
        &self,
        limit: usize,
        local_topic_capacity: &HashMap<String, usize>,
    ) -> Result<Vec<ActivityClaim>, DurableError> {
        if limit == 0 || local_topic_capacity.values().all(|capacity| *capacity == 0) {
            return Ok(Vec::new());
        }
        let topic_definitions = self.topics.definitions();
        let registered_topics: Vec<_> = topic_definitions
            .iter()
            .map(|definition| definition.key.clone())
            .collect();
        let local_definitions = self.activities.definition_keys();
        let worker_id = self.worker_id.clone();
        let mut connection = self.pool.get().await?;
        self.topics.seed_locks(&mut connection).await?;
        let (result, rolled_back) = crate::trace::capture_rollback(crate::dialect::transaction(
            &mut connection,
            async move |connection| {
                let locked_topics = durable_topic_lock::table
                    .filter(durable_topic_lock::topic.eq_any(&registered_topics))
                    .order(durable_topic_lock::topic.asc())
                    .for_update()
                    .skip_locked()
                    .select((
                        durable_topic_lock::topic,
                        durable_topic_lock::max_concurrency,
                    ))
                    .load::<(String, i32)>(connection)
                    .await?;
                if locked_topics.len() != registered_topics.len() {
                    return Ok(Vec::<ActivityClaim>::new());
                }
                crate::trace::actor(&worker_id);

                let lease_sample_started = tokio::time::Instant::now();
                let now = persistence::database_now_millis(connection).await?;
                let mut claims: Vec<ActivityClaim> = Vec::with_capacity(limit);
                let mut reconciled = Vec::new();
                for (topic, persisted_limit) in locked_topics {
                    reconciled.extend(reconcile_expired(connection, &topic, now).await?);
                    let in_flight = durable_activity::table
                        .filter(durable_activity::topic.eq(&topic))
                        .filter(durable_activity::status.eq(ActivityStatus::Running))
                        .filter(durable_activity::lease_expires_at.gt(now))
                        .count()
                        .get_result::<i64>(connection)
                        .await?;
                    let global_available =
                        i64::from(persisted_limit).saturating_sub(in_flight).max(0) as usize;
                    let wanted = global_available
                        .min(
                            local_topic_capacity
                                .get(&topic)
                                .copied()
                                .unwrap_or_default(),
                        )
                        .min(limit.saturating_sub(claims.len()));
                    crate::trace::note("in_flight_seen", || topic_value(&topic, in_flight.into()));
                    crate::trace::note("wanted", || topic_value(&topic, wanted.into()));
                    crate::trace::note("local_avail", || {
                        let local = local_topic_capacity
                            .get(&topic)
                            .copied()
                            .unwrap_or_default()
                            .min(limit.saturating_sub(claims.len()));
                        topic_value(&topic, local.into())
                    });
                    if wanted == 0 {
                        continue;
                    }

                    let mut pending = durable_activity::table
                        .inner_join(
                            durable_workflow::table
                                .on(durable_workflow::id.eq(durable_activity::workflow_id)),
                        )
                        .into_boxed::<crate::Db>();
                    let mut definitions = local_definitions.iter();
                    let Some((kind, version)) = definitions.next() else {
                        declare_activity_claims(&registered_topics, &reconciled, &claims);
                        return Ok(claims);
                    };
                    pending = pending.filter(
                        durable_activity::kind
                            .eq(kind)
                            .and(durable_activity::version.eq(version)),
                    );
                    for (kind, version) in definitions {
                        pending = pending.or_filter(
                            durable_activity::kind
                                .eq(kind)
                                .and(durable_activity::version.eq(version)),
                        );
                    }
                    let candidates = pending
                        .filter(durable_activity::topic.eq(&topic))
                        .filter(durable_activity::status.eq(ActivityStatus::Pending))
                        .filter(durable_activity::available_at.le(now))
                        .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
                        .filter(
                            durable_workflow::wait_reference_id.eq(durable_activity::id.nullable()),
                        )
                        .order((
                            durable_activity::available_at.asc(),
                            diesel::dsl::case_when(
                                durable_activity::available_at
                                    .eq(crate::transition::CONTINUATION_READY_AT_MILLIS),
                                durable_activity::workflow_id,
                            )
                            .otherwise(durable_activity::id)
                            .asc(),
                            durable_activity::id.asc(),
                        ))
                        .select((durable_activity::id, durable_activity::workflow_id))
                        .limit(CLAIM_CANDIDATE_SCAN_LIMIT)
                        .load::<(i64, i64)>(connection)
                        .await?;
                    for (candidate_id, workflow_id) in candidates {
                        if claims.len() >= limit
                            || claims
                                .iter()
                                .filter(|claim| claim.row.topic == topic)
                                .count()
                                >= wanted
                        {
                            break;
                        }
                        if let Some(claim) = self
                            .claim_locked_candidate(
                                connection,
                                candidate_id,
                                workflow_id,
                                now,
                                lease_sample_started,
                                &worker_id,
                            )
                            .await?
                        {
                            claims.push(claim);
                        }
                    }
                }
                declare_activity_claims(&registered_topics, &reconciled, &claims);
                Ok(claims)
            },
        ))
        .await;
        drop(connection);
        self.record_claim_error(rolled_back).await;
        result
    }

    /// Records a T-W1 that `claim_locked_candidate` aborted (`TW1_Error`, G10)
    /// once the transaction has rolled back and its connection is released.
    async fn record_claim_error(&self, rolled_back: Option<crate::trace::Action>) {
        if let Some(action) = rolled_back {
            crate::trace::record_local(&self.pool, &self.worker_id, action).await;
        }
    }

    async fn claim_locked_candidate(
        &self,
        connection: &mut crate::DurableConnection,
        candidate_id: i64,
        workflow_id: i64,
        now: i64,
        lease_sample_started: tokio::time::Instant,
        worker_id: &str,
    ) -> Result<Option<ActivityClaim>, DurableError> {
        let locked_workflow = durable_workflow::table
            .find(workflow_id)
            .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
            .filter(durable_workflow::wait_reference_id.eq(Some(candidate_id)))
            .for_update()
            .skip_locked()
            .select(durable_workflow::schedule_run_id)
            .first::<Option<i64>>(connection)
            .await
            .optional()?;
        let Some(schedule_run_id) = locked_workflow else {
            return Ok(None);
        };
        let Some(mut row) = durable_activity::table
            .find(candidate_id)
            .filter(durable_activity::workflow_id.eq(workflow_id))
            .filter(durable_activity::status.eq(ActivityStatus::Pending))
            .filter(durable_activity::available_at.le(now))
            .for_update()
            .skip_locked()
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(connection)
            .await
            .optional()?
        else {
            return Ok(None);
        };
        if !self.activities.contains(&row.kind, row.version) {
            declare_claim_error("missing_definition", row.id);
            return Err(DurableError::MissingDefinition {
                kind: row.kind,
                version: row.version,
            });
        }
        let attempt_number = row
            .attempt_count
            .checked_add(1)
            .ok_or_else(|| DurableError::InvalidState("activity attempt overflow".to_string()))?;
        if attempt_number > row.max_attempts {
            declare_claim_error("attempt_cap", row.id);
            return Err(DurableError::InvalidState(format!(
                "pending activity {} has exhausted its attempt cap",
                row.id
            )));
        }
        if row.timeout_millis <= 0 || row.lease_duration_millis <= row.timeout_millis {
            declare_claim_error("invalid_bounds", row.id);
            return Err(DurableError::InvalidState(format!(
                "activity {} has invalid timeout or lease bounds",
                row.id
            )));
        }
        let lease_token = uuid::Uuid::new_v4().to_string();
        let lease_expires_at = now.checked_add(row.lease_duration_millis).ok_or_else(|| {
            DurableError::InvalidState("activity lease timestamp overflow".to_string())
        })?;
        let changed = diesel::update(
            durable_activity::table
                .find(row.id)
                .filter(durable_activity::status.eq(ActivityStatus::Pending))
                .filter(durable_activity::attempt_count.eq(row.attempt_count)),
        )
        .set((
            durable_activity::status.eq(ActivityStatus::Running),
            durable_activity::attempt_count.eq(attempt_number),
            durable_activity::lease_owner.eq(Some(worker_id.to_string())),
            durable_activity::lease_token.eq(Some(lease_token.clone())),
            durable_activity::lease_expires_at.eq(Some(lease_expires_at)),
            durable_activity::updated_at.eq(now),
        ))
        .execute(connection)
        .await?;
        ensure_fenced(changed)?;
        diesel::insert_into(durable_activity_attempt::table)
            .values(NewActivityAttemptRow {
                activity_id: row.id,
                attempt_number,
                worker_id: worker_id.to_string(),
                lease_token: lease_token.clone(),
                started_at: now,
                heartbeat_at: now,
                finished_at: None,
                outcome: None,
                error_category: None,
                error_message: None,
                provider_result_json: None,
            })
            .execute(connection)
            .await?;
        crate::trace::touch_act(row.id);
        crate::trace::touch_att(row.id, attempt_number);
        row.status = ActivityStatus::Running;
        row.attempt_count = attempt_number;
        row.lease_token = Some(lease_token.clone());
        row.lease_owner = Some(worker_id.to_string());
        row.lease_expires_at = Some(lease_expires_at);
        let lease_deadline =
            lease_deadline_from_sample(lease_sample_started, row.lease_duration_millis)?;
        Ok(Some(ActivityClaim {
            lease_deadline,
            row,
            schedule_run_id,
            attempt_number,
            lease_token,
        }))
    }

    pub(crate) async fn execute_claim_traced(
        &self,
        claim: ActivityClaim,
    ) -> Result<(), DurableError> {
        let activity_id = claim.activity_id()?;
        let lease_fingerprint = lease_fingerprint(&claim.lease_token);
        let span = tracing::info_span!(
            "durable.activity.attempt",
            otel.kind = "consumer",
            workflow_id = claim.row.workflow_id,
            schedule_run_id = ?claim.schedule_run_id,
            activity_id = activity_id.get(),
            attempt_number = claim.attempt_number,
            kind = %claim.row.kind,
            version = claim.row.version,
            topic = %claim.row.topic,
            lease_fingerprint = %lease_fingerprint,
            worker_id = %self.worker_id,
        );
        self.execute_claim(claim).instrument(span).await
    }

    pub async fn heartbeat(&self, claim: &ActivityClaim) -> Result<(), DurableError> {
        heartbeat_once(
            &self.pool,
            claim,
            &self.worker_id,
            crate::trace::next_heartbeat_id(),
            &std::sync::atomic::AtomicBool::new(false),
        )
        .await
        .map(|_| ())
    }

    async fn execute_claim(&self, claim: ActivityClaim) -> Result<(), DurableError> {
        if claim.lease_deadline <= tokio::time::Instant::now() {
            record_execution_step(&self.pool, &self.worker_id, &claim, "LocalDeadline", None).await;
            return Err(DurableError::FencedWrite);
        }
        let lease_duration = duration_from_millis(claim.row.lease_duration_millis)?;
        let heartbeat_interval = self
            .config
            .heartbeat_interval
            .min((lease_duration / 3).max(Duration::from_millis(1)));
        let heartbeat_stop = CancellationToken::new();
        let heartbeat = heartbeat_loop(
            self.pool.clone(),
            claim.clone(),
            heartbeat_interval,
            heartbeat_stop.clone(),
            self.worker_id.clone(),
        );
        tokio::pin!(heartbeat);

        let child_cancellation = self.cancellation.child_token();
        let progress = ProgressReporter::new(
            self.pool.clone(),
            claim.activity_id()?,
            claim.attempt_number,
            claim.lease_token.clone(),
        );
        let context = ActivityContext::for_execution(
            self.context.as_ref(),
            claim.activity_id()?,
            claim.attempt_number()?,
            &claim.lease_token,
            claim.row.operation_key.as_deref(),
            child_cancellation.clone(),
            progress,
        );
        let execution = self.activities.execute_claimed(
            &claim.row.kind,
            claim.row.version,
            context,
            &claim.row.payload_json,
        );
        tokio::pin!(execution);
        let timeout = tokio::time::sleep(duration_from_millis(claim.row.timeout_millis)?);
        tokio::pin!(timeout);
        let shutdown_deadline = tokio::time::sleep(self.config.shutdown_grace);
        tokio::pin!(shutdown_deadline);
        let mut shutdown_requested = false;
        let mut forced_shutdown_requested = false;
        let mut heartbeat_stopped = false;
        let mut execution_finished = false;
        let mut timeout_cleanup_pending = false;
        let mut requested_outcome = None;

        let outcome = loop {
            if heartbeat_stopped {
                if forced_shutdown_requested {
                    return Ok(());
                }
                if !timeout_cleanup_pending {
                    if let Some(outcome) = requested_outcome.take() {
                        break outcome;
                    }
                }
            }

            tokio::select! {
                biased;
                _ = self.forced_cancellation.cancelled(), if !forced_shutdown_requested => {
                    child_cancellation.cancel();
                    heartbeat_stop.cancel();
                    forced_shutdown_requested = true;
                }
                _ = self.cancellation.cancelled(), if !shutdown_requested => {
                    child_cancellation.cancel();
                    shutdown_deadline
                        .as_mut()
                        .reset(tokio::time::Instant::now() + self.config.shutdown_grace);
                    shutdown_requested = true;
                }
                _ = &mut shutdown_deadline,
                    if (shutdown_requested && requested_outcome.is_none()) || timeout_cleanup_pending => {
                    heartbeat_stop.cancel();
                    if timeout_cleanup_pending {
                        tracing::warn!("timed-out activity cleanup grace elapsed");
                        timeout_cleanup_pending = false;
                    }
                    if requested_outcome.is_none() {
                        requested_outcome = Some(ExecutionOutcome::Retryable(
                            ActivityError::retryable(
                                "cancelled", "activity worker shutdown grace elapsed"),
                        ));
                    }
                }
                _ = &mut timeout, if requested_outcome.is_none() => {
                    child_cancellation.cancel();
                    if !shutdown_requested {
                        shutdown_deadline
                            .as_mut()
                            .reset(tokio::time::Instant::now() + self.config.shutdown_grace);
                        shutdown_requested = true;
                    }
                    timeout_cleanup_pending = true;
                    requested_outcome = Some(ExecutionOutcome::Retryable(
                        ActivityError::retryable(
                            "timeout", "activity execution exceeded its configured timeout"),
                    ));
                }
                result = &mut heartbeat, if !heartbeat_stopped => {
                    if let Err(failure) = result {
                        child_cancellation.cancel();
                        heartbeat_stop.cancel();
                        if !execution_finished {
                            let deadline = if shutdown_requested {
                                shutdown_deadline.deadline()
                            } else {
                                tokio::time::Instant::now() + self.config.shutdown_grace
                            }.min(failure.lease_deadline);
                            // Cleanup cannot outlive the last confirmed lease when renewal fails.
                            if deadline > tokio::time::Instant::now() {
                                tokio::select! {
                                    biased;
                                    _ = self.forced_cancellation.cancelled() => {}
                                    _ = wait_for_lease_deadline(deadline) => {
                                        tracing::warn!("revoked activity cleanup grace elapsed");
                                    }
                                    result = &mut execution => {
                                        if let Err(cleanup_error) = result {
                                            tracing::warn!(%cleanup_error, "revoked activity cleanup returned an error");
                                        }
                                    }
                                }
                            }
                        }
                        return Err(failure.error);
                    }
                    heartbeat_stopped = true;
                }
                result = &mut execution, if !execution_finished => {
                    execution_finished = true;
                    heartbeat_stop.cancel();
                    if timeout_cleanup_pending {
                        if let Err(cleanup_error) = &result {
                            tracing::warn!(%cleanup_error, "timed-out activity cleanup returned an error");
                        }
                        timeout_cleanup_pending = false;
                    }
                    if requested_outcome.is_none() && !forced_shutdown_requested {
                        requested_outcome = Some(dispatch_outcome(result));
                    }
                }
            }
        };
        record_execution_step(
            &self.pool,
            &self.worker_id,
            &claim,
            "HandlerReturn",
            Some(serde_json::json!({ "outcome": outcome.trace_name() })),
        )
        .await;
        self.finish_claim(&claim, outcome).await
    }

    async fn finish_claim(
        &self,
        claim: &ActivityClaim,
        outcome: ExecutionOutcome,
    ) -> Result<(), DurableError> {
        let mut connection = self.pool.get().await?;
        let finished = claim.clone();
        let actor = self.worker_id.as_str();
        let result = crate::dialect::transaction(&mut connection, async move |connection| {
            crate::trace::actor(actor);
            finish_on_connection(connection, &finished, outcome).await
        })
        .await;
        drop(connection);
        if matches!(result, Err(DurableError::FencedWrite)) {
            record_execution_step(&self.pool, &self.worker_id, claim, "TW3_FenceMiss", None).await;
        }
        result
    }
}

struct HeartbeatFailure {
    error: DurableError,
    lease_deadline: tokio::time::Instant,
}

async fn wait_for_lease_deadline(deadline: tokio::time::Instant) {
    let sleep = tokio::time::sleep_until(deadline);
    tokio::pin!(sleep);
    // Timer readiness can lag monotonic time after the executor has been blocked.
    std::future::poll_fn(|context| {
        if tokio::time::Instant::now() >= deadline {
            std::task::Poll::Ready(())
        } else {
            std::future::Future::poll(sleep.as_mut(), context)
        }
    })
    .await
}

async fn heartbeat_loop(
    pool: DurablePool,
    claim: ActivityClaim,
    heartbeat_interval: Duration,
    stop: CancellationToken,
    actor: String,
) -> Result<(), HeartbeatFailure> {
    let mut lease_deadline = claim.lease_deadline;
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_interval,
        heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            _ = wait_for_lease_deadline(lease_deadline) => {
                if crate::trace::ENABLED {
                    // Detached: the deadline can pass because the pool is exhausted,
                    // and cleanup must not wait for a connection. A later record is
                    // safe: no other step depends on this execution ending.
                    let (pool, actor, claim) = (pool.clone(), actor.clone(), claim.clone());
                    tokio::spawn(async move {
                        record_execution_step(&pool, &actor, &claim, "LocalDeadline", None).await;
                    });
                }
                return Err(HeartbeatFailure { error: DurableError::FencedWrite, lease_deadline });
            }
            _ = heartbeat.tick() => {}
        }
        let hb = crate::trace::next_heartbeat_id();
        let sent = std::sync::atomic::AtomicBool::new(false);
        let mut dropped = false;
        let renewal = tokio::select! {
            biased;
            _ = wait_for_lease_deadline(lease_deadline) => {
                dropped = true;
                Err(DurableError::FencedWrite)
            }
            result = heartbeat_once(&pool, &claim, &actor, hb, &sent) => result,
        };
        // A heartbeat dropped before its checkout sent nothing (and the pool may be
        // exhausted): only a sent one is recorded.
        if dropped
            && std::sync::atomic::AtomicBool::load(&sent, std::sync::atomic::Ordering::Relaxed)
        {
            // The in-flight heartbeat future is gone; its COMMIT may still have
            // landed, in which case the generator discards this record.
            record_heartbeat_step(&pool, &actor, "TW2_Drop", hb).await;
        }
        match renewal {
            Ok(deadline) => lease_deadline = deadline,
            Err(error) => {
                return Err(HeartbeatFailure {
                    error,
                    lease_deadline,
                })
            }
        }
    }
}

async fn heartbeat_once(
    pool: &DurablePool,
    claim: &ActivityClaim,
    actor: &str,
    hb: u64,
    sent: &std::sync::atomic::AtomicBool,
) -> Result<tokio::time::Instant, DurableError> {
    let checkout_pool = pool.clone();
    // A bb8 checkout cancelled during validation can return a half-used connection to the pool.
    // Only checkout outlives lease cancellation; no renewal query runs in this task.
    let mut connection = tokio::spawn(async move { checkout_pool.get_owned().await })
        .await
        .map_err(|error| {
            DurableError::InvalidState(format!("heartbeat checkout task failed: {error}"))
        })??;
    // Recorded on the heartbeat's own connection: a failed checkout sends nothing,
    // and recording never waits on the pool the heartbeat is renewing through.
    if crate::trace::ENABLED {
        crate::trace::record_local_on(
            &mut connection,
            actor,
            crate::trace::Action::new(
                "TW2_Send",
                serde_json::json!({
                    "activity_id": claim.row.id,
                    "attempt": claim.attempt_number,
                    "token": claim.lease_token,
                    "hb": hb,
                }),
            ),
        )
        .await;
        sent.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    let result = crate::dialect::transaction(&mut connection, async move |connection| {
        crate::trace::actor(actor);
        let lease_sample_started = tokio::time::Instant::now();
        let now = persistence::database_now_millis(connection).await?;
        let lease_expires_at = now
            .checked_add(claim.row.lease_duration_millis)
            .ok_or_else(|| DurableError::InvalidState("activity lease overflow".to_string()))?;
        let changed = diesel::update(fenced_activity!(claim))
            .set((
                durable_activity::lease_expires_at.eq(Some(lease_expires_at)),
                durable_activity::updated_at.eq(now),
            ))
            .execute(connection)
            .await?;
        ensure_fenced(changed)?;
        let changed = diesel::update(
            durable_activity_attempt::table
                .find((claim.row.id, claim.attempt_number))
                .filter(durable_activity_attempt::lease_token.eq(&claim.lease_token))
                .filter(durable_activity_attempt::finished_at.is_null()),
        )
        .set(durable_activity_attempt::heartbeat_at.eq(now))
        .execute(connection)
        .await?;
        ensure_fenced(changed)?;
        crate::trace::touch_act(claim.row.id);
        crate::trace::declare(|| {
            crate::trace::Action::new(
                "TW2_Commit",
                serde_json::json!({
                    "hb": hb,
                    "activity_id": claim.row.id,
                    "lease_expires_at": lease_expires_at,
                }),
            )
        });
        lease_deadline_from_sample(lease_sample_started, claim.row.lease_duration_millis)
    })
    .await;
    // A fence miss rolled back cleanly. Other errors may have broken the
    // connection; they are not recorded (the model keeps the heartbeat in flight).
    if crate::trace::ENABLED && matches!(result, Err(DurableError::FencedWrite)) {
        crate::trace::record_local_on(
            &mut connection,
            actor,
            crate::trace::Action::new("TW2_FenceMiss", serde_json::json!({ "hb": hb })),
        )
        .await;
    }
    result
}

fn lease_deadline_from_sample(
    started: tokio::time::Instant,
    duration_millis: i64,
) -> Result<tokio::time::Instant, DurableError> {
    // Database timestamps have millisecond precision; never round authority upward.
    let duration = duration_from_millis(duration_millis)?.saturating_sub(Duration::from_millis(1));
    started.checked_add(duration).ok_or_else(|| {
        DurableError::InvalidState("activity monotonic lease deadline overflow".to_string())
    })
}

enum ExecutionOutcome {
    Succeeded(String),
    Retryable(ActivityError),
    Permanent(ActivityError),
}

impl ExecutionOutcome {
    /// The model's handler outcome (`HandlerReturn`, `TW3_Finish`).
    fn trace_name(&self) -> &'static str {
        match self {
            Self::Succeeded(_) => "succeeded",
            Self::Retryable(_) => "retryable",
            Self::Permanent(_) => "permanent",
        }
    }
}

fn dispatch_outcome(result: Result<String, ActivityDispatchError>) -> ExecutionOutcome {
    match result {
        Ok(output) => ExecutionOutcome::Succeeded(output),
        Err(ActivityDispatchError::Handler(ActivityError::Retryable { category, message })) => {
            ExecutionOutcome::Retryable(ActivityError::retryable(category, message))
        }
        Err(ActivityDispatchError::Handler(ActivityError::Permanent { category, message })) => {
            ExecutionOutcome::Permanent(ActivityError::permanent(category, message))
        }
        Err(error) => {
            ExecutionOutcome::Permanent(ActivityError::permanent("dispatch", error.to_string()))
        }
    }
}

/// Returns the reconciled rows (`TW1_Claim` `reconciled`) in order.
async fn reconcile_expired(
    connection: &mut crate::DurableConnection,
    topic: &str,
    now: i64,
) -> Result<Vec<serde_json::Value>, DurableError> {
    let mut reconciled = Vec::new();
    let candidates = durable_activity::table
        .filter(durable_activity::topic.eq(topic))
        .filter(durable_activity::status.eq(ActivityStatus::Running))
        .filter(
            durable_activity::lease_expires_at
                .le(now)
                .or(durable_activity::lease_expires_at.is_null()),
        )
        .select((durable_activity::id, durable_activity::workflow_id))
        .load::<(i64, i64)>(connection)
        .await?;
    for (activity_id, workflow_id) in candidates {
        let workflow = durable_workflow::table
            .find(workflow_id)
            .for_update()
            .select((
                durable_workflow::status,
                durable_workflow::wait_reference_id,
            ))
            .first::<(WorkflowStatus, Option<i64>)>(connection)
            .await?;
        let Some(row) = durable_activity::table
            .find(activity_id)
            .filter(durable_activity::topic.eq(topic))
            .filter(durable_activity::status.eq(ActivityStatus::Running))
            .filter(
                durable_activity::lease_expires_at
                    .le(now)
                    .or(durable_activity::lease_expires_at.is_null()),
            )
            .for_update()
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(connection)
            .await
            .optional()?
        else {
            continue;
        };
        let lease_token = row.lease_token.clone().ok_or_else(|| {
            DurableError::InvalidState(format!("running activity {} has no lease token", row.id))
        })?;
        let exhausted = row.attempt_count >= row.max_attempts;
        let available_at = if exhausted {
            now
        } else {
            let retry_policy: RetryPolicy = serde_json::from_str(&row.retry_policy_json)?;
            let attempt = u32::try_from(row.attempt_count)
                .map_err(|_| DurableError::InvalidState("negative activity attempt".to_string()))?;
            let jitter_percentile = crate::deterministic_jitter_percentile(format!(
                "activity:{}:lease_recovery:{}",
                row.id, attempt
            ));
            now.saturating_add(duration_millis(
                retry_policy.delay_for_attempt(attempt, jitter_percentile)?,
            )?)
        };
        let changed = diesel::update(
            durable_activity::table
                .find(row.id)
                .filter(durable_activity::status.eq(ActivityStatus::Running))
                .filter(durable_activity::attempt_count.eq(row.attempt_count))
                .filter(durable_activity::lease_token.eq(Some(lease_token.clone()))),
        )
        .set((
            durable_activity::status.eq(if exhausted {
                ActivityStatus::DeadLettered
            } else {
                ActivityStatus::Pending
            }),
            durable_activity::available_at.eq(available_at),
            durable_activity::last_error_category.eq(Some("lease_expired".to_string())),
            durable_activity::last_error_message.eq(Some("activity lease expired".to_string())),
            durable_activity::lease_owner.eq(None::<String>),
            durable_activity::lease_token.eq(None::<String>),
            durable_activity::lease_expires_at.eq(None::<i64>),
            durable_activity::updated_at.eq(now),
            durable_activity::completed_at.eq(exhausted.then_some(now)),
        ))
        .execute(connection)
        .await?;
        ensure_fenced(changed)?;
        let changed = diesel::update(
            durable_activity_attempt::table
                .find((row.id, row.attempt_count))
                .filter(durable_activity_attempt::lease_token.eq(lease_token))
                .filter(durable_activity_attempt::finished_at.is_null()),
        )
        .set((
            durable_activity_attempt::finished_at.eq(Some(now)),
            durable_activity_attempt::outcome.eq(Some("lease_expired".to_string())),
            durable_activity_attempt::error_category.eq(Some("lease_expired".to_string())),
            durable_activity_attempt::error_message.eq(Some("activity lease expired".to_string())),
        ))
        .execute(connection)
        .await?;
        ensure_fenced(changed)?;
        append_activity_history(
            connection,
            &row,
            "activity_lease_expired",
            Some("activity lease expired".to_string()),
            now,
        )
        .await?;
        let blocks = exhausted
            && workflow.0 == WorkflowStatus::WaitingActivity
            && workflow.1 == Some(row.id);
        if blocks {
            block_workflow(
                connection,
                &row,
                "lease_expired",
                "activity lease expired",
                now,
            )
            .await?;
            crate::trace::touch_wf(row.workflow_id);
        }
        crate::trace::touch_act(row.id);
        crate::trace::touch_att(row.id, row.attempt_count);
        if crate::trace::ENABLED {
            reconciled.push(serde_json::json!({
                "activity_id": row.id,
                "exhausted": exhausted,
                "workflow_blocked": blocks,
                "available_at": available_at,
            }));
        }
    }
    Ok(reconciled)
}

async fn append_activity_history(
    connection: &mut crate::DurableConnection,
    row: &ActivityRow,
    event_type: &str,
    reason: Option<String>,
    now: i64,
) -> Result<(), DurableError> {
    let workflow_id = WorkflowId::new(row.workflow_id)?;
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: row.workflow_id,
            sequence,
            delivery_sequence: None,
            event_type: event_type.to_string(),
            metadata_json: None,
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason,
            created_at: now,
        },
    )
    .await
}

async fn finish_on_connection(
    connection: &mut crate::DurableConnection,
    claim: &ActivityClaim,
    outcome: ExecutionOutcome,
) -> Result<(), DurableError> {
    durable_workflow::table
        .find(claim.row.workflow_id)
        .for_update()
        .select(durable_workflow::id)
        .first::<i64>(connection)
        .await?;
    let now = persistence::database_now_millis(connection).await?;
    match outcome {
        ExecutionOutcome::Succeeded(output) => {
            let changed = diesel::update(fenced_activity!(claim))
                .set((
                    durable_activity::status.eq(ActivityStatus::Succeeded),
                    durable_activity::provider_result_json.eq(Some(output.clone())),
                    durable_activity::last_error_category.eq(None::<String>),
                    durable_activity::last_error_message.eq(None::<String>),
                    durable_activity::lease_owner.eq(None::<String>),
                    durable_activity::lease_token.eq(None::<String>),
                    durable_activity::lease_expires_at.eq(None::<i64>),
                    durable_activity::updated_at.eq(now),
                    durable_activity::completed_at.eq(Some(now)),
                ))
                .execute(connection)
                .await?;
            ensure_fenced(changed)?;
            finish_attempt(
                connection,
                claim,
                "succeeded",
                None,
                None,
                Some(output.clone()),
                now,
            )
            .await?;
            declare_finish(claim, "succeeded", None);
            wake_workflow(connection, claim, output, now).await
        }
        ExecutionOutcome::Retryable(error) => {
            let (category, message) = activity_error_parts(error);
            if claim.attempt_number >= claim.row.max_attempts {
                declare_finish(claim, "retryable", None);
                dead_letter(connection, claim, &category, &message, now).await
            } else {
                let retry_policy: RetryPolicy = serde_json::from_str(&claim.row.retry_policy_json)?;
                let attempt = u32::try_from(claim.attempt_number).map_err(|_| {
                    DurableError::InvalidState("negative activity attempt".to_string())
                })?;
                let jitter_percentile = crate::deterministic_jitter_percentile(format!(
                    "activity:{}:retry:{}",
                    claim.row.id, attempt
                ));
                let delay = retry_policy.delay_for_attempt(attempt, jitter_percentile)?;
                let available_at = now.saturating_add(duration_millis(delay)?);
                let changed = diesel::update(fenced_activity!(claim))
                    .set((
                        durable_activity::status.eq(ActivityStatus::Pending),
                        durable_activity::available_at.eq(available_at),
                        durable_activity::last_error_category.eq(Some(category.clone())),
                        durable_activity::last_error_message.eq(Some(message.clone())),
                        durable_activity::lease_owner.eq(None::<String>),
                        durable_activity::lease_token.eq(None::<String>),
                        durable_activity::lease_expires_at.eq(None::<i64>),
                        durable_activity::updated_at.eq(now),
                    ))
                    .execute(connection)
                    .await?;
                ensure_fenced(changed)?;
                declare_finish(claim, "retryable", Some(available_at));
                finish_attempt(
                    connection,
                    claim,
                    "retryable_failure",
                    Some(category),
                    Some(message.clone()),
                    None,
                    now,
                )
                .await?;
                append_activity_history(
                    connection,
                    &claim.row,
                    "activity_retry_scheduled",
                    Some(message),
                    now,
                )
                .await
            }
        }
        ExecutionOutcome::Permanent(error) => {
            let (category, message) = activity_error_parts(error);
            declare_finish(claim, "permanent", None);
            dead_letter(connection, claim, &category, &message, now).await
        }
    }
}

async fn dead_letter(
    connection: &mut crate::DurableConnection,
    claim: &ActivityClaim,
    category: &str,
    message: &str,
    now: i64,
) -> Result<(), DurableError> {
    let changed = diesel::update(fenced_activity!(claim))
        .set((
            durable_activity::status.eq(ActivityStatus::DeadLettered),
            durable_activity::last_error_category.eq(Some(category.to_string())),
            durable_activity::last_error_message.eq(Some(message.to_string())),
            durable_activity::lease_owner.eq(None::<String>),
            durable_activity::lease_token.eq(None::<String>),
            durable_activity::lease_expires_at.eq(None::<i64>),
            durable_activity::updated_at.eq(now),
            durable_activity::completed_at.eq(Some(now)),
        ))
        .execute(connection)
        .await?;
    ensure_fenced(changed)?;
    finish_attempt(
        connection,
        claim,
        "dead_lettered",
        Some(category.to_string()),
        Some(message.to_string()),
        None,
        now,
    )
    .await?;
    block_workflow(connection, &claim.row, category, message, now).await
}

async fn finish_attempt(
    connection: &mut crate::DurableConnection,
    claim: &ActivityClaim,
    outcome: &str,
    category: Option<String>,
    message: Option<String>,
    provider_result: Option<String>,
    now: i64,
) -> Result<(), DurableError> {
    let changed = diesel::update(
        durable_activity_attempt::table
            .find((claim.row.id, claim.attempt_number))
            .filter(durable_activity_attempt::lease_token.eq(&claim.lease_token))
            .filter(durable_activity_attempt::finished_at.is_null()),
    )
    .set((
        durable_activity_attempt::finished_at.eq(Some(now)),
        durable_activity_attempt::outcome.eq(Some(outcome.to_string())),
        durable_activity_attempt::error_category.eq(category),
        durable_activity_attempt::error_message.eq(message),
        durable_activity_attempt::provider_result_json.eq(provider_result),
    ))
    .execute(connection)
    .await?;
    ensure_fenced(changed)
}

async fn wake_workflow(
    connection: &mut crate::DurableConnection,
    claim: &ActivityClaim,
    output: String,
    now: i64,
) -> Result<(), DurableError> {
    let workflow = durable_workflow::table
        .find(claim.row.workflow_id)
        .for_update()
        .select((
            durable_workflow::status,
            durable_workflow::wait_reference_id,
            durable_workflow::delivered_event_sequence,
        ))
        .first::<(WorkflowStatus, Option<i64>, i32)>(connection)
        .await?;
    if workflow.0 != WorkflowStatus::WaitingActivity || workflow.1 != Some(claim.row.id) {
        return Err(DurableError::FencedWrite);
    }
    let workflow_id = WorkflowId::new(claim.row.workflow_id)?;
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    let event = WorkflowEvent::ActivitySucceeded {
        command_sequence: u32::try_from(claim.row.command_sequence).map_err(|_| {
            DurableError::InvalidState("negative activity command sequence".to_string())
        })?,
        result: ActivityResult::new(claim.row.kind.clone(), claim.row.version, output)?,
    };
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: claim.row.workflow_id,
            sequence,
            delivery_sequence: Some(workflow.2.saturating_add(1)),
            event_type: "activity_succeeded".to_string(),
            metadata_json: Some(serde_json::to_string(&event)?),
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason: None,
            created_at: now,
        },
    )
    .await?;
    let changed = diesel::update(
        durable_workflow::table
            .find(claim.row.workflow_id)
            .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
            .filter(durable_workflow::wait_reference_id.eq(Some(claim.row.id))),
    )
    .set((
        durable_workflow::status.eq(WorkflowStatus::Ready),
        durable_workflow::wait_kind.eq(None::<String>),
        durable_workflow::wait_reference_id.eq(None::<i64>),
        durable_workflow::available_at.eq(now),
        durable_workflow::updated_at.eq(now),
    ))
    .execute(connection)
    .await?;
    ensure_fenced(changed)
}

async fn block_workflow(
    connection: &mut crate::DurableConnection,
    row: &ActivityRow,
    category: &str,
    message: &str,
    now: i64,
) -> Result<(), DurableError> {
    let changed = diesel::update(
        durable_workflow::table
            .find(row.workflow_id)
            .filter(durable_workflow::status.eq(WorkflowStatus::WaitingActivity))
            .filter(durable_workflow::wait_reference_id.eq(Some(row.id))),
    )
    .set((
        durable_workflow::status.eq(WorkflowStatus::Blocked),
        durable_workflow::error_category.eq(Some(category.to_string())),
        durable_workflow::error_message.eq(Some(message.to_string())),
        durable_workflow::updated_at.eq(now),
    ))
    .execute(connection)
    .await?;
    ensure_fenced(changed)?;
    let workflow_id = WorkflowId::new(row.workflow_id)?;
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: row.workflow_id,
            sequence,
            delivery_sequence: None,
            event_type: "activity_dead_lettered".to_string(),
            metadata_json: None,
            actor_type: Some("system".to_string()),
            actor_id: None,
            reason: Some(message.to_string()),
            created_at: now,
        },
    )
    .await
}

fn activity_error_parts(error: ActivityError) -> (String, String) {
    match error {
        ActivityError::Retryable { category, message }
        | ActivityError::Permanent { category, message } => (category, message),
    }
}

fn duration_millis(duration: Duration) -> Result<i64, DurableError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        DurableError::InvalidDefinition("duration exceeds the database range".to_string())
    })
}

fn duration_from_millis(millis: i64) -> Result<Duration, DurableError> {
    let millis = u64::try_from(millis).map_err(|_| {
        DurableError::InvalidState("stored duration cannot be negative".to_string())
    })?;
    Ok(Duration::from_millis(millis))
}

fn topic_value(topic: &str, value: serde_json::Value) -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::from_iter([(topic.to_string(), value)]))
}

/// Declares `TW1_Claim` when the transaction reconciled or claimed a row.
fn declare_activity_claims(
    topics: &[String],
    reconciled: &[serde_json::Value],
    claims: &[ActivityClaim],
) {
    if claims.is_empty() && reconciled.is_empty() {
        return;
    }
    crate::trace::declare(|| {
        crate::trace::Action::new(
            "TW1_Claim",
            serde_json::json!({
                "topics": topics,
                "reconciled": reconciled,
                "claimed": claims
                    .iter()
                    .map(|claim| serde_json::json!({
                        "activity_id": claim.row.id,
                        "attempt": claim.attempt_number,
                        "token": claim.lease_token,
                        "lease_expires_at": claim.row.lease_expires_at,
                        "workflow_id": claim.row.workflow_id,
                    }))
                    .collect::<Vec<_>>(),
            }),
        )
    });
}

/// `TW1_Error`: `claim_locked_candidate` aborts the whole T-W1 (G10).
fn declare_claim_error(reason: &'static str, activity_id: i64) {
    crate::trace::declare_rollback(|| {
        crate::trace::Action::new(
            "TW1_Error",
            serde_json::json!({ "reason": reason, "activity_id": activity_id }),
        )
    });
}

/// `TW3_Finish` with the handler outcome; `available_at` for a retry.
fn declare_finish(claim: &ActivityClaim, outcome: &'static str, available_at: Option<i64>) {
    crate::trace::touch_act(claim.row.id);
    crate::trace::touch_att(claim.row.id, claim.attempt_number);
    crate::trace::touch_wf(claim.row.workflow_id);
    crate::trace::declare(|| {
        crate::trace::Action::new(
            "TW3_Finish",
            serde_json::json!({
                "activity_id": claim.row.id,
                "attempt": claim.attempt_number,
                "max_attempts": claim.row.max_attempts,
                "token": claim.lease_token,
                "outcome": outcome,
                "available_at": available_at,
                "workflow_id": claim.row.workflow_id,
            }),
        )
    });
}

/// A local step of execution `(worker, activity, token)`: `HandlerReturn`,
/// `LocalDeadline`, `TW3_FenceMiss`. The caller holds no pooled
/// connection; a handler polled on the same task may (see `trace::record_local`).
async fn record_execution_step(
    pool: &DurablePool,
    actor: &str,
    claim: &ActivityClaim,
    name: &str,
    extra: Option<serde_json::Value>,
) {
    if !crate::trace::ENABLED {
        return;
    }
    let mut params = serde_json::json!({
        "activity_id": claim.row.id,
        "attempt": claim.attempt_number,
        "token": claim.lease_token,
    });
    if let (Some(serde_json::Value::Object(extra)), serde_json::Value::Object(params)) =
        (extra, &mut params)
    {
        params.extend(extra);
    }
    crate::trace::record_local(pool, actor, crate::trace::Action::new(name, params)).await;
}

/// `TW2_FenceMiss` / `TW2_Drop` of heartbeat `hb`.
async fn record_heartbeat_step(pool: &DurablePool, actor: &str, name: &str, hb: u64) {
    if crate::trace::ENABLED {
        crate::trace::record_local(
            pool,
            actor,
            crate::trace::Action::new(name, serde_json::json!({ "hb": hb })),
        )
        .await;
    }
}

fn ensure_fenced(changed: usize) -> Result<(), DurableError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}
