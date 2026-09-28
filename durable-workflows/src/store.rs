use crate::tx::Tx;
use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper, TextExpressionMethods};
use diesel_async::RunQueryDsl;

use crate::{
    error::ensure_size,
    persistence::{
        self, ActivityRow, ActivityStatus, ApprovalRow, ApprovalStatus, AttemptOutcome,
        NewWorkflowEventRow, NewWorkflowRow, Wait, WaitKind, WorkflowRow, WorkflowStatus,
    },
    schema::{durable_activity, durable_activity_attempt, durable_approval, durable_workflow},
    tx::{self, Locked, TxScope},
    DurableConnection, DurableError, DurablePool, ScheduleRunId, WorkflowHandler, WorkflowId,
    MAX_INPUT_STATE_PAYLOAD_BYTES,
};

const DEFAULT_MAX_ACTIVATION_ATTEMPTS: i32 = 8;
const MAX_DEDUPLICATION_KEY_CHARS: usize = 191;

#[derive(Debug, Clone, Default)]
pub struct StartOptions {
    pub available_at: Option<DateTime<Utc>>,
    pub schedule_run_id: Option<ScheduleRunId>,
    lineage: StartLineage,
}

/// What identifies a start besides its id: the caller's deduplication key or
/// the engine's restart lineage. One enum, so a start with both a key and a
/// restart source has no value (it was a runtime check).
#[derive(Debug, Clone, Default)]
enum StartLineage {
    #[default]
    Fresh,
    Deduplicated(String),
    /// Set only by the engine (T-X2 recoverable start and the admin
    /// restart), which checks that the source may be restarted.
    Restart {
        root: WorkflowId,
        from: WorkflowId,
    },
}

impl StartOptions {
    pub fn with_deduplication_key(mut self, key: impl Into<String>) -> Self {
        self.lineage = StartLineage::Deduplicated(key.into());
        self
    }

    /// The deduplication key [`with_deduplication_key`](Self::with_deduplication_key) set.
    pub fn deduplication_key(&self) -> Option<&str> {
        match &self.lineage {
            StartLineage::Deduplicated(key) => Some(key),
            StartLineage::Fresh | StartLineage::Restart { .. } => None,
        }
    }

    /// Makes this the start of a restart successor of `from` in the chain
    /// rooted at `root`, replacing any deduplication key.
    pub(crate) fn restarted(mut self, root: WorkflowId, from: WorkflowId) -> Self {
        self.lineage = StartLineage::Restart { root, from };
        self
    }

    fn root_workflow_id(&self) -> Option<WorkflowId> {
        match self.lineage {
            StartLineage::Restart { root, .. } => Some(root),
            StartLineage::Fresh | StartLineage::Deduplicated(_) => None,
        }
    }

    fn restarted_from_workflow_id(&self) -> Option<WorkflowId> {
        match self.lineage {
            StartLineage::Restart { from, .. } => Some(from),
            StartLineage::Fresh | StartLineage::Deduplicated(_) => None,
        }
    }

    /// Starts the workflow no earlier than `available_at`.
    pub fn with_available_at(mut self, available_at: DateTime<Utc>) -> Self {
        self.available_at = Some(available_at);
        self
    }

    /// Links the workflow to the schedule run that starts it
    /// (`ScheduleHandler::start_occurrence`).
    pub fn with_schedule_run_id(mut self, schedule_run_id: ScheduleRunId) -> Self {
        self.schedule_run_id = Some(schedule_run_id);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartOutcome {
    pub workflow_id: WorkflowId,
    pub inserted: bool,
}

/// Application API for starting, finding and cancelling workflows.
///
/// # `*_with_conn` methods
///
/// Each `*_with_conn` method runs inside the caller's transaction, at the
/// caller's isolation level, and the caller commits. It is correct under
/// READ COMMITTED on both backends and under REPEATABLE READ on MySQL. On
/// Postgres under REPEATABLE READ or SERIALIZABLE it can fail with a
/// serialization error or [`DurableError::Conflict`]; the caller then retries
/// the whole transaction. The methods without `_with_conn` open their own
/// transaction, pinned to READ COMMITTED.
#[derive(Clone)]
pub struct DurableStore {
    pool: DurablePool,
}

impl DurableStore {
    pub fn new(pool: DurablePool) -> Self {
        Self { pool }
    }

    /// Cancels application-owned work atomically with the caller's state changes.
    /// Repeated cancellation and cancellation of terminal workflows are no-ops.
    ///
    /// The cancel reaches the child workflows this workflow started without a
    /// key ([`WfCtx::child`](crate::WfCtx::child)), every recovery generation of
    /// them, and their children in turn; children started
    /// with [`WfCtx::child_with_key`](crate::WfCtx::child_with_key) keep running.
    /// A child that finishes at the same moment can deadlock with the cascade;
    /// retry the transaction when the error [`is_transient`](DurableError::is_transient).
    ///
    /// Runs in the caller's transaction; see the `*_with_conn` contract on
    /// [`DurableStore`].
    pub async fn cancel_with_conn(
        connection: &mut DurableConnection,
        workflow_id: WorkflowId,
        reason: &str,
    ) -> Result<(), DurableError> {
        let reason = reason.trim();
        if reason.is_empty() || reason.len() > crate::MAX_ERROR_REASON_BYTES {
            return Err(DurableError::InvalidDefinition(format!(
                "cancellation reason must contain 1 to {} bytes",
                crate::MAX_ERROR_REASON_BYTES
            )));
        }
        crate::tx::caller_transaction(connection, async move |Tx { connection, scope }| {
            let workflow = tx::lock_optional(
                connection,
                scope,
                durable_workflow::table
                    .find(workflow_id.get())
                    .for_update()
                    .select(WorkflowRow::as_select()),
            )
            .await?
            .ok_or_else(|| DurableError::NotFound {
                resource: "workflow",
                identifier: workflow_id.to_string(),
            })?;
            if workflow.status.is_terminal() {
                return Ok(());
            }
            let now = persistence::database_now_millis(connection).await?;
            crate::trace::declare(|| {
                crate::trace::Action::new(
                    "TX3_Cancel",
                    serde_json::json!({ "workflow_id": workflow.id }),
                )
            });
            cancel_locked_workflow(connection, workflow.as_ref(), reason, None, now).await
        })
        .await
    }

    pub async fn start<W>(
        &self,
        workflow: &W,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError>
    where
        W: WorkflowHandler,
    {
        let (result, rolled_back) = crate::trace::capture_rollback(async {
            let mut connection = self.pool.get().await?;
            // The inner transaction becomes a savepoint inside this pinned one.
            crate::dialect::transaction(&mut connection, async move |Tx { connection, .. }| {
                Self::start_with_conn(connection, workflow, options).await
            })
            .await
        })
        .await;
        if let Some(action) = rolled_back {
            crate::trace::record_local(&self.pool, "app", action).await;
        }
        result
    }

    /// Runs in the caller's transaction; see the `*_with_conn` contract on
    /// [`DurableStore`].
    pub async fn start_with_conn<W>(
        connection: &mut DurableConnection,
        workflow: &W,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError>
    where
        W: WorkflowHandler,
    {
        validate_definition::<W>()?;
        validate_options(&options)?;

        let input_json = serde_json::to_string(workflow)?;
        ensure_size("workflow input", &input_json, MAX_INPUT_STATE_PAYLOAD_BYTES)?;
        let state_json = serde_json::to_string(&workflow.initial_state())?;
        ensure_size("workflow state", &state_json, MAX_INPUT_STATE_PAYLOAD_BYTES)?;

        crate::tx::caller_transaction(
            connection,
            async move |Tx {
                            connection: transaction,
                            scope,
                        }| {
                Self::insert_prepared(
                    transaction,
                    scope,
                    W::KIND,
                    W::VERSION,
                    input_json,
                    state_json,
                    options,
                )
                .await
            },
        )
        .await
    }

    /// Finds a previously accepted workflow using the workflow definition's
    /// kind and a caller-owned deduplication key.
    ///
    /// Runs in the caller's transaction; see the `*_with_conn` contract on
    /// [`DurableStore`].
    pub async fn find_by_deduplication_key_with_conn<W>(
        connection: &mut DurableConnection,
        deduplication_key: &str,
    ) -> Result<Option<WorkflowId>, DurableError>
    where
        W: WorkflowHandler,
    {
        validate_definition::<W>()?;
        validate_options(&StartOptions::default().with_deduplication_key(deduplication_key))?;
        let Some(existing) =
            persistence::find_by_deduplication_key(connection, W::KIND, deduplication_key).await?
        else {
            return Ok(None);
        };
        Ok(Some(WorkflowId::new(existing.id)?))
    }

    pub async fn start_or_restart_recoverable<W>(
        &self,
        workflow: &W,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError>
    where
        W: WorkflowHandler,
    {
        let (result, rolled_back) = crate::trace::capture_rollback(async {
            let mut connection = self.pool.get().await?;
            // The inner transaction becomes a savepoint inside this pinned one.
            crate::dialect::transaction(&mut connection, async move |Tx { connection, .. }| {
                Self::start_or_restart_recoverable_with_conn(connection, workflow, options).await
            })
            .await
        })
        .await;
        if let Some(action) = rolled_back {
            crate::trace::record_local(&self.pool, "app", action).await;
        }
        result
    }

    /// Runs in the caller's transaction; see the `*_with_conn` contract on
    /// [`DurableStore`].
    pub async fn start_or_restart_recoverable_with_conn<W>(
        connection: &mut DurableConnection,
        workflow: &W,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError>
    where
        W: WorkflowHandler,
    {
        validate_definition::<W>()?;
        validate_options(&options)?;

        let input_json = serde_json::to_string(workflow)?;
        ensure_size("workflow input", &input_json, MAX_INPUT_STATE_PAYLOAD_BYTES)?;
        let state_json = serde_json::to_string(&workflow.initial_state())?;
        ensure_size("workflow state", &state_json, MAX_INPUT_STATE_PAYLOAD_BYTES)?;

        crate::tx::caller_transaction(
            connection,
            async move |Tx {
                            connection: transaction,
                            scope,
                        }| {
                let Some(key) = options.deduplication_key().map(str::to_owned) else {
                    return Self::insert_prepared(
                        transaction,
                        scope,
                        W::KIND,
                        W::VERSION,
                        input_json,
                        state_json,
                        options,
                    )
                    .await;
                };
                let original = tx::lock_optional(
                    transaction,
                    scope,
                    durable_workflow::table
                        .filter(durable_workflow::kind.eq(W::KIND))
                        .filter(durable_workflow::deduplication_key.eq(&key))
                        .for_update()
                        .select(WorkflowRow::as_select()),
                )
                .await?;
                let Some(original) = original else {
                    let outcome = Self::insert_prepared_untraced(
                        transaction,
                        scope,
                        W::KIND,
                        W::VERSION,
                        input_json,
                        state_json,
                        options,
                    )
                    .await?;
                    declare_recoverable_start(
                        W::KIND,
                        W::VERSION,
                        &key,
                        None,
                        false,
                        outcome.workflow_id.get(),
                        outcome.inserted,
                    );
                    return Ok(outcome);
                };
                let root_id = original.root_workflow_id.unwrap_or(original.id);
                let original_id = original.id;
                let latest = lock_newest_generation(transaction, original).await?;
                if !latest.status.is_start_recoverable() {
                    declare_recoverable_start(
                        W::KIND,
                        W::VERSION,
                        &key,
                        Some((original_id, latest.id)),
                        false,
                        latest.id,
                        false,
                    );
                    return Ok(StartOutcome {
                        workflow_id: WorkflowId::new(latest.id)?,
                        inserted: false,
                    });
                }

                // The successor goes in first: a restart-key `Conflict` then
                // rolls back before anything else is written, and a blocked
                // row's waiting parents can be re-pointed at it.
                let lineage = Some((original_id, latest.id));
                let outcome = Self::insert_prepared_untraced(
                    transaction,
                    scope,
                    W::KIND,
                    W::VERSION,
                    input_json,
                    state_json,
                    options.restarted(WorkflowId::new(root_id)?, WorkflowId::new(latest.id)?),
                )
                .await;
                let successor = match outcome {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        // The restart key already has a successor: everything rolls back.
                        if matches!(error, DurableError::Conflict(_)) {
                            crate::trace::declare_rollback(|| {
                                recoverable_start_action(
                                    W::KIND,
                                    W::VERSION,
                                    &key,
                                    lineage,
                                    false,
                                    0,
                                    false,
                                )
                            });
                        }
                        return Err(error);
                    }
                };

                let now = persistence::database_now_millis(transaction).await?;
                if crate::trace::ENABLED {
                    for id in durable_activity::table
                        .filter(durable_activity::workflow_id.eq(latest.id))
                        .filter(durable_activity::status.eq(ActivityStatus::DeadLettered))
                        .select(durable_activity::id)
                        .load::<i64>(transaction)
                        .await?
                    {
                        crate::trace::touch_act(id);
                    }
                    crate::trace::touch_wf(latest.id);
                }
                diesel::update(
                    durable_activity::table
                        .filter(durable_activity::workflow_id.eq(latest.id))
                        .filter(durable_activity::status.eq(ActivityStatus::DeadLettered)),
                )
                .set((
                    durable_activity::status.eq(ActivityStatus::Cancelled),
                    persistence::LeaseCleared::new(),
                    durable_activity::updated_at.eq(now),
                    durable_activity::completed_at.eq(Some(now)),
                ))
                .execute(transaction)
                .await?;
                if latest.status == WorkflowStatus::Blocked {
                    let changed = diesel::update(
                        durable_workflow::table
                            .find(latest.id)
                            .filter(durable_workflow::status.eq(WorkflowStatus::Blocked)),
                    )
                    .set((
                        durable_workflow::status.eq(WorkflowStatus::Cancelled),
                        durable_workflow::lease_owner.eq(None::<String>),
                        durable_workflow::lease_token.eq(None::<String>),
                        durable_workflow::lease_expires_at.eq(None::<i64>),
                        durable_workflow::updated_at.eq(now),
                        durable_workflow::completed_at.eq(Some(now)),
                    ))
                    .execute(transaction)
                    .await?;
                    if changed != 1 {
                        return Err(DurableError::FencedWrite);
                    }
                    let sequence =
                        persistence::next_event_sequence(transaction, WorkflowId::new(latest.id)?)
                            .await?;
                    persistence::append_event(
                        transaction,
                        NewWorkflowEventRow {
                            workflow_id: latest.id,
                            sequence,
                            delivery_sequence: None,
                            event_type: "workflow_superseded_by_recovery".to_string(),
                            metadata_json: None,
                            actor_type: Some("system".to_string()),
                            actor_id: None,
                            reason: Some("a successor recovery generation was started".to_string()),
                            created_at: now,
                        },
                    )
                    .await?;
                    hand_waiting_parents_to_successor(
                        transaction,
                        latest.as_ref(),
                        successor.workflow_id,
                        W::VERSION,
                        now,
                    )
                    .await?;
                }

                declare_recoverable_start(
                    W::KIND,
                    W::VERSION,
                    &key,
                    lineage,
                    true,
                    successor.workflow_id.get(),
                    successor.inserted,
                );
                Ok(successor)
            },
        )
        .await
    }

    /// Runs in the caller's transaction; see the `*_with_conn` contract on
    /// [`DurableStore`].
    pub async fn start_prepared_with_conn(
        connection: &mut DurableConnection,
        prepared: crate::PreparedWorkflowStart,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError> {
        validate_options(&options)?;
        ensure_size(
            "workflow input",
            prepared.input_json(),
            MAX_INPUT_STATE_PAYLOAD_BYTES,
        )?;
        ensure_size(
            "workflow state",
            prepared.state_json(),
            MAX_INPUT_STATE_PAYLOAD_BYTES,
        )?;
        crate::tx::caller_transaction(
            connection,
            async move |Tx {
                            connection: transaction,
                            scope,
                        }| {
                Self::insert_prepared(
                    transaction,
                    scope,
                    prepared.kind(),
                    prepared.version(),
                    prepared.input_json().to_string(),
                    prepared.state_json().to_string(),
                    options,
                )
                .await
            },
        )
        .await
    }

    /// T-X1: `insert_prepared_untraced` declared as `TX1_Start`.
    async fn insert_prepared<'tx>(
        connection: &mut DurableConnection,
        scope: TxScope<'tx>,
        kind: &str,
        version: i32,
        input_json: String,
        state_json: String,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError> {
        let key = options.deduplication_key().map(str::to_owned);
        let from = options.restarted_from_workflow_id().map(WorkflowId::get);
        let outcome = Self::insert_prepared_untraced(
            connection, scope, kind, version, input_json, state_json, options,
        )
        .await;
        match &outcome {
            Ok(outcome) => declare_start(
                kind,
                version,
                key.as_deref(),
                from,
                outcome.workflow_id.get(),
                outcome.inserted,
            ),
            // Only the restart key can collide without a deduplication key.
            Err(DurableError::Conflict(_)) if key.is_none() && from.is_some() => {
                crate::trace::declare_rollback(|| {
                    start_action(kind, version, None, from, 0, false)
                });
            }
            Err(_) => {}
        }
        outcome
    }

    async fn insert_prepared_untraced<'tx>(
        connection: &mut DurableConnection,
        scope: TxScope<'tx>,
        kind: &str,
        version: i32,
        input_json: String,
        state_json: String,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError> {
        if let Some(key) = options.deduplication_key() {
            if let Some(existing) =
                persistence::find_by_deduplication_key(connection, kind, key).await?
            {
                return Ok(StartOutcome {
                    workflow_id: WorkflowId::new(existing.id)?,
                    inserted: false,
                });
            }
        }

        let now = persistence::database_now_millis(connection).await?;
        let row = NewWorkflowRow {
            kind: kind.to_string(),
            version,
            input_json,
            state_json,
            state_version: 1,
            status: persistence::WorkflowStatus::Ready,
            result_json: None,
            error_category: None,
            error_message: None,
            wait_kind: None,
            wait_reference_id: None,
            available_at: options
                .available_at
                .map_or(now, |available_at| available_at.timestamp_millis()),
            activation_attempts: 0,
            max_activation_attempts: DEFAULT_MAX_ACTIVATION_ATTEMPTS,
            consecutive_continuations: 0,
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
            deduplication_key: options.deduplication_key().map(str::to_owned),
            schedule_run_id: options.schedule_run_id.map(ScheduleRunId::get),
            root_workflow_id: options.root_workflow_id().map(WorkflowId::get),
            restarted_from_workflow_id: options.restarted_from_workflow_id().map(WorkflowId::get),
            parent_workflow_id: None,
            parent_command_sequence: None,
            command_sequence: 0,
            delivered_event_sequence: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };

        let (id, inserted) = match persistence::insert_started(connection, scope, row).await? {
            persistence::StartedInsert::Inserted(id) => (id, true),
            persistence::StartedInsert::Existing(existing) => (existing.id, false),
        };
        Ok(StartOutcome {
            workflow_id: WorkflowId::new(id)?,
            inserted,
        })
    }

    /// Inserts a child workflow inside the parent's commit transaction.
    ///
    /// The caller supplies the resolved deduplication key so a re-committed
    /// parent transition converges on one child instance. A key that already
    /// exists, whether seen by the pre-read or by the insert's conflict,
    /// resolves to its newest generation (D4), whose version must match (G6).
    pub(crate) async fn insert_child<'tx>(
        connection: &mut DurableConnection,
        scope: TxScope<'tx>,
        child: &crate::ChildWorkflowCommand,
        deduplication_key: String,
        parent_workflow_id: i64,
        parent_command_sequence: i32,
        root_workflow_id: i64,
    ) -> Result<ChildStart<'tx>, DurableError> {
        let key_length = deduplication_key.chars().count();
        if key_length == 0 || key_length > MAX_DEDUPLICATION_KEY_CHARS {
            return Err(DurableError::InvalidDefinition(format!(
                "child workflow deduplication key must contain 1 to {MAX_DEDUPLICATION_KEY_CHARS} characters"
            )));
        }
        // Locking the keyed row first waits out a T-X2 that is superseding it,
        // so the walk below sees that T-X2's successor.
        let keyed = match persistence::find_by_deduplication_key(
            connection,
            child.kind(),
            &deduplication_key,
        )
        .await?
        {
            Some(keyed) => {
                persistence::lock_workflow_by_id(connection, scope, WorkflowId::new(keyed.id)?)
                    .await?
            }
            None => match Self::insert_new_child(
                connection,
                scope,
                child,
                deduplication_key,
                parent_workflow_id,
                parent_command_sequence,
                root_workflow_id,
            )
            .await?
            {
                persistence::StartedInsert::Inserted(id) => {
                    return Ok(ChildStart::Inserted(WorkflowId::new(id)?));
                }
                // A concurrent commit inserted the key after the pre-read (G6).
                persistence::StartedInsert::Existing(keyed) => *keyed,
            },
        };
        let existing = lock_newest_generation(connection, keyed).await?;
        if existing.version != child.version() {
            return Err(DurableError::DefinitionMismatch {
                actual_kind: existing.kind.clone(),
                actual_version: existing.version,
                expected_kind: child.kind().to_string(),
                expected_version: child.version(),
            });
        }
        WorkflowId::new(existing.id)?;
        Ok(ChildStart::Existing(Box::new(existing)))
    }

    async fn insert_new_child<'tx>(
        connection: &mut DurableConnection,
        scope: TxScope<'tx>,
        child: &crate::ChildWorkflowCommand,
        deduplication_key: String,
        parent_workflow_id: i64,
        parent_command_sequence: i32,
        root_workflow_id: i64,
    ) -> Result<persistence::StartedInsert<'tx>, DurableError> {
        let now = persistence::database_now_millis(connection).await?;
        let row = NewWorkflowRow {
            kind: child.kind().to_string(),
            version: child.version(),
            input_json: child.input_json().to_string(),
            state_json: child.state_json().to_string(),
            state_version: 1,
            status: persistence::WorkflowStatus::Ready,
            result_json: None,
            error_category: None,
            error_message: None,
            wait_kind: None,
            wait_reference_id: None,
            available_at: now,
            activation_attempts: 0,
            max_activation_attempts: DEFAULT_MAX_ACTIVATION_ATTEMPTS,
            consecutive_continuations: 0,
            lease_owner: None,
            lease_token: None,
            lease_expires_at: None,
            deduplication_key: Some(deduplication_key),
            schedule_run_id: None,
            root_workflow_id: Some(root_workflow_id),
            restarted_from_workflow_id: None,
            parent_workflow_id: Some(parent_workflow_id),
            parent_command_sequence: Some(parent_command_sequence),
            command_sequence: 0,
            delivered_event_sequence: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };
        persistence::insert_started(connection, scope, row).await
    }
}

/// Outcome of [`DurableStore::insert_child`]. `Existing` carries the child's
/// lock, taken by `insert_child` before the parent's fenced update (G9): a
/// commit path cannot wait on an existing child it has not locked.
pub(crate) enum ChildStart<'tx> {
    Inserted(WorkflowId),
    Existing(Box<Locked<'tx, WorkflowRow>>),
}

impl ChildStart<'_> {
    pub(crate) fn workflow_id(&self) -> Result<WorkflowId, DurableError> {
        match self {
            Self::Inserted(id) => Ok(*id),
            Self::Existing(row) => WorkflowId::new(row.id),
        }
    }

    pub(crate) fn inserted(&self) -> bool {
        matches!(self, Self::Inserted(_))
    }
}

/// Follows the `restarted_from_workflow_id` chain from `row` (already locked by
/// the caller) and returns its newest generation, locking each successor
/// `FOR UPDATE` on the way. The unique restart key gives each row at most one
/// successor, so the chain is linear.
pub(crate) async fn lock_newest_generation<'tx>(
    connection: &mut DurableConnection,
    row: Locked<'tx, WorkflowRow>,
) -> Result<Locked<'tx, WorkflowRow>, DurableError> {
    let mut current = row;
    while let Some(successor) = lock_successor(connection, current.as_ref()).await? {
        current = successor;
    }
    Ok(current)
}

/// Locks the successor of `row` (locked by the caller), if it has one.
async fn lock_successor<'tx>(
    connection: &mut DurableConnection,
    row: Locked<'tx, &WorkflowRow>,
) -> Result<Option<Locked<'tx, WorkflowRow>>, DurableError> {
    Ok(tx::lock_optional(
        connection,
        row.scope(),
        durable_workflow::table
            .filter(durable_workflow::restarted_from_workflow_id.eq(Some(row.id)))
            .for_update()
            .select(WorkflowRow::as_select()),
    )
    .await?)
}

/// T-X2 supersedes the blocked row `superseded` with `successor`. Parents that
/// wait on it (`waiting_child`, or `paused` with a child wait) wait on the
/// successor instead when it runs the same version; otherwise they receive
/// `child_failed` with category `child_superseded`, as after an operator restart.
async fn hand_waiting_parents_to_successor<'tx>(
    connection: &mut DurableConnection,
    superseded: Locked<'tx, &WorkflowRow>,
    successor: WorkflowId,
    successor_version: i32,
    now: i64,
) -> Result<(), DurableError> {
    if successor_version != superseded.version {
        return persistence::wake_waiting_parents_on_child_terminal(
            connection,
            superseded,
            Err((
                "child_superseded".to_string(),
                format!(
                    "child workflow {} was superseded by recovery generation {} at version {successor_version}",
                    superseded.id,
                    successor.get()
                ),
            )),
            now,
        )
        .await;
    }
    let parents = durable_workflow::table
        .filter(durable_workflow::wait_kind.eq(WaitKind::Child))
        .filter(durable_workflow::wait_reference_id.eq(superseded.id))
        .filter(durable_workflow::status.eq_any(WorkflowStatus::CHILD_WAITERS))
        .for_update()
        .select(WorkflowRow::as_select())
        .load::<WorkflowRow>(connection)
        .await?;
    for parent in parents {
        crate::trace::touch_wf(parent.id);
        let changed = diesel::update(
            durable_workflow::table
                .find(parent.id)
                .filter(durable_workflow::status.eq(&parent.status))
                .filter(durable_workflow::wait_kind.eq(WaitKind::Child))
                .filter(durable_workflow::wait_reference_id.eq(superseded.id)),
        )
        .set((
            persistence::WaitColumns::on(Wait::Child(successor)),
            durable_workflow::updated_at.eq(now),
        ))
        .execute(connection)
        .await?;
        if changed != 1 {
            return Err(DurableError::FencedWrite);
        }
        let parent_id = WorkflowId::new(parent.id)?;
        let sequence = persistence::next_event_sequence(connection, parent_id).await?;
        persistence::append_event(
            connection,
            NewWorkflowEventRow {
                workflow_id: parent.id,
                sequence,
                delivery_sequence: None,
                event_type: "child_wait_reattached".to_string(),
                metadata_json: Some(
                    serde_json::json!({ "from": superseded.id, "to": successor.get() }).to_string(),
                ),
                actor_type: Some("system".to_string()),
                actor_id: None,
                reason: Some(format!(
                    "child workflow {} was superseded by recovery generation {}",
                    superseded.id,
                    successor.get()
                )),
                created_at: now,
            },
        )
        .await?;
    }
    Ok(())
}

fn validate_definition<W: WorkflowHandler>() -> Result<(), DurableError> {
    if W::KIND.is_empty() || W::VERSION <= 0 {
        return Err(DurableError::InvalidDefinition(
            "workflow kind must be non-empty and version must be positive".to_string(),
        ));
    }
    Ok(())
}

fn validate_options(options: &StartOptions) -> Result<(), DurableError> {
    if let Some(key) = options.deduplication_key() {
        let length = key.chars().count();
        if length == 0 || length > MAX_DEDUPLICATION_KEY_CHARS {
            return Err(DurableError::InvalidDefinition(format!(
                "workflow deduplication key must contain 1 to {MAX_DEDUPLICATION_KEY_CHARS} characters"
            )));
        }
    }
    Ok(())
}

/// Cancels `workflow`, which the caller locked in this transaction, and the
/// children it owns (G11, [`cancel_owned_descendants`]).
pub(crate) async fn cancel_locked_workflow<'tx>(
    connection: &mut DurableConnection,
    workflow: Locked<'tx, &WorkflowRow>,
    reason: &str,
    operator_id: Option<&str>,
    now: i64,
) -> Result<(), DurableError> {
    cancel_one_workflow(connection, workflow, reason, operator_id, now).await?;
    cancel_owned_descendants(connection, workflow, reason, operator_id, now).await
}

/// G11: a parent's cancel reaches every generation of its owned children.
/// `parent` was cancelled or superseded in this transaction. A child is owned
/// when it carries the key the engine generated for it,
/// `child:{parent}:{parent_command_sequence}` (`commit_child`, no
/// `child_with_key`); its generations are the rows on its
/// `restarted_from_workflow_id` chain (T-X2 and T-A5 successors, which carry
/// no key and no parent). A child started with a domain key may be shared
/// with other parents and keeps running.
///
/// For each owned child (siblings in id order) the cascade locks the child
/// with `lock_workflow_by_id`, then each successor on its chain, oldest first,
/// and cancels every non-terminal generation like T-X3 with the reason
/// `parent workflow {p} cancelled: {reason}`; each cancelled generation's own
/// owned children follow. The cascade locks parent before descendant, the
/// reverse of the child-terminal order (INVARIANTS §2.8): a child that commits
/// a terminal transition at the same moment can deadlock with it. The
/// database aborts one of the two transactions with an error for which
/// [`DurableError::is_transient`] holds; the aborted side retries. The work
/// list keeps the recursion out of the async call graph.
pub(crate) async fn cancel_owned_descendants<'tx>(
    connection: &mut DurableConnection,
    parent: Locked<'tx, &WorkflowRow>,
    reason: &str,
    operator_id: Option<&str>,
    now: i64,
) -> Result<(), DurableError> {
    let scope = parent.scope();
    let mut parents = vec![(parent.id, reason.to_string())];
    while let Some((parent_id, parent_reason)) = parents.pop() {
        let reason = crate::error::truncate_utf8(
            format!("parent workflow {parent_id} cancelled: {parent_reason}"),
            crate::MAX_ERROR_REASON_BYTES,
        );
        for child_id in owned_children(connection, parent_id).await? {
            let mut generation =
                persistence::lock_workflow_by_id(connection, scope, WorkflowId::new(child_id)?)
                    .await?;
            loop {
                if !generation.status.is_terminal() {
                    cancel_one_workflow(connection, generation.as_ref(), &reason, operator_id, now)
                        .await?;
                    parents.push((generation.id, reason.clone()));
                }
                match lock_successor(connection, generation.as_ref()).await? {
                    Some(successor) => generation = successor,
                    None => break,
                }
            }
        }
    }
    Ok(())
}

/// The children `parent_id` owns (see [`cancel_owned_descendants`]), in id
/// order, terminal ones included: a terminal child's successor may be live. A
/// locking read, so it sees children committed after the caller's snapshot;
/// the `LIKE` keeps it from locking domain-keyed children.
async fn owned_children(
    connection: &mut DurableConnection,
    parent_id: i64,
) -> Result<Vec<i64>, DurableError> {
    let children = durable_workflow::table
        .filter(durable_workflow::parent_workflow_id.eq(Some(parent_id)))
        .filter(durable_workflow::deduplication_key.like(format!("child:{parent_id}:%")))
        .order(durable_workflow::id.asc())
        .for_update()
        .select((
            durable_workflow::id,
            durable_workflow::deduplication_key,
            durable_workflow::parent_command_sequence,
        ))
        .load::<(i64, Option<String>, Option<i32>)>(connection)
        .await?;
    Ok(children
        .into_iter()
        .filter(|(_, key, command)| {
            command.is_some_and(|command| {
                key.as_deref() == Some(format!("child:{parent_id}:{command}").as_str())
            })
        })
        .map(|(id, _, _)| id)
        .collect())
}

/// Cancels `workflow` alone: its activities and approvals, the fenced status
/// update, history, and the wake of its waiting parents.
async fn cancel_one_workflow<'tx>(
    connection: &mut DurableConnection,
    workflow: Locked<'tx, &WorkflowRow>,
    reason: &str,
    operator_id: Option<&str>,
    now: i64,
) -> Result<(), DurableError> {
    let workflow_id = WorkflowId::new(workflow.id)?;
    crate::trace::touch_wf(workflow.id);
    let attempt_outcome = if operator_id.is_some() {
        AttemptOutcome::OperatorCancelled
    } else {
        AttemptOutcome::ApplicationCancelled
    };
    cancel_activities(connection, workflow.id, reason, attempt_outcome, now).await?;
    cancel_approvals(connection, workflow.id, reason, now).await?;
    let changed = diesel::update(
        durable_workflow::table
            .find(workflow.id)
            .filter(durable_workflow::status.eq(&workflow.status)),
    )
    .set((
        durable_workflow::status.eq(WorkflowStatus::Cancelled),
        persistence::WaitColumns::cleared(),
        durable_workflow::lease_owner.eq(None::<String>),
        durable_workflow::lease_token.eq(None::<String>),
        durable_workflow::lease_expires_at.eq(None::<i64>),
        durable_workflow::updated_at.eq(now),
        durable_workflow::completed_at.eq(Some(now)),
    ))
    .execute(connection)
    .await?;
    ensure_cancel_changed(changed)?;
    let sequence = persistence::next_event_sequence(connection, workflow_id).await?;
    persistence::append_event(
        connection,
        NewWorkflowEventRow {
            workflow_id: workflow.id,
            sequence,
            delivery_sequence: None,
            event_type: "workflow_cancelled".to_string(),
            metadata_json: None,
            actor_type: Some(
                if operator_id.is_some() {
                    "operator"
                } else {
                    "system"
                }
                .to_string(),
            ),
            actor_id: operator_id.map(str::to_string),
            reason: Some(reason.to_string()),
            created_at: now,
        },
    )
    .await?;
    persistence::wake_waiting_parents_on_child_terminal(
        connection,
        workflow,
        Err(("child_cancelled".to_string(), reason.to_string())),
        now,
    )
    .await
}

fn ensure_cancel_changed(changed: usize) -> Result<(), DurableError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DurableError::FencedWrite)
    }
}

pub(crate) async fn cancel_activities(
    connection: &mut DurableConnection,
    workflow_id: i64,
    reason: &str,
    attempt_outcome: AttemptOutcome,
    now: i64,
) -> Result<(), DurableError> {
    let activities = durable_activity::table
        .filter(durable_activity::workflow_id.eq(workflow_id))
        .filter(durable_activity::status.eq_any(ActivityStatus::NON_TERMINAL))
        .for_update()
        .select(ActivityRow::as_select())
        .load::<ActivityRow>(connection)
        .await?;
    for activity in activities {
        crate::trace::touch_act(activity.id);
        let changed = match activity.status {
            // The handler may still be executing: the row keeps its lease,
            // topic slot and open attempt until the handler stops or the
            // lease expires (`settle_revoked`, N2).
            ActivityStatus::Running | ActivityStatus::Cancelling => {
                let lease_token = activity.lease_token.as_deref().ok_or_else(|| {
                    DurableError::InvalidState(format!(
                        "{} activity {} has no lease token",
                        activity.status, activity.id
                    ))
                })?;
                diesel::update(
                    durable_activity::table
                        .find(activity.id)
                        .filter(durable_activity::status.eq(activity.status))
                        .filter(durable_activity::attempt_count.eq(activity.attempt_count))
                        .filter(durable_activity::lease_token.eq(lease_token)),
                )
                .set((
                    durable_activity::status.eq(ActivityStatus::Cancelling),
                    durable_activity::last_error_category.eq(Some(attempt_outcome.as_str())),
                    durable_activity::last_error_message.eq(Some(reason.to_string())),
                    durable_activity::updated_at.eq(now),
                ))
                .execute(connection)
                .await?
            }
            ActivityStatus::Pending => {
                diesel::update(
                    durable_activity::table
                        .find(activity.id)
                        .filter(durable_activity::status.eq(ActivityStatus::Pending)),
                )
                .set((
                    durable_activity::status.eq(ActivityStatus::Cancelled),
                    persistence::LeaseCleared::new(),
                    durable_activity::updated_at.eq(now),
                    durable_activity::completed_at.eq(Some(now)),
                ))
                .execute(connection)
                .await?
            }
            ActivityStatus::Succeeded
            | ActivityStatus::DeadLettered
            | ActivityStatus::Cancelled => {
                return Err(DurableError::InvalidState(format!(
                    "cancel loaded activity {} in terminal status {}",
                    activity.id, activity.status
                )));
            }
        };
        ensure_cancel_changed(changed)?;
    }
    Ok(())
}

pub(crate) async fn close_attempt(
    connection: &mut DurableConnection,
    activity: &ActivityRow,
    outcome: AttemptOutcome,
    reason: &str,
    now: i64,
) -> Result<(), DurableError> {
    let lease_token = activity.lease_token.as_deref().ok_or_else(|| {
        DurableError::InvalidState(format!(
            "running activity {} has no lease token",
            activity.id
        ))
    })?;
    crate::trace::touch_att(activity.id, activity.attempt_count);
    let changed = diesel::update(
        durable_activity_attempt::table
            .find((activity.id, activity.attempt_count))
            .filter(durable_activity_attempt::lease_token.eq(lease_token))
            .filter(durable_activity_attempt::finished_at.is_null()),
    )
    .set((
        durable_activity_attempt::finished_at.eq(Some(now)),
        durable_activity_attempt::outcome.eq(Some(outcome)),
        durable_activity_attempt::error_category.eq(Some(outcome.as_str())),
        durable_activity_attempt::error_message.eq(Some(reason.to_string())),
    ))
    .execute(connection)
    .await?;
    ensure_cancel_changed(changed)
}

pub(crate) async fn cancel_approvals(
    connection: &mut DurableConnection,
    workflow_id: i64,
    reason: &str,
    now: i64,
) -> Result<(), DurableError> {
    let approvals = durable_approval::table
        .filter(durable_approval::workflow_id.eq(workflow_id))
        .filter(durable_approval::status.eq(ApprovalStatus::Pending))
        .for_update()
        .select(ApprovalRow::as_select())
        .load::<ApprovalRow>(connection)
        .await?;
    for approval in approvals {
        let changed = diesel::update(
            durable_approval::table
                .find(approval.id)
                .filter(durable_approval::status.eq(ApprovalStatus::Pending)),
        )
        .set((
            durable_approval::status.eq(ApprovalStatus::Cancelled),
            durable_approval::operator_reason.eq(Some(reason.to_string())),
            durable_approval::resolved_at.eq(Some(now)),
        ))
        .execute(connection)
        .await?;
        ensure_cancel_changed(changed)?;
    }
    Ok(())
}

fn declare_start(
    kind: &str,
    version: i32,
    deduplication_key: Option<&str>,
    from: Option<i64>,
    id: i64,
    inserted: bool,
) {
    crate::trace::declare(|| start_action(kind, version, deduplication_key, from, id, inserted));
}

/// `TX1_Start`; `workflow_id` 0 with `inserted` false is a restart-key `Conflict`.
fn start_action(
    kind: &str,
    version: i32,
    deduplication_key: Option<&str>,
    from: Option<i64>,
    id: i64,
    inserted: bool,
) -> crate::trace::Action {
    crate::trace::Action::new(
        "TX1_Start",
        serde_json::json!({
            "kind": kind,
            "version": version,
            "dedup_key": deduplication_key,
            "from": from,
            "workflow_id": id,
            "inserted": inserted,
        }),
    )
}

fn declare_recoverable_start(
    kind: &str,
    version: i32,
    deduplication_key: &str,
    lineage: Option<(i64, i64)>,
    superseded: bool,
    id: i64,
    inserted: bool,
) {
    crate::trace::declare(|| {
        recoverable_start_action(
            kind,
            version,
            deduplication_key,
            lineage,
            superseded,
            id,
            inserted,
        )
    });
}

/// `TX2_RecoverableStart`: `lineage` = the locked (original, latest) rows;
/// `workflow_id` = the inserted or returned row, 0 for a `Conflict`.
fn recoverable_start_action(
    kind: &str,
    version: i32,
    deduplication_key: &str,
    lineage: Option<(i64, i64)>,
    superseded: bool,
    id: i64,
    inserted: bool,
) -> crate::trace::Action {
    crate::trace::Action::new(
        "TX2_RecoverableStart",
        serde_json::json!({
            "kind": kind,
            "version": version,
            "dedup_key": deduplication_key,
            "original": lineage.map(|(original, _)| original),
            "latest": lineage.map(|(_, latest)| latest),
            "superseded": superseded,
            "workflow_id": id,
            "inserted": inserted,
            "conflict": lineage.is_some() && id == 0,
        }),
    )
}
