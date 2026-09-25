use chrono::{DateTime, Utc};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::{AsyncConnection, RunQueryDsl};

use crate::{
    error::ensure_size,
    persistence::{
        self, ActivityRow, ActivityStatus, ApprovalRow, NewWorkflowEventRow, NewWorkflowRow,
        WorkflowRow, WorkflowStatus,
    },
    schema::{durable_activity, durable_activity_attempt, durable_approval, durable_workflow},
    DurableConnection, DurableError, DurablePool, ScheduleRunId, WorkflowHandler, WorkflowId,
    MAX_INPUT_STATE_PAYLOAD_BYTES,
};

const DEFAULT_MAX_ACTIVATION_ATTEMPTS: i32 = 8;
const MAX_DEDUPLICATION_KEY_CHARS: usize = 191;

#[derive(Debug, Clone, Default)]
pub struct StartOptions {
    pub deduplication_key: Option<String>,
    pub available_at: Option<DateTime<Utc>>,
    pub schedule_run_id: Option<ScheduleRunId>,
    pub root_workflow_id: Option<WorkflowId>,
    pub restarted_from_workflow_id: Option<WorkflowId>,
}

impl StartOptions {
    pub fn with_deduplication_key(mut self, key: impl Into<String>) -> Self {
        self.deduplication_key = Some(key.into());
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
        connection
            .transaction(async move |connection| {
                crate::trace::scoped(connection, async move |connection| {
                    let workflow = durable_workflow::table
                        .find(workflow_id.get())
                        .for_update()
                        .select(WorkflowRow::as_select())
                        .first::<WorkflowRow>(connection)
                        .await
                        .optional()?
                        .ok_or_else(|| DurableError::NotFound {
                            resource: "workflow",
                            identifier: workflow_id.to_string(),
                        })?;
                    if matches!(
                        workflow.status,
                        WorkflowStatus::Succeeded
                            | WorkflowStatus::Failed
                            | WorkflowStatus::Cancelled
                    ) {
                        return Ok(());
                    }
                    let now = persistence::database_now_millis(connection).await?;
                    crate::trace::declare(|| {
                        crate::trace::Action::new(
                            "TX3_Cancel",
                            serde_json::json!({ "workflow_id": workflow.id }),
                        )
                    });
                    cancel_locked_workflow(connection, &workflow, reason, None, now).await
                })
                .await
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
            crate::dialect::transaction(&mut connection, async move |connection| {
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

        connection
            .transaction(async move |transaction| {
                crate::trace::scoped(transaction, async move |transaction| {
                    Self::insert_prepared(
                        transaction,
                        W::KIND,
                        W::VERSION,
                        input_json,
                        state_json,
                        options,
                    )
                    .await
                })
                .await
            })
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
            crate::dialect::transaction(&mut connection, async move |connection| {
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

        connection
            .transaction(async move |transaction| {
                crate::trace::scoped(transaction, async move |transaction| {
                    let Some(key) = options.deduplication_key.clone() else {
                        return Self::insert_prepared(
                            transaction,
                            W::KIND,
                            W::VERSION,
                            input_json,
                            state_json,
                            options,
                        )
                        .await;
                    };
                    let original = durable_workflow::table
                        .filter(durable_workflow::kind.eq(W::KIND))
                        .filter(durable_workflow::deduplication_key.eq(&key))
                        .for_update()
                        .select(WorkflowRow::as_select())
                        .first::<WorkflowRow>(transaction)
                        .await
                        .optional()?;
                    let Some(original) = original else {
                        let outcome = Self::insert_prepared_untraced(
                            transaction,
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
                    let latest = durable_workflow::table
                        .filter(durable_workflow::kind.eq(W::KIND))
                        .filter(
                            durable_workflow::id
                                .eq(root_id)
                                .or(durable_workflow::root_workflow_id.eq(Some(root_id))),
                        )
                        .order(durable_workflow::id.desc())
                        .for_update()
                        .select(WorkflowRow::as_select())
                        .first::<WorkflowRow>(transaction)
                        .await?;
                    if !matches!(
                        latest.status,
                        WorkflowStatus::Failed | WorkflowStatus::Blocked
                    ) {
                        declare_recoverable_start(
                            W::KIND,
                            W::VERSION,
                            &key,
                            Some((original.id, latest.id)),
                            false,
                            latest.id,
                            false,
                        );
                        return Ok(StartOutcome {
                            workflow_id: WorkflowId::new(latest.id)?,
                            inserted: false,
                        });
                    }

                    let now = persistence::database_now_millis(transaction).await?;
                    if crate::trace::ENABLED {
                        for id in durable_activity::table
                            .filter(durable_activity::workflow_id.eq(latest.id))
                            .filter(durable_activity::status.eq("dead_lettered"))
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
                            .filter(durable_activity::status.eq("dead_lettered")),
                    )
                    .set((
                        durable_activity::status.eq("cancelled"),
                        durable_activity::lease_owner.eq(None::<String>),
                        durable_activity::lease_token.eq(None::<String>),
                        durable_activity::lease_expires_at.eq(None::<i64>),
                        durable_activity::updated_at.eq(now),
                        durable_activity::completed_at.eq(Some(now)),
                    ))
                    .execute(transaction)
                    .await?;
                    if latest.status == WorkflowStatus::Blocked {
                        let changed = diesel::update(
                            durable_workflow::table
                                .find(latest.id)
                                .filter(durable_workflow::status.eq("blocked")),
                        )
                        .set((
                            durable_workflow::status.eq("cancelled"),
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
                        let sequence = persistence::next_event_sequence(
                            transaction,
                            WorkflowId::new(latest.id)?,
                        )
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
                                reason: Some(
                                    "a successor recovery generation was started".to_string(),
                                ),
                                created_at: now,
                            },
                        )
                        .await?;
                    }

                    let lineage = Some((original.id, latest.id));
                    let outcome = Self::insert_prepared_untraced(
                        transaction,
                        W::KIND,
                        W::VERSION,
                        input_json,
                        state_json,
                        StartOptions {
                            deduplication_key: None,
                            root_workflow_id: Some(WorkflowId::new(root_id)?),
                            restarted_from_workflow_id: Some(WorkflowId::new(latest.id)?),
                            ..options
                        },
                    )
                    .await;
                    match &outcome {
                        Ok(outcome) => declare_recoverable_start(
                            W::KIND,
                            W::VERSION,
                            &key,
                            lineage,
                            true,
                            outcome.workflow_id.get(),
                            outcome.inserted,
                        ),
                        // The restart key already has a successor: everything rolls back.
                        Err(DurableError::Conflict(_)) => crate::trace::declare_rollback(|| {
                            recoverable_start_action(
                                W::KIND,
                                W::VERSION,
                                &key,
                                lineage,
                                false,
                                0,
                                false,
                            )
                        }),
                        Err(_) => {}
                    }
                    outcome
                })
                .await
            })
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
        connection
            .transaction(async move |transaction| {
                crate::trace::scoped(transaction, async move |transaction| {
                    Self::insert_prepared(
                        transaction,
                        prepared.kind(),
                        prepared.version(),
                        prepared.input_json().to_string(),
                        prepared.state_json().to_string(),
                        options,
                    )
                    .await
                })
                .await
            })
            .await
    }

    /// T-X1: `insert_prepared_untraced` declared as `TX1_Start`.
    async fn insert_prepared(
        connection: &mut DurableConnection,
        kind: &str,
        version: i32,
        input_json: String,
        state_json: String,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError> {
        let key = options.deduplication_key.clone();
        let from = options.restarted_from_workflow_id.map(WorkflowId::get);
        let outcome = Self::insert_prepared_untraced(
            connection, kind, version, input_json, state_json, options,
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

    async fn insert_prepared_untraced(
        connection: &mut DurableConnection,
        kind: &str,
        version: i32,
        input_json: String,
        state_json: String,
        options: StartOptions,
    ) -> Result<StartOutcome, DurableError> {
        if let Some(key) = options.deduplication_key.as_deref() {
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
            deduplication_key: options.deduplication_key.clone(),
            schedule_run_id: options.schedule_run_id.map(ScheduleRunId::get),
            root_workflow_id: options.root_workflow_id.map(WorkflowId::get),
            restarted_from_workflow_id: options.restarted_from_workflow_id.map(WorkflowId::get),
            parent_workflow_id: None,
            parent_command_sequence: None,
            command_sequence: 0,
            delivered_event_sequence: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };

        let (id, inserted) = persistence::insert_started(connection, row).await?;
        Ok(StartOutcome {
            workflow_id: WorkflowId::new(id)?,
            inserted,
        })
    }

    /// Inserts a child workflow inside the parent's commit transaction.
    ///
    /// The caller supplies the resolved deduplication key so a re-committed
    /// parent transition converges on one child instance.
    pub(crate) async fn insert_child(
        connection: &mut DurableConnection,
        child: &crate::ChildWorkflowCommand,
        deduplication_key: String,
        parent_workflow_id: i64,
        parent_command_sequence: i32,
        root_workflow_id: i64,
    ) -> Result<StartOutcome, DurableError> {
        let key_length = deduplication_key.chars().count();
        if key_length == 0 || key_length > MAX_DEDUPLICATION_KEY_CHARS {
            return Err(DurableError::InvalidDefinition(format!(
                "child workflow deduplication key must contain 1 to {MAX_DEDUPLICATION_KEY_CHARS} characters"
            )));
        }
        if let Some(existing) =
            persistence::find_by_deduplication_key(connection, child.kind(), &deduplication_key)
                .await?
        {
            if existing.version != child.version() {
                return Err(DurableError::DefinitionMismatch {
                    actual_kind: existing.kind,
                    actual_version: existing.version,
                    expected_kind: child.kind().to_string(),
                    expected_version: child.version(),
                });
            }
            return Ok(StartOutcome {
                workflow_id: WorkflowId::new(existing.id)?,
                inserted: false,
            });
        }

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

        let (id, inserted) = persistence::insert_started(connection, row).await?;
        Ok(StartOutcome {
            workflow_id: WorkflowId::new(id)?,
            inserted,
        })
    }
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
    if options.deduplication_key.is_some() && options.restarted_from_workflow_id.is_some() {
        return Err(DurableError::InvalidDefinition(
            "workflow start cannot set both a deduplication key and a restart source".to_string(),
        ));
    }
    if let Some(key) = options.deduplication_key.as_deref() {
        let length = key.chars().count();
        if length == 0 || length > MAX_DEDUPLICATION_KEY_CHARS {
            return Err(DurableError::InvalidDefinition(format!(
                "workflow deduplication key must contain 1 to {MAX_DEDUPLICATION_KEY_CHARS} characters"
            )));
        }
    }
    Ok(())
}

pub(crate) async fn cancel_locked_workflow(
    connection: &mut DurableConnection,
    workflow: &WorkflowRow,
    reason: &str,
    operator_id: Option<&str>,
    now: i64,
) -> Result<(), DurableError> {
    let workflow_id = WorkflowId::new(workflow.id)?;
    crate::trace::touch_wf(workflow.id);
    let attempt_outcome = if operator_id.is_some() {
        "operator_cancelled"
    } else {
        "application_cancelled"
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
        durable_workflow::wait_kind.eq(None::<String>),
        durable_workflow::wait_reference_id.eq(None::<i64>),
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
        workflow.id,
        &workflow.kind,
        workflow.version,
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
    attempt_outcome: &str,
    now: i64,
) -> Result<(), DurableError> {
    let activities = durable_activity::table
        .filter(durable_activity::workflow_id.eq(workflow_id))
        .filter(durable_activity::status.eq_any([ActivityStatus::Pending, ActivityStatus::Running]))
        .for_update()
        .select(ActivityRow::as_select())
        .load::<ActivityRow>(connection)
        .await?;
    for activity in activities {
        crate::trace::touch_act(activity.id);
        if activity.status == ActivityStatus::Running {
            close_attempt(connection, &activity, attempt_outcome, reason, now).await?;
        }
        let changed = diesel::update(
            durable_activity::table
                .find(activity.id)
                .filter(durable_activity::status.eq(&activity.status)),
        )
        .set((
            durable_activity::status.eq(ActivityStatus::Cancelled),
            durable_activity::lease_owner.eq(None::<String>),
            durable_activity::lease_token.eq(None::<String>),
            durable_activity::lease_expires_at.eq(None::<i64>),
            durable_activity::updated_at.eq(now),
            durable_activity::completed_at.eq(Some(now)),
        ))
        .execute(connection)
        .await?;
        ensure_cancel_changed(changed)?;
    }
    Ok(())
}

pub(crate) async fn close_attempt(
    connection: &mut DurableConnection,
    activity: &ActivityRow,
    outcome: &str,
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
        durable_activity_attempt::outcome.eq(Some(outcome.to_string())),
        durable_activity_attempt::error_category.eq(Some(outcome.to_string())),
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
        .filter(durable_approval::status.eq("pending"))
        .for_update()
        .select(ApprovalRow::as_select())
        .load::<ApprovalRow>(connection)
        .await?;
    for approval in approvals {
        let changed = diesel::update(
            durable_approval::table
                .find(approval.id)
                .filter(durable_approval::status.eq("pending")),
        )
        .set((
            durable_approval::status.eq("cancelled"),
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
