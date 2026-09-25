mod support;

use async_trait::async_trait;
use diesel_async::AsyncConnection;
use durable_workflows::{
    DurableWorkflow, StartOptions, WorkflowContext, WorkflowEvent, WorkflowHandler,
    WorkflowTransition,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StartWorkflow {
    value: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StartWorkflowV2 {
    value: i32,
}

impl DurableWorkflow for StartWorkflowV2 {
    const KIND: &'static str = "start_workflow";
    const VERSION: i32 = 2;
}

#[async_trait]
impl WorkflowHandler for StartWorkflowV2 {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OtherStartWorkflow {
    value: i32,
}

impl DurableWorkflow for OtherStartWorkflow {
    const KIND: &'static str = "other_start_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for OtherStartWorkflow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LargeInputWorkflow {
    value: String,
}

impl DurableWorkflow for LargeInputWorkflow {
    const KIND: &'static str = "large_input_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for LargeInputWorkflow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        1
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LargeStateWorkflow;

#[derive(Debug, serde::Deserialize)]
struct SerializationFailureWorkflow;

impl serde::Serialize for SerializationFailureWorkflow {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("intentional test failure"))
    }
}

#[derive(Debug, serde::Deserialize)]
struct FailingSerializeWorkflow;

impl serde::Serialize for FailingSerializeWorkflow {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("intentional input failure"))
    }
}

impl DurableWorkflow for FailingSerializeWorkflow {
    const KIND: &'static str = "failing_serialize_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for FailingSerializeWorkflow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        1
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

impl DurableWorkflow for SerializationFailureWorkflow {
    const KIND: &'static str = "serialization_failure_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for SerializationFailureWorkflow {
    type Context = ();
    type State = ();
    type Approval = serde_json::Value;
    type Output = ();

    fn initial_state(&self) -> Self::State {}

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

impl DurableWorkflow for LargeStateWorkflow {
    const KIND: &'static str = "large_state_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for LargeStateWorkflow {
    type Context = ();
    type State = String;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        "x".repeat(256 * 1024)
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        _state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: 1 })
    }
}

impl DurableWorkflow for StartWorkflow {
    const KIND: &'static str = "start_workflow";
    const VERSION: i32 = 1;
}

#[async_trait]
impl WorkflowHandler for StartWorkflow {
    type Context = ();
    type State = i32;
    type Approval = serde_json::Value;
    type Output = i32;

    fn initial_state(&self) -> Self::State {
        self.value
    }

    async fn step(
        &self,
        _context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        _event: WorkflowEvent,
    ) -> Result<
        WorkflowTransition<Self::State, Self::Approval, Self::Output>,
        durable_workflows::WorkflowError,
    > {
        Ok(WorkflowTransition::Complete { output: state })
    }
}

#[test]
fn start_options_are_explicit_and_default_to_immediate_unrelated_work() {
    let options = StartOptions::default().with_deduplication_key("customer-42");

    assert_eq!(options.deduplication_key.as_deref(), Some("customer-42"));
    assert!(options.available_at.is_none());
    assert!(options.schedule_run_id.is_none());
    assert!(options.root_workflow_id.is_none());
    assert!(options.restarted_from_workflow_id.is_none());
}

#[tokio::test]
async fn start_with_conn_persists_input_state_and_started_event() {
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{
        persistence::{WorkflowEventRow, WorkflowRow},
        schema::{durable_workflow, durable_workflow_event},
    };

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };

    let outcome = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 7 },
        StartOptions::default().with_deduplication_key("start-7"),
    )
    .await
    .expect("workflow starts");

    assert!(outcome.inserted);
    assert!(outcome.workflow_id.get() > 0);

    let workflow = durable_workflow::table
        .filter(durable_workflow::id.eq(outcome.workflow_id.get()))
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("stored workflow loads");
    assert_eq!(workflow.kind, "start_workflow");
    assert_eq!(workflow.version, 1);
    assert_eq!(workflow.input_json, r#"{"value":7}"#);
    assert_eq!(workflow.state_json, "7");
    assert_eq!(workflow.status.as_str(), "ready");

    let event = durable_workflow_event::table
        .filter(durable_workflow_event::workflow_id.eq(outcome.workflow_id.get()))
        .select(WorkflowEventRow::as_select())
        .first::<WorkflowEventRow>(&mut connection)
        .await
        .expect("started event loads");
    assert_eq!(event.sequence, 1);
    assert_eq!(event.delivery_sequence, Some(1));
    assert_eq!(event.event_type, "started");

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn prepared_start_uses_registry_validated_input_and_the_same_transactional_contract() {
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{persistence::WorkflowRow, schema::durable_workflow, WorkflowRegistry};

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let mut registry = WorkflowRegistry::new();
    registry
        .register::<StartWorkflow>()
        .expect("workflow registry");
    let prepared = registry
        .prepare_start_exact("start_workflow", 1, r#"{"value":23}"#)
        .expect("prepared workflow");

    let outcome = durable_workflows::DurableStore::start_prepared_with_conn(
        &mut connection,
        prepared,
        StartOptions::default(),
    )
    .await
    .expect("prepared workflow starts");
    let workflow = durable_workflow::table
        .filter(durable_workflow::id.eq(outcome.workflow_id.get()))
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .expect("stored workflow");
    assert_eq!(workflow.input_json, r#"{"value":23}"#);
    assert_eq!(workflow.state_json, "23");

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn outer_transaction_rollback_removes_the_workflow_and_event() {
    use diesel::QueryDsl;
    use diesel_async::{AsyncConnection, RunQueryDsl};
    use durable_workflows::{
        persistence::NewTopicLockRow,
        schema::{durable_topic_lock, durable_workflow, durable_workflow_event},
    };

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };

    let result: Result<(), durable_workflows::DurableError> = connection
        .transaction(async move |transaction| {
            diesel::insert_into(durable_topic_lock::table)
                .values(NewTopicLockRow {
                    topic: "domain-mutation".to_string(),
                    max_concurrency: 1,
                    updated_at: durable_workflows::persistence::now_millis(),
                })
                .execute(transaction)
                .await?;
            durable_workflows::DurableStore::start_with_conn(
                transaction,
                &StartWorkflow { value: 1 },
                StartOptions::default(),
            )
            .await?;
            Err(durable_workflows::DurableError::InvalidState(
                "force outer rollback".to_string(),
            ))
        })
        .await;
    assert!(result.is_err());
    assert_eq!(
        durable_topic_lock::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        durable_workflow::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        durable_workflow_event::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        0
    );

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn recovering_start_dedupes_active_successful_and_cancelled_generations_but_restarts_failures_and_blocks(
) {
    use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl, SelectableHelper};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{persistence::WorkflowRow, schema::durable_workflow, DurableStore};

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };

    let active_options = StartOptions::default().with_deduplication_key("recover-active");
    let active = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        active_options.clone(),
    )
    .await
    .expect("active generation starts");
    let active_duplicate = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        active_options,
    )
    .await
    .expect("active generation dedupes");
    assert!(!active_duplicate.inserted);
    assert_eq!(active_duplicate.workflow_id, active.workflow_id);

    diesel::update(durable_workflow::table.find(active.workflow_id.get()))
        .set(durable_workflow::status.eq("succeeded"))
        .execute(&mut connection)
        .await
        .unwrap();
    let succeeded_duplicate = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        StartOptions::default().with_deduplication_key("recover-active"),
    )
    .await
    .expect("successful generation dedupes");
    assert!(!succeeded_duplicate.inserted);
    assert_eq!(succeeded_duplicate.workflow_id, active.workflow_id);

    let cancelled = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 3 },
        StartOptions::default().with_deduplication_key("recover-cancelled"),
    )
    .await
    .expect("cancelled lineage starts");
    diesel::update(durable_workflow::table.find(cancelled.workflow_id.get()))
        .set(durable_workflow::status.eq("cancelled"))
        .execute(&mut connection)
        .await
        .unwrap();
    let cancelled_duplicate = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 3 },
        StartOptions::default().with_deduplication_key("recover-cancelled"),
    )
    .await
    .expect("cancelled generation dedupes");
    assert!(!cancelled_duplicate.inserted);
    assert_eq!(cancelled_duplicate.workflow_id, cancelled.workflow_id);

    let failed = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 2 },
        StartOptions::default().with_deduplication_key("recover-failed"),
    )
    .await
    .expect("failed lineage starts");
    diesel::update(durable_workflow::table.find(failed.workflow_id.get()))
        .set(durable_workflow::status.eq("failed"))
        .execute(&mut connection)
        .await
        .unwrap();

    let recovery = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 2 },
        StartOptions::default().with_deduplication_key("recover-failed"),
    )
    .await
    .expect("failed generation restarts");
    assert!(recovery.inserted);
    assert_ne!(recovery.workflow_id, failed.workflow_id);
    let recovery_row = durable_workflow::table
        .find(recovery.workflow_id.get())
        .select(WorkflowRow::as_select())
        .first::<WorkflowRow>(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        recovery_row.root_workflow_id,
        Some(failed.workflow_id.get())
    );
    assert_eq!(
        recovery_row.restarted_from_workflow_id,
        Some(failed.workflow_id.get())
    );
    assert!(recovery_row.deduplication_key.is_none());

    let recovery_duplicate = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 2 },
        StartOptions::default().with_deduplication_key("recover-failed"),
    )
    .await
    .expect("live recovery dedupes");
    assert!(!recovery_duplicate.inserted);
    assert_eq!(recovery_duplicate.workflow_id, recovery.workflow_id);

    diesel::update(durable_workflow::table.find(recovery.workflow_id.get()))
        .set((
            durable_workflow::status.eq("blocked"),
            durable_workflow::error_category.eq(Some("activity_retry_exhausted")),
        ))
        .execute(&mut connection)
        .await
        .unwrap();
    let second_recovery = DurableStore::start_or_restart_recoverable_with_conn(
        &mut connection,
        &StartWorkflow { value: 2 },
        StartOptions::default().with_deduplication_key("recover-failed"),
    )
    .await
    .expect("activity-exhausted blocked generation restarts");
    assert!(second_recovery.inserted);
    assert_eq!(
        durable_workflow::table
            .filter(
                durable_workflow::id
                    .eq(failed.workflow_id.get())
                    .or(durable_workflow::root_workflow_id.eq(Some(failed.workflow_id.get()))),
            )
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        3
    );

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn deduplication_returns_the_original_without_mutating_it() {
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::durable_workflow;

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let options = StartOptions::default().with_deduplication_key("stable-key");
    let first = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        options.clone(),
    )
    .await
    .unwrap();
    let second = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 99 },
        options,
    )
    .await
    .unwrap();

    assert!(first.inserted);
    assert!(!second.inserted);
    assert_eq!(first.workflow_id, second.workflow_id);
    assert_eq!(
        durable_workflow::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        1
    );

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn typed_deduplication_lookup_returns_only_the_matching_workflow_kind() {
    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let key = "typed-lookup-key";
    let started = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        StartOptions::default().with_deduplication_key(key),
    )
    .await
    .unwrap();

    let found =
        durable_workflows::DurableStore::find_by_deduplication_key_with_conn::<StartWorkflow>(
            &mut connection,
            key,
        )
        .await
        .unwrap();
    let other = durable_workflows::DurableStore::find_by_deduplication_key_with_conn::<
        OtherStartWorkflow,
    >(&mut connection, key)
    .await
    .unwrap();

    assert_eq!(found, Some(started.workflow_id));
    assert_eq!(other, None);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn typed_deduplication_lookup_returns_the_root_receipt_after_a_version_bump() {
    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let key = "typed-version-bump-key";
    let started = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        StartOptions::default().with_deduplication_key(key),
    )
    .await
    .unwrap();

    let found = durable_workflows::DurableStore::find_by_deduplication_key_with_conn::<
        StartWorkflowV2,
    >(&mut connection, key)
    .await
    .unwrap();

    assert_eq!(found, Some(started.workflow_id));
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn concurrent_deduplicated_starts_inside_outer_transactions_return_the_original() {
    let Some(pool) = support::fresh_pool().await else {
        return;
    };
    const CALLERS: usize = 24;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(CALLERS));
    let mut starts = tokio::task::JoinSet::new();

    for value in 0..CALLERS {
        let pool = pool.clone();
        let barrier = barrier.clone();
        starts.spawn(async move {
            barrier.wait().await;
            let mut connection = pool.get().await?;
            connection
                .transaction(async move |connection| {
                    durable_workflows::DurableStore::start_with_conn(
                        connection,
                        &StartWorkflow {
                            value: i32::try_from(value).expect("test value fits i32"),
                        },
                        StartOptions::default().with_deduplication_key("concurrent-key"),
                    )
                    .await
                })
                .await
        });
    }

    let mut outcomes = Vec::with_capacity(CALLERS);
    while let Some(joined) = starts.join_next().await {
        outcomes.push(joined.expect("start task joins").expect("start succeeds"));
    }
    let original = outcomes
        .iter()
        .find(|outcome| outcome.inserted)
        .expect("one caller inserts");
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.inserted).count(),
        1
    );
    assert!(outcomes
        .iter()
        .all(|outcome| outcome.workflow_id == original.workflow_id));

    let mut connection = pool.get().await.expect("test connection");
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn different_workflow_kinds_can_reuse_a_deduplication_key() {
    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let key = "shared-domain-key";
    let first = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        StartOptions::default().with_deduplication_key(key),
    )
    .await
    .unwrap();
    let second = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &OtherStartWorkflow { value: 1 },
        StartOptions::default().with_deduplication_key(key),
    )
    .await
    .unwrap();

    assert_ne!(first.workflow_id, second.workflow_id);
    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn oversized_input_or_initial_state_creates_no_rows() {
    use diesel::QueryDsl;
    use diesel_async::RunQueryDsl;
    use durable_workflows::schema::durable_workflow;

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };

    let serialization_error = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &FailingSerializeWorkflow,
        StartOptions::default(),
    )
    .await
    .expect_err("serialization failure stops before insert");
    assert!(serialization_error.to_string().contains("serialization"));

    let input_error = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &LargeInputWorkflow {
            value: "x".repeat(256 * 1024),
        },
        StartOptions::default(),
    )
    .await
    .expect_err("oversized input fails");
    assert!(input_error.to_string().contains("maximum"));

    let state_error = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &LargeStateWorkflow,
        StartOptions::default(),
    )
    .await
    .expect_err("oversized state fails");
    assert!(state_error.to_string().contains("maximum"));

    let serialization_error = durable_workflows::DurableStore::start_with_conn(
        &mut connection,
        &SerializationFailureWorkflow,
        StartOptions::default(),
    )
    .await
    .expect_err("serialization failure is returned");
    assert!(serialization_error.to_string().contains("serialization"));
    assert_eq!(
        durable_workflow::table
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        0
    );

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn second_restart_of_a_source_conflicts_and_leaves_the_caller_transaction_usable() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use durable_workflows::{schema::durable_workflow, DurableError, DurableStore, WorkflowId};

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let source = DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        StartOptions::default(),
    )
    .await
    .expect("source starts");
    let restart = |source: WorkflowId| StartOptions {
        restarted_from_workflow_id: Some(source),
        ..StartOptions::default()
    };
    let successor = DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 2 },
        restart(source.workflow_id),
    )
    .await
    .expect("first restart starts");
    assert!(successor.inserted);

    let unrelated = connection
        .transaction(async move |transaction| {
            let collision = DurableStore::start_with_conn(
                transaction,
                &StartWorkflow { value: 3 },
                restart(source.workflow_id),
            )
            .await;
            match collision {
                Err(DurableError::Conflict(message)) => assert!(
                    message.contains(&format!(
                        "workflow {} already has a successor",
                        source.workflow_id.get()
                    )),
                    "unexpected conflict message: {message}"
                ),
                other => panic!("expected a restart conflict, got {other:?}"),
            }
            let successors = durable_workflow::table
                .filter(
                    durable_workflow::restarted_from_workflow_id.eq(Some(source.workflow_id.get())),
                )
                .count()
                .get_result::<i64>(transaction)
                .await?;
            assert_eq!(successors, 1);
            DurableStore::start_with_conn(
                transaction,
                &OtherStartWorkflow { value: 4 },
                StartOptions::default(),
            )
            .await
        })
        .await
        .expect("caller transaction commits after the conflict");
    assert!(unrelated.inserted);

    let rows = durable_workflow::table
        .select((
            durable_workflow::id,
            durable_workflow::restarted_from_workflow_id,
        ))
        .order(durable_workflow::id.asc())
        .load::<(i64, Option<i64>)>(&mut connection)
        .await
        .expect("workflows load");
    assert_eq!(
        rows,
        vec![
            (source.workflow_id.get(), None),
            (successor.workflow_id.get(), Some(source.workflow_id.get())),
            (unrelated.workflow_id.get(), None),
        ]
    );

    support::drop_durable_tables(&mut connection).await;
}

#[tokio::test]
async fn start_options_with_deduplication_and_restart_keys_are_rejected() {
    use durable_workflows::{DurableError, DurableStore};

    let Some(mut connection) = support::fresh_connection().await else {
        return;
    };
    let source = DurableStore::start_with_conn(
        &mut connection,
        &StartWorkflow { value: 1 },
        StartOptions::default(),
    )
    .await
    .expect("source starts");
    let options = StartOptions {
        restarted_from_workflow_id: Some(source.workflow_id),
        ..StartOptions::default().with_deduplication_key("both-keys")
    };
    let error =
        DurableStore::start_with_conn(&mut connection, &StartWorkflow { value: 2 }, options)
            .await
            .expect_err("both keys are rejected");
    assert!(
        matches!(error, DurableError::InvalidDefinition(_)),
        "unexpected error: {error:?}"
    );

    support::drop_durable_tables(&mut connection).await;
}
