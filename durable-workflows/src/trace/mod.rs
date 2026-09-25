//! Test-only recorder for trace checking against the Quint model
//! (`docs/design/trace-checking.md` §3). Built only with the `trace-model`
//! feature; `noop.rs` supplies inert items with the same signatures.
//!
//! Every outermost library transaction runs inside [`scoped`]. Code sites
//! declare the model action and mark the rows they write; the scope then
//! inserts one `durable_trace` row as the transaction's last statement, so
//! the row's `seq` is the commit order.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use diesel::{
    sql_types::{BigInt, Integer, Nullable, Text},
    ExpressionMethods, OptionalExtension, QueryDsl, QueryableByName, SelectableHelper,
};
use diesel_async::{AsyncConnection, RunQueryDsl, TransactionManager};
use serde_json::{json, Map, Value};

use crate::{
    dialect::TransactionCallback,
    persistence::{ActivityAttemptRow, ActivityRow, WorkflowEventRow, WorkflowRow},
    schema::{
        durable_activity, durable_activity_attempt, durable_workflow, durable_workflow_event,
    },
    DurableConnection, DurablePool,
};

pub(crate) const ENABLED: bool = true;

/// Creates `durable_trace` and `durable_trace_meta`. Test fixtures run it
/// (through [`trace_up_sql`], which adds the external-write triggers); it is
/// never part of the baseline migration.
#[cfg(feature = "mysql")]
pub const TRACE_UP_SQL: &str = "\
CREATE TABLE durable_trace (
  seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  txn_id CHAR(36) NOT NULL,
  actor VARCHAR(191) NOT NULL,
  action VARCHAR(64) NOT NULL,
  depth INT NOT NULL,
  begin_seq BIGINT NULL,
  now_sampled BIGINT NULL,
  end_now BIGINT NULL,
  params_json LONGTEXT NOT NULL,
  post_json LONGTEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE durable_trace_meta (
  k VARCHAR(64) NOT NULL PRIMARY KEY,
  v LONGTEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE durable_trace_marker (
  conn BIGINT UNSIGNED NOT NULL PRIMARY KEY
) ENGINE=InnoDB;
";

#[cfg(feature = "postgres")]
pub const TRACE_UP_SQL: &str = "\
CREATE TABLE durable_trace (
  seq BIGSERIAL PRIMARY KEY,
  txn_id CHAR(36) NOT NULL,
  actor VARCHAR(191) NOT NULL,
  action VARCHAR(64) NOT NULL,
  depth INT NOT NULL,
  begin_seq BIGINT NULL,
  now_sampled BIGINT NULL,
  end_now BIGINT NULL,
  params_json TEXT NOT NULL,
  post_json TEXT NOT NULL
);
CREATE TABLE durable_trace_meta (
  k VARCHAR(64) NOT NULL PRIMARY KEY,
  v TEXT NOT NULL
);
";

/// Columns of each modeled table that an `External` record carries (the
/// inputs of the recorder's post-images).
const EXTERNAL_COLUMNS: &[(&str, &[&str])] = &[
    (
        "durable_workflow",
        &[
            "id",
            "status",
            "kind",
            "wait_kind",
            "wait_reference_id",
            "available_at",
            "lease_token",
            "lease_expires_at",
            "command_sequence",
            "delivered_event_sequence",
            "activation_attempts",
            "deduplication_key",
            "parent_workflow_id",
            "root_workflow_id",
            "restarted_from_workflow_id",
        ],
    ),
    (
        "durable_activity",
        &[
            "id",
            "status",
            "workflow_id",
            "topic",
            "attempt_count",
            "max_attempts",
            "available_at",
            "lease_token",
            "lease_expires_at",
            "timeout_millis",
            "lease_duration_millis",
        ],
    ),
    (
        "durable_activity_attempt",
        &[
            "activity_id",
            "attempt_number",
            "lease_token",
            "finished_at",
        ],
    ),
    (
        "durable_workflow_event",
        &[
            "workflow_id",
            "delivery_sequence",
            "event_type",
            "metadata_json",
        ],
    ),
];

/// The trace tables plus the external-write capture (design §3, "External
/// writes"): triggers on the modeled tables append an `External` row to
/// `durable_trace` for every change made outside a traced library
/// transaction, so it shares the `seq` order with the recorded steps. Only
/// deliverable event rows (`delivery_sequence` set) are captured.
///
/// A traced transaction marks itself for its own duration: on MySQL with a
/// `durable_trace_marker` row keyed by `CONNECTION_ID()`, inserted at scope
/// open and deleted at scope close (a rollback removes it, so a pooled
/// connection never keeps it); on Postgres with the transaction-local
/// setting `durable.trace_scope`.
#[cfg(feature = "mysql")]
pub fn trace_up_sql() -> String {
    let mut sql = TRACE_UP_SQL.to_string();
    for (table, columns) in EXTERNAL_COLUMNS {
        for (op, row) in [("INSERT", "NEW"), ("UPDATE", "NEW"), ("DELETE", "OLD")] {
            let image = columns
                .iter()
                .map(|column| format!("'{column}', {row}.{column}"))
                .collect::<Vec<_>>()
                .join(", ");
            let deliverable = if *table == "durable_workflow_event" {
                format!("{row}.delivery_sequence IS NOT NULL AND ")
            } else {
                String::new()
            };
            let _ = std::fmt::Write::write_fmt(
                &mut sql,
                format_args!(
                    "CREATE TRIGGER {table}_trace_{op} AFTER {op} ON {table} FOR EACH ROW \
                     INSERT INTO durable_trace (txn_id, actor, action, depth, params_json, post_json) \
                     SELECT UUID(), 'external', 'External', 0, \
                     JSON_OBJECT('table', '{table}', 'op', '{lower}'), \
                     JSON_OBJECT('row', JSON_OBJECT({image})) FROM DUAL \
                     WHERE {deliverable}NOT EXISTS \
                     (SELECT 1 FROM durable_trace_marker WHERE conn = CONNECTION_ID());\n",
                    lower = op.to_ascii_lowercase(),
                ),
            );
        }
    }
    sql
}

#[cfg(feature = "postgres")]
pub fn trace_up_sql() -> String {
    let mut sql = TRACE_UP_SQL.to_string();
    sql.push_str(
        "CREATE FUNCTION durable_trace_external() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  image jsonb;
BEGIN
  IF coalesce(current_setting('durable.trace_scope', true), '') = '1' THEN
    RETURN NULL;
  END IF;
  IF TG_OP = 'DELETE' THEN
    image := to_jsonb(OLD);
  ELSE
    image := to_jsonb(NEW);
  END IF;
  IF TG_TABLE_NAME = 'durable_workflow_event' AND image->'delivery_sequence' = 'null'::jsonb THEN
    RETURN NULL;
  END IF;
  INSERT INTO durable_trace (txn_id, actor, action, depth, params_json, post_json)
  VALUES (gen_random_uuid()::text, 'external', 'External', 0,
          jsonb_build_object('table', TG_TABLE_NAME, 'op', lower(TG_OP))::text,
          jsonb_build_object('row', image)::text);
  RETURN NULL;
END
$$;
",
    );
    for (table, _) in EXTERNAL_COLUMNS {
        let _ = std::fmt::Write::write_fmt(
            &mut sql,
            format_args!(
                "CREATE TRIGGER {table}_trace AFTER INSERT OR UPDATE OR DELETE ON {table} \
                 FOR EACH ROW EXECUTE FUNCTION durable_trace_external();\n"
            ),
        );
    }
    sql
}

#[cfg(feature = "mysql")]
const MARK_SCOPE_SQL: &str = "INSERT INTO durable_trace_marker (conn) VALUES (CONNECTION_ID())";
#[cfg(feature = "mysql")]
const UNMARK_SCOPE_SQL: &str = "DELETE FROM durable_trace_marker WHERE conn = CONNECTION_ID()";
#[cfg(feature = "postgres")]
const MARK_SCOPE_SQL: &str = "SELECT set_config('durable.trace_scope', '1', true)";
#[cfg(feature = "postgres")]
const UNMARK_SCOPE_SQL: &str = "SELECT set_config('durable.trace_scope', '', true)";

#[cfg(feature = "mysql")]
const INSERT_TRACE_SQL: &str = "INSERT INTO durable_trace \
     (txn_id, actor, action, depth, begin_seq, now_sampled, end_now, params_json, post_json) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
#[cfg(feature = "postgres")]
const INSERT_TRACE_SQL: &str = "INSERT INTO durable_trace \
     (txn_id, actor, action, depth, begin_seq, now_sampled, end_now, params_json, post_json) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

#[cfg(feature = "mysql")]
const INSERT_META_SQL: &str = "INSERT INTO durable_trace_meta (k, v) VALUES (?, ?)";
#[cfg(feature = "postgres")]
const INSERT_META_SQL: &str = "INSERT INTO durable_trace_meta (k, v) VALUES ($1, $2)";

const DEFAULT_ACTOR: &str = "app";

/// A model action and its parameters as recorded in `durable_trace`.
#[derive(Debug, Clone)]
pub struct Action {
    name: String,
    params: Value,
}

impl Action {
    pub fn new(name: impl Into<String>, params: Value) -> Self {
        Self {
            name: name.into(),
            params,
        }
    }
}

#[derive(Default)]
struct Scope {
    connection: usize,
    actor: Option<String>,
    declared: Vec<Action>,
    now_sampled: Option<i64>,
    notes: Map<String, Value>,
    workflows: BTreeSet<i64>,
    activities: BTreeSet<i64>,
    attempts: BTreeSet<(i64, i32)>,
    events: BTreeMap<(i64, i32), String>,
    rollback: Option<Action>,
}

impl Scope {
    fn is_empty(&self) -> bool {
        self.declared.is_empty()
            && self.workflows.is_empty()
            && self.activities.is_empty()
            && self.attempts.is_empty()
            && self.events.is_empty()
    }
}

tokio::task_local! {
    static SCOPE: Arc<Mutex<Scope>>;
    static ROLLBACK: Arc<Mutex<Option<Action>>>;
}

fn in_scope() -> bool {
    SCOPE.try_with(|_| ()).is_ok()
}

fn with_scope<T>(update: impl FnOnce(&mut Scope) -> T) -> Option<T> {
    SCOPE
        .try_with(|scope| update(&mut scope.lock().expect("trace scope lock")))
        .ok()
}

fn touch(what: &str, update: impl FnOnce(&mut Scope)) {
    if with_scope(update).is_none() {
        panic!("trace-model: {what} touched outside a library transaction scope");
    }
}

/// Runs `callback` as a traced transaction body. Opens a scope when none is
/// active on this task; a nested call on the same connection passes through.
pub(crate) async fn scoped<R, E, F>(connection: &mut DurableConnection, callback: F) -> Result<R, E>
where
    for<'r> F: AsyncFnOnce(&'r mut DurableConnection) -> Result<R, E>
        + TransactionCallback<&'r mut DurableConnection, Result<R, E>, Fut: Send>
        + Send,
    E: Send,
    R: Send,
{
    let address = std::ptr::from_ref::<DurableConnection>(connection) as usize;
    if let Some(outer) = with_scope(|scope| scope.connection) {
        assert_eq!(
            outer, address,
            "trace-model: a library transaction opened on a second connection inside another \
             transaction's trace scope"
        );
        return callback(connection).await;
    }

    let depth = transaction_depth(connection);
    let begin_seq = max_seq(connection).await;
    // A failed transaction (or savepoint) rolls the mark back with it.
    scope_mark(connection, MARK_SCOPE_SQL).await;
    let scope = Arc::new(Mutex::new(Scope {
        connection: address,
        ..Scope::default()
    }));
    let result = SCOPE.scope(scope.clone(), callback(connection)).await;
    let mut scope = std::mem::take(&mut *scope.lock().expect("trace scope lock"));
    if result.is_ok() {
        if !scope.is_empty() {
            record_transaction(connection, scope, depth, begin_seq).await;
        }
        scope_mark(connection, UNMARK_SCOPE_SQL).await;
    } else if let Some(action) = scope.rollback.take() {
        // Handed to the enclosing `capture_rollback`, if any; otherwise lost.
        let _ = ROLLBACK.try_with(|slot| *slot.lock().expect("trace rollback lock") = Some(action));
    }
    result
}

/// Declares the model step of a transaction that rolls back (for example a
/// `Conflict` start). The latest declaration wins. It reaches the trace only
/// through an enclosing [`capture_rollback`], whose caller records it with
/// [`record_local`] once it holds no connection.
pub(crate) fn declare_rollback(action: impl FnOnce() -> Action) {
    touch("a rollback declaration", |scope| {
        scope.rollback = Some(action())
    });
}

/// Runs `future` and returns the rollback step a traced transaction inside it
/// declared, if that transaction failed.
pub(crate) async fn capture_rollback<T>(
    future: impl std::future::Future<Output = T>,
) -> (T, Option<Action>) {
    let slot = Arc::new(Mutex::new(None));
    let output = ROLLBACK.scope(slot.clone(), future).await;
    let action = slot.lock().expect("trace rollback lock").take();
    (output, action)
}

/// Declares the model action of the enclosing transaction. A transaction
/// that runs several declared steps (a schedule materialization starting
/// many workflows) is recorded as one `Batch` of them in declaration order.
pub(crate) fn declare(action: impl FnOnce() -> Action) {
    touch("a declaration", |scope| scope.declared.push(action()));
}

/// Declares a transaction the model does not cover (`Unmodeled{name}`).
/// `writes_modeled`: whether it can write a modeled table (workflow,
/// activity, attempt, deliverable event); the generator excludes such a
/// trace and skips the others as stutters.
pub(crate) fn declare_unmodeled(name: &'static str, writes_modeled: bool) {
    declare(|| {
        Action::new(
            "Unmodeled",
            json!({ "name": name, "writes_modeled": writes_modeled }),
        )
    });
}

static NEXT_HEARTBEAT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A process-wide heartbeat id (`TW2_Send` .. `TW2_Commit`/`TW2_FenceMiss`/
/// `TW2_Drop`); the generator renumbers them densely per trace (`ghost.nextHb`).
pub(crate) fn next_heartbeat_id() -> u64 {
    NEXT_HEARTBEAT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Names the process that runs the enclosing transaction (default `app`).
pub(crate) fn actor(name: &str) {
    touch("an actor", |scope| scope.actor = Some(name.to_string()));
}

pub(crate) fn touch_wf(id: i64) {
    touch("a workflow", |scope| {
        scope.workflows.insert(id);
    });
}

pub(crate) fn touch_act(id: i64) {
    touch("an activity", |scope| {
        scope.activities.insert(id);
    });
}

pub(crate) fn touch_att(activity_id: i64, attempt_number: i32) {
    touch("an attempt", |scope| {
        scope.attempts.insert((activity_id, attempt_number));
    });
}

/// Deliverable events only; history rows are not part of the model.
pub(crate) fn touch_event(workflow_id: i64, delivery_sequence: i32, event_type: &str) {
    touch("an event", |scope| {
        scope
            .events
            .insert((workflow_id, delivery_sequence), event_type.to_string());
    });
}

/// Records an observed read as an action parameter. Object values merge
/// with an earlier note under the same key.
pub(crate) fn note(key: &str, value: impl FnOnce() -> Value) {
    touch("a note", |scope| {
        let value = value();
        match (scope.notes.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(more)) => existing.extend(more),
            (_, value) => {
                scope.notes.insert(key.to_string(), value);
            }
        }
    });
}

/// Keeps the transaction's first database clock sample as `now_sampled`.
pub(crate) fn sample_now(millis: i64) {
    with_scope(|scope| {
        scope.now_sampled.get_or_insert(millis);
    });
}

/// Records a local (non-database) step as its own autocommit row.
///
/// Call only when the task holds no pooled connection, so recording cannot
/// exhaust the pool. `actor` names the runtime the way the library does:
/// `"{runtime_id}:coordinator"`, `"{runtime_id}:dispatcher"` (or any worker
/// id); the generator strips the suffix. A test or driver that aborts a
/// runtime records `Action::new("Crash", json!({}))` with any actor of that
/// runtime, after the runtime's tasks have stopped.
pub async fn record_local(pool: &DurablePool, actor: &str, action: Action) {
    assert!(
        !in_scope(),
        "trace-model: record_local called inside an open trace scope"
    );
    let mut connection = pool
        .get()
        .await
        .unwrap_or_else(|error| panic!("trace-model: no connection for a local record: {error}"));
    insert_trace_row(
        &mut connection,
        actor,
        &action,
        0,
        None,
        None,
        None,
        &Value::Object(Map::new()),
    )
    .await;
}

/// Records a local step on a connection the caller already holds outside
/// any transaction (the heartbeat's own checkout), so recording needs no
/// second pool connection.
pub(crate) async fn record_local_on(
    connection: &mut DurableConnection,
    actor: &str,
    action: Action,
) {
    assert!(
        !in_scope(),
        "trace-model: record_local_on called inside an open trace scope"
    );
    insert_trace_row(
        connection,
        actor,
        &action,
        0,
        None,
        None,
        None,
        &Value::Object(Map::new()),
    )
    .await;
}

/// Stores the trace's name (the test path) and backend in
/// `durable_trace_meta`.
pub async fn begin_trace(connection: &mut DurableConnection, name: &str) {
    let backend = match crate::BACKEND {
        crate::BackendKind::Mysql => "mysql",
        crate::BackendKind::Postgres => "postgres",
    };
    for (key, value) in [("test", name), ("backend", backend)] {
        diesel::sql_query(INSERT_META_SQL)
            .bind::<Text, _>(key)
            .bind::<Text, _>(value)
            .execute(connection)
            .await
            .unwrap_or_else(|error| {
                panic!("trace-model: failed to write durable_trace_meta: {error}")
            });
    }
}

async fn scope_mark(connection: &mut DurableConnection, sql: &str) {
    diesel::sql_query(sql)
        .execute(connection)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "trace-model: cannot mark the trace scope ({error}); with the trace-model feature \
                 on, the test database must be created with trace::trace_up_sql()"
            )
        });
}

fn transaction_depth(connection: &mut DurableConnection) -> i32 {
    let status =
        <<DurableConnection as AsyncConnection>::TransactionManager as TransactionManager<
            DurableConnection,
        >>::transaction_manager_status_mut(connection);
    let depth = status
        .transaction_depth()
        .unwrap_or_else(|error| panic!("trace-model: broken transaction state: {error}"));
    depth.map_or(0, |depth| i32::try_from(depth.get()).unwrap_or(i32::MAX))
}

#[derive(QueryableByName)]
struct MaxSeq {
    #[diesel(sql_type = Nullable<BigInt>)]
    seq: Option<i64>,
}

async fn max_seq(connection: &mut DurableConnection) -> i64 {
    diesel::sql_query("SELECT MAX(seq) AS seq FROM durable_trace")
        .get_result::<MaxSeq>(connection)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "trace-model: cannot read durable_trace ({error}); with the trace-model feature \
                 on, the test database must be created with trace::TRACE_UP_SQL"
            )
        })
        .seq
        .unwrap_or(0)
}

async fn record_transaction(
    connection: &mut DurableConnection,
    scope: Scope,
    depth: i32,
    begin_seq: i64,
) {
    let post = post_images(connection, &scope)
        .await
        .unwrap_or_else(|error| panic!("trace-model: failed to read post-images: {error}"));
    let end_now = crate::dialect::now_millis(connection)
        .await
        .unwrap_or_else(|error| panic!("trace-model: failed to sample end_now: {error}"));
    let mut declared = scope.declared;
    let mut action = match declared.len() {
        0 => Action::new("Unknown", Value::Object(Map::new())),
        1 => declared.remove(0),
        _ => Action::new(
            "Batch",
            json!({
                "actions": declared
                    .into_iter()
                    .map(|action| json!({ "action": action.name, "params": action.params }))
                    .collect::<Vec<_>>(),
            }),
        ),
    };
    if !scope.notes.is_empty() {
        match &mut action.params {
            Value::Object(params) => params.extend(scope.notes),
            params => *params = json!({ "value": params.take(), "notes": scope.notes }),
        }
    }
    insert_trace_row(
        connection,
        scope.actor.as_deref().unwrap_or(DEFAULT_ACTOR),
        &action,
        depth,
        Some(begin_seq),
        scope.now_sampled,
        Some(end_now),
        &post,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn insert_trace_row(
    connection: &mut DurableConnection,
    actor: &str,
    action: &Action,
    depth: i32,
    begin_seq: Option<i64>,
    now_sampled: Option<i64>,
    end_now: Option<i64>,
    post: &Value,
) {
    diesel::sql_query(INSERT_TRACE_SQL)
        .bind::<Text, _>(uuid::Uuid::new_v4().to_string())
        .bind::<Text, _>(actor)
        .bind::<Text, _>(&action.name)
        .bind::<Integer, _>(depth)
        .bind::<Nullable<BigInt>, _>(begin_seq)
        .bind::<Nullable<BigInt>, _>(now_sampled)
        .bind::<Nullable<BigInt>, _>(end_now)
        .bind::<Text, _>(action.params.to_string())
        .bind::<Text, _>(post.to_string())
        .execute(connection)
        .await
        .unwrap_or_else(|error| panic!("trace-model: failed to insert a trace row: {error}"));
}

async fn post_images(
    connection: &mut DurableConnection,
    scope: &Scope,
) -> Result<Value, diesel::result::Error> {
    let mut workflows = Map::new();
    for id in &scope.workflows {
        let row = durable_workflow::table
            .find(*id)
            .select(WorkflowRow::as_select())
            .first::<WorkflowRow>(connection)
            .await
            .optional()?;
        workflows.insert(
            id.to_string(),
            row.as_ref().map_or(Value::Null, workflow_image),
        );
    }
    let mut activities = Map::new();
    for id in &scope.activities {
        let row = durable_activity::table
            .find(*id)
            .select(ActivityRow::as_select())
            .first::<ActivityRow>(connection)
            .await
            .optional()?;
        activities.insert(
            id.to_string(),
            row.as_ref().map_or(Value::Null, activity_image),
        );
    }
    let mut attempts = Map::new();
    for (activity_id, attempt_number) in &scope.attempts {
        let row = durable_activity_attempt::table
            .find((*activity_id, *attempt_number))
            .select(ActivityAttemptRow::as_select())
            .first::<ActivityAttemptRow>(connection)
            .await
            .optional()?;
        attempts.insert(
            format!("{activity_id}:{attempt_number}"),
            row.map_or(
                Value::Null,
                |row| json!({ "lease_token": row.lease_token, "finished_at": row.finished_at }),
            ),
        );
    }
    let mut events = Vec::with_capacity(scope.events.len());
    for ((workflow_id, delivery_sequence), event_type) in &scope.events {
        let row = durable_workflow_event::table
            .filter(durable_workflow_event::workflow_id.eq(*workflow_id))
            .filter(durable_workflow_event::delivery_sequence.eq(Some(*delivery_sequence)))
            .select(WorkflowEventRow::as_select())
            .first::<WorkflowEventRow>(connection)
            .await
            .optional()?;
        events.push(json!({
            "wf": workflow_id,
            "dseq": delivery_sequence,
            "type": event_type,
            "cmd": row.as_ref().map_or(0, |row| event_data(row, "command_sequence").and_then(|value| value.as_i64()).unwrap_or(0)),
            "category": row.as_ref().and_then(|row| event_data(row, "category")),
        }));
    }
    Ok(json!({
        "wf": workflows,
        "act": activities,
        "att": attempts,
        "events": events,
    }))
}

fn workflow_image(row: &WorkflowRow) -> Value {
    json!({
        "status": row.status.as_str(),
        "kind": row.kind,
        "wait_kind": row.wait_kind,
        "wait_reference_id": row.wait_reference_id,
        "available_at": row.available_at,
        "lease_token": row.lease_token,
        "lease_expires_at": row.lease_expires_at,
        "command_sequence": row.command_sequence,
        "delivered_event_sequence": row.delivered_event_sequence,
        "activation_attempts": row.activation_attempts,
        "deduplication_key": row.deduplication_key,
        "parent_workflow_id": row.parent_workflow_id,
        "root_workflow_id": row.root_workflow_id,
        "restarted_from_workflow_id": row.restarted_from_workflow_id,
    })
}

fn activity_image(row: &ActivityRow) -> Value {
    json!({
        "status": row.status.as_str(),
        "workflow_id": row.workflow_id,
        "topic": row.topic,
        "attempt_count": row.attempt_count,
        "max_attempts": row.max_attempts,
        "available_at": row.available_at,
        "lease_token": row.lease_token,
        "lease_expires_at": row.lease_expires_at,
        "timeout_millis": row.timeout_millis,
        "lease_duration_millis": row.lease_duration_millis,
    })
}

/// A field of a typed event's data (`{"type": .., "data": {..}}`); `None` for
/// `started` and `continued`.
fn event_data(row: &WorkflowEventRow, field: &str) -> Option<Value> {
    row.metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<Value>(json).ok())
        .and_then(|event| event.pointer(&format!("/data/{field}")).cloned())
}

#[cfg(test)]
mod tests {
    //! Declaration coverage (design §11 phase 2): every library transaction
    //! declares its model step, directly or through a named callee.

    use std::path::{Path, PathBuf};

    /// `(file, enclosing fn, callee, reason)`: transactions whose declaration
    /// lives in a callee. The callee must be called in the enclosing function
    /// and must itself declare (directly or through another entry).
    const DELEGATED: &[(&str, &str, &str, &str)] = &[
        (
            "src/runtime/activity_worker.rs",
            "finish_claim",
            "finish_on_connection",
            "TW3_Finish is declared per outcome branch",
        ),
        (
            "src/runtime/coordinator.rs",
            "commit_transition",
            "commit_on_connection",
            "TC2_Commit is declared per transition",
        ),
        (
            "src/store.rs",
            "start",
            "start_with_conn",
            "outermost wrapper; start_with_conn runs as a savepoint inside it",
        ),
        (
            "src/store.rs",
            "start_with_conn",
            "insert_prepared",
            "TX1_Start (and its Conflict rollback) is declared by insert_prepared",
        ),
        (
            "src/store.rs",
            "start_prepared_with_conn",
            "insert_prepared",
            "TX1_Start is declared by insert_prepared",
        ),
        (
            "src/store.rs",
            "start_or_restart_recoverable",
            "start_or_restart_recoverable_with_conn",
            "outermost wrapper; the savepoint inside declares TX2_RecoverableStart",
        ),
    ];

    struct Function {
        file: String,
        name: String,
        body: String,
    }

    fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("source dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// Functions with their bodies (naive brace matching; good enough for this crate).
    fn functions(file: &str, text: &str) -> Vec<(usize, Function)> {
        let mut found = Vec::new();
        let bytes = text.as_bytes();
        let mut search = 0;
        while let Some(offset) = text[search..].find("fn ") {
            let start = search + offset;
            search = start + 3;
            if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
                continue;
            }
            let name: String = text[start + 3..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            let Some(open) = text[start..].find(['{', ';']).map(|at| start + at) else {
                continue;
            };
            if bytes[open] == b';' {
                continue;
            }
            let mut depth = 0usize;
            let mut end = open;
            for (index, byte) in bytes.iter().enumerate().skip(open) {
                match byte {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            end = index;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            found.push((
                start,
                Function {
                    file: file.to_string(),
                    name,
                    body: text[open..=end].to_string(),
                },
            ));
        }
        found
    }

    /// A `trace::declare*` call, or a call of a local `declare_*` helper that makes one.
    fn declares_directly(body: &str, helpers: &[String]) -> bool {
        body.contains("trace::declare")
            || helpers
                .iter()
                .any(|helper| body.contains(&format!("{helper}(")))
    }

    #[test]
    fn every_transaction_site_declares() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut files = Vec::new();
        sources(&root.join("src"), &mut files);
        files.sort();
        let mut all = Vec::new();
        let mut sites = Vec::new();
        for path in &files {
            let file = path
                .strip_prefix(root)
                .expect("under the crate")
                .to_string_lossy()
                .replace('\\', "/");
            // The hook itself and the recorder.
            if file.starts_with("src/dialect/") || file.starts_with("src/trace/") {
                continue;
            }
            let text = std::fs::read_to_string(path).expect("source");
            let text = text
                .split("\n#[cfg(test)]")
                .next()
                .unwrap_or(&text)
                .to_string();
            let functions = functions(&file, &text);
            let mut markers = vec!["dialect::transaction("];
            if file == "src/store.rs" {
                markers.push(".transaction(async move");
            }
            for marker in markers {
                for (at, _) in text.match_indices(marker) {
                    // The innermost function whose body spans the call.
                    let enclosing = functions
                        .iter()
                        .filter(|(start, function)| {
                            *start < at && start + function.body.len() + 200 > at
                        })
                        .filter(|(start, function)| {
                            let open = text[*start..].find('{').map_or(0, |o| start + o);
                            open < at && at < open + function.body.len()
                        })
                        .max_by_key(|(start, _)| *start)
                        .map(|(_, function)| function.name.clone())
                        .unwrap_or_else(|| panic!("{file}: no function encloses offset {at}"));
                    sites.push((file.clone(), enclosing));
                }
            }
            all.extend(functions.into_iter().map(|(_, function)| function));
        }
        assert!(
            sites.len() >= 26,
            "found only {} transaction sites",
            sites.len()
        );
        let helpers: Vec<String> = all
            .iter()
            .filter(|function| {
                function.name.starts_with("declare_") && function.body.contains("trace::declare")
            })
            .map(|function| function.name.clone())
            .collect();

        let body_of = |file: &str, name: &str| {
            all.iter()
                .find(|function| function.file == file && function.name == name)
                .map(|function| function.body.as_str())
        };
        let callee_declares = |callee: &str, depth: usize| -> bool {
            fn go(all: &[Function], helpers: &[String], callee: &str, depth: usize) -> bool {
                depth < 4
                    && all.iter().filter(|f| f.name == callee).any(|f| {
                        declares_directly(&f.body, helpers)
                            || DELEGATED.iter().any(|(file, name, next, _)| {
                                *file == f.file
                                    && *name == f.name
                                    && go(all, helpers, next, depth + 1)
                            })
                    })
            }
            go(&all, &helpers, callee, depth)
        };

        let mut missing = Vec::new();
        let mut used = vec![false; DELEGATED.len()];
        for (file, name) in &sites {
            let body = body_of(file, name).expect("enclosing body");
            if declares_directly(body, &helpers) {
                continue;
            }
            let delegated = DELEGATED.iter().enumerate().find(|(_, (f, n, callee, _))| {
                f == file
                    && n == name
                    && body.contains(&format!("{callee}("))
                    && callee_declares(callee, 0)
            });
            match delegated {
                Some((index, _)) => used[index] = true,
                None => missing.push(format!("{file}: {name}")),
            }
        }
        assert!(
            missing.is_empty(),
            "transactions without a trace declaration (declare in the site or add a DELEGATED \
             entry with a reason):\n{}",
            missing.join("\n")
        );
        let stale: Vec<_> = DELEGATED
            .iter()
            .zip(&used)
            .filter(|(_, used)| !**used)
            .map(|((file, name, callee, _), _)| format!("{file}: {name} -> {callee}"))
            .collect();
        assert!(
            stale.is_empty(),
            "stale DELEGATED entries:\n{}",
            stale.join("\n")
        );
    }
}
