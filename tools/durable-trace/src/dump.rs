//! `dump`: reads `durable_trace`, `durable_trace_meta` and `durable_topic_lock`
//! from every recorded test database and writes the §4 trace JSON.

use std::{collections::BTreeMap, path::Path};

use diesel::{
    sql_types::{BigInt, Integer, Nullable, Text},
    QueryableByName,
};
use diesel_async::{AsyncConnection, RunQueryDsl};
use durable_workflows::{BackendKind, DurableConnection, BACKEND};
use serde_json::{json, Map, Value};

#[derive(QueryableByName)]
struct Name {
    #[diesel(sql_type = Text)]
    name: String,
}

#[derive(QueryableByName)]
struct TraceRow {
    #[diesel(sql_type = BigInt)]
    seq: i64,
    #[diesel(sql_type = Text)]
    txn: String,
    #[diesel(sql_type = Text)]
    actor: String,
    #[diesel(sql_type = Text)]
    action: String,
    #[diesel(sql_type = Integer)]
    depth: i32,
    #[diesel(sql_type = Nullable<BigInt>)]
    begin_seq: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    now_sampled: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    end_now: Option<i64>,
    #[diesel(sql_type = Text)]
    params_json: String,
    #[diesel(sql_type = Text)]
    post_json: String,
}

#[derive(QueryableByName)]
struct MetaRow {
    #[diesel(sql_type = Text)]
    k: String,
    #[diesel(sql_type = Text)]
    v: String,
}

#[derive(QueryableByName)]
struct TopicCap {
    #[diesel(sql_type = Text)]
    topic: String,
    #[diesel(sql_type = Integer)]
    max_concurrency: i32,
}

pub async fn run(
    server: &str,
    out: &Path,
    only_db: Option<&str>,
    only_test: Option<&str>,
) -> Result<(), String> {
    let mut databases = match only_db {
        Some(name) => vec![name.to_string()],
        None => trace_databases(server).await?,
    };
    // Oldest first, so a newer recording of the same test replaces an older one.
    databases.sort_by_key(|name| created_millis(name));
    std::fs::create_dir_all(out).map_err(|error| format!("{}: {error}", out.display()))?;

    let mut traces: BTreeMap<String, (String, Value)> = BTreeMap::new();
    for database in databases {
        let trace = read_trace(server, &database).await?;
        let test = trace["test"].as_str().unwrap_or(&database).to_string();
        if only_test.is_some_and(|filter| !test.contains(filter)) {
            continue;
        }
        let file = format!("{}.json", crate::sanitize(&test));
        if let Some((older, _)) = traces.insert(file.clone(), (database.clone(), trace)) {
            eprintln!("{file}: {database} replaces the older recording {older}");
        }
    }
    for (file, (database, trace)) in &traces {
        let path = out.join(file);
        let text = serde_json::to_string_pretty(trace).map_err(|error| error.to_string())?;
        std::fs::write(&path, text + "\n")
            .map_err(|error| format!("{}: {error}", path.display()))?;
        println!(
            "{} <- {database} ({} records)",
            path.display(),
            trace["records"].as_array().map_or(0, Vec::len)
        );
    }
    if traces.is_empty() {
        eprintln!("no recorded trace matched");
    }
    Ok(())
}

async fn connect(url: &str) -> Result<DurableConnection, String> {
    DurableConnection::establish(url)
        .await
        .map_err(|error| format!("cannot connect to {}: {error}", redact(url)))
}

/// Test databases (`dwt_%`) that hold a `durable_trace` table.
async fn trace_databases(server: &str) -> Result<Vec<String>, String> {
    let mut connection = connect(server).await?;
    match BACKEND {
        BackendKind::Mysql => {
            let rows: Vec<Name> = diesel::sql_query(
                "SELECT table_schema AS name FROM information_schema.tables \
                 WHERE table_name = 'durable_trace' AND table_schema LIKE 'dwt\\_%'",
            )
            .load(&mut connection)
            .await
            .map_err(|error| format!("listing trace databases: {error}"))?;
            Ok(rows.into_iter().map(|row| row.name).collect())
        }
        BackendKind::Postgres => {
            let rows: Vec<Name> = diesel::sql_query(
                "SELECT datname AS name FROM pg_database WHERE datname LIKE 'dwt\\_%'",
            )
            .load(&mut connection)
            .await
            .map_err(|error| format!("listing test databases: {error}"))?;
            let mut found = Vec::new();
            for row in rows {
                let mut database = connect(&with_database_name(server, &row.name)).await?;
                let tables: Vec<Name> = diesel::sql_query(
                    "SELECT table_name::text AS name FROM information_schema.tables \
                     WHERE table_schema = current_schema() AND table_name = 'durable_trace'",
                )
                .load(&mut database)
                .await
                .map_err(|error| format!("{}: {error}", row.name))?;
                if !tables.is_empty() {
                    found.push(row.name);
                }
            }
            Ok(found)
        }
    }
}

async fn read_trace(server: &str, database: &str) -> Result<Value, String> {
    let mut connection = connect(&with_database_name(server, database)).await?;
    let txn = match BACKEND {
        BackendKind::Mysql => "txn_id",
        BackendKind::Postgres => "txn_id::text",
    };
    let rows: Vec<TraceRow> = diesel::sql_query(format!(
        "SELECT seq, {txn} AS txn, actor, action, depth, begin_seq, now_sampled, end_now, \
         params_json, post_json FROM durable_trace ORDER BY seq"
    ))
    .load(&mut connection)
    .await
    .map_err(|error| format!("{database}: reading durable_trace: {error}"))?;
    let meta: Vec<MetaRow> = diesel::sql_query("SELECT k, v FROM durable_trace_meta")
        .load(&mut connection)
        .await
        .map_err(|error| format!("{database}: reading durable_trace_meta: {error}"))?;
    let caps: Vec<TopicCap> =
        diesel::sql_query("SELECT topic, max_concurrency FROM durable_topic_lock ORDER BY topic")
            .load(&mut connection)
            .await
            .map_err(|error| format!("{database}: reading durable_topic_lock: {error}"))?;

    let meta: BTreeMap<String, String> = meta.into_iter().map(|row| (row.k, row.v)).collect();
    let topics: Map<String, Value> = caps
        .into_iter()
        .map(|cap| (cap.topic, cap.max_concurrency.into()))
        .collect();
    let backend = match BACKEND {
        BackendKind::Mysql => "mysql",
        BackendKind::Postgres => "postgres",
    };
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let params: Value = serde_json::from_str(&row.params_json)
            .map_err(|error| format!("{database} seq {}: params_json: {error}", row.seq))?;
        let mut post: Value = serde_json::from_str(&row.post_json)
            .map_err(|error| format!("{database} seq {}: post_json: {error}", row.seq))?;
        if row.action == "External" {
            post = external_post(&params, &post["row"])
                .map_err(|error| format!("{database} seq {}: {error}", row.seq))?;
        }
        records.push(json!({
            "seq": row.seq,
            "txn": row.txn.trim(),
            "actor": row.actor,
            "action": row.action,
            "depth": row.depth,
            "begin_seq": row.begin_seq,
            "now": row.now_sampled,
            "end_now": row.end_now,
            "params": params,
            "post": post,
        }));
    }
    Ok(json!({
        "schema": 1,
        "test": meta.get("test").cloned().unwrap_or_else(|| database.to_string()),
        "backend": meta.get("backend").map_or(backend, String::as_str),
        "database": database,
        "topics": topics,
        "records": records,
    }))
}

/// The post-image of an `External` record (a row a trigger captured outside
/// any traced transaction) in the shape the recorder writes: one row, `null`
/// for a delete. An event image also carries its typed `data`.
fn external_post(params: &Value, row: &Value) -> Result<Value, String> {
    let deleted = params["op"] == "delete";
    let image = |columns: &[&str]| {
        if deleted {
            return Value::Null;
        }
        Value::Object(
            columns
                .iter()
                .map(|column| ((*column).to_string(), row[*column].clone()))
                .collect(),
        )
    };
    let id = |column: &str| {
        row[column]
            .as_i64()
            .ok_or_else(|| format!("external row without {column}"))
    };
    let mut post = json!({ "wf": {}, "act": {}, "att": {}, "events": [] });
    match params["table"].as_str().unwrap_or("") {
        "durable_workflow" => {
            post["wf"][id("id")?.to_string()] = image(&[
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
            ]);
        }
        "durable_activity" => {
            post["act"][id("id")?.to_string()] = image(&[
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
            ]);
        }
        "durable_activity_attempt" => {
            let key = format!("{}:{}", id("activity_id")?, id("attempt_number")?);
            post["att"][key] = image(&["lease_token", "finished_at"]);
        }
        "durable_workflow_event" => {
            let event: Value = row["metadata_json"]
                .as_str()
                .and_then(|text| serde_json::from_str(text).ok())
                .unwrap_or(Value::Null);
            post["events"] = json!([{
                "wf": id("workflow_id")?,
                "dseq": id("delivery_sequence")?,
                "type": row["event_type"],
                "cmd": event.pointer("/data/command_sequence").and_then(Value::as_i64).unwrap_or(0),
                "category": event.pointer("/data/category").cloned().unwrap_or(Value::Null),
                "data": event.get("data").cloned().unwrap_or(Value::Null),
            }]);
        }
        other => return Err(format!("external write to unknown table {other}")),
    }
    Ok(post)
}

/// Trailing `_<millis>` of a fixture database name (`dwt_<pid>_<n>_<millis>`).
fn created_millis(name: &str) -> u128 {
    name.rsplit('_')
        .next()
        .and_then(|millis| millis.parse().ok())
        .unwrap_or(0)
}

fn with_database_name(url: &str, name: &str) -> String {
    let path_start = url
        .find("://")
        .and_then(|scheme_end| {
            url[scheme_end + 3..]
                .find('/')
                .map(|slash| scheme_end + 3 + slash)
        })
        .unwrap_or(url.len());
    let query_start = url[path_start..]
        .find('?')
        .map_or(url.len(), |offset| path_start + offset);
    format!("{}/{name}{}", &url[..path_start], &url[query_start..])
}

fn redact(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end => {
            format!("{}://***{}", &url[..scheme_end], &url[at..])
        }
        _ => url.to_string(),
    }
}
