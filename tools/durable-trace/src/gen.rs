//! `gen`: translates trace JSON into Quint runs (design §5, §6, §8) against
//! the model interface in `spec/README.md` "Trace-checking interface".
//!
//! Each record becomes `run step_k = step_{k-1}.then(all { keepPrev, Action(..) })
//! .expect(..)`, where the expectation compares the model's view of every
//! touched row with the abstraction of the recorded post-image. A trace with
//! a record the generator cannot replay is excluded with a reason.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Write as _,
    hash::Hash,
    path::{Path, PathBuf},
};

use serde_json::{json, Map, Value};

/// `TRACE_IFACE_VERSION` in `spec/durable.qnt` this generator targets.
const IFACE_VERSION: i64 = 4;

/// Invariants checked after every step.
const STEP_INVARIANTS: &[&str] = &["safety", "inv_S17_capAtClaim"];

pub fn run(input: &Path, out: &Path, expect_violation: Option<&Path>) -> Result<(), String> {
    let model = check_interface_version(out)?;
    let safety = safety_conjuncts(&model)?;
    let gaps = match expect_violation {
        Some(path) => read_gaps(path)?,
        None => Vec::new(),
    };
    std::fs::create_dir_all(out).map_err(|error| format!("{}: {error}", out.display()))?;
    let mut entries = Vec::new();
    for path in trace_files(input)? {
        let text = std::fs::read_to_string(&path)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        let trace: Value =
            serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        let stem = path
            .file_stem()
            .map_or_else(String::new, |stem| stem.to_string_lossy().into_owned());
        let test = trace["test"].as_str().unwrap_or(&stem).to_string();
        let backend = trace["backend"].as_str().unwrap_or("unknown").to_string();
        let module = format!(
            "trace_{}_{}",
            crate::sanitize(&backend),
            crate::sanitize(&test)
        )
        .replace('-', "_");
        let file = format!("{module}.qnt");
        let mut entry = json!({
            "test": test,
            "backend": backend,
            // Absolute, so trace-check.sh can read it from spec/.
            "source": path.canonicalize().unwrap_or_else(|_| path.clone()).display().to_string(),
            "module": module,
            "file": file,
        });
        let expected = gaps
            .iter()
            .find(|(pattern, _)| test_matches(pattern, &test))
            .map(|(_, invariant)| invariant.clone());
        if let Some(reason) = expected
            .as_deref()
            .and_then(|value| value.strip_prefix("exclude:"))
        {
            println!("{}: excluded ({reason})", path.display());
            entry["verdict"] = "excluded".into();
            entry["reason"] = reason.into();
            entries.push(entry);
            continue;
        }
        let invariants = step_invariants(&safety, expected.as_deref());
        let separate: Vec<String> = invariants
            .iter()
            .flat_map(|name| {
                if name == "safety" {
                    safety.clone()
                } else {
                    vec![name.clone()]
                }
            })
            .collect();
        match generate(
            &trace,
            &module,
            &path,
            &invariants,
            &separate,
            expected.as_deref(),
        ) {
            Ok(generated) => {
                let target = out.join(&file);
                std::fs::write(&target, generated.text)
                    .map_err(|error| format!("{}: {error}", target.display()))?;
                entry["verdict"] = if let Some(invariant) = &expected {
                    entry["expect_violation"] = invariant.as_str().into();
                    "expect_violation".into()
                } else {
                    "expect_pass".into()
                };
                entry["steps"] = generated.step_seqs.len().into();
                entry["step_seqs"] = generated.step_seqs.into();
                entry["step_class"] = generated.step_class.into();
                println!(
                    "{}: {} ({} steps)",
                    target.display(),
                    entry["verdict"].as_str().unwrap_or("?"),
                    entry["steps"]
                );
            }
            Err(reason) => {
                println!("{}: excluded ({reason})", path.display());
                entry["verdict"] = "excluded".into();
                entry["reason"] = reason.into();
            }
        }
        entries.push(entry);
    }
    let index = out.join("index.json");
    let text = serde_json::to_string_pretty(&json!({ "schema": 1, "traces": entries }))
        .map_err(|error| error.to_string())?;
    std::fs::write(&index, text + "\n").map_err(|error| format!("{}: {error}", index.display()))
}

/// Invariants each step checks: `safety` and `inv_S17_capAtClaim`, with
/// `safety` spelled out as its conjuncts minus `expected` when that
/// invariant is one of them.
fn step_invariants(safety: &[String], expected: Option<&str>) -> Vec<String> {
    match expected {
        Some(invariant) if safety.iter().any(|name| name == invariant) => safety
            .iter()
            .filter(|name| *name != invariant)
            .cloned()
            .chain(
                STEP_INVARIANTS
                    .iter()
                    .filter(|name| **name != "safety" && **name != invariant)
                    .map(|name| (*name).to_string()),
            )
            .collect(),
        Some(invariant) => STEP_INVARIANTS
            .iter()
            .filter(|name| **name != invariant)
            .map(|name| (*name).to_string())
            .collect(),
        None => STEP_INVARIANTS
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    }
}

/// The conjuncts of `val safety = and { .. }` in the model.
fn safety_conjuncts(model: &str) -> Result<Vec<String>, String> {
    let start = model
        .find("val safety = and {")
        .ok_or("the model has no `val safety = and {`")?;
    let body = &model[start + "val safety = and {".len()..];
    let end = body.find('}').ok_or("unterminated `safety`")?;
    Ok(body[..end]
        .split([',', '\n'])
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect())
}

/// `(test-name glob, invariant)` pairs from a JSON object or a flat YAML
/// mapping (`pattern: invariant` lines; `#` comments).
fn read_gaps(path: &Path) -> Result<Vec<(String, String)>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if text.trim_start().starts_with('{') {
        let map: Map<String, Value> =
            serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        return map
            .into_iter()
            .map(|(pattern, invariant)| {
                invariant
                    .as_str()
                    .map(|invariant| (pattern.clone(), invariant.to_string()))
                    .ok_or_else(|| format!("{}: {pattern}: not a string", path.display()))
            })
            .collect();
    }
    let unquote = |value: &str| value.trim().trim_matches(['"', '\'']).to_string();
    text.lines()
        .map(|line| line.split_once(" #").map_or(line, |(line, _)| line))
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .map(|line| {
            line.split_once(": ")
                .map(|(pattern, invariant)| (unquote(pattern), unquote(invariant)))
                .filter(|(pattern, invariant)| !pattern.is_empty() && !invariant.is_empty())
                .ok_or_else(|| format!("{}: not `pattern: invariant`: {line}", path.display()))
        })
        .collect()
}

/// Whether the glob (`*` = any text) matches the test name or its last path segment.
fn test_matches(pattern: &str, test: &str) -> bool {
    fn glob(pattern: &[u8], text: &[u8]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some((b'*', rest)) => (0..=text.len()).any(|skip| glob(rest, &text[skip..])),
            Some((byte, rest)) => text.first() == Some(byte) && glob(rest, &text[1..]),
        }
    }
    let last = test.rsplit("::").next().unwrap_or(test);
    glob(pattern.as_bytes(), test.as_bytes()) || glob(pattern.as_bytes(), last.as_bytes())
}

/// The generated modules import `../durable`, so `out` must sit next to the model.
/// Returns the model text.
fn check_interface_version(out: &Path) -> Result<String, String> {
    let model = out.join("../durable.qnt");
    let text = std::fs::read_to_string(&model).map_err(|error| {
        format!(
            "{}: {error} (--out must be spec/traces, next to the model)",
            model.display()
        )
    })?;
    let version = text
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("pure val TRACE_IFACE_VERSION =")
                .and_then(|value| value.trim().parse::<i64>().ok())
        })
        .ok_or_else(|| format!("{}: no TRACE_IFACE_VERSION", model.display()))?;
    if version == IFACE_VERSION {
        Ok(text)
    } else {
        Err(format!(
            "the model's TRACE_IFACE_VERSION is {version}; this generator supports {IFACE_VERSION}"
        ))
    }
}

fn trace_files(input: &Path) -> Result<Vec<PathBuf>, String> {
    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(input)
        .map_err(|error| format!("{}: {error}", input.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "json")
                && path.file_name().is_some_and(|name| name != "index.json")
        })
        .collect();
    files.sort();
    Ok(files)
}

struct Generated {
    text: String,
    step_seqs: Vec<i64>,
    step_class: Vec<&'static str>,
}

struct Step {
    seq: i64,
    class: &'static str,
    label: String,
    call: String,
    expect: Vec<String>,
}

/// Dense 1..n numbering in order of first appearance.
struct Interner<K> {
    ids: HashMap<K, i64>,
}

impl<K: Eq + Hash> Interner<K> {
    fn new() -> Self {
        Self {
            ids: HashMap::new(),
        }
    }

    fn id(&mut self, key: K) -> i64 {
        let next = i64::try_from(self.ids.len()).unwrap_or(i64::MAX) + 1;
        *self.ids.entry(key).or_insert(next)
    }

    fn len(&self) -> i64 {
        i64::try_from(self.ids.len()).unwrap_or(i64::MAX)
    }
}

struct Ctx {
    wf: Interner<i64>,
    act: Interner<i64>,
    tok: Interner<String>,
    rt: Interner<String>,
    key: Interner<String>,
    topics: BTreeSet<String>,
    versions: HashMap<(String, String), i64>,
    max_attempts: i64,
    /// Largest `activation_attempts` seen; `MAX_ACTIVATION` when no cap is recorded.
    seen_activation: i64,
    /// Largest activation cap the coordinator recorded (T-C1, T-C3).
    max_activation: Option<i64>,
    local_slots: i64,
    max_now: i64,
    /// Heartbeat ids in `TW2_Send` order (`ghost.nextHb` starts at 1).
    hb: Interner<i64>,
    /// Heartbeats whose `TW2_Commit` is in the trace: their `TW2_Drop` is void.
    committed_hbs: BTreeSet<i64>,
    /// Activities (real ids) the model holds with invalid timeout/lease bounds.
    invalid_bounds: BTreeSet<i64>,
    /// The trace needs `ENABLE_ENV_EDITS` (an external step or an invalid-bounds insert).
    env_edits: bool,
    /// Invariants every step checks.
    invariants: Vec<String>,
}

type Excluded = String;

fn generate(
    trace: &Value,
    module: &str,
    source: &Path,
    invariants: &[String],
    separate: &[String],
    expected: Option<&str>,
) -> Result<Generated, Excluded> {
    if trace["schema"].as_i64() != Some(1) {
        return Err("malformed:schema".into());
    }
    let records = trace["records"]
        .as_array()
        .ok_or_else(|| "malformed:records".to_string())?;
    if records.is_empty() {
        return Err("empty".into());
    }
    let caps = trace["topics"].as_object().cloned().unwrap_or_default();

    let mut ctx = Ctx {
        wf: Interner::new(),
        act: Interner::new(),
        tok: Interner::new(),
        rt: Interner::new(),
        key: Interner::new(),
        topics: collect_topics(&caps, records),
        versions: HashMap::new(),
        max_attempts: 1,
        seen_activation: 1,
        max_activation: None,
        local_slots: 1,
        max_now: 0,
        hb: Interner::new(),
        committed_hbs: records
            .iter()
            .filter(|record| record["action"] == "TW2_Commit")
            .filter_map(|record| record["params"]["hb"].as_i64())
            .collect(),
        invalid_bounds: BTreeSet::new(),
        env_edits: false,
        invariants: invariants.to_vec(),
    };
    let mut steps = Vec::new();
    for record in records {
        if let Some(step) = external_step(&mut ctx, record)? {
            steps.push(step);
        }
        if let Some(step) = translate(&mut ctx, record)? {
            steps.push(step);
        }
    }
    if steps.is_empty() {
        return Err("empty".into());
    }
    // Consecutive external writes are one environment step for the invariants:
    // no recorded step observes the states between them (a test inserts an
    // activity, then points its workflow at it).
    for index in 0..steps.len().saturating_sub(1) {
        if steps[index].class == "external" && steps[index + 1].class == "external" {
            steps[index]
                .expect
                .retain(|clause| !invariants.contains(clause));
        }
    }
    let crash = records.iter().any(|record| record["action"] == "Crash");

    let mut topics = ctx.topics.clone();
    if topics.is_empty() {
        // TOPICS must be a non-empty typed set; a placeholder topic has no activities.
        topics.insert("_none".to_string());
    }
    // The worker writes durable_topic_lock; without a T-W1 the cap constrains nothing.
    let claims = records.iter().any(|record| record["action"] == "TW1_Claim");
    let mut topic_caps = Vec::new();
    for topic in &topics {
        let cap = if (topic == "_none" && ctx.topics.is_empty()) || !claims {
            caps.get(topic).and_then(Value::as_i64).unwrap_or(1)
        } else {
            caps.get(topic)
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("missing_topic_cap:{topic}"))?
        };
        topic_caps.push(format!("{} -> {cap}", quote(topic)));
    }
    let runtimes = (1..=ctx.rt.len().max(1))
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let mut text = String::new();
    let test = trace["test"].as_str().unwrap_or("");
    let backend = trace["backend"].as_str().unwrap_or("");
    let _ = writeln!(
        text,
        "// Generated by `durable-trace gen` from {}. Do not edit.\n// test: {test}\n// backend: {backend}",
        source.display()
    );
    let _ = writeln!(text, "module {module} {{");
    let _ = writeln!(text, "  import durable(");
    let _ = writeln!(
        text,
        "    RUNTIMES = Set({runtimes}), MAX_WF = {}, MAX_ACT = {},",
        ctx.wf.len().max(1),
        ctx.act.len().max(1)
    );
    let _ = writeln!(
        text,
        "    TOPICS = Set({}), TOPIC_CAP = Map({}),",
        topics
            .iter()
            .map(|t| quote(t))
            .collect::<Vec<_>>()
            .join(", "),
        topic_caps.join(", ")
    );
    let _ = writeln!(
        text,
        "    LOCAL_SLOTS = {}, MAX_ATTEMPTS = {}, MAX_ACTIVATION = {}, LEASE_W = 1, LEASE_A = 1, MAX_TIME = {},",
        ctx.local_slots,
        ctx.max_attempts,
        ctx.max_activation.unwrap_or(ctx.seen_activation),
        ctx.max_now
    );
    let _ = writeln!(
        text,
        "    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = {crash}, ENABLE_ENV_EDITS = {},",
        ctx.env_edits
    );
    // RuntimeConfig::default().max_task_restarts; the recorder does not see the runtime config.
    let _ = writeln!(text, "    MAX_TASK_RESTARTS = 8");
    let _ = writeln!(text, "  ).* from \"../durable\"\n");
    for (index, step) in steps.iter().enumerate() {
        let k = index + 1;
        let previous = if k == 1 {
            "init".to_string()
        } else {
            format!("step_{}", k - 1)
        };
        let _ = writeln!(
            text,
            "  // seq {} ({}): {}",
            step.seq, step.class, step.label
        );
        let _ = writeln!(
            text,
            "  run step_{k} = {previous}.then(all {{ keepPrev, {} }})",
            step.call
        );
        let _ = writeln!(text, "    .expect(and {{");
        for clause in &step.expect {
            let _ = writeln!(text, "      {clause},");
        }
        let _ = writeln!(text, "    }})\n");
    }
    // Which invariant an external step breaks, for trace-check.sh (a test's
    // own setup that breaks one is `external_invariant:<inv>`, not a FAIL).
    for (index, step) in steps.iter().enumerate() {
        let group_end = steps
            .get(index + 1)
            .is_none_or(|next| next.class != "external");
        if step.class != "external" || !group_end {
            continue;
        }
        let k = index + 1;
        let previous = if k == 1 {
            "init".to_string()
        } else {
            format!("step_{}", k - 1)
        };
        for invariant in separate {
            let _ = writeln!(
                text,
                "  run extinv_{k}_{invariant} = {previous}.then(all {{ keepPrev, {} }}).expect({invariant})",
                step.call
            );
        }
    }
    // A gap violation can be transient (N2 frees the slot only while the
    // handler runs): trace-check.sh confirms it when some `viol_k` passes.
    if let Some(invariant) = expected {
        for k in 1..=steps.len() {
            let _ = writeln!(text, "  run viol_{k} = step_{k}.expect(not({invariant}))");
        }
    }
    let _ = writeln!(text, "\n  run trace = step_{}", steps.len());
    let _ = writeln!(text, "}}");
    Ok(Generated {
        text,
        step_seqs: steps.iter().map(|step| step.seq).collect(),
        step_class: steps.iter().map(|step| step.class).collect(),
    })
}

/// The environment step an unrecorded external write implies, placed just
/// before the record that exposes it: a `TW1_Error{invalid_bounds}` naming an
/// activity the model still holds with valid bounds was preceded by a write the
/// recorder does not see (the G10 gap test edits the row with raw SQL).
fn external_step(ctx: &mut Ctx, record: &Value) -> Result<Option<Step>, Excluded> {
    let params = &record["params"];
    if record["action"] != "TW1_Error" || params["reason"] != "invalid_bounds" {
        return Ok(None);
    }
    let seq = record["seq"].as_i64().ok_or("malformed:seq")?;
    let id = int(params, "activity_id", seq)?;
    if !ctx.invalid_bounds.insert(id) {
        return Ok(None);
    }
    ctx.env_edits = true;
    let a = ctx.act.id(id);
    let mut expect = vec![format!(
        "lastAction == {}",
        quote("EnvCorruptActivityBounds")
    )];
    expect.extend(ctx.invariants.iter().cloned());
    expect.push(format!("viewAct(db, {a}).invalidBounds"));
    Ok(Some(Step {
        seq,
        class: "external",
        label: format!("external EnvCorruptActivityBounds (unrecorded write to activity {id})"),
        call: format!("EnvCorruptActivityBounds({a})"),
        expect,
    }))
}

/// A recorded external write (an `External` record: one row a trigger
/// captured outside any traced transaction) as its environment action.
fn env_step(ctx: &mut Ctx, record: &Value, seq: i64) -> Result<Step, Excluded> {
    ctx.env_edits = true;
    let params = &record["params"];
    let table = params["table"].as_str().unwrap_or("?");
    let op = params["op"].as_str().unwrap_or("?");
    let post = &record["post"];
    let one = |rows: &Value| {
        rows.as_object()
            .and_then(|rows| rows.iter().next())
            .map(|(id, row)| (id.clone(), row.clone()))
    };
    let (call, last, label) = if let Some((id, row)) = one(&post["wf"]) {
        let w = ctx.wf.id(parse_id(&id, seq)?);
        let view = if row.is_null() {
            "emptyWf".to_string()
        } else {
            wf_row(ctx, &row, seq)?
        };
        (
            format!("EnvSetWf({w}, {view})"),
            "EnvSetWf",
            format!("workflow {id}"),
        )
    } else if let Some((id, row)) = one(&post["act"]) {
        let real = parse_id(&id, seq)?;
        let a = ctx.act.id(real);
        let view = if row.is_null() {
            "emptyAct".to_string()
        } else {
            act_row(ctx, real, &row, seq)?
        };
        (
            format!("EnvSetAct({a}, {view})"),
            "EnvSetAct",
            format!("activity {id}"),
        )
    } else if let Some((id, row)) = one(&post["att"]) {
        let (activity, number) = id
            .split_once(':')
            .and_then(|(a, n)| Some((a.parse::<i64>().ok()?, n.parse::<i64>().ok()?)))
            .ok_or_else(|| format!("malformed:att_key:{seq}"))?;
        ctx.max_attempts = ctx.max_attempts.max(number);
        let a = ctx.act.id(activity);
        let view = if row.is_null() {
            "noAtt".to_string()
        } else {
            let tok = token(ctx, &row["lease_token"]);
            format!(
                "{{ used: true, token: {tok}, open: {} }}",
                row["finished_at"].is_null()
            )
        };
        (
            format!("EnvSetAtt({a}, {number}, {view})"),
            "EnvSetAtt",
            format!("attempt {id}"),
        )
    } else if let Some(event) = post["events"].as_array().and_then(|events| events.first()) {
        if op != "insert" {
            return Err(format!("external_write:{table}:{op}"));
        }
        let w = ctx.wf.id(int(event, "wf", seq)?);
        let typ = text(event, "type", seq)?;
        let reference = match typ {
            "started" | "continued" => 0,
            other => return Err(format!("external_write:{table}:event_ref:{other}")),
        };
        let view = format!(
            "{{ dseq: {}, typ: {}, ref: {reference}, cmd: {} }}",
            int(event, "dseq", seq)?,
            quote(typ),
            event["cmd"].as_i64().unwrap_or(0)
        );
        (
            format!("EnvAppendEvent({w}, {view})"),
            "EnvAppendEvent",
            format!("event of workflow {}", event["wf"]),
        )
    } else {
        return Err(format!("external_write:{table}:empty"));
    };
    let mut expect = vec![format!("lastAction == {}", quote(last))];
    expect.extend(ctx.invariants.iter().cloned());
    let mut rows = post.clone();
    rows["events"] = json!([]);
    expect.extend(post_expectations(
        ctx,
        &json!({ "post": rows, "params": {} }),
        seq,
    )?);
    if let Some(event) = post["events"].as_array().and_then(|events| events.first()) {
        let w = ctx.wf.id(int(event, "wf", seq)?);
        expect.push(format!(
            "deliverable(db, {w}).exists(e => e.dseq == {})",
            int(event, "dseq", seq)?
        ));
    }
    Ok(Step {
        seq,
        class: "external",
        label: format!("external {op} {label} ({table})"),
        call,
        expect,
    })
}

/// `strict` when no other transaction committed while this one ran
/// (`begin_seq == seq - 1`); local records have no window and count as strict.
pub(crate) fn classify(record: &Value) -> &'static str {
    let seq = record["seq"].as_i64().unwrap_or(0);
    match record["begin_seq"].as_i64() {
        None => "strict",
        Some(begin) if begin == seq - 1 => "strict",
        Some(_) => "concurrent",
    }
}

/// Every topic the trace mentions: the header caps and the records.
fn collect_topics(caps: &Map<String, Value>, records: &[Value]) -> BTreeSet<String> {
    let mut topics: BTreeSet<String> = caps.keys().cloned().collect();
    for record in records {
        let params = &record["params"];
        if let Some(topic) = params["topic"].as_str() {
            topics.insert(topic.to_string());
        }
        for key in ["in_flight_seen", "local_avail"] {
            if let Some(map) = params[key].as_object() {
                topics.extend(map.keys().cloned());
            }
        }
        if let Some(list) = params["topics"].as_array() {
            topics.extend(list.iter().filter_map(Value::as_str).map(str::to_string));
        }
        if let Some(rows) = record["post"]["act"].as_object() {
            topics.extend(
                rows.values()
                    .filter_map(|row| row["topic"].as_str())
                    .map(str::to_string),
            );
        }
    }
    topics
}

/// Whether the record touched a modeled row (workflow, activity, attempt,
/// deliverable event).
fn touches_modeled(record: &Value) -> bool {
    let post = &record["post"];
    ["wf", "act", "att"].iter().any(|table| {
        post[*table]
            .as_object()
            .is_some_and(|rows| !rows.is_empty())
    }) || post["events"]
        .as_array()
        .is_some_and(|events| !events.is_empty())
}

/// `None`: a stutter the model does not see (an unmodeled transaction that
/// writes no modeled table, or a `TW2_Drop` that is void).
fn translate(ctx: &mut Ctx, record: &Value) -> Result<Option<Step>, Excluded> {
    let seq = record["seq"].as_i64().ok_or("malformed:seq")?;
    let action = record["action"]
        .as_str()
        .ok_or_else(|| format!("malformed:action:{seq}"))?;
    let params = &record["params"];
    match action {
        "Unknown" => return Err("unknown_action".into()),
        "External" => return env_step(ctx, record, seq).map(Some),
        "Batch" => {
            let members = params["actions"].as_array().cloned().unwrap_or_default();
            return Err(members
                .iter()
                .find(|member| member["action"] == "Unmodeled")
                .map_or_else(
                    || "batch".to_string(),
                    |member| {
                        format!(
                            "unmodeled:{}",
                            member["params"]["name"].as_str().unwrap_or("Unmodeled")
                        )
                    },
                ));
        }
        _ => {}
    }
    if action.starts_with("Unmodeled") {
        let name = params["name"].as_str().unwrap_or(action);
        // Unmodeled writes to unmodeled tables only are stutters (design §0).
        if params["writes_modeled"] == false && !touches_modeled(record) {
            return Ok(None);
        }
        return Err(format!("unmodeled:{name}"));
    }
    if record["depth"].as_i64().unwrap_or(0) > 1 {
        return Err("deferred_commit".into());
    }
    let actor = record["actor"].as_str().unwrap_or("");
    let label = format!("{actor} {action}");

    let (call, last) = match action {
        "TX1_Start" => {
            let kind = text(params, "kind", seq)?;
            let id = int(params, "workflow_id", seq)?;
            let inserted = params["inserted"]
                .as_bool()
                .ok_or_else(|| format!("malformed:inserted:{seq}"))?;
            let key_text = params["dedup_key"].as_str();
            check_version(ctx, kind, key_text, params, seq)?;
            let key = key_id(ctx, key_text)?;
            let from = opt_int(params, "from")
                .or_else(|| {
                    record["post"]["wf"][id.to_string()]["restarted_from_workflow_id"].as_i64()
                })
                .map_or(0, |source| ctx.wf.id(source));
            // A dedup hit and a Conflict may not sample DB time.
            let tnow = tnow_or_model(ctx, record);
            if inserted {
                let w = ctx.wf.id(id);
                let tnow = inserted_tnow(ctx, record, id)?;
                (
                    format!(
                        "TX1_Start({w}, {}, {key}, {from}, true, {tnow})",
                        quote(kind)
                    ),
                    "TX1_Start",
                )
            } else if key != 0 {
                let w = ctx.wf.id(id);
                (
                    format!("TX1_Start({w}, {}, {key}, 0, false, {tnow})", quote(kind)),
                    "TX1_StartExisting",
                )
            } else if from != 0 {
                (
                    format!("TX1_Start(0, {}, 0, {from}, false, {tnow})", quote(kind)),
                    "TX1_StartConflict",
                )
            } else {
                return Err(format!("malformed:TX1_Start:{seq}"));
            }
        }
        "TX2_RecoverableStart" => {
            let kind = text(params, "kind", seq)?;
            let key_text = text(params, "dedup_key", seq)?;
            check_version(ctx, kind, Some(key_text), params, seq)?;
            let key = key_id(ctx, Some(key_text))?;
            let orig = opt_int(params, "original").map_or(0, |id| ctx.wf.id(id));
            let latest = opt_int(params, "latest").map_or(0, |id| ctx.wf.id(id));
            let superseded = flag(params, "superseded", seq)?;
            let inserted = flag(params, "inserted", seq)?;
            let conflict = flag(params, "conflict", seq)?;
            if orig == 0 && !inserted {
                // The locking read missed a concurrent insert; the model's T-X2 is atomic.
                return Err("concurrent:tx2_dedup_race".into());
            }
            let (s_new, tnow) = if inserted {
                let id = int(params, "workflow_id", seq)?;
                (ctx.wf.id(id), inserted_tnow(ctx, record, id)?)
            } else {
                (0, tnow_or_model(ctx, record))
            };
            let last = if conflict {
                "TX2_Conflict"
            } else if orig == 0 {
                "TX2_StartNew"
            } else if superseded {
                "TX2_RecoverableStart"
            } else {
                "TX2_ReturnLatest"
            };
            (
                format!(
                    "TX2_RecoverableStart({}, {key}, {orig}, {latest}, {superseded}, {s_new}, {tnow})",
                    quote(kind)
                ),
                last,
            )
        }
        "TX3_Cancel" => {
            let tnow = tnow(ctx, record)?;
            let w = ctx.wf.id(int(params, "workflow_id", seq)?);
            (format!("TX3_Cancel({w}, {tnow})"), "TX3_Cancel")
        }
        "AdminPause" | "AdminResume" | "AdminCancel" => {
            let tnow = tnow(ctx, record)?;
            let w = ctx.wf.id(int(params, "workflow_id", seq)?);
            let last = if action == "AdminPause" && params["paused_activity"].is_i64() {
                "AdminPauseActivity"
            } else {
                action
            };
            (format!("{action}({w}, {tnow})"), last)
        }
        "TC1_Claim" => {
            let tnow = tnow(ctx, record)?;
            let r = runtime(ctx, actor, seq)?;
            let recovered = opt_int(params, "recovered").map_or(0, |id| ctx.wf.id(id));
            let claimed = opt_int(params, "claimed").map_or(0, |id| ctx.wf.id(id));
            let token = token(ctx, &params["token"]);
            let lease = opt_int(params, "lease_expires_at").unwrap_or(0);
            if let Some(cap) = opt_int(params, "max_activation") {
                ctx.max_activation = Some(ctx.max_activation.unwrap_or(0).max(cap));
            }
            (
                format!("TC1_Claim({r}, {recovered}, {claimed}, {token}, {lease}, {tnow})"),
                if claimed == 0 {
                    "TC1_Recover"
                } else {
                    "TC1_Claim"
                },
            )
        }
        "TC2_Commit" => {
            let tnow = tnow(ctx, record)?;
            let r = runtime(ctx, actor, seq)?;
            let w = ctx.wf.id(int(params, "workflow_id", seq)?);
            let tok = token(ctx, &params["token"]);
            match text(params, "transition", seq)? {
                "run_activity" => {
                    let a = ctx.act.id(int(params, "activity_id", seq)?);
                    let topic = text(params, "topic", seq)?;
                    let max_attempts = int(params, "max_attempts", seq)?;
                    ctx.max_attempts = ctx.max_attempts.max(max_attempts);
                    let available_at = int(params, "available_at", seq)?;
                    // Continuation-priority activities are inserted at
                    // CONTINUATION_READY_AT_MILLIS (the model's CONTINUATION_READY_AT).
                    let prio = flag(params, "continuation_priority", seq)?;
                    let invalid = invalid_bounds(
                        &record["post"]["act"][int(params, "activity_id", seq)?.to_string()],
                    );
                    ctx.env_edits |= invalid;
                    (
                        format!(
                            "TC2_RunActivity({r}, {w}, {tok}, {a}, {}, {max_attempts}, {invalid}, {prio}, {available_at}, {tnow})",
                            quote(topic)
                        ),
                        "TC2_RunActivity",
                    )
                }
                "complete" => (
                    format!("TC2_Complete({r}, {w}, {tok}, {tnow})"),
                    "TC2_Complete",
                ),
                "continue" => {
                    let available_at = int(params, "available_at", seq)?;
                    (
                        format!("TC2_Continue({r}, {w}, {tok}, {available_at}, {tnow})"),
                        "TC2_Continue",
                    )
                }
                "run_child" => {
                    let parent = int(params, "workflow_id", seq)?;
                    let kind = text(params, "child_kind", seq)?;
                    let key = key_id(ctx, Some(text(params, "child_key", seq)?))?;
                    let child = int(params, "child_id", seq)?;
                    let mut tnow = tnow;
                    let (existing, c_new, last) = if flag(params, "inserted", seq)? {
                        tnow = inserted_tnow(ctx, record, child)?;
                        (0, ctx.wf.id(child), "TC2_RunChild_New")
                    } else if params["woke"][parent.to_string()].is_i64() {
                        (ctx.wf.id(child), 0, "TC2_RunChild_AttachTerminal")
                    } else {
                        (ctx.wf.id(child), 0, "TC2_RunChild_Attach")
                    };
                    (
                        format!(
                            "TC2_RunChild({r}, {w}, {tok}, {}, {key}, {existing}, {c_new}, {tnow})",
                            quote(kind)
                        ),
                        last,
                    )
                }
                other => return Err(format!("unsupported_action:TC2_Commit/{other}")),
            }
        }
        "TC3_ActivationFailure" => {
            let tnow = tnow(ctx, record)?;
            let r = runtime(ctx, actor, seq)?;
            let w = ctx.wf.id(int(params, "workflow_id", seq)?);
            let tok = token(ctx, &params["token"]);
            let attempt = int(params, "attempt", seq)?;
            let cap = int(params, "max_activation", seq)?;
            ctx.max_activation = Some(ctx.max_activation.unwrap_or(0).max(cap));
            let available_at = int(params, "available_at", seq)?;
            let exhausted = flag(params, "exhausted", seq)?;
            (
                format!(
                    "TC3_ActivationFailure({r}, {w}, {tok}, {attempt}, {cap}, {available_at}, {tnow})"
                ),
                if exhausted {
                    "TC3_Exhausted"
                } else {
                    "TC3_Retry"
                },
            )
        }
        "CoordFenceMiss" => {
            let r = runtime(ctx, actor, seq)?;
            let w = ctx.wf.id(int(params, "workflow_id", seq)?);
            let tok = token(ctx, &params["token"]);
            (format!("CoordFenceMiss({r}, {w}, {tok})"), "CoordFenceMiss")
        }
        "LC1_NoEvent" => {
            let r = runtime(ctx, actor, seq)?;
            let w = ctx.wf.id(int(params, "workflow_id", seq)?);
            (format!("LC1_NoEvent({r}, {w})"), "LC1_NoEvent")
        }
        "TW1_Claim" => {
            let tnow = tnow(ctx, record)?;
            let r = runtime(ctx, actor, seq)?;
            (tw1_claim(ctx, record, r, tnow, seq)?, "TW1_Claim")
        }
        "HandlerReturn" => {
            let r = runtime(ctx, actor, seq)?;
            let a = ctx.act.id(int(params, "activity_id", seq)?);
            let tok = token(ctx, &params["token"]);
            let outcome = text(params, "outcome", seq)?;
            (
                format!("HandlerReturn({r}, {a}, {tok}, {})", quote(outcome)),
                "HandlerReturn",
            )
        }
        "TW3_Finish" => {
            let tnow = tnow(ctx, record)?;
            let r = runtime(ctx, actor, seq)?;
            let a = ctx.act.id(int(params, "activity_id", seq)?);
            let tok = token(ctx, &params["token"]);
            let outcome = text(params, "outcome", seq)?;
            // availableAt only matters for a retry.
            let (available_at, last) = match outcome {
                "succeeded" => (tnow, "TW3_Succeeded"),
                "retryable" if int(params, "attempt", seq)? < int(params, "max_attempts", seq)? => {
                    (int(params, "available_at", seq)?, "TW3_Retry")
                }
                "retryable" | "permanent" => (tnow, "TW3_DeadLetter"),
                other => return Err(format!("unsupported_action:TW3_Finish/{other}")),
            };
            (
                format!(
                    "TW3_Finish({r}, {a}, {tok}, {}, {available_at}, {tnow})",
                    quote(outcome)
                ),
                last,
            )
        }
        "TW3_FenceMiss" | "LocalDeadline" => {
            let r = runtime(ctx, actor, seq)?;
            let a = ctx.act.id(int(params, "activity_id", seq)?);
            let tok = token(ctx, &params["token"]);
            (format!("{action}({r}, {a}, {tok})"), action)
        }
        "TW1_Error" => {
            let r = runtime(ctx, actor, seq)?;
            let a = ctx.act.id(int(params, "activity_id", seq)?);
            match text(params, "reason", seq)? {
                reason @ ("attempt_cap" | "invalid_bounds") => (
                    format!("TW1_Error({r}, {a}, {})", quote(reason)),
                    "TW1_Error",
                ),
                // Every runtime has every definition in the model (F3).
                "missing_definition" => return Err("unmodeled:missing_definition".into()),
                other => return Err(format!("unsupported:tw1_error_{other}")),
            }
        }
        "TW2_Send" => {
            let r = runtime(ctx, actor, seq)?;
            let a = ctx.act.id(int(params, "activity_id", seq)?);
            let tok = token(ctx, &params["token"]);
            let hb = ctx.hb.id(int(params, "hb", seq)?);
            (format!("TW2_Send({r}, {a}, {tok}, {hb})"), "TW2_Send")
        }
        "TW2_Commit" => {
            let sample = tnow(ctx, record)?;
            let hb = sent_hb(ctx, params, seq)?.ok_or_else(|| format!("malformed:hb:{seq}"))?;
            let lease = int(params, "lease_expires_at", seq)?;
            (format!("TW2_Commit({hb}, {sample}, {lease})"), "TW2_Commit")
        }
        "TW2_FenceMiss" => {
            let hb = sent_hb(ctx, params, seq)?.ok_or_else(|| format!("malformed:hb:{seq}"))?;
            (format!("TW2_FenceMiss({hb})"), "TW2_FenceMiss")
        }
        "TW2_Drop" => {
            // A drop whose COMMIT landed, or of a heartbeat dropped before its
            // TW2_Send was recorded, changed nothing the model sees.
            if params["hb"]
                .as_i64()
                .is_some_and(|hb| ctx.committed_hbs.contains(&hb))
            {
                return Ok(None);
            }
            let Some(hb) = sent_hb(ctx, params, seq)? else {
                return Ok(None);
            };
            (format!("TW2_Drop({hb})"), "TW2_Drop")
        }
        "Crash" => {
            let r = runtime(ctx, actor, seq)?;
            // A runtime that held nothing process-local loses nothing: a stutter.
            (
                format!("if (crashLoses({r})) Crash({r}) else commit(db, proc, ghost, \"Crash\")"),
                "Crash",
            )
        }
        other => return Err(format!("unsupported_action:{other}")),
    };

    let mut expect = vec![format!("lastAction == {}", quote(last))];
    expect.extend(ctx.invariants.iter().cloned());
    expect.extend(post_expectations(ctx, record, seq)?);
    Ok(Some(Step {
        seq,
        class: classify(record),
        label,
        call,
        expect,
    }))
}

/// The model id of a heartbeat already sent; `None` if its `TW2_Send` is not
/// in the trace.
fn sent_hb(ctx: &Ctx, params: &Value, seq: i64) -> Result<Option<i64>, Excluded> {
    let hb = int(params, "hb", seq)?;
    Ok(ctx.hb.ids.get(&hb).copied())
}

/// `TW1_Claim(r, tnow, localAvail, reconciled, inFlightSeen, claimed)`.
/// The maps cover every topic; a topic the transaction did not visit gets a
/// value that cannot constrain (no claims there).
fn tw1_claim(
    ctx: &mut Ctx,
    record: &Value,
    r: i64,
    tnow: i64,
    seq: i64,
) -> Result<String, Excluded> {
    let params = &record["params"];
    let mut per_topic: BTreeMap<String, i64> = BTreeMap::new();
    let mut claimed = Vec::new();
    for claim in params["claimed"]
        .as_array()
        .ok_or_else(|| format!("malformed:claimed:{seq}"))?
    {
        let id = int(claim, "activity_id", seq)?;
        let topic = record["post"]["act"][id.to_string()]["topic"]
            .as_str()
            .ok_or_else(|| format!("malformed:claimed_topic:{seq}"))?;
        *per_topic.entry(topic.to_string()).or_default() += 1;
        let a = ctx.act.id(id);
        let tok = token(ctx, &claim["token"]);
        let lease = int(claim, "lease_expires_at", seq)?;
        claimed.push(format!("{{ a: {a}, tok: {tok}, leaseExp: {lease} }}"));
    }
    let mut reconciled = Vec::new();
    if let Some(rows) = params["reconciled"].as_array() {
        for row in rows {
            let a = ctx.act.id(int(row, "activity_id", seq)?);
            let exhausted = row["exhausted"]
                .as_bool()
                .ok_or_else(|| format!("malformed:reconciled:{seq}"))?;
            let available_at = int(row, "available_at", seq)?;
            reconciled.push(format!(
                "{{ a: {a}, exhausted: {exhausted}, availableAt: {available_at} }}"
            ));
        }
    }
    let mut local = Vec::new();
    let mut in_flight = Vec::new();
    for topic in &ctx.topics {
        let claims_here = per_topic.get(topic).copied().unwrap_or(0);
        let available = params["local_avail"][topic].as_i64().unwrap_or(claims_here);
        ctx.local_slots = ctx.local_slots.max(available);
        local.push(format!("{} -> {available}", quote(topic)));
        let seen = params["in_flight_seen"][topic].as_i64().unwrap_or(0);
        in_flight.push(format!("{} -> {seen}", quote(topic)));
    }
    Ok(format!(
        "TW1_Claim({r}, {tnow}, Map({}), [{}], Map({}), [{}])",
        local.join(", "),
        reconciled.join(", "),
        in_flight.join(", "),
        claimed.join(", ")
    ))
}

fn post_expectations(ctx: &mut Ctx, record: &Value, seq: i64) -> Result<Vec<String>, Excluded> {
    let post = &record["post"];
    let params = &record["params"];
    let mut clauses = Vec::new();
    if let Some(rows) = post["wf"].as_object() {
        for (id, row) in rows {
            let real = parse_id(id, seq)?;
            let w = ctx.wf.id(real);
            if row.is_null() {
                clauses.push(format!("viewWf(db, {w}).status == \"none\""));
                continue;
            }
            let view = wf_row(ctx, row, seq)?;
            clauses.push(format!("viewWf(db, {w}) == {view}"));
        }
    }
    if let Some(rows) = post["act"].as_object() {
        for (id, row) in rows {
            let real = parse_id(id, seq)?;
            let a = ctx.act.id(real);
            clauses.push(if row.is_null() {
                format!("viewAct(db, {a}).status == \"none\"")
            } else {
                format!("viewAct(db, {a}) == {}", act_row(ctx, real, row, seq)?)
            });
        }
    }
    if let Some(rows) = post["att"].as_object() {
        for (id, row) in rows {
            let (activity, number) = id
                .split_once(':')
                .and_then(|(a, n)| Some((a.parse::<i64>().ok()?, n.parse::<i64>().ok()?)))
                .ok_or_else(|| format!("malformed:att_key:{seq}"))?;
            ctx.max_attempts = ctx.max_attempts.max(number);
            let a = ctx.act.id(activity);
            let view = if row.is_null() {
                "{ used: false, token: 0, open: false }".to_string()
            } else {
                let tok = token(ctx, &row["lease_token"]);
                let open = row["finished_at"].is_null();
                format!("{{ used: true, token: {tok}, open: {open} }}")
            };
            clauses.push(format!("viewAtt(db, {a}, {number}) == {view}"));
        }
    }
    if let Some(events) = post["events"].as_array() {
        for event in events {
            let w = ctx.wf.id(int(event, "wf", seq)?);
            let dseq = int(event, "dseq", seq)?;
            let recorded = text(event, "type", seq)?;
            // The code delivers a cancelled child as child_failed with category
            // child_cancelled; the model names that event child_cancelled.
            let typ = if recorded == "child_failed" && event["category"] == "child_cancelled" {
                "child_cancelled"
            } else {
                recorded
            };
            // `started` and `continued` rows store no command_sequence; the model
            // stamps the workflow's command sequence at append time, which is the
            // post-image's (neither transition changes it).
            let cmd = match (typ, event["cmd"].as_i64()) {
                ("started" | "continued", None | Some(0)) => post["wf"][event["wf"].to_string()]
                    ["command_sequence"]
                    .as_i64()
                    .unwrap_or(0),
                (_, cmd) => cmd.unwrap_or(0),
            };
            let reference = match typ {
                "started" | "continued" => 0,
                t if t.starts_with("activity_") => ctx.act.id(int(params, "activity_id", seq)?),
                // `woke` maps each woken parent to its child.
                t if t.starts_with("child_") => ctx.wf.id(params["woke"][event["wf"].to_string()]
                    .as_i64()
                    .map_or_else(|| int(params, "workflow_id", seq), Ok)?),
                other => return Err(format!("unmodeled:event:{other}")),
            };
            clauses.push(format!(
                "deliverable(db, {w}).contains({{ dseq: {dseq}, typ: {}, ref: {reference}, cmd: {cmd} }})",
                quote(typ)
            ));
        }
    }
    Ok(clauses)
}

/// `WfRow` literal: the §5 abstraction of a recorded workflow row.
fn wf_row(ctx: &mut Ctx, row: &Value, seq: i64) -> Result<String, Excluded> {
    let wait_kind = row["wait_kind"].as_str();
    let wait_ref = match (wait_kind, opt_int(row, "wait_reference_id")) {
        (_, None) => 0,
        (Some("activity"), Some(id)) => ctx.act.id(id),
        (Some("child"), Some(id)) => ctx.wf.id(id),
        (Some(other), Some(_)) => return Err(format!("unmodeled:wait_kind:{other}")),
        (None, Some(_)) => return Err(format!("malformed:wait_reference_id:{seq}")),
    };
    if let Some(kind @ ("timer" | "approval")) = wait_kind {
        return Err(format!("unmodeled:wait_kind:{kind}"));
    }
    let activation_attempts = int(row, "activation_attempts", seq)?;
    ctx.seen_activation = ctx.seen_activation.max(activation_attempts);
    let dedup = key_id(ctx, row["deduplication_key"].as_str())?;
    let parent = opt_int(row, "parent_workflow_id").map_or(0, |id| ctx.wf.id(id));
    let root = opt_int(row, "root_workflow_id").map_or(0, |id| ctx.wf.id(id));
    let restarted = opt_int(row, "restarted_from_workflow_id").map_or(0, |id| ctx.wf.id(id));
    let tok = token(ctx, &row["lease_token"]);
    Ok(format!(
        "{{ status: {}, kind: {}, dedup: {dedup}, waitKind: {}, waitRef: {wait_ref}, availableAt: {}, \
         token: {tok}, leaseExp: {}, cmdSeq: {}, delivered: {}, actAttempts: {activation_attempts}, \
         parent: {parent}, root: {root}, restartedFrom: {restarted} }}",
        quote(text(row, "status", seq)?),
        quote(text(row, "kind", seq)?),
        quote(wait_kind.unwrap_or("")),
        int(row, "available_at", seq)?,
        opt_int(row, "lease_expires_at").unwrap_or(0),
        int(row, "command_sequence", seq)?,
        int(row, "delivered_event_sequence", seq)?,
    ))
}

/// `ActRow` literal: the §5 abstraction of a recorded activity row.
fn act_row(ctx: &mut Ctx, id: i64, row: &Value, seq: i64) -> Result<String, Excluded> {
    let wf = ctx.wf.id(int(row, "workflow_id", seq)?);
    let max_attempts = int(row, "max_attempts", seq)?;
    ctx.max_attempts = ctx.max_attempts.max(max_attempts);
    let tok = token(ctx, &row["lease_token"]);
    let invalid = invalid_bounds(row);
    if invalid {
        ctx.invalid_bounds.insert(id);
    } else {
        ctx.invalid_bounds.remove(&id);
    }
    Ok(format!(
        "{{ status: {}, wf: {wf}, topic: {}, attemptCount: {}, maxAttempts: {max_attempts}, \
         availableAt: {}, token: {tok}, leaseExp: {}, invalidBounds: {invalid} }}",
        quote(text(row, "status", seq)?),
        quote(text(row, "topic", seq)?),
        int(row, "attempt_count", seq)?,
        int(row, "available_at", seq)?,
        opt_int(row, "lease_expires_at").unwrap_or(0),
    ))
}

/// `invalidBounds` of a recorded activity row: the check in
/// `claim_locked_candidate` (`timeout_millis <= 0 || lease_duration_millis <=
/// timeout_millis`); false when the image has no bounds.
fn invalid_bounds(row: &Value) -> bool {
    match (
        opt_int(row, "timeout_millis"),
        opt_int(row, "lease_duration_millis"),
    ) {
        (Some(timeout), Some(lease)) => timeout <= 0 || lease <= timeout,
        _ => false,
    }
}

/// `KeyMap`: NULL -> 0, `child:{P}:{C}` -> `autoKey(WfMap[P], C)`, else 1..j.
fn key_id(ctx: &mut Ctx, key: Option<&str>) -> Result<i64, Excluded> {
    let Some(key) = key else {
        return Ok(0);
    };
    if let Some(rest) = key.strip_prefix("child:") {
        let (parent, command) = rest
            .split_once(':')
            .and_then(|(p, c)| Some((p.parse::<i64>().ok()?, c.parse::<i64>().ok()?)))
            .ok_or_else(|| format!("malformed:child_key:{key}"))?;
        return Ok(1000 + ctx.wf.id(parent) * 10 + command);
    }
    Ok(ctx.key.id(key.to_string()))
}

/// `RtMap`: the actor with its `:coordinator` / `:dispatcher` suffix removed.
fn runtime(ctx: &mut Ctx, actor: &str, seq: i64) -> Result<i64, Excluded> {
    if actor.is_empty() || actor == "app" {
        return Err(format!("malformed:runtime_actor:{seq}"));
    }
    let base = actor
        .strip_suffix(":coordinator")
        .or_else(|| actor.strip_suffix(":dispatcher"))
        .unwrap_or(actor);
    Ok(ctx.rt.id(base.to_string()))
}

/// `TokMap`: tokens numbered by first appearance; NULL -> 0.
fn token(ctx: &mut Ctx, value: &Value) -> i64 {
    value
        .as_str()
        .map_or(0, |token| ctx.tok.id(token.to_string()))
}

/// Rejects a trace where one `(kind, dedup key)` appears with two versions.
fn check_version(
    ctx: &mut Ctx,
    kind: &str,
    key: Option<&str>,
    params: &Value,
    seq: i64,
) -> Result<(), Excluded> {
    let Some(key) = key else {
        return Ok(());
    };
    let version = int(params, "version", seq)?;
    let known = ctx
        .versions
        .entry((kind.to_string(), key.to_string()))
        .or_insert(version);
    if *known == version {
        Ok(())
    } else {
        Err(format!("versions:{kind}"))
    }
}

/// `tnow` of a step that inserts workflow `id`: the new row's `available_at`.
/// The code samples the clock again for the insert (store.rs `insert_prepared`,
/// `insert_child`), and the model sets the new row's `availableAt` to `tnow`;
/// any sample inside `[now, end_now]` is a time the transaction observed.
fn inserted_tnow(ctx: &mut Ctx, record: &Value, id: i64) -> Result<i64, Excluded> {
    let seq = record["seq"].as_i64().unwrap_or(0);
    let first = tnow(ctx, record)?;
    let available_at = record["post"]["wf"][id.to_string()]["available_at"]
        .as_i64()
        .ok_or_else(|| format!("malformed:inserted_available_at:{seq}"))?;
    let last = record["end_now"].as_i64().unwrap_or(first);
    if available_at < first || available_at > last {
        // StartOptions.available_at is not modeled.
        return Err("unmodeled:start_available_at".into());
    }
    ctx.max_now = ctx.max_now.max(available_at);
    Ok(available_at)
}

/// The record's DB time, or the model's current `now` (a no-op for
/// `now' = max(now, tnow)`) for a step that did not sample the clock.
fn tnow_or_model(ctx: &mut Ctx, record: &Value) -> i64 {
    tnow(ctx, record).unwrap_or(ctx.max_now)
}

fn flag(value: &Value, field: &str, seq: i64) -> Result<bool, Excluded> {
    value[field]
        .as_bool()
        .ok_or_else(|| format!("malformed:{field}:{seq}"))
}

fn tnow(ctx: &mut Ctx, record: &Value) -> Result<i64, Excluded> {
    let now = record["now"]
        .as_i64()
        .ok_or_else(|| format!("missing_now:{}", record["seq"]))?;
    ctx.max_now = ctx.max_now.max(now);
    Ok(now)
}

fn int(value: &Value, field: &str, seq: i64) -> Result<i64, Excluded> {
    value[field]
        .as_i64()
        .ok_or_else(|| format!("malformed:{field}:{seq}"))
}

fn opt_int(value: &Value, field: &str) -> Option<i64> {
    value[field].as_i64()
}

fn text<'a>(value: &'a Value, field: &str, seq: i64) -> Result<&'a str, Excluded> {
    value[field]
        .as_str()
        .ok_or_else(|| format!("malformed:{field}:{seq}"))
}

fn parse_id(id: &str, seq: i64) -> Result<i64, Excluded> {
    id.parse().map_err(|_| format!("malformed:row_id:{seq}"))
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}
