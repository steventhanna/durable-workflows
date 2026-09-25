//! `report`: explains a failing trace step. Without `--qnt` it names the
//! first failing step in a `quint test --match '^step_'` log and prints its
//! record. With `--qnt` it replays the step in the Quint REPL and prints the
//! model's value of every expectation next to the value the generator took
//! from the recorded row, field by field.
//!
//! `quint test` prints no states, so the diff runs `step_{k-1}` in the REPL
//! (TypeScript backend: the Rust one drops piped input that arrives before it
//! is ready), prints each expectation's left-hand side before and after the
//! step's action, and compares the values with the expected ones parsed from
//! the generated module.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use serde_json::Value;

/// What `report` is asked to explain.
pub struct Request<'a> {
    pub trace: &'a Path,
    pub quint_output: Option<&'a Path>,
    pub qnt: Option<&'a Path>,
    pub step: Option<usize>,
    pub quint: &'a Path,
}

pub fn run(request: &Request<'_>) -> Result<(), String> {
    let trace: Value = serde_json::from_str(&read(request.trace)?)
        .map_err(|error| format!("{}: {error}", request.trace.display()))?;
    println!(
        "trace: {} ({})",
        trace["test"].as_str().unwrap_or("?"),
        trace["backend"].as_str().unwrap_or("?")
    );
    let step = match (request.step, request.quint_output) {
        (Some(step), _) => step,
        (None, Some(log)) => match first_failing_step(&read(log)?) {
            Some(step) => step,
            None => {
                println!("no failing step in the quint output");
                return Ok(());
            }
        },
        (None, None) => return Err("report needs --step or --quint-output".into()),
    };
    let Some(qnt) = request.qnt else {
        let records = trace["records"].as_array().cloned().unwrap_or_default();
        let record = step
            .checked_sub(1)
            .and_then(|index| records.get(index))
            .ok_or_else(|| format!("step_{step} has no record"))?;
        print_header(step, record, crate::gen::classify(record));
        println!(
            "{}",
            serde_json::to_string_pretty(record).map_err(|error| error.to_string())?
        );
        return Ok(());
    };

    let block = StepBlock::parse(&read(qnt)?, step)?;
    let record = trace["records"]
        .as_array()
        .and_then(|records| records.iter().find(|record| record["seq"] == block.seq))
        .cloned()
        .unwrap_or(Value::Null);
    print_header(step, &record, &block.class);
    let values = evaluate(request.quint, qnt, &block)?;
    print_diff(&block, &values);
    Ok(())
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn print_header(step: usize, record: &Value, class: &str) {
    println!(
        "failing step: step_{step} (seq {}, {} {}, {class})",
        record["seq"].as_i64().unwrap_or(0),
        record["actor"].as_str().unwrap_or("?"),
        record["action"].as_str().unwrap_or("?"),
    );
}

/// Smallest `k` among the `step_k` tests the log reports as failed.
pub fn first_failing_step(log: &str) -> Option<usize> {
    log.lines()
        .filter(|line| line.contains('✖') || line.contains("FAILED") || line.contains("failed"))
        .filter_map(|line| {
            let start = line.find("step_")? + "step_".len();
            let digits: String = line[start..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse().ok()
        })
        .min()
}

/// One expectation of a step, as the generator wrote it.
enum Clause {
    /// `lhs == rhs`
    Equals { lhs: String, rhs: String },
    /// `set.contains(element)`
    Contains { set: String, element: String },
    /// Any other Boolean clause (`safety`, an invariant).
    Holds(String),
}

impl Clause {
    fn parse(text: &str) -> Self {
        if let Some((lhs, rhs)) = split_top_level(text, " == ") {
            return Self::Equals {
                lhs: lhs.trim().to_string(),
                rhs: rhs.trim().to_string(),
            };
        }
        if let Some((set, element)) = text
            .strip_suffix(')')
            .and_then(|inner| split_top_level(inner, ".contains("))
        {
            return Self::Contains {
                set: set.trim().to_string(),
                element: element.trim().to_string(),
            };
        }
        Self::Holds(text.to_string())
    }

    /// The expression whose model value the report shows.
    fn subject(&self) -> &str {
        match self {
            Self::Equals { lhs, .. } => lhs,
            Self::Contains { set, .. } => set,
            Self::Holds(text) => text,
        }
    }
}

/// `// seq <n> (<class>): <label>` and
/// `run step_k = <previous>.then(<action>).expect(and { <clauses> })`.
struct StepBlock {
    previous: String,
    action: String,
    clauses: Vec<Clause>,
    seq: Value,
    class: String,
}

impl StepBlock {
    fn parse(module: &str, step: usize) -> Result<Self, String> {
        let lines: Vec<&str> = module.lines().collect();
        let head = format!("run step_{step} = ");
        let at = lines
            .iter()
            .position(|line| line.trim_start().starts_with(&head))
            .ok_or_else(|| format!("the module has no `run step_{step}`"))?;
        let rest = &lines[at].trim_start()[head.len()..];
        let (previous, then) = rest
            .split_once(".then(")
            .ok_or_else(|| format!("step_{step}: no `.then(`"))?;
        let action = then
            .strip_suffix(')')
            .ok_or_else(|| format!("step_{step}: the action does not end the line"))?;
        let clauses = lines[at + 1..]
            .iter()
            .map(|line| line.trim())
            .skip_while(|line| *line == ".expect(and {")
            .take_while(|line| !line.starts_with("})") && !line.is_empty())
            .map(|line| Clause::parse(line.trim_end_matches(',')))
            .collect();
        let comment = at
            .checked_sub(1)
            .and_then(|index| lines[index].trim().strip_prefix("// seq "))
            .unwrap_or("");
        let (seq, rest) = comment.split_once(' ').unwrap_or(("", ""));
        let class = rest
            .strip_prefix('(')
            .and_then(|rest| rest.split_once(')'))
            .map_or("?", |(class, _)| class);
        Ok(Self {
            previous: previous.to_string(),
            action: action.to_string(),
            clauses,
            seq: seq.parse::<i64>().map_or(Value::Null, Value::from),
            class: class.to_string(),
        })
    }
}

/// Model values around the step: the subjects before the action, whether
/// the action was enabled, and the subjects after it.
struct Values {
    before: Vec<String>,
    enabled: bool,
    after: Vec<String>,
}

const MARK: &str = "@@durable-trace@@";

fn evaluate(quint: &Path, qnt: &Path, block: &StepBlock) -> Result<Values, String> {
    // Generated modules import `../durable`; the REPL resolves that from its
    // working directory, so it runs next to the module.
    let dir = qnt
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file = qnt
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("{}: not a file name", qnt.display()))?;
    let module = file.trim_end_matches(".qnt");
    let quint = if quint.components().count() > 1 {
        std::fs::canonicalize(quint).map_err(|error| format!("{}: {error}", quint.display()))?
    } else {
        quint.to_path_buf()
    };

    let subjects: Vec<&str> = block.clauses.iter().map(Clause::subject).collect();
    let script: Vec<&str> = std::iter::once(block.previous.as_str())
        .chain(subjects.iter().copied())
        .chain(std::iter::once(block.action.as_str()))
        .chain(subjects.iter().copied())
        .collect();
    let input: String = script
        .iter()
        .map(|expr| format!("{expr}\n\"{MARK}\"\n"))
        .collect();

    let mut child = Command::new(&quint)
        .args(["repl", "-q", "--backend", "typescript", "-r"])
        .arg(format!("{file}::{module}"))
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("{}: {error}", quint.display()))?;
    child
        .stdin
        .take()
        .ok_or("quint repl: no stdin")?
        .write_all(input.as_bytes())
        .map_err(|error| format!("quint repl: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("quint repl: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut outputs: Vec<String> = stdout
        .split(&format!("\"{MARK}\""))
        .map(|chunk| chunk.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    if outputs.len() <= script.len() {
        return Err(format!(
            "quint repl printed {} of {} values; stderr:\n{}\nstdout:\n{stdout}",
            outputs.len() - 1,
            script.len(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if outputs[0] != "true" {
        return Err(format!(
            "{} did not replay in the REPL: {}",
            block.previous, outputs[0]
        ));
    }
    let n = subjects.len();
    let after = outputs.drain(n + 2..2 * n + 2).collect();
    let enabled = outputs[n + 1] == "true";
    let before = outputs.drain(1..=n).collect();
    Ok(Values {
        before,
        enabled,
        after,
    })
}

fn print_diff(block: &StepBlock, values: &Values) {
    println!("action: {}", block.action);
    let (model, column) = if values.enabled {
        (&values.after, "model")
    } else {
        println!(
            "the action is NOT enabled in the model state after {}; \
             the model column shows that state",
            block.previous
        );
        (&values.before, "model (before)")
    };
    println!();
    println!("{:<32} {:<30} recorded", "expectation / field", column);
    let mut matched = 0;
    for (clause, model) in block.clauses.iter().zip(model) {
        match clause {
            Clause::Equals { lhs, rhs } => match (parse_record(model), parse_record(rhs)) {
                (Some(model_fields), Some(recorded_fields)) => {
                    let names: BTreeSet<&String> =
                        model_fields.keys().chain(recorded_fields.keys()).collect();
                    let mut header = false;
                    for name in names {
                        let m = model_fields.get(name).map_or("<absent>", String::as_str);
                        let r = recorded_fields.get(name).map_or("<absent>", String::as_str);
                        if m == r {
                            matched += 1;
                            continue;
                        }
                        if !header {
                            println!("{lhs}");
                            header = true;
                        }
                        println!("  .{name:<29} {m:<30} {r}   <-- differs");
                    }
                }
                _ if normalize(model) == normalize(rhs) => matched += 1,
                _ => println!("{lhs:<32} {model:<30} {rhs}   <-- differs"),
            },
            Clause::Contains { set, element } => {
                let wanted = canonical(element);
                if parse_set(model)
                    .iter()
                    .any(|member| canonical(member) == wanted)
                {
                    matched += 1;
                } else {
                    println!("{set} lacks the recorded element   <-- differs");
                    println!("  recorded element: {element}");
                    println!("  model set:        {model}");
                }
            }
            Clause::Holds(text) => {
                if model == "true" {
                    matched += 1;
                } else {
                    println!("{text:<32} {model:<30} true   <-- differs");
                }
            }
        }
    }
    println!("({matched} other expectations or fields agree)");
}

/// Removes whitespace outside string literals.
fn normalize(text: &str) -> String {
    let mut out = String::new();
    let mut in_string = false;
    for c in text.chars() {
        if c == '"' {
            in_string = !in_string;
        }
        if in_string || !c.is_whitespace() {
            out.push(c);
        }
    }
    out
}

/// A value with record fields in name order (Quint prints them sorted).
fn canonical(text: &str) -> String {
    parse_record(text).map_or_else(
        || normalize(text),
        |fields| {
            let fields: Vec<String> = fields
                .into_iter()
                .map(|(name, value)| format!("{name}:{value}"))
                .collect();
            format!("{{{}}}", fields.join(","))
        },
    )
}

/// Splits `text` at the first `sep` outside brackets and string literals.
fn split_top_level<'a>(text: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0i32;
    let mut in_string = false;
    for (index, c) in text.char_indices() {
        if depth == 0 && !in_string && text[index..].starts_with(sep) {
            return Some((&text[..index], &text[index + sep.len()..]));
        }
        match c {
            '"' => in_string = !in_string,
            '(' | '{' | '[' if !in_string => depth += 1,
            ')' | '}' | ']' if !in_string => depth -= 1,
            _ => {}
        }
    }
    None
}

/// Items of `text` separated by top-level commas.
fn top_level_items(text: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut rest = text;
    while let Some((item, tail)) = split_top_level(rest, ",") {
        items.push(item.trim().to_string());
        rest = tail;
    }
    if !rest.trim().is_empty() {
        items.push(rest.trim().to_string());
    }
    items
}

/// Fields of a flat record literal `{ a: 1, b: "x" }`, values normalized.
fn parse_record(text: &str) -> Option<BTreeMap<String, String>> {
    let inner = text.trim().strip_prefix('{')?.strip_suffix('}')?;
    top_level_items(inner)
        .into_iter()
        .map(|item| {
            let (name, value) = item.split_once(':')?;
            Some((name.trim().to_string(), normalize(value)))
        })
        .collect()
}

/// Members of `Set(...)` (or a list `[...]`).
fn parse_set(text: &str) -> Vec<String> {
    let text = text.trim();
    let inner = text
        .strip_prefix("Set(")
        .and_then(|rest| rest.strip_suffix(')'))
        .or_else(|| {
            text.strip_prefix('[')
                .and_then(|rest| rest.strip_suffix(']'))
        })
        .unwrap_or(text);
    top_level_items(inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clauses_parse_by_shape() {
        assert!(matches!(
            Clause::parse(r#"viewWf(db, 1) == { status: "ready", token: 0 }"#),
            Clause::Equals { ref lhs, .. } if lhs == "viewWf(db, 1)"
        ));
        assert!(matches!(
            Clause::parse(r#"deliverable(db, 1).contains({ dseq: 1, typ: "started", ref: 0, cmd: 0 })"#),
            Clause::Contains { ref set, ref element }
                if set == "deliverable(db, 1)" && element.starts_with("{ dseq")
        ));
        assert!(matches!(Clause::parse("safety"), Clause::Holds(_)));
    }

    #[test]
    fn records_and_sets_parse() {
        let record = parse_record(r#"{ a: 1, b: "x, y", c: Map("e" -> 1) }"#).unwrap();
        assert_eq!(record["b"], r#""x, y""#);
        assert_eq!(record["c"], r#"Map("e"->1)"#);
        assert_eq!(
            parse_set(r#"Set({ cmd: 0, dseq: 1 }, { cmd: 1, dseq: 2 })"#).len(),
            2
        );
        assert!(parse_set("Set()").is_empty());
        assert_eq!(
            canonical("{ dseq: 2, typ: \"x\", cmd: 1 }"),
            canonical("{ cmd: 1, dseq: 2, typ: \"x\" }")
        );
    }

    #[test]
    fn step_block_parses_generated_text() {
        let module = r#"
  // seq 3 (strict): rt1:coordinator TC2_Commit
  run step_3 = step_2.then(all { keepPrev, TC2_Complete(1, 1, 3, 17) })
    .expect(and {
      lastAction == "TC2_Complete",
      safety,
    })
"#;
        let block = StepBlock::parse(module, 3).unwrap();
        assert_eq!(block.previous, "step_2");
        assert_eq!(block.action, "all { keepPrev, TC2_Complete(1, 1, 3, 17) }");
        assert_eq!(block.clauses.len(), 2);
        assert_eq!(block.seq, Value::from(3));
        assert_eq!(block.class, "strict");
    }
}
