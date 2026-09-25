//! Property tests for `DurableFlow` replay over a `FlowJournal`, driven
//! through the public `WorkflowHandler::step` API (pure, no database).

use std::time::Duration;

use async_trait::async_trait;
use durable_workflows::{
    ActivityContext, ActivityError, ActivityHandler, ActivityResult, ActivityTopic, ChildResult,
    DurableActivity, DurableFlow, DurableWorkflow, FlowJournal, RetryPolicy, WfCtx, WfError,
    WorkflowContext, WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowTransition,
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const WORKFLOW_ID: i64 = 7;

#[derive(Clone, Copy)]
struct PropTopic;

impl ActivityTopic for PropTopic {
    fn key(self) -> &'static str {
        "prop_topic"
    }

    fn max_concurrency(self) -> u32 {
        1
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Double {
    value: i64,
}

impl DurableActivity for Double {
    type Topic = PropTopic;
    const KIND: &'static str = "prop_double";
    const VERSION: i32 = 3;
    const MAX_ATTEMPTS: u32 = 1;
    const TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE_DURATION: Duration = Duration::from_secs(10);

    fn topic() -> Self::Topic {
        PropTopic
    }

    fn retry_policy() -> RetryPolicy {
        RetryPolicy::fixed(1).expect("valid policy")
    }
}

#[async_trait]
impl ActivityHandler for Double {
    type Context = ();
    type Output = i64;

    async fn execute(&self, _context: ActivityContext<'_, ()>) -> Result<i64, ActivityError> {
        Ok(self.value.wrapping_mul(2))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Echo {
    message: String,
}

impl DurableWorkflow for Echo {
    const KIND: &'static str = "prop_echo";
    const VERSION: i32 = 2;
}

#[async_trait]
impl DurableFlow for Echo {
    type Context = ();
    type Output = String;

    async fn run(&self, _ctx: &mut WfCtx<'_, ()>) -> Result<String, WfError> {
        Ok(self.message.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Step {
    Activity(i64),
    Child { message: String, fails: bool },
    Sleep(i64),
}

impl Step {
    fn same_kind(&self, other: &Step) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// A deterministic flow that executes a script of steps and records each
/// result. A failed child is compensated by recording the failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Script {
    steps: Vec<Step>,
}

impl DurableWorkflow for Script {
    const KIND: &'static str = "prop_script";
    const VERSION: i32 = 1;
}

#[async_trait]
impl DurableFlow for Script {
    type Context = ();
    type Output = Vec<String>;

    async fn run(&self, ctx: &mut WfCtx<'_, ()>) -> Result<Vec<String>, WfError> {
        let mut log = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            match step {
                Step::Activity(value) => {
                    let doubled = ctx.run(&Double { value: *value }).await?;
                    log.push(format!("activity:{doubled}"));
                }
                Step::Child { message, .. } => {
                    match ctx
                        .child(&Echo {
                            message: message.clone(),
                        })
                        .await
                    {
                        Ok(echoed) => log.push(format!("child:{echoed}")),
                        Err(WfError::ChildFailed { category, .. }) => {
                            log.push(format!("child_failed:{category}"))
                        }
                        Err(other) => return Err(other),
                    }
                }
                Step::Sleep(wake_at) => {
                    ctx.sleep_until(*wake_at).await?;
                    log.push(format!("slept:{wake_at}"));
                }
            }
        }
        Ok(log)
    }
}

type Transition = WorkflowTransition<FlowJournal, (), Vec<String>>;

fn context() -> WorkflowContext<'static, ()> {
    WorkflowContext::new(&()).with_workflow_id(WorkflowId::new(WORKFLOW_ID).expect("workflow id"))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
}

fn step(
    runtime: &tokio::runtime::Runtime,
    flow: &Script,
    journal: FlowJournal,
    event: WorkflowEvent,
) -> Result<Transition, WorkflowError> {
    runtime.block_on(flow.step(context(), journal, event))
}

fn journal_len(journal: &FlowJournal) -> usize {
    serde_json::to_value(journal).unwrap()["steps"]
        .as_array()
        .map_or(0, Vec::len)
}

fn expected_log(steps: &[Step]) -> Vec<String> {
    steps
        .iter()
        .map(|step| match step {
            Step::Activity(value) => format!("activity:{}", value.wrapping_mul(2)),
            Step::Child { fails: true, .. } => "child_failed:child_cancelled".to_string(),
            Step::Child { message, .. } => format!("child:{message}"),
            Step::Sleep(wake_at) => format!("slept:{wake_at}"),
        })
        .collect()
}

/// The event that resolves script step `index` (0-based).
fn resolving_event(step: &Step, index: usize) -> WorkflowEvent {
    let command_sequence = u32::try_from(index + 1).unwrap();
    match step {
        Step::Activity(value) => WorkflowEvent::ActivitySucceeded {
            command_sequence,
            result: ActivityResult::new(
                Double::KIND,
                Double::VERSION,
                value.wrapping_mul(2).to_string(),
            )
            .unwrap(),
        },
        Step::Child { fails: true, .. } => WorkflowEvent::ChildFailed {
            command_sequence,
            kind: Echo::KIND.to_string(),
            version: Echo::VERSION,
            category: "child_cancelled".to_string(),
            message: "operator cancelled".to_string(),
        },
        Step::Child { message, .. } => WorkflowEvent::ChildSucceeded {
            command_sequence,
            result: ChildResult::new(
                Echo::KIND,
                Echo::VERSION,
                serde_json::to_string(message).unwrap(),
            )
            .unwrap(),
        },
        Step::Sleep(_) => WorkflowEvent::TimerFired { command_sequence },
    }
}

/// The journal entry the engine records for a resolved step, in the
/// serialized `FlowJournal` format.
fn journal_entry_json(step: &Step) -> Value {
    match step {
        Step::Activity(value) => json!({
            "stepKind": format!("activity:{}", Double::KIND),
            "stepVersion": Double::VERSION,
            "outcome": {"status": "succeeded", "result_json": value.wrapping_mul(2).to_string()},
        }),
        Step::Child { fails: true, .. } => json!({
            "stepKind": format!("child:{}", Echo::KIND),
            "stepVersion": Echo::VERSION,
            "outcome": {"status": "failed", "category": "child_cancelled", "message": "operator cancelled"},
        }),
        Step::Child { message, .. } => json!({
            "stepKind": format!("child:{}", Echo::KIND),
            "stepVersion": Echo::VERSION,
            "outcome": {"status": "succeeded", "result_json": serde_json::to_string(message).unwrap()},
        }),
        Step::Sleep(_) => json!({
            "stepKind": "timer",
            "outcome": {"status": "succeeded", "result_json": "null"},
        }),
    }
}

fn full_journal(steps: &[Step]) -> FlowJournal {
    let entries: Vec<Value> = steps.iter().map(journal_entry_json).collect();
    serde_json::from_value(json!({ "steps": entries })).expect("journal json")
}

/// Asserts that a suspension transition requests exactly `expected` at
/// 0-based position `index` with a journal of length `index`.
fn assert_requests(
    transition: &Transition,
    expected: &Step,
    index: usize,
) -> Result<(), TestCaseError> {
    match (transition, expected) {
        (WorkflowTransition::RunActivity { state, activity }, Step::Activity(value)) => {
            prop_assert_eq!(journal_len(state), index);
            prop_assert_eq!(activity.kind(), Double::KIND);
            prop_assert_eq!(activity.version(), Double::VERSION);
            let payload: Double = serde_json::from_str(activity.payload_json()).unwrap();
            prop_assert_eq!(payload.value, *value);
            let expected_key = format!("wf:{WORKFLOW_ID}:step:{}", index + 1);
            prop_assert_eq!(activity.operation_key(), Some(expected_key.as_str()));
        }
        (WorkflowTransition::RunChild { state, child }, Step::Child { message, .. }) => {
            prop_assert_eq!(journal_len(state), index);
            prop_assert_eq!(child.kind(), Echo::KIND);
            let input: Echo = serde_json::from_str(child.input_json()).unwrap();
            prop_assert_eq!(&input.message, message);
            prop_assert!(child.deduplication_key().is_none());
        }
        (
            WorkflowTransition::SleepUntil {
                state,
                wake_at_millis,
            },
            Step::Sleep(wake_at),
        ) => {
            prop_assert_eq!(journal_len(state), index);
            prop_assert_eq!(wake_at_millis, wake_at);
        }
        (other, expected) => {
            prop_assert!(
                false,
                "position {}: expected {:?}, got {:?}",
                index,
                expected,
                other
            );
        }
    }
    Ok(())
}

fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        any::<i64>().prop_map(Step::Activity),
        (".{0,12}", any::<bool>()).prop_map(|(message, fails)| Step::Child { message, fails }),
        any::<i64>().prop_map(Step::Sleep),
    ]
}

fn script_strategy() -> impl Strategy<Value = Vec<Step>> {
    proptest::collection::vec(step_strategy(), 0..12)
}

/// Runs the script to completion, returning every suspension transition.
fn drive(
    runtime: &tokio::runtime::Runtime,
    flow: &Script,
) -> Result<(Vec<Transition>, Vec<String>), TestCaseError> {
    let mut journal = FlowJournal::default();
    let mut event = WorkflowEvent::Started;
    let mut suspensions = Vec::new();
    for index in 0..=flow.steps.len() {
        let transition = step(runtime, flow, journal.clone(), event).unwrap();
        if let WorkflowTransition::Complete { output } = transition {
            prop_assert_eq!(index, flow.steps.len(), "completed early");
            return Ok((suspensions, output));
        }
        assert_requests(&transition, &flow.steps[index], index)?;
        journal = match &transition {
            WorkflowTransition::RunActivity { state, .. }
            | WorkflowTransition::RunChild { state, .. }
            | WorkflowTransition::SleepUntil { state, .. } => state.clone(),
            other => {
                return Err(TestCaseError::fail(format!(
                    "unexpected transition {other:?}"
                )))
            }
        };
        event = resolving_event(&flow.steps[index], index);
        suspensions.push(transition);
    }
    Err(TestCaseError::fail("flow did not complete after all steps"))
}

fn state_of(transition: &Transition) -> FlowJournal {
    match transition {
        WorkflowTransition::RunActivity { state, .. }
        | WorkflowTransition::RunChild { state, .. }
        | WorkflowTransition::SleepUntil { state, .. } => state.clone(),
        other => panic!("not a suspension: {other:?}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Each activation issues exactly the first unjournaled step; the flow
    /// completes after one activation per step with the expected output.
    #[test]
    fn flow_issues_each_step_once_in_order(steps in script_strategy()) {
        let runtime = runtime();
        let flow = Script { steps: steps.clone() };
        let (suspensions, output) = drive(&runtime, &flow)?;
        prop_assert_eq!(suspensions.len(), steps.len());
        prop_assert_eq!(output, expected_log(&steps));
    }

    /// Re-activating a persisted journal without a new result reissues the
    /// same pending step, never an earlier (journaled) one.
    #[test]
    fn continued_replay_reissues_only_the_pending_step(steps in script_strategy()) {
        let runtime = runtime();
        let flow = Script { steps: steps.clone() };
        let (suspensions, _) = drive(&runtime, &flow)?;
        for (index, suspension) in suspensions.iter().enumerate() {
            let replayed = step(&runtime, &flow, state_of(suspension), WorkflowEvent::Continued).unwrap();
            prop_assert_eq!(&replayed, suspension);
            assert_requests(&replayed, &steps[index], index)?;
        }
    }

    /// Replaying the complete journal of a deterministic flow completes with
    /// the same output and issues no command.
    #[test]
    fn full_journal_replay_completes_with_same_output(steps in script_strategy()) {
        let runtime = runtime();
        let flow = Script { steps: steps.clone() };
        let (_, output) = drive(&runtime, &flow)?;
        let journal = full_journal(&steps);
        prop_assert_eq!(journal_len(&journal), steps.len());
        for event in [WorkflowEvent::Continued, WorkflowEvent::Started] {
            let replayed = step(&runtime, &flow, journal.clone(), event).unwrap();
            prop_assert_eq!(replayed, WorkflowTransition::Complete { output: output.clone() });
        }
    }

    /// A result whose command sequence is not `journal length + 1` is
    /// rejected instead of being journaled at the wrong position.
    #[test]
    fn out_of_sequence_results_fail_closed(
        steps in proptest::collection::vec(step_strategy(), 1..10),
        position in any::<prop::sample::Index>(),
        wrong in any::<u32>(),
    ) {
        let runtime = runtime();
        let flow = Script { steps: steps.clone() };
        let (suspensions, _) = drive(&runtime, &flow)?;
        let index = position.index(steps.len());
        let expected = u32::try_from(index + 1).unwrap();
        prop_assume!(wrong != expected);
        let event = match resolving_event(&steps[index], index) {
            WorkflowEvent::ActivitySucceeded { result, .. } => {
                WorkflowEvent::ActivitySucceeded { command_sequence: wrong, result }
            }
            WorkflowEvent::ChildSucceeded { result, .. } => {
                WorkflowEvent::ChildSucceeded { command_sequence: wrong, result }
            }
            WorkflowEvent::ChildFailed { kind, version, category, message, .. } => {
                WorkflowEvent::ChildFailed { command_sequence: wrong, kind, version, category, message }
            }
            WorkflowEvent::TimerFired { .. } => WorkflowEvent::TimerFired { command_sequence: wrong },
            other => unreachable!("{other:?}"),
        };
        let result = step(&runtime, &flow, state_of(&suspensions[index]), event);
        prop_assert!(result.is_err(), "accepted sequence {} at position {}", wrong, index + 1);
    }

    /// Replaying a journal against flow code whose step kind changed at an
    /// already-journaled position fails closed.
    #[test]
    fn changed_step_kind_fails_closed(
        steps in proptest::collection::vec(step_strategy(), 1..10),
        position in any::<prop::sample::Index>(),
        replacement in step_strategy(),
    ) {
        let index = position.index(steps.len());
        prop_assume!(!steps[index].same_kind(&replacement));
        let runtime = runtime();
        let mut changed = steps.clone();
        changed[index] = replacement;
        let changed_flow = Script { steps: changed };
        let result = step(&runtime, &changed_flow, full_journal(&steps), WorkflowEvent::Continued);
        let is_definition_error = matches!(&result, Err(error) if error.category == "flow_definition");
        prop_assert!(is_definition_error, "got {:?}", result);
    }

    /// Flow code that stops before consuming the whole journal fails closed.
    #[test]
    fn unconsumed_journal_fails_closed(
        steps in proptest::collection::vec(step_strategy(), 1..10),
        keep in any::<prop::sample::Index>(),
    ) {
        let kept = keep.index(steps.len());
        let runtime = runtime();
        let shorter = Script { steps: steps[..kept].to_vec() };
        let result = step(&runtime, &shorter, full_journal(&steps), WorkflowEvent::Continued);
        let is_definition_error = matches!(&result, Err(error) if error.category == "flow_definition");
        prop_assert!(is_definition_error, "got {:?}", result);
    }
    /// Replaying a journal whose recorded step version differs from the
    /// code's definition version fails closed.
    #[test]
    fn changed_step_version_fails_closed(
        steps in proptest::collection::vec(step_strategy(), 1..10),
        position in any::<prop::sample::Index>(),
        version in prop_oneof![Just(None), any::<i32>().prop_map(Some)],
    ) {
        let index = position.index(steps.len());
        let mut journal = serde_json::to_value(full_journal(&steps)).unwrap();
        let entry = &mut journal["steps"][index];
        let original = entry.get("stepVersion").and_then(Value::as_i64).map(|v| v as i32);
        prop_assume!(original != version);
        match version {
            Some(version) => entry["stepVersion"] = json!(version),
            None => {
                entry.as_object_mut().unwrap().remove("stepVersion");
            }
        }
        let journal: FlowJournal = serde_json::from_value(journal).unwrap();
        let runtime = runtime();
        let flow = Script { steps };
        let result = step(&runtime, &flow, journal, WorkflowEvent::Continued);
        let is_definition_error = matches!(&result, Err(error) if error.category == "flow_definition");
        prop_assert!(is_definition_error, "got {:?}", result);
    }
}
