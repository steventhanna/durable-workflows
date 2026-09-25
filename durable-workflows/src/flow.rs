//! Durable-async flows: write a workflow as ordinary `async` control flow and
//! let the runtime replay it deterministically over a persisted step journal.
//!
//! A flow is re-executed from the top on every activation. Completed steps are
//! recorded in the workflow's `state_json` as a [`FlowJournal`]; a [`WfCtx`]
//! await either replays a recorded result instantly or suspends at the first
//! unresolved step, which lowers to exactly one engine transition
//! (`RunActivity`, `RunChild`, or `SleepUntil`). Flow code between awaits must
//! be deterministic: replay verifies each requested step against the recorded
//! entry and fails closed on a mismatch.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    ActivityCommand, ActivityHandler, ChildWorkflowCommand, DurableWorkflow, WorkflowContext,
    WorkflowError, WorkflowEvent, WorkflowHandler, WorkflowId, WorkflowTransition,
};

/// One completed step in a flow's replay journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    step_kind: String,
    /// Activity/child definition version. Timers leave this unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    step_version: Option<i32>,
    outcome: StepOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum StepOutcome {
    Succeeded { result_json: String },
    Failed { category: String, message: String },
}

/// The durable state of a flow: the ordered results of every completed step.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowJournal {
    steps: Vec<JournalEntry>,
}

/// Errors surfaced to and from flow code.
#[derive(Debug, thiserror::Error)]
pub enum WfError {
    /// Internal control-flow marker used to suspend at an unresolved step.
    /// Flow code must propagate this error (usually via `?`) rather than
    /// swallow it.
    #[doc(hidden)]
    #[error("flow suspended at a durable step")]
    Suspended,

    /// A child workflow reached a terminal failure (`failed`, `cancelled`, or
    /// operator-superseded). The flow may match on this to compensate, or
    /// propagate it to fail the whole workflow.
    #[error("child workflow {kind} failed: {category}: {message}")]
    ChildFailed {
        kind: String,
        category: String,
        message: String,
    },

    /// A definition or replay-determinism problem.
    #[error("flow definition error: {0}")]
    Definition(String),

    /// A domain error raised by flow code.
    #[error("{category}: {message}")]
    Domain { category: String, message: String },
}

impl WfError {
    pub fn new(category: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Domain {
            category: category.into(),
            message: message.into(),
        }
    }
}

impl From<crate::DurableError> for WfError {
    fn from(error: crate::DurableError) -> Self {
        Self::Definition(error.to_string())
    }
}

enum PendingStep {
    Activity(ActivityCommand),
    Child(ChildWorkflowCommand),
    Sleep { wake_at_millis: i64 },
}

/// The execution context handed to a flow on every activation.
///
/// Awaiting [`WfCtx::run`], [`WfCtx::child`], or [`WfCtx::sleep_until`] either
/// replays the recorded result for that position or suspends the flow.
pub struct WfCtx<'a, C> {
    application: &'a C,
    workflow_id: Option<WorkflowId>,
    steps: Vec<JournalEntry>,
    cursor: usize,
    pending: Option<PendingStep>,
}

impl<'a, C> WfCtx<'a, C> {
    fn new(application: &'a C, workflow_id: Option<WorkflowId>, journal: FlowJournal) -> Self {
        Self {
            application,
            workflow_id,
            steps: journal.steps,
            cursor: 0,
            pending: None,
        }
    }

    pub fn application(&self) -> &'a C {
        self.application
    }

    pub fn workflow_id(&self) -> Option<WorkflowId> {
        self.workflow_id
    }

    /// Runs an activity durably, returning its typed output.
    ///
    /// The operation key defaults to `wf:{workflow_id}:step:{position}` so
    /// activity side effects are deduplicable without hand-rolled keys.
    pub async fn run<A>(&mut self, activity: &A) -> Result<A::Output, WfError>
    where
        A: ActivityHandler<Context = C>,
    {
        let operation_key = self.auto_operation_key();
        self.run_step(activity, operation_key).await
    }

    /// Runs an activity durably with an explicit idempotency key.
    pub async fn run_with_key<A>(
        &mut self,
        activity: &A,
        operation_key: impl Into<String>,
    ) -> Result<A::Output, WfError>
    where
        A: ActivityHandler<Context = C>,
    {
        self.run_step(activity, Some(operation_key.into())).await
    }

    async fn run_step<A>(
        &mut self,
        activity: &A,
        operation_key: Option<String>,
    ) -> Result<A::Output, WfError>
    where
        A: ActivityHandler<Context = C>,
    {
        let step_kind = format!("activity:{}", A::KIND);
        match self.replay(&step_kind, Some(A::VERSION))? {
            Some(StepOutcome::Succeeded { result_json }) => {
                serde_json::from_str(&result_json).map_err(|error| {
                    WfError::Definition(format!(
                        "recorded {step_kind} result no longer decodes: {error}"
                    ))
                })
            }
            Some(StepOutcome::Failed { category, message }) => Err(WfError::Definition(format!(
                "recorded {step_kind} result is a failure ({category}: {message}); activities never journal failures"
            ))),
            None => {
                let command = ActivityCommand::new(activity, operation_key)
                    .map_err(|error| WfError::Definition(error.to_string()))?;
                self.pending = Some(PendingStep::Activity(command));
                Err(WfError::Suspended)
            }
        }
    }

    /// Starts a child workflow durably and awaits its typed output.
    ///
    /// The child is deduplicated per parent step
    /// (`child:{parent_id}:{command_sequence}`) unless
    /// [`WfCtx::child_with_key`] supplies a domain key. A terminal child
    /// failure surfaces as [`WfError::ChildFailed`].
    pub async fn child<W>(&mut self, workflow: &W) -> Result<W::Output, WfError>
    where
        W: WorkflowHandler<Context = C>,
    {
        self.child_step(workflow, None).await
    }

    /// Starts a child workflow durably with an explicit deduplication key.
    pub async fn child_with_key<W>(
        &mut self,
        workflow: &W,
        deduplication_key: impl Into<String>,
    ) -> Result<W::Output, WfError>
    where
        W: WorkflowHandler<Context = C>,
    {
        self.child_step(workflow, Some(deduplication_key.into()))
            .await
    }

    async fn child_step<W>(
        &mut self,
        workflow: &W,
        deduplication_key: Option<String>,
    ) -> Result<W::Output, WfError>
    where
        W: WorkflowHandler<Context = C>,
    {
        let step_kind = format!("child:{}", W::KIND);
        match self.replay(&step_kind, Some(W::VERSION))? {
            Some(StepOutcome::Succeeded { result_json }) => serde_json::from_str(&result_json)
                .map_err(|error| {
                    WfError::Definition(format!(
                        "recorded {step_kind} result no longer decodes: {error}"
                    ))
                }),
            Some(StepOutcome::Failed { category, message }) => Err(WfError::ChildFailed {
                kind: W::KIND.to_string(),
                category,
                message,
            }),
            None => {
                let mut command = ChildWorkflowCommand::new(workflow)
                    .map_err(|error| WfError::Definition(error.to_string()))?;
                if let Some(deduplication_key) = deduplication_key {
                    command = command.with_deduplication_key(deduplication_key);
                }
                self.pending = Some(PendingStep::Child(command));
                Err(WfError::Suspended)
            }
        }
    }

    /// Sleeps durably until the given UTC millisecond timestamp.
    pub async fn sleep_until(&mut self, wake_at_millis: i64) -> Result<(), WfError> {
        match self.replay("timer", None)? {
            Some(StepOutcome::Succeeded { .. }) => Ok(()),
            Some(StepOutcome::Failed { category, message }) => Err(WfError::Definition(format!(
                "recorded timer result is a failure ({category}: {message})"
            ))),
            None => {
                self.pending = Some(PendingStep::Sleep { wake_at_millis });
                Err(WfError::Suspended)
            }
        }
    }

    fn replay(
        &mut self,
        step_kind: &str,
        step_version: Option<i32>,
    ) -> Result<Option<StepOutcome>, WfError> {
        if self.pending.is_some() {
            // The flow caught a suspension and kept requesting steps; every
            // later step also suspends so only the first unresolved step wins.
            return Err(WfError::Suspended);
        }
        let Some(entry) = self.steps.get(self.cursor) else {
            return Ok(None);
        };
        if entry.step_kind != step_kind {
            return Err(WfError::Definition(format!(
                "nondeterministic flow: step {} requested {step_kind} but the journal recorded {}",
                self.cursor + 1,
                entry.step_kind
            )));
        }
        if entry.step_version != step_version {
            return Err(WfError::Definition(format!(
                "nondeterministic flow: step {} requested {step_kind} version {step_version:?} but the journal recorded version {:?}",
                self.cursor + 1,
                entry.step_version
            )));
        }
        let outcome = entry.outcome.clone();
        self.cursor += 1;
        Ok(Some(outcome))
    }

    fn auto_operation_key(&self) -> Option<String> {
        self.workflow_id
            .map(|workflow_id| format!("wf:{workflow_id}:step:{}", self.cursor + 1))
    }
}

/// A workflow written as ordinary async control flow over durable steps.
///
/// Implementors gain a [`WorkflowHandler`] implementation automatically, so a
/// flow registers, starts, and composes as a child exactly like a hand-written
/// state machine. Use the `#[durable_flow]` attribute macro to derive this
/// from an `async fn`.
#[async_trait]
pub trait DurableFlow: DurableWorkflow {
    type Context: Send + Sync + 'static;
    type Output: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;

    async fn run(&self, ctx: &mut WfCtx<'_, Self::Context>) -> Result<Self::Output, WfError>;
}

#[async_trait]
impl<F> WorkflowHandler for F
where
    F: DurableFlow,
{
    type Context = F::Context;
    type State = FlowJournal;
    type Approval = ();
    type Output = F::Output;

    fn initial_state(&self) -> Self::State {
        FlowJournal::default()
    }

    async fn step(
        &self,
        context: WorkflowContext<'_, Self::Context>,
        state: Self::State,
        event: WorkflowEvent,
    ) -> Result<WorkflowTransition<Self::State, Self::Approval, Self::Output>, WorkflowError> {
        flow_step(self, context, state, event).await
    }
}

async fn flow_step<F>(
    flow: &F,
    context: WorkflowContext<'_, F::Context>,
    mut journal: FlowJournal,
    event: WorkflowEvent,
) -> Result<WorkflowTransition<FlowJournal, (), F::Output>, WorkflowError>
where
    F: DurableFlow,
{
    apply_event(&mut journal, event)?;
    let mut ctx = WfCtx::new(context.application(), context.workflow_id(), journal);
    match flow.run(&mut ctx).await {
        Ok(output) => {
            if ctx.cursor != ctx.steps.len() {
                return Err(WorkflowError::new(
                    "flow_definition",
                    format!(
                        "flow completed without replaying {} recorded journal step(s)",
                        ctx.steps.len().saturating_sub(ctx.cursor)
                    ),
                ));
            }
            Ok(WorkflowTransition::Complete { output })
        }
        Err(WfError::Suspended) => {
            let pending = ctx.pending.take();
            let state = FlowJournal { steps: ctx.steps };
            match pending {
                Some(PendingStep::Activity(activity)) => {
                    Ok(WorkflowTransition::RunActivity { state, activity })
                }
                Some(PendingStep::Child(child)) => {
                    Ok(WorkflowTransition::RunChild { state, child })
                }
                Some(PendingStep::Sleep { wake_at_millis }) => Ok(WorkflowTransition::SleepUntil {
                    state,
                    wake_at_millis,
                }),
                None => Err(WorkflowError::new(
                    "flow",
                    "flow suspended without a pending step",
                )),
            }
        }
        Err(WfError::ChildFailed {
            kind,
            category,
            message,
        }) => Err(WorkflowError::new(
            "child_failed",
            format!("child workflow {kind} failed: {category}: {message}"),
        )),
        Err(WfError::Definition(message)) => Err(WorkflowError::new("flow_definition", message)),
        Err(WfError::Domain { category, message }) => Err(WorkflowError::new(category, message)),
    }
}

fn apply_event(journal: &mut FlowJournal, event: WorkflowEvent) -> Result<(), WorkflowError> {
    let expected_sequence = u32::try_from(journal.steps.len().saturating_add(1))
        .map_err(|_| WorkflowError::new("flow", "flow journal length overflow"))?;
    let entry = match event {
        WorkflowEvent::Started | WorkflowEvent::Continued => return Ok(()),
        WorkflowEvent::ActivitySucceeded {
            command_sequence,
            result,
        } => {
            ensure_sequence(command_sequence, expected_sequence)?;
            JournalEntry {
                step_kind: format!("activity:{}", result.kind()),
                step_version: Some(result.version()),
                outcome: StepOutcome::Succeeded {
                    result_json: result.output_json().to_string(),
                },
            }
        }
        WorkflowEvent::ChildSucceeded {
            command_sequence,
            result,
        } => {
            ensure_sequence(command_sequence, expected_sequence)?;
            JournalEntry {
                step_kind: format!("child:{}", result.kind()),
                step_version: Some(result.version()),
                outcome: StepOutcome::Succeeded {
                    result_json: result.output_json().to_string(),
                },
            }
        }
        WorkflowEvent::ChildFailed {
            command_sequence,
            kind,
            version,
            category,
            message,
        } => {
            ensure_sequence(command_sequence, expected_sequence)?;
            JournalEntry {
                step_kind: format!("child:{kind}"),
                step_version: Some(version),
                outcome: StepOutcome::Failed { category, message },
            }
        }
        WorkflowEvent::TimerFired { command_sequence } => {
            ensure_sequence(command_sequence, expected_sequence)?;
            JournalEntry {
                step_kind: "timer".to_string(),
                step_version: None,
                outcome: StepOutcome::Succeeded {
                    result_json: "null".to_string(),
                },
            }
        }
        WorkflowEvent::ApprovalResolved { .. } | WorkflowEvent::ApprovalExpired { .. } => {
            return Err(WorkflowError::new(
                "flow",
                "flows do not support approval waits",
            ));
        }
    };
    journal.steps.push(entry);
    Ok(())
}

fn ensure_sequence(actual: u32, expected: u32) -> Result<(), WorkflowError> {
    if actual == expected {
        return Ok(());
    }
    Err(WorkflowError::new(
        "flow",
        format!("flow received step result {actual} but the journal expects {expected}"),
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        ActivityContext, ActivityError, ActivityResult, ActivityTopic, BackoffPolicy, ChildResult,
        DurableActivity, RetryPolicy,
    };

    use super::*;

    #[derive(Clone, Copy)]
    struct TestTopic;

    impl ActivityTopic for TestTopic {
        fn key(self) -> &'static str {
            "flow_test_topic"
        }

        fn max_concurrency(self) -> u32 {
            1
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct DoubleActivity {
        value: i64,
    }

    impl DurableActivity for DoubleActivity {
        type Topic = TestTopic;
        const KIND: &'static str = "double_activity";
        const VERSION: i32 = 1;
        const MAX_ATTEMPTS: u32 = 1;
        const TIMEOUT: Duration = Duration::from_secs(5);
        const LEASE_DURATION: Duration = Duration::from_secs(10);

        fn topic() -> Self::Topic {
            TestTopic
        }

        fn retry_policy() -> RetryPolicy {
            RetryPolicy::from_validated(BackoffPolicy::Fixed { delay_secs: 1 })
        }
    }

    #[async_trait]
    impl ActivityHandler for DoubleActivity {
        type Context = ();
        type Output = i64;

        async fn execute(&self, _context: ActivityContext<'_, ()>) -> Result<i64, ActivityError> {
            Ok(self.value * 2)
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct ChildEcho {
        message: String,
    }

    impl DurableWorkflow for ChildEcho {
        const KIND: &'static str = "child_echo";
        const VERSION: i32 = 1;
    }

    #[async_trait]
    impl DurableFlow for ChildEcho {
        type Context = ();
        type Output = String;

        async fn run(&self, _ctx: &mut WfCtx<'_, ()>) -> Result<String, WfError> {
            Ok(self.message.clone())
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct PipelineFlow {
        value: i64,
        with_child: bool,
    }

    impl DurableWorkflow for PipelineFlow {
        const KIND: &'static str = "pipeline_flow";
        const VERSION: i32 = 1;
    }

    #[async_trait]
    impl DurableFlow for PipelineFlow {
        type Context = ();
        type Output = i64;

        async fn run(&self, ctx: &mut WfCtx<'_, ()>) -> Result<i64, WfError> {
            let doubled = ctx.run(&DoubleActivity { value: self.value }).await?;
            if self.with_child {
                let echoed = ctx
                    .child(&ChildEcho {
                        message: doubled.to_string(),
                    })
                    .await?;
                let parsed: i64 = echoed
                    .parse()
                    .map_err(|_| WfError::new("parse", "child echo was not numeric"))?;
                let redoubled = ctx.run(&DoubleActivity { value: parsed }).await?;
                return Ok(redoubled);
            }
            Ok(doubled)
        }
    }

    fn context() -> WorkflowContext<'static, ()> {
        WorkflowContext::new(&()).with_workflow_id(WorkflowId::new(7).expect("workflow id"))
    }

    fn activity_result(value: i64) -> ActivityResult {
        ActivityResult::new(
            DoubleActivity::KIND,
            DoubleActivity::VERSION,
            value.to_string(),
        )
        .expect("activity result")
    }

    #[tokio::test]
    async fn started_flow_suspends_at_the_first_activity_with_an_auto_key() {
        let flow = PipelineFlow {
            value: 3,
            with_child: true,
        };
        let transition = flow
            .step(context(), FlowJournal::default(), WorkflowEvent::Started)
            .await
            .expect("first transition");
        let WorkflowTransition::RunActivity { state, activity } = transition else {
            panic!("expected an activity suspension");
        };
        assert!(state.steps.is_empty());
        assert_eq!(activity.kind(), DoubleActivity::KIND);
        assert_eq!(activity.operation_key(), Some("wf:7:step:1"));
    }

    #[tokio::test]
    async fn activity_result_replays_into_a_child_suspension() {
        let flow = PipelineFlow {
            value: 3,
            with_child: true,
        };
        let transition = flow
            .step(
                context(),
                FlowJournal::default(),
                WorkflowEvent::ActivitySucceeded {
                    command_sequence: 1,
                    result: activity_result(6),
                },
            )
            .await
            .expect("child transition");
        let WorkflowTransition::RunChild { state, child } = transition else {
            panic!("expected a child suspension");
        };
        assert_eq!(state.steps.len(), 1);
        assert_eq!(child.kind(), ChildEcho::KIND);
        assert!(child.deduplication_key().is_none());
    }

    #[tokio::test]
    async fn child_result_replays_and_the_flow_completes() {
        let flow = PipelineFlow {
            value: 3,
            with_child: true,
        };
        let journal = FlowJournal {
            steps: vec![JournalEntry {
                step_kind: format!("activity:{}", DoubleActivity::KIND),
                step_version: Some(DoubleActivity::VERSION),
                outcome: StepOutcome::Succeeded {
                    result_json: "6".to_string(),
                },
            }],
        };
        let transition = flow
            .step(
                context(),
                journal,
                WorkflowEvent::ChildSucceeded {
                    command_sequence: 2,
                    result: ChildResult::new(ChildEcho::KIND, ChildEcho::VERSION, "\"6\"".into())
                        .expect("child result"),
                },
            )
            .await
            .expect("second activity transition");
        let WorkflowTransition::RunActivity { state, activity } = transition else {
            panic!("expected the second activity suspension");
        };
        assert_eq!(state.steps.len(), 2);
        assert_eq!(activity.operation_key(), Some("wf:7:step:3"));

        let journal = state;
        let transition = flow
            .step(
                context(),
                journal,
                WorkflowEvent::ActivitySucceeded {
                    command_sequence: 3,
                    result: activity_result(12),
                },
            )
            .await
            .expect("completion");
        assert_eq!(transition, WorkflowTransition::Complete { output: 12 });
    }

    #[tokio::test]
    async fn child_failure_replays_as_a_typed_error_and_fails_the_flow() {
        let flow = PipelineFlow {
            value: 3,
            with_child: true,
        };
        let journal = FlowJournal {
            steps: vec![JournalEntry {
                step_kind: format!("activity:{}", DoubleActivity::KIND),
                step_version: Some(DoubleActivity::VERSION),
                outcome: StepOutcome::Succeeded {
                    result_json: "6".to_string(),
                },
            }],
        };
        let error = flow
            .step(
                context(),
                journal,
                WorkflowEvent::ChildFailed {
                    command_sequence: 2,
                    kind: ChildEcho::KIND.to_string(),
                    version: ChildEcho::VERSION,
                    category: "child_cancelled".to_string(),
                    message: "operator cancelled".to_string(),
                },
            )
            .await
            .expect_err("child failure must fail the flow");
        assert_eq!(error.category, "child_failed");
        assert!(error.message.contains("child_cancelled"));
    }

    #[tokio::test]
    async fn nondeterministic_replay_fails_closed() {
        let flow = PipelineFlow {
            value: 3,
            with_child: false,
        };
        let journal = FlowJournal {
            steps: vec![JournalEntry {
                step_kind: "child:unexpected_workflow".to_string(),
                step_version: Some(1),
                outcome: StepOutcome::Succeeded {
                    result_json: "null".to_string(),
                },
            }],
        };
        let error = flow
            .step(context(), journal, WorkflowEvent::Continued)
            .await
            .expect_err("mismatched journal must fail");
        assert_eq!(error.category, "flow_definition");
        assert!(error.message.contains("nondeterministic"));
    }

    #[tokio::test]
    async fn activity_version_mismatch_on_replay_fails_closed() {
        let flow = PipelineFlow {
            value: 3,
            with_child: false,
        };
        let journal = FlowJournal {
            steps: vec![JournalEntry {
                step_kind: format!("activity:{}", DoubleActivity::KIND),
                step_version: Some(DoubleActivity::VERSION + 1),
                outcome: StepOutcome::Succeeded {
                    result_json: "6".to_string(),
                },
            }],
        };
        let error = flow
            .step(context(), journal, WorkflowEvent::Continued)
            .await
            .expect_err("version mismatch must fail");
        assert_eq!(error.category, "flow_definition");
        assert!(error.message.contains("recorded version"));
    }

    #[tokio::test]
    async fn child_version_mismatch_on_replay_fails_closed() {
        let flow = PipelineFlow {
            value: 3,
            with_child: true,
        };
        let journal = FlowJournal {
            steps: vec![
                JournalEntry {
                    step_kind: format!("activity:{}", DoubleActivity::KIND),
                    step_version: Some(DoubleActivity::VERSION),
                    outcome: StepOutcome::Succeeded {
                        result_json: "6".to_string(),
                    },
                },
                JournalEntry {
                    step_kind: format!("child:{}", ChildEcho::KIND),
                    step_version: Some(ChildEcho::VERSION + 1),
                    outcome: StepOutcome::Succeeded {
                        result_json: "\"6\"".to_string(),
                    },
                },
            ],
        };
        let error = flow
            .step(context(), journal, WorkflowEvent::Continued)
            .await
            .expect_err("child version mismatch must fail");
        assert_eq!(error.category, "flow_definition");
        assert!(error.message.contains("recorded version"));
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct EmptyFlow;

    impl DurableWorkflow for EmptyFlow {
        const KIND: &'static str = "empty_flow";
        const VERSION: i32 = 1;
    }

    #[async_trait]
    impl DurableFlow for EmptyFlow {
        type Context = ();
        type Output = ();

        async fn run(&self, _ctx: &mut WfCtx<'_, ()>) -> Result<(), WfError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn completion_rejects_unreplayed_journal_steps() {
        let journal = FlowJournal {
            steps: vec![JournalEntry {
                step_kind: format!("activity:{}", DoubleActivity::KIND),
                step_version: Some(DoubleActivity::VERSION),
                outcome: StepOutcome::Succeeded {
                    result_json: "6".to_string(),
                },
            }],
        };
        let error = EmptyFlow
            .step(context(), journal, WorkflowEvent::Continued)
            .await
            .expect_err("unconsumed journal steps must fail closed");
        assert_eq!(error.category, "flow_definition");
        assert!(error.message.contains("without replaying"));
    }

    #[tokio::test]
    async fn out_of_order_step_results_fail_closed() {
        let flow = PipelineFlow {
            value: 3,
            with_child: false,
        };
        let error = flow
            .step(
                context(),
                FlowJournal::default(),
                WorkflowEvent::ActivitySucceeded {
                    command_sequence: 4,
                    result: activity_result(6),
                },
            )
            .await
            .expect_err("sequence gap must fail");
        assert_eq!(error.category, "flow");
        assert!(error.message.contains("expects 1"));
    }

    #[tokio::test]
    async fn flows_without_a_workflow_id_omit_auto_operation_keys() {
        let flow = PipelineFlow {
            value: 3,
            with_child: false,
        };
        let transition = flow
            .step(
                WorkflowContext::new(&()),
                FlowJournal::default(),
                WorkflowEvent::Started,
            )
            .await
            .expect("first transition");
        let WorkflowTransition::RunActivity { activity, .. } = transition else {
            panic!("expected an activity suspension");
        };
        assert_eq!(activity.operation_key(), None);
    }
}
