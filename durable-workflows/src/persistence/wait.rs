//! The persisted wait of a workflow: the `wait_kind` and `wait_reference_id`
//! columns of `durable_workflow`, read through one parser and written
//! through one changeset fragment.

use diesel::AsChangeset;

use super::{WaitKind, WorkflowRow};
use crate::ids::{ActivityId, ApprovalId, WorkflowId};
use crate::schema::durable_workflow;
use crate::DurableError;

/// What a workflow waits on. Each kind carries the one reference its
/// `wait_reference_id` holds, so a kind without a reference, or a
/// reference without a kind, has no value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wait {
    /// A durable timer, referenced by the command sequence that set it.
    Timer {
        command_sequence: u32,
    },
    /// An activity row; a dead-lettered activity keeps this wait while its
    /// workflow is blocked.
    Activity(ActivityId),
    Child(WorkflowId),
    Approval(ApprovalId),
}

impl Wait {
    /// The one reading of the two wait columns of workflow `workflow_id`.
    pub(crate) fn parse(
        workflow_id: WorkflowId,
        kind: Option<WaitKind>,
        reference: Option<i64>,
    ) -> Result<Option<Self>, DurableError> {
        let (kind, reference) = match (kind, reference) {
            (None, None) => return Ok(None),
            (Some(kind), Some(reference)) => (kind, reference),
            (Some(kind), None) => {
                return Err(DurableError::InvalidState(format!(
                    "workflow {workflow_id} {kind} wait has no reference"
                )))
            }
            (None, Some(reference)) => {
                return Err(DurableError::InvalidState(format!(
                    "workflow {workflow_id} wait reference {reference} has no wait kind"
                )))
            }
        };
        Ok(Some(match kind {
            WaitKind::Timer => Self::Timer {
                command_sequence: u32::try_from(reference).map_err(|_| {
                    DurableError::InvalidState(format!(
                        "workflow {workflow_id} timer reference {reference} is not a command sequence"
                    ))
                })?,
            },
            WaitKind::Activity => Self::Activity(ActivityId::new(reference)?),
            WaitKind::Child => Self::Child(WorkflowId::new(reference)?),
            WaitKind::Approval => Self::Approval(ApprovalId::new(reference)?),
        }))
    }

    pub(crate) const fn kind(self) -> WaitKind {
        match self {
            Self::Timer { .. } => WaitKind::Timer,
            Self::Activity(_) => WaitKind::Activity,
            Self::Child(_) => WaitKind::Child,
            Self::Approval(_) => WaitKind::Approval,
        }
    }

    pub(crate) fn reference_id(self) -> i64 {
        match self {
            Self::Timer { command_sequence } => i64::from(command_sequence),
            Self::Activity(id) => id.get(),
            Self::Child(id) => id.get(),
            Self::Approval(id) => id.get(),
        }
    }
}

impl WorkflowRow {
    /// This row's wait, parsed from its two wait columns.
    pub(crate) fn wait(&self) -> Result<Option<Wait>, DurableError> {
        Wait::parse(self.id, self.wait_kind, self.wait_reference_id)
    }
}

/// Changeset fragment that writes both wait columns from one `Wait` (or
/// clears both), so an update cannot set one without the other.
#[derive(Debug, Clone, Copy, AsChangeset)]
#[diesel(table_name = durable_workflow)]
#[diesel(treat_none_as_null = true)]
pub(crate) struct WaitColumns {
    wait_kind: Option<WaitKind>,
    wait_reference_id: Option<i64>,
}

impl WaitColumns {
    pub(crate) fn on(wait: Wait) -> Self {
        Self {
            wait_kind: Some(wait.kind()),
            wait_reference_id: Some(wait.reference_id()),
        }
    }

    pub(crate) const fn cleared() -> Self {
        Self {
            wait_kind: None,
            wait_reference_id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Wait, WaitKind};
    use crate::ids::{ActivityId, ApprovalId, WorkflowId};
    use crate::DurableError;

    #[test]
    fn every_wait_round_trips_through_its_columns() {
        let workflow = WorkflowId::new(1).unwrap();
        let waits = [
            Wait::Timer {
                command_sequence: 7,
            },
            Wait::Activity(ActivityId::new(11).unwrap()),
            Wait::Child(WorkflowId::new(12).unwrap()),
            Wait::Approval(ApprovalId::new(13).unwrap()),
        ];
        for wait in waits {
            let parsed =
                Wait::parse(workflow, Some(wait.kind()), Some(wait.reference_id())).unwrap();
            assert_eq!(parsed, Some(wait));
        }
        assert_eq!(Wait::parse(workflow, None, None).unwrap(), None);
    }

    #[test]
    fn half_a_wait_is_an_error() {
        let workflow = WorkflowId::new(1).unwrap();
        for kind in WaitKind::ALL {
            assert!(matches!(
                Wait::parse(workflow, Some(*kind), None),
                Err(DurableError::InvalidState(_))
            ));
        }
        assert!(matches!(
            Wait::parse(workflow, None, Some(5)),
            Err(DurableError::InvalidState(_))
        ));
        assert!(matches!(
            Wait::parse(workflow, Some(WaitKind::Timer), Some(-1)),
            Err(DurableError::InvalidState(_))
        ));
        assert!(matches!(
            Wait::parse(workflow, Some(WaitKind::Activity), Some(0)),
            Err(DurableError::InvalidId(0))
        ));
    }
}
