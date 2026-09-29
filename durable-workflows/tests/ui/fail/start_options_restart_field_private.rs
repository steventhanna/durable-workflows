// The restart lineage is set only by the engine (recoverable start and the
// admin restart), which checks that the source may be restarted (N3).
use durable_workflows::{StartOptions, WorkflowId};

fn main() {
    let source = WorkflowId::new(1).expect("valid workflow id");
    let _ = StartOptions {
        restarted_from_workflow_id: Some(source),
        root_workflow_id: Some(source),
        ..StartOptions::default()
    };
}
