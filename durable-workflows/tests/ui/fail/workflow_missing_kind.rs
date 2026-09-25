#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableWorkflow)]
#[workflow(version = 1)]
struct MissingWorkflowKind;

fn main() {}
