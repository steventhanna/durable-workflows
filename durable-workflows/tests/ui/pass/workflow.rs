use durable_workflows::DurableWorkflow as _;

#[derive(serde::Serialize, serde::Deserialize, durable_workflows::DurableWorkflow)]
#[workflow(kind = "test_workflow", version = 3)]
struct TestWorkflow {
    value: String,
}

fn main() {
    assert_eq!(TestWorkflow::KIND, "test_workflow");
    assert_eq!(TestWorkflow::VERSION, 3);
}
