use durable_workflows::{durable_flow, DurableWorkflow as _, WfCtx, WfError};

#[durable_flow(kind = "test_flow", version = 2)]
async fn test_flow(_ctx: &mut WfCtx<'_, ()>, value: i64, label: String) -> Result<i64, WfError> {
    let _ = label;
    Ok(value)
}

fn main() {
    assert_eq!(TestFlow::KIND, "test_flow");
    assert_eq!(TestFlow::VERSION, 2);
    let flow = TestFlow {
        value: 7,
        label: "seven".to_string(),
    };
    assert_eq!(flow.clone().value, 7);
}
