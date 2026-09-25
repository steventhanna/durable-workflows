use durable_workflows::{durable_flow, WfError};

#[durable_flow(kind = "ctxless_flow", version = 1)]
async fn ctxless_flow(value: i64) -> Result<i64, WfError> {
    Ok(value)
}

fn main() {}
