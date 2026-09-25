use durable_workflows::{durable_flow, WfCtx, WfError};

#[durable_flow(version = 1)]
async fn missing_kind(_ctx: &mut WfCtx<'_, ()>) -> Result<(), WfError> {
    Ok(())
}

fn main() {}
