use durable_workflows::{durable_flow, WfCtx, WfError};

#[durable_flow(kind = "sync_flow", version = 1)]
fn sync_flow(_ctx: &mut WfCtx<'_, ()>) -> Result<(), WfError> {
    Ok(())
}

fn main() {}
