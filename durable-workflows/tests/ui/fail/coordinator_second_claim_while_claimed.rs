// A coordinator holds one outstanding workflow claim: the claim borrows the
// coordinator mutably until it is activated or dropped, which is the model's
// one-claim-per-runtime rule (`TC1_Claim`).
use durable_workflows::{DurableError, WorkflowCoordinator};

async fn second_claim_while_claimed(
    coordinator: &mut WorkflowCoordinator<()>,
) -> Result<(), DurableError> {
    let first = coordinator.claim_one().await?;
    let second = coordinator.claim_one().await?;
    drop(second);
    if let Some(first) = first {
        first.activate().await?;
    }
    Ok(())
}

fn main() {
    let _ = second_claim_while_claimed;
}
