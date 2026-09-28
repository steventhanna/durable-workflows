// `RetryPolicy` has no unchecked constructor outside the crate: the former
// `from_validated` hatch is gone (E0599), and the note lists the only
// constructors, all checked. Its field is private too
// (`retry_policy_field_private`). Every policy is within the retry bounds.
use durable_workflows::{BackoffPolicy, RetryPolicy};

fn main() {
    let backoff = BackoffPolicy::Exponential {
        initial_secs: 10,
        max_secs: 1,
        jitter_percent: 0,
    };
    let _ = RetryPolicy::from_validated(backoff);
}
