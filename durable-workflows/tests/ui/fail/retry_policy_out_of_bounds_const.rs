// `RetryPolicy::from_validated` (emitted by `#[derive(DurableActivity)]` in a
// `const` block) checks the retry bounds at compile time: an out-of-bounds
// policy is E0080, so the macro's own checks cannot drift from the library's.
use durable_workflows::{BackoffPolicy, RetryPolicy};

const POLICY: RetryPolicy = RetryPolicy::from_validated(BackoffPolicy::Exponential {
    initial_secs: 10,
    max_secs: 1,
    jitter_percent: 0,
});

fn main() {
    let _ = POLICY;
}
