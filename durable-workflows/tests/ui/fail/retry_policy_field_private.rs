// `RetryPolicy`'s field is private (E0451), so a struct expression cannot
// build a policy outside the retry bounds; see
// `retry_policy_unchecked_constructor`.
use durable_workflows::{BackoffPolicy, RetryPolicy};

fn main() {
    let backoff = BackoffPolicy::Exponential {
        initial_secs: 10,
        max_secs: 1,
        jitter_percent: 0,
    };
    let _ = RetryPolicy { backoff };
}
