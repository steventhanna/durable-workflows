use std::time::Duration;

use durable_workflows::{BackoffPolicy, DurableError, RetryPolicy};

#[test]
fn fixed_backoff_is_independent_of_attempt_and_jitter() {
    let policy = RetryPolicy::fixed(9).unwrap();

    assert_eq!(
        policy.delay_for_attempt(1, 0).unwrap(),
        Duration::from_secs(9)
    );
    assert_eq!(
        policy.delay_for_attempt(8, 100).unwrap(),
        Duration::from_secs(9)
    );
}

#[test]
fn exponential_backoff_caps_before_applying_deterministic_jitter() {
    let policy = RetryPolicy::exponential(5, 20, 20).unwrap();

    assert_eq!(
        policy.delay_for_attempt(1, 50).unwrap(),
        Duration::from_secs(5)
    );
    assert_eq!(
        policy.delay_for_attempt(4, 50).unwrap(),
        Duration::from_secs(20)
    );
    assert_eq!(
        policy.delay_for_attempt(4, 0).unwrap(),
        Duration::from_secs(16)
    );
    assert_eq!(
        policy.delay_for_attempt(4, 100).unwrap(),
        Duration::from_secs(24)
    );
}

#[test]
fn backoff_rejects_zero_attempts_and_out_of_range_jitter() {
    let policy = RetryPolicy::fixed(1).unwrap();

    assert!(matches!(
        policy.delay_for_attempt(0, 50),
        Err(DurableError::InvalidDefinition(_))
    ));
    assert!(matches!(
        policy.delay_for_attempt(1, 101),
        Err(DurableError::InvalidDefinition(_))
    ));

    assert_eq!(policy.backoff(), BackoffPolicy::Fixed { delay_secs: 1 });
}
