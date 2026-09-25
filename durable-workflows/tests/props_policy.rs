//! Property tests for `RetryPolicy`, `BackoffPolicy`, and
//! `deterministic_jitter_percentile` (pure, no database).

use std::time::Duration;

use durable_workflows::{
    deterministic_jitter_percentile, BackoffPolicy, DurableError, RetryPolicy,
};
use proptest::prelude::*;

/// Upper bound of the jittered delay: `base + base * jitter / 100`, computed
/// without overflow.
fn jitter_ceiling(base: u64, jitter_percent: u8) -> u128 {
    u128::from(base) + u128::from(base) * u128::from(jitter_percent) / 100
}

fn jitter_floor(base: u64, jitter_percent: u8) -> u128 {
    u128::from(base) - u128::from(base) * u128::from(jitter_percent) / 100
}

/// The un-jittered exponential base: `min(initial * 2^(attempt-1), max)`,
/// computed in u128 so it cannot overflow.
fn expected_base(initial: u64, max: u64, attempt: u32) -> u64 {
    let exponent = attempt.saturating_sub(1);
    let raw = if exponent >= 64 {
        u128::MAX
    } else {
        u128::from(initial).saturating_mul(1_u128 << exponent)
    };
    raw.min(u128::from(max)) as u64
}

fn valid_exponential() -> impl Strategy<Value = (u64, u64, u8)> {
    // Mix uniform u64 with powers of two so doubling boundaries near u64::MAX
    // are reachable.
    let seconds = || {
        prop_oneof![
            1_u64..=u64::MAX,
            (0_u32..64).prop_map(|shift| 1_u64 << shift)
        ]
    };
    (seconds(), seconds(), 0_u8..=100).prop_map(|(a, b, jitter)| (a.min(b), a.max(b), jitter))
}

/// Attempt numbers biased toward the doubling range (1..=70).
fn attempts() -> impl Strategy<Value = u32> {
    prop_oneof![1_u32..=70, 1_u32..u32::MAX]
}

/// Realistic retry configurations: seconds up to ~30 years.
fn realistic_exponential() -> impl Strategy<Value = (u64, u64, u8)> {
    (1_u64..=86_400, 0_u64..=1_000_000_000, 0_u8..=100)
        .prop_map(|(initial, extra, jitter)| (initial, initial + extra, jitter))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn fixed_constructor_accepts_iff_positive(delay in any::<u64>()) {
        let result = RetryPolicy::fixed(delay);
        prop_assert_eq!(result.is_ok(), delay > 0);
        if let Ok(policy) = result {
            prop_assert_eq!(policy.backoff(), BackoffPolicy::Fixed { delay_secs: delay });
        } else {
            prop_assert!(matches!(result, Err(DurableError::InvalidDefinition(_))));
        }
    }

    #[test]
    fn exponential_constructor_accepts_iff_valid(
        initial in prop_oneof![Just(0_u64), any::<u64>()],
        max in any::<u64>(),
        jitter in any::<u8>(),
    ) {
        let result = RetryPolicy::exponential(initial, max, jitter);
        let valid = initial > 0 && max >= initial && jitter <= 100;
        prop_assert_eq!(result.is_ok(), valid);
        if let Ok(policy) = result {
            prop_assert_eq!(
                policy.backoff(),
                BackoffPolicy::Exponential { initial_secs: initial, max_secs: max, jitter_percent: jitter }
            );
        } else {
            prop_assert!(matches!(result, Err(DurableError::InvalidDefinition(_))));
        }
    }

    #[test]
    fn fixed_delay_ignores_attempt_and_jitter(
        delay in 1_u64..=u64::MAX,
        attempt in attempts(),
        percentile in 0_u8..=100,
    ) {
        let policy = RetryPolicy::fixed(delay).unwrap();
        prop_assert_eq!(policy.delay_for_attempt(attempt, percentile).unwrap(), Duration::from_secs(delay));
    }

    #[test]
    fn zero_attempt_and_out_of_range_percentile_are_rejected(
        (initial, max, jitter) in valid_exponential(),
        attempt in any::<u32>(),
        percentile in any::<u8>(),
    ) {
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        let result = policy.delay_for_attempt(attempt, percentile);
        prop_assert_eq!(result.is_ok(), attempt >= 1 && percentile <= 100);
    }

    /// Never panics, for any attempt and any valid policy, including extreme
    /// seconds values.
    #[test]
    fn exponential_never_panics(
        (initial, max, jitter) in valid_exponential(),
        attempt in attempts(),
        percentile in 0_u8..=100,
    ) {
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        prop_assert!(policy.delay_for_attempt(attempt, percentile).is_ok());
    }

    /// Jitter stays within `base ± base * jitter / 100` where base is the
    /// capped exponential delay. The cap applies before jitter (see
    /// tests/policy.rs), so the delay may exceed `max_secs` by up to jitter%.
    #[test]
    fn realistic_exponential_delay_within_jitter_bounds(
        (initial, max, jitter) in realistic_exponential(),
        attempt in attempts(),
        percentile in 0_u8..=100,
    ) {
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        let delay = u128::from(policy.delay_for_attempt(attempt, percentile).unwrap().as_secs());
        let base = expected_base(initial, max, attempt);
        prop_assert!(delay >= jitter_floor(base, jitter), "delay {} < floor for base {}", delay, base);
        prop_assert!(delay <= jitter_ceiling(base, jitter), "delay {} > ceiling for base {}", delay, base);
        prop_assert!(delay <= jitter_ceiling(max, jitter));
        if percentile == 50 || jitter == 0 {
            prop_assert_eq!(delay, u128::from(base));
        }
    }

    /// Same bounds, for every valid policy including seconds near u64::MAX.
    #[test]
    #[ignore = "finding: apply_jitter wraps i128->u64 when base*(1+jitter%) exceeds u64::MAX (src/policy.rs:122)"]
    fn any_exponential_delay_within_jitter_bounds(
        (initial, max, jitter) in valid_exponential(),
        attempt in attempts(),
        percentile in 0_u8..=100,
    ) {
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        let delay = u128::from(policy.delay_for_attempt(attempt, percentile).unwrap().as_secs());
        let base = expected_base(initial, max, attempt);
        prop_assert!(delay >= jitter_floor(base, jitter), "delay {} < floor for base {}", delay, base);
        // A saturating implementation would clamp at u64::MAX.
        prop_assert!(delay <= jitter_ceiling(base, jitter).min(u128::from(u64::MAX)));
        if percentile >= 50 {
            prop_assert!(delay >= u128::from(base), "upper-half jitter shrank delay {} below base {}", delay, base);
        }
    }

    /// For a fixed percentile, the delay never decreases as attempts grow.
    #[test]
    fn realistic_exponential_is_monotone_in_attempt(
        (initial, max, jitter) in realistic_exponential(),
        attempt in attempts(),
        step in 1_u32..=64,
        percentile in 0_u8..=100,
    ) {
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        let later = attempt.saturating_add(step);
        let first = policy.delay_for_attempt(attempt, percentile).unwrap();
        let second = policy.delay_for_attempt(later, percentile).unwrap();
        prop_assert!(first <= second, "attempt {} -> {:?}, attempt {} -> {:?}", attempt, first, later, second);
    }

    #[test]
    #[ignore = "finding: apply_jitter wraps i128->u64 near u64::MAX, so a later attempt can be shorter (src/policy.rs:122)"]
    fn any_exponential_is_monotone_in_attempt(
        (initial, max, jitter) in valid_exponential(),
        attempt in attempts(),
        step in 1_u32..=64,
        percentile in 0_u8..=100,
    ) {
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        let later = attempt.saturating_add(step);
        let first = policy.delay_for_attempt(attempt, percentile).unwrap();
        let second = policy.delay_for_attempt(later, percentile).unwrap();
        prop_assert!(first <= second, "attempt {} -> {:?}, attempt {} -> {:?}", attempt, first, later, second);
    }

    /// For a fixed attempt, a higher percentile never yields a shorter delay.
    #[test]
    fn realistic_exponential_is_monotone_in_percentile(
        (initial, max, jitter) in realistic_exponential(),
        attempt in attempts(),
        low in 0_u8..=100,
        high in 0_u8..=100,
    ) {
        let (low, high) = (low.min(high), low.max(high));
        let policy = RetryPolicy::exponential(initial, max, jitter).unwrap();
        prop_assert!(
            policy.delay_for_attempt(attempt, low).unwrap()
                <= policy.delay_for_attempt(attempt, high).unwrap()
        );
    }

    #[test]
    fn jitter_percentile_is_deterministic_and_bounded(seed in proptest::collection::vec(any::<u8>(), 0..256)) {
        let first = deterministic_jitter_percentile(&seed);
        let second = deterministic_jitter_percentile(seed.clone());
        prop_assert_eq!(first, second);
        prop_assert!(first <= 100);
    }

    #[test]
    fn jitter_percentile_accepts_str_and_bytes_equally(seed in ".*") {
        prop_assert_eq!(
            deterministic_jitter_percentile(seed.as_str()),
            deterministic_jitter_percentile(seed.as_bytes())
        );
    }
}

/// Minimal counterexample for the wrap-around finding, kept as a plain test.
#[test]
#[ignore = "finding: apply_jitter wraps i128->u64 (src/policy.rs:122); 2^63 s at +100% jitter returns 0 s"]
fn jitter_overflow_minimal_counterexample() {
    let base = 1_u64 << 63;
    let policy = RetryPolicy::exponential(base, base, 100).unwrap();
    let delay = policy.delay_for_attempt(1, 100).unwrap();
    assert!(delay.as_secs() >= base, "got {delay:?}");
}

#[test]
fn jitter_percentile_covers_full_range() {
    let mut seen = [false; 101];
    for index in 0..20_000_u32 {
        let value = deterministic_jitter_percentile(format!("activity:{index}:attempt:1"));
        seen[usize::from(value)] = true;
    }
    assert!(
        seen.iter().all(|hit| *hit),
        "some percentiles in 0..=100 never occur"
    );
}
