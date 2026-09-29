use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::DurableError;

/// Deterministic jitter percentile in `0..=100` derived from durable identifiers.
///
/// Using a stable hash (instead of a fixed mid-point) spreads retries after a
/// shared outage while remaining reproducible for the same seed.
pub fn deterministic_jitter_percentile(seed: impl AsRef<[u8]>) -> u8 {
    let digest = Sha256::digest(seed.as_ref());
    let value = u16::from_le_bytes([digest[0], digest[1]]) % 101;
    value as u8
}

/// The largest delay, in seconds, that [`RetryPolicy::fixed`] and
/// [`RetryPolicy::exponential`] accept. A delay at this bound with +100%
/// jitter still fits the database's millisecond range (`i64`).
pub const MAX_RETRY_DELAY_SECS: u64 = (i64::MAX / 2_000) as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum BackoffPolicy {
    Fixed {
        delay_secs: u64,
    },
    Exponential {
        initial_secs: u64,
        max_secs: u64,
        jitter_percent: u8,
    },
}

/// A validated retry policy. Every value, including one deserialized from a
/// stored `retry_policy_json`, is within the bounds [`RetryPolicy::fixed`]
/// and [`RetryPolicy::exponential`] check (`#[serde(try_from)]`), so a
/// corrupt stored policy is a decode error, never an unchecked delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "StoredRetryPolicy")]
pub struct RetryPolicy {
    backoff: BackoffPolicy,
}

/// The serialized shape of [`RetryPolicy`], before its bounds are checked.
#[derive(serde::Deserialize)]
struct StoredRetryPolicy {
    backoff: BackoffPolicy,
}

impl TryFrom<StoredRetryPolicy> for RetryPolicy {
    type Error = DurableError;

    fn try_from(stored: StoredRetryPolicy) -> Result<Self, Self::Error> {
        Self::validated(stored.backoff)
    }
}

/// A bound a [`BackoffPolicy`] breaks. The one list of retry bounds, shared
/// by the constructors, deserialization and the const-evaluated
/// [`RetryPolicy::from_checked`] the derive macros emit.
#[derive(Debug, Clone, Copy)]
enum BoundViolation {
    FixedZero,
    FixedTooLong,
    InitialZero,
    MaxBelowInitial,
    MaxTooLong,
    JitterOver100,
}

impl BoundViolation {
    const fn check(backoff: BackoffPolicy) -> Result<BackoffPolicy, Self> {
        match backoff {
            BackoffPolicy::Fixed { delay_secs } => {
                if delay_secs == 0 {
                    Err(Self::FixedZero)
                } else if delay_secs > MAX_RETRY_DELAY_SECS {
                    Err(Self::FixedTooLong)
                } else {
                    Ok(backoff)
                }
            }
            BackoffPolicy::Exponential {
                initial_secs,
                max_secs,
                jitter_percent,
            } => {
                if initial_secs == 0 {
                    Err(Self::InitialZero)
                } else if max_secs < initial_secs {
                    Err(Self::MaxBelowInitial)
                } else if max_secs > MAX_RETRY_DELAY_SECS {
                    Err(Self::MaxTooLong)
                } else if jitter_percent > 100 {
                    Err(Self::JitterOver100)
                } else {
                    Ok(backoff)
                }
            }
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::FixedZero => "fixed retry delay must be greater than zero",
            Self::FixedTooLong => "fixed retry delay cannot exceed MAX_RETRY_DELAY_SECS",
            Self::InitialZero => "exponential initial delay must be greater than zero",
            Self::MaxBelowInitial => "exponential max delay must be at least the initial delay",
            Self::MaxTooLong => "exponential max delay cannot exceed MAX_RETRY_DELAY_SECS",
            Self::JitterOver100 => "retry jitter_percent cannot exceed 100",
        }
    }

    fn into_error(self) -> DurableError {
        DurableError::InvalidDefinition(match self {
            Self::FixedTooLong => {
                format!("fixed retry delay cannot exceed {MAX_RETRY_DELAY_SECS} seconds")
            }
            Self::MaxTooLong => {
                format!("exponential max delay cannot exceed {MAX_RETRY_DELAY_SECS} seconds")
            }
            Self::FixedZero | Self::InitialZero | Self::MaxBelowInitial | Self::JitterOver100 => {
                self.as_str().to_string()
            }
        })
    }
}

impl RetryPolicy {
    /// For the derive macros, which emit it in a `const` block: a policy out
    /// of bounds is a compile error (E0080) there, and a panic in a
    /// non-const call.
    #[doc(hidden)]
    #[track_caller]
    pub const fn from_checked(backoff: BackoffPolicy) -> Self {
        match BoundViolation::check(backoff) {
            Ok(backoff) => Self { backoff },
            Err(violation) => panic!("{}", violation.as_str()),
        }
    }

    fn validated(backoff: BackoffPolicy) -> Result<Self, DurableError> {
        BoundViolation::check(backoff)
            .map(|backoff| Self { backoff })
            .map_err(BoundViolation::into_error)
    }

    pub fn fixed(delay_secs: u64) -> Result<Self, DurableError> {
        Self::validated(BackoffPolicy::Fixed { delay_secs })
    }

    pub fn exponential(
        initial_secs: u64,
        max_secs: u64,
        jitter_percent: u8,
    ) -> Result<Self, DurableError> {
        Self::validated(BackoffPolicy::Exponential {
            initial_secs,
            max_secs,
            jitter_percent,
        })
    }

    pub fn backoff(self) -> BackoffPolicy {
        self.backoff
    }

    pub fn delay_for_attempt(
        self,
        attempt: u32,
        jitter_percentile: u8,
    ) -> Result<Duration, DurableError> {
        if attempt == 0 {
            return Err(DurableError::InvalidDefinition(
                "attempt numbers are one-based".to_string(),
            ));
        }
        if jitter_percentile > 100 {
            return Err(DurableError::InvalidDefinition(
                "jitter percentile cannot exceed 100".to_string(),
            ));
        }

        let seconds = match self.backoff {
            BackoffPolicy::Fixed { delay_secs } => delay_secs,
            BackoffPolicy::Exponential {
                initial_secs,
                max_secs,
                jitter_percent,
            } => {
                let exponent = attempt.saturating_sub(1).min(63);
                let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
                let base = initial_secs.saturating_mul(multiplier).min(max_secs);
                apply_jitter(base, jitter_percent, jitter_percentile)
            }
        };

        Ok(Duration::from_secs(seconds))
    }
}

fn apply_jitter(base: u64, jitter_percent: u8, percentile: u8) -> u64 {
    let spread = (u128::from(base) * u128::from(jitter_percent) / 100) as i128;
    let centered_percentile = i128::from(percentile) * 2 - 100;
    let delta = spread * centered_percentile / 100;
    (i128::from(base) + delta).clamp(0, i128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_jitter_percentile_is_bounded_and_stable() {
        let first = deterministic_jitter_percentile(b"activity:1:attempt:2");
        let second = deterministic_jitter_percentile(b"activity:1:attempt:2");
        assert_eq!(first, second);
        assert!(first <= 100);
        let other = deterministic_jitter_percentile(b"activity:2:attempt:2");
        // Different seeds almost always differ; if they collide the bound still holds.
        assert!(other <= 100);
    }

    #[test]
    fn stored_policy_round_trips_and_out_of_bounds_is_a_decode_error() {
        let policy = RetryPolicy::exponential(1, 60, 20).expect("valid policy");
        let json = serde_json::to_string(&policy).expect("serialize");
        assert_eq!(
            serde_json::from_str::<RetryPolicy>(&json).expect("decode"),
            policy
        );
        for stored in [
            r#"{"backoff":{"Fixed":{"delay_secs":0}}}"#,
            r#"{"backoff":{"Fixed":{"delay_secs":18446744073709551615}}}"#,
            r#"{"backoff":{"Exponential":{"initial_secs":0,"max_secs":1,"jitter_percent":0}}}"#,
            r#"{"backoff":{"Exponential":{"initial_secs":5,"max_secs":1,"jitter_percent":0}}}"#,
            r#"{"backoff":{"Exponential":{"initial_secs":1,"max_secs":2,"jitter_percent":101}}}"#,
        ] {
            assert!(
                serde_json::from_str::<RetryPolicy>(stored).is_err(),
                "{stored} decoded"
            );
        }
    }

    /// Minimal counterexample for the former wrap-around finding: 2^63 s at
    /// +100% jitter returned 0 s. The constructors and deserialization reject
    /// this policy, so only code inside this module can build it; if the
    /// bounds ever let it through, `delay_for_attempt` saturates instead.
    #[test]
    fn jitter_overflow_saturates_outside_the_bounds() {
        let base = 1_u64 << 63;
        let policy = RetryPolicy {
            backoff: BackoffPolicy::Exponential {
                initial_secs: base,
                max_secs: base,
                jitter_percent: 100,
            },
        };
        let delay = policy.delay_for_attempt(1, 100).unwrap();
        assert_eq!(delay.as_secs(), u64::MAX, "got {delay:?}");
    }
}

/// Kani proofs (`cargo kani`; CLAUDE.md, "Bounded model checking"). No
/// harness here needs an unwind bound: the delay code has no loops. The
/// reference computations avoid division and multiplication by symbolic
/// values where they can: a SAT solver proves such arithmetic equal to the
/// code's only slowly.
#[cfg(kani)]
mod verification {
    use std::mem::ManuallyDrop;

    use super::*;

    /// A symbolic policy that the checked constructors accept.
    fn any_valid_policy() -> RetryPolicy {
        let backoff = if kani::any() {
            BackoffPolicy::Fixed {
                delay_secs: kani::any(),
            }
        } else {
            BackoffPolicy::Exponential {
                initial_secs: kani::any(),
                max_secs: kani::any(),
                jitter_percent: kani::any(),
            }
        };
        // The bound check the constructors use, without building the
        // `DurableError` they return.
        let checked = BoundViolation::check(backoff);
        kani::assume(checked.is_ok());
        RetryPolicy {
            backoff: checked.unwrap_or(backoff),
        }
    }

    /// `policy.delay_for_attempt`, which must succeed. The result is not
    /// dropped: `DurableError`'s drop glue calls through `dyn` pointers,
    /// which the verifier cannot bound.
    fn delay(policy: RetryPolicy, attempt: u32, percentile: u8) -> Duration {
        let result = ManuallyDrop::new(policy.delay_for_attempt(attempt, percentile));
        match &*result {
            Ok(delay) => *delay,
            Err(_) => panic!("a valid attempt and percentile were rejected"),
        }
    }

    /// `apply_jitter` never panics or overflows for any input, and with
    /// both percents in `0..=100` the result is at most
    /// `floor(base * jitter_percent / 100)` away from `base` (checked as
    /// `100 * |delay - base| <= base * jitter_percent`, the same bound on
    /// integers); the midpoint percentile adds nothing.
    #[kani::proof]
    fn apply_jitter_is_total_and_bounded() {
        let base: u64 = kani::any();
        let jitter_percent: u8 = kani::any();
        let percentile: u8 = kani::any();
        let delay = apply_jitter(base, jitter_percent, percentile);
        if jitter_percent <= 100 && percentile <= 100 {
            let distance = u128::from(delay.abs_diff(base));
            assert!(100 * distance <= u128::from(base) * u128::from(jitter_percent));
            if percentile == 50 {
                assert!(delay == base);
            }
        }
    }

    /// For every valid policy, attempt and percentile the call never
    /// panics, errors exactly on attempt 0 or a percentile above 100, and
    /// fits the database's millisecond range (`MAX_RETRY_DELAY_SECS`); a
    /// fixed policy's delay is its `delay_secs`.
    #[kani::proof]
    fn delay_for_attempt_is_total_and_fits_millis() {
        let policy = any_valid_policy();
        let attempt: u32 = kani::any();
        let percentile: u8 = kani::any();
        let result = ManuallyDrop::new(policy.delay_for_attempt(attempt, percentile));
        assert!(result.is_ok() == (attempt >= 1 && percentile <= 100));
        let Ok(delay) = result.as_ref().copied() else {
            return;
        };
        // The delay is whole seconds, so this is `as_millis() <= i64::MAX`.
        assert!(delay.subsec_nanos() == 0);
        assert!(delay.as_secs() <= i64::MAX as u64 / 1_000);
        if let BackoffPolicy::Fixed { delay_secs } = policy.backoff {
            assert!(delay.as_secs() == delay_secs);
        }
    }

    /// At the midpoint percentile (no jitter) an exponential policy's delay
    /// is `min(initial_secs * 2^(attempt - 1), max_secs)`, computed here
    /// with an exact `u128` shift instead of the saturating multiply.
    #[kani::proof]
    fn exponential_delay_without_jitter_is_capped_doubling() {
        let initial_secs: u64 = kani::any();
        let max_secs: u64 = kani::any();
        let jitter_percent: u8 = kani::any();
        let backoff = BackoffPolicy::Exponential {
            initial_secs,
            max_secs,
            jitter_percent,
        };
        kani::assume(BoundViolation::check(backoff).is_ok());
        let policy = RetryPolicy { backoff };
        let attempt: u32 = kani::any();
        kani::assume(attempt >= 1);

        let exponent = attempt - 1;
        let expected = if exponent >= 63 {
            // initial_secs >= 1, so initial * 2^63 is above any valid max.
            max_secs
        } else {
            (u128::from(initial_secs) << exponent).min(u128::from(max_secs)) as u64
        };
        assert!(delay(policy, attempt, 50).as_secs() == expected);
    }

    /// The un-jittered delay (the midpoint percentile) never decreases as
    /// the attempt number grows.
    #[kani::proof]
    fn delay_without_jitter_is_non_decreasing_in_attempt() {
        let policy = any_valid_policy();
        let earlier: u32 = kani::any();
        let later: u32 = kani::any();
        kani::assume(1 <= earlier && earlier <= later);
        assert!(delay(policy, earlier, 50) <= delay(policy, later, 50));
    }
}
