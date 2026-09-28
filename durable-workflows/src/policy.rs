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
