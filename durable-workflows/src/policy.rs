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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicy {
    backoff: BackoffPolicy,
}

impl RetryPolicy {
    #[doc(hidden)]
    pub const fn from_validated(backoff: BackoffPolicy) -> Self {
        Self { backoff }
    }

    pub fn fixed(delay_secs: u64) -> Result<Self, DurableError> {
        if delay_secs == 0 {
            return Err(DurableError::InvalidDefinition(
                "fixed retry delay must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            backoff: BackoffPolicy::Fixed { delay_secs },
        })
    }

    pub fn exponential(
        initial_secs: u64,
        max_secs: u64,
        jitter_percent: u8,
    ) -> Result<Self, DurableError> {
        if initial_secs == 0 {
            return Err(DurableError::InvalidDefinition(
                "exponential initial delay must be greater than zero".to_string(),
            ));
        }
        if max_secs < initial_secs {
            return Err(DurableError::InvalidDefinition(
                "exponential max delay must be at least the initial delay".to_string(),
            ));
        }
        if jitter_percent > 100 {
            return Err(DurableError::InvalidDefinition(
                "retry jitter_percent cannot exceed 100".to_string(),
            ));
        }
        Ok(Self {
            backoff: BackoffPolicy::Exponential {
                initial_secs,
                max_secs,
                jitter_percent,
            },
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
    (i128::from(base) + delta).max(0) as u64
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
}
