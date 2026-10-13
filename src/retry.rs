//! Retry configuration for [`crate::PipelineBuilder::try_map_retry`].
//!
//! A [`RetryPolicy`] bounds the number of attempts and chooses the delay
//! between them ([`Backoff`]), optionally with full jitter so many
//! pipelines retrying the same dependency do not synchronise. Every retry
//! is counted in [`crate::RunStats::retries`] and reported as
//! [`crate::Event::Retry`]. When the last attempt fails, the stage's
//! [`crate::ErrorPolicy`] decides what happens.

use core::time::Duration;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use crate::error::{ConfigError, ConfigErrorKind};
use crate::stage_id::StageId;

/// Delay schedule between attempts.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Backoff {
    /// Retry immediately.
    #[default]
    None,
    /// Wait the same duration before every retry.
    Fixed(Duration),
    /// Wait `initial`, then double the delay before each further retry,
    /// never exceeding `max`.
    Exponential {
        /// Delay before the first retry.
        initial: Duration,
        /// Upper bound for any delay.
        max: Duration,
    },
}

/// How often, and how patiently, a fallible stage retries.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use pipe_io::{Backoff, RetryPolicy};
///
/// let policy = RetryPolicy::new(4).backoff(Backoff::Exponential {
///     initial: Duration::from_millis(10),
///     max: Duration::from_millis(25),
/// });
/// assert_eq!(policy.max_attempts(), 4);
/// assert_eq!(policy.base_delay(1), Duration::from_millis(10));
/// assert_eq!(policy.base_delay(2), Duration::from_millis(20));
/// assert_eq!(policy.base_delay(3), Duration::from_millis(25));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RetryPolicy {
    max_attempts: u32,
    backoff: Backoff,
    jitter: bool,
}

impl RetryPolicy {
    /// Allow up to `max_attempts` calls per item (the first call plus
    /// `max_attempts - 1` retries), without delay or jitter.
    /// `max_attempts` must be at least 1.
    #[must_use]
    pub const fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            backoff: Backoff::None,
            jitter: false,
        }
    }

    /// Set the delay schedule.
    #[must_use]
    pub const fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Enable full jitter: each delay is drawn uniformly from
    /// `[0, base_delay]`.
    #[must_use]
    pub const fn jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    /// Configured attempt limit.
    #[must_use]
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Delay before the retry that follows failed attempt `attempt`
    /// (1-based), before jitter.
    #[must_use]
    pub fn base_delay(&self, attempt: u32) -> Duration {
        match self.backoff {
            Backoff::None => Duration::ZERO,
            Backoff::Fixed(d) => d,
            Backoff::Exponential { initial, max } => {
                let doublings = attempt.saturating_sub(1).min(63);
                let factor = 1_u64 << doublings;
                let nanos = initial.as_nanos().saturating_mul(u128::from(factor));
                let delay = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
                delay.min(max)
            }
        }
    }

    /// Check the policy.
    ///
    /// # Errors
    ///
    /// [`ConfigErrorKind::ZeroLimit`] when `max_attempts` is zero. The
    /// error is attributed to the stage id `"try_map_retry"`.
    pub const fn validate(&self) -> Result<(), ConfigError> {
        match self.check() {
            Ok(()) => Ok(()),
            Err(kind) => Err(ConfigError::new(StageId::new("try_map_retry"), kind)),
        }
    }

    pub(crate) const fn check(&self) -> Result<(), ConfigErrorKind> {
        if self.max_attempts == 0 {
            Err(ConfigErrorKind::ZeroLimit {
                field: "max_attempts",
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn delay_for(&self, attempt: u32, jitter: &mut Jitter) -> Duration {
        let base = self.base_delay(attempt);
        if self.jitter && !base.is_zero() {
            jitter.scale(base)
        } else {
            base
        }
    }
}

/// Small xorshift generator for jitter; seeded from the standard library's
/// per-process random hasher keys, so no dependency is needed.
#[derive(Debug, Clone)]
pub(crate) struct Jitter(u64);

impl Jitter {
    pub(crate) fn seeded() -> Self {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(0x9e37_79b9_7f4a_7c15);
        Self(hasher.finish() | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A duration uniformly drawn from `[0, max]`.
    fn scale(&mut self, max: Duration) -> Duration {
        let span = max.as_nanos().saturating_add(1);
        let pick = u128::from(self.next()) % span;
        Duration::from_nanos(u64::try_from(pick).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_schedules() {
        let none = RetryPolicy::new(3);
        assert_eq!(none.base_delay(1), Duration::ZERO);
        let fixed = RetryPolicy::new(3).backoff(Backoff::Fixed(Duration::from_millis(7)));
        assert_eq!(fixed.base_delay(5), Duration::from_millis(7));
        let exp = RetryPolicy::new(10).backoff(Backoff::Exponential {
            initial: Duration::from_millis(1),
            max: Duration::from_secs(1),
        });
        assert_eq!(exp.base_delay(0), Duration::from_millis(1));
        assert_eq!(exp.base_delay(1), Duration::from_millis(1));
        assert_eq!(exp.base_delay(4), Duration::from_millis(8));
        assert_eq!(
            exp.base_delay(11),
            Duration::from_millis(1024).min(Duration::from_secs(1))
        );
        assert_eq!(exp.base_delay(u32::MAX), Duration::from_secs(1));
        let huge = RetryPolicy::new(2).backoff(Backoff::Exponential {
            initial: Duration::MAX,
            max: Duration::MAX,
        });
        assert_eq!(huge.base_delay(64), Duration::from_nanos(u64::MAX));
    }

    #[test]
    fn test_validation() {
        assert!(RetryPolicy::new(1).validate().is_ok());
        let err = RetryPolicy::new(0).validate().unwrap_err();
        assert_eq!(
            err.kind(),
            ConfigErrorKind::ZeroLimit {
                field: "max_attempts"
            }
        );
        assert_eq!(RetryPolicy::new(5).max_attempts(), 5);
    }

    #[test]
    fn test_jitter_generator_is_well_mixed() {
        let mut jitter = Jitter(0x1234_5678_9abc_def1);
        let mut ones = 0_u32;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let v = jitter.next();
            ones += v.count_ones();
            let _new = seen.insert(v);
        }
        assert_eq!(seen.len(), 1000, "no short cycle");
        assert!(
            (30_000..34_000).contains(&ones),
            "bits are balanced: {ones}"
        );
        assert_ne!(Jitter::seeded().0 & 1, 0, "the seed is never zero");
    }

    #[test]
    fn test_jitter_stays_within_bounds() {
        let policy = RetryPolicy::new(3)
            .backoff(Backoff::Fixed(Duration::from_millis(10)))
            .jitter(true);
        let mut jitter = Jitter::seeded();
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..200 {
            let d = policy.delay_for(1, &mut jitter);
            assert!(d <= Duration::from_millis(10));
            let _new = distinct.insert(d);
        }
        assert!(distinct.len() > 1, "jitter varies the delay");
        let no_jitter = RetryPolicy::new(3).backoff(Backoff::Fixed(Duration::from_millis(10)));
        assert_eq!(
            no_jitter.delay_for(1, &mut jitter),
            Duration::from_millis(10)
        );
        let zero = RetryPolicy::new(3).jitter(true);
        assert_eq!(zero.delay_for(1, &mut jitter), Duration::ZERO);
    }
}
