//! Time sources for time-based stages.
//!
//! Windowing and age-triggered batching read the current instant through
//! the [`Clock`] trait instead of calling [`std::time::Instant::now`]
//! directly, so tests and simulations can drive time explicitly with
//! [`ManualClock`].
//!
//! Clocks are assumed to be monotonic. A clock that moves backwards never
//! causes a panic or a hang: elapsed-time arithmetic saturates at zero, so
//! an item stamped "in the past" joins the window that is currently open.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Source of the current instant for windowing and age-triggered batching.
///
/// # Example
///
/// ```
/// use pipe_io::{Clock, SystemClock};
///
/// let clock = SystemClock;
/// let a = clock.now();
/// assert!(clock.now() >= a);
/// ```
pub trait Clock: Send {
    /// Return the current instant.
    fn now(&self) -> Instant;
}

impl<C: Clock + ?Sized> Clock for Box<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

/// Default [`Clock`], backed by [`std::time::Instant::now`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A [`Clock`] that only moves when told to. Clones share the same time,
/// so keep one clone in the test and hand another to the pipeline.
///
/// # Example
///
/// ```
/// use std::time::{Duration, Instant};
/// use pipe_io::{Clock, ManualClock};
///
/// let start = Instant::now();
/// let clock = ManualClock::new(start);
/// let handle = clock.clone();
/// handle.advance(Duration::from_secs(5));
/// assert_eq!(clock.now(), start + Duration::from_secs(5));
/// ```
#[derive(Debug, Clone)]
pub struct ManualClock {
    now: Arc<Mutex<Instant>>,
}

impl ManualClock {
    /// Create a clock frozen at `start`.
    #[must_use]
    pub fn new(start: Instant) -> Self {
        Self {
            now: Arc::new(Mutex::new(start)),
        }
    }

    /// Move the clock forward by `by`. If the resulting instant is not
    /// representable the clock is left unchanged instead of panicking.
    pub fn advance(&self, by: Duration) {
        let mut guard = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(next) = guard.checked_add(by) {
            *guard = next;
        }
    }

    /// Set the clock to `to`. Moving it backwards is allowed (useful for
    /// testing clock skew handling).
    pub fn set(&self, to: Instant) {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner) = to;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manual_clock_advance_and_set() {
        let t0 = Instant::now();
        let c = ManualClock::new(t0);
        let shared = c.clone();
        shared.advance(Duration::from_millis(10));
        assert_eq!(c.now(), t0 + Duration::from_millis(10));
        c.set(t0);
        assert_eq!(shared.now(), t0);
    }

    #[test]
    fn test_manual_clock_advance_saturates() {
        let t0 = Instant::now();
        let c = ManualClock::new(t0);
        c.advance(Duration::MAX);
        assert_eq!(
            c.now(),
            t0,
            "unrepresentable advance leaves the clock unchanged"
        );
    }

    #[test]
    fn test_boxed_clock_delegates() {
        let t0 = Instant::now();
        let boxed: Box<dyn Clock> = Box::new(ManualClock::new(t0));
        assert_eq!(boxed.now(), t0);
        assert!(SystemClock.now() >= t0);
    }
}
