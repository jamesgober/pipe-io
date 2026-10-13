//! Observability hooks.
//!
//! An [`Observer`] installed with [`crate::PipelineBuilder::observe`]
//! receives an [`Event`] for every lifecycle transition and every
//! non-hot-path occurrence: stage errors (including the ones a `Continue`
//! policy swallows), dead-letter routing, retries, idle ticks, teardown
//! failures that could not be returned to the caller, and the final
//! statistics. Per-item events are deliberately absent so an observer
//! never sits on the hot path; per-item counts are in [`crate::RunStats`].
//!
//! Observers are called synchronously on the thread running the stage
//! that produced the event. Under the threaded driver several threads may
//! call the same observer concurrently, hence the `Send + Sync` bound.
//! Keep the callback cheap: forward to a metrics registry, a log, or a
//! channel.
//!
//! # Example
//!
//! ```
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use std::sync::Arc;
//! use pipe_io::{ErrorPolicy, Event, Pipeline, sink::NullSink};
//!
//! let errors = Arc::new(AtomicU64::new(0));
//! let seen = Arc::clone(&errors);
//!
//! Pipeline::from_iter(["1", "x", "3"])
//!     .observe(move |event: &Event<'_>| {
//!         if let Event::StageError { .. } = event {
//!             seen.fetch_add(1, Ordering::Relaxed);
//!         }
//!     })
//!     .on_error(ErrorPolicy::Continue)
//!     .try_map(|s: &str| s.parse::<u32>())
//!     .sink(NullSink::new())
//!     .run()?;
//!
//! assert_eq!(errors.load(Ordering::Relaxed), 1);
//! # Ok::<(), pipe_io::Error>(())
//! ```

use core::fmt;
use core::time::Duration;

use crate::driver::RunStats;
use crate::error::{Error, ErrorPolicy};
use crate::stage_id::StageId;

/// Something that happened inside a running pipeline. See the module docs.
#[non_exhaustive]
#[derive(Debug)]
pub enum Event<'a> {
    /// The run started: the pipeline was assembled and the first pull is
    /// about to happen.
    Started,
    /// A stage returned an error. Emitted under every [`ErrorPolicy`],
    /// before the policy is applied.
    StageError {
        /// The failing stage.
        stage: StageId,
        /// The policy that will handle the error.
        policy: ErrorPolicy,
        /// The error the stage returned.
        error: &'a (dyn core::error::Error + Send + Sync + 'static),
    },
    /// A failed attempt is about to be retried after `delay`.
    Retry {
        /// The retrying stage.
        stage: StageId,
        /// The attempt that just failed (1 for the first try).
        attempt: u32,
        /// How long the stage waits before the next attempt.
        delay: Duration,
        /// The error of the failed attempt.
        error: &'a (dyn core::error::Error + Send + Sync + 'static),
    },
    /// A failure was written to the dead-letter sink.
    DeadLettered {
        /// The stage whose failure was routed.
        stage: StageId,
    },
    /// The idle tick fired: no item arrived within the tick interval and
    /// time-based stages were given a chance to emit.
    Idle,
    /// A flush or close during teardown failed after an earlier error had
    /// already been chosen as the run's result. The run returns the first
    /// error; later ones are reported here so they are never lost.
    TeardownError {
        /// The secondary error.
        error: &'a Error,
    },
    /// The run ended. `error` is the error the run returns, if any.
    Finished {
        /// Final counters.
        stats: &'a RunStats,
        /// The error returned to the caller, if the run failed.
        error: Option<&'a Error>,
    },
}

/// Receiver of pipeline [`Event`]s.
///
/// Implemented for every `Fn(&Event<'_>) + Send + Sync` closure.
pub trait Observer: Send + Sync {
    /// Handle one event. Called synchronously; keep it cheap.
    fn on_event(&self, event: &Event<'_>);
}

impl<F> Observer for F
where
    F: Fn(&Event<'_>) + Send + Sync,
{
    fn on_event(&self, event: &Event<'_>) {
        self(event);
    }
}

/// Shared pointer used to hold the installed observer. Threaded execution
/// needs to share it between worker threads; targets without pointer-sized
/// atomics (no `alloc::sync::Arc`) only run the single-threaded driver and
/// own it outright.
#[cfg(target_has_atomic = "ptr")]
pub(crate) type ObserverPtr = alloc::sync::Arc<dyn Observer>;
#[cfg(not(target_has_atomic = "ptr"))]
pub(crate) type ObserverPtr = alloc::boxed::Box<dyn Observer>;

/// Optional observer plus the emit helper used throughout the crate.
#[derive(Default)]
pub(crate) struct ObserverSlot(Option<ObserverPtr>);

impl ObserverSlot {
    pub(crate) fn new(observer: Option<ObserverPtr>) -> Self {
        Self(observer)
    }

    #[inline]
    pub(crate) const fn is_active(&self) -> bool {
        self.0.is_some()
    }

    #[inline]
    pub(crate) fn emit(&self, event: &Event<'_>) {
        if let Some(observer) = &self.0 {
            observer.on_event(event);
        }
    }

    #[cfg(feature = "std")]
    pub(crate) fn share(&self) -> Self {
        Self(self.0.clone())
    }
}

impl fmt::Debug for ObserverSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "Some(<observer>)"
        } else {
            "None"
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use alloc::boxed::Box;
    use core::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_observer_slot_emits_to_closure() {
        static HITS: AtomicU32 = AtomicU32::new(0);
        let observer: Box<dyn Observer> = Box::new(|e: &Event<'_>| {
            if matches!(e, Event::Idle) {
                let _prev = HITS.fetch_add(1, Ordering::Relaxed);
            }
        });
        let slot = ObserverSlot::new(Some(ObserverPtr::from(observer)));
        slot.emit(&Event::Idle);
        slot.emit(&Event::Started);
        assert_eq!(HITS.load(Ordering::Relaxed), 1);
        assert_eq!(alloc::format!("{slot:?}"), "Some(<observer>)");
        assert_eq!(alloc::format!("{:?}", ObserverSlot::default()), "None");
        ObserverSlot::default().emit(&Event::Idle);
    }
}
