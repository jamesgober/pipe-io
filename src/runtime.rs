//! Execution machinery shared by the builder and the drivers.
//!
//! This module is `pub` only because its types appear in the bounds of
//! the builder's type-state parameter ([`crate::Assemble`]). It is
//! hidden from the documentation and is not covered by Semantic
//! Versioning; nothing here is meant to be named by users.
//!
//! # How a built pipeline executes
//!
//! Every stage is a boxed closure ([`BoxedStageFn`]) that owns the next
//! stage's closure. The driver pulls an item from the source and calls
//! the first closure with [`StageOp::Process`]; each closure transforms
//! the item and calls the next one, down to the sink. Control signals
//! ([`Signal`]) travel the same path. A per-run [`Ctx`] is passed by
//! `&mut` through every call; it carries counters, the dead-letter sink,
//! and the observer, so the hot path needs no shared state or atomics.
//!
//! Under the threaded driver, a `buffer` edge splits the chain: the part
//! downstream of the edge runs on its own thread and receives
//! [`StageOp`]s through a bounded `sync_channel`.

use alloc::boxed::Box;
use core::any::Any;
use core::fmt;

use crate::driver::RunStats;
use crate::emit::{Emit, EmitError};
use crate::error::{BoxError, Error, ErrorPolicy, StageFailure};
use crate::observe::{Event, ObserverSlot};
use crate::stage::Stage;
use crate::stage_id::StageId;

/// Control signal sent through the stage chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// The idle tick fired: time-based stages may emit, then forward.
    Tick,
    /// End of stream: stages emit buffered output, then forward. The
    /// sink flushes.
    Flush,
    /// Sent after a successful `Flush`: stages forward, the sink closes.
    Close,
    /// The run failed: stages forward without emitting; the sink flushes
    /// and closes on a best-effort basis.
    Abort,
}

/// Operation handed to every stage closure.
#[derive(Debug)]
pub enum StageOp<T> {
    /// One item.
    Process(T),
    /// A control signal.
    Control(Signal),
}

/// Why a stage closure stopped the run.
#[derive(Debug)]
pub enum Halt {
    /// A component failed; this is the error to report.
    Failed(Error),
    /// The downstream half of a `buffer` edge is gone because another
    /// thread failed. That thread reports the real error.
    Disconnected,
}

impl From<Error> for Halt {
    fn from(value: Error) -> Self {
        Self::Failed(value)
    }
}

/// Result of one stage-closure call.
pub type StageResult = core::result::Result<(), Halt>;

/// A type-erased stage closure: consumes `T`, forwards to the next stage.
pub type BoxedStageFn<T> = Box<dyn FnMut(StageOp<T>, &mut Ctx) -> StageResult + Send + 'static>;

/// Per-run counters. Each worker thread keeps its own and the driver sums
/// them, so counting needs no synchronisation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) items_in: u64,
    pub(crate) items_out: u64,
    pub(crate) errors: u64,
    pub(crate) dead_lettered: u64,
    pub(crate) retries: u64,
}

impl Tally {
    #[cfg(feature = "std")]
    pub(crate) const fn merge(&mut self, other: Self) {
        self.items_in = self.items_in.saturating_add(other.items_in);
        self.items_out = self.items_out.saturating_add(other.items_out);
        self.errors = self.errors.saturating_add(other.errors);
        self.dead_lettered = self.dead_lettered.saturating_add(other.dead_lettered);
        self.retries = self.retries.saturating_add(other.retries);
    }

    pub(crate) fn to_stats(self) -> RunStats {
        RunStats {
            items_in: self.items_in,
            items_out: self.items_out,
            errors: self.errors,
            dead_lettered: self.dead_lettered,
            retries: self.retries,
            ..RunStats::default()
        }
    }
}

/// Operations understood by the dead-letter sink closure.
pub(crate) enum DeadLetterOp {
    Write(StageFailure),
    Flush,
    Close,
}

/// Boxed dead-letter sink.
pub(crate) type DeadLetterFn =
    Box<dyn FnMut(DeadLetterOp) -> core::result::Result<(), Error> + Send + 'static>;

/// Where dead-letter failures go during a run.
pub(crate) enum Dlq {
    /// No dead-letter sink (the builder guarantees no stage needs one).
    Absent,
    /// Single-threaded execution owns the sink outright.
    Local(DeadLetterFn),
    /// Threaded execution shares one sink between worker threads.
    #[cfg(feature = "std")]
    Shared(std::sync::Arc<std::sync::Mutex<DeadLetterFn>>),
}

impl Dlq {
    pub(crate) fn new(sink: Option<DeadLetterFn>) -> Self {
        sink.map_or(Self::Absent, Self::Local)
    }

    fn call(&mut self, op: DeadLetterOp) -> core::result::Result<(), Error> {
        match self {
            Self::Absent => Ok(()),
            Self::Local(f) => f(op),
            #[cfg(feature = "std")]
            Self::Shared(shared) => {
                // A panic inside the user's dead-letter sink poisons the
                // lock; the sink itself is still usable, so recover it.
                let mut f = shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (*f)(op)
            }
        }
    }

    fn route(&mut self, failure: StageFailure) -> core::result::Result<(), Error> {
        self.call(DeadLetterOp::Write(failure))
    }

    /// Flush then close. Both are attempted; the first error is returned
    /// and a second one is reported to the observer.
    pub(crate) fn finish(&mut self, observer: &ObserverSlot) -> core::result::Result<(), Error> {
        let flushed = self.call(DeadLetterOp::Flush);
        let closed = self.call(DeadLetterOp::Close);
        first_of(flushed, closed, observer)
    }

    /// Convert a local sink into a shared one and return another handle to
    /// it (for a worker thread).
    #[cfg(feature = "std")]
    pub(crate) fn share(&mut self) -> Self {
        let current = core::mem::replace(self, Self::Absent);
        *self = match current {
            Self::Local(f) => Self::Shared(std::sync::Arc::new(std::sync::Mutex::new(f))),
            other => other,
        };
        match self {
            Self::Absent | Self::Local(_) => Self::Absent,
            Self::Shared(shared) => Self::Shared(std::sync::Arc::clone(shared)),
        }
    }
}

impl fmt::Debug for Dlq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Absent => "Absent",
            Self::Local(_) => "Local",
            #[cfg(feature = "std")]
            Self::Shared(_) => "Shared",
        })
    }
}

/// Keep the first error of two; report the second to the observer.
pub(crate) fn first_of(
    a: core::result::Result<(), Error>,
    b: core::result::Result<(), Error>,
    observer: &ObserverSlot,
) -> core::result::Result<(), Error> {
    match (a, b) {
        (Err(first), Err(second)) => {
            observer.emit(&Event::TeardownError { error: &second });
            Err(first)
        }
        (Err(e), Ok(())) | (Ok(()), Err(e)) => Err(e),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Collects the first error of a run; later ones go to the observer.
#[derive(Debug, Default)]
pub(crate) struct FirstError(Option<Error>);

impl FirstError {
    pub(crate) fn offer(&mut self, error: Error, observer: &ObserverSlot) {
        if self.0.is_none() {
            self.0 = Some(error);
        } else {
            observer.emit(&Event::TeardownError { error: &error });
        }
    }

    pub(crate) fn offer_result(
        &mut self,
        result: core::result::Result<(), Error>,
        observer: &ObserverSlot,
    ) {
        if let Err(e) = result {
            self.offer(e, observer);
        }
    }

    pub(crate) fn offer_halt(&mut self, result: StageResult, observer: &ObserverSlot) {
        if let Err(Halt::Failed(e)) = result {
            self.offer(e, observer);
        }
    }

    pub(crate) fn into_result(self) -> core::result::Result<(), Error> {
        self.0.map_or(Ok(()), Err)
    }
}

/// Per-run (per-thread under the threaded driver) execution context.
#[derive(Debug)]
pub struct Ctx {
    pub(crate) tally: Tally,
    pub(crate) dlq: Dlq,
    pub(crate) observer: ObserverSlot,
}

impl Ctx {
    pub(crate) const fn new(dlq: Dlq, observer: ObserverSlot) -> Self {
        Self {
            tally: Tally {
                items_in: 0,
                items_out: 0,
                errors: 0,
                dead_lettered: 0,
                retries: 0,
            },
            dlq,
            observer,
        }
    }
}

/// Assembly-time context: decides how `buffer` edges are materialised.
pub struct Wiring {
    #[cfg(feature = "std")]
    pub(crate) threaded: Option<crate::threaded::ThreadedWiring>,
}

impl Wiring {
    pub(crate) const fn sync() -> Self {
        Self {
            #[cfg(feature = "std")]
            threaded: None,
        }
    }
}

impl fmt::Debug for Wiring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wiring").finish_non_exhaustive()
    }
}

/// Apply the stage's error policy to a failure.
///
/// Split in two so the common case (`Continue` with no observer) inlines
/// into the stage closure as a counter increment, while the rest stays
/// out of line.
#[inline]
pub(crate) fn handle_failure<E: Into<BoxError>>(
    ctx: &mut Ctx,
    policy: ErrorPolicy,
    stage: StageId,
    error: E,
    input: Option<Box<dyn Any + Send + Sync>>,
    attempts: u32,
) -> StageResult {
    ctx.tally.errors = ctx.tally.errors.wrapping_add(1);
    if policy == ErrorPolicy::Continue && !ctx.observer.is_active() {
        // Nobody will look at the error: drop it without boxing.
        return Ok(());
    }
    report_failure(ctx, policy, stage, error.into(), input, attempts)
}

#[cold]
#[inline(never)]
fn report_failure(
    ctx: &mut Ctx,
    policy: ErrorPolicy,
    stage: StageId,
    error: BoxError,
    input: Option<Box<dyn Any + Send + Sync>>,
    attempts: u32,
) -> StageResult {
    ctx.observer.emit(&Event::StageError {
        stage,
        policy,
        error: &*error,
    });
    match policy {
        ErrorPolicy::FailFast => Err(Halt::Failed(Error::Stage {
            stage,
            source: error,
        })),
        ErrorPolicy::Continue => Ok(()),
        ErrorPolicy::DeadLetter => {
            let mut failure = StageFailure::new(stage, error).with_attempts(attempts);
            failure.input = input;
            ctx.dlq.route(failure)?;
            ctx.tally.dead_lettered = ctx.tally.dead_lettered.saturating_add(1);
            ctx.observer.emit(&Event::DeadLettered { stage });
            Ok(())
        }
    }
}

/// [`Emit`] adapter handed to user [`Stage`]s: forwards straight into the
/// next stage and remembers why the downstream stopped, so the driver can
/// report the real error even if the stage ignores [`EmitError::Closed`].
struct StageEmit<'a, U> {
    next: &'a mut BoxedStageFn<U>,
    ctx: &'a mut Ctx,
    halted: Option<Halt>,
}

impl<U> Emit for StageEmit<'_, U> {
    type Item = U;

    #[inline]
    fn emit(&mut self, item: U) -> core::result::Result<(), EmitError> {
        if self.halted.is_some() {
            return Err(EmitError::Closed);
        }
        match (self.next)(StageOp::Process(item), self.ctx) {
            Ok(()) => Ok(()),
            Err(halt) => {
                self.halted = Some(halt);
                Err(EmitError::Closed)
            }
        }
    }
}

/// Run one stage method with an emit adapter, then apply the error policy.
pub(crate) fn drive<St, F>(
    stage: &mut St,
    next: &mut BoxedStageFn<St::Output>,
    ctx: &mut Ctx,
    policy: ErrorPolicy,
    id: StageId,
    call: F,
) -> StageResult
where
    St: Stage,
    F: FnOnce(&mut St, &mut dyn Emit<Item = St::Output>) -> core::result::Result<(), St::Error>,
{
    let mut emit = StageEmit {
        next,
        ctx,
        halted: None,
    };
    let result = call(stage, &mut emit);
    let StageEmit { ctx, halted, .. } = emit;
    if let Some(halt) = halted {
        return Err(halt);
    }
    match result {
        Ok(()) => Ok(()),
        Err(e) => handle_failure(ctx, policy, id, e, None, 1),
    }
}

/// Run `f`, containing any panic it raises (std only). Used during
/// teardown so a panicking `flush`/`close` cannot turn an unwinding panic
/// into an abort.
#[cfg(feature = "std")]
pub(crate) fn contain<R>(f: impl FnOnce() -> R) -> Option<R> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok()
}

/// Without `std` there is no `catch_unwind`; teardown runs directly.
#[cfg(not(feature = "std"))]
pub(crate) fn contain<R>(f: impl FnOnce() -> R) -> Option<R> {
    Some(f())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[cfg(feature = "std")]
    #[test]
    fn test_tally_merge_saturates() {
        let mut a = Tally {
            items_in: u64::MAX,
            items_out: 1,
            errors: 2,
            dead_lettered: 3,
            retries: 4,
        };
        a.merge(a);
        assert_eq!(a.items_in, u64::MAX);
        assert_eq!(a.items_out, 2);
        let stats = a.to_stats();
        assert_eq!(stats.errors, 4);
        assert_eq!(stats.dead_lettered, 6);
        assert_eq!(stats.retries, 8);
    }

    #[test]
    fn test_first_error_keeps_first() {
        let observer = ObserverSlot::default();
        let mut first = FirstError::default();
        first.offer_result(Ok(()), &observer);
        first.offer_halt(Err(Halt::Disconnected), &observer);
        assert!(first.0.is_none());
        first.offer(Error::Cancelled, &observer);
        first.offer(Error::Closed, &observer);
        first.offer_halt(Err(Halt::Failed(Error::Closed)), &observer);
        assert!(first.into_result().unwrap_err().is_cancelled());
    }

    #[test]
    fn test_first_of_prefers_first() {
        let o = ObserverSlot::default();
        assert!(first_of(Ok(()), Ok(()), &o).is_ok());
        assert!(
            first_of(Err(Error::Cancelled), Ok(()), &o)
                .unwrap_err()
                .is_cancelled()
        );
        assert!(
            first_of(Ok(()), Err(Error::Cancelled), &o)
                .unwrap_err()
                .is_cancelled()
        );
        assert!(
            first_of(Err(Error::Cancelled), Err(Error::Closed), &o)
                .unwrap_err()
                .is_cancelled()
        );
    }

    #[test]
    fn test_dlq_absent_and_debug() {
        let o = ObserverSlot::default();
        let mut d = Dlq::new(None);
        assert_eq!(alloc::format!("{d:?}"), "Absent");
        d.route(StageFailure::new(StageId::new("x"), "e".into()))
            .unwrap();
        d.finish(&o).unwrap();
        let mut local = Dlq::new(Some(Box::new(|_op| Ok(()))));
        assert_eq!(alloc::format!("{local:?}"), "Local");
        local.finish(&o).unwrap();
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_dlq_share_converts_once() {
        let o = ObserverSlot::default();
        let mut d = Dlq::new(Some(Box::new(|_op| Ok(()))));
        let mut other = d.share();
        assert_eq!(alloc::format!("{d:?}"), "Shared");
        assert_eq!(alloc::format!("{other:?}"), "Shared");
        let mut third = d.share();
        other.finish(&o).unwrap();
        third.finish(&o).unwrap();
        let mut none = Dlq::new(None);
        assert_eq!(alloc::format!("{:?}", none.share()), "Absent");
    }
}
