//! [`Pipeline`], [`PipelineBuilder`], and single-threaded execution.
//!
//! The builder records each stage as a closure that, given the next
//! stage's closure, produces its own. Nothing is wired together until a
//! driver starts the run: at that point the recorded closures are
//! assembled from the sink backwards into one chain (see
//! [`crate::runtime`]). Assembling late lets the same built pipeline run
//! on the caller's thread, step by step under an external executor, or
//! split across threads at its `buffer` edges.

use alloc::boxed::Box;
use core::fmt;
use core::marker::PhantomData;
use core::time::Duration;

use crate::batch::{Batch, BatchPolicy, BatchStage, ByteSize, Measure};
use crate::driver::{Driver, RunStats, SyncDriver};
use crate::error::{
    ConfigError, ConfigErrorKind, Error, ErrorPolicy, Result, StageError, StageFailure,
};
use crate::observe::{Event, Observer, ObserverPtr, ObserverSlot};
use crate::runtime::{
    BoxedStageFn, Ctx, DeadLetterFn, DeadLetterOp, Dlq, FirstError, Halt, StageOp, Wiring, contain,
    drive, handle_failure,
};
use crate::sink::Sink;
use crate::source::{IterSource, Pull, Source};
use crate::stage::Stage;
use crate::stage_id::StageId;

/// The builder's type-state: everything recorded so far, as one closure
/// that assembles the chain from a `T`-consuming tail.
///
/// Users never implement this trait; it exists so helper functions can
/// name the builder type they accept or return:
///
/// ```
/// # #[cfg(feature = "std")]
/// # fn main() -> Result<(), pipe_io::Error> {
/// use pipe_io::{Assemble, Pipeline, PipelineBuilder, Source, sink::VecSink};
///
/// fn parse<S, A>(b: PipelineBuilder<String, S, A>) -> PipelineBuilder<u32, S, impl Assemble<u32, S::Item>>
/// where
///     S: Source + 'static,
///     S::Item: Send + 'static,
///     A: Assemble<String, S::Item>,
/// {
///     b.stage_id("parse").filter_map(|s: String| s.parse().ok())
/// }
///
/// let sink = VecSink::new();
/// let out = sink.handle();
/// parse(Pipeline::from_iter(["1", "x", "3"]).map(String::from)).sink(sink).run()?;
/// assert_eq!(out.take(), vec![1, 3]);
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "std"))]
/// # fn main() {}
/// ```
///
/// The trait is sealed: it cannot be implemented or called outside this
/// crate, and its internals (the per-item calling convention) are not
/// part of the public API, so they can change in a minor release.
///
/// ```compile_fail
/// // The assembly machinery is private.
/// use pipe_io::runtime::{BoxedStageFn, Wiring};
/// ```
///
/// ```compile_fail
/// // An `Assemble` value cannot be invoked from outside the crate.
/// fn call<A: pipe_io::Assemble<u32, u32>>(
///     a: A,
///     next: pipe_io::runtime::BoxedStageFn<u32>,
///     wiring: &mut pipe_io::runtime::Wiring,
/// ) -> pipe_io::runtime::BoxedStageFn<u32> {
///     a(next, wiring)
/// }
/// ```
pub trait Assemble<T, I>: sealed::Sealed<T, I> {}

impl<T, I, F> Assemble<T, I> for F where F: sealed::Sealed<T, I> {}

/// Private supertrait of [`Assemble`]. Being reachable only inside the
/// crate, it seals `Assemble` and keeps the assembly signature private.
mod sealed {
    use crate::runtime::{BoxedStageFn, Wiring};

    pub trait Sealed<T, I>: Send + 'static {
        /// Assemble this part of the chain in front of `next`.
        fn assemble(self, next: BoxedStageFn<T>, wiring: &mut Wiring) -> BoxedStageFn<I>;
    }

    impl<T, I, F> Sealed<T, I> for F
    where
        F: FnOnce(BoxedStageFn<T>, &mut Wiring) -> BoxedStageFn<I> + Send + 'static,
    {
        #[inline]
        fn assemble(self, next: BoxedStageFn<T>, wiring: &mut Wiring) -> BoxedStageFn<I> {
            self(next, wiring)
        }
    }
}

/// Result of one [`Pipeline::step`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    /// One item was pulled and pushed through the pipeline.
    Item,
    /// The idle tick fired (no item within the tick interval); time-based
    /// stages had a chance to emit.
    Idle,
    /// The source is exhausted and the pipeline was flushed and closed.
    /// [`Pipeline::stats`] has the final counters.
    Done,
}

// ---------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------

/// Requests a graceful stop of a running pipeline.
///
/// Obtain one with [`Pipeline::cancel_handle`] before starting the run.
/// After [`CancelHandle::cancel`], the driver stops pulling from the
/// source at the next item boundary, flushes every stage and the sink,
/// closes everything, and returns [`Error::Cancelled`]. A source blocked
/// inside `pull` is not interrupted; give it a timeout (see
/// [`Source::pull_timeout`] and [`PipelineBuilder::idle_tick`]) so the
/// cancellation is observed promptly. A `try_map_retry` stage waiting
/// out a backoff delay stops waiting when the run is cancelled.
///
/// # Example
///
/// ```
/// use pipe_io::{Error, Pipeline, sink::NullSink};
///
/// let mut pipeline = Pipeline::from_iter(0_u64..).sink(NullSink::new());
/// let handle = pipeline.cancel_handle();
/// let worker = std::thread::spawn(move || pipeline.run());
/// handle.cancel();
/// let result = worker.join().expect("worker thread");
/// assert!(matches!(result, Err(Error::Cancelled)));
/// ```
#[cfg(target_has_atomic = "ptr")]
#[derive(Debug, Clone)]
pub struct CancelHandle {
    flag: alloc::sync::Arc<core::sync::atomic::AtomicBool>,
}

#[cfg(target_has_atomic = "ptr")]
impl CancelHandle {
    /// Ask the pipeline to stop. Idempotent.
    pub fn cancel(&self) {
        self.flag.store(true, core::sync::atomic::Ordering::Release);
    }

    /// True once [`CancelHandle::cancel`] was called.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(core::sync::atomic::Ordering::Acquire)
    }
}

/// The pipeline's view of its (optional) cancellation flag.
#[derive(Debug, Default, Clone)]
pub(crate) struct CancelSlot {
    #[cfg(target_has_atomic = "ptr")]
    flag: Option<alloc::sync::Arc<core::sync::atomic::AtomicBool>>,
}

impl CancelSlot {
    /// True if a cancel handle exists, i.e. the run can be cancelled.
    #[inline]
    pub(crate) const fn is_armed(&self) -> bool {
        #[cfg(target_has_atomic = "ptr")]
        {
            self.flag.is_some()
        }
        #[cfg(not(target_has_atomic = "ptr"))]
        {
            false
        }
    }

    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        #[cfg(target_has_atomic = "ptr")]
        {
            self.flag
                .as_ref()
                .is_some_and(|f| f.load(core::sync::atomic::Ordering::Relaxed))
        }
        #[cfg(not(target_has_atomic = "ptr"))]
        {
            false
        }
    }
}

fn halted(halt: Halt) -> PumpEnd {
    match halt {
        Halt::Failed(e) => PumpEnd::Failed(*e),
        Halt::Disconnected => PumpEnd::Disconnected,
    }
}

// ---------------------------------------------------------------------
// Runner: owns the source and the assembled chain for one run.
// ---------------------------------------------------------------------

/// How the pull loop ended.
pub(crate) enum PumpEnd {
    /// Source exhausted, or the caller asked for a graceful stop.
    Graceful,
    /// The cancel handle fired.
    Cancelled,
    /// A component failed.
    Failed(Error),
    /// A downstream thread went away (threaded execution only).
    Disconnected,
}

/// Outcome of a single pull.
pub(crate) enum Pulled {
    Item,
    Idle,
    End,
}

/// Owns the source, the assembled chain, and the per-run context.
///
/// Dropping a runner whose run has not been finished (because a stage
/// panicked, or because a stepped pipeline was dropped mid-run) tears the
/// run down: the sink and the dead-letter sink are flushed and closed and
/// the source is closed.
pub(crate) struct Runner<S: Source> {
    source: S,
    source_id: StageId,
    chain: BoxedStageFn<S::Item>,
    pub(crate) ctx: Ctx,
    owns_dlq: bool,
    /// True until teardown has completed. A panic in the middle of
    /// `finish` or `abort` leaves it set, so `Drop` completes the teardown.
    live: bool,
    /// `Source::close` has been called (it is called at most once).
    source_closed: bool,
    /// The dead-letter sink has been finished.
    dlq_finished: bool,
    /// Threaded execution: disconnects once every downstream segment has
    /// ended. The source is closed only after that.
    #[cfg(feature = "std")]
    segments_done: Option<std::sync::mpsc::Receiver<()>>,
}

impl<S: Source> Runner<S> {
    pub(crate) fn new(
        source: S,
        source_id: StageId,
        chain: BoxedStageFn<S::Item>,
        ctx: Ctx,
        owns_dlq: bool,
    ) -> Self {
        Self {
            source,
            source_id,
            chain,
            ctx,
            owns_dlq,
            live: true,
            source_closed: false,
            dlq_finished: false,
            #[cfg(feature = "std")]
            segments_done: None,
        }
    }

    /// Threaded execution: before closing the source, wait until `done`
    /// disconnects, i.e. every downstream segment has finished.
    #[cfg(feature = "std")]
    pub(crate) fn close_source_after(&mut self, done: std::sync::mpsc::Receiver<()>) {
        self.segments_done = Some(done);
    }

    #[inline]
    fn pull(&mut self, idle_tick: Option<Duration>) -> core::result::Result<Pull<S::Item>, Error> {
        let pulled = match idle_tick {
            None => self.source.pull().map(Pull::from),
            Some(timeout) => self.source.pull_timeout(timeout),
        };
        pulled.map_err(|e| Error::Source {
            stage: self.source_id,
            source: e.into(),
        })
    }

    /// Pull one item (or time out) and push it through the chain.
    #[inline]
    pub(crate) fn step(
        &mut self,
        idle_tick: Option<Duration>,
    ) -> core::result::Result<Pulled, Halt> {
        match self.pull(idle_tick)? {
            Pull::Item(item) => {
                self.ctx.tally.items_in = self.ctx.tally.items_in.saturating_add(1);
                self.call(StageOp::Process(item))?;
                Ok(Pulled::Item)
            }
            Pull::Idle => {
                self.tick()?;
                Ok(Pulled::Idle)
            }
            Pull::End => Ok(Pulled::End),
        }
    }

    pub(crate) fn tick(&mut self) -> core::result::Result<(), Halt> {
        self.ctx.observer.emit(&Event::Idle);
        self.call(StageOp::Tick)
    }

    /// Send one operation down the chain and count what reached the sink.
    #[inline]
    fn call(&mut self, op: StageOp<S::Item>) -> core::result::Result<(), Halt> {
        let written = (self.chain)(op, &mut self.ctx)?;
        self.ctx.tally.items_out = self.ctx.tally.items_out.wrapping_add(written);
        Ok(())
    }

    /// Pull until the source ends, the run fails, or cancellation.
    pub(crate) fn pump(&mut self, idle_tick: Option<Duration>, cancel: &CancelSlot) -> PumpEnd {
        match idle_tick {
            None => self.pump_blocking(cancel),
            Some(_) => loop {
                if cancel.is_cancelled() {
                    return PumpEnd::Cancelled;
                }
                match self.step(idle_tick) {
                    Ok(Pulled::Item | Pulled::Idle) => {}
                    Ok(Pulled::End) => return PumpEnd::Graceful,
                    Err(halt) => return halted(halt),
                }
            },
        }
    }

    /// The hot loop without idle ticks: the item counters live in
    /// registers and are published once at the end, and the cancellation
    /// check is compiled out when no handle was requested.
    fn pump_blocking(&mut self, cancel: &CancelSlot) -> PumpEnd {
        let mut counts = (0_u64, 0_u64);
        let end = if cancel.is_armed() {
            self.pull_loop::<true>(cancel, &mut counts)
        } else {
            self.pull_loop::<false>(cancel, &mut counts)
        };
        let (pulled, written) = counts;
        self.ctx.tally.items_in = self.ctx.tally.items_in.saturating_add(pulled);
        self.ctx.tally.items_out = self.ctx.tally.items_out.wrapping_add(written);
        end
    }

    #[inline(always)]
    fn pull_loop<const CANCELLABLE: bool>(
        &mut self,
        cancel: &CancelSlot,
        (pulled, written): &mut (u64, u64),
    ) -> PumpEnd {
        loop {
            if CANCELLABLE && cancel.is_cancelled() {
                return PumpEnd::Cancelled;
            }
            match self.source.pull() {
                Ok(Some(item)) => {
                    *pulled = pulled.wrapping_add(1);
                    match (self.chain)(StageOp::Process(item), &mut self.ctx) {
                        Ok(n) => *written = written.wrapping_add(n),
                        Err(halt) => return halted(halt),
                    }
                }
                Ok(None) => return PumpEnd::Graceful,
                Err(e) => {
                    return PumpEnd::Failed(Error::Source {
                        stage: self.source_id,
                        source: e.into(),
                    });
                }
            }
        }
    }

    /// Graceful end: flush and close the chain, close the source, finish
    /// the dead-letter sink. If the flush fails the chain is aborted
    /// instead of closed.
    pub(crate) fn finish(&mut self, first: &mut FirstError) {
        match self.call(StageOp::Flush) {
            Ok(()) => {
                let closed = self.call(StageOp::Close);
                first.offer_halt(closed, &self.ctx.observer);
            }
            Err(halt) => {
                first.offer_halt(Err(halt), &self.ctx.observer);
                let aborted = self.call(StageOp::Abort);
                first.offer_halt(aborted, &self.ctx.observer);
            }
        }
        self.close_rest(first);
        self.live = false;
    }

    /// Failure end: abort the chain (the sink is flushed and closed, stage
    /// buffers are discarded), close the source, finish the dead-letter
    /// sink.
    pub(crate) fn abort(&mut self, first: &mut FirstError) {
        let aborted = self.call(StageOp::Abort);
        first.offer_halt(aborted, &self.ctx.observer);
        self.close_rest(first);
        self.live = false;
    }

    fn close_rest(&mut self, first: &mut FirstError) {
        #[cfg(feature = "std")]
        if let Some(done) = self.segments_done.take() {
            // Drop the chain, and with it the sending side of every edge
            // this thread feeds: a segment that did not receive `Close` or
            // `Abort` then sees the disconnect and tears itself down, so the
            // wait below always ends.
            self.chain = Box::new(|_op, _ctx| Ok(0));
            // Blocks until every segment has dropped its sender.
            let _disconnected = done.recv();
        }
        if !self.source_closed {
            self.source_closed = true;
            let source_id = self.source_id;
            let closed = self.source.close().map_err(|e| Error::Source {
                stage: source_id,
                source: e.into(),
            });
            first.offer_result(closed, &self.ctx.observer);
        }
        if self.owns_dlq && !self.dlq_finished {
            let finished = self.ctx.dlq.finish(&self.ctx.observer);
            // Set afterwards: if a dead-letter `flush` panicked, the
            // teardown in `Drop` retries and the sink's own guard (see
            // `dead_letter`) skips straight to `close`.
            self.dlq_finished = true;
            first.offer_result(finished, &self.ctx.observer);
        }
    }
}

impl<S: Source> Drop for Runner<S> {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        let mut errors = FirstError::default();
        // A panic inside a user `flush`/`close` during teardown is contained
        // so it cannot escalate an unwinding panic into an abort. Each
        // lifecycle call runs at most once, so retrying moves past the
        // call that panicked; there are at most five such calls (sink
        // flush and close, source close, dead-letter flush and close).
        for _attempt in 0..6 {
            if !self.live {
                break;
            }
            let _completed = contain(|| self.abort(&mut errors));
        }
        if let Err(e) = errors.into_result() {
            let observer = &self.ctx.observer;
            let _reported = contain(|| observer.emit(&Event::TeardownError { error: &e }));
        }
    }
}

impl<S: Source> fmt::Debug for Runner<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runner")
            .field("source_id", &self.source_id)
            .field("ctx", &self.ctx)
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------

/// The recorded but not yet assembled pipeline.
pub(crate) type Assembly<I> = Box<dyn FnOnce(&mut Wiring) -> BoxedStageFn<I> + Send + 'static>;

pub(crate) struct Built<S: Source> {
    pub(crate) source: S,
    pub(crate) assembly: Assembly<S::Item>,
    pub(crate) dead_letter: Option<DeadLetterFn>,
    pub(crate) observer: Option<ObserverPtr>,
}

enum State<S: Source> {
    Built(Built<S>),
    Running {
        runner: Runner<S>,
        #[cfg(feature = "std")]
        started: std::time::Instant,
    },
    Ended {
        stats: RunStats,
        succeeded: bool,
    },
}

/// A built pipeline, ready to run.
///
/// Run it to completion with [`Pipeline::run`] (on the calling thread),
/// [`Pipeline::run_threaded`] (one thread per `buffer` segment, `std`
/// only), or [`Pipeline::run_with`] (any [`Driver`]). External executors
/// can instead drive it one item at a time with [`Pipeline::step`] and
/// end it with [`Pipeline::finish`].
///
/// # Lifecycle guarantees
///
/// Once a run starts, the sink and the dead-letter sink are flushed and
/// closed, and the source is closed, on every exit path: end of stream,
/// cancellation, an error from any component, a panic, and dropping a
/// stepped pipeline before it finished. The first error is returned;
/// later teardown errors are reported to the observer
/// ([`Event::TeardownError`]). A pipeline whose configuration is invalid
/// never starts: it returns [`Error::Config`] without calling any
/// component.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "std")]
/// # fn main() -> Result<(), pipe_io::Error> {
/// use pipe_io::{Pipeline, sink::VecSink};
///
/// let sink = VecSink::new();
/// let out = sink.handle();
/// let stats = Pipeline::from_iter(1..=5)
///     .map(|n: i32| n * 10)
///     .filter(|n: &i32| *n > 20)
///     .sink(sink)
///     .run()?;
/// assert_eq!(out.take(), vec![30, 40, 50]);
/// assert_eq!((stats.items_in, stats.items_out), (5, 3));
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "std"))]
/// # fn main() {}
/// ```
pub struct Pipeline<S: Source> {
    source_id: StageId,
    idle_tick: Option<Duration>,
    config_error: Option<ConfigError>,
    cancel: CancelSlot,
    state: State<S>,
    /// Set while `step` or `tick` runs user code. Still set on the next
    /// call means the previous one panicked and the panic was caught.
    in_call: bool,
}

impl<S: Source> fmt::Debug for Pipeline<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            State::Built(_) => "built",
            State::Running { .. } => "running",
            State::Ended {
                succeeded: true, ..
            } => "done",
            State::Ended { .. } => "failed",
        };
        f.debug_struct("Pipeline")
            .field("source_id", &self.source_id)
            .field("state", &state)
            .field("idle_tick", &self.idle_tick)
            .field("config_error", &self.config_error)
            .finish_non_exhaustive()
    }
}

fn identity<I>(tail: BoxedStageFn<I>, _wiring: &mut Wiring) -> BoxedStageFn<I> {
    tail
}

impl<S> Pipeline<S>
where
    S: Source + 'static,
    S::Item: Send + 'static,
{
    /// Start building a pipeline from a [`Source`].
    pub fn from_source(source: S) -> PipelineBuilder<S::Item, S, impl Assemble<S::Item, S::Item>> {
        PipelineBuilder {
            source,
            source_id: StageId::new("source"),
            finalize: identity::<S::Item>,
            error_policy: ErrorPolicy::FailFast,
            pending_stage_id: None,
            dead_letter: None,
            dead_letter_user: None,
            config_error: None,
            observer: None,
            idle_tick: None,
            _marker: PhantomData,
        }
    }

    /// Run on the calling thread until the source is exhausted.
    ///
    /// Equivalent to `self.run_with(SyncDriver::new())`, without the
    /// `Send` bounds of the [`Driver`] trait.
    ///
    /// # Errors
    ///
    /// The first error produced by the source, a `FailFast` stage, the
    /// sink, or the dead-letter sink; [`Error::Config`] for an invalid
    /// configuration; [`Error::Cancelled`] after a cancellation;
    /// [`Error::Closed`] if the pipeline already ran.
    ///
    /// # Panics
    ///
    /// A panic raised by user code propagates unchanged, after the sink,
    /// the dead-letter sink, and the source were flushed and closed.
    pub fn run(self) -> Result<RunStats> {
        SyncDriver::new().run(self)
    }

    /// Run with an explicit [`Driver`].
    ///
    /// # Errors
    ///
    /// As for [`Pipeline::run`], plus whatever the driver reports.
    pub fn run_with<D>(self, driver: D) -> Result<RunStats>
    where
        D: Driver,
        S: Send,
    {
        driver.run(self)
    }

    /// Run with the [`crate::driver::ThreadedDriver`]: every segment
    /// between `buffer` edges runs on its own thread.
    ///
    /// # Errors
    ///
    /// As for [`Pipeline::run`], plus [`Error::Driver`] if a worker
    /// thread cannot be spawned.
    ///
    /// # Panics
    ///
    /// A panic in any worker thread is re-raised on the calling thread
    /// with its original payload, after every thread was joined and every
    /// component torn down.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn run_threaded(self) -> Result<RunStats>
    where
        S: Send,
    {
        crate::driver::ThreadedDriver::new().run(self)
    }

    /// Pull one item and push it through the pipeline.
    ///
    /// The first call starts the run. Returns [`Step::Done`] once the
    /// source is exhausted; by then every stage, the sink, the
    /// dead-letter sink, and the source have been flushed and closed.
    /// Calling `step` again after `Done` keeps returning `Done`.
    ///
    /// # Errors
    ///
    /// As for [`Pipeline::run`]. After an error the pipeline is already
    /// torn down; further calls return [`Error::Closed`].
    ///
    /// # Panics
    ///
    /// A panic raised by user code propagates unchanged. If the caller
    /// catches it (as executors that isolate tasks do), the item in flight
    /// is lost, so the pipeline is poisoned: the next call to `step`,
    /// [`Pipeline::tick`], or [`Pipeline::finish`] tears it down (the sink
    /// and dead-letter sink are flushed and closed, the source is closed)
    /// and returns [`Error::Driver`]; later calls return [`Error::Closed`].
    ///
    /// # Example
    ///
    /// ```
    /// # #[cfg(feature = "std")]
    /// # fn main() -> Result<(), pipe_io::Error> {
    /// use pipe_io::{Pipeline, Step, sink::VecSink};
    ///
    /// let sink = VecSink::new();
    /// let out = sink.handle();
    /// let mut p = Pipeline::from_iter([1, 2]).sink(sink);
    /// assert_eq!(p.step()?, Step::Item);
    /// assert_eq!(out.snapshot(), vec![1]);
    /// assert_eq!(p.step()?, Step::Item);
    /// assert_eq!(p.step()?, Step::Done);
    /// assert_eq!(p.stats().items_out, 2);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "std"))]
    /// # fn main() {}
    /// ```
    pub fn step(&mut self) -> Result<Step> {
        self.ensure_started()?;
        self.check_poisoned()?;
        let cancelled = self.cancel.is_cancelled();
        let idle_tick = self.idle_tick;
        let outcome = match &mut self.state {
            State::Running { runner, .. } if !cancelled => {
                self.in_call = true;
                let outcome = runner.step(idle_tick);
                self.in_call = false;
                outcome
            }
            State::Running { .. } => {
                return self.conclude(PumpEnd::Cancelled).map(|_| Step::Done);
            }
            State::Ended {
                succeeded: true, ..
            } => return Ok(Step::Done),
            State::Ended { .. } | State::Built(_) => return Err(Error::Closed),
        };
        match outcome {
            Ok(Pulled::Item) => Ok(Step::Item),
            Ok(Pulled::Idle) => Ok(Step::Idle),
            Ok(Pulled::End) => self.conclude(PumpEnd::Graceful).map(|_| Step::Done),
            Err(Halt::Failed(e)) => self.conclude(PumpEnd::Failed(*e)).map(|_| Step::Done),
            Err(Halt::Disconnected) => self.conclude(PumpEnd::Disconnected).map(|_| Step::Done),
        }
    }

    /// Deliver an idle tick now: time-based stages (windows, age-triggered
    /// batches) emit whatever has expired. Useful for executors that keep
    /// their own timers. Starts the run if it has not started.
    ///
    /// # Errors
    ///
    /// As for [`Pipeline::step`].
    pub fn tick(&mut self) -> Result<()> {
        self.ensure_started()?;
        self.check_poisoned()?;
        let result = match &mut self.state {
            State::Running { runner, .. } => {
                self.in_call = true;
                let result = runner.tick();
                self.in_call = false;
                result
            }
            State::Ended { .. } | State::Built(_) => return Err(Error::Closed),
        };
        match result {
            Ok(()) => Ok(()),
            Err(Halt::Failed(e)) => self.conclude(PumpEnd::Failed(*e)).map(|_| ()),
            Err(Halt::Disconnected) => self.conclude(PumpEnd::Disconnected).map(|_| ()),
        }
    }

    /// End the run gracefully, whether or not the source is exhausted:
    /// flush every stage, flush and close the sink and the dead-letter
    /// sink, close the source. A pipeline that never started is started
    /// and immediately finished, so its components still see the full
    /// lifecycle.
    ///
    /// # Errors
    ///
    /// The first error raised while flushing or closing;
    /// [`Error::Config`] for an invalid configuration; [`Error::Closed`]
    /// if the run already failed.
    pub fn finish(mut self) -> Result<RunStats> {
        match &self.state {
            State::Ended {
                stats,
                succeeded: true,
            } => return Ok(*stats),
            State::Ended { .. } => return Err(Error::Closed),
            State::Built(_) | State::Running { .. } => {}
        }
        self.ensure_started()?;
        self.check_poisoned()?;
        self.conclude(PumpEnd::Graceful)
    }

    /// Counters so far. Final once the run ended.
    #[must_use]
    pub fn stats(&self) -> RunStats {
        match &self.state {
            State::Built(_) => RunStats::default(),
            #[cfg(feature = "std")]
            State::Running { runner, started } => {
                let mut stats = runner.ctx.tally.to_stats();
                stats.duration = started.elapsed();
                stats
            }
            #[cfg(not(feature = "std"))]
            State::Running { runner } => runner.ctx.tally.to_stats(),
            State::Ended { stats, .. } => *stats,
        }
    }

    /// True once the run ended (successfully or not).
    #[must_use]
    pub const fn is_finished(&self) -> bool {
        matches!(self.state, State::Ended { .. })
    }

    /// The configuration error recorded by the builder, if any. A
    /// pipeline with a configuration error refuses to start.
    #[must_use]
    pub const fn config_error(&self) -> Option<ConfigError> {
        self.config_error
    }

    /// A handle that stops the run gracefully from another thread; see
    /// [`CancelHandle`]. All handles of one pipeline share the same flag.
    #[cfg(target_has_atomic = "ptr")]
    pub fn cancel_handle(&mut self) -> CancelHandle {
        let flag = self.cancel.flag.get_or_insert_with(|| {
            alloc::sync::Arc::new(core::sync::atomic::AtomicBool::new(false))
        });
        CancelHandle {
            flag: alloc::sync::Arc::clone(flag),
        }
    }

    // -- internals shared with the drivers ------------------------------

    /// Take the recorded pipeline out for a driver. Fails on an invalid
    /// configuration (without touching any component) or if the pipeline
    /// already started.
    pub(crate) fn take_built(&mut self) -> Result<Built<S>> {
        if let Some(e) = self.config_error {
            if matches!(self.state, State::Built(_)) {
                self.state = State::Ended {
                    stats: RunStats::default(),
                    succeeded: false,
                };
                return Err(Error::Config(e));
            }
        }
        match core::mem::replace(
            &mut self.state,
            State::Ended {
                stats: RunStats::default(),
                succeeded: false,
            },
        ) {
            State::Built(built) => Ok(built),
            other => {
                self.state = other;
                Err(Error::Closed)
            }
        }
    }

    #[cfg(feature = "std")]
    pub(crate) const fn idle_tick(&self) -> Option<Duration> {
        self.idle_tick
    }

    #[cfg(feature = "std")]
    pub(crate) const fn source_id(&self) -> StageId {
        self.source_id
    }

    #[cfg(feature = "std")]
    pub(crate) fn cancel_slot(&self) -> CancelSlot {
        self.cancel.clone()
    }

    /// A previous `step` or `tick` panicked and the caller caught the
    /// panic: the item in flight is lost, so the run cannot succeed. Tear
    /// down (flush and close the sink, close the source) and fail.
    fn check_poisoned(&mut self) -> Result<()> {
        if !self.in_call {
            return Ok(());
        }
        self.in_call = false;
        if matches!(self.state, State::Running { .. }) {
            let poisoned = Error::Driver {
                source: "a previous call panicked; the pipeline was torn down".into(),
            };
            return self.conclude(PumpEnd::Failed(poisoned)).map(|_| ());
        }
        Ok(())
    }

    fn ensure_started(&mut self) -> Result<()> {
        if !matches!(self.state, State::Built(_)) {
            return Ok(());
        }
        let built = self.take_built()?;
        let Built {
            source,
            assembly,
            dead_letter,
            observer,
        } = built;
        let mut wiring = Wiring::sync();
        let chain = assembly(&mut wiring);
        let ctx = Ctx::new(
            Dlq::new(dead_letter),
            ObserverSlot::new(observer),
            self.cancel.clone(),
        );
        ctx.observer.emit(&Event::Started);
        let runner = Runner::new(source, self.source_id, chain, ctx, true);
        self.state = State::Running {
            runner,
            #[cfg(feature = "std")]
            started: std::time::Instant::now(),
        };
        Ok(())
    }

    /// Single-threaded run to completion (the `SyncDriver`).
    pub(crate) fn run_sync(mut self) -> Result<RunStats> {
        self.ensure_started()?;
        let idle_tick = self.idle_tick;
        let end = match &mut self.state {
            State::Running { runner, .. } => runner.pump(idle_tick, &self.cancel),
            State::Built(_) | State::Ended { .. } => return Err(Error::Closed),
        };
        self.conclude(end)
    }

    /// Tear down according to how the run ended and record the outcome.
    fn conclude(&mut self, end: PumpEnd) -> Result<RunStats> {
        let interim = State::Ended {
            stats: RunStats::default(),
            succeeded: false,
        };
        let state = core::mem::replace(&mut self.state, interim);
        #[cfg(feature = "std")]
        let State::Running {
            mut runner,
            started,
        } = state
        else {
            self.state = state;
            return Err(Error::Closed);
        };
        #[cfg(not(feature = "std"))]
        let State::Running { mut runner } = state else {
            self.state = state;
            return Err(Error::Closed);
        };

        let mut first = FirstError::default();
        let cancelled = matches!(end, PumpEnd::Cancelled);
        match end {
            PumpEnd::Graceful | PumpEnd::Cancelled => runner.finish(&mut first),
            PumpEnd::Failed(e) => {
                first.offer(e, &runner.ctx.observer);
                runner.abort(&mut first);
            }
            PumpEnd::Disconnected => {
                let lost = Error::Driver {
                    source: "a pipeline segment stopped unexpectedly".into(),
                };
                first.offer(lost, &runner.ctx.observer);
                runner.abort(&mut first);
            }
        }

        #[cfg_attr(not(feature = "std"), allow(unused_mut))]
        let mut stats = runner.ctx.tally.to_stats();
        #[cfg(feature = "std")]
        {
            stats.duration = started.elapsed();
        }
        let result = match first.into_result() {
            Err(e) => Err(e),
            Ok(()) if cancelled => Err(Error::Cancelled),
            Ok(()) => Ok(stats),
        };
        runner.ctx.observer.emit(&Event::Finished {
            stats: &stats,
            error: result.as_ref().err(),
        });
        self.state = State::Ended {
            stats,
            succeeded: result.is_ok(),
        };
        result
    }
}

impl Pipeline<IterSource<core::iter::Empty<()>>> {
    /// Start building a pipeline from anything iterable.
    ///
    /// # Example
    ///
    /// ```
    /// # #[cfg(feature = "std")]
    /// # fn main() -> Result<(), pipe_io::Error> {
    /// use pipe_io::{Pipeline, sink::VecSink};
    ///
    /// let sink = VecSink::new();
    /// let out = sink.handle();
    /// Pipeline::from_iter(0..3).sink(sink).run()?;
    /// assert_eq!(out.take(), vec![0, 1, 2]);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "std"))]
    /// # fn main() {}
    /// ```
    #[allow(clippy::should_implement_trait)] // mirrors the iterator idiom; FromIterator cannot express the builder type
    pub fn from_iter<II>(
        iter: II,
    ) -> PipelineBuilder<II::Item, IterSource<II::IntoIter>, impl Assemble<II::Item, II::Item>>
    where
        II: IntoIterator,
        II::Item: Send + 'static,
        II::IntoIter: 'static,
    {
        Pipeline::from_source(IterSource::new(iter))
    }
}

// ---------------------------------------------------------------------
// PipelineBuilder
// ---------------------------------------------------------------------

/// Typed builder returned by [`Pipeline::from_source`] and
/// [`Pipeline::from_iter`].
///
/// `T` is the item type at the current end of the pipeline; each
/// transform changes it. `Acc` is the opaque record of the stages added so
/// far (see [`Assemble`]).
///
/// Builder methods never panic. An invalid setting (a zero window size, a
/// batch policy without a trigger, ...) is recorded and reported by
/// [`PipelineBuilder::try_sink`], or by the run as [`Error::Config`].
#[must_use = "a builder does nothing until it is terminated with `sink` and run"]
pub struct PipelineBuilder<T, S, Acc>
where
    S: Source,
    Acc: Assemble<T, S::Item>,
{
    source: S,
    source_id: StageId,
    finalize: Acc,
    error_policy: ErrorPolicy,
    pending_stage_id: Option<StageId>,
    dead_letter: Option<DeadLetterFn>,
    dead_letter_user: Option<StageId>,
    config_error: Option<ConfigError>,
    observer: Option<ObserverPtr>,
    idle_tick: Option<Duration>,
    _marker: PhantomData<fn() -> T>,
}

impl<T, S, Acc> fmt::Debug for PipelineBuilder<T, S, Acc>
where
    S: Source,
    Acc: Assemble<T, S::Item>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineBuilder")
            .field("source_id", &self.source_id)
            .field("error_policy", &self.error_policy)
            .field("pending_stage_id", &self.pending_stage_id)
            .field("dead_letter", &self.dead_letter.is_some())
            .field("config_error", &self.config_error)
            .field("idle_tick", &self.idle_tick)
            .finish_non_exhaustive()
    }
}

impl<T, S, Acc> PipelineBuilder<T, S, Acc>
where
    S: Source + 'static,
    S::Item: Send + 'static,
    T: Send + 'static,
    Acc: Assemble<T, S::Item>,
{
    // -- internal helpers ------------------------------------------------

    fn take_stage_id(&mut self, default: &'static str) -> StageId {
        self.pending_stage_id
            .take()
            .unwrap_or_else(|| StageId::new(default))
    }

    fn record(&mut self, error: ConfigError) {
        if self.config_error.is_none() {
            self.config_error = Some(error);
        }
    }

    /// Note that a fallible stage was added under the current policy.
    fn note_fallible(&mut self, id: StageId) {
        if self.error_policy == ErrorPolicy::DeadLetter && self.dead_letter_user.is_none() {
            self.dead_letter_user = Some(id);
        }
    }

    /// Append one stage: `make` turns the next stage (consuming `U`) into
    /// this stage (consuming `T`).
    fn link<U, M>(self, make: M) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        U: Send + 'static,
        M: FnOnce(BoxedStageFn<U>, &mut Wiring) -> BoxedStageFn<T> + Send + 'static,
    {
        let Self {
            source,
            source_id,
            finalize,
            error_policy,
            pending_stage_id: _,
            dead_letter,
            dead_letter_user,
            config_error,
            observer,
            idle_tick,
            _marker,
        } = self;
        let finalize = move |next: BoxedStageFn<U>, wiring: &mut Wiring| {
            let this = make(next, wiring);
            finalize.assemble(this, wiring)
        };
        PipelineBuilder {
            source,
            source_id,
            finalize,
            error_policy,
            pending_stage_id: None,
            dead_letter,
            dead_letter_user,
            config_error,
            observer,
            idle_tick,
            _marker: PhantomData,
        }
    }

    /// Append a [`Stage`] under the current error policy.
    fn stage_as<St>(
        mut self,
        stage: St,
        id: StageId,
        fallible: bool,
    ) -> PipelineBuilder<St::Output, S, impl Assemble<St::Output, S::Item>>
    where
        St: Stage<Input = T> + Send + 'static,
        St::Output: Send + 'static,
    {
        if fallible {
            self.note_fallible(id);
        }
        let policy = self.error_policy;
        self.link(
            move |mut next: BoxedStageFn<St::Output>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                let mut stage = stage;
                Box::new(move |op, ctx| match op {
                    StageOp::Process(item) => {
                        drive(&mut stage, &mut next, ctx, policy, id, |s, out| {
                            s.process(item, out)
                        })
                    }
                    StageOp::Tick => {
                        let emitted =
                            drive(&mut stage, &mut next, ctx, policy, id, |s, out| s.tick(out))?;
                        Ok(emitted.wrapping_add(next(StageOp::Tick, ctx)?))
                    }
                    StageOp::Flush => {
                        // A flush error handled by Continue/DeadLetter returns
                        // Ok here, so the flush still reaches every later stage
                        // and the sink.
                        let emitted = drive(&mut stage, &mut next, ctx, policy, id, |s, out| {
                            s.flush(out)
                        })?;
                        Ok(emitted.wrapping_add(next(StageOp::Flush, ctx)?))
                    }
                    StageOp::Close => next(StageOp::Close, ctx),
                    StageOp::Abort => next(StageOp::Abort, ctx),
                })
            },
        )
    }

    // -- settings ----------------------------------------------------------

    /// Name the next stage (used in errors, dead-letter records, and
    /// observer events). Applies to the next stage-adding call, including
    /// `sink`.
    pub fn stage_id<I: Into<StageId>>(mut self, id: I) -> Self {
        self.pending_stage_id = Some(id.into());
        self
    }

    /// Name the source (default `"source"`). Source errors carry this id.
    pub fn source_id<I: Into<StageId>>(mut self, id: I) -> Self {
        self.source_id = id.into();
        self
    }

    /// Set the [`ErrorPolicy`] for fallible stages added after this call.
    pub fn on_error(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    /// Install the sink that receives [`StageFailure`] records from stages
    /// running under [`ErrorPolicy::DeadLetter`]. May be called before or
    /// after those stages; calling it again replaces the previous sink
    /// (which is dropped without being flushed or closed, since it never
    /// started).
    ///
    /// The dead-letter sink is flushed and closed after the main sink on
    /// every exit path. Its errors are reported as [`Error::Sink`] with the
    /// stage id `"dead_letter"`.
    pub fn dead_letter<Sk>(mut self, sink: Sk) -> Self
    where
        Sk: Sink<Item = StageFailure> + Send + 'static,
    {
        let mut sink = sink;
        let id = StageId::new("dead_letter");
        let fail = move |e: Sk::Error| Error::Sink {
            stage: id,
            source: e.into(),
        };
        // `flush` and `close` run at most once each, even if teardown is
        // retried after one of them panicked.
        let (mut flushed, mut closed) = (false, false);
        self.dead_letter = Some(Box::new(move |op| match op {
            DeadLetterOp::Write(failure) => sink.write(failure).map_err(fail),
            DeadLetterOp::Flush if !flushed => {
                flushed = true;
                sink.flush().map_err(fail)
            }
            DeadLetterOp::Close if !closed => {
                closed = true;
                sink.close().map_err(fail)
            }
            DeadLetterOp::Flush | DeadLetterOp::Close => Ok(()),
        }));
        self
    }

    /// Install an [`Observer`] for lifecycle, error, retry, and idle
    /// events. Replaces any previously installed observer.
    pub fn observe<O>(mut self, observer: O) -> Self
    where
        O: Observer + 'static,
    {
        let boxed: Box<dyn Observer> = Box::new(observer);
        self.observer = Some(ObserverPtr::from(boxed));
        self
    }

    /// Enable the idle tick: when the source delivers nothing for
    /// `interval`, time-based stages (windows, age-triggered batches, and
    /// custom stages overriding [`Stage::tick`]) get a chance to emit.
    ///
    /// The source must support waiting with a timeout
    /// ([`Source::pull_timeout`]); [`crate::source::ChannelSource`] does,
    /// sources using the default implementation block in `pull` and never
    /// report idleness. `interval` must be non-zero.
    pub fn idle_tick(mut self, interval: Duration) -> Self {
        if interval.is_zero() {
            self.record(ConfigError::new(
                StageId::new("idle_tick"),
                ConfigErrorKind::ZeroDuration { field: "idle_tick" },
            ));
        }
        self.idle_tick = Some(interval);
        self
    }

    // -- infallible transforms ---------------------------------------------

    /// Apply a 1:1 transform.
    pub fn map<U, F>(self, mut f: F) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        U: Send + 'static,
        F: FnMut(T) -> U + Send + 'static,
    {
        self.link(
            move |mut next: BoxedStageFn<U>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => next(StageOp::Process(f(item)), ctx),
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Keep only items for which `pred` returns `true`.
    pub fn filter<F>(self, mut pred: F) -> PipelineBuilder<T, S, impl Assemble<T, S::Item>>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        self.link(
            move |mut next: BoxedStageFn<T>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) if pred(&item) => next(StageOp::Process(item), ctx),
                    Ok(_) => Ok(0),
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Map and filter in one step: `None` drops the item.
    pub fn filter_map<U, F>(self, mut f: F) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        U: Send + 'static,
        F: FnMut(T) -> Option<U> + Send + 'static,
    {
        self.link(
            move |mut next: BoxedStageFn<U>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => match f(item) {
                        Some(out) => next(StageOp::Process(out), ctx),
                        None => Ok(0),
                    },
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Emit zero or more items per input.
    pub fn flat_map<U, F, II>(self, mut f: F) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        U: Send + 'static,
        II: IntoIterator<Item = U>,
        F: FnMut(T) -> II + Send + 'static,
    {
        self.link(
            move |mut next: BoxedStageFn<U>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => {
                        let mut written: u64 = 0;
                        for out in f(item) {
                            written = written.wrapping_add(next(StageOp::Process(out), ctx)?);
                        }
                        Ok(written)
                    }
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Observe items without changing them.
    pub fn inspect<F>(self, mut f: F) -> PipelineBuilder<T, S, impl Assemble<T, S::Item>>
    where
        F: FnMut(&T) + Send + 'static,
    {
        self.link(
            move |mut next: BoxedStageFn<T>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => {
                        f(&item);
                        next(StageOp::Process(item), ctx)
                    }
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    // -- fallible transforms -------------------------------------------------

    /// Fallible 1:1 transform under the current [`ErrorPolicy`]. The input
    /// is moved into `f`, so a dead-letter record cannot carry it; use
    /// [`PipelineBuilder::try_map_ref`] when the failing item must be kept.
    pub fn try_map<U, F, E>(mut self, mut f: F) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        U: Send + 'static,
        E: StageError,
        F: FnMut(T) -> core::result::Result<U, E> + Send + 'static,
    {
        let id = self.take_stage_id("try_map");
        self.note_fallible(id);
        let policy = self.error_policy;
        self.link(
            move |mut next: BoxedStageFn<U>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => match f(item) {
                        Ok(out) => next(StageOp::Process(out), ctx),
                        Err(e) => handle_failure(ctx, policy, id, e, None, 1),
                    },
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Fallible transform that borrows its input. Under
    /// [`ErrorPolicy::DeadLetter`] the failing item is moved into
    /// [`StageFailure::input`] so it can be inspected or replayed.
    ///
    /// The item type must be `Sync` because the captured input travels
    /// inside [`StageFailure`], which stays `Send + Sync` (so it converts
    /// into `Box<dyn Error + Send + Sync>` and similar). For items that are
    /// not `Sync`, use [`PipelineBuilder::try_map`].
    ///
    /// # Example
    ///
    /// ```
    /// # #[cfg(feature = "std")]
    /// # fn main() -> Result<(), pipe_io::Error> {
    /// use pipe_io::{ErrorPolicy, Pipeline, StageFailure, sink::{NullSink, VecSink}};
    ///
    /// let dlq = VecSink::<StageFailure>::new();
    /// let failures = dlq.handle();
    /// Pipeline::from_iter(["1", "x"])
    ///     .on_error(ErrorPolicy::DeadLetter)
    ///     .try_map_ref(|s: &&str| s.parse::<u32>())
    ///     .dead_letter(dlq)
    ///     .sink(NullSink::new())
    ///     .run()?;
    /// let mut failed = failures.take();
    /// assert_eq!(failed[0].take_input::<&str>(), Some("x"));
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "std"))]
    /// # fn main() {}
    /// ```
    pub fn try_map_ref<U, F, E>(
        mut self,
        mut f: F,
    ) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        T: Sync,
        U: Send + 'static,
        E: StageError,
        F: FnMut(&T) -> core::result::Result<U, E> + Send + 'static,
    {
        let id = self.take_stage_id("try_map_ref");
        self.note_fallible(id);
        let policy = self.error_policy;
        self.link(
            move |mut next: BoxedStageFn<U>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => match f(&item) {
                        Ok(out) => next(StageOp::Process(out), ctx),
                        Err(e) => {
                            let input = keep_input(policy, item);
                            handle_failure(ctx, policy, id, e, input, 1)
                        }
                    },
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Fallible transform retried according to `retry` before the
    /// [`ErrorPolicy`] applies. The closure borrows its input so it can be
    /// called again; the failing item is attached to dead-letter records.
    ///
    /// Backoff sleeps on the thread running the stage (under the threaded
    /// driver, only that segment's thread). Retries are counted in
    /// [`RunStats::retries`] and reported as [`Event::Retry`]. Delays are
    /// not capped, but a cancelled run ([`CancelHandle`]) stops waiting
    /// within about 10 ms: the item then fails with the attempts made so
    /// far and the [`ErrorPolicy`] applies.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    /// use pipe_io::{Backoff, Pipeline, RetryPolicy, sink::VecSink};
    ///
    /// let mut calls = 0;
    /// let sink = VecSink::new();
    /// let out = sink.handle();
    /// let stats = Pipeline::from_iter([7_u32])
    ///     .try_map_retry(
    ///         RetryPolicy::new(3).backoff(Backoff::Fixed(Duration::from_millis(1))),
    ///         move |n: &u32| {
    ///             calls += 1;
    ///             if calls < 3 { Err("transient") } else { Ok(*n * 2) }
    ///         },
    ///     )
    ///     .sink(sink)
    ///     .run()?;
    /// assert_eq!(out.take(), vec![14]);
    /// assert_eq!(stats.retries, 2);
    /// # Ok::<(), pipe_io::Error>(())
    /// ```
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn try_map_retry<U, F, E>(
        mut self,
        retry: crate::retry::RetryPolicy,
        mut f: F,
    ) -> PipelineBuilder<U, S, impl Assemble<U, S::Item>>
    where
        T: Sync,
        U: Send + 'static,
        E: StageError,
        F: FnMut(&T) -> core::result::Result<U, E> + Send + 'static,
    {
        let id = self.take_stage_id("try_map_retry");
        if let Err(kind) = retry.check() {
            self.record(ConfigError::new(id, kind));
        }
        self.note_fallible(id);
        let policy = self.error_policy;
        self.link(
            move |mut next: BoxedStageFn<U>, _wiring: &mut Wiring| -> BoxedStageFn<T> {
                let mut jitter = crate::retry::Jitter::seeded();
                Box::new(move |op, ctx| match op.into_item() {
                    Ok(item) => {
                        let mut attempt: u32 = 1;
                        loop {
                            match f(&item) {
                                Ok(out) => return next(StageOp::Process(out), ctx),
                                Err(e) if attempt < retry.max_attempts() => {
                                    let error: crate::error::BoxError = e.into();
                                    let delay = retry.delay_for(attempt, &mut jitter);
                                    ctx.tally.retries = ctx.tally.retries.saturating_add(1);
                                    ctx.observer.emit(&Event::Retry {
                                        stage: id,
                                        attempt,
                                        delay,
                                        error: &*error,
                                    });
                                    if !sleep_unless_cancelled(delay, &ctx.cancel) {
                                        // Cancelled while backing off: give
                                        // up on this item now, so the
                                        // cancellation is not delayed by
                                        // the remaining attempts.
                                        let input = keep_input(policy, item);
                                        return handle_failure(
                                            ctx, policy, id, error, input, attempt,
                                        );
                                    }
                                    attempt = attempt.saturating_add(1);
                                }
                                Err(e) => {
                                    let input = keep_input(policy, item);
                                    return handle_failure(ctx, policy, id, e, input, attempt);
                                }
                            }
                        }
                    }
                    Err(signal) => next(signal.op(), ctx),
                })
            },
        )
    }

    /// Plug in a custom [`Stage`] under the current [`ErrorPolicy`].
    pub fn stage<St>(
        mut self,
        stage: St,
    ) -> PipelineBuilder<St::Output, S, impl Assemble<St::Output, S::Item>>
    where
        St: Stage<Input = T> + Send + 'static,
        St::Output: Send + 'static,
    {
        let id = self.take_stage_id("stage");
        self.stage_as(stage, id, true)
    }

    // -- batching ------------------------------------------------------------

    /// Group items into [`Batch`]es by count and/or age.
    ///
    /// The policy must have a trigger and non-zero limits; a `max_bytes`
    /// trigger requires [`PipelineBuilder::batch_bytes`]. Violations are
    /// reported as [`Error::Config`].
    pub fn batch(
        mut self,
        policy: BatchPolicy,
    ) -> PipelineBuilder<Batch<T>, S, impl Assemble<Batch<T>, S::Item>> {
        let id = self.take_stage_id("batch");
        self.check_batch(&policy, id, false);
        let stage = BatchStage::<T>::new(&policy, None);
        #[cfg(feature = "std")]
        let stage = stage.with_clock(&policy, Box::new(crate::clock::SystemClock));
        self.stage_as(stage, id, false)
    }

    /// Group items into [`Batch`]es by count, byte size, and/or age.
    pub fn batch_bytes(
        mut self,
        policy: BatchPolicy,
    ) -> PipelineBuilder<Batch<T>, S, impl Assemble<Batch<T>, S::Item>>
    where
        T: ByteSize,
    {
        let id = self.take_stage_id("batch");
        self.check_batch(&policy, id, true);
        let stage = BatchStage::<T>::new(&policy, Some(<T as ByteSize>::byte_size as Measure<T>));
        #[cfg(feature = "std")]
        let stage = stage.with_clock(&policy, Box::new(crate::clock::SystemClock));
        self.stage_as(stage, id, false)
    }

    /// [`PipelineBuilder::batch`] with an explicit [`crate::Clock`] for
    /// the age trigger (deterministic tests, simulated time).
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn batch_with<C>(
        mut self,
        policy: BatchPolicy,
        clock: C,
    ) -> PipelineBuilder<Batch<T>, S, impl Assemble<Batch<T>, S::Item>>
    where
        C: crate::clock::Clock + 'static,
    {
        let id = self.take_stage_id("batch");
        self.check_batch(&policy, id, false);
        let stage = BatchStage::<T>::new(&policy, None).with_clock(&policy, Box::new(clock));
        self.stage_as(stage, id, false)
    }

    /// [`PipelineBuilder::batch_bytes`] with an explicit [`crate::Clock`].
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn batch_bytes_with<C>(
        mut self,
        policy: BatchPolicy,
        clock: C,
    ) -> PipelineBuilder<Batch<T>, S, impl Assemble<Batch<T>, S::Item>>
    where
        T: ByteSize,
        C: crate::clock::Clock + 'static,
    {
        let id = self.take_stage_id("batch");
        self.check_batch(&policy, id, true);
        let stage = BatchStage::<T>::new(&policy, Some(<T as ByteSize>::byte_size as Measure<T>))
            .with_clock(&policy, Box::new(clock));
        self.stage_as(stage, id, false)
    }

    fn check_batch(&mut self, policy: &BatchPolicy, id: StageId, byte_aware: bool) {
        if let Err(kind) = policy.check() {
            self.record(ConfigError::new(id, kind));
        } else if !byte_aware && policy.bytes_limit().is_some() {
            self.record(ConfigError::new(id, ConfigErrorKind::BytesRequireByteSize));
        }
    }

    // -- windowing -----------------------------------------------------------

    /// Group items into time [`crate::Window`]s using the system clock.
    /// Accepts a [`crate::WindowPolicy`] or a [`crate::WindowSpec`].
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn window<W>(
        self,
        spec: W,
    ) -> PipelineBuilder<
        crate::window::Window<T>,
        S,
        impl Assemble<crate::window::Window<T>, S::Item>,
    >
    where
        T: Clone,
        W: Into<crate::window::WindowSpec>,
    {
        self.window_with(spec, crate::clock::SystemClock)
    }

    /// [`PipelineBuilder::window`] with an explicit [`crate::Clock`].
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn window_with<W, C>(
        mut self,
        spec: W,
        clock: C,
    ) -> PipelineBuilder<
        crate::window::Window<T>,
        S,
        impl Assemble<crate::window::Window<T>, S::Item>,
    >
    where
        T: Clone,
        W: Into<crate::window::WindowSpec>,
        C: crate::clock::Clock + 'static,
    {
        let spec = spec.into();
        let id = self.take_stage_id("window");
        if let Err(kind) = spec.check() {
            self.record(ConfigError::new(id, kind));
        }
        self.stage_as(crate::window::WindowStage::new(spec, clock), id, false)
    }

    /// Group items into time windows per key, emitting `(key, window)`.
    /// Windows of all keys share one time grid.
    ///
    /// Every item (of any key) and every idle tick first emits the expired
    /// windows of *all* keys, in order of their end, so a key that goes
    /// quiet on a busy stream still has its window emitted on time. Keys
    /// with nothing left buffered are released. The cost is O(1) per item
    /// when nothing expires and O(log keys) per emitted window.
    ///
    /// Bound the number of keys with [`crate::WindowSpec::max_keys`]: when
    /// a new key arrives at the limit, the least recently active key is
    /// flushed and evicted in O(1), so high-cardinality (or adversarial)
    /// keys cost the same per item whatever the limit. Without a limit,
    /// state is kept for every key with an open window.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    /// use pipe_io::{Pipeline, WindowPolicy, sink::VecSink};
    ///
    /// let sink = VecSink::new();
    /// let out = sink.handle();
    /// Pipeline::from_iter([("a", 1), ("b", 2), ("a", 3)])
    ///     .window_by_key(|e: &(&str, i32)| e.0, WindowPolicy::tumbling(Duration::from_secs(60)))
    ///     .sink(sink)
    ///     .run()?;
    /// let windows = out.take();
    /// assert_eq!(windows.len(), 2);
    /// assert_eq!(windows[0].0, "a");
    /// assert_eq!(windows[0].1.len(), 2);
    /// # Ok::<(), pipe_io::Error>(())
    /// ```
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[allow(clippy::type_complexity)] // the opaque builder type has to be spelled out once
    pub fn window_by_key<K, F, W>(
        self,
        key: F,
        spec: W,
    ) -> PipelineBuilder<
        (K, crate::window::Window<T>),
        S,
        impl Assemble<(K, crate::window::Window<T>), S::Item>,
    >
    where
        T: Clone,
        K: core::hash::Hash + Eq + Clone + Send + 'static,
        F: FnMut(&T) -> K + Send + 'static,
        W: Into<crate::window::WindowSpec>,
    {
        self.window_by_key_with(key, spec, crate::clock::SystemClock)
    }

    /// [`PipelineBuilder::window_by_key`] with an explicit
    /// [`crate::Clock`].
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[allow(clippy::type_complexity)] // the opaque builder type has to be spelled out once
    pub fn window_by_key_with<K, F, W, C>(
        mut self,
        key: F,
        spec: W,
        clock: C,
    ) -> PipelineBuilder<
        (K, crate::window::Window<T>),
        S,
        impl Assemble<(K, crate::window::Window<T>), S::Item>,
    >
    where
        T: Clone,
        K: core::hash::Hash + Eq + Clone + Send + 'static,
        F: FnMut(&T) -> K + Send + 'static,
        W: Into<crate::window::WindowSpec>,
        C: crate::clock::Clock + 'static,
    {
        let spec = spec.into();
        let id = self.take_stage_id("window");
        if let Err(kind) = spec.check() {
            self.record(ConfigError::new(id, kind));
        }
        self.stage_as(
            crate::window::KeyedWindowStage::new(spec, key, clock),
            id,
            false,
        )
    }

    // -- threading -----------------------------------------------------------

    /// Insert a bounded edge holding at most `capacity` items.
    ///
    /// Under [`Pipeline::run_threaded`] everything downstream of the edge
    /// runs on its own thread, fed through a bounded channel of this
    /// capacity: when it is full, the upstream thread blocks until the
    /// downstream side catches up. Items keep their order. Memory for
    /// queued items is allocated as they arrive (in small blocks) and
    /// released as they are consumed; nothing is reserved up front, so the
    /// capacity costs only what is actually in flight.
    ///
    /// A capacity of 0 makes every hand-off a rendezvous: the upstream
    /// thread waits until the downstream thread has taken each item. That
    /// keeps the two threads in lock step and costs a thread wake-up per
    /// item (roughly 10x slower than a capacity of 64 or more); prefer a
    /// small non-zero capacity unless lock step is the goal.
    ///
    /// `capacity` must not exceed 1,048,576 (2^20); a larger value is
    /// reported as [`ConfigErrorKind::LimitTooLarge`] (through
    /// [`PipelineBuilder::try_sink`] or the run), because an edge that
    /// large no longer applies meaningful backpressure.
    ///
    /// Under single-threaded execution ([`Pipeline::run`],
    /// [`Pipeline::step`]) the edge is a direct call: there is nothing to
    /// buffer because each item travels to the sink before the next one
    /// is pulled.
    ///
    /// # Example
    ///
    /// ```
    /// use pipe_io::{Pipeline, sink::VecSink};
    ///
    /// let sink = VecSink::new();
    /// let out = sink.handle();
    /// Pipeline::from_iter(0..1_000)
    ///     .map(|n: u32| n * 2)          // thread 1 (with the source)
    ///     .buffer(64)
    ///     .filter(|n: &u32| n % 3 == 0) // thread 2
    ///     .buffer(64)
    ///     .sink(sink)                   // thread 3
    ///     .run_threaded()?;
    /// assert_eq!(out.take().len(), 334);
    /// # Ok::<(), pipe_io::Error>(())
    /// ```
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn buffer(mut self, capacity: usize) -> PipelineBuilder<T, S, impl Assemble<T, S::Item>> {
        let id = self.take_stage_id("buffer");
        if capacity > crate::edge::MAX_CAPACITY {
            self.record(ConfigError::new(
                id,
                ConfigErrorKind::LimitTooLarge {
                    field: "capacity",
                    max: crate::edge::MAX_CAPACITY as u64,
                },
            ));
        }
        self.link(
            move |next: BoxedStageFn<T>, wiring: &mut Wiring| -> BoxedStageFn<T> {
                match wiring.threaded.as_mut() {
                    Some(threaded) => threaded.split(next, capacity, id),
                    None => next,
                }
            },
        )
    }

    // -- termination -----------------------------------------------------------

    /// Terminate the pipeline with a [`Sink`].
    ///
    /// Always returns a [`Pipeline`]; if the builder recorded a
    /// configuration error, every way of running it returns
    /// [`Error::Config`] without touching the source or the sink. Use
    /// [`PipelineBuilder::try_sink`] to get the error immediately.
    pub fn sink<Sk>(mut self, sink: Sk) -> Pipeline<S>
    where
        Sk: Sink<Item = T> + Send + 'static,
    {
        let id = self.take_stage_id("sink");
        if let Some(user) = self.dead_letter_user {
            if self.dead_letter.is_none() {
                self.record(ConfigError::new(
                    user,
                    ConfigErrorKind::DeadLetterSinkMissing,
                ));
            }
        }
        let tail = sink_fn(sink, id);
        let finalize = self.finalize;
        let assembly: Assembly<S::Item> =
            Box::new(move |wiring: &mut Wiring| finalize.assemble(tail, wiring));
        Pipeline {
            source_id: self.source_id,
            idle_tick: self.idle_tick,
            config_error: self.config_error,
            cancel: CancelSlot::default(),
            in_call: false,
            state: State::Built(Built {
                source: self.source,
                assembly,
                dead_letter: self.dead_letter,
                observer: self.observer,
            }),
        }
    }

    /// Like [`PipelineBuilder::sink`], but returns the first recorded
    /// configuration error instead of a pipeline that would refuse to run.
    ///
    /// # Errors
    ///
    /// The first [`ConfigError`] recorded while building.
    ///
    /// # Example
    ///
    /// ```
    /// use pipe_io::{BatchPolicy, ConfigErrorKind, Pipeline, sink::NullSink};
    ///
    /// let err = Pipeline::from_iter(0..10)
    ///     .batch(BatchPolicy::new().max_items(0))
    ///     .try_sink(NullSink::new())
    ///     .unwrap_err();
    /// assert_eq!(err.kind(), ConfigErrorKind::ZeroLimit { field: "max_items" });
    /// assert_eq!(err.stage().as_str(), "batch");
    /// ```
    pub fn try_sink<Sk>(self, sink: Sk) -> core::result::Result<Pipeline<S>, ConfigError>
    where
        Sk: Sink<Item = T> + Send + 'static,
    {
        let pipeline = self.sink(sink);
        match pipeline.config_error {
            Some(e) => Err(e),
            None => Ok(pipeline),
        }
    }
}

/// Sleep for `delay` in short slices, returning early (with `false`) once
/// the run is cancelled.
#[cfg(feature = "std")]
fn sleep_unless_cancelled(delay: Duration, cancel: &CancelSlot) -> bool {
    const SLICE: Duration = Duration::from_millis(10);
    let deadline = std::time::Instant::now().checked_add(delay);
    loop {
        if cancel.is_cancelled() {
            return false;
        }
        let left = match deadline {
            Some(deadline) => deadline.saturating_duration_since(std::time::Instant::now()),
            None => SLICE,
        };
        if left.is_zero() {
            return true;
        }
        std::thread::sleep(if cancel.is_armed() {
            left.min(SLICE)
        } else {
            left
        });
    }
}

/// Keep the failing item only when it is going to the dead-letter sink.
fn keep_input<T: Send + Sync + 'static>(
    policy: ErrorPolicy,
    item: T,
) -> Option<Box<dyn core::any::Any + Send + Sync>> {
    match policy {
        ErrorPolicy::DeadLetter => Some(Box::new(item)),
        ErrorPolicy::FailFast | ErrorPolicy::Continue => None,
    }
}

/// The closure at the tail of every chain.
fn sink_fn<T, Sk>(mut sink: Sk, id: StageId) -> BoxedStageFn<T>
where
    Sk: Sink<Item = T> + Send + 'static,
{
    let fail = move |e: Sk::Error| {
        Halt::from(Error::Sink {
            stage: id,
            source: e.into(),
        })
    };
    // `flush` and `close` are each called at most once per run, even when
    // one of them fails or panics and the run is then aborted: the flags
    // are set before the call, so the abort skips what was already tried.
    let mut flushed = false;
    let mut closed = false;
    Box::new(move |op, ctx| match op {
        StageOp::Process(item) => {
            sink.write(item).map_err(fail)?;
            Ok(1)
        }
        StageOp::Tick => Ok(0),
        StageOp::Flush if !flushed => {
            flushed = true;
            sink.flush().map(|()| 0).map_err(fail)
        }
        StageOp::Close if !closed => {
            closed = true;
            sink.close().map(|()| 0).map_err(fail)
        }
        StageOp::Flush | StageOp::Close => Ok(0),
        StageOp::Abort => {
            let flush_result = if flushed {
                Ok(())
            } else {
                flushed = true;
                sink.flush().map_err(fail)
            };
            let close_result = if closed {
                Ok(())
            } else {
                closed = true;
                sink.close().map_err(fail)
            };
            match (flush_result, close_result) {
                (Err(first), Err(Halt::Failed(second))) => {
                    ctx.observer.emit(&Event::TeardownError { error: &second });
                    Err(first)
                }
                (Err(e), _) | (Ok(()), Err(e)) => Err(e),
                (Ok(()), Ok(())) => Ok(0),
            }
        }
    })
}
