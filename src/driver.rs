//! Pipeline execution drivers.
//!
//! * [`SyncDriver`] runs the whole pipeline on the calling thread. Every
//!   item travels from the source to the sink before the next one is
//!   pulled, so the only memory in flight is what stages buffer
//!   themselves (open batches and windows).
//! * [`ThreadedDriver`] (`std`) runs each segment between
//!   [`crate::PipelineBuilder::buffer`] edges on its own thread. Segments
//!   are connected by bounded channels: a full edge blocks the upstream
//!   thread, which is how backpressure propagates back to the source.
//! * Custom executors implement [`Driver`]. A driver can hand the whole
//!   run to [`crate::Pipeline::run`] on a thread of its choosing, or drive
//!   it item by item with [`crate::Pipeline::step`],
//!   [`crate::Pipeline::tick`], and [`crate::Pipeline::finish`], which is
//!   what lets a driver interleave pipelines, enforce budgets, observe
//!   progress, or cancel between items. See `examples/tokio_driver.rs`
//!   and `examples/rayon_driver.rs`.

use core::time::Duration;

use crate::error::Result;
use crate::pipeline::Pipeline;
use crate::source::Source;

/// Counters returned by a run (and by [`Pipeline::stats`] while it runs).
///
/// The struct is `#[non_exhaustive]`: read the fields, and use
/// `RunStats::default()` plus field assignment if you need to build one.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunStats {
    /// Items pulled from the source.
    pub items_in: u64,
    /// Items accepted by the sink (`write` returned `Ok`).
    pub items_out: u64,
    /// Stage errors, under every [`crate::ErrorPolicy`] (a `FailFast`
    /// error counts once before it stops the run).
    pub errors: u64,
    /// Failures written to the dead-letter sink.
    pub dead_lettered: u64,
    /// Retry attempts made by `try_map_retry` stages.
    pub retries: u64,
    /// Wall-clock time from the start of the run to its end. Always
    /// [`Duration::ZERO`] without the `std` feature, which has no clock.
    pub duration: Duration,
}

/// Executor for built pipelines.
///
/// The trait requires the source and its items to be `Send` so an
/// implementation may move the pipeline to another thread. To drive a
/// pipeline whose source is not `Send`, call [`Pipeline::run`] or
/// [`Pipeline::step`] directly on the current thread.
///
/// # Example
///
/// A driver that runs the pipeline in bounded slices and reports progress
/// between them:
///
/// ```
/// use pipe_io::driver::{Driver, RunStats};
/// use pipe_io::{Pipeline, Result, Source, Step, sink::NullSink};
///
/// struct Sliced {
///     slice: usize,
/// }
///
/// impl Driver for Sliced {
///     fn run<S>(self, mut pipeline: Pipeline<S>) -> Result<RunStats>
///     where
///         S: Source + Send + 'static,
///         S::Item: Send + 'static,
///         S::Error: Send + 'static,
///     {
///         loop {
///             for _ in 0..self.slice {
///                 if pipeline.step()? == Step::Done {
///                     return Ok(pipeline.stats());
///                 }
///             }
///             // Between slices: yield, check a deadline, report progress...
///         }
///     }
/// }
///
/// let stats = Pipeline::from_iter(0..10).sink(NullSink::new()).run_with(Sliced { slice: 3 })?;
/// assert_eq!(stats.items_out, 10);
/// # Ok::<(), pipe_io::Error>(())
/// ```
pub trait Driver {
    /// Drive `pipeline` to completion.
    ///
    /// # Errors
    ///
    /// The run's first error (see [`Pipeline::run`]) or a driver-specific
    /// failure.
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static;
}

/// Runs the pipeline on the calling thread.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SyncDriver;

impl SyncDriver {
    /// Construct a sync driver.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Drive a pipeline on the calling thread. Unlike the [`Driver`] impl,
    /// the source does not need to be `Send`.
    ///
    /// # Errors
    ///
    /// See [`Pipeline::run`].
    pub fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + 'static,
        S::Item: Send + 'static,
    {
        pipeline.run_sync()
    }
}

impl Driver for SyncDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        pipeline.run_sync()
    }
}

/// Runs each segment between `buffer` edges on its own OS thread.
///
/// # Semantics
///
/// * **Threads**: one per segment. The first segment (source plus the
///   stages before the first `buffer`) runs on a new thread too; the
///   calling thread only waits. A pipeline without `buffer` edges runs on
///   a single worker thread.
/// * **Ordering**: each edge is a FIFO channel with one producer and one
///   consumer, so items reach the sink in source order.
/// * **Backpressure**: an edge holds at most its capacity; a full edge
///   blocks the upstream thread.
/// * **Errors**: the first error in time wins. The failing thread stops,
///   its downstream segments receive an abort (the sink is flushed and
///   closed, stage buffers are discarded), and its upstream segments see
///   the edge close and stop pulling. Later errors are reported to the
///   observer.
/// * **Panics**: a panic tears the pipeline down the same way; once every
///   thread has been joined the original panic payload is re-raised on
///   the calling thread.
/// * **Cancellation**: checked by the source thread between pulls; the
///   pipeline then drains downstream gracefully and the run returns
///   [`crate::Error::Cancelled`].
/// * **Idle tick**: with [`crate::PipelineBuilder::idle_tick`] each
///   segment thread ticks its own stages after the interval without input.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ThreadedDriver;

#[cfg(feature = "std")]
impl ThreadedDriver {
    /// Construct a threaded driver.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Drive a pipeline with worker threads. Same as the [`Driver`] impl;
    /// provided so callers do not need the trait in scope.
    ///
    /// # Errors
    ///
    /// See [`Pipeline::run_threaded`].
    pub fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
    {
        crate::threaded::run(pipeline)
    }
}

#[cfg(feature = "std")]
impl Driver for ThreadedDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        Self::run(self, pipeline)
    }
}
