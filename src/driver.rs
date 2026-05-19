//! Pipeline execution drivers.
//!
//! [`SyncDriver`] runs the pipeline single-threaded on the calling
//! thread. [`ThreadedDriver`] (under `std`) drives the pipeline on a
//! background OS thread. Custom executors can implement the [`Driver`]
//! trait and be selected at run time via
//! [`crate::Pipeline::run_with`].
//!
//! The [`Driver`] trait carries the stricter `Send` bound (matching
//! [`ThreadedDriver`]). The inherent `SyncDriver::run` method keeps the
//! looser bound for callers that drive non-`Send` sources on the
//! current thread.

#[cfg(feature = "std")]
use crate::error::Error;
use crate::error::Result;
use crate::pipeline::Pipeline;
use crate::source::Source;

#[cfg(feature = "std")]
use core::time::Duration;

/// Statistics returned by a successful pipeline run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RunStats {
    /// Number of items pulled from the source.
    pub items_in: u64,
    /// Wall-clock duration of the run. Always `Duration::ZERO` under
    /// `no_std` (no monotonic clock available).
    #[cfg(feature = "std")]
    pub duration: Duration,
}

/// Generic executor for built pipelines.
///
/// Implement for custom executors (tokio's runtime, a rayon thread
/// pool, a sharded worker farm, etc.). Built-in implementations are
/// [`SyncDriver`] and [`ThreadedDriver`].
///
/// The trait requires every part of the pipeline to be `Send`: the
/// source, its item type, and its error type. This matches
/// [`ThreadedDriver`]'s natural bounds and lets a custom executor
/// move a pipeline to another thread without extra constraints. If
/// you need to drive a non-`Send` source on the calling thread, use
/// [`SyncDriver::run`] (the inherent method) directly; that path
/// keeps the looser bound.
///
/// # Example
///
/// ```
/// use pipe_io::driver::{Driver, RunStats, SyncDriver};
/// use pipe_io::{sink::NullSink, Pipeline, Result};
///
/// fn run_anything<D: Driver>(driver: D) -> Result<RunStats> {
///     let pipeline = Pipeline::from_iter(0..5).sink(NullSink::<i32>::new());
///     driver.run(pipeline)
/// }
///
/// run_anything(SyncDriver::new()).unwrap();
/// ```
pub trait Driver {
    /// Drive a pipeline to completion.
    ///
    /// # Errors
    ///
    /// Returns the first error produced by the source, any stage, or
    /// the sink.
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static;
}

/// Single-threaded pipeline driver.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncDriver;

impl SyncDriver {
    /// Construct a new sync driver.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Drive a pipeline to completion on the calling thread.
    ///
    /// This inherent method carries a looser bound than the
    /// [`Driver`] trait impl: the source and its item/error types do
    /// not need to be `Send`. Use this method directly when the
    /// pipeline cannot satisfy `Send` (for example, a source holding
    /// an `Rc`). When the `Send` bounds hold, the trait impl and the
    /// inherent method behave identically.
    ///
    /// # Errors
    ///
    /// Returns the first error produced by the source, any stage, or
    /// the sink.
    pub fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + 'static,
        S::Item: 'static,
        S::Error: 'static,
    {
        crate::pipeline::run_sync(pipeline)
    }
}

impl Driver for SyncDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        crate::pipeline::run_sync(pipeline)
    }
}

/// Background-thread pipeline driver. Spawns a single OS thread that
/// runs the pipeline; the calling thread blocks on join.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadedDriver;

#[cfg(feature = "std")]
impl ThreadedDriver {
    /// Construct a new threaded driver.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Drive a pipeline to completion on a spawned thread.
    ///
    /// # Errors
    ///
    /// Returns the first error produced by the source, any stage, or
    /// the sink. Returns [`Error::Cancelled`] if the worker thread
    /// panics.
    pub fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        let handle = std::thread::spawn(move || crate::pipeline::run_sync(pipeline));
        match handle.join() {
            Ok(result) => result,
            Err(_) => Err(Error::Cancelled),
        }
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
