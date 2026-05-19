//! Pipeline execution drivers.
//!
//! [`SyncDriver`] runs the pipeline single-threaded on the calling
//! thread and is `no_std`-compatible. [`ThreadedDriver`] (under `std`)
//! drives the pipeline on a background OS thread.
//!
//! A unified `Driver` trait is deferred past `0.3.x`: `SyncDriver` and
//! `ThreadedDriver` differ in their `Send` bound requirements, and a
//! single trait surface would force the stricter bound on both.
//! Consumers select a driver by calling [`crate::Pipeline::run`] (sync)
//! or [`crate::Pipeline::run_threaded`] (std).

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
