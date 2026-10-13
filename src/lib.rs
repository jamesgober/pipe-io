//! Typed source-transform-sink pipelines with bounded stage edges,
//! batching, windowing, and per-stage error isolation.
//!
//! `pipe-io` is an in-process stream processor: the middle ground between
//! raw iterators and distributed stream processing. It has no runtime
//! dependencies, contains no `unsafe` code, and its core (sources, stages,
//! sinks, batching, error policies, the single-threaded driver) works
//! under `no_std` with `alloc`.
//!
//! # Quick start
//!
//! ```
//! # #[cfg(feature = "std")]
//! # fn main() -> Result<(), pipe_io::Error> {
//! use pipe_io::{Pipeline, sink::VecSink};
//!
//! let sink = VecSink::new();
//! let out = sink.handle();
//!
//! let stats = Pipeline::from_iter(1..=5)
//!     .map(|n: i32| i64::from(n) * 10)
//!     .filter(|n: &i64| *n > 20)
//!     .sink(sink)
//!     .run()?;
//!
//! assert_eq!(out.take(), vec![30, 40, 50]);
//! assert_eq!(stats.items_out, 3);
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "std"))]
//! # fn main() {}
//! ```
//!
//! # Concepts
//!
//! * A [`Source`] is pulled for items; a [`Stage`] turns each input into
//!   zero or more outputs through an [`Emit`] handle; a [`Sink`] consumes
//!   the results. The builder ([`PipelineBuilder`]) tracks the item type
//!   across every step at compile time.
//! * Fallible stages follow an [`ErrorPolicy`]: stop the run, skip the
//!   item, or route it to a dead-letter sink as a [`StageFailure`].
//! * [`PipelineBuilder::batch`] and [`PipelineBuilder::window`] group
//!   items by count, bytes, or time, with explicit memory limits.
//! * [`Pipeline::run`] executes on the calling thread.
//!   [`Pipeline::run_threaded`] runs each segment between
//!   [`PipelineBuilder::buffer`] edges on its own thread, connected by
//!   bounded channels that apply backpressure. [`Pipeline::step`] lets
//!   any executor drive a pipeline one item at a time.
//! * Every run flushes and closes the sink, the dead-letter sink, and the
//!   source on every exit path, including errors and panics.
//!
//! Design notes are in `docs/ARCHITECTURE.md`; a walkthrough is in
//! `docs/GUIDE.md`.
//!
//! # Features
//!
//! * `std` (default): threads ([`driver::ThreadedDriver`],
//!   [`PipelineBuilder::buffer`]), time ([`Clock`], windowing, batch age
//!   triggers, retry backoff), and the std adapters
//!   ([`source::ChannelSource`], [`source::ReaderSource`],
//!   [`sink::ChannelSink`], [`sink::WriterSink`], [`sink::VecSink`]).
//!   Without it the crate is `no_std` + `alloc`.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
#![deny(unused_results)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unreachable)]
#![deny(clippy::undocumented_unsafe_blocks)]
#![deny(clippy::missing_safety_doc)]

extern crate alloc;

/// Crate version string, populated by Cargo at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod batch;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod clock;
pub mod driver;
pub mod emit;
pub mod error;
pub mod observe;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod retry;
mod runtime;
pub mod sink;
pub mod source;
pub mod stage;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod window;

#[cfg(feature = "std")]
mod edge;
mod pipeline;
mod stage_id;
#[cfg(feature = "std")]
mod threaded;

pub use crate::batch::{Batch, BatchPolicy, ByteSize};
#[cfg(feature = "std")]
pub use crate::clock::{Clock, ManualClock, SystemClock};
pub use crate::driver::{Driver, RunStats};
pub use crate::emit::{Emit, EmitError};
pub use crate::error::{
    BoxError, ConfigError, ConfigErrorKind, Error, ErrorPolicy, Result, StageError, StageFailure,
};
pub use crate::observe::{Event, Observer};
#[cfg(target_has_atomic = "ptr")]
pub use crate::pipeline::CancelHandle;
pub use crate::pipeline::{Assemble, Pipeline, PipelineBuilder, Step};
#[cfg(feature = "std")]
pub use crate::retry::{Backoff, RetryPolicy};
pub use crate::sink::Sink;
pub use crate::source::{Pull, Source};
pub use crate::stage::Stage;
pub use crate::stage_id::StageId;
#[cfg(feature = "std")]
pub use crate::window::{Window, WindowPolicy, WindowSpec};

// Compile and run the code in the README and the user guide as doctests,
// so the documentation cannot drift from the API.
#[cfg(all(doctest, feature = "std"))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[cfg(all(doctest, feature = "std"))]
#[doc = include_str!("../docs/GUIDE.md")]
struct GuideDoctests;
