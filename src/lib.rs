//! Typed source-transform-sink pipelines with backpressure, batching, and
//! per-stage error isolation. A lightweight runtime-agnostic stream processor
//! for in-process workloads. The missing middle ground between raw iterators
//! and full distributed stream processing.
//!
//! # Quick start
//!
//! ```
//! use pipe_io::{Pipeline, sink::VecSink};
//!
//! let sink = VecSink::<i64>::new();
//! let handle = sink.handle();
//!
//! Pipeline::from_iter(1..=5)
//!     .map(|n: i32| i64::from(n) * 10)
//!     .filter(|n: &i64| *n > 20)
//!     .sink(sink)
//!     .run()
//!     .expect("pipeline run");
//!
//! assert_eq!(handle.take(), vec![30, 40, 50]);
//! ```
//!
//! # Features
//!
//! * `std` (default): enables [`source::ChannelSource`],
//!   [`source::ReaderSource`], [`sink::ChannelSink`],
//!   [`sink::WriterSink`], and [`driver::ThreadedDriver`]. With `std`
//!   disabled the crate compiles in `no_std` mode (requires `alloc`)
//!   with the data primitives, closure-based adapters, batching, and
//!   [`driver::SyncDriver`] available.
//!
//! # Design notes
//!
//! Sources are pull-based (`Source::pull`). Stages receive items one at
//! a time and emit zero or more outputs via an [`Emit`] callback handed
//! in by the driver. Sinks are push-based (`Sink::write`). The
//! builder is fully typed; the carrier type of the pipeline is tracked
//! at compile time across every stage transition. See `REPS.md` for the
//! binding API contract and `.dev/DESIGN.md` for design notes.

#![doc(html_root_url = "https://docs.rs/pipe-io")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

extern crate alloc;

/// Crate version string, populated by Cargo at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod batch;
pub mod driver;
pub mod emit;
pub mod error;
pub mod sink;
pub mod source;
pub mod stage;
#[cfg(feature = "std")]
pub mod window;

mod pipeline;
mod stage_id;

pub use crate::batch::{Batch, BatchPolicy, ByteSize};
pub use crate::driver::{Driver, RunStats};
pub use crate::emit::{Emit, EmitError};
pub use crate::error::{BoxError, Error, ErrorPolicy, Result, StageError, StageFailure};
pub use crate::pipeline::{Pipeline, PipelineBuilder};
pub use crate::sink::Sink;
pub use crate::source::Source;
pub use crate::stage::Stage;
pub use crate::stage_id::StageId;
#[cfg(feature = "std")]
pub use crate::window::{Clock, SystemClock, Window, WindowPolicy};
