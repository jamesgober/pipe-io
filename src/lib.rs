//! Typed source-transform-sink pipelines with backpressure, batching, windowing, and per-stage error isolation. A lightweight runtime-agnostic stream processor for in-process workloads. The missing middle ground between raw iterators and full distributed stream processing.
//!
//! # Status
//!
//! This crate is in early scaffolding. The public API is not yet
//! defined. See [the repository](https://github.com/jamesgober/pipe-io)
//! and `.dev/ROADMAP.md` for the milestone plan.

#![doc(html_root_url = "https://docs.rs/pipe-io")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// Crate version string, populated by Cargo at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
