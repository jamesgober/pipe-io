//! Error model for `pipe-io`.
//!
//! The crate uses a single top-level [`Error`] type that wraps the boxed
//! source error from any stage. Stage-supplied errors only need to
//! satisfy [`StageError`] (`Debug + Display + Send + Sync + 'static`),
//! which every `std::error::Error` type already does via the blanket
//! impl below. The `core::error::Error` super-trait was avoided so the
//! error model works at the crate's MSRV of 1.75.

use alloc::boxed::Box;
use core::fmt;

use crate::StageId;

/// Crate result alias.
pub type Result<T> = core::result::Result<T, Error>;

/// Marker trait for error types that stages can produce. Blanket-
/// implemented for every `T: Debug + Display + Send + Sync + 'static`,
/// which includes every `std::error::Error` implementor.
pub trait StageError: fmt::Debug + fmt::Display + Send + Sync + 'static {}

impl<T> StageError for T where T: fmt::Debug + fmt::Display + Send + Sync + 'static {}

/// Boxed stage error. Carried by [`Error`] variants.
pub type BoxError = Box<dyn StageError>;

/// Kind of buffer-related failure surfaced by a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BufferErrorKind {
    /// The downstream buffer was full and could not accept the item.
    Full,
    /// The buffer was closed (typically because a downstream stage
    /// terminated unexpectedly).
    Closed,
}

impl fmt::Display for BufferErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("buffer full"),
            Self::Closed => f.write_str("buffer closed"),
        }
    }
}

/// Top-level error type returned by every pipeline operation.
///
/// Variants carry the [`StageId`] that produced them, and (where
/// applicable) the underlying boxed error.
#[non_exhaustive]
pub enum Error {
    /// A source produced an error or could not produce an item.
    Source {
        /// Stage identifier of the failing source.
        stage: StageId,
        /// Boxed source error.
        source: BoxError,
    },
    /// A transform stage produced an error.
    Stage {
        /// Stage identifier of the failing stage.
        stage: StageId,
        /// Boxed stage error.
        source: BoxError,
    },
    /// A sink produced an error while writing or flushing.
    Sink {
        /// Stage identifier of the failing sink.
        stage: StageId,
        /// Boxed sink error.
        source: BoxError,
    },
    /// An inter-stage buffer failed.
    Buffer {
        /// Stage identifier of the affected stage.
        stage: StageId,
        /// Kind of buffer failure.
        kind: BufferErrorKind,
    },
    /// The pipeline was cancelled before completion.
    Cancelled,
    /// The pipeline was already closed when an operation was attempted.
    Closed,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { stage, source } => f
                .debug_struct("Source")
                .field("stage", stage)
                .field("source", &format_args!("{source}"))
                .finish(),
            Self::Stage { stage, source } => f
                .debug_struct("Stage")
                .field("stage", stage)
                .field("source", &format_args!("{source}"))
                .finish(),
            Self::Sink { stage, source } => f
                .debug_struct("Sink")
                .field("stage", stage)
                .field("source", &format_args!("{source}"))
                .finish(),
            Self::Buffer { stage, kind } => f
                .debug_struct("Buffer")
                .field("stage", stage)
                .field("kind", kind)
                .finish(),
            Self::Cancelled => f.write_str("Cancelled"),
            Self::Closed => f.write_str("Closed"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { stage, source } => write!(f, "source `{stage}`: {source}"),
            Self::Stage { stage, source } => write!(f, "stage `{stage}`: {source}"),
            Self::Sink { stage, source } => write!(f, "sink `{stage}`: {source}"),
            Self::Buffer { stage, kind } => write!(f, "buffer at `{stage}`: {kind}"),
            Self::Cancelled => f.write_str("pipeline cancelled"),
            Self::Closed => f.write_str("pipeline closed"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

impl Error {
    /// Stage identifier associated with this error, if any.
    #[must_use]
    pub fn stage(&self) -> Option<StageId> {
        match self {
            Self::Source { stage, .. }
            | Self::Stage { stage, .. }
            | Self::Sink { stage, .. }
            | Self::Buffer { stage, .. } => Some(*stage),
            Self::Cancelled | Self::Closed => None,
        }
    }
}

/// Per-stage policy for how errors propagate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ErrorPolicy {
    /// The first stage error cancels the entire pipeline and bubbles
    /// out of [`crate::Pipeline::run`]. Default.
    #[default]
    FailFast,
    /// Stage errors are dropped along with the failing item; downstream
    /// continues to receive subsequent items.
    Continue,
    /// Stage errors are routed as [`StageFailure`] records to a
    /// dead-letter sink installed via
    /// [`crate::PipelineBuilder::dead_letter`]. If no dead-letter sink
    /// is installed the failing record is silently dropped (same as
    /// [`ErrorPolicy::Continue`]).
    DeadLetter,
}

/// Record describing a per-item stage failure, routed to the
/// dead-letter sink when [`ErrorPolicy::DeadLetter`] is active.
///
/// The struct is `#[non_exhaustive]`; future releases may add fields
/// (for example, a type-erased copy of the failing input) without
/// breaking SemVer.
#[non_exhaustive]
pub struct StageFailure {
    /// Stage that failed.
    pub stage: StageId,
    /// Underlying error.
    pub source: BoxError,
}

impl StageFailure {
    /// Construct a new stage failure.
    #[must_use]
    pub fn new(stage: StageId, source: BoxError) -> Self {
        Self { stage, source }
    }
}

impl fmt::Debug for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageFailure")
            .field("stage", &self.stage)
            .field("source", &format_args!("{}", self.source))
            .finish()
    }
}

impl fmt::Display for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stage `{}`: {}", self.stage, self.source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::string::ToString;

    #[derive(Debug)]
    struct Boom;
    impl fmt::Display for Boom {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("boom")
        }
    }

    #[test]
    fn display_source_error() {
        let e = Error::Source {
            stage: StageId::new("ingest"),
            source: Box::new(Boom),
        };
        assert_eq!(e.to_string(), "source `ingest`: boom");
    }

    #[test]
    fn display_stage_error() {
        let e = Error::Stage {
            stage: StageId::new("parse"),
            source: Box::new(Boom),
        };
        assert_eq!(e.to_string(), "stage `parse`: boom");
    }

    #[test]
    fn display_sink_error() {
        let e = Error::Sink {
            stage: StageId::new("writer"),
            source: Box::new(Boom),
        };
        assert_eq!(e.to_string(), "sink `writer`: boom");
    }

    #[test]
    fn display_buffer_error_kinds() {
        let full = Error::Buffer {
            stage: StageId::new("edge"),
            kind: BufferErrorKind::Full,
        };
        let closed = Error::Buffer {
            stage: StageId::new("edge"),
            kind: BufferErrorKind::Closed,
        };
        assert_eq!(full.to_string(), "buffer at `edge`: buffer full");
        assert_eq!(closed.to_string(), "buffer at `edge`: buffer closed");
    }

    #[test]
    fn display_terminal_variants() {
        assert_eq!(Error::Cancelled.to_string(), "pipeline cancelled");
        assert_eq!(Error::Closed.to_string(), "pipeline closed");
    }

    #[test]
    fn error_stage_extraction() {
        let e = Error::Stage {
            stage: StageId::new("middle"),
            source: Box::new(Boom),
        };
        assert_eq!(e.stage(), Some(StageId::new("middle")));
        assert_eq!(Error::Cancelled.stage(), None);
    }

    #[test]
    fn error_policy_default_is_fail_fast() {
        assert_eq!(ErrorPolicy::default(), ErrorPolicy::FailFast);
    }

    #[test]
    fn stage_failure_display() {
        let f = StageFailure {
            stage: StageId::new("parse"),
            source: Box::new(Boom),
        };
        assert_eq!(format!("{f}"), "stage `parse`: boom");
    }
}
