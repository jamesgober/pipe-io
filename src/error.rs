//! Error model for `pipe-io`.
//!
//! Every pipeline operation returns the single top-level [`Error`]. Errors
//! raised by user code (sources, stages, sinks) are boxed into a
//! [`BoxError`] and carried inside the variant that names the failing
//! component, so the original error stays reachable through
//! [`core::error::Error::source`], [`Error::inner`], and
//! [`Error::downcast_ref`].
//!
//! User error types only need to satisfy [`StageError`], which is
//! blanket-implemented for anything convertible into a [`BoxError`]: every
//! `core::error::Error + Send + Sync + 'static` type, plus `&'static str`
//! and `String` for quick prototypes.
//!
//! Misconfiguration (a zero window size, a batch policy without a trigger,
//! a dead-letter policy without a dead-letter sink, ...) is reported as a
//! typed [`ConfigError`] before any item is pulled. The builder never
//! panics on bad configuration.

use alloc::boxed::Box;
use core::any::Any;
use core::fmt;

use crate::StageId;

/// Crate result alias. The error type defaults to [`Error`].
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Boxed, thread-safe error carried by [`Error`] variants and by
/// [`StageFailure`].
pub type BoxError = Box<dyn core::error::Error + Send + Sync + 'static>;

/// Bound satisfied by every error type a [`crate::Source`],
/// [`crate::Stage`], or [`crate::Sink`] may return.
///
/// Blanket-implemented for every `T: Into<BoxError> + Debug + Send +
/// 'static`. That covers all `core::error::Error + Send + Sync + 'static`
/// types, `&'static str`, `String`, `Box<dyn Error + Send + Sync>`, and
/// [`core::convert::Infallible`]. The conversion into [`BoxError`] happens
/// once, when the error leaves the user component, so the original value
/// can be recovered later with [`Error::downcast_ref`].
pub trait StageError: Into<BoxError> + fmt::Debug + Send + 'static {}

impl<T> StageError for T where T: Into<BoxError> + fmt::Debug + Send + 'static {}

/// Top-level error type returned by every pipeline operation.
///
/// Component variants carry the [`StageId`] that failed and the boxed
/// error that component produced. The enum is `#[non_exhaustive]`; match
/// with a wildcard arm.
///
/// # Example
///
/// ```
/// use pipe_io::{Pipeline, sink::NullSink};
///
/// let err = Pipeline::from_iter(["1", "x", "3"])
///     .stage_id("parse")
///     .try_map(|s: &str| s.parse::<u32>())
///     .sink(NullSink::new())
///     .run()
///     .unwrap_err();
///
/// assert_eq!(err.stage().map(|id| id.as_str()), Some("parse"));
/// assert!(err.downcast_ref::<core::num::ParseIntError>().is_some());
/// ```
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// The source failed while producing an item or while closing.
    /// Source errors end the run; restart the pipeline to retry.
    Source {
        /// Identifier of the failing source (see
        /// [`crate::PipelineBuilder::source_id`]).
        stage: StageId,
        /// The error the source returned.
        source: BoxError,
    },
    /// A transform stage failed under [`ErrorPolicy::FailFast`].
    Stage {
        /// Identifier of the failing stage.
        stage: StageId,
        /// The error the stage returned.
        source: BoxError,
    },
    /// A sink (the main sink or the dead-letter sink) failed while
    /// writing, flushing, or closing.
    Sink {
        /// Identifier of the failing sink. The dead-letter sink reports
        /// `"dead_letter"`.
        stage: StageId,
        /// The error the sink returned.
        source: BoxError,
    },
    /// The pipeline configuration is invalid. Reported before the source
    /// is pulled; no lifecycle method of any component has run. Fix the
    /// configuration; retrying cannot succeed.
    Config(ConfigError),
    /// The run was stopped through a cancellation handle (see
    /// `Pipeline::cancel_handle`). Items already pulled were flushed to
    /// the sink before this was returned.
    Cancelled,
    /// The pipeline already finished (successfully or not) and cannot be
    /// stepped or finished again.
    Closed,
    /// The driver could not execute the pipeline, for example because the
    /// operating system refused to spawn a worker thread. Every component
    /// that was already started has been torn down.
    Driver {
        /// The underlying driver failure.
        source: BoxError,
    },
}

impl Error {
    /// Identifier of the component associated with this error, if any.
    #[must_use]
    pub const fn stage(&self) -> Option<StageId> {
        match self {
            Self::Source { stage, .. } | Self::Stage { stage, .. } | Self::Sink { stage, .. } => {
                Some(*stage)
            }
            Self::Config(cfg) => Some(cfg.stage()),
            Self::Cancelled | Self::Closed | Self::Driver { .. } => None,
        }
    }

    /// The boxed error produced by user code, if this variant carries one.
    #[must_use]
    pub fn inner(&self) -> Option<&(dyn core::error::Error + Send + Sync + 'static)> {
        match self {
            Self::Source { source, .. }
            | Self::Stage { source, .. }
            | Self::Sink { source, .. }
            | Self::Driver { source } => Some(&**source),
            Self::Config(_) | Self::Cancelled | Self::Closed => None,
        }
    }

    /// Consume the error and return the boxed error produced by user code,
    /// if this variant carries one.
    #[must_use]
    pub fn into_inner(self) -> Option<BoxError> {
        match self {
            Self::Source { source, .. }
            | Self::Stage { source, .. }
            | Self::Sink { source, .. }
            | Self::Driver { source } => Some(source),
            Self::Config(_) | Self::Cancelled | Self::Closed => None,
        }
    }

    /// Downcast the inner error to a concrete type.
    ///
    /// Returns `None` when the variant carries no inner error or when the
    /// inner error is not an `E`. Errors given as `&'static str` or
    /// `String` are boxed as an opaque message type and cannot be
    /// downcast; compare their [`fmt::Display`] output instead.
    #[must_use]
    pub fn downcast_ref<E: core::error::Error + 'static>(&self) -> Option<&E> {
        let inner: &(dyn core::error::Error + 'static) = self.inner()?;
        inner.downcast_ref::<E>()
    }

    /// True for [`Error::Cancelled`].
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { stage, source } => write!(f, "source `{stage}`: {source}"),
            Self::Stage { stage, source } => write!(f, "stage `{stage}`: {source}"),
            Self::Sink { stage, source } => write!(f, "sink `{stage}`: {source}"),
            Self::Config(cfg) => write!(f, "invalid pipeline configuration: {cfg}"),
            Self::Cancelled => f.write_str("pipeline cancelled"),
            Self::Closed => f.write_str("pipeline already finished"),
            Self::Driver { source } => write!(f, "driver failure: {source}"),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Config(cfg) => Some(cfg),
            other => other
                .inner()
                .map(|e| e as &(dyn core::error::Error + 'static)),
        }
    }
}

impl From<ConfigError> for Error {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

/// Invalid pipeline configuration, naming the stage it belongs to.
///
/// Produced by [`crate::PipelineBuilder::try_sink`], by
/// [`crate::Pipeline::run`] (and the other entry points) when the builder
/// recorded a problem, and by the `validate` methods on the policy types.
///
/// # Example
///
/// ```
/// use pipe_io::{BatchPolicy, ConfigErrorKind};
///
/// let err = BatchPolicy::new().validate().unwrap_err();
/// assert_eq!(err.kind(), ConfigErrorKind::NoBatchTrigger);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConfigError {
    stage: StageId,
    kind: ConfigErrorKind,
}

impl ConfigError {
    /// Construct a configuration error for `stage`.
    #[must_use]
    pub const fn new(stage: StageId, kind: ConfigErrorKind) -> Self {
        Self { stage, kind }
    }

    /// Stage the invalid setting belongs to.
    #[must_use]
    pub const fn stage(&self) -> StageId {
        self.stage
    }

    /// What is wrong with the setting.
    #[must_use]
    pub const fn kind(&self) -> ConfigErrorKind {
        self.kind
    }

    /// Return the same error attributed to another stage.
    #[must_use]
    pub const fn with_stage(self, stage: StageId) -> Self {
        Self {
            stage,
            kind: self.kind,
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stage `{}`: {}", self.stage, self.kind)
    }
}

impl core::error::Error for ConfigError {}

/// The specific configuration problem inside a [`ConfigError`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigErrorKind {
    /// A duration that must be positive is zero (a window `size`, `slide`,
    /// or `idle`, a batch `max_age`, or the idle tick interval). A zero
    /// duration would make time-based logic loop without progress.
    ZeroDuration {
        /// Name of the offending setting.
        field: &'static str,
    },
    /// A count or byte limit that must be positive is zero (batch
    /// `max_items` / `max_bytes`, window `max_items` / `max_keys`, retry
    /// `max_attempts`).
    ZeroLimit {
        /// Name of the offending setting.
        field: &'static str,
    },
    /// A sliding window's `slide` is larger than its `size`. Items that
    /// arrive in the gap between windows would be dropped silently, so the
    /// configuration is rejected.
    SlideExceedsSize,
    /// A sliding window's `size / slide` ratio means every item would be
    /// copied into more than `max` overlapping windows.
    TooManyOverlappingWindows {
        /// Number of windows each item would join.
        overlap: u64,
        /// Largest supported overlap.
        max: u64,
    },
    /// A batch policy has no trigger, so a batch would never be emitted
    /// before end of stream.
    NoBatchTrigger,
    /// A batch policy sets `max_bytes`, but the stage was added with
    /// [`crate::PipelineBuilder::batch`]; use
    /// [`crate::PipelineBuilder::batch_bytes`] (requires
    /// [`crate::ByteSize`]).
    BytesRequireByteSize,
    /// A stage runs under [`ErrorPolicy::DeadLetter`] but no dead-letter
    /// sink was installed with [`crate::PipelineBuilder::dead_letter`].
    /// Failures would be dropped silently.
    DeadLetterSinkMissing,
    /// A limit is larger than the supported maximum (the capacity of a
    /// [`crate::PipelineBuilder::buffer`] edge).
    LimitTooLarge {
        /// Name of the offending setting.
        field: &'static str,
        /// Largest accepted value.
        max: u64,
    },
}

impl fmt::Display for ConfigErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDuration { field } | Self::ZeroLimit { field } => {
                write!(f, "`{field}` must be greater than zero")
            }
            Self::SlideExceedsSize => f.write_str(
                "sliding window `slide` is larger than `size`; items between windows would be lost",
            ),
            Self::TooManyOverlappingWindows { overlap, max } => write!(
                f,
                "sliding window places each item in {overlap} windows (maximum {max}); \
                 increase `slide` or decrease `size`"
            ),
            Self::NoBatchTrigger => {
                f.write_str("batch policy needs `max_items`, `max_bytes`, or `max_age`")
            }
            Self::BytesRequireByteSize => f.write_str(
                "`max_bytes` requires `batch_bytes` and an item type implementing `ByteSize`",
            ),
            Self::DeadLetterSinkMissing => f.write_str(
                "stage uses ErrorPolicy::DeadLetter but no dead-letter sink is installed",
            ),
            Self::LimitTooLarge { field, max } => {
                write!(f, "`{field}` must not exceed {max}")
            }
        }
    }
}

/// Per-stage policy for how a fallible stage's errors propagate.
///
/// Set with [`crate::PipelineBuilder::on_error`]; it applies to every
/// fallible stage added afterwards until it is changed again.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ErrorPolicy {
    /// The first error stops the pipeline and is returned from the run.
    /// The sink, the dead-letter sink, and the source are still flushed
    /// and closed. Default.
    #[default]
    FailFast,
    /// The failing item is dropped, the error is counted in
    /// [`crate::RunStats::errors`] and reported to the observer, and the
    /// stream continues.
    Continue,
    /// The failure is written to the dead-letter sink as a
    /// [`StageFailure`] and the stream continues. A dead-letter sink must
    /// be installed with [`crate::PipelineBuilder::dead_letter`], otherwise
    /// the pipeline reports [`ConfigErrorKind::DeadLetterSinkMissing`].
    DeadLetter,
}

/// A failed item routed to the dead-letter sink under
/// [`ErrorPolicy::DeadLetter`].
///
/// `input` holds the original item when the failing stage could keep it:
/// [`crate::PipelineBuilder::try_map_ref`] (and, with `std`,
/// `try_map_retry`) attach it; `try_map` and custom stages cannot, because
/// their input was moved into user code. Recover it with
/// [`StageFailure::take_input`].
#[non_exhaustive]
pub struct StageFailure {
    /// Stage that failed.
    pub stage: StageId,
    /// The error the stage returned on its final attempt.
    pub source: BoxError,
    /// The failing item, type-erased, when the stage could keep it.
    pub input: Option<Box<dyn Any + Send + Sync>>,
    /// Number of attempts made, including retries. `1` without retry.
    pub attempts: u32,
}

impl StageFailure {
    /// Construct a failure record without a captured input.
    #[must_use]
    pub fn new(stage: StageId, source: BoxError) -> Self {
        Self {
            stage,
            source,
            input: None,
            attempts: 1,
        }
    }

    /// Attach the failing item.
    #[must_use]
    pub fn with_input<T: Any + Send + Sync>(mut self, input: T) -> Self {
        self.input = Some(Box::new(input));
        self
    }

    /// Set the number of attempts made.
    #[must_use]
    pub const fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Borrow the captured input as a `T`, if present and of that type.
    #[must_use]
    pub fn input_ref<T: Any>(&self) -> Option<&T> {
        self.input.as_ref()?.downcast_ref::<T>()
    }

    /// Take the captured input out as a `T`. Leaves the input in place
    /// when it is not a `T`.
    pub fn take_input<T: Any>(&mut self) -> Option<T> {
        let boxed = self.input.take()?;
        match boxed.downcast::<T>() {
            Ok(value) => Some(*value),
            Err(original) => {
                self.input = Some(original);
                None
            }
        }
    }
}

impl fmt::Debug for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageFailure")
            .field("stage", &self.stage)
            .field("source", &self.source)
            .field("input", &self.input.as_ref().map(|_| "<captured>"))
            .field("attempts", &self.attempts)
            .finish()
    }
}

impl fmt::Display for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stage `{}`: {}", self.stage, self.source)
    }
}

impl core::error::Error for StageFailure {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&*self.source)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::string::{String, ToString};
    use core::error::Error as _;

    #[derive(Debug, PartialEq)]
    struct Boom;
    impl fmt::Display for Boom {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("boom")
        }
    }
    impl core::error::Error for Boom {}

    fn stage_err() -> Error {
        Error::Stage {
            stage: StageId::new("parse"),
            source: Box::new(Boom),
        }
    }

    #[test]
    fn test_display_component_variants() {
        let src = Error::Source {
            stage: StageId::new("ingest"),
            source: Box::new(Boom),
        };
        let snk = Error::Sink {
            stage: StageId::new("writer"),
            source: "disk full".into(),
        };
        let drv = Error::Driver {
            source: "no threads".into(),
        };
        assert_eq!(src.to_string(), "source `ingest`: boom");
        assert_eq!(stage_err().to_string(), "stage `parse`: boom");
        assert_eq!(snk.to_string(), "sink `writer`: disk full");
        assert_eq!(drv.to_string(), "driver failure: no threads");
    }

    #[test]
    fn test_display_terminal_variants() {
        assert_eq!(Error::Cancelled.to_string(), "pipeline cancelled");
        assert_eq!(Error::Closed.to_string(), "pipeline already finished");
        let cfg = Error::Config(ConfigError::new(
            StageId::new("window"),
            ConfigErrorKind::SlideExceedsSize,
        ));
        assert!(
            cfg.to_string()
                .starts_with("invalid pipeline configuration: stage `window`")
        );
    }

    #[test]
    fn test_error_stage_extraction() {
        assert_eq!(stage_err().stage(), Some(StageId::new("parse")));
        assert_eq!(Error::Cancelled.stage(), None);
        assert_eq!(Error::Closed.stage(), None);
        let cfg: Error =
            ConfigError::new(StageId::new("b"), ConfigErrorKind::NoBatchTrigger).into();
        assert_eq!(cfg.stage(), Some(StageId::new("b")));
        let drv = Error::Driver { source: "x".into() };
        assert_eq!(drv.stage(), None);
    }

    #[test]
    fn test_error_source_chain_reaches_user_error() {
        let e = stage_err();
        let src = e.source().expect("stage error has a source");
        assert_eq!(src.to_string(), "boom");
        assert!(src.downcast_ref::<Boom>().is_some());
        assert_eq!(e.downcast_ref::<Boom>(), Some(&Boom));
        assert!(Error::Cancelled.source().is_none());
        assert!(Error::Cancelled.downcast_ref::<Boom>().is_none());
        let cfg = Error::Config(ConfigError::new(
            StageId::new("x"),
            ConfigErrorKind::NoBatchTrigger,
        ));
        assert!(
            cfg.source()
                .unwrap()
                .downcast_ref::<ConfigError>()
                .is_some()
        );
    }

    #[test]
    fn test_error_inner_and_into_inner() {
        let e = stage_err();
        assert_eq!(e.inner().unwrap().to_string(), "boom");
        let boxed = e.into_inner().unwrap();
        assert!(boxed.downcast_ref::<Boom>().is_some());
        assert!(Error::Closed.into_inner().is_none());
        assert!(Error::Closed.inner().is_none());
        assert!(Error::Cancelled.is_cancelled());
        assert!(!Error::Closed.is_cancelled());
    }

    #[test]
    fn test_string_errors_convert_into_box_error() {
        let a: BoxError = "static message".into();
        let b: BoxError = String::from("owned message").into();
        assert_eq!(a.to_string(), "static message");
        assert_eq!(b.to_string(), "owned message");
    }

    #[test]
    fn test_debug_shows_inner_debug() {
        let rendered = format!("{:?}", stage_err());
        assert!(rendered.contains("Stage"));
        assert!(rendered.contains("Boom"));
    }

    #[test]
    fn test_config_error_accessors_and_display() {
        let e = ConfigError::new(
            StageId::new("w"),
            ConfigErrorKind::ZeroDuration { field: "size" },
        );
        assert_eq!(e.stage(), StageId::new("w"));
        assert_eq!(e.kind(), ConfigErrorKind::ZeroDuration { field: "size" });
        assert_eq!(e.with_stage(StageId::new("z")).stage(), StageId::new("z"));
        assert_eq!(e.to_string(), "stage `w`: `size` must be greater than zero");
        let kinds = [
            ConfigErrorKind::ZeroLimit { field: "max_items" },
            ConfigErrorKind::SlideExceedsSize,
            ConfigErrorKind::TooManyOverlappingWindows { overlap: 9, max: 4 },
            ConfigErrorKind::NoBatchTrigger,
            ConfigErrorKind::BytesRequireByteSize,
            ConfigErrorKind::DeadLetterSinkMissing,
        ];
        for k in kinds {
            assert!(!k.to_string().is_empty());
        }
    }

    #[test]
    fn test_error_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<Error>();
        assert_send_sync::<ConfigError>();
        assert_send_sync::<StageFailure>();
        assert_send_sync::<BoxError>();
    }

    #[test]
    fn test_error_policy_default_is_fail_fast() {
        assert_eq!(ErrorPolicy::default(), ErrorPolicy::FailFast);
    }

    #[test]
    fn test_stage_failure_input_roundtrip() {
        let mut f = StageFailure::new(StageId::new("parse"), Box::new(Boom))
            .with_input(42_u32)
            .with_attempts(3);
        assert_eq!(f.attempts, 3);
        assert_eq!(f.input_ref::<u32>(), Some(&42));
        assert_eq!(f.input_ref::<u64>(), None);
        assert_eq!(f.take_input::<u64>(), None);
        assert!(
            f.input.is_some(),
            "wrong-type take leaves the input in place"
        );
        assert_eq!(f.take_input::<u32>(), Some(42));
        assert!(f.input.is_none());
        assert_eq!(f.take_input::<u32>(), None);
    }

    #[test]
    fn test_stage_failure_display_debug_and_source() {
        let f = StageFailure::new(StageId::new("parse"), Box::new(Boom)).with_input(1_u8);
        assert_eq!(format!("{f}"), "stage `parse`: boom");
        let dbg = format!("{f:?}");
        assert!(dbg.contains("<captured>"));
        assert!(f.source().unwrap().downcast_ref::<Boom>().is_some());
    }
}
