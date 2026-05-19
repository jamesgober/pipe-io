//! The [`Stage`] trait.
//!
//! Stages receive items one at a time and emit zero or more outputs via
//! an [`Emit`] handle. The closure-based adapters (`map`, `filter`, ...)
//! are exposed as methods on [`crate::PipelineBuilder`].

use crate::emit::Emit;
use crate::error::StageError;

/// Transform stage in a pipeline.
///
/// Stages emit zero or more outputs per input via the `out` handle.
/// 1:1 maps emit once per input, filters emit 0..1 per input,
/// batching/windowing emit 0+ per input and additionally use
/// [`Stage::flush`] to emit a final partial group at end-of-stream.
pub trait Stage {
    /// Type of item this stage consumes.
    type Input;
    /// Type of item this stage emits.
    type Output;
    /// Error type the stage can produce.
    type Error: StageError;

    /// Process a single input item.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on stage failure. The driver wraps
    /// this in [`crate::Error::Stage`].
    fn process(
        &mut self,
        item: Self::Input,
        out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error>;

    /// Emit any buffered output at end-of-stream.
    ///
    /// Default impl does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on stage failure.
    fn flush(&mut self, _out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        Ok(())
    }
}
