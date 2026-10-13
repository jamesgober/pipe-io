//! The [`Stage`] trait.
//!
//! Stages receive items one at a time and emit zero or more outputs via an
//! [`Emit`] handle. The closure adapters (`map`, `filter`, `try_map`, ...)
//! are methods on [`crate::PipelineBuilder`]; implement `Stage` directly
//! for stateful transforms and plug them in with
//! [`crate::PipelineBuilder::stage`].
//!
//! # Lifecycle
//!
//! For every item the driver calls [`Stage::process`]. When the idle tick
//! is enabled ([`crate::PipelineBuilder::idle_tick`]) and the source has
//! nothing to deliver, the driver calls [`Stage::tick`] so time-based
//! stages can emit without waiting for the next item. At end of stream
//! (or on cancellation) it calls [`Stage::flush`] exactly once.
//!
//! Errors from `process`, `tick`, and `flush` are handled by the
//! [`crate::ErrorPolicy`] active when the stage was added. Under
//! `Continue` and `DeadLetter` a failed `flush` is recorded and the flush
//! still propagates to every later stage and the sink, so buffered data
//! downstream is never lost because one stage's flush failed.
//!
//! When the run fails (a `FailFast` error, a source error, a sink error, a
//! panic), no further stage method is called: buffered stage state is
//! discarded and only the sink, the dead-letter sink, and the source are
//! flushed and closed.

use crate::emit::Emit;
use crate::error::StageError;

/// Transform stage in a pipeline.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "std")]
/// # fn main() -> Result<(), pipe_io::Error> {
/// use pipe_io::{Emit, Pipeline, Stage, sink::VecSink};
///
/// /// Emits running totals.
/// struct RunningSum(u64);
///
/// impl Stage for RunningSum {
///     type Input = u64;
///     type Output = u64;
///     type Error = core::convert::Infallible;
///
///     fn process(
///         &mut self,
///         item: u64,
///         out: &mut dyn Emit<Item = u64>,
///     ) -> Result<(), Self::Error> {
///         self.0 += item;
///         // A closed downstream means the run is ending; nothing to do.
///         let _closed = out.emit(self.0).is_err();
///         Ok(())
///     }
/// }
///
/// let sink = VecSink::new();
/// let out = sink.handle();
/// Pipeline::from_iter([1_u64, 2, 3]).stage(RunningSum(0)).sink(sink).run()?;
/// assert_eq!(out.take(), vec![1, 3, 6]);
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "std"))]
/// # fn main() {}
/// ```
pub trait Stage {
    /// Type of item this stage consumes.
    type Input;
    /// Type of item this stage emits.
    type Output;
    /// Error type the stage can produce.
    type Error: StageError;

    /// Process a single input item, emitting zero or more outputs.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on failure. The active
    /// [`crate::ErrorPolicy`] decides whether the run stops
    /// ([`crate::Error::Stage`]), continues, or routes the failure to the
    /// dead-letter sink.
    fn process(
        &mut self,
        item: Self::Input,
        out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error>;

    /// Called when the idle tick fires and no item arrived during the tick
    /// interval. Time-based stages use it to emit expired groups on quiet
    /// streams. The default does nothing.
    ///
    /// # Errors
    ///
    /// Same handling as [`Stage::process`].
    fn tick(&mut self, _out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Emit any buffered output. Called once at end of stream or on
    /// cancellation, before the sink is flushed. The default does nothing.
    ///
    /// # Errors
    ///
    /// Same handling as [`Stage::process`]; under `Continue` and
    /// `DeadLetter` the flush still reaches every later stage.
    fn flush(&mut self, _out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::emit::EmitError;
    use alloc::vec::Vec;

    struct Double;
    impl Stage for Double {
        type Input = u32;
        type Output = u32;
        type Error = &'static str;
        fn process(
            &mut self,
            item: u32,
            out: &mut dyn Emit<Item = u32>,
        ) -> Result<(), Self::Error> {
            if item == 0 {
                return Err("zero");
            }
            out.emit(item * 2).map_err(|_| "closed")
        }
    }

    struct Collect(Vec<u32>, bool);
    impl Emit for Collect {
        type Item = u32;
        fn emit(&mut self, item: u32) -> Result<(), EmitError> {
            if self.1 {
                return Err(EmitError::Closed);
            }
            self.0.push(item);
            Ok(())
        }
    }

    #[test]
    fn test_stage_default_tick_and_flush_emit_nothing() {
        let mut s = Double;
        let mut out = Collect(Vec::new(), false);
        s.tick(&mut out).unwrap();
        s.flush(&mut out).unwrap();
        assert!(out.0.is_empty());
    }

    #[test]
    fn test_stage_process_emits_and_fails() {
        let mut s = Double;
        let mut out = Collect(Vec::new(), false);
        s.process(4, &mut out).unwrap();
        assert_eq!(out.0, alloc::vec![8]);
        assert_eq!(s.process(0, &mut out), Err("zero"));
        let mut closed = Collect(Vec::new(), true);
        assert_eq!(s.process(1, &mut closed), Err("closed"));
    }
}
