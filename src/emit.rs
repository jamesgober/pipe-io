//! The [`Emit`] trait and its error type.
//!
//! A [`crate::Stage`] receives a `&mut dyn Emit<Item = Output>` and pushes
//! zero or more outputs through it per input. Inside a running pipeline
//! the handle forwards each item straight into the next stage (or, across
//! a `buffer` edge under the threaded driver, into a bounded channel that
//! blocks while full). Backpressure is therefore expressed by `emit` taking
//! longer, never by an error.
//!
//! `emit` fails only with [`EmitError::Closed`]: a later stage or the sink
//! failed, or the run is shutting down. The stage should stop emitting and
//! return `Ok(())`; the driver already holds the real error and reports it.

use core::fmt;

/// Errors returned by [`Emit::emit`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmitError {
    /// The downstream side stopped accepting items, either because a
    /// later stage or the sink failed or because the run is shutting
    /// down. The item passed to `emit` was dropped. Stop emitting and
    /// return `Ok(())`: the pipeline surfaces the downstream error itself.
    Closed,
}

impl fmt::Display for EmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("downstream closed"),
        }
    }
}

impl core::error::Error for EmitError {}

/// Object-safe handle used by [`crate::Stage`] implementations to push
/// items downstream.
///
/// # Example
///
/// ```
/// use pipe_io::{Emit, EmitError};
///
/// struct Collect(Vec<u32>);
/// impl Emit for Collect {
///     type Item = u32;
///     fn emit(&mut self, item: u32) -> Result<(), EmitError> {
///         self.0.push(item);
///         Ok(())
///     }
/// }
///
/// let mut out = Collect(Vec::new());
/// out.emit(7)?;
/// assert_eq!(out.0, vec![7]);
/// # Ok::<(), EmitError>(())
/// ```
pub trait Emit {
    /// The type of item this handle accepts.
    type Item;

    /// Push one item downstream. May block while a bounded downstream
    /// buffer is full.
    ///
    /// # Errors
    ///
    /// Returns [`EmitError::Closed`] when the downstream side has stopped
    /// accepting items; see the variant docs for how to react.
    fn emit(&mut self, item: Self::Item) -> Result<(), EmitError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn test_emit_error_display() {
        assert_eq!(EmitError::Closed.to_string(), "downstream closed");
    }
}
