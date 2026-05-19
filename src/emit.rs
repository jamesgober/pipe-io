//! The [`Emit`] trait and its error type.
//!
//! Stages receive a `&mut dyn Emit<Item = Output>` from the driver and
//! emit zero or more outputs per input. Backpressure surfaces through
//! [`EmitError`]: drivers either block until space is available
//! ([`crate::driver::ThreadedDriver`]) or return
//! [`EmitError::WouldBlock`] for the caller to retry
//! ([`crate::driver::SyncDriver`]).

use core::fmt;

/// Errors returned by [`Emit::emit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmitError {
    /// The downstream is closed and will not accept further items.
    /// Stages should stop emitting and return cleanly from `process`.
    Closed,
    /// The downstream buffer is currently full; the caller should
    /// retry later. Only returned by drivers that do not block on
    /// backpressure.
    WouldBlock,
}

impl fmt::Display for EmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("downstream closed"),
            Self::WouldBlock => f.write_str("downstream would block"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for EmitError {}

/// Object-safe handle to push items downstream from inside
/// [`crate::Stage::process`].
pub trait Emit {
    /// The type of item this handle accepts.
    type Item;
    /// Push one item downstream.
    ///
    /// # Errors
    ///
    /// Returns [`EmitError::Closed`] when the downstream has shut down
    /// (often because a later stage produced an error and the driver
    /// is unwinding). Returns [`EmitError::WouldBlock`] under a
    /// non-blocking driver if the downstream buffer is full.
    fn emit(&mut self, item: Self::Item) -> Result<(), EmitError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn emit_error_display() {
        assert_eq!(EmitError::Closed.to_string(), "downstream closed");
        assert_eq!(EmitError::WouldBlock.to_string(), "downstream would block");
    }
}
