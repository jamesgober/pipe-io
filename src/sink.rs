//! The [`Sink`] trait and built-in sink adapters.

use crate::error::StageError;
use crate::source::Infallible;

#[cfg(feature = "std")]
use alloc::vec::Vec;
use core::marker::PhantomData;

/// Terminal consumer at the tail of a pipeline.
pub trait Sink {
    /// Type of item this sink accepts.
    type Item;
    /// Error type the sink can return.
    type Error: StageError;

    /// Write a single item.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on write failure. The driver wraps
    /// this in [`crate::Error::Sink`].
    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error>;

    /// Flush any buffered output. Default impl does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on flush failure.
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Release any resources. Default impl does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on shutdown failure.
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Sink that discards every item.
///
/// # Example
///
/// ```
/// use pipe_io::sink::{NullSink, Sink};
///
/// let mut s: NullSink<u32> = NullSink::new();
/// s.write(42).unwrap();
/// ```
pub struct NullSink<T> {
    _marker: PhantomData<fn(T)>,
}

impl<T> NullSink<T> {
    /// Construct a new null sink.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for NullSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static> Sink for NullSink<T> {
    type Item = T;
    type Error = Infallible;

    fn write(&mut self, _item: Self::Item) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Sink that pushes every item into an internal `Vec`. Useful in tests.
///
/// The collected items live behind a [`SharedHandle`] returned by
/// [`VecSink::handle`]. The handle is `Send + Sync` (via internal
/// `Arc<Mutex<_>>`) and outlives the sink, so callers can drop the
/// sink and still read the result.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub struct VecSink<T> {
    inner: SharedHandle<T>,
}

#[cfg(feature = "std")]
impl<T> VecSink<T> {
    /// Construct a new vec sink.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: SharedHandle::new(),
        }
    }

    /// Return a cloneable handle to the underlying storage. Items
    /// written to the sink appear in the handle.
    #[must_use]
    pub fn handle(&self) -> SharedHandle<T> {
        self.inner.clone()
    }
}

#[cfg(feature = "std")]
impl<T> Default for VecSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "std")]
impl<T: Send + 'static> Sink for VecSink<T> {
    type Item = T;
    type Error = Infallible;

    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        self.inner.push(item);
        Ok(())
    }
}

/// Sink adapter over a closure.
///
/// # Example
///
/// ```
/// use pipe_io::sink::{FnSink, Sink};
///
/// let mut total = 0u64;
/// {
///     let mut s = FnSink::new(|item: u64| -> Result<(), &'static str> {
///         total += item;
///         Ok(())
///     });
///     s.write(1).unwrap();
///     s.write(2).unwrap();
///     s.write(3).unwrap();
/// }
/// assert_eq!(total, 6);
/// ```
pub struct FnSink<F, T, E> {
    func: F,
    _marker: PhantomData<fn(T) -> Result<(), E>>,
}

impl<F, T, E> FnSink<F, T, E>
where
    F: FnMut(T) -> Result<(), E>,
{
    /// Wrap a closure into a sink.
    pub fn new(func: F) -> Self {
        Self {
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, T, E> Sink for FnSink<F, T, E>
where
    F: FnMut(T) -> Result<(), E>,
    E: StageError,
    T: 'static,
{
    type Item = T;
    type Error = E;

    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        (self.func)(item)
    }
}

/// Sink adapter over an [`std::sync::mpsc::SyncSender`] (bounded).
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub struct ChannelSink<T> {
    tx: std::sync::mpsc::SyncSender<T>,
}

#[cfg(feature = "std")]
impl<T> ChannelSink<T> {
    /// Wrap a sender into a sink.
    #[must_use]
    pub fn new(tx: std::sync::mpsc::SyncSender<T>) -> Self {
        Self { tx }
    }
}

#[cfg(feature = "std")]
impl<T: 'static + Send> Sink for ChannelSink<T> {
    type Item = T;
    type Error = ChannelSinkError;

    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        self.tx
            .send(item)
            .map_err(|_| ChannelSinkError::Disconnected)
    }
}

/// Error returned by [`ChannelSink`] when the receiver has been dropped.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelSinkError {
    /// The receiving end of the channel has been dropped.
    Disconnected,
}

#[cfg(feature = "std")]
impl core::fmt::Display for ChannelSinkError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Disconnected => f.write_str("channel sink disconnected"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ChannelSinkError {}

/// Sink that line-writes `String` items into any [`std::io::Write`].
/// Each item is followed by `\n`. Upstream stages can convert via
/// `.map(|x| x.to_string())` for any [`core::fmt::Display`] type.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub struct WriterSink<W: std::io::Write> {
    writer: W,
}

#[cfg(feature = "std")]
impl<W: std::io::Write> WriterSink<W> {
    /// Wrap a writer into a sink.
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Consume the sink and return the wrapped writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

#[cfg(feature = "std")]
impl<W> Sink for WriterSink<W>
where
    W: std::io::Write + Send + 'static,
{
    type Item = alloc::string::String;
    type Error = std::io::Error;

    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        writeln!(self.writer, "{item}")
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        std::io::Write::flush(&mut self.writer)
    }
}

// ---------------------------------------------------------------------
// SharedHandle: thread-safe (std) or single-threaded (no_std) storage.
// ---------------------------------------------------------------------

/// Cloneable handle to the items collected by a [`VecSink`].
#[cfg(feature = "std")]
pub struct SharedHandle<T> {
    inner: std::sync::Arc<std::sync::Mutex<Vec<T>>>,
}

#[cfg(feature = "std")]
impl<T> SharedHandle<T> {
    fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn push(&self, item: T) {
        self.inner
            .lock()
            .expect("VecSink mutex poisoned")
            .push(item);
    }

    /// Drain the collected items.
    pub fn take(&self) -> Vec<T> {
        let mut guard = self.inner.lock().expect("VecSink mutex poisoned");
        core::mem::take(&mut *guard)
    }

    /// Number of items currently buffered.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("VecSink mutex poisoned").len()
    }

    /// True if no items are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(feature = "std")]
impl<T> Clone for SharedHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn null_sink_discards() {
        let mut s: NullSink<i32> = NullSink::new();
        s.write(1).unwrap();
        s.write(2).unwrap();
        s.flush().unwrap();
        s.close().unwrap();
    }

    #[cfg(feature = "std")]
    #[test]
    fn vec_sink_collects() {
        let mut s = VecSink::<i32>::new();
        let h = s.handle();
        s.write(1).unwrap();
        s.write(2).unwrap();
        s.write(3).unwrap();
        assert_eq!(h.len(), 3);
        assert_eq!(h.take(), vec![1, 2, 3]);
        assert!(h.is_empty());
    }

    #[test]
    fn fn_sink_invokes_closure() {
        let mut count = 0u32;
        {
            let mut s: FnSink<_, u32, &'static str> = FnSink::new(|n: u32| {
                count += n;
                Ok(())
            });
            s.write(2).unwrap();
            s.write(3).unwrap();
        }
        assert_eq!(count, 5);
    }

    #[cfg(feature = "std")]
    #[test]
    fn channel_sink_sends_until_disconnect() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<i32>(4);
        let mut s = ChannelSink::new(tx);
        s.write(1).unwrap();
        s.write(2).unwrap();
        drop(rx);
        assert!(s.write(3).is_err());
    }

    #[cfg(feature = "std")]
    #[test]
    fn writer_sink_line_writes() {
        use std::io::Cursor;
        let buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let mut s = WriterSink::<Cursor<Vec<u8>>>::new(buf);
        s.write("alpha".into()).unwrap();
        s.write("beta".into()).unwrap();
        let cur = s.into_inner();
        let body = String::from_utf8(cur.into_inner()).unwrap();
        assert_eq!(body, "alpha\nbeta\n");
    }
}
