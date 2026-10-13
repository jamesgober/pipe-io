//! The [`Sink`] trait and built-in sink adapters.
//!
//! # Lifecycle
//!
//! The driver calls [`Sink::write`] for every item, then [`Sink::flush`]
//! and [`Sink::close`] once at the end of the run. `flush` and `close` are
//! called on every exit path: end of stream, cancellation, a stage or
//! source error, a sink error, and panic unwinding. A sink can therefore
//! rely on `close` to release resources and on `flush` to persist what it
//! accepted, even when the run fails.

use core::marker::PhantomData;

use alloc::boxed::Box;

use crate::error::{BoxError, StageError};

/// Terminal consumer at the tail of a pipeline.
///
/// # Example
///
/// ```
/// use pipe_io::{Pipeline, Sink};
///
/// #[derive(Default)]
/// struct Sum(u64);
///
/// impl Sink for Sum {
///     type Item = u64;
///     type Error = core::convert::Infallible;
///
///     fn write(&mut self, item: u64) -> Result<(), Self::Error> {
///         self.0 += item;
///         Ok(())
///     }
/// }
///
/// let stats = Pipeline::from_iter(1_u64..=4).sink(Sum::default()).run()?;
/// assert_eq!(stats.items_out, 4);
/// # Ok::<(), pipe_io::Error>(())
/// ```
pub trait Sink {
    /// Type of item this sink accepts.
    type Item;
    /// Error type the sink can return.
    type Error: StageError;

    /// Write a single item.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on write failure. Sink errors end the
    /// run and are reported as [`crate::Error::Sink`]; the sink is still
    /// flushed and closed afterwards.
    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error>;

    /// Persist buffered output. The default does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on flush failure.
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Release resources. Called after [`Sink::flush`]. The default does
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` on shutdown failure.
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Sink that discards every item. Useful for benchmarks and for pipelines
/// that act through `inspect`.
///
/// # Example
///
/// ```
/// use pipe_io::sink::{NullSink, Sink};
///
/// let mut s = NullSink::<u32>::new();
/// s.write(42)?;
/// # Ok::<(), core::convert::Infallible>(())
/// ```
pub struct NullSink<T> {
    _marker: PhantomData<fn(T)>,
}

impl<T> NullSink<T> {
    /// Construct a null sink.
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

impl<T> core::fmt::Debug for NullSink<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("NullSink")
    }
}

impl<T> Sink for NullSink<T> {
    type Item = T;
    type Error = core::convert::Infallible;

    #[inline]
    fn write(&mut self, _item: Self::Item) -> Result<(), Self::Error> {
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
/// let mut total = 0_u64;
/// {
///     let mut s = FnSink::new(|item: u64| -> Result<(), &'static str> {
///         total += item;
///         Ok(())
///     });
///     s.write(1)?;
///     s.write(2)?;
/// }
/// assert_eq!(total, 3);
/// # Ok::<(), &'static str>(())
/// ```
pub struct FnSink<F, T, E> {
    func: F,
    _marker: PhantomData<fn(T) -> Result<(), E>>,
}

impl<F, T, E> FnSink<F, T, E>
where
    F: FnMut(T) -> Result<(), E>,
{
    /// Wrap a closure.
    pub const fn new(func: F) -> Self {
        Self {
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, T, E> core::fmt::Debug for FnSink<F, T, E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FnSink").finish_non_exhaustive()
    }
}

impl<F, T, E> Sink for FnSink<F, T, E>
where
    F: FnMut(T) -> Result<(), E>,
    E: StageError,
{
    type Item = T;
    type Error = E;

    #[inline]
    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        (self.func)(item)
    }
}

/// Sink that writes every item to two sinks (the first receives a clone).
///
/// `flush` and `close` reach both sinks even if the first one fails; the
/// first error is returned.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "std")]
/// # fn main() -> Result<(), pipe_io::Error> {
/// use pipe_io::{Pipeline, sink::{TeeSink, VecSink}};
///
/// let (a, b) = (VecSink::new(), VecSink::new());
/// let (ha, hb) = (a.handle(), b.handle());
/// Pipeline::from_iter([1, 2]).sink(TeeSink::new(a, b)).run()?;
/// assert_eq!(ha.take(), vec![1, 2]);
/// assert_eq!(hb.take(), vec![1, 2]);
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "std"))]
/// # fn main() {}
/// ```
#[derive(Debug)]
pub struct TeeSink<A, B> {
    first: A,
    second: B,
}

impl<A, B> TeeSink<A, B>
where
    A: Sink,
    A::Item: Clone,
    B: Sink<Item = A::Item>,
{
    /// Fan out to `first` and `second`.
    pub const fn new(first: A, second: B) -> Self {
        Self { first, second }
    }

    /// Recover the two sinks.
    pub fn into_inner(self) -> (A, B) {
        (self.first, self.second)
    }
}

impl<A, B> Sink for TeeSink<A, B>
where
    A: Sink,
    A::Item: Clone,
    B: Sink<Item = A::Item>,
{
    type Item = A::Item;
    type Error = BoxError;

    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        self.first.write(item.clone()).map_err(Into::into)?;
        self.second.write(item).map_err(Into::into)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        let first = self.first.flush().map_err(Into::into);
        let second = self.second.flush().map_err(Into::into);
        both(first, second)
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        let first = self.first.close().map_err(Into::into);
        let second = self.second.close().map_err(Into::into);
        both(first, second)
    }
}

/// Combine the outcomes of the same call on both halves of a
/// [`TeeSink`]; if both failed, neither error is lost.
fn both(first: Result<(), BoxError>, second: Result<(), BoxError>) -> Result<(), BoxError> {
    match (first, second) {
        (Err(first), Err(second)) => Err(Box::new(BothFailed { first, second })),
        (Err(e), Ok(())) | (Ok(()), Err(e)) => Err(e),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Both sinks of a [`TeeSink`] failed. `source()` is the first sink's
/// error; the message includes both.
#[derive(Debug)]
struct BothFailed {
    first: BoxError,
    second: BoxError,
}

impl core::fmt::Display for BothFailed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{} (second sink also failed: {})",
            self.first, self.second
        )
    }
}

impl core::error::Error for BothFailed {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&*self.first)
    }
}

#[cfg(feature = "std")]
pub use self::std_sinks::*;

#[cfg(feature = "std")]
mod std_sinks {
    use alloc::string::String;
    use alloc::vec::Vec;
    use core::fmt;
    use std::sync::mpsc::SyncSender;
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

    use super::Sink;

    /// Sink that collects items into a shared `Vec`, mainly for tests.
    ///
    /// Read the items through the [`SharedHandle`] returned by
    /// [`VecSink::handle`]; the handle outlives the sink, so the pipeline
    /// can consume the sink while the caller keeps the handle.
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub struct VecSink<T> {
        inner: SharedHandle<T>,
    }

    impl<T> VecSink<T> {
        /// Construct an empty sink.
        #[must_use]
        pub fn new() -> Self {
            Self {
                inner: SharedHandle::new(),
            }
        }

        /// A handle to the collected items.
        #[must_use]
        pub fn handle(&self) -> SharedHandle<T> {
            self.inner.clone()
        }
    }

    impl<T> Default for VecSink<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T> fmt::Debug for VecSink<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("VecSink")
                .field("len", &self.inner.len())
                .finish()
        }
    }

    impl<T> Sink for VecSink<T> {
        type Item = T;
        type Error = core::convert::Infallible;

        fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
            self.inner.lock().push(item);
            Ok(())
        }
    }

    /// Cloneable handle to the items collected by a [`VecSink`].
    ///
    /// The handle never panics: if a thread panicked while holding the
    /// internal lock, the data written so far is still returned.
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub struct SharedHandle<T> {
        inner: Arc<Mutex<Vec<T>>>,
    }

    impl<T> SharedHandle<T> {
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub(super) fn lock(&self) -> MutexGuard<'_, Vec<T>> {
            self.inner.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// Remove and return every collected item.
        #[must_use]
        pub fn take(&self) -> Vec<T> {
            core::mem::take(&mut *self.lock())
        }

        /// Number of items currently collected.
        #[must_use]
        pub fn len(&self) -> usize {
            self.lock().len()
        }

        /// True if no items are currently collected.
        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }

    impl<T: Clone> SharedHandle<T> {
        /// Clone the collected items without removing them.
        #[must_use]
        pub fn snapshot(&self) -> Vec<T> {
            self.lock().clone()
        }
    }

    impl<T> Clone for SharedHandle<T> {
        fn clone(&self) -> Self {
            Self {
                inner: Arc::clone(&self.inner),
            }
        }
    }

    impl<T> fmt::Debug for SharedHandle<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("SharedHandle")
                .field("len", &self.len())
                .finish()
        }
    }

    /// Sink over a bounded [`std::sync::mpsc::SyncSender`]. `write`
    /// blocks while the channel is full, which hands backpressure on to
    /// whatever consumes the receiver.
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[derive(Debug)]
    pub struct ChannelSink<T> {
        tx: SyncSender<T>,
    }

    impl<T> ChannelSink<T> {
        /// Wrap a sender.
        #[must_use]
        pub const fn new(tx: SyncSender<T>) -> Self {
            Self { tx }
        }
    }

    impl<T: Send> Sink for ChannelSink<T> {
        type Item = T;
        type Error = ChannelSinkError;

        fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
            self.tx
                .send(item)
                .map_err(|_| ChannelSinkError::Disconnected)
        }
    }

    /// Error returned by [`ChannelSink`].
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[non_exhaustive]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ChannelSinkError {
        /// The receiving end was dropped; the item was not delivered.
        Disconnected,
    }

    impl fmt::Display for ChannelSinkError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Disconnected => f.write_str("channel sink disconnected"),
            }
        }
    }

    impl std::error::Error for ChannelSinkError {}

    /// Sink that writes each `String` item followed by `\n` to any
    /// [`std::io::Write`]. `flush` and `close` flush the writer.
    ///
    /// Convert other item types upstream, for example with
    /// `.map(|x| x.to_string())`.
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub struct WriterSink<W> {
        writer: W,
    }

    impl<W: std::io::Write> WriterSink<W> {
        /// Wrap a writer.
        pub const fn new(writer: W) -> Self {
            Self { writer }
        }

        /// Consume the sink and return the writer.
        pub fn into_inner(self) -> W {
            self.writer
        }
    }

    impl<W> fmt::Debug for WriterSink<W> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("WriterSink").finish_non_exhaustive()
        }
    }

    impl<W: std::io::Write> Sink for WriterSink<W> {
        type Item = String;
        type Error = std::io::Error;

        fn write(&mut self, item: Self::Item) -> Result<(), Self::Error> {
            self.writer.write_all(item.as_bytes())?;
            self.writer.write_all(b"\n")
        }

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.writer.flush()
        }

        fn close(&mut self) -> Result<(), Self::Error> {
            self.writer.flush()
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec::Vec;

    #[test]
    fn test_null_sink_discards() {
        let mut s: NullSink<i32> = NullSink::default();
        s.write(1).unwrap();
        s.flush().unwrap();
        s.close().unwrap();
        assert_eq!(alloc::format!("{s:?}"), "NullSink");
    }

    #[test]
    fn test_fn_sink_invokes_closure() {
        let mut count = 0_u32;
        {
            let mut s = FnSink::new(|n: u32| -> Result<(), &'static str> {
                count += n;
                Ok(())
            });
            s.write(2).unwrap();
            s.write(3).unwrap();
            assert_eq!(alloc::format!("{s:?}"), "FnSink { .. }");
        }
        assert_eq!(count, 5);
    }

    struct Recorder {
        log: Vec<&'static str>,
        fail: Option<&'static str>,
    }
    impl Sink for Recorder {
        type Item = u8;
        type Error = &'static str;
        fn write(&mut self, _: u8) -> Result<(), &'static str> {
            self.log.push("write");
            if self.fail == Some("write") {
                Err("write failed")
            } else {
                Ok(())
            }
        }
        fn flush(&mut self) -> Result<(), &'static str> {
            self.log.push("flush");
            if self.fail == Some("flush") {
                Err("flush failed")
            } else {
                Ok(())
            }
        }
        fn close(&mut self) -> Result<(), &'static str> {
            self.log.push("close");
            if self.fail == Some("close") {
                Err("close failed")
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_tee_sink_reaches_both_even_on_failure() {
        let a = Recorder {
            log: Vec::new(),
            fail: Some("flush"),
        };
        let b = Recorder {
            log: Vec::new(),
            fail: Some("close"),
        };
        let mut tee = TeeSink::new(a, b);
        tee.write(1).unwrap();
        assert_eq!(tee.flush().unwrap_err().to_string(), "flush failed");
        assert_eq!(tee.close().unwrap_err().to_string(), "close failed");
        let (a, b) = tee.into_inner();
        assert_eq!(a.log, ["write", "flush", "close"]);
        assert_eq!(b.log, ["write", "flush", "close"]);
        let mut failing = TeeSink::new(
            Recorder {
                log: Vec::new(),
                fail: Some("write"),
            },
            Recorder {
                log: Vec::new(),
                fail: None,
            },
        );
        assert!(failing.write(1).is_err());
        assert!(
            failing.into_inner().1.log.is_empty(),
            "second sink skipped after a failed write"
        );
        // When both halves fail, neither error is lost.
        let mut both_fail = TeeSink::new(
            Recorder {
                log: Vec::new(),
                fail: Some("flush"),
            },
            Recorder {
                log: Vec::new(),
                fail: Some("flush"),
            },
        );
        let err = both_fail.flush().unwrap_err();
        assert_eq!(
            err.to_string(),
            "flush failed (second sink also failed: flush failed)"
        );
        assert_eq!(
            core::error::Error::source(&*err).map(ToString::to_string),
            Some("flush failed".to_string())
        );
    }

    #[cfg(feature = "std")]
    mod std_tests {
        use super::super::*;
        use alloc::string::String;
        use alloc::vec;
        use alloc::vec::Vec;

        #[test]
        fn test_vec_sink_collects() {
            let mut s = VecSink::<i32>::default();
            let h = s.handle();
            s.write(1).unwrap();
            s.write(2).unwrap();
            assert_eq!(h.len(), 2);
            assert_eq!(h.snapshot(), vec![1, 2]);
            assert_eq!(h.take(), vec![1, 2]);
            assert!(h.is_empty());
            assert!(alloc::format!("{s:?}").contains("len: 0"));
            assert!(alloc::format!("{h:?}").contains("SharedHandle"));
        }

        #[test]
        fn test_vec_sink_survives_poisoned_lock() {
            let s = VecSink::<i32>::new();
            let h = s.handle();
            let poison = h.clone();
            let joined = std::thread::spawn(move || {
                let _guard = poison.lock();
                panic!("poison the lock");
            })
            .join();
            assert!(joined.is_err());
            let mut s = s;
            s.write(7).unwrap();
            assert_eq!(h.take(), vec![7]);
        }

        #[test]
        fn test_channel_sink_sends_until_disconnect() {
            let (tx, rx) = std::sync::mpsc::sync_channel::<i32>(4);
            let mut s = ChannelSink::new(tx);
            s.write(1).unwrap();
            drop(rx);
            assert_eq!(s.write(3), Err(ChannelSinkError::Disconnected));
            assert_eq!(
                ChannelSinkError::Disconnected.to_string(),
                "channel sink disconnected"
            );
        }

        #[test]
        fn test_writer_sink_line_writes_and_flushes() {
            let mut s = WriterSink::new(Vec::<u8>::new());
            s.write("alpha".into()).unwrap();
            s.write("beta".into()).unwrap();
            s.flush().unwrap();
            s.close().unwrap();
            assert_eq!(alloc::format!("{s:?}"), "WriterSink { .. }");
            let body = String::from_utf8(s.into_inner()).unwrap();
            assert_eq!(body, "alpha\nbeta\n");
        }
    }
}
