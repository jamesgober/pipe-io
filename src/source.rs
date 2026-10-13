//! The [`Source`] trait and built-in source adapters.
//!
//! A source produces items one at a time. The driver pulls until the
//! source reports end of stream, then flushes and closes every stage and
//! the sink, and finally calls [`Source::close`]. `close` is also called
//! when the run fails or is cancelled, so a source can rely on it for
//! cleanup.
//!
//! When the idle tick is enabled ([`crate::PipelineBuilder::idle_tick`])
//! the driver calls [`Source::pull_timeout`] instead of [`Source::pull`].
//! Sources that can wait with a deadline (such as [`ChannelSource`])
//! override it so time-based stages keep making progress on quiet
//! streams; the default implementation simply blocks in `pull`.

use core::marker::PhantomData;
use core::time::Duration;

use crate::error::{BoxError, StageError};

/// Producer at the head of a pipeline.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "std")]
/// # fn main() -> Result<(), pipe_io::Error> {
/// use pipe_io::{Pipeline, Source, sink::VecSink};
///
/// struct Countdown(u32);
///
/// impl Source for Countdown {
///     type Item = u32;
///     type Error = core::convert::Infallible;
///
///     fn pull(&mut self) -> Result<Option<u32>, Self::Error> {
///         if self.0 == 0 {
///             return Ok(None);
///         }
///         self.0 -= 1;
///         Ok(Some(self.0))
///     }
/// }
///
/// let sink = VecSink::new();
/// let out = sink.handle();
/// Pipeline::from_source(Countdown(3)).sink(sink).run()?;
/// assert_eq!(out.take(), vec![2, 1, 0]);
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "std"))]
/// # fn main() {}
/// ```
pub trait Source {
    /// Type of item produced.
    type Item;
    /// Error type the source can return.
    type Error: StageError;

    /// Pull the next item. `Ok(None)` signals end of stream.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if the source fails. Source errors end
    /// the run and are reported as [`crate::Error::Source`].
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error>;

    /// Pull the next item, waiting at most `timeout` for one to arrive.
    ///
    /// Returns [`Pull::Idle`] when nothing arrived in time. The default
    /// implementation ignores the timeout and blocks in [`Source::pull`];
    /// override it for sources that can wait with a deadline.
    ///
    /// # Errors
    ///
    /// As for [`Source::pull`].
    fn pull_timeout(&mut self, _timeout: Duration) -> Result<Pull<Self::Item>, Self::Error> {
        self.pull().map(Pull::from)
    }

    /// Release resources. Called exactly once per run on every exit path
    /// (end of stream, error, cancellation, panic unwinding). The default
    /// does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if cleanup fails. Reported as
    /// [`crate::Error::Source`] unless an earlier error is already being
    /// returned.
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Outcome of [`Source::pull_timeout`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pull<T> {
    /// An item is available.
    Item(T),
    /// No item arrived within the timeout; the stream is still open.
    Idle,
    /// End of stream.
    End,
}

impl<T> From<Option<T>> for Pull<T> {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::End, Self::Item)
    }
}

/// Former never-returned error type of the built-in adapters.
#[deprecated(
    since = "2.0.0",
    note = "use core::convert::Infallible; the built-in adapters now use it directly"
)]
pub type Infallible = core::convert::Infallible;

/// [`Source`] over any iterator.
///
/// # Example
///
/// ```
/// use pipe_io::source::{IterSource, Source};
///
/// let mut s = IterSource::new(vec![1, 2]);
/// assert_eq!(s.pull()?, Some(1));
/// assert_eq!(s.pull()?, Some(2));
/// assert_eq!(s.pull()?, None);
/// # Ok::<(), core::convert::Infallible>(())
/// ```
#[derive(Debug, Clone)]
pub struct IterSource<I> {
    iter: I,
}

impl<I: Iterator> IterSource<I> {
    /// Wrap anything iterable.
    pub fn new<II>(into_iter: II) -> Self
    where
        II: IntoIterator<IntoIter = I>,
    {
        Self {
            iter: into_iter.into_iter(),
        }
    }
}

impl<I: Iterator> Source for IterSource<I> {
    type Item = I::Item;
    type Error = core::convert::Infallible;

    #[inline]
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(self.iter.next())
    }
}

/// [`Source`] over a closure returning `Ok(Some(item))`, `Ok(None)` (end
/// of stream), or `Err(e)`.
///
/// # Example
///
/// ```
/// use pipe_io::source::{FnSource, Source};
///
/// let mut n = 0_u32;
/// let mut s = FnSource::new(move || -> Result<Option<u32>, &'static str> {
///     n += 1;
///     Ok((n <= 2).then_some(n))
/// });
/// assert_eq!(s.pull()?, Some(1));
/// assert_eq!(s.pull()?, Some(2));
/// assert_eq!(s.pull()?, None);
/// # Ok::<(), &'static str>(())
/// ```
pub struct FnSource<F, T, E> {
    func: F,
    _marker: PhantomData<PullFn<T, E>>,
}

/// Signature marker: makes `FnSource` covariant in `T`/`E` and `Send`
/// regardless of them, like the closure it wraps.
type PullFn<T, E> = fn() -> Result<Option<T>, E>;

impl<F, T, E> FnSource<F, T, E>
where
    F: FnMut() -> Result<Option<T>, E>,
{
    /// Wrap a closure.
    pub const fn new(func: F) -> Self {
        Self {
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, T, E> core::fmt::Debug for FnSource<F, T, E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FnSource").finish_non_exhaustive()
    }
}

impl<F, T, E> Source for FnSource<F, T, E>
where
    F: FnMut() -> Result<Option<T>, E>,
    E: StageError,
{
    type Item = T;
    type Error = E;

    #[inline]
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        (self.func)()
    }
}

/// [`Source`] that drains `first`, closes it, then continues with
/// `second`. Errors from either side are boxed.
///
/// # Example
///
/// ```
/// use pipe_io::source::{ChainSource, IterSource, Source};
///
/// let mut s = ChainSource::new(IterSource::new([1, 2]), IterSource::new([3]));
/// assert_eq!(s.pull()?, Some(1));
/// assert_eq!(s.pull()?, Some(2));
/// assert_eq!(s.pull()?, Some(3));
/// assert_eq!(s.pull()?, None);
/// # Ok::<(), pipe_io::BoxError>(())
/// ```
#[derive(Debug)]
pub struct ChainSource<A, B> {
    first: Option<A>,
    second: B,
}

impl<A, B> ChainSource<A, B>
where
    A: Source,
    B: Source<Item = A::Item>,
{
    /// Chain two sources.
    pub const fn new(first: A, second: B) -> Self {
        Self {
            first: Some(first),
            second,
        }
    }

    /// Close and drop the first source once it is exhausted.
    fn retire_first(&mut self) -> Result<(), BoxError> {
        match self.first.take() {
            Some(mut first) => first.close().map_err(Into::into),
            None => Ok(()),
        }
    }
}

impl<A, B> Source for ChainSource<A, B>
where
    A: Source,
    B: Source<Item = A::Item>,
{
    type Item = A::Item;
    type Error = BoxError;

    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(first) = self.first.as_mut() {
            if let Some(item) = first.pull().map_err(Into::into)? {
                return Ok(Some(item));
            }
            self.retire_first()?;
        }
        self.second.pull().map_err(Into::into)
    }

    fn pull_timeout(&mut self, timeout: Duration) -> Result<Pull<Self::Item>, Self::Error> {
        #[cfg(feature = "std")]
        let started = std::time::Instant::now();
        #[cfg_attr(not(feature = "std"), allow(unused_mut))]
        let mut remaining = timeout;
        if let Some(first) = self.first.as_mut() {
            match first.pull_timeout(timeout).map_err(Into::into)? {
                Pull::End => self.retire_first()?,
                other => return Ok(other),
            }
            // Only the rest of the timeout is left for the second source
            // (without `std` there is no clock, so it gets the full timeout
            // again at the switch).
            #[cfg(feature = "std")]
            {
                remaining = timeout.saturating_sub(started.elapsed());
            }
        }
        self.second.pull_timeout(remaining).map_err(Into::into)
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        let first = self.retire_first();
        let second = self.second.close().map_err(Into::into);
        first.and(second)
    }
}

#[cfg(feature = "std")]
pub use self::std_sources::*;

#[cfg(feature = "std")]
mod std_sources {
    use alloc::string::String;
    use alloc::vec::Vec;
    use core::fmt;
    use core::time::Duration;
    use std::io::{self, BufRead, BufReader, Read};
    use std::sync::mpsc::{Receiver, RecvTimeoutError};

    use super::{Pull, Source};

    /// [`Source`] over an [`std::sync::mpsc::Receiver`].
    ///
    /// Reports end of stream once every sender is dropped and the channel
    /// is drained. Implements [`Source::pull_timeout`] with
    /// `recv_timeout`, so it cooperates with the idle tick.
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[derive(Debug)]
    pub struct ChannelSource<T> {
        rx: Receiver<T>,
    }

    impl<T> ChannelSource<T> {
        /// Wrap a receiver.
        #[must_use]
        pub const fn new(rx: Receiver<T>) -> Self {
            Self { rx }
        }
    }

    impl<T> Source for ChannelSource<T> {
        type Item = T;
        type Error = core::convert::Infallible;

        fn pull(&mut self) -> Result<Option<T>, Self::Error> {
            Ok(self.rx.recv().ok())
        }

        fn pull_timeout(&mut self, timeout: Duration) -> Result<Pull<T>, Self::Error> {
            Ok(match self.rx.recv_timeout(timeout) {
                Ok(item) => Pull::Item(item),
                Err(RecvTimeoutError::Timeout) => Pull::Idle,
                Err(RecvTimeoutError::Disconnected) => Pull::End,
            })
        }
    }

    /// Default line-length limit of [`ReaderSource`] and
    /// [`ByteLinesSource`]: 1 MiB.
    pub const DEFAULT_MAX_LINE_LEN: usize = 1 << 20;

    /// What a line reader does with a line longer than its limit.
    #[non_exhaustive]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub enum LineOverflow {
        /// Fail with [`ReaderError::LineTooLong`] (ends the run). Default.
        #[default]
        Error,
        /// Discard the line and continue with the next one. Memory stays
        /// bounded by the limit even for an endless line.
        Skip,
        /// Keep the first `limit` bytes and discard the rest of the line.
        /// For [`ReaderSource`] the cut may split a UTF-8 sequence, which
        /// is then handled by the UTF-8 policy.
        Truncate,
    }

    /// Error returned by [`ReaderSource`] and [`ByteLinesSource`].
    #[non_exhaustive]
    #[derive(Debug)]
    pub enum ReaderError {
        /// The underlying reader failed.
        Io(io::Error),
        /// A line exceeded the configured limit under
        /// [`LineOverflow::Error`].
        LineTooLong {
            /// 1-based line number.
            line: u64,
            /// The configured limit in bytes.
            limit: usize,
        },
        /// A line was not valid UTF-8 and lossy decoding is off.
        InvalidUtf8 {
            /// 1-based line number.
            line: u64,
        },
    }

    impl fmt::Display for ReaderError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Io(e) => write!(f, "read failed: {e}"),
                Self::LineTooLong { line, limit } => {
                    write!(f, "line {line} is longer than the {limit}-byte limit")
                }
                Self::InvalidUtf8 { line } => write!(f, "line {line} is not valid UTF-8"),
            }
        }
    }

    impl std::error::Error for ReaderError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Io(e) => Some(e),
                Self::LineTooLong { .. } | Self::InvalidUtf8 { .. } => None,
            }
        }
    }

    impl From<io::Error> for ReaderError {
        fn from(value: io::Error) -> Self {
            Self::Io(value)
        }
    }

    /// Bounded line splitter shared by the two reader sources.
    struct LineReader<R> {
        reader: BufReader<R>,
        max_len: usize,
        overflow: LineOverflow,
        line_no: u64,
    }

    impl<R: Read> LineReader<R> {
        fn new(reader: BufReader<R>) -> Self {
            Self {
                reader,
                max_len: DEFAULT_MAX_LINE_LEN,
                overflow: LineOverflow::Error,
                line_no: 0,
            }
        }

        /// Read the next line without its `\n` / `\r\n` terminator.
        /// Never buffers more than `max_len` bytes of one line.
        fn next_line(&mut self) -> Result<Option<Vec<u8>>, ReaderError> {
            loop {
                match self.read_one()? {
                    ReadLine::Eof => return Ok(None),
                    ReadLine::Skipped => {}
                    ReadLine::Line(line) => return Ok(Some(line)),
                }
            }
        }

        fn read_one(&mut self) -> Result<ReadLine, ReaderError> {
            let mut line = Vec::new();
            let mut saw_bytes = false;
            let mut overflowed = false;
            loop {
                let available = match self.reader.fill_buf() {
                    Ok(buf) => buf,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(ReaderError::Io(e)),
                };
                if available.is_empty() {
                    if !saw_bytes {
                        return Ok(ReadLine::Eof);
                    }
                    break;
                }
                saw_bytes = true;
                let newline = available.iter().position(|&b| b == b'\n');
                let (chunk_len, consumed) = match newline {
                    Some(i) => (i, i + 1),
                    None => (available.len(), available.len()),
                };
                if !overflowed {
                    let room = self.max_len - line.len();
                    if chunk_len <= room {
                        line.extend_from_slice(&available[..chunk_len]);
                    } else {
                        overflowed = true;
                        match self.overflow {
                            LineOverflow::Error => {
                                self.line_no += 1;
                                return Err(ReaderError::LineTooLong {
                                    line: self.line_no,
                                    limit: self.max_len,
                                });
                            }
                            LineOverflow::Truncate => {
                                line.extend_from_slice(&available[..room]);
                            }
                            LineOverflow::Skip => line = Vec::new(),
                        }
                    }
                }
                self.reader.consume(consumed);
                if newline.is_some() {
                    break;
                }
            }
            self.line_no += 1;
            if overflowed && self.overflow == LineOverflow::Skip {
                return Ok(ReadLine::Skipped);
            }
            if !overflowed && line.last() == Some(&b'\r') {
                let _carriage_return = line.pop();
            }
            Ok(ReadLine::Line(line))
        }
    }

    enum ReadLine {
        Eof,
        Skipped,
        Line(Vec<u8>),
    }

    impl<R> fmt::Debug for LineReader<R> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("LineReader")
                .field("max_len", &self.max_len)
                .field("overflow", &self.overflow)
                .field("line_no", &self.line_no)
                .finish_non_exhaustive()
        }
    }

    /// Line-oriented [`Source`] of `String`s over any [`std::io::Read`].
    ///
    /// Lines are split on `\n`; a trailing `\r` is removed. Input is
    /// treated as untrusted: a single line never buffers more than
    /// [`ReaderSource::max_line_len`] bytes (1 MiB by default), and what
    /// happens to a longer line is set by [`ReaderSource::on_overflow`].
    /// Invalid UTF-8 fails with [`ReaderError::InvalidUtf8`] unless
    /// [`ReaderSource::lossy`] is enabled.
    ///
    /// # Example
    ///
    /// ```
    /// use pipe_io::source::{LineOverflow, ReaderSource, Source};
    ///
    /// let input = "short\nthis line is too long\nok\n";
    /// let mut s = ReaderSource::new(input.as_bytes())
    ///     .max_line_len(8)
    ///     .on_overflow(LineOverflow::Skip);
    /// assert_eq!(s.pull()?.as_deref(), Some("short"));
    /// assert_eq!(s.pull()?.as_deref(), Some("ok"));
    /// assert_eq!(s.pull()?, None);
    /// # Ok::<(), pipe_io::source::ReaderError>(())
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[derive(Debug)]
    pub struct ReaderSource<R> {
        lines: LineReader<R>,
        lossy: bool,
    }

    impl<R: Read> ReaderSource<R> {
        /// Wrap a reader with the default 8 KiB read buffer.
        pub fn new(reader: R) -> Self {
            Self {
                lines: LineReader::new(BufReader::new(reader)),
                lossy: false,
            }
        }

        /// Wrap a reader with a read buffer of `capacity` bytes. Values
        /// below 1 are raised to 1 (a zero-sized buffer could never read).
        pub fn with_capacity(capacity: usize, reader: R) -> Self {
            Self {
                lines: LineReader::new(BufReader::with_capacity(capacity.max(1), reader)),
                lossy: false,
            }
        }

        /// Set the longest accepted line in bytes, excluding the
        /// terminator. Values below 1 are raised to 1.
        #[must_use]
        pub fn max_line_len(mut self, limit: usize) -> Self {
            self.lines.max_len = limit.max(1);
            self
        }

        /// Choose what happens to a line longer than the limit.
        #[must_use]
        pub const fn on_overflow(mut self, policy: LineOverflow) -> Self {
            self.lines.overflow = policy;
            self
        }

        /// Replace invalid UTF-8 with U+FFFD instead of failing.
        #[must_use]
        pub const fn lossy(mut self, lossy: bool) -> Self {
            self.lossy = lossy;
            self
        }
    }

    impl<R: Read> Source for ReaderSource<R> {
        type Item = String;
        type Error = ReaderError;

        fn pull(&mut self) -> Result<Option<String>, ReaderError> {
            let Some(bytes) = self.lines.next_line()? else {
                return Ok(None);
            };
            match String::from_utf8(bytes) {
                Ok(line) => Ok(Some(line)),
                Err(err) if self.lossy => {
                    Ok(Some(String::from_utf8_lossy(err.as_bytes()).into_owned()))
                }
                Err(_) => Err(ReaderError::InvalidUtf8 {
                    line: self.lines.line_no,
                }),
            }
        }
    }

    /// Line-oriented [`Source`] of raw `Vec<u8>` lines over any
    /// [`std::io::Read`]. Same splitting and limits as [`ReaderSource`],
    /// without UTF-8 decoding.
    ///
    /// # Example
    ///
    /// ```
    /// use pipe_io::source::{ByteLinesSource, Source};
    ///
    /// let mut s = ByteLinesSource::new(&b"a\xff\r\nb"[..]);
    /// assert_eq!(s.pull()?, Some(b"a\xff".to_vec()));
    /// assert_eq!(s.pull()?, Some(b"b".to_vec()));
    /// assert_eq!(s.pull()?, None);
    /// # Ok::<(), pipe_io::source::ReaderError>(())
    /// ```
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[derive(Debug)]
    pub struct ByteLinesSource<R> {
        lines: LineReader<R>,
    }

    impl<R: Read> ByteLinesSource<R> {
        /// Wrap a reader with the default 8 KiB read buffer.
        pub fn new(reader: R) -> Self {
            Self {
                lines: LineReader::new(BufReader::new(reader)),
            }
        }

        /// Set the longest accepted line in bytes. Values below 1 are
        /// raised to 1.
        #[must_use]
        pub fn max_line_len(mut self, limit: usize) -> Self {
            self.lines.max_len = limit.max(1);
            self
        }

        /// Choose what happens to a line longer than the limit.
        #[must_use]
        pub const fn on_overflow(mut self, policy: LineOverflow) -> Self {
            self.lines.overflow = policy;
            self
        }
    }

    impl<R: Read> Source for ByteLinesSource<R> {
        type Item = Vec<u8>;
        type Error = ReaderError;

        fn pull(&mut self) -> Result<Option<Vec<u8>>, ReaderError> {
            self.lines.next_line()
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;
    use alloc::vec::Vec;

    #[test]
    fn test_iter_source_exhausts() {
        let mut s = IterSource::new(vec![10, 20, 30]);
        let mut out = Vec::new();
        while let Some(v) = s.pull().unwrap() {
            out.push(v);
        }
        assert_eq!(out, vec![10, 20, 30]);
        s.close().unwrap();
    }

    #[test]
    fn test_default_pull_timeout_delegates_to_pull() {
        let mut s = IterSource::new([1]);
        assert_eq!(
            s.pull_timeout(Duration::from_millis(1)).unwrap(),
            Pull::Item(1)
        );
        assert_eq!(s.pull_timeout(Duration::from_millis(1)).unwrap(), Pull::End);
        assert_eq!(Pull::from(Some(3)), Pull::Item(3));
        assert_eq!(Pull::<u8>::from(None), Pull::End);
    }

    #[test]
    fn test_fn_source_can_fail() {
        let mut state = 0;
        let mut s = FnSource::new(move || -> Result<Option<u32>, &'static str> {
            state += 1;
            if state == 2 {
                Err("nope")
            } else {
                Ok(Some(state))
            }
        });
        assert_eq!(s.pull().unwrap(), Some(1));
        assert!(s.pull().is_err());
        assert_eq!(alloc::format!("{s:?}"), "FnSource { .. }");
    }

    #[test]
    fn test_chain_source_runs_first_then_second() {
        let mut s = ChainSource::new(IterSource::new([1, 2]), IterSource::new([3]));
        assert_eq!(s.pull_timeout(Duration::ZERO).unwrap(), Pull::Item(1));
        assert_eq!(s.pull().unwrap(), Some(2));
        assert_eq!(s.pull_timeout(Duration::ZERO).unwrap(), Pull::Item(3));
        assert_eq!(s.pull().unwrap(), None);
        s.close().unwrap();
    }

    #[test]
    fn test_chain_source_propagates_errors() {
        let failing = FnSource::new(|| -> Result<Option<u8>, &'static str> { Err("first broke") });
        let mut s = ChainSource::new(failing, IterSource::new([1_u8]));
        assert_eq!(s.pull().unwrap_err().to_string(), "first broke");
        struct BadClose;
        impl Source for BadClose {
            type Item = u8;
            type Error = &'static str;
            fn pull(&mut self) -> Result<Option<u8>, &'static str> {
                Ok(None)
            }
            fn close(&mut self) -> Result<(), &'static str> {
                Err("close broke")
            }
        }
        let mut s = ChainSource::new(BadClose, IterSource::new([9_u8]));
        assert_eq!(s.pull().unwrap_err().to_string(), "close broke");
        assert_eq!(
            s.pull().unwrap(),
            Some(9),
            "first source is retired after closing"
        );
        let mut s = ChainSource::new(IterSource::new([1_u8]), BadClose);
        assert_eq!(s.close().unwrap_err().to_string(), "close broke");
    }

    #[cfg(feature = "std")]
    mod std_tests {
        use super::*;
        use std::io::{Cursor, Read};

        fn lines<R: Read>(mut s: ReaderSource<R>) -> Vec<String> {
            let mut out = Vec::new();
            while let Some(l) = s.pull().unwrap() {
                out.push(l);
            }
            out
        }

        #[test]
        fn test_reader_source_splits_lines() {
            let s = ReaderSource::new(Cursor::new("alpha\nbeta\r\ngamma"));
            assert_eq!(lines(s), vec!["alpha", "beta", "gamma"]);
        }

        #[test]
        fn test_reader_source_edge_inputs() {
            assert!(lines(ReaderSource::new(Cursor::new(""))).is_empty());
            assert_eq!(lines(ReaderSource::new(Cursor::new("\n"))), vec![""]);
            assert_eq!(
                lines(ReaderSource::new(Cursor::new("\r\n\r\n"))),
                vec!["", ""]
            );
            assert_eq!(lines(ReaderSource::new(Cursor::new("a\r"))), vec!["a"]);
            // Lines longer than the BufReader buffer are reassembled.
            let long = "x".repeat(50);
            let input = alloc::format!("{long}\nend\n");
            let s = ReaderSource::with_capacity(4, Cursor::new(input));
            assert_eq!(lines(s), vec![long.as_str(), "end"]);
        }

        #[test]
        fn test_reader_source_rejects_overlong_line_by_default() {
            let mut s = ReaderSource::new(Cursor::new("ok\n12345678901\n")).max_line_len(10);
            assert_eq!(s.pull().unwrap().as_deref(), Some("ok"));
            match s.pull() {
                Err(ReaderError::LineTooLong { line: 2, limit: 10 }) => {}
                other => panic!("unexpected {other:?}"),
            }
        }

        #[test]
        fn test_reader_source_exact_limit_is_accepted() {
            let s = ReaderSource::new(Cursor::new("1234567890\n")).max_line_len(10);
            assert_eq!(lines(s), vec!["1234567890"]);
        }

        #[test]
        fn test_reader_source_endless_line_is_bounded() {
            // 8 MiB without a newline: Skip must not buffer it.
            let endless = std::io::repeat(b'a')
                .take(8 << 20)
                .chain(Cursor::new("\nok\n"));
            let s = ReaderSource::with_capacity(64 * 1024, endless)
                .max_line_len(1024)
                .on_overflow(LineOverflow::Skip);
            assert_eq!(lines(s), vec!["ok"]);
        }

        #[test]
        fn test_reader_source_truncate_keeps_prefix() {
            let s = ReaderSource::new(Cursor::new("abcdefgh\r\nxy\n"))
                .max_line_len(4)
                .on_overflow(LineOverflow::Truncate);
            assert_eq!(lines(s), vec!["abcd", "xy"]);
        }

        #[test]
        fn test_reader_source_utf8_policy() {
            let bytes: &[u8] = b"ok\n\xff\xfe\nafter\n";
            let mut strict = ReaderSource::new(bytes);
            assert_eq!(strict.pull().unwrap().as_deref(), Some("ok"));
            match strict.pull() {
                Err(ReaderError::InvalidUtf8 { line: 2 }) => {}
                other => panic!("unexpected {other:?}"),
            }
            let lossy = ReaderSource::new(bytes).lossy(true);
            assert_eq!(lines(lossy), vec!["ok", "\u{fffd}\u{fffd}", "after"]);
        }

        #[test]
        fn test_reader_source_io_error_and_interrupt() {
            struct Flaky(u8);
            impl Read for Flaky {
                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                    self.0 += 1;
                    match self.0 {
                        1 => Err(std::io::ErrorKind::Interrupted.into()),
                        2 => {
                            buf[..2].copy_from_slice(b"a\n");
                            Ok(2)
                        }
                        _ => Err(std::io::Error::other("disk gone")),
                    }
                }
            }
            let mut s = ReaderSource::new(Flaky(0));
            assert_eq!(s.pull().unwrap().as_deref(), Some("a"));
            let err = s.pull().unwrap_err();
            assert!(matches!(err, ReaderError::Io(_)));
            assert!(std::error::Error::source(&err).is_some());
            assert!(err.to_string().contains("disk gone"));
        }

        #[test]
        fn test_byte_lines_source() {
            let mut s = ByteLinesSource::new(Cursor::new(b"a\xff\nlonglonglong\nz".to_vec()))
                .max_line_len(4)
                .on_overflow(LineOverflow::Truncate);
            assert_eq!(s.pull().unwrap(), Some(b"a\xff".to_vec()));
            assert_eq!(s.pull().unwrap(), Some(b"long".to_vec()));
            assert_eq!(s.pull().unwrap(), Some(b"z".to_vec()));
            assert_eq!(s.pull().unwrap(), None);
        }

        #[test]
        fn test_reader_error_display() {
            let e = ReaderError::LineTooLong { line: 3, limit: 9 };
            assert_eq!(e.to_string(), "line 3 is longer than the 9-byte limit");
            assert_eq!(
                ReaderError::InvalidUtf8 { line: 1 }.to_string(),
                "line 1 is not valid UTF-8"
            );
            assert!(std::error::Error::source(&e).is_none());
            let io: ReaderError = std::io::Error::other("x").into();
            assert!(matches!(io, ReaderError::Io(_)));
        }

        #[test]
        fn test_channel_source_terminates_when_sender_dropped() {
            let (tx, rx) = std::sync::mpsc::channel::<i32>();
            let mut s = ChannelSource::new(rx);
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            assert_eq!(s.pull().unwrap(), Some(1));
            assert_eq!(
                s.pull_timeout(Duration::from_millis(1)).unwrap(),
                Pull::Item(2)
            );
            assert_eq!(
                s.pull_timeout(Duration::from_millis(1)).unwrap(),
                Pull::Idle
            );
            drop(tx);
            assert_eq!(s.pull_timeout(Duration::from_millis(1)).unwrap(), Pull::End);
            assert_eq!(s.pull().unwrap(), None);
        }
    }
}
