//! The [`Source`] trait and built-in source adapters.
//!
//! A source produces items one at a time via [`Source::pull`]. The
//! driver pulls until `Ok(None)`, then invokes [`Source::close`] before
//! flushing downstream stages.

// `FnSource` carries a `PhantomData<fn() -> Result<Option<T>, E>>`
// to fix variance and Send-ness. Clippy considers it complex; the
// shape mirrors `FnMut() -> Result<Option<T>, E>` deliberately.
#![allow(clippy::type_complexity)]

use crate::error::StageError;

#[cfg(feature = "std")]
use alloc::string::String;
use core::marker::PhantomData;

/// Producer at the head of a pipeline.
pub trait Source {
    /// Type of item produced.
    type Item;
    /// Error type the source can return.
    type Error: StageError;

    /// Pull the next item.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if the source fails. The driver
    /// wraps this in [`crate::Error::Source`].
    ///
    /// `Ok(None)` signals end-of-stream.
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error>;

    /// Close the source. Default impl does nothing.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if cleanup fails.
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// [`Source`] adapter over any [`IntoIterator`].
///
/// # Example
///
/// ```
/// use pipe_io::source::{IterSource, Source};
///
/// let mut s = IterSource::new(vec![1, 2, 3]);
/// assert_eq!(s.pull().unwrap(), Some(1));
/// assert_eq!(s.pull().unwrap(), Some(2));
/// assert_eq!(s.pull().unwrap(), Some(3));
/// assert_eq!(s.pull().unwrap(), None);
/// ```
pub struct IterSource<I: Iterator> {
    iter: I,
}

impl<I> IterSource<I>
where
    I: Iterator,
{
    /// Wrap an iterator into a source.
    pub fn new<II>(into_iter: II) -> Self
    where
        II: IntoIterator<IntoIter = I>,
    {
        Self {
            iter: into_iter.into_iter(),
        }
    }
}

/// An error type that no `IterSource` ever returns.
#[derive(Debug)]
pub enum Infallible {}

impl core::fmt::Display for Infallible {
    fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {}
    }
}

impl<I> Source for IterSource<I>
where
    I: Iterator,
{
    type Item = I::Item;
    type Error = Infallible;

    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(self.iter.next())
    }
}

/// [`Source`] adapter over a closure that produces fallible items.
///
/// The closure returns `Ok(Some(item))` to emit, `Ok(None)` to signal
/// end-of-stream, or `Err(E)` to raise a source error.
///
/// # Example
///
/// ```
/// use pipe_io::source::{FnSource, Source};
///
/// let mut n = 0u32;
/// let mut s = FnSource::new(move || -> Result<Option<u32>, &'static str> {
///     n += 1;
///     if n <= 3 { Ok(Some(n)) } else { Ok(None) }
/// });
///
/// assert_eq!(s.pull().unwrap(), Some(1));
/// assert_eq!(s.pull().unwrap(), Some(2));
/// assert_eq!(s.pull().unwrap(), Some(3));
/// assert_eq!(s.pull().unwrap(), None);
/// ```
pub struct FnSource<F, T, E> {
    func: F,
    _marker: PhantomData<fn() -> Result<Option<T>, E>>,
}

impl<F, T, E> FnSource<F, T, E>
where
    F: FnMut() -> Result<Option<T>, E>,
{
    /// Wrap a closure into a source.
    pub fn new(func: F) -> Self {
        Self {
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, T, E> Source for FnSource<F, T, E>
where
    F: FnMut() -> Result<Option<T>, E>,
    E: StageError,
{
    type Item = T;
    type Error = E;

    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        (self.func)()
    }
}

/// [`Source`] adapter over an [`std::sync::mpsc::Receiver`].
///
/// `pull` returns `Ok(None)` when the channel is empty AND all senders
/// have been dropped.
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub struct ChannelSource<T> {
    rx: std::sync::mpsc::Receiver<T>,
}

#[cfg(feature = "std")]
impl<T> ChannelSource<T> {
    /// Wrap a receiver into a source.
    #[must_use]
    pub fn new(rx: std::sync::mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

#[cfg(feature = "std")]
impl<T> Source for ChannelSource<T>
where
    T: 'static,
{
    type Item = T;
    type Error = Infallible;

    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        match self.rx.recv() {
            Ok(item) => Ok(Some(item)),
            Err(_) => Ok(None),
        }
    }
}

/// Line-buffered [`Source`] over any [`std::io::Read`]. Each `pull`
/// returns one line (without the trailing newline).
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub struct ReaderSource<R: std::io::Read> {
    reader: std::io::BufReader<R>,
    buf: String,
}

#[cfg(feature = "std")]
impl<R: std::io::Read> ReaderSource<R> {
    /// Wrap a reader into a line-buffered source.
    pub fn new(reader: R) -> Self {
        Self {
            reader: std::io::BufReader::new(reader),
            buf: String::new(),
        }
    }

    /// Wrap a reader with a specific BufReader capacity.
    pub fn with_capacity(capacity: usize, reader: R) -> Self {
        Self {
            reader: std::io::BufReader::with_capacity(capacity, reader),
            buf: String::new(),
        }
    }
}

#[cfg(feature = "std")]
impl<R: std::io::Read> Source for ReaderSource<R> {
    type Item = String;
    type Error = std::io::Error;

    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        use std::io::BufRead;
        self.buf.clear();
        let read = self.reader.read_line(&mut self.buf)?;
        if read == 0 {
            return Ok(None);
        }
        if self.buf.ends_with('\n') {
            self.buf.pop();
            if self.buf.ends_with('\r') {
                self.buf.pop();
            }
        }
        Ok(Some(core::mem::take(&mut self.buf)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;

    #[test]
    fn iter_source_exhausts() {
        let mut s = IterSource::new(vec![10, 20, 30]);
        let mut out = Vec::new();
        while let Some(v) = s.pull().unwrap() {
            out.push(v);
        }
        assert_eq!(out, vec![10, 20, 30]);
    }

    #[test]
    fn fn_source_can_fail() {
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
    }

    #[cfg(feature = "std")]
    #[test]
    fn reader_source_splits_lines() {
        use std::io::Cursor;
        let data = "alpha\nbeta\r\ngamma";
        let mut s = ReaderSource::new(Cursor::new(data));
        assert_eq!(s.pull().unwrap().as_deref(), Some("alpha"));
        assert_eq!(s.pull().unwrap().as_deref(), Some("beta"));
        assert_eq!(s.pull().unwrap().as_deref(), Some("gamma"));
        assert_eq!(s.pull().unwrap(), None);
    }

    #[cfg(feature = "std")]
    #[test]
    fn channel_source_terminates_when_sender_dropped() {
        let (tx, rx) = std::sync::mpsc::channel::<i32>();
        let mut s = ChannelSource::new(rx);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);
        assert_eq!(s.pull().unwrap(), Some(1));
        assert_eq!(s.pull().unwrap(), Some(2));
        assert_eq!(s.pull().unwrap(), None);
    }
}
