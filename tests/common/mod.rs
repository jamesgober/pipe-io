//! Shared helpers for the integration tests.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use pipe_io::{Clock, Sink, Source};

/// A typed error with a proper `std::error::Error` impl.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fail(pub &'static str);

impl fmt::Display for Fail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fail: {}", self.0)
    }
}

impl std::error::Error for Fail {}

/// Shared, ordered record of lifecycle calls.
#[derive(Clone, Default)]
pub struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    pub fn push(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(entry.into());
    }

    pub fn entries(&self) -> Vec<String> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn count(&self, entry: &str) -> usize {
        self.entries()
            .iter()
            .filter(|e| e.as_str() == entry)
            .count()
    }

    pub fn contains(&self, entry: &str) -> bool {
        self.count(entry) > 0
    }
}

/// Which lifecycle call of a recording component fails (or panics).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Fault {
    #[default]
    None,
    /// The n-th (1-based) `pull`/`write` fails.
    At(u32),
    /// The n-th (1-based) `pull`/`write` panics.
    PanicAt(u32),
    Flush,
    Close,
    PanicFlush,
}

/// Source yielding `1..=n` and logging `source.close`.
pub struct RecSource {
    pub log: Log,
    pub n: u32,
    pub next: u32,
    pub fault: Fault,
}

impl RecSource {
    pub fn new(log: &Log, n: u32) -> Self {
        Self {
            log: log.clone(),
            n,
            next: 0,
            fault: Fault::None,
        }
    }

    pub fn fault(mut self, fault: Fault) -> Self {
        self.fault = fault;
        self
    }
}

impl Source for RecSource {
    type Item = u32;
    type Error = Fail;

    fn pull(&mut self) -> Result<Option<u32>, Fail> {
        self.next += 1;
        match self.fault {
            Fault::At(k) if k == self.next => return Err(Fail("source")),
            Fault::PanicAt(k) if k == self.next => panic!("source panic"),
            _ => {}
        }
        Ok((self.next <= self.n).then_some(self.next))
    }

    fn close(&mut self) -> Result<(), Fail> {
        self.log.push("source.close");
        if self.fault == Fault::Close {
            return Err(Fail("source close"));
        }
        Ok(())
    }
}

/// Sink logging every lifecycle call under `name`.
pub struct RecSink<T> {
    pub log: Log,
    pub name: &'static str,
    pub fault: Fault,
    pub items: Arc<Mutex<Vec<T>>>,
    writes: u32,
}

impl<T> RecSink<T> {
    pub fn new(log: &Log, name: &'static str) -> Self {
        Self {
            log: log.clone(),
            name,
            fault: Fault::None,
            items: Arc::default(),
            writes: 0,
        }
    }

    pub fn fault(mut self, fault: Fault) -> Self {
        self.fault = fault;
        self
    }

    pub fn items(&self) -> Arc<Mutex<Vec<T>>> {
        Arc::clone(&self.items)
    }
}

impl<T: Send + 'static> Sink for RecSink<T> {
    type Item = T;
    type Error = Fail;

    fn write(&mut self, item: T) -> Result<(), Fail> {
        self.writes += 1;
        self.log.push(format!("{}.write", self.name));
        match self.fault {
            Fault::At(k) if k == self.writes => return Err(Fail("sink write")),
            Fault::PanicAt(k) if k == self.writes => panic!("sink panic"),
            _ => {}
        }
        self.items.lock().unwrap().push(item);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Fail> {
        self.log.push(format!("{}.flush", self.name));
        match self.fault {
            Fault::Flush => Err(Fail("sink flush")),
            Fault::PanicFlush => panic!("sink flush panic"),
            _ => Ok(()),
        }
    }

    fn close(&mut self) -> Result<(), Fail> {
        self.log.push(format!("{}.close", self.name));
        if self.fault == Fault::Close {
            return Err(Fail("sink close"));
        }
        Ok(())
    }
}

/// Clock that hands out a scripted sequence of instants, then repeats the
/// last one.
#[derive(Clone)]
pub struct ScriptedClock {
    times: Arc<Mutex<std::vec::IntoIter<Instant>>>,
    last: Arc<Mutex<Instant>>,
}

impl ScriptedClock {
    pub fn new(times: Vec<Instant>) -> Self {
        let last = *times.last().expect("at least one time");
        Self {
            times: Arc::new(Mutex::new(times.into_iter())),
            last: Arc::new(Mutex::new(last)),
        }
    }
}

impl Clock for ScriptedClock {
    fn now(&self) -> Instant {
        let mut iter = self.times.lock().unwrap();
        let t = iter.next().unwrap_or(*self.last.lock().unwrap());
        *self.last.lock().unwrap() = t;
        t
    }
}
