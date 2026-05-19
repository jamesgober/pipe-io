//! Implementing the [`Source`] trait directly. Useful when the data
//! origin doesn't fit `IterSource`, `FnSource`, `ChannelSource`, or
//! `ReaderSource`.
//!
//! `Fibonacci` produces Fibonacci numbers up to `limit`. It's stateful
//! and would be awkward to express as a closure capturing two `u64`s,
//! so a dedicated struct is the clearest fit.
//!
//! Run with:
//!
//! ```text
//! cargo run --example custom_source
//! ```

use pipe_io::source::Infallible;
use pipe_io::{sink::VecSink, Pipeline, Source};

struct Fibonacci {
    limit: u64,
    a: u64,
    b: u64,
}

impl Fibonacci {
    fn new(limit: u64) -> Self {
        Self { limit, a: 0, b: 1 }
    }
}

impl Source for Fibonacci {
    type Item = u64;
    type Error = Infallible;

    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if self.a > self.limit {
            return Ok(None);
        }
        let current = self.a;
        let next = self.a + self.b;
        self.a = self.b;
        self.b = next;
        Ok(Some(current))
    }
}

fn main() {
    let sink = VecSink::<u64>::new();
    let handle = sink.handle();

    Pipeline::from_source(Fibonacci::new(100))
        .filter(|n: &u64| *n != 0)
        .sink(sink)
        .run()
        .expect("pipeline run");

    let out = handle.take();
    println!("Fibonacci numbers up to 100: {out:?}");
    assert_eq!(out, vec![1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]);
}
