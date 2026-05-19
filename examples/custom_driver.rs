//! Implementing the [`Driver`] trait for a custom executor.
//!
//! `LoggingDriver` wraps `SyncDriver` and records the wall-clock
//! duration plus the items processed. Real custom drivers can plug
//! into tokio, rayon, glommio, or any other scheduling primitive.
//!
//! Run with:
//!
//! ```text
//! cargo run --example custom_driver
//! ```

use std::time::Instant;

use pipe_io::driver::{Driver, RunStats, SyncDriver};
use pipe_io::{sink::VecSink, Pipeline, Result};

struct LoggingDriver {
    name: &'static str,
}

impl Driver for LoggingDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: pipe_io::Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        let start = Instant::now();
        let result = SyncDriver::new().run(pipeline);
        match &result {
            Ok(stats) => {
                println!(
                    "[{}] ran {} items in {:?}",
                    self.name,
                    stats.items_in,
                    start.elapsed()
                );
            }
            Err(e) => {
                println!("[{}] failed after {:?}: {e}", self.name, start.elapsed());
            }
        }
        result
    }
}

fn main() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();

    Pipeline::from_iter(0..1_000)
        .map(|n: i32| n * 2)
        .filter(|n: &i32| *n % 3 == 0)
        .sink(sink)
        .run_with(LoggingDriver { name: "demo" })
        .expect("pipeline run");

    let out = handle.take();
    println!("collected {} items", out.len());
}
