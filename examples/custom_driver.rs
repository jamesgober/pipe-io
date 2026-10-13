//! Implementing the [`Driver`] trait on top of `Pipeline::step`.
//!
//! `ProgressDriver` runs the pipeline item by item, enforces a deadline,
//! and reports progress every `every` items. Nothing here is special to
//! this example: any executor can drive a pipeline the same way.
//!
//! Run with:
//!
//! ```text
//! cargo run --example custom_driver
//! ```

use std::time::{Duration, Instant};

use pipe_io::driver::{Driver, RunStats};
use pipe_io::{Pipeline, Result, Source, Step, sink::VecSink};

struct ProgressDriver {
    every: u64,
    deadline: Duration,
}

impl Driver for ProgressDriver {
    fn run<S>(self, mut pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        let started = Instant::now();
        loop {
            match pipeline.step()? {
                Step::Done => return Ok(pipeline.stats()),
                _ if started.elapsed() > self.deadline => {
                    println!("deadline reached, finishing early");
                    return pipeline.finish();
                }
                _ => {
                    let stats = pipeline.stats();
                    if stats.items_in % self.every == 0 {
                        println!("progress: {} in, {} out", stats.items_in, stats.items_out);
                    }
                }
            }
        }
    }
}

fn main() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();

    let stats = Pipeline::from_iter(0..1_000)
        .map(|n: i32| n * 2)
        .filter(|n: &i32| n % 3 == 0)
        .sink(sink)
        .run_with(ProgressDriver {
            every: 250,
            deadline: Duration::from_secs(10),
        })
        .expect("pipeline run");

    println!(
        "collected {} items in {:?}",
        handle.take().len(),
        stats.duration
    );
}
