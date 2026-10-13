//! Running pipelines on a rayon thread pool.
//!
//! `RayonDriver` executes a pipeline on a pool thread instead of the
//! caller's. `run_all` runs many independent pipelines (for example one
//! per input file or partition) in parallel on the same bounded pool,
//! which caps the number of threads no matter how many pipelines there
//! are.
//!
//! Run with:
//!
//! ```text
//! cargo run --example rayon_driver
//! ```

use std::sync::Arc;

use pipe_io::driver::{Driver, RunStats};
use pipe_io::{Pipeline, Result, Source, sink::VecSink};
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

/// Runs a pipeline on a pool thread; the caller waits for the result.
struct RayonDriver {
    pool: Arc<ThreadPool>,
}

impl Driver for RayonDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        // `install` runs the closure on the pool and propagates panics.
        self.pool.install(move || pipeline.run())
    }
}

/// Run independent pipelines in parallel on the pool.
fn run_all<S>(pool: &ThreadPool, pipelines: Vec<Pipeline<S>>) -> Vec<Result<RunStats>>
where
    S: Source + Send + 'static,
    S::Item: Send + 'static,
{
    pool.install(|| pipelines.into_par_iter().map(Pipeline::run).collect())
}

fn main() {
    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(4)
            .thread_name(|i| format!("pipe-pool-{i}"))
            .build()
            .expect("rayon pool"),
    );

    // One pipeline through the Driver trait.
    let sink = VecSink::new();
    let out = sink.handle();
    let stats = Pipeline::from_iter(0..10)
        .filter(|n: &u32| n % 2 == 0)
        .sink(sink)
        .run_with(RayonDriver {
            pool: Arc::clone(&pool),
        })
        .expect("pipeline run");
    println!("driver: {} items -> {:?}", stats.items_out, out.take());

    // Sixteen partitions, four threads.
    let handles: Vec<_> = (0..16).map(|_| VecSink::<u64>::new()).collect();
    let outputs: Vec<_> = handles.iter().map(VecSink::handle).collect();
    let pipelines: Vec<_> = handles
        .into_iter()
        .enumerate()
        .map(|(partition, sink)| {
            let base = partition as u64 * 1_000;
            Pipeline::from_iter(base..base + 1_000)
                .map(|n: u64| n * 2)
                .sink(sink)
        })
        .collect();
    let results = run_all(&pool, pipelines);
    let total: u64 = results
        .iter()
        .map(|r| r.as_ref().expect("partition").items_out)
        .sum();
    println!("run_all: {} partitions, {total} items", results.len());
    assert_eq!(total, 16_000);
    assert!(outputs.iter().all(|h| h.len() == 1_000));
}
