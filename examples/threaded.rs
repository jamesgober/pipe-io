//! Driving a pipeline on a background thread via `ThreadedDriver`.
//!
//! The calling thread blocks on `run_threaded()` until the worker
//! finishes. Use this when you want the pipeline body to run off the
//! current thread (for example, alongside an event loop that owns
//! the main thread) without bringing in an async runtime.
//!
//! Run with:
//!
//! ```text
//! cargo run --example threaded
//! ```

use pipe_io::{sink::VecSink, Pipeline};

fn main() {
    let sink = VecSink::<i64>::new();
    let handle = sink.handle();

    let stats = Pipeline::from_iter(1i32..=5)
        .map(|n: i32| i64::from(n) * 100)
        .filter(|n: &i64| *n >= 200)
        .sink(sink)
        .run_threaded()
        .expect("pipeline run on worker thread");

    println!("processed {} items in {:?}", stats.items_in, stats.duration);
    println!("output: {:?}", handle.take());
}
