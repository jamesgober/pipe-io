//! Pipelined parallelism with bounded edges.
//!
//! Every `.buffer(capacity)` splits the pipeline: under `run_threaded`
//! each segment runs on its own thread and the segments are connected by
//! bounded channels. Here a slow "parse" stage and a slow "write" stage
//! overlap instead of running back to back, and the source can never run
//! more than a few items ahead of the writer.
//!
//! Run with:
//!
//! ```text
//! cargo run --release --example threaded
//! ```

use std::thread;
use std::time::{Duration, Instant};

use pipe_io::{Pipeline, sink::FnSink};

fn slow(work: Duration) {
    thread::sleep(work);
}

fn main() {
    let items = 200_u32;
    let step = Duration::from_micros(500);

    let sync = {
        let started = Instant::now();
        Pipeline::from_iter(0..items)
            .map(move |n: u32| {
                slow(step); // parse
                n
            })
            .sink(FnSink::new(move |_n: u32| -> Result<(), &'static str> {
                slow(step); // write
                Ok(())
            }))
            .run()
            .expect("sync run");
        started.elapsed()
    };

    let threaded = {
        let started = Instant::now();
        let stats = Pipeline::from_iter(0..items)
            .buffer(8)
            .map(move |n: u32| {
                slow(step); // parse, on thread 2
                n
            })
            .buffer(8)
            .sink(FnSink::new(move |_n: u32| -> Result<(), &'static str> {
                slow(step); // write, on thread 3
                Ok(())
            }))
            .run_threaded()
            .expect("threaded run");
        assert_eq!(stats.items_out, u64::from(items));
        started.elapsed()
    };

    println!("single thread : {sync:?}");
    println!("three threads : {threaded:?} (parse and write overlap)");
}
