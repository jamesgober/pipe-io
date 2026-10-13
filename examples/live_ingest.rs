//! Live ingest: a channel-fed pipeline that batches by count or age,
//! flushes on quiet periods, and shuts down gracefully on request.
//!
//! * `idle_tick` lets the age trigger fire while the producer is quiet,
//!   so no batch waits for the next record.
//! * A `buffer` edge moves the (slow) writer onto its own thread.
//! * The observer counts events for metrics; the cancel handle stops the
//!   run and drains what was already received.
//!
//! Run with:
//!
//! ```text
//! cargo run --example live_ingest
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pipe_io::source::ChannelSource;
use pipe_io::{Batch, BatchPolicy, Error, Event, Pipeline, sink::FnSink};

fn main() {
    let (tx, rx) = mpsc::sync_channel::<String>(1024);
    let idle_ticks = Arc::new(AtomicU64::new(0));
    let ticks = Arc::clone(&idle_ticks);

    let mut pipeline = Pipeline::from_source(ChannelSource::new(rx))
        .source_id("events")
        .idle_tick(Duration::from_millis(20))
        .observe(move |event: &Event<'_>| {
            if matches!(event, Event::Idle) {
                ticks.fetch_add(1, Ordering::Relaxed);
            }
        })
        .batch(
            BatchPolicy::new()
                .max_items(100)
                .max_age(Duration::from_millis(50)),
        )
        .buffer(4)
        .stage_id("writer")
        .sink(FnSink::new(
            |batch: Batch<String>| -> Result<(), std::io::Error> {
                println!("wrote batch of {:>3} (first: {})", batch.len(), batch[0]);
                Ok(())
            },
        ));
    let stop = pipeline.cancel_handle();
    let worker = thread::spawn(move || pipeline.run_threaded());

    // A bursty producer: 250 events, a pause, 30 more, then idle.
    for i in 0..250 {
        tx.send(format!("event-{i}")).expect("send");
    }
    thread::sleep(Duration::from_millis(200));
    for i in 250..280 {
        tx.send(format!("event-{i}")).expect("send");
    }
    thread::sleep(Duration::from_millis(200));

    stop.cancel();
    match worker.join().expect("worker thread") {
        Err(Error::Cancelled) => println!("stopped; every received event was written"),
        other => println!("unexpected result: {other:?}"),
    }
    println!(
        "idle ticks observed: {}",
        idle_ticks.load(Ordering::Relaxed)
    );
}
