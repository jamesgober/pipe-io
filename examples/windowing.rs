//! Tumbling windows over a metric stream, per key and overall.
//!
//! A [`ManualClock`] drives time so the output is reproducible; a real
//! pipeline calls `.window(policy)` and uses the system clock.
//!
//! Run with:
//!
//! ```text
//! cargo run --example windowing
//! ```

use std::time::{Duration, Instant};

use pipe_io::{ManualClock, Pipeline, Step, Window, WindowPolicy, sink::VecSink};

fn main() {
    let clock = ManualClock::new(Instant::now());
    let readings = vec![
        ("cpu", 5_u64),
        ("mem", 7),
        ("cpu", 3),
        ("cpu", 8),
        ("mem", 2),
        ("cpu", 4),
    ];
    let gaps = [0_u64, 3, 6, 3, 6, 4]; // seconds before each reading

    let sink = VecSink::<(&str, u64)>::new();
    let handle = sink.handle();
    let mut pipeline = Pipeline::from_iter(readings)
        .window_by_key_with(
            |r: &(&str, u64)| r.0,
            WindowPolicy::tumbling(Duration::from_secs(10)).max_keys(1_000),
            clock.clone(),
        )
        .map(|(key, w): (&str, Window<(&str, u64)>)| (key, w.iter().map(|r| r.1).sum()))
        .sink(sink);

    for gap in gaps {
        clock.advance(Duration::from_secs(gap));
        assert_eq!(pipeline.step().expect("step"), Step::Item);
    }
    assert_eq!(pipeline.step().expect("step"), Step::Done);

    let rollups = handle.take();
    println!("per-key 10s rollups: {rollups:?}");
    // Windows close when the next reading for the same key arrives:
    // cpu [0,10) = 5+3 at t=12, mem [0,10) = 7 at t=18, cpu [10,20) = 8
    // at t=22. End of stream flushes cpu [20,30) = 4 and mem [10,20) = 2.
    assert_eq!(
        rollups,
        vec![("cpu", 8), ("mem", 7), ("cpu", 8), ("cpu", 4), ("mem", 2)]
    );
}
