//! Tumbling window over a synthetic metric stream. Each window emits
//! a rollup (sum) for the items that fell inside it.
//!
//! Uses a deterministic clock so the example output is reproducible.
//! In a real pipeline, you would use the default [`pipe_io::SystemClock`]
//! by calling `.window(policy)` instead of `.window_with(policy, clock)`.
//!
//! Run with:
//!
//! ```text
//! cargo run --example windowing
//! ```

use core::time::Duration;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use pipe_io::source::IterSource;
use pipe_io::{sink::VecSink, Clock, Pipeline, Window, WindowPolicy};

#[derive(Clone)]
struct ScriptedClock {
    times: Arc<Mutex<std::vec::IntoIter<Instant>>>,
    fallback: Arc<Mutex<Instant>>,
}

impl ScriptedClock {
    fn new(times: Vec<Instant>) -> Self {
        let last = *times.last().expect("at least one time");
        Self {
            times: Arc::new(Mutex::new(times.into_iter())),
            fallback: Arc::new(Mutex::new(last)),
        }
    }
}

impl Clock for ScriptedClock {
    fn now(&self) -> Instant {
        let mut iter = self.times.lock().unwrap();
        if let Some(t) = iter.next() {
            *self.fallback.lock().unwrap() = t;
            t
        } else {
            *self.fallback.lock().unwrap()
        }
    }
}

fn main() {
    let t0 = Instant::now();
    // 6 items scattered across 25 seconds; tumbling window size 10s.
    // Plus a final flush at t=25.
    let clock = ScriptedClock::new(vec![
        t0,                           // item 1 at t=0
        t0 + Duration::from_secs(3),  // item 2 at t=3
        t0 + Duration::from_secs(9),  // item 3 at t=9
        t0 + Duration::from_secs(12), // item 4 at t=12 - closes window [0,10)
        t0 + Duration::from_secs(18), // item 5 at t=18
        t0 + Duration::from_secs(22), // item 6 at t=22 - closes window [10,20)
        t0 + Duration::from_secs(25), // flush
    ]);

    let sink = VecSink::<u64>::new();
    let handle = sink.handle();

    Pipeline::from_source(IterSource::new(vec![5u64, 7, 3, 8, 2, 4]))
        .window_with(
            WindowPolicy::Tumbling {
                size: Duration::from_secs(10),
            },
            clock,
        )
        .map(|w: Window<u64>| w.into_inner().iter().sum::<u64>())
        .sink(sink)
        .run()
        .expect("pipeline run");

    let rollups = handle.take();
    println!("emitted {} window rollups: {:?}", rollups.len(), rollups);
    // Window [0,10): 5+7+3 = 15. Window [10,20): 8+2 = 10. Window [20,25): 4.
    assert_eq!(rollups, vec![15, 10, 4]);
}
