//! Integration tests for windowing (`v0.5.0`).
//!
//! Each test uses a custom [`Clock`] so behaviour is deterministic,
//! independent of wall-clock progress during the test run.

#![cfg(feature = "std")]

use core::time::Duration;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use pipe_io::sink::VecSink;
use pipe_io::source::IterSource;
use pipe_io::{Clock, Pipeline, Window, WindowPolicy};

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

#[test]
fn tumbling_window_rolls_up_metric() {
    let t0 = Instant::now();
    // Times handed out on each clock read. WindowStage::process reads now()
    // once per item, and flush reads now() once. So we provide 5 process
    // times + 1 flush time.
    let clock = ScriptedClock::new(vec![
        t0,                           // item 1 at t=0
        t0 + Duration::from_secs(2),  // item 2 at t=2
        t0 + Duration::from_secs(9),  // item 3 at t=9
        t0 + Duration::from_secs(11), // item 4 at t=11 - closes [0,10)
        t0 + Duration::from_secs(15), // item 5 at t=15
        t0 + Duration::from_secs(20), // flush
    ]);

    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();

    Pipeline::from_source(IterSource::new(vec![1u32, 2, 3, 4, 5]))
        .window_with(
            WindowPolicy::Tumbling {
                size: Duration::from_secs(10),
            },
            clock,
        )
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");

    let groups = handle.take();
    // First window [0, 10) emits when item 4 at t=11 arrives, with items
    // {1, 2, 3}. Then item 4 starts new window [10, 20); item 5 at t=15
    // is added. At flush (t=20), the window [10, 20) is still open with
    // {4, 5}.
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0], vec![1, 2, 3]);
    assert_eq!(groups[1], vec![4, 5]);
}

#[test]
fn session_window_boundary() {
    let t0 = Instant::now();
    let clock = ScriptedClock::new(vec![
        t0,                           // item 1
        t0 + Duration::from_secs(1),  // item 2 - within session
        t0 + Duration::from_secs(2),  // item 3 - within session
        t0 + Duration::from_secs(30), // item 4 - new session (>5s gap)
        t0 + Duration::from_secs(31), // item 5 - within new session
        t0 + Duration::from_secs(32), // flush
    ]);

    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();

    Pipeline::from_source(IterSource::new(vec![1u32, 2, 3, 4, 5]))
        .window_with(
            WindowPolicy::Session {
                idle: Duration::from_secs(5),
            },
            clock,
        )
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");

    let groups = handle.take();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0], vec![1, 2, 3]);
    assert_eq!(groups[1], vec![4, 5]);
}

#[test]
fn sliding_window_overlaps() {
    let t0 = Instant::now();
    // size=10s, slide=5s. Windows start at t=0, 5, 10, 15, ...
    let clock = ScriptedClock::new(vec![
        t0,                           // item 1 at t=0
        t0 + Duration::from_secs(3),  // item 2 at t=3
        t0 + Duration::from_secs(7),  // item 3 at t=7 - now [5,15) is also active
        t0 + Duration::from_secs(11), // item 4 at t=11 - [0,10) closed, [10,20) active
        t0 + Duration::from_secs(20), // flush at t=20
    ]);

    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();

    Pipeline::from_source(IterSource::new(vec![1u32, 2, 3, 4]))
        .window_with(
            WindowPolicy::Sliding {
                size: Duration::from_secs(10),
                slide: Duration::from_secs(5),
            },
            clock,
        )
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");

    let groups = handle.take();
    // At t=11, window [0,10) ends -> emit {1,2,3}.
    // Active windows at flush time t=20: [5,15) has {3,4}, [10,20) has {4},
    // [15,25) has {} (empty windows still emitted? no - flush only emits
    // non-empty).
    // Wait: at t=11, after closing [0,10), we spawn [5,15) (started at 5,
    // includes items at t>=5), [10,20) (started at 10), and the next
    // spawn check sees next_start=15 > 11. So we have [5,15) and [10,20).
    // Item 4 at t=11 belongs to both. So:
    //   [5,15) holds {3, 4}
    //   [10,20) holds {4}
    // At flush (t=20), [5,15) ends at 15 -> would emit (but flush doesn't
    // re-check time, it just dumps everything non-empty).
    assert_eq!(groups[0], vec![1, 2, 3]);
    // Remaining windows after flush.
    assert!(groups.iter().skip(1).any(|g| *g == vec![3, 4]));
    assert!(groups.iter().skip(1).any(|g| *g == vec![4]));
}

#[test]
fn tumbling_with_no_items_emits_nothing() {
    let t0 = Instant::now();
    let clock = ScriptedClock::new(vec![t0]); // only the flush call

    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();

    Pipeline::from_source(IterSource::new(std::iter::empty::<u32>()))
        .window_with(
            WindowPolicy::Tumbling {
                size: Duration::from_secs(10),
            },
            clock,
        )
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");

    assert!(handle.take().is_empty());
}
