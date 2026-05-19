#![no_main]
//! Fuzz target: tumbling windows are lossless across any input
//! sequence with a synthetic monotonic clock.

use core::time::Duration;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::VecSink;
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

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let items: Vec<u8> = data.to_vec();
    let t0 = Instant::now();
    let mut times: Vec<Instant> = (0..items.len())
        .map(|i| t0 + Duration::from_millis(i as u64 * 50))
        .collect();
    times.push(t0 + Duration::from_secs(60));
    let clock = ScriptedClock::new(times);

    let sink = VecSink::<Vec<u8>>::new();
    let handle = sink.handle();
    let result = Pipeline::from_iter(items.clone())
        .window_with(
            WindowPolicy::Tumbling {
                size: Duration::from_secs(1),
            },
            clock,
        )
        .map(|w: Window<u8>| w.into_inner())
        .sink(sink)
        .run();

    assert!(result.is_ok());
    let flat: Vec<u8> = handle.take().into_iter().flatten().collect();
    assert_eq!(flat, items);
});
