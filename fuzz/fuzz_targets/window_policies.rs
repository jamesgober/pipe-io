#![no_main]
//! Fuzz target: every window policy, with arbitrary (including zero and
//! huge) durations and arbitrary arrival gaps, either reports a
//! configuration error or terminates with only non-empty windows that
//! account for every item.

use core::time::Duration;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::VecSink;
use pipe_io::{Clock, Error, Pipeline, Window, WindowPolicy, WindowSpec};

/// Each read returns the next scripted instant (then repeats the last).
#[derive(Clone)]
struct Script(Arc<Mutex<(Vec<Instant>, usize)>>);

impl Clock for Script {
    fn now(&self) -> Instant {
        let mut g = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let i = g.1.min(g.0.len() - 1);
        g.1 += 1;
        g.0[i]
    }
}

fn duration(byte: u8, scale: u8) -> Duration {
    match scale % 4 {
        0 => Duration::from_millis(u64::from(byte)),
        1 => Duration::from_micros(u64::from(byte)),
        2 => Duration::from_secs(u64::from(byte)),
        _ => Duration::MAX / u32::from(byte.max(1)),
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let (head, gaps) = data.split_at(4);
    let size = duration(head[1], head[0] >> 2);
    let other = duration(head[2], head[0] >> 4);
    let policy = match head[0] % 3 {
        0 => WindowPolicy::tumbling(size),
        1 => WindowPolicy::sliding(size, other),
        _ => WindowPolicy::session(size),
    };
    let max_items = usize::from(head[3] % 8);

    let t0 = Instant::now() + Duration::from_secs(1_000_000);
    let mut t = t0;
    let mut times = Vec::with_capacity(gaps.len() + 1);
    for g in gaps {
        t += Duration::from_millis(u64::from(*g));
        times.push(t);
    }
    times.push(t + Duration::from_secs(3600));
    let clock = Script(Arc::new(Mutex::new((times, 0))));

    let items: Vec<u32> = (0..gaps.len() as u32).collect();
    let sink = VecSink::<Window<u32>>::new();
    let handle = sink.handle();
    let spec = if max_items == 0 {
        WindowSpec::from(policy)
    } else {
        policy.max_items(max_items)
    };
    let result = Pipeline::from_iter(items.clone())
        .window_with(spec, clock)
        .sink(sink)
        .run();
    match result {
        Err(Error::Config(_)) => {
            assert!(policy.validate().is_err());
            assert!(handle.is_empty());
        }
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(_) => {
            let windows = handle.take();
            assert!(windows.iter().all(|w| !w.is_empty() && w.start() <= w.end()));
            if max_items > 0 {
                assert!(windows.iter().all(|w| w.len() <= max_items));
            }
            let mut seen: Vec<u32> = windows.iter().flat_map(|w| w.iter().copied()).collect();
            if !matches!(policy, WindowPolicy::Sliding { .. }) {
                assert_eq!(seen, items, "tumbling and session windows partition the stream");
            }
            seen.sort_unstable();
            seen.dedup();
            assert_eq!(seen, items, "every item reaches at least one window");
        }
    }
});
