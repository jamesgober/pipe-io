//! Regression tests for the 2026-11 review. Each test fails on 2.0.0.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use proptest::prelude::*;

use pipe_io::sink::{FnSink, VecSink};
use pipe_io::source::FnSource;
use pipe_io::window::{DEFAULT_MAX_ITEMS, DEFAULT_MAX_KEYS};
use pipe_io::{ManualClock, Pipeline, Window, WindowPolicy};

/// What `timed_source` yields: `(key, index)`.
type Timed = (u32, u32);

/// A source that yields `(key, index)` for each step after setting the
/// clock to `t0` plus the step's offset in seconds, so the window stage
/// reads that instant.
fn timed_source(
    clock: ManualClock,
    t0: Instant,
    steps: Vec<(u32, u64)>,
) -> FnSource<impl FnMut() -> Result<Option<Timed>, &'static str>, Timed, &'static str> {
    let mut steps = steps.into_iter().enumerate();
    FnSource::new(move || {
        Ok(steps.next().map(|(i, (key, offset))| {
            clock.set(t0 + Duration::from_secs(offset));
            (key, u32::try_from(i).unwrap())
        }))
    })
}

/// Review F-1: a sliding window whose clock steps back re-created and
/// re-emitted windows that were already closed.
#[test]
fn sliding_window_backwards_clock_does_not_reemit_closed_windows() {
    let t0 = Instant::now() + Duration::from_secs(3600);
    let clock = ManualClock::new(t0);
    let sink = VecSink::new();
    let out = sink.handle();
    let src = timed_source(clock.clone(), t0, vec![(0, 0), (0, 20), (0, 1)]);
    Pipeline::from_source(src)
        .map(|(_, i): (u32, u32)| i)
        .window_with(
            WindowPolicy::sliding(Duration::from_secs(10), Duration::from_secs(5)),
            clock,
        )
        .sink(sink)
        .run()
        .unwrap();
    let windows: Vec<Window<u32>> = out.take();
    let mut spans: Vec<(Instant, Instant)> = windows.iter().map(|w| (w.start(), w.end())).collect();
    let total = spans.len();
    spans.sort();
    spans.dedup();
    assert_eq!(spans.len(), total, "a window was emitted twice");
    let starts: Vec<Instant> = windows.iter().map(Window::start).collect();
    let mut sorted = starts.clone();
    sorted.sort();
    assert_eq!(starts, sorted, "windows are emitted in order");
    // The late item (index 2) joins the windows that were open, with item 1.
    let late: Vec<&Window<u32>> = windows.iter().filter(|w| w.items().contains(&2)).collect();
    assert_eq!(late.len(), 2);
    assert!(late.iter().all(|w| w.items() == [1, 2]));
}

fn policies() -> [WindowPolicy; 3] {
    [
        WindowPolicy::tumbling(Duration::from_secs(10)),
        WindowPolicy::sliding(Duration::from_secs(10), Duration::from_secs(5)),
        WindowPolicy::session(Duration::from_secs(4)),
    ]
}

/// Every `(key, start, end)` must be emitted at most once.
fn assert_unique(spans: &[(u32, Instant, Instant)], what: &str) -> Result<(), TestCaseError> {
    let mut sorted = spans.to_vec();
    sorted.sort();
    sorted.dedup();
    prop_assert_eq!(
        sorted.len(),
        spans.len(),
        "{}: a window was emitted twice",
        what
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Review F-1: with a clock that jumps back and forth, no policy,
    /// keyed or not, emits the same window twice, and every item is
    /// emitted.
    #[test]
    fn non_monotonic_clock_never_emits_a_window_twice(
        steps in proptest::collection::vec((0u32..3, 0u64..60), 1..40),
    ) {
        let t0 = Instant::now() + Duration::from_secs(3600);
        for policy in policies() {
            // Unkeyed.
            let clock = ManualClock::new(t0);
            let sink = VecSink::new();
            let out = sink.handle();
            Pipeline::from_source(timed_source(clock.clone(), t0, steps.clone()))
                .map(|(_, i): (u32, u32)| i)
                .window_with(policy, clock)
                .sink(sink)
                .run()
                .unwrap();
            let windows: Vec<Window<u32>> = out.take();
            let spans: Vec<(u32, Instant, Instant)> =
                windows.iter().map(|w| (0, w.start(), w.end())).collect();
            assert_unique(&spans, &format!("{policy:?} unkeyed"))?;
            let mut seen: Vec<u32> = windows.iter().flat_map(|w| w.items().to_vec()).collect();
            seen.sort_unstable();
            seen.dedup();
            prop_assert_eq!(seen.len(), steps.len());

            // Keyed.
            let clock = ManualClock::new(t0);
            let sink = VecSink::new();
            let out = sink.handle();
            Pipeline::from_source(timed_source(clock.clone(), t0, steps.clone()))
                .window_by_key_with(|(k, _): &(u32, u32)| *k, policy, clock)
                .sink(sink)
                .run()
                .unwrap();
            let keyed: Vec<(u32, Window<(u32, u32)>)> = out.take();
            let spans: Vec<(u32, Instant, Instant)> =
                keyed.iter().map(|(k, w)| (*k, w.start(), w.end())).collect();
            assert_unique(&spans, &format!("{policy:?} keyed"))?;
            let mut seen: Vec<u32> = keyed
                .iter()
                .flat_map(|(_, w)| w.items().iter().map(|(_, i)| *i).collect::<Vec<_>>())
                .collect();
            seen.sort_unstable();
            seen.dedup();
            prop_assert_eq!(seen.len(), steps.len());
        }
    }
}

/// Review F-2: `max_keys` defaulted to unbounded, so a keyed window fed
/// distinct keys held every one of them until the end of the stream.
#[test]
fn keyed_window_caps_keys_by_default() {
    let n = DEFAULT_MAX_KEYS as u64 + 2;
    let pulled = Arc::new(AtomicU64::new(0));
    let first_emit_at = Arc::new(Mutex::new(None::<u64>));
    let (p1, p2, f2) = (
        Arc::clone(&pulled),
        Arc::clone(&pulled),
        Arc::clone(&first_emit_at),
    );
    let clock = ManualClock::new(Instant::now());
    Pipeline::from_iter(0..n)
        .inspect(move |_: &u64| {
            p1.fetch_add(1, Ordering::Relaxed);
        })
        .window_by_key_with(
            |x: &u64| *x,
            WindowPolicy::tumbling(Duration::from_secs(60)),
            clock,
        )
        .sink(FnSink::new(
            move |_w: (u64, Window<u64>)| -> Result<(), &'static str> {
                let mut first = f2.lock().unwrap();
                if first.is_none() {
                    *first = Some(p2.load(Ordering::Relaxed));
                }
                Ok(())
            },
        ))
        .run()
        .unwrap();
    let first = first_emit_at.lock().unwrap().expect("windows were emitted");
    assert_eq!(
        first,
        DEFAULT_MAX_KEYS as u64 + 1,
        "the least recently active key is evicted when key number DEFAULT_MAX_KEYS + 1 arrives"
    );
}

/// Review F-2: `max_items` defaulted to unbounded.
#[test]
fn window_caps_items_by_default() {
    let n = DEFAULT_MAX_ITEMS as u64 + 1;
    let sizes = Arc::new(Mutex::new(Vec::new()));
    let s2 = Arc::clone(&sizes);
    let clock = ManualClock::new(Instant::now());
    Pipeline::from_iter(0..n)
        .window_with(WindowPolicy::tumbling(Duration::from_secs(60)), clock)
        .sink(FnSink::new(
            move |w: Window<u64>| -> Result<(), &'static str> {
                s2.lock().unwrap().push(w.len());
                Ok(())
            },
        ))
        .run()
        .unwrap();
    assert_eq!(*sizes.lock().unwrap(), vec![DEFAULT_MAX_ITEMS, 1]);
}

/// The opt-out still works: `usize::MAX` keeps every key.
#[test]
fn keyed_window_opt_out_keeps_every_key() {
    let n = 1_000u64;
    let emitted = Arc::new(AtomicU64::new(0));
    let e2 = Arc::clone(&emitted);
    let clock = ManualClock::new(Instant::now());
    Pipeline::from_iter(0..n)
        .window_by_key_with(
            |x: &u64| *x,
            WindowPolicy::tumbling(Duration::from_secs(60))
                .max_keys(usize::MAX)
                .max_items(usize::MAX),
            clock,
        )
        .sink(FnSink::new(
            move |_w: (u64, Window<u64>)| -> Result<(), &'static str> {
                e2.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        ))
        .run()
        .unwrap();
    assert_eq!(emitted.load(Ordering::Relaxed), n);
}
