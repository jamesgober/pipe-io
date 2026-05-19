//! Property tests: invariants that should hold across any input.
//!
//! Uses `proptest` (dev-dependency only) to generate random inputs
//! and assert shape-level invariants. Tests exercise the closure
//! adapters, batching, windowing, and error policies.

#![cfg(feature = "std")]

use core::time::Duration;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use proptest::prelude::*;

use pipe_io::sink::VecSink;
use pipe_io::{
    Batch, BatchPolicy, Clock, ErrorPolicy, Pipeline, StageFailure, Window, WindowPolicy,
};

// -------- arbitrary inputs --------

fn arb_small_vec<T>(elem: impl Strategy<Value = T>) -> impl Strategy<Value = Vec<T>>
where
    T: Clone + std::fmt::Debug + 'static,
{
    prop::collection::vec(elem, 0..32)
}

// -------- deterministic clock --------

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

// -------- properties --------

proptest! {
    // map preserves length when no filter/flat_map is in play.
    #[test]
    fn map_preserves_length(items in arb_small_vec(any::<i32>())) {
        let n = items.len();
        let sink = VecSink::<i64>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items)
            .map(|x: i32| i64::from(x).wrapping_mul(2))
            .sink(sink)
            .run()
            .unwrap();
        prop_assert_eq!(handle.take().len(), n);
    }

    // filter keeps only items for which the predicate holds.
    #[test]
    fn filter_admits_only_satisfying_items(items in arb_small_vec(any::<i32>())) {
        let expected: Vec<i32> = items.iter().copied().filter(|n| *n >= 0).collect();
        let sink = VecSink::<i32>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items)
            .filter(|n: &i32| *n >= 0)
            .sink(sink)
            .run()
            .unwrap();
        prop_assert_eq!(handle.take(), expected);
    }

    // map(f).filter(p) is equivalent to filter(|x| p(&f(x))).map(f)... no, that's not
    // true in general. Use a simpler invariant: order is preserved.
    #[test]
    fn pipeline_preserves_order(items in arb_small_vec(any::<i32>())) {
        let sink = VecSink::<i32>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .sink(sink)
            .run()
            .unwrap();
        prop_assert_eq!(handle.take(), items);
    }

    // Batching is lossless: every input item appears exactly once across all batches.
    #[test]
    fn batching_is_lossless(items in arb_small_vec(any::<u32>()), batch_size in 1usize..16) {
        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .batch(BatchPolicy::new().max_items(batch_size))
            .map(|b: Batch<u32>| b.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        let flattened: Vec<u32> = handle.take().into_iter().flatten().collect();
        prop_assert_eq!(flattened, items);
    }

    // No batch exceeds the configured max_items.
    #[test]
    fn batches_respect_max_items(items in arb_small_vec(any::<u32>()), batch_size in 1usize..16) {
        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items)
            .batch(BatchPolicy::new().max_items(batch_size))
            .map(|b: Batch<u32>| b.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        for batch in handle.take() {
            prop_assert!(batch.len() <= batch_size);
            prop_assert!(!batch.is_empty());
        }
    }

    // Tumbling windows are lossless across the items that arrived.
    #[test]
    fn tumbling_windowing_is_lossless(items in arb_small_vec(any::<u32>())) {
        if items.is_empty() {
            return Ok(());
        }
        let t0 = Instant::now();
        // Hand out one timestamp per item plus one for the flush.
        let mut times: Vec<Instant> = (0..items.len())
            .map(|i| t0 + Duration::from_millis(i as u64 * 100))
            .collect();
        times.push(t0 + Duration::from_secs(10));
        let clock = ScriptedClock::new(times);

        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .window_with(WindowPolicy::Tumbling { size: Duration::from_secs(1) }, clock)
            .map(|w: Window<u32>| w.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        let flat: Vec<u32> = handle.take().into_iter().flatten().collect();
        prop_assert_eq!(flat, items);
    }

    // Tumbling windows always have start <= end.
    #[test]
    fn tumbling_window_times_are_monotonic(items in arb_small_vec(any::<u32>())) {
        if items.is_empty() {
            return Ok(());
        }
        let t0 = Instant::now();
        let mut times: Vec<Instant> = (0..items.len())
            .map(|i| t0 + Duration::from_millis(i as u64 * 100))
            .collect();
        times.push(t0 + Duration::from_secs(10));
        let clock = ScriptedClock::new(times);

        let sink = VecSink::<(Instant, Instant)>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items)
            .window_with(WindowPolicy::Tumbling { size: Duration::from_secs(1) }, clock)
            .map(|w: Window<u32>| (w.start(), w.end()))
            .sink(sink)
            .run()
            .unwrap();
        for (start, end) in handle.take() {
            prop_assert!(start <= end);
        }
    }

    // ErrorPolicy::Continue never produces a run-level error from a
    // try_map that returns Err for some inputs.
    #[test]
    fn continue_policy_swallows_errors(items in arb_small_vec(any::<i32>())) {
        let sink = VecSink::<i32>::new();
        let handle = sink.handle();
        let result = Pipeline::from_iter(items.clone())
            .on_error(ErrorPolicy::Continue)
            .try_map(|n: i32| -> Result<i32, &'static str> {
                if n.is_negative() { Err("no negatives") } else { Ok(n) }
            })
            .sink(sink)
            .run();
        prop_assert!(result.is_ok());
        // Output should be the non-negative items.
        let expected: Vec<i32> = items.into_iter().filter(|n| !n.is_negative()).collect();
        prop_assert_eq!(handle.take(), expected);
    }

    // DeadLetter routes every failed item; main sink gets only successes.
    #[test]
    fn dead_letter_partitions_items(items in arb_small_vec(any::<i32>())) {
        let main = VecSink::<i32>::new();
        let main_h = main.handle();
        let dlq = VecSink::<StageFailure>::new();
        let dlq_h = dlq.handle();

        Pipeline::from_iter(items.clone())
            .on_error(ErrorPolicy::DeadLetter)
            .try_map(|n: i32| -> Result<i32, &'static str> {
                if n.is_negative() { Err("no negatives") } else { Ok(n) }
            })
            .dead_letter(dlq)
            .sink(main)
            .run()
            .unwrap();

        let successes = main_h.take();
        let failures = dlq_h.take();

        // No double-counting.
        prop_assert_eq!(successes.len() + failures.len(), items.len());
        // Successes are exactly the non-negatives in order.
        let expected_ok: Vec<i32> = items.iter().copied().filter(|n| !n.is_negative()).collect();
        prop_assert_eq!(successes, expected_ok);
        // Each failure carries the right stage id.
        for f in &failures {
            prop_assert_eq!(f.stage.as_str(), "try_map");
        }
    }

    // run() and run_with(SyncDriver) produce identical results.
    #[test]
    fn run_and_run_with_sync_agree(items in arb_small_vec(any::<i32>())) {
        let sink1 = VecSink::<i32>::new();
        let h1 = sink1.handle();
        Pipeline::from_iter(items.clone())
            .map(|n: i32| n.wrapping_mul(2))
            .sink(sink1)
            .run()
            .unwrap();

        let sink2 = VecSink::<i32>::new();
        let h2 = sink2.handle();
        Pipeline::from_iter(items)
            .map(|n: i32| n.wrapping_mul(2))
            .sink(sink2)
            .run_with(pipe_io::driver::SyncDriver::new())
            .unwrap();

        prop_assert_eq!(h1.take(), h2.take());
    }

    // run_threaded produces the same output as run (modulo timing).
    #[test]
    fn run_threaded_agrees_with_sync(items in arb_small_vec(any::<i32>())) {
        let sink1 = VecSink::<i32>::new();
        let h1 = sink1.handle();
        Pipeline::from_iter(items.clone())
            .filter(|n: &i32| *n >= 0)
            .map(|n: i32| n.wrapping_add(1))
            .sink(sink1)
            .run()
            .unwrap();

        let sink2 = VecSink::<i32>::new();
        let h2 = sink2.handle();
        Pipeline::from_iter(items)
            .filter(|n: &i32| *n >= 0)
            .map(|n: i32| n.wrapping_add(1))
            .sink(sink2)
            .run_threaded()
            .unwrap();

        prop_assert_eq!(h1.take(), h2.take());
    }
}
