//! Property tests: invariants that hold for any input.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use core::time::Duration;
use std::time::Instant;

use proptest::prelude::*;

use common::ScriptedClock;
use pipe_io::sink::{NullSink, VecSink};
use pipe_io::{
    Batch, BatchPolicy, Emit, ErrorPolicy, Pipeline, Stage, StageFailure, Window, WindowPolicy,
};

fn arb_vec<T>(elem: impl Strategy<Value = T>) -> impl Strategy<Value = Vec<T>>
where
    T: Clone + std::fmt::Debug + 'static,
{
    prop::collection::vec(elem, 0..48)
}

/// One timestamp per item (strictly increasing by `gaps`), plus a final
/// flush timestamp.
fn timeline(gaps_ms: &[u64]) -> Vec<Instant> {
    let t0 = Instant::now() + Duration::from_secs(3600);
    let mut t = t0;
    let mut out = Vec::with_capacity(gaps_ms.len() + 1);
    for gap in gaps_ms {
        t += Duration::from_millis(*gap);
        out.push(t);
    }
    out.push(t + Duration::from_secs(1000));
    out
}

/// A stage that buffers everything and fails its own flush, after
/// emitting what it holds.
struct FlakyFlush(Vec<u32>);
impl Stage for FlakyFlush {
    type Input = u32;
    type Output = u32;
    type Error = &'static str;
    fn process(&mut self, item: u32, _out: &mut dyn Emit<Item = u32>) -> Result<(), Self::Error> {
        self.0.push(item);
        Ok(())
    }
    fn flush(&mut self, out: &mut dyn Emit<Item = u32>) -> Result<(), Self::Error> {
        for item in self.0.drain(..) {
            if out.emit(item).is_err() {
                break;
            }
        }
        Err("flush failed")
    }
}

proptest! {
    #[test]
    fn map_preserves_length(items in arb_vec(any::<i32>())) {
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

    #[test]
    fn filter_admits_only_satisfying_items(items in arb_vec(any::<i32>())) {
        let expected: Vec<i32> = items.iter().copied().filter(|n| *n >= 0).collect();
        let sink = VecSink::<i32>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items).filter(|n: &i32| *n >= 0).sink(sink).run().unwrap();
        prop_assert_eq!(handle.take(), expected);
    }

    #[test]
    fn stats_count_items(items in arb_vec(any::<i32>())) {
        let kept = items.iter().filter(|n| **n % 3 != 0).count() as u64;
        let stats = Pipeline::from_iter(items.clone())
            .filter(|n: &i32| *n % 3 != 0)
            .sink(NullSink::new())
            .run()
            .unwrap();
        prop_assert_eq!(stats.items_in, items.len() as u64);
        prop_assert_eq!(stats.items_out, kept);
    }

    #[test]
    fn batching_is_lossless_and_bounded(items in arb_vec(any::<u32>()), size in 1_usize..16) {
        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .batch(BatchPolicy::new().max_items(size))
            .map(|b: Batch<u32>| b.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        let batches = handle.take();
        for b in &batches {
            prop_assert!(!b.is_empty() && b.len() <= size);
        }
        let flat: Vec<u32> = batches.into_iter().flatten().collect();
        prop_assert_eq!(flat, items);
    }

    #[test]
    fn byte_batches_never_exceed_limit_plus_last_item(
        items in arb_vec("[a-z]{0,12}"),
        limit in 1_usize..40,
    ) {
        let sink = VecSink::<Vec<String>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .batch_bytes(BatchPolicy::new().max_bytes(limit))
            .map(|b: Batch<String>| b.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        let batches = handle.take();
        for b in &batches {
            let total: usize = b.iter().map(String::len).sum();
            let last = b.last().map_or(0, String::len);
            prop_assert!(total - last < limit, "only the last item may cross the limit");
        }
        let flat: Vec<String> = batches.into_iter().flatten().collect();
        prop_assert_eq!(flat, items);
    }

    #[test]
    fn flush_always_reaches_the_sink(items in arb_vec(any::<u32>()), dead_letter in any::<bool>()) {
        // A stage whose flush fails under Continue/DeadLetter must not stop
        // the flush: the downstream batch and the sink still get it.
        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        let builder = Pipeline::from_iter(items.clone())
            .on_error(if dead_letter { ErrorPolicy::DeadLetter } else { ErrorPolicy::Continue })
            .stage(FlakyFlush(Vec::new()))
            .dead_letter(NullSink::<StageFailure>::new())
            .batch(BatchPolicy::new().max_items(1000))
            .map(|b: Batch<u32>| b.into_inner());
        let stats = builder.sink(sink).run().unwrap();
        let flat: Vec<u32> = handle.take().into_iter().flatten().collect();
        prop_assert_eq!(flat, items);
        prop_assert_eq!(stats.errors, 1);
    }

    #[test]
    fn tumbling_windowing_is_lossless(gaps in arb_vec(0_u64..2500)) {
        let items: Vec<u32> = (0..gaps.len() as u32).collect();
        let clock = ScriptedClock::new(timeline(&gaps));
        let sink = VecSink::<Window<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .window_with(WindowPolicy::tumbling(Duration::from_secs(1)), clock)
            .sink(sink)
            .run()
            .unwrap();
        let windows = handle.take();
        for w in &windows {
            prop_assert!(!w.is_empty());
            prop_assert!(w.start() <= w.end());
        }
        let flat: Vec<u32> = windows.into_iter().flat_map(Window::into_inner).collect();
        prop_assert_eq!(flat, items);
    }

    #[test]
    fn session_windows_partition_the_stream(gaps in arb_vec(0_u64..4000)) {
        let items: Vec<u32> = (0..gaps.len() as u32).collect();
        let clock = ScriptedClock::new(timeline(&gaps));
        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .window_with(WindowPolicy::session(Duration::from_secs(2)), clock)
            .map(|w: Window<u32>| w.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        let windows = handle.take();
        // Consecutive items share a session iff their gap is <= idle.
        let expected_sessions = if items.is_empty() {
            0
        } else {
            1 + gaps.iter().skip(1).filter(|g| **g > 2000).count()
        };
        prop_assert_eq!(windows.len(), expected_sessions);
        prop_assert!(windows.iter().all(|w| !w.is_empty()));
        let flat: Vec<u32> = windows.into_iter().flatten().collect();
        prop_assert_eq!(flat, items);
    }

    #[test]
    fn sliding_windows_place_each_item_in_size_over_slide_windows(
        gaps in arb_vec(1_u64..3000),
        ratio in 1_u64..5,
    ) {
        let slide = Duration::from_millis(500);
        let size = slide * u32::try_from(ratio).unwrap();
        let items: Vec<u32> = (0..gaps.len() as u32).collect();
        let clock = ScriptedClock::new(timeline(&gaps));
        let sink = VecSink::<Vec<u32>>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .window_with(WindowPolicy::sliding(size, slide), clock)
            .map(|w: Window<u32>| w.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        let windows = handle.take();
        for item in &items {
            let hits = windows.iter().filter(|w| w.contains(item)).count() as u64;
            prop_assert_eq!(hits, ratio, "item {} in {} windows", item, hits);
        }
    }

    #[test]
    fn continue_policy_swallows_errors(items in arb_vec(any::<i32>())) {
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
        let expected: Vec<i32> = items.into_iter().filter(|n| !n.is_negative()).collect();
        prop_assert_eq!(handle.take(), expected);
    }

    #[test]
    fn dead_letter_partitions_items_and_keeps_inputs(items in arb_vec(any::<i32>())) {
        let main = VecSink::<i32>::new();
        let main_h = main.handle();
        let dlq = VecSink::<StageFailure>::new();
        let dlq_h = dlq.handle();
        Pipeline::from_iter(items.clone())
            .on_error(ErrorPolicy::DeadLetter)
            .try_map_ref(|n: &i32| -> Result<i32, &'static str> {
                if n.is_negative() { Err("no negatives") } else { Ok(*n) }
            })
            .dead_letter(dlq)
            .sink(main)
            .run()
            .unwrap();
        let successes = main_h.take();
        let mut failures = dlq_h.take();
        prop_assert_eq!(successes.len() + failures.len(), items.len());
        let expected_ok: Vec<i32> = items.iter().copied().filter(|n| !n.is_negative()).collect();
        prop_assert_eq!(successes, expected_ok);
        let failed_inputs: Vec<i32> = failures.iter_mut().map(|f| f.take_input::<i32>().unwrap()).collect();
        let expected_bad: Vec<i32> = items.into_iter().filter(|n| n.is_negative()).collect();
        prop_assert_eq!(failed_inputs, expected_bad);
    }

    #[test]
    fn threaded_with_buffers_agrees_with_sync(
        items in arb_vec(any::<i32>()),
        cap_a in 0_usize..4,
        cap_b in 0_usize..4,
    ) {
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
        let stats = Pipeline::from_iter(items.clone())
            .buffer(cap_a)
            .filter(|n: &i32| *n >= 0)
            .buffer(cap_b)
            .map(|n: i32| n.wrapping_add(1))
            .sink(sink2)
            .run_threaded()
            .unwrap();

        let out = h2.take();
        prop_assert_eq!(stats.items_out, out.len() as u64);
        prop_assert_eq!(stats.items_in, items.len() as u64);
        prop_assert_eq!(h1.take(), out);
    }

    #[test]
    fn keyed_windows_partition_by_key(items in arb_vec((0_u8..4, any::<u16>()))) {
        let sink = VecSink::<(u8, Vec<(u8, u16)>)>::new();
        let handle = sink.handle();
        Pipeline::from_iter(items.clone())
            .window_by_key(|e: &(u8, u16)| e.0, WindowPolicy::tumbling(Duration::from_secs(3600)))
            .map(|(k, w): (u8, Window<(u8, u16)>)| (k, w.into_inner()))
            .sink(sink)
            .run()
            .unwrap();
        let out = handle.take();
        let mut total = 0;
        for (key, window) in &out {
            prop_assert!(window.iter().all(|e| e.0 == *key));
            let expected: Vec<(u8, u16)> = items.iter().copied().filter(|e| e.0 == *key).collect();
            prop_assert_eq!(window, &expected);
            total += window.len();
        }
        prop_assert_eq!(total, items.len());
    }
}
