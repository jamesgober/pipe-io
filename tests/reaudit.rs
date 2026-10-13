//! Regression tests for the 2026-10-13 re-audit of 2.0.0 (findings
//! PIPE-R01 ... PIPE-R13). Each test fails on the tree that was audited
//! (`97a5640`) and passes with the fix.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use common::{Fail, Fault, Log, RecSink, RecSource};
use pipe_io::sink::{FnSink, NullSink, VecSink};
use pipe_io::source::{ChainSource, ChannelSource, FnSource, ReaderSource, Source};
use pipe_io::{
    Backoff, Clock, ConfigErrorKind, Error, ErrorPolicy, ManualClock, Pipeline, Pull, RetryPolicy,
    StageFailure, Step, Window, WindowPolicy,
};

fn quiet_panics() {
    std::panic::set_hook(Box::new(|_| {}));
}

fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    let _worker = thread::spawn(move || {
        let _sent = tx.send(f());
    });
    rx.recv_timeout(limit).expect("finished in time")
}

// ---------------------------------------------------------------------
// R01: keyed windows close on time while other keys keep the stream busy
// ---------------------------------------------------------------------

/// Key `a` sends once at t = 0; key `b` sends every 10 ms for 5 s of
/// simulated time. `a`'s 1 s window must be emitted when the first item
/// at or after t = 1 s arrives (1.0 s), not at end of stream (5.0 s).
#[test]
fn r01_quiet_key_window_is_emitted_on_time_on_a_busy_stream() {
    for policy in [
        WindowPolicy::tumbling(Duration::from_secs(1)),
        WindowPolicy::sliding(Duration::from_secs(1), Duration::from_millis(500)),
        WindowPolicy::session(Duration::from_secs(1)),
    ] {
        let clock = ManualClock::new(Instant::now());
        let start = clock.now();
        let feed = clock.clone();
        let mut i = 0_u64;
        let source = FnSource::new(move || -> Result<Option<(&'static str, u64)>, Fail> {
            i += 1;
            if i > 501 {
                return Ok(None);
            }
            if i == 1 {
                return Ok(Some(("a", 0)));
            }
            feed.advance(Duration::from_millis(10));
            Ok(Some(("b", i)))
        });
        let seen = clock.clone();
        let emitted: Arc<Mutex<Vec<(&'static str, Duration)>>> = Arc::default();
        let log = Arc::clone(&emitted);
        Pipeline::from_source(source)
            .window_by_key_with(|e: &(&'static str, u64)| e.0, policy, clock)
            .sink(FnSink::new(
                move |(k, _w): (&'static str, Window<(&'static str, u64)>)| -> Result<(), Fail> {
                    log.lock().unwrap().push((k, seen.now() - start));
                    Ok(())
                },
            ))
            .run()
            .unwrap();
        let emitted = emitted.lock().unwrap();
        let at = emitted
            .iter()
            .find(|(k, _)| *k == "a")
            .expect("a emitted")
            .1;
        // The tumbling window ends at 1 s. The sliding grid starts one
        // slide before the first item, so `a`'s first window is
        // [-0.5 s, 0.5 s). A session ends once more than 1 s has passed
        // since its only item (first seen by the item at 1.01 s).
        let due = match policy {
            WindowPolicy::Session { .. } => Duration::from_millis(1010),
            WindowPolicy::Sliding { .. } => Duration::from_millis(500),
            _ => Duration::from_secs(1),
        };
        assert_eq!(at, due, "{policy:?}");
    }
}

/// The idle tick also closes every key's expired window, in deadline
/// order (deterministic, not hash order).
#[test]
fn r01_idle_tick_closes_every_expired_key_in_deadline_order() {
    let clock = ManualClock::new(Instant::now());
    let (tx, rx) = mpsc::channel::<u32>();
    let sink = VecSink::new();
    let out = sink.handle();
    let mut pipeline = Pipeline::from_source(ChannelSource::new(rx))
        .idle_tick(Duration::from_millis(1))
        .window_by_key_with(
            |v: &u32| *v,
            WindowPolicy::session(Duration::from_secs(1)),
            clock.clone(),
        )
        .map(|(k, _w): (u32, Window<u32>)| k)
        .sink(sink);
    for key in [5_u32, 3, 9, 1, 7] {
        tx.send(key).unwrap();
        assert_eq!(pipeline.step().unwrap(), Step::Item);
        clock.advance(Duration::from_millis(10));
    }
    clock.advance(Duration::from_secs(2));
    assert_eq!(pipeline.step().unwrap(), Step::Idle);
    assert_eq!(out.take(), vec![5, 3, 9, 1, 7]);
    drop(tx);
    assert_eq!(pipeline.step().unwrap(), Step::Done);
}

// ---------------------------------------------------------------------
// R02: key eviction is O(1), not a scan of every key
// ---------------------------------------------------------------------

/// 100,000 distinct keys with `max_keys(50_000)`: 50,000 evictions. With a
/// full scan per eviction this is 2.5 billion entry visits (minutes in a
/// debug build); with O(1) eviction it takes well under a second.
#[test]
fn r02_eviction_cost_does_not_grow_with_the_key_limit() {
    let stats = within(Duration::from_secs(60), || {
        let started = Instant::now();
        let stats = Pipeline::from_iter(0_u64..100_000)
            .window_by_key(
                |x: &u64| *x,
                WindowPolicy::tumbling(Duration::from_secs(3600)).max_keys(50_000),
            )
            .sink(NullSink::new())
            .run()
            .unwrap();
        (stats, started.elapsed())
    });
    assert_eq!(stats.0.items_out, 100_000);
    assert!(stats.1 < Duration::from_secs(20), "took {:?}", stats.1);
}

/// Eviction picks the least recently *active* key, also after keys were
/// touched again.
#[test]
fn r02_eviction_order_is_least_recently_active() {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter([1_u32, 2, 3, 1, 4, 2, 5])
        .window_by_key(
            |v: &u32| *v,
            WindowPolicy::tumbling(Duration::from_secs(3600)).max_keys(3),
        )
        .map(|(k, w): (u32, Window<u32>)| (k, w.len()))
        .sink(sink)
        .run()
        .unwrap();
    // 4 evicts 2 (1 was touched again); 2 then evicts 3; 5 evicts 1.
    assert_eq!(
        out.take(),
        vec![(2, 1), (3, 1), (1, 2), (4, 1), (2, 1), (5, 1)]
    );
}

// ---------------------------------------------------------------------
// R03: buffer capacity is validated and not preallocated
// ---------------------------------------------------------------------

#[test]
fn r03_oversized_buffer_capacity_is_a_config_error() {
    for capacity in [usize::MAX, (1 << 20) + 1] {
        let err = Pipeline::from_iter(0_u32..3)
            .stage_id("edge")
            .buffer(capacity)
            .try_sink(NullSink::new())
            .unwrap_err();
        assert_eq!(
            err.kind(),
            ConfigErrorKind::LimitTooLarge {
                field: "capacity",
                max: 1 << 20
            }
        );
        assert_eq!(err.stage().as_str(), "edge");
        // Running it instead reports the same error without touching
        // anything (and without allocating the edge).
        let run = Pipeline::from_iter(0_u32..3)
            .buffer(capacity)
            .sink(NullSink::new())
            .run_threaded();
        assert!(matches!(run, Err(Error::Config(_))));
    }
    // The largest accepted capacity and zero (a hand-off) are fine.
    for capacity in [0, 1 << 20] {
        let stats = Pipeline::from_iter(0_u32..3)
            .buffer(capacity)
            .try_sink(NullSink::new())
            .unwrap()
            .run_threaded()
            .unwrap();
        assert_eq!(stats.items_out, 3);
    }
}

// The memory side of R03 is tested in `tests/edge_memory.rs` (it needs
// a counting global allocator).

// ---------------------------------------------------------------------
// R04: under run_threaded the source is closed after the sink
// ---------------------------------------------------------------------

fn threaded_log(n: u32, sink_fault: Fault, buffers: usize) -> (Vec<String>, bool) {
    let log = Log::default();
    let log2 = log.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        let builder = Pipeline::from_source(RecSource::new(&log2, n))
            .on_error(ErrorPolicy::DeadLetter)
            .try_map(|v: u32| if v == 2 { Err(Fail("two")) } else { Ok(v) })
            .dead_letter(RecSink::<StageFailure>::new(&log2, "dlq"));
        let sink = RecSink::<u32>::new(&log2, "main").fault(sink_fault);
        match buffers {
            1 => builder.buffer(1).sink(sink).run_threaded(),
            _ => builder
                .buffer(0)
                .map(|v: u32| v)
                .buffer(2)
                .sink(sink)
                .run_threaded(),
        }
    }));
    let ok = matches!(result, Ok(Ok(_)));
    (log.entries(), ok)
}

/// The documented order: every write, then the sink's flush and close,
/// then `Source::close`, then the dead-letter sink.
#[test]
fn r04_threaded_teardown_closes_the_source_after_the_sink() {
    quiet_panics();
    for round in 0..100 {
        for buffers in [1, 2] {
            let (entries, ok) = threaded_log(4, Fault::None, buffers);
            assert!(ok);
            // The dead-letter write happens on the source thread, so it may
            // interleave with the sink's writes; everything else is fixed.
            let without_dlq_write: Vec<&str> = entries
                .iter()
                .map(String::as_str)
                .filter(|e| *e != "dlq.write")
                .collect();
            assert_eq!(
                without_dlq_write,
                [
                    "main.write",
                    "main.write",
                    "main.write",
                    "main.flush",
                    "main.close",
                    "source.close",
                    "dlq.flush",
                    "dlq.close"
                ],
                "round {round}, {buffers} buffer(s)"
            );
        }
    }
}

/// The same order holds when the sink fails or panics.
#[test]
fn r04_threaded_teardown_order_on_sink_failure_and_panic() {
    quiet_panics();
    for fault in [
        Fault::At(1),
        Fault::PanicAt(1),
        Fault::Flush,
        Fault::PanicFlush,
    ] {
        for round in 0..20 {
            let (entries, ok) = threaded_log(4, fault, 2);
            assert!(!ok);
            let tail: Vec<&str> = entries
                .iter()
                .map(String::as_str)
                .filter(|e| !e.ends_with(".write"))
                .collect();
            assert_eq!(
                tail,
                [
                    "main.flush",
                    "main.close",
                    "source.close",
                    "dlq.flush",
                    "dlq.close"
                ],
                "{fault:?}, round {round}: {entries:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// R05: retry backoff observes cancellation
// ---------------------------------------------------------------------

#[test]
fn r05_cancellation_interrupts_retry_backoff() {
    let mut pipeline = Pipeline::from_iter(0_u32..10)
        .on_error(ErrorPolicy::Continue)
        .try_map_retry(
            RetryPolicy::new(1_000).backoff(Backoff::Fixed(Duration::from_secs(3600))),
            |_n: &u32| -> Result<u32, Fail> { Err(Fail("always")) },
        )
        .sink(NullSink::new());
    let handle = pipeline.cancel_handle();
    let started = Instant::now();
    let worker = thread::spawn(move || pipeline.run());
    thread::sleep(Duration::from_millis(50));
    handle.cancel();
    let result = within(Duration::from_secs(10), move || worker.join().unwrap());
    assert!(matches!(result, Err(Error::Cancelled)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

// ---------------------------------------------------------------------
// R06: a zero read buffer still reads
// ---------------------------------------------------------------------

#[test]
fn r06_reader_source_with_zero_capacity_reads_lines() {
    let mut source = ReaderSource::with_capacity(0, Cursor::new("hello\nworld\n"));
    assert_eq!(source.pull().unwrap().as_deref(), Some("hello"));
    assert_eq!(source.pull().unwrap().as_deref(), Some("world"));
    assert_eq!(source.pull().unwrap(), None);
}

// ---------------------------------------------------------------------
// R07: flush and close run at most once, and close always runs
// ---------------------------------------------------------------------

#[test]
fn r07_failing_flush_is_called_once_and_close_still_runs() {
    for threaded in [false, true] {
        let log = Log::default();
        let builder = Pipeline::from_source(RecSource::new(&log, 2));
        let sink = RecSink::<u32>::new(&log, "main").fault(Fault::Flush);
        let result = if threaded {
            builder.buffer(1).sink(sink).run_threaded()
        } else {
            builder.sink(sink).run()
        };
        assert!(result.is_err());
        assert_eq!(log.count("main.flush"), 1, "threaded: {threaded}");
        assert_eq!(log.count("main.close"), 1, "threaded: {threaded}");
        assert_eq!(log.count("source.close"), 1, "threaded: {threaded}");
    }
}

#[test]
fn r07_panicking_flush_or_close_still_closes_everything_once() {
    quiet_panics();
    for threaded in [false, true] {
        for fault in [Fault::PanicFlush, Fault::Close] {
            let log = Log::default();
            let log2 = log.clone();
            let result = catch_unwind(AssertUnwindSafe(move || {
                let builder = Pipeline::from_source(RecSource::new(&log2, 2))
                    .dead_letter(RecSink::<StageFailure>::new(&log2, "dlq"));
                let sink = RecSink::<u32>::new(&log2, "main").fault(fault);
                if threaded {
                    builder.buffer(1).sink(sink).run_threaded()
                } else {
                    builder.sink(sink).run()
                }
            }));
            assert!(!matches!(result, Ok(Ok(_))));
            let what = format!("threaded {threaded}, {fault:?}: {:?}", log.entries());
            assert_eq!(log.count("main.flush"), 1, "{what}");
            assert_eq!(log.count("main.close"), 1, "{what}");
            assert_eq!(log.count("source.close"), 1, "{what}");
            assert_eq!(log.count("dlq.close"), 1, "{what}");
        }
    }
}

// ---------------------------------------------------------------------
// R08: a stepped pipeline is poisoned by a caught panic
// ---------------------------------------------------------------------

#[test]
fn r08_step_after_a_caught_panic_fails_and_tears_down() {
    quiet_panics();
    let log = Log::default();
    let mut pipeline = Pipeline::from_source(RecSource::new(&log, 5))
        .map(|v: u32| if v == 2 { panic!("boom") } else { v })
        .sink(RecSink::<u32>::new(&log, "main"));
    assert_eq!(pipeline.step().unwrap(), Step::Item);
    let caught = catch_unwind(AssertUnwindSafe(|| pipeline.step()));
    assert!(caught.is_err());
    let err = pipeline.step().unwrap_err();
    assert!(matches!(err, Error::Driver { .. }), "{err:?}");
    assert_eq!(
        log.entries(),
        ["main.write", "main.flush", "main.close", "source.close"]
    );
    assert!(matches!(pipeline.step(), Err(Error::Closed)));
    assert!(pipeline.finish().is_err());
}

// ---------------------------------------------------------------------
// R13: ChainSource keeps to the timeout when it switches sources
// ---------------------------------------------------------------------

#[test]
fn r13_chain_source_timeout_is_not_doubled_at_the_switch() {
    struct SlowEnd;
    impl Source for SlowEnd {
        type Item = u32;
        type Error = Fail;
        fn pull(&mut self) -> Result<Option<u32>, Fail> {
            Ok(None)
        }
        fn pull_timeout(&mut self, timeout: Duration) -> Result<Pull<u32>, Fail> {
            // Uses up the whole timeout before reporting the end.
            thread::sleep(timeout);
            Ok(Pull::End)
        }
    }
    let (_tx, rx) = mpsc::channel::<u32>();
    let mut chained = ChainSource::new(SlowEnd, ChannelSource::new(rx));
    let started = Instant::now();
    assert!(matches!(
        chained.pull_timeout(Duration::from_millis(200)).unwrap(),
        Pull::Idle
    ));
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_millis(380), "waited {elapsed:?}");
}

/// Sanity check for the counters used above: a rendezvous edge under
/// many items keeps order and loses nothing.
#[test]
fn r03_hand_off_edge_keeps_order() {
    let count = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&count);
    let mut expected = 0_u32;
    Pipeline::from_iter(0_u32..20_000)
        .buffer(0)
        .sink(FnSink::new(move |v: u32| -> Result<(), Fail> {
            assert_eq!(v, expected);
            expected += 1;
            let _previous = seen.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }))
        .run_threaded()
        .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 20_000);
}
