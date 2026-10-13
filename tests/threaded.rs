//! The threaded driver and bounded `buffer` edges (PIPE-008, PIPE-011):
//! ordering, backpressure, error, panic, cancellation, and shutdown
//! semantics.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use common::{Fault, Log, RecSink, RecSource};
use pipe_io::sink::{FnSink, NullSink, VecSink};
use pipe_io::source::{ChannelSource, FnSource};
use pipe_io::{ErrorPolicy, Event, Pipeline, Sink, StageFailure, Window, WindowPolicy};

fn quiet_panics() {
    std::panic::set_hook(Box::new(|_| {}));
}

#[test]
fn order_is_preserved_across_segments() {
    let sink = VecSink::new();
    let out = sink.handle();
    let stats = Pipeline::from_iter(0_u32..100_000)
        .map(|n: u32| n.wrapping_mul(3))
        .buffer(16)
        .filter(|n: &u32| n % 2 == 0)
        .buffer(0)
        .map(|n: u32| n + 1)
        .buffer(1)
        .sink(sink)
        .run_threaded()
        .unwrap();
    let expected: Vec<u32> = (0_u32..100_000)
        .map(|n| n.wrapping_mul(3))
        .filter(|n| n % 2 == 0)
        .map(|n| n + 1)
        .collect();
    assert_eq!(out.take(), expected);
    assert_eq!(stats.items_in, 100_000);
    assert_eq!(stats.items_out, 50_000);
}

#[test]
fn each_segment_runs_on_its_own_thread() {
    let ids: Arc<Mutex<Vec<(u8, ThreadId)>>> = Arc::default();
    let (a, b, c) = (Arc::clone(&ids), Arc::clone(&ids), Arc::clone(&ids));
    let caller = thread::current().id();
    Pipeline::from_iter(0..4_u32)
        .inspect(move |_| a.lock().unwrap().push((0, thread::current().id())))
        .buffer(2)
        .inspect(move |_| b.lock().unwrap().push((1, thread::current().id())))
        .buffer(2)
        .sink(FnSink::new(move |_n: u32| -> Result<(), &'static str> {
            c.lock().unwrap().push((2, thread::current().id()));
            Ok(())
        }))
        .run_threaded()
        .unwrap();
    let ids = ids.lock().unwrap();
    let per_segment: Vec<HashSet<ThreadId>> = (0..3)
        .map(|seg| {
            ids.iter()
                .filter(|(s, _)| *s == seg)
                .map(|(_, t)| *t)
                .collect()
        })
        .collect();
    assert!(
        per_segment.iter().all(|set| set.len() == 1),
        "one thread per segment"
    );
    let all: HashSet<ThreadId> = per_segment.iter().flatten().copied().collect();
    assert_eq!(all.len(), 3, "three distinct threads");
    assert!(!all.contains(&caller), "the caller only waits");
}

#[test]
fn buffer_bounds_items_in_flight() {
    // A slow sink behind a capacity-4 edge: the source may run ahead by at
    // most the edge capacity plus the item held by each thread.
    const CAP: usize = 4;
    let pulled = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(AtomicUsize::new(0));
    let max_gap = Arc::new(AtomicUsize::new(0));
    let (p, w, g) = (
        Arc::clone(&pulled),
        Arc::clone(&written),
        Arc::clone(&max_gap),
    );
    let mut next = 0_u32;
    let source = FnSource::new(move || -> Result<Option<u32>, &'static str> {
        if next == 60 {
            return Ok(None);
        }
        next += 1;
        let ahead = p.fetch_add(1, Ordering::SeqCst) + 1 - w.load(Ordering::SeqCst);
        let _prev = g.fetch_max(ahead, Ordering::SeqCst);
        Ok(Some(next))
    });
    let w2 = Arc::clone(&written);
    Pipeline::from_source(source)
        .buffer(CAP)
        .sink(FnSink::new(move |_n: u32| -> Result<(), &'static str> {
            thread::sleep(Duration::from_millis(1));
            let _prev = w2.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
        .run_threaded()
        .unwrap();
    let gap = max_gap.load(Ordering::SeqCst);
    assert!(
        gap <= CAP + 2,
        "source ran {gap} items ahead of a slow sink"
    );
    assert!(gap >= CAP, "the edge did buffer ({gap})");
}

#[test]
fn single_threaded_execution_treats_buffer_as_a_direct_edge() {
    let sink = VecSink::new();
    let out = sink.handle();
    let mut pipeline = Pipeline::from_iter([1, 2, 3])
        .buffer(8)
        .map(|n: i32| n * 2)
        .sink(sink);
    assert_eq!(pipeline.step().unwrap(), pipe_io::Step::Item);
    assert_eq!(
        out.snapshot(),
        vec![2],
        "no buffering on the caller's thread"
    );
    assert_eq!(pipeline.finish().unwrap().items_out, 1);
}

#[test]
fn downstream_error_stops_the_source_and_tears_down() {
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 1_000_000))
        .buffer(4)
        .stage_id("check")
        .try_map(|n: u32| if n == 10 { Err("bad item") } else { Ok(n) })
        .buffer(4)
        .sink(RecSink::<u32>::new(&log, "main"))
        .run_threaded()
        .unwrap_err();
    assert_eq!(err.to_string(), "stage `check`: bad item");
    assert!(log.contains("main.flush") && log.contains("main.close"));
    assert!(log.contains("source.close"));
    assert_eq!(log.count("main.write"), 9);
}

#[test]
fn source_error_tears_down_downstream_segments() {
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 10).fault(Fault::At(4)))
        .buffer(1)
        .map(|n: u32| n * 2)
        .buffer(1)
        .sink(RecSink::<u32>::new(&log, "main"))
        .run_threaded()
        .unwrap_err();
    assert!(matches!(err, pipe_io::Error::Source { .. }));
    assert_eq!(log.count("main.write"), 3);
    assert_eq!(log.count("main.flush"), 1);
    assert_eq!(log.count("main.close"), 1);
    assert_eq!(log.count("source.close"), 1);
}

#[test]
fn sink_error_in_last_segment_is_reported() {
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 100))
        .buffer(2)
        .sink(RecSink::<u32>::new(&log, "main").fault(Fault::At(5)))
        .run_threaded()
        .unwrap_err();
    assert_eq!(err.to_string(), "sink `sink`: fail: sink write");
    assert_eq!(log.count("main.close"), 1);
    assert_eq!(log.count("source.close"), 1);
}

#[test]
fn panic_in_downstream_segment_is_reraised_with_its_payload() {
    quiet_panics();
    let log = Log::default();
    let log2 = log.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_source(RecSource::new(&log2, 1_000))
            .buffer(2)
            .map(|n: u32| if n == 7 { panic!("boom in segment") } else { n })
            .buffer(2)
            .sink(RecSink::<u32>::new(&log2, "main"))
            .run_threaded()
    }));
    let payload = result.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom in segment"));
    assert_eq!(log.count("main.flush"), 1, "sink torn down");
    assert_eq!(log.count("main.close"), 1);
    assert_eq!(log.count("source.close"), 1, "source closed");
}

#[test]
fn panic_in_source_segment_is_reraised_and_sink_torn_down() {
    quiet_panics();
    let log = Log::default();
    let log2 = log.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_source(RecSource::new(&log2, 100).fault(Fault::PanicAt(3)))
            .buffer(2)
            .sink(RecSink::<u32>::new(&log2, "main"))
            .run_threaded()
    }));
    let payload = result.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"source panic"));
    assert_eq!(log.count("main.write"), 2);
    assert_eq!(log.count("main.close"), 1);
    assert_eq!(log.count("source.close"), 1);
}

#[test]
fn panic_without_buffers_matches_sync_driver() {
    quiet_panics();
    let result = catch_unwind(|| {
        Pipeline::from_iter(0..3)
            .map(|n: i32| if n == 1 { panic!("boom in map") } else { n })
            .sink(NullSink::new())
            .run_threaded()
    });
    let payload = result.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom in map"));
}

#[test]
fn dead_letter_sink_is_shared_by_all_segments() {
    let log = Log::default();
    let dlq = RecSink::<StageFailure>::new(&log, "dlq");
    let failures = dlq.items();
    let stats = Pipeline::from_iter(0_u32..100)
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("first")
        .try_map(|n: u32| if n % 10 == 0 { Err("tens") } else { Ok(n) })
        .buffer(4)
        .stage_id("second")
        .try_map(|n: u32| if n % 10 == 5 { Err("fives") } else { Ok(n) })
        .buffer(4)
        .dead_letter(dlq)
        .sink(NullSink::new())
        .run_threaded()
        .unwrap();
    assert_eq!(stats.dead_lettered, 20);
    assert_eq!(stats.errors, 20);
    assert_eq!(stats.items_out, 80);
    let stages: HashSet<&str> = failures
        .lock()
        .unwrap()
        .iter()
        .map(|f| f.stage.as_str())
        .collect();
    assert_eq!(stages, HashSet::from(["first", "second"]));
    assert_eq!(log.count("dlq.flush"), 1);
    assert_eq!(log.count("dlq.close"), 1);
}

#[test]
fn panicking_dead_letter_sink_does_not_deadlock() {
    quiet_panics();
    struct PanickyDlq(Log);
    impl Sink for PanickyDlq {
        type Item = StageFailure;
        type Error = core::convert::Infallible;
        fn write(&mut self, _f: StageFailure) -> Result<(), Self::Error> {
            panic!("dlq panic");
        }
        fn flush(&mut self) -> Result<(), Self::Error> {
            self.0.push("dlq.flush");
            Ok(())
        }
        fn close(&mut self) -> Result<(), Self::Error> {
            self.0.push("dlq.close");
            Ok(())
        }
    }
    let log = Log::default();
    let dlq = PanickyDlq(log.clone());
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_iter(0_u32..10)
            .buffer(1)
            .on_error(ErrorPolicy::DeadLetter)
            .try_map(|n: u32| if n == 3 { Err("x") } else { Ok(n) })
            .dead_letter(dlq)
            .sink(NullSink::new())
            .run_threaded()
    }));
    let payload = result.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"dlq panic"));
    // The lock poisoned by the panic is recovered for the final flush.
    assert!(log.contains("dlq.flush") && log.contains("dlq.close"));
}

#[test]
fn cancellation_drains_and_reports_cancelled() {
    let sink = VecSink::<u64>::new();
    let out = sink.handle();
    let produced = Arc::new(AtomicU64::new(0));
    let p2 = Arc::clone(&produced);
    let mut n = 0_u64;
    let source = FnSource::new(move || -> Result<Option<u64>, &'static str> {
        n += 1;
        p2.store(n, Ordering::SeqCst);
        Ok(Some(n))
    });
    let mut pipeline = Pipeline::from_source(source).buffer(8).sink(sink);
    let cancel = pipeline.cancel_handle();
    let worker = thread::spawn(move || pipeline.run_threaded());
    while produced.load(Ordering::SeqCst) < 1_000 {
        thread::yield_now();
    }
    cancel.cancel();
    assert!(cancel.is_cancelled());
    let err = worker.join().unwrap().unwrap_err();
    assert!(err.is_cancelled());
    let delivered = out.take();
    assert!(!delivered.is_empty());
    // Everything pulled before the stop reached the sink, in order.
    assert!(delivered.windows(2).all(|w| w[1] == w[0] + 1));
    assert_eq!(delivered.len() as u64, *delivered.last().unwrap());
}

#[test]
fn idle_tick_runs_in_downstream_segments() {
    let clock = pipe_io::ManualClock::new(Instant::now());
    let (tx, rx) = mpsc::channel::<u32>();
    let sink = VecSink::<Vec<u32>>::new();
    let out = sink.handle();
    let ticks = Arc::new(AtomicU64::new(0));
    let t2 = Arc::clone(&ticks);
    let pipeline = Pipeline::from_source(ChannelSource::new(rx))
        .idle_tick(Duration::from_millis(1))
        .observe(move |e: &Event<'_>| {
            if matches!(e, Event::Idle) {
                let _prev = t2.fetch_add(1, Ordering::Relaxed);
            }
        })
        .buffer(4)
        .window_with(
            WindowPolicy::tumbling(Duration::from_secs(10)),
            clock.clone(),
        )
        .map(|w: Window<u32>| w.into_inner())
        .buffer(4)
        .sink(sink);
    let worker = thread::spawn(move || pipeline.run_threaded());
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    // Let both items reach the window, then move time past its end.
    thread::sleep(Duration::from_millis(50));
    clock.advance(Duration::from_secs(10));
    let deadline = Instant::now() + Duration::from_secs(10);
    while out.is_empty() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        out.take(),
        vec![vec![1, 2]],
        "closed by a tick, not by end of stream"
    );
    drop(tx);
    worker.join().unwrap().unwrap();
    assert!(ticks.load(Ordering::Relaxed) > 0);
}

#[test]
fn observer_sees_start_and_finish_once() {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let e2 = Arc::clone(&events);
    Pipeline::from_iter(0..10)
        .observe(move |e: &Event<'_>| match e {
            Event::Started => e2.lock().unwrap().push("started".into()),
            Event::Finished { stats, error } => {
                e2.lock().unwrap().push(format!(
                    "finished {} {}",
                    stats.items_out,
                    error.is_none()
                ));
            }
            _ => {}
        })
        .buffer(2)
        .sink(NullSink::<i32>::new())
        .run_threaded()
        .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["started", "finished 10 true"]);
}

#[test]
fn second_run_is_rejected() {
    let mut pipeline = Pipeline::from_iter(0..2).sink(NullSink::<i32>::new());
    assert_eq!(pipeline.step().unwrap(), pipe_io::Step::Item);
    // A pipeline that already started cannot be handed to another driver.
    let err = pipe_io::driver::ThreadedDriver::new()
        .run(pipeline)
        .unwrap_err();
    assert!(matches!(err, pipe_io::Error::Closed));
}
