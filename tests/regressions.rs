//! The reproductions from the 2026-10-13 audit, kept as regression tests.
//! Each test names the finding it covers and fails on `v1.0.0`.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::Read;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use pipe_io::sink::{NullSink, VecSink};
use pipe_io::source::{FnSource, ReaderError, ReaderSource, Source};
use pipe_io::{
    Batch, BatchPolicy, ByteSize, Emit, Error, ErrorPolicy, ManualClock, Pipeline, Stage,
    StageFailure, Window, WindowPolicy,
};

fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    let _worker = thread::spawn(move || {
        let _sent = tx.send(f());
    });
    rx.recv_timeout(limit).expect("finished in time")
}

/// r01: a stage flush error under Continue/DeadLetter dropped every
/// buffered batch downstream while `run` returned `Ok`.
#[test]
fn pipe_001_flush_error_does_not_lose_downstream_data() {
    struct FailOnFlush;
    impl Stage for FailOnFlush {
        type Input = u32;
        type Output = u32;
        type Error = &'static str;
        fn process(&mut self, i: u32, out: &mut dyn Emit<Item = u32>) -> Result<(), Self::Error> {
            out.emit(i).map_err(|_| "closed")
        }
        fn flush(&mut self, _out: &mut dyn Emit<Item = u32>) -> Result<(), Self::Error> {
            Err("flush failed")
        }
    }
    for policy in [ErrorPolicy::Continue, ErrorPolicy::DeadLetter] {
        let sink = VecSink::<Vec<u32>>::new();
        let h = sink.handle();
        Pipeline::from_iter(1_u32..=5)
            .on_error(policy)
            .stage(FailOnFlush)
            .dead_letter(NullSink::<StageFailure>::new())
            .batch(BatchPolicy::new().max_items(100))
            .map(|b: Batch<u32>| b.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        assert_eq!(h.take(), vec![vec![1, 2, 3, 4, 5]], "{policy:?}");
    }
}

/// r02: zero window size / slide looped forever (and grew memory).
#[test]
fn pipe_002_zero_window_durations_fail_fast() {
    for policy in [
        WindowPolicy::Tumbling {
            size: Duration::ZERO,
        },
        WindowPolicy::Sliding {
            size: Duration::from_secs(1),
            slide: Duration::ZERO,
        },
    ] {
        let result = within(Duration::from_secs(5), move || {
            Pipeline::from_iter(0_u32..10)
                .window(policy)
                .sink(NullSink::<Window<u32>>::new())
                .run()
        });
        assert!(matches!(result, Err(Error::Config(_))), "{policy:?}");
    }
}

/// r04: an idle gap emitted one (mostly empty) window per elapsed
/// interval: a million windows for a one-second gap with 1 µs windows.
#[test]
fn pipe_003_idle_gap_emits_only_non_empty_windows() {
    /// Each read returns the current time, then moves it forward 1 s.
    #[derive(Clone)]
    struct Stepping(ManualClock);
    impl pipe_io::Clock for Stepping {
        fn now(&self) -> Instant {
            let now = self.0.now();
            self.0.advance(Duration::from_secs(1));
            now
        }
    }
    for (policy, expected) in [
        (WindowPolicy::tumbling(Duration::from_micros(1)), vec![1, 1]),
        (
            WindowPolicy::sliding(Duration::from_micros(2), Duration::from_micros(1)),
            vec![1, 1, 1, 1],
        ),
    ] {
        let clock = Stepping(ManualClock::new(Instant::now() + Duration::from_secs(10)));
        let sink = VecSink::<usize>::new();
        let h = sink.handle();
        let started = Instant::now();
        Pipeline::from_iter([1_u32, 2])
            .window_with(policy, clock)
            .map(|w: Window<u32>| w.len())
            .sink(sink)
            .run()
            .unwrap();
        assert_eq!(h.take(), expected, "{policy:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}

/// r05: `Sliding { size: Duration::MAX }` overflowed `Instant`.
#[test]
fn pipe_004_duration_max_does_not_panic() {
    for policy in [
        WindowPolicy::Sliding {
            size: Duration::MAX,
            slide: Duration::MAX,
        },
        WindowPolicy::Tumbling {
            size: Duration::MAX,
        },
        WindowPolicy::Session {
            idle: Duration::MAX,
        },
    ] {
        let stats = Pipeline::from_iter(vec![1_u32, 2])
            .window(policy)
            .sink(NullSink::new())
            .run()
            .unwrap();
        assert_eq!(stats.items_out, 1, "{policy:?}");
    }
}

/// r05: `max_items(usize::MAX)` panicked with "capacity overflow";
/// `max_items(0)` was silently turned into batches of one.
#[test]
fn pipe_005_batch_limits_are_safe() {
    let stats = Pipeline::from_iter(0_u64..3)
        .batch(BatchPolicy::new().max_items(usize::MAX))
        .sink(NullSink::<Batch<u64>>::new())
        .run()
        .unwrap();
    assert_eq!(stats.items_out, 1);
    let started = Instant::now();
    Pipeline::from_iter(0_u64..3)
        .batch(BatchPolicy::new().max_items(50_000_000))
        .sink(NullSink::<Batch<u64>>::new())
        .run()
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "no 400 MB reservation"
    );
    let err = Pipeline::from_iter(0_u64..3)
        .batch(BatchPolicy::new().max_items(0))
        .sink(NullSink::<Batch<u64>>::new())
        .run()
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

/// r05: the byte counter overflowed (panic in debug, wrap in release).
#[test]
fn pipe_006_byte_counter_saturates() {
    struct Big;
    impl ByteSize for Big {
        fn byte_size(&self) -> usize {
            usize::MAX / 2 + 1
        }
    }
    let stats = Pipeline::from_iter(vec![Big, Big, Big])
        .batch_bytes(BatchPolicy::new().max_items(10).max_bytes(usize::MAX))
        .sink(NullSink::<Batch<Big>>::new())
        .run()
        .unwrap();
    assert_eq!(stats.items_in, 3);
}

/// r07: on an error, the sink, the dead-letter sink, and the source were
/// never flushed or closed.
#[test]
fn pipe_007_error_path_runs_teardown() {
    use std::sync::{Arc, Mutex};
    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<String>>>);
    struct S(Log, u32);
    impl Source for S {
        type Item = u32;
        type Error = &'static str;
        fn pull(&mut self) -> Result<Option<u32>, &'static str> {
            self.1 += 1;
            Ok((self.1 <= 5).then_some(self.1))
        }
        fn close(&mut self) -> Result<(), &'static str> {
            self.0.0.lock().unwrap().push("source.close".into());
            Ok(())
        }
    }
    struct K<T>(Log, &'static str, std::marker::PhantomData<fn(T)>);
    impl<T: 'static> pipe_io::Sink for K<T> {
        type Item = T;
        type Error = &'static str;
        fn write(&mut self, _: T) -> Result<(), &'static str> {
            self.0.0.lock().unwrap().push(format!("{}.write", self.1));
            Ok(())
        }
        fn flush(&mut self) -> Result<(), &'static str> {
            self.0.0.lock().unwrap().push(format!("{}.flush", self.1));
            Ok(())
        }
        fn close(&mut self) -> Result<(), &'static str> {
            self.0.0.lock().unwrap().push(format!("{}.close", self.1));
            Ok(())
        }
    }
    let log = Log::default();
    let res = Pipeline::from_source(S(log.clone(), 0))
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("one")
        .try_map(|n: u32| if n == 1 { Err("one") } else { Ok(n) })
        .on_error(ErrorPolicy::FailFast)
        .stage_id("three")
        .try_map(|n: u32| if n == 3 { Err("three") } else { Ok(n) })
        .dead_letter(K::<StageFailure>(log.clone(), "dlq", Default::default()))
        .sink(K::<u32>(log.clone(), "main", Default::default()))
        .run();
    assert!(res.is_err());
    let calls = log.0.lock().unwrap().clone();
    for expected in [
        "main.flush",
        "main.close",
        "dlq.flush",
        "dlq.close",
        "source.close",
    ] {
        assert!(
            calls.iter().any(|c| c == expected),
            "missing {expected} in {calls:?}"
        );
    }
}

/// r08: `ReaderSource` buffered an unbounded line; invalid UTF-8 failed
/// the pipeline with no way to continue.
#[test]
fn pipe_010_reader_source_is_bounded() {
    let mut s = ReaderSource::new(std::io::repeat(b'a').take(256 * 1024 * 1024));
    assert!(matches!(
        s.pull(),
        Err(ReaderError::LineTooLong { line: 1, .. })
    ));
    let mut lossy = ReaderSource::new(&[0xff_u8, 0xfe, b'\n', b'o', b'k', b'\n'][..]).lossy(true);
    assert_eq!(lossy.pull().unwrap().as_deref(), Some("\u{fffd}\u{fffd}"));
    assert_eq!(lossy.pull().unwrap().as_deref(), Some("ok"));
}

/// r08: the threaded driver replaced a panic with `Error::Cancelled`.
#[test]
fn pipe_011_threaded_panic_payload_is_preserved() {
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(|| {
        Pipeline::from_iter(0..3)
            .map(|n: i32| if n == 1 { panic!("boom in map") } else { n })
            .sink(NullSink::new())
            .run_threaded()
    });
    assert_eq!(
        result.unwrap_err().downcast_ref::<&str>(),
        Some(&"boom in map")
    );
}

/// r08: the source could not be named.
#[test]
fn pipe_012_source_can_be_named() {
    let src = FnSource::new(|| -> Result<Option<u32>, &'static str> { Err("disk gone") });
    let e = Pipeline::from_source(src)
        .source_id("ingest")
        .map(|n: u32| n)
        .sink(NullSink::new())
        .run()
        .unwrap_err();
    assert_eq!(e.stage().map(|s| s.as_str()), Some("ingest"));
}

/// r08: `Error::source()` returned `None`.
#[test]
fn pipe_013_error_source_is_available() {
    use std::error::Error as _;
    let src = FnSource::new(|| -> Result<Option<u32>, std::io::Error> {
        Err(std::io::Error::other("io"))
    });
    let e = Pipeline::from_source(src)
        .sink(NullSink::new())
        .run()
        .unwrap_err();
    assert_eq!(e.source().map(|s| s.to_string()).as_deref(), Some("io"));
}
