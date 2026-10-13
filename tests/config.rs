//! Configuration validation (PIPE-002..006): bad settings are reported as
//! typed errors before anything runs, never as panics, hangs, or huge
//! allocations.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use common::{Log, RecSink, RecSource};
use pipe_io::sink::{NullSink, VecSink};
use pipe_io::{
    Batch, BatchPolicy, ByteSize, ConfigErrorKind, Error, Pipeline, RetryPolicy, Window,
    WindowPolicy, WindowSpec,
};

/// Run `f` on a thread and fail the test (instead of hanging) if it takes
/// longer than `limit`.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    let _worker = thread::spawn(move || {
        let _sent = tx.send(f());
    });
    rx.recv_timeout(limit)
        .expect("operation did not finish in time")
}

fn config_kind(err: Error) -> ConfigErrorKind {
    match err {
        Error::Config(cfg) => cfg.kind(),
        other => panic!("expected a config error, got {other:?}"),
    }
}

fn run_window(policy: impl Into<WindowSpec> + Send + 'static) -> Result<u64, Error> {
    Pipeline::from_iter([1_u32, 2, 3])
        .window(policy)
        .sink(NullSink::<Window<u32>>::new())
        .run()
        .map(|s| s.items_in)
}

#[test]
fn zero_window_durations_are_rejected_without_hanging() {
    let cases = [
        (
            WindowPolicy::tumbling(Duration::ZERO),
            ConfigErrorKind::ZeroDuration { field: "size" },
        ),
        (
            WindowPolicy::sliding(Duration::ZERO, Duration::from_secs(1)),
            ConfigErrorKind::ZeroDuration { field: "size" },
        ),
        (
            WindowPolicy::sliding(Duration::from_secs(1), Duration::ZERO),
            ConfigErrorKind::ZeroDuration { field: "slide" },
        ),
        (
            WindowPolicy::session(Duration::ZERO),
            ConfigErrorKind::ZeroDuration { field: "idle" },
        ),
    ];
    for (policy, kind) in cases {
        let err = within(Duration::from_secs(5), move || run_window(policy)).unwrap_err();
        assert_eq!(config_kind(err), kind, "{policy:?}");
    }
}

#[test]
fn unsupported_sliding_shapes_are_rejected() {
    let err = run_window(WindowPolicy::sliding(
        Duration::from_secs(1),
        Duration::from_secs(5),
    ))
    .unwrap_err();
    assert_eq!(config_kind(err), ConfigErrorKind::SlideExceedsSize);
    let err = run_window(WindowPolicy::sliding(
        Duration::from_secs(3600),
        Duration::from_millis(1),
    ))
    .unwrap_err();
    assert!(matches!(
        config_kind(err),
        ConfigErrorKind::TooManyOverlappingWindows {
            overlap: 3_600_000,
            ..
        }
    ));
}

#[test]
fn extreme_window_durations_run_without_panicking() {
    for policy in [
        WindowPolicy::tumbling(Duration::MAX),
        WindowPolicy::session(Duration::MAX),
        WindowPolicy::sliding(Duration::MAX, Duration::MAX),
        WindowPolicy::sliding(Duration::MAX, Duration::MAX / 1000),
    ] {
        let items = within(Duration::from_secs(5), move || run_window(policy)).unwrap();
        assert_eq!(items, 3, "{policy:?}");
    }
}

#[test]
fn window_limits_must_be_positive() {
    let err = run_window(WindowPolicy::tumbling(Duration::from_secs(1)).max_items(0)).unwrap_err();
    assert_eq!(
        config_kind(err),
        ConfigErrorKind::ZeroLimit { field: "max_items" }
    );
    let err = Pipeline::from_iter([1_u32])
        .window_by_key(
            |n: &u32| *n,
            WindowPolicy::tumbling(Duration::from_secs(1)).max_keys(0),
        )
        .try_sink(NullSink::<(u32, Window<u32>)>::new())
        .unwrap_err();
    assert_eq!(err.kind(), ConfigErrorKind::ZeroLimit { field: "max_keys" });
}

#[test]
fn batch_policies_are_validated() {
    let cases = [
        (BatchPolicy::new(), ConfigErrorKind::NoBatchTrigger),
        (
            BatchPolicy::new().max_items(0),
            ConfigErrorKind::ZeroLimit { field: "max_items" },
        ),
        (
            BatchPolicy::new().max_bytes(8),
            ConfigErrorKind::BytesRequireByteSize,
        ),
        (
            BatchPolicy::new().max_age(Duration::ZERO),
            ConfigErrorKind::ZeroDuration { field: "max_age" },
        ),
    ];
    for (policy, kind) in cases {
        let err = Pipeline::from_iter([1_u32])
            .stage_id("grouper")
            .batch(policy)
            .try_sink(NullSink::<Batch<u32>>::new())
            .unwrap_err();
        assert_eq!(err.kind(), kind, "{policy:?}");
        assert_eq!(err.stage().as_str(), "grouper");
    }
    let err = Pipeline::from_iter([String::new()])
        .batch_bytes(BatchPolicy::new().max_bytes(0))
        .try_sink(NullSink::<Batch<String>>::new())
        .unwrap_err();
    assert_eq!(
        err.kind(),
        ConfigErrorKind::ZeroLimit { field: "max_bytes" }
    );
}

#[test]
fn huge_batch_limits_do_not_allocate_up_front() {
    // Previously `max_items(usize::MAX)` panicked with "capacity overflow"
    // and large limits reserved max_items slots per batch.
    let started = Instant::now();
    let sink = VecSink::<usize>::new();
    let handle = sink.handle();
    Pipeline::from_iter(0_u64..3)
        .batch(BatchPolicy::new().max_items(usize::MAX))
        .map(|b: Batch<u64>| b.into_inner().capacity())
        .sink(sink)
        .run()
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(handle.take()[0] <= 1024);
}

#[test]
fn byte_counter_cannot_overflow() {
    #[derive(Clone)]
    struct Big;
    impl ByteSize for Big {
        fn byte_size(&self) -> usize {
            usize::MAX / 2 + 1
        }
    }
    let sink = VecSink::<usize>::new();
    let handle = sink.handle();
    Pipeline::from_iter(vec![Big, Big, Big])
        .batch_bytes(BatchPolicy::new().max_items(10).max_bytes(usize::MAX))
        .map(|b: Batch<Big>| b.len())
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![2, 1]);
}

#[test]
fn idle_tick_and_retry_are_validated() {
    let err = Pipeline::from_iter([1_u32])
        .idle_tick(Duration::ZERO)
        .try_sink(NullSink::new())
        .unwrap_err();
    assert_eq!(
        err.kind(),
        ConfigErrorKind::ZeroDuration { field: "idle_tick" }
    );
    let err = Pipeline::from_iter([1_u32])
        .try_map_retry(RetryPolicy::new(0), |n: &u32| Ok::<u32, &'static str>(*n))
        .try_sink(NullSink::new())
        .unwrap_err();
    assert_eq!(
        err.kind(),
        ConfigErrorKind::ZeroLimit {
            field: "max_attempts"
        }
    );
}

#[test]
fn config_error_touches_no_component_and_is_sticky() {
    let log = Log::default();
    let mut pipeline = Pipeline::from_source(RecSource::new(&log, 3))
        .batch(BatchPolicy::new())
        .sink(RecSink::<Batch<u32>>::new(&log, "main"));
    assert_eq!(
        pipeline.config_error().map(|e| e.kind()),
        Some(ConfigErrorKind::NoBatchTrigger)
    );
    assert!(matches!(pipeline.step(), Err(Error::Config(_))));
    assert!(matches!(pipeline.step(), Err(Error::Closed)));
    assert!(pipeline.is_finished());
    assert!(matches!(pipeline.finish(), Err(Error::Closed)));
    assert!(log.entries().is_empty());

    let err = Pipeline::from_source(RecSource::new(&log, 3))
        .batch(BatchPolicy::new())
        .sink(RecSink::<Batch<u32>>::new(&log, "main"))
        .run_threaded()
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)));
    assert!(log.entries().is_empty());
}

#[test]
fn first_config_error_wins() {
    let err = Pipeline::from_iter([1_u32])
        .stage_id("first")
        .batch(BatchPolicy::new())
        .map(|b: Batch<u32>| b.len())
        .stage_id("second")
        .window(WindowPolicy::tumbling(Duration::ZERO))
        .try_sink(NullSink::<Window<usize>>::new())
        .unwrap_err();
    assert_eq!(err.stage().as_str(), "first");
}

#[test]
fn valid_pipeline_passes_try_sink() {
    let pipeline = Pipeline::from_iter([1_u32])
        .batch(BatchPolicy::new().max_items(1))
        .try_sink(NullSink::<Batch<u32>>::new())
        .unwrap();
    assert!(pipeline.config_error().is_none());
    assert_eq!(pipeline.run().unwrap().items_out, 1);
}
