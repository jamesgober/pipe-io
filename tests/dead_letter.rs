//! Integration tests for dead-letter routing (`v0.6.0`).

#![cfg(feature = "std")]

use pipe_io::sink::{NullSink, VecSink};
use pipe_io::source::IterSource;
use pipe_io::{ErrorPolicy, Pipeline, StageFailure};

#[derive(Debug)]
struct ParseFail(&'static str);
impl core::fmt::Display for ParseFail {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "parse fail: {}", self.0)
    }
}

#[derive(Debug)]
struct SinkBoom;
impl core::fmt::Display for SinkBoom {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("dead-letter sink boom")
    }
}

#[test]
fn dead_letter_routes_failures() {
    let main = VecSink::<u32>::new();
    let main_handle = main.handle();

    let dlq = VecSink::<StageFailure>::new();
    let dlq_handle = dlq.handle();

    Pipeline::from_source(IterSource::new(vec!["1", "x", "3", "y", "5"]))
        .stage_id("parse")
        .on_error(ErrorPolicy::DeadLetter)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .dead_letter(dlq)
        .sink(main)
        .run()
        .expect("run");

    // Main sink gets only the parseable items.
    assert_eq!(main_handle.take(), vec![1, 3, 5]);

    // DLQ gets the failures with their stage id.
    let failures = dlq_handle.take();
    assert_eq!(failures.len(), 2);
    for f in &failures {
        assert_eq!(f.stage.as_str(), "parse");
    }
    // Sanity-check the displayed source.
    let messages: Vec<String> = failures.iter().map(|f| format!("{}", f.source)).collect();
    assert!(messages.iter().all(|m| m.contains("not int")));
}

#[test]
fn dead_letter_callable_before_failing_stage() {
    // Same outcome as the previous test, but .dead_letter() is called
    // *before* the failing stage to verify install order doesn't matter.
    let main = VecSink::<u32>::new();
    let main_handle = main.handle();

    let dlq = VecSink::<StageFailure>::new();
    let dlq_handle = dlq.handle();

    Pipeline::from_source(IterSource::new(vec!["1", "x", "3"]))
        .dead_letter(dlq)
        .stage_id("parse")
        .on_error(ErrorPolicy::DeadLetter)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .sink(main)
        .run()
        .expect("run");

    assert_eq!(main_handle.take(), vec![1, 3]);
    let failures = dlq_handle.take();
    assert_eq!(failures.len(), 1);
}

#[test]
fn dead_letter_without_sink_silently_drops() {
    // No .dead_letter() call. DeadLetter policy degrades to Continue.
    let main = VecSink::<u32>::new();
    let main_handle = main.handle();

    Pipeline::from_source(IterSource::new(vec!["1", "x", "3", "y"]))
        .on_error(ErrorPolicy::DeadLetter)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .sink(main)
        .run()
        .expect("run");

    assert_eq!(main_handle.take(), vec![1, 3]);
}

#[test]
fn dead_letter_sink_error_bubbles_up() {
    let dlq =
        pipe_io::sink::FnSink::new(|_f: StageFailure| -> Result<(), SinkBoom> { Err(SinkBoom) });

    let res = Pipeline::from_source(IterSource::new(vec!["1", "x"]))
        .on_error(ErrorPolicy::DeadLetter)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .dead_letter(dlq)
        .sink(NullSink::<u32>::new())
        .run();

    let err = res.expect_err("dead-letter sink failure should bubble up");
    assert!(matches!(err, pipe_io::Error::Sink { .. }));
    assert_eq!(err.stage().map(|s| s.as_str()), Some("dead_letter"));
}

#[test]
fn fail_fast_overrides_dead_letter_install() {
    // Even with a dead-letter sink installed, FailFast policy still
    // bails out on the first error.
    let dlq = VecSink::<StageFailure>::new();
    let dlq_handle = dlq.handle();

    let res = Pipeline::from_source(IterSource::new(vec!["1", "x", "3"]))
        .on_error(ErrorPolicy::FailFast)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .dead_letter(dlq)
        .sink(NullSink::<u32>::new())
        .run();

    assert!(res.is_err());
    assert!(dlq_handle.take().is_empty());
}

#[test]
fn continue_does_not_route_to_dead_letter() {
    let dlq = VecSink::<StageFailure>::new();
    let dlq_handle = dlq.handle();

    let main = VecSink::<u32>::new();
    let main_handle = main.handle();

    Pipeline::from_source(IterSource::new(vec!["1", "x", "3"]))
        .on_error(ErrorPolicy::Continue)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not int")))
        .dead_letter(dlq)
        .sink(main)
        .run()
        .expect("run");

    assert_eq!(main_handle.take(), vec![1, 3]);
    assert!(dlq_handle.take().is_empty());
}
