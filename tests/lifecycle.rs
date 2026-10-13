//! Lifecycle guarantees: flush propagation and teardown on every exit
//! path (PIPE-001, PIPE-007, PIPE-019).

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use common::{Fail, Fault, Log, RecSink, RecSource};
use pipe_io::sink::VecSink;
use pipe_io::{
    Batch, BatchPolicy, Emit, Error, ErrorPolicy, Event, Pipeline, Stage, StageFailure, Step,
};

/// Passes items through, fails (or panics) on demand.
struct Faulty {
    fail_on_item: Option<u32>,
    panic_on_item: Option<u32>,
    fail_flush: bool,
    fail_tick: bool,
}

impl Faulty {
    const fn new() -> Self {
        Self {
            fail_on_item: None,
            panic_on_item: None,
            fail_flush: false,
            fail_tick: false,
        }
    }
}

impl Stage for Faulty {
    type Input = u32;
    type Output = u32;
    type Error = Fail;

    fn process(&mut self, item: u32, out: &mut dyn Emit<Item = u32>) -> Result<(), Fail> {
        if self.panic_on_item == Some(item) {
            panic!("stage panic on {item}");
        }
        if self.fail_on_item == Some(item) {
            return Err(Fail("stage"));
        }
        if out.emit(item).is_err() {
            return Ok(());
        }
        Ok(())
    }

    fn tick(&mut self, _out: &mut dyn Emit<Item = u32>) -> Result<(), Fail> {
        if self.fail_tick {
            Err(Fail("tick"))
        } else {
            Ok(())
        }
    }

    fn flush(&mut self, _out: &mut dyn Emit<Item = u32>) -> Result<(), Fail> {
        if self.fail_flush {
            Err(Fail("stage flush"))
        } else {
            Ok(())
        }
    }
}

fn quiet_panics() {
    // Keep test output readable; panics are asserted explicitly.
    std::panic::set_hook(Box::new(|_| {}));
}

#[test]
fn happy_path_order_is_sink_then_source() {
    let log = Log::default();
    let sink = RecSink::<u32>::new(&log, "main");
    Pipeline::from_source(RecSource::new(&log, 2))
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(
        log.entries(),
        [
            "main.write",
            "main.write",
            "main.flush",
            "main.close",
            "source.close"
        ]
    );
}

#[test]
fn stage_flush_error_under_continue_still_flushes_downstream() {
    for policy in [ErrorPolicy::Continue, ErrorPolicy::DeadLetter] {
        let log = Log::default();
        let sink = RecSink::<Vec<u32>>::new(&log, "main");
        let items = sink.items();
        let mut stage = Faulty::new();
        stage.fail_flush = true;
        let stats = Pipeline::from_source(RecSource::new(&log, 5))
            .on_error(policy)
            .stage(stage)
            .dead_letter(RecSink::<StageFailure>::new(&log, "dlq"))
            .batch(BatchPolicy::new().max_items(100))
            .map(|b: Batch<u32>| b.into_inner())
            .sink(sink)
            .run()
            .unwrap();
        assert_eq!(
            *items.lock().unwrap(),
            vec![vec![1, 2, 3, 4, 5]],
            "{policy:?}"
        );
        assert!(
            log.contains("main.flush") && log.contains("main.close"),
            "{policy:?}"
        );
        assert_eq!(stats.errors, 1);
        let routed = u64::from(policy == ErrorPolicy::DeadLetter);
        assert_eq!(stats.dead_lettered, routed);
        assert_eq!(log.count("dlq.write") as u64, routed);
    }
}

#[test]
fn stage_flush_error_under_fail_fast_aborts_but_tears_down() {
    let log = Log::default();
    let mut stage = Faulty::new();
    stage.fail_flush = true;
    let err = Pipeline::from_source(RecSource::new(&log, 3))
        .stage(stage)
        .sink(RecSink::<u32>::new(&log, "main"))
        .run()
        .unwrap_err();
    assert_eq!(err.to_string(), "stage `stage`: fail: stage flush");
    let entries = log.entries();
    assert_eq!(
        &entries[3..],
        ["main.flush", "main.close", "source.close"],
        "sink flushed once by abort, then closed; source closed"
    );
}

#[test]
fn fail_fast_error_mid_stream_tears_down_everything() {
    let log = Log::default();
    let res = Pipeline::from_source(RecSource::new(&log, 5))
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("one")
        .try_map(|n: u32| if n == 1 { Err("one") } else { Ok(n) })
        .on_error(ErrorPolicy::FailFast)
        .stage_id("three")
        .try_map(|n: u32| if n == 3 { Err("three") } else { Ok(n) })
        .dead_letter(RecSink::<StageFailure>::new(&log, "dlq"))
        .sink(RecSink::<u32>::new(&log, "main"))
        .run();
    let err = res.unwrap_err();
    assert_eq!(err.stage().map(|s| s.as_str()), Some("three"));
    assert_eq!(
        log.entries(),
        [
            "dlq.write",
            "main.write",
            "main.flush",
            "main.close",
            "source.close",
            "dlq.flush",
            "dlq.close"
        ]
    );
}

#[test]
fn source_error_tears_down_sink_and_closes_source() {
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 5).fault(Fault::At(3)))
        .sink(RecSink::<u32>::new(&log, "main"))
        .run()
        .unwrap_err();
    assert!(matches!(err, Error::Source { .. }));
    assert_eq!(
        log.entries(),
        [
            "main.write",
            "main.write",
            "main.flush",
            "main.close",
            "source.close"
        ]
    );
}

#[test]
fn sink_write_error_still_flushes_closes_and_keeps_first_error() {
    let log = Log::default();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_obs = Arc::clone(&seen);
    let err = Pipeline::from_source(RecSource::new(&log, 5).fault(Fault::Close))
        .observe(move |e: &Event<'_>| {
            if let Event::TeardownError { error } = e {
                seen_obs.lock().unwrap().push(error.to_string());
            }
        })
        .sink(RecSink::<u32>::new(&log, "main").fault(Fault::At(2)))
        .run()
        .unwrap_err();
    assert_eq!(err.to_string(), "sink `sink`: fail: sink write");
    assert!(log.contains("main.flush") && log.contains("main.close"));
    assert!(log.contains("source.close"));
    // The later source-close failure is not lost: it reaches the observer.
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["source `source`: fail: source close"]
    );
}

#[test]
fn source_close_error_on_success_is_returned() {
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 1).fault(Fault::Close))
        .sink(RecSink::<u32>::new(&log, "main"))
        .run()
        .unwrap_err();
    assert!(matches!(err, Error::Source { .. }));
}

#[test]
fn sink_flush_error_on_success_is_returned_and_sink_is_still_closed() {
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 1))
        .sink(RecSink::<u32>::new(&log, "main").fault(Fault::Flush))
        .run()
        .unwrap_err();
    assert_eq!(err.to_string(), "sink `sink`: fail: sink flush");
    assert!(log.contains("main.close"));
    assert!(log.contains("source.close"));
}

#[test]
fn panic_in_stage_propagates_after_teardown() {
    quiet_panics();
    let log = Log::default();
    let log2 = log.clone();
    let mut stage = Faulty::new();
    stage.panic_on_item = Some(2);
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_source(RecSource::new(&log2, 5))
            .on_error(ErrorPolicy::DeadLetter)
            .stage(stage)
            .dead_letter(RecSink::<StageFailure>::new(&log2, "dlq"))
            .sink(RecSink::<u32>::new(&log2, "main"))
            .run()
    }));
    let payload = result.unwrap_err();
    assert_eq!(
        payload.downcast_ref::<String>().map(String::as_str),
        Some("stage panic on 2")
    );
    assert_eq!(
        log.entries(),
        [
            "main.write",
            "main.flush",
            "main.close",
            "source.close",
            "dlq.flush",
            "dlq.close"
        ]
    );
}

#[test]
fn panic_in_sink_still_flushes_closes_and_closes_source() {
    quiet_panics();
    let log = Log::default();
    let log2 = log.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_source(RecSource::new(&log2, 5))
            .sink(RecSink::<u32>::new(&log2, "main").fault(Fault::PanicAt(2)))
            .run()
    }));
    assert!(result.is_err());
    assert_eq!(
        log.entries(),
        [
            "main.write",
            "main.write",
            "main.flush",
            "main.close",
            "source.close"
        ]
    );
}

#[test]
fn panic_during_teardown_is_contained() {
    // A stage panics, then the sink's flush panics during teardown. The
    // second panic must not abort the process; the original payload wins.
    quiet_panics();
    let log = Log::default();
    let log2 = log.clone();
    let mut stage = Faulty::new();
    stage.panic_on_item = Some(1);
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_source(RecSource::new(&log2, 3))
            .stage(stage)
            .sink(RecSink::<u32>::new(&log2, "main").fault(Fault::PanicFlush))
            .run()
    }));
    let payload = result.unwrap_err();
    assert_eq!(
        payload.downcast_ref::<String>().map(String::as_str),
        Some("stage panic on 1")
    );
    assert!(log.contains("main.flush"));
}

#[test]
fn panic_in_source_still_tears_down_sink() {
    quiet_panics();
    let log = Log::default();
    let log2 = log.clone();
    let result = catch_unwind(AssertUnwindSafe(move || {
        Pipeline::from_source(RecSource::new(&log2, 5).fault(Fault::PanicAt(2)))
            .sink(RecSink::<u32>::new(&log2, "main"))
            .run()
    }));
    assert!(result.is_err());
    assert_eq!(
        log.entries(),
        ["main.write", "main.flush", "main.close", "source.close"]
    );
}

#[test]
fn dropping_a_stepped_pipeline_tears_it_down() {
    let log = Log::default();
    let mut pipeline = Pipeline::from_source(RecSource::new(&log, 5))
        .dead_letter(RecSink::<StageFailure>::new(&log, "dlq"))
        .sink(RecSink::<u32>::new(&log, "main"));
    assert_eq!(pipeline.step().unwrap(), Step::Item);
    drop(pipeline);
    assert_eq!(
        log.entries(),
        [
            "main.write",
            "main.flush",
            "main.close",
            "source.close",
            "dlq.flush",
            "dlq.close"
        ]
    );
}

#[test]
fn dropping_an_unstarted_pipeline_calls_nothing() {
    let log = Log::default();
    let pipeline =
        Pipeline::from_source(RecSource::new(&log, 5)).sink(RecSink::<u32>::new(&log, "main"));
    drop(pipeline);
    assert!(log.entries().is_empty());
}

#[test]
fn downstream_error_surfaces_even_if_stage_ignores_closed() {
    // `Faulty` swallows EmitError::Closed; the sink error must still win.
    let log = Log::default();
    let err = Pipeline::from_source(RecSource::new(&log, 5))
        .stage(Faulty::new())
        .sink(RecSink::<u32>::new(&log, "main").fault(Fault::At(1)))
        .run()
        .unwrap_err();
    assert!(matches!(err, Error::Sink { .. }));
    assert_eq!(log.count("main.write"), 1, "no writes after the failure");
}

#[test]
fn custom_stage_errors_follow_policy() {
    let log = Log::default();
    let mut stage = Faulty::new();
    stage.fail_on_item = Some(2);
    let out = VecSink::new();
    let handle = out.handle();
    let dlq = VecSink::<StageFailure>::new();
    let failures = dlq.handle();
    Pipeline::from_source(RecSource::new(&log, 3))
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("faulty")
        .stage(stage)
        .dead_letter(dlq)
        .sink(out)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![1, 3]);
    let failed = failures.take();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].stage.as_str(), "faulty");
    assert!(
        failed[0].input.is_none(),
        "custom stages cannot return their input"
    );
    assert_eq!(failed[0].attempts, 1);
}

#[test]
fn items_emitted_before_a_handled_error_are_counted() {
    /// Emits the item twice, then fails.
    struct EchoThenFail;
    impl Stage for EchoThenFail {
        type Input = u32;
        type Output = u32;
        type Error = Fail;
        fn process(&mut self, item: u32, out: &mut dyn Emit<Item = u32>) -> Result<(), Fail> {
            for _ in 0..2 {
                if out.emit(item).is_err() {
                    return Ok(());
                }
            }
            Err(Fail("after emitting"))
        }
    }
    let log = Log::default();
    let sink = RecSink::<u32>::new(&log, "main");
    let items = sink.items();
    let stats = Pipeline::from_source(RecSource::new(&log, 3))
        .on_error(ErrorPolicy::Continue)
        .stage(EchoThenFail)
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(*items.lock().unwrap(), vec![1, 1, 2, 2, 3, 3]);
    assert_eq!((stats.items_in, stats.items_out, stats.errors), (3, 6, 3));
}

#[test]
fn custom_stage_tick_errors_follow_policy() {
    let log = Log::default();
    let mut stage = Faulty::new();
    stage.fail_tick = true;
    let mut pipeline = Pipeline::from_source(RecSource::new(&log, 3))
        .on_error(ErrorPolicy::Continue)
        .stage(stage)
        .sink(RecSink::<u32>::new(&log, "main"));
    pipeline.tick().unwrap();
    assert_eq!(pipeline.stats().errors, 1);

    let mut stage = Faulty::new();
    stage.fail_tick = true;
    let mut strict = Pipeline::from_source(RecSource::new(&log, 3))
        .stage(stage)
        .sink(RecSink::<u32>::new(&log, "strict"));
    let err = strict.tick().unwrap_err();
    assert_eq!(err.to_string(), "stage `stage`: fail: tick");
    assert!(log.contains("strict.close"));
    assert!(matches!(strict.step(), Err(Error::Closed)));
}
