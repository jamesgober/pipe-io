//! Retry, input capture, observability, error chains, and the std
//! adapters end to end.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::error::Error as _;
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use common::Fail;
use pipe_io::sink::{ChannelSink, NullSink, TeeSink, VecSink, WriterSink};
use pipe_io::source::{
    ByteLinesSource, ChainSource, ChannelSource, FnSource, IterSource, LineOverflow, ReaderError,
    ReaderSource,
};
use pipe_io::{
    Backoff, Batch, BatchPolicy, Error, ErrorPolicy, Event, ManualClock, Pipeline, RetryPolicy,
    StageFailure, Step,
};

#[test]
fn retry_succeeds_after_transient_failures() {
    let calls = Arc::new(Mutex::new(0));
    let c2 = Arc::clone(&calls);
    let sink = VecSink::new();
    let out = sink.handle();
    let started = Instant::now();
    let stats = Pipeline::from_iter([10_u32, 20])
        .try_map_retry(
            RetryPolicy::new(3).backoff(Backoff::Fixed(Duration::from_millis(5))),
            move |n: &u32| {
                let mut c = c2.lock().unwrap();
                *c += 1;
                // The first item fails twice, the second never.
                if *n == 10 && *c < 3 {
                    Err(Fail("flaky"))
                } else {
                    Ok(*n + 1)
                }
            },
        )
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(out.take(), vec![11, 21]);
    assert_eq!(stats.retries, 2);
    assert_eq!(stats.errors, 0);
    assert!(
        started.elapsed() >= Duration::from_millis(10),
        "backoff slept"
    );
    assert_eq!(*calls.lock().unwrap(), 4);
}

#[test]
fn retry_exhaustion_routes_input_and_attempts_to_dead_letter() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let e2 = Arc::clone(&events);
    let dlq = VecSink::<StageFailure>::new();
    let failures = dlq.handle();
    let stats = Pipeline::from_iter(["ok", "bad"])
        .observe(move |e: &Event<'_>| {
            if let Event::Retry { stage, attempt, .. } = e {
                e2.lock().unwrap().push((stage.as_str(), *attempt));
            }
        })
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("call")
        .try_map_retry(
            RetryPolicy::new(3)
                .backoff(Backoff::Exponential {
                    initial: Duration::from_millis(1),
                    max: Duration::from_millis(2),
                })
                .jitter(true),
            |s: &&str| {
                if *s == "bad" {
                    Err(Fail("down"))
                } else {
                    Ok(s.len())
                }
            },
        )
        .dead_letter(dlq)
        .sink(NullSink::new())
        .run()
        .unwrap();
    assert_eq!(
        (stats.retries, stats.errors, stats.dead_lettered),
        (2, 1, 1)
    );
    assert_eq!(*events.lock().unwrap(), vec![("call", 1), ("call", 2)]);
    let mut failed = failures.take();
    assert_eq!(failed[0].attempts, 3);
    assert_eq!(failed[0].take_input::<&str>(), Some("bad"));
    assert!(failed[0].source.downcast_ref::<Fail>().is_some());
}

#[test]
fn retry_exhaustion_under_fail_fast_stops() {
    let err = Pipeline::from_iter([1_u32])
        .try_map_retry(RetryPolicy::new(2), |_: &u32| Err::<u32, _>(Fail("always")))
        .sink(NullSink::new())
        .run()
        .unwrap_err();
    assert_eq!(err.to_string(), "stage `try_map_retry`: fail: always");
}

#[test]
fn try_map_ref_keeps_input_only_for_dead_letter() {
    let dlq = VecSink::<StageFailure>::new();
    let failures = dlq.handle();
    Pipeline::from_iter(vec![String::from("7"), String::from("x")])
        .on_error(ErrorPolicy::DeadLetter)
        .try_map_ref(|s: &String| s.parse::<u8>())
        .dead_letter(dlq)
        .sink(NullSink::new())
        .run()
        .unwrap();
    let mut failed = failures.take();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].input_ref::<String>().map(String::as_str),
        Some("x")
    );
    assert_eq!(failed[0].take_input::<String>().as_deref(), Some("x"));

    let stats = Pipeline::from_iter(["x"])
        .on_error(ErrorPolicy::Continue)
        .try_map_ref(|s: &&str| s.parse::<u8>())
        .sink(NullSink::new())
        .run()
        .unwrap();
    assert_eq!(stats.errors, 1);
}

#[test]
fn observer_receives_lifecycle_and_error_events() {
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let e2 = Arc::clone(&events);
    Pipeline::from_iter(["1", "x", "y"])
        .observe(move |e: &Event<'_>| {
            let line = match e {
                Event::Started => "started".to_owned(),
                Event::StageError {
                    stage,
                    policy,
                    error,
                } => {
                    format!("error {stage} {policy:?} {error}")
                }
                Event::DeadLettered { stage } => format!("dead-lettered {stage}"),
                Event::Finished { stats, error } => {
                    format!(
                        "finished in={} out={} ok={}",
                        stats.items_in,
                        stats.items_out,
                        error.is_none()
                    )
                }
                other => format!("{other:?}"),
            };
            e2.lock().unwrap().push(line);
        })
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("parse")
        .try_map(|s: &str| s.parse::<u8>())
        .dead_letter(NullSink::new())
        .sink(NullSink::new())
        .run()
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            "started",
            "error parse DeadLetter invalid digit found in string",
            "dead-lettered parse",
            "error parse DeadLetter invalid digit found in string",
            "dead-lettered parse",
            "finished in=3 out=1 ok=true",
        ]
    );
}

#[test]
fn error_source_chain_reaches_the_original_error() {
    let source = FnSource::new(|| -> Result<Option<u32>, std::io::Error> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "no access",
        ))
    });
    let err = Pipeline::from_source(source)
        .sink(NullSink::new())
        .run()
        .unwrap_err();
    let inner = err.source().expect("chain continues past pipe_io::Error");
    assert_eq!(inner.to_string(), "no access");
    let io = err.downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
    let boxed = err.into_inner().unwrap();
    assert!(boxed.downcast_ref::<std::io::Error>().is_some());
}

#[test]
fn reader_source_limits_protect_the_pipeline() {
    // An attacker-controlled stream with one endless line in the middle.
    let input = Cursor::new(b"first\n".to_vec())
        .chain(std::io::repeat(b'z').take(4 << 20))
        .chain(Cursor::new(b"\nlast\n".to_vec()));
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_source(
        ReaderSource::new(input)
            .max_line_len(64 * 1024)
            .on_overflow(LineOverflow::Skip),
    )
    .sink(sink)
    .run()
    .unwrap();
    assert_eq!(out.take(), vec!["first", "last"]);

    let err =
        Pipeline::from_source(ReaderSource::new(Cursor::new("a\nbbbbbbbbbbbb\n")).max_line_len(4))
            .source_id("stdin")
            .sink(NullSink::new())
            .run()
            .unwrap_err();
    assert_eq!(err.stage().map(|s| s.as_str()), Some("stdin"));
    assert!(matches!(
        err.downcast_ref::<ReaderError>(),
        Some(ReaderError::LineTooLong { line: 2, limit: 4 })
    ));
}

#[test]
fn byte_lines_and_writer_sink_round_trip() {
    let sink = WriterSink::new(Vec::<u8>::new());
    let collected = Arc::new(Mutex::new(Vec::new()));
    let c2 = Arc::clone(&collected);
    Pipeline::from_source(ByteLinesSource::new(Cursor::new(b"a\r\nbc\n".to_vec())))
        .map(|line: Vec<u8>| String::from_utf8_lossy(&line).to_uppercase())
        .inspect(move |s: &String| c2.lock().unwrap().push(s.clone()))
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(*collected.lock().unwrap(), vec!["A", "BC"]);
}

#[test]
fn tee_and_chain_compose() {
    let (a, b) = (VecSink::new(), VecSink::new());
    let (ha, hb) = (a.handle(), b.handle());
    Pipeline::from_source(ChainSource::new(
        IterSource::new([1, 2]),
        IterSource::new([3]),
    ))
    .sink(TeeSink::new(a, b))
    .run()
    .unwrap();
    assert_eq!(ha.take(), vec![1, 2, 3]);
    assert_eq!(hb.take(), vec![1, 2, 3]);
}

#[test]
fn channel_source_to_channel_sink_with_a_slow_consumer() {
    let (in_tx, in_rx) = mpsc::sync_channel::<u32>(2);
    let (out_tx, out_rx) = mpsc::sync_channel::<u32>(1);
    let producer = thread::spawn(move || {
        for n in 0..50 {
            in_tx.send(n).unwrap();
        }
    });
    let consumer = thread::spawn(move || {
        let mut got = Vec::new();
        while let Ok(n) = out_rx.recv() {
            thread::sleep(Duration::from_micros(200));
            got.push(n);
        }
        got
    });
    let stats = Pipeline::from_source(ChannelSource::new(in_rx))
        .map(|n: u32| n * 2)
        .sink(ChannelSink::new(out_tx))
        .run()
        .unwrap();
    producer.join().unwrap();
    let got = consumer.join().unwrap();
    assert_eq!(stats.items_out, 50);
    assert_eq!(got, (0..50).map(|n| n * 2).collect::<Vec<_>>());
}

#[test]
fn channel_sink_disconnect_is_a_sink_error() {
    let (tx, rx) = mpsc::sync_channel::<u32>(4);
    drop(rx);
    let err = Pipeline::from_iter([1_u32])
        .sink(ChannelSink::new(tx))
        .run()
        .unwrap_err();
    assert!(matches!(err, Error::Sink { .. }));
}

#[test]
fn age_triggered_batch_flushes_on_a_quiet_channel() {
    let clock = ManualClock::new(Instant::now());
    let (tx, rx) = mpsc::channel::<u32>();
    let sink = VecSink::<Vec<u32>>::new();
    let out = sink.handle();
    let mut p = Pipeline::from_source(ChannelSource::new(rx))
        .idle_tick(Duration::from_millis(1))
        .batch_with(
            BatchPolicy::new()
                .max_items(100)
                .max_age(Duration::from_secs(1)),
            clock.clone(),
        )
        .map(|b: Batch<u32>| b.into_inner())
        .sink(sink);
    tx.send(1).unwrap();
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(p.step().unwrap(), Step::Idle);
    assert!(out.is_empty());
    clock.advance(Duration::from_secs(1));
    assert_eq!(p.step().unwrap(), Step::Idle);
    assert_eq!(out.take(), vec![vec![1]]);
    drop(tx);
    assert_eq!(p.step().unwrap(), Step::Done);
}

#[test]
fn batch_bytes_with_clock() {
    let clock = ManualClock::new(Instant::now());
    let sink = VecSink::<usize>::new();
    let out = sink.handle();
    Pipeline::from_iter(["ab", "cd", "ef"])
        .batch_bytes_with(
            BatchPolicy::new()
                .max_bytes(4)
                .max_age(Duration::from_secs(9)),
            clock,
        )
        .map(|b: Batch<&str>| b.len())
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(out.take(), vec![2, 1]);
}
