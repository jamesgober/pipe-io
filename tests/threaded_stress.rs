//! Seeded, randomised stress test of the threaded driver: random edge
//! layouts and capacities (including hand-off edges), random failures,
//! panics, and cancellation at random points. Every run must end (no
//! hang), keep order, lose nothing on success, call each lifecycle method
//! exactly once, close the source only after the sink, and re-raise
//! panics with their payload.
//!
//! The default run covers a few hundred seeds; set `PIPE_IO_STRESS_RUNS`
//! for a longer soak (for example 100000).

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use common::{Fail, Fault, Log, RecSink, RecSource};
use pipe_io::{Error, ErrorPolicy, Pipeline};

/// xorshift64*: deterministic per seed, no dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, options: &[T]) -> T {
        options[self.below(options.len() as u64) as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    Clean,
    StageError,
    StagePanic,
    SinkError,
    SinkPanic,
    SourceError,
    Cancel,
}

#[derive(Debug)]
struct Case {
    items: u32,
    caps: Vec<usize>,
    plan: Plan,
    at: u32,
}

fn case(seed: u64) -> Case {
    let mut rng = Rng::new(seed);
    let items = rng.below(200) as u32;
    let edges = 1 + rng.below(3) as usize;
    let caps = (0..edges).map(|_| rng.pick(&[0, 1, 2, 16, 64])).collect();
    let plan = rng.pick(&[
        Plan::Clean,
        Plan::Clean,
        Plan::StageError,
        Plan::StagePanic,
        Plan::SinkError,
        Plan::SinkPanic,
        Plan::SourceError,
        Plan::Cancel,
    ]);
    let at = 1 + rng.below(u64::from(items.max(1))) as u32;
    Case {
        items,
        caps,
        plan,
        at,
    }
}

/// Outcome of one run: the run's result (`Err` holds a panic message),
/// the items the sink accepted, and the lifecycle log.
type Outcome = (Result<Result<u64, Error>, String>, Vec<u32>, Vec<String>);

/// Run one case.
fn run(case: &Case) -> Outcome {
    let log = Log::default();
    let fault = |p: Plan, k: u32| match p {
        Plan::SinkError if case.plan == p => Fault::At(k),
        Plan::SinkPanic if case.plan == p => Fault::PanicAt(k),
        _ => Fault::None,
    };
    let source_fault = if case.plan == Plan::SourceError {
        Fault::At(case.at)
    } else {
        Fault::None
    };
    let sink = RecSink::<u32>::new(&log, "main").fault(match case.plan {
        Plan::SinkError => fault(Plan::SinkError, case.at),
        Plan::SinkPanic => fault(Plan::SinkPanic, case.at),
        _ => Fault::None,
    });
    let items = sink.items();
    let (plan, at) = (case.plan, case.at);
    let stage = move |v: u32| -> Result<u32, Fail> {
        match plan {
            Plan::StageError if v == at => Err(Fail("stage")),
            Plan::StagePanic if v == at => panic!("stage panic {v}"),
            _ => Ok(v),
        }
    };
    let builder = Pipeline::from_source(RecSource::new(&log, case.items).fault(source_fault))
        .on_error(ErrorPolicy::FailFast);
    let mut pipeline = match case.caps.as_slice() {
        [a] => builder.buffer(*a).try_map(stage).sink(sink),
        [a, b] => builder
            .buffer(*a)
            .try_map(stage)
            .buffer(*b)
            .map(|v: u32| v)
            .sink(sink),
        [a, b, c, ..] => builder
            .buffer(*a)
            .map(|v: u32| v)
            .buffer(*b)
            .try_map(stage)
            .buffer(*c)
            .sink(sink),
        [] => unreachable!(),
    };
    let cancel = pipeline.cancel_handle();
    if case.plan == Plan::Cancel {
        let _canceller = thread::spawn(move || {
            thread::sleep(Duration::from_micros(u64::from(at) * 3));
            cancel.cancel();
        });
    }
    let result = catch_unwind(AssertUnwindSafe(move || {
        pipeline.run_threaded().map(|s| s.items_out)
    }))
    .map_err(|payload| {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default()
    });
    let delivered = items.lock().unwrap().clone();
    (result, delivered, log.entries())
}

fn check(seed: u64) {
    let case = case(seed);
    let (result, delivered, log) = run(&case);
    let what = format!("seed {seed}: {case:?} -> {result:?}, log {log:?}");
    // Order: whatever arrived is a prefix of the source, in order.
    assert!(
        delivered.iter().copied().eq(1..=delivered.len() as u32),
        "{what}"
    );
    // Lifecycle: each call exactly once; the source is closed last.
    let count = |e: &str| log.iter().filter(|x| x.as_str() == e).count();
    assert_eq!(count("main.flush"), 1, "{what}");
    assert_eq!(count("main.close"), 1, "{what}");
    assert_eq!(count("source.close"), 1, "{what}");
    assert_eq!(
        log.last().map(String::as_str),
        Some("source.close"),
        "{what}"
    );
    match (case.plan, &result) {
        (Plan::Clean, Ok(Ok(out))) => {
            assert_eq!(*out, u64::from(case.items), "{what}");
            assert_eq!(delivered.len() as u32, case.items, "{what}");
        }
        (Plan::Cancel, Ok(Ok(out))) => assert_eq!(*out, u64::from(case.items), "{what}"),
        (Plan::Cancel, Ok(Err(e))) => assert!(e.is_cancelled(), "{what}"),
        (Plan::StagePanic, Err(msg)) => {
            assert_eq!(*msg, format!("stage panic {}", case.at), "{what}");
        }
        (Plan::SinkPanic, Err(msg)) => assert_eq!(msg, "sink panic", "{what}"),
        (Plan::StageError | Plan::SinkError | Plan::SourceError, Ok(Err(_))) => {}
        // The failure point lies beyond the stream: a clean run.
        (_, Ok(Ok(out))) if case.at > case.items => {
            assert_eq!(*out, u64::from(case.items), "{what}");
        }
        _ => panic!("unexpected outcome: {what}"),
    }
}

#[test]
fn threaded_driver_survives_randomised_runs() {
    std::panic::set_hook(Box::new(|_| {}));
    let runs: u64 = std::env::var("PIPE_IO_STRESS_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let (tx, rx) = mpsc::channel();
    let _worker = thread::spawn(move || {
        for seed in 0..runs {
            // The panic hook is silenced for the injected panics, so turn
            // a failed check into a message for the main thread.
            if let Err(payload) = catch_unwind(|| check(seed)) {
                let msg = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .unwrap_or_else(|| format!("seed {seed}: check panicked"));
                let _sent = tx.send(Err(msg));
                return;
            }
        }
        let _sent = tx.send(Ok(()));
    });
    // Generous: a hang anywhere fails the test instead of blocking CI.
    let limit = Duration::from_secs(60 + runs / 50);
    let outcome = rx.recv_timeout(limit);
    drop(std::panic::take_hook());
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(msg)) => panic!("{msg}"),
        Err(_) => panic!("the stress runs did not finish within {limit:?} (hang)"),
    }
}
