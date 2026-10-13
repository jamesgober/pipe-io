//! The steppable pipeline API (PIPE-009) and cancellation.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use common::{Fault, Log, RecSink, RecSource};
use pipe_io::driver::{Driver, RunStats, SyncDriver};
use pipe_io::sink::{NullSink, VecSink};
use pipe_io::{
    Batch, BatchPolicy, Error, ManualClock, Pipeline, Result, Source, Step, Window, WindowPolicy,
};

#[test]
fn step_drives_one_item_at_a_time() {
    let log = Log::default();
    let sink = RecSink::<u32>::new(&log, "main");
    let items = sink.items();
    let mut p = Pipeline::from_source(RecSource::new(&log, 3)).sink(sink);
    assert_eq!(p.stats(), RunStats::default());
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(*items.lock().unwrap(), vec![1]);
    assert_eq!(p.stats().items_in, 1);
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(p.step().unwrap(), Step::Item);
    assert!(!p.is_finished());
    assert_eq!(p.step().unwrap(), Step::Done);
    assert!(p.is_finished());
    assert_eq!(p.step().unwrap(), Step::Done, "Done is sticky");
    assert_eq!(p.stats().items_out, 3);
    assert_eq!(
        log.entries()[3..],
        ["main.flush", "main.close", "source.close"]
    );
    assert_eq!(p.finish().unwrap().items_out, 3);
}

#[test]
fn finish_stops_early_and_flushes() {
    let log = Log::default();
    let sink = RecSink::<Batch<u32>>::new(&log, "main");
    let items = sink.items();
    let mut p = Pipeline::from_source(RecSource::new(&log, 100))
        .batch(BatchPolicy::new().max_items(10))
        .sink(sink);
    for _ in 0..4 {
        assert_eq!(p.step().unwrap(), Step::Item);
    }
    let stats = p.finish().unwrap();
    assert_eq!(stats.items_in, 4);
    assert_eq!(items.lock().unwrap()[0].len(), 4, "open batch flushed");
    assert!(log.contains("source.close"));
}

#[test]
fn finish_without_start_runs_the_full_lifecycle() {
    let log = Log::default();
    let p =
        Pipeline::from_source(RecSource::new(&log, 100)).sink(RecSink::<u32>::new(&log, "main"));
    let stats = p.finish().unwrap();
    assert_eq!(stats.items_in, 0);
    assert_eq!(log.entries(), ["main.flush", "main.close", "source.close"]);
}

#[test]
fn errors_end_the_run_and_later_calls_report_closed() {
    let log = Log::default();
    let mut p =
        Pipeline::from_source(RecSource::new(&log, 5).fault(Fault::At(2)))
            .sink(RecSink::<u32>::new(&log, "main"));
    assert_eq!(p.step().unwrap(), Step::Item);
    assert!(matches!(p.step(), Err(Error::Source { .. })));
    assert!(log.contains("main.close") && log.contains("source.close"));
    assert!(matches!(p.step(), Err(Error::Closed)));
    assert!(matches!(p.tick(), Err(Error::Closed)));
    assert!(matches!(p.finish(), Err(Error::Closed)));
}

#[test]
fn tick_lets_an_executor_drive_time() {
    let clock = ManualClock::new(Instant::now());
    let sink = VecSink::<Vec<u32>>::new();
    let out = sink.handle();
    let mut p = Pipeline::from_iter([1_u32, 2, 3])
        .window_with(
            WindowPolicy::tumbling(Duration::from_secs(5)),
            clock.clone(),
        )
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink);
    p.tick().unwrap(); // starts the run; nothing to emit yet
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(p.step().unwrap(), Step::Item);
    clock.advance(Duration::from_secs(5));
    p.tick().unwrap();
    assert_eq!(out.take(), vec![vec![1, 2]]);
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(p.step().unwrap(), Step::Done);
    assert_eq!(out.take(), vec![vec![3]]);
}

#[test]
fn cancel_while_stepping_drains_then_reports_cancelled() {
    let log = Log::default();
    let sink = RecSink::<Batch<u32>>::new(&log, "main");
    let items = sink.items();
    let mut p = Pipeline::from_source(RecSource::new(&log, 100))
        .batch(BatchPolicy::new().max_items(50))
        .sink(sink);
    let handle = p.cancel_handle();
    let again = p.cancel_handle();
    assert_eq!(p.step().unwrap(), Step::Item);
    assert!(!again.is_cancelled());
    handle.cancel();
    assert!(again.is_cancelled(), "handles share one flag");
    assert!(matches!(p.step(), Err(Error::Cancelled)));
    assert_eq!(
        items.lock().unwrap()[0].len(),
        1,
        "pulled items were flushed"
    );
    assert!(log.contains("source.close"));
    assert!(matches!(p.step(), Err(Error::Closed)));
}

#[test]
fn cancel_stops_an_endless_sync_run() {
    let mut p = Pipeline::from_iter(0_u64..).sink(NullSink::new());
    let handle = p.cancel_handle();
    let worker = thread::spawn(move || p.run());
    thread::sleep(Duration::from_millis(20));
    handle.cancel();
    let err = worker.join().unwrap().unwrap_err();
    assert!(err.is_cancelled());
}

/// An executor that interleaves several pipelines on one thread,
/// round-robin, which needs nothing but `step`.
struct RoundRobin;

impl RoundRobin {
    fn run_all<S>(pipelines: Vec<Pipeline<S>>) -> Vec<Result<RunStats>>
    where
        S: Source + 'static,
        S::Item: Send + 'static,
    {
        let mut active: Vec<(usize, Pipeline<S>)> = pipelines.into_iter().enumerate().collect();
        let mut results: Vec<Option<Result<RunStats>>> = (0..active.len()).map(|_| None).collect();
        while !active.is_empty() {
            let mut still = Vec::new();
            for (idx, mut p) in active {
                match p.step() {
                    Ok(Step::Done) => results[idx] = Some(Ok(p.stats())),
                    Ok(_) => still.push((idx, p)),
                    Err(e) => results[idx] = Some(Err(e)),
                }
            }
            active = still;
        }
        results.into_iter().map(|r| r.unwrap()).collect()
    }
}

#[test]
fn external_executor_can_interleave_pipelines() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let make = |tag: char, n: u32| {
        let order = Arc::clone(&order);
        Pipeline::from_iter(0..n).sink(pipe_io::sink::FnSink::new(
            move |_x: u32| -> std::result::Result<(), &'static str> {
                order.lock().unwrap().push(tag);
                Ok(())
            },
        ))
    };
    let results = RoundRobin::run_all(vec![make('a', 3), make('b', 3)]);
    assert!(results.iter().all(|r| r.as_ref().unwrap().items_out == 3));
    assert_eq!(order.lock().unwrap().iter().collect::<String>(), "ababab");
}

struct Counting<'a>(&'a Mutex<u32>);
impl Driver for Counting<'_> {
    fn run<S>(self, mut pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        loop {
            *self.0.lock().unwrap() += 1;
            if pipeline.step()? == Step::Done {
                return Ok(pipeline.stats());
            }
        }
    }
}

#[test]
fn custom_driver_built_on_step() {
    let steps = Mutex::new(0);
    let stats = Pipeline::from_iter(0..5)
        .sink(NullSink::<i32>::new())
        .run_with(Counting(&steps))
        .unwrap();
    assert_eq!(stats.items_out, 5);
    assert_eq!(*steps.lock().unwrap(), 6);
    let sync = Pipeline::from_iter(0..5)
        .sink(NullSink::<i32>::new())
        .run_with(SyncDriver::new());
    assert_eq!(sync.unwrap().items_out, 5);
}

#[test]
fn duration_is_measured() {
    let stats = Pipeline::from_iter(0..3)
        .inspect(|_| thread::sleep(Duration::from_millis(2)))
        .sink(NullSink::<i32>::new())
        .run()
        .unwrap();
    assert!(stats.duration >= Duration::from_millis(6));
}
