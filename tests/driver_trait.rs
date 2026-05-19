//! Integration tests for the [`Driver`] trait (`v0.7.0`).

#![cfg(feature = "std")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use pipe_io::driver::{Driver, RunStats, SyncDriver, ThreadedDriver};
use pipe_io::sink::VecSink;
use pipe_io::source::IterSource;
use pipe_io::{Pipeline, Result};

// Helper that drives any pipeline through any Driver impl.
fn run_via_trait<D: Driver>(driver: D, sink: VecSink<i64>) -> Result<RunStats> {
    let pipeline = Pipeline::from_source(IterSource::new(vec![1i32, 2, 3, 4, 5]))
        .map(|n: i32| i64::from(n) * 2)
        .sink(sink);
    driver.run(pipeline)
}

#[test]
fn sync_driver_through_trait() {
    let sink = VecSink::<i64>::new();
    let handle = sink.handle();
    let stats = run_via_trait(SyncDriver::new(), sink).unwrap();
    assert_eq!(handle.take(), vec![2i64, 4, 6, 8, 10]);
    assert_eq!(stats.items_in, 5);
}

#[test]
fn threaded_driver_through_trait() {
    let sink = VecSink::<i64>::new();
    let handle = sink.handle();
    let stats = run_via_trait(ThreadedDriver::new(), sink).unwrap();
    assert_eq!(handle.take(), vec![2i64, 4, 6, 8, 10]);
    assert_eq!(stats.items_in, 5);
}

#[test]
fn pipeline_run_with_sync() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    let stats = Pipeline::from_iter(0..3)
        .sink(sink)
        .run_with(SyncDriver::new())
        .unwrap();
    assert_eq!(handle.take(), vec![0, 1, 2]);
    assert_eq!(stats.items_in, 3);
}

#[test]
fn pipeline_run_with_threaded() {
    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    let stats = Pipeline::from_iter(0..3)
        .sink(sink)
        .run_with(ThreadedDriver::new())
        .unwrap();
    assert_eq!(handle.take(), vec![0, 1, 2]);
    assert_eq!(stats.items_in, 3);
}

// A custom driver that records how many times it was invoked, then
// delegates to SyncDriver. Demonstrates external Driver impls.
struct CountingDriver {
    counter: Arc<AtomicU32>,
}

impl Driver for CountingDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: pipe_io::Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        self.counter.fetch_add(1, Ordering::SeqCst);
        SyncDriver::new().run(pipeline)
    }
}

#[test]
fn custom_driver_impl_works() {
    let counter = Arc::new(AtomicU32::new(0));
    let driver = CountingDriver {
        counter: Arc::clone(&counter),
    };

    let sink = VecSink::<i32>::new();
    let handle = sink.handle();
    let stats = Pipeline::from_iter(0..4)
        .map(|n: i32| n + 100)
        .sink(sink)
        .run_with(driver)
        .unwrap();

    assert_eq!(handle.take(), vec![100, 101, 102, 103]);
    assert_eq!(stats.items_in, 4);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn driver_trait_is_object_safe_for_owned_drivers() {
    // The trait is consumed by-value (self), so it can't be used via
    // `&dyn Driver`. But it should accept generic Driver impls without
    // friction. This test asserts the trait object is NOT used; only
    // generic dispatch.
    fn assert_generic<D: Driver>(_d: D) {}
    assert_generic(SyncDriver::new());
    assert_generic(ThreadedDriver::new());
    assert_generic(CountingDriver {
        counter: Arc::new(AtomicU32::new(0)),
    });
}
