//! Smoke test: the crate builds and a minimal pipeline runs with or
//! without the `std` feature.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use core::sync::atomic::{AtomicU64, Ordering};

use pipe_io::sink::FnSink;
use pipe_io::{BatchPolicy, ErrorPolicy, Pipeline, StageFailure, Step};

#[test]
fn version_is_set() {
    assert!(!pipe_io::VERSION.is_empty());
}

#[test]
fn minimal_pipeline_runs_without_std_features() {
    static TOTAL: AtomicU64 = AtomicU64::new(0);
    let stats = Pipeline::from_iter(1_u64..=10)
        .map(|n: u64| n * 2)
        .filter(|n: &u64| n % 3 != 0)
        .batch(BatchPolicy::new().max_items(4))
        .sink(FnSink::new(
            |b: pipe_io::Batch<u64>| -> Result<(), &'static str> {
                let _prev = TOTAL.fetch_add(b.iter().sum::<u64>(), Ordering::Relaxed);
                Ok(())
            },
        ))
        .run()
        .unwrap();
    assert_eq!(stats.items_in, 10);
    assert_eq!(stats.items_out, 2);
    assert_eq!(TOTAL.load(Ordering::Relaxed), 2 + 4 + 8 + 10 + 14 + 16 + 20);
}

#[test]
fn dead_letter_and_stepping_work_without_std_features() {
    static FAILED: AtomicU64 = AtomicU64::new(0);
    let mut p = Pipeline::from_iter(["1", "x"])
        .on_error(ErrorPolicy::DeadLetter)
        .try_map(|s: &str| s.parse::<u8>())
        .dead_letter(FnSink::new(
            |_f: StageFailure| -> Result<(), &'static str> {
                let _prev = FAILED.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        ))
        .sink(FnSink::new(|_n: u8| -> Result<(), &'static str> { Ok(()) }));
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(p.step().unwrap(), Step::Item);
    assert_eq!(p.step().unwrap(), Step::Done);
    assert_eq!(p.stats().dead_lettered, 1);
    assert_eq!(FAILED.load(Ordering::Relaxed), 1);
}
