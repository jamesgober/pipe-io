//! End-to-end pipe-io throughput bench.
//!
//! Hand-rolled (no Criterion) to keep the crate dependency-free. Each
//! row reports the steady-state cost per source item after a warm-up.
//!
//! Run with:
//!
//! ```text
//! cargo bench --bench pipeline
//! ```

use std::hint::black_box;
use std::time::Instant;

use pipe_io::sink::{FnSink, NullSink};
use pipe_io::source::FnSource;
use pipe_io::{BatchPolicy, ErrorPolicy, Pipeline};

const ITEMS: u64 = 200_000;

#[derive(Debug)]
struct ParseFail;
impl core::fmt::Display for ParseFail {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("parse fail")
    }
}

fn make_source(
    n: u64,
) -> FnSource<impl FnMut() -> Result<Option<u64>, &'static str>, u64, &'static str> {
    let mut i = 0u64;
    FnSource::new(move || {
        if i >= n {
            Ok(None)
        } else {
            i += 1;
            Ok(Some(i))
        }
    })
}

// ---------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------

fn bench_raw_iter_baseline() {
    let run = || {
        let mut count = 0u64;
        let it = (1u64..=ITEMS)
            .map(|n| n.wrapping_mul(3))
            .filter(|n| n & 1 == 0);
        for v in it {
            count = count.wrapping_add(v);
        }
        black_box(count);
        ITEMS
    };
    let _ = run(); // warmup
    let start = Instant::now();
    let _ = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(ITEMS);
    let mps = (ITEMS as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s (raw Iterator baseline, {} items)",
        "raw iter map+filter", ITEMS
    );
}

fn bench_source_only_null_sink() {
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .sink(NullSink::<u64>::new())
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let total_processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(total_processed);
    let mps = (total_processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> null sink"
    );
}

fn bench_one_map() {
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .map(|n: u64| n.wrapping_mul(3))
            .sink(FnSink::new(|n: u64| -> Result<(), ParseFail> {
                black_box(n);
                Ok(())
            }))
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> map -> sink"
    );
}

fn bench_chain_three_stages() {
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .map(|n: u64| n.wrapping_mul(3))
            .filter(|n: &u64| *n & 1 == 0)
            .map(|n: u64| n.wrapping_add(7))
            .sink(FnSink::new(|n: u64| -> Result<(), ParseFail> {
                black_box(n);
                Ok(())
            }))
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> map -> filter -> map -> sink"
    );
}

fn bench_filter_drop() {
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .filter(|_n: &u64| false)
            .sink(NullSink::<u64>::new())
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> filter(false) -> sink"
    );
}

fn bench_batch_100() {
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .batch(BatchPolicy::new().max_items(100))
            .sink(FnSink::new(
                |batch: pipe_io::Batch<u64>| -> Result<(), ParseFail> {
                    black_box(batch.len());
                    Ok(())
                },
            ))
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> batch(100) -> sink"
    );
}

fn bench_try_map_fail_fast_ok() {
    // No errors raised; measures the try_map overhead in the happy path.
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .try_map(|n: u64| -> Result<u64, ParseFail> { Ok(n.wrapping_mul(3)) })
            .sink(NullSink::<u64>::new())
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> try_map(ok) -> sink"
    );
}

fn bench_try_map_continue_half_error() {
    // Half the items error; with Continue policy, failing items are dropped.
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .on_error(ErrorPolicy::Continue)
            .try_map(|n: u64| -> Result<u64, ParseFail> {
                if n & 1 == 0 {
                    Ok(n.wrapping_mul(3))
                } else {
                    Err(ParseFail)
                }
            })
            .sink(NullSink::<u64>::new())
            .run()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "source -> try_map(50% err, Continue)"
    );
}

fn bench_threaded_driver() {
    let run = || {
        let stats = Pipeline::from_source(make_source(ITEMS))
            .map(|n: u64| n.wrapping_mul(3))
            .sink(FnSink::new(|n: u64| -> Result<(), ParseFail> {
                black_box(n);
                Ok(())
            }))
            .run_threaded()
            .expect("run");
        stats.items_in
    };
    let warm = run();
    black_box(warm);
    let start = Instant::now();
    let processed = run();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() / u128::from(processed);
    let mps = (processed as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "{label:>36}: {ns:>5} ns/item, {mps:>8.2} M items/s",
        label = "threaded: source -> map -> sink"
    );
}

fn main() {
    println!("=== pipe-io throughput benches ({ITEMS} items per run) ===");
    bench_raw_iter_baseline();
    bench_source_only_null_sink();
    bench_one_map();
    bench_chain_three_stages();
    bench_filter_drop();
    bench_batch_100();
    bench_try_map_fail_fast_ok();
    bench_try_map_continue_half_error();
    bench_threaded_driver();
}
