//! Throughput benchmarks (criterion).
//!
//! Every scenario pushes `ITEMS` items from a closure source through the
//! pipeline and reports elements per second. Items are passed through
//! `black_box` at the source and the sink so the optimiser cannot fold the
//! work away. The `raw_iter_map_filter` row is a plain iterator chain for
//! reference.
//!
//! Run with:
//!
//! ```text
//! cargo bench --bench pipeline
//! ```
//!
//! Methodology and recorded results: `docs/BENCH.md`.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pipe_io::sink::{FnSink, NullSink};
use pipe_io::source::FnSource;
use pipe_io::{Batch, BatchPolicy, ErrorPolicy, Pipeline, Window, WindowPolicy};

const ITEMS: u64 = 100_000;

#[derive(Debug)]
struct Fail;
impl core::fmt::Display for Fail {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("fail")
    }
}
impl std::error::Error for Fail {}

fn source(n: u64) -> FnSource<impl FnMut() -> Result<Option<u64>, Fail>, u64, Fail> {
    let mut i = 0_u64;
    FnSource::new(move || {
        if i >= n {
            Ok(None)
        } else {
            i += 1;
            Ok(Some(black_box(i)))
        }
    })
}

fn sink_u64() -> FnSink<impl FnMut(u64) -> Result<(), Fail>, u64, Fail> {
    FnSink::new(|n: u64| {
        black_box(n);
        Ok(())
    })
}

fn throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("pipeline");
    g.throughput(Throughput::Elements(ITEMS));
    g.sample_size(40);

    g.bench_function(BenchmarkId::new("raw_iter_map_filter", ITEMS), |b| {
        b.iter(|| {
            let mut acc = 0_u64;
            for v in (1..=ITEMS)
                .map(|n| black_box(n).wrapping_mul(3))
                .filter(|n| n & 1 == 0)
            {
                acc = acc.wrapping_add(black_box(v));
            }
            acc
        });
    });
    g.bench_function(BenchmarkId::new("null_sink", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .sink(NullSink::<u64>::new())
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("map", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .map(|n: u64| n.wrapping_mul(3))
                .sink(sink_u64())
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("map_filter_map", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .map(|n: u64| n.wrapping_mul(3))
                .filter(|n: &u64| *n & 1 == 0)
                .map(|n: u64| n.wrapping_add(7))
                .sink(sink_u64())
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("filter_false", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .filter(|_n: &u64| false)
                .sink(NullSink::<u64>::new())
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("batch_100", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .batch(BatchPolicy::new().max_items(100))
                .sink(FnSink::new(|batch: Batch<u64>| -> Result<(), Fail> {
                    black_box(batch.len());
                    Ok(())
                }))
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("try_map_ok", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .try_map(|n: u64| -> Result<u64, Fail> { Ok(n.wrapping_mul(3)) })
                .sink(NullSink::<u64>::new())
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("try_map_half_err_continue", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .on_error(ErrorPolicy::Continue)
                .try_map(|n: u64| -> Result<u64, Fail> {
                    if n & 1 == 0 { Ok(n) } else { Err(Fail) }
                })
                .sink(NullSink::<u64>::new())
                .run()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("threaded_map", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .map(|n: u64| n.wrapping_mul(3))
                .sink(sink_u64())
                .run_threaded()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("threaded_buffer_1024", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .map(|n: u64| n.wrapping_mul(3))
                .buffer(1024)
                .sink(sink_u64())
                .run_threaded()
                .map(|s| s.items_in)
        });
    });
    g.bench_function(BenchmarkId::new("window_tumbling_1ms", ITEMS), |b| {
        b.iter(|| {
            Pipeline::from_source(source(ITEMS))
                .window(WindowPolicy::tumbling(Duration::from_millis(1)))
                .sink(FnSink::new(|w: Window<u64>| -> Result<(), Fail> {
                    black_box(w.len());
                    Ok(())
                }))
                .run()
                .map(|s| s.items_in)
        });
    });
    g.finish();
}

criterion_group!(benches, throughput);
criterion_main!(benches);
