# pipe-io Benchmarks

> Numbers are indicative, not contractual. Rerun `cargo bench` on your
> own hardware. Everything below was measured for `2.0.0` on
> 2026-10-13.

## Machine and method

- **Machine:** AMD Ryzen 9 9950X3D (16 cores / 32 threads), 128 GB RAM,
  Windows 11 Pro 10.0.26200, `x86_64-pc-windows-msvc`.
- **Toolchain:** rustc 1.99.0, release profile with `lto = "thin"`,
  criterion 0.7.
- **Harness:** `benches/pipeline.rs`. Each scenario pushes 100,000 items
  from a closure source through the pipeline; criterion reports the
  time per iteration and throughput in elements per second (40 samples
  per scenario). Items pass through `black_box` at the source and at
  the sink, so the optimiser cannot fold the work away (the 1.x
  harness only `black_box`ed a final sum, which is why its iterator
  baseline read "~0 ns").
- **Load:** the machine was running other heavy builds during the
  measurement, so the benchmark process ran at high priority pinned to
  four cores. Absolute numbers on an idle machine are somewhat higher;
  the before/after comparison below is insulated from this by running
  both versions in the same process.

## Throughput (`2.0.0`)

| Scenario | ns / item | M items / s |
|----------|----------:|------------:|
| raw `Iterator` `map` + `filter` (reference) | 0.56 | 1,774 |
| source -> null sink | 1.93 | 519 |
| source -> map -> sink | 2.55 | 392 |
| source -> map -> filter -> map -> sink | 3.75 | 267 |
| source -> filter(false) -> sink | 2.14 | 467 |
| source -> batch(100) -> sink | 6.61 | 151 |
| source -> try_map(ok) -> sink | 2.30 | 434 |
| source -> try_map(50% err, Continue) -> sink | 2.65 | 377 |
| `run_threaded`, no edges: source -> map -> sink | 4.02 | 248 |
| `run_threaded`, map -> `buffer(1024)` -> sink | 23.8 | 42 |
| tumbling window (1 ms) -> sink, system clock | 33.0 | 30 |

Reading the numbers:

- Each stage costs about 0.6-1 ns per item: one indirect call. The
  per-item path keeps its arguments and results in registers (see
  [`ARCHITECTURE.md`](ARCHITECTURE.md#performance-model)).
- `filter(false)` is cheaper than `map` because nothing reaches the
  sink.
- Batching includes allocating one `Vec` per batch.
- Under `Continue`, a failed item costs about the same as a successful
  one when no observer is installed: the error is counted and dropped
  without being boxed.
- `run_threaded` without edges runs everything on one worker thread; the
  difference from `run` is the thread spawn and join, amortised over
  the 100,000 items.
- A `buffer` edge costs one `std::sync::mpsc` hand-off per item (about
  20 ns here, more under load). It pays off when the stages on either
  side do more work than that: see `examples/threaded.rs`, where two
  0.5 ms stages overlap and the run time halves. To move many small
  items across an edge, batch them first so whole batches cross it.
- Windows read the clock once per item. With `SystemClock` on Windows
  that read dominates the cost of a window stage on small items.

## Before and after (`1.0.0` -> `2.0.0`)

`1.0.0` and `2.0.0` were linked into one benchmark binary (`pipe-io`
renamed per version) and every scenario ran alternately for both
versions, five rounds, high priority, pinned to two cores. Medians of
the five rounds (M items / s):

| Scenario | 1.0.0 | 2.0.0 | Change |
|----------|------:|------:|-------:|
| source -> null sink | 500 | 524 | +5% |
| source -> map -> sink | 257 | 383 | +49% |
| source -> map -> filter -> map -> sink | 180 | 274 | +53% |
| source -> filter(false) -> sink | 442 | 475 | +8% |
| source -> batch(100) -> sink | 135 | 149 | +11% |
| source -> try_map(ok) -> sink | 284 | 424 | +49% |
| source -> try_map(50% err, Continue) -> sink | 312 | 340 | +9% |
| `run_threaded`: source -> map -> sink | 144 | 188 | inconclusive (high variance under load) |

No scenario regressed. The gains come from the per-item calling
convention: control operations became unit variants so an item and its
tag travel in two registers, each stage call returns the number of
items the sink accepted (instead of the sink writing a counter in
memory), and the error carried by a failed call is boxed so the result
fits in two registers. Batching also stopped reading the clock when no
`max_age` is set.

An intermediate `2.0` build that added the run context and counters
without these changes measured 10-20% slower than `1.0.0`; that is what
the register work recovered.

## How to reproduce

```text
cargo bench --bench pipeline
```

The bench needs the default `std` feature. For stable numbers on a busy
machine, run the bench executable at high priority and pin it to a few
cores.
