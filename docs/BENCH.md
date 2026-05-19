# pipe-io Benchmarks

> Numbers below are indicative, not contractual. Rerun with
> `cargo bench` on your target hardware. Hardware: developer laptop
> (Windows 11, x86_64-pc-windows-msvc, release build with
> `lto = "thin"`).

## Methodology

`benches/pipeline.rs` is hand-rolled (no Criterion) to keep the crate
dependency-free. Each scenario builds a pipeline, runs it through
200,000 source items, and reports per-item nanoseconds and millions
of items per second. Every scenario runs once as warm-up, then once
as the timed pass. All sinks discard their input (either a `NullSink`
or a `FnSink` wrapping `black_box`), so the numbers measure pipeline
overhead, not sink work.

The raw-iterator baseline measures the same map/filter chain expressed
as a stdlib `Iterator`. Comparing the two gives the cost of pipe-io's
type-erased `Box<dyn FnMut(StageOp<T>) -> Result<()>>` chain over a
fully monomorphized iterator.

## Results

200,000 items per run. Hardware: developer laptop. Numbers from
`cargo bench --bench pipeline` on `0.4.0`.

| Scenario                                  | ns / item | M items / s |
|-------------------------------------------|----------:|------------:|
| raw `Iterator::map().filter()` baseline   |     ~0    |   ~2,200    |
| source -> null sink                       |      1    |     ~500    |
| source -> map -> sink                     |      3    |     ~260    |
| source -> map -> filter -> map -> sink    |      5    |     ~170    |
| source -> filter(false) -> sink           |      2    |     ~340    |
| source -> batch(100) -> sink              |      7    |     ~140    |
| source -> try_map(ok) -> sink             |      3    |     ~290    |
| source -> try_map(50% err, Continue)      |      3    |     ~325    |
| threaded driver: source -> map -> sink    |     46    |      ~21    |

Reading the numbers:

* The raw-iterator baseline is essentially zero-cost because the
  compiler can fully monomorphize and frequently vectorize a stdlib
  iterator chain over `1..=N`. Pipe-io's flat 500 M / s ceiling on
  the source-only path is the cost of one vtable dispatch per item
  into the boxed stage chain. Each additional stage adds roughly
  1-2 ns per item, which is one vtable call per stage.
* Filter overhead is paid only when the item is passed through:
  `filter(false)` is faster than `map` because the closure returns
  before any downstream emit.
* Batching with `max_items(100)` runs at ~140 M / s. The per-item
  cost includes pushing onto the batch's `Vec`; the cost of emitting
  the batch is amortized across 100 items.
* `try_map` in the happy path matches plain `map`. With 50% errors
  routed through `ErrorPolicy::Continue`, throughput is unchanged:
  the swallowed error path is also a single vtable hop.
* The threaded driver pays for thread spawn + join plus the cost of
  moving the pipeline closure into the worker. For 200,000 items
  these fixed costs amortize to roughly 40 ns / item. For longer
  runs the per-item cost approaches the sync driver. The threaded
  driver pays off when work is mixed with long blocking I/O on the
  source side; it is not a speedup for trivially-fast stages.

## Architectural cost

Pipe-io's builder collapses every stage into a single boxed closure
of type
`Box<dyn FnMut(StageOp<T>) -> Result<()> + Send>`. Each stage in the
chain calls into the next via this boxed handle, so each item pays
one vtable dispatch per stage edge. That is the gap between the raw
iterator baseline and pipe-io's per-stage cost.

The alternative (full type-state monomorphization, where the entire
stage chain is one nested generic struct) eliminates the vtable cost
but produces large, hard-to-name types in the builder and slower
compile times. For the in-process pipeline workloads this crate
targets, single-digit-nanosecond per-stage overhead is acceptable
and the type ergonomics matter more.

If a future release adds an `Iterator`-style flat trait surface for
small pipelines (no batching, no per-stage error policy, fixed
chains), that path will sit alongside the current builder and offer
the lower overhead. It is not in scope for `0.4.0`.

## How to reproduce

```text
cargo bench --bench pipeline
```

The bench requires the default `std` feature. It is a single binary
that prints results to stdout; numbers vary by hardware and by the
state of the cache after compile. Rerun a few times if the first
run looks anomalous.
