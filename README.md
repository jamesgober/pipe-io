<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <strong>pipe-io</strong>
    <br>
    <sup><sub>TYPED DATA PIPELINE PRIMITIVES FOR RUST</sub></sup>
</h1>

<p align="center">
    <a href="https://crates.io/crates/pipe-io"><img alt="crates.io" src="https://img.shields.io/crates/v/pipe-io.svg"></a>
    <a href="https://crates.io/crates/pipe-io"><img alt="downloads" src="https://img.shields.io/crates/d/pipe-io.svg?color=0099ff"></a>
    <a href="https://docs.rs/pipe-io"><img alt="docs.rs" src="https://docs.rs/pipe-io/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.75%2B-blue"></a>
    <a href="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml/badge.svg"></a>
</p>

<p align="center">
    <strong>Source -&gt; transform -&gt; sink, with backpressure, batching, windowing, and isolated error lanes per stage.</strong>
    <br>
    <em>The missing middle ground between raw iterators and full distributed stream processing.</em>
</p>

<p>
    <b>pipe-io</b> is an in-process stream processor for Rust. It gives you a typed builder for source / transform / sink pipelines, bounded buffers for backpressure, count / byte / age-triggered batching, tumbling / sliding / session windows behind a pluggable <code>Clock</code>, per-stage <code>FailFast</code> / <code>Continue</code> / <code>DeadLetter</code> error policies, and a synchronous and threaded driver - all on <strong>zero runtime dependencies</strong> with <code>#![forbid(unsafe_code)]</code>.
</p>

<p>
    Use it when raw <code>Iterator</code> is too thin (no backpressure, no batching, no per-stage error isolation) and a full distributed processor is too heavy. <strong>Single host, single process, in memory.</strong>
</p>

<p>
    Custom executors are first-class: the <code>Driver</code> trait is open, so a future <code>pipe-io-tokio</code> adapter (or your own rayon-backed driver) plugs in without coordinating with this crate.
</p>

<hr>

## Features

### Pipeline composition

- **Typed builder** - the carrier type is tracked at compile time across every stage transition. Wrong types are a build error, not a runtime panic.
- **Closure adapters** - `map`, `filter`, `filter_map`, `flat_map`, `inspect`, `try_map` all live on the builder. No `.next()` ceremony.
- **Custom stages** - implement `Stage` directly when state spreads across multiple fields or you need 1:N emission.
- **Custom sources and sinks** - the `Source` and `Sink` traits are two methods each (with `flush` / `close` defaults).

### Backpressure, batching, windowing

- **Bounded buffers** - inter-stage edges are bounded; full downstream slows the upstream.
- **Count / byte / age batching** - `BatchPolicy::new().max_items(N).max_bytes(M).max_age(D)`. Triggers OR together.
- **Tumbling / sliding / session windows** - behind a pluggable `Clock` trait. Custom clocks for deterministic tests and embedded time sources.
- **`ByteSize` trait** - opt-in for byte-aware batching; blanket impls for `&str`, `String`, `Vec<u8>`, `&[u8]`.

### Error isolation

- **`ErrorPolicy::FailFast`** (default) - first error in a stage bubbles out of `run`.
- **`ErrorPolicy::Continue`** - drop the failing item, keep going. Useful for noisy enrichment stages.
- **`ErrorPolicy::DeadLetter`** - route the failure to a dead-letter `Sink<Item = StageFailure>` installed via `.dead_letter(sink)`. Sink install order does not matter.
- **`StageId`** - every error carries the `&'static str` name of the stage that produced it.

### Runtime and execution

- **`SyncDriver`** - drives the pipeline on the calling thread; no_std-compatible.
- **`ThreadedDriver`** - spawns a worker thread; the calling thread blocks on `join`.
- **`Driver` trait** - open for external executors. Implement for tokio, rayon, glommio, or custom thread farms.
- **`Pipeline::run`**, **`run_threaded`**, **`run_with(driver)`** - three terminal forms.

### Reliability and safety

- **`#![forbid(unsafe_code)]`** at the crate root.
- **Zero runtime dependencies.** Built on `core` + `alloc` (always) plus `std` (default feature). `proptest` is a dev-only dependency.
- **84 tests** pass under `--all-features` (32 unit + 42 integration + 10 doctest), plus **11 property tests** and **4 fuzz harnesses**.
- **`cargo-semver-checks`** runs in CI from `0.9.0` onward and is a hard gate from `1.0.0`.
- **MSRV 1.75**, locked. Bumps require a minor version increment.

> Benchmark numbers under `--release` on a developer laptop: source -> null sink ~500 M items/s; source -> map -> sink ~260 M items/s; full three-stage chain ~170 M items/s. See [`docs/BENCH.md`](docs/BENCH.md).

---

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
pipe-io = "1"
```

For `no_std` builds (data primitives and `SyncDriver` only):

```toml
[dependencies]
pipe-io = { version = "1", default-features = false }
```

### Feature flags

| Flag    | Default | Effect                                                                                                |
|---------|---------|-------------------------------------------------------------------------------------------------------|
| `std`   | yes     | `ThreadedDriver`, `Pipeline::run_threaded`, `ChannelSource`, `ReaderSource`, `ChannelSink`, `WriterSink`, `VecSink`, `Window` / `Clock` / `SystemClock`, `BatchPolicy::max_age`, dead-letter routing. |

The `no_std` build still ships the full trait surface, closure adapters, count + byte batching, the synchronous driver, and the error model. The `std`-only items above are the ones that need `std::sync::Mutex` or `std::thread::spawn`.

---

## Quick start

```rust
use pipe_io::{Pipeline, sink::VecSink};

let sink = VecSink::<i64>::new();
let handle = sink.handle();

Pipeline::from_iter(1..=5)
    .map(|n: i32| i64::from(n) * 10)
    .filter(|n: &i64| *n > 20)
    .sink(sink)
    .run()
    .expect("pipeline run");

assert_eq!(handle.take(), vec![30, 40, 50]);
```

### Batching

```rust
use pipe_io::{Pipeline, Batch, BatchPolicy, sink::VecSink};

let sink = VecSink::<Vec<u32>>::new();
let handle = sink.handle();

Pipeline::from_iter(1u32..=11)
    .batch(BatchPolicy::new().max_items(4))
    .map(|b: Batch<u32>| b.into_inner())
    .sink(sink)
    .run()
    .unwrap();

assert_eq!(handle.take().len(), 3);
```

### Windowing

```rust
use core::time::Duration;
use pipe_io::{Pipeline, Window, WindowPolicy, sink::VecSink};

let sink = VecSink::<u64>::new();
Pipeline::from_iter(metric_stream)
    .window(WindowPolicy::Tumbling { size: Duration::from_secs(60) })
    .map(|w: Window<u64>| w.into_inner().iter().sum::<u64>())
    .sink(sink)
    .run()?;
```

### Dead-letter routing

```rust
use pipe_io::{ErrorPolicy, Pipeline, StageFailure, sink::VecSink};

let dlq = VecSink::<StageFailure>::new();
let dlq_handle = dlq.handle();

Pipeline::from_iter(records)
    .stage_id("parse")
    .on_error(ErrorPolicy::DeadLetter)
    .try_map(parse_row)
    .dead_letter(dlq)
    .sink(main_sink)
    .run()?;
```

### Threaded driver

```rust
Pipeline::from_iter(records)
    .map(transform)
    .sink(writer)
    .run_threaded()?;
```

### Custom driver

```rust
use pipe_io::driver::{Driver, RunStats, SyncDriver};

struct MyDriver;

impl Driver for MyDriver {
    fn run<S>(self, pipeline: pipe_io::Pipeline<S>) -> pipe_io::Result<RunStats>
    where
        S: pipe_io::Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        SyncDriver::new().run(pipeline)
    }
}
```

More patterns are in [`docs/GUIDE.md`](docs/GUIDE.md) and the [`examples/`](examples/) directory.

---

## Examples

Every example lives under [`examples/`](examples/) and runs with `cargo run --example <name>`:

| Example         | Demonstrates                                                  |
|-----------------|---------------------------------------------------------------|
| `basic`         | Minimal map / filter pipeline collecting into `VecSink`.      |
| `batching`      | `BatchPolicy::new().max_items(N)` count-triggered batching.   |
| `windowing`     | Tumbling window with a deterministic scripted clock.          |
| `dead_letter`   | `ErrorPolicy::DeadLetter` + `.dead_letter(sink)` routing.     |
| `threaded`      | `Pipeline::run_threaded()` on a spawned worker thread.        |
| `custom_driver` | Implementing the `Driver` trait with timing instrumentation.  |
| `custom_source` | Implementing `Source` for a stateful Fibonacci producer.      |
| `etl`           | Multi-stage CSV ETL with `Continue`, enrichment, batching.    |

---

## Status

**`1.0.0` - Stable.** The public API is frozen. Backwards-incompatible changes after `1.0.0` require a major version bump per Semantic Versioning. See [`REPS.md`](REPS.md) section 8 for the binding policy and [`docs/API.md`](docs/API.md) for the complete public-symbol reference.

**Stability rules (from `1.0.0` forward):**

- **Patch (`1.0.x`)** - bug fixes, doc improvements, internal performance work, test additions. No new public items.
- **Minor (`1.x.0`)** - pure additions to the public surface, new opt-in features, new variants on enums reserved for growth, MSRV bumps.
- **Major (`2.0.0`)** - removes, renames, or signature changes of public symbols, or non-opt-in runtime dependency additions.

The `cargo-semver-checks` CI job enforces this for the public API surface from `1.0.0` onward.

---

## Documentation

- [User guide (`docs/GUIDE.md`)](docs/GUIDE.md) - 11-section walkthrough of every public surface.
- [API reference (`docs/API.md`)](docs/API.md) - offline mirror of the rustdoc on docs.rs.
- [Project specification (`REPS.md`)](REPS.md) - the binding public surface and stability policy.
- [Benchmarks (`docs/BENCH.md`)](docs/BENCH.md) - methodology and measured numbers.
- [Migration guide (`docs/MIGRATION.md`)](docs/MIGRATION.md) - per-version upgrade notes.
- [Release notes (`docs/release/`)](docs/release/) - one note per tagged release.
- [Examples (`examples/`)](examples/) - 8 runnable example files.

External:

- **docs.rs** - <https://docs.rs/pipe-io>
- **crates.io** - <https://crates.io/crates/pipe-io>

---

## Version compatibility

| Crate version | MSRV    | Status                |
|---------------|---------|-----------------------|
| `1.0.x`       | `1.75`  | Stable (this release) |
| `0.9.x`       | `1.75`  | Pre-1.0 stabilization |
| `0.x` earlier | `1.75`  | Feature releases      |

MSRV is locked at `1.75`. A bump requires a minor version increment and a CHANGELOG entry under `### Changed` with rationale, per `REPS.md` section 6.

---

## Contributing

The public surface is locked at `1.0.0`. Issues, bug reports, and pull requests for bug fixes, documentation improvements, and internal performance work are welcome. Additions to the public API need a minor version bump and a documented motivation in `.dev/` planning material.

See [`.dev/DIRECTIVES.md`](.dev/DIRECTIVES.md) (not published) for the project's development directives: code style, build matrix, banned-word policy, and commit conventions.

---

<div id="license">

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for the full text.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you shall be licensed as above, without any additional terms or conditions.

</div>

<!--
:: COPYRIGHT
=============================================== -->
<div align="center">
  <br>
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2026 <strong>JAMES GOBER.</strong></sup>
</div>

---
