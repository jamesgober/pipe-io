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
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue"></a>
    <a href="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml/badge.svg"></a>
</p>

<p align="center">
    <strong>Source -&gt; transform -&gt; sink, with bounded stage edges, batching, windowing, and per-stage error isolation.</strong>
    <br>
    <em>The middle ground between raw iterators and distributed stream processing.</em>
</p>

<p>
    <b>pipe-io</b> is an in-process stream processor for Rust. A typed builder composes sources, transforms, and sinks; batching groups items by count, bytes, or age; windows group them by time (tumbling, sliding, session, per key); every fallible stage chooses to fail fast, skip, or dead-letter; and the same pipeline runs on the calling thread, across threads joined by bounded channels, or step by step under any executor. It has <strong>zero runtime dependencies</strong>, <code>#![forbid(unsafe_code)]</code>, and a <code>no_std</code> + <code>alloc</code> core.
</p>

<p>
    Use it when a raw <code>Iterator</code> is too thin (no batching, no time windows, no per-stage error isolation, no pipelined parallelism) and a distributed stream processor is far too heavy. <strong>Single host, single process, in memory.</strong>
</p>

<hr>

## Features

### Pipeline composition

- **Typed builder**: the item type is tracked across every step; a mismatch is a compile error.
- **Closure adapters**: `map`, `filter`, `filter_map`, `flat_map`, `inspect`, `try_map`, `try_map_ref`, `try_map_retry`.
- **Custom stages, sources, sinks**: small traits with sensible defaults; `TeeSink` fans out, `ChainSource` concatenates.

### Bounded memory and backpressure

- **Bounded edges**: `.buffer(capacity)` splits the pipeline; under `run_threaded` each segment runs on its own thread, connected by a bounded channel. A full edge blocks the thread feeding it, so a slow sink slows the source.
- **Count / byte / age batching**: `BatchPolicy::new().max_items(n).max_bytes(m).max_age(d)`; triggers OR together.
- **Windows**: tumbling, sliding, session, and per-key, behind a pluggable `Clock`. Per-window item caps and per-stage key caps bound memory; idle gaps cost O(1).
- **Idle tick**: age triggers and windows fire on quiet live streams, not only when the next item arrives.

### Error isolation and correctness

- **Per-stage policies**: `FailFast`, `Continue` (counted), or `DeadLetter` (the failing item travels with the error).
- **Retry with backoff**: fixed or exponential, with optional jitter.
- **Guaranteed teardown**: the sink, the dead-letter sink, and the source are flushed and closed on every exit path: success, error, cancellation, and panic.
- **Validated configuration**: bad settings are typed `ConfigError`s, never panics or hangs.
- **Real error chains**: `std::error::Error::source` and `downcast_ref` reach the original error.

### Execution

- **`run`**: on the calling thread. **`run_threaded`**: one thread per segment. **`run_with(driver)`**: any `Driver`.
- **`step` / `tick` / `finish`**: drive a pipeline from tokio, rayon, or your own scheduler (examples included).
- **Cancellation handle**, **observer events**, and **run counters** (`items_in`, `items_out`, `errors`, `dead_lettered`, `retries`).

### Reliability and safety

- `#![forbid(unsafe_code)]`; zero runtime dependencies.
- Untrusted input: `ReaderSource` caps line length and handles invalid UTF-8 by policy.
- 300+ tests (unit, integration, property, doctest, the audit's reproductions), six fuzz targets, mutation-tested, `cargo-semver-checks` in CI.
- MSRV 1.85, edition 2024.

> Measured on an AMD Ryzen 9 9950X3D (Windows 11, rustc 1.99.0): source -> sink about 0.5 G items/s, each extra stage about 1-2 ns per item. Full numbers and method in [`docs/BENCH.md`](docs/BENCH.md).

---

## Installation

```toml
[dependencies]
pipe-io = "2"
```

`no_std` (needs `alloc`):

```toml
[dependencies]
pipe-io = { version = "2", default-features = false }
```

### Feature flags

| Flag  | Default | Enables |
|-------|---------|---------|
| `std` | yes     | Threads (`run_threaded`, `buffer`, `ThreadedDriver`), time (`Clock`, windows, `max_age`, retry), and the std adapters (`ChannelSource`, `ReaderSource`, `ByteLinesSource`, `ChannelSink`, `WriterSink`, `VecSink`). |

Without `std` you keep the traits, the closure adapters, count and byte batching, error policies including dead-letter routing, the observer, stepping, and `SyncDriver`.

---

## Quick start

```rust
use pipe_io::{Pipeline, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();

    let stats = Pipeline::from_iter(1..=5)
        .map(|n: i32| i64::from(n) * 10)
        .filter(|n: &i64| *n > 20)
        .sink(sink)
        .run()?;

    assert_eq!(out.take(), vec![30, 40, 50]);
    assert_eq!(stats.items_out, 3);
    Ok(())
}
```

### Batching

```rust
use pipe_io::{Batch, BatchPolicy, Pipeline, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter(1_u32..=11)
        .batch(BatchPolicy::new().max_items(4))
        .map(|b: Batch<u32>| b.into_inner())
        .sink(sink)
        .run()?;
    assert_eq!(out.take().len(), 3);
    Ok(())
}
```

### Windowing

```rust
use std::time::Duration;
use pipe_io::{Pipeline, Window, WindowPolicy, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter([("cpu", 3_u64), ("mem", 5), ("cpu", 4)])
        .window_by_key(
            |m: &(&str, u64)| m.0,
            WindowPolicy::tumbling(Duration::from_secs(60)).max_keys(10_000),
        )
        .map(|(key, w): (&str, Window<(&str, u64)>)| (key, w.iter().map(|m| m.1).sum::<u64>()))
        .sink(sink)
        .run()?;
    assert_eq!(out.take(), vec![("cpu", 7), ("mem", 5)]);
    Ok(())
}
```

### Dead-letter routing

```rust
use pipe_io::{ErrorPolicy, Pipeline, StageFailure, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let dlq = VecSink::<StageFailure>::new();
    let failures = dlq.handle();
    let ok = VecSink::new();
    let parsed = ok.handle();

    Pipeline::from_iter(["1", "two", "3"])
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("parse")
        .try_map_ref(|s: &&str| s.parse::<u32>())
        .dead_letter(dlq)
        .sink(ok)
        .run()?;

    assert_eq!(parsed.take(), vec![1, 3]);
    assert_eq!(failures.take()[0].take_input::<&str>(), Some("two"));
    Ok(())
}
```

### Threads and bounded edges

```rust
use pipe_io::{Pipeline, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter(0_u32..10_000)
        .map(|n: u32| n.wrapping_mul(31)) // thread 1, with the source
        .buffer(256)                      // at most 256 items in flight
        .filter(|n: &u32| n % 2 == 0)     // thread 2
        .buffer(256)
        .sink(sink)                       // thread 3
        .run_threaded()?;
    assert_eq!(out.take().len(), 5_000);
    Ok(())
}
```

### Driving a pipeline yourself

```rust
use pipe_io::{Error, Pipeline, Step, sink::NullSink};

fn main() -> Result<(), pipe_io::Error> {
    let mut pipeline = Pipeline::from_iter(0..100).sink(NullSink::<i32>::new());
    let cancel = pipeline.cancel_handle();
    loop {
        match pipeline.step() {
            Ok(Step::Done) => break,
            Ok(_) if pipeline.stats().items_in == 50 => cancel.cancel(),
            Ok(_) => {}
            // The step after `cancel` drains, closes, and reports this.
            Err(Error::Cancelled) => break,
            Err(e) => return Err(e),
        }
    }
    assert_eq!(pipeline.stats().items_out, 50);
    Ok(())
}
```

More patterns are in the [user guide](docs/GUIDE.md) and the [`examples/`](examples/) directory.

---

## Examples

Run with `cargo run --example <name>`:

| Example         | Demonstrates                                                        |
|-----------------|---------------------------------------------------------------------|
| `basic`         | Minimal map / filter pipeline collecting into `VecSink`.            |
| `batching`      | Count-triggered batching.                                           |
| `windowing`     | Per-key tumbling windows driven by a `ManualClock`.                 |
| `dead_letter`   | `DeadLetter` routing with the failing input attached.               |
| `threaded`      | `buffer` edges overlapping two slow stages on separate threads.     |
| `live_ingest`   | Channel ingest with age-triggered batches, idle ticks, and cancellation. |
| `custom_driver` | A `Driver` built on `step` with progress reports and a deadline.    |
| `tokio_driver`  | Running and cancelling pipelines from a tokio runtime.              |
| `rayon_driver`  | Running one or many pipelines on a rayon pool.                      |
| `custom_source` | Implementing `Source` for a stateful producer.                      |
| `etl`           | Multi-stage CSV load with `Continue`, enrichment, and batching.     |

---

## Status

**`2.0.0` - Stable.** `2.0` reworks the execution core: guaranteed teardown, validated configuration, real bounded edges, and a steppable pipeline. Upgrading from `1.x` needs small source changes; see [`docs/MIGRATION.md`](docs/MIGRATION.md).

The public API follows Semantic Versioning, enforced by `cargo-semver-checks` in CI:

- **Patch (`2.0.x`)**: bug fixes, documentation, internal performance work.
- **Minor (`2.x.0`)**: additions, new opt-in features, new variants of `#[non_exhaustive]` enums.
- **Major (`3.0.0`)**: removals, renames, signature or behaviour changes, MSRV bumps.

See [`REPS.md`](REPS.md) for the binding API contract.

---

## Documentation

- [User guide](docs/GUIDE.md): every feature with a runnable snippet.
- [API reference](docs/API.md): offline mirror of the public surface.
- [Architecture](docs/ARCHITECTURE.md): execution model, lifecycle, threading, memory bounds.
- [Benchmarks](docs/BENCH.md): method and measured numbers.
- [Platform notes](docs/PLATFORM-NOTES.md): OS and target specifics.
- [Migration guide](docs/MIGRATION.md): upgrade notes per version.
- [Release notes](docs/release/): one note per release.
- [Project specification](REPS.md): the binding contract.

External: [docs.rs](https://docs.rs/pipe-io) and [crates.io](https://crates.io/crates/pipe-io).

---

## Version compatibility

| Crate version | MSRV   | Edition | Status          |
|---------------|--------|---------|-----------------|
| `2.0.x`       | `1.85` | 2024    | Current         |
| `1.0.x`       | `1.75` | 2021    | Superseded      |
| `0.x`         | `1.75` | 2021    | Pre-release     |

The MSRV is verified in CI. Raising it is a major version change.

---

## Contributing

Issues and pull requests are welcome. Every change must pass the CI gate (fmt, clippy with `-D warnings`, tests on Linux, macOS, and Windows with every feature combination, MSRV, `no_std` builds, docs, `cargo audit`, `cargo deny`, semver checks, and the fuzz smoke run). Additions to the public API need a documented motivation and a CHANGELOG entry.

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
