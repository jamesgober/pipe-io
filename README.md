<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <strong>pipe-io</strong>
    <br>
    <sup><sub>TYPED DATA PIPELINE PRIMITIVES FOR RUST</sub></sup>
</h1>

<p align="center">
    <a href="https://crates.io/crates/pipe-io"><img alt="crates.io" src="https://img.shields.io/crates/v/pipe-io.svg"></a>
    <a href="https://crates.io/crates/pipe-io"><img alt="downloads" src="https://img.shields.io/crates/d/pipe-io.svg"></a>
    <a href="https://docs.rs/pipe-io"><img alt="docs.rs" src="https://docs.rs/pipe-io/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.75%2B-blue"></a>
    <a href="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml/badge.svg"></a>
</p>

<p align="center">
    Source -> transform -> sink, with backpressure, batching, windowing, and isolated error lanes per stage.
</p>


## What it does

Typed source-transform-sink pipelines with backpressure, batching, windowing, and per-stage error isolation. A lightweight runtime-agnostic stream processor for in-process workloads. The missing middle ground between raw iterators and full distributed stream processing.

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

## Documentation

- [Quick reference (`docs/API.md`)](docs/API.md)
- [User guide (`docs/GUIDE.md`)](docs/GUIDE.md) - patterns and how-tos
- [Project specification (`REPS.md`)](REPS.md) - locked public surface
- [Benchmarks (`docs/BENCH.md`)](docs/BENCH.md) - measured throughput
- [Examples](examples/) - 8 runnable examples covering batching,
  windowing, dead-letter routing, threaded execution, and custom
  drivers/sources

Run any example with `cargo run --example <name>`, for instance:

```text
cargo run --example basic
cargo run --example etl
```

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE)
for the full text.


<!--
:: COPYRIGHT
=============================================== -->
<div align="center">
  <br>
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2025 <strong>JAMES GOBER.</strong></sup>
</div>

