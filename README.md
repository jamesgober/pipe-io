<h1 align="center">
    <strong>pipe-io</strong>
    <br>
    <sup><sub>TYPED DATA PIPELINE PRIMITIVES FOR RUST</sub></sup>
</h1>

<p align="center">
    <a href="https://crates.io/crates/pipe-io"><img alt="crates.io" src="https://img.shields.io/crates/v/pipe-io.svg"></a>
    <a href="https://crates.io/crates/pipe-io"><img alt="downloads" src="https://img.shields.io/crates/d/pipe-io.svg"></a>
    <a href="https://docs.rs/pipe-io"><img alt="docs.rs" src="https://docs.rs/pipe-io/badge.svg"></a>
    <a href="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jamesgober/pipe-io/actions/workflows/ci.yml/badge.svg"></a>
</p>

<p align="center">
    Source -> transform -> sink, with backpressure, batching, windowing, and isolated error lanes per stage.
</p>

---

## Status

Status: CONSIDERING - placeholder repo to claim the crates.io name. Active design begins when a concrete consumer (HiveDB stream API, log aggregation, ETL) is on the critical path.

This repository is published primarily to reserve the crate name and
to establish the project scaffolding. Implementation work proceeds on
the schedule documented in `.dev/ROADMAP.md`.

## What it does

Typed source-transform-sink pipelines with backpressure, batching, windowing, and per-stage error isolation. A lightweight runtime-agnostic stream processor for in-process workloads. The missing middle ground between raw iterators and full distributed stream processing.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE)
for the full text.

Copyright (C) 2026 James Gober.
