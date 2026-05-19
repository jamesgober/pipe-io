# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-05-19

Polish and benchmarking release. No public API changes from `0.3.0`;
this version adds a benchmark harness, measured numbers, and a
performance section in the docs.

### Added

- `benches/pipeline.rs` - hand-rolled (no Criterion) throughput
  benches covering source-only, single map, three-stage chain,
  filter-drop, batch(100), `try_map` happy path, `try_map` with 50%
  errors under `ErrorPolicy::Continue`, and the threaded driver.
  Runs via `cargo bench --bench pipeline`.
- `docs/BENCH.md` documents methodology, hardware, measured numbers,
  the per-stage architectural cost (one vtable dispatch per stage
  edge through the boxed chain), and how to reproduce.
- Performance summary added to `docs/API.md`, cross-linked to
  `BENCH.md`.
- `[[bench]] name = "pipeline"` entry in `Cargo.toml` with
  `harness = false` and `required-features = ["std"]`.

### Notes

- Headline numbers on a developer laptop (Windows, x86_64, release
  with `lto = "thin"`, 200,000 items per run): source-only at
  ~500 M items / s, single map at ~260 M items / s, three-stage
  chain at ~170 M items / s, batch(100) at ~140 M items / s,
  threaded driver at ~21 M items / s.
- No optimization pass landed in this release; measured per-stage
  cost (~1-2 ns per item per stage) matches the architectural
  model (one boxed-dyn vtable hop per stage edge). Closing the gap
  to the raw-iterator baseline would require full type-state
  monomorphization, which is deferred past `1.0.0`.

## [0.3.0] - 2026-05-19

First substantive release. Lands the design lock from the prior
documentation push plus a minimum viable implementation of the locked
public surface.

### Added

#### Design lock

- `REPS.md` section 4 locks the public API surface. Modules: crate
  root, `source`, `stage`, `sink`, `batch`, `window` (std, deferred
  past `0.3.0`), `error`, `driver`. Builder methods, feature flags,
  MSRV, and runtime dependency posture are now binding.
- `REPS.md` sections 3 (Scope), 10 (Testing requirements), and 12
  (Out of scope) filled in.
- `.dev/DESIGN.md` documents the major design trade-offs:
  synchronous core, hybrid pull/push via `Emit`, bounded buffers
  for backpressure, per-stage error policy, `Clock` trait for
  windowing, dual-driver model (`SyncDriver` and `ThreadedDriver`),
  `&'static str` stage identity, and the typed builder. Includes
  boundaries with `Iterator`, `futures::Stream`, `crossbeam`,
  `rayon`, distributed processors, and the sibling `log-io` crate,
  plus four consumer use cases.

#### Implementation

- Core traits: `Source`, `Stage`, `Sink`, `Emit`.
- Error model: `Error`, `Result`, `StageError` (blanket impl over
  `Debug + Display + Send + Sync + 'static`), `BoxError`, `StageId`,
  `StageFailure`, `BufferErrorKind`, `ErrorPolicy`.
- Source adapters: `IterSource`, `FnSource`, plus `ChannelSource`
  and `ReaderSource` under `std`.
- Sink adapters: `NullSink`, `FnSink`, plus `VecSink` (with
  cloneable `SharedHandle`), `ChannelSink`, and `WriterSink` under
  `std`.
- Builder closure adapters: `map`, `filter`, `filter_map`,
  `flat_map`, `inspect`, `try_map`, and the generic `stage` method
  for plugging in any custom `Stage` implementation.
- Batching: `Batch<T>`, `BatchPolicy` (count, byte, and age
  triggers; age requires `std`), `ByteSize` trait with blanket
  impls for `&str`, `String`, `Vec<u8>`, `&[u8]`.
- Builder methods: `stage_id`, `on_error`, `batch`, `batch_bytes`,
  `sink`.
- `Pipeline::from_source`, `Pipeline::from_iter`, `Pipeline::run`,
  `Pipeline::run_threaded`.
- Drivers: `SyncDriver` (no_std-compatible), `ThreadedDriver`
  (std-only).
- `RunStats` returned by every run, with `items_in` always present
  and `duration` under `std`.
- Crate version constant `pipe_io::VERSION`.
- `docs/API.md` rewritten as the offline mirror of the public surface.
- 50 tests pass under `--all-features`: 27 unit tests, 15
  integration tests covering map / filter / filter_map / flat_map /
  inspect / batching / byte-batching / error policies / driver
  variants / a log-shipper shape / an ETL continue-on-failure
  shape, and 8 doctests.

### Changed

- `REPS.md` section 8 narrows the stability statement to point at
  the `1.0.0` policy and the `cargo-semver-checks` CI gate that
  arrives at `0.9.0`.
- `REPS.md` section 4.7 records that `StageError` uses a
  `Debug + Display + Send + Sync + 'static` bound rather than
  `core::error::Error` (which stabilized in Rust 1.81, post-MSRV).
- `REPS.md` section 4.8: `Driver` trait deferred past `0.3.x`
  pending reconciliation of `Send` bounds between `SyncDriver` and
  `ThreadedDriver`. Pipelines pick a driver via
  `Pipeline::run` / `Pipeline::run_threaded`.
- `REPS.md` section 4.6: `window` module deferred past `0.3.0`;
  the locked surface is preserved for the version that ships it.
- `VecSink` is `std`-only because its `SharedHandle` requires
  `Arc<Mutex<_>>`; no_std variant deferred.
- `WriterSink<W>` pins `Item = String`; consumers convert from
  any `Display` via an upstream `.map(|x| x.to_string())`.

## [0.1.0] - 2026-05-12

### Added

- Initial repository scaffold.
- Apache-2.0 license, README, REPS specification stub, CI workflow,
  `.dev/` planning structure (DIRECTIVES, ROADMAP, PROMPTS).
- Crate name reserved on crates.io.

[Unreleased]: https://github.com/jamesgober/pipe-io/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/jamesgober/pipe-io/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/pipe-io/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/jamesgober/pipe-io/releases/tag/v0.1.0
