# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] - 2026-05-19

Pre-1.0 stabilization release. No public API changes. Adds the
hardening infrastructure that gates the path to `1.0.0`: property
tests, fuzz harnesses, a `cargo-semver-checks` CI job (advisory
until `1.0.0`), a documentation audit, and a per-version migration
guide.

### Added

- `tests/property.rs` - 11 property tests using `proptest` (new
  dev-dependency, std-only). Covers length-preservation for `map`,
  predicate semantics for `filter`, order preservation for trivial
  pipelines, batching losslessness, batch size caps, tumbling-
  window losslessness, monotonic window time bounds,
  `Continue`-policy never-fails, `DeadLetter` partitioning, and
  agreement between `run` / `run_with(SyncDriver)` /
  `run_threaded`.
- `fuzz/` workspace (excluded from the main workspace) with four
  `cargo-fuzz` targets:
  - `batching_count` - count-triggered batching is lossless.
  - `batching_bytes` - byte-triggered batching is lossless.
  - `try_map_continue` - `Continue` never produces run-level
    errors.
  - `window_tumbling` - tumbling windows are lossless under a
    fake clock.
  Requires nightly + `cargo install cargo-fuzz`.
- CI gains a `semver-checks` job that runs
  `cargo-semver-checks check-release`. Advisory (`|| true`) until
  a `1.0.0` baseline lands on crates.io; flips to a hard gate at
  release time.
- `docs/MIGRATION.md` - per-version upgrade notes covering
  `v0.1.0 -> v0.3.0` through `v0.8.0 -> v0.9.0`, plus the
  `1.0.0` stability commitment.
- `proptest = "1"` added as a dev-dependency. Dev-only; the
  published crate ships zero runtime dependencies.

### Changed

- `Cargo.toml` adds `workspace.exclude = ["fuzz"]` so the fuzz
  crate stays out of the main build.
- `docs/README.md` indexes `MIGRATION.md`.

### Notes

- No public API changes. Existing source compiles identically.
- Documentation audit pass: `cargo doc --all-features --no-deps`
  with `RUSTDOCFLAGS=-D warnings -D rustdoc::broken-intra-doc-links`
  passes clean. Every public item has rustdoc; `# Errors` /
  `# Panics` sections present where applicable.

## [0.8.0] - 2026-05-19

Examples and guide release. Pure documentation and example
expansion. No public API changes from `0.7.0`.

### Added

- 8 runnable examples under `examples/`:
  - `basic` - smallest map/filter pipeline.
  - `batching` - count-triggered batching with `BatchPolicy`.
  - `windowing` - tumbling rollup with a deterministic clock.
  - `dead_letter` - `try_map` + `.dead_letter(sink)` for failure
    routing.
  - `threaded` - `ThreadedDriver` via `Pipeline::run_threaded`.
  - `custom_driver` - implementing the `Driver` trait, wrapping
    `SyncDriver` with timing instrumentation.
  - `custom_source` - implementing `Source` for a stateful
    Fibonacci producer.
  - `etl` - multi-stage ETL with `ErrorPolicy::Continue`,
    enrichment lookup, batching, and a counting sink.
- `[[example]]` entries in `Cargo.toml` with
  `required-features = ["std"]` for all eight.
- `docs/GUIDE.md` - 11-section user guide covering the mental
  model, all closure adapters, custom stage / source / sink /
  driver implementations, batching, windowing, error policies,
  dead-letter routing, picking a driver, and common pitfalls.
- `README.md` gains a Quick start snippet and a Documentation
  section linking to the guide, API reference, REPS, benches,
  and examples directory.
- `docs/README.md` updated to index the new documentation.

### Notes

- All 8 examples build cleanly under
  `cargo clippy --all-targets --all-features -- -D warnings` and
  run end-to-end with the expected output.
- No changes to `src/` or `tests/`; this release is documentation
  and examples only. Existing 73 tests continue to pass.

## [0.7.0] - 2026-05-19

Driver trait release. Closes the third (and last) deferral from
the `0.3.0` design lock. All locked surfaces from the original
design lock are now shipped.

### Added

- `pub trait Driver` in `pipe_io::driver`, re-exported as
  `pipe_io::Driver`. Generic executor abstraction with `Send`
  bounds on the source and its item/error types. Not sealed;
  external executors (tokio, rayon, custom thread farms) can
  implement it.
- `impl Driver for SyncDriver` and (under `std`)
  `impl Driver for ThreadedDriver`. Both delegate to their existing
  inherent `run` methods.
- `Pipeline::run_with<D: Driver>(driver: D)` builder-terminal
  method. Lets callers select any `Driver` impl explicitly.
- 6 integration tests in `tests/driver_trait.rs`: sync via trait,
  threaded via trait, `run_with(SyncDriver)`, `run_with(ThreadedDriver)`,
  a custom `CountingDriver` impl, and a static check that the
  built-in and custom drivers all satisfy `Driver`.

### Changed

- `REPS.md` section 4.8 un-defers the `Driver` trait; the
  trait-based abstraction is now part of the locked surface.
- `docs/API.md` documents the trait and the `Pipeline::run_with`
  method.
- `SyncDriver::run` (inherent method) keeps its looser bound (no
  `Send` requirement on the source). The trait impl uses the
  stricter bound. Both compile to the same call.

### Notes

- The trait deliberately carries the stricter `Send` bound so
  that any `Driver` impl can be a threaded executor. To drive a
  non-`Send` source on the calling thread, call
  `SyncDriver::run` directly (inherent method); the trait method
  is unavailable for non-`Send` sources by design.
- This is the last design-lock deferral. The locked `1.0.0`
  surface in `REPS.md` is now fully implemented except for
  `PipelineBuilder::buffer(capacity)`, which was listed in §4.9
  but is not yet shipped; it lands in a future release alongside
  per-stage threading or as a separate `0.8.x` slot.

## [0.6.0] - 2026-05-19

Dead-letter routing release. Wires up `ErrorPolicy::DeadLetter`
(reserved since `0.3.0`) to a `Sink<Item = StageFailure>` installed
via the new `.dead_letter(sink)` builder method. Closes the second
of the three deferrals from the `0.3.0` design lock.

### Added

- `PipelineBuilder::dead_letter(sink)` (std-only): installs a
  `Sink<Item = StageFailure>` that receives the failures produced by
  stages running under `ErrorPolicy::DeadLetter`. Cloneable shared
  handle internally, so the sink can be installed before *or* after
  the failing stages; installation order does not matter. Calling
  `dead_letter` more than once replaces the previous sink.
- `StageFailure::new(stage, source)` constructor.
- 6 integration tests in `tests/dead_letter.rs`: routing, install
  order independence, no-sink fallback to Continue, sink-error
  bubble-up, FailFast override, Continue does not route.

### Changed

- `ErrorPolicy::DeadLetter` now routes to the installed dead-letter
  sink instead of behaving identically to `Continue`. If no sink is
  installed, it still degrades to `Continue` (silent drop) - this is
  documented as the no-sink fallback rather than a stub.
- Errors raised by the dead-letter sink itself bubble up from
  `Pipeline::run` as `Error::Sink { stage: StageId("dead_letter"), .. }`.
- The dead-letter sink receives `Flush` and `Close` after the main
  chain completes, so users can install a buffered or batching sink
  for failures.
- `REPS.md` sections 4.7 and 4.9 un-defer dead-letter routing and
  mark the builder method as `(std)`. `docs/API.md` updates the
  signature.

### Notes

- The locked `StageFailure` carries `stage` and `source` only; it
  does not capture the failing input. Carrying the input would
  require type erasure (`Box<dyn Any + Send>`) at every failure
  site, which is a significant complexity trade-off. The struct is
  `#[non_exhaustive]` so a future release can add an `input` field
  without breaking SemVer.
- Under `no_std`, `ErrorPolicy::DeadLetter` continues to behave
  identically to `ErrorPolicy::Continue` (the routing handle
  requires `std::sync::Mutex`).

## [0.5.0] - 2026-05-19

Windowing release. Lands the `window` module that was deferred at
`0.3.0`. Closes one of the three locked-but-not-shipped surfaces
from the design lock.

### Added

- `pipe_io::window` module (std, default-on).
- `trait Clock: Send` with a single `fn now(&self) -> Instant` method.
- `struct SystemClock` - default `Clock` impl wrapping
  `std::time::Instant::now`.
- `enum WindowPolicy` with `Tumbling { size }`, `Sliding { size, slide }`,
  and `Session { idle }` variants.
- `struct Window<T>` with `items`, `len`, `is_empty`, `start`, `end`,
  `into_inner` accessors; `IntoIterator` for `Window<T>` and `&Window<T>`.
- `PipelineBuilder::window(policy)` using the default `SystemClock`.
- `PipelineBuilder::window_with(policy, clock)` for user-supplied
  clocks (deterministic tests, embedded time sources).
- 5 unit tests in `src/window.rs` with a deterministic in-memory
  clock (tumbling boundary emission, session idle close, sliding
  overlap, tumbling flush of partial window, `Window::into_inner`).
- 4 integration tests in `tests/window.rs` (tumbling rollup,
  session boundary, sliding overlap, empty-source no-emit).

### Changed

- `REPS.md` section 4.6 un-defers the `window` module and documents
  the `T: Clone` requirement plus the no-background-timer semantics.
- `docs/API.md` adds a `pipe_io::window` section and lists `Window`,
  `WindowPolicy`, `Clock`, `SystemClock` in the crate root table.

### Notes

- Both window builder methods require `T: Clone` because sliding
  windows duplicate items across overlapping windows. Consumers
  with non-`Clone` types and tumbling semantics can use `.batch()`
  with `BatchPolicy::max_age` as a substitute.
- The pure synchronous core does not run a background timer. A
  session window that goes idle with no further items waiting will
  close only when `Pipeline::run` reaches end-of-stream and flushes
  the chain. This is documented in the module-level rustdoc.

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

[Unreleased]: https://github.com/jamesgober/pipe-io/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/jamesgober/pipe-io/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/jamesgober/pipe-io/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/jamesgober/pipe-io/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/jamesgober/pipe-io/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/jamesgober/pipe-io/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/pipe-io/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/pipe-io/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/jamesgober/pipe-io/releases/tag/v0.1.0
