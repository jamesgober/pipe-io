# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-10-13

**Major release.** Fixes the correctness problems found by the
2026-10-13 audit, makes the "bounded buffers / backpressure" claim
true, and makes `Pipeline` drivable by external executors. Several
fixes change observable behaviour and the error model changes so error
chains work, so this is a major version. Upgrade notes:
[`docs/MIGRATION.md`](docs/MIGRATION.md#v100---v200).

### Added

- `PipelineBuilder::buffer(capacity)` (std): a bounded edge. Under
  `run_threaded` each segment between edges runs on its own thread,
  connected by a bounded channel; a full edge blocks the upstream
  thread. Ordering is preserved; errors, panics, cancellation, and the
  teardown order have defined semantics (see `ThreadedDriver`). Edge
  memory follows the items in flight (nothing is reserved for the whole
  capacity); `capacity` must not exceed 2^20 (`LimitTooLarge`).
- `Pipeline::step`, `Pipeline::tick`, `Pipeline::finish`,
  `Pipeline::stats`, `Pipeline::is_finished`, `Pipeline::config_error`,
  and the `Step` enum: drive a pipeline one item at a time from any
  executor. New examples `tokio_driver`, `rayon_driver`,
  `custom_driver` (rewritten), and `live_ingest`.
- `Pipeline::cancel_handle` and `CancelHandle`: graceful cancellation
  that drains and closes everything, then returns `Error::Cancelled`.
- `PipelineBuilder::try_sink` and `ConfigError` / `ConfigErrorKind`
  (including `LimitTooLarge { field, max }`): every invalid setting is
  a typed error; `BatchPolicy::validate`,
  `WindowPolicy::validate`, `WindowSpec::validate`,
  `RetryPolicy::validate`.
- `PipelineBuilder::idle_tick`, `Source::pull_timeout` (default
  method), `Pull`, `Stage::tick` (default method): age triggers and
  windows fire on quiet streams. `ChannelSource` implements
  `pull_timeout`.
- `PipelineBuilder::observe`, `Observer`, `Event`: lifecycle, error,
  retry, dead-letter, idle, and teardown events.
- `RunStats::items_out`, `errors`, `dead_lettered`, `retries`.
- `PipelineBuilder::try_map_ref` (borrowing closure; the failing input
  is attached to dead-letter records) and `try_map_retry` (std) with
  `RetryPolicy` and `Backoff` (fixed or exponential, optional jitter).
- `StageFailure::input`, `attempts`, `with_input`, `with_attempts`,
  `input_ref`, `take_input`; `StageFailure` implements `Error`.
- `Error::source()` (via `core::error::Error`), `Error::inner`,
  `Error::into_inner`, `Error::downcast_ref`, `Error::is_cancelled`,
  `Error::Config`, `Error::Driver`.
- Keyed windows: `window_by_key`, `window_by_key_with`. Every item (of
  any key) and every idle tick closes the expired windows of all keys,
  in order of their end, so a quiet key's window is emitted on time on a
  busy stream; O(1) per item when nothing expires, O(log keys) per
  window. `WindowSpec` with `max_items` (per window) and `max_keys` (per
  stage; the least recently active key is evicted in O(1)). `WindowPolicy::tumbling`, `sliding`,
  `session` constructors.
- `pipe_io::clock` module with `ManualClock`; `batch_with` and
  `batch_bytes_with` take an explicit clock.
- `ReaderSource::max_line_len` (default 1 MiB), `on_overflow`
  (`LineOverflow::Error`/`Skip`/`Truncate`), `lossy`; `ReaderError`;
  `ByteLinesSource`.
- `TeeSink` (fan-out to two sinks) and `ChainSource` (concatenate two
  sources).
- `PipelineBuilder::source_id`.
- `Assemble` trait naming the builder's type-state. It is sealed: it
  cannot be implemented or invoked outside the crate, and the execution
  internals behind it are private.
- `Debug` for every public type; `Batch` and `Window` gain `PartialEq`,
  `Eq`, `AsRef<[T]>`, `From<Vec<T>>` (Batch), `Default`/`Hash` (Batch),
  `Deref` (Window); `SharedHandle::snapshot`; more `ByteSize` impls
  (`[u8; N]`, `Box`, `Rc`, `Arc`, `Cow`).
- `docs/ARCHITECTURE.md`, `docs/PLATFORM-NOTES.md`, `tests/smoke.rs`,
  `tests/regressions.rs` (every audit reproduction),
  `tests/reaudit.rs` and `tests/edge_memory.rs` (every finding of the
  2.0.0 re-audit), a seeded randomised stress test of the threaded
  driver (`tests/threaded_stress.rs`, `PIPE_IO_STRESS_RUNS` for long
  soaks), a differential test of keyed windows against a naive model,
  lifecycle, threaded, stepping, configuration, and feature test
  suites, fuzz targets `window_policies`, `policy_mix`, `reader_lines`,
  `keyed_windows`, and a `keyed_eviction` benchmark.
- `rust-toolchain.toml` (pins the local toolchain; CI overrides it
  per job) and `deny.toml`.

### Changed

- **MSRV 1.85, edition 2024.**
- `StageError` is now "`Into<BoxError> + Debug + Send + 'static`" and
  `BoxError` is `Box<dyn core::error::Error + Send + Sync>`. Error types
  must implement `std::error::Error` (or be `&'static str` / `String`).
- Builder methods no longer panic. `batch` with no trigger, a byte
  trigger on `batch`, zero limits and durations, sliding `slide > size`,
  more than 1024 overlapping sliding windows, and `DeadLetter` without a
  dead-letter sink are reported as `Error::Config` before anything runs.
- `run_threaded` runs one thread per `buffer` segment and re-raises a
  worker panic with its original payload (1.x returned
  `Error::Cancelled` and lost the message). Its teardown order matches
  the single-threaded driver: the source thread waits for every
  downstream segment (and so the sink's flush and close) before calling
  `Source::close`.
- Each lifecycle call runs exactly once per run on every exit path:
  sink `flush` and `close`, `Source::close`, dead-letter `flush` and
  `close`. A failing `flush` is not called again by the abort that
  follows, and a panicking `flush` no longer prevents `close`.
- A stepped pipeline whose `step` or `tick` panicked (with the panic
  caught by the caller) is poisoned: the next `step`, `tick`, or
  `finish` tears it down and returns `Error::Driver`.
- `try_map_retry` backoff stops waiting (within about 10 ms) when the
  run is cancelled.
- Windows never emit empty windows; idle gaps are skipped in O(1).
  Tumbling windows flushed at end of stream report their nominal end.
  Sliding windows place the first item in every window that covers it.
- `RunStats`, `ErrorPolicy`, `WindowPolicy`, `EmitError`,
  `ChannelSinkError` are `#[non_exhaustive]`; `RunStats::duration`
  exists without `std` (always zero there).
- `ReaderSource`'s error type is `ReaderError`; lines over 1 MiB are
  rejected by default.
- `WriterSink::close` flushes the writer. `Error`'s `Debug` prints the
  inner error's `Debug`.
- Faster per-item path: control operations are unit variants, each
  stage call returns the number of items the sink accepted, and failures
  are boxed, so every stage call keeps its arguments and result in
  registers. Multi-stage chains measured up to about 50% faster than
  `1.0.0`; the bare source -> sink path is within about 0.5 ns per item
  of `1.0.0` (one independent measurement found it 20-25% slower there;
  `docs/BENCH.md`).
- Batching no longer reserves `max_items` slots up front (at most 1024
  slots and 64 KiB per open batch) and reads the clock only when
  `max_age` is set.
- `ReaderSource::with_capacity(0, ..)` uses a 1-byte buffer instead of
  reporting an empty stream. `ChainSource::pull_timeout` gives the
  second source only what is left of the timeout. When both halves of a
  `TeeSink` fail, the error keeps both messages.
- `#![deny(warnings)]` removed from the crate root (CI still denies
  warnings), so a new compiler lint cannot break downstream builds of
  the crate's own source.
- The `Error::Closed` variant now means "the pipeline already ran".
- Benchmarks use criterion 0.7 with `black_box` on every item; README,
  guide, and API reference rewritten; README and guide snippets are
  compiled as doctests.
- CI: `actions/checkout@v7`, `permissions: contents: read`, tests for
  default / all / no default features on Linux, macOS, and Windows,
  doc tests, MSRV build and test, `no_std` builds for
  `thumbv7em-none-eabihf` and `thumbv6m-none-eabi`, `cargo audit`,
  `cargo deny`, coverage (90% line gate), semver checks, and a 60-second
  fuzz run per target.

### Deprecated

- `pipe_io::source::Infallible`: use `core::convert::Infallible`.

### Removed

- `EmitError::WouldBlock`, `Error::Buffer`, and `BufferErrorKind`: never
  produced by any driver.

### Fixed

- A custom stage whose `flush` failed under `Continue`/`DeadLetter`
  stopped the flush, silently dropping every downstream buffered batch
  or window and the sink flush while `run` returned `Ok` (PIPE-001).
- Zero window `size`/`slide` looped forever and grew memory without
  bound (PIPE-002).
- An idle gap emitted one window per elapsed interval, mostly empty
  (PIPE-003).
- `Sliding { size: Duration::MAX }` panicked on `Instant` overflow
  (PIPE-004).
- `max_items(usize::MAX)` panicked with "capacity overflow"; large
  limits reserved memory per batch; `max_items(0)` was silently treated
  as 1 (PIPE-005).
- The byte counter of byte-aware batching could overflow (PIPE-006).
- On any error the sink, the dead-letter sink, and the source were
  never flushed or closed (PIPE-007).
- The threaded driver discarded panic payloads (PIPE-011).
- Source errors could not carry a custom stage id (PIPE-012).
- `Error::source()` returned `None` (PIPE-013).
- Sliding windows with `slide > size` silently dropped items; windows
  before the first item were missed (PIPE-016).
- `max_age` batches and windows never fired on a quiet stream
  (PIPE-017).
- `cargo test --no-default-features` failed (two doctests) (PIPE-018).
- Documentation claims that did not match the code (backpressure,
  `WouldBlock`, custom tokio/rayon drivers, lifecycle order, error
  counting, test counts, stale benchmark numbers, broken `.dev/` links).
- `expect`/`unreachable!` in library code replaced; poisoned locks are
  recovered instead of panicking.
- Fixed before release, after the 2.0.0 re-audit: keyed windows of a
  quiet key stayed open while other keys kept the stream busy (R01);
  `max_keys` eviction scanned every key, so capped keyed windows were
  quadratic (R02); `buffer(capacity)` was not validated and reserved
  every slot up front, so a huge capacity aborted the process (R03);
  `run_threaded` closed the source before the sink had flushed and
  closed (R04); retry backoff ignored cancellation (R05);
  `ReaderSource::with_capacity(0, ..)` read nothing (R06); a failing
  sink `flush` ran twice and a panicking one skipped `close` (R07); a
  stepped pipeline kept running after a caught panic (R08).

### Security

- Keyed windows with `max_keys` cost O(1) per eviction, so
  high-cardinality or adversarial keys cannot turn the key cap into a
  CPU denial of service (previously O(keys) per new key).
- A `buffer` edge allocates only for queued items and rejects
  capacities above 2^20, so a large capacity can no longer reserve
  gigabytes or abort the process.
- `ReaderSource` and `ByteLinesSource` bound the memory used per line
  (default 1 MiB) and offer `Skip`/`Truncate` so untrusted input cannot
  exhaust memory or end the run with one long line (PIPE-010).
- Library code contains no `unwrap`, `expect`, `panic!`, or
  `unreachable!` (enforced by lints); configuration that used to panic
  or hang is rejected; size and time arithmetic is checked or
  saturating.

### Dependencies

No runtime dependencies (unchanged). New dev-dependencies, used only by
benchmarks and examples: `criterion` 0.7 (statistically sound
benchmarks; newest line compatible with the 1.85 MSRV), `tokio` 1
(`rt-multi-thread`, `time`; the tokio driver example), `rayon` 1 (the
rayon driver example). `proptest` stays at 1.11 (1.12 needs Rust 1.88).

## [1.0.0] - 2026-05-19

**Stable API.** The public surface is frozen. Backwards-
incompatible changes after `1.0.0` require a major version bump
per Semantic Versioning. See [`REPS.md`](REPS.md) section 8 for
the binding policy and [`docs/API.md`](docs/API.md) for the
complete public-symbol reference.

No source-level changes from `v0.9.0`. `1.0.0` is a freeze, a
documentation polish, and the CI gate that protects the
guarantee going forward.

Behind the line: the journey from `v0.1.0` (scaffold) to `v1.0.0`
landed the full `REPS.md` section 4 surface - `Source`, `Stage`,
`Sink`, `Emit`, the typed builder with closure adapters, count /
byte / age batching, tumbling / sliding / session windowing
behind a pluggable `Clock`, the `FailFast` / `Continue` /
`DeadLetter` error policies with shared-handle dead-letter
routing, both `SyncDriver` and `ThreadedDriver`, the open
`Driver` trait for custom executors, an 84-test suite covering
unit, integration, property, and doctest paths, and four
`cargo-fuzz` harnesses.

### Changed - `cargo-semver-checks` CI is now a hard gate

The CI `semver-checks` job no longer runs with `|| true`.
Backwards-incompatible additions to the public surface fail CI
before they can ship.

### Changed - `REPS.md` section 8 declares stability

The section was previously "Stability begins at `1.0.0`,
governed by [...]". It now declares the public API frozen and
enumerates the patch / minor / major rules in full, plus the
`#[non_exhaustive]` reminder for `Error` and `StageFailure`.

### Changed - `REPS.md` section 7 records the measured perf contract

The 7 placeholder ("To be specified at `0.4.0`") is replaced
with the headline numbers from `docs/BENCH.md` plus a note that
they are indicative, not contractual.

### Changed - `REPS.md` section 4.9 drops the unimplemented `.buffer(...)`

The `.buffer(capacity)` builder method was listed in the locked
surface but never implemented. Section 4.9 records it as
post-1.0 minor-release roadmap material; section 4.10 keeps the
feature-flag table accurate.

### Changed - `ROADMAP.md` re-anchored

All `0.x` releases listed as shipped. Forward-looking work
(`.buffer`, per-stage threading, `pipe-io-tokio`,
`StageFailure::input`) explicitly marked as post-1.0 minor
releases.

### Changed - `README.md` rewritten for production

Hero block, feature highlights grouped by concern (pipeline
composition / backpressure-batching-windowing / error
isolation / runtime / reliability), installation block with
feature-flag table, quick-start snippets per surface
(basic / batching / windowing / dead-letter / threaded /
custom driver), examples table, status section declaring
1.0.0 stable, documentation links, version compatibility,
contributing notes, license.

### Changed - `docs/API.md` declared frozen

The intro now points at the `1.0.0` freeze and the
[Stability](docs/API.md#stability) section spells out the SemVer
rules and the `#[non_exhaustive]` reminder. The duplicated
feature-flag tables are harmonized.

### Changed - `docs/MIGRATION.md` records the `v0.9.0 -> v1.0.0` upgrade

Pure version marker; no source changes required.

There are no source-level changes to the crate code between
`v0.9.0` and `v1.0.0`. The package contents on crates.io are
functionally identical; the version bump signals API stability.

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

[Unreleased]: https://github.com/jamesgober/pipe-io/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/jamesgober/pipe-io/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/jamesgober/pipe-io/compare/v0.9.0...v1.0.0
[0.9.0]: https://github.com/jamesgober/pipe-io/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/jamesgober/pipe-io/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/jamesgober/pipe-io/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/jamesgober/pipe-io/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/jamesgober/pipe-io/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/pipe-io/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/pipe-io/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/jamesgober/pipe-io/releases/tag/v0.1.0
