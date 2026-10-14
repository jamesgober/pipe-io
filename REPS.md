# pipe-io - Project Specification (REPS)

> Authoritative specification for the public API surface and design
> contract of `pipe-io`. Where this file and the portfolio-wide REPS
> differ, this file wins for this crate.

## 1. Identity

- **Crate name:** `pipe-io`
- **Author:** James Gober <me@jamesgober.com>
- **Repository:** https://github.com/jamesgober/pipe-io
- **License:** Apache-2.0
- **MSRV:** 1.85 (edition 2024)

## 2. Mission

Typed source-transform-sink pipelines with bounded, backpressured
stage edges, batching, windowing, and per-stage error isolation. A
zero-dependency, in-process stream processor: the middle ground between
raw iterators and distributed stream processing.

## 3. Scope

`pipe-io` provides:

1. Typed pipeline primitives: `Source`, `Stage`, `Sink`, `Emit`.
2. A builder that records stages and produces a runnable `Pipeline`.
3. Bounded stage edges (`buffer(capacity)`): under the threaded driver
   each segment runs on its own thread and edges are bounded channels
   that block when full.
4. Batching with count, byte, and age triggers.
5. Time-based windowing (tumbling, sliding, session, per key) under a
   pluggable `Clock`, with per-window and per-key memory limits.
6. Per-stage error policies (fail fast, continue, dead letter), retry
   with backoff, and capture of the failing input.
7. Execution on the calling thread (`SyncDriver`), across threads
   (`ThreadedDriver`), or step by step under any executor
   (`Pipeline::step`); the `Driver` trait is open.
8. Guaranteed teardown: sink, dead-letter sink, and source are flushed
   and closed on every exit path.
9. Validated configuration (`ConfigError`), cancellation, run counters,
   and an observer for lifecycle and error events.

The crate stays in-process. Distribution, replication, exactly-once
semantics across hosts, and durable state are out of scope (section 12).

## 4. Public API

Items marked **(std)** need the `std` feature (on by default). The full
listing with signatures is [`docs/API.md`](docs/API.md); this section is
the contract.

### 4.1 Crate root

`VERSION`, `Pipeline`, `PipelineBuilder`, `Assemble`, `Step`,
`CancelHandle` (pointer-sized atomics), `RunStats`, `Driver`, `Error`,
`Result<T, E = Error>`, `BoxError`, `StageError`, `ConfigError`,
`ConfigErrorKind`, `ErrorPolicy`, `StageFailure`, `StageId`, `Source`,
`Pull`, `Stage`, `Emit`, `EmitError`, `Sink`, `Batch`, `BatchPolicy`,
`ByteSize`, `Observer`, `Event`; (std) `Clock`, `SystemClock`,
`ManualClock`, `Window`, `WindowPolicy`, `WindowSpec`, `RetryPolicy`,
`Backoff`.

### 4.2 `pipe_io::source`

```text
trait Source {
    type Item;
    type Error: StageError;
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error>;
    fn pull_timeout(&mut self, timeout: Duration) -> Result<Pull<Self::Item>, Self::Error>; // default: pull
    fn close(&mut self) -> Result<(), Self::Error>;                                       // default: Ok
}
```

Adapters: `IterSource`, `FnSource`, `ChainSource`; (std)
`ChannelSource` (implements `pull_timeout`), `ReaderSource` (String
lines) and `ByteLinesSource` (byte lines) with a line-length limit
(default 1 MiB), an overflow policy (`LineOverflow`), and lossy UTF-8.

### 4.3 `pipe_io::stage`, `pipe_io::emit`

```text
trait Stage {
    type Input;
    type Output;
    type Error: StageError;
    fn process(&mut self, item: Self::Input, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error>;
    fn tick(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error>;  // default: Ok
    fn flush(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error>; // default: Ok
}
trait Emit { type Item; fn emit(&mut self, item: Self::Item) -> Result<(), EmitError>; }
enum EmitError { Closed }   // non-exhaustive
```

`emit` blocks while a bounded edge is full; `Closed` means the
downstream failed or the run is ending.

### 4.4 `pipe_io::sink`

```text
trait Sink {
    type Item;
    type Error: StageError;
    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>; // default: Ok
    fn close(&mut self) -> Result<(), Self::Error>; // default: Ok
}
```

Adapters: `NullSink`, `FnSink`, `TeeSink`; (std) `VecSink` +
`SharedHandle`, `ChannelSink` + `ChannelSinkError`, `WriterSink`.

### 4.5 `pipe_io::batch`

`BatchPolicy` (`max_items`, `max_bytes`, (std) `max_age`, accessors,
`validate`), `Batch<T>`, `ByteSize`. Triggers OR together; at least one
is required and every limit must be non-zero.

### 4.6 `pipe_io::window`, `pipe_io::clock` (std)

`WindowPolicy` (`Tumbling`, `Sliding`, `Session`; non-exhaustive),
`WindowSpec` (`max_items`, `max_keys`), `Window<T>`,
`MAX_SLIDING_OVERLAP`; `Clock`, `SystemClock`, `ManualClock`. Windows
emit only when non-empty; idle gaps cost O(1); instant arithmetic is
checked.

### 4.7 `pipe_io::error`

- `Error` (non-exhaustive): `Source`, `Stage`, `Sink`, `Config`,
  `Cancelled`, `Closed`, `Driver`. Implements `core::error::Error`
  with `source()`; `stage`, `inner`, `into_inner`, `downcast_ref`.
- `ConfigError` + `ConfigErrorKind` (non-exhaustive).
- `ErrorPolicy` (non-exhaustive): `FailFast` (default), `Continue`,
  `DeadLetter`. `DeadLetter` without a dead-letter sink is a
  configuration error.
- `StageFailure` (non-exhaustive): `stage`, `source`, `input`,
  `attempts`.
- `StageError`: blanket over `Into<BoxError> + Debug + Send + 'static`;
  `BoxError = Box<dyn core::error::Error + Send + Sync>`.

### 4.8 `pipe_io::driver`, execution

- `trait Driver { fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats> where S: Source + Send + 'static, S::Item: Send + 'static, S::Error: Send + 'static; }`
- `SyncDriver`; (std) `ThreadedDriver` (one thread per `buffer`
  segment; ordering preserved; first error wins; panics re-raised with
  their payload; graceful cancellation).
- `RunStats` (non-exhaustive): `items_in`, `items_out`, `errors`,
  `dead_lettered`, `retries`, `duration`.
- `Pipeline`: `run`, `run_with`, (std) `run_threaded`, `step`, `tick`,
  `finish`, `stats`, `is_finished`, `config_error`, `cancel_handle`.

### 4.9 Builder surface

```text
Pipeline::from_source(source) / Pipeline::from_iter(iter) -> PipelineBuilder<T, S, impl Assemble<..>>

settings:   stage_id, source_id, on_error, dead_letter, observe, idle_tick
transforms: map, filter, filter_map, flat_map, inspect,
            try_map, try_map_ref, (std) try_map_retry, stage,
            batch, batch_bytes, (std) batch_with, batch_bytes_with,
            (std) window, window_with, window_by_key, window_by_key_with,
            (std) buffer
terminals:  sink -> Pipeline, try_sink -> Result<Pipeline, ConfigError>
```

Builder methods never panic. Invalid settings are reported as
`ConfigError`.

### 4.10 `pipe_io::observe`

`Observer` (implemented for `Fn(&Event<'_>) + Send + Sync`) and `Event`
(non-exhaustive): `Started`, `StageError`, `Retry`, `DeadLettered`,
`Idle`, `TeardownError`, `Finished`. No per-item events.

### 4.11 Feature flags

| Flag  | Default | Effect |
|-------|---------|--------|
| `std` | yes     | Threads, time (clock, windows, `max_age`, retry), and the std adapters. |

New features must be opt-in and additive.

### 4.12 Runtime dependencies

Zero. `core` + `alloc`, plus `std` with the `std` feature.

## 5. Safety contract

`#![forbid(unsafe_code)]` at the crate root. No `unsafe` in `src/`. The
crate root also carries the portfolio lint block (`deny(warnings)`,
`missing_docs`, `missing_debug_implementations`, `unused_results`,
`clippy::unwrap_used`, `clippy::expect_used`, `clippy::panic`,
`clippy::unreachable`, ...). No panic is reachable from the public API
on any input or configuration.

## 6. MSRV policy

1.85, verified in CI. Raising the MSRV is a major version change, per
the portfolio `VERSIONING.md`.

## 7. Performance contract

Measured numbers and method: [`docs/BENCH.md`](docs/BENCH.md). The
architectural cost is one indirect call per stage per item, no
allocation per item in the pipeline machinery, and one channel
hand-off per item per `buffer` edge. Numbers are indicative, not
contractual. A change that regresses a tracked benchmark by more than
5% needs a recorded justification.

## 8. Stability guarantees

The public API follows Semantic Versioning:

- **Patch (`2.0.x`)**: bug fixes, documentation, internal performance
  work, tests.
- **Minor (`2.x.0`)**: additions, new opt-in features, new variants of
  `#[non_exhaustive]` enums, new fields of `#[non_exhaustive]` structs.
- **Major (`3.0.0`)**: removals, renames, signature changes, behaviour
  changes users would notice, MSRV bumps, non-opt-in dependencies.

`cargo-semver-checks` runs in CI on every pull request and push to
`main`. The execution internals are private and `Assemble` is sealed,
so they are not part of the API.

## 9. Dependency policy

Zero runtime dependencies. An optional dependency needs a non-default
feature, a justification in `Cargo.toml` and the CHANGELOG, and must
pass `cargo deny` (`deny.toml`) and `cargo audit`.

## 10. Testing requirements

- Unit tests next to the code; integration tests under `tests/` for
  every adapter, driver, trigger, window policy, error policy, and
  lifecycle path (including panics).
- Every audit reproduction lives on as a regression test
  (`tests/regressions.rs`).
- Property tests (`tests/property.rs`) for ordering, losslessness,
  partitioning, and agreement between drivers.
- `cargo-fuzz` targets under `fuzz/`, run for 60 seconds each in CI.
- Doctests on public items; the README and the user guide are compiled
  as doctests.
- Line coverage of at least 90% (CI gate) and periodic mutation testing.

## 11. Documentation requirements

- Every public item has rustdoc with `# Errors`, `# Panics`, and
  examples where applicable.
- `docs/API.md` mirrors the public surface; `docs/GUIDE.md` walks
  through every feature; `docs/ARCHITECTURE.md` records the design and
  its trade-offs; `docs/BENCH.md` holds measured numbers;
  `docs/MIGRATION.md` holds upgrade notes; `docs/PLATFORM-NOTES.md`
  holds platform specifics.
- Documentation makes no claim the code does not meet.

## 12. Out of scope

- Distributed execution, sharding across hosts.
- Durable state, checkpointing, exactly-once recovery.
- Persistent storage beyond user-supplied sinks.
- A built-in async runtime. `Pipeline::step` is the integration point;
  tokio and rayon integration is shown in `examples/`.
- An actor model or a query language.

## 13. References

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) - design and
  trade-offs.
- [`docs/API.md`](docs/API.md) - public API reference.
- [`CHANGELOG.md`](CHANGELOG.md) - release history.
