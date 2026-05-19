# pipe-io - Project Specification (REPS)

> Authoritative specification for the public API surface and design
> contract of `pipe-io`.

## 1. Identity

- **Crate name:** `pipe-io`
- **Author:** James Gober <me@jamesgober.com>
- **Repository:** https://github.com/jamesgober/pipe-io
- **License:** Apache-2.0
- **MSRV:** 1.75

## 2. Mission

Typed source-transform-sink pipelines with backpressure, batching, windowing, and per-stage error isolation. A lightweight runtime-agnostic stream processor for in-process workloads. The missing middle ground between raw iterators and full distributed stream processing.

## 3. Scope

`pipe-io` provides:

1. Typed pipeline primitives: `Source`, `Stage`, `Sink`.
2. A builder that composes those primitives into a runnable `Pipeline`.
3. Backpressure between stages via bounded buffers.
4. Batching with count, byte, and age triggers.
5. Time-based windowing (tumbling, sliding, session) under a
   pluggable `Clock`.
6. Per-stage error policies: fail-fast, continue, dead-letter.
7. Two built-in execution drivers: a single-threaded `SyncDriver`
   suitable for `no_std` and a multi-threaded `ThreadedDriver` for
   `std` users. Third parties can implement the `Driver` trait
   to plug in their own runtime.

The crate stays in-process. Distribution, replication, exactly-once
semantics across hosts, and durable state are out of scope (see §12).

## 4. Public API

The public surface is grouped by module. Items marked **(std)**
require the `std` feature (on by default); everything else is
available under `no_std`.

### 4.1 Crate root

- `pipe_io::VERSION` - `&'static str`, populated by Cargo.
- `pipe_io::Pipeline` - a built, runnable pipeline.
- `pipe_io::PipelineBuilder` - typed builder; produced by `Pipeline::from_source`.
- `pipe_io::RunStats` - counters and timing returned by `Pipeline::run`.
- `pipe_io::Error`, `pipe_io::Result<T>` - top-level error type and alias.
- `pipe_io::StageId` - `&'static str` newtype identifying a stage in errors and stats.

Re-exports for ergonomics:

- `pipe_io::Source`, `pipe_io::Stage`, `pipe_io::Sink`, `pipe_io::Emit`
  (also live under their respective modules).
- `pipe_io::BatchPolicy`, `pipe_io::Batch`.
- `pipe_io::WindowPolicy`, `pipe_io::Window`.
- `pipe_io::ErrorPolicy`.

### 4.2 `pipe_io::source`

- `trait Source` - pull-based producer.

  ```text
  trait Source {
      type Item;
      type Error: StageError;
      fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error>;
      fn close(&mut self) -> Result<(), Self::Error> { Ok(()) }
  }
  ```

  `Ok(None)` signals end-of-stream. `close` is invoked by the
  driver once the pipeline shuts down.

- `IterSource<I>` - adapts any `IntoIterator` into a `Source`.
- `FnSource<F, T, E>` - adapts a `FnMut() -> Result<Option<T>, E>`.
- `ChannelSource<T>` **(std)** - adapts an `mpsc::Receiver<T>`.
- `ReaderSource<R>` **(std)** - line-buffered `Source<Item = String>` over any `io::Read`.

### 4.3 `pipe_io::stage`

- `trait Stage` - the core transform abstraction.

  ```text
  trait Stage {
      type Input;
      type Output;
      type Error: StageError;
      fn process(
          &mut self,
          item: Self::Input,
          out: &mut dyn Emit<Item = Self::Output>,
      ) -> Result<(), Self::Error>;
      fn flush(
          &mut self,
          out: &mut dyn Emit<Item = Self::Output>,
      ) -> Result<(), Self::Error> { Ok(()) }
  }
  ```

  Stages emit zero or more outputs per input via `Emit`, which
  supports 1:1 (map), 0..1 (filter), and 1:N (batching, windowing,
  flat-map) fan-out under one trait.

- `trait Emit` - bounded downstream handle.

  ```text
  trait Emit {
      type Item;
      fn emit(&mut self, item: Self::Item) -> Result<(), EmitError>;
  }
  ```

  `EmitError::Closed` reports downstream shutdown. Driver
  implementations decide whether `emit` blocks on a full buffer or
  returns `EmitError::WouldBlock`.

- `enum EmitError { Closed, WouldBlock }`.

- `map(f)`, `filter(p)`, `filter_map(f)`, `flat_map(f)`,
  `inspect(f)` - builder methods on `PipelineBuilder` that wrap
  closures into `Stage` impls.

### 4.4 `pipe_io::sink`

- `trait Sink`.

  ```text
  trait Sink {
      type Item;
      type Error: StageError;
      fn write(&mut self, item: Self::Item) -> Result<(), Self::Error>;
      fn flush(&mut self) -> Result<(), Self::Error> { Ok(()) }
      fn close(&mut self) -> Result<(), Self::Error> { Ok(()) }
  }
  ```

- `FnSink<F, T, E>` - adapts a `FnMut(T) -> Result<(), E>`.
- `VecSink<T>` **(std)** - collects into a `Vec<T>` behind an
  `Arc<Mutex<_>>` handle; useful for tests. `std` because the
  shared handle requires `Arc<Mutex>`; a `no_std`-compatible
  variant lands in a later release.
- `NullSink<T>` - discards items; useful for benchmarking.
- `ChannelSink<T>` **(std)** - adapts an `mpsc::SyncSender<T>`.
- `WriterSink<W>` **(std)** - line-writes any `Display` item to any `io::Write`.

Built-in stages and sinks all assume `Send` so they can be driven
by [`crate::driver::ThreadedDriver`] alongside [`crate::driver::SyncDriver`].
Stages and sinks supplied by consumers must also be `Send` to enter
the pipeline.

### 4.5 `pipe_io::batch`

- `struct BatchPolicy` - trigger configuration. All triggers OR
  together; the first satisfied trigger flushes the batch.

  ```text
  BatchPolicy::new()
      .max_items(usize)
      .max_bytes(usize)        // requires Item: ByteSize
      .max_age(Duration)       // (std)
  ```

- `trait ByteSize { fn byte_size(&self) -> usize; }` - opt-in,
  enables `max_bytes`. Blanket impls for `&str`, `String`, `Vec<u8>`,
  `&[u8]`.

- `struct Batch<T>` - owned slice newtype with `len`, `is_empty`,
  `iter`, `into_inner`. `Deref<Target = [T]>` and `IntoIterator`.

- `PipelineBuilder::batch(policy)` - inserts a batching stage,
  changing the carrier type from `T` to `Batch<T>`.

### 4.6 `pipe_io::window` **(std)**

- `trait Clock: Send { fn now(&self) -> Instant; }`.
- `struct SystemClock` - default `Clock` impl wrapping
  `std::time::Instant::now`.
- `enum WindowPolicy { Tumbling { size }, Sliding { size, slide }, Session { idle } }`.
- `struct Window<T>` - items plus `start: Instant`, `end: Instant`.
  Methods: `new`, `items`, `len`, `is_empty`, `start`, `end`,
  `into_inner`. Implements `IntoIterator`.
- `PipelineBuilder::window(policy)` - default `SystemClock`.
- `PipelineBuilder::window_with(policy, clock)` - user-supplied `Clock`.

Both builder methods require `T: Clone` because the sliding policy
duplicates items across overlapping windows. Consumers with
non-`Clone` types and tumbling semantics can use `.batch()` with
`BatchPolicy::max_age` as a substitute.

Windows close on the next item arriving after the close condition
fires, or at end-of-stream via `Stage::flush`. The pure synchronous
core does not run a background timer; an idle session with no
arriving items will not close until end-of-stream.

### 4.7 `pipe_io::error`

- `enum Error` (non-exhaustive):
  - `Source { stage: StageId, source: BoxError }`
  - `Stage  { stage: StageId, source: BoxError }`
  - `Sink   { stage: StageId, source: BoxError }`
  - `Buffer { stage: StageId, kind: BufferErrorKind }`
  - `Cancelled`
  - `Closed`
- `enum BufferErrorKind { Full, Closed }`.
- `type Result<T> = core::result::Result<T, Error>`.
- `trait StageError: core::fmt::Debug + core::fmt::Display + Send + Sync + 'static`
  with a blanket impl. The `core::error::Error` super-trait was
  considered but `core::error::Error` stabilized in Rust 1.81 and
  the crate's MSRV is 1.75; the lighter bound covers every
  `std::error::Error` type via the blanket impl while remaining
  no_std-compatible at MSRV 1.75.
- `type BoxError = alloc::boxed::Box<dyn StageError>`.
- `enum ErrorPolicy { FailFast, Continue, DeadLetter }` -
  attached per stage by the builder.

### 4.8 `pipe_io::driver`

A trait-based driver abstraction is deferred past `0.3.0` because
`SyncDriver` and `ThreadedDriver` have different `Send` bounds on
the source and item types, and exposing a single unified trait
locks in the stricter bounds for both. The trait will land once
the bound difference is reconciled (likely via a sealed
helper-trait pattern or two separate trait surfaces).

`0.3.x` ships:

- `SyncDriver` - zero-sized marker. Pumps the pipeline on the
  caller's thread. `no_std`-compatible.
- `ThreadedDriver` **(std)** - zero-sized marker. Pumps the
  pipeline on a single background thread; the calling thread
  blocks on `join`. Per-stage threading is a future enhancement.
- `RunStats` - statistics returned by a successful run.

`Pipeline` exposes:

- `.run()` - synchronous; equivalent to `SyncDriver::default().run(...)`.
- `.run_threaded()` **(std)** - threaded; equivalent to
  `ThreadedDriver::default().run(...)`.

### 4.9 Builder surface (full)

```text
Pipeline::from_source(source) -> PipelineBuilder<T>

PipelineBuilder<T>:
    .stage_id(name: &'static str)             // labels the next stage
    .map(F)               where F: FnMut(T) -> U                       -> PipelineBuilder<U>
    .filter(P)            where P: FnMut(&T) -> bool                   -> PipelineBuilder<T>
    .filter_map(F)        where F: FnMut(T) -> Option<U>               -> PipelineBuilder<U>
    .flat_map(F)          where F: FnMut(T) -> I, I: IntoIterator      -> PipelineBuilder<I::Item>
    .inspect(F)           where F: FnMut(&T)                           -> PipelineBuilder<T>
    .try_map(F)           where F: FnMut(T) -> Result<U, E>            -> PipelineBuilder<U>
    .stage(S: Stage<Input = T>)                                        -> PipelineBuilder<S::Output>
    .batch(BatchPolicy)                                                -> PipelineBuilder<Batch<T>>
    .window(WindowPolicy)                              // (std)        -> PipelineBuilder<Window<T>>
    .window_with(WindowPolicy, C: Clock)               // (std)        -> PipelineBuilder<Window<T>>
    .on_error(ErrorPolicy)                                             -> PipelineBuilder<T>
    .dead_letter(S: Sink<Item = StageFailure>)                         -> PipelineBuilder<T>
    .buffer(capacity: usize)                                           -> PipelineBuilder<T>
    .sink(S: Sink<Item = T>)                                           -> Pipeline

Pipeline:
    .run() -> Result<RunStats>                          // uses default driver
    .run_with<D: Driver>(driver: D) -> Result<RunStats>
```

`StageFailure` is a non-exhaustive struct carrying `stage: StageId`,
the failing input (when available), and the boxed `Error`.

### 4.10 Feature flags

| Flag    | Default | Effect                                                                  |
|---------|---------|-------------------------------------------------------------------------|
| `std`   | yes     | Threaded driver, channel adapters, reader/writer adapters, windowing, batch age triggers. |

Additional opt-in adapter features may be added later (for example
`tokio-adapter`) under SemVer minor releases. They are not part of
the `0.2.0` design lock.

### 4.11 MSRV

`1.75`. Bumps require a minor version increment and a CHANGELOG
entry per §6.

### 4.12 Runtime dependencies

Zero. Built on `core` and `alloc` (for `Box`, `Vec`) plus `std`
when the `std` feature is enabled. Dev-dependencies and adapter
crates are not part of the runtime dependency footprint.

## 5. Safety contract

The crate sets `#![forbid(unsafe_code)]` at the root. No `unsafe`
is permitted in `src/`. If a future release lifts that ban, every
`unsafe` block must carry a `// SAFETY:` comment per
`.dev/DIRECTIVES.md` section 3.

## 6. MSRV policy

Pinned at 1.75. Bumps require a minor version increment and a
CHANGELOG entry under `### Changed` with rationale.

## 7. Performance contract

To be specified at `0.4.0` once `benches/` lands. Targets under
consideration:

- Steady-state allocation-free hot path for `.map` / `.filter`.
- Single-thread throughput within 2x of a hand-rolled
  `Iterator` chain for trivial transforms.
- Bounded-buffer wake latency under 1 microsecond on the
  threaded driver for the common case.

These are non-binding pre-`1.0` targets.

## 8. Stability guarantees

`0.x.y` releases are not API-stable. Stability begins at `1.0.0`,
governed by the same rules as `log-io` section 8: patch is
bug-fix-only, minor is purely additive, major is removal /
rename / signature change. `cargo-semver-checks` gates the
public surface in CI from `0.9.0` onward.

## 9. Dependency policy

Zero runtime dependencies preferred. Any added dependency requires
a documented justification in `.dev/DESIGN.md` (or its successor
when audit material is split out).

## 10. Testing requirements

- Unit tests next to code in `#[cfg(test)] mod tests`.
- Integration tests under `tests/` covering each public adapter,
  each driver, each batch trigger, each window policy, and each
  error policy.
- `proptest`-based property tests under `tests/property.rs` for
  ordering, completeness, and dead-letter routing invariants.
- Stress tests under `tests/stress.rs` exercising backpressure
  and multi-stage error recovery.
- Doctests on every public item once §11 is satisfied.

## 11. Documentation requirements

- Every public item carries a rustdoc block with `# Errors`,
  `# Panics`, and `# Example` sections where applicable.
- `docs/API.md` mirrors the rustdoc for offline reading.
- `docs/GUIDE.md` (added at `0.3.0`) walks through the common
  pipeline patterns with runnable examples.

## 12. Out of scope

- Distributed execution. Sharding, partitioning, cross-host
  coordination.
- Durable state. Checkpointing, exactly-once recovery, replay logs.
- Persistent storage. The crate does not write to disk except
  through user-supplied `Sink` impls.
- A built-in async runtime. The `Driver` trait is the integration
  point; tokio and async-std adapters can live in companion
  crates.
- General-purpose actor model. `pipe-io` is a data-flow primitive,
  not an actor framework. Stages have no addresses and cannot
  send messages other than their declared output type.
- Stream SQL or a query language. Pipelines are built in Rust.

## 13. References

- `.dev/ROADMAP.md` - milestone schedule.
- `.dev/DESIGN.md` - design trade-offs, alternatives, ecosystem boundaries, consumer use cases.
- `.dev/DIRECTIVES.md` - project-wide standards.
- `docs/API.md` - public API reference (rustdoc mirror).
