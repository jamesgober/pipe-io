# pipe-io - Migration Guide

> Per-version upgrade notes: what to change in your code, and the
> behavioural changes worth checking even when your code keeps
> compiling. `v2.0.0` is the first release with breaking changes.
>
> Publishing note: `v0.2.0` (a documentation-only design pass) and
> `v0.9.0` were tagged but never published to crates.io. Their content
> shipped in `v0.3.0` and `v1.0.0` respectively.

## Contents

- [v1.0.0 -> v2.0.0](#v100---v200)
- [v0.9.0 -> v1.0.0](#v090---v100)
- [v0.8.0 -> v0.9.0](#v080---v090)
- [v0.7.0 -> v0.8.0](#v070---v080)
- [v0.6.0 -> v0.7.0](#v060---v070)
- [v0.5.0 -> v0.6.0](#v050---v060)
- [v0.4.0 -> v0.5.0](#v040---v050)
- [v0.3.0 -> v0.4.0](#v030---v040)
- [v0.1.0 -> v0.3.0](#v010---v030)
- [Stability commitment at v1.0.0](#stability-commitment-at-v100)

---

## v1.0.0 -> v2.0.0

**Major release.** `2.0` fixes correctness problems found in the
2026-10-13 audit (lost flushes, missing teardown on errors, hangs and
panics from bad configuration) and makes the backpressure claim real.
Several fixes change observable behaviour, and the error model changes
so that error chains work; both require a major version. Most code
needs no more than the first two steps below.

### 1. Toolchain

MSRV is now **1.85** (edition 2024). Rust 1.75-1.84 cannot build 2.0.

### 2. Error types must be real errors

`StageError` was "`Debug + Display + Send + Sync`". It is now "anything
convertible into `Box<dyn Error + Send + Sync>`": every
`std::error::Error` type, plus `&'static str` and `String`. A type that
only implemented `Debug` and `Display` needs one line:

```rust
#[derive(Debug)]
struct ParseFail(&'static str);

impl std::fmt::Display for ParseFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parse failed: {}", self.0)
    }
}

impl std::error::Error for ParseFail {} // new in 2.0
```

In exchange `pipe_io::Error` now implements `source()`, and
`err.downcast_ref::<ParseFail>()` reaches your error.

`pipe_io::source::Infallible` is deprecated; use
`core::convert::Infallible` (it implements `Error`).

### 3. Builder panics became configuration errors

`.batch(...)` with no trigger used to panic while building; zero
window durations hung the run. Now every invalid setting is reported
as `Error::Config(ConfigError)` when the pipeline runs, before any
component is touched, or immediately by `.try_sink(sink)`.

Settings that used to be accepted and are now rejected:

| Setting | 1.x behaviour | 2.0 |
|---------|---------------|-----|
| `BatchPolicy::new()` (no trigger) | panic in `.batch()` | `ConfigErrorKind::NoBatchTrigger` |
| `max_bytes` with `.batch()` | panic | `BytesRequireByteSize` |
| `max_items(0)` | batches of one | `ZeroLimit { field: "max_items" }` |
| `max_bytes(0)`, `max_age(ZERO)` | accepted | rejected |
| Window `size`, `slide`, or `idle` of zero | hang / unbounded memory | `ZeroDuration` |
| Sliding `slide > size` | items between windows silently dropped | `SlideExceedsSize` |
| Sliding `size / slide > 1024` | very slow | `TooManyOverlappingWindows` |
| `ErrorPolicy::DeadLetter` with no `.dead_letter(sink)` | failures silently dropped | `DeadLetterSinkMissing` |

If dropping dead letters was intended, use `ErrorPolicy::Continue`.
Tests that expected a panic (`#[should_panic]`) should assert on the
`ConfigError` instead.

### 4. Behaviour changes to check

- **Flush propagation.** Under `Continue`/`DeadLetter`, a stage whose
  `flush` fails no longer stops the flush: later stages and the sink
  are flushed. (In 1.x their buffered data was silently lost.)
- **Teardown on errors.** On any error, cancellation, or panic, the
  sink and dead-letter sink are now flushed and closed and the source
  is closed, each exactly once and in a fixed order (sink, then source,
  then dead-letter sink) under every driver. A sink that assumed `flush`
  only runs on success should check that assumption. A failing `flush`
  is followed by `close`, not by a second `flush`.
- **Windows.** Empty windows are never emitted (1.x emitted one per
  elapsed interval after a gap). A tumbling window flushed at end of
  stream reports its nominal end (`start + size`) instead of the flush
  time. Sliding windows now place the first item in every window that
  covers it, so the first emitted window can start before the first
  item.
- **Threaded driver.** `run_threaded` now runs one thread per segment
  between `.buffer(cap)` edges (a pipeline without edges still uses one
  worker). A panic in a worker is re-raised with its payload; 1.x
  returned `Error::Cancelled`. The source is closed only after the sink
  has been flushed and closed, so a source may commit offsets in
  `close`.
- **`ReaderSource`** rejects lines longer than 1 MiB by default and its
  error type is now `ReaderError` instead of `io::Error`. Raise the
  limit with `.max_line_len(n)`, or choose `LineOverflow::Skip` /
  `Truncate`.
- **`WriterSink::close`** now flushes the writer.
- **`Error` Debug output** prints the inner error's `Debug` (1.x printed
  its `Display`).

### 5. Type changes

- `RunStats` is `#[non_exhaustive]` and gained `items_out`, `errors`,
  `dead_lettered`, and `retries`; `duration` now exists without `std`
  too (always zero there). Read fields as before; build one with
  `RunStats::default()` if needed.
- `ErrorPolicy`, `WindowPolicy`, `EmitError`, `ChannelSinkError` are now
  `#[non_exhaustive]`; add a wildcard arm where you match them.
- Removed: `EmitError::WouldBlock`, `Error::Buffer`, `BufferErrorKind`
  (never produced). `Error::Closed` now means "the pipeline already
  ran". New variants: `Error::Config`, `Error::Driver`.
- The builder's third type parameter is now bounded by
  `pipe_io::Assemble<T, S::Item>`. Helper functions that pass builders
  around can name it: `impl Assemble<U, S::Item>`. The trait is sealed:
  name it in bounds and return types, but do not implement or call it.
- `ByteSize` for `&str` and `&[u8]` is now provided by blanket impls
  for `&T`, `Box<T>`, `Rc<T>`, `Arc<T>`, and `Cow<T>`; existing uses keep
  working.
- `Stage` gained a `tick` method and `Source` a `pull_timeout` method,
  both with default implementations.
- `StageError` is no longer usable as `dyn StageError` (it now has an
  `Into` supertrait); `BoxError` is `Box<dyn Error + Send + Sync>`.
- `window` and `window_with` take `impl Into<WindowSpec>`; calls that
  spelled out generic parameters with a turbofish need updating.
- `PipelineBuilder` is no longer `Sync` or `UnwindSafe` (it may hold a
  boxed dead-letter sink). Builders are consumed by value, so this only
  matters for code that wrapped one in `catch_unwind`; use
  `AssertUnwindSafe`.

### 6. New, optional

`.buffer(cap)` edges, `Pipeline::step`/`tick`/`finish`/`stats`,
`cancel_handle`, `.observe(...)`, `try_map_ref`, `try_map_retry`,
`window_by_key`, `WindowSpec` limits, `idle_tick`, `batch_with`,
`ManualClock`, `TeeSink`, `ChainSource`, `ByteLinesSource`, and
`.source_id(...)`. See the [user guide](GUIDE.md).

---

## v0.9.0 -> v1.0.0

**Stable release.** The public surface is frozen.

**Source changes:** none required. Existing code compiles
identically.

**What landed:**

- API freeze. The locked `REPS.md` section 4 surface is now
  binding under SemVer. Patch releases (`1.0.x`) ship bug fixes
  only; minor releases (`1.x.0`) are purely additive.
- `cargo-semver-checks` CI job promoted from advisory to a
  **hard gate**. Pull requests that introduce a breaking change
  to the public surface fail CI.
- README rewritten with feature highlights, installation,
  feature-flag table, quick-start snippets per surface
  (basic / batching / windowing / dead-letter / threaded /
  custom driver), and documentation links.
- `REPS.md` section 7 (performance contract) and section 8
  (stability) updated with the locked rules and measured
  numbers. Section 4.9 drops the unimplemented `.buffer(...)`
  reference (rescheduled as `1.x.0` roadmap material).
- `ROADMAP.md` re-anchored: shipped releases listed,
  forward-looking work explicitly marked as post-1.0 minor work.

**Stability rules that take effect now (from `REPS.md` section 8):**

- **Patch (`1.0.x`)** - bug fixes, doc improvements, internal
  performance work, test additions. No new public items.
- **Minor (`1.x.0`)** - pure additions to the public surface,
  new opt-in features, new variants on `#[non_exhaustive]` enums
  reserved for growth, MSRV bumps.
- **Major (`2.0.0`)** - removes, renames, or signature changes
  of public symbols, or non-opt-in runtime dependency additions.

**`#[non_exhaustive]` reminder:** `Error` and `StageFailure` are
non-exhaustive. Match arms on them must include a wildcard:

```rust
match err {
    Error::Source { stage, .. } => /* ... */,
    Error::Stage  { stage, .. } => /* ... */,
    Error::Sink   { stage, .. } => /* ... */,
    _ => /* required - the enum may gain variants in 1.x.0 */,
}
```

**No new public items, no removed items, no renamed items.** The
upgrade is a pure SemVer marker.

---

## v0.8.0 -> v0.9.0

Pre-1.0 stabilization release. No public API changes.

**Source changes:** none required. Existing code compiles
identically.

**What landed:**

- Property tests (`tests/property.rs`, gated on `feature = "std"`).
  These run under `cargo test --all-features` and verify
  invariants across batching, windowing, error policies, and
  driver behaviour with `proptest`.
- Fuzz harnesses under `fuzz/` (excluded from the main workspace).
  Four targets for batching, byte-batching, `Continue` policy,
  and tumbling windows. Require `cargo +nightly fuzz run <target>`.
- CI gains a `semver-checks` job that runs
  `cargo-semver-checks check-release` advisorily. Will be
  promoted to a blocking gate at `v1.0.0`.
- Documentation audit: `docs/MIGRATION.md` (this file) added.

**`proptest` dev-dependency** is new. It is dev-only and does not
affect the published crate or downstream consumers.

---

## v0.7.0 -> v0.8.0

Bridge release. No source changes; documentation and examples
expansion.

**Source changes:** none required.

**What landed:**

- 8 runnable examples under `examples/`.
- `docs/GUIDE.md` user guide.
- `README.md` Quick start + Documentation section.

---

## v0.6.0 -> v0.7.0

Adds the `Driver` trait and `Pipeline::run_with`.

**Source changes:** none required. The trait and `run_with` are
pure additions.

**To start using a custom driver:**

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
        // ... pre/post hooks ...
        SyncDriver::new().run(pipeline)
    }
}

pipeline.run_with(MyDriver)?;
```

**Bound to be aware of:** the `Driver` trait requires `Send` on
the source. For non-`Send` sources, call `SyncDriver::new().run(p)`
directly (inherent method, looser bound). Both built-in drivers
satisfy the trait without changes.

---

## v0.5.0 -> v0.6.0

Wires up `ErrorPolicy::DeadLetter`.

**Source changes:** none required.

**Behavioural change worth noting:**

Before `0.6.0`, `ErrorPolicy::DeadLetter` silently dropped failing
items (it behaved identically to `Continue`). From `0.6.0` onward,
if a dead-letter sink is installed via
`PipelineBuilder::dead_letter(sink)`, failures route to it. If
no sink is installed, the policy still drops silently.

**If you were relying on the silent-drop behaviour:**

- Don't call `.dead_letter(...)` - the silent-drop path is still
  there as the no-sink fallback.
- Or switch to `ErrorPolicy::Continue` explicitly to make the
  intent clear.

**To start collecting failures:**

```rust
use pipe_io::{ErrorPolicy, StageFailure, sink::VecSink};

let dlq = VecSink::<StageFailure>::new();
let dlq_handle = dlq.handle();

pipeline
    .on_error(ErrorPolicy::DeadLetter)
    .try_map(parse)
    .dead_letter(dlq)
    .sink(main_sink)
    .run()?;

for failure in dlq_handle.take() {
    log::warn!("dropped at `{}`: {}", failure.stage, failure.source);
}
```

---

## v0.4.0 -> v0.5.0

Adds the `window` module.

**Source changes:** none required.

**To start using windows:**

```rust
use core::time::Duration;
use pipe_io::{WindowPolicy, Window};

pipeline
    .window(WindowPolicy::Tumbling { size: Duration::from_secs(60) })
    .map(|w: Window<u64>| w.into_inner())
    .sink(writer)
    .run()?;
```

**Constraint to know:** `.window()` and `.window_with()` both
require `T: Clone`. Sliding windows duplicate items across
overlapping windows, and a unified internal stage type made the
`Clone` bound apply uniformly. For non-`Clone` types and tumbling-
only semantics, `.batch(BatchPolicy::new().max_age(...))` is the
substitute.

**No background timer.** Tumbling/Session windows close on the
*next item arriving* after the threshold elapses, or at
end-of-stream. An idle pipeline holding a session window will not
close it until something else happens.

---

## v0.3.0 -> v0.4.0

Polish and benchmarking. No public API changes.

**Source changes:** none required.

**What landed:**

- `benches/pipeline.rs` (hand-rolled, no Criterion).
- `docs/BENCH.md` with measured numbers.
- Performance section in `docs/API.md`.

To reproduce on your hardware: `cargo bench --bench pipeline`.

---

## v0.1.0 -> v0.3.0

The `v0.1.0` release was a scaffold with only `pipe_io::VERSION`
as a public item. `v0.3.0` shipped the entire pipeline surface
(traits, adapters, builder, drivers, batching). There is no
migration path because there was nothing to rename.

A minimal pipeline:

```rust
use pipe_io::{Pipeline, sink::VecSink};

let sink = VecSink::<i64>::new();
let handle = sink.handle();

Pipeline::from_iter(1..=5)
    .map(|n: i32| i64::from(n) * 10)
    .filter(|n: &i64| *n > 20)
    .sink(sink)
    .run()
    .unwrap();

assert_eq!(handle.take(), vec![30, 40, 50]);
```

See [`GUIDE.md`](GUIDE.md) for the full walkthrough and
[`examples/`](../examples/) for runnable code.

`v0.2.0` was a docs-only design-lock pass that landed on `main`
without its own SemVer release. Its content is folded into the
`v0.3.0` CHANGELOG entry.

---

## Stability commitment at v1.0.0

From `1.0.0` forward (see `REPS.md` section 8 for the binding
policy):

- **Patch (`1.0.x`)** - bug fixes, doc improvements, internal
  performance work, test additions. No new public items.
- **Minor (`1.x.0`)** - pure additions to the public surface,
  new opt-in features, new variants on enums reserved for growth,
  MSRV bumps.
- **Major (`2.0.0`)** - anything that removes, renames, or
  changes the signature of a public symbol, or that adds a
  non-opt-in runtime dependency.

CI runs `cargo-semver-checks` against the published baseline
on every pull request and every push to `main`. The job is a
**hard gate** from `1.0.0` onward (advisory under `0.9.x`).
Breaking changes to the public surface fail CI before they can
ship.

The same rules apply to the `2.x` line, with `3.0.0` as the next
major version. This file is updated on every release that ships
behavioural or surface changes worth calling out.
