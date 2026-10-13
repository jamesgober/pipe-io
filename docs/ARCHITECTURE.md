# pipe-io - Architecture

How the crate is put together, why, and what it guarantees. Read this
before changing anything under `src/`. User-facing material is in
[`GUIDE.md`](GUIDE.md); the public surface is in [`API.md`](API.md).

## Contents

- [Module map](#module-map)
- [Recording versus running](#recording-versus-running)
- [The stage chain](#the-stage-chain)
- [Lifecycle and teardown](#lifecycle-and-teardown)
- [Threaded execution](#threaded-execution)
- [Where memory can accumulate](#where-memory-can-accumulate)
- [Time](#time)
- [Errors and configuration](#errors-and-configuration)
- [Performance model](#performance-model)
- [`no_std`](#no_std)
- [Decisions and rejected alternatives](#decisions-and-rejected-alternatives)

## Module map

| Module            | Responsibility                                                                 |
|-------------------|--------------------------------------------------------------------------------|
| `source`          | `Source` trait, `Pull`, built-in sources (iterator, closure, channel, readers, chain). |
| `stage`           | `Stage` trait (`process`, `tick`, `flush`).                                    |
| `emit`            | `Emit` handle given to stages.                                                 |
| `sink`            | `Sink` trait and built-in sinks (null, closure, tee, vec, channel, writer).    |
| `batch`           | `BatchPolicy`, `Batch`, `ByteSize`, and the batching stage.                    |
| `window` (std)    | `WindowPolicy`, `WindowSpec`, `Window`, unkeyed and keyed window stages.       |
| `clock` (std)     | `Clock`, `SystemClock`, `ManualClock`.                                         |
| `retry` (std)     | `RetryPolicy`, `Backoff`, jitter.                                              |
| `error`           | `Error`, `ConfigError`, `ErrorPolicy`, `StageFailure`, `StageError`.           |
| `observe`         | `Observer`, `Event`.                                                           |
| `driver`          | `Driver` trait, `SyncDriver`, `ThreadedDriver` (std), `RunStats`.              |
| `pipeline`        | `PipelineBuilder`, `Pipeline`, the single-threaded runner, cancellation.       |
| `threaded` (std)  | Segment threads and channel edges for `ThreadedDriver`.                        |
| `runtime`         | Hidden plumbing shared by the above: `StageOp`, `Ctx`, error routing.          |

## Recording versus running

The builder does not wire anything together. Each builder method
records a closure of the form "given the next stage, produce this
stage". The records nest into one value (the `Acc` type parameter,
named through the `Assemble` trait) that is boxed when `.sink()` is
called.

Assembly happens when a driver starts the run. The recorded closures are
called from the sink backwards with a `Wiring` value that tells each
`buffer` edge how to materialise:

- single-threaded (`run`, `step`): the edge returns the downstream stage
  unchanged, so it costs nothing;
- threaded (`run_threaded`): the edge creates a `sync_channel`, packages
  the downstream part of the chain as a worker segment, and returns a
  closure that sends into the channel.

Late assembly is what lets one built `Pipeline` be run synchronously,
stepped by an external executor, or split across threads, with no
change to the builder API.

Validation happens while recording. A builder method that receives an
invalid setting records a `ConfigError` (the first one wins) instead of
panicking; every way of running the pipeline then returns
`Error::Config` before any component is touched. `try_sink` surfaces the
error at build time.

## The stage chain

At run time every stage is a `Box<dyn FnMut(StageOp<T>, &mut Ctx) ->
Result<(), Halt>>` that owns the next stage's box. The driver pulls an
item, calls the first box with `StageOp::Process(item)`, and the item
travels down the chain by direct calls until it reaches the sink or is
dropped by a filter. Cost per stage per item: one indirect call.

Control signals travel the same path:

| Signal  | Stages                                   | Sink                         |
|---------|------------------------------------------|------------------------------|
| `Tick`  | time-based stages emit what has expired, then forward | ignored        |
| `Flush` | emit buffered output, then forward       | `flush()`                    |
| `Close` | forward                                  | `close()`                    |
| `Abort` | forward without emitting                 | `flush()` then `close()`, best effort |

`Ctx` is created per run (per thread under the threaded driver) and is
passed by `&mut` through every call. It holds the run counters, the
dead-letter sink, and the observer. Passing it down instead of sharing
it means counting is a plain add on a register-adjacent field: no
atomics or locks on the hot path.

User `Stage` implementations receive an `Emit` adapter that forwards
straight into the next box. If the downstream fails, the adapter records
why and returns `EmitError::Closed`; the driver reports the recorded
error even if the stage ignored `Closed`.

## Lifecycle and teardown

The guarantee: once a run has started, the sink and the dead-letter sink
are flushed and closed, and the source is closed, on every exit path.

| How the run ends            | Chain receives        | Then                                       | Result                 |
|-----------------------------|-----------------------|--------------------------------------------|------------------------|
| Source exhausted            | `Flush`, then `Close` | `source.close()`, dead-letter flush/close  | `Ok(stats)`            |
| `finish()` called early     | `Flush`, then `Close` | same                                       | `Ok(stats)`            |
| Cancelled                   | `Flush`, then `Close` | same                                       | `Err(Cancelled)`       |
| Error (any component)       | `Abort`               | same                                       | `Err(first error)`     |
| `Flush` fails               | `Abort`               | same                                       | `Err(first error)`     |
| Panic in user code          | `Abort` (from `Drop`) | same                                       | the panic resumes      |
| Stepped pipeline dropped    | `Abort` (from `Drop`) | same                                       | errors go to observer  |

The first error is the one returned. Errors raised during teardown
after that are reported to the observer as `Event::TeardownError`;
install an observer to log them. Without one they are dropped, because
a run can only return one error.

`Flush` is the subtle one. A stage's `flush` error is handled by the
stage's error policy; under `Continue` and `DeadLetter` it is recorded
and the flush is still forwarded, so later stages (an open batch, an
open window) and the sink are flushed. Under `FailFast` the run fails
and the chain is aborted: stage buffers are discarded by design, but the
sink still flushes what it already accepted.

Panics are handled by RAII. The object that owns the source and the
chain (`Runner`, and each threaded `ChannelSegment`) tears the run down
in `Drop` if it was not finished. Teardown calls user code (`flush`,
`close`) while a panic is already unwinding; with `std`, each of those
calls runs under `catch_unwind` so a second panic cannot escalate into
an abort. Without `std` there is no `catch_unwind`; a panicking `close`
during unwinding aborts, which is also what most `no_std` targets do on
any panic.

## Threaded execution

`ThreadedDriver` runs one thread per segment: the source plus the stages
before the first `buffer`, and then the part after each `buffer`. Each
edge is a `std::sync::mpsc::sync_channel(capacity)` carrying `StageOp`s.

- **Ordering.** Each edge has one producer and one consumer, so items
  stay in source order end to end.
- **Backpressure.** `send` blocks while an edge is full, so a slow sink
  slows the segment feeding it, and so on back to the source. A tick is
  sent with `try_send` and dropped if the edge is full (the receiving
  thread is busy and ticks itself).
- **Errors.** The failing thread records its error in a shared
  first-error slot, drops its receiver (which wakes a blocked upstream
  sender with an error), and aborts its downstream. Upstream threads
  see their edge disconnect, stop pulling, abort, and the source thread
  closes the source. The driver returns the first error recorded.
- **Panics.** A panicking segment tears itself down from `Drop` and
  unwinds; its neighbours see disconnected edges and shut down as for an
  error. After joining every thread the driver re-raises the first panic
  payload on the caller's thread.
- **Cancellation.** The source thread checks the flag between pulls and
  then ends gracefully (`Flush`, `Close`), so the downstream threads
  drain normally. The driver returns `Err(Cancelled)` unless the drain
  itself failed.
- **Dead letters.** The dead-letter sink is shared by all segments
  behind a `Mutex`. A panic inside it poisons the lock; the lock is
  recovered (`PoisonError::into_inner`) so the final flush and close
  still run. It is flushed and closed once, by the driver, after every
  thread has stopped.
- **Spawn failure.** Segments are spawned sink-first. If the OS refuses
  a thread, the segments not yet started and the source are dropped,
  which tears them down, the running ones see their edges close, and the
  driver returns `Error::Driver`.

## Where memory can accumulate

| Place                    | Bound                                                                 |
|--------------------------|-----------------------------------------------------------------------|
| Between stages (sync)    | None needed: one item in flight.                                      |
| `buffer(cap)` edge       | `cap` items plus one in each adjacent thread.                         |
| Open batch               | `max_items`; `max_bytes` plus the item that crossed it; with only `max_age`, the arrivals within that age. |
| Open window              | `WindowSpec::max_items` per window (default unbounded).               |
| Sliding windows          | At most `ceil(size / slide)` open windows, capped at 1024 by validation. |
| Keyed windows            | `WindowSpec::max_keys` keys (default unbounded); least recently active key is evicted. |
| `ReaderSource` line      | `max_line_len` bytes (1 MiB default).                                 |
| Dead-letter sink         | Whatever the sink buffers.                                            |

## Time

All time-based stages read a `Clock`. `SystemClock` wraps
`Instant::now`; `ManualClock` is driven explicitly in tests.

Windows use a time grid anchored at the first item. Tumbling windows
are `[anchor + k*size, anchor + (k+1)*size)`. A new item computes its
window index arithmetically, so an idle gap of any length costs O(1)
and emits nothing for the empty intervals. Sliding windows anchor the
grid `ceil(size/slide) - 1` slides before the first item so the first
item joins every window covering its arrival, like every later item.
All instant arithmetic is checked: a window whose end is not
representable never closes by time and reports the flush instant as its
end.

Quiet streams need the idle tick. With `idle_tick(interval)` the driver
calls `Source::pull_timeout(interval)` instead of `pull`; on timeout it
sends `Tick` down the chain. Under the threaded driver each segment
also ticks itself after `interval` without input.

## Errors and configuration

User errors only need `StageError`: `Into<BoxError> + Debug + Send +
'static`, where `BoxError = Box<dyn core::error::Error + Send + Sync>`.
That covers every error type plus `&'static str` and `String`. Errors
are boxed once, when they leave user code, so `Error::source`,
`Error::inner`, and `Error::downcast_ref` reach the original value. The
one exception is the hot `Continue` path without an observer: the error
is counted and dropped without being boxed.

`ConfigError` names the stage and the problem (`ConfigErrorKind`). The
checks are pure functions on the policy types (`validate`), reused by
the builder.

## Performance model

- One indirect call per stage per item; no allocation per item in the
  pipeline machinery (batches and windows allocate their own `Vec`s).
- Counters are plain integers in `Ctx`; the pull loop keeps the
  item-in counter in a register and only checks the cancel flag when a
  `CancelHandle` exists.
- The error path under `Continue` is a counter increment when no
  observer is installed.
- A `buffer` edge costs one channel hand-off per item (tens of
  nanoseconds with `std::sync::mpsc`). Put edges around stages whose
  work dominates that, or batch before the edge so whole batches cross
  it.

Measured numbers: [`BENCH.md`](BENCH.md).

## `no_std`

Without `std` the crate needs `alloc`. Everything that does not need
threads, a clock, or I/O is available: sources, stages, sinks, count and
byte batching, error policies including dead-letter routing, the
observer, stepping, and `SyncDriver`.

Targets without pointer-sized atomics (for example `thumbv6m`) have no
`alloc::sync::Arc`. There, `CancelHandle` and `Pipeline::cancel_handle`
are not available and the observer is held in a `Box`; everything else
works. CI builds both `thumbv7em-none-eabihf` and `thumbv6m-none-eabi`.

## Decisions and rejected alternatives

- **Synchronous core, explicit threads.** An async runtime inside the
  crate would force a dependency and an executor model on every user.
  Instead `Pipeline::step` lets any executor drive a pipeline;
  `examples/tokio_driver.rs` and `examples/rayon_driver.rs` show how.
- **Explicit `buffer` edges instead of a thread per stage.** Most
  stages are far cheaper than a cross-thread hand-off; splitting every
  stage would make typical pipelines slower. The user decides where the
  expensive boundaries are.
- **`std::sync::mpsc`, not crossbeam.** Zero runtime dependencies is a
  project rule. The std channel is fast enough when edges sit around
  real work.
- **No `WouldBlock`.** Edges block when full; a non-blocking emit would
  push retry logic into every stage. `EmitError::Closed` is the only
  emit error.
- **Late validation through `ConfigError`, not `Result`-returning
  builder methods.** Keeping each builder step infallible keeps call
  chains readable; `try_sink` gives callers a single place to check.
- **Event-time windows with watermarks** were considered and deferred:
  they need an `EventTime` contract on items and lateness handling that
  deserve their own design. Processing-time windows with a pluggable
  clock cover the in-process use cases this crate targets.
