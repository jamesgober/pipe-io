# pipe-io - User Guide

> A walkthrough of the common pipeline patterns. Pair this with
> [`API.md`](API.md) for the full reference and the [examples
> directory](../examples/) for runnable code.

## Contents

- [Mental model](#mental-model)
- [Building your first pipeline](#building-your-first-pipeline)
- [Closure adapters](#closure-adapters)
- [Implementing custom stages](#implementing-custom-stages)
- [Batching](#batching)
- [Windowing](#windowing)
- [Error policies](#error-policies)
- [Dead-letter routing](#dead-letter-routing)
- [Picking a driver](#picking-a-driver)
- [Implementing a custom driver](#implementing-a-custom-driver)
- [Implementing a custom source](#implementing-a-custom-source)
- [Implementing a custom sink](#implementing-a-custom-sink)
- [Pitfalls](#pitfalls)

## Mental model

Three roles:

```text
Source ---(pull)---> Stage ---(emit)---> Stage ---(emit)---> Sink
```

* A **`Source`** produces items one at a time (`pull`).
* A **`Stage`** consumes one input and emits zero or more outputs
  via an `Emit` handle. Closure adapters (`map`, `filter`,
  `filter_map`, `flat_map`, `inspect`, `try_map`) wrap closures
  into stages; `.stage(impl Stage)` plugs in a hand-written one.
* A **`Sink`** consumes the final output.

The builder is **typed**: the carrier type changes as you chain
stages, and a type mismatch is a compile error.

## Building your first pipeline

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

Runnable: [`examples/basic.rs`](../examples/basic.rs).

Things to notice:

* `Pipeline::from_iter` wraps any `IntoIterator` as a source.
  `Pipeline::from_source(src)` accepts any `Source` impl.
* `.map`, `.filter`, etc. are typed: the closure signatures must
  match the current carrier type.
* `.sink(sink)` terminates the builder and returns a `Pipeline`.
* `.run()` drives it on the calling thread.
* `VecSink` lets the caller hold a clonable `SharedHandle` to the
  collected items, so the sink can be moved into the pipeline and
  the results still read afterward.

## Closure adapters

Every closure-based combinator lives on the builder:

| Adapter      | Closure signature                  | Output      |
|--------------|------------------------------------|-------------|
| `.map`       | `FnMut(T) -> U`                    | `U`         |
| `.filter`    | `FnMut(&T) -> bool`                | `T`         |
| `.filter_map`| `FnMut(T) -> Option<U>`            | `U`         |
| `.flat_map`  | `FnMut(T) -> IntoIterator<Item=U>` | `U`         |
| `.inspect`   | `FnMut(&T)`                        | `T`         |
| `.try_map`   | `FnMut(T) -> Result<U, E>`         | `U`         |

`.try_map` is the only one that honours [`ErrorPolicy`](#error-policies).
The others are infallible by construction.

## Implementing custom stages

When a closure is awkward (state spread across many fields,
multi-output emission with bespoke logic), implement [`Stage`]:

```rust
use pipe_io::{Emit, Stage};

struct WordSplitter;

impl Stage for WordSplitter {
    type Input = String;
    type Output = String;
    type Error = pipe_io::source::Infallible;

    fn process(
        &mut self,
        line: String,
        out: &mut dyn Emit<Item = String>,
    ) -> Result<(), Self::Error> {
        for word in line.split_whitespace() {
            let _ = out.emit(word.to_string());
        }
        Ok(())
    }
}
```

Insert it with `.stage(WordSplitter)`. The carrier type transitions
according to `Stage::Input` and `Stage::Output`. Errors raised by
the stage's `process` or `flush` honour the active `ErrorPolicy`,
identical to `.try_map`.

## Batching

Group items into fixed-size or time-bounded batches before they
reach the sink:

```rust
use pipe_io::{BatchPolicy, Batch, Pipeline, sink::VecSink};

Pipeline::from_iter(1u32..=11)
    .batch(BatchPolicy::new().max_items(4))
    .map(|b: Batch<u32>| b.into_inner())
    .sink(VecSink::<Vec<u32>>::new())
    .run()?;
```

Runnable: [`examples/batching.rs`](../examples/batching.rs).

Triggers OR together:

* `max_items(usize)` - emit when the batch reaches N items.
* `max_age(Duration)` (std) - emit when the oldest item is older
  than D. Needs `Pipeline::run` to see another item or to flush;
  there is no background timer.
* `max_bytes(usize)` + `T: ByteSize` - emit when the byte sum
  reaches N. Use `.batch_bytes(policy)` for this path.

## Windowing

Time-based grouping with three policies:

* `Tumbling { size }` - fixed-width, non-overlapping windows.
* `Sliding { size, slide }` - overlapping windows; items are
  duplicated into every active window (`T: Clone` required).
* `Session { idle }` - adaptive windows that close after `idle`
  without activity.

```rust
use core::time::Duration;
use pipe_io::{Pipeline, Window, WindowPolicy, sink::VecSink};

Pipeline::from_iter(metric_stream)
    .window(WindowPolicy::Tumbling { size: Duration::from_secs(60) })
    .map(|w: Window<u64>| w.into_inner().iter().sum::<u64>())
    .sink(VecSink::<u64>::new())
    .run()?;
```

Runnable: [`examples/windowing.rs`](../examples/windowing.rs).

Windows close on the **next item arriving** after the close
condition fires, or at end-of-stream via `Stage::flush`. The
synchronous core does not run a background timer; an idle session
with no further items closes only at end-of-stream.

For deterministic tests, swap `SystemClock` for a custom `Clock`
via `.window_with(policy, clock)`.

## Error policies

Each fallible stage runs under one of three policies, set with
`.on_error(policy)`:

```rust
use pipe_io::ErrorPolicy;

Pipeline::from_iter(rows)
    .on_error(ErrorPolicy::Continue)    // applies to subsequent stages
    .try_map(parse_row)
    .on_error(ErrorPolicy::FailFast)    // applies from here onward
    .try_map(validate)
    .sink(writer)
    .run()?;
```

* **`FailFast`** (default) - first error bubbles out of `run`. Use
  for invariants that should never fail in normal operation.
* **`Continue`** - drop the failing item, count the error, keep
  going. Use for noisy stages where partial results matter.
* **`DeadLetter`** - route the failure to a dead-letter sink (or
  drop if no sink is installed). Covered next.

## Dead-letter routing

When `Continue` is too quiet and `FailFast` is too aggressive,
route failures to a side-channel sink:

```rust
use pipe_io::{ErrorPolicy, Pipeline, StageFailure, sink::VecSink};

let dlq = VecSink::<StageFailure>::new();
let dlq_handle = dlq.handle();

Pipeline::from_iter(records)
    .stage_id("parse")
    .on_error(ErrorPolicy::DeadLetter)
    .try_map(parse_row)
    .dead_letter(dlq)
    .sink(main_sink)
    .run()?;

for failure in dlq_handle.take() {
    eprintln!("dropped at `{}`: {}", failure.stage, failure.source);
}
```

Runnable: [`examples/dead_letter.rs`](../examples/dead_letter.rs).

Notes:

* `.dead_letter()` can be called **before or after** the failing
  stages. The handle is shared and the install slot is filled
  whenever the call lands.
* If no dead-letter sink is installed, `DeadLetter` falls back to
  `Continue` (silent drop). Document this in your project if
  the sink is meant to be mandatory.
* Errors raised by the dead-letter sink itself surface from `run`
  as `Error::Sink { stage: "dead_letter", .. }`. Don't catch and
  swallow them inside the sink unless that's intentional.

## Picking a driver

| Driver           | Where it runs                | When to use it                                 |
|------------------|------------------------------|------------------------------------------------|
| `SyncDriver`     | Calling thread               | Tests, single-shot CLI, embedded use, no_std   |
| `ThreadedDriver` | One spawned OS thread (std)  | Run alongside an existing event loop          |
| Custom `Driver`  | Whatever you build           | Tokio integration, rayon, sharded farms       |

Equivalent invocations:

```rust
pipeline.run();                                  // sync, default
pipeline.run_threaded();                         // threaded, std only
pipeline.run_with(SyncDriver::new());            // explicit sync
pipeline.run_with(ThreadedDriver::new());        // explicit threaded
pipeline.run_with(MyDriver::new());              // any Driver impl
```

The `Driver` trait carries `Send` bounds on the source. If your
source is not `Send` (e.g. holds an `Rc`), call
`SyncDriver::new().run(pipeline)` directly; the inherent method
has a looser bound and stays on the current thread.

## Implementing a custom driver

Custom drivers plug in any scheduling primitive. The trait is
small:

```rust
use pipe_io::driver::{Driver, RunStats, SyncDriver};
use pipe_io::{Pipeline, Result};

struct MyDriver;

impl Driver for MyDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: pipe_io::Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        // ...pre-run setup, scheduling decisions, etc.
        let result = SyncDriver::new().run(pipeline);
        // ...post-run cleanup or metrics emission.
        result
    }
}
```

Runnable: [`examples/custom_driver.rs`](../examples/custom_driver.rs).

The trait method consumes the pipeline by value, so the driver
takes full ownership of the run. To distribute across multiple
threads, the driver would typically split the pipeline before
running (out of scope for `0.x` releases).

## Implementing a custom source

Anything that produces items can be a source. The trait is two
methods, only `pull` is required:

```rust
use pipe_io::source::Infallible;
use pipe_io::Source;

struct Fibonacci { a: u64, b: u64, limit: u64 }

impl Source for Fibonacci {
    type Item = u64;
    type Error = Infallible;

    fn pull(&mut self) -> Result<Option<u64>, Infallible> {
        if self.a > self.limit { return Ok(None); }
        let cur = self.a;
        let next = self.a + self.b;
        self.a = self.b;
        self.b = next;
        Ok(Some(cur))
    }
}
```

Runnable: [`examples/custom_source.rs`](../examples/custom_source.rs).

Return `Ok(None)` to signal end-of-stream. Errors are wrapped as
`Error::Source { stage, source }` when they bubble out of the
pipeline.

## Implementing a custom sink

Sinks are write-only consumers. Three methods, only `write` is
required:

```rust
use pipe_io::Sink;

struct CountingSink { count: u64 }

impl Sink for CountingSink {
    type Item = u32;
    type Error = pipe_io::source::Infallible;

    fn write(&mut self, _item: u32) -> Result<(), Self::Error> {
        self.count += 1;
        Ok(())
    }
}
```

`flush` and `close` have empty defaults. Override them when the
sink buffers or holds resources that need explicit teardown.

## Pitfalls

A handful of things that catch first-time users:

* **`T: Clone` for windowing.** Sliding windows duplicate items
  across overlapping windows, so `.window()` / `.window_with()`
  require `T: Clone`. For non-`Clone` types and tumbling-only
  semantics, use `.batch(BatchPolicy::new().max_age(...))` instead.
* **No background timer.** Time-based batch/window triggers fire
  on the *next item arriving* after the threshold elapses, or
  at end-of-stream. An idle pipeline with a long timeout will
  not emit until something happens.
* **`Pipeline::run_threaded` runs one worker thread.** Stages
  inside the worker still run sequentially. Per-stage parallelism
  is a future enhancement.
* **`VecSink` is std-only.** `Arc<Mutex<_>>` underneath. For
  no_std, write a custom sink or use `FnSink` with whatever
  shared state your platform supports.
* **Source errors are terminal.** `Source::pull` returning `Err`
  immediately fails the pipeline (no `Continue`/`DeadLetter`
  policy at the source level). Wrap fallible sources in an early
  `.try_map` if you want lossy source error handling.
* **`#![forbid(unsafe_code)]`.** The crate sets this at the root.
  All public types implement `Send` via the compiler's auto-trait
  derivation; the dead-letter handle uses `Arc<Mutex>` only inside
  the std feature.

For anything else, see [`API.md`](API.md) for the surface
reference and the [`REPS.md`](../REPS.md) project specification.
