# pipe-io - User Guide

> A walkthrough of the common pipeline patterns. Pair it with
> [`API.md`](API.md) for the reference, [`ARCHITECTURE.md`](ARCHITECTURE.md)
> for how things work inside, and the [examples](../examples/) for
> runnable programs. Every Rust snippet below is compiled and run by
> `cargo test --doc`.

## Contents

- [Mental model](#mental-model)
- [Your first pipeline](#your-first-pipeline)
- [Closure adapters](#closure-adapters)
- [Custom stages](#custom-stages)
- [Batching](#batching)
- [Windowing](#windowing)
- [Quiet streams and the idle tick](#quiet-streams-and-the-idle-tick)
- [Error policies](#error-policies)
- [Dead-letter routing](#dead-letter-routing)
- [Retries](#retries)
- [Configuration errors](#configuration-errors)
- [Running a pipeline](#running-a-pipeline)
- [Threads and bounded edges](#threads-and-bounded-edges)
- [Stepping and external executors](#stepping-and-external-executors)
- [Cancellation](#cancellation)
- [Observability](#observability)
- [Custom sources and sinks](#custom-sources-and-sinks)
- [Untrusted input](#untrusted-input)
- [Pitfalls](#pitfalls)

## Mental model

```text
Source --pull--> Stage --emit--> Stage --emit--> Sink
```

- A **`Source`** produces items one at a time.
- A **`Stage`** consumes one input and emits zero or more outputs. The
  closure adapters (`map`, `filter`, ...) are stages; `.stage(...)` plugs
  in your own.
- A **`Sink`** consumes the results.

The builder is typed: each step changes the item type, and a mismatch is
a compile error. Nothing runs until you call a run method.

## Your first pipeline

```rust
use pipe_io::{Pipeline, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();

    let stats = Pipeline::from_iter(1..=5)
        .map(|n: i32| i64::from(n) * 10)
        .filter(|n: &i64| *n > 20)
        .sink(sink)
        .run()?;

    assert_eq!(out.take(), vec![30, 40, 50]);
    assert_eq!((stats.items_in, stats.items_out), (5, 3));
    Ok(())
}
```

- `Pipeline::from_iter` wraps anything iterable; `Pipeline::from_source`
  takes any `Source`.
- `.sink(...)` ends the builder and returns a `Pipeline`.
- `.run()` runs it on the calling thread and returns `RunStats`.
- `VecSink` keeps its items behind a cloneable handle, so the sink can be
  moved into the pipeline and the results read afterwards.

## Closure adapters

| Adapter          | Closure                                    | Output |
|------------------|--------------------------------------------|--------|
| `.map`           | `FnMut(T) -> U`                            | `U`    |
| `.filter`        | `FnMut(&T) -> bool`                        | `T`    |
| `.filter_map`    | `FnMut(T) -> Option<U>`                    | `U`    |
| `.flat_map`      | `FnMut(T) -> impl IntoIterator<Item = U>`  | `U`    |
| `.inspect`       | `FnMut(&T)`                                | `T`    |
| `.try_map`       | `FnMut(T) -> Result<U, E>`                 | `U`    |
| `.try_map_ref`   | `FnMut(&T) -> Result<U, E>`                | `U`    |
| `.try_map_retry` | `RetryPolicy`, `FnMut(&T) -> Result<U, E>` | `U`    |

The `try_*` adapters and custom stages follow the active
[error policy](#error-policies). `E` can be any error type, `&'static
str`, or `String`.

## Custom stages

Implement `Stage` for stateful transforms or 1:N emission:

```rust
use pipe_io::{Emit, Pipeline, Stage, sink::VecSink};

struct Words;

impl Stage for Words {
    type Input = String;
    type Output = String;
    type Error = core::convert::Infallible;

    fn process(
        &mut self,
        line: String,
        out: &mut dyn Emit<Item = String>,
    ) -> Result<(), Self::Error> {
        for word in line.split_whitespace() {
            // `Closed` means a later stage failed; the run reports it.
            if out.emit(word.to_owned()).is_err() {
                break;
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter(vec!["a b".to_owned(), "c".to_owned()])
        .stage(Words)
        .sink(sink)
        .run()?;
    assert_eq!(out.take(), vec!["a", "b", "c"]);
    Ok(())
}
```

`Stage` also has `flush` (emit buffered output at end of stream) and
`tick` (emit what has expired on a quiet stream), both defaulting to
no-ops. Errors from all three follow the stage's error policy; under
`Continue` and `DeadLetter` a failed `flush` still reaches every later
stage and the sink.

## Batching

```rust
use std::time::Duration;
use pipe_io::{Batch, BatchPolicy, Pipeline, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter(1_u32..=11)
        .batch(
            BatchPolicy::new()
                .max_items(4)
                .max_age(Duration::from_secs(1)),
        )
        .map(|b: Batch<u32>| b.len())
        .sink(sink)
        .run()?;
    assert_eq!(out.take(), vec![4, 4, 3]);
    Ok(())
}
```

Triggers OR together:

- `max_items(n)`: emit at `n` items.
- `max_bytes(n)`: emit when the summed `ByteSize` reaches `n`; needs
  `.batch_bytes(...)` and an item type implementing `ByteSize`.
- `max_age(d)` (std): emit once the first item is `d` old. Checked when
  an item arrives and, with the [idle tick](#quiet-streams-and-the-idle-tick),
  while the stream is quiet.

Every limit must be non-zero and at least one trigger must be set;
otherwise the run reports a [configuration error](#configuration-errors).
A large `max_items` does not reserve memory up front.

## Windowing

```rust
use std::time::{Duration, Instant};
use pipe_io::{ManualClock, Pipeline, Window, WindowPolicy, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let clock = ManualClock::new(Instant::now());
    let sink = VecSink::new();
    let out = sink.handle();
    let mut pipeline = Pipeline::from_iter([5_u64, 7, 3])
        .window_with(WindowPolicy::tumbling(Duration::from_secs(10)), clock.clone())
        .map(|w: Window<u64>| w.iter().sum::<u64>())
        .sink(sink);

    pipeline.step()?; // 5 at t=0
    pipeline.step()?; // 7 at t=0
    clock.advance(Duration::from_secs(12));
    pipeline.step()?; // 3 at t=12 closes [0,10)
    assert_eq!(out.take(), vec![12]);
    pipeline.finish()?; // flushes [10,20)
    assert_eq!(out.take(), vec![3]);
    Ok(())
}
```

Policies:

- `WindowPolicy::tumbling(size)`: fixed, non-overlapping windows.
- `WindowPolicy::sliding(size, slide)`: overlapping windows; every item
  joins `ceil(size / slide)` of them (`T: Clone`). `slide` must not
  exceed `size`.
- `WindowPolicy::session(idle)`: closes after `idle` without items.

Only non-empty windows are emitted, and a long quiet gap costs nothing:
window boundaries skip forward arithmetically.

Bound memory with `WindowSpec`: `.max_items(n)` emits a window as soon
as it holds `n` items (the interval continues in a new window).

Per-key windows use `.window_by_key(key_fn, spec)` and emit `(key,
window)`. Every item, of any key, first closes the expired windows of
all keys (in order of their end), so a key that goes quiet still gets
its window emitted on time while other keys keep the stream busy. Bound
the number of tracked keys with `.max_keys(n)`; at the limit the least
recently active key is flushed and evicted. Eviction is O(1), so keys
from untrusted input (IDs, IP addresses) cost the same per item whatever
the limit. Without `.max_keys` every key with an open window is kept,
so set a limit for unbounded key spaces.

Use `.window(policy)` in production (system clock) and
`.window_with(policy, clock)` with a `ManualClock` in tests.

## Quiet streams and the idle tick

Time-based stages normally act when an item arrives. On a live stream
that goes quiet, enable the idle tick so they act on time alone:

```rust
use std::sync::mpsc;
use std::time::Duration;
use pipe_io::{Batch, BatchPolicy, Pipeline, Step, source::ChannelSource, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let (tx, rx) = mpsc::channel();
    let sink = VecSink::new();
    let out = sink.handle();
    let mut pipeline = Pipeline::from_source(ChannelSource::new(rx))
        .idle_tick(Duration::from_millis(5))
        .batch(BatchPolicy::new().max_items(100).max_age(Duration::from_millis(20)))
        .map(|b: Batch<u32>| b.into_inner())
        .sink(sink);

    tx.send(1).expect("receiver is alive");
    assert_eq!(pipeline.step()?, Step::Item);
    while out.is_empty() {
        // No more input: ticks fire until the batch is old enough.
        assert_eq!(pipeline.step()?, Step::Idle);
    }
    assert_eq!(out.take(), vec![vec![1]]);
    drop(tx);
    assert_eq!(pipeline.step()?, Step::Done);
    Ok(())
}
```

The idle tick needs a source that can wait with a timeout
(`Source::pull_timeout`). `ChannelSource` can; a source using the default
implementation blocks in `pull` and never reports idleness. Executors
with their own timers can call `Pipeline::tick()` directly.

## Error policies

```rust
use pipe_io::{ErrorPolicy, Pipeline, sink::NullSink};

fn main() -> Result<(), pipe_io::Error> {
    let stats = Pipeline::from_iter(["1", "x", "300"])
        .on_error(ErrorPolicy::Continue) // applies to the stages after it
        .try_map(|s: &str| s.parse::<u32>())
        .on_error(ErrorPolicy::FailFast)
        .try_map(|n: u32| if n < 1_000 { Ok(n) } else { Err("too large") })
        .sink(NullSink::new())
        .run()?;
    assert_eq!((stats.items_out, stats.errors), (2, 1));
    Ok(())
}
```

- **`FailFast`** (default): the first error stops the run and is
  returned. The sink, the dead-letter sink, and the source are still
  flushed and closed.
- **`Continue`**: the item is dropped, the error is counted in
  `RunStats::errors` and reported to the observer.
- **`DeadLetter`**: the failure goes to the dead-letter sink.

Errors keep their identity: `err.stage()` names the failing stage,
`err.downcast_ref::<E>()` and `std::error::Error::source` reach the
original error.

Source errors always end the run (there is no item to skip).

## Dead-letter routing

```rust
use pipe_io::{ErrorPolicy, Pipeline, StageFailure, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let dlq = VecSink::<StageFailure>::new();
    let failures = dlq.handle();
    let ok = VecSink::new();
    let out = ok.handle();

    Pipeline::from_iter(["1", "x", "3"])
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("parse")
        .try_map_ref(|s: &&str| s.parse::<u32>())
        .dead_letter(dlq)
        .sink(ok)
        .run()?;

    assert_eq!(out.take(), vec![1, 3]);
    let mut failed = failures.take();
    assert_eq!(failed[0].stage.as_str(), "parse");
    assert_eq!(failed[0].take_input::<&str>(), Some("x"));
    Ok(())
}
```

- `.dead_letter(sink)` may come before or after the stages that use it.
- A `DeadLetter` stage without a dead-letter sink is a configuration
  error (failures would otherwise vanish).
- `try_map_ref` and `try_map_retry` attach the failing item
  (`StageFailure::take_input`); `try_map` and custom stages cannot, since
  their input was moved into user code.
- The dead-letter sink is flushed and closed after the main sink on every
  exit path. Its own errors are returned as `Error::Sink` with the stage
  id `dead_letter`.

## Retries

```rust
use std::time::Duration;
use pipe_io::{Backoff, ErrorPolicy, Pipeline, RetryPolicy, StageFailure, sink::NullSink};

fn main() -> Result<(), pipe_io::Error> {
    let retry = RetryPolicy::new(3)
        .backoff(Backoff::Exponential {
            initial: Duration::from_millis(1),
            max: Duration::from_millis(10),
        })
        .jitter(true);
    let stats = Pipeline::from_iter([1_u32, 2])
        .on_error(ErrorPolicy::DeadLetter)
        .try_map_retry(retry, |n: &u32| if *n == 2 { Err("down") } else { Ok(*n) })
        .dead_letter(NullSink::<StageFailure>::new())
        .sink(NullSink::new())
        .run()?;
    assert_eq!((stats.retries, stats.dead_lettered), (2, 1));
    Ok(())
}
```

After the last attempt the stage's error policy applies; dead-letter
records carry the item and the attempt count. Backoff sleeps on the
thread running the stage.

## Configuration errors

Builder methods never panic. Invalid settings (zero sizes or limits, a
sliding `slide` larger than `size`, a batch with no trigger, `max_bytes`
without `batch_bytes`, `DeadLetter` without a dead-letter sink, a
`buffer` capacity above 2^20, ...) are recorded and reported before
anything runs:

```rust
use pipe_io::{Batch, BatchPolicy, ConfigErrorKind, Pipeline, sink::NullSink};

fn main() {
    let err = Pipeline::from_iter(0..10)
        .stage_id("grouper")
        .batch(BatchPolicy::new().max_items(0))
        .try_sink(NullSink::<Batch<i32>>::new())
        .unwrap_err();
    assert_eq!(err.stage().as_str(), "grouper");
    assert_eq!(err.kind(), ConfigErrorKind::ZeroLimit { field: "max_items" });
}
```

`.sink(...)` keeps the error and returns it from `run` as
`Error::Config`. The policy types also have `validate()` for checking
configuration loaded at startup.

## Running a pipeline

| Call                      | Runs on                                       |
|---------------------------|-----------------------------------------------|
| `.run()`                  | the calling thread                            |
| `.run_threaded()` (std)   | one thread per segment between `buffer` edges |
| `.run_with(driver)`       | any `Driver` implementation                   |
| `.step()` / `.finish()`   | the caller, one item at a time                |

`SyncDriver::new().run(pipeline)` also accepts a source that is not
`Send`. A pipeline runs once; running it again returns `Error::Closed`.

## Threads and bounded edges

```rust
use pipe_io::{Pipeline, sink::VecSink};

fn main() -> Result<(), pipe_io::Error> {
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_iter(0_u32..1_000)
        .map(|n: u32| n * 2)          // thread 1, with the source
        .buffer(64)                   // bounded edge: at most 64 items in flight
        .filter(|n: &u32| n % 3 == 0) // thread 2
        .buffer(64)
        .sink(sink)                   // thread 3
        .run_threaded()?;
    assert_eq!(out.take().len(), 334);
    Ok(())
}
```

- Order is preserved. A full edge blocks the thread feeding it, which is
  how a slow sink slows the source. An edge allocates memory only for
  the items queued in it, not for its whole capacity; capacities above
  2^20 are rejected as `ConfigErrorKind::LimitTooLarge`.
- `buffer(0)` is a hand-off: the upstream thread waits until the
  downstream thread has taken each item. That costs a thread wake-up per
  item (hundreds of nanoseconds); use a small capacity such as 16 or 64
  unless lock step is what you want.
- On an error the failing thread stops, downstream threads tear down,
  upstream threads stop pulling, and the first error is returned. The
  source thread notices a downstream failure when it next sends, so give
  a source that can block in `pull` (a quiet `ChannelSource`) an
  `idle_tick`.
- The sink is flushed and closed before `Source::close`, as with `run`.
- A panic in any thread is re-raised on the calling thread with its
  original payload after everything is torn down.
- Under `run`/`step` an edge is a direct call (nothing to buffer).
- An edge costs a channel hand-off per item. Put edges around expensive
  stages, or batch before the edge.

## Stepping and external executors

`Pipeline::step` pulls one item and pushes it through; `finish` ends the
run gracefully; `stats` reports counters at any time. This is all an
executor needs:

```rust
use pipe_io::driver::{Driver, RunStats};
use pipe_io::{Pipeline, Result, Source, Step, sink::NullSink};

struct Sliced(usize);

impl Driver for Sliced {
    fn run<S>(self, mut pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        loop {
            for _ in 0..self.0 {
                if pipeline.step()? == Step::Done {
                    return Ok(pipeline.stats());
                }
            }
            // Yield, check a deadline, report progress, ...
        }
    }
}

fn main() -> Result<()> {
    let stats = Pipeline::from_iter(0..10)
        .sink(NullSink::<i32>::new())
        .run_with(Sliced(4))?;
    assert_eq!(stats.items_out, 10);
    Ok(())
}
```

For tokio, run the step loop under `tokio::task::spawn_blocking` (a
pipeline may block, so it must not run on a worker thread directly). For
rayon, run pipelines on the pool with `pool.install`. Complete programs:
[`examples/tokio_driver.rs`](../examples/tokio_driver.rs) and
[`examples/rayon_driver.rs`](../examples/rayon_driver.rs).

Dropping a pipeline in the middle of a stepped run tears it down (the
sink and source are flushed and closed). If user code panics inside
`step` and your executor catches the panic, the item in flight is lost:
the pipeline is poisoned, and the next `step`, `tick`, or `finish` tears
it down and returns `Error::Driver`.

## Cancellation

```rust
use pipe_io::{Error, Pipeline, sink::NullSink};

fn main() {
    let mut pipeline = Pipeline::from_iter(0_u64..).sink(NullSink::new());
    let cancel = pipeline.cancel_handle();
    let worker = std::thread::spawn(move || pipeline.run());
    cancel.cancel();
    let result = worker.join().expect("worker");
    assert!(matches!(result, Err(Error::Cancelled)));
}
```

The run stops at the next item boundary, flushes every stage and the
sink, closes everything, and returns `Error::Cancelled`. A source blocked
in `pull` is not interrupted; use a source with a timeout and the idle
tick so the check runs regularly. A `try_map_retry` stage waiting out a
backoff delay stops waiting (within about 10 ms) when the run is
cancelled.

## Observability

`RunStats` counts items in and out, errors, dead-lettered items, and
retries. For live events, install an observer:

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use pipe_io::{ErrorPolicy, Event, Pipeline, sink::NullSink};

fn main() -> Result<(), pipe_io::Error> {
    let errors = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&errors);
    Pipeline::from_iter(["1", "x"])
        .observe(move |event: &Event<'_>| {
            if let Event::StageError { stage, error, .. } = event {
                counter.fetch_add(1, Ordering::Relaxed);
                eprintln!("stage {stage} failed: {error}");
            }
        })
        .on_error(ErrorPolicy::Continue)
        .try_map(|s: &str| s.parse::<u32>())
        .sink(NullSink::new())
        .run()?;
    assert_eq!(errors.load(Ordering::Relaxed), 1);
    Ok(())
}
```

Events: `Started`, `StageError`, `Retry`, `DeadLettered`, `Idle`,
`TeardownError` (a later error during teardown; the run returns the
first one), and `Finished`. There are no per-item events, so an observer
never sits on the hot path. Bridge them to `log`, `tracing`, or a
metrics registry in the callback.

## Custom sources and sinks

```rust
use pipe_io::{Pipeline, Sink, Source};

struct Countdown(u32);

impl Source for Countdown {
    type Item = u32;
    type Error = core::convert::Infallible;
    fn pull(&mut self) -> Result<Option<u32>, Self::Error> {
        if self.0 == 0 {
            return Ok(None);
        }
        self.0 -= 1;
        Ok(Some(self.0))
    }
}

#[derive(Default)]
struct Sum(u64);

impl Sink for Sum {
    type Item = u32;
    type Error = std::io::Error;
    fn write(&mut self, item: u32) -> Result<(), Self::Error> {
        self.0 += u64::from(item);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        // Persist what was accepted. Called on every exit path.
        Ok(())
    }
}

fn main() -> Result<(), pipe_io::Error> {
    let stats = Pipeline::from_source(Countdown(3)).sink(Sum::default()).run()?;
    assert_eq!(stats.items_out, 3);
    Ok(())
}
```

- `Source::close` and `Sink::flush`/`close` are each called exactly once
  per run on every exit path, including errors and panics (a failing or
  panicking `flush` is not retried, and `close` still runs), so they are
  the right place for cleanup. `Source::close` runs after the sink has
  been flushed and closed, under every driver, so a source can commit
  offsets or acknowledge messages there.
- Override `Source::pull_timeout` if your source can wait with a
  deadline; it enables the idle tick.
- Built-ins: `IterSource`, `FnSource`, `ChainSource`, `ChannelSource`,
  `ReaderSource`, `ByteLinesSource`; `NullSink`, `FnSink`, `TeeSink`,
  `VecSink`, `ChannelSink`, `WriterSink`.

## Untrusted input

`ReaderSource` (lines as `String`) and `ByteLinesSource` (lines as bytes)
never buffer more than `max_line_len` bytes of one line (1 MiB by
default):

```rust
use pipe_io::{Pipeline, sink::VecSink};
use pipe_io::source::{LineOverflow, ReaderSource};

fn main() -> Result<(), pipe_io::Error> {
    let input = "ok\nthis line is far too long\nfine\n";
    let sink = VecSink::new();
    let out = sink.handle();
    Pipeline::from_source(
        ReaderSource::new(input.as_bytes())
            .max_line_len(10)
            .on_overflow(LineOverflow::Skip)
            .lossy(true),
    )
    .sink(sink)
    .run()?;
    assert_eq!(out.take(), vec!["ok", "fine"]);
    Ok(())
}
```

`LineOverflow::Error` (default) fails the run, `Skip` drops the line,
`Truncate` keeps its first `max_line_len` bytes. `lossy(true)` replaces
invalid UTF-8 instead of failing.

## Pitfalls

- **`T: Clone` for windows.** Sliding windows copy items, so all window
  methods require `Clone`. For non-`Clone` items and tumbling semantics,
  batch with `max_age` instead.
- **Time triggers on quiet streams** need the idle tick (and a source
  with `pull_timeout`), or `Pipeline::tick()` from your executor.
- **Edges are not free.** A `buffer` edge pays a channel hand-off per
  item; adding one between two cheap closures makes the pipeline slower.
- **`FailFast` discards stage buffers.** On a failure the open batches
  and windows are dropped; the sink still flushes what it accepted.
- **Source errors are terminal.** Handle recoverable read errors inside
  the source (or use `LineOverflow::Skip` and `lossy` for readers).
- **A pipeline runs once.** Build a new one to run again.
