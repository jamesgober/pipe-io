# pipe-io - API Reference

> Reference for the public API of `pipe-io 2.0`. It mirrors the rustdoc
> on docs.rs, which has the full per-item documentation and examples.
> Items marked **(std)** need the default `std` feature. The surface
> follows Semantic Versioning; see [Stability](#stability).

## Contents

- [Crate root](#crate-root)
- [Builder](#builder)
- [`Pipeline`](#pipeline)
- [`pipe_io::source`](#pipe_iosource)
- [`pipe_io::stage` and `pipe_io::emit`](#pipe_iostage-and-pipe_ioemit)
- [`pipe_io::sink`](#pipe_iosink)
- [`pipe_io::batch`](#pipe_iobatch)
- [`pipe_io::window` (std)](#pipe_iowindow-std)
- [`pipe_io::clock` (std)](#pipe_ioclock-std)
- [`pipe_io::retry` (std)](#pipe_ioretry-std)
- [`pipe_io::error`](#pipe_ioerror)
- [`pipe_io::observe`](#pipe_ioobserve)
- [`pipe_io::driver`](#pipe_iodriver)
- [Feature flags](#feature-flags)
- [MSRV and dependencies](#msrv-and-dependencies)
- [Stability](#stability)
- [Performance](#performance)

## Crate root

Re-exports for everyday use:

| Item | Kind | Notes |
|------|------|-------|
| `VERSION` | const | Crate version string. |
| `Pipeline<S>`, `PipelineBuilder<T, S, Acc>` | struct | See [Builder](#builder) and [`Pipeline`](#pipeline). |
| `Assemble<T, I>` | trait | The builder's opaque type-state; name it in helper functions (`impl Assemble<U, S::Item>`). Not meant to be implemented. |
| `Step` | enum | `Item`, `Idle`, `Done` (non-exhaustive). |
| `CancelHandle` | struct | `cancel()`, `is_cancelled()`. Needs pointer-sized atomics. |
| `Source`, `Pull`, `Stage`, `Emit`, `EmitError`, `Sink` | traits / enums | Component traits. |
| `Batch`, `BatchPolicy`, `ByteSize` | | Batching. |
| `Window`, `WindowPolicy`, `WindowSpec` (std) | | Windowing. |
| `Clock`, `SystemClock`, `ManualClock` (std) | | Time sources. |
| `RetryPolicy`, `Backoff` (std) | | Retries. |
| `Error`, `Result<T, E = Error>`, `BoxError`, `StageError`, `ConfigError`, `ConfigErrorKind`, `ErrorPolicy`, `StageFailure` | | Errors. |
| `Observer`, `Event` | | Observability. |
| `Driver`, `RunStats` | | Execution. |
| `StageId` | struct | `&'static str` newtype naming a stage. |

## Builder

`Pipeline::from_source(source)` and `Pipeline::from_iter(iter)` return a
`PipelineBuilder`. Every method consumes and returns the builder.
Methods never panic: invalid settings are recorded and reported as
`Error::Config` by every run method (or by `try_sink` immediately).

Settings (apply to what follows):

| Method | Effect |
|--------|--------|
| `.stage_id(id)` | Name the next stage (errors, dead-letter records, events). |
| `.source_id(id)` | Name the source (default `"source"`). |
| `.on_error(ErrorPolicy)` | Policy for fallible stages added afterwards. |
| `.dead_letter(sink)` | Install the `Sink<Item = StageFailure>` for `DeadLetter` stages. |
| `.observe(observer)` | Install an `Observer`. |
| `.idle_tick(Duration)` | Enable idle ticks (see `Source::pull_timeout`). |

Transforms (`T` is the current item type):

| Method | Closure / argument | New item type |
|--------|-------------------|---------------|
| `.map(f)` | `FnMut(T) -> U` | `U` |
| `.filter(p)` | `FnMut(&T) -> bool` | `T` |
| `.filter_map(f)` | `FnMut(T) -> Option<U>` | `U` |
| `.flat_map(f)` | `FnMut(T) -> I`, `I: IntoIterator<Item = U>` | `U` |
| `.inspect(f)` | `FnMut(&T)` | `T` |
| `.try_map(f)` | `FnMut(T) -> Result<U, E>` | `U` |
| `.try_map_ref(f)` | `FnMut(&T) -> Result<U, E>`, `T: Sync`; failing input kept for dead letters | `U` |
| `.try_map_retry(policy, f)` (std) | `RetryPolicy`, `FnMut(&T) -> Result<U, E>`, `T: Sync` | `U` |
| `.stage(stage)` | `impl Stage<Input = T>` | `Stage::Output` |
| `.batch(policy)` | `BatchPolicy` (count, age) | `Batch<T>` |
| `.batch_bytes(policy)` | `BatchPolicy` (count, bytes, age), `T: ByteSize` | `Batch<T>` |
| `.batch_with(policy, clock)` (std) | as `batch`, explicit `Clock` | `Batch<T>` |
| `.batch_bytes_with(policy, clock)` (std) | as `batch_bytes`, explicit `Clock` | `Batch<T>` |
| `.window(spec)` (std) | `impl Into<WindowSpec>`, `T: Clone` | `Window<T>` |
| `.window_with(spec, clock)` (std) | plus explicit `Clock` | `Window<T>` |
| `.window_by_key(key, spec)` (std) | `FnMut(&T) -> K` | `(K, Window<T>)` |
| `.window_by_key_with(key, spec, clock)` (std) | plus explicit `Clock` | `(K, Window<T>)` |
| `.buffer(capacity)` (std) | bounded edge; a thread boundary under `run_threaded` | `T` |

Terminals:

| Method | Returns |
|--------|---------|
| `.sink(sink)` | `Pipeline<S>`; a recorded configuration error is returned when it runs. |
| `.try_sink(sink)` | `Result<Pipeline<S>, ConfigError>`. |

`E` (error types of fallible closures) must implement `StageError`:
any `std::error::Error + Send + Sync + 'static`, `&'static str`, or
`String`.

## `Pipeline`

| Method | Description |
|--------|-------------|
| `run(self) -> Result<RunStats>` | Run on the calling thread. |
| `run_threaded(self) -> Result<RunStats>` (std) | One thread per `buffer` segment; panics re-raised with their payload. |
| `run_with(self, driver) -> Result<RunStats>` | Any `Driver`. |
| `step(&mut self) -> Result<Step>` | Pull and process one item; `Done` after a graceful end. |
| `tick(&mut self) -> Result<()>` | Deliver an idle tick now. |
| `finish(self) -> Result<RunStats>` | End gracefully (flush, close), even before the source is exhausted. |
| `stats(&self) -> RunStats` | Counters so far. |
| `is_finished(&self) -> bool` | True once the run ended. |
| `config_error(&self) -> Option<ConfigError>` | The recorded configuration error. |
| `cancel_handle(&mut self) -> CancelHandle` | A handle that stops the run gracefully. |

Lifecycle guarantee: once a run starts, the sink and the dead-letter
sink are flushed and closed and the source is closed on every exit path
(success, error, cancellation, panic, dropping a stepped pipeline). The
first error is returned; later ones are reported as
`Event::TeardownError`.

## `pipe_io::source`

```text
trait Source {
    type Item;
    type Error: StageError;
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error>;
    fn pull_timeout(&mut self, timeout: Duration) -> Result<Pull<Self::Item>, Self::Error>; // default: blocks in pull
    fn close(&mut self) -> Result<(), Self::Error>;                                       // default: Ok
}
enum Pull<T> { Item(T), Idle, End }   // non-exhaustive
```

| Type | Notes |
|------|-------|
| `IterSource<I>` | Any iterator. |
| `FnSource<F, T, E>` | `FnMut() -> Result<Option<T>, E>`. |
| `ChainSource<A, B>` | Drains `A`, closes it, continues with `B`; errors boxed. |
| `ChannelSource<T>` (std) | `mpsc::Receiver<T>`; implements `pull_timeout`. |
| `ReaderSource<R>` (std) | `String` lines from `io::Read`; `max_line_len` (default `DEFAULT_MAX_LINE_LEN` = 1 MiB), `on_overflow(LineOverflow)`, `lossy(bool)`, `with_capacity`. Error: `ReaderError`. |
| `ByteLinesSource<R>` (std) | `Vec<u8>` lines; same limits, no UTF-8 decoding. |
| `LineOverflow` (std) | `Error` (default), `Skip`, `Truncate`. |
| `ReaderError` (std) | `Io(io::Error)`, `LineTooLong { line, limit }`, `InvalidUtf8 { line }`. |
| `Infallible` | Deprecated alias of `core::convert::Infallible`. |

## `pipe_io::stage` and `pipe_io::emit`

```text
trait Stage {
    type Input;
    type Output;
    type Error: StageError;
    fn process(&mut self, item: Self::Input, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error>;
    fn tick(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error>;  // default: Ok
    fn flush(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error>; // default: Ok
}

trait Emit {
    type Item;
    fn emit(&mut self, item: Self::Item) -> Result<(), EmitError>;
}
enum EmitError { Closed }   // non-exhaustive
```

`emit` may block while a bounded downstream edge is full. `Closed`
means a later stage or the sink failed; stop emitting and return
`Ok(())`, the run reports the real error.

## `pipe_io::sink`

```text
trait Sink {
    type Item;
    type Error: StageError;
    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>; // default: Ok
    fn close(&mut self) -> Result<(), Self::Error>; // default: Ok
}
```

| Type | Notes |
|------|-------|
| `NullSink<T>` | Discards items. |
| `FnSink<F, T, E>` | `FnMut(T) -> Result<(), E>`. |
| `TeeSink<A, B>` | Writes to both (clone to the first); `flush`/`close` reach both; errors boxed. |
| `VecSink<T>` (std) | Collects into a shared `Vec`; `handle()` returns a `SharedHandle<T>` (`take`, `snapshot`, `len`, `is_empty`). Never panics on a poisoned lock. |
| `ChannelSink<T>` (std) | `mpsc::SyncSender<T>`; `ChannelSinkError::Disconnected`. |
| `WriterSink<W>` (std) | Writes `String` items plus `\n` to `io::Write`; flushes on `flush` and `close`. |

## `pipe_io::batch`

| Item | Notes |
|------|-------|
| `BatchPolicy` | `new`, `max_items`, `max_bytes`, `max_age` (std), `items_limit`, `bytes_limit`, `age_limit` (std), `has_trigger`, `validate`. |
| `Batch<T>` | `new`, `len`, `is_empty`, `iter`, `into_inner`; `Deref<Target = [T]>`, `AsRef<[T]>`, `From<Vec<T>>`, `Into<Vec<T>>`, `IntoIterator`, `Clone`, `PartialEq`, `Eq`, `Hash`, `Default`. |
| `ByteSize` | `byte_size(&self) -> usize`; implemented for `str`, `[u8]`, `[u8; N]`, `String`, `Vec<u8>`, and `&T`, `Box<T>`, `Rc<T>`, `Arc<T>`, `Cow<B>` of implementors. |

## `pipe_io::window` (std)

| Item | Notes |
|------|-------|
| `WindowPolicy` | Non-exhaustive enum `Tumbling { size }`, `Sliding { size, slide }`, `Session { idle }`; constructors `tumbling`, `sliding`, `session`; `max_items(n)` and `max_keys(n)` build a `WindowSpec`; `validate`. |
| `WindowSpec` | Policy plus `max_items` (per window) and `max_keys` (keyed stages); `From<WindowPolicy>`; `validate`. |
| `Window<T>` | `new`, `items`, `len`, `is_empty`, `iter`, `start`, `end`, `into_inner`; `Deref<Target = [T]>`, `AsRef<[T]>`, `IntoIterator`, `Clone`, `PartialEq`, `Eq`. |
| `MAX_SLIDING_OVERLAP` | `1024`: the largest `ceil(size / slide)`. |
| `Clock`, `SystemClock`, `ManualClock` | Re-exported from `pipe_io::clock`. |

## `pipe_io::clock` (std)

| Item | Notes |
|------|-------|
| `trait Clock: Send` | `fn now(&self) -> Instant`; implemented for `Box<C>`. |
| `SystemClock` | `Instant::now`. |
| `ManualClock` | `new(start)`, `advance(by)`, `set(to)`; clones share the time. |

## `pipe_io::retry` (std)

| Item | Notes |
|------|-------|
| `RetryPolicy` | `new(max_attempts)`, `backoff(Backoff)`, `jitter(bool)`, `max_attempts`, `base_delay(attempt)`, `validate`. |
| `Backoff` | Non-exhaustive: `None` (default), `Fixed(Duration)`, `Exponential { initial, max }`. |

## `pipe_io::error`

| Item | Notes |
|------|-------|
| `Error` | Non-exhaustive: `Source { stage, source }`, `Stage { stage, source }`, `Sink { stage, source }`, `Config(ConfigError)`, `Cancelled`, `Closed`, `Driver { source }`. Methods: `stage`, `inner`, `into_inner`, `downcast_ref`, `is_cancelled`. Implements `core::error::Error` with `source()`. |
| `ConfigError` | `new`, `stage`, `kind`, `with_stage`; implements `Error`. |
| `ConfigErrorKind` | Non-exhaustive: `ZeroDuration { field }`, `ZeroLimit { field }`, `SlideExceedsSize`, `TooManyOverlappingWindows { overlap, max }`, `NoBatchTrigger`, `BytesRequireByteSize`, `DeadLetterSinkMissing`. |
| `ErrorPolicy` | Non-exhaustive: `FailFast` (default), `Continue`, `DeadLetter`. |
| `StageFailure` | Non-exhaustive struct: `stage`, `source`, `input: Option<Box<dyn Any + Send + Sync>>`, `attempts`; `new`, `with_input`, `with_attempts`, `input_ref`, `take_input`; implements `Error`. |
| `StageError` | Blanket trait: `Into<BoxError> + Debug + Send + 'static`. |
| `BoxError` | `Box<dyn core::error::Error + Send + Sync + 'static>`. |
| `Result<T, E = Error>` | Alias. |

## `pipe_io::observe`

| Item | Notes |
|------|-------|
| `trait Observer: Send + Sync` | `fn on_event(&self, event: &Event<'_>)`; implemented for `Fn(&Event<'_>) + Send + Sync`. |
| `Event<'a>` | Non-exhaustive: `Started`, `StageError { stage, policy, error }`, `Retry { stage, attempt, delay, error }`, `DeadLettered { stage }`, `Idle`, `TeardownError { error }`, `Finished { stats, error }`. |

## `pipe_io::driver`

| Item | Notes |
|------|-------|
| `trait Driver` | `fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>` with `S: Source + Send + 'static`, `S::Item: Send + 'static`, `S::Error: Send + 'static`. |
| `SyncDriver` | Calling thread; the inherent `run` also accepts a non-`Send` source. |
| `ThreadedDriver` (std) | One thread per `buffer` segment; see its rustdoc for ordering, backpressure, error, panic, and cancellation semantics. |
| `RunStats` | Non-exhaustive: `items_in`, `items_out`, `errors`, `dead_lettered`, `retries`, `duration` (always present; zero without `std`). |

## Feature flags

| Flag  | Default | Enables |
|-------|---------|---------|
| `std` | yes | `run_threaded`, `buffer`, `ThreadedDriver`; `clock`, `window`, `retry` modules, `max_age`, `batch_with`, `try_map_retry`; `ChannelSource`, `ReaderSource`, `ByteLinesSource`, `ChannelSink`, `WriterSink`, `VecSink`. |

Without `std` the crate is `no_std` + `alloc`. On targets without
pointer-sized atomics, `CancelHandle` and `Pipeline::cancel_handle` are
unavailable.

## MSRV and dependencies

- MSRV **1.85**, edition 2024. Verified in CI. Raising it is a major
  version change.
- **Zero runtime dependencies.** Dev-dependencies (proptest, criterion,
  tokio, rayon) are used only by tests, benchmarks, and examples.

## Stability

- **Patch (`2.0.x`)**: bug fixes, documentation, internal performance
  work.
- **Minor (`2.x.0`)**: additions, new opt-in features, new variants of
  `#[non_exhaustive]` enums and new fields of `#[non_exhaustive]`
  structs.
- **Major (`3.0.0`)**: removals, renames, signature or behaviour
  changes, MSRV bumps.

Non-exhaustive types: `Error`, `ConfigErrorKind`, `ErrorPolicy`,
`EmitError`, `Event`, `Pull`, `Step`, `Backoff`, `WindowPolicy`,
`LineOverflow`, `ReaderError`, `ChannelSinkError`, `StageFailure`,
`RunStats`. Match enums with a wildcard arm; read structs by field.

The `runtime` module is hidden and not covered by these rules.
`cargo-semver-checks` runs in CI on every push and pull request.

## Performance

One indirect call per stage per item; no allocation per item in the
pipeline machinery. Measured numbers, method, and machine:
[`BENCH.md`](BENCH.md).
