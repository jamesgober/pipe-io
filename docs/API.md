# pipe-io - API Reference

> Authoritative reference for the public API of `pipe-io` at the
> current version. Mirrors the rustdoc on docs.rs. Section numbers
> match `REPS.md` section 4.

## Contents

- [Crate root](#crate-root)
- [`pipe_io::source`](#pipe_iosource)
- [`pipe_io::stage`](#pipe_iostage)
- [`pipe_io::sink`](#pipe_iosink)
- [`pipe_io::batch`](#pipe_iobatch)
- [`pipe_io::error`](#pipe_ioerror)
- [`pipe_io::driver`](#pipe_iodriver)
- [`pipe_io::emit`](#pipe_ioemit)
- [Feature flags](#feature-flags)
- [MSRV](#msrv)
- [Runtime dependencies](#runtime-dependencies)

## Crate root

| Item                                | Kind     | Notes                                              |
|-------------------------------------|----------|----------------------------------------------------|
| `pipe_io::VERSION`                  | const    | `&'static str`, populated by Cargo at build time.  |
| `pipe_io::Pipeline<S>`              | struct   | A built, runnable pipeline.                        |
| `pipe_io::PipelineBuilder<T, S, A>` | struct   | Typed builder. Carrier type `T` is tracked.        |
| `pipe_io::RunStats`                 | struct   | Counters and timing returned by `Pipeline::run`.   |
| `pipe_io::Error`                    | enum     | Top-level error.                                   |
| `pipe_io::Result<T>`                | alias    | `Result<T, Error>`.                                |
| `pipe_io::StageId`                  | struct   | `&'static str` newtype for stage identification.   |
| `pipe_io::StageError`               | trait    | Marker for error types stages can produce.         |
| `pipe_io::StageFailure`             | struct   | Per-item stage failure (dead-letter material).     |
| `pipe_io::BoxError`                 | alias    | `Box<dyn StageError>`.                             |
| `pipe_io::ErrorPolicy`              | enum     | Per-stage error policy.                            |
| `pipe_io::Source`                   | trait    | Re-export from `pipe_io::source`.                  |
| `pipe_io::Stage`                    | trait    | Re-export from `pipe_io::stage`.                   |
| `pipe_io::Sink`                     | trait    | Re-export from `pipe_io::sink`.                    |
| `pipe_io::Emit`                     | trait    | Re-export from `pipe_io::emit`.                    |
| `pipe_io::EmitError`                | enum     | Re-export from `pipe_io::emit`.                    |
| `pipe_io::Batch<T>`                 | struct   | Owned group of items emitted by `.batch()`.        |
| `pipe_io::BatchPolicy`              | struct   | Batch trigger configuration.                       |
| `pipe_io::ByteSize`                 | trait    | Opt-in for byte-aware batching.                    |

## `pipe_io::source`

```rust
pub trait Source {
    type Item;
    type Error: StageError;
    fn pull(&mut self) -> Result<Option<Self::Item>, Self::Error>;
    fn close(&mut self) -> Result<(), Self::Error> { Ok(()) }
}
```

Adapters:

| Item                                            | Kind    | Notes |
|-------------------------------------------------|---------|-------|
| `IterSource<I: Iterator>`                       | struct  | Wraps any `IntoIterator`. `Error = Infallible`. |
| `FnSource<F, T, E>`                             | struct  | Wraps `FnMut() -> Result<Option<T>, E>`.        |
| `ChannelSource<T>` **(std)**                    | struct  | Wraps `mpsc::Receiver<T>`.                      |
| `ReaderSource<R: io::Read>` **(std)**           | struct  | Line-buffered. `Error = std::io::Error`.        |
| `Infallible`                                    | enum    | Uninhabited error type for sources that cannot fail. |

## `pipe_io::stage`

```rust
pub trait Stage {
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
        _out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error> { Ok(()) }
}
```

Stages emit zero or more outputs per input via `Emit`. Closure-based
adapters (`map`, `filter`, `filter_map`, `flat_map`, `inspect`,
`try_map`) live on `PipelineBuilder`.

## `pipe_io::sink`

```rust
pub trait Sink {
    type Item;
    type Error: StageError;
    fn write(&mut self, item: Self::Item) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error> { Ok(()) }
    fn close(&mut self) -> Result<(), Self::Error> { Ok(()) }
}
```

Adapters:

| Item                              | Kind    | Notes |
|-----------------------------------|---------|-------|
| `NullSink<T>`                     | struct  | Discards every item.                            |
| `FnSink<F, T, E>`                 | struct  | Wraps `FnMut(T) -> Result<(), E>`.              |
| `VecSink<T>` **(std)**            | struct  | Collects into a `Vec<T>`. Items accessible via `.handle()` (cloneable, `Send + Sync`). |
| `SharedHandle<T>` **(std)**       | struct  | `Arc<Mutex<Vec<T>>>` wrapper returned by `VecSink::handle`. Methods: `take`, `len`, `is_empty`, `clone`. |
| `ChannelSink<T>` **(std)**        | struct  | Wraps `mpsc::SyncSender<T>`.                    |
| `ChannelSinkError` **(std)**      | enum    | `Disconnected` variant.                         |
| `WriterSink<W: io::Write>` **(std)** | struct | Line-writes `String` items into any writer.   |

## `pipe_io::batch`

```rust
pub struct BatchPolicy { /* opaque */ }

impl BatchPolicy {
    pub const fn new() -> Self;
    pub const fn max_items(self, n: usize) -> Self;
    pub const fn max_bytes(self, n: usize) -> Self;
    pub const fn max_age(self, age: Duration) -> Self;  // std only

    pub const fn items_limit(&self) -> Option<usize>;
    pub const fn bytes_limit(&self) -> Option<usize>;
    pub const fn age_limit(&self) -> Option<Duration>;  // std only
    pub const fn has_trigger(&self) -> bool;
}

pub trait ByteSize { fn byte_size(&self) -> usize; }

pub struct Batch<T> {
    pub fn new(items: Vec<T>) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn iter(&self) -> slice::Iter<'_, T>;
    pub fn into_inner(self) -> Vec<T>;
}
// Deref<Target = [T]>, IntoIterator for Batch<T> and &Batch<T>.
```

Blanket `ByteSize` impls: `&str`, `String`, `Vec<u8>`, `&[u8]`.

Insert a batching stage via `PipelineBuilder::batch(policy)` (count
and age triggers) or `PipelineBuilder::batch_bytes(policy)` (when the
policy has a `max_bytes` trigger; requires `T: ByteSize`).

## `pipe_io::error`

```rust
pub trait StageError: Debug + Display + Send + Sync + 'static {}
impl<T> StageError for T where T: Debug + Display + Send + Sync + 'static {}

pub type BoxError = Box<dyn StageError>;
pub type Result<T> = core::result::Result<T, Error>;

#[non_exhaustive]
pub enum Error {
    Source { stage: StageId, source: BoxError },
    Stage  { stage: StageId, source: BoxError },
    Sink   { stage: StageId, source: BoxError },
    Buffer { stage: StageId, kind: BufferErrorKind },
    Cancelled,
    Closed,
}

impl Error {
    pub fn stage(&self) -> Option<StageId>;
}

pub enum BufferErrorKind { Full, Closed }

#[derive(Default)]
pub enum ErrorPolicy {
    #[default] FailFast,
    Continue,
    DeadLetter,  // routes to dead-letter sink (not yet wired in 0.3.x)
}

#[non_exhaustive]
pub struct StageFailure {
    pub stage: StageId,
    pub source: BoxError,
}
```

`StageError`'s blanket impl admits every `std::error::Error` type
plus any other `Debug + Display + Send + Sync + 'static` type. The
`core::error::Error` super-trait was avoided because it stabilized
in Rust 1.81 and the crate's MSRV is 1.75.

## `pipe_io::driver`

```rust
#[derive(Default, Clone, Copy)]
pub struct RunStats {
    pub items_in: u64,
    pub duration: Duration,  // std only
}

#[derive(Default, Clone, Copy)]
pub struct SyncDriver;

impl SyncDriver {
    pub const fn new() -> Self;
    pub fn run<S: Source>(self, pipeline: Pipeline<S>) -> Result<RunStats>;
}

#[derive(Default, Clone, Copy)]
pub struct ThreadedDriver;  // std only

impl ThreadedDriver {
    pub const fn new() -> Self;
    pub fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static;
}
```

A unified `Driver` trait is deferred past `0.3.0`; consumers
select a driver by calling `Pipeline::run` (sync) or
`Pipeline::run_threaded` (std).

## `pipe_io::emit`

```rust
pub trait Emit {
    type Item;
    fn emit(&mut self, item: Self::Item) -> Result<(), EmitError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmitError {
    Closed,
    WouldBlock,
}
```

## Builder surface

```rust
impl<S: Source> Pipeline<S> {
    pub fn from_source(source: S) -> PipelineBuilder<S::Item, S, _>;
    pub fn from_iter<II: IntoIterator>(iter: II) -> PipelineBuilder<II::Item, IterSource<II::IntoIter>, _>;

    pub fn run(self) -> Result<RunStats>;
    pub fn run_threaded(self) -> Result<RunStats>;  // std only
}

impl<T, S, Acc> PipelineBuilder<T, S, Acc>
where
    S: Source + 'static,
    T: Send + 'static,
    S::Item: Send + 'static,
{
    pub fn stage_id<I: Into<StageId>>(self, id: I) -> Self;
    pub fn on_error(self, policy: ErrorPolicy) -> Self;

    pub fn map<U, F>(self, f: F) -> PipelineBuilder<U, S, _>
        where F: FnMut(T) -> U + Send + 'static;
    pub fn filter<F>(self, pred: F) -> PipelineBuilder<T, S, _>
        where F: FnMut(&T) -> bool + Send + 'static;
    pub fn filter_map<U, F>(self, f: F) -> PipelineBuilder<U, S, _>
        where F: FnMut(T) -> Option<U> + Send + 'static;
    pub fn flat_map<U, F, II>(self, f: F) -> PipelineBuilder<U, S, _>
        where II: IntoIterator<Item = U>, F: FnMut(T) -> II + Send + 'static;
    pub fn inspect<F>(self, f: F) -> PipelineBuilder<T, S, _>
        where F: FnMut(&T) + Send + 'static;
    pub fn try_map<U, F, E>(self, f: F) -> PipelineBuilder<U, S, _>
        where E: StageError, F: FnMut(T) -> Result<U, E> + Send + 'static;
    pub fn stage<St: Stage<Input = T>>(self, stage: St) -> PipelineBuilder<St::Output, S, _>;
    pub fn batch(self, policy: BatchPolicy) -> PipelineBuilder<Batch<T>, S, _>;
    pub fn batch_bytes(self, policy: BatchPolicy) -> PipelineBuilder<Batch<T>, S, _>
        where T: ByteSize;

    pub fn sink<Sk: Sink<Item = T>>(self, sink: Sk) -> Pipeline<S>;
}
```

The `Acc` parameter is an anonymous closure type the builder
threads through; the user never names it.

## Feature flags

| Flag    | Default | Effect                                                                                                |
|---------|---------|-------------------------------------------------------------------------------------------------------|
| `std`   | yes     | `ChannelSource`, `ReaderSource`, `ChannelSink`, `WriterSink`, `VecSink`, `ThreadedDriver`, batch age trigger. |

## MSRV

`1.75`.

## Runtime dependencies

Zero. Built on `core` + `alloc`, plus `std` when the `std` feature
is enabled.

## Quick example

```rust
use pipe_io::{Pipeline, sink::VecSink, BatchPolicy};

let sink = VecSink::<Vec<i64>>::new();
let handle = sink.handle();

Pipeline::from_iter(1..=10)
    .map(|n: i32| i64::from(n) * 10)
    .filter(|n: &i64| *n > 30)
    .batch(BatchPolicy::new().max_items(2))
    .map(|b: pipe_io::Batch<i64>| b.into_inner())
    .sink(sink)
    .run()
    .unwrap();

assert_eq!(handle.take(), vec![vec![40, 50], vec![60, 70], vec![80, 90], vec![100]]);
```
