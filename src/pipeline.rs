//! [`Pipeline`], [`PipelineBuilder`], and the synchronous run loop.
//!
//! The builder accumulates a closure that, given a sink-side
//! [`BoxedStageFn`] handling `T`, returns a source-side
//! [`BoxedStageFn`] handling `S::Item`. Each chained builder method
//! wraps the next closure in one more layer. At `.sink()`, the
//! accumulated closure is invoked with a sink-terminated closure to
//! materialize the full chain.

// The builder methods return `impl FnOnce(...) -> ...` types. Clippy
// flags these as "type_complexity", but they are inherent to the
// type-state builder pattern - the only alternative is a `Box<dyn
// FnOnce>` per stage, which costs an extra allocation per call.
//
// `from_iter` is a deliberate alias matching the iterator-construction
// idiom; the standard `FromIterator::from_iter` signature does not fit
// the source-builder shape.
#![allow(clippy::type_complexity, clippy::should_implement_trait)]

use alloc::boxed::Box;
use core::marker::PhantomData;

use crate::batch::{Batch, BatchPolicy, BatchStage, BatchStageBytes, ByteSize};
use crate::driver::{RunStats, SyncDriver};
use crate::emit::{Emit, EmitError};
use crate::error::{Error, ErrorPolicy, Result, StageError};
use crate::sink::Sink;
use crate::source::{IterSource, Source};
use crate::stage::Stage;
use crate::stage_id::StageId;

/// Item operation handed to every stage closure inside a built
/// pipeline. The driver invokes the chain with `Process(item)` for
/// each source item, then `Flush`, then `Close`.
///
/// This is an implementation detail of the builder type machinery.
/// It must be `pub` because it appears in the `impl Trait` return
/// types of the builder methods; consumers do not interact with it.
#[doc(hidden)]
pub enum StageOp<T> {
    Process(T),
    Flush,
    Close,
}

/// Type-erased per-stage closure. The whole chain collapses into a
/// nested set of these at `.sink()` time.
///
/// Implementation detail; `pub` only because it appears in the
/// `impl Trait` return types of the builder methods.
#[doc(hidden)]
pub type BoxedStageFn<T> = Box<dyn FnMut(StageOp<T>) -> Result<()> + Send + 'static>;

/// Internal [`Emit`] adapter that forwards into the next stage's
/// [`BoxedStageFn`]. Caches any downstream error so the driver can
/// surface it even when the upstream stage swallows
/// [`EmitError::Closed`].
struct StageEmit<'a, U> {
    next_fn: &'a mut BoxedStageFn<U>,
    cached_err: Option<Error>,
}

impl<'a, U> Emit for StageEmit<'a, U> {
    type Item = U;

    fn emit(&mut self, item: U) -> core::result::Result<(), EmitError> {
        if self.cached_err.is_some() {
            return Err(EmitError::Closed);
        }
        match (self.next_fn)(StageOp::Process(item)) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.cached_err = Some(e);
                Err(EmitError::Closed)
            }
        }
    }
}

/// A built pipeline. Run with [`Pipeline::run`] (sync) or
/// [`Pipeline::run_threaded`] (std).
pub struct Pipeline<S>
where
    S: Source,
{
    pub(crate) source: S,
    pub(crate) source_id: StageId,
    pub(crate) stage_fn: BoxedStageFn<S::Item>,
}

impl<S> Pipeline<S>
where
    S: Source + 'static,
    S::Item: Send + 'static,
    S::Error: Send + 'static,
{
    /// Start a new pipeline from a [`Source`].
    pub fn from_source(
        source: S,
    ) -> PipelineBuilder<
        S::Item,
        S,
        impl FnOnce(BoxedStageFn<S::Item>) -> BoxedStageFn<S::Item> + Send + 'static,
    > {
        PipelineBuilder {
            source,
            source_id: StageId::new("source"),
            finalize: identity_finalize::<S::Item>,
            error_policy: ErrorPolicy::FailFast,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Run the pipeline to completion on the calling thread.
    ///
    /// # Errors
    ///
    /// Returns the first error produced by the source, any stage, or
    /// the sink.
    pub fn run(self) -> Result<RunStats> {
        SyncDriver::new().run(self)
    }

    /// Run the pipeline to completion on a spawned thread.
    ///
    /// # Errors
    ///
    /// Returns the first error produced by the source, any stage, or
    /// the sink. Returns [`Error::Cancelled`] if the worker thread
    /// panics.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn run_threaded(self) -> Result<RunStats>
    where
        S: Send,
        S::Item: Send,
        S::Error: Send,
    {
        crate::driver::ThreadedDriver::new().run(self)
    }
}

impl Pipeline<IterSource<core::iter::Empty<()>>> {
    /// Start a new pipeline from any [`IntoIterator`].
    ///
    /// # Example
    ///
    /// ```
    /// use pipe_io::{Pipeline, sink::VecSink};
    /// let sink = VecSink::<i32>::new();
    /// let handle = sink.handle();
    /// Pipeline::from_iter(0..3).sink(sink).run().unwrap();
    /// assert_eq!(handle.take(), vec![0, 1, 2]);
    /// ```
    pub fn from_iter<II>(
        iter: II,
    ) -> PipelineBuilder<
        II::Item,
        IterSource<II::IntoIter>,
        impl FnOnce(BoxedStageFn<II::Item>) -> BoxedStageFn<II::Item> + Send + 'static,
    >
    where
        II: IntoIterator,
        II::Item: Send + 'static,
        II::IntoIter: Send + 'static,
    {
        Pipeline::from_source(IterSource::new(iter))
    }
}

/// Typed builder. Each transformer changes the carrier type `T`.
pub struct PipelineBuilder<T, S, Acc>
where
    S: Source,
    Acc: FnOnce(BoxedStageFn<T>) -> BoxedStageFn<S::Item> + Send + 'static,
{
    source: S,
    source_id: StageId,
    finalize: Acc,
    error_policy: ErrorPolicy,
    pending_stage_id: Option<StageId>,
    _marker: PhantomData<fn() -> T>,
}

fn identity_finalize<T: 'static + Send>(f: BoxedStageFn<T>) -> BoxedStageFn<T> {
    f
}

impl<T, S, Acc> PipelineBuilder<T, S, Acc>
where
    S: Source + 'static,
    S::Item: Send + 'static,
    S::Error: Send + 'static,
    T: Send + 'static,
    Acc: FnOnce(BoxedStageFn<T>) -> BoxedStageFn<S::Item> + Send + 'static,
{
    /// Label the next stage with a [`StageId`]. Applies to the next
    /// `.map` / `.filter` / `.batch` / `.sink` / etc. call.
    #[must_use]
    pub fn stage_id<I: Into<StageId>>(mut self, id: I) -> Self {
        self.pending_stage_id = Some(id.into());
        self
    }

    /// Set the [`ErrorPolicy`] applied to stages added after this
    /// call (until overridden).
    #[must_use]
    pub fn on_error(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    /// Apply a 1:1 transform.
    pub fn map<U, F>(
        self,
        mut f: F,
    ) -> PipelineBuilder<U, S, impl FnOnce(BoxedStageFn<U>) -> BoxedStageFn<S::Item> + Send + 'static>
    where
        U: Send + 'static,
        F: FnMut(T) -> U + Send + 'static,
    {
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<U>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => next(StageOp::Process(f(item))),
                StageOp::Flush => next(StageOp::Flush),
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Apply a predicate. Items for which `pred` returns `true` pass
    /// through; the rest are dropped.
    pub fn filter<F>(
        self,
        mut pred: F,
    ) -> PipelineBuilder<T, S, impl FnOnce(BoxedStageFn<T>) -> BoxedStageFn<S::Item> + Send + 'static>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<T>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => {
                    if pred(&item) {
                        next(StageOp::Process(item))
                    } else {
                        Ok(())
                    }
                }
                StageOp::Flush => next(StageOp::Flush),
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Map and filter in one step.
    pub fn filter_map<U, F>(
        self,
        mut f: F,
    ) -> PipelineBuilder<U, S, impl FnOnce(BoxedStageFn<U>) -> BoxedStageFn<S::Item> + Send + 'static>
    where
        U: Send + 'static,
        F: FnMut(T) -> Option<U> + Send + 'static,
    {
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<U>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => match f(item) {
                    Some(out) => next(StageOp::Process(out)),
                    None => Ok(()),
                },
                StageOp::Flush => next(StageOp::Flush),
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Emit zero or more items per input.
    pub fn flat_map<U, F, II>(
        self,
        mut f: F,
    ) -> PipelineBuilder<U, S, impl FnOnce(BoxedStageFn<U>) -> BoxedStageFn<S::Item> + Send + 'static>
    where
        U: Send + 'static,
        II: IntoIterator<Item = U>,
        F: FnMut(T) -> II + Send + 'static,
    {
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<U>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => {
                    for out in f(item) {
                        next(StageOp::Process(out))?;
                    }
                    Ok(())
                }
                StageOp::Flush => next(StageOp::Flush),
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Observe items without modifying the stream.
    pub fn inspect<F>(
        self,
        mut f: F,
    ) -> PipelineBuilder<T, S, impl FnOnce(BoxedStageFn<T>) -> BoxedStageFn<S::Item> + Send + 'static>
    where
        F: FnMut(&T) + Send + 'static,
    {
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<T>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => {
                    f(&item);
                    next(StageOp::Process(item))
                }
                StageOp::Flush => next(StageOp::Flush),
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Fallible 1:1 transform. Honors the active [`ErrorPolicy`].
    pub fn try_map<U, F, E>(
        self,
        mut f: F,
    ) -> PipelineBuilder<U, S, impl FnOnce(BoxedStageFn<U>) -> BoxedStageFn<S::Item> + Send + 'static>
    where
        U: Send + 'static,
        E: StageError,
        F: FnMut(T) -> core::result::Result<U, E> + Send + 'static,
    {
        let stage_id = self.pending_stage_id.unwrap_or(StageId::new("try_map"));
        let policy = self.error_policy;
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<U>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => match f(item) {
                    Ok(out) => next(StageOp::Process(out)),
                    Err(e) => match policy {
                        ErrorPolicy::FailFast => Err(Error::Stage {
                            stage: stage_id,
                            source: Box::new(e),
                        }),
                        ErrorPolicy::Continue | ErrorPolicy::DeadLetter => Ok(()),
                    },
                },
                StageOp::Flush => next(StageOp::Flush),
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Plug in a custom [`Stage`].
    pub fn stage<St>(
        self,
        mut stage: St,
    ) -> PipelineBuilder<
        St::Output,
        S,
        impl FnOnce(BoxedStageFn<St::Output>) -> BoxedStageFn<S::Item> + Send + 'static,
    >
    where
        St: Stage<Input = T> + Send + 'static,
        St::Output: Send + 'static,
        St::Error: 'static,
    {
        let stage_id = self.pending_stage_id.unwrap_or(StageId::new("stage"));
        let policy = self.error_policy;
        let old_finalize = self.finalize;
        let new_finalize = move |next: BoxedStageFn<St::Output>| -> BoxedStageFn<S::Item> {
            let mut next = next;
            let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
                StageOp::Process(item) => {
                    let mut adapter = StageEmit {
                        next_fn: &mut next,
                        cached_err: None,
                    };
                    let stage_result = stage.process(item, &mut adapter);
                    if let Some(err) = adapter.cached_err {
                        return Err(err);
                    }
                    match stage_result {
                        Ok(()) => Ok(()),
                        Err(e) => match policy {
                            ErrorPolicy::FailFast => Err(Error::Stage {
                                stage: stage_id,
                                source: Box::new(e),
                            }),
                            ErrorPolicy::Continue | ErrorPolicy::DeadLetter => Ok(()),
                        },
                    }
                }
                StageOp::Flush => {
                    let mut adapter = StageEmit {
                        next_fn: &mut next,
                        cached_err: None,
                    };
                    let stage_result = stage.flush(&mut adapter);
                    if let Some(err) = adapter.cached_err {
                        return Err(err);
                    }
                    match stage_result {
                        Ok(()) => next(StageOp::Flush),
                        Err(e) => Err(Error::Stage {
                            stage: stage_id,
                            source: Box::new(e),
                        }),
                    }
                }
                StageOp::Close => next(StageOp::Close),
            });
            old_finalize(t_fn)
        };
        PipelineBuilder {
            source: self.source,
            source_id: self.source_id,
            finalize: new_finalize,
            error_policy: self.error_policy,
            pending_stage_id: None,
            _marker: PhantomData,
        }
    }

    /// Group items into [`Batch<T>`] according to `policy`. The policy
    /// must have at least one trigger configured ([`BatchPolicy::max_items`]
    /// or [`BatchPolicy::max_age`]).
    ///
    /// # Panics
    ///
    /// Panics if `policy` has no configured trigger or has a
    /// `max_bytes` trigger without `T: ByteSize` (use
    /// [`PipelineBuilder::batch_bytes`] for byte-aware batching).
    pub fn batch(
        mut self,
        policy: BatchPolicy,
    ) -> PipelineBuilder<
        Batch<T>,
        S,
        impl FnOnce(BoxedStageFn<Batch<T>>) -> BoxedStageFn<S::Item> + Send + 'static,
    > {
        assert!(
            policy.has_trigger(),
            "BatchPolicy must have at least one trigger configured"
        );
        assert!(
            policy.bytes_limit().is_none(),
            "BatchPolicy::max_bytes requires PipelineBuilder::batch_bytes (T: ByteSize)"
        );
        let id = self
            .pending_stage_id
            .take()
            .unwrap_or(StageId::new("batch"));
        self.stage_id(id).stage(BatchStage::<T>::new(policy))
    }

    /// Byte-aware batching. Required when `policy` has a
    /// [`BatchPolicy::max_bytes`] trigger.
    ///
    /// # Panics
    ///
    /// Panics if `policy` has no configured trigger.
    pub fn batch_bytes(
        mut self,
        policy: BatchPolicy,
    ) -> PipelineBuilder<
        Batch<T>,
        S,
        impl FnOnce(BoxedStageFn<Batch<T>>) -> BoxedStageFn<S::Item> + Send + 'static,
    >
    where
        T: ByteSize,
    {
        assert!(
            policy.has_trigger(),
            "BatchPolicy must have at least one trigger configured"
        );
        let id = self
            .pending_stage_id
            .take()
            .unwrap_or(StageId::new("batch"));
        self.stage_id(id).stage(BatchStageBytes::<T>::new(policy))
    }

    /// Install a windowing stage using the default
    /// [`crate::SystemClock`]. The carrier type changes from `T` to
    /// [`crate::Window<T>`] after the call.
    ///
    /// `T: Clone` is required because sliding windows duplicate items
    /// across overlapping windows. Consumers with non-Clone types and
    /// tumbling semantics can use `.batch()` with `BatchPolicy::max_age`
    /// as a substitute.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn window(
        mut self,
        policy: crate::window::WindowPolicy,
    ) -> PipelineBuilder<
        crate::window::Window<T>,
        S,
        impl FnOnce(BoxedStageFn<crate::window::Window<T>>) -> BoxedStageFn<S::Item> + Send + 'static,
    >
    where
        T: Clone,
    {
        let id = self
            .pending_stage_id
            .take()
            .unwrap_or(StageId::new("window"));
        self.stage_id(id).stage(
            crate::window::WindowStage::<T, crate::window::SystemClock>::new(
                policy,
                crate::window::SystemClock,
            ),
        )
    }

    /// Install a windowing stage with a user-supplied [`crate::Clock`].
    /// Useful for deterministic tests and for hosts that have their
    /// own monotonic time source.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    pub fn window_with<C>(
        mut self,
        policy: crate::window::WindowPolicy,
        clock: C,
    ) -> PipelineBuilder<
        crate::window::Window<T>,
        S,
        impl FnOnce(BoxedStageFn<crate::window::Window<T>>) -> BoxedStageFn<S::Item> + Send + 'static,
    >
    where
        T: Clone,
        C: crate::window::Clock + 'static,
    {
        let id = self
            .pending_stage_id
            .take()
            .unwrap_or(StageId::new("window"));
        self.stage_id(id)
            .stage(crate::window::WindowStage::<T, C>::new(policy, clock))
    }

    /// Terminate the pipeline with a [`Sink`].
    pub fn sink<Sk>(self, sink: Sk) -> Pipeline<S>
    where
        Sk: Sink<Item = T> + Send + 'static,
        Sk::Error: 'static,
    {
        let mut sink = sink;
        let sink_id = self.pending_stage_id.unwrap_or(StageId::new("sink"));
        let t_fn: BoxedStageFn<T> = Box::new(move |op| match op {
            StageOp::Process(item) => sink.write(item).map_err(|e| Error::Sink {
                stage: sink_id,
                source: Box::new(e),
            }),
            StageOp::Flush => sink.flush().map_err(|e| Error::Sink {
                stage: sink_id,
                source: Box::new(e),
            }),
            StageOp::Close => sink.close().map_err(|e| Error::Sink {
                stage: sink_id,
                source: Box::new(e),
            }),
        });
        let item_fn: BoxedStageFn<S::Item> = (self.finalize)(t_fn);
        Pipeline {
            source: self.source,
            source_id: self.source_id,
            stage_fn: item_fn,
        }
    }
}

// ---------------------------------------------------------------------
// Synchronous run loop. Used by both SyncDriver and ThreadedDriver
// (the latter just wraps this in a spawned thread).
// ---------------------------------------------------------------------

pub(crate) fn run_sync<S>(mut pipeline: Pipeline<S>) -> Result<RunStats>
where
    S: Source + 'static,
    S::Item: 'static,
    S::Error: 'static,
{
    #[cfg(feature = "std")]
    let start = std::time::Instant::now();

    let mut stats = RunStats::default();

    loop {
        match pipeline.source.pull() {
            Ok(Some(item)) => {
                stats.items_in = stats.items_in.saturating_add(1);
                (pipeline.stage_fn)(StageOp::Process(item))?;
            }
            Ok(None) => break,
            Err(e) => {
                return Err(Error::Source {
                    stage: pipeline.source_id,
                    source: Box::new(e),
                });
            }
        }
    }

    (pipeline.stage_fn)(StageOp::Flush)?;
    (pipeline.stage_fn)(StageOp::Close)?;
    let _ = pipeline.source.close();

    #[cfg(feature = "std")]
    {
        stats.duration = start.elapsed();
    }

    Ok(stats)
}
