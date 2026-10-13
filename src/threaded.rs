//! Threaded execution: one OS thread per segment between `buffer` edges.
//!
//! At assembly time every `buffer(capacity)` edge becomes a
//! `sync_channel(capacity)`. The part of the chain downstream of the edge
//! is packaged as a [`ChannelSegment`] that runs on its own thread and
//! receives [`StageOp`]s from the channel; the upstream side gets a closure
//! that sends into it. The source and the stages before the first edge
//! run on a dedicated source thread. See [`crate::driver::ThreadedDriver`]
//! for the user-facing semantics.

use alloc::boxed::Box;
use alloc::format;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;
use core::time::Duration;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::driver::RunStats;
use crate::error::{Error, Result};
use crate::observe::{Event, ObserverSlot};
use crate::pipeline::{Built, CancelSlot, Pipeline, PumpEnd, Runner};
use crate::runtime::{
    BoxedStageFn, Ctx, Dlq, FirstError, Halt, Signal, StageOp, StageResult, Tally, Wiring, contain,
};
use crate::source::Source;
use crate::stage_id::StageId;

/// Assembly state for threaded execution.
pub(crate) struct ThreadedWiring {
    segments: Vec<Box<dyn SegmentTask>>,
    dlq: Dlq,
    observer: ObserverSlot,
}

impl ThreadedWiring {
    /// Turn `next` (the chain downstream of a `buffer` edge) into a worker
    /// segment and return the sending side for the upstream chain.
    pub(crate) fn split<T: Send + 'static>(
        &mut self,
        next: BoxedStageFn<T>,
        capacity: usize,
        id: StageId,
    ) -> BoxedStageFn<T> {
        let (tx, rx) = sync_channel::<StageOp<T>>(capacity);
        let ctx = Ctx::new(self.dlq.share(), self.observer.share());
        self.segments.push(Box::new(ChannelSegment {
            id,
            rx: Some(rx),
            chain: next,
            ctx,
            live: true,
        }));
        Box::new(move |op, _ctx| send(&tx, op))
    }
}

/// Upstream side of an edge.
fn send<T>(tx: &SyncSender<StageOp<T>>, op: StageOp<T>) -> StageResult {
    match op {
        // A tick is advisory: if the edge is full the downstream thread is
        // busy and will tick itself, so never block on one.
        StageOp::Control(Signal::Tick) => match tx.try_send(op) {
            Ok(()) | Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Disconnected(_)) => Err(Halt::Disconnected),
        },
        // Abort is best effort: the downstream thread may already be gone,
        // in which case it has torn itself down.
        StageOp::Control(Signal::Abort) => match tx.send(op) {
            Ok(()) | Err(_) => Ok(()),
        },
        other => tx.send(other).map_err(|_| Halt::Disconnected),
    }
}

/// The first error across all threads; later ones go to the observer.
#[derive(Debug, Default)]
struct SharedFirst(Mutex<FirstError>);

impl SharedFirst {
    fn offer(&self, error: Error, observer: &ObserverSlot) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .offer(error, observer);
    }

    fn take(&self) -> FirstError {
        core::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// A unit of work that runs on its own thread.
trait SegmentTask: Send {
    fn name(&self) -> StageId;
    fn run(self: Box<Self>, idle_tick: Option<Duration>, first: &SharedFirst) -> Tally;
}

/// The chain downstream of one `buffer` edge.
struct ChannelSegment<T> {
    id: StageId,
    rx: Option<Receiver<StageOp<T>>>,
    chain: BoxedStageFn<T>,
    ctx: Ctx,
    /// True until the chain has been closed or aborted.
    live: bool,
}

impl<T> ChannelSegment<T> {
    fn recv(&self, idle_tick: Option<Duration>) -> Option<StageOp<T>> {
        let rx = self.rx.as_ref()?;
        match idle_tick {
            None => rx.recv().ok(),
            Some(timeout) => match rx.recv_timeout(timeout) {
                Ok(op) => Some(op),
                Err(RecvTimeoutError::Timeout) => {
                    self.ctx.observer.emit(&Event::Idle);
                    Some(StageOp::Control(Signal::Tick))
                }
                Err(RecvTimeoutError::Disconnected) => None,
            },
        }
    }

    fn abort(&mut self, first: &SharedFirst) {
        self.live = false;
        if let Err(Halt::Failed(e)) = (self.chain)(StageOp::Control(Signal::Abort), &mut self.ctx) {
            first.offer(e, &self.ctx.observer);
        }
    }
}

impl<T: Send + 'static> SegmentTask for ChannelSegment<T> {
    fn name(&self) -> StageId {
        self.id
    }

    fn run(mut self: Box<Self>, idle_tick: Option<Duration>, first: &SharedFirst) -> Tally {
        loop {
            let Some(op) = self.recv(idle_tick) else {
                // The upstream thread ended without closing the edge: it
                // panicked. Tear this side down.
                self.rx = None;
                self.abort(first);
                break;
            };
            let closing = matches!(op, StageOp::Control(Signal::Close | Signal::Abort));
            match (self.chain)(op, &mut self.ctx) {
                Ok(()) if closing => {
                    self.live = false;
                    break;
                }
                Ok(()) => {}
                Err(halt) => {
                    // Drop the receiver first so a blocked upstream sender
                    // wakes up and stops.
                    self.rx = None;
                    if let Halt::Failed(e) = halt {
                        first.offer(e, &self.ctx.observer);
                    }
                    if closing {
                        self.live = false;
                    } else {
                        self.abort(first);
                    }
                    break;
                }
            }
        }
        self.ctx.tally
    }
}

impl<T> Drop for ChannelSegment<T> {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        // Reached when a segment thread panics, or when a segment was never
        // spawned. Contain a second panic so teardown cannot abort.
        let shared = SharedFirst::default();
        let _completed = contain(|| self.abort(&shared));
        if let Err(e) = shared.take().into_result() {
            self.ctx.observer.emit(&Event::TeardownError { error: &e });
        }
    }
}

/// The source thread: pull, then finish or abort.
fn source_loop<S: Source>(
    mut runner: Runner<S>,
    idle_tick: Option<Duration>,
    cancel: &CancelSlot,
    first: &SharedFirst,
) -> (Tally, bool) {
    let end = runner.pump(idle_tick, cancel);
    let cancelled = matches!(end, PumpEnd::Cancelled);
    let mut local = FirstError::default();
    match end {
        PumpEnd::Graceful | PumpEnd::Cancelled => runner.finish(&mut local),
        PumpEnd::Failed(e) => {
            first.offer(e, &runner.ctx.observer);
            runner.abort(&mut local);
        }
        // A downstream thread failed and already reported its error.
        PumpEnd::Disconnected => runner.abort(&mut local),
    }
    if let Err(e) = local.into_result() {
        first.offer(e, &runner.ctx.observer);
    }
    (runner.ctx.tally, cancelled)
}

/// Results collected from the worker threads.
#[derive(Default)]
struct Joined {
    tally: Tally,
    cancelled: bool,
    panic: Option<Box<dyn Any + Send>>,
}

impl Joined {
    fn record_panic(&mut self, payload: Box<dyn Any + Send>) {
        if self.panic.is_none() {
            self.panic = Some(payload);
        }
    }
}

fn spawn_failure(error: std::io::Error) -> Error {
    Error::Driver {
        source: Box::new(error),
    }
}

/// Spawn every segment thread. Segments are listed sink-side first, so
/// each consumer is running before its producer starts. If a spawn fails,
/// the remaining segments are dropped (which tears them down) and `false`
/// is returned.
fn spawn_segments(
    segments: Vec<Box<dyn SegmentTask>>,
    idle_tick: Option<Duration>,
    first: &Arc<SharedFirst>,
    observer: &ObserverSlot,
    workers: &mut Vec<JoinHandle<Tally>>,
) -> bool {
    for segment in segments {
        let name = format!("pipe-io:{}", segment.name());
        let shared = Arc::clone(first);
        let spawned = thread::Builder::new()
            .name(name)
            .spawn(move || segment.run(idle_tick, &shared));
        match spawned {
            Ok(handle) => workers.push(handle),
            Err(e) => {
                // The closure, and the segment inside it, was dropped by
                // `spawn`, which tore the segment down. The rest of the
                // iterator is dropped on return and tears down the same way.
                first.offer(spawn_failure(e), observer);
                return false;
            }
        }
    }
    true
}

fn join_all(source: Option<JoinHandle<(Tally, bool)>>, workers: Vec<JoinHandle<Tally>>) -> Joined {
    let mut joined = Joined::default();
    if let Some(handle) = source {
        match handle.join() {
            Ok((tally, cancelled)) => {
                joined.tally.merge(tally);
                joined.cancelled = cancelled;
            }
            Err(payload) => joined.record_panic(payload),
        }
    }
    for handle in workers {
        match handle.join() {
            Ok(tally) => joined.tally.merge(tally),
            Err(payload) => joined.record_panic(payload),
        }
    }
    joined
}

/// Entry point of the threaded driver.
pub(crate) fn run<S>(mut pipeline: Pipeline<S>) -> Result<RunStats>
where
    S: Source + Send + 'static,
    S::Item: Send + 'static,
{
    let started = Instant::now();
    let idle_tick = pipeline.idle_tick();
    let cancel = pipeline.cancel_slot();
    let source_id = pipeline.source_id();
    let Built {
        source,
        assembly,
        dead_letter,
        observer,
    } = pipeline.take_built()?;

    // Assemble with every `buffer` edge split into a channel + segment.
    let observer = ObserverSlot::new(observer);
    let mut dlq = Dlq::new(dead_letter);
    let source_dlq = dlq.share();
    let mut wiring = Wiring {
        threaded: Some(ThreadedWiring {
            segments: Vec::new(),
            dlq: dlq.share(),
            observer: observer.share(),
        }),
    };
    let chain = assembly(&mut wiring);
    let segments = wiring.threaded.map(|t| t.segments).unwrap_or_default();
    let ctx = Ctx::new(source_dlq, observer.share());
    let runner = Runner::new(source, source_id, chain, ctx, false);
    observer.emit(&Event::Started);

    let first = Arc::new(SharedFirst::default());
    let mut workers = Vec::with_capacity(segments.len());
    let mut source_handle = None;
    if spawn_segments(segments, idle_tick, &first, &observer, &mut workers) {
        let shared = Arc::clone(&first);
        let spawned = thread::Builder::new()
            .name(format!("pipe-io:{source_id}"))
            .spawn(move || source_loop(runner, idle_tick, &cancel, &shared));
        match spawned {
            Ok(handle) => source_handle = Some(handle),
            Err(e) => first.offer(spawn_failure(e), &observer),
        }
    } else {
        // Dropping the unstarted runner aborts downstream and closes the
        // source.
        drop(runner);
    }

    let joined = join_all(source_handle, workers);
    let mut first = first.take();
    first.offer_result(dlq.finish(&observer), &observer);
    if let Some(payload) = joined.panic {
        std::panic::resume_unwind(payload);
    }

    let mut stats = joined.tally.to_stats();
    stats.duration = started.elapsed();
    let result = match first.into_result() {
        Err(e) => Err(e),
        Ok(()) if joined.cancelled => Err(Error::Cancelled),
        Ok(()) => Ok(stats),
    };
    observer.emit(&Event::Finished {
        stats: &stats,
        error: result.as_ref().err(),
    });
    result
}
