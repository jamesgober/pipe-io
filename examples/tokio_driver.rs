//! Running pipelines from a tokio application.
//!
//! A pipeline is synchronous: its source may block and its stages may do
//! CPU work, so it must never run on a tokio worker thread directly. The
//! pattern below hands it to tokio's blocking pool and drives it with
//! `Pipeline::step`, which lets async code cancel it (through a
//! `CancelHandle`) and observe progress while it runs.
//!
//! Two entry points are shown:
//!
//! * `run_async` for async callers (awaits completion without blocking
//!   the executor);
//! * `TokioDriver`, a `Driver` implementation for synchronous callers that
//!   own a runtime handle.
//!
//! Run with:
//!
//! ```text
//! cargo run --example tokio_driver
//! ```

use std::time::Duration;

use pipe_io::driver::{Driver, RunStats};
use pipe_io::{Error, Pipeline, Result, Source, Step, sink::NullSink, sink::VecSink};
use tokio::runtime::{Builder, Handle};

/// Run a pipeline on tokio's blocking pool and await it.
async fn run_async<S>(mut pipeline: Pipeline<S>) -> Result<RunStats>
where
    S: Source + Send + 'static,
    S::Item: Send + 'static,
{
    let joined = tokio::task::spawn_blocking(move || {
        loop {
            if pipeline.step()? == Step::Done {
                return Ok(pipeline.stats());
            }
        }
    })
    .await;
    match joined {
        Ok(result) => result,
        // A panic inside the pipeline: re-raise it like the built-in
        // drivers do.
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => Err(Error::Driver {
            source: Box::new(e),
        }),
    }
}

/// A `Driver` for synchronous code that holds a tokio runtime handle.
struct TokioDriver {
    handle: Handle,
}

impl Driver for TokioDriver {
    fn run<S>(self, pipeline: Pipeline<S>) -> Result<RunStats>
    where
        S: Source + Send + 'static,
        S::Item: Send + 'static,
        S::Error: Send + 'static,
    {
        self.handle.block_on(run_async(pipeline))
    }
}

fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("tokio runtime");

    // 1. From synchronous code, through the Driver trait.
    let sink = VecSink::new();
    let out = sink.handle();
    let stats = Pipeline::from_iter(1..=5)
        .map(|n: u64| n * n)
        .sink(sink)
        .run_with(TokioDriver {
            handle: runtime.handle().clone(),
        })
        .expect("pipeline run");
    println!("driver: {} items -> {:?}", stats.items_out, out.take());

    // 2. From async code: run an endless pipeline and cancel it after a
    //    timeout, without ever blocking a worker thread.
    runtime.block_on(async {
        let mut endless = Pipeline::from_iter(0_u64..).sink(NullSink::new());
        let cancel = endless.cancel_handle();
        let task = tokio::spawn(run_async(endless));
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        let result = task.await.expect("join");
        assert!(matches!(result, Err(Error::Cancelled)));
        println!("async: endless pipeline cancelled cleanly");
    });
}
