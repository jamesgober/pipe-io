//! `try_map` with `ErrorPolicy::DeadLetter` and a side-channel sink
//! for failed records. The main sink only sees successful items;
//! parse failures land in the DLQ for inspection.
//!
//! Run with:
//!
//! ```text
//! cargo run --example dead_letter
//! ```

use pipe_io::{sink::VecSink, ErrorPolicy, Pipeline, StageFailure};

#[derive(Debug)]
struct ParseFail(&'static str);
impl core::fmt::Display for ParseFail {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "parse fail: {}", self.0)
    }
}

fn main() {
    let main = VecSink::<u32>::new();
    let main_handle = main.handle();

    let dlq = VecSink::<StageFailure>::new();
    let dlq_handle = dlq.handle();

    Pipeline::from_iter(vec!["1", "two", "3", "four", "5", "six"])
        .stage_id("parse")
        .on_error(ErrorPolicy::DeadLetter)
        .try_map(|s: &str| s.parse::<u32>().map_err(|_| ParseFail("not an integer")))
        .dead_letter(dlq)
        .sink(main)
        .run()
        .expect("pipeline run");

    let successes = main_handle.take();
    let failures = dlq_handle.take();

    println!("parsed {} items: {:?}", successes.len(), successes);
    println!("dropped {} items:", failures.len());
    for f in &failures {
        println!("  stage `{}`: {}", f.stage, f.source);
    }

    assert_eq!(successes, vec![1, 3, 5]);
    assert_eq!(failures.len(), 3);
}
