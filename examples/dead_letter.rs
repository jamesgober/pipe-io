//! `ErrorPolicy::DeadLetter` with a side-channel sink for failed records.
//! The main sink only sees successful items; failures land in the
//! dead-letter sink together with the original input, ready for replay.
//!
//! Run with:
//!
//! ```text
//! cargo run --example dead_letter
//! ```

use pipe_io::{ErrorPolicy, Pipeline, StageFailure, sink::VecSink};

fn main() {
    let main = VecSink::<u32>::new();
    let main_handle = main.handle();

    let dlq = VecSink::<StageFailure>::new();
    let dlq_handle = dlq.handle();

    let stats = Pipeline::from_iter(vec!["1", "two", "3", "four", "5", "six"])
        .on_error(ErrorPolicy::DeadLetter)
        .stage_id("parse")
        // `try_map_ref` borrows the input, so a failing record can be
        // attached to its dead-letter entry.
        .try_map_ref(|s: &&str| s.parse::<u32>())
        .dead_letter(dlq)
        .sink(main)
        .run()
        .expect("pipeline run");

    let successes = main_handle.take();
    let mut failures = dlq_handle.take();

    println!("parsed {} items: {successes:?}", successes.len());
    println!("dead-lettered {} items:", stats.dead_lettered);
    for f in &mut failures {
        let input = f.take_input::<&str>().unwrap_or("<not captured>");
        println!("  stage `{}` rejected {input:?}: {}", f.stage, f.source);
    }

    assert_eq!(successes, vec![1, 3, 5]);
    assert_eq!(failures.len(), 3);
}
