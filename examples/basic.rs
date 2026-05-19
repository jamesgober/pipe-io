//! The smallest useful pipeline: iterate, transform, filter, collect.
//!
//! Run with:
//!
//! ```text
//! cargo run --example basic
//! ```

use pipe_io::{sink::VecSink, Pipeline};

fn main() {
    let sink = VecSink::<i64>::new();
    let handle = sink.handle();

    Pipeline::from_iter(1..=10)
        .map(|n: i32| i64::from(n) * 10)
        .filter(|n: &i64| *n > 30)
        .sink(sink)
        .run()
        .expect("pipeline run");

    let out = handle.take();
    println!("collected {} items: {:?}", out.len(), out);
    assert_eq!(out, vec![40, 50, 60, 70, 80, 90, 100]);
}
