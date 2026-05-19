//! Count-triggered batching. Group items into fixed-size groups
//! before they reach the sink. Mirrors the "write N records at a
//! time" pattern common in database writers.
//!
//! Run with:
//!
//! ```text
//! cargo run --example batching
//! ```

use pipe_io::{sink::VecSink, Batch, BatchPolicy, Pipeline};

fn main() {
    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();

    Pipeline::from_iter(1u32..=11)
        .batch(BatchPolicy::new().max_items(4))
        .map(|b: Batch<u32>| b.into_inner())
        .sink(sink)
        .run()
        .expect("pipeline run");

    let groups = handle.take();
    println!("emitted {} batches", groups.len());
    for (i, batch) in groups.iter().enumerate() {
        println!("  batch {i}: {batch:?}");
    }
    // Expect three batches: [1,2,3,4], [5,6,7,8], [9,10,11].
    assert_eq!(groups.len(), 3);
    assert_eq!(groups[0], vec![1, 2, 3, 4]);
    assert_eq!(groups[2], vec![9, 10, 11]);
}
