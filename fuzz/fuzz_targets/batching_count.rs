#![no_main]
//! Fuzz target: count-triggered batching is lossless.

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::VecSink;
use pipe_io::{Batch, BatchPolicy, Pipeline};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Derive batch size from the first byte; clamp to 1..=64.
    let size = (data[0] as usize % 64) + 1;
    let items: Vec<u8> = data[1..].to_vec();
    let expected = items.clone();

    let sink = VecSink::<Vec<u8>>::new();
    let handle = sink.handle();
    let _ = Pipeline::from_iter(items)
        .batch(BatchPolicy::new().max_items(size))
        .map(|b: Batch<u8>| b.into_inner())
        .sink(sink)
        .run();

    let flat: Vec<u8> = handle.take().into_iter().flatten().collect();
    assert_eq!(flat, expected);
});
