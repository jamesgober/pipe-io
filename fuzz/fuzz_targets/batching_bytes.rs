#![no_main]
//! Fuzz target: byte-triggered batching is lossless and respects the
//! byte limit.

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::VecSink;
use pipe_io::{Batch, BatchPolicy, ByteSize, Pipeline};

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let max_bytes = ((u16::from(data[0]) | (u16::from(data[1]) << 8)) as usize % 1024) + 1;

    // Convert remaining bytes into String items (using lossy UTF-8 substrings).
    let items: Vec<String> = data[2..]
        .chunks(8)
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .collect();
    let expected = items.clone();

    let sink = VecSink::<Vec<String>>::new();
    let handle = sink.handle();
    let _ = Pipeline::from_iter(items)
        .batch_bytes(BatchPolicy::new().max_bytes(max_bytes))
        .map(|b: Batch<String>| b.into_inner())
        .sink(sink)
        .run();

    let groups = handle.take();
    let flat: Vec<String> = groups.iter().flat_map(|g| g.iter().cloned()).collect();
    assert_eq!(flat, expected);
});
