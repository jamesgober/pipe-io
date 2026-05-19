#![no_main]
//! Fuzz target: `ErrorPolicy::Continue` never produces a run-level
//! error even when `try_map` fails on arbitrary inputs.

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::NullSink;
use pipe_io::{ErrorPolicy, Pipeline};

fuzz_target!(|data: &[u8]| {
    let items: Vec<u8> = data.to_vec();
    let result = Pipeline::from_iter(items)
        .on_error(ErrorPolicy::Continue)
        .try_map(|n: u8| -> Result<u8, &'static str> {
            if n.is_multiple_of(13) {
                Err("multiple of 13")
            } else {
                Ok(n)
            }
        })
        .sink(NullSink::<u8>::new())
        .run();
    assert!(result.is_ok(), "Continue policy must not surface errors");
});
