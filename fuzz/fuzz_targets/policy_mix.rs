#![no_main]
//! Fuzz target: a random mix of error policies never loses or duplicates
//! an item. Every input ends up in exactly one place: the main sink, the
//! dead-letter sink (with its input attached), or the Continue drop
//! count; FailFast stops the run with an error.

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::VecSink;
use pipe_io::{ErrorPolicy, Pipeline, StageFailure};

fn policy(bits: u8) -> ErrorPolicy {
    match bits % 3 {
        0 => ErrorPolicy::Continue,
        1 => ErrorPolicy::DeadLetter,
        _ => ErrorPolicy::FailFast,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 {
        return;
    }
    let (head, items) = data.split_at(3);
    let (p1, p2) = (policy(head[0]), policy(head[1]));
    let (m1, m2) = (head[2] % 7 + 2, head[2] / 7 % 5 + 2);
    let items: Vec<u8> = items.to_vec();

    let main = VecSink::<u8>::new();
    let ok = main.handle();
    let dlq = VecSink::<StageFailure>::new();
    let dead = dlq.handle();
    let result = Pipeline::from_iter(items.clone())
        .on_error(p1)
        .stage_id("first")
        .try_map_ref(move |n: &u8| if n % m1 == 0 { Err("first") } else { Ok(*n) })
        .on_error(p2)
        .stage_id("second")
        .try_map_ref(move |n: &u8| if n % m2 == 0 { Err("second") } else { Ok(*n) })
        .dead_letter(dlq)
        .sink(main)
        .run();

    let passed = ok.take();
    let mut failed = dead.take();
    let dead_inputs: Vec<u8> = failed
        .iter_mut()
        .map(|f| f.take_input::<u8>().expect("try_map_ref attaches the input"))
        .collect();
    match result {
        Ok(stats) => {
            assert_eq!(stats.items_in as usize, items.len());
            assert_eq!(stats.items_out as usize, passed.len());
            assert_eq!(stats.dead_lettered as usize, dead_inputs.len());
            let dropped = stats.errors - stats.dead_lettered;
            assert_eq!(passed.len() + dead_inputs.len() + dropped as usize, items.len());
            let expected_ok: Vec<u8> = items
                .iter()
                .copied()
                .filter(|n| n % m1 != 0 && n % m2 != 0)
                .collect();
            assert_eq!(passed, expected_ok);
        }
        Err(e) => {
            assert!(p1 == ErrorPolicy::FailFast || p2 == ErrorPolicy::FailFast, "{e}");
            assert!(passed.len() + dead_inputs.len() <= items.len());
        }
    }
});
