#![no_main]
//! Fuzz target: the line readers never panic on arbitrary bytes and never
//! return a line longer than the configured limit.

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use pipe_io::source::{ByteLinesSource, LineOverflow, ReaderError, ReaderSource, Source};

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 {
        return;
    }
    let (head, input) = data.split_at(3);
    let limit = usize::from(u16::from_le_bytes([head[0], head[1]]) % 512);
    let overflow = match head[2] % 3 {
        0 => LineOverflow::Error,
        1 => LineOverflow::Skip,
        _ => LineOverflow::Truncate,
    };
    let effective = limit.max(1);

    let mut bytes = ByteLinesSource::new(Cursor::new(input.to_vec()))
        .max_line_len(limit)
        .on_overflow(overflow);
    let mut lines = 0_usize;
    loop {
        match bytes.pull() {
            Ok(Some(line)) => {
                assert!(line.len() <= effective);
                lines += 1;
                assert!(lines <= input.len() + 1);
            }
            Ok(None) => break,
            Err(ReaderError::LineTooLong { limit: l, .. }) => {
                assert_eq!(overflow, LineOverflow::Error);
                assert_eq!(l, effective);
                break;
            }
            Err(other) => panic!("unexpected {other:?}"),
        }
    }

    // The String reader must not panic either, strict or lossy.
    let mut text = ReaderSource::new(Cursor::new(input.to_vec()))
        .max_line_len(limit)
        .on_overflow(overflow)
        .lossy(head[2] & 0x80 != 0);
    while let Ok(Some(_)) = text.pull() {}
});
