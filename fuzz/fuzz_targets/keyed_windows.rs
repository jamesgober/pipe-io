#![no_main]
//! Fuzz target: keyed windows with arbitrary keys, arrival gaps, idle
//! ticks, item caps, and key caps. Every run terminates, every item is
//! emitted exactly once (tumbling and session windows do not copy items),
//! windows are non-empty and hold only their key's items, and each key's
//! windows are in time order.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use libfuzzer_sys::fuzz_target;
use pipe_io::sink::FnSink;
use pipe_io::{Clock, ManualClock, Pipeline, Pull, Source, Window, WindowPolicy, WindowSpec};

/// Items (key, id) separated by scripted gaps; a gap byte with the high
/// bit set reports an idle tick instead of an item.
struct Script {
    data: Vec<u8>,
    pos: usize,
    next_id: u32,
    clock: ManualClock,
}

impl Source for Script {
    type Item = (u8, u32);
    type Error = &'static str;

    fn pull(&mut self) -> Result<Option<(u8, u32)>, &'static str> {
        loop {
            match self.pull_timeout(Duration::ZERO)? {
                Pull::Item(item) => return Ok(Some(item)),
                Pull::Idle => continue,
                Pull::End => return Ok(None),
                _ => return Ok(None),
            }
        }
    }

    fn pull_timeout(&mut self, _timeout: Duration) -> Result<Pull<(u8, u32)>, &'static str> {
        let Some(pair) = self.data.get(self.pos..self.pos + 2) else {
            return Ok(Pull::End);
        };
        let (gap, key) = (pair[0], pair[1]);
        self.pos += 2;
        self.clock.advance(Duration::from_millis(u64::from(gap & 0x7f) * 7));
        if gap & 0x80 != 0 {
            return Ok(Pull::Idle);
        }
        self.next_id += 1;
        Ok(Pull::Item((key % 16, self.next_id)))
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 {
        return;
    }
    let (head, rest) = data.split_at(3);
    let size = Duration::from_millis(1 + u64::from(head[1]) * 5);
    let policy = if head[0] & 1 == 0 {
        WindowPolicy::tumbling(size)
    } else {
        WindowPolicy::session(size)
    };
    let max_items = usize::from(head[2] & 0x07);
    let max_keys = usize::from((head[2] >> 3) & 0x07);
    let mut spec = WindowSpec::from(policy);
    if max_items > 0 {
        spec = spec.max_items(max_items);
    }
    if max_keys > 0 {
        spec = spec.max_keys(max_keys);
    }

    let clock = ManualClock::new(Instant::now() + Duration::from_secs(1_000_000));
    let source = Script {
        data: rest.to_vec(),
        pos: 0,
        next_id: 0,
        clock: clock.clone(),
    };
    let seen = clock.clone();
    let out: Arc<Mutex<Vec<(u8, Window<(u8, u32)>, Instant)>>> = Arc::default();
    let sink_out = Arc::clone(&out);
    let stats = Pipeline::from_source(source)
        .idle_tick(Duration::from_millis(1))
        .window_by_key_with(|e: &(u8, u32)| e.0, spec, clock)
        .sink(FnSink::new(
            move |(k, w): (u8, Window<(u8, u32)>)| -> Result<(), &'static str> {
                sink_out.lock().unwrap().push((k, w, seen.now()));
                Ok(())
            },
        ))
        .run()
        .expect("valid configuration runs to completion");

    let out = out.lock().unwrap();
    let mut ids: Vec<u32> = Vec::new();
    let mut last_start: HashMap<u8, Instant> = HashMap::new();
    for (key, window, _at) in out.iter() {
        assert!(!window.is_empty(), "only non-empty windows");
        assert!(window.iter().all(|(k, _)| k == key), "items keep their key");
        if let Some(prev) = last_start.insert(*key, window.start()) {
            assert!(prev <= window.start(), "windows of one key are in order");
        }
        ids.extend(window.iter().map(|(_, id)| *id));
    }
    ids.sort_unstable();
    let expected: Vec<u32> = (1..=stats.items_in as u32).collect();
    assert_eq!(ids, expected, "every item emitted exactly once");
});
