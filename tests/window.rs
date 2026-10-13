//! Integration tests for windowing, driven by deterministic clocks.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use core::time::Duration;
use std::sync::mpsc;
use std::time::Instant;

use common::ScriptedClock;
use pipe_io::sink::VecSink;
use pipe_io::source::{ChannelSource, IterSource};
use pipe_io::{ManualClock, Pipeline, Step, Window, WindowPolicy, WindowSpec};

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn tumbling_window_rolls_up_metric() {
    let t0 = Instant::now();
    // One clock read per item, plus one for the flush.
    let clock = ScriptedClock::new(vec![
        t0,
        t0 + secs(2),
        t0 + secs(9),
        t0 + secs(11), // closes [0,10)
        t0 + secs(15),
        t0 + secs(20), // flush
    ]);
    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();
    Pipeline::from_source(IterSource::new(vec![1_u32, 2, 3, 4, 5]))
        .window_with(WindowPolicy::tumbling(secs(10)), clock)
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");
    assert_eq!(handle.take(), vec![vec![1, 2, 3], vec![4, 5]]);
}

#[test]
fn session_window_boundary() {
    let t0 = Instant::now();
    let clock = ScriptedClock::new(vec![
        t0,
        t0 + secs(1),
        t0 + secs(2),
        t0 + secs(30), // > 5s gap: new session
        t0 + secs(31),
        t0 + secs(32), // flush
    ]);
    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();
    Pipeline::from_source(IterSource::new(vec![1_u32, 2, 3, 4, 5]))
        .window_with(WindowPolicy::session(secs(5)), clock)
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");
    assert_eq!(handle.take(), vec![vec![1, 2, 3], vec![4, 5]]);
}

#[test]
fn sliding_window_overlaps_and_each_item_joins_every_covering_window() {
    let t0 = Instant::now() + secs(60);
    // size 10, slide 5: the grid starts at t=-5, windows [-5,5) [0,10)
    // [5,15) [10,20) ...
    let clock = ScriptedClock::new(vec![
        t0,            // 1 -> [-5,5) [0,10)
        t0 + secs(3),  // 2 -> [-5,5) [0,10)
        t0 + secs(7),  // 3 -> [0,10) [5,15); [-5,5) closes
        t0 + secs(11), // 4 -> [5,15) [10,20); [0,10) closes
        t0 + secs(20), // flush
    ]);
    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();
    Pipeline::from_source(IterSource::new(vec![1_u32, 2, 3, 4]))
        .window_with(WindowPolicy::sliding(secs(10), secs(5)), clock)
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");
    assert_eq!(
        handle.take(),
        vec![vec![1, 2], vec![1, 2, 3], vec![3, 4], vec![4]]
    );
}

#[test]
fn tumbling_with_no_items_emits_nothing() {
    let clock = ScriptedClock::new(vec![Instant::now()]);
    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();
    Pipeline::from_source(IterSource::new(std::iter::empty::<u32>()))
        .window_with(WindowPolicy::tumbling(secs(10)), clock)
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink)
        .run()
        .expect("run");
    assert!(handle.take().is_empty());
}

#[test]
fn window_spec_cap_bounds_window_size() {
    let clock = ManualClock::new(Instant::now());
    let sink = VecSink::<usize>::new();
    let handle = sink.handle();
    Pipeline::from_iter(0..10_u32)
        .window_with(
            WindowSpec::new(WindowPolicy::tumbling(secs(60))).max_items(4),
            clock,
        )
        .map(|w: Window<u32>| w.len())
        .sink(sink)
        .run()
        .unwrap();
    assert_eq!(handle.take(), vec![4, 4, 2]);
}

#[test]
fn idle_tick_closes_windows_on_a_quiet_channel() {
    // A live source that goes quiet: without the idle tick the window
    // would stay open until the channel closes.
    let t0 = Instant::now();
    let clock = ManualClock::new(t0);
    let (tx, rx) = mpsc::channel::<u32>();
    let sink = VecSink::<Vec<u32>>::new();
    let handle = sink.handle();
    let mut pipeline = Pipeline::from_source(ChannelSource::new(rx))
        .idle_tick(Duration::from_millis(1))
        .window_with(WindowPolicy::tumbling(secs(10)), clock.clone())
        .map(|w: Window<u32>| w.into_inner())
        .sink(sink);

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(pipeline.step().unwrap(), Step::Item);
    assert_eq!(pipeline.step().unwrap(), Step::Item);
    assert_eq!(pipeline.step().unwrap(), Step::Idle);
    assert!(handle.is_empty(), "window still open");

    clock.advance(secs(10));
    assert_eq!(pipeline.step().unwrap(), Step::Idle);
    assert_eq!(
        handle.take(),
        vec![vec![1, 2]],
        "the tick closed the window"
    );

    drop(tx);
    assert_eq!(pipeline.step().unwrap(), Step::Done);
    assert!(handle.take().is_empty());
}

#[test]
fn keyed_windows_end_to_end() {
    let clock = ManualClock::new(Instant::now());
    let sink = VecSink::<(char, Vec<u32>)>::new();
    let handle = sink.handle();
    Pipeline::from_iter([('a', 1_u32), ('b', 2), ('a', 3), ('c', 4)])
        .window_by_key_with(
            |e: &(char, u32)| e.0,
            WindowPolicy::tumbling(secs(60)).max_keys(2),
            clock,
        )
        .map(|(k, w): (char, Window<(char, u32)>)| (k, w.iter().map(|e| e.1).collect()))
        .sink(sink)
        .run()
        .unwrap();
    // `c` arrives with two keys open and evicts `b`, the least recently
    // active one; the rest flush in order of first appearance.
    assert_eq!(
        handle.take(),
        vec![('b', vec![2]), ('a', vec![1, 3]), ('c', vec![4])]
    );
}
