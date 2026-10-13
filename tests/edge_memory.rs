//! PIPE-R03 (2.0.0 re-audit): a `buffer` edge must not reserve memory for
//! its whole capacity up front. Measured with a counting global
//! allocator, so this file holds only this test.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use pipe_io::Pipeline;
use pipe_io::sink::NullSink;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to the system allocator unchanged; only counts bytes.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as the caller's.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            let _previous = PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _previous = LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The largest accepted capacity with 4 KiB items would reserve about
/// 4 GiB with a preallocating channel. Only what is in flight may be
/// allocated.
#[test]
fn r03_buffer_memory_follows_items_in_flight_not_capacity() {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let stats = Pipeline::from_iter(0_u32..64)
        .map(|n: u32| [n as u8; 4096])
        .buffer(1 << 20)
        .sink(NullSink::new())
        .run_threaded()
        .unwrap();
    assert_eq!(stats.items_out, 64);
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
    assert!(peak < 16 << 20, "peak allocation {peak} bytes");
}
