//! Windowing primitives: [`Clock`], [`SystemClock`], [`WindowPolicy`],
//! and [`Window<T>`].
//!
//! Windowing groups items by wall-clock time. Three policies are
//! supported:
//!
//! * [`WindowPolicy::Tumbling`] - fixed, non-overlapping windows of
//!   duration `size`.
//! * [`WindowPolicy::Sliding`] - overlapping windows of width `size`
//!   that advance by `slide` each tick. Each item belongs to multiple
//!   active windows, so `T: Clone` is required.
//! * [`WindowPolicy::Session`] - adaptive windows that close after
//!   `idle` of no activity.
//!
//! Windows close on the *next* item arriving after the close condition
//! is satisfied (or at end-of-stream via [`crate::Stage::flush`]). With
//! pure synchronous execution there is no background timer that fires
//! while items are not flowing; if a session window is `idle` and no
//! further items arrive, it closes only when [`crate::Pipeline::run`]
//! reaches end-of-stream and flushes.
//!
//! # Example
//!
//! ```
//! use core::time::Duration;
//! use pipe_io::WindowPolicy;
//!
//! let p = WindowPolicy::Tumbling { size: Duration::from_secs(5) };
//! assert!(matches!(p, WindowPolicy::Tumbling { .. }));
//! ```

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::time::Duration;
use std::time::Instant;

use crate::emit::Emit;
use crate::source::Infallible;
use crate::stage::Stage;

/// Source of wall-clock time used by [`crate::PipelineBuilder::window`].
///
/// The default impl is [`SystemClock`], which wraps
/// [`std::time::Instant::now`]. Custom impls let tests advance time
/// deterministically.
pub trait Clock: Send {
    /// Return the current instant.
    fn now(&self) -> Instant;
}

/// Default [`Clock`] using [`std::time::Instant::now`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Window emission strategy.
///
/// Pass to [`crate::PipelineBuilder::window`] to install a windowing
/// stage. The carrier type changes from `T` to [`Window<T>`] after
/// the call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowPolicy {
    /// Fixed-width, non-overlapping windows. The first window starts
    /// when the first item arrives; subsequent windows are anchored
    /// at multiples of `size` from that point.
    Tumbling {
        /// Width of each window.
        size: Duration,
    },
    /// Overlapping windows of width `size`. A new window starts every
    /// `slide` interval. Each item is duplicated into every active
    /// window, so the item type must be `Clone`.
    Sliding {
        /// Width of each window.
        size: Duration,
        /// Time between successive window starts.
        slide: Duration,
    },
    /// Adaptive window that stays open as long as new items arrive
    /// within `idle`. Closes (and emits) on the next item arriving
    /// after `idle` has elapsed since the last item, or at
    /// end-of-stream.
    Session {
        /// Idle gap after which the window closes.
        idle: Duration,
    },
}

/// A closed window of items with a start and end instant.
///
/// `start <= end` always holds. For tumbling and sliding windows
/// `end - start == size` (modulo end-of-stream flushes). For session
/// windows `end` is the instant of the last item that arrived inside
/// the session.
#[derive(Debug, Clone)]
pub struct Window<T> {
    items: Vec<T>,
    start: Instant,
    end: Instant,
}

impl<T> Window<T> {
    /// Construct a window directly from parts. Primarily for tests
    /// and custom Stage implementations.
    #[must_use]
    pub fn new(items: Vec<T>, start: Instant, end: Instant) -> Self {
        Self { items, start, end }
    }

    /// Items in this window, in arrival order.
    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Number of items in the window.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True if the window holds no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Window start instant.
    #[must_use]
    pub fn start(&self) -> Instant {
        self.start
    }

    /// Window end instant. For tumbling/sliding, this is `start + size`.
    /// For session, this is the timestamp of the last item.
    #[must_use]
    pub fn end(&self) -> Instant {
        self.end
    }

    /// Unwrap the inner item vec.
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.items
    }
}

impl<T> IntoIterator for Window<T> {
    type Item = T;
    type IntoIter = alloc::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Window<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

// ---------------------------------------------------------------------
// Internal stage implementation. Used by PipelineBuilder::window.
// Requires T: Clone universally because sliding windows duplicate
// items across overlapping windows. Tumbling/session do not strictly
// need Clone, but keeping a unified stage type simplifies the builder.
// Consumers with non-Clone item types can use `.batch()` with a
// `max_age` trigger as a substitute for tumbling windows.
// ---------------------------------------------------------------------

struct PendingWindow<T> {
    start: Instant,
    end: Instant,
    items: Vec<T>,
}

pub(crate) struct WindowStage<T: Clone, C: Clock> {
    policy: WindowPolicy,
    clock: C,
    state: WindowState<T>,
}

enum WindowState<T> {
    Tumbling {
        start: Option<Instant>,
        items: Vec<T>,
    },
    Sliding {
        windows: VecDeque<PendingWindow<T>>,
        next_window_start: Option<Instant>,
    },
    Session {
        start: Option<Instant>,
        last_seen: Option<Instant>,
        items: Vec<T>,
    },
}

impl<T: Clone, C: Clock> WindowStage<T, C> {
    pub(crate) fn new(policy: WindowPolicy, clock: C) -> Self {
        let state = match policy {
            WindowPolicy::Tumbling { .. } => WindowState::Tumbling {
                start: None,
                items: Vec::new(),
            },
            WindowPolicy::Sliding { .. } => WindowState::Sliding {
                windows: VecDeque::new(),
                next_window_start: None,
            },
            WindowPolicy::Session { .. } => WindowState::Session {
                start: None,
                last_seen: None,
                items: Vec::new(),
            },
        };
        Self {
            policy,
            clock,
            state,
        }
    }
}

impl<T, C> Stage for WindowStage<T, C>
where
    T: Clone + Send + 'static,
    C: Clock + 'static,
{
    type Input = T;
    type Output = Window<T>;
    type Error = Infallible;

    fn process(
        &mut self,
        item: Self::Input,
        out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error> {
        let now = self.clock.now();
        match (&self.policy, &mut self.state) {
            (WindowPolicy::Tumbling { size }, WindowState::Tumbling { start, items }) => {
                if start.is_none() {
                    *start = Some(now);
                }
                // Advance window boundaries past any elapsed sizes.
                while let Some(s) = *start {
                    if now.saturating_duration_since(s) >= *size {
                        let window_items = core::mem::take(items);
                        let _ = out.emit(Window::new(window_items, s, s + *size));
                        *start = Some(s + *size);
                    } else {
                        break;
                    }
                }
                items.push(item);
            }
            (
                WindowPolicy::Session { idle },
                WindowState::Session {
                    start,
                    last_seen,
                    items,
                },
            ) => {
                if let Some(ls) = *last_seen {
                    if now.saturating_duration_since(ls) > *idle {
                        if let Some(s) = *start {
                            let window_items = core::mem::take(items);
                            let _ = out.emit(Window::new(window_items, s, ls));
                        }
                        *start = Some(now);
                    }
                } else {
                    *start = Some(now);
                }
                *last_seen = Some(now);
                items.push(item);
            }
            (
                WindowPolicy::Sliding { size, slide },
                WindowState::Sliding {
                    windows,
                    next_window_start,
                },
            ) => {
                if next_window_start.is_none() {
                    *next_window_start = Some(now);
                }
                // Spawn new windows whose start is at or before now.
                while let Some(s) = *next_window_start {
                    if s <= now {
                        windows.push_back(PendingWindow {
                            start: s,
                            end: s + *size,
                            items: Vec::new(),
                        });
                        *next_window_start = Some(s + *slide);
                    } else {
                        break;
                    }
                }
                // Emit windows that have already ended.
                while let Some(w) = windows.front() {
                    if w.end <= now {
                        let w = windows.pop_front().expect("front exists");
                        let _ = out.emit(Window::new(w.items, w.start, w.end));
                    } else {
                        break;
                    }
                }
                // Add the new item to all currently-active windows.
                for w in windows.iter_mut() {
                    if w.start <= now && now < w.end {
                        w.items.push(item.clone());
                    }
                }
            }
            _ => unreachable!(
                "policy/state mismatch is impossible by construction; \
                 WindowStage::new enforces alignment"
            ),
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        let now = self.clock.now();
        match &mut self.state {
            WindowState::Tumbling { start, items } => {
                if !items.is_empty() {
                    let s = start.unwrap_or(now);
                    let window_items = core::mem::take(items);
                    let _ = out.emit(Window::new(window_items, s, now));
                }
            }
            WindowState::Session {
                start,
                last_seen,
                items,
            } => {
                if !items.is_empty() {
                    let s = start.unwrap_or(now);
                    let e = last_seen.unwrap_or(now);
                    let window_items = core::mem::take(items);
                    let _ = out.emit(Window::new(window_items, s, e));
                }
            }
            WindowState::Sliding { windows, .. } => {
                while let Some(w) = windows.pop_front() {
                    if !w.items.is_empty() {
                        let _ = out.emit(Window::new(w.items, w.start, w.end));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emit::EmitError;
    use std::sync::{Arc, Mutex};

    // A deterministic Clock for tests. Each call to `now()` returns the
    // current set time. Tests advance time explicitly.
    #[derive(Clone)]
    struct FakeClock {
        inner: Arc<Mutex<Instant>>,
    }

    impl FakeClock {
        fn new(start: Instant) -> Self {
            Self {
                inner: Arc::new(Mutex::new(start)),
            }
        }

        fn advance(&self, by: Duration) {
            let mut g = self.inner.lock().unwrap();
            *g += by;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            *self.inner.lock().unwrap()
        }
    }

    struct Collect<T> {
        out: Vec<Window<T>>,
    }
    impl<T> Emit for Collect<T> {
        type Item = Window<T>;
        fn emit(&mut self, w: Window<T>) -> Result<(), EmitError> {
            self.out.push(w);
            Ok(())
        }
    }

    #[test]
    fn tumbling_emits_on_boundary() {
        let t0 = Instant::now();
        let clock = FakeClock::new(t0);
        let mut stage = WindowStage::<u32, _>::new(
            WindowPolicy::Tumbling {
                size: Duration::from_secs(10),
            },
            clock.clone(),
        );
        let mut emit = Collect::<u32> { out: Vec::new() };

        // t=0: item 1
        stage.process(1, &mut emit).unwrap();
        assert!(emit.out.is_empty());

        // t=5: item 2 (still inside first window)
        clock.advance(Duration::from_secs(5));
        stage.process(2, &mut emit).unwrap();
        assert!(emit.out.is_empty());

        // t=10: item 3 (boundary; emit window [0, 10) with items 1, 2)
        clock.advance(Duration::from_secs(5));
        stage.process(3, &mut emit).unwrap();
        assert_eq!(emit.out.len(), 1);
        assert_eq!(emit.out[0].items(), &[1, 2]);

        // t=20: item 4 (emit window [10, 20) with item 3)
        clock.advance(Duration::from_secs(10));
        stage.process(4, &mut emit).unwrap();
        assert_eq!(emit.out.len(), 2);
        assert_eq!(emit.out[1].items(), &[3]);

        // Flush emits the remaining window with item 4.
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 3);
        assert_eq!(emit.out[2].items(), &[4]);
    }

    #[test]
    fn session_closes_after_idle() {
        let t0 = Instant::now();
        let clock = FakeClock::new(t0);
        let mut stage = WindowStage::<u32, _>::new(
            WindowPolicy::Session {
                idle: Duration::from_secs(5),
            },
            clock.clone(),
        );
        let mut emit = Collect::<u32> { out: Vec::new() };

        // t=0: 1
        stage.process(1, &mut emit).unwrap();
        // t=2: 2 (within session)
        clock.advance(Duration::from_secs(2));
        stage.process(2, &mut emit).unwrap();
        // t=4: 3 (within session)
        clock.advance(Duration::from_secs(2));
        stage.process(3, &mut emit).unwrap();
        assert!(emit.out.is_empty());

        // t=20: 4 (>5s gap, closes prior session, starts new one)
        clock.advance(Duration::from_secs(16));
        stage.process(4, &mut emit).unwrap();
        assert_eq!(emit.out.len(), 1);
        assert_eq!(emit.out[0].items(), &[1, 2, 3]);

        // Flush closes the second session.
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 2);
        assert_eq!(emit.out[1].items(), &[4]);
    }

    #[test]
    fn sliding_overlapping_windows() {
        let t0 = Instant::now();
        let clock = FakeClock::new(t0);
        // size=10s, slide=5s: windows [0,10), [5,15), [10,20), ...
        let mut stage = WindowStage::<u32, _>::new(
            WindowPolicy::Sliding {
                size: Duration::from_secs(10),
                slide: Duration::from_secs(5),
            },
            clock.clone(),
        );
        let mut emit = Collect::<u32> { out: Vec::new() };

        // t=0: item 1. Spawns window [0,10). Item belongs.
        stage.process(1, &mut emit).unwrap();
        // t=3: item 2. Still only window [0,10) active.
        clock.advance(Duration::from_secs(3));
        stage.process(2, &mut emit).unwrap();
        // t=5: item 3. Spawns window [5,15). Item belongs to both [0,10) and [5,15).
        clock.advance(Duration::from_secs(2));
        stage.process(3, &mut emit).unwrap();
        // t=10: item 4. Window [0,10) ends -> emit. Spawn [10,20). Item belongs to [5,15) and [10,20).
        clock.advance(Duration::from_secs(5));
        stage.process(4, &mut emit).unwrap();
        assert_eq!(emit.out.len(), 1);
        assert_eq!(emit.out[0].items(), &[1, 2, 3]);

        // Flush emits remaining windows: [5,15) with {3,4}, [10,20) with {4}.
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 3);
        assert_eq!(emit.out[1].items(), &[3, 4]);
        assert_eq!(emit.out[2].items(), &[4]);
    }

    #[test]
    fn tumbling_flush_emits_partial() {
        let t0 = Instant::now();
        let clock = FakeClock::new(t0);
        let mut stage = WindowStage::<u32, _>::new(
            WindowPolicy::Tumbling {
                size: Duration::from_secs(10),
            },
            clock.clone(),
        );
        let mut emit = Collect::<u32> { out: Vec::new() };

        stage.process(1, &mut emit).unwrap();
        stage.process(2, &mut emit).unwrap();
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 1);
        assert_eq!(emit.out[0].items(), &[1, 2]);
    }

    #[test]
    fn window_into_inner_returns_items() {
        let t = Instant::now();
        let w = Window::new(alloc::vec![10u32, 20, 30], t, t);
        assert_eq!(w.len(), 3);
        assert!(!w.is_empty());
        assert_eq!(w.into_inner(), alloc::vec![10, 20, 30]);
    }
}
