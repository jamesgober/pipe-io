//! Time-based windowing: [`WindowPolicy`], [`WindowSpec`], and
//! [`Window`].
//!
//! Windowing groups items by the time they arrive, read from a
//! [`Clock`]. Three policies are supported:
//!
//! * [`WindowPolicy::Tumbling`]: fixed, non-overlapping windows of width
//!   `size`, aligned to the arrival time of the first item.
//! * [`WindowPolicy::Sliding`]: overlapping windows of width `size`
//!   starting every `slide`, so each item joins `ceil(size / slide)`
//!   windows (`T: Clone` is required for the copies).
//! * [`WindowPolicy::Session`]: a window stays open while items keep
//!   arriving within `idle` of each other.
//!
//! # Emission rules
//!
//! * Only non-empty windows are emitted. A quiet gap of any length costs
//!   O(1): window boundaries skip forward arithmetically instead of
//!   emitting one empty window per elapsed interval.
//! * A window is emitted when an item arrives after its end, when the idle
//!   tick fires after its end ([`crate::PipelineBuilder::idle_tick`]), or
//!   at end of stream. Without the idle tick a window on a stream that
//!   stops producing waits for the next item or end of stream.
//! * [`Window::start`] and [`Window::end`] report the window's nominal
//!   interval (for sessions: first and last item). A window flushed early
//!   (end of stream, or a [`WindowSpec::max_items`] split) still reports
//!   its nominal interval. If the nominal end is not representable as an
//!   [`std::time::Instant`] (for example `size = Duration::MAX`), the
//!   window never closes by time and reports the flush instant as its end.
//!
//! # Memory bound
//!
//! An open window holds every item that arrived inside it. Bound it with
//! [`WindowSpec::max_items`]: a window that reaches the cap is emitted
//! immediately and a fresh window continues the same interval. Keyed
//! windows ([`crate::PipelineBuilder::window_by_key`]) also take
//! [`WindowSpec::max_keys`]; when a new key would exceed it, the key that
//! has been idle longest is flushed and evicted.
//!
//! # Example
//!
//! ```
//! use core::time::Duration;
//! use pipe_io::{WindowPolicy, WindowSpec};
//!
//! let spec = WindowPolicy::tumbling(Duration::from_secs(5)).max_items(10_000);
//! assert!(spec.validate().is_ok());
//! assert!(WindowPolicy::tumbling(Duration::ZERO).validate().is_err());
//! # let _ = WindowSpec::from(WindowPolicy::session(Duration::from_secs(1)));
//! ```

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::hash::Hash;
use core::ops::Deref;
use core::time::Duration;
use std::collections::HashMap;
use std::time::Instant;

pub use crate::clock::{Clock, ManualClock, SystemClock};
use crate::emit::{Emit, EmitError};
use crate::error::{ConfigError, ConfigErrorKind};
use crate::stage::Stage;
use crate::stage_id::StageId;

/// Largest number of overlapping sliding windows an item may join
/// (`ceil(size / slide)`). Larger ratios are rejected by validation
/// because every item is cloned once per window.
pub const MAX_SLIDING_OVERLAP: u64 = 1024;

/// Window emission strategy.
///
/// Prefer the constructors ([`WindowPolicy::tumbling`],
/// [`WindowPolicy::sliding`], [`WindowPolicy::session`]); the variants stay
/// public for pattern matching.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowPolicy {
    /// Fixed-width, non-overlapping windows. Boundaries are multiples of
    /// `size` from the arrival of the first item.
    Tumbling {
        /// Width of each window. Must be non-zero.
        size: Duration,
    },
    /// Overlapping windows of width `size`, one starting every `slide`.
    Sliding {
        /// Width of each window. Must be non-zero.
        size: Duration,
        /// Time between window starts. Must be non-zero, at most `size`,
        /// and `ceil(size / slide)` must not exceed
        /// [`MAX_SLIDING_OVERLAP`].
        slide: Duration,
    },
    /// A window that closes once no item has arrived for more than
    /// `idle`.
    Session {
        /// Inactivity gap that closes the window. Must be non-zero.
        idle: Duration,
    },
}

impl WindowPolicy {
    /// Tumbling windows of width `size`.
    #[must_use]
    pub const fn tumbling(size: Duration) -> Self {
        Self::Tumbling { size }
    }

    /// Sliding windows of width `size`, one starting every `slide`.
    #[must_use]
    pub const fn sliding(size: Duration, slide: Duration) -> Self {
        Self::Sliding { size, slide }
    }

    /// Session windows closed after `idle` without items.
    #[must_use]
    pub const fn session(idle: Duration) -> Self {
        Self::Session { idle }
    }

    /// Cap the number of items per window. Shorthand for
    /// `WindowSpec::from(policy).max_items(n)`.
    #[must_use]
    pub const fn max_items(self, n: usize) -> WindowSpec {
        WindowSpec::new(self).max_items(n)
    }

    /// Cap the number of keys tracked by a keyed window stage. Shorthand
    /// for `WindowSpec::from(policy).max_keys(n)`.
    #[must_use]
    pub const fn max_keys(self, n: usize) -> WindowSpec {
        WindowSpec::new(self).max_keys(n)
    }

    /// Check the policy.
    ///
    /// # Errors
    ///
    /// [`ConfigErrorKind::ZeroDuration`] for a zero `size`, `slide`, or
    /// `idle`; [`ConfigErrorKind::SlideExceedsSize`] and
    /// [`ConfigErrorKind::TooManyOverlappingWindows`] for unsupported
    /// sliding shapes. The error is attributed to the stage id
    /// `"window"`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.check()
            .map_err(|kind| ConfigError::new(StageId::new("window"), kind))
    }

    pub(crate) fn check(&self) -> Result<(), ConfigErrorKind> {
        match *self {
            Self::Tumbling { size } => nonzero(size, "size"),
            Self::Session { idle } => nonzero(idle, "idle"),
            Self::Sliding { size, slide } => {
                nonzero(size, "size")?;
                nonzero(slide, "slide")?;
                if slide > size {
                    return Err(ConfigErrorKind::SlideExceedsSize);
                }
                let overlap = size.as_nanos().div_ceil(slide.as_nanos());
                if overlap > u128::from(MAX_SLIDING_OVERLAP) {
                    return Err(ConfigErrorKind::TooManyOverlappingWindows {
                        overlap: u64::try_from(overlap).unwrap_or(u64::MAX),
                        max: MAX_SLIDING_OVERLAP,
                    });
                }
                Ok(())
            }
        }
    }
}

const fn nonzero(d: Duration, field: &'static str) -> Result<(), ConfigErrorKind> {
    if d.is_zero() {
        Err(ConfigErrorKind::ZeroDuration { field })
    } else {
        Ok(())
    }
}

/// A [`WindowPolicy`] plus memory limits.
///
/// Every builder method that takes a window accepts
/// `impl Into<WindowSpec>`, so a bare [`WindowPolicy`] works wherever a
/// spec is expected.
///
/// # Example
///
/// ```
/// use core::time::Duration;
/// use pipe_io::{WindowPolicy, WindowSpec};
///
/// let spec = WindowSpec::new(WindowPolicy::session(Duration::from_secs(30)))
///     .max_items(50_000)
///     .max_keys(10_000);
/// assert_eq!(spec.items_limit(), Some(50_000));
/// assert_eq!(spec.keys_limit(), Some(10_000));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    policy: WindowPolicy,
    max_items: Option<usize>,
    max_keys: Option<usize>,
}

impl WindowSpec {
    /// Wrap a policy with no limits.
    #[must_use]
    pub const fn new(policy: WindowPolicy) -> Self {
        Self {
            policy,
            max_items: None,
            max_keys: None,
        }
    }

    /// Emit a window as soon as it holds `n` items; a new window then
    /// continues the same interval. `n` must be at least 1.
    #[must_use]
    pub const fn max_items(mut self, n: usize) -> Self {
        self.max_items = Some(n);
        self
    }

    /// For keyed windows: the most keys tracked at once. When a new key
    /// arrives at the limit, the least recently active key is flushed and
    /// evicted. `n` must be at least 1. Ignored by unkeyed windows.
    #[must_use]
    pub const fn max_keys(mut self, n: usize) -> Self {
        self.max_keys = Some(n);
        self
    }

    /// The wrapped policy.
    #[must_use]
    pub const fn policy(&self) -> WindowPolicy {
        self.policy
    }

    /// Configured per-window item cap.
    #[must_use]
    pub const fn items_limit(&self) -> Option<usize> {
        self.max_items
    }

    /// Configured key cap.
    #[must_use]
    pub const fn keys_limit(&self) -> Option<usize> {
        self.max_keys
    }

    /// Check the policy and limits.
    ///
    /// # Errors
    ///
    /// Everything [`WindowPolicy::validate`] reports, plus
    /// [`ConfigErrorKind::ZeroLimit`] for a zero `max_items` or
    /// `max_keys`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.check()
            .map_err(|kind| ConfigError::new(StageId::new("window"), kind))
    }

    pub(crate) fn check(&self) -> Result<(), ConfigErrorKind> {
        self.policy.check()?;
        if self.max_items == Some(0) {
            return Err(ConfigErrorKind::ZeroLimit { field: "max_items" });
        }
        if self.max_keys == Some(0) {
            return Err(ConfigErrorKind::ZeroLimit { field: "max_keys" });
        }
        Ok(())
    }
}

impl From<WindowPolicy> for WindowSpec {
    fn from(policy: WindowPolicy) -> Self {
        Self::new(policy)
    }
}

/// A closed window: its items in arrival order plus its interval.
///
/// Derefs to `[T]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window<T> {
    items: Vec<T>,
    start: Instant,
    end: Instant,
}

impl<T> Window<T> {
    /// Construct a window from parts (for tests and custom stages).
    #[must_use]
    pub const fn new(items: Vec<T>, start: Instant, end: Instant) -> Self {
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

    /// True if the window holds no items. Windows emitted by the pipeline
    /// are never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Iterate over the items by reference.
    pub fn iter(&self) -> core::slice::Iter<'_, T> {
        self.items.iter()
    }

    /// Start of the window's interval (see the module docs).
    #[must_use]
    pub const fn start(&self) -> Instant {
        self.start
    }

    /// End of the window's interval (see the module docs).
    #[must_use]
    pub const fn end(&self) -> Instant {
        self.end
    }

    /// Unwrap the items.
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.items
    }
}

impl<T> Deref for Window<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.items
    }
}

impl<T> AsRef<[T]> for Window<T> {
    fn as_ref(&self) -> &[T] {
        &self.items
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
// Time arithmetic. All of it saturates or uses checked operations so no
// configuration or clock behaviour can panic.
// ---------------------------------------------------------------------

const NANOS_PER_SEC: u128 = 1_000_000_000;

fn duration_from_nanos(nanos: u128) -> Duration {
    let secs = u64::try_from(nanos / NANOS_PER_SEC).unwrap_or(u64::MAX);
    // The remainder is always below one second, so it fits a u32.
    let sub = u32::try_from(nanos % NANOS_PER_SEC).unwrap_or(0);
    Duration::new(secs, sub)
}

/// `origin + n * step` where the caller guarantees the offset does not
/// exceed `now - origin`; falls back to `fallback` if it is somehow not
/// representable.
fn offset(origin: Instant, n: u128, step: Duration, fallback: Instant) -> Instant {
    let nanos = n.saturating_mul(step.as_nanos());
    origin
        .checked_add(duration_from_nanos(nanos))
        .unwrap_or(fallback)
}

/// Emit callback: returns `false` once downstream is closed.
type Sink<'a, T> = dyn FnMut(Window<T>) -> bool + 'a;

/// One open sliding window, identified by its index `k` from the anchor.
struct Slot<T> {
    k: u128,
    start: Instant,
    end: Option<Instant>,
    items: Vec<T>,
}

/// Per-key (or the single unkeyed) window state machine. Each variant
/// carries its own durations, so the state can never disagree with the
/// policy it was built from.
enum Core<T> {
    Tumbling {
        size: Duration,
        start: Option<Instant>,
        items: Vec<T>,
    },
    Sliding {
        size: Duration,
        slide: Duration,
        slots: VecDeque<Slot<T>>,
    },
    Session {
        idle: Duration,
        start: Option<Instant>,
        last: Option<Instant>,
        items: Vec<T>,
    },
}

/// Origin of the window grid for a stage whose first item arrives at
/// `first`. Tumbling windows start at the first item. Sliding windows are
/// anchored `ceil(size / slide) - 1` slides earlier, so the first item
/// joins every window that overlaps its arrival (not just the one starting
/// at it), exactly like any later item. Sessions do not use the grid.
fn grid_origin(policy: WindowPolicy, first: Instant) -> Instant {
    match policy {
        WindowPolicy::Sliding { size, slide } => {
            let overlap = size.as_nanos().div_ceil(slide.as_nanos().max(1));
            let back = overlap.saturating_sub(1).saturating_mul(slide.as_nanos());
            first
                .checked_sub(duration_from_nanos(back))
                .unwrap_or(first)
        }
        WindowPolicy::Tumbling { .. } | WindowPolicy::Session { .. } => first,
    }
}

impl<T: Clone> Core<T> {
    const fn new(policy: WindowPolicy) -> Self {
        match policy {
            WindowPolicy::Tumbling { size } => Self::Tumbling {
                size,
                start: None,
                items: Vec::new(),
            },
            WindowPolicy::Sliding { size, slide } => Self::Sliding {
                size,
                slide,
                slots: VecDeque::new(),
            },
            WindowPolicy::Session { idle } => Self::Session {
                idle,
                start: None,
                last: None,
                items: Vec::new(),
            },
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Tumbling { items, .. } | Self::Session { items, .. } => items.is_empty(),
            Self::Sliding { slots, .. } => slots.iter().all(|s| s.items.is_empty()),
        }
    }

    /// Add `item` arriving at `now`. `anchor` is the stage's grid origin
    /// (shared by all keys so keyed windows align).
    fn on_item(
        &mut self,
        max_items: usize,
        anchor: Instant,
        now: Instant,
        item: T,
        out: &mut Sink<'_, T>,
    ) {
        match self {
            Self::Tumbling { size, start, items } => {
                let size = *size;
                let current = *start.get_or_insert_with(|| align(anchor, now, size));
                if let Some(end) = current.checked_add(size).filter(|end| now >= *end) {
                    if !items.is_empty() && !out(Window::new(core::mem::take(items), current, end))
                    {
                        return;
                    }
                    *start = Some(align(anchor, now, size));
                }
                items.push(item);
                if items.len() >= max_items {
                    let s = start.unwrap_or(now);
                    let e = s.checked_add(size).unwrap_or(now);
                    let _still_open = out(Window::new(core::mem::take(items), s, e));
                }
            }
            Self::Session {
                idle,
                start,
                last,
                items,
            } => {
                match *last {
                    Some(prev) if now.saturating_duration_since(prev) > *idle => {
                        let s = start.unwrap_or(prev);
                        if !items.is_empty() && !out(Window::new(core::mem::take(items), s, prev)) {
                            return;
                        }
                        *start = Some(now);
                    }
                    Some(_) => {}
                    None => *start = Some(now),
                }
                *last = Some(now);
                items.push(item);
                if items.len() >= max_items {
                    let s = start.unwrap_or(now);
                    let _still_open = out(Window::new(core::mem::take(items), s, now));
                    *start = Some(now);
                }
            }
            Self::Sliding { size, slide, slots } => {
                let shape = (*size, *slide);
                sliding_on_item(slots, max_items, shape, anchor, now, item, out);
            }
        }
    }

    /// Emit every window whose end has passed at `now`.
    fn on_tick(&mut self, anchor: Instant, now: Instant, out: &mut Sink<'_, T>) {
        match self {
            Self::Tumbling { size, start, items } => {
                let size = *size;
                let expired = start
                    .and_then(|s| s.checked_add(size).map(|e| (s, e)))
                    .filter(|(_, e)| now >= *e);
                if let Some((s, e)) = expired {
                    if !items.is_empty() {
                        let _still_open = out(Window::new(core::mem::take(items), s, e));
                    }
                    *start = Some(align(anchor, now, size));
                }
            }
            Self::Session {
                idle,
                start,
                last,
                items,
            } => {
                let idle = *idle;
                if let Some(prev) = last.filter(|prev| now.saturating_duration_since(*prev) > idle)
                {
                    if !items.is_empty() {
                        let s = start.unwrap_or(prev);
                        let _still_open = out(Window::new(core::mem::take(items), s, prev));
                    }
                    *start = None;
                    *last = None;
                }
            }
            Self::Sliding { size, slide, slots } => {
                let (k_min, _) = sliding_range(anchor, now, *size, *slide);
                emit_slots_before(slots, k_min, out);
            }
        }
    }

    /// Emit everything that is still open.
    fn on_flush(&mut self, now: Instant, out: &mut Sink<'_, T>) {
        match self {
            Self::Tumbling { size, start, items } => {
                if !items.is_empty() {
                    let s = start.unwrap_or(now);
                    let e = s.checked_add(*size).unwrap_or(now);
                    let _still_open = out(Window::new(core::mem::take(items), s, e));
                }
                *start = None;
            }
            Self::Session {
                start, last, items, ..
            } => {
                if !items.is_empty() {
                    let e = last.unwrap_or(now);
                    let s = start.unwrap_or(e);
                    let _still_open = out(Window::new(core::mem::take(items), s, e));
                }
                *start = None;
                *last = None;
            }
            Self::Sliding { slots, .. } => {
                while let Some(slot) = slots.pop_front() {
                    if !slot.items.is_empty() {
                        let end = slot.end.unwrap_or(now);
                        if !out(Window::new(slot.items, slot.start, end)) {
                            slots.clear();
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Start of the tumbling window containing `now`, given windows anchored
/// at `anchor`. O(1) regardless of how much time has passed.
fn align(anchor: Instant, now: Instant, size: Duration) -> Instant {
    let elapsed = now.saturating_duration_since(anchor).as_nanos();
    let step = size.as_nanos().max(1);
    offset(anchor, elapsed / step, size, now)
}

/// Inclusive range of sliding window indices containing `now`.
fn sliding_range(anchor: Instant, now: Instant, size: Duration, slide: Duration) -> (u128, u128) {
    let e = now.saturating_duration_since(anchor).as_nanos();
    let slide_n = slide.as_nanos().max(1);
    let size_n = size.as_nanos();
    let k_max = e / slide_n;
    let k_min = if e >= size_n {
        (e - size_n) / slide_n + 1
    } else {
        0
    };
    (k_min, k_max)
}

fn emit_slots_before<T>(slots: &mut VecDeque<Slot<T>>, k_min: u128, out: &mut Sink<'_, T>) {
    while slots.front().is_some_and(|s| s.k < k_min) {
        if let Some(slot) = slots.pop_front() {
            if !slot.items.is_empty() {
                let end = slot.end.unwrap_or(slot.start);
                if !out(Window::new(slot.items, slot.start, end)) {
                    return;
                }
            }
        }
    }
}

fn sliding_on_item<T: Clone>(
    slots: &mut VecDeque<Slot<T>>,
    max_items: usize,
    (size, slide): (Duration, Duration),
    anchor: Instant,
    now: Instant,
    item: T,
    out: &mut Sink<'_, T>,
) {
    let (k_min, k_max) = sliding_range(anchor, now, size, slide);
    emit_slots_before(slots, k_min, out);

    // Locate or create every slot k in [k_min, k_max]. The deque is sorted
    // by k, so a single forward pass suffices.
    let mut idx = 0;
    let mut k = k_min;
    let mut item = Some(item);
    while k <= k_max {
        while slots.get(idx).is_some_and(|s| s.k < k) {
            idx += 1;
        }
        if slots.get(idx).is_none_or(|s| s.k != k) {
            let start = offset(anchor, k, slide, now);
            slots.insert(
                idx,
                Slot {
                    k,
                    start,
                    end: start.checked_add(size),
                    items: Vec::new(),
                },
            );
        }
        let value = if k == k_max {
            item.take()
        } else {
            item.clone()
        };
        if let (Some(value), Some(slot)) = (value, slots.get_mut(idx)) {
            slot.items.push(value);
            if slot.items.len() >= max_items {
                let end = slot.end.unwrap_or(now);
                let _open = out(Window::new(
                    core::mem::take(&mut slot.items),
                    slot.start,
                    end,
                ));
            }
        }
        idx += 1;
        k += 1;
    }
}

/// Adapter turning a `&mut dyn Emit` into the closure the cores expect.
fn emit_into<T>(out: &mut dyn Emit<Item = Window<T>>) -> impl FnMut(Window<T>) -> bool + '_ {
    move |w| match out.emit(w) {
        Ok(()) => true,
        // A later stage or the sink failed; the driver reports that error.
        Err(EmitError::Closed) => false,
    }
}

// ---------------------------------------------------------------------
// Unkeyed stage.
// ---------------------------------------------------------------------

pub(crate) struct WindowStage<T, C> {
    policy: WindowPolicy,
    max_items: usize,
    clock: C,
    anchor: Option<Instant>,
    core: Core<T>,
}

impl<T: Clone, C: Clock> WindowStage<T, C> {
    pub(crate) fn new(spec: WindowSpec, clock: C) -> Self {
        Self {
            policy: spec.policy,
            max_items: spec.max_items.unwrap_or(usize::MAX).max(1),
            clock,
            anchor: None,
            core: Core::new(spec.policy),
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
    type Error = core::convert::Infallible;

    fn process(
        &mut self,
        item: T,
        out: &mut dyn Emit<Item = Window<T>>,
    ) -> Result<(), Self::Error> {
        let now = self.clock.now();
        let policy = self.policy;
        let anchor = *self.anchor.get_or_insert_with(|| grid_origin(policy, now));
        self.core
            .on_item(self.max_items, anchor, now, item, &mut emit_into(out));
        Ok(())
    }

    fn tick(&mut self, out: &mut dyn Emit<Item = Window<T>>) -> Result<(), Self::Error> {
        if let Some(anchor) = self.anchor {
            let now = self.clock.now();
            self.core.on_tick(anchor, now, &mut emit_into(out));
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Emit<Item = Window<T>>) -> Result<(), Self::Error> {
        let now = self.clock.now();
        self.core.on_flush(now, &mut emit_into(out));
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Keyed stage: one core per key, emitting `(K, Window<T>)`.
// ---------------------------------------------------------------------

struct KeyState<T> {
    core: Core<T>,
    /// Logical time of the last item, for least-recently-active eviction.
    touched: u64,
    /// Order of first appearance, for deterministic flush order.
    created: u64,
}

pub(crate) struct KeyedWindowStage<T, K, F, C> {
    policy: WindowPolicy,
    max_items: usize,
    max_keys: usize,
    clock: C,
    key_fn: F,
    anchor: Option<Instant>,
    keys: HashMap<K, KeyState<T>>,
    seq: u64,
}

impl<T, K, F, C> KeyedWindowStage<T, K, F, C>
where
    T: Clone,
    K: Hash + Eq + Clone,
    F: FnMut(&T) -> K,
    C: Clock,
{
    pub(crate) fn new(spec: WindowSpec, key_fn: F, clock: C) -> Self {
        Self {
            policy: spec.policy,
            max_items: spec.max_items.unwrap_or(usize::MAX).max(1),
            max_keys: spec.max_keys.unwrap_or(usize::MAX).max(1),
            clock,
            key_fn,
            anchor: None,
            keys: HashMap::new(),
            seq: 0,
        }
    }

    /// Flush and remove the least recently active key. O(keys); only runs
    /// when the key limit is reached.
    fn evict_one(&mut self, now: Instant, out: &mut dyn Emit<Item = (K, Window<T>)>) {
        let victim = self
            .keys
            .iter()
            .min_by_key(|(_, state)| state.touched)
            .map(|(k, _)| k.clone());
        if let Some((key, mut state)) = victim.and_then(|k| self.keys.remove_entry(&k)) {
            state.core.on_flush(now, &mut keyed_emit(&key, out));
        }
    }
}

fn keyed_emit<'a, K: Clone, T>(
    key: &'a K,
    out: &'a mut dyn Emit<Item = (K, Window<T>)>,
) -> impl FnMut(Window<T>) -> bool + 'a {
    move |w| match out.emit((key.clone(), w)) {
        Ok(()) => true,
        // A later stage or the sink failed; the driver reports that error.
        Err(EmitError::Closed) => false,
    }
}

impl<T, K, F, C> Stage for KeyedWindowStage<T, K, F, C>
where
    T: Clone + Send + 'static,
    K: Hash + Eq + Clone + Send + 'static,
    F: FnMut(&T) -> K + Send + 'static,
    C: Clock + 'static,
{
    type Input = T;
    type Output = (K, Window<T>);
    type Error = core::convert::Infallible;

    fn process(
        &mut self,
        item: T,
        out: &mut dyn Emit<Item = (K, Window<T>)>,
    ) -> Result<(), Self::Error> {
        let now = self.clock.now();
        let policy = self.policy;
        let anchor = *self.anchor.get_or_insert_with(|| grid_origin(policy, now));
        let key = (self.key_fn)(&item);
        self.seq = self.seq.wrapping_add(1);
        if !self.keys.contains_key(&key) && self.keys.len() >= self.max_keys {
            self.evict_one(now, out);
        }
        let seq = self.seq;
        let state = self.keys.entry(key.clone()).or_insert_with(|| KeyState {
            core: Core::new(policy),
            touched: seq,
            created: seq,
        });
        state.touched = seq;
        state.core.on_item(
            self.max_items,
            anchor,
            now,
            item,
            &mut keyed_emit(&key, out),
        );
        Ok(())
    }

    fn tick(&mut self, out: &mut dyn Emit<Item = (K, Window<T>)>) -> Result<(), Self::Error> {
        let Some(anchor) = self.anchor else {
            return Ok(());
        };
        let now = self.clock.now();
        self.keys.retain(|key, state| {
            state.core.on_tick(anchor, now, &mut keyed_emit(key, out));
            !state.core.is_empty()
        });
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Emit<Item = (K, Window<T>)>) -> Result<(), Self::Error> {
        let now = self.clock.now();
        let mut states: Vec<(K, KeyState<T>)> = self.keys.drain().collect();
        states.sort_by_key(|(_, s)| s.created);
        for (key, mut state) in states {
            state.core.on_flush(now, &mut keyed_emit(&key, out));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use alloc::string::String;
    use alloc::vec;

    struct Collect<T> {
        out: Vec<T>,
        closed: bool,
    }
    impl<T> Collect<T> {
        const fn new() -> Self {
            Self {
                out: Vec::new(),
                closed: false,
            }
        }
    }
    impl<T> Emit for Collect<T> {
        type Item = T;
        fn emit(&mut self, w: T) -> Result<(), EmitError> {
            if self.closed {
                return Err(EmitError::Closed);
            }
            self.out.push(w);
            Ok(())
        }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn items<T: Clone>(ws: &[Window<T>]) -> Vec<Vec<T>> {
        ws.iter().map(|w| w.items().to_vec()).collect()
    }

    fn stage(policy: WindowPolicy, clock: &ManualClock) -> WindowStage<u32, ManualClock> {
        WindowStage::new(WindowSpec::new(policy), clock.clone())
    }

    #[test]
    fn test_policy_validation() {
        assert!(WindowPolicy::tumbling(secs(1)).validate().is_ok());
        assert_eq!(
            WindowPolicy::tumbling(Duration::ZERO).check(),
            Err(ConfigErrorKind::ZeroDuration { field: "size" })
        );
        assert_eq!(
            WindowPolicy::session(Duration::ZERO).check(),
            Err(ConfigErrorKind::ZeroDuration { field: "idle" })
        );
        assert_eq!(
            WindowPolicy::sliding(Duration::ZERO, secs(1)).check(),
            Err(ConfigErrorKind::ZeroDuration { field: "size" })
        );
        assert_eq!(
            WindowPolicy::sliding(secs(1), Duration::ZERO).check(),
            Err(ConfigErrorKind::ZeroDuration { field: "slide" })
        );
        assert_eq!(
            WindowPolicy::sliding(secs(1), secs(2)).check(),
            Err(ConfigErrorKind::SlideExceedsSize)
        );
        assert!(WindowPolicy::sliding(secs(1), secs(1)).check().is_ok());
        assert!(
            WindowPolicy::sliding(Duration::from_nanos(1024), Duration::from_nanos(1))
                .check()
                .is_ok()
        );
        assert_eq!(
            WindowPolicy::sliding(Duration::from_nanos(1025), Duration::from_nanos(1)).check(),
            Err(ConfigErrorKind::TooManyOverlappingWindows {
                overlap: 1025,
                max: MAX_SLIDING_OVERLAP
            })
        );
        assert_eq!(
            WindowPolicy::sliding(Duration::MAX, Duration::from_nanos(1)).check(),
            Err(ConfigErrorKind::TooManyOverlappingWindows {
                overlap: u64::MAX,
                max: MAX_SLIDING_OVERLAP
            })
        );
        let err = WindowPolicy::tumbling(Duration::ZERO)
            .validate()
            .unwrap_err();
        assert_eq!(err.stage(), StageId::new("window"));
    }

    #[test]
    fn test_spec_validation_and_accessors() {
        let spec = WindowPolicy::tumbling(secs(1)).max_items(5).max_keys(2);
        assert_eq!(spec.policy(), WindowPolicy::tumbling(secs(1)));
        assert_eq!(spec.items_limit(), Some(5));
        assert_eq!(spec.keys_limit(), Some(2));
        assert!(spec.validate().is_ok());
        assert_eq!(
            WindowPolicy::tumbling(secs(1)).max_items(0).check(),
            Err(ConfigErrorKind::ZeroLimit { field: "max_items" })
        );
        assert_eq!(
            WindowSpec::new(WindowPolicy::tumbling(secs(1)))
                .max_keys(0)
                .check(),
            Err(ConfigErrorKind::ZeroLimit { field: "max_keys" })
        );
        assert!(
            WindowSpec::new(WindowPolicy::tumbling(Duration::ZERO))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn test_tumbling_emits_on_boundary() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::tumbling(secs(10)), &clock);
        let mut emit = Collect::new();

        s.process(1, &mut emit).unwrap();
        clock.advance(secs(5));
        s.process(2, &mut emit).unwrap();
        assert!(emit.out.is_empty());

        clock.advance(secs(5)); // t=10: exactly on the boundary
        s.process(3, &mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2]]);
        assert_eq!(emit.out[0].start(), t0);
        assert_eq!(emit.out[0].end(), t0 + secs(10));

        clock.advance(secs(10));
        s.process(4, &mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![3]]);

        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![3], vec![4]]);
        assert_eq!(emit.out[2].start(), t0 + secs(20));
        assert_eq!(
            emit.out[2].end(),
            t0 + secs(30),
            "flush reports the nominal end"
        );
    }

    #[test]
    fn test_tumbling_long_gap_skips_empty_windows() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::tumbling(Duration::from_micros(1)), &clock);
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        clock.advance(Duration::from_secs(3600)); // 3.6e9 elapsed intervals
        s.process(2, &mut emit).unwrap();
        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1], vec![2]]);
        assert_eq!(emit.out[1].start(), t0 + Duration::from_secs(3600));
    }

    #[test]
    fn test_tumbling_windows_stay_aligned_after_gap() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::tumbling(secs(10)), &clock);
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        clock.advance(Duration::from_millis(37_500));
        s.process(2, &mut emit).unwrap();
        s.flush(&mut emit).unwrap();
        assert_eq!(emit.out[1].start(), t0 + secs(30));
        assert_eq!(emit.out[1].end(), t0 + secs(40));
    }

    #[test]
    fn test_tumbling_duration_max_never_overflows() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::tumbling(Duration::MAX), &clock);
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        clock.advance(secs(1_000_000));
        s.process(2, &mut emit).unwrap();
        s.tick(&mut emit).unwrap();
        assert!(emit.out.is_empty());
        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2]]);
        assert_eq!(
            emit.out[0].end(),
            t0 + secs(1_000_000),
            "unrepresentable end -> flush time"
        );
    }

    #[test]
    fn test_tumbling_tick_closes_quiet_window() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::tumbling(secs(10)), &clock);
        let mut emit = Collect::new();
        s.tick(&mut emit).unwrap(); // before any item: no-op
        s.process(1, &mut emit).unwrap();
        clock.advance(secs(9));
        s.tick(&mut emit).unwrap();
        assert!(emit.out.is_empty());
        clock.advance(secs(1));
        s.tick(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1]]);
        clock.advance(secs(100));
        s.tick(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 1, "no empty windows from ticks");
        s.process(2, &mut emit).unwrap();
        s.flush(&mut emit).unwrap();
        assert_eq!(emit.out[1].start(), t0 + secs(110));
    }

    #[test]
    fn test_tumbling_max_items_splits_window() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = WindowStage::<u32, _>::new(
            WindowPolicy::tumbling(secs(10)).max_items(2),
            clock.clone(),
        );
        let mut emit = Collect::new();
        for i in 1..=5 {
            s.process(i, &mut emit).unwrap();
        }
        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![3, 4], vec![5]]);
        assert!(
            emit.out
                .iter()
                .all(|w| w.start() == t0 && w.end() == t0 + secs(10))
        );
    }

    #[test]
    fn test_session_closes_after_idle() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::session(secs(5)), &clock);
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        clock.advance(secs(2));
        s.process(2, &mut emit).unwrap();
        clock.advance(secs(5)); // exactly idle: still the same session
        s.process(3, &mut emit).unwrap();
        assert!(emit.out.is_empty());
        clock.advance(Duration::from_millis(5_001));
        s.process(4, &mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2, 3]]);
        assert_eq!(emit.out[0].start(), t0);
        assert_eq!(emit.out[0].end(), t0 + secs(7));
        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2, 3], vec![4]]);
    }

    #[test]
    fn test_session_tick_and_cap() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s =
            WindowStage::<u32, _>::new(WindowPolicy::session(secs(5)).max_items(2), clock.clone());
        let mut emit = Collect::new();
        s.tick(&mut emit).unwrap();
        for i in 1..=3 {
            s.process(i, &mut emit).unwrap();
        }
        assert_eq!(items(&emit.out), vec![vec![1, 2]]);
        clock.advance(secs(5));
        s.tick(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 1, "exactly idle does not close");
        clock.advance(secs(1));
        s.tick(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![3]]);
        s.flush(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 2);
    }

    #[test]
    fn test_sliding_overlapping_windows() {
        // size 10, slide 5: the grid is anchored one slide before the
        // first item, so every item joins exactly two windows.
        let t0 = Instant::now() + secs(60);
        let clock = ManualClock::new(t0);
        let mut s = stage(WindowPolicy::sliding(secs(10), secs(5)), &clock);
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap(); // t=0  -> [-5,5) [0,10)
        clock.advance(secs(3));
        s.process(2, &mut emit).unwrap(); // t=3  -> [-5,5) [0,10)
        clock.advance(secs(2));
        s.process(3, &mut emit).unwrap(); // t=5  -> [0,10) [5,15); [-5,5) closes
        assert_eq!(items(&emit.out), vec![vec![1, 2]]);
        assert_eq!(emit.out[0].start(), t0 - secs(5));
        assert_eq!(emit.out[0].end(), t0 + secs(5));
        clock.advance(secs(5));
        s.process(4, &mut emit).unwrap(); // t=10 -> [5,15) [10,20); [0,10) closes
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![1, 2, 3]]);
        assert_eq!(emit.out[1].start(), t0);
        assert_eq!(emit.out[1].end(), t0 + secs(10));
        s.flush(&mut emit).unwrap();
        assert_eq!(
            items(&emit.out),
            vec![vec![1, 2], vec![1, 2, 3], vec![3, 4], vec![4]]
        );
        assert_eq!(emit.out[3].start(), t0 + secs(10));
    }

    #[test]
    fn test_sliding_gap_does_not_flood() {
        let t0 = Instant::now() + secs(60);
        let clock = ManualClock::new(t0);
        let mut s = stage(
            WindowPolicy::sliding(Duration::from_micros(2), Duration::from_micros(1)),
            &clock,
        );
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        clock.advance(secs(1));
        s.process(2, &mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1], vec![1]]);
        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1], vec![1], vec![2], vec![2]]);
    }

    #[test]
    fn test_sliding_tick_emits_expired_and_cap_splits() {
        // size 4, slide 2, anchored at t=-2: windows [-2,2) [0,4) [2,6) ...
        let t0 = Instant::now() + secs(60);
        let clock = ManualClock::new(t0);
        let mut s = WindowStage::<u32, _>::new(
            WindowPolicy::sliding(secs(4), secs(2)).max_items(2),
            clock.clone(),
        );
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        s.process(2, &mut emit).unwrap(); // both open windows reach the cap
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![1, 2]]);
        clock.advance(secs(2));
        s.process(3, &mut emit).unwrap(); // [0,4) and [2,6)
        clock.advance(secs(2));
        s.tick(&mut emit).unwrap(); // t=4: [0,4) closes
        assert_eq!(items(&emit.out), vec![vec![1, 2], vec![1, 2], vec![3]]);
        assert_eq!(emit.out[2].end(), t0 + secs(4));
        clock.advance(secs(10));
        s.tick(&mut emit).unwrap();
        assert_eq!(
            items(&emit.out),
            vec![vec![1, 2], vec![1, 2], vec![3], vec![3]]
        );
        s.flush(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 4);
    }

    #[test]
    fn test_sliding_duration_overflow_is_safe() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let big = Duration::MAX / 2;
        let mut s = stage(WindowPolicy::sliding(Duration::MAX, big), &clock);
        let mut emit = Collect::new();
        s.process(1, &mut emit).unwrap();
        clock.advance(secs(5));
        s.process(2, &mut emit).unwrap();
        s.flush(&mut emit).unwrap();
        assert_eq!(items(&emit.out), vec![vec![1, 2]]);
    }

    #[test]
    fn test_clock_going_backwards_is_tolerated() {
        let t0 = Instant::now() + secs(100);
        let clock = ManualClock::new(t0);
        for policy in [
            WindowPolicy::tumbling(secs(10)),
            WindowPolicy::sliding(secs(10), secs(5)),
            WindowPolicy::session(secs(5)),
        ] {
            clock.set(t0);
            let mut s = stage(policy, &clock);
            let mut emit = Collect::new();
            s.process(1, &mut emit).unwrap();
            clock.advance(secs(30));
            s.process(2, &mut emit).unwrap();
            clock.set(t0 - secs(50));
            s.process(3, &mut emit).unwrap();
            s.tick(&mut emit).unwrap();
            s.flush(&mut emit).unwrap();
            let mut all: Vec<u32> = emit.out.iter().flat_map(|w| w.iter().copied()).collect();
            all.sort_unstable();
            all.dedup();
            assert_eq!(all, vec![1, 2, 3], "{policy:?}");
        }
    }

    #[test]
    fn test_closed_downstream_stops_emission() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        for policy in [
            WindowPolicy::tumbling(secs(1)),
            WindowPolicy::sliding(secs(2), secs(1)),
            WindowPolicy::session(secs(1)),
        ] {
            let mut s = stage(policy, &clock);
            let mut emit = Collect::new();
            emit.closed = true;
            s.process(1, &mut emit).unwrap();
            clock.advance(secs(5));
            s.process(2, &mut emit).unwrap();
            s.tick(&mut emit).unwrap();
            s.flush(&mut emit).unwrap();
            assert!(emit.out.is_empty());
        }
    }

    #[test]
    fn test_window_accessors() {
        let t = Instant::now();
        let w = Window::new(vec![10_u32, 20, 30], t, t + secs(1));
        assert_eq!(w.len(), 3);
        assert!(!w.is_empty());
        assert!(Window::<u32>::new(Vec::new(), t, t).is_empty());
        assert_eq!(&*w, &[10, 20, 30]);
        assert_eq!(w.as_ref(), &[10, 20, 30]);
        assert_eq!(w.iter().sum::<u32>(), 60);
        assert_eq!((&w).into_iter().count(), 3);
        assert_eq!(w.clone().into_iter().collect::<Vec<_>>(), vec![10, 20, 30]);
        assert_eq!(w.clone(), w);
        assert_eq!(w.into_inner(), vec![10, 20, 30]);
    }

    #[test]
    fn test_keyed_windows_per_key_and_eviction() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let spec = WindowPolicy::tumbling(secs(10)).max_keys(2);
        let mut s = KeyedWindowStage::new(spec, |s: &String| s.as_bytes()[0], clock.clone());
        let mut emit = Collect::new();
        for v in ["a1", "b1", "a2"] {
            s.process(v.into(), &mut emit).unwrap();
        }
        assert!(emit.out.is_empty());
        // A third key evicts the least recently active key, `b`.
        s.process("c1".into(), &mut emit).unwrap();
        assert_eq!(emit.out.len(), 1);
        assert_eq!(emit.out[0].0, b'b');
        clock.advance(secs(10));
        s.process("a3".into(), &mut emit).unwrap();
        assert_eq!(emit.out[1].0, b'a');
        assert_eq!(emit.out[1].1.items(), &["a1".to_owned(), "a2".to_owned()]);
        s.flush(&mut emit).unwrap();
        let keys: Vec<u8> = emit.out.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            keys,
            vec![b'b', b'a', b'a', b'c'],
            "flush order follows first appearance"
        );
    }

    #[test]
    fn test_keyed_windows_align_across_keys_and_tick_releases_keys() {
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let mut s = KeyedWindowStage::new(
            WindowSpec::new(WindowPolicy::tumbling(secs(10))),
            |v: &u32| v % 2,
            clock.clone(),
        );
        let mut emit = Collect::new();
        s.tick(&mut emit).unwrap();
        s.process(2, &mut emit).unwrap();
        clock.advance(secs(7));
        s.process(3, &mut emit).unwrap();
        clock.advance(secs(3));
        s.tick(&mut emit).unwrap();
        assert_eq!(emit.out.len(), 2);
        assert!(
            emit.out
                .iter()
                .all(|(_, w)| w.start() == t0 && w.end() == t0 + secs(10))
        );
        assert!(s.keys.is_empty(), "keys with nothing buffered are released");
    }
}
