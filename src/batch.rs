//! Batching primitives: [`BatchPolicy`], [`Batch`], and [`ByteSize`].
//!
//! Batching is inserted with [`crate::PipelineBuilder::batch`] (count and
//! age triggers) or [`crate::PipelineBuilder::batch_bytes`] (adds the byte
//! trigger, requires [`ByteSize`]). The carrier type changes from `T` to
//! `Batch<T>` after the call.
//!
//! # Memory bound
//!
//! A batching stage holds at most one open batch. With `max_items` set
//! the batch never exceeds `max_items` items; with `max_bytes` set it
//! never exceeds `max_bytes` plus the size of the item that crossed the
//! limit. A policy with only `max_age` is bounded by the arrival rate over
//! that age, so pair it with a count or byte limit when the rate is not
//! under your control.
//!
//! # Age trigger and quiet streams
//!
//! `max_age` is checked when an item arrives and, if the idle tick is
//! enabled ([`crate::PipelineBuilder::idle_tick`]), whenever the source is
//! quiet for one tick interval. Without the idle tick a batch on a stream
//! that stops producing waits for the next item or end of stream.

use alloc::vec::Vec;
use core::ops::Deref;
use core::slice;

#[cfg(feature = "std")]
use core::time::Duration;
#[cfg(feature = "std")]
use std::time::Instant;

use crate::emit::{Emit, EmitError};
use crate::error::{ConfigError, ConfigErrorKind};
use crate::stage::Stage;
use crate::stage_id::StageId;

#[cfg(feature = "std")]
use crate::clock::Clock;

/// Most slots reserved up front for an open batch. A policy with a large
/// `max_items` does not reserve that much memory up front; the batch grows
/// as items arrive.
const MAX_PREALLOC: usize = 1024;

/// Most bytes reserved up front for an open batch (so a batch of large
/// items, or one with only an age trigger, does not reserve 1024 large
/// slots).
const MAX_PREALLOC_BYTES: usize = 64 * 1024;

/// Trigger configuration for a batching stage.
///
/// All configured triggers OR together: the batch is emitted as soon as
/// any one is satisfied. At least one trigger must be set and every set
/// limit must be non-zero; [`BatchPolicy::validate`] checks this, and the
/// builder reports a violation as [`crate::Error::Config`].
///
/// # Example
///
/// ```
/// use pipe_io::BatchPolicy;
///
/// let p = BatchPolicy::new().max_items(1_000);
/// assert_eq!(p.items_limit(), Some(1_000));
/// assert!(p.validate().is_ok());
/// assert!(BatchPolicy::new().max_items(0).validate().is_err());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct BatchPolicy {
    max_items: Option<usize>,
    max_bytes: Option<usize>,
    #[cfg(feature = "std")]
    max_age: Option<Duration>,
}

impl BatchPolicy {
    /// Construct an empty policy. Set at least one trigger before use.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_items: None,
            max_bytes: None,
            #[cfg(feature = "std")]
            max_age: None,
        }
    }

    /// Emit the batch once it holds `n` items. `n` must be at least 1.
    #[must_use]
    pub const fn max_items(mut self, n: usize) -> Self {
        self.max_items = Some(n);
        self
    }

    /// Emit the batch once the summed [`ByteSize`] of its items reaches
    /// `n`. `n` must be at least 1. Requires
    /// [`crate::PipelineBuilder::batch_bytes`].
    #[must_use]
    pub const fn max_bytes(mut self, n: usize) -> Self {
        self.max_bytes = Some(n);
        self
    }

    /// Emit the batch once its first item is at least `age` old. `age`
    /// must be non-zero. See the module docs for how quiet streams are
    /// handled.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[must_use]
    pub const fn max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Configured item-count trigger.
    #[must_use]
    pub const fn items_limit(&self) -> Option<usize> {
        self.max_items
    }

    /// Configured byte trigger.
    #[must_use]
    pub const fn bytes_limit(&self) -> Option<usize> {
        self.max_bytes
    }

    /// Configured age trigger.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[must_use]
    pub const fn age_limit(&self) -> Option<Duration> {
        self.max_age
    }

    /// True if at least one trigger is configured.
    #[must_use]
    pub const fn has_trigger(&self) -> bool {
        if self.max_items.is_some() || self.max_bytes.is_some() {
            return true;
        }
        #[cfg(feature = "std")]
        {
            if self.max_age.is_some() {
                return true;
            }
        }
        false
    }

    /// Check the policy.
    ///
    /// # Errors
    ///
    /// [`ConfigErrorKind::NoBatchTrigger`] when no trigger is set,
    /// [`ConfigErrorKind::ZeroLimit`] for `max_items(0)` or
    /// `max_bytes(0)`, and [`ConfigErrorKind::ZeroDuration`] for a zero
    /// `max_age`. The error is attributed to the stage id `"batch"`.
    pub const fn validate(&self) -> Result<(), ConfigError> {
        match self.check() {
            Ok(()) => Ok(()),
            Err(kind) => Err(ConfigError::new(StageId::new("batch"), kind)),
        }
    }

    pub(crate) const fn check(&self) -> Result<(), ConfigErrorKind> {
        if !self.has_trigger() {
            return Err(ConfigErrorKind::NoBatchTrigger);
        }
        if matches!(self.max_items, Some(0)) {
            return Err(ConfigErrorKind::ZeroLimit { field: "max_items" });
        }
        if matches!(self.max_bytes, Some(0)) {
            return Err(ConfigErrorKind::ZeroLimit { field: "max_bytes" });
        }
        #[cfg(feature = "std")]
        {
            if let Some(age) = self.max_age {
                if age.is_zero() {
                    return Err(ConfigErrorKind::ZeroDuration { field: "max_age" });
                }
            }
        }
        Ok(())
    }
}

/// Opt-in trait for items that contribute a measurable byte size to a
/// batch. Required by [`crate::PipelineBuilder::batch_bytes`].
///
/// Implemented for `str`, `[u8]`, `String`, `Vec<u8>`, `[u8; N]`, and for
/// references, `Box`, `Rc`, `Arc`, and `Cow` of any implementor.
pub trait ByteSize {
    /// Number of bytes this item contributes to the batch.
    fn byte_size(&self) -> usize;
}

impl ByteSize for str {
    #[inline]
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl ByteSize for [u8] {
    #[inline]
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl<const N: usize> ByteSize for [u8; N] {
    #[inline]
    fn byte_size(&self) -> usize {
        N
    }
}

impl ByteSize for alloc::string::String {
    #[inline]
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl ByteSize for Vec<u8> {
    #[inline]
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl<T: ByteSize + ?Sized> ByteSize for &T {
    #[inline]
    fn byte_size(&self) -> usize {
        (**self).byte_size()
    }
}

impl<T: ByteSize + ?Sized> ByteSize for alloc::boxed::Box<T> {
    #[inline]
    fn byte_size(&self) -> usize {
        (**self).byte_size()
    }
}

impl<T: ByteSize + ?Sized> ByteSize for alloc::rc::Rc<T> {
    #[inline]
    fn byte_size(&self) -> usize {
        (**self).byte_size()
    }
}

#[cfg(target_has_atomic = "ptr")]
impl<T: ByteSize + ?Sized> ByteSize for alloc::sync::Arc<T> {
    #[inline]
    fn byte_size(&self) -> usize {
        (**self).byte_size()
    }
}

impl<B> ByteSize for alloc::borrow::Cow<'_, B>
where
    B: ByteSize + alloc::borrow::ToOwned + ?Sized,
{
    #[inline]
    fn byte_size(&self) -> usize {
        (**self).byte_size()
    }
}

/// A group of items emitted by a batching stage, in arrival order.
///
/// Derefs to `[T]`, so every slice method is available. Take ownership of
/// the items with [`Batch::into_inner`] or by iterating by value.
///
/// # Example
///
/// ```
/// use pipe_io::Batch;
///
/// let b = Batch::from(vec![1, 2, 3]);
/// assert_eq!(b.len(), 3);
/// assert_eq!(b.iter().sum::<i32>(), 6);
/// assert_eq!(b.into_inner(), vec![1, 2, 3]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Batch<T> {
    items: Vec<T>,
}

impl<T> Batch<T> {
    /// Wrap an existing `Vec`.
    #[must_use]
    pub const fn new(items: Vec<T>) -> Self {
        Self { items }
    }

    /// Number of items in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True if the batch holds no items. Batches emitted by the pipeline
    /// are never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Iterate over the items by reference.
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.items.iter()
    }

    /// Unwrap and return the inner `Vec`.
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.items
    }
}

impl<T> Deref for Batch<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl<T> AsRef<[T]> for Batch<T> {
    fn as_ref(&self) -> &[T] {
        &self.items
    }
}

impl<T> From<Vec<T>> for Batch<T> {
    fn from(items: Vec<T>) -> Self {
        Self { items }
    }
}

impl<T> From<Batch<T>> for Vec<T> {
    fn from(batch: Batch<T>) -> Self {
        batch.items
    }
}

impl<T> IntoIterator for Batch<T> {
    type Item = T;
    type IntoIter = alloc::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Batch<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

// ---------------------------------------------------------------------
// BatchStage: one implementation for count, byte, and age triggers. The
// byte trigger is enabled by passing a `measure` function, which only the
// `batch_bytes` builder (bound on `T: ByteSize`) can supply.
// ---------------------------------------------------------------------

/// Function that measures an item's byte size.
pub(crate) type Measure<T> = fn(&T) -> usize;

pub(crate) struct BatchStage<T> {
    max_items: usize,
    max_bytes: usize,
    measure: Option<Measure<T>>,
    items: Vec<T>,
    bytes: usize,
    capacity_hint: usize,
    #[cfg(feature = "std")]
    age: Option<AgeTrigger>,
}

#[cfg(feature = "std")]
struct AgeTrigger {
    max_age: Duration,
    clock: alloc::boxed::Box<dyn Clock>,
    opened_at: Option<Instant>,
}

#[cfg(feature = "std")]
impl AgeTrigger {
    fn expired(&self, now: Instant) -> bool {
        self.opened_at
            .is_some_and(|opened| now.saturating_duration_since(opened) >= self.max_age)
    }
}

impl<T> BatchStage<T> {
    /// Build a stage from a policy. The policy is not validated here (the
    /// builder does that); any policy, including an invalid one, produces
    /// a stage without panicking or reserving more than `MAX_PREALLOC`
    /// slots.
    pub(crate) fn new(policy: &BatchPolicy, measure: Option<Measure<T>>) -> Self {
        let max_items = policy.max_items.unwrap_or(usize::MAX).max(1);
        let slots_in_budget = MAX_PREALLOC_BYTES / core::mem::size_of::<T>().max(1);
        let capacity_hint = max_items.min(MAX_PREALLOC).min(slots_in_budget.max(1));
        // Without a byte limit there is nothing to measure.
        let measure = measure.filter(|_| policy.max_bytes.is_some());
        Self {
            max_items,
            max_bytes: policy.max_bytes.unwrap_or(usize::MAX).max(1),
            measure,
            items: Vec::with_capacity(capacity_hint),
            bytes: 0,
            capacity_hint,
            #[cfg(feature = "std")]
            age: None,
        }
    }

    #[cfg(feature = "std")]
    pub(crate) fn with_clock(
        mut self,
        policy: &BatchPolicy,
        clock: alloc::boxed::Box<dyn Clock>,
    ) -> Self {
        self.age = policy.max_age.map(|max_age| AgeTrigger {
            max_age,
            clock,
            opened_at: None,
        });
        self
    }

    fn take_batch(&mut self) -> Batch<T> {
        let items = core::mem::replace(&mut self.items, Vec::with_capacity(self.capacity_hint));
        self.bytes = 0;
        #[cfg(feature = "std")]
        if let Some(age) = self.age.as_mut() {
            age.opened_at = None;
        }
        Batch::new(items)
    }

    fn emit_open_batch(&mut self, out: &mut dyn Emit<Item = Batch<T>>) {
        if self.items.is_empty() {
            return;
        }
        let batch = self.take_batch();
        // `Closed` means a later stage or the sink already failed; the
        // driver reports that error, so the batch is dropped here on
        // purpose.
        match out.emit(batch) {
            Ok(()) | Err(EmitError::Closed) => {}
        }
    }

    fn limit_reached(&self) -> bool {
        self.items.len() >= self.max_items || self.bytes >= self.max_bytes
    }
}

impl<T: 'static> Stage for BatchStage<T> {
    type Input = T;
    type Output = Batch<T>;
    type Error = core::convert::Infallible;

    #[inline]
    fn process(
        &mut self,
        item: Self::Input,
        out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error> {
        if let Some(measure) = self.measure {
            self.bytes = self.bytes.saturating_add(measure(&item));
        }
        self.items.push(item);
        #[cfg(feature = "std")]
        if let Some(age) = self.age.as_mut() {
            let now = age.clock.now();
            if age.opened_at.is_none() {
                age.opened_at = Some(now);
            }
            if age.expired(now) {
                self.emit_open_batch(out);
                return Ok(());
            }
        }
        if self.limit_reached() {
            self.emit_open_batch(out);
        }
        Ok(())
    }

    fn tick(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        #[cfg(feature = "std")]
        {
            let expired = self
                .age
                .as_ref()
                .is_some_and(|age| age.expired(age.clock.now()));
            if expired {
                self.emit_open_batch(out);
            }
        }
        #[cfg(not(feature = "std"))]
        let _no_age_trigger = out;
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        self.emit_open_batch(out);
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
        out: Vec<Batch<T>>,
        closed: bool,
    }
    impl<T> Collect<T> {
        fn new() -> Self {
            Self {
                out: Vec::new(),
                closed: false,
            }
        }
        fn lens(&self) -> Vec<usize> {
            self.out.iter().map(Batch::len).collect()
        }
    }
    impl<T> Emit for Collect<T> {
        type Item = Batch<T>;
        fn emit(&mut self, b: Batch<T>) -> Result<(), EmitError> {
            if self.closed {
                return Err(EmitError::Closed);
            }
            self.out.push(b);
            Ok(())
        }
    }

    #[allow(clippy::ptr_arg)] // must have the exact `fn(&T) -> usize` shape for T = String
    fn string_len(s: &String) -> usize {
        s.len()
    }

    #[test]
    fn test_policy_records_triggers() {
        let p = BatchPolicy::new().max_items(10).max_bytes(1024);
        assert_eq!(p.items_limit(), Some(10));
        assert_eq!(p.bytes_limit(), Some(1024));
        assert!(p.has_trigger());
        assert!(p.validate().is_ok());
    }

    #[test]
    fn test_policy_validation_rejects_bad_limits() {
        assert_eq!(
            BatchPolicy::new().check(),
            Err(ConfigErrorKind::NoBatchTrigger)
        );
        assert!(!BatchPolicy::new().has_trigger());
        assert_eq!(
            BatchPolicy::new().max_items(0).check(),
            Err(ConfigErrorKind::ZeroLimit { field: "max_items" })
        );
        assert_eq!(
            BatchPolicy::new().max_bytes(0).check(),
            Err(ConfigErrorKind::ZeroLimit { field: "max_bytes" })
        );
        let err = BatchPolicy::new().validate().unwrap_err();
        assert_eq!(err.stage(), StageId::new("batch"));
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_policy_validation_rejects_zero_age() {
        let p = BatchPolicy::new().max_age(Duration::ZERO);
        assert!(p.has_trigger());
        assert_eq!(p.age_limit(), Some(Duration::ZERO));
        assert_eq!(
            p.check(),
            Err(ConfigErrorKind::ZeroDuration { field: "max_age" })
        );
        assert!(
            BatchPolicy::new()
                .max_age(Duration::from_millis(1))
                .check()
                .is_ok()
        );
    }

    #[test]
    fn test_batch_stage_emits_on_count() {
        let mut stage = BatchStage::<i32>::new(&BatchPolicy::new().max_items(3), None);
        let mut emit = Collect::new();
        for i in 1..=7 {
            stage.process(i, &mut emit).unwrap();
        }
        assert_eq!(emit.lens(), vec![3, 3]);
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.lens(), vec![3, 3, 1]);
        let all: Vec<i32> = emit.out.into_iter().flat_map(Batch::into_inner).collect();
        assert_eq!(all, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_batch_stage_count_boundary_is_exact() {
        // capacity-1, capacity, capacity+1
        for (n, expected) in [(2_usize, vec![2]), (3, vec![3]), (4, vec![3, 1])] {
            let mut stage = BatchStage::<usize>::new(&BatchPolicy::new().max_items(3), None);
            let mut emit = Collect::new();
            for i in 0..n {
                stage.process(i, &mut emit).unwrap();
            }
            stage.flush(&mut emit).unwrap();
            assert_eq!(emit.lens(), expected, "n = {n}");
        }
    }

    #[test]
    fn test_batch_stage_huge_max_items_does_not_preallocate() {
        let stage = BatchStage::<u64>::new(&BatchPolicy::new().max_items(usize::MAX), None);
        assert!(stage.items.capacity() <= MAX_PREALLOC);
        // Large items reserve at most `MAX_PREALLOC_BYTES`, also when only
        // an age trigger is set (no item limit).
        let big = BatchStage::<[u8; 1024]>::new(&BatchPolicy::new().max_items(usize::MAX), None);
        assert_eq!(big.items.capacity(), MAX_PREALLOC_BYTES / 1024);
        let huge = BatchStage::<[u8; 1 << 20]>::new(&BatchPolicy::new().max_items(8), None);
        assert_eq!(huge.items.capacity(), 1);
        let invalid = BatchStage::<u64>::new(&BatchPolicy::new().max_items(0), None);
        assert_eq!(
            invalid.max_items, 1,
            "invalid zero limit is clamped, never a hang"
        );
    }

    #[test]
    fn test_batch_stage_emits_on_bytes() {
        let policy = BatchPolicy::new().max_bytes(8);
        let mut stage = BatchStage::<String>::new(&policy, Some(string_len));
        let mut emit = Collect::new();
        for s in ["aa", "bb", "cccc", "dd"] {
            stage.process(s.into(), &mut emit).unwrap();
        }
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.lens(), vec![3, 1]);
    }

    #[test]
    fn test_batch_stage_combined_items_and_bytes_triggers() {
        // The count trigger fires first for small items, the byte trigger
        // for large ones.
        let policy = BatchPolicy::new().max_items(3).max_bytes(10);
        let mut stage = BatchStage::<String>::new(&policy, Some(string_len));
        let mut emit = Collect::new();
        for s in ["a", "b", "c", "0123456789", "d"] {
            stage.process(s.into(), &mut emit).unwrap();
        }
        stage.flush(&mut emit).unwrap();
        assert_eq!(emit.lens(), vec![3, 1, 1]);
    }

    #[test]
    fn test_batch_stage_byte_counter_saturates() {
        fn huge(_: &u8) -> usize {
            usize::MAX / 2 + 1
        }
        let policy = BatchPolicy::new().max_items(10).max_bytes(usize::MAX);
        let mut stage = BatchStage::<u8>::new(&policy, Some(huge));
        let mut emit = Collect::new();
        stage.process(1, &mut emit).unwrap();
        assert_eq!(stage.bytes, usize::MAX / 2 + 1);
        // The second item would overflow; the counter saturates at the
        // limit, which emits the batch instead of wrapping around.
        stage.process(2, &mut emit).unwrap();
        assert_eq!(emit.lens(), vec![2]);
        assert_eq!(stage.bytes, 0);
    }

    #[test]
    fn test_batch_stage_closed_downstream_is_not_an_error() {
        let mut stage = BatchStage::<u8>::new(&BatchPolicy::new().max_items(1), None);
        let mut emit = Collect::new();
        emit.closed = true;
        stage.process(1, &mut emit).unwrap();
        assert!(emit.out.is_empty());
        assert!(
            stage.items.is_empty(),
            "batch is handed off even when dropped"
        );
    }

    #[test]
    fn test_flush_on_empty_stage_emits_nothing() {
        let mut stage = BatchStage::<u8>::new(&BatchPolicy::new().max_items(4), None);
        let mut emit = Collect::new();
        stage.flush(&mut emit).unwrap();
        stage.tick(&mut emit).unwrap();
        assert!(emit.out.is_empty());
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_batch_stage_age_trigger_on_item_and_tick() {
        use crate::clock::ManualClock;
        let t0 = Instant::now();
        let clock = ManualClock::new(t0);
        let policy = BatchPolicy::new().max_age(Duration::from_secs(5));
        let mut stage = BatchStage::<u32>::new(&policy, None)
            .with_clock(&policy, alloc::boxed::Box::new(clock.clone()));
        let mut emit = Collect::new();

        stage.process(1, &mut emit).unwrap();
        clock.advance(Duration::from_secs(4));
        stage.process(2, &mut emit).unwrap();
        stage.tick(&mut emit).unwrap();
        assert!(emit.out.is_empty(), "4s < 5s: still open");

        clock.advance(Duration::from_secs(1));
        stage.tick(&mut emit).unwrap();
        assert_eq!(emit.lens(), vec![2], "tick at exactly max_age emits");

        stage.process(3, &mut emit).unwrap();
        clock.advance(Duration::from_secs(5));
        stage.process(4, &mut emit).unwrap();
        assert_eq!(emit.lens(), vec![2, 2], "item arriving at max_age emits");
        stage.tick(&mut emit).unwrap();
        assert_eq!(emit.lens(), vec![2, 2], "nothing open, tick is a no-op");
    }

    #[test]
    fn test_batch_accessors_and_conversions() {
        let b: Batch<i32> = vec![1, 2, 3].into();
        assert!(!b.is_empty());
        assert!(Batch::<i32>::default().is_empty());
        assert_eq!(b.as_ref(), &[1, 2, 3]);
        assert_eq!(b.first(), Some(&1));
        assert_eq!((&b).into_iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(b.clone().into_iter().collect::<Vec<_>>(), vec![1, 2, 3]);
        let v: Vec<i32> = b.clone().into();
        assert_eq!(v, vec![1, 2, 3]);
        assert_eq!(b, Batch::new(vec![1, 2, 3]));
    }

    #[test]
    fn test_byte_size_impls() {
        use alloc::borrow::Cow;
        use alloc::boxed::Box;
        use alloc::rc::Rc;
        assert_eq!("abc".byte_size(), 3);
        assert_eq!(String::from("abcd").byte_size(), 4);
        assert_eq!(vec![1_u8, 2, 3, 4, 5].byte_size(), 5);
        assert_eq!([1_u8, 2][..].byte_size(), 2);
        assert_eq!([0_u8; 7].byte_size(), 7);
        assert_eq!(Box::<str>::from("xy").byte_size(), 2);
        assert_eq!(Rc::<[u8]>::from(&[1_u8, 2, 3][..]).byte_size(), 3);
        assert_eq!(Cow::Borrowed("hello").byte_size(), 5);
        #[cfg(target_has_atomic = "ptr")]
        assert_eq!(alloc::sync::Arc::<str>::from("abc").byte_size(), 3);
    }
}
