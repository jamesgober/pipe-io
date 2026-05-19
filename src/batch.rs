//! Batching primitives: [`BatchPolicy`], [`Batch`], and [`ByteSize`].
//!
//! Batching is inserted into a pipeline via
//! [`crate::PipelineBuilder::batch`]. The carrier type changes from `T`
//! to `Batch<T>` after the call.

use alloc::vec::Vec;
use core::ops::Deref;
use core::slice;

use crate::emit::Emit;
use crate::source::Infallible;
use crate::stage::Stage;

#[cfg(feature = "std")]
use core::time::Duration;
#[cfg(feature = "std")]
use std::time::Instant;

/// Trigger configuration for a batching stage.
///
/// All configured triggers OR together: the batch is flushed as soon
/// as any one of them is satisfied. At least one of `max_items`,
/// `max_bytes`, or `max_age` must be set for the builder to accept
/// the policy.
///
/// # Example
///
/// ```
/// use pipe_io::BatchPolicy;
/// let p = BatchPolicy::new().max_items(1_000);
/// assert_eq!(p.items_limit(), Some(1_000));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchPolicy {
    max_items: Option<usize>,
    max_bytes: Option<usize>,
    #[cfg(feature = "std")]
    max_age: Option<Duration>,
}

impl BatchPolicy {
    /// Construct a new empty policy. At least one trigger must be set
    /// before the policy is used by the builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_items: None,
            max_bytes: None,
            #[cfg(feature = "std")]
            max_age: None,
        }
    }

    /// Set the maximum number of items per batch.
    #[must_use]
    pub const fn max_items(mut self, n: usize) -> Self {
        self.max_items = Some(n);
        self
    }

    /// Set the maximum byte size per batch. Requires the item type to
    /// implement [`ByteSize`].
    #[must_use]
    pub const fn max_bytes(mut self, n: usize) -> Self {
        self.max_bytes = Some(n);
        self
    }

    /// Set the maximum age of a batch (time between first push and
    /// flush). Requires the `std` feature.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[must_use]
    pub const fn max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Current items trigger.
    #[must_use]
    pub const fn items_limit(&self) -> Option<usize> {
        self.max_items
    }

    /// Current byte trigger.
    #[must_use]
    pub const fn bytes_limit(&self) -> Option<usize> {
        self.max_bytes
    }

    /// Current age trigger.
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
}

/// Opt-in trait for items that contribute a measurable byte size to a
/// batch. Required when [`BatchPolicy::max_bytes`] is set.
pub trait ByteSize {
    /// Number of bytes this item contributes to the batch.
    fn byte_size(&self) -> usize;
}

impl ByteSize for &str {
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl ByteSize for alloc::string::String {
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl ByteSize for Vec<u8> {
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl ByteSize for &[u8] {
    fn byte_size(&self) -> usize {
        self.len()
    }
}

/// A flushed group of items produced by the batching stage.
///
/// Deref's to `[T]`, so all slice methods are available. Owned items
/// are exposed via [`Batch::into_inner`].
#[derive(Debug, Clone)]
pub struct Batch<T> {
    items: Vec<T>,
}

impl<T> Batch<T> {
    /// Construct a batch wrapping an existing `Vec`.
    #[must_use]
    pub const fn new(items: Vec<T>) -> Self {
        Self { items }
    }

    /// Number of items in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True if the batch holds no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Slice iterator over the items.
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
// BatchStage: count-and-age trigger Stage<Input = T, Output = Batch<T>>.
// The builder picks BatchStage when policy has no bytes trigger, and
// BatchStageBytes when it does (which requires T: ByteSize).
// ---------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct BatchStage<T> {
    policy: BatchPolicy,
    items: Vec<T>,
    #[cfg(feature = "std")]
    started_at: Option<Instant>,
}

impl<T> BatchStage<T> {
    pub(crate) fn new(policy: BatchPolicy) -> Self {
        let cap = policy.max_items.unwrap_or(64);
        Self {
            policy,
            items: Vec::with_capacity(cap),
            #[cfg(feature = "std")]
            started_at: None,
        }
    }

    fn should_flush(&self) -> bool {
        if let Some(limit) = self.policy.max_items {
            if self.items.len() >= limit {
                return true;
            }
        }
        #[cfg(feature = "std")]
        {
            if let (Some(age), Some(start)) = (self.policy.max_age, self.started_at) {
                if start.elapsed() >= age {
                    return true;
                }
            }
        }
        false
    }

    fn drain(&mut self) -> Batch<T> {
        let cap = self.policy.max_items.unwrap_or(self.items.capacity());
        let items = core::mem::replace(&mut self.items, Vec::with_capacity(cap));
        #[cfg(feature = "std")]
        {
            self.started_at = None;
        }
        Batch::new(items)
    }
}

impl<T> Stage for BatchStage<T>
where
    T: 'static,
{
    type Input = T;
    type Output = Batch<T>;
    type Error = Infallible;

    fn process(
        &mut self,
        item: Self::Input,
        out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error> {
        #[cfg(feature = "std")]
        {
            if self.started_at.is_none() {
                self.started_at = Some(Instant::now());
            }
        }
        self.items.push(item);
        if self.should_flush() {
            let batch = self.drain();
            let _ = out.emit(batch);
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        if !self.items.is_empty() {
            let batch = self.drain();
            let _ = out.emit(batch);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Byte-aware variant. Used when policy.max_bytes is set.
// ---------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct BatchStageBytes<T: ByteSize> {
    policy: BatchPolicy,
    items: Vec<T>,
    bytes: usize,
    #[cfg(feature = "std")]
    started_at: Option<Instant>,
}

impl<T: ByteSize> BatchStageBytes<T> {
    pub(crate) fn new(policy: BatchPolicy) -> Self {
        let cap = policy.max_items.unwrap_or(64);
        Self {
            policy,
            items: Vec::with_capacity(cap),
            bytes: 0,
            #[cfg(feature = "std")]
            started_at: None,
        }
    }

    fn should_flush(&self) -> bool {
        if let Some(limit) = self.policy.max_items {
            if self.items.len() >= limit {
                return true;
            }
        }
        if let Some(limit) = self.policy.max_bytes {
            if self.bytes >= limit {
                return true;
            }
        }
        #[cfg(feature = "std")]
        {
            if let (Some(age), Some(start)) = (self.policy.max_age, self.started_at) {
                if start.elapsed() >= age {
                    return true;
                }
            }
        }
        false
    }

    fn drain(&mut self) -> Batch<T> {
        let cap = self.policy.max_items.unwrap_or(self.items.capacity());
        let items = core::mem::replace(&mut self.items, Vec::with_capacity(cap));
        self.bytes = 0;
        #[cfg(feature = "std")]
        {
            self.started_at = None;
        }
        Batch::new(items)
    }
}

impl<T> Stage for BatchStageBytes<T>
where
    T: ByteSize + 'static,
{
    type Input = T;
    type Output = Batch<T>;
    type Error = Infallible;

    fn process(
        &mut self,
        item: Self::Input,
        out: &mut dyn Emit<Item = Self::Output>,
    ) -> Result<(), Self::Error> {
        #[cfg(feature = "std")]
        {
            if self.started_at.is_none() {
                self.started_at = Some(Instant::now());
            }
        }
        self.bytes += item.byte_size();
        self.items.push(item);
        if self.should_flush() {
            let batch = self.drain();
            let _ = out.emit(batch);
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Emit<Item = Self::Output>) -> Result<(), Self::Error> {
        if !self.items.is_empty() {
            let batch = self.drain();
            let _ = out.emit(batch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emit::EmitError;
    use alloc::vec;

    struct Collect<'a, T> {
        out: &'a mut Vec<Batch<T>>,
    }
    impl<'a, T> Emit for Collect<'a, T> {
        type Item = Batch<T>;
        fn emit(&mut self, b: Batch<T>) -> Result<(), EmitError> {
            self.out.push(b);
            Ok(())
        }
    }

    #[test]
    fn policy_builder_records_triggers() {
        let p = BatchPolicy::new().max_items(10).max_bytes(1024);
        assert_eq!(p.items_limit(), Some(10));
        assert_eq!(p.bytes_limit(), Some(1024));
        assert!(p.has_trigger());
    }

    #[test]
    fn empty_policy_has_no_trigger() {
        let p = BatchPolicy::new();
        assert!(!p.has_trigger());
    }

    #[test]
    fn batch_stage_emits_on_count() {
        let mut stage = BatchStage::<i32>::new(BatchPolicy::new().max_items(3));
        let mut got: Vec<Batch<i32>> = Vec::new();
        let mut emit = Collect { out: &mut got };
        for i in 1..=7 {
            stage.process(i, &mut emit).unwrap();
        }
        stage.flush(&mut emit).unwrap();
        let lens: Vec<_> = got.iter().map(Batch::len).collect();
        assert_eq!(lens, vec![3, 3, 1]);
        let all: Vec<i32> = got.into_iter().flat_map(Batch::into_inner).collect();
        assert_eq!(all, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn byte_size_blanket_impls() {
        let s: &str = "abc";
        assert_eq!(s.byte_size(), 3);
        let owned: alloc::string::String = "abcd".into();
        assert_eq!(owned.byte_size(), 4);
        let v: Vec<u8> = vec![1, 2, 3, 4, 5];
        assert_eq!(v.byte_size(), 5);
        let sl: &[u8] = &[1, 2];
        assert_eq!(sl.byte_size(), 2);
    }

    #[test]
    fn batch_stage_bytes_emits_on_bytes() {
        let policy = BatchPolicy::new().max_bytes(8);
        let mut stage = BatchStageBytes::<alloc::string::String>::new(policy);
        let mut got: Vec<Batch<alloc::string::String>> = Vec::new();
        let mut emit = Collect { out: &mut got };
        stage.process("aa".into(), &mut emit).unwrap();
        stage.process("bb".into(), &mut emit).unwrap();
        stage.process("cccc".into(), &mut emit).unwrap();
        stage.process("dd".into(), &mut emit).unwrap();
        stage.flush(&mut emit).unwrap();
        let lens: Vec<_> = got.iter().map(Batch::len).collect();
        assert_eq!(lens, vec![3, 1]);
    }
}
