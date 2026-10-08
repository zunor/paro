// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::{Drain, Iter, Keys, Values, ValuesMut};
use std::collections::HashMap;
use std::hash::Hash;

use super::bytes_for_capacity;
use crate::allocator::MemoryTag;
use crate::memory::{MemoryAccountingClass, MemoryDomain, MemoryError, MemoryGrant, MemoryResult};

/// Grant-accounted `HashMap`.
#[derive(Debug)]
pub struct AccountedHashMap<K, V> {
    inner: HashMap<K, V>,
    grant: MemoryGrant,
    accounted_bytes: usize,
    publication: Option<AccountedHashMapPublication>,
}

#[derive(Debug, Clone, Copy)]
struct AccountedHashMapPublication {
    tag: MemoryTag,
    class: MemoryAccountingClass,
    auto_grow_grant: bool,
}

/// An unpublished replacement table owns its consumed capacity until commit.
/// Any allocation, reconciliation or hashing failure drops the candidate and
/// refunds that capacity while the original table retains its accounting.
struct PendingTable<'a, K, V> {
    // Fields drop in declaration order: free the physical table before its
    // consumed/issued capacity becomes available to another allocation.
    inner: HashMap<K, V>,
    capacity: PendingCapacity<'a>,
}

struct PendingCapacity<'a> {
    grant: &'a MemoryGrant,
    consumed_bytes: usize,
    reserved_floor: usize,
}

impl Drop for PendingCapacity<'_> {
    fn drop(&mut self) {
        self.grant.refund(self.consumed_bytes);
        // Return newly grown unused capacity; pre-existing grant headroom is
        // retained. A failed reserve therefore restores the owner's issued
        // bytes as well as the map's publication and consumed counters.
        self.grant.release_available(
            self.grant
                .reserved_bytes()
                .saturating_sub(self.reserved_floor),
        );
    }
}

impl<K, V> AccountedHashMap<K, V>
where
    K: Eq + Hash,
{
    pub fn new(grant: MemoryGrant) -> Self {
        Self {
            inner: HashMap::new(),
            grant,
            accounted_bytes: 0,
            publication: None,
        }
    }

    pub fn new_with_accounting(
        grant: MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            inner: HashMap::new(),
            grant,
            accounted_bytes: 0,
            publication: Some(AccountedHashMapPublication {
                tag,
                class,
                auto_grow_grant: true,
            }),
        }
    }

    pub fn with_capacity(capacity: usize, grant: MemoryGrant) -> MemoryResult<Self> {
        let mut map = Self::new(grant);
        map.try_reserve(capacity)?;
        Ok(map)
    }

    /// Grow transactionally: on a memory error, entries and capacity are intact.
    /// Keeping the original table until reconciliation completes requires a
    /// temporary old-plus-new capacity budget and rehashes retained entries.
    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        let old_capacity = self.inner.capacity();
        let overflow = || MemoryError::physical_allocation_failed(usize::MAX);
        let target = self
            .inner
            .len()
            .checked_add(additional)
            .ok_or_else(overflow)?;
        if target <= old_capacity {
            return Ok(());
        }

        let estimated_capacity = target.checked_next_power_of_two().ok_or_else(overflow)?;
        let estimated_bytes = estimated_capacity
            .checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or_else(overflow)?;
        let mut candidate = PendingTable {
            inner: HashMap::with_hasher(self.inner.hasher().clone()),
            capacity: PendingCapacity {
                grant: &self.grant,
                consumed_bytes: 0,
                reserved_floor: self.grant.reserved_bytes(),
            },
        };
        self.consume_capacity(estimated_bytes)?;
        candidate.capacity.consumed_bytes = estimated_bytes;
        candidate
            .inner
            .try_reserve(target)
            .map_err(|_| MemoryError::physical_allocation_failed(estimated_bytes))?;
        let actual_bytes = candidate
            .inner
            .capacity()
            .checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or_else(overflow)?;
        if actual_bytes > estimated_bytes {
            self.consume_capacity(actual_bytes - estimated_bytes)?;
        } else if estimated_bytes > actual_bytes {
            self.grant.refund(estimated_bytes - actual_bytes);
        }
        candidate.capacity.consumed_bytes = actual_bytes;

        // No fallible memory operations follow. The replacement has capacity
        // for every retained entry plus the requested additional entries.
        candidate.inner.extend(self.inner.drain());
        let old_table = std::mem::replace(&mut self.inner, candidate.inner);
        drop(old_table);
        self.release_capacity(self.accounted_bytes);
        self.publish_capacity(actual_bytes);
        self.accounted_bytes = actual_bytes;
        candidate.capacity.consumed_bytes = 0;
        candidate.capacity.reserved_floor = candidate.capacity.reserved_floor.max(actual_bytes);
        Ok(())
    }

    pub fn try_insert(&mut self, key: K, value: V) -> MemoryResult<Option<V>> {
        if self.inner.len() == self.inner.capacity() {
            self.try_reserve(1)?;
        }
        Ok(self.inner.insert(key, value))
    }

    pub fn try_get_or_insert_with<F>(&mut self, key: K, value: F) -> MemoryResult<&mut V>
    where
        F: FnOnce() -> V,
    {
        if !self.inner.contains_key(&key) {
            self.try_reserve(1)?;
        }
        Ok(self.inner.entry(key).or_insert_with(value))
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get_mut(key)
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(key)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains_key(key)
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        self.inner.iter()
    }

    pub fn keys(&self) -> Keys<'_, K, V> {
        self.inner.keys()
    }

    pub fn values(&self) -> Values<'_, K, V> {
        self.inner.values()
    }

    pub fn values_mut(&mut self) -> ValuesMut<'_, K, V> {
        self.inner.values_mut()
    }

    pub fn drain(&mut self) -> Drain<'_, K, V> {
        self.inner.drain()
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.inner.retain(f);
    }

    pub fn shrink_to_fit_and_refund(&mut self) {
        self.inner.shrink_to_fit();
        let new_bytes = bytes_for_capacity::<(K, V)>(self.inner.capacity());
        if self.accounted_bytes > new_bytes {
            self.release_capacity(self.accounted_bytes - new_bytes);
        }
        self.accounted_bytes = new_bytes;
    }

    fn consume_capacity(&self, bytes: usize) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        if self
            .publication
            .map(|publication| publication.auto_grow_grant)
            .unwrap_or(false)
            && self.grant.available_bytes() < bytes
        {
            self.grant.grow(bytes - self.grant.available_bytes())?;
        }
        self.grant.try_consume(bytes)
    }

    fn publish_capacity(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let Some(publication) = self.publication else {
            return;
        };
        if let Some(owner) = self.grant.owner() {
            owner.record_allocation(
                self.grant.domain(),
                publication.tag,
                publication.class,
                bytes,
            );
        }
    }

    fn release_capacity(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if let Some(publication) = self.publication {
            if let Some(owner) = self.grant.owner() {
                owner.release_allocation(
                    self.grant.domain(),
                    publication.tag,
                    publication.class,
                    bytes,
                );
            }
        }
        self.grant.refund(bytes);
    }

    #[inline]
    pub fn domain(&self) -> MemoryDomain {
        self.grant.domain()
    }
}

impl<K, V> Drop for AccountedHashMap<K, V> {
    fn drop(&mut self) {
        if self.accounted_bytes > 0 {
            if let Some(publication) = self.publication {
                if let Some(owner) = self.grant.owner() {
                    owner.release_allocation(
                        self.grant.domain(),
                        publication.tag,
                        publication.class,
                        self.accounted_bytes,
                    );
                }
            }
            self.grant.refund(self.accounted_bytes);
        }
        self.accounted_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryOwner;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct TestOwner {
        issued: AtomicUsize,
        published: AtomicUsize,
        leaked: AtomicUsize,
        acquisitions: AtomicUsize,
        peak_issued: AtomicUsize,
        failure: Mutex<Option<(usize, MemoryError)>>,
    }

    impl TestOwner {
        fn fail_after(&self, acquisitions: usize, error: MemoryError) {
            let next = self.acquisitions.load(Ordering::SeqCst) + acquisitions;
            *self.failure.lock().unwrap() = Some((next, error));
        }

        fn assert_dropped(&self) {
            assert_eq!(self.published.load(Ordering::SeqCst), 0);
            assert_eq!(self.issued.load(Ordering::SeqCst), 0);
            assert_eq!(self.leaked.load(Ordering::SeqCst), 0);
        }
    }

    impl MemoryOwner for TestOwner {
        fn acquire_capacity(&self, _domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
            if bytes == 0 {
                return Ok(());
            }
            let acquisition = self.acquisitions.fetch_add(1, Ordering::SeqCst) + 1;
            let mut failure = self.failure.lock().unwrap();
            if failure.as_ref().is_some_and(|(at, _)| *at == acquisition) {
                return Err(failure.take().unwrap().1);
            }
            let issued = self.issued.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.peak_issued.fetch_max(issued, Ordering::SeqCst);
            Ok(())
        }

        fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
            let before = self.issued.fetch_sub(bytes, Ordering::SeqCst);
            assert!(before >= bytes);
        }

        fn record_allocation(
            &self,
            _domain: MemoryDomain,
            _tag: MemoryTag,
            _class: MemoryAccountingClass,
            bytes: usize,
        ) {
            self.published.fetch_add(bytes, Ordering::SeqCst);
        }

        fn release_allocation(
            &self,
            _domain: MemoryDomain,
            _tag: MemoryTag,
            _class: MemoryAccountingClass,
            bytes: usize,
        ) {
            let before = self.published.fetch_sub(bytes, Ordering::SeqCst);
            assert!(before >= bytes);
        }

        fn record_leaked_grant(&self, _domain: MemoryDomain, bytes: usize) {
            self.leaked.fetch_add(bytes, Ordering::SeqCst);
        }
    }

    // Same key/entry width as the three-column nullable i64 set-operation path.
    type Key = ([i64; 3], u8);
    type TestMap = AccountedHashMap<Key, usize>;

    fn key(value: i64) -> Key {
        ([value, value + 1, value + 2], 0b111)
    }

    fn map(owner: Arc<TestOwner>) -> TestMap {
        TestMap::new_with_accounting(
            MemoryGrant::new(0, MemoryDomain::Host, owner).unwrap(),
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        )
    }

    fn failures() -> [MemoryError; 3] {
        [
            MemoryError::quota_exhausted(MemoryDomain::Host, 80, 0),
            // Common has no statement/cancellation dependency. An owner can
            // cancel pending progress by returning its original blocked error.
            MemoryError::blocked("owner canceled the pending capacity request"),
            MemoryError::reclaim_failed("owner reclaim failed during reconciliation"),
        ]
    }

    #[test]
    fn empty_map_refunds_prepaid_capacity_when_actual_table_reconciliation_fails() {
        for error in failures() {
            let owner = Arc::new(TestOwner::default());
            owner.fail_after(2, error.clone());
            let mut map = map(owner.clone());
            assert_eq!(map.try_insert(key(0), 17), Err(error));
            assert_eq!(owner.acquisitions.load(Ordering::SeqCst), 2);
            assert!(map.is_empty());
            assert_eq!(map.capacity(), 0);
            assert_eq!(map.grant.used_bytes(), 0);
            assert_eq!(map.grant.available_bytes(), 0);
            assert_eq!(map.grant.reserved_bytes(), 0);
            assert_eq!(owner.issued.load(Ordering::SeqCst), 0);
            assert_eq!(owner.published.load(Ordering::SeqCst), 0);
            // No unpaid candidate capacity survived: a retry performs a real
            // fallible reserve and can safely publish the same key afterward.
            assert_eq!(map.try_insert(key(0), 17).unwrap(), None);
            assert_eq!(map.get(&key(0)), Some(&17));
            drop(map);
            owner.assert_dropped();
        }
    }

    #[test]
    fn populated_map_preserves_entries_and_capacity_on_prepaid_or_reconciliation_failure() {
        for failure_after in [1, 2] {
            for error in failures() {
                let owner = Arc::new(TestOwner::default());
                let mut map = map(owner.clone());
                map.try_insert(key(0), 100).unwrap();
                let capacity = map.capacity();
                for value in 1..capacity {
                    map.try_insert(key(value as i64), 100 + value).unwrap();
                }
                let before_used = map.grant.used_bytes();
                let before_issued = owner.issued.load(Ordering::SeqCst);
                let before_published = owner.published.load(Ordering::SeqCst);
                owner.fail_after(failure_after, error.clone());
                assert_eq!(map.try_insert(key(capacity as i64), 999), Err(error));
                assert_eq!(map.len(), capacity);
                assert_eq!(map.capacity(), capacity);
                for value in 0..capacity {
                    assert_eq!(map.get(&key(value as i64)), Some(&(100 + value)));
                }
                assert!(!map.contains_key(&key(capacity as i64)));
                assert_eq!(map.grant.used_bytes(), before_used);
                assert_eq!(owner.published.load(Ordering::SeqCst), before_published);
                assert_eq!(owner.issued.load(Ordering::SeqCst), before_issued);
                assert_eq!(owner.leaked.load(Ordering::SeqCst), 0);
                assert_eq!(
                    map.grant.reserved_bytes(),
                    map.grant.used_bytes() + map.grant.available_bytes()
                );
                // Subsequent growth is usable and pays the temporary old+new
                // table budget; the failed reserve did not corrupt the map.
                map.try_insert(key(capacity as i64), 999).unwrap();
                assert_eq!(map.get(&key(capacity as i64)), Some(&999));
                assert_eq!(map.len(), capacity + 1);
                assert!(
                    owner.peak_issued.load(Ordering::SeqCst) >= before_used + map.accounted_bytes
                );
                drop(map);
                owner.assert_dropped();
            }
        }
    }

    #[test]
    fn reconciliation_failure_preserves_preexisting_grant_headroom() {
        let owner = Arc::new(TestOwner::default());
        let entry_bytes = std::mem::size_of::<(Key, usize)>();
        let grant = MemoryGrant::new(entry_bytes, MemoryDomain::Host, owner.clone()).unwrap();
        let mut map = TestMap::new_with_accounting(
            grant,
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        // Prepayment fits the existing grant; only the additional physical
        // capacity requests owner progress and must return the original error.
        let error = MemoryError::blocked("owner canceled reconciliation");
        owner.fail_after(1, error.clone());
        assert_eq!(map.try_insert(key(0), 3), Err(error));
        assert!(map.is_empty());
        assert_eq!(map.capacity(), 0);
        assert_eq!(map.grant.used_bytes(), 0);
        assert_eq!(map.grant.available_bytes(), entry_bytes);
        assert_eq!(map.grant.reserved_bytes(), entry_bytes);
        assert_eq!(owner.issued.load(Ordering::SeqCst), entry_bytes);
        assert_eq!(owner.published.load(Ordering::SeqCst), 0);
        drop(map);
        owner.assert_dropped();
    }

    #[test]
    fn physical_capacity_overflow_refunds_prepaid_capacity_without_an_allocation() {
        let owner = Arc::new(TestOwner::default());
        let grant = MemoryGrant::new(usize::MAX, MemoryDomain::Host, owner.clone()).unwrap();
        let mut map = AccountedHashMap::<u64, u64>::new_with_accounting(
            grant,
            MemoryTag::HashTable,
            MemoryAccountingClass::NonRevocable,
        );
        // The estimate fits usize, but the std table's bucket layout exceeds
        // its allocation limit. This deterministically fails before heap I/O.
        let target = 1usize << (usize::BITS - 5);
        let bytes = target * std::mem::size_of::<(u64, u64)>();
        assert_eq!(
            map.try_reserve(target),
            Err(MemoryError::physical_allocation_failed(bytes))
        );
        assert_eq!(map.capacity(), 0);
        assert_eq!(map.grant.used_bytes(), 0);
        assert_eq!(map.grant.available_bytes(), usize::MAX);
        assert_eq!(owner.published.load(Ordering::SeqCst), 0);
        drop(map);
        owner.assert_dropped();
    }

    #[test]
    fn arithmetic_capacity_overflow_preserves_map_without_requesting_capacity() {
        let owner = Arc::new(TestOwner::default());
        let mut map = map(owner.clone());
        let initial_calls = owner.acquisitions.load(Ordering::SeqCst);
        // Power-of-two rounding, then entry-byte multiplication overflow.
        for target in [usize::MAX, 1usize << (usize::BITS - 1)] {
            assert_eq!(
                map.try_reserve(target),
                Err(MemoryError::physical_allocation_failed(usize::MAX))
            );
            assert_eq!(owner.acquisitions.load(Ordering::SeqCst), initial_calls);
            assert_eq!(map.capacity(), 0);
            assert_eq!(map.grant.used_bytes(), 0);
        }
        map.try_insert(key(0), 7).unwrap();
        let initial_calls = owner.acquisitions.load(Ordering::SeqCst);
        let capacity = map.capacity();
        assert_eq!(
            map.try_reserve(usize::MAX),
            Err(MemoryError::physical_allocation_failed(usize::MAX))
        );
        assert_eq!(map.capacity(), capacity);
        assert_eq!(map.get(&key(0)), Some(&7));
        assert_eq!(owner.acquisitions.load(Ordering::SeqCst), initial_calls);
        drop(map);
        owner.assert_dropped();
    }
}
