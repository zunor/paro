// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::RandomState;
use std::collections::hash_set::{Drain, Iter};
use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};

use super::bytes_for_capacity;
use crate::allocator::MemoryTag;
use crate::memory::{MemoryAccountingClass, MemoryGrant, MemoryResult};

/// Grant-accounted `HashSet`.
#[derive(Debug)]
pub struct AccountedHashSet<T, S = RandomState> {
    inner: HashSet<T, S>,
    grant: MemoryGrant,
    accounted_bytes: usize,
    publication: Option<AccountedHashSetPublication>,
}

#[derive(Debug, Clone, Copy)]
struct AccountedHashSetPublication {
    tag: MemoryTag,
    class: MemoryAccountingClass,
    auto_grow_grant: bool,
}

/// Build hasher for keys that already feed a high-quality u64 hash.
#[derive(Debug, Default, Clone, Copy)]
pub struct PrecomputedHashBuildHasher;

#[derive(Debug, Default)]
pub struct PrecomputedHashHasher {
    hash: u64,
    initialized: bool,
}

impl BuildHasher for PrecomputedHashBuildHasher {
    type Hasher = PrecomputedHashHasher;

    fn build_hasher(&self) -> Self::Hasher {
        PrecomputedHashHasher::default()
    }
}

impl Hasher for PrecomputedHashHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.initialized {
            self.hash
        } else {
            0xcbf2_9ce4_8422_2325
        };
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.hash = hash;
        self.initialized = true;
    }

    fn write_u64(&mut self, value: u64) {
        self.hash = value;
        self.initialized = true;
    }
}

impl<T> AccountedHashSet<T>
where
    T: Eq + Hash,
{
    pub fn new(grant: MemoryGrant) -> Self {
        Self::new_with_hasher(grant, RandomState::new())
    }

    pub fn new_with_accounting(
        grant: MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self::new_with_accounting_and_hasher(grant, tag, class, RandomState::new())
    }

    pub fn with_capacity(capacity: usize, grant: MemoryGrant) -> MemoryResult<Self> {
        let mut set = Self::new(grant);
        set.try_reserve(capacity)?;
        Ok(set)
    }
}

impl<T, S> AccountedHashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    pub fn new_with_hasher(grant: MemoryGrant, hasher: S) -> Self {
        Self {
            inner: HashSet::with_hasher(hasher),
            grant,
            accounted_bytes: 0,
            publication: None,
        }
    }

    pub fn new_with_accounting_and_hasher(
        grant: MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        hasher: S,
    ) -> Self {
        Self {
            inner: HashSet::with_hasher(hasher),
            grant,
            accounted_bytes: 0,
            publication: Some(AccountedHashSetPublication {
                tag,
                class,
                auto_grow_grant: true,
            }),
        }
    }

    pub fn with_capacity_and_hasher(
        capacity: usize,
        grant: MemoryGrant,
        hasher: S,
    ) -> MemoryResult<Self> {
        let mut set = Self::new_with_hasher(grant, hasher);
        set.try_reserve(capacity)?;
        Ok(set)
    }

    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        let target = self.inner.len().saturating_add(additional);
        if target <= self.inner.capacity() {
            return Ok(());
        }
        let estimated = bytes_for_capacity::<T>(target.next_power_of_two());
        let delta = estimated.saturating_sub(self.accounted_bytes);
        self.consume_capacity(delta)?;
        match self.inner.try_reserve(additional) {
            Ok(()) => {
                let actual = bytes_for_capacity::<T>(self.inner.capacity());
                if actual > estimated {
                    self.consume_capacity(actual - estimated)?;
                } else if estimated > actual {
                    self.grant.refund(estimated - actual);
                }
                self.publish_capacity(actual.saturating_sub(self.accounted_bytes));
                self.accounted_bytes = actual;
                Ok(())
            }
            Err(_) => {
                self.grant.refund(delta);
                Err(crate::memory::MemoryError::physical_allocation_failed(
                    delta,
                ))
            }
        }
    }

    pub fn try_insert(&mut self, value: T) -> MemoryResult<bool> {
        if self.inner.len() == self.inner.capacity() {
            self.try_reserve(1)?;
        }
        Ok(self.inner.insert(value))
    }

    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains(value)
    }

    pub fn remove<Q>(&mut self, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(value)
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

    pub fn iter(&self) -> Iter<'_, T> {
        self.inner.iter()
    }

    pub fn drain(&mut self) -> Drain<'_, T> {
        self.inner.drain()
    }

    pub fn shrink_to_fit_and_refund(&mut self) {
        self.inner.shrink_to_fit();
        let new_bytes = bytes_for_capacity::<T>(self.inner.capacity());
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
}

impl<T, S> Drop for AccountedHashSet<T, S> {
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
