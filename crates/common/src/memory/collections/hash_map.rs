// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::{Drain, Iter, Keys, Values, ValuesMut};
use std::collections::HashMap;
use std::hash::Hash;

use super::bytes_for_capacity;
use crate::allocator::MemoryTag;
use crate::memory::{MemoryAccountingClass, MemoryDomain, MemoryGrant, MemoryResult};

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

    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        let old_capacity = self.inner.capacity();
        let target = self.inner.len().saturating_add(additional);
        if target <= old_capacity {
            return Ok(());
        }

        let estimated_capacity = target.next_power_of_two();
        let estimated_bytes = bytes_for_capacity::<(K, V)>(estimated_capacity);
        let delta = estimated_bytes.saturating_sub(self.accounted_bytes);
        self.consume_capacity(delta)?;
        match self.inner.try_reserve(additional) {
            Ok(()) => {
                self.reconcile_capacity(estimated_bytes, self.accounted_bytes)?;
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

    fn reconcile_capacity(&mut self, prepaid_bytes: usize, old_bytes: usize) -> MemoryResult<()> {
        let actual_bytes = bytes_for_capacity::<(K, V)>(self.inner.capacity());
        if actual_bytes > prepaid_bytes {
            self.consume_capacity(actual_bytes - prepaid_bytes)?;
        } else if prepaid_bytes > actual_bytes {
            self.grant.refund(prepaid_bytes - actual_bytes);
        }
        self.publish_capacity(actual_bytes.saturating_sub(old_bytes));
        self.accounted_bytes = actual_bytes;
        Ok(())
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
