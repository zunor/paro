// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};

use super::bytes_for_capacity;
use crate::allocator::MemoryTag;
use crate::memory::{MemoryAccountingClass, MemoryDomain, MemoryGrant, MemoryResult};

/// Grant-accounted `Vec`.
#[derive(Debug)]
pub struct AccountedVec<T> {
    inner: Vec<T>,
    grant: MemoryGrant,
    accounted_bytes: usize,
    publication: Option<AccountedVecPublication>,
}

#[derive(Debug, Clone, Copy)]
struct AccountedVecPublication {
    tag: MemoryTag,
    class: MemoryAccountingClass,
    auto_grow_grant: bool,
}

impl<T> AccountedVec<T> {
    pub fn new(grant: MemoryGrant) -> Self {
        Self {
            inner: Vec::new(),
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
            inner: Vec::new(),
            grant,
            accounted_bytes: 0,
            publication: Some(AccountedVecPublication {
                tag,
                class,
                auto_grow_grant: true,
            }),
        }
    }

    pub fn with_capacity(capacity: usize, grant: MemoryGrant) -> MemoryResult<Self> {
        let mut vec = Self::new(grant);
        vec.try_reserve(capacity)?;
        Ok(vec)
    }

    #[inline]
    pub fn grant(&self) -> &MemoryGrant {
        &self.grant
    }

    #[inline]
    pub fn domain(&self) -> MemoryDomain {
        self.grant.domain()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        let required = self.inner.len().saturating_add(additional);
        if required <= self.inner.capacity() || std::mem::size_of::<T>() == 0 {
            return Ok(());
        }

        let old_bytes = self.accounted_bytes;
        let new_bytes = bytes_for_capacity::<T>(required);
        let delta = new_bytes.saturating_sub(old_bytes);
        self.consume_capacity(delta)?;
        match self
            .inner
            .try_reserve_exact(required.saturating_sub(self.inner.capacity()))
        {
            Ok(()) => {
                let actual_bytes = bytes_for_capacity::<T>(self.inner.capacity());
                if actual_bytes > new_bytes {
                    self.consume_capacity(actual_bytes - new_bytes)?;
                } else if new_bytes > actual_bytes {
                    self.grant.refund(new_bytes - actual_bytes);
                }
                self.publish_capacity(actual_bytes.saturating_sub(old_bytes));
                self.accounted_bytes = actual_bytes;
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

    pub fn try_push(&mut self, value: T) -> MemoryResult<()> {
        if self.inner.len() == self.inner.capacity() {
            self.try_reserve(1)?;
        }
        self.inner.push(value);
        Ok(())
    }

    pub fn try_extend<I>(&mut self, iter: I) -> MemoryResult<()>
    where
        I: IntoIterator<Item = T>,
    {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        self.try_reserve(lower)?;
        for item in iterator {
            self.try_push(item)?;
        }
        Ok(())
    }

    pub fn try_extend_from_slice(&mut self, values: &[T]) -> MemoryResult<()>
    where
        T: Clone,
    {
        self.try_reserve(values.len())?;
        self.inner.extend_from_slice(values);
        Ok(())
    }

    pub fn try_resize_with<F>(&mut self, new_len: usize, f: F) -> MemoryResult<()>
    where
        F: FnMut() -> T,
    {
        if new_len > self.inner.len() {
            self.try_reserve(new_len - self.inner.len())?;
        }
        self.inner.resize_with(new_len, f);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop()
    }

    pub fn truncate(&mut self, len: usize) {
        self.inner.truncate(len);
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, T> {
        self.inner.drain(..)
    }

    pub fn as_slice(&self) -> &[T] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.inner
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

impl<T> Drop for AccountedVec<T> {
    fn drop(&mut self) {
        self.release_capacity(self.accounted_bytes);
        self.accounted_bytes = 0;
    }
}

impl<T> Deref for AccountedVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for AccountedVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
