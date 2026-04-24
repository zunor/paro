// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Owner-accounted Vec-like buffer for thread-local operator states.

use std::mem::size_of;
use std::ops::{Deref, DerefMut};

use paro_common::memory::{MemoryAccountingContext, MemoryError, MemoryResult};

use super::RetainedMemoryHandle;

/// A `Vec<T>` whose capacity bytes are published to a memory owner.
///
/// Unlike `AccountedVec`, this type does not embed `MemoryGrant`, so it remains
/// `Sync` when `T` is `Sync` and can live inside `OperatorState`.
#[derive(Debug)]
pub struct AccountedBuffer<T> {
    memory: MemoryAccountingContext,
    inner: Vec<T>,
    accounted_bytes: usize,
    releases: Vec<RetainedMemoryHandle>,
}

impl<T> AccountedBuffer<T> {
    pub fn new(memory: MemoryAccountingContext) -> Self {
        Self {
            memory,
            inner: Vec::new(),
            accounted_bytes: 0,
            releases: Vec::new(),
        }
    }

    pub fn with_capacity(memory: MemoryAccountingContext, capacity: usize) -> MemoryResult<Self> {
        let mut buffer = Self::new(memory);
        buffer.try_reserve(capacity)?;
        Ok(buffer)
    }

    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        let required = self.inner.len().saturating_add(additional);
        self.ensure_capacity(required)
    }

    pub fn ensure_capacity(&mut self, capacity: usize) -> MemoryResult<()> {
        if capacity <= self.inner.capacity() || size_of::<T>() == 0 {
            return Ok(());
        }

        let expected_bytes = capacity.saturating_mul(size_of::<T>());
        let delta = expected_bytes.saturating_sub(self.accounted_bytes);
        let release = self.memory.retain(delta)?;
        match self
            .inner
            .try_reserve_exact(capacity.saturating_sub(self.inner.capacity()))
        {
            Ok(()) => {
                self.accounted_bytes = expected_bytes;
                self.releases.push(RetainedMemoryHandle::new(release));
                self.sync_capacity()?;
                Ok(())
            }
            Err(_) => {
                drop(RetainedMemoryHandle::new(release));
                Err(MemoryError::physical_allocation_failed(delta))
            }
        }
    }

    pub fn sync_capacity(&mut self) -> MemoryResult<()> {
        let actual_bytes = self.inner.capacity().saturating_mul(size_of::<T>());
        if actual_bytes <= self.accounted_bytes || size_of::<T>() == 0 {
            return Ok(());
        }
        let delta = actual_bytes - self.accounted_bytes;
        let release = self.memory.retain(delta)?;
        self.accounted_bytes = actual_bytes;
        self.releases.push(RetainedMemoryHandle::new(release));
        Ok(())
    }

    pub fn try_push(&mut self, value: T) -> MemoryResult<()> {
        if self.inner.len() == self.inner.capacity() {
            self.try_reserve(1)?;
        }
        self.inner.push(value);
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

    pub fn try_resize(&mut self, new_len: usize, value: T) -> MemoryResult<()>
    where
        T: Clone,
    {
        if new_len > self.inner.len() {
            self.try_reserve(new_len - self.inner.len())?;
        }
        self.inner.resize(new_len, value);
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

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn truncate(&mut self, len: usize) {
        self.inner.truncate(len);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop()
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, T> {
        self.inner.drain(..)
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

    pub fn as_slice(&self) -> &[T] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.inner
    }

    pub fn as_mut_vec(&mut self) -> &mut Vec<T> {
        &mut self.inner
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.inner.iter()
    }

    pub fn memory_context(&self) -> MemoryAccountingContext {
        self.memory.clone()
    }
}

impl<T> Deref for AccountedBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for AccountedBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
