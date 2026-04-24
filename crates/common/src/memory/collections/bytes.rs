// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::allocator::MemoryTag;
use crate::memory::{AccountedVec, MemoryAccountingClass, MemoryGrant, MemoryResult};

/// Grant-accounted byte buffer.
#[derive(Debug)]
pub struct AccountedBytesMut {
    inner: AccountedVec<u8>,
}

impl AccountedBytesMut {
    pub fn new(grant: MemoryGrant) -> Self {
        Self {
            inner: AccountedVec::new(grant),
        }
    }

    pub fn new_with_accounting(
        grant: MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            inner: AccountedVec::new_with_accounting(grant, tag, class),
        }
    }

    pub fn try_reserve(&mut self, additional: usize) -> MemoryResult<()> {
        self.inner.try_reserve(additional)
    }

    pub fn try_push(&mut self, byte: u8) -> MemoryResult<()> {
        self.inner.try_push(byte)
    }

    pub fn try_extend_from_slice(&mut self, bytes: &[u8]) -> MemoryResult<()> {
        self.inner.try_extend_from_slice(bytes)
    }

    pub fn try_resize(&mut self, len: usize, value: u8) -> MemoryResult<()> {
        self.inner.try_resize_with(len, || value)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.inner.as_mut_slice()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
