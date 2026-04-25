// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Grant-aware arena wrapper.

use std::sync::Arc;

use crate::allocator::{Allocator, ArenaAllocator, MemoryTag};

use super::{MemoryAccountingClass, MemoryAccumulator, MemoryGrant, MemoryResult};

/// Arena wrapper that reconciles arena chunk growth with a memory grant.
#[derive(Debug)]
pub struct GrantArena<'scope> {
    inner: ArenaAllocator,
    grant: &'scope MemoryGrant,
    accumulator: &'scope MemoryAccumulator,
    tag: MemoryTag,
    class: MemoryAccountingClass,
}

impl<'scope> GrantArena<'scope> {
    pub fn new(
        allocator: Arc<dyn Allocator>,
        grant: &'scope MemoryGrant,
        accumulator: &'scope MemoryAccumulator,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            inner: ArenaAllocator::new(allocator),
            grant,
            accumulator,
            tag,
            class,
        }
    }

    pub fn allocate(&mut self, len: usize) -> MemoryResult<*mut u8> {
        self.allocate_with_alignment(len, 1)
    }

    pub fn allocate_aligned(&mut self, len: usize) -> MemoryResult<*mut u8> {
        self.allocate_with_alignment(len, 8)
    }

    pub fn allocate_with_alignment(
        &mut self,
        len: usize,
        alignment: usize,
    ) -> MemoryResult<*mut u8> {
        let additional = self
            .inner
            .additional_capacity_for_allocation(len, alignment)
            .map_err(|_| super::MemoryError::physical_allocation_failed(len))?;
        self.grant.try_consume(additional)?;
        let result = self
            .inner
            .allocate_with_alignment(len, alignment)
            .map_err(|_| super::MemoryError::physical_allocation_failed(len));
        match result {
            Ok(ptr) => {
                if additional > 0 {
                    let _ = self.accumulator.record(additional as isize);
                    if let Some(owner) = self.grant.owner() {
                        owner.record_allocation(
                            self.grant.domain(),
                            self.tag,
                            self.class,
                            additional,
                        );
                    }
                }
                Ok(ptr)
            }
            Err(err) => {
                self.grant.refund(additional);
                Err(err)
            }
        }
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }

    pub fn destroy(&mut self) {
        let bytes = self.inner.allocation_size();
        self.inner.destroy();
        self.grant.refund(bytes);
        let _ = self.accumulator.record(-(bytes as isize));
        if let Some(owner) = self.grant.owner() {
            owner.release_allocation(self.grant.domain(), self.tag, self.class, bytes);
        }
    }

    #[inline]
    pub fn allocation_size(&self) -> usize {
        self.inner.allocation_size()
    }
}

impl Drop for GrantArena<'_> {
    fn drop(&mut self) {
        self.destroy();
    }
}
