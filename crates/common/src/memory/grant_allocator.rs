// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Grant-gated allocator wrapper.

use std::sync::Arc;

use super::{
    GrantBuffer, MemoryAccountingClass, MemoryAccumulator, MemoryError, MemoryGrant,
    MemoryReleaseHandle, MemoryResult,
};
use crate::allocator::{Allocator, MemoryTag};

/// Allocator facade that consumes grant capacity before physical allocation.
#[derive(Clone, Copy)]
pub struct GrantAllocator<'scope> {
    inner: &'scope Arc<dyn Allocator>,
    grant: &'scope MemoryGrant,
    accumulator: &'scope MemoryAccumulator,
    tag: MemoryTag,
    class: MemoryAccountingClass,
}

impl<'scope> GrantAllocator<'scope> {
    pub fn new(
        inner: &'scope Arc<dyn Allocator>,
        grant: &'scope MemoryGrant,
        accumulator: &'scope MemoryAccumulator,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            inner,
            grant,
            accumulator,
            tag,
            class,
        }
    }

    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    #[inline]
    pub fn accounting_class(&self) -> MemoryAccountingClass {
        self.class
    }

    #[inline]
    pub fn grant(&self) -> &MemoryGrant {
        self.grant
    }

    #[inline]
    pub fn inner(&self) -> &Arc<dyn Allocator> {
        self.inner
    }

    pub fn allocate(&self, size: usize) -> MemoryResult<*mut u8> {
        self.allocate_inner(size, false)
    }

    pub fn allocate_zeroed(&self, size: usize) -> MemoryResult<*mut u8> {
        self.allocate_inner(size, true)
    }

    pub fn allocate_buffer(&self, size: usize) -> MemoryResult<GrantBuffer> {
        if self.grant.owner().is_none() && size > 0 {
            return Err(MemoryError::reclaim_failed(
                "GrantBuffer requires an owner-backed grant",
            ));
        }
        let ptr = self.allocate(size)?;
        let release = self.release_handle(size);
        self.grant.commit_consumed(size);
        Ok(GrantBuffer::new(self.inner.clone(), ptr, size, release))
    }

    pub fn allocate_zeroed_buffer(&self, size: usize) -> MemoryResult<GrantBuffer> {
        if self.grant.owner().is_none() && size > 0 {
            return Err(MemoryError::reclaim_failed(
                "GrantBuffer requires an owner-backed grant",
            ));
        }
        let ptr = self.allocate_zeroed(size)?;
        let release = self.release_handle(size);
        self.grant.commit_consumed(size);
        Ok(GrantBuffer::new(self.inner.clone(), ptr, size, release))
    }

    pub fn free(&self, ptr: *mut u8, size: usize) {
        if size == 0 {
            return;
        }
        self.inner.free(ptr, size);
        self.grant.refund(size);
        let _ = self.accumulator.record(-(size as isize));
        if let Some(owner) = self.grant.owner() {
            owner.release_allocation(self.grant.domain(), self.tag, self.class, size);
        }
    }

    pub fn reallocate(
        &self,
        ptr: *mut u8,
        old_size: usize,
        new_size: usize,
    ) -> MemoryResult<*mut u8> {
        if new_size == old_size {
            return Ok(ptr);
        }

        let grow_delta = new_size.saturating_sub(old_size);
        if grow_delta > 0 {
            self.grant.try_consume(grow_delta)?;
        }

        match self.inner.reallocate(ptr, old_size, new_size) {
            Ok(new_ptr) => {
                if new_size > old_size {
                    self.publish_positive(grow_delta);
                } else {
                    let shrink_delta = old_size - new_size;
                    self.grant.refund(shrink_delta);
                    let _ = self.accumulator.record(-(shrink_delta as isize));
                    if let Some(owner) = self.grant.owner() {
                        owner.release_allocation(
                            self.grant.domain(),
                            self.tag,
                            self.class,
                            shrink_delta,
                        );
                    }
                }
                Ok(new_ptr)
            }
            Err(err) => {
                if grow_delta > 0 {
                    self.grant.refund(grow_delta);
                }
                Err(memory_error_from_paro(err, new_size))
            }
        }
    }

    pub fn release_handle(&self, bytes: usize) -> MemoryReleaseHandle {
        self.grant.release_handle(self.tag, self.class, bytes)
    }

    fn allocate_inner(&self, size: usize, zeroed: bool) -> MemoryResult<*mut u8> {
        if size == 0 {
            return Ok(std::ptr::null_mut());
        }

        self.grant.try_consume(size)?;
        let allocation = if zeroed {
            self.inner.allocate_zeroed(size)
        } else {
            self.inner.allocate(size)
        };

        match allocation {
            Ok(ptr) => {
                self.publish_positive(size);
                Ok(ptr)
            }
            Err(err) => {
                self.grant.refund(size);
                Err(memory_error_from_paro(err, size))
            }
        }
    }

    fn publish_positive(&self, size: usize) {
        let _ = self.accumulator.record(size as isize);
        if let Some(owner) = self.grant.owner() {
            owner.record_allocation(self.grant.domain(), self.tag, self.class, size);
        }
    }
}

fn memory_error_from_paro(_err: crate::error::ParoError, bytes: usize) -> MemoryError {
    MemoryError::physical_allocation_failed(bytes)
}
