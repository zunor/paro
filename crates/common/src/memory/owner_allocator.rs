// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Owner-backed allocator adapter.

use std::sync::Arc;

use crate::allocator::{Allocator, MemoryTag};
use crate::error::Result;

use super::{MemoryAccountingClass, MemoryDomain, MemoryOwner};

/// `Allocator` facade that hard-gates vector/chunk allocations through a
/// `MemoryOwner` before touching the physical allocator.
#[derive(Clone)]
pub struct MemoryOwnerAllocator {
    inner: Arc<dyn Allocator>,
    owner: Arc<dyn MemoryOwner>,
    domain: MemoryDomain,
    tag: MemoryTag,
    class: MemoryAccountingClass,
}

impl MemoryOwnerAllocator {
    pub fn new(
        inner: Arc<dyn Allocator>,
        owner: Arc<dyn MemoryOwner>,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            inner,
            owner,
            domain,
            tag,
            class,
        }
    }

    #[inline]
    pub fn inner(&self) -> &Arc<dyn Allocator> {
        &self.inner
    }

    #[inline]
    pub fn owner(&self) -> &Arc<dyn MemoryOwner> {
        &self.owner
    }

    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    #[inline]
    pub fn accounting_class(&self) -> MemoryAccountingClass {
        self.class
    }
}

impl Allocator for MemoryOwnerAllocator {
    fn allocate(&self, size: usize) -> Result<*mut u8> {
        if size == 0 {
            return Ok(std::ptr::null_mut());
        }

        self.owner.acquire_capacity(self.domain, size)?;
        match self.inner.allocate(size) {
            Ok(ptr) => {
                self.owner
                    .record_allocation(self.domain, self.tag, self.class, size);
                Ok(ptr)
            }
            Err(error) => {
                self.owner.release_capacity(self.domain, size);
                Err(error)
            }
        }
    }

    fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
        if size == 0 {
            return Ok(std::ptr::null_mut());
        }

        self.owner.acquire_capacity(self.domain, size)?;
        match self.inner.allocate_zeroed(size) {
            Ok(ptr) => {
                self.owner
                    .record_allocation(self.domain, self.tag, self.class, size);
                Ok(ptr)
            }
            Err(error) => {
                self.owner.release_capacity(self.domain, size);
                Err(error)
            }
        }
    }

    fn free(&self, ptr: *mut u8, size: usize) {
        if ptr.is_null() || size == 0 {
            return;
        }
        self.inner.free(ptr, size);
        self.owner
            .release_allocation(self.domain, self.tag, self.class, size);
        self.owner.release_capacity(self.domain, size);
    }

    fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
        if ptr.is_null() {
            return self.allocate(new_size);
        }
        if new_size == 0 {
            self.free(ptr, old_size);
            return Ok(std::ptr::null_mut());
        }
        if new_size == old_size {
            return Ok(ptr);
        }

        let grow_delta = new_size.saturating_sub(old_size);
        if grow_delta > 0 {
            self.owner.acquire_capacity(self.domain, grow_delta)?;
        }

        match self.inner.reallocate(ptr, old_size, new_size) {
            Ok(new_ptr) => {
                if new_size > old_size {
                    self.owner
                        .record_allocation(self.domain, self.tag, self.class, grow_delta);
                } else {
                    let shrink_delta = old_size - new_size;
                    self.owner
                        .release_allocation(self.domain, self.tag, self.class, shrink_delta);
                    self.owner.release_capacity(self.domain, shrink_delta);
                }
                Ok(new_ptr)
            }
            Err(error) => {
                if grow_delta > 0 {
                    self.owner.release_capacity(self.domain, grow_delta);
                }
                Err(error)
            }
        }
    }

    fn name(&self) -> &'static str {
        "MemoryOwnerAllocator"
    }
}

impl std::fmt::Debug for MemoryOwnerAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryOwnerAllocator")
            .field("domain", &self.domain)
            .field("tag", &self.tag)
            .field("class", &self.class)
            .field("inner", &self.inner.name())
            .finish()
    }
}
