// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Owned grant-backed raw buffer.

use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::allocator::Allocator;

use super::MemoryReleaseHandle;

/// Allocation metadata embedded in a grant-backed buffer.
pub struct GrantAllocation {
    ptr: NonNull<u8>,
    size: usize,
    allocator: Arc<dyn Allocator>,
    release: MemoryReleaseHandle,
}

impl GrantAllocation {
    pub fn new(
        allocator: Arc<dyn Allocator>,
        ptr: *mut u8,
        size: usize,
        release: MemoryReleaseHandle,
    ) -> Option<Self> {
        if size == 0 {
            return None;
        }
        let ptr = NonNull::new(ptr)?;
        Some(Self {
            ptr,
            size,
            allocator,
            release,
        })
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }
}

impl fmt::Debug for GrantAllocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrantAllocation")
            .field("ptr", &self.ptr)
            .field("size", &self.size)
            .field("allocator", &self.allocator.name())
            .field("release", &self.release)
            .finish()
    }
}

// SAFETY: GrantAllocation owns its allocation and only frees it through a Send+Sync allocator.
unsafe impl Send for GrantAllocation {}
// SAFETY: Shared references only expose pointer metadata; mutation requires raw-pointer unsafe code by callers.
unsafe impl Sync for GrantAllocation {}

/// Owned raw buffer whose release path carries size/tag/domain metadata.
#[derive(Debug)]
pub struct GrantBuffer {
    allocation: Option<GrantAllocation>,
}

impl GrantBuffer {
    pub fn new(
        allocator: Arc<dyn Allocator>,
        ptr: *mut u8,
        size: usize,
        release: MemoryReleaseHandle,
    ) -> Self {
        Self {
            allocation: GrantAllocation::new(allocator, ptr, size, release),
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.allocation
            .as_ref()
            .map(GrantAllocation::as_ptr)
            .unwrap_or(std::ptr::null_mut())
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.allocation
            .as_ref()
            .map(GrantAllocation::size)
            .unwrap_or(0)
    }
}

impl Drop for GrantBuffer {
    fn drop(&mut self) {
        if let Some(allocation) = self.allocation.take() {
            allocation
                .allocator
                .free(allocation.ptr.as_ptr(), allocation.size);
            allocation.release.release();
        }
    }
}
