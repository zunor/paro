// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Owned accounting context for long-lived storage allocations.

use std::sync::Arc;

use crate::allocator::{Allocator, MemoryTag};

use super::{
    GrantAllocator, GrantBuffer, MemoryAccountingClass, MemoryDomain, MemoryError, MemoryOwner,
    MemoryReleaseHandle, MemoryResult,
};

/// Cloneable owner binding used by storage structures that outlive one operator call.
#[derive(Clone)]
pub struct MemoryAccountingContext {
    owner: Option<Arc<dyn MemoryOwner>>,
    domain: MemoryDomain,
    tag: MemoryTag,
    class: MemoryAccountingClass,
}

impl MemoryAccountingContext {
    pub fn new(
        owner: Option<Arc<dyn MemoryOwner>>,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            owner,
            domain,
            tag,
            class,
        }
    }

    pub fn detached(tag: MemoryTag, class: MemoryAccountingClass) -> Self {
        Self::new(None, MemoryDomain::Host, tag, class)
    }

    pub fn from_owner(
        owner: Arc<dyn MemoryOwner>,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self::new(Some(owner), domain, tag, class)
    }

    pub fn from_grant_allocator(grant_allocator: &GrantAllocator<'_>) -> Self {
        Self::new(
            grant_allocator.grant().owner(),
            grant_allocator.grant().domain(),
            grant_allocator.tag(),
            grant_allocator.accounting_class(),
        )
    }

    pub fn with_class(&self, class: MemoryAccountingClass) -> Self {
        Self::new(self.owner(), self.domain, self.tag, class)
    }

    pub fn with_tag_and_class(&self, tag: MemoryTag, class: MemoryAccountingClass) -> Self {
        Self::new(self.owner(), self.domain, tag, class)
    }

    #[inline]
    pub fn owner(&self) -> Option<Arc<dyn MemoryOwner>> {
        self.owner.as_ref().map(Arc::clone)
    }

    #[inline]
    pub fn domain(&self) -> MemoryDomain {
        self.domain
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
    pub fn is_owner_backed(&self) -> bool {
        self.owner.is_some()
    }

    /// Whether two long-lived structures publish allocations to exactly the
    /// same accounting target.
    pub fn has_same_target(&self, other: &Self) -> bool {
        self.domain == other.domain
            && self.tag == other.tag
            && self.class == other.class
            && match (&self.owner, &other.owner) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
    }

    pub fn retain(&self, bytes: usize) -> MemoryResult<MemoryReleaseHandle> {
        if bytes == 0 {
            return Ok(MemoryReleaseHandle::new(
                self.owner.clone(),
                self.domain,
                self.tag,
                self.class,
                0,
            ));
        }

        if let Some(owner) = &self.owner {
            if self.class == MemoryAccountingClass::Spill {
                owner.record_allocation(self.domain, self.tag, self.class, bytes);
                return Ok(MemoryReleaseHandle::new_observed(
                    self.owner.clone(),
                    self.domain,
                    self.tag,
                    self.class,
                    bytes,
                ));
            }
            owner.acquire_capacity(self.domain, bytes)?;
            owner.record_allocation(self.domain, self.tag, self.class, bytes);
        }

        Ok(MemoryReleaseHandle::new(
            self.owner.clone(),
            self.domain,
            self.tag,
            self.class,
            bytes,
        ))
    }

    pub fn grant(&self) -> MemoryResult<super::MemoryGrant> {
        if let Some(owner) = self.owner() {
            super::MemoryGrant::new(0, self.domain, owner)
        } else {
            Ok(super::MemoryGrant::detached(usize::MAX / 4, self.domain))
        }
    }

    /// Reserve a bounded structure's complete capacity with one owner call.
    ///
    /// Callers can split the returned grant among independently owned fields.
    /// This preserves exact per-allocation publication while avoiding one
    /// contended capacity acquisition for every fixed-size child buffer.
    pub fn reserve_grant(&self, bytes: usize) -> MemoryResult<super::MemoryGrant> {
        if let Some(owner) = self.owner() {
            super::MemoryGrant::new(bytes, self.domain, owner)
        } else {
            Ok(super::MemoryGrant::detached(bytes, self.domain))
        }
    }

    pub fn allocate_buffer(
        &self,
        allocator: Arc<dyn Allocator>,
        size: usize,
    ) -> MemoryResult<GrantBuffer> {
        self.allocate_inner(allocator, size, false)
    }

    pub fn allocate_zeroed_buffer(
        &self,
        allocator: Arc<dyn Allocator>,
        size: usize,
    ) -> MemoryResult<GrantBuffer> {
        self.allocate_inner(allocator, size, true)
    }

    fn allocate_inner(
        &self,
        allocator: Arc<dyn Allocator>,
        size: usize,
        zeroed: bool,
    ) -> MemoryResult<GrantBuffer> {
        if size == 0 {
            return Ok(GrantBuffer::new(
                allocator,
                std::ptr::null_mut(),
                0,
                self.retain(0)?,
            ));
        }

        let release = self.retain(size)?;
        let allocation = if zeroed {
            allocator.allocate_zeroed(size)
        } else {
            allocator.allocate(size)
        };
        match allocation {
            Ok(ptr) => Ok(GrantBuffer::new(allocator, ptr, size, release)),
            Err(_) => {
                release.release();
                Err(MemoryError::physical_allocation_failed(size))
            }
        }
    }
}

impl std::fmt::Debug for MemoryAccountingContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryAccountingContext")
            .field("domain", &self.domain)
            .field("tag", &self.tag)
            .field("class", &self.class)
            .field("has_owner", &self.owner.is_some())
            .finish()
    }
}
