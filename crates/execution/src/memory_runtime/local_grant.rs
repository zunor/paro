// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Local state memory grant.

use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::memory::{
    MemoryAccountingClass, MemoryDomain, MemoryGrant, MemoryOwner, MemoryReleaseHandle,
    MemoryResult,
};

pub const DEFAULT_LOCAL_INITIAL_GRANT_BYTES: usize = 0;
pub const DEFAULT_LOCAL_REFILL_FLOOR_BYTES: usize = 64 * 1024;
pub const DEFAULT_LOCAL_REFILL_CAP_BYTES: usize = 8 * 1024 * 1024;

/// Send + !Sync grant held inside a local operator state.
pub struct LocalMemoryGrant {
    grant: MemoryGrant,
    tag: MemoryTag,
    accounting_class: MemoryAccountingClass,
    inner_allocator: Arc<dyn Allocator>,
}

impl std::fmt::Debug for LocalMemoryGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalMemoryGrant")
            .field("grant", &self.grant)
            .field("tag", &self.tag)
            .field("accounting_class", &self.accounting_class)
            .field("allocator", &self.inner_allocator.name())
            .finish()
    }
}

impl LocalMemoryGrant {
    pub fn new(
        owner: Arc<dyn MemoryOwner>,
        reserved_bytes: usize,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
        inner_allocator: Arc<dyn Allocator>,
    ) -> MemoryResult<Self> {
        Ok(Self {
            grant: MemoryGrant::new(reserved_bytes, MemoryDomain::Host, owner)?,
            tag,
            accounting_class,
            inner_allocator,
        })
    }

    pub fn detached(
        reserved_bytes: usize,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
        inner_allocator: Arc<dyn Allocator>,
    ) -> Self {
        Self {
            grant: MemoryGrant::detached(reserved_bytes, MemoryDomain::Host),
            tag,
            accounting_class,
            inner_allocator,
        }
    }

    pub fn grant(&self) -> &MemoryGrant {
        &self.grant
    }

    pub fn domain(&self) -> MemoryDomain {
        self.grant.domain()
    }

    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    pub fn accounting_class(&self) -> MemoryAccountingClass {
        self.accounting_class
    }

    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.inner_allocator
    }

    /// Split a sub-grant using Cell-backed internal mutability.
    pub fn split_sub_grant(&self, bytes: usize) -> MemoryResult<MemoryGrant> {
        self.grant.split(bytes)
    }

    /// Initial child split used when an operator local state seeds sub-structures.
    ///
    /// This takes `&self` because `MemoryGrant` uses `Cell` for the hot counters:
    /// local states are Send + !Sync, and the executor only accesses a given
    /// grant from one task at a time.
    pub fn split_initial(&self, bytes: usize) -> MemoryResult<MemoryGrant> {
        self.split_sub_grant(bytes)
    }

    /// Refill local available capacity with geometric growth capped per refill.
    pub fn refill_local(&self, required_bytes: usize, cap_bytes: usize) -> MemoryResult<usize> {
        let available = self.grant.available_bytes();
        if available >= required_bytes {
            return Ok(0);
        }

        let shortage = required_bytes - available;
        let geometric = self
            .grant
            .reserved_bytes()
            .max(DEFAULT_LOCAL_REFILL_FLOOR_BYTES)
            .saturating_mul(2);
        let refill = shortage.max(geometric).min(cap_bytes.max(shortage));
        self.grant.grow(refill)?;
        if let Some(owner) = self.grant.owner() {
            owner.record_local_refill(self.domain(), refill);
        }
        Ok(refill)
    }

    pub fn refill_local_default(&self, required_bytes: usize) -> MemoryResult<usize> {
        self.refill_local(required_bytes, DEFAULT_LOCAL_REFILL_CAP_BYTES)
    }

    /// Account bytes owned by a structure that is not yet physically allocated
    /// through `GrantAllocator`.
    pub fn retain_external_allocation(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
        bytes: usize,
    ) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        let available = self.grant.available_bytes();
        if available < bytes {
            self.grant.grow(bytes - available)?;
        }
        self.grant.try_consume(bytes)?;
        if let Some(owner) = self.grant.owner() {
            owner.record_allocation(self.domain(), tag, accounting_class, bytes);
        }
        Ok(())
    }

    pub fn retain_external_allocation_handle(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
        bytes: usize,
    ) -> MemoryResult<MemoryReleaseHandle> {
        if bytes == 0 {
            return Ok(self.grant.release_handle(tag, accounting_class, 0));
        }
        let available = self.grant.available_bytes();
        if available < bytes {
            self.grant.grow(bytes - available)?;
        }
        self.grant.try_consume(bytes)?;
        if let Some(owner) = self.grant.owner() {
            owner.record_allocation(self.domain(), tag, accounting_class, bytes);
        }
        let handle = self.grant.release_handle(tag, accounting_class, bytes);
        self.grant.commit_consumed(bytes);
        Ok(handle)
    }

    pub fn release_external_allocation(
        &self,
        tag: MemoryTag,
        accounting_class: MemoryAccountingClass,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        let released = bytes.min(self.grant.used_bytes());
        debug_assert_eq!(
            released,
            bytes,
            "releasing more external bytes ({bytes}) than grant used ({})",
            self.grant.used_bytes()
        );
        if let Some(owner) = self.grant.owner() {
            owner.release_allocation(self.domain(), tag, accounting_class, released);
        }
        self.grant.refund(released);
        self.grant.release_available(released);
    }

    pub fn release_available(&self, bytes: usize) -> usize {
        self.grant.release_available(bytes)
    }
}
