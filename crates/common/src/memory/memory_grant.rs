// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Grant-backed hard capacity token.

use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::allocator::MemoryTag;

use super::{MemoryAccountingClass, MemoryDomain, MemoryError, MemoryOwner, MemoryResult};

/// RAII capacity token for a single memory domain.
pub struct MemoryGrant {
    reserved_bytes: Cell<usize>,
    locally_available_bytes: Cell<usize>,
    used_bytes: Cell<usize>,
    domain: MemoryDomain,
    owner: Option<Arc<dyn MemoryOwner>>,
}

impl MemoryGrant {
    /// Create a grant backed by an owner/account.
    pub fn new(
        reserved_bytes: usize,
        domain: MemoryDomain,
        owner: Arc<dyn MemoryOwner>,
    ) -> MemoryResult<Self> {
        owner.acquire_capacity(domain, reserved_bytes)?;
        Ok(Self {
            reserved_bytes: Cell::new(reserved_bytes),
            locally_available_bytes: Cell::new(reserved_bytes),
            used_bytes: Cell::new(0),
            domain,
            owner: Some(owner),
        })
    }

    /// Create a detached grant for tests and isolated low-level structures.
    pub fn detached(reserved_bytes: usize, domain: MemoryDomain) -> Self {
        Self {
            reserved_bytes: Cell::new(reserved_bytes),
            locally_available_bytes: Cell::new(reserved_bytes),
            used_bytes: Cell::new(0),
            domain,
            owner: None,
        }
    }

    #[inline]
    pub fn domain(&self) -> MemoryDomain {
        self.domain
    }

    #[inline]
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes.get()
    }

    #[inline]
    pub fn available_bytes(&self) -> usize {
        self.locally_available_bytes.get()
    }

    #[inline]
    pub fn used_bytes(&self) -> usize {
        self.used_bytes.get()
    }

    #[inline]
    pub fn owner(&self) -> Option<Arc<dyn MemoryOwner>> {
        self.owner.as_ref().map(Arc::clone)
    }

    /// Hard gate before touching a physical allocator.
    pub fn try_consume(&self, bytes: usize) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        let available = self.locally_available_bytes.get();
        if available < bytes {
            return Err(MemoryError::quota_exhausted(self.domain, bytes, available));
        }
        self.locally_available_bytes.set(available - bytes);
        self.used_bytes
            .set(self.used_bytes.get().saturating_add(bytes));
        Ok(())
    }

    /// Synchronously restore consumed capacity to this local grant.
    pub fn refund(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let used = self.used_bytes.get();
        debug_assert!(
            used >= bytes,
            "refunding more bytes ({bytes}) than grant used ({used})"
        );
        let refunded = bytes.min(used);
        self.used_bytes.set(used - refunded);
        self.locally_available_bytes
            .set(self.locally_available_bytes.get().saturating_add(refunded));
    }

    /// Release locally available capacity back to the owner/account.
    ///
    /// This is the synchronous revoke path used at operator safe points. It only
    /// releases unused capacity; already-consumed bytes remain owned by the
    /// allocation that consumed them.
    pub fn release_available(&self, bytes: usize) -> usize {
        if bytes == 0 {
            return 0;
        }

        let released = bytes.min(self.locally_available_bytes.get());
        if released == 0 {
            return 0;
        }

        self.locally_available_bytes
            .set(self.locally_available_bytes.get() - released);
        self.reserved_bytes
            .set(self.reserved_bytes.get().saturating_sub(released));
        if let Some(owner) = &self.owner {
            owner.release_capacity(self.domain, released);
        }
        released
    }

    /// Split locally available capacity into an independent grant without
    /// asking the owner for more capacity.
    pub fn split(&self, bytes: usize) -> MemoryResult<Self> {
        if bytes == 0 {
            return Ok(Self::from_issued_capacity(0, self.domain, self.owner()));
        }
        let available = self.locally_available_bytes.get();
        if available < bytes {
            return Err(MemoryError::quota_exhausted(self.domain, bytes, available));
        }
        self.locally_available_bytes.set(available - bytes);
        self.reserved_bytes
            .set(self.reserved_bytes.get().saturating_sub(bytes));
        Ok(Self::from_issued_capacity(bytes, self.domain, self.owner()))
    }

    /// Commit consumed bytes to an external release handle.
    ///
    /// After this call, this grant no longer owns the committed capacity.
    pub fn commit_consumed(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let used = self.used_bytes.get();
        debug_assert!(
            used >= bytes,
            "committing more bytes ({bytes}) than grant used ({used})"
        );
        let committed = bytes.min(used);
        self.used_bytes.set(used - committed);
        self.reserved_bytes
            .set(self.reserved_bytes.get().saturating_sub(committed));
    }

    /// Grow this grant by asking the owner/account for more capacity.
    pub fn grow(&self, delta: usize) -> MemoryResult<()> {
        if delta == 0 {
            return Ok(());
        }
        if let Some(owner) = &self.owner {
            owner.acquire_capacity(self.domain, delta)?;
        }
        self.reserved_bytes
            .set(self.reserved_bytes.get().saturating_add(delta));
        self.locally_available_bytes
            .set(self.locally_available_bytes.get().saturating_add(delta));
        Ok(())
    }

    /// Merge all local counters into another grant in the same domain.
    pub fn merge_into(&self, target: &MemoryGrant) -> MemoryResult<()> {
        if self.domain != target.domain {
            return Err(MemoryError::reclaim_failed(format!(
                "cannot merge grants from {:?} into {:?}",
                self.domain, target.domain
            )));
        }

        target.reserved_bytes.set(
            target
                .reserved_bytes
                .get()
                .saturating_add(self.reserved_bytes.get()),
        );
        target.locally_available_bytes.set(
            target
                .locally_available_bytes
                .get()
                .saturating_add(self.locally_available_bytes.get()),
        );
        target.used_bytes.set(
            target
                .used_bytes
                .get()
                .saturating_add(self.used_bytes.get()),
        );

        self.reserved_bytes.set(0);
        self.locally_available_bytes.set(0);
        self.used_bytes.set(0);
        Ok(())
    }

    /// Move this grant to another owner/account.
    pub fn transfer_to(mut self, new_owner: Arc<dyn MemoryOwner>) -> MemoryResult<Self> {
        let reserved = self.reserved_bytes.get();
        new_owner.acquire_capacity(self.domain, reserved)?;
        if let Some(owner) = &self.owner {
            owner.release_capacity(self.domain, reserved);
        }
        self.owner = Some(new_owner);
        Ok(self)
    }

    /// Build a release handle for a successful allocation.
    pub fn release_handle(
        &self,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) -> MemoryReleaseHandle {
        MemoryReleaseHandle::new(self.owner(), self.domain, tag, class, bytes)
    }

    fn from_issued_capacity(
        reserved_bytes: usize,
        domain: MemoryDomain,
        owner: Option<Arc<dyn MemoryOwner>>,
    ) -> Self {
        Self {
            reserved_bytes: Cell::new(reserved_bytes),
            locally_available_bytes: Cell::new(reserved_bytes),
            used_bytes: Cell::new(0),
            domain,
            owner,
        }
    }
}

impl Drop for MemoryGrant {
    fn drop(&mut self) {
        let reserved = self.reserved_bytes.replace(0);
        let used = self.used_bytes.replace(0);
        self.locally_available_bytes.set(0);
        if reserved > 0 {
            if let Some(owner) = &self.owner {
                if used > 0 {
                    owner.record_leaked_grant(self.domain, used);
                }
                owner.release_capacity(self.domain, reserved);
            }
        }
    }
}

impl fmt::Debug for MemoryGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryGrant")
            .field("reserved_bytes", &self.reserved_bytes.get())
            .field(
                "locally_available_bytes",
                &self.locally_available_bytes.get(),
            )
            .field("used_bytes", &self.used_bytes.get())
            .field("domain", &self.domain)
            .field("has_owner", &self.owner.is_some())
            .finish()
    }
}

/// Send + Sync release handle stored by shareable allocation owners.
#[must_use = "MemoryReleaseHandle must be stored by an owner or explicitly released"]
pub struct MemoryReleaseHandle {
    owner: Option<Arc<dyn MemoryOwner>>,
    domain: MemoryDomain,
    tag: MemoryTag,
    class: MemoryAccountingClass,
    bytes: usize,
    released: AtomicBool,
}

impl MemoryReleaseHandle {
    pub fn new(
        owner: Option<Arc<dyn MemoryOwner>>,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) -> Self {
        Self {
            owner,
            domain,
            tag,
            class,
            bytes,
            released: AtomicBool::new(false),
        }
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
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn release(&self) {
        if self.bytes == 0 {
            return;
        }
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(owner) = &self.owner {
            owner.release_allocation(self.domain, self.tag, self.class, self.bytes);
            owner.release_capacity(self.domain, self.bytes);
        }
    }
}

impl fmt::Debug for MemoryReleaseHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryReleaseHandle")
            .field("domain", &self.domain)
            .field("tag", &self.tag)
            .field("class", &self.class)
            .field("bytes", &self.bytes)
            .field("released", &self.released.load(Ordering::Acquire))
            .field("has_owner", &self.owner.is_some())
            .finish()
    }
}
