// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-operator memory account.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::allocator::{MemoryTag, MEMORY_TAG_COUNT};
use paro_common::memory::{
    MemoryAccountingClass, MemoryDomain, MemoryOwner, MemoryResult, MEMORY_DOMAIN_COUNT,
};

use super::{
    GrowOutcome, LocalMemoryGrant, MemoryDomainTagBytes, MemoryRuntimeStats, MemoryTagBytes,
    QueryMemoryPool,
};

/// Cache-line aligned wrapper to reduce false sharing on hot counters.
#[repr(align(64))]
#[derive(Debug, Default)]
pub struct CacheAligned<T>(pub T);

/// Hot counters touched by allocation/release paths.
#[derive(Debug, Default)]
pub struct HotCounters {
    pub non_revocable: AtomicUsize,
    pub revocable: AtomicUsize,
}

/// Cold counters used for observability.
#[derive(Debug, Default)]
pub struct ColdCounters {
    pub spill: AtomicUsize,
    pub prefetch: AtomicUsize,
    pub metadata: AtomicUsize,
}

/// Per-operator logical memory account.
pub struct OperatorMemoryAccount {
    hot: CacheAligned<HotCounters>,
    cold: ColdCounters,
    tag_bytes: [AtomicUsize; MEMORY_TAG_COUNT],
    domain_tag_bytes: [[AtomicUsize; MEMORY_TAG_COUNT]; MEMORY_DOMAIN_COUNT],
    issued_bytes: AtomicUsize,
    revoke_requested_bytes: AtomicUsize,
    leaked_grant_bytes: AtomicUsize,
    local_refill_count: AtomicUsize,
    local_refill_bytes: AtomicUsize,
    output_buffer_bytes: AtomicUsize,
    parent: Arc<QueryMemoryPool>,
}

impl OperatorMemoryAccount {
    pub fn new(parent: Arc<QueryMemoryPool>) -> Self {
        Self {
            hot: CacheAligned(HotCounters::default()),
            cold: ColdCounters::default(),
            tag_bytes: std::array::from_fn(|_| AtomicUsize::new(0)),
            domain_tag_bytes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicUsize::new(0))),
            issued_bytes: AtomicUsize::new(0),
            revoke_requested_bytes: AtomicUsize::new(0),
            leaked_grant_bytes: AtomicUsize::new(0),
            local_refill_count: AtomicUsize::new(0),
            local_refill_bytes: AtomicUsize::new(0),
            output_buffer_bytes: AtomicUsize::new(0),
            parent,
        }
    }

    pub fn parent(&self) -> Arc<QueryMemoryPool> {
        self.parent.clone()
    }

    pub fn issued_bytes(&self) -> usize {
        self.issued_bytes.load(Ordering::Relaxed)
    }

    pub fn non_revocable_bytes(&self) -> usize {
        self.hot.0.non_revocable.load(Ordering::Relaxed)
    }

    pub fn revocable_bytes(&self) -> usize {
        self.hot.0.revocable.load(Ordering::Relaxed)
    }

    pub fn reclaimable_bytes(&self) -> usize {
        self.revocable_bytes()
            .saturating_add(self.cold.prefetch.load(Ordering::Relaxed))
    }

    pub fn spill_bytes(&self) -> usize {
        self.cold.spill.load(Ordering::Relaxed)
    }

    pub fn prefetch_bytes(&self) -> usize {
        self.cold.prefetch.load(Ordering::Relaxed)
    }

    pub fn metadata_bytes(&self) -> usize {
        self.cold.metadata.load(Ordering::Relaxed)
    }

    pub fn leaked_grant_bytes(&self) -> usize {
        self.leaked_grant_bytes.load(Ordering::Relaxed)
    }

    pub fn local_refill_count(&self) -> usize {
        self.local_refill_count.load(Ordering::Relaxed)
    }

    pub fn local_refill_bytes(&self) -> usize {
        self.local_refill_bytes.load(Ordering::Relaxed)
    }

    pub fn output_buffer_bytes(&self) -> usize {
        self.output_buffer_bytes.load(Ordering::Relaxed)
    }

    pub fn tag_memory_snapshot(&self) -> Vec<MemoryTagBytes> {
        MemoryTag::all()
            .iter()
            .filter_map(|tag| {
                let bytes = self.tag_bytes[tag.as_index()].load(Ordering::Relaxed);
                (bytes > 0).then_some(MemoryTagBytes { tag: *tag, bytes })
            })
            .collect()
    }

    pub fn domain_tag_memory_snapshot(&self) -> Vec<MemoryDomainTagBytes> {
        MemoryDomain::all()
            .iter()
            .flat_map(|domain| {
                MemoryTag::all().iter().filter_map(move |tag| {
                    let bytes = self.domain_tag_bytes[domain.as_index()][tag.as_index()]
                        .load(Ordering::Relaxed);
                    (bytes > 0).then_some(MemoryDomainTagBytes {
                        domain: *domain,
                        tag: *tag,
                        bytes,
                    })
                })
            })
            .collect()
    }

    pub fn runtime_stats(&self) -> MemoryRuntimeStats {
        MemoryRuntimeStats {
            issued_bytes: self.issued_bytes(),
            published_used_bytes: self
                .tag_memory_snapshot()
                .iter()
                .map(|entry| entry.bytes)
                .sum(),
            non_revocable_bytes: self.non_revocable_bytes(),
            revocable_bytes: self.revocable_bytes(),
            spill_bytes: self.spill_bytes(),
            prefetch_bytes: self.prefetch_bytes(),
            metadata_bytes: self.metadata_bytes(),
            reclaimable_bytes: self.reclaimable_bytes(),
            leaked_grant_bytes: self.leaked_grant_bytes(),
            local_refill_count: self.local_refill_count(),
            local_refill_bytes: self.local_refill_bytes(),
            output_buffer_bytes: self.output_buffer_bytes(),
            tag_bytes: self.tag_memory_snapshot(),
            domain_tag_bytes: self.domain_tag_memory_snapshot(),
            ..Default::default()
        }
    }

    pub fn try_grow(&self, bytes: usize) -> MemoryResult<()> {
        self.parent.try_grow(bytes)?;
        self.issued_bytes.fetch_add(bytes, Ordering::AcqRel);
        Ok(())
    }

    pub fn try_grow_or_block(
        &self,
        bytes: usize,
        interrupt: Option<paro_scheduler::task::InterruptState>,
    ) -> MemoryResult<GrowOutcome> {
        match self.parent.try_grow_or_block(bytes, interrupt)? {
            GrowOutcome::Granted => {
                self.issued_bytes.fetch_add(bytes, Ordering::AcqRel);
                Ok(GrowOutcome::Granted)
            }
            GrowOutcome::Blocked(handle) => Ok(GrowOutcome::Blocked(handle)),
        }
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .issued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
        self.parent.release(bytes);
    }

    pub fn retain_external_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        self.try_grow(bytes)?;
        self.record_allocation(domain, tag, class, bytes);
        Ok(())
    }

    pub fn release_external_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        if bytes == 0 {
            return;
        }
        self.release_allocation(domain, tag, class, bytes);
        self.release(bytes);
    }

    pub fn reclassify(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        from: MemoryAccountingClass,
        to: MemoryAccountingClass,
        bytes: usize,
    ) {
        self.reclassify_allocation(domain, tag, from, to, bytes);
    }

    pub fn request_revoke(&self, bytes: usize) {
        let _ = self.revoke_requested_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Relaxed,
            |current| Some(current.max(bytes)),
        );
    }

    pub fn revoke_requested_bytes(&self) -> usize {
        self.revoke_requested_bytes.load(Ordering::Acquire)
    }

    /// Execute a safe-point revoke against unused local capacity.
    pub fn sync_revoke(&self, grant: &LocalMemoryGrant) -> usize {
        let requested = self.revoke_requested_bytes.swap(0, Ordering::AcqRel);
        grant.release_available(requested)
    }
}

impl MemoryOwner for OperatorMemoryAccount {
    fn acquire_capacity(&self, _domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
        self.try_grow(bytes)
    }

    fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
        self.release(bytes);
    }

    fn record_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        add_class_bytes(&self.hot.0, &self.cold, class, bytes);
        add_tag_bytes(&self.tag_bytes, &self.domain_tag_bytes, domain, tag, bytes);
        self.parent.record_allocation(domain, tag, class, bytes);
    }

    fn release_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        sub_class_bytes(&self.hot.0, &self.cold, class, bytes);
        sub_tag_bytes(&self.tag_bytes, &self.domain_tag_bytes, domain, tag, bytes);
        self.parent.release_allocation(domain, tag, class, bytes);
    }

    fn reclassify_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        from: MemoryAccountingClass,
        to: MemoryAccountingClass,
        bytes: usize,
    ) {
        if bytes == 0 || from == to {
            return;
        }
        sub_class_bytes(&self.hot.0, &self.cold, from, bytes);
        add_class_bytes(&self.hot.0, &self.cold, to, bytes);
        self.parent
            .reclassify_allocation(domain, tag, from, to, bytes);
    }

    fn record_leaked_grant(&self, domain: MemoryDomain, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.leaked_grant_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.parent.record_leaked_grant(domain, bytes);
    }

    fn record_local_refill(&self, domain: MemoryDomain, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.local_refill_count.fetch_add(1, Ordering::Relaxed);
        self.local_refill_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.parent.record_local_refill(domain, bytes);
    }

    fn record_output_buffer_bytes(&self, domain: MemoryDomain, bytes: usize) {
        let _ = self.output_buffer_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(bytes)),
        );
        self.parent.record_output_buffer_bytes(domain, bytes);
    }
}

fn add_tag_bytes(
    tag_bytes: &[AtomicUsize; MEMORY_TAG_COUNT],
    domain_tag_bytes: &[[AtomicUsize; MEMORY_TAG_COUNT]; MEMORY_DOMAIN_COUNT],
    domain: MemoryDomain,
    tag: MemoryTag,
    bytes: usize,
) {
    tag_bytes[tag.as_index()].fetch_add(bytes, Ordering::Relaxed);
    domain_tag_bytes[domain.as_index()][tag.as_index()].fetch_add(bytes, Ordering::Relaxed);
}

fn sub_tag_bytes(
    tag_bytes: &[AtomicUsize; MEMORY_TAG_COUNT],
    domain_tag_bytes: &[[AtomicUsize; MEMORY_TAG_COUNT]; MEMORY_DOMAIN_COUNT],
    domain: MemoryDomain,
    tag: MemoryTag,
    bytes: usize,
) {
    saturating_sub(&tag_bytes[tag.as_index()], bytes);
    saturating_sub(&domain_tag_bytes[domain.as_index()][tag.as_index()], bytes);
}

fn add_class_bytes(
    hot: &HotCounters,
    cold: &ColdCounters,
    class: MemoryAccountingClass,
    bytes: usize,
) {
    match class {
        MemoryAccountingClass::NonRevocable => {
            hot.non_revocable.fetch_add(bytes, Ordering::Relaxed);
        }
        MemoryAccountingClass::Revocable => {
            hot.revocable.fetch_add(bytes, Ordering::Relaxed);
        }
        MemoryAccountingClass::Spill => {
            cold.spill.fetch_add(bytes, Ordering::Relaxed);
        }
        MemoryAccountingClass::Prefetch => {
            cold.prefetch.fetch_add(bytes, Ordering::Relaxed);
        }
        MemoryAccountingClass::Metadata => {
            cold.metadata.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

fn sub_class_bytes(
    hot: &HotCounters,
    cold: &ColdCounters,
    class: MemoryAccountingClass,
    bytes: usize,
) {
    match class {
        MemoryAccountingClass::NonRevocable => saturating_sub(&hot.non_revocable, bytes),
        MemoryAccountingClass::Revocable => saturating_sub(&hot.revocable, bytes),
        MemoryAccountingClass::Spill => saturating_sub(&cold.spill, bytes),
        MemoryAccountingClass::Prefetch => saturating_sub(&cold.prefetch, bytes),
        MemoryAccountingClass::Metadata => saturating_sub(&cold.metadata, bytes),
    }
}

fn saturating_sub(counter: &AtomicUsize, bytes: usize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(bytes))
    });
}

impl fmt::Debug for OperatorMemoryAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperatorMemoryAccount")
            .field("issued_bytes", &self.issued_bytes())
            .field("non_revocable_bytes", &self.non_revocable_bytes())
            .field("revocable_bytes", &self.revocable_bytes())
            .field("spill_bytes", &self.spill_bytes())
            .field("prefetch_bytes", &self.prefetch_bytes())
            .field("metadata_bytes", &self.metadata_bytes())
            .field("leaked_grant_bytes", &self.leaked_grant_bytes())
            .field("local_refill_count", &self.local_refill_count())
            .field("local_refill_bytes", &self.local_refill_bytes())
            .field("revoke_requested_bytes", &self.revoke_requested_bytes())
            .finish()
    }
}
