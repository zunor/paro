// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared memory owner interface used by low-level memory primitives.

use crate::allocator::MemoryTag;

use super::{MemoryDomain, MemoryResult};

/// Reclaim semantics for a committed memory allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MemoryAccountingClass {
    /// Committed state that cannot be reclaimed without destroying operator semantics.
    #[default]
    NonRevocable,
    /// Operator payload that can be reclaimed or spilled at a safe point.
    Revocable,
    /// Bytes already moved to spill files. Tracked for observability, not pressure.
    Spill,
    /// In-flight prefetch budget that should be released before query working set.
    Prefetch,
    /// Small bookkeeping structures.
    Metadata,
}

impl MemoryAccountingClass {
    /// Conservative fallback for legacy callers that have not yet split payload classes.
    pub fn default_for_tag(tag: MemoryTag) -> Self {
        match tag {
            MemoryTag::ExternalFileCache => Self::Prefetch,
            MemoryTag::Metadata => Self::Metadata,
            _ => Self::NonRevocable,
        }
    }
}

/// Owner/account that backs memory grants and allocation release handles.
pub trait MemoryOwner: Send + Sync + std::fmt::Debug {
    /// Acquire logical capacity for a grant.
    fn acquire_capacity(&self, domain: MemoryDomain, bytes: usize) -> MemoryResult<()>;

    /// Release logical capacity previously issued to a grant.
    fn release_capacity(&self, domain: MemoryDomain, bytes: usize);

    /// Publish an allocation delta for observability/accounting.
    fn record_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    );

    /// Publish a deallocation delta for observability/accounting.
    fn release_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    );

    /// Move already-published bytes between accounting classes without changing capacity.
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
        self.release_allocation(domain, tag, from, bytes);
        self.record_allocation(domain, tag, to, bytes);
    }

    /// Record a grant drop that still had consumed bytes.
    fn record_leaked_grant(&self, _domain: MemoryDomain, _bytes: usize) {}

    /// Record local grant refill activity for performance diagnostics.
    fn record_local_refill(&self, _domain: MemoryDomain, _bytes: usize) {}

    /// Record the current retained output-buffer byte count.
    fn record_output_buffer_bytes(&self, _domain: MemoryDomain, _bytes: usize) {}
}

/// Memory owner that only enforces local grant capacity.
#[derive(Debug, Default)]
pub struct DetachedMemoryOwner;

impl MemoryOwner for DetachedMemoryOwner {
    fn acquire_capacity(&self, _domain: MemoryDomain, _bytes: usize) -> MemoryResult<()> {
        Ok(())
    }

    fn release_capacity(&self, _domain: MemoryDomain, _bytes: usize) {}

    fn record_allocation(
        &self,
        _domain: MemoryDomain,
        _tag: MemoryTag,
        _class: MemoryAccountingClass,
        _bytes: usize,
    ) {
    }

    fn release_allocation(
        &self,
        _domain: MemoryDomain,
        _tag: MemoryTag,
        _class: MemoryAccountingClass,
        _bytes: usize,
    ) {
    }
}
