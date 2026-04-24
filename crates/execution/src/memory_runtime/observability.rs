// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Memory runtime snapshots used by metrics and EXPLAIN.

use paro_common::allocator::MemoryTag;
use paro_common::memory::MemoryDomain;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryTagBytes {
    pub tag: MemoryTag,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryDomainTagBytes {
    pub domain: MemoryDomain,
    pub tag: MemoryTag,
    pub bytes: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryRuntimeStats {
    pub capacity_bytes: usize,
    pub issued_bytes: usize,
    pub published_used_bytes: usize,
    pub non_revocable_bytes: usize,
    pub revocable_bytes: usize,
    pub spill_bytes: usize,
    pub prefetch_bytes: usize,
    pub metadata_bytes: usize,
    pub reclaimable_bytes: usize,
    pub leaked_grant_bytes: usize,
    pub local_refill_count: usize,
    pub local_refill_bytes: usize,
    pub reclaim_attempt_count: usize,
    pub reclaimed_bytes: usize,
    pub spilled_bytes: usize,
    pub reclaim_latency_us: usize,
    pub spill_latency_us: usize,
    pub output_buffer_bytes: usize,
    pub tag_bytes: Vec<MemoryTagBytes>,
    pub domain_tag_bytes: Vec<MemoryDomainTagBytes>,
}
