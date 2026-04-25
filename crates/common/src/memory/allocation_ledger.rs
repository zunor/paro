// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation identity ledger with reference counts.

use std::hash::Hash;

use crate::allocator::MemoryTag;

use super::{AccountedHashMap, MemoryAccountingClass, MemoryGrant, MemoryResult};

/// Stable allocation identity used by retained chunk buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllocationId(pub u64);

/// Allocation ledger entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocationEntry {
    pub bytes: usize,
    pub ref_count: u32,
}

/// Refcounted allocation ledger.
#[derive(Debug)]
pub struct AllocationLedger {
    entries: AccountedHashMap<AllocationId, AllocationEntry>,
    total_bytes: usize,
}

impl AllocationLedger {
    pub fn new(grant: MemoryGrant) -> Self {
        Self {
            entries: AccountedHashMap::new(grant),
            total_bytes: 0,
        }
    }

    pub fn new_with_accounting(
        grant: MemoryGrant,
        tag: MemoryTag,
        class: MemoryAccountingClass,
    ) -> Self {
        Self {
            entries: AccountedHashMap::new_with_accounting(grant, tag, class),
            total_bytes: 0,
        }
    }

    pub fn add(&mut self, id: AllocationId, bytes: usize) -> MemoryResult<usize> {
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.ref_count = entry.ref_count.saturating_add(1);
            return Ok(0);
        }
        self.entries.try_insert(
            id,
            AllocationEntry {
                bytes,
                ref_count: 1,
            },
        )?;
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        Ok(bytes)
    }

    pub fn remove(&mut self, id: AllocationId) -> usize {
        let Some(entry) = self.entries.get_mut(&id) else {
            return 0;
        };
        if entry.ref_count > 1 {
            entry.ref_count -= 1;
            return 0;
        }
        let bytes = entry.bytes;
        let _ = self.entries.remove(&id);
        self.total_bytes = self.total_bytes.saturating_sub(bytes);
        bytes
    }

    pub fn contains(&self, id: AllocationId) -> bool {
        self.entries.contains_key(&id)
    }

    pub fn clear(&mut self) -> usize {
        let bytes = self.total_bytes;
        self.entries.clear();
        self.total_bytes = 0;
        bytes
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
