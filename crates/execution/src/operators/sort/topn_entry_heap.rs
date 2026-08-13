// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::memory::{
    AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryReleaseHandle,
};

use super::{grant_for_metadata, metadata_context};

/// TopN entry whose persistent sort-key storage is query-accounted metadata.
#[derive(Debug)]
pub(super) struct TopNEntry {
    pub(super) sort_key: Vec<u8>,
    pub(super) index: usize,
    _sort_key_memory: TopNKeyReservation,
}

#[derive(Debug)]
struct TopNKeyReservation(MemoryReleaseHandle);

impl Drop for TopNKeyReservation {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl TopNEntry {
    pub(super) fn try_new(
        sort_key: Vec<u8>,
        index: usize,
        memory: &MemoryAccountingContext,
    ) -> Result<Self> {
        let reservation = metadata_context(memory).retain(sort_key.capacity())?;
        Ok(Self {
            sort_key,
            index,
            _sort_key_memory: TopNKeyReservation(reservation),
        })
    }
}

impl PartialEq for TopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}

impl Eq for TopNEntry {}

impl PartialOrd for TopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key.cmp(&other.sort_key)
    }
}

/// Query-accounted binary max-heap over an `AccountedVec` backing.
#[derive(Debug)]
pub(super) struct TopNEntryHeap {
    entries: AccountedVec<TopNEntry>,
}

impl TopNEntryHeap {
    pub(super) fn new(memory: &MemoryAccountingContext) -> Self {
        let metadata = metadata_context(memory);
        Self {
            entries: AccountedVec::new_with_accounting(
                grant_for_metadata(&metadata),
                paro_common::allocator::MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
        }
    }

    pub(super) fn try_with_capacity(
        memory: &MemoryAccountingContext,
        capacity: usize,
    ) -> Result<Self> {
        let mut heap = Self::new(memory);
        heap.entries.try_reserve(capacity)?;
        Ok(heap)
    }

    pub(super) fn try_push(&mut self, entry: TopNEntry) -> Result<()> {
        self.entries.try_push(entry)?;
        let mut child = self.entries.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if self.entries[parent] >= self.entries[child] {
                break;
            }
            self.entries.swap(parent, child);
            child = parent;
        }
        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<TopNEntry> {
        let last = self.entries.len().checked_sub(1)?;
        self.entries.swap(0, last);
        let result = self.entries.pop();
        self.sift_down(0);
        result
    }

    pub(super) fn peek(&self) -> Option<&TopNEntry> {
        self.entries.first()
    }

    pub(super) fn rebuild(&mut self) {
        for parent in (0..self.entries.len() / 2).rev() {
            self.sift_down(parent);
        }
    }

    fn sift_down(&mut self, mut parent: usize) {
        loop {
            let left = parent.saturating_mul(2).saturating_add(1);
            if left >= self.entries.len() {
                return;
            }
            let right = left + 1;
            let child = if right < self.entries.len() && self.entries[right] > self.entries[left] {
                right
            } else {
                left
            };
            if self.entries[parent] >= self.entries[child] {
                return;
            }
            self.entries.swap(parent, child);
            parent = child;
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub(super) fn iter(&self) -> std::slice::Iter<'_, TopNEntry> {
        self.entries.iter()
    }

    pub(super) fn as_slice(&self) -> &[TopNEntry] {
        self.entries.as_slice()
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [TopNEntry] {
        self.entries.as_mut_slice()
    }

    pub(super) fn drain(&mut self) -> std::vec::Drain<'_, TopNEntry> {
        self.entries.drain()
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(super) fn push_prepared(&mut self, entry: TopNEntry) {
        debug_assert!(self.entries.len() < self.entries.capacity());
        self.try_push(entry)
            .expect("pre-admitted TopN heap backing cannot grow");
    }
}
