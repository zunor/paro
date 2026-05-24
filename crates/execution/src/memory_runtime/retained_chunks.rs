// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Accounted retained chunk vectors used by blocking operators.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::memory::{
    AccountedVec, AllocationLedger, MemoryAccountingClass, MemoryAccountingContext, MemoryGrant,
    MemoryResult,
};

#[derive(Debug)]
pub struct RetainedChunkVec {
    memory: MemoryAccountingContext,
    chunks: AccountedVec<Chunk>,
    ledger: AllocationLedger,
    retained_bytes: usize,
}

impl RetainedChunkVec {
    pub fn new(memory: MemoryAccountingContext) -> Self {
        let metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        Self {
            memory,
            chunks: AccountedVec::new_with_accounting(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
            ledger: AllocationLedger::new_with_accounting(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
            retained_bytes: 0,
        }
    }

    pub fn detached(tag: MemoryTag, class: MemoryAccountingClass) -> Self {
        Self::new(MemoryAccountingContext::detached(tag, class))
    }

    pub fn push(&mut self, chunk: Chunk) -> MemoryResult<()> {
        self.chunks.try_reserve(1)?;
        self.retain_chunk_allocations(&chunk)?;
        self.chunks.try_push(chunk)
    }

    pub fn append_from(&mut self, other: &mut Self) -> MemoryResult<()> {
        let additional = other.len();
        if additional == 0 {
            return Ok(());
        }

        self.chunks.try_reserve(additional)?;
        for (idx, chunk) in other.as_slice().iter().enumerate() {
            if let Err(err) = self.retain_chunk_allocations(chunk) {
                for retained in &other.as_slice()[..idx] {
                    self.release_chunk_allocations(retained);
                }
                return Err(err);
            }
        }

        // untracked_small_metadata: move scratch holds chunk handles only during ownership transfer.
        let drained = other.chunks.drain().collect::<Vec<_>>();
        for chunk in &drained {
            other.release_chunk_allocations(chunk);
        }
        for chunk in drained {
            self.chunks.try_push(chunk)?;
        }
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Chunk> {
        let chunk = self.chunks.pop()?;
        self.release_chunk_allocations(&chunk);
        Some(chunk)
    }

    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    pub fn drain_chunks(&mut self) -> Vec<Chunk> {
        let drained = self.chunks.drain().collect::<Vec<_>>();
        for chunk in &drained {
            self.release_chunk_allocations(chunk);
        }
        self.retained_bytes = 0;
        drained
    }

    pub fn as_slice(&self) -> &[Chunk] {
        self.chunks.as_slice()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Chunk> {
        self.chunks.iter()
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(Chunk::size).sum()
    }

    pub fn clone_chunks(&self) -> Vec<Chunk> {
        self.chunks.to_vec()
    }

    fn retain_chunk_allocations(&mut self, chunk: &Chunk) -> MemoryResult<()> {
        // untracked_small_metadata: temporary allocation id collection; retained ledger metadata is accounted.
        let mut entries = Vec::new();
        chunk.collect_allocation_entries(&mut entries);

        // untracked_small_metadata: rollback scratch lives only for this append attempt.
        let mut touched_ids = Vec::with_capacity(entries.len());
        let mut retained_delta = 0usize;
        for (id, bytes) in entries {
            let added = self.ledger.add(id, bytes)?;
            touched_ids.push(id);
            if added == 0 {
                continue;
            }
            if let Err(err) = self.publish_retained(added) {
                for touched in touched_ids.into_iter().rev() {
                    let released = self.ledger.remove(touched);
                    if released > 0 {
                        self.release_retained(released);
                    }
                }
                return Err(err);
            }
            retained_delta = retained_delta.saturating_add(added);
        }
        self.retained_bytes = self.retained_bytes.saturating_add(retained_delta);
        Ok(())
    }

    fn release_chunk_allocations(&mut self, chunk: &Chunk) {
        // untracked_small_metadata: temporary allocation id collection; no retained ownership.
        let mut entries = Vec::new();
        chunk.collect_allocation_entries(&mut entries);
        for (id, _) in entries {
            let released = self.ledger.remove(id);
            if released > 0 {
                self.release_retained(released);
                self.retained_bytes = self.retained_bytes.saturating_sub(released);
            }
        }
    }

    fn publish_retained(&self, bytes: usize) -> MemoryResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        if let Some(owner) = self.memory.owner() {
            owner.acquire_capacity(self.memory.domain(), bytes)?;
            owner.record_allocation(
                self.memory.domain(),
                self.memory.tag(),
                self.memory.accounting_class(),
                bytes,
            );
        }
        Ok(())
    }

    fn release_retained(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if let Some(owner) = self.memory.owner() {
            owner.release_allocation(
                self.memory.domain(),
                self.memory.tag(),
                self.memory.accounting_class(),
                bytes,
            );
            owner.release_capacity(self.memory.domain(), bytes);
        }
    }
}

impl Drop for RetainedChunkVec {
    fn drop(&mut self) {
        self.clear();
    }
}

fn grant_for_context(memory: &MemoryAccountingContext) -> MemoryGrant {
    memory.grant().expect("zero-byte retained grant should fit")
}
