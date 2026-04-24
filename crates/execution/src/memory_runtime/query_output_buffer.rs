// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Byte-bounded query output buffer.

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::ParoError;
use paro_common::memory::{
    AllocationId, AllocationLedger, MemoryAccountingClass, MemoryAccountingContext, MemoryResult,
};
use paro_scheduler::task::InterruptState;

use super::RetainedChunkVec;

#[derive(Debug)]
pub enum OutputAppendResult {
    Success,
    Full(Chunk),
    Closed,
}

/// Streaming query output queue with retained-byte accounting.
pub struct QueryOutputBuffer {
    chunks: VecDeque<Chunk>,
    max_buffered_bytes: usize,
    memory: MemoryAccountingContext,
    ledger: AllocationLedger,
    retained_bytes: usize,
    closed: bool,
    error: Option<ParoError>,
    blocked_sinks: VecDeque<InterruptState>,
    allocator: Arc<dyn Allocator>,
}

impl QueryOutputBuffer {
    pub fn new(
        max_buffered_bytes: usize,
        memory: MemoryAccountingContext,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        Self {
            chunks: VecDeque::new(),
            max_buffered_bytes: max_buffered_bytes.max(1),
            ledger: ledger_for_context(&memory),
            memory,
            retained_bytes: 0,
            closed: false,
            error: None,
            blocked_sinks: VecDeque::new(),
            allocator,
        }
    }

    pub fn detached(max_buffered_bytes: usize, allocator: Arc<dyn Allocator>) -> Self {
        Self::new(
            max_buffered_bytes,
            MemoryAccountingContext::detached(
                MemoryTag::Allocator,
                MemoryAccountingClass::NonRevocable,
            ),
            allocator,
        )
    }

    pub fn try_append(&mut self, chunk: Chunk) -> MemoryResult<OutputAppendResult> {
        if self.closed {
            return Ok(OutputAppendResult::Closed);
        }

        let delta = self.incremental_retained_bytes(&chunk);
        if self.retained_bytes.saturating_add(delta) > self.max_buffered_bytes {
            if self.chunks.is_empty() && delta > self.max_buffered_bytes {
                return Err(paro_common::memory::MemoryError::quota_exhausted(
                    self.memory.domain(),
                    delta,
                    self.max_buffered_bytes,
                ));
            }
            return Ok(OutputAppendResult::Full(chunk));
        }

        self.chunks.try_reserve(1).map_err(|_| {
            paro_common::memory::MemoryError::physical_allocation_failed(
                std::mem::size_of::<Chunk>(),
            )
        })?;
        self.retain_chunk_allocations(&chunk)?;
        self.chunks.push_back(chunk);
        Ok(OutputAppendResult::Success)
    }

    pub fn scan(&mut self) -> Option<Chunk> {
        let chunk = self.chunks.pop_front()?;
        self.release_chunk_allocations(&chunk);
        self.unblock_sinks();
        Some(chunk)
    }

    pub fn transfer_to_retained(
        &mut self,
        memory: MemoryAccountingContext,
    ) -> MemoryResult<RetainedChunkVec> {
        let mut retained = RetainedChunkVec::new(memory);
        while let Some(chunk) = self.chunks.front().cloned() {
            retained.push(chunk)?;
            let chunk = self
                .chunks
                .pop_front()
                .expect("front chunk was present before pop");
            self.release_chunk_allocations(&chunk);
        }
        self.unblock_sinks();
        Ok(retained)
    }

    pub fn transfer_to_session(
        &mut self,
        session_memory: MemoryAccountingContext,
    ) -> MemoryResult<RetainedChunkVec> {
        self.transfer_to_retained(session_memory)
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.retained_bytes >= self.max_buffered_bytes
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.drain_blocked_sinks();
    }

    pub fn set_error(&mut self, error: ParoError) {
        self.error = Some(error);
        self.closed = true;
        self.drain_blocked_sinks();
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn error(&self) -> Option<&ParoError> {
        self.error.as_ref()
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn max_buffered_bytes(&self) -> usize {
        self.max_buffered_bytes
    }

    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    pub fn block_sink(&mut self, interrupt_state: InterruptState) -> bool {
        if self.closed {
            return false;
        }
        self.blocked_sinks.push_back(interrupt_state);
        true
    }

    pub fn unblock_sinks(&mut self) -> usize {
        if self.is_full() {
            return 0;
        }

        let mut unblocked = 0;
        while !self.blocked_sinks.is_empty() && !self.is_full() {
            if let Some(interrupt_state) = self.blocked_sinks.pop_front() {
                let _ = interrupt_state.callback();
                unblocked += 1;
            }
        }
        unblocked
    }

    fn drain_blocked_sinks(&mut self) -> usize {
        let mut unblocked = 0;
        while let Some(interrupt_state) = self.blocked_sinks.pop_front() {
            let _ = interrupt_state.callback();
            unblocked += 1;
        }
        unblocked
    }

    pub fn blocked_sink_count(&self) -> usize {
        self.blocked_sinks.len()
    }

    fn incremental_retained_bytes(&self, chunk: &Chunk) -> usize {
        // untracked_small_metadata: stack-local allocation id scratch; bounded by vectors in one chunk.
        let mut entries = Vec::new();
        chunk.collect_allocation_entries(&mut entries);
        // untracked_small_metadata: per-append duplicate filter for the same scratch set.
        let mut new_ids = Vec::new();
        let mut delta = 0usize;
        for (id, bytes) in entries {
            if self.ledger.contains(id) || new_ids.contains(&id) {
                continue;
            }
            new_ids.push(id);
            delta = delta.saturating_add(bytes);
        }
        delta
    }

    fn retain_chunk_allocations(&mut self, chunk: &Chunk) -> MemoryResult<()> {
        // untracked_small_metadata: temporary allocation id collection; retained ledger metadata is accounted.
        let mut entries = Vec::new();
        chunk.collect_allocation_entries(&mut entries);

        // untracked_small_metadata: rollback scratch lives only for this append attempt.
        let mut touched_ids = Vec::<AllocationId>::with_capacity(entries.len());
        let mut retained_delta = 0usize;
        for (id, bytes) in entries {
            let added = match self.ledger.add(id, bytes) {
                Ok(added) => added,
                Err(err) => {
                    for touched in touched_ids.into_iter().rev() {
                        let _ = self.ledger.remove(touched);
                    }
                    return Err(err);
                }
            };
            touched_ids.push(id);
            retained_delta = retained_delta.saturating_add(added);
        }

        if let Err(err) = self.publish_retained(retained_delta) {
            for touched in touched_ids.into_iter().rev() {
                let _ = self.ledger.remove(touched);
            }
            return Err(err);
        }

        self.retained_bytes = self.retained_bytes.saturating_add(retained_delta);
        if let Some(owner) = self.memory.owner() {
            owner.record_output_buffer_bytes(self.memory.domain(), self.retained_bytes);
        }
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

fn ledger_for_context(memory: &MemoryAccountingContext) -> AllocationLedger {
    let metadata_memory =
        memory.with_tag_and_class(MemoryTag::Metadata, MemoryAccountingClass::Metadata);
    AllocationLedger::new_with_accounting(
        metadata_memory
            .grant()
            .expect("zero-byte output ledger metadata grant should fit"),
        MemoryTag::Metadata,
        MemoryAccountingClass::Metadata,
    )
}

impl Drop for QueryOutputBuffer {
    fn drop(&mut self) {
        self.drain_blocked_sinks();
        while let Some(chunk) = self.chunks.pop_front() {
            self.release_chunk_allocations(&chunk);
        }
    }
}

impl std::fmt::Debug for QueryOutputBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryOutputBuffer")
            .field("len", &self.len())
            .field("retained_bytes", &self.retained_bytes)
            .field("max_buffered_bytes", &self.max_buffered_bytes)
            .field("closed", &self.closed)
            .field("blocked_sinks", &self.blocked_sinks.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use paro_common::allocator::default_allocator;
    use paro_scheduler::task::InterruptState;

    use super::QueryOutputBuffer;

    fn counting_interrupt(counter: Arc<AtomicUsize>) -> InterruptState {
        InterruptState::with_callback(Arc::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
    }

    #[test]
    fn close_wakes_blocked_sinks_even_when_buffer_is_full() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut buffer = QueryOutputBuffer::detached(1, Arc::new(default_allocator()));
        buffer.retained_bytes = buffer.max_buffered_bytes;
        assert!(buffer.block_sink(counting_interrupt(calls.clone())));

        buffer.close();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(buffer.blocked_sink_count(), 0);
    }

    #[test]
    fn drop_wakes_blocked_sinks() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut buffer = QueryOutputBuffer::detached(1, Arc::new(default_allocator()));
        assert!(buffer.block_sink(counting_interrupt(calls.clone())));

        drop(buffer);

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
