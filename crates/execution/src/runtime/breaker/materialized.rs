// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generic materialization breaker handle.
//!
//! Writers append task-local chunks during `merge_local()`, then `finish()`
//! seals the handle into immutable chunk storage. Consumers capture that
//! immutable snapshot in their operator-global state, keeping read cursors and
//! synchronization out of the producer-owned handle.

#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Clone, Debug)]
pub struct FoundBits {
    bits: Arc<[AtomicU8]>,
}

impl FoundBits {
    fn new(total_rows: usize) -> Self {
        Self {
            bits: allocate_found_bits(total_rows),
        }
    }

    #[inline]
    pub fn mark(&self, global_idx: usize) {
        let byte_idx = global_idx / 8;
        let bit = 1u8 << (global_idx % 8);
        if byte_idx < self.bits.len() {
            self.bits[byte_idx].fetch_or(bit, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn is_marked(&self, global_idx: usize) -> bool {
        let byte_idx = global_idx / 8;
        let bit = 1u8 << (global_idx % 8);
        byte_idx < self.bits.len() && self.bits[byte_idx].load(Ordering::Relaxed) & bit != 0
    }
}

#[derive(Debug)]
pub struct MaterializedHandle {
    metadata: BreakerHandleMetadata,
    pending_chunks: Mutex<Vec<Chunk>>,
    sealed_chunks: Mutex<Option<Arc<[Chunk]>>>,
    #[cfg(test)]
    sealed_chunk_reads: AtomicUsize,
    sealed: AtomicBool,
    cleanup: CleanupState,
    found_bits: Mutex<Option<FoundBits>>,
}

/// Runtime-scoped reader for one immutable materialized-handle snapshot.
///
/// Pipeline globals may be created before their producer runs, so the snapshot
/// is bound lazily after sealing. Keeping the cache outside `MaterializedHandle`
/// is important: recursive execution resets and reuses the producer handle,
/// while each new pipeline runtime gets a fresh reader for that iteration.
#[derive(Debug)]
pub struct MaterializedReader {
    handle: Arc<MaterializedHandle>,
    consumer: &'static str,
    chunks: OnceLock<Arc<[Chunk]>>,
}

impl MaterializedReader {
    pub fn new(handle: Arc<MaterializedHandle>, consumer: &'static str) -> Self {
        Self {
            handle,
            consumer,
            chunks: OnceLock::new(),
        }
    }

    pub fn initialize_found_bits_for_chunks(&self, chunks: &[Chunk]) -> FoundBits {
        self.handle.initialize_found_bits_for_chunks(chunks)
    }

    pub fn found_bits(&self) -> Option<FoundBits> {
        self.handle.found_bits()
    }

    /// Binds and returns this runtime's immutable snapshot.
    ///
    /// A sealed handle without chunks violates the publication invariant:
    /// `MaterializedHandle::seal` stores the snapshot before its release-store
    /// to `sealed`, and readers acquire-load that flag here. Recursive resets
    /// are scheduled only after the prior runtime has stopped polling.
    pub fn sealed_chunks(&self) -> Result<&Arc<[Chunk]>> {
        if let Some(chunks) = self.chunks.get() {
            return Ok(chunks);
        }
        if !self.handle.is_sealed() {
            return Err(paro_error::internal(format!(
                "{} was scheduled before producer sealed the handle",
                self.consumer
            )));
        }
        let chunks = self.handle.sealed_chunks().ok_or_else(|| {
            paro_error::internal(format!("sealed {} chunks are missing", self.consumer))
        })?;
        let _ = self.chunks.set(chunks);
        Ok(self
            .chunks
            .get()
            .expect("materialized reader snapshot initialized above"))
    }
}

impl MaterializedHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            pending_chunks: Mutex::new(Vec::new()),
            sealed_chunks: Mutex::new(None),
            #[cfg(test)]
            sealed_chunk_reads: AtomicUsize::new(0),
            sealed: AtomicBool::new(false),
            cleanup: CleanupState::default(),
            found_bits: Mutex::new(None),
        }
    }

    /// Returns a cheap snapshot handle for hot row loops to avoid touching the
    /// resettable handle mutex per row.
    pub fn initialize_found_bits_for_chunks(&self, chunks: &[Chunk]) -> FoundBits {
        let mut found_bits = self.found_bits.lock();
        if let Some(bits) = found_bits.as_ref() {
            return bits.clone();
        }
        let total_rows = chunks.iter().map(Chunk::size).sum();
        let bits = FoundBits::new(total_rows);
        *found_bits = Some(bits.clone());
        bits
    }

    pub fn found_bits(&self) -> Option<FoundBits> {
        self.found_bits.lock().clone()
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn append_chunks(&self, chunks: &mut Vec<Chunk>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append to a sealed materialized breaker handle",
            ));
        }
        self.pending_chunks.lock().extend(chunks.drain(..));
        Ok(())
    }

    pub fn seal(&self) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }

        let chunks = {
            let mut pending = self.pending_chunks.lock();
            std::mem::take(&mut *pending)
        };
        *self.sealed_chunks.lock() = Some(Arc::from(chunks.into_boxed_slice()));
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn sealed_chunks(&self) -> Option<Arc<[Chunk]>> {
        #[cfg(test)]
        self.sealed_chunk_reads.fetch_add(1, Ordering::Relaxed);
        self.sealed_chunks.lock().clone()
    }

    #[cfg(test)]
    fn sealed_chunk_read_count(&self) -> usize {
        self.sealed_chunk_reads.load(Ordering::Relaxed)
    }

    pub fn reset_for_reuse(&self) {
        debug_assert!(
            !self.cleanup.is_cleaned(),
            "cannot reset a cleaned materialized breaker handle"
        );
        self.pending_chunks.lock().clear();
        *self.sealed_chunks.lock() = None;
        *self.found_bits.lock() = None;
        self.sealed.store(false, Ordering::Release);
    }

    #[inline]
    pub fn pending_chunk_count(&self) -> usize {
        self.pending_chunks.lock().len()
    }

    #[inline]
    pub fn sealed_chunk_count(&self) -> usize {
        self.sealed_chunks
            .lock()
            .as_ref()
            .map(|chunks| chunks.len())
            .unwrap_or(0)
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

fn allocate_found_bits(total_rows: usize) -> Arc<[AtomicU8]> {
    let byte_count = (total_rows + 7) / 8;
    Arc::from(
        (0..byte_count)
            .map(|_| AtomicU8::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    )
}

impl RuntimeCleanup for MaterializedHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.pending_chunks.lock().clear();
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};

    use super::*;

    fn metadata() -> BreakerHandleMetadata {
        BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind: BreakerHandleKind::Materialized,
            row_type: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Box::new([]),
            properties: PipelineProperties::default(),
        }
    }

    #[test]
    fn materialized_handle_seals_chunks_into_an_immutable_snapshot() {
        let handle = MaterializedHandle::new(metadata());
        let vector =
            Vector::try_from_i32(&[1, 2, 3], paro_common::test_utils::test_allocator()).unwrap();
        let mut chunks = vec![paro_common::test_utils::test_chunk_from_vectors(vec![
            vector,
        ])];
        handle.append_chunks(&mut chunks).unwrap();
        assert!(chunks.is_empty());
        assert_eq!(handle.pending_chunk_count(), 1);

        handle.seal().unwrap();
        assert!(handle.is_sealed());
        assert_eq!(handle.sealed_chunk_count(), 1);
    }

    #[test]
    fn materialized_handle_resets_for_recursive_iteration_reuse() {
        let handle = MaterializedHandle::new(metadata());
        let vector =
            Vector::try_from_i32(&[1, 2, 3], paro_common::test_utils::test_allocator()).unwrap();
        let mut chunks = vec![paro_common::test_utils::test_chunk_from_vectors(vec![
            vector,
        ])];
        handle.append_chunks(&mut chunks).unwrap();
        handle.seal().unwrap();
        let sealed = handle.sealed_chunks().expect("sealed chunks");
        let found_bits = handle.initialize_found_bits_for_chunks(sealed.as_ref());
        found_bits.mark(0);
        assert!(found_bits.is_marked(0));

        handle.reset_for_reuse();

        assert!(!handle.is_sealed());
        assert_eq!(handle.pending_chunk_count(), 0);
        assert_eq!(handle.sealed_chunk_count(), 0);
        assert!(handle.found_bits().is_none());

        let vector = Vector::try_from_i32(&[4], paro_common::test_utils::test_allocator()).unwrap();
        let mut chunks = vec![paro_common::test_utils::test_chunk_from_vectors(vec![
            vector,
        ])];
        handle.append_chunks(&mut chunks).unwrap();
        handle.seal().unwrap();
        assert_eq!(handle.sealed_chunk_count(), 1);
    }

    #[test]
    fn materialized_reader_binds_once_and_is_scoped_to_one_runtime() {
        let handle = Arc::new(MaterializedHandle::new(metadata()));
        let allocator = paro_common::test_utils::test_allocator();
        let mut chunks = vec![paro_common::test_utils::test_chunk_from_vectors(vec![
            Vector::try_from_i32(&[10], allocator.clone()).unwrap(),
        ])];
        handle.append_chunks(&mut chunks).unwrap();
        handle.seal().unwrap();

        let first_runtime = MaterializedReader::new(Arc::clone(&handle), "test consumer");
        let first_snapshot = first_runtime.sealed_chunks().unwrap();
        assert!(std::ptr::eq(
            first_snapshot,
            first_runtime.sealed_chunks().unwrap()
        ));
        assert_eq!(handle.sealed_chunk_read_count(), 1);

        handle.reset_for_reuse();
        let mut chunks = vec![paro_common::test_utils::test_chunk_from_vectors(vec![
            Vector::try_from_i32(&[20], allocator).unwrap(),
        ])];
        handle.append_chunks(&mut chunks).unwrap();
        handle.seal().unwrap();

        assert_eq!(
            first_runtime.sealed_chunks().unwrap()[0]
                .column(0)
                .unwrap()
                .get_i32(0),
            Some(10)
        );
        let second_runtime = MaterializedReader::new(Arc::clone(&handle), "test consumer");
        assert_eq!(
            second_runtime.sealed_chunks().unwrap()[0]
                .column(0)
                .unwrap()
                .get_i32(0),
            Some(20)
        );
        assert_eq!(handle.sealed_chunk_read_count(), 2);
    }
}
