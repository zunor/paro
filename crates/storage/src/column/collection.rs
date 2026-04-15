// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ColumnDataCollection - columnar intermediate storage with optional buffer-managed backing.

use std::sync::{Arc, Mutex};

use crate::buffer::{BlockId, BufferPool, MemoryTag};
use crate::column::allocator::{
    ChunkManagementState, ColumnDataAllocator, ColumnDataAllocatorType,
};
use crate::wal::wal_entry::SerializedDataChunk;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

#[derive(Debug, Default)]
pub struct ColumnDataAppendState {
    pub current_chunk_state: ChunkManagementState,
}

impl ColumnDataAppendState {
    pub fn new() -> Self {
        Self {
            current_chunk_state: ChunkManagementState::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ColumnDataScanState {
    pub current_chunk_state: ChunkManagementState,
    pub chunk_index: usize,
    pub column_ids: Vec<usize>,
}

impl ColumnDataScanState {
    pub fn new() -> Self {
        Self {
            current_chunk_state: ChunkManagementState::new(),
            chunk_index: 0,
            column_ids: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ColumnDataParallelScanState {
    pub scan_state: ColumnDataScanState,
    pub lock: Mutex<()>,
}

impl ColumnDataParallelScanState {
    pub fn new() -> Self {
        Self {
            scan_state: ColumnDataScanState::new(),
            lock: Mutex::new(()),
        }
    }
}

#[derive(Debug, Default)]
pub struct ColumnDataLocalScanState {
    pub current_chunk_state: ChunkManagementState,
    pub current_chunk_index: Option<usize>,
}

impl ColumnDataLocalScanState {
    pub fn new() -> Self {
        Self {
            current_chunk_state: ChunkManagementState::new(),
            current_chunk_index: None,
        }
    }
}

#[derive(Debug)]
enum StoredChunk {
    InMemory {
        chunk: Chunk,
        serialized_size: usize,
    },
    BufferManaged {
        block_id: i64,
        byte_len: usize,
        row_count: usize,
    },
}

impl StoredChunk {
    fn row_count(&self) -> usize {
        match self {
            StoredChunk::InMemory { chunk, .. } => chunk.size(),
            StoredChunk::BufferManaged { row_count, .. } => *row_count,
        }
    }

    fn size_in_bytes(&self) -> usize {
        match self {
            StoredChunk::InMemory {
                serialized_size, ..
            } => *serialized_size,
            StoredChunk::BufferManaged { byte_len, .. } => *byte_len,
        }
    }
}

/// Columnar data collection with row-batch append and scan interfaces.
#[derive(Debug)]
pub struct ColumnDataCollection {
    allocator: Arc<ColumnDataAllocator>,
    types: Vec<LogicalType>,
    chunks: Vec<Option<StoredChunk>>,
    count: usize,
    size_in_bytes: usize,
}

impl ColumnDataCollection {
    pub fn new(allocator: Arc<ColumnDataAllocator>, types: Vec<LogicalType>) -> Self {
        Self {
            allocator,
            types,
            chunks: Vec::new(),
            count: 0,
            size_in_bytes: 0,
        }
    }

    pub fn with_buffer_pool(
        buffer_pool: Arc<BufferPool>,
        types: Vec<LogicalType>,
        memory_tag: MemoryTag,
        allocator_type: ColumnDataAllocatorType,
    ) -> Self {
        let allocator = match allocator_type {
            ColumnDataAllocatorType::InMemoryAllocator => {
                Arc::new(ColumnDataAllocator::in_memory())
            }
            ColumnDataAllocatorType::BufferManagerAllocator => {
                Arc::new(ColumnDataAllocator::buffer_manager(buffer_pool, memory_tag))
            }
            ColumnDataAllocatorType::Hybrid => {
                Arc::new(ColumnDataAllocator::hybrid(buffer_pool, memory_tag))
            }
        };
        Self::new(allocator, types)
    }

    pub fn allocator(&self) -> &Arc<ColumnDataAllocator> {
        &self.allocator
    }

    pub fn allocator_type(&self) -> ColumnDataAllocatorType {
        self.allocator.allocator_type()
    }

    pub fn types(&self) -> &[LogicalType] {
        &self.types
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.iter().filter(|entry| entry.is_some()).count()
    }

    pub fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    pub fn initialize_append(&self, state: &mut ColumnDataAppendState) {
        state.current_chunk_state.clear();
    }

    pub fn append(&mut self, state: &mut ColumnDataAppendState, input: &Chunk) -> Result<()> {
        self.assert_chunk_types(input)?;
        if input.size() == 0 {
            return Ok(());
        }

        let serialized = SerializedDataChunk::from_chunk(input)?;
        let bytes = serialized.serialize();
        let stored = if self.allocator.is_buffer_managed() {
            let block_id = self.allocator.allocate_serialized_chunk(&bytes)?;
            StoredChunk::BufferManaged {
                block_id,
                byte_len: bytes.len(),
                row_count: input.size(),
            }
        } else {
            StoredChunk::InMemory {
                chunk: input.clone(),
                serialized_size: bytes.len(),
            }
        };

        self.count += input.size();
        self.size_in_bytes += stored.size_in_bytes();
        self.chunks.push(Some(stored));
        state.current_chunk_state.clear();
        Ok(())
    }

    pub fn append_chunk(&mut self, input: &Chunk) -> Result<()> {
        let mut state = ColumnDataAppendState::new();
        self.initialize_append(&mut state);
        self.append(&mut state, input)
    }

    pub fn initialize_scan(&self, state: &mut ColumnDataScanState, column_ids: Option<Vec<usize>>) {
        state.current_chunk_state.clear();
        state.chunk_index = 0;
        state.column_ids = column_ids.unwrap_or_else(|| (0..self.types.len()).collect());
    }

    pub fn initialize_parallel_scan(
        &self,
        state: &mut ColumnDataParallelScanState,
        column_ids: Option<Vec<usize>>,
    ) {
        self.initialize_scan(&mut state.scan_state, column_ids);
    }

    pub fn scan(&self, state: &mut ColumnDataScanState, output: &mut Chunk) -> Result<bool> {
        let Some(storage_index) = self.next_live_chunk_index(state.chunk_index) else {
            output.set_cardinality(0);
            return Ok(false);
        };

        state.chunk_index = storage_index + 1;
        self.fetch_chunk_by_storage_index(
            storage_index,
            &state.column_ids,
            &mut state.current_chunk_state,
            output,
        )?;
        Ok(true)
    }

    pub fn scan_parallel(
        &self,
        gstate: &mut ColumnDataParallelScanState,
        lstate: &mut ColumnDataLocalScanState,
        output: &mut Chunk,
    ) -> Result<bool> {
        let column_ids = if gstate.scan_state.column_ids.is_empty() {
            (0..self.types.len()).collect::<Vec<_>>()
        } else {
            gstate.scan_state.column_ids.clone()
        };

        let assigned = {
            let _guard = gstate.lock.lock().unwrap();
            let next = self.next_live_chunk_index(gstate.scan_state.chunk_index);
            if let Some(idx) = next {
                gstate.scan_state.chunk_index = idx + 1;
            }
            next
        };

        let Some(storage_index) = assigned else {
            output.set_cardinality(0);
            return Ok(false);
        };

        self.fetch_chunk_by_storage_index(
            storage_index,
            &column_ids,
            &mut lstate.current_chunk_state,
            output,
        )?;
        lstate.current_chunk_index = Some(storage_index);
        Ok(true)
    }

    pub fn combine(&mut self, other: &mut ColumnDataCollection) -> Result<()> {
        if self.types != other.types {
            return Err(paro_error::invalid_input(
                "Cannot combine ColumnDataCollection with mismatched types",
            ));
        }
        if self.allocator_type() != other.allocator_type() {
            return Err(paro_error::invalid_input(
                "Cannot combine ColumnDataCollection with mismatched allocator types",
            ));
        }

        self.count += other.count;
        self.size_in_bytes += other.size_in_bytes;
        self.chunks.append(&mut other.chunks);

        other.count = 0;
        other.size_in_bytes = 0;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        let mut to_release = Vec::new();
        for entry in &mut self.chunks {
            if let Some(stored) = entry.take() {
                to_release.push(stored);
            }
        }
        for stored in to_release {
            self.release_stored_chunk(stored)?;
        }
        self.chunks.clear();
        self.count = 0;
        self.size_in_bytes = 0;
        Ok(())
    }

    pub fn consume_chunk(&mut self, storage_index: usize) -> Result<()> {
        let stored = self
            .chunks
            .get_mut(storage_index)
            .and_then(|entry| entry.take());
        if let Some(stored) = stored {
            self.release_stored_chunk(stored)?;
        }
        Ok(())
    }

    pub fn chunk_storage_indexes(&self) -> Vec<usize> {
        self.chunks
            .iter()
            .enumerate()
            .filter_map(|(idx, entry)| entry.as_ref().map(|_| idx))
            .collect()
    }

    pub fn chunk_block_id(&self, storage_index: usize) -> Option<BlockId> {
        self.chunks
            .get(storage_index)
            .and_then(|entry| entry.as_ref())
            .and_then(|stored| match stored {
                StoredChunk::BufferManaged { block_id, .. } => Some(*block_id),
                StoredChunk::InMemory { .. } => None,
            })
    }

    pub(crate) fn chunk_min_block_id(&self, storage_index: usize) -> Option<BlockId> {
        self.chunk_block_id(storage_index)
    }

    pub fn fetch_chunk(&self, chunk_idx: usize, output: &mut Chunk) -> Result<usize> {
        let storage_index = self
            .chunk_storage_indexes()
            .get(chunk_idx)
            .copied()
            .ok_or_else(|| {
                paro_error::internal(format!("Chunk index {} out of bounds", chunk_idx))
            })?;

        let mut state = ChunkManagementState::new();
        let column_ids: Vec<usize> = (0..self.types.len()).collect();
        self.fetch_chunk_by_storage_index(storage_index, &column_ids, &mut state, output)
    }

    pub fn fetch_chunk_by_storage_index(
        &self,
        storage_index: usize,
        column_ids: &[usize],
        state: &mut ChunkManagementState,
        output: &mut Chunk,
    ) -> Result<usize> {
        let entry = self
            .chunks
            .get(storage_index)
            .and_then(|entry| entry.as_ref())
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Chunk storage index {} is not available",
                    storage_index
                ))
            })?;

        let full_chunk = match entry {
            StoredChunk::InMemory { chunk, .. } => chunk.clone(),
            StoredChunk::BufferManaged {
                block_id, byte_len, ..
            } => {
                let bytes = self
                    .allocator
                    .read_block_bytes(*block_id, *byte_len, state)?;
                let mut offset = 0;
                let serialized = SerializedDataChunk::deserialize(&bytes, &mut offset)?;
                serialized.to_chunk()?
            }
        };

        let projected = self.project_chunk(&full_chunk, column_ids)?;
        output.reference(&projected);
        Ok(projected.size())
    }

    fn next_live_chunk_index(&self, start: usize) -> Option<usize> {
        (start..self.chunks.len()).find(|idx| self.chunks[*idx].is_some())
    }

    fn project_chunk(&self, chunk: &Chunk, column_ids: &[usize]) -> Result<Chunk> {
        if column_ids.len() == chunk.column_count()
            && column_ids.iter().enumerate().all(|(idx, col)| idx == *col)
        {
            return Ok(chunk.clone());
        }

        let mut columns = Vec::with_capacity(column_ids.len());
        for col_idx in column_ids {
            let column = chunk.column(*col_idx).ok_or_else(|| {
                paro_error::internal(format!("Projected column index {} out of bounds", col_idx))
            })?;
            columns.push(Arc::clone(column));
        }

        Ok(Chunk::from_arc_vectors(columns))
    }

    fn assert_chunk_types(&self, chunk: &Chunk) -> Result<()> {
        if chunk.column_count() != self.types.len() {
            return Err(paro_error::invalid_input(format!(
                "Column count mismatch: expected {}, got {}",
                self.types.len(),
                chunk.column_count()
            )));
        }

        let input_types = chunk.types();
        for (idx, (expected, actual)) in self.types.iter().zip(input_types.iter()).enumerate() {
            if expected != actual {
                return Err(paro_error::invalid_input(format!(
                    "Column type mismatch at index {}: expected {:?}, got {:?}",
                    idx, expected, actual
                )));
            }
        }
        Ok(())
    }

    fn release_stored_chunk(&mut self, stored: StoredChunk) -> Result<()> {
        self.count = self.count.saturating_sub(stored.row_count());
        self.size_in_bytes = self.size_in_bytes.saturating_sub(stored.size_in_bytes());

        if let StoredChunk::BufferManaged { block_id, .. } = stored {
            self.allocator.free_block(block_id)?;
        }
        Ok(())
    }
}

impl Drop for ColumnDataCollection {
    fn drop(&mut self) {
        let _ = self.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::MemoryTag;
    use paro_common::vector::Vector;

    fn build_test_chunk(start: i32, count: usize) -> Chunk {
        Chunk::from_vectors(vec![Vector::from_i32(
            &(start..start + count as i32).collect::<Vec<_>>(),
        )])
    }

    #[test]
    fn test_in_memory_append_scan_roundtrip() {
        let allocator = Arc::new(ColumnDataAllocator::in_memory());
        let mut collection = ColumnDataCollection::new(allocator, vec![LogicalType::Integer]);

        let chunk = build_test_chunk(0, 64);
        collection.append_chunk(&chunk).unwrap();
        assert_eq!(collection.count(), 64);
        assert_eq!(collection.chunk_count(), 1);

        let mut state = ColumnDataScanState::new();
        collection.initialize_scan(&mut state, None);
        let mut out = Chunk::initialize(&[LogicalType::Integer], 64);
        assert!(collection.scan(&mut state, &mut out).unwrap());
        assert_eq!(out.size(), 64);
        assert_eq!(out.column(0).unwrap().get_i32(0), Some(0));
        assert_eq!(out.column(0).unwrap().get_i32(63), Some(63));
        assert!(!collection.scan(&mut state, &mut out).unwrap());
    }

    #[test]
    fn test_parallel_scan_roundtrip() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut collection = ColumnDataCollection::with_buffer_pool(
            pool,
            vec![LogicalType::Integer],
            MemoryTag::ColumnData,
            ColumnDataAllocatorType::BufferManagerAllocator,
        );

        collection.append_chunk(&build_test_chunk(0, 64)).unwrap();
        collection.append_chunk(&build_test_chunk(64, 64)).unwrap();

        let mut gstate = ColumnDataParallelScanState::new();
        collection.initialize_parallel_scan(&mut gstate, None);

        let mut lstate1 = ColumnDataLocalScanState::new();
        let mut lstate2 = ColumnDataLocalScanState::new();
        let mut out1 = Chunk::initialize(&[LogicalType::Integer], 64);
        let mut out2 = Chunk::initialize(&[LogicalType::Integer], 64);

        let mut values = Vec::new();
        loop {
            let has1 = collection
                .scan_parallel(&mut gstate, &mut lstate1, &mut out1)
                .unwrap();
            if has1 {
                for i in 0..out1.size() {
                    values.push(out1.column(0).unwrap().get_i32(i).unwrap());
                }
            }

            let has2 = collection
                .scan_parallel(&mut gstate, &mut lstate2, &mut out2)
                .unwrap();
            if has2 {
                for i in 0..out2.size() {
                    values.push(out2.column(0).unwrap().get_i32(i).unwrap());
                }
            }

            if !has1 && !has2 {
                break;
            }
        }

        values.sort_unstable();
        let expected: Vec<i32> = (0..128).collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn test_eviction_reload_roundtrip() {
        let pool = BufferPool::new_arc(8 * 1024 * 1024);
        let temp_dir = std::env::temp_dir().join(format!(
            "paro_column_roundtrip_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("t")
        ));
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();

        let mut collection = ColumnDataCollection::with_buffer_pool(
            pool.clone(),
            vec![LogicalType::Integer],
            MemoryTag::ColumnData,
            ColumnDataAllocatorType::BufferManagerAllocator,
        );

        collection.append_chunk(&build_test_chunk(0, 2048)).unwrap();
        collection
            .append_chunk(&build_test_chunk(2048, 2048))
            .unwrap();

        for idx in collection.chunk_storage_indexes() {
            if let Some(Some(StoredChunk::BufferManaged { block_id, .. })) =
                collection.chunks.get(idx)
            {
                pool.add_to_eviction_queue(*block_id);
            }
        }
        let evicted = pool.evict_blocks(MemoryTag::ColumnData, 0, 0, None);
        assert!(evicted.success);
        assert!(pool.get_temporary_spill_metrics().write_bytes > 0);

        let mut out = Chunk::initialize(&[LogicalType::Integer], 4096);
        let scanned = collection.fetch_chunk(0, &mut out).unwrap();
        assert_eq!(scanned, 2048);
        assert_eq!(out.column(0).unwrap().get_i32(0), Some(0));
        assert_eq!(out.column(0).unwrap().get_i32(2047), Some(2047));

        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
