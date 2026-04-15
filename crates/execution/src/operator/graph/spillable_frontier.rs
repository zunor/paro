// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::column::{
    ChunkManagementState, ColumnDataAllocatorType, ColumnDataAppendState, ColumnDataCollection,
};

#[derive(Debug)]
pub struct SpillableFrontier {
    spill_threshold_rows: usize,
    chunk_rows: usize,
    memory_frontier: Vec<u32>,
    external_chunk_ends: Vec<usize>,
    external_row_count: usize,
    collection: Option<ColumnDataCollection>,
    buffer_pool: Arc<BufferPool>,
    count: usize,
}

#[derive(Debug)]
pub struct SpillableFrontierCursor {
    loaded_chunk_idx: Option<usize>,
    chunk_state: ChunkManagementState,
    chunk: Chunk,
}

impl SpillableFrontierCursor {
    pub fn new() -> Self {
        Self::with_chunk_rows(VECTOR_SIZE)
    }

    pub fn with_chunk_rows(chunk_rows: usize) -> Self {
        Self {
            loaded_chunk_idx: None,
            chunk_state: ChunkManagementState::new(),
            chunk: Chunk::initialize(&frontier_collection_types(), chunk_rows.max(1)),
        }
    }

    fn invalidate_cache(&mut self) {
        self.loaded_chunk_idx = None;
        self.chunk_state.clear();
    }
}

impl Default for SpillableFrontierCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl SpillableFrontier {
    pub fn new(buffer_pool: Arc<BufferPool>, spill_threshold_rows: usize) -> Self {
        Self::with_chunk_rows(buffer_pool, spill_threshold_rows, VECTOR_SIZE)
    }

    pub fn with_chunk_rows(
        buffer_pool: Arc<BufferPool>,
        spill_threshold_rows: usize,
        chunk_rows: usize,
    ) -> Self {
        let chunk_rows = chunk_rows.max(1);
        let collection = if spill_threshold_rows == 0 {
            Some(new_frontier_collection(buffer_pool.clone()))
        } else {
            None
        };
        Self {
            spill_threshold_rows,
            chunk_rows,
            memory_frontier: Vec::new(),
            external_chunk_ends: Vec::new(),
            external_row_count: 0,
            collection,
            buffer_pool,
            count: 0,
        }
    }

    pub fn push(&mut self, vertex_id: u32) -> Result<()> {
        self.append_from_slice(&[vertex_id])
    }

    pub fn append_from_slice(&mut self, vertices: &[u32]) -> Result<()> {
        if vertices.is_empty() {
            return Ok(());
        }

        if self.collection.is_some() {
            self.append_external(vertices)?;
            self.count += vertices.len();
            return Ok(());
        }

        if self.spill_threshold_rows > 0 && self.count + vertices.len() >= self.spill_threshold_rows
        {
            self.externalize_in_memory()?;
            self.append_external(vertices)?;
        } else {
            self.memory_frontier.extend_from_slice(vertices);
        }
        self.count += vertices.len();
        Ok(())
    }

    pub fn get(&self, index: usize, cursor: &mut SpillableFrontierCursor) -> Result<u32> {
        if index >= self.count {
            return Err(paro_error::internal(format!(
                "Frontier index {} out of bounds (len={})",
                index, self.count
            )));
        }

        if let Some(collection) = &self.collection {
            let chunk_idx = self
                .external_chunk_ends
                .partition_point(|&end| end <= index);
            let chunk_start = if chunk_idx == 0 {
                0
            } else {
                self.external_chunk_ends[chunk_idx - 1]
            };
            if cursor.loaded_chunk_idx != Some(chunk_idx) {
                cursor.invalidate_cache();
                let storage_index = collection
                    .chunk_storage_indexes()
                    .get(chunk_idx)
                    .copied()
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "Frontier chunk index {} out of bounds",
                            chunk_idx
                        ))
                    })?;
                collection.fetch_chunk_by_storage_index(
                    storage_index,
                    &[0],
                    &mut cursor.chunk_state,
                    &mut cursor.chunk,
                )?;
                cursor.loaded_chunk_idx = Some(chunk_idx);
            }

            let row_idx = index - chunk_start;
            cursor
                .chunk
                .column(0)
                .and_then(|col| col.get_u32(row_idx))
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing frontier value at index {} (chunk {})",
                        index, chunk_idx
                    ))
                })
        } else {
            self.memory_frontier.get(index).copied().ok_or_else(|| {
                paro_error::internal(format!(
                    "Frontier memory index {} out of bounds (len={})",
                    index,
                    self.memory_frontier.len()
                ))
            })
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn resident_memory_bytes(&self) -> usize {
        if self.collection.is_some() {
            0
        } else {
            self.memory_frontier.len() * std::mem::size_of::<u32>()
        }
    }

    pub fn ensure_external(&mut self) -> Result<()> {
        self.externalize_in_memory()
    }

    pub fn is_external(&self) -> bool {
        self.collection.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn clear(&mut self) -> Result<()> {
        if let Some(collection) = &mut self.collection {
            collection.reset()?;
        }
        self.memory_frontier.clear();
        self.external_chunk_ends.clear();
        self.external_row_count = 0;
        self.count = 0;
        if self.spill_threshold_rows == 0 && self.collection.is_none() {
            self.collection = Some(new_frontier_collection(self.buffer_pool.clone()));
        }
        Ok(())
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(
            self,
            Self::with_chunk_rows(
                self.buffer_pool.clone(),
                self.spill_threshold_rows,
                self.chunk_rows,
            ),
        )
    }

    fn externalize_in_memory(&mut self) -> Result<()> {
        if self.collection.is_none() {
            self.collection = Some(new_frontier_collection(self.buffer_pool.clone()));
        }
        if !self.memory_frontier.is_empty() {
            let existing = std::mem::take(&mut self.memory_frontier);
            self.append_external(&existing)?;
        }
        Ok(())
    }

    fn append_external(&mut self, vertices: &[u32]) -> Result<()> {
        let collection = self
            .collection
            .get_or_insert_with(|| new_frontier_collection(self.buffer_pool.clone()));
        let mut append_state = ColumnDataAppendState::new();
        collection.initialize_append(&mut append_state);

        for start in (0..vertices.len()).step_by(self.chunk_rows) {
            let end = (start + self.chunk_rows).min(vertices.len());
            let chunk = build_frontier_chunk(&vertices[start..end]);
            collection.append(&mut append_state, &chunk)?;
            self.external_row_count += end - start;
            self.external_chunk_ends.push(self.external_row_count);
        }
        Ok(())
    }
}

fn frontier_collection_types() -> Vec<LogicalType> {
    vec![LogicalType::UInteger]
}

fn new_frontier_collection(buffer_pool: Arc<BufferPool>) -> ColumnDataCollection {
    ColumnDataCollection::with_buffer_pool(
        buffer_pool,
        frontier_collection_types(),
        MemoryTag::ColumnData,
        ColumnDataAllocatorType::BufferManagerAllocator,
    )
}

fn build_frontier_chunk(vertices: &[u32]) -> Chunk {
    let row_count = vertices.len();
    let mut vector = Vector::with_capacity(LogicalType::UInteger, row_count.max(1));
    vector.set_len(row_count);
    for (idx, vertex_id) in vertices.iter().copied().enumerate() {
        vector.set_u32(idx, vertex_id);
    }
    Chunk::from_vectors(vec![vector])
}

#[cfg(test)]
mod tests {
    use super::{SpillableFrontier, SpillableFrontierCursor};

    use paro_storage::buffer::BufferPool;

    #[test]
    fn test_small_frontier_stays_in_vec_mode() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut frontier = SpillableFrontier::with_chunk_rows(pool, 8, 4);
        frontier.append_from_slice(&[10, 11, 12]).unwrap();

        assert_eq!(frontier.len(), 3);
        assert!(!frontier.is_empty());
        assert!(frontier.collection.is_none());

        let mut cursor = SpillableFrontierCursor::with_chunk_rows(4);
        assert_eq!(frontier.get(0, &mut cursor).unwrap(), 10);
        assert_eq!(frontier.get(2, &mut cursor).unwrap(), 12);
    }

    #[test]
    fn test_large_frontier_uses_collection_mode() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut frontier = SpillableFrontier::with_chunk_rows(pool, 4, 3);
        frontier.append_from_slice(&[1, 2, 3]).unwrap();
        assert!(frontier.collection.is_none());

        frontier.append_from_slice(&[4, 5, 6, 7]).unwrap();
        assert_eq!(frontier.len(), 7);
        assert!(frontier.collection.is_some());
        assert!(frontier.memory_frontier.is_empty());

        let storage_indexes = frontier
            .collection
            .as_ref()
            .unwrap()
            .chunk_storage_indexes();
        assert_eq!(storage_indexes.len(), 3);

        let mut cursor = SpillableFrontierCursor::with_chunk_rows(3);
        let values: Vec<u32> = (0..frontier.len())
            .map(|idx| frontier.get(idx, &mut cursor).unwrap())
            .collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_resume_from_middle_index_across_chunks() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut frontier = SpillableFrontier::with_chunk_rows(pool, 1, 4);
        frontier
            .append_from_slice(&(100..111).collect::<Vec<u32>>())
            .unwrap();

        let mut cursor = SpillableFrontierCursor::with_chunk_rows(4);
        assert_eq!(frontier.get(7, &mut cursor).unwrap(), 107);
        assert_eq!(cursor.loaded_chunk_idx, Some(1));
        assert_eq!(frontier.get(8, &mut cursor).unwrap(), 108);
        assert_eq!(cursor.loaded_chunk_idx, Some(2));
        assert_eq!(frontier.get(2, &mut cursor).unwrap(), 102);
        assert_eq!(cursor.loaded_chunk_idx, Some(0));
        assert_eq!(frontier.get(5, &mut cursor).unwrap(), 105);
        assert_eq!(cursor.loaded_chunk_idx, Some(1));
    }

    #[test]
    fn test_force_external_threshold_zero() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut frontier = SpillableFrontier::with_chunk_rows(pool, 0, 2);
        assert!(frontier.collection.is_some());
        assert!(frontier.memory_frontier.is_empty());

        frontier.append_from_slice(&[42, 43, 44]).unwrap();
        assert_eq!(frontier.len(), 3);
        assert!(frontier.collection.is_some());
        assert!(frontier.memory_frontier.is_empty());
        assert_eq!(
            frontier
                .collection
                .as_ref()
                .unwrap()
                .chunk_storage_indexes()
                .len(),
            2
        );

        let mut cursor = SpillableFrontierCursor::with_chunk_rows(2);
        assert_eq!(frontier.get(0, &mut cursor).unwrap(), 42);
        assert_eq!(frontier.get(2, &mut cursor).unwrap(), 44);
    }

    #[test]
    fn test_multiple_small_external_appends_preserve_indexing() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut frontier = SpillableFrontier::with_chunk_rows(pool, 0, 4);

        frontier.push(10).unwrap();
        frontier.push(20).unwrap();
        frontier.append_from_slice(&[30, 40]).unwrap();
        frontier.push(50).unwrap();

        let mut cursor = SpillableFrontierCursor::with_chunk_rows(4);
        let values: Vec<u32> = (0..frontier.len())
            .map(|idx| frontier.get(idx, &mut cursor).unwrap())
            .collect();
        assert_eq!(values, vec![10, 20, 30, 40, 50]);
    }
}
