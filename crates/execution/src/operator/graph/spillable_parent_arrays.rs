// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::{Allocator, BufferAllocator, BufferManager};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext, MemoryOwnerAllocator};
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::column::{
    ChunkManagementState, ColumnDataAllocatorType, ColumnDataAppendState, ColumnDataCollection,
};

use crate::memory_runtime::AccountedBuffer;

#[derive(Debug)]
pub struct SpilledParentHop {
    collection: ColumnDataCollection,
    row_count: usize,
}

impl SpilledParentHop {
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn collection(&self) -> &ColumnDataCollection {
        &self.collection
    }
}

#[derive(Debug)]
pub struct SpillableParentArrays {
    num_vertices: usize,
    chunk_rows: usize,
    memory: MemoryAccountingContext,
    hops: AccountedBuffer<SpilledParentHop>,
    current_parent_vertex: AccountedBuffer<u32>,
    current_parent_edge: AccountedBuffer<u64>,
    buffer_pool: Arc<BufferPool>,
}

#[derive(Debug)]
pub struct ParentLookupState {
    loaded_hop: Option<usize>,
    loaded_chunk_idx: Option<usize>,
    chunk_state: ChunkManagementState,
    chunk_rows: usize,
    chunk: Option<Chunk>,
}

impl ParentLookupState {
    pub fn new() -> Self {
        Self::with_chunk_rows(VECTOR_SIZE)
    }

    pub fn with_chunk_rows(chunk_rows: usize) -> Self {
        Self {
            loaded_hop: None,
            loaded_chunk_idx: None,
            chunk_state: ChunkManagementState::new(),
            chunk_rows: chunk_rows.max(1),
            chunk: None,
        }
    }

    fn invalidate_cache(&mut self) {
        self.loaded_hop = None;
        self.loaded_chunk_idx = None;
        self.chunk_state.clear();
    }
}

impl Default for ParentLookupState {
    fn default() -> Self {
        Self::new()
    }
}

impl SpillableParentArrays {
    pub fn new(num_vertices: usize, buffer_pool: Arc<BufferPool>) -> Result<Self> {
        Self::with_chunk_rows(num_vertices, buffer_pool, VECTOR_SIZE)
    }

    pub fn new_with_memory(
        num_vertices: usize,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        Self::with_chunk_rows_and_memory(num_vertices, buffer_pool, VECTOR_SIZE, memory)
    }

    pub fn with_chunk_rows(
        num_vertices: usize,
        buffer_pool: Arc<BufferPool>,
        chunk_rows: usize,
    ) -> Result<Self> {
        Self::with_chunk_rows_and_memory(
            num_vertices,
            buffer_pool,
            chunk_rows,
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn with_chunk_rows_and_memory(
        num_vertices: usize,
        buffer_pool: Arc<BufferPool>,
        chunk_rows: usize,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        let chunk_rows = chunk_rows.max(1);
        let mut current_parent_vertex = AccountedBuffer::new(memory.clone());
        current_parent_vertex.try_resize(num_vertices, u32::MAX)?;
        let mut current_parent_edge = AccountedBuffer::new(memory.clone());
        current_parent_edge.try_resize(num_vertices, 0u64)?;
        Ok(Self {
            num_vertices,
            chunk_rows,
            memory: memory.clone(),
            hops: AccountedBuffer::new(memory),
            current_parent_vertex,
            current_parent_edge,
            buffer_pool,
        })
    }

    pub fn set_parent(&mut self, dst: u32, src_vertex: u32, edge_rowid: u64) {
        let dst_idx = dst as usize;
        if dst_idx >= self.num_vertices {
            return;
        }
        self.current_parent_vertex[dst_idx] = src_vertex;
        self.current_parent_edge[dst_idx] = edge_rowid;
    }

    pub fn commit_hop(&mut self) -> Result<()> {
        let mut collection = ColumnDataCollection::with_buffer_pool_and_memory(
            self.buffer_pool.clone(),
            parent_collection_types(),
            MemoryTag::ColumnData,
            ColumnDataAllocatorType::BufferManagerAllocator,
            self.memory.clone(),
        );
        let mut append_state = ColumnDataAppendState::new();
        collection.initialize_append(&mut append_state);

        for start in (0..self.num_vertices).step_by(self.chunk_rows) {
            let end = (start + self.chunk_rows).min(self.num_vertices);
            let chunk = build_parent_chunk(
                &self.current_parent_vertex[start..end],
                &self.current_parent_edge[start..end],
                graph_memory_allocator(&self.buffer_pool, &self.memory),
            )?;
            collection.append(&mut append_state, &chunk)?;
        }

        self.hops.try_push(SpilledParentHop {
            collection,
            row_count: self.num_vertices,
        })?;
        self.current_parent_vertex.fill(u32::MAX);
        self.current_parent_edge.fill(0);
        Ok(())
    }

    pub fn lookup_parent(
        &self,
        hop_idx: usize,
        local_id: usize,
        lookup_state: &mut ParentLookupState,
    ) -> Result<(u32, u64)> {
        if hop_idx == self.hops.len() {
            if local_id >= self.num_vertices {
                return Err(paro_error::internal(format!(
                    "Parent local_id {} out of bounds for in-memory hop {}",
                    local_id, hop_idx
                )));
            }
            lookup_state.invalidate_cache();
            return Ok((
                self.current_parent_vertex[local_id],
                self.current_parent_edge[local_id],
            ));
        }

        let hop = self.hops.get(hop_idx).ok_or_else(|| {
            paro_error::internal(format!("Parent hop index {} out of bounds", hop_idx))
        })?;
        if local_id >= hop.row_count {
            return Err(paro_error::internal(format!(
                "Parent local_id {} out of bounds for hop {}",
                local_id, hop_idx
            )));
        }

        if hop.row_count == 0 {
            return Ok((u32::MAX, 0));
        }

        let chunk_idx = local_id / self.chunk_rows;
        if lookup_state.loaded_hop != Some(hop_idx)
            || lookup_state.loaded_chunk_idx != Some(chunk_idx)
        {
            lookup_state.invalidate_cache();
            let storage_index = hop
                .collection
                .chunk_storage_indexes()
                .get(chunk_idx)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Parent chunk index {} out of bounds for hop {}",
                        chunk_idx, hop_idx
                    ))
                })?;
            if lookup_state.chunk.is_none() {
                lookup_state.chunk = Some(Chunk::try_initialize(
                    &parent_collection_types(),
                    lookup_state.chunk_rows.max(self.chunk_rows).max(1),
                    graph_memory_allocator(&self.buffer_pool, &self.memory),
                )?);
            }
            let chunk = lookup_state
                .chunk
                .as_mut()
                .expect("parent lookup chunk initialized above");
            hop.collection.fetch_chunk_by_storage_index(
                storage_index,
                &[0, 1],
                &mut lookup_state.chunk_state,
                chunk,
            )?;
            lookup_state.loaded_hop = Some(hop_idx);
            lookup_state.loaded_chunk_idx = Some(chunk_idx);
        }

        let row_idx = local_id % self.chunk_rows;
        let parent_vertex = lookup_state
            .chunk
            .as_ref()
            .expect("parent lookup chunk loaded above")
            .column(0)
            .and_then(|col| col.get_u32(row_idx))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing parent vertex at hop {}, local_id {}",
                    hop_idx, local_id
                ))
            })?;
        let parent_edge = lookup_state
            .chunk
            .as_ref()
            .expect("parent lookup chunk loaded above")
            .column(1)
            .and_then(|col| col.get_u64(row_idx))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing parent edge at hop {}, local_id {}",
                    hop_idx, local_id
                ))
            })?;
        Ok((parent_vertex, parent_edge))
    }

    pub fn hop_count(&self) -> usize {
        self.hops.len()
    }

    pub fn current_in_memory_bytes(&self) -> usize {
        self.current_parent_vertex.len() * std::mem::size_of::<u32>()
            + self.current_parent_edge.len() * std::mem::size_of::<u64>()
    }
}

fn parent_collection_types() -> Vec<LogicalType> {
    vec![LogicalType::UInteger, LogicalType::UBigInt]
}

fn graph_memory_allocator(
    buffer_pool: &Arc<BufferPool>,
    memory: &MemoryAccountingContext,
) -> Arc<dyn Allocator> {
    let inner: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
        buffer_pool.clone() as Arc<dyn BufferManager>,
        memory.tag(),
    ));
    if let Some(owner) = memory.owner() {
        Arc::new(MemoryOwnerAllocator::new(
            inner,
            owner,
            memory.domain(),
            memory.tag(),
            memory.accounting_class(),
        ))
    } else {
        inner
    }
}

fn build_parent_chunk(
    parent_vertices: &[u32],
    parent_edges: &[u64],
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    debug_assert_eq!(parent_vertices.len(), parent_edges.len());
    let row_count = parent_vertices.len();
    let mut vertex_vector =
        Vector::try_new(LogicalType::UInteger, row_count.max(1), allocator.clone())?;
    let mut edge_vector =
        Vector::try_new(LogicalType::UBigInt, row_count.max(1), allocator.clone())?;
    vertex_vector.set_len(row_count);
    edge_vector.set_len(row_count);

    for (idx, (&parent_vertex, &parent_edge)) in
        parent_vertices.iter().zip(parent_edges.iter()).enumerate()
    {
        vertex_vector.set_u32(idx, parent_vertex);
        edge_vector.set_u64(idx, parent_edge);
    }

    Ok(Chunk::from_vectors(
        vec![vertex_vector, edge_vector],
        allocator,
    ))
}

#[cfg(test)]
mod tests {
    use super::{ParentLookupState, SpillableParentArrays};

    use paro_storage::buffer::{BufferPool, MemoryTag};

    #[test]
    fn test_lookup_parent_across_chunk_boundaries() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut parents = SpillableParentArrays::with_chunk_rows(10, pool, 4).unwrap();

        for idx in 0..10u32 {
            parents.set_parent(idx, idx + 10, idx as u64 + 100);
        }
        parents.commit_hop().unwrap();

        for idx in 0..10u32 {
            if idx % 2 == 0 {
                parents.set_parent(idx, idx + 20, idx as u64 + 200);
            }
        }
        parents.commit_hop().unwrap();

        assert_eq!(parents.hop_count(), 2);

        let mut lookup_state = ParentLookupState::with_chunk_rows(4);
        assert_eq!(
            parents.lookup_parent(0, 0, &mut lookup_state).unwrap(),
            (10, 100)
        );
        assert_eq!(
            parents.lookup_parent(0, 3, &mut lookup_state).unwrap(),
            (13, 103)
        );
        assert_eq!(
            parents.lookup_parent(0, 4, &mut lookup_state).unwrap(),
            (14, 104)
        );
        assert_eq!(
            parents.lookup_parent(0, 9, &mut lookup_state).unwrap(),
            (19, 109)
        );

        assert_eq!(
            parents.lookup_parent(1, 0, &mut lookup_state).unwrap(),
            (20, 200)
        );
        assert_eq!(
            parents.lookup_parent(1, 1, &mut lookup_state).unwrap(),
            (u32::MAX, 0)
        );
        assert_eq!(
            parents.lookup_parent(1, 4, &mut lookup_state).unwrap(),
            (24, 204)
        );
        assert_eq!(
            parents.lookup_parent(1, 9, &mut lookup_state).unwrap(),
            (u32::MAX, 0)
        );
        assert_eq!(
            parents.lookup_parent(1, 8, &mut lookup_state).unwrap(),
            (28, 208)
        );
    }

    #[test]
    fn test_lookup_parent_reads_current_uncommitted_hop() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut parents = SpillableParentArrays::with_chunk_rows(8, pool, 4).unwrap();

        for idx in 0..8u32 {
            parents.set_parent(idx, idx + 10, idx as u64 + 100);
        }
        parents.commit_hop().unwrap();

        parents.set_parent(3, 33, 303);
        parents.set_parent(6, 66, 606);

        let mut lookup_state = ParentLookupState::with_chunk_rows(4);
        assert_eq!(
            parents.lookup_parent(1, 3, &mut lookup_state).unwrap(),
            (33, 303)
        );
        assert_eq!(
            parents.lookup_parent(1, 6, &mut lookup_state).unwrap(),
            (66, 606)
        );
        assert_eq!(
            parents.lookup_parent(1, 2, &mut lookup_state).unwrap(),
            (u32::MAX, 0)
        );
    }

    #[test]
    fn test_lookup_parent_eviction_reload_roundtrip() {
        let pool = BufferPool::new_arc(8 * 1024 * 1024);
        let temp_dir = std::env::temp_dir().join(format!(
            "paro_spillable_parent_arrays_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("t")
        ));
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();

        let mut parents = SpillableParentArrays::with_chunk_rows(4096, pool.clone(), 1024).unwrap();
        for idx in 0..4096u32 {
            parents.set_parent(idx, idx + 1, idx as u64 + 10);
        }
        parents.commit_hop().unwrap();

        for idx in 0..4096u32 {
            parents.set_parent(idx, idx + 100, idx as u64 + 1000);
        }
        parents.commit_hop().unwrap();

        for hop in parents.hops.iter() {
            for storage_index in hop.collection.chunk_storage_indexes() {
                if let Some(block_id) = hop.collection.chunk_block_id(storage_index) {
                    pool.add_to_eviction_queue(block_id);
                }
            }
        }

        let evicted = pool.evict_blocks(MemoryTag::ColumnData, 0, 0, None);
        assert!(evicted.success);
        assert!(pool.get_temporary_spill_metrics().write_bytes > 0);

        let mut lookup_state = ParentLookupState::with_chunk_rows(1024);
        assert_eq!(
            parents.lookup_parent(0, 0, &mut lookup_state).unwrap(),
            (1, 10)
        );
        assert_eq!(
            parents.lookup_parent(0, 1024, &mut lookup_state).unwrap(),
            (1025, 1034)
        );
        assert_eq!(
            parents.lookup_parent(0, 4095, &mut lookup_state).unwrap(),
            (4096, 4105)
        );
        assert_eq!(
            parents.lookup_parent(1, 0, &mut lookup_state).unwrap(),
            (100, 1000)
        );
        assert_eq!(
            parents.lookup_parent(1, 2048, &mut lookup_state).unwrap(),
            (2148, 3048)
        );
        assert_eq!(
            parents.lookup_parent(1, 4095, &mut lookup_state).unwrap(),
            (4195, 5095)
        );

        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
