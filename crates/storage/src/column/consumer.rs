// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ColumnDataConsumer - destructive scan helper for column data collections.

use std::collections::HashSet;
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::Result;

use super::allocator::ChunkManagementState;
use super::collection::ColumnDataCollection;

#[derive(Debug, Default)]
pub struct ColumnDataConsumerScanState {
    pub current_chunk_state: ChunkManagementState,
    pub chunk_ref_index: Option<usize>,
}

impl ColumnDataConsumerScanState {
    pub fn new() -> Self {
        Self {
            current_chunk_state: ChunkManagementState::new(),
            chunk_ref_index: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkReference {
    storage_index: usize,
    min_block_id: i64,
}

#[derive(Debug, Default)]
struct ConsumerRuntimeState {
    chunk_references: Vec<ChunkReference>,
    current_chunk_index: usize,
    delete_cursor: usize,
    chunks_in_progress: HashSet<usize>,
    chunks_finished: HashSet<usize>,
}

#[derive(Debug)]
pub struct ColumnDataConsumer {
    collection: Mutex<ColumnDataCollection>,
    column_ids: Vec<usize>,
    runtime: Mutex<ConsumerRuntimeState>,
}

impl ColumnDataConsumer {
    pub fn new(collection: ColumnDataCollection, column_ids: Option<Vec<usize>>) -> Self {
        let ids = column_ids.unwrap_or_else(|| (0..collection.types().len()).collect());
        Self {
            collection: Mutex::new(collection),
            column_ids: ids,
            runtime: Mutex::new(ConsumerRuntimeState::default()),
        }
    }

    pub fn initialize_scan(&self) {
        let mut runtime = self.runtime.lock().unwrap();
        let collection = self.collection.lock().unwrap();

        let mut refs: Vec<ChunkReference> = collection
            .chunk_storage_indexes()
            .into_iter()
            .map(|storage_index| ChunkReference {
                storage_index,
                min_block_id: collection
                    .chunk_min_block_id(storage_index)
                    .unwrap_or(i64::MAX),
            })
            .collect();

        refs.sort_by(|left, right| {
            left.min_block_id
                .cmp(&right.min_block_id)
                .then_with(|| left.storage_index.cmp(&right.storage_index))
        });

        runtime.chunk_references = refs;
        runtime.current_chunk_index = 0;
        runtime.delete_cursor = 0;
        runtime.chunks_in_progress.clear();
        runtime.chunks_finished.clear();
    }

    pub fn count(&self) -> usize {
        self.collection.lock().unwrap().count()
    }

    pub fn chunk_count(&self) -> usize {
        self.runtime.lock().unwrap().chunk_references.len()
    }

    pub fn remaining_chunk_count(&self) -> usize {
        self.collection.lock().unwrap().chunk_count()
    }

    pub fn assign_chunk(&self, state: &mut ColumnDataConsumerScanState) -> bool {
        let mut runtime = self.runtime.lock().unwrap();

        if runtime.current_chunk_index >= runtime.chunk_references.len() {
            state.current_chunk_state.clear();
            state.chunk_ref_index = None;
            return false;
        }

        let chunk_ref_index = runtime.current_chunk_index;
        runtime.current_chunk_index += 1;
        runtime.chunks_in_progress.insert(chunk_ref_index);

        state.chunk_ref_index = Some(chunk_ref_index);
        true
    }

    pub fn scan_chunk(
        &self,
        state: &mut ColumnDataConsumerScanState,
        output: &mut Chunk,
    ) -> Result<usize> {
        let Some(chunk_ref_index) = state.chunk_ref_index else {
            output.try_set_cardinality(0)?;
            return Ok(0);
        };

        let storage_index = {
            let runtime = self.runtime.lock().unwrap();
            let Some(chunk_ref) = runtime.chunk_references.get(chunk_ref_index) else {
                output.try_set_cardinality(0)?;
                return Ok(0);
            };
            chunk_ref.storage_index
        };

        let collection = self.collection.lock().unwrap();
        collection.fetch_chunk_by_storage_index(
            storage_index,
            &self.column_ids,
            &mut state.current_chunk_state,
            output,
        )
    }

    pub fn finish_chunk(&self, state: &mut ColumnDataConsumerScanState) -> Result<()> {
        let Some(chunk_ref_index) = state.chunk_ref_index.take() else {
            state.current_chunk_state.clear();
            return Ok(());
        };

        let mut to_delete_storage_indexes = Vec::new();
        {
            let mut runtime = self.runtime.lock().unwrap();
            runtime.chunks_in_progress.remove(&chunk_ref_index);
            runtime.chunks_finished.insert(chunk_ref_index);

            while runtime.delete_cursor < runtime.chunk_references.len() {
                let cursor = runtime.delete_cursor;
                if runtime.chunks_in_progress.contains(&cursor)
                    || !runtime.chunks_finished.contains(&cursor)
                {
                    break;
                }
                to_delete_storage_indexes.push(runtime.chunk_references[cursor].storage_index);
                runtime.delete_cursor += 1;
            }
        }

        // Release local pins before deleting consumed chunks.
        state.current_chunk_state.clear();

        if !to_delete_storage_indexes.is_empty() {
            let mut collection = self.collection.lock().unwrap();
            for storage_index in to_delete_storage_indexes {
                collection.consume_chunk(storage_index)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use crate::buffer::{BufferPool, MemoryTag};

    use super::super::{ColumnDataAllocatorType, ColumnDataCollection};
    use super::{ColumnDataConsumer, ColumnDataConsumerScanState};

    fn build_test_chunk(start: i32, count: usize) -> Chunk {
        test_chunk_from_vectors(vec![test_i32_vector(
            &(start..start + count as i32).collect::<Vec<_>>(),
        )])
    }

    #[test]
    fn test_consumer_parallel_assign_scan_finish() {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let mut collection = ColumnDataCollection::with_buffer_pool(
            pool,
            vec![LogicalType::Integer],
            MemoryTag::ColumnData,
            ColumnDataAllocatorType::BufferManagerAllocator,
        );

        collection.append_chunk(&build_test_chunk(0, 64)).unwrap();
        collection.append_chunk(&build_test_chunk(64, 64)).unwrap();
        collection.append_chunk(&build_test_chunk(128, 64)).unwrap();

        let consumer = ColumnDataConsumer::new(collection, None);
        consumer.initialize_scan();
        assert_eq!(consumer.chunk_count(), 3);

        let mut s1 = ColumnDataConsumerScanState::new();
        let mut s2 = ColumnDataConsumerScanState::new();

        assert!(consumer.assign_chunk(&mut s1));
        assert!(consumer.assign_chunk(&mut s2));

        let mut out1 = test_chunk_with_capacity(&[LogicalType::Integer], 64);
        let mut out2 = test_chunk_with_capacity(&[LogicalType::Integer], 64);

        let n1 = consumer.scan_chunk(&mut s1, &mut out1).unwrap();
        let n2 = consumer.scan_chunk(&mut s2, &mut out2).unwrap();
        assert_eq!(n1, 64);
        assert_eq!(n2, 64);

        consumer.finish_chunk(&mut s2).unwrap();
        assert_eq!(consumer.remaining_chunk_count(), 3);

        consumer.finish_chunk(&mut s1).unwrap();
        assert_eq!(consumer.remaining_chunk_count(), 1);

        assert!(consumer.assign_chunk(&mut s1));
        let n3 = consumer.scan_chunk(&mut s1, &mut out1).unwrap();
        assert_eq!(n3, 64);
        consumer.finish_chunk(&mut s1).unwrap();

        assert_eq!(consumer.remaining_chunk_count(), 0);
        assert!(!consumer.assign_chunk(&mut s2));
    }

    #[test]
    fn test_consumer_read_once_delete_semantics_after_spill() {
        let pool = BufferPool::new_arc(8 * 1024 * 1024);
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!(
            "paro_column_consumer_spill_{}_{}",
            std::process::id(),
            suffix
        ));
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();

        let mut collection = ColumnDataCollection::with_buffer_pool(
            pool.clone(),
            vec![LogicalType::Integer],
            MemoryTag::ColumnData,
            ColumnDataAllocatorType::BufferManagerAllocator,
        );

        collection.append_chunk(&build_test_chunk(0, 4096)).unwrap();

        for idx in collection.chunk_storage_indexes() {
            if let Some(block_id) = collection.chunk_block_id(idx) {
                pool.add_to_eviction_queue(block_id);
            }
        }

        let evicted = pool.evict_blocks(MemoryTag::ColumnData, 0, 0, None);
        assert!(evicted.success);
        assert!(pool.get_temporary_spill_metrics().write_bytes > 0);

        let consumer = ColumnDataConsumer::new(collection, None);
        consumer.initialize_scan();

        let mut state = ColumnDataConsumerScanState::new();
        assert!(consumer.assign_chunk(&mut state));

        let mut out = test_chunk_with_capacity(&[LogicalType::Integer], 4096);
        let scanned = consumer.scan_chunk(&mut state, &mut out).unwrap();
        assert_eq!(scanned, 4096);
        assert_eq!(out.column(0).unwrap().get_i32(0), Some(0));
        assert_eq!(out.column(0).unwrap().get_i32(4095), Some(4095));

        consumer.finish_chunk(&mut state).unwrap();
        assert_eq!(consumer.remaining_chunk_count(), 0);
        assert!(pool.get_temporary_files().is_empty());

        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
