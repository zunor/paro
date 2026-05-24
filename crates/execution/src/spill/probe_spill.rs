// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Probe-side spill orchestration for external hash join.
//!
//! - thread-local append into partitioned row stores
//! - finalize merges locals into sealed radix partitions
//! - `prepare_next_probe()` returns a destructive `RowStore` for replay

use std::sync::{Arc, Mutex};

use paro_common::allocator::{BufferAllocator, BufferManager};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::row::{
    RadixPartitionedRows, RadixPartitionedRowsBuilder, RadixPartitioning, RowLayout, RowStore,
    RowStoreBuilder, RowValidityType,
};

const MAX_RADIX_BITS: usize = 12;

/// Per-thread local probe spill state.
#[derive(Debug, Clone)]
pub struct ProbeSpillLocalState {
    local_partitions: Arc<Mutex<Option<RadixPartitionedRowsBuilder>>>,
}

/// Probe-side spill coordinator.
#[derive(Debug)]
pub struct ProbeSpill {
    buffer_pool: Arc<BufferPool>,
    probe_layout: Arc<RowLayout>,
    memory: MemoryAccountingContext,
    radix_bits: usize,
    hash_col_idx: usize,
    local_states: Vec<ProbeSpillLocalState>,
    global_partitions: Option<RadixPartitionedRows>,
    current_partitions: Option<Vec<bool>>,
}

impl ProbeSpill {
    /// Create a probe spill instance partitioned by radix bits over `hash_col_idx`.
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        probe_types: Vec<LogicalType>,
        radix_bits: usize,
        hash_col_idx: usize,
    ) -> Result<Self> {
        Self::new_with_memory(
            buffer_pool,
            probe_types,
            radix_bits,
            hash_col_idx,
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        probe_types: Vec<LogicalType>,
        radix_bits: usize,
        hash_col_idx: usize,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        if radix_bits == 0 || radix_bits > MAX_RADIX_BITS {
            return Err(paro_error::invalid_input(format!(
                "invalid probe spill radix bits: radix_bits={radix_bits}, allowed=1..={MAX_RADIX_BITS}"
            )));
        }
        if hash_col_idx >= probe_types.len() {
            return Err(paro_error::invalid_input(format!(
                "probe spill hash column out of bounds: hash_col_idx={hash_col_idx}, column_count={}",
                probe_types.len()
            )));
        }
        if probe_types[hash_col_idx] != LogicalType::UBigInt {
            return Err(paro_error::invalid_input(format!(
                "probe spill hash column must be UBigInt, found {:?}",
                probe_types[hash_col_idx]
            )));
        }

        Ok(Self {
            buffer_pool,
            probe_layout: Arc::new(RowLayout::from_types(
                probe_types,
                RowValidityType::CanHaveNullValues,
            )),
            memory,
            radix_bits,
            hash_col_idx,
            local_states: Vec::new(),
            global_partitions: None,
            current_partitions: None,
        })
    }

    pub fn partition_count(&self) -> usize {
        RadixPartitioning::number_of_partitions(self.radix_bits)
    }

    /// Set active partitions for the next probe round.
    ///
    /// If unset, all partitions are considered active.
    pub fn set_current_partitions(&mut self, current_partitions: Vec<bool>) -> Result<()> {
        if current_partitions.len() != self.partition_count() {
            return Err(paro_error::invalid_input(format!(
                "current_partitions length mismatch: expected={}, got={}",
                self.partition_count(),
                current_partitions.len()
            )));
        }
        self.current_partitions = Some(current_partitions);
        Ok(())
    }

    pub fn clear_current_partitions(&mut self) {
        self.current_partitions = None;
    }

    /// Register one thread-local append state.
    pub fn register_thread(&mut self) -> ProbeSpillLocalState {
        let builder = self.new_partition_builder();
        let local_state = ProbeSpillLocalState {
            local_partitions: Arc::new(Mutex::new(Some(builder))),
        };
        self.local_states.push(local_state.clone());
        local_state
    }

    /// Append one probe chunk into the given thread-local state.
    pub fn append(&mut self, chunk: &Chunk, local_state: &mut ProbeSpillLocalState) -> Result<()> {
        let mut local_partitions = local_state.local_partitions.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock probe spill local partition: {e}"))
        })?;
        let builder = local_partitions.as_mut().ok_or_else(|| {
            paro_error::internal("probe spill local partition already finalized".to_string())
        })?;
        builder.append(chunk)
    }

    /// Flush and merge all local partitions into sealed global partitions.
    pub fn finalize(&mut self) -> Result<()> {
        let mut merged: Option<RadixPartitionedRowsBuilder> = None;
        for local_state in std::mem::take(&mut self.local_states) {
            let mut local_partitions = local_state.local_partitions.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock probe spill local partition: {e}"))
            })?;
            let Some(local_builder) = local_partitions.take() else {
                continue;
            };
            if let Some(global) = merged.as_mut() {
                global.absorb(local_builder);
            } else {
                merged = Some(local_builder);
            }
        }

        self.global_partitions = Some(
            merged
                .unwrap_or_else(|| self.new_partition_builder())
                .seal(),
        );
        Ok(())
    }

    /// Prepare the next external probe round.
    ///
    /// Moves active partitions into a single sealed row store for replay.
    pub fn prepare_next_probe(&mut self) -> Result<Option<RowStore>> {
        if !self.local_states.is_empty() {
            return Err(paro_error::invalid_input(
                "probe spill contains unfinalized local states; call finalize() before prepare_next_probe()",
            ));
        }

        let partition_count = self.partition_count();
        let Some(global_partitions) = self.global_partitions.as_mut() else {
            return Ok(None);
        };
        let active_partitions = match &self.current_partitions {
            Some(current) => {
                if current.len() != partition_count {
                    return Err(paro_error::invalid_input(format!(
                        "current_partitions length mismatch during prepare_next_probe: expected={}, got={}",
                        partition_count,
                        current.len()
                    )));
                }
                current.clone()
            }
            None => vec![true; partition_count],
        };

        if !active_partitions.iter().any(|active| *active) {
            return Ok(None);
        }

        let mut builder = RowStoreBuilder::new_with_memory(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.probe_layout),
            MemoryTag::HashTable,
            self.memory.clone(),
        );
        let replay_allocator = Arc::new(BufferAllocator::new(
            Arc::clone(&self.buffer_pool) as Arc<dyn BufferManager>,
            MemoryTag::HashTable,
        ));
        let mut replay_chunk = Chunk::try_new(replay_allocator)?;
        for (partition_idx, active) in active_partitions.into_iter().enumerate() {
            if !active {
                continue;
            }

            let partition = global_partitions.take_partition(partition_idx);
            if partition.count() == 0 {
                continue;
            }

            let mut scanner = partition.scanner();
            loop {
                let scanned = scanner.next_chunk(&mut replay_chunk)?;
                if scanned == 0 {
                    break;
                }
                builder.append(&replay_chunk)?;
            }
        }

        if builder.count() == 0 {
            return Ok(None);
        }
        Ok(Some(builder.seal()))
    }

    fn new_partition_builder(&self) -> RadixPartitionedRowsBuilder {
        RadixPartitionedRowsBuilder::new_with_memory(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.probe_layout),
            MemoryTag::HashTable,
            self.radix_bits,
            self.hash_col_idx,
            self.memory.clone(),
        )
        .expect("probe spill builder configuration must stay valid")
    }
}

#[cfg(test)]
mod tests {
    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;

    use paro_storage::buffer::BufferPool;

    use super::ProbeSpill;

    fn build_chunk_with_hashes(keys: &[i32], hashes: &[u64]) -> Chunk {
        let mut hash_vector = paro_common::test_utils::test_vector_with_capacity(
            LogicalType::UBigInt,
            hashes.len().max(1),
        );
        hash_vector.set_count(hashes.len());
        for (idx, hash) in hashes.iter().enumerate() {
            hash_vector.set_u64(idx, *hash);
        }
        Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    keys,
                    paro_common::test_utils::test_allocator(),
                ),
                hash_vector,
            ],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn build_probe_chunk(start: i32, count: usize, radix_bits: usize) -> Chunk {
        let partition_count = 1usize << radix_bits;
        let mut keys = Vec::with_capacity(count);
        let mut hashes = Vec::with_capacity(count);
        for i in 0..count {
            let key = start + i as i32;
            let partition_idx = (key.unsigned_abs() as usize) % partition_count;
            let hash = ((partition_idx as u64) << (u64::BITS as usize - radix_bits)) | (key as u64);
            keys.push(key);
            hashes.push(hash);
        }
        build_chunk_with_hashes(&keys, &hashes)
    }

    fn scan_all_rows(store: &paro_storage::row::RowStore) -> Result<Vec<(i32, u64)>> {
        let mut rows = Vec::new();
        let mut scanner = store.scanner();
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer, LogicalType::UBigInt],
            4096,
        );
        loop {
            let scanned = scanner.next_chunk(&mut chunk)?;
            if scanned == 0 {
                break;
            }
            for row_idx in 0..scanned {
                rows.push((
                    chunk.column(0).unwrap().get_i32(row_idx).unwrap(),
                    chunk.column(1).unwrap().get_u64(row_idx).unwrap(),
                ));
            }
        }
        Ok(rows)
    }

    #[test]
    fn test_probe_spill_finalize_and_replay_roundtrip() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let probe_types = vec![LogicalType::Integer, LogicalType::UBigInt];
        let radix_bits = 3usize;
        let partition_count = 1usize << radix_bits;

        let mut spill = ProbeSpill::new(buffer_pool, probe_types.clone(), radix_bits, 1).unwrap();
        let mut local1 = spill.register_thread();
        let mut local2 = spill.register_thread();

        spill
            .append(&build_probe_chunk(0, 1024, radix_bits), &mut local1)
            .unwrap();
        spill
            .append(&build_probe_chunk(1024, 1024, radix_bits), &mut local2)
            .unwrap();
        spill.finalize().unwrap();

        let mut even_partitions = vec![false; partition_count];
        let mut odd_partitions = vec![false; partition_count];
        for partition_idx in 0..partition_count {
            if partition_idx % 2 == 0 {
                even_partitions[partition_idx] = true;
            } else {
                odd_partitions[partition_idx] = true;
            }
        }

        spill.set_current_partitions(even_partitions).unwrap();
        let even_store = spill.prepare_next_probe().unwrap().unwrap();
        let mut even_rows = scan_all_rows(&even_store).unwrap();

        spill.set_current_partitions(odd_partitions).unwrap();
        let odd_store = spill.prepare_next_probe().unwrap().unwrap();
        let mut odd_rows = scan_all_rows(&odd_store).unwrap();

        spill.clear_current_partitions();
        assert!(spill.prepare_next_probe().unwrap().is_none());

        let mut all_keys = Vec::new();
        for (key, hash) in even_rows.drain(..) {
            let partition_idx =
                ((hash >> (u64::BITS as usize - radix_bits)) as usize) & (partition_count - 1);
            assert_eq!(partition_idx % 2, 0);
            all_keys.push(key);
        }
        for (key, hash) in odd_rows.drain(..) {
            let partition_idx =
                ((hash >> (u64::BITS as usize - radix_bits)) as usize) & (partition_count - 1);
            assert_eq!(partition_idx % 2, 1);
            all_keys.push(key);
        }

        all_keys.sort_unstable();
        assert_eq!(all_keys, (0..2048).collect::<Vec<i32>>());
    }

    #[test]
    fn test_prepare_next_probe_requires_finalize() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let probe_types = vec![LogicalType::Integer, LogicalType::UBigInt];
        let radix_bits = 4usize;

        let mut spill = ProbeSpill::new(buffer_pool, probe_types, radix_bits, 1).unwrap();
        let mut local_state = spill.register_thread();
        spill
            .append(&build_probe_chunk(0, 64, radix_bits), &mut local_state)
            .unwrap();

        let err = spill.prepare_next_probe().unwrap_err();
        assert!(err.to_string().contains("unfinalized"));
    }
}
