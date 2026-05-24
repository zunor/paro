// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;
use paro_planner::operator::join::{JoinCondition, JoinType};
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::row::{
    RadixPartitionedRows, RadixPartitionedRowsBuilder, RowLayout, RowStore, RowValidityType,
};

use crate::join_hashtable::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JoinBuildId(pub u32);

#[derive(Debug)]
pub struct JoinBuildHandle {
    metadata: BreakerHandleMetadata,
    pub join_id: JoinBuildId,
    pub spill: Arc<JoinSpillState>,
    pub completion: CompletionLatch,
    pub stats: JoinBuildStats,
    table: OnceLock<Arc<JoinHashTable>>,
    mode: AtomicU8,
    external: OnceLock<JoinExternalModeConfig>,
    cleanup: CleanupState,
}

impl JoinBuildHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            join_id: JoinBuildId(metadata.id.index() as u32),
            metadata,
            spill: Arc::new(JoinSpillState::default()),
            completion: CompletionLatch::default(),
            stats: JoinBuildStats::default(),
            table: OnceLock::new(),
            mode: AtomicU8::new(JoinBuildMode::InMemory as u8),
            external: OnceLock::new(),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    #[inline]
    pub fn mode(&self) -> JoinBuildMode {
        JoinBuildMode::from_u8(self.mode.load(Ordering::Acquire))
    }

    #[inline]
    pub fn is_external(&self) -> bool {
        self.mode() == JoinBuildMode::External
    }

    #[inline]
    pub fn external_config(&self) -> Option<&JoinExternalModeConfig> {
        self.external.get()
    }

    pub fn set_external_mode(&self, config: JoinExternalModeConfig) -> Result<()> {
        self.external
            .set(config)
            .map_err(|_| paro_error::internal("join build external mode was already set"))?;
        self.mode
            .store(JoinBuildMode::External as u8, Ordering::Release);
        Ok(())
    }

    pub fn initialize_table(
        &self,
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        conditions: Vec<JoinCondition>,
        build_types: Vec<LogicalType>,
        join_type: JoinType,
        memory: MemoryAccountingContext,
    ) -> Arc<JoinHashTable> {
        Arc::clone(self.table.get_or_init(|| {
            Arc::new(JoinHashTable::new_with_memory(
                buffer_pool,
                allocator,
                conditions,
                build_types,
                join_type,
                JoinHashTableConfig::default(),
                memory,
            ))
        }))
    }

    #[inline]
    pub fn table(&self) -> Option<Arc<JoinHashTable>> {
        self.table.get().cloned()
    }

    pub fn require_table(&self) -> Result<Arc<JoinHashTable>> {
        self.table
            .get()
            .cloned()
            .ok_or_else(|| paro_error::internal("hash join build handle has no hash table"))
    }

    pub fn finalize_in_memory(&self) -> Result<()> {
        let table = self.require_table()?;
        table.finalize()?;
        self.completion.mark_complete();
        Ok(())
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for JoinBuildHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.spill.cleanup(ctx, reason)?;
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JoinBuildMode {
    InMemory = 0,
    External = 1,
}

impl JoinBuildMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::External,
            _ => Self::InMemory,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinExternalModeConfig {
    pub radix_bits: u8,
    pub build_partitions: JoinPartitionSet,
    pub probe_partitions: ProbeSpillSet,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinPartitionSet {
    pub partition_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProbeSpillSet {
    pub partition_count: usize,
}

#[derive(Debug, Default)]
pub struct JoinSpillState {
    build_partition_count: AtomicUsize,
    probe_partition_count: AtomicUsize,
    replay_partition: AtomicUsize,
    sealed: AtomicBool,
    inner: Mutex<JoinSpillInner>,
    cleanup: CleanupState,
}

impl JoinSpillState {
    pub fn install_build_partitions(&self, build: RadixPartitionedRows) -> Result<()> {
        let partition_count = build.partition_count();
        let mut inner = self.inner.lock();
        if inner.build_partitions.is_some() {
            return Err(paro_error::internal(
                "hash join build spill partitions already installed",
            ));
        }
        inner.radix_bits = build.radix_bits();
        inner.build_partitions = Some(build);
        self.build_partition_count
            .store(partition_count, Ordering::Release);
        Ok(())
    }

    pub fn append_probe_chunk(
        &self,
        buffer_pool: Arc<BufferPool>,
        radix_bits: usize,
        hash_col_idx: usize,
        chunk: &Chunk,
        memory: MemoryAccountingContext,
    ) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append hash join probe spill after replay partitions are sealed",
            ));
        }
        let mut inner = self.inner.lock();
        if inner.probe_builder.is_none() {
            inner.probe_builder = Some(RadixPartitionedRowsBuilder::new_with_memory(
                buffer_pool,
                Arc::new(RowLayout::from_types(
                    chunk.types(),
                    RowValidityType::CanHaveNullValues,
                )),
                MemoryTag::HashTable,
                radix_bits,
                hash_col_idx,
                memory,
            )?);
        }
        let builder = inner
            .probe_builder
            .as_mut()
            .expect("probe spill builder was initialized");
        builder.append(chunk)?;
        self.probe_partition_count
            .store(builder.partition_count(), Ordering::Release);
        Ok(())
    }

    pub fn take_next_replay_partition(&self) -> Result<Option<JoinReplayPartition>> {
        self.seal_probe_partitions()?;
        loop {
            let (build_count, probe_count) = self.partition_counts();
            let partition_count = build_count.max(probe_count);
            let partition_idx = self.replay_partition.fetch_add(1, Ordering::AcqRel);
            if partition_idx >= partition_count {
                return Ok(None);
            }

            let mut inner = self.inner.lock();
            let build_len = inner
                .build_partitions
                .as_ref()
                .ok_or_else(|| {
                    paro_error::internal(
                        "hash join spill replay requested before build partitions were installed",
                    )
                })?
                .partition_count();
            let Some(probe_len) = inner
                .probe_partitions
                .as_ref()
                .map(RadixPartitionedRows::partition_count)
            else {
                return Ok(None);
            };
            if build_len != probe_len {
                return Err(paro_error::internal(format!(
                    "hash join spill partition count mismatch during replay: build={build_len}, probe={probe_len}"
                )));
            }
            if partition_idx >= build_len {
                return Ok(None);
            }

            let build_rows = inner
                .build_partitions
                .as_mut()
                .expect("build partitions checked above")
                .take_partition(partition_idx);
            let probe_rows = inner
                .probe_partitions
                .as_mut()
                .expect("probe partitions checked above")
                .take_partition(partition_idx);
            if probe_rows.is_empty() {
                continue;
            }
            return Ok(Some(JoinReplayPartition {
                partition_idx,
                build_rows,
                probe_rows,
            }));
        }
    }

    pub fn partition_counts(&self) -> (usize, usize) {
        (
            self.build_partition_count.load(Ordering::Acquire),
            self.probe_partition_count.load(Ordering::Acquire),
        )
    }

    pub fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }

    fn seal_probe_partitions(&self) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        if inner.probe_partitions.is_none() {
            inner.probe_partitions = inner
                .probe_builder
                .take()
                .map(RadixPartitionedRowsBuilder::seal);
        }
        if let Some(probe_partitions) = inner.probe_partitions.as_ref() {
            if let Some(build_partitions) = inner.build_partitions.as_ref() {
                if build_partitions.partition_count() != probe_partitions.partition_count() {
                    return Err(paro_error::internal(format!(
                        "hash join spill partition count mismatch: build={}, probe={}",
                        build_partitions.partition_count(),
                        probe_partitions.partition_count()
                    )));
                }
            }
            self.probe_partition_count
                .store(probe_partitions.partition_count(), Ordering::Release);
        }
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }
}

impl RuntimeCleanup for JoinSpillState {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        let mut inner = self.inner.lock();
        *inner = JoinSpillInner::default();
        self.build_partition_count.store(0, Ordering::Release);
        self.probe_partition_count.store(0, Ordering::Release);
        self.replay_partition.store(0, Ordering::Release);
        self.sealed.store(false, Ordering::Release);
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct JoinSpillInner {
    radix_bits: usize,
    build_partitions: Option<RadixPartitionedRows>,
    probe_builder: Option<RadixPartitionedRowsBuilder>,
    probe_partitions: Option<RadixPartitionedRows>,
}

#[derive(Debug)]
pub struct JoinReplayPartition {
    pub partition_idx: usize,
    pub build_rows: RowStore,
    pub probe_rows: RowStore,
}

#[derive(Debug, Default)]
pub struct CompletionLatch {
    complete: AtomicBool,
}

impl CompletionLatch {
    pub fn mark_complete(&self) {
        self.complete.store(true, Ordering::Release);
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
}

#[derive(Debug, Default)]
pub struct JoinBuildStats {
    pub build_rows: AtomicU64,
    pub spilled_rows: AtomicU64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::test_utils::{
        test_allocator, test_chunk_from_vectors, test_vector_with_capacity,
    };
    use paro_common::types::LogicalType;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_storage::buffer::{BufferPool, MemoryTag};
    use paro_storage::row::RowValidityType;

    use crate::explain::profiler::OperatorProfiler;
    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};
    use crate::runtime::context::{OperatorCleanupContext, QueryRuntimeContext};
    use crate::runtime::parameter::ParameterBindings;
    use crate::runtime::scratch::TaskMemoryGrants;
    use crate::runtime::QueryOutputPort;
    use crate::thread_context::ThreadContext;

    use super::*;

    fn metadata() -> BreakerHandleMetadata {
        BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind: BreakerHandleKind::HashJoinBuild,
            row_type: RowType::new(vec!["a".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Box::new([]),
            properties: PipelineProperties::default(),
        }
    }

    fn query_context() -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        )
    }

    fn with_cleanup_context<R>(
        query: &QueryRuntimeContext,
        f: impl FnOnce(&mut OperatorCleanupContext<'_>) -> R,
    ) -> R {
        let thread = ThreadContext::single_threaded();
        let memory = TaskMemoryGrants::detached(test_allocator());
        let mut profiler = OperatorProfiler::disabled();
        let mut ctx = OperatorCleanupContext {
            query,
            pipeline: None,
            operator: None,
            thread: &thread,
            memory: memory.call_scope(),
            cancel: &query.cancellation,
            profiler: &mut profiler,
        };
        f(&mut ctx)
    }

    fn radix_input() -> Chunk {
        let mut hashes = test_vector_with_capacity(LogicalType::UBigInt, 4);
        hashes.set_u64(0, 0);
        hashes.set_u64(1, 1 << 63);
        hashes.set_u64(2, 0);
        hashes.set_u64(3, 1 << 63);
        hashes.set_count(4);

        let mut payload = test_vector_with_capacity(LogicalType::Integer, 4);
        payload.set_i32(0, 10);
        payload.set_i32(1, 20);
        payload.set_i32(2, 30);
        payload.set_i32(3, 40);
        payload.set_count(4);

        test_chunk_from_vectors(vec![hashes, payload])
    }

    fn partitioned_rows() -> RadixPartitionedRows {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let layout = Arc::new(RowLayout::from_types(
            vec![LogicalType::UBigInt, LogicalType::Integer],
            RowValidityType::CanHaveNullValues,
        ));
        let mut builder =
            RadixPartitionedRowsBuilder::new(pool, layout, MemoryTag::HashTable, 1, 0)
                .expect("radix builder");
        builder.append(&radix_input()).expect("append radix input");
        builder.seal()
    }

    #[test]
    fn join_build_mode_uses_atomic_discriminant_and_once_external_config() {
        let handle = JoinBuildHandle::new(metadata());
        assert_eq!(handle.mode(), JoinBuildMode::InMemory);
        assert!(!handle.is_external());
        assert!(handle.external_config().is_none());

        handle
            .set_external_mode(JoinExternalModeConfig {
                radix_bits: 4,
                build_partitions: JoinPartitionSet { partition_count: 8 },
                probe_partitions: ProbeSpillSet { partition_count: 8 },
            })
            .expect("external mode should be set once");

        assert_eq!(handle.mode(), JoinBuildMode::External);
        assert!(handle.is_external());
        assert_eq!(
            handle
                .external_config()
                .expect("external config")
                .build_partitions
                .partition_count,
            8
        );
        assert!(handle
            .set_external_mode(JoinExternalModeConfig {
                radix_bits: 5,
                build_partitions: JoinPartitionSet {
                    partition_count: 16
                },
                probe_partitions: ProbeSpillSet {
                    partition_count: 16
                },
            })
            .is_err());
    }

    #[test]
    fn join_spill_cleanup_releases_partitions_and_resets_replay_state() {
        let spill = JoinSpillState::default();
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        spill
            .install_build_partitions(partitioned_rows())
            .expect("install build partitions");
        spill
            .append_probe_chunk(
                pool,
                1,
                0,
                &radix_input(),
                MemoryAccountingContext::detached(
                    paro_common::allocator::MemoryTag::HashTable,
                    paro_common::memory::MemoryAccountingClass::Revocable,
                ),
            )
            .expect("append probe partition chunk");
        assert_eq!(spill.partition_counts(), (2, 2));

        let first_partition = spill
            .take_next_replay_partition()
            .expect("first replay partition")
            .expect("partition");
        assert_eq!(first_partition.partition_idx, 0);
        assert!(spill.is_sealed());

        let query = query_context();
        with_cleanup_context(&query, |ctx| {
            spill
                .cleanup(
                    ctx,
                    CleanupReason::Cancelled(paro_context::StatementCancelReason::UserRequest),
                )
                .expect("cleanup spill");
        });

        assert_eq!(spill.partition_counts(), (0, 0));
        assert!(!spill.is_sealed());
        assert_eq!(spill.cleanup_status(), CleanupStatus::Cancelled);
        assert!(spill
            .take_next_replay_partition()
            .expect("cleanup should leave replay in an empty state")
            .is_none());
    }
}
