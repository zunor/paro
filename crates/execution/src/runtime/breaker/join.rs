// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;
use paro_common::memory::{MemoryAccountingContext, MemoryError, MemoryResult};
use paro_common::types::LogicalType;
use paro_planner::operator::join::{JoinCondition, JoinType};
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::index::{ColumnId, PredicateTree};
use paro_storage::row::{
    RadixPartitionedRows, RadixPartitionedRowsBuilder, ReclaimableRowStore, RowLayout, RowStore,
    RowValidityType,
};

use crate::join_hashtable::table::BuildTimeIntegerIndexBuilder;
use crate::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use crate::memory_runtime::{ReclaimStats, Reclaimer, SpillCost};
use crate::pipeline::graph::PipelineId;
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[path = "join_runtime_filter.rs"]
mod runtime_filter;
use runtime_filter::JoinRuntimeFilter;
pub use runtime_filter::JoinRuntimeFilterBuilder;

pub const HASH_JOIN_SPILL_MIN_RADIX_BITS: usize = 1;
pub const HASH_JOIN_SPILL_MAX_RADIX_BITS: usize = 12;
pub const HASH_JOIN_SPILL_MIN_TARGET_PARTITION_BYTES: usize = 1024 * 1024;
pub const HASH_JOIN_SPILL_TARGET_PARTITION_BYTES: usize = 64 * 1024 * 1024;

static NEXT_HASH_JOIN_LOCAL_BUILD_RECLAIMER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum BuildReclaimState {
    Disabled = 0,
    Enabled = 1,
    Sealed = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JoinBuildId(pub u32);

#[derive(Debug)]
pub struct JoinBuildHandle {
    metadata: BreakerHandleMetadata,
    pub join_id: JoinBuildId,
    pub spill: Arc<JoinSpillState>,
    pub completion: CompletionLatch,
    pub stats: JoinBuildStats,
    table: Mutex<JoinHashTableState>,
    pending_consumers: Mutex<HashSet<PipelineId>>,
    runtime_filter_builder: Mutex<Option<JoinRuntimeFilterBuilder>>,
    runtime_filter: OnceLock<JoinRuntimeFilter>,
    build_time_integer_builder: Mutex<Option<Arc<BuildTimeIntegerIndexBuilder>>>,
    mode: AtomicU8,
    build_reclaim_state: AtomicU8,
    build_spill_gate: Mutex<()>,
    spill_radix_bits: OnceLock<usize>,
    external: OnceLock<JoinExternalModeConfig>,
    cleanup: CleanupState,
}

impl JoinBuildHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        let pending_consumers = metadata.consumers.iter().copied().collect();
        Self {
            join_id: JoinBuildId(metadata.id.index() as u32),
            metadata,
            spill: Arc::new(JoinSpillState::default()),
            completion: CompletionLatch::default(),
            stats: JoinBuildStats::default(),
            table: Mutex::new(JoinHashTableState::Uninitialized),
            pending_consumers: Mutex::new(pending_consumers),
            runtime_filter_builder: Mutex::new(None),
            runtime_filter: OnceLock::new(),
            build_time_integer_builder: Mutex::new(None),
            mode: AtomicU8::new(JoinBuildMode::InMemory as u8),
            build_reclaim_state: AtomicU8::new(BuildReclaimState::Disabled as u8),
            build_spill_gate: Mutex::new(()),
            spill_radix_bits: OnceLock::new(),
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

    pub(crate) fn share_build_time_integer_builder(
        &self,
        builder: Arc<BuildTimeIntegerIndexBuilder>,
    ) -> Arc<BuildTimeIntegerIndexBuilder> {
        let mut state = self.build_time_integer_builder.lock();
        if let Some(existing) = state.as_ref() {
            return Arc::clone(existing);
        }
        *state = Some(Arc::clone(&builder));
        builder
    }

    pub(crate) fn build_time_integer_builder(&self) -> Option<Arc<BuildTimeIntegerIndexBuilder>> {
        self.build_time_integer_builder.lock().as_ref().cloned()
    }

    pub(crate) fn take_build_time_integer_builder(
        &self,
    ) -> Result<Option<BuildTimeIntegerIndexBuilder>> {
        let Some(builder) = self.build_time_integer_builder.lock().take() else {
            return Ok(None);
        };
        Arc::try_unwrap(builder).map(Some).map_err(|_| {
            paro_error::internal("unique integer join builder retained a local reference")
        })
    }

    pub fn initialize_table(
        &self,
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        conditions: Vec<JoinCondition>,
        build_types: Vec<LogicalType>,
        join_type: JoinType,
        memory: MemoryAccountingContext,
    ) -> Result<Arc<JoinHashTable>> {
        let build_output_count = build_types.len();
        self.initialize_table_with_output_count(
            buffer_pool,
            allocator,
            conditions,
            build_types,
            build_output_count,
            join_type,
            false,
            memory,
        )
    }

    pub fn initialize_table_with_output_count(
        &self,
        buffer_pool: Arc<BufferPool>,
        allocator: Arc<dyn Allocator>,
        conditions: Vec<JoinCondition>,
        build_types: Vec<LogicalType>,
        build_output_count: usize,
        join_type: JoinType,
        build_keys_unique: bool,
        memory: MemoryAccountingContext,
    ) -> Result<Arc<JoinHashTable>> {
        let runtime_filter_key_types = conditions
            .iter()
            .map(|condition| condition.right.return_type())
            .collect::<Vec<_>>();
        self.initialize_runtime_filter_builder(
            &runtime_filter_key_types,
            memory.with_class(MemoryAccountingClass::Metadata),
        );
        let mut state = self.table.lock();
        match &*state {
            JoinHashTableState::Live(table) => return Ok(Arc::clone(table)),
            JoinHashTableState::Released => {
                return Err(paro_error::internal(
                    "hash join table cannot be reinitialized after its consumers finished",
                ));
            }
            JoinHashTableState::Uninitialized => {}
        }
        let table = Arc::new(JoinHashTable::new_with_memory_and_output_count(
            buffer_pool,
            allocator,
            conditions,
            build_types,
            build_output_count,
            join_type,
            JoinHashTableConfig {
                build_keys_unique,
                ..Default::default()
            },
            memory,
        ));
        *state = JoinHashTableState::Live(Arc::clone(&table));
        Ok(table)
    }

    #[inline]
    pub fn table(&self) -> Option<Arc<JoinHashTable>> {
        match &*self.table.lock() {
            JoinHashTableState::Live(table) => Some(Arc::clone(table)),
            JoinHashTableState::Uninitialized | JoinHashTableState::Released => None,
        }
    }

    pub fn require_table(&self) -> Result<Arc<JoinHashTable>> {
        self.table()
            .ok_or_else(|| paro_error::internal("hash join build handle has no live hash table"))
    }

    /// Release the build table when the last pipeline that consumes this
    /// breaker finishes. The catalog is the single source of truth for
    /// consumer ownership, so replay and unmatched-output branches naturally
    /// extend the lifetime without relying on scheduler order. Removing from
    /// the pending set also makes duplicate completion notifications idempotent.
    pub fn consumer_finished(&self, pipeline: PipelineId) -> bool {
        let released = {
            let mut pending = self.pending_consumers.lock();
            if !pending.remove(&pipeline) {
                return false;
            }
            pending.is_empty()
        };
        if released {
            let old = std::mem::replace(&mut *self.table.lock(), JoinHashTableState::Released);
            drop(old);
        }
        released
    }

    pub fn finalize_in_memory(&self) -> Result<()> {
        let table = self.require_table()?;
        self.publish_runtime_filter_from_builder()?;
        table.finalize()?;
        self.completion.mark_complete();
        Ok(())
    }

    pub fn initialize_runtime_filter_builder(
        &self,
        key_types: &[LogicalType],
        memory: MemoryAccountingContext,
    ) {
        let mut builder = self.runtime_filter_builder.lock();
        if builder.is_none() {
            *builder = Some(JoinRuntimeFilterBuilder::empty_with_memory(
                key_types, memory,
            ));
        }
    }

    pub fn merge_runtime_filter_builder(
        &self,
        incoming: Option<JoinRuntimeFilterBuilder>,
    ) -> Result<()> {
        let Some(incoming) = incoming else {
            return Ok(());
        };
        let mut builder = self.runtime_filter_builder.lock();
        match builder.as_mut() {
            Some(existing) => existing.merge(incoming)?,
            None => *builder = Some(incoming),
        }
        Ok(())
    }

    pub fn publish_runtime_filter_from_builder(&self) -> Result<()> {
        let mut builder = self.runtime_filter_builder.lock();
        if self.runtime_filter.get().is_some() {
            return Ok(());
        }
        let filter = builder
            .take()
            .unwrap_or_else(|| JoinRuntimeFilterBuilder::empty(&[]))
            .freeze();
        // The builder lock serializes concurrent finalize/reclaim publishers,
        // so ownership can move into the immutable filter without cloning it.
        self.runtime_filter.set(filter).map_err(|_| {
            paro_error::internal("hash join runtime filter was published concurrently")
        })?;
        Ok(())
    }

    pub fn runtime_filter_predicate(
        &self,
        build_key_index: usize,
        probe_column_id: ColumnId,
    ) -> Option<PredicateTree> {
        self.runtime_filter
            .get()
            .and_then(|filter| filter.predicate_for_column(build_key_index, probe_column_id))
    }

    pub fn runtime_filter_ready(&self) -> bool {
        self.runtime_filter.get().is_some()
    }

    pub fn enable_build_reclaim(&self) {
        if !self.completion.is_complete() && !self.is_external() {
            let _ = self.build_reclaim_state.compare_exchange(
                BuildReclaimState::Disabled as u8,
                BuildReclaimState::Enabled as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    pub fn disable_build_reclaim(&self) {
        let _ =
            self.build_reclaim_state
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                    (state != BuildReclaimState::Sealed as u8)
                        .then_some(BuildReclaimState::Disabled as u8)
                });
    }

    /// Permanently closes the build-reclaim window and waits for a reclaimer
    /// that already entered the spill gate. After this returns, an in-memory
    /// finalizer may treat the build store as immutable.
    pub fn seal_build_reclaim(&self) {
        self.build_reclaim_state
            .store(BuildReclaimState::Sealed as u8, Ordering::Release);
        let _spill_guard = self.build_spill_gate.lock();
    }

    pub fn build_reclaim_enabled(&self) -> bool {
        self.build_reclaim_state.load(Ordering::Acquire) == BuildReclaimState::Enabled as u8
    }

    pub fn has_build_spill(&self) -> bool {
        self.spill.has_build_spill()
    }

    pub fn build_spill_radix_bits(&self, build_bytes: usize, query_memory_cap: usize) -> usize {
        *self
            .spill_radix_bits
            .get_or_init(|| choose_hash_join_radix_bits(build_bytes, query_memory_cap))
    }

    pub fn reclaim_build(
        &self,
        target_bytes: usize,
        query_memory_cap: usize,
        memory: MemoryAccountingContext,
    ) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let _spill_guard = self.build_spill_gate.lock();
        if !self.build_reclaim_enabled() || self.completion.is_complete() || self.is_external() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        self.spill_build_locked(target_bytes, query_memory_cap, memory)
    }

    pub fn spill_build_for_external(
        &self,
        target_bytes: usize,
        query_memory_cap: usize,
        memory: MemoryAccountingContext,
    ) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let _spill_guard = self.build_spill_gate.lock();
        if self.completion.is_complete() || self.is_external() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        self.spill_build_locked(target_bytes, query_memory_cap, memory)
    }

    fn spill_build_locked(
        &self,
        target_bytes: usize,
        query_memory_cap: usize,
        memory: MemoryAccountingContext,
    ) -> MemoryResult<ReclaimStats> {
        let Some(table) = self.table() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let before = table.build_rows_size_in_bytes();
        if before == 0 && !self.has_build_spill() {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        self.publish_runtime_filter_from_builder()
            .map_err(hash_join_reclaim_error)?;
        let radix_bits = self.build_spill_radix_bits(before, query_memory_cap);
        if before > 0 {
            let mut buffer = JoinBuildSpillBuffer::new(
                table.buffer_pool().clone(),
                radix_bits,
                table.hash_column_index(),
                table.layout().types().to_vec(),
                memory,
            )
            .map_err(hash_join_reclaim_error)?;
            table
                .drain_build_store_spill_chunks(|chunk| buffer.append(chunk))
                .map_err(hash_join_reclaim_error)?;
            self.spill
                .append_build_buffer(buffer)
                .map_err(hash_join_reclaim_error)?;
        }
        let partition_count = self
            .spill
            .seal_build_partitions()
            .map_err(hash_join_reclaim_error)?;
        self.set_external_mode(JoinExternalModeConfig {
            radix_bits: radix_bits as u8,
            build_partitions: JoinPartitionSet { partition_count },
            probe_partitions: ProbeSpillSet::default(),
        })
        .map_err(hash_join_reclaim_error)?;
        self.completion.mark_complete();
        self.disable_build_reclaim();
        Ok(ReclaimStats::new(target_bytes, before, before))
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

#[derive(Debug)]
enum JoinHashTableState {
    Uninitialized,
    Live(Arc<JoinHashTable>),
    Released,
}

fn hash_join_reclaim_error(err: paro_common::error::ParoError) -> MemoryError {
    MemoryError::reclaim_failed(format!("hash join build spill reclaim failed: {err}"))
}

#[derive(Debug)]
pub struct HashJoinBuildSpillReclaimer {
    name: String,
    handle: Arc<JoinBuildHandle>,
    memory: MemoryAccountingContext,
    query_memory_cap: usize,
}

impl HashJoinBuildSpillReclaimer {
    pub fn new(
        handle: Arc<JoinBuildHandle>,
        memory: MemoryAccountingContext,
        query_memory_cap: usize,
    ) -> Self {
        Self {
            name: Self::name_for(handle.as_ref()),
            handle,
            memory,
            query_memory_cap,
        }
    }

    pub fn name_for(handle: &JoinBuildHandle) -> String {
        format!("hash_join_build_spill:{}", handle.metadata().id.index())
    }
}

impl Reclaimer for HashJoinBuildSpillReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        if !self.handle.build_reclaim_enabled()
            || self.handle.completion.is_complete()
            || self.handle.is_external()
        {
            return 0;
        }
        self.handle
            .table()
            .map_or(0, |table| table.build_rows_size_in_bytes())
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if !self.handle.build_reclaim_enabled() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        self.handle
            .reclaim_build(target_bytes, self.query_memory_cap, self.memory.clone())
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::Repartition
    }
}

#[derive(Debug)]
pub struct HashJoinLocalBuildSpillReclaimer {
    name: String,
    handle: Arc<JoinBuildHandle>,
    table: Arc<JoinHashTable>,
    build_spill: Arc<Mutex<Option<JoinBuildSpillBuffer>>>,
    memory: MemoryAccountingContext,
    query_memory_cap: usize,
}

impl HashJoinLocalBuildSpillReclaimer {
    pub(crate) fn new(
        handle: Arc<JoinBuildHandle>,
        local_id: u64,
        table: Arc<JoinHashTable>,
        build_spill: Arc<Mutex<Option<JoinBuildSpillBuffer>>>,
        memory: MemoryAccountingContext,
        query_memory_cap: usize,
    ) -> Self {
        Self {
            name: Self::name_for(&handle, local_id),
            handle,
            table,
            build_spill,
            memory,
            query_memory_cap,
        }
    }

    pub fn next_local_id() -> u64 {
        NEXT_HASH_JOIN_LOCAL_BUILD_RECLAIMER_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn name_for(handle: &JoinBuildHandle, local_id: u64) -> String {
        format!(
            "hash_join_local_build_spill:{}:{}",
            handle.metadata().id.index(),
            local_id
        )
    }
}

impl Reclaimer for HashJoinLocalBuildSpillReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        if self.handle.completion.is_complete() || self.handle.is_external() {
            return 0;
        }
        self.table.try_build_rows_size_in_bytes().unwrap_or(0)
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 || self.handle.completion.is_complete() || self.handle.is_external() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let Some(before) = self.table.try_build_rows_size_in_bytes() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        if before == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        let radix_bits = self
            .handle
            .build_spill_radix_bits(before, self.query_memory_cap);
        let mut build_spill = self.build_spill.lock();
        if build_spill.is_none() {
            *build_spill = Some(
                JoinBuildSpillBuffer::new(
                    self.table.buffer_pool().clone(),
                    radix_bits,
                    self.table.hash_column_index(),
                    self.table.layout().types().to_vec(),
                    self.memory.clone(),
                )
                .map_err(hash_join_reclaim_error)?,
            );
        }
        let spill = build_spill
            .as_mut()
            .expect("hash join local build spill initialized above");
        let spill_bytes_before = spill.size_in_bytes();
        let Some(drained_bytes) = self
            .table
            .try_drain_build_store_spill_chunks(|chunk| spill.append(chunk))
            .map_err(hash_join_reclaim_error)?
        else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        if drained_bytes == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let spilled_bytes = spill.size_in_bytes().saturating_sub(spill_bytes_before);
        Ok(ReclaimStats::new(
            target_bytes,
            drained_bytes,
            spilled_bytes,
        ))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

impl RuntimeCleanup for JoinBuildHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.disable_build_reclaim();
        ctx.query
            .memory
            .unregister_reclaimer_by_name(&HashJoinBuildSpillReclaimer::name_for(self));
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
    build_rows: AtomicU64,
    build_bytes: AtomicU64,
    probe_rows: AtomicU64,
    probe_bytes: AtomicU64,
    max_partition_bytes: AtomicU64,
    spill_latency_us: AtomicU64,
    repartition_depth: AtomicUsize,
    inner: Mutex<JoinSpillInner>,
    cleanup: CleanupState,
}

#[derive(Debug)]
pub struct JoinBuildSpillBuffer {
    builder: RadixPartitionedRowsBuilder,
    rows: u64,
    bytes: u64,
    latency_us: u64,
}

#[derive(Debug)]
pub struct JoinProbeSpillBuffer {
    builder: RadixPartitionedRowsBuilder,
    rows: u64,
    bytes: u64,
    latency_us: u64,
}

impl JoinBuildSpillBuffer {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        radix_bits: usize,
        hash_col_idx: usize,
        types: Vec<LogicalType>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        let memory = memory.with_class(MemoryAccountingClass::Spill);
        Ok(Self {
            builder: RadixPartitionedRowsBuilder::new_with_memory(
                buffer_pool,
                Arc::new(RowLayout::from_types(
                    types,
                    RowValidityType::CanHaveNullValues,
                )),
                MemoryTag::HashTable,
                radix_bits,
                hash_col_idx,
                memory,
            )?,
            rows: 0,
            bytes: 0,
            latency_us: 0,
        })
    }

    pub fn append(&mut self, chunk: &Chunk) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let started_at = Instant::now();
        let before_bytes = self.builder.size_in_bytes();
        self.builder.append(chunk)?;
        let after_bytes = self.builder.size_in_bytes();
        self.rows = self.rows.saturating_add(chunk.size() as u64);
        self.bytes = self
            .bytes
            .saturating_add(after_bytes.saturating_sub(before_bytes) as u64);
        self.latency_us = self
            .latency_us
            .saturating_add(started_at.elapsed().as_micros() as u64);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn size_in_bytes(&self) -> usize {
        self.builder.size_in_bytes()
    }

    fn partition_count(&self) -> usize {
        self.builder.partition_count()
    }

    fn into_parts(self) -> (RadixPartitionedRowsBuilder, u64, u64, u64) {
        (self.builder, self.rows, self.bytes, self.latency_us)
    }
}

impl JoinProbeSpillBuffer {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        radix_bits: usize,
        hash_col_idx: usize,
        types: Vec<LogicalType>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        let memory = memory.with_class(MemoryAccountingClass::Spill);
        Ok(Self {
            builder: RadixPartitionedRowsBuilder::new_with_memory(
                buffer_pool,
                Arc::new(RowLayout::from_types(
                    types,
                    RowValidityType::CanHaveNullValues,
                )),
                MemoryTag::HashTable,
                radix_bits,
                hash_col_idx,
                memory,
            )?,
            rows: 0,
            bytes: 0,
            latency_us: 0,
        })
    }

    pub fn append(&mut self, chunk: &Chunk) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let started_at = Instant::now();
        let before_bytes = self.builder.size_in_bytes();
        self.builder.append(chunk)?;
        let after_bytes = self.builder.size_in_bytes();
        self.rows = self.rows.saturating_add(chunk.size() as u64);
        self.bytes = self
            .bytes
            .saturating_add(after_bytes.saturating_sub(before_bytes) as u64);
        self.latency_us = self
            .latency_us
            .saturating_add(started_at.elapsed().as_micros() as u64);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    fn partition_count(&self) -> usize {
        self.builder.partition_count()
    }

    fn into_parts(self) -> (RadixPartitionedRowsBuilder, u64, u64, u64) {
        (self.builder, self.rows, self.bytes, self.latency_us)
    }
}

impl JoinSpillState {
    pub fn install_build_partitions(&self, build: RadixPartitionedRows) -> Result<()> {
        let partition_count = build.partition_count();
        let build_rows = build.count();
        let build_bytes = build.size_in_bytes() as u64;
        let max_partition_bytes = max_partition_size(&build) as u64;
        let mut inner = self.inner.lock();
        if inner.build_builder.is_some() || inner.build_partitions.is_some() {
            return Err(paro_error::internal(
                "hash join build spill partitions already installed",
            ));
        }
        inner.radix_bits = build.radix_bits();
        inner.build_partitions = Some(build);
        self.build_partition_count
            .store(partition_count, Ordering::Release);
        self.build_rows.store(build_rows, Ordering::Release);
        self.build_bytes.store(build_bytes, Ordering::Release);
        self.update_max_partition_bytes(max_partition_bytes);
        Ok(())
    }

    pub fn append_build_buffer(&self, buffer: JoinBuildSpillBuffer) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append hash join build spill after replay partitions are sealed",
            ));
        }

        let partition_count = buffer.partition_count();
        let (builder, rows, bytes, latency_us) = buffer.into_parts();
        let incoming_radix_bits = builder.radix_bits();
        let mut inner = self.inner.lock();
        if inner.build_partitions.is_some() {
            return Err(paro_error::internal(
                "cannot append hash join build spill after build partitions are sealed",
            ));
        }
        let (partition_count, radix_bits) = if let Some(existing) = inner.build_builder.as_mut() {
            existing.try_absorb(builder)?;
            (existing.partition_count(), existing.radix_bits())
        } else {
            inner.build_builder = Some(builder);
            (partition_count, incoming_radix_bits)
        };
        inner.radix_bits = radix_bits;
        self.build_rows.fetch_add(rows, Ordering::Relaxed);
        self.build_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.spill_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.build_partition_count
            .store(partition_count, Ordering::Release);
        Ok(())
    }

    pub fn append_probe_buffer(&self, buffer: JoinProbeSpillBuffer) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append hash join probe spill after replay partitions are sealed",
            ));
        }

        let partition_count = buffer.partition_count();
        let (builder, rows, bytes, latency_us) = buffer.into_parts();
        let mut inner = self.inner.lock();
        let partition_count = if let Some(existing) = inner.probe_builder.as_mut() {
            existing.try_absorb(builder)?;
            existing.partition_count()
        } else {
            inner.probe_builder = Some(builder);
            partition_count
        };
        self.probe_rows.fetch_add(rows, Ordering::Relaxed);
        self.probe_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.spill_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.probe_partition_count
            .store(partition_count, Ordering::Release);
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
            if let Some(probe_len) = inner
                .probe_partitions
                .as_ref()
                .map(RadixPartitionedRows::partition_count)
            {
                if build_len != probe_len {
                    return Err(paro_error::internal(format!(
                        "hash join spill partition count mismatch during replay: build={build_len}, probe={probe_len}"
                    )));
                }
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
                .map(|partitions| partitions.take_partition(partition_idx))
                .filter(|rows| !rows.is_empty());
            if build_rows.is_empty() && probe_rows.is_none() {
                continue;
            }
            return Ok(Some(JoinReplayPartition {
                partition_idx,
                build_rows: build_rows.into_reclaimable(),
                probe_rows: probe_rows.map(RowStore::into_reclaimable),
            }));
        }
    }

    pub fn partition_counts(&self) -> (usize, usize) {
        (
            self.build_partition_count.load(Ordering::Acquire),
            self.probe_partition_count.load(Ordering::Acquire),
        )
    }

    pub fn has_build_spill(&self) -> bool {
        self.build_rows.load(Ordering::Acquire) > 0
            || self.build_partition_count.load(Ordering::Acquire) > 0
    }

    pub fn seal_build_partitions(&self) -> Result<usize> {
        let mut inner = self.inner.lock();
        self.seal_build_partitions_locked(&mut inner);
        Ok(inner
            .build_partitions
            .as_ref()
            .map_or(0, RadixPartitionedRows::partition_count))
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

    pub fn stats(&self) -> JoinSpillStats {
        JoinSpillStats {
            build_rows: self.build_rows.load(Ordering::Acquire),
            build_bytes: self.build_bytes.load(Ordering::Acquire),
            probe_rows: self.probe_rows.load(Ordering::Acquire),
            probe_bytes: self.probe_bytes.load(Ordering::Acquire),
            max_partition_bytes: self.max_partition_bytes.load(Ordering::Acquire),
            spill_latency_us: self.spill_latency_us.load(Ordering::Acquire),
            repartition_depth: self.repartition_depth.load(Ordering::Acquire),
        }
    }

    fn seal_probe_partitions(&self) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        self.seal_build_partitions_locked(&mut inner);
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
            self.repartition_oversized_partitions(&mut inner)?;
            self.probe_partition_count.store(
                inner
                    .probe_partitions
                    .as_ref()
                    .map_or(0, |p| p.partition_count()),
                Ordering::Release,
            );
        }
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    fn seal_build_partitions_locked(&self, inner: &mut JoinSpillInner) {
        if inner.build_partitions.is_none() {
            inner.build_partitions = inner
                .build_builder
                .take()
                .map(RadixPartitionedRowsBuilder::seal);
        }
        if let Some(build_partitions) = inner.build_partitions.as_ref() {
            let partition_count = build_partitions.partition_count();
            self.build_partition_count
                .store(partition_count, Ordering::Release);
            self.build_rows
                .store(build_partitions.count(), Ordering::Release);
            self.build_bytes
                .store(build_partitions.size_in_bytes() as u64, Ordering::Release);
            self.update_max_partition_bytes(max_partition_size(build_partitions) as u64);
        }
    }

    fn repartition_oversized_partitions(&self, inner: &mut JoinSpillInner) -> Result<()> {
        loop {
            let (Some(build), Some(probe)) = (
                inner.build_partitions.as_ref(),
                inner.probe_partitions.as_ref(),
            ) else {
                return Ok(());
            };
            let max_bytes = max_partition_size(build).max(max_partition_size(probe));
            self.update_max_partition_bytes(max_bytes as u64);
            if max_bytes <= HASH_JOIN_SPILL_TARGET_PARTITION_BYTES
                || inner.radix_bits >= HASH_JOIN_SPILL_MAX_RADIX_BITS
            {
                return Ok(());
            }
            let next_bits = inner
                .radix_bits
                .saturating_add(1)
                .min(HASH_JOIN_SPILL_MAX_RADIX_BITS);
            let build = inner
                .build_partitions
                .take()
                .expect("build partitions checked above")
                .into_repartitioned(next_bits)?;
            let probe = inner
                .probe_partitions
                .take()
                .expect("probe partitions checked above")
                .into_repartitioned(next_bits)?;
            inner.radix_bits = next_bits;
            self.repartition_depth.fetch_add(1, Ordering::Relaxed);
            self.build_partition_count
                .store(build.partition_count(), Ordering::Release);
            self.probe_partition_count
                .store(probe.partition_count(), Ordering::Release);
            self.build_bytes
                .store(build.size_in_bytes() as u64, Ordering::Release);
            self.probe_bytes
                .store(probe.size_in_bytes() as u64, Ordering::Release);
            inner.build_partitions = Some(build);
            inner.probe_partitions = Some(probe);
        }
    }

    fn update_max_partition_bytes(&self, value: u64) {
        let _ =
            self.max_partition_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.max(value))
                });
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
        self.build_rows.store(0, Ordering::Release);
        self.build_bytes.store(0, Ordering::Release);
        self.probe_rows.store(0, Ordering::Release);
        self.probe_bytes.store(0, Ordering::Release);
        self.max_partition_bytes.store(0, Ordering::Release);
        self.spill_latency_us.store(0, Ordering::Release);
        self.repartition_depth.store(0, Ordering::Release);
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct JoinSpillInner {
    radix_bits: usize,
    build_builder: Option<RadixPartitionedRowsBuilder>,
    build_partitions: Option<RadixPartitionedRows>,
    probe_builder: Option<RadixPartitionedRowsBuilder>,
    probe_partitions: Option<RadixPartitionedRows>,
}

#[derive(Debug)]
pub struct JoinReplayPartition {
    pub partition_idx: usize,
    pub build_rows: ReclaimableRowStore,
    pub probe_rows: Option<ReclaimableRowStore>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JoinSpillStats {
    pub build_rows: u64,
    pub build_bytes: u64,
    pub probe_rows: u64,
    pub probe_bytes: u64,
    pub max_partition_bytes: u64,
    pub spill_latency_us: u64,
    pub repartition_depth: usize,
}

pub fn choose_hash_join_radix_bits(build_bytes: usize, query_memory_cap: usize) -> usize {
    let target = query_memory_cap
        .checked_div(4)
        .unwrap_or(HASH_JOIN_SPILL_TARGET_PARTITION_BYTES)
        .clamp(
            HASH_JOIN_SPILL_MIN_TARGET_PARTITION_BYTES,
            HASH_JOIN_SPILL_TARGET_PARTITION_BYTES,
        );
    let desired_partitions = build_bytes.div_ceil(target).max(2);
    desired_partitions
        .next_power_of_two()
        .trailing_zeros()
        .try_into()
        .unwrap_or(HASH_JOIN_SPILL_MAX_RADIX_BITS)
        .clamp(
            HASH_JOIN_SPILL_MIN_RADIX_BITS,
            HASH_JOIN_SPILL_MAX_RADIX_BITS,
        )
}

fn max_partition_size(rows: &RadixPartitionedRows) -> usize {
    rows.partitions()
        .iter()
        .map(RowStore::size_in_bytes)
        .max()
        .unwrap_or(0)
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
#[path = "join_tests.rs"]
mod tests;
