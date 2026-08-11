// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime aggregate breaker handle.
//!
//! Build sinks update task-local aggregate state on the per-chunk path. The
//! handle is touched only during local merge, finish, cleanup, and the emit
//! source's first poll. The emit source takes ownership of finalized state so
//! scan batches do not lock the shared handle.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::{ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryError, MemoryResult,
};
use paro_common::vector::Vector;
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_storage::buffer::BufferPool;
use paro_storage::row::{RowSpillWriter, RowStore, RowStoreSpillReader, RowStoreSpillWriter};

use crate::memory_runtime::{ReclaimStats, Reclaimer, SpillCost};
use crate::operators::aggregate::aggregate_kernel::destroy_states;
use crate::operators::aggregate::aggregate_object::AggregateObject;
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::distinct_state::DistinctAggregateState;
use crate::operators::aggregate::ordered_helpers::OrderedAggregateCollector;
use crate::operators::aggregate::payload_spill::{
    AggregateSpilledPayload, AggregateSpilledState, AggregateStateEncoding,
    AggregateStateSpillBuffer,
};
use crate::operators::aggregate::perfect_aggregate_hashtable::{
    FinalizedPerfectAggregateTable, PerfectAggregateHashTable,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::operators::aggregate::row_format::AggregateGroupFormat;
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

static NEXT_AGGREGATE_LOCAL_BUILD_RECLAIMER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct AggregateHandle {
    metadata: BreakerHandleMetadata,
    state: OnceLock<Mutex<Option<AggregateRuntimeState>>>,
    finalized: AtomicBool,
    reclaim_enabled: AtomicBool,
    reclaim_in_progress: AtomicBool,
    cleanup: CleanupState,
}

impl AggregateHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            state: OnceLock::new(),
            finalized: AtomicBool::new(false),
            reclaim_enabled: AtomicBool::new(false),
            reclaim_in_progress: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn initialize(&self, state: AggregateRuntimeState) -> Result<()> {
        match self.state.set(Mutex::new(Some(state))) {
            Ok(()) => Ok(()),
            Err(state) => {
                if let Some(mut state) = state.into_inner() {
                    state.destroy()?;
                }
                Ok(())
            }
        }
    }

    pub fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut AggregateRuntimeState) -> Result<R>,
    ) -> Result<R> {
        let state = self.state.get().ok_or_else(|| {
            paro_error::internal("aggregate handle has no initialized runtime state")
        })?;
        let mut guard = state.lock();
        let state = guard.as_mut().ok_or_else(|| {
            paro_error::internal("aggregate handle state was already moved to emit source")
        })?;
        f(state)
    }

    pub fn take_state(&self) -> Result<Option<AggregateRuntimeState>> {
        self.disable_state_reclaim();
        let state = self.state.get().ok_or_else(|| {
            paro_error::internal("aggregate handle has no initialized runtime state")
        })?;
        Ok(state.lock().take())
    }

    #[inline]
    pub fn mark_finalized(&self) {
        self.finalized.store(true, Ordering::Release);
    }

    #[inline]
    pub fn enable_state_reclaim(&self) {
        if self.is_finalized() {
            self.reclaim_enabled.store(true, Ordering::Release);
        }
    }

    #[inline]
    pub fn disable_state_reclaim(&self) {
        self.reclaim_enabled.store(false, Ordering::Release);
    }

    #[inline]
    pub fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    #[inline]
    pub fn state_reclaim_enabled(&self) -> bool {
        self.reclaim_enabled.load(Ordering::Acquire)
    }

    pub fn reclaimable_state_bytes(&self) -> usize {
        if !self.state_reclaim_enabled() {
            return 0;
        }
        let Some(state) = self.state.get() else {
            return 0;
        };
        let guard = state.lock();
        guard
            .as_ref()
            .map(AggregateRuntimeState::reclaimable_finalized_memory)
            .unwrap_or(0)
    }

    pub fn reclaimable_build_state_bytes(&self) -> usize {
        if self.is_finalized() {
            return 0;
        }
        let Some(state) = self.state.get() else {
            return 0;
        };
        let Some(guard) = state.try_lock() else {
            return 0;
        };
        guard
            .as_ref()
            .map(AggregateRuntimeState::reclaimable_build_memory)
            .unwrap_or(0)
    }

    pub fn reclaim_build_state_memory(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 || self.is_finalized() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        if self.reclaim_in_progress.swap(true, Ordering::AcqRel) {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        struct ReclaimGuard<'a>(&'a AtomicBool);
        impl Drop for ReclaimGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _guard = ReclaimGuard(&self.reclaim_in_progress);

        let Some(state) = self.state.get() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let Some(mut guard) = state.try_lock() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let Some(state) = guard.as_mut() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let reclaimed = state.reclaim_build_memory(target_bytes);
        Ok(ReclaimStats::new(target_bytes, reclaimed, 0))
    }

    pub fn reclaim_state_memory(
        &self,
        target_bytes: usize,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 || !self.state_reclaim_enabled() {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        if self.reclaim_in_progress.swap(true, Ordering::AcqRel) {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        struct ReclaimGuard<'a>(&'a AtomicBool);
        impl Drop for ReclaimGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _guard = ReclaimGuard(&self.reclaim_in_progress);

        let Some(state) = self.state.get() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let mut guard = state.lock();
        let Some(state) = guard.as_mut() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        state
            .reclaim_finalized_memory(target_bytes, buffer_pool, memory)
            .map_err(|err| MemoryError::reclaim_failed(err.to_string()))
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for AggregateHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.disable_state_reclaim();
        ctx.query
            .memory
            .unregister_reclaimer_by_name(&AggregateFinalizedStateReclaimer::name_for(self));
        ctx.query
            .memory
            .unregister_reclaimer_by_name(&AggregateBuildCompactionReclaimer::name_for(self));
        if let Some(state) = self.state.get() {
            if let Some(mut state) = state.lock().take() {
                state.destroy()?;
            }
        }
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug)]
/// Pressure-selectable compaction for aggregate build state.
///
/// This deliberately only releases over-reserved row/varlen capacity while the
/// grouped state is still accepting rows. External grouped spill needs a
/// separate owner because lookup entries and aggregate states remain live until
/// merge/finish can replay a mergeable spilled representation.
pub struct AggregateBuildCompactionReclaimer {
    name: String,
    handle: Arc<AggregateHandle>,
}

impl AggregateBuildCompactionReclaimer {
    pub fn new(handle: Arc<AggregateHandle>) -> Self {
        Self {
            name: Self::name_for(&handle),
            handle,
        }
    }

    pub fn name_for(handle: &AggregateHandle) -> String {
        format!(
            "aggregate_build_compaction:{}",
            handle.metadata().id.index()
        )
    }
}

impl Reclaimer for AggregateBuildCompactionReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        self.handle.reclaimable_build_state_bytes()
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        self.handle
            .reclaim_build_state_memory(target_bytes)
            .map_err(|err| {
                MemoryError::reclaim_failed(format!("aggregate build state reclaim failed: {err}"))
            })
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::AccountingRelease
    }
}

#[derive(Debug)]
pub struct AggregateLocalBuildCompactionReclaimer {
    name: String,
    tables: Arc<Mutex<Vec<AggregateHashTable>>>,
}

impl AggregateLocalBuildCompactionReclaimer {
    pub(crate) fn new(
        handle: &AggregateHandle,
        local_id: u64,
        tables: Arc<Mutex<Vec<AggregateHashTable>>>,
    ) -> Self {
        Self {
            name: Self::name_for(handle, local_id),
            tables,
        }
    }

    pub fn next_local_id() -> u64 {
        NEXT_AGGREGATE_LOCAL_BUILD_RECLAIMER_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn name_for(handle: &AggregateHandle, local_id: u64) -> String {
        format!(
            "aggregate_local_build_compaction:{}:{}",
            handle.metadata().id.index(),
            local_id
        )
    }
}

impl Reclaimer for AggregateLocalBuildCompactionReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        let Some(tables) = self.tables.try_lock() else {
            return 0;
        };
        tables
            .iter()
            .map(AggregateHashTable::reclaimable_build_memory)
            .sum()
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let Some(mut tables) = self.tables.try_lock() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let mut reclaimed = 0usize;
        for table in tables.iter_mut() {
            if reclaimed >= target_bytes {
                break;
            }
            reclaimed =
                reclaimed.saturating_add(table.reclaim_build_memory(target_bytes - reclaimed));
        }
        Ok(ReclaimStats::new(target_bytes, reclaimed, 0))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::AccountingRelease
    }
}

#[derive(Debug)]
pub struct AggregateLocalPayloadSpillReclaimer {
    name: String,
    tables: Arc<Mutex<Vec<AggregateHashTable>>>,
    raw_payload_spill_requested: Arc<AtomicBool>,
}

#[derive(Debug)]
pub struct AggregateLocalStateSpillReclaimer {
    name: String,
    tables: Arc<Mutex<Vec<AggregateHashTable>>>,
    state_spill: Arc<Mutex<Option<AggregateStateSpillBuffer>>>,
    raw_payload_spill_requested: Arc<AtomicBool>,
    buffer_pool: Arc<BufferPool>,
    group_types: Vec<paro_common::types::LogicalType>,
    state_width: usize,
    state_encoding: AggregateStateEncoding,
    radix_bits: usize,
    memory: MemoryAccountingContext,
}

impl AggregateLocalPayloadSpillReclaimer {
    pub(crate) fn new(
        handle: &AggregateHandle,
        local_id: u64,
        tables: Arc<Mutex<Vec<AggregateHashTable>>>,
        raw_payload_spill_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            name: Self::name_for(handle, local_id),
            tables,
            raw_payload_spill_requested,
        }
    }

    pub fn name_for(handle: &AggregateHandle, local_id: u64) -> String {
        format!(
            "aggregate_local_payload_spill:{}:{}",
            handle.metadata().id.index(),
            local_id
        )
    }

    fn reclaimable_empty_table_bytes(tables: &[AggregateHashTable]) -> usize {
        if tables.iter().any(|table| table.count() > 0) {
            return 0;
        }
        tables.iter().map(AggregateHashTable::memory_usage).sum()
    }
}

impl Reclaimer for AggregateLocalPayloadSpillReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        if self.raw_payload_spill_requested.load(Ordering::Acquire) {
            return 0;
        }
        let Some(tables) = self.tables.try_lock() else {
            return 0;
        };
        Self::reclaimable_empty_table_bytes(&tables)
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 || self.raw_payload_spill_requested.load(Ordering::Acquire) {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let Some(mut tables) = self.tables.try_lock() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let before = Self::reclaimable_empty_table_bytes(&tables);
        if before == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        for table in tables.iter_mut() {
            table
                .destroy()
                .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        }
        tables.clear();
        self.raw_payload_spill_requested
            .store(true, Ordering::Release);
        Ok(ReclaimStats::new(target_bytes, before, 0))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

impl AggregateLocalStateSpillReclaimer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        handle: &AggregateHandle,
        local_id: u64,
        tables: Arc<Mutex<Vec<AggregateHashTable>>>,
        state_spill: Arc<Mutex<Option<AggregateStateSpillBuffer>>>,
        raw_payload_spill_requested: Arc<AtomicBool>,
        buffer_pool: Arc<BufferPool>,
        group_types: Vec<paro_common::types::LogicalType>,
        state_width: usize,
        state_encoding: AggregateStateEncoding,
        radix_bits: usize,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            name: Self::name_for(handle, local_id),
            tables,
            state_spill,
            raw_payload_spill_requested,
            buffer_pool,
            group_types,
            state_width,
            state_encoding,
            radix_bits,
            memory,
        }
    }

    pub fn name_for(handle: &AggregateHandle, local_id: u64) -> String {
        format!(
            "aggregate_local_state_spill:{}:{}",
            handle.metadata().id.index(),
            local_id
        )
    }

    fn reclaimable_table_bytes(tables: &[AggregateHashTable]) -> usize {
        tables
            .iter()
            .filter(|table| table.count() > 0)
            .map(AggregateHashTable::memory_usage)
            .sum()
    }
}

impl Reclaimer for AggregateLocalStateSpillReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        if self.raw_payload_spill_requested.load(Ordering::Acquire) {
            return 0;
        }
        let Some(tables) = self.tables.try_lock() else {
            return 0;
        };
        Self::reclaimable_table_bytes(&tables)
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        if target_bytes == 0 || self.raw_payload_spill_requested.load(Ordering::Acquire) {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let Some(mut tables) = self.tables.try_lock() else {
            return Ok(ReclaimStats::empty(target_bytes));
        };
        let before = Self::reclaimable_table_bytes(&tables);
        if before == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        let mut state_spill = self.state_spill.lock();
        if state_spill.is_none() {
            *state_spill = Some(
                AggregateStateSpillBuffer::new(
                    Arc::clone(&self.buffer_pool),
                    self.group_types.clone(),
                    self.state_width,
                    self.state_encoding,
                    self.radix_bits,
                    self.memory.clone(),
                )
                .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?,
            );
        }
        let spill = state_spill
            .as_mut()
            .expect("aggregate state spill initialized above");
        let spill_bytes_before = spill.size_in_bytes();
        for table in tables.iter() {
            if table.count() > 0 {
                spill
                    .append_table(table)
                    .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
            }
        }
        let spilled = spill.size_in_bytes().saturating_sub(spill_bytes_before);

        for table in tables.iter_mut() {
            table
                .destroy()
                .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
        }
        tables.clear();
        self.raw_payload_spill_requested
            .store(true, Ordering::Release);
        Ok(ReclaimStats::new(
            target_bytes,
            before.saturating_sub(spilled),
            spilled,
        ))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

#[derive(Debug)]
pub struct AggregateFinalizedStateReclaimer {
    name: String,
    handle: Arc<AggregateHandle>,
    buffer_pool: Arc<BufferPool>,
    memory: MemoryAccountingContext,
}

impl AggregateFinalizedStateReclaimer {
    pub fn new(
        handle: Arc<AggregateHandle>,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            name: Self::name_for(&handle),
            handle,
            buffer_pool,
            memory,
        }
    }

    pub fn for_query(
        handle: Arc<AggregateHandle>,
        buffer_pool: Arc<BufferPool>,
        query_memory: Arc<crate::memory_runtime::QueryMemoryPool>,
    ) -> Self {
        let memory = MemoryAccountingContext::from_owner(
            query_memory,
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        Self::new(handle, buffer_pool, memory)
    }

    pub fn name_for(handle: &AggregateHandle) -> String {
        format!("aggregate_finalized_state:{}", handle.metadata().id.index())
    }
}

impl Reclaimer for AggregateFinalizedStateReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        self.handle.reclaimable_state_bytes()
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        self.handle
            .reclaim_state_memory(
                target_bytes,
                Arc::clone(&self.buffer_pool),
                self.memory.clone(),
            )
            .map_err(|err| {
                MemoryError::reclaim_failed(format!(
                    "aggregate finalized state reclaim failed: {err}"
                ))
            })
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

#[derive(Debug)]
pub enum AggregateRuntimeState {
    Hash(HashAggregateRuntimeState),
    Ungrouped(UngroupedAggregateRuntimeState),
    Perfect(PerfectHashAggregateRuntimeState),
}

impl AggregateRuntimeState {
    fn destroy(&mut self) -> Result<()> {
        match self {
            Self::Hash(state) => state.destroy(),
            Self::Ungrouped(state) => state.destroy(),
            Self::Perfect(state) => state.destroy(),
        }
    }

    fn reclaimable_finalized_memory(&self) -> usize {
        match self {
            Self::Hash(state) => state.reclaimable_finalized_memory(),
            Self::Ungrouped(_) => 0,
            Self::Perfect(state) => state.finalized_table.as_ref().map_or(
                0,
                FinalizedPerfectAggregateTable::reclaimable_finalized_memory,
            ),
        }
    }

    fn reclaimable_build_memory(&self) -> usize {
        match self {
            Self::Hash(state) => state.reclaimable_build_memory(),
            // Ungrouped state is constant-sized. Perfect state is a bounded,
            // planner-admitted direct-address table accounted as non-revocable;
            // neither advertises bytes that its reclaimer cannot release.
            Self::Ungrouped(_) | Self::Perfect(_) => 0,
        }
    }

    fn reclaim_build_memory(&mut self, target_bytes: usize) -> usize {
        match self {
            Self::Hash(state) => state.reclaim_build_memory(target_bytes),
            Self::Ungrouped(_) | Self::Perfect(_) => 0,
        }
    }

    fn reclaim_finalized_memory(
        &mut self,
        target_bytes: usize,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<ReclaimStats> {
        match self {
            Self::Hash(state) => state.reclaim_finalized_memory(target_bytes, buffer_pool, memory),
            Self::Ungrouped(_) => Ok(ReclaimStats::empty(target_bytes)),
            Self::Perfect(state) => {
                let reclaimed = state
                    .finalized_table
                    .as_mut()
                    .map_or(0, |table| table.reclaim_finalized_memory(target_bytes));
                Ok(ReclaimStats::new(target_bytes, reclaimed, 0))
            }
        }
    }
}

#[derive(Debug)]
pub struct HashAggregateRuntimeState {
    pub tables: Vec<AggregateHashTable>,
    pub(crate) distinct: DistinctAggregateState,
    pub(crate) spilled_payloads: Vec<AggregateSpilledPayload>,
    pub(crate) spilled_states: Vec<AggregateSpilledState>,
    pub spilled_outputs: Option<Vec<Option<AggregateSpilledOutput>>>,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
}

impl HashAggregateRuntimeState {
    fn destroy(&mut self) -> Result<()> {
        for table in &mut self.tables {
            table.destroy()?;
        }
        Ok(())
    }

    fn reclaimable_finalized_memory(&self) -> usize {
        self.tables
            .iter()
            .map(AggregateHashTable::reclaimable_finalized_memory)
            .sum()
    }

    fn reclaimable_build_memory(&self) -> usize {
        self.tables
            .iter()
            .map(AggregateHashTable::reclaimable_build_memory)
            .sum()
    }

    fn reclaim_build_memory(&mut self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }
        let mut reclaimed = 0usize;
        for table in &mut self.tables {
            if reclaimed >= target_bytes {
                break;
            }
            reclaimed =
                reclaimed.saturating_add(table.reclaim_build_memory(target_bytes - reclaimed));
        }
        reclaimed
    }

    fn reclaim_finalized_memory(
        &mut self,
        target_bytes: usize,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<ReclaimStats> {
        if target_bytes == 0 {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let mut reclaimed = 0usize;
        for table in &mut self.tables {
            if reclaimed >= target_bytes {
                break;
            }
            reclaimed =
                reclaimed.saturating_add(table.reclaim_finalized_memory(target_bytes - reclaimed));
        }
        let mut spilled = 0usize;
        if reclaimed < target_bytes {
            let spill_stats =
                self.spill_finalized_outputs(target_bytes - reclaimed, buffer_pool, memory)?;
            reclaimed = reclaimed.saturating_add(spill_stats.reclaimed_bytes);
            spilled = spilled.saturating_add(spill_stats.spilled_bytes);
        }
        Ok(ReclaimStats::new(target_bytes, reclaimed, spilled))
    }

    fn spill_finalized_outputs(
        &mut self,
        target_bytes: usize,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<ReclaimStats> {
        if self.tables.is_empty() || self.spilled_outputs.is_some() {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        let before = self
            .tables
            .iter()
            .map(AggregateHashTable::memory_usage)
            .sum::<usize>();
        let mut spilled = 0usize;
        let mut outputs = Vec::with_capacity(self.tables.len());
        for mut table in std::mem::take(&mut self.tables) {
            let output_types = table.scan_output_types();
            let format = AggregateGroupFormat::finalized_output(
                output_types.clone(),
                output_types.len().saturating_sub(table.aggregate_count()),
                table.aggregate_count(),
            );
            let mut writer = RowStoreSpillWriter::new(
                Arc::clone(&buffer_pool),
                format.clone(),
                MemoryTag::HashTable,
                memory.clone(),
            );
            let mut position = Default::default();
            let mut chunk = Chunk::try_initialize(&output_types, 1, table.allocator())?;
            while table.scan(&mut position, &mut chunk)? {
                writer.append_chunk(&chunk)?;
            }
            if writer.count() == 0 {
                outputs.push(None);
                continue;
            }
            let rows = writer.finish()?;
            spilled = spilled.saturating_add(rows.size_in_bytes());
            outputs.push(Some(AggregateSpilledOutput { format, rows }));
        }
        self.spilled_outputs = Some(outputs);
        let reclaimed = before.saturating_sub(spilled);
        Ok(ReclaimStats::new(target_bytes, reclaimed, spilled))
    }
}

#[derive(Debug)]
pub struct AggregateSpilledOutput {
    format: AggregateGroupFormat,
    rows: RowStore,
}

impl AggregateSpilledOutput {
    pub(crate) fn new(format: AggregateGroupFormat, rows: RowStore) -> Self {
        Self { format, rows }
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.rows.size_in_bytes()
    }

    pub fn into_reader(self) -> RowStoreSpillReader<AggregateGroupFormat> {
        RowStoreSpillReader::new(self.format, self.rows)
    }
}

#[derive(Debug)]
pub struct PerfectHashAggregateRuntimeState {
    pub(crate) build_table: Option<PerfectAggregateHashTable>,
    pub(crate) finalized_table: Option<FinalizedPerfectAggregateTable>,
    pub(crate) pending_tables: Vec<PerfectAggregateHashTable>,
}

impl PerfectHashAggregateRuntimeState {
    fn destroy(&mut self) -> Result<()> {
        if let Some(table) = self.build_table.as_mut() {
            table.destroy()?;
        }
        if let Some(table) = self.finalized_table.as_mut() {
            table.destroy()?;
        }
        for table in &mut self.pending_tables {
            table.destroy()?;
        }
        Ok(())
    }
}

pub struct UngroupedAggregateRuntimeState {
    pub aggregate_objects: std::sync::Arc<[AggregateObject]>,
    pub layout: AggregateStateLayout,
    pub aggregate_inputs: std::sync::Arc<[Vec<usize>]>,
    pub(crate) ordered_collectors: Vec<OrderedAggregateCollector>,
    pub(crate) distinct: DistinctAggregateState,
    pub state_buffer: Vec<u64>,
    pub arena_allocator: ArenaAllocator,
    pub destroyed: bool,
}

impl std::fmt::Debug for UngroupedAggregateRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UngroupedAggregateRuntimeState")
            .field("aggregate_count", &self.aggregate_objects.len())
            .field("ordered_aggregate_count", &self.ordered_collectors.len())
            .field("state_buffer_words", &self.state_buffer.len())
            .field("destroyed", &self.destroyed)
            .finish()
    }
}

impl UngroupedAggregateRuntimeState {
    pub fn base_ptr(&mut self) -> *mut u8 {
        self.state_buffer.as_mut_ptr() as *mut u8
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.destroyed {
            return Ok(());
        }
        let addresses = single_state_addresses(
            self.base_ptr(),
            self.arena_allocator.get_allocator().clone(),
        )?;
        let mut input_data = AggregateInputData::new(
            None,
            &mut self.arena_allocator,
            AggregateCombineType::PreserveInput,
        );
        destroy_states(&self.aggregate_objects, &mut input_data, &addresses, 1)?;
        self.destroyed = true;
        Ok(())
    }
}

pub fn single_state_addresses(
    base_ptr: *mut u8,
    allocator: std::sync::Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Vector> {
    let mut addresses = Vector::try_new(paro_common::types::LogicalType::BigInt, 1, allocator)?;
    addresses.set_count(1);
    unsafe {
        *addresses.flat_data_mut::<*mut u8>() = base_ptr;
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_common::allocator::MemoryTag;
    use paro_common::chunk::Chunk;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::types::LogicalType;
    use paro_common::vector::VECTOR_SIZE;
    use paro_storage::buffer::BufferPool;
    use paro_storage::row::RowSpillReader;

    use crate::memory_runtime::QueryMemoryPool;
    use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};

    fn metadata() -> BreakerHandleMetadata {
        BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind: BreakerHandleKind::Aggregate,
            row_type: RowType::new(vec!["k".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Vec::new().into_boxed_slice(),
            properties: PipelineProperties::default(),
        }
    }

    #[test]
    fn aggregate_build_compaction_reclaimer_reclaims_over_reserved_hash_storage() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator,
            memory,
        )
        .expect("aggregate table");
        let handle = Arc::new(AggregateHandle::new(metadata()));
        handle
            .initialize(AggregateRuntimeState::Hash(HashAggregateRuntimeState {
                tables: vec![table],
                distinct: Default::default(),
                spilled_payloads: Vec::new(),
                spilled_states: Vec::new(),
                spilled_outputs: None,
                ordered_collectors: Vec::new(),
            }))
            .expect("initialize aggregate");

        let reclaimer = AggregateBuildCompactionReclaimer::new(Arc::clone(&handle));
        let before = reclaimer.reclaimable_bytes();
        assert!(before > 0, "expected over-reserved build storage");
        let stats = reclaimer
            .reclaim_sync(before)
            .expect("reclaim build storage");
        assert!(stats.reclaimed_bytes > 0);
        assert_eq!(stats.spilled_bytes, 0);
        assert_eq!(reclaimer.reclaimable_bytes(), 0);

        handle.mark_finalized();
        assert_eq!(
            reclaimer.reclaimable_bytes(),
            0,
            "build owner must turn inert after finalize"
        );
    }

    #[test]
    fn aggregate_local_build_compaction_reclaimer_reclaims_over_reserved_hash_storage() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator,
            memory,
        )
        .expect("aggregate table");
        let tables = Arc::new(Mutex::new(vec![table]));
        let handle = AggregateHandle::new(metadata());
        let reclaimer =
            AggregateLocalBuildCompactionReclaimer::new(&handle, 7, Arc::clone(&tables));

        let before = reclaimer.reclaimable_bytes();
        assert!(before > 0, "expected local over-reserved build storage");
        let stats = reclaimer
            .reclaim_sync(before)
            .expect("reclaim local build storage");
        assert!(stats.reclaimed_bytes > 0);
        assert_eq!(stats.spilled_bytes, 0);
        assert_eq!(reclaimer.reclaimable_bytes(), 0);
    }

    #[test]
    fn aggregate_local_payload_spill_reclaimer_arms_empty_local_spill_mode() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator,
            memory,
        )
        .expect("aggregate table");
        let tables = Arc::new(Mutex::new(vec![table]));
        let request = Arc::new(AtomicBool::new(false));
        let handle = AggregateHandle::new(metadata());
        let reclaimer = AggregateLocalPayloadSpillReclaimer::new(
            &handle,
            9,
            Arc::clone(&tables),
            Arc::clone(&request),
        );
        let query_memory = QueryMemoryPool::unbounded();
        query_memory.register_reclaimer_once_by_name(Arc::new(reclaimer));

        let reclaimed = query_memory.reclaim(1).expect("query memory reclaim");
        assert!(
            reclaimed > 0,
            "expected empty table storage to be reclaimed"
        );
        assert!(
            request.load(Ordering::Acquire),
            "reclaim should request raw payload spill mode"
        );
        assert!(tables.lock().is_empty());
    }

    #[test]
    fn aggregate_local_payload_spill_reclaimer_ignores_non_empty_local_state() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let mut table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator.clone(),
            memory,
        )
        .expect("aggregate table");
        let groups = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[42],
                allocator,
            )],
            paro_common::test_utils::test_allocator(),
        );
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("insert group");
        assert_eq!(table.count(), 1);

        let tables = Arc::new(Mutex::new(vec![table]));
        let request = Arc::new(AtomicBool::new(false));
        let handle = AggregateHandle::new(metadata());
        let reclaimer = AggregateLocalPayloadSpillReclaimer::new(
            &handle,
            10,
            Arc::clone(&tables),
            Arc::clone(&request),
        );
        let query_memory = QueryMemoryPool::unbounded();
        query_memory.register_reclaimer_once_by_name(Arc::new(reclaimer));

        let reclaimed = query_memory.reclaim(1).expect("query memory reclaim");
        assert_eq!(reclaimed, 0);
        assert!(
            !request.load(Ordering::Acquire),
            "non-empty local state must not be switched to raw payload spill"
        );
        assert_eq!(
            tables
                .lock()
                .iter()
                .map(AggregateHashTable::count)
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn aggregate_local_state_spill_reclaimer_spills_non_empty_local_state() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let mut table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator.clone(),
            memory.clone(),
        )
        .expect("aggregate table");
        let groups = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[7],
                allocator,
            )],
            paro_common::test_utils::test_allocator(),
        );
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("insert group");

        let tables = Arc::new(Mutex::new(vec![table]));
        let state_spill = Arc::new(Mutex::new(None));
        let request = Arc::new(AtomicBool::new(false));
        let handle = AggregateHandle::new(metadata());
        let reclaimer = AggregateLocalStateSpillReclaimer::new(
            &handle,
            11,
            Arc::clone(&tables),
            Arc::clone(&state_spill),
            Arc::clone(&request),
            BufferPool::new_arc(16 * 1024 * 1024),
            vec![LogicalType::Integer],
            0,
            AggregateStateEncoding::RawBytes,
            1,
            memory,
        );
        let before = reclaimer.reclaimable_bytes();
        assert!(before > 0);

        let stats = reclaimer
            .reclaim_sync(before)
            .expect("spill local aggregate state");
        assert!(stats.reclaimed_bytes > 0);
        assert!(request.load(Ordering::Acquire));
        assert!(tables.lock().is_empty());
        assert_eq!(
            state_spill
                .lock()
                .as_ref()
                .expect("state spill buffer")
                .count(),
            1
        );
    }

    #[test]
    fn aggregate_finalized_state_reclaimer_reclaims_hash_table_storage() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let mut table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator.clone(),
            memory.clone(),
        )
        .expect("aggregate table");
        let keys = (0..4096).map(|idx| (idx % 8) as i32).collect::<Vec<_>>();
        let groups = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &keys,
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("insert groups");
        assert_eq!(table.count(), 8);

        let handle = Arc::new(AggregateHandle::new(metadata()));
        handle
            .initialize(AggregateRuntimeState::Hash(HashAggregateRuntimeState {
                tables: vec![table],
                distinct: Default::default(),
                spilled_payloads: Vec::new(),
                spilled_states: Vec::new(),
                spilled_outputs: None,
                ordered_collectors: Vec::new(),
            }))
            .expect("initialize aggregate");
        let reclaimer = AggregateFinalizedStateReclaimer::new(
            Arc::clone(&handle),
            BufferPool::new_arc(64 * 1024 * 1024),
            memory,
        );
        assert_eq!(reclaimer.reclaimable_bytes(), 0);

        handle.mark_finalized();
        handle.enable_state_reclaim();
        let before = reclaimer.reclaimable_bytes();
        assert!(
            before > 0,
            "expected reclaimable finalized aggregate storage"
        );
        let stats = reclaimer
            .reclaim_sync(1)
            .expect("compact finalized aggregate state");
        assert!(stats.reclaimed_bytes > 0);
        assert_eq!(stats.spilled_bytes, 0);
        assert_eq!(reclaimer.reclaimable_bytes(), 0);

        handle
            .with_state_mut(|state| {
                let AggregateRuntimeState::Hash(state) = state else {
                    return Err(paro_error::internal("expected hash aggregate state"));
                };
                assert_eq!(state.tables[0].count(), 8);
                Ok(())
            })
            .expect("state remains readable");
    }

    #[test]
    fn aggregate_finalized_state_reclaimer_spills_hash_output() {
        let allocator = paro_common::test_utils::test_allocator();
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let mut table = AggregateHashTable::new_flat_with_memory(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator.clone(),
            memory.clone(),
        )
        .expect("aggregate table");
        let values = (0..64).collect::<Vec<i32>>();
        let groups = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &values,
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("insert groups");

        let handle = Arc::new(AggregateHandle::new(metadata()));
        handle
            .initialize(AggregateRuntimeState::Hash(HashAggregateRuntimeState {
                tables: vec![table],
                distinct: Default::default(),
                spilled_payloads: Vec::new(),
                spilled_states: Vec::new(),
                spilled_outputs: None,
                ordered_collectors: Vec::new(),
            }))
            .expect("initialize aggregate");
        handle.mark_finalized();
        handle.enable_state_reclaim();

        let reclaimer = AggregateFinalizedStateReclaimer::new(
            Arc::clone(&handle),
            BufferPool::new_arc(64 * 1024 * 1024),
            memory,
        );
        let stats = reclaimer
            .reclaim_sync(reclaimer.reclaimable_bytes().saturating_add(1))
            .expect("spill finalized aggregate output");
        assert!(stats.spilled_bytes > 0);

        handle
            .with_state_mut(|state| {
                let AggregateRuntimeState::Hash(state) = state else {
                    return Err(paro_error::internal("expected hash aggregate state"));
                };
                assert!(state.tables.is_empty());
                let outputs = state
                    .spilled_outputs
                    .as_mut()
                    .expect("spilled aggregate outputs");
                let output = outputs[0].take().expect("first grouping set output");
                let mut reader = output.into_reader();
                let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
                    &[LogicalType::Integer],
                    VECTOR_SIZE,
                );
                let scanned = reader.read_next(&mut chunk)?;
                assert_eq!(scanned, values.len());
                let mut actual = (0..chunk.size())
                    .map(
                        |row| match chunk.column(0).expect("group column").get_value(row) {
                            paro_common::runtime_value::Value::Integer(value) => value,
                            other => panic!("unexpected spilled group value: {other:?}"),
                        },
                    )
                    .collect::<Vec<_>>();
                actual.sort_unstable();
                assert_eq!(actual, values);
                Ok(())
            })
            .expect("spilled output remains readable");
    }
}
