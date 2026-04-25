// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Recursive CTE Operator
//!
//!
//! ## Design Notes
//! - Acts as both Sink and Source
//! - Anchor term is executed once through a child MetaPipeline
//! - Recursive term is executed iteratively by re-scheduling a dedicated MetaPipeline
//! - `working_table` stores the previous iteration result
//! - `intermediate_table` stores newly produced rows for current iteration

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::{Allocator, BufferAllocator, BufferManager, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedHashSet, MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryError,
    MemoryGrant, MemoryOwner, MemoryOwnerAllocator, MemoryResult,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_storage::buffer::BufferPool;
use paro_storage::column::ChunkManagementState;

use crate::execution_context::ExecutionContext;
use crate::memory_runtime::{ReclaimStats, Reclaimer, RetainedChunkVec, SpillCost};
use crate::operator::set::cte::{cte_memory_context, CteWorkingTable};
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;
use crate::query_executor::executor::Executor;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};

fn cte_metadata_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::Metadata,
        MemoryAccountingClass::Metadata,
    )
}

#[cfg(test)]
fn detached_table_memory_context() -> MemoryAccountingContext {
    MemoryAccountingContext::detached(MemoryTag::ColumnData, MemoryAccountingClass::Revocable)
}

#[cfg(test)]
fn detached_metadata_memory_context() -> MemoryAccountingContext {
    MemoryAccountingContext::detached(MemoryTag::Metadata, MemoryAccountingClass::Metadata)
}

fn grant_for_context(memory: &MemoryAccountingContext) -> MemoryGrant {
    if let Some(owner) = memory.owner() {
        MemoryGrant::new(0, memory.domain(), owner)
            .expect("zero-byte recursive CTE grant should fit")
    } else {
        MemoryGrant::detached(usize::MAX / 4, memory.domain())
    }
}

fn new_distinct_hash_set(memory: &MemoryAccountingContext) -> AccountedHashSet<u64> {
    AccountedHashSet::new_with_accounting(
        grant_for_context(memory),
        memory.tag(),
        memory.accounting_class(),
    )
}

#[derive(Debug)]
struct RecursiveCteReclaimer {
    working_table: Arc<CteWorkingTable>,
    intermediate_table: Arc<CteWorkingTable>,
    distinct_table: Arc<CteWorkingTable>,
}

impl Reclaimer for RecursiveCteReclaimer {
    fn name(&self) -> &str {
        "recursive_cte_tables"
    }

    fn reclaimable_bytes(&self) -> usize {
        self.intermediate_table
            .reclaimable_bytes()
            .saturating_add(self.working_table.reclaimable_bytes())
            .saturating_add(self.distinct_table.reclaimable_bytes())
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        let mut reclaimed = 0usize;
        for table in [
            &self.intermediate_table,
            &self.working_table,
            &self.distinct_table,
        ] {
            if reclaimed >= target_bytes {
                break;
            }
            let remaining = target_bytes - reclaimed;
            let table_reclaimed = table
                .reclaim(remaining)
                .map_err(|err| MemoryError::reclaim_failed(err.to_string()))?;
            reclaimed = reclaimed.saturating_add(table_reclaimed);
        }
        Ok(ReclaimStats::new(target_bytes, reclaimed, reclaimed))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

/// Physical recursive CTE operator.
///
/// The operator is both:
/// - Sink: receives rows from anchor/recursive branches
/// - Source: emits rows and drives recursive re-execution
#[derive(Debug)]
pub struct RecursiveCTE {
    /// CTE name (for debugging).
    pub cte_name: String,
    /// CTE index (unique identifier).
    pub cte_index: usize,
    /// Output types.
    pub types: Vec<LogicalType>,
    /// Whether this is UNION ALL (true) or UNION DISTINCT (false).
    pub union_all: bool,
    /// Anchor query child.
    pub anchor: Arc<dyn PhysicalOperator>,
    /// Recursive query child.
    pub recursive: Arc<dyn PhysicalOperator>,
    /// Delta table visible to recursive term scans.
    pub working_table: Arc<CteWorkingTable>,

    /// Newly produced rows of the current step.
    intermediate_table: Arc<CteWorkingTable>,
    /// Spillable seen-state for UNION DISTINCT semantics.
    distinct_table: Arc<CteWorkingTable>,
    /// Advisory fingerprint index to avoid scanning the spillable seen-state for unique rows.
    distinct_row_hashes: Mutex<Option<AccountedHashSet<u64>>>,
    /// Serializes UNION DISTINCT dedup against the spillable seen-state.
    distinct_lock: Mutex<()>,
    /// Query-pool callback that spills recursive CTE tables under memory pressure.
    reclaimer: Mutex<Option<Arc<dyn Reclaimer>>>,
    /// Scratch scan allocation binding for distinct-table probes.
    scan_allocation: Mutex<Option<(Arc<BufferPool>, MemoryAccountingContext)>>,
    /// Dedicated recursive MetaPipeline tree (not part of root schedule).
    recursive_meta_pipeline: Mutex<Option<Arc<MetaPipeline>>>,
    /// Tracks lifecycle of one recursive execution.
    execution_initialized: AtomicBool,
    /// Number of productive recursive rounds completed by the recursive term.
    productive_iterations: AtomicUsize,
}

impl RecursiveCTE {
    /// Create a new `RecursiveCTE`.
    pub fn new(
        cte_name: String,
        cte_index: usize,
        types: Vec<LogicalType>,
        union_all: bool,
        anchor: Arc<dyn PhysicalOperator>,
        recursive: Arc<dyn PhysicalOperator>,
        working_table: Arc<CteWorkingTable>,
    ) -> Self {
        Self {
            cte_name,
            cte_index,
            types: types.clone(),
            union_all,
            anchor,
            recursive,
            working_table,
            intermediate_table: Arc::new(CteWorkingTable::new(types.clone())),
            distinct_table: Arc::new(CteWorkingTable::new(types.clone())),
            distinct_row_hashes: Mutex::new(None),
            distinct_lock: Mutex::new(()),
            reclaimer: Mutex::new(None),
            scan_allocation: Mutex::new(None),
            recursive_meta_pipeline: Mutex::new(None),
            execution_initialized: AtomicBool::new(false),
            productive_iterations: AtomicUsize::new(0),
        }
    }

    /// Get the recursive MetaPipeline if it has been built.
    pub fn recursive_meta_pipeline(&self) -> Option<Arc<MetaPipeline>> {
        self.recursive_meta_pipeline
            .lock()
            .ok()
            .and_then(|m| m.as_ref().cloned())
    }

    fn prepare_tables_with_memory(
        &self,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        table_memory: MemoryAccountingContext,
        metadata_memory: MemoryAccountingContext,
    ) -> Result<()> {
        self.working_table
            .prepare_for_execution(Arc::clone(&buffer_pool), table_memory.clone())?;
        self.intermediate_table
            .prepare_for_execution(Arc::clone(&buffer_pool), table_memory.clone())?;
        self.distinct_table
            .prepare_for_execution(Arc::clone(&buffer_pool), table_memory.clone())?;
        *self.scan_allocation.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE scan allocation: {}",
                e
            ))
        })? = Some((buffer_pool, table_memory.clone()));
        let mut hashes = self.distinct_row_hashes.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE distinct hashes: {}",
                e
            ))
        })?;
        if hashes.is_none() {
            *hashes = Some(new_distinct_hash_set(&metadata_memory));
        }
        Ok(())
    }

    fn scan_allocator(&self) -> Result<Arc<dyn Allocator>> {
        let guard = self.scan_allocation.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE scan allocation: {}",
                e
            ))
        })?;
        let (buffer_pool, memory) = guard.as_ref().ok_or_else(|| {
            paro_error::internal("Recursive CTE tables have not been prepared".to_string())
        })?;
        let inner: Arc<dyn Allocator> = Arc::new(BufferAllocator::new(
            buffer_pool.clone() as Arc<dyn BufferManager>,
            memory.tag(),
        ));
        if let Some(owner) = memory.owner() {
            Ok(Arc::new(MemoryOwnerAllocator::new(
                inner,
                owner,
                memory.domain(),
                memory.tag(),
                memory.accounting_class(),
            )))
        } else {
            Ok(inner)
        }
    }

    fn prepare_tables(&self, ctx: &ExecutionContext) -> Result<()> {
        self.prepare_tables_with_memory(
            ctx.buffer_pool().clone(),
            cte_memory_context(ctx),
            cte_metadata_memory_context(ctx),
        )?;
        self.register_reclaimer(ctx)
    }

    fn reset_tables_for_execution(&self, ctx: &ExecutionContext) -> Result<()> {
        let table_memory = cte_memory_context(ctx);
        self.working_table
            .reset_with_memory(ctx.buffer_pool().clone(), table_memory.clone())?;
        self.intermediate_table
            .reset_with_memory(ctx.buffer_pool().clone(), table_memory.clone())?;
        self.distinct_table
            .reset_with_memory(ctx.buffer_pool().clone(), table_memory)?;
        let metadata_memory = cte_metadata_memory_context(ctx);
        let mut hashes = self.distinct_row_hashes.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE distinct hashes: {}",
                e
            ))
        })?;
        *hashes = Some(new_distinct_hash_set(&metadata_memory));
        Ok(())
    }

    fn register_reclaimer(&self, ctx: &ExecutionContext) -> Result<()> {
        let mut slot = self.reclaimer.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock recursive CTE reclaimer: {}", e))
        })?;
        let reclaimer = slot.get_or_insert_with(|| {
            Arc::new(RecursiveCteReclaimer {
                working_table: Arc::clone(&self.working_table),
                intermediate_table: Arc::clone(&self.intermediate_table),
                distinct_table: Arc::clone(&self.distinct_table),
            }) as Arc<dyn Reclaimer>
        });
        ctx.query_memory_pool()
            .register_reclaimer(reclaimer.clone());
        Ok(())
    }

    fn row_values(chunk: &Chunk, row_idx: usize) -> Vec<Value> {
        chunk
            .data
            .iter()
            .map(|col| col.get_value(row_idx))
            .collect()
    }

    fn row_fingerprint(row_values: &[Value]) -> u64 {
        let mut hasher = DefaultHasher::new();
        row_values.hash(&mut hasher);
        hasher.finish()
    }

    fn chunk_contains_row(chunk: &Chunk, row_values: &[Value]) -> bool {
        (0..chunk.size()).any(|row_idx| Self::row_values(chunk, row_idx) == row_values)
    }

    fn distinct_table_contains_row(&self, row_values: &[Value]) -> Result<bool> {
        let storage_indexes = self.distinct_table.storage_indexes()?;
        if storage_indexes.is_empty() {
            return Ok(false);
        }

        let mut scan_state = ChunkManagementState::new();
        let allocator = self.scan_allocator()?;
        let mut chunk = Chunk::try_init_empty(&self.types, allocator)?;
        for storage_index in storage_indexes {
            self.distinct_table.fetch_chunk_by_storage_index(
                storage_index,
                &mut scan_state,
                &mut chunk,
            )?;
            if Self::chunk_contains_row(&chunk, row_values) {
                scan_state.clear();
                return Ok(true);
            }
            scan_state.clear();
        }
        Ok(false)
    }

    fn copy_rows(chunk: &Chunk, rows: &[usize]) -> Result<Chunk> {
        let types = chunk.types();
        let mut result = Chunk::try_initialize(&types, rows.len(), chunk.allocator().clone())?;
        result.set_cardinality(rows.len());

        for (col_idx, source_col) in chunk.data.iter().enumerate() {
            let out_col = result
                .column_mut(col_idx)
                .expect("output column index should be valid");
            for (out_row, source_row) in rows.iter().copied().enumerate() {
                out_col.copy_at(out_row, source_col, source_row);
            }
        }
        Ok(result)
    }

    fn append_chunk_to_intermediate(&self, chunk: Chunk) -> Result<usize> {
        if chunk.size() == 0 {
            return Ok(0);
        }

        if self.union_all {
            let row_count = chunk.size();
            self.intermediate_table.append(&chunk)?;
            return Ok(row_count);
        }

        let _guard = self.distinct_lock.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE distinct state: {}",
                e
            ))
        })?;
        let mut distinct_row_hashes_guard = self.distinct_row_hashes.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE distinct row hashes: {}",
                e
            ))
        })?;
        let distinct_row_hashes = distinct_row_hashes_guard.as_mut().ok_or_else(|| {
            paro_error::internal(
                "Recursive CTE distinct row hashes have not been initialized".to_string(),
            )
        })?;

        let mut local_seen = HashSet::new();
        let mut new_rows = Vec::new();
        for row in 0..chunk.size() {
            let key = Self::row_values(&chunk, row);
            if !local_seen.insert(key.clone()) {
                continue;
            }
            let fingerprint = Self::row_fingerprint(&key);
            let hash_seen = !distinct_row_hashes
                .try_insert(fingerprint)
                .map_err(paro_error::ParoError::from)?;
            if !hash_seen || !self.distinct_table_contains_row(&key)? {
                new_rows.push(row);
            }
        }

        if new_rows.is_empty() {
            return Ok(0);
        }

        let filtered = Self::copy_rows(&chunk, &new_rows)?;
        let row_count = filtered.size();
        self.intermediate_table.append(&filtered)?;
        self.distinct_table.append(&filtered)?;
        Ok(row_count)
    }

    fn move_intermediate_to_working_table(&self) -> Result<bool> {
        self.working_table.replace_with(&self.intermediate_table)
    }

    fn reset_recursive_pipeline_states(&self, recursive_meta: &Arc<MetaPipeline>) {
        let pipelines = recursive_meta.get_pipelines_recursive();
        for pipeline in pipelines {
            pipeline.clear_runtime_states();
        }
    }

    fn execute_recursive_pipelines(&self, ctx: &ExecutionContext) -> Result<()> {
        let recursive_meta = {
            let guard = self.recursive_meta_pipeline.lock().map_err(|e| {
                paro_error::internal(format!("Failed to lock recursive meta pipeline: {}", e))
            })?;
            guard.clone().ok_or_else(|| {
                paro_error::internal(
                    "Missing recursive meta pipeline for recursive CTE".to_string(),
                )
            })?
        };

        // Ensure recursive branch pipelines are ready and reset for this round.
        recursive_meta.ready();
        self.reset_recursive_pipeline_states(&recursive_meta);

        // Re-schedule recursive branch and run it to completion.
        let executor = Executor::new(ctx.session.clone());
        executor
            .execute_meta_pipelines_blocking(recursive_meta.get_meta_pipelines_recursive(true))?;
        Ok(())
    }

    fn prepare_next_iteration(&self, ctx: &ExecutionContext) -> Result<bool> {
        // Move the just-consumed delta to working table for recursive term scans.
        if !self.move_intermediate_to_working_table()? {
            return Ok(false);
        }

        // Execute one recursive step.
        self.execute_recursive_pipelines(ctx)?;

        // Check if recursive step produced new rows.
        let has_new_rows = self.intermediate_table.chunk_count() > 0;
        if has_new_rows {
            self.productive_iterations.fetch_add(1, Ordering::Relaxed);
        }
        Ok(has_new_rows)
    }
}

// ========== States ==========

#[derive(Debug, Default)]
struct RecursiveCteGlobalSinkState {
    total_rows: AtomicUsize,
}

impl GlobalSinkState for RecursiveCteGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct RecursiveCteLocalSinkState {
    local_chunks: RetainedChunkVec,
}

impl RecursiveCteLocalSinkState {
    fn new(memory: MemoryAccountingContext) -> Self {
        Self {
            local_chunks: RetainedChunkVec::new(memory),
        }
    }
}

impl LocalSinkState for RecursiveCteLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct RecursiveCteGlobalSourceState;

impl GlobalSourceState for RecursiveCteGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct RecursiveCteLocalSourceState {
    storage_indexes: Vec<usize>,
    current_chunk: usize,
    scan_state: ChunkManagementState,
}

impl LocalSourceState for RecursiveCteLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== PhysicalOperator ==========

impl PhysicalOperator for RecursiveCTE {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::RecursiveCte
    }

    fn explain_params(&self) -> Vec<String> {
        vec![
            format!("CTE Name: {}", self.cte_name),
            format!(
                "Set Operation: {}",
                if self.union_all { "UNION ALL" } else { "UNION" }
            ),
        ]
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn children_count(&self) -> usize {
        2
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        match index {
            0 => Some(self.anchor.as_ref()),
            1 => Some(self.recursive.as_ref()),
            _ => None,
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        match index {
            0 => Some(self.anchor.clone()),
            1 => Some(self.recursive.clone()),
            _ => None,
        }
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        self.prepare_tables(ctx)?;
        if !self.execution_initialized.swap(true, Ordering::SeqCst) {
            self.reset_tables_for_execution(ctx)?;
            self.productive_iterations.store(0, Ordering::Relaxed);
        }
        Ok(Box::new(RecursiveCteGlobalSinkState::default()))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(RecursiveCteLocalSinkState::new(
            cte_memory_context(ctx),
        )))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        if chunk.size() == 0 {
            return Ok(SinkResultType::NeedMoreInput);
        }

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<RecursiveCteLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        lstate.local_chunks.push(chunk.clone())?;
        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<RecursiveCteLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<RecursiveCteGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        let mut appended = 0usize;
        for chunk in lstate.local_chunks.drain_chunks() {
            appended += self.append_chunk_to_intermediate(chunk)?;
        }
        gstate.total_rows.fetch_add(appended, Ordering::Relaxed);

        Ok(SinkCombineResultType::Finished)
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        self.prepare_tables(ctx)?;
        Ok(Box::new(RecursiveCteGlobalSourceState))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(RecursiveCteLocalSourceState::default()))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<RecursiveCteLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        loop {
            if lstate.storage_indexes.is_empty() && lstate.current_chunk == 0 {
                lstate.storage_indexes = self.intermediate_table.storage_indexes()?;
            }

            if let Some(storage_index) = lstate.storage_indexes.get(lstate.current_chunk).copied() {
                self.intermediate_table.fetch_chunk_by_storage_index(
                    storage_index,
                    &mut lstate.scan_state,
                    chunk,
                )?;
                chunk.flatten();
                lstate.scan_state.clear();
                lstate.current_chunk += 1;
                return Ok(SourceResultType::HaveMoreOutput);
            }

            // Current round exhausted: trigger next recursive round.
            // Release the pinned chunk from the just-finished round before we
            // reset/swap the spill-backed working tables for the next round.
            lstate.scan_state.clear();
            if !self.prepare_next_iteration(ctx)? {
                self.execution_initialized.store(false, Ordering::SeqCst);
                chunk.set_cardinality(0);
                return Ok(SourceResultType::Finished);
            }

            lstate.storage_indexes = self.intermediate_table.storage_indexes()?;
            lstate.current_chunk = 0;
        }
    }

    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        self.execution_initialized.store(false, Ordering::SeqCst);
        let _ = self.working_table.reset();
        let _ = self.intermediate_table.reset();
        let _ = self.distinct_table.reset();
        if let Ok(mut hashes) = self.distinct_row_hashes.lock() {
            *hashes = None;
        }
        self.productive_iterations.store(0, Ordering::Relaxed);
        if let Ok(mut slot) = self.recursive_meta_pipeline.lock() {
            *slot = None;
        }

        // The current pipeline reads from RecursiveCTE source.
        state.set_pipeline_source(current, self_arc.clone());

        // Build anchor side as a regular child MetaPipeline feeding this sink.
        let anchor_meta = meta_pipeline.create_child_meta_pipeline(
            current,
            self_arc.clone(),
            MetaPipelineType::Regular,
        );
        anchor_meta.build(&self.anchor, state);

        // Build recursive side as an independent recursive MetaPipeline.
        let recursive_meta = MetaPipeline::new(Some(self_arc.clone()), MetaPipelineType::Regular);
        recursive_meta.set_recursive_cte();
        recursive_meta.build(&self.recursive, state);
        recursive_meta.ready();

        if let Ok(mut slot) = self.recursive_meta_pipeline.lock() {
            *slot = Some(recursive_meta);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl RecursiveCTE {
    pub fn productive_iterations(&self) -> usize {
        self.productive_iterations.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{detached_metadata_memory_context, detached_table_memory_context, RecursiveCTE};
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_storage::buffer::BufferPool;

    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::set::cte::CteWorkingTable;
    use crate::operator::PhysicalOperator;
    use crate::pipeline::build_state::PipelineBuildState;
    use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};

    fn make_unique_large_string_chunk(prefix: &str, start: usize, rows: usize) -> Chunk {
        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Varchar], rows);
        chunk.set_cardinality(rows);
        for row in 0..rows {
            chunk
                .column_mut(0)
                .expect("varchar column should exist")
                .set_string(row, &format!("{prefix}_{:04}", start + row));
        }
        chunk
    }

    fn create_spill_test_pool(prefix: &str) -> (Arc<BufferPool>, std::path::PathBuf) {
        let pool = BufferPool::new_arc(1024 * 1024);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("{prefix}_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .expect("set temp directory");
        (pool, temp_dir)
    }

    #[test]
    fn build_creates_recursive_meta_pipeline() {
        let anchor: Arc<dyn PhysicalOperator> =
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]));
        let recursive: Arc<dyn PhysicalOperator> =
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]));

        let op = Arc::new(RecursiveCTE::new(
            "r".to_string(),
            1,
            vec![LogicalType::Integer],
            true,
            anchor,
            recursive,
            Arc::new(CteWorkingTable::new(vec![LogicalType::Integer])),
        ));

        let root_meta = MetaPipeline::new(None, MetaPipelineType::Regular);
        let mut state = PipelineBuildState::new();
        let op_arc: Arc<dyn PhysicalOperator> = op.clone();
        root_meta.build(&op_arc, &mut state);

        let recursive_meta = op
            .recursive_meta_pipeline()
            .expect("recursive meta pipeline should be created");
        assert!(recursive_meta.has_recursive_cte());
        assert!(!recursive_meta.pipelines().is_empty());
    }

    #[test]
    fn distinct_mode_filters_duplicates() {
        let anchor: Arc<dyn PhysicalOperator> =
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]));
        let recursive: Arc<dyn PhysicalOperator> =
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]));

        let op = RecursiveCTE::new(
            "r".to_string(),
            1,
            vec![LogicalType::Integer],
            false, // UNION DISTINCT
            anchor,
            recursive,
            Arc::new(CteWorkingTable::new(vec![LogicalType::Integer])),
        );
        let (pool, temp_dir) = create_spill_test_pool("paro_recursive_cte_distinct");
        op.prepare_tables_with_memory(
            pool.clone(),
            detached_table_memory_context(),
            detached_metadata_memory_context(),
        )
        .expect("prepare recursive CTE tables");

        let mut input =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 4);
        input.set_cardinality(4);
        let col = input.column_mut(0).expect("integer column should exist");
        col.set_value(0, &Value::Integer(1));
        col.set_value(1, &Value::Integer(1));
        col.set_value(2, &Value::Integer(2));
        col.set_value(3, &Value::Integer(2));

        let appended = op
            .append_chunk_to_intermediate(input)
            .expect("append should succeed");
        assert_eq!(appended, 2);
        assert_eq!(op.intermediate_table.row_count(), 2);
        assert_eq!(op.distinct_table.row_count(), 2);

        let _ = pool.set_temporary_directory(String::new());
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn distinct_state_can_spill() {
        let anchor: Arc<dyn PhysicalOperator> =
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Varchar]));
        let recursive: Arc<dyn PhysicalOperator> =
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Varchar]));

        let op = RecursiveCTE::new(
            "r".to_string(),
            1,
            vec![LogicalType::Varchar],
            false,
            anchor,
            recursive,
            Arc::new(CteWorkingTable::new(vec![LogicalType::Varchar])),
        );
        let (pool, temp_dir) = create_spill_test_pool("paro_recursive_cte_seen");
        op.prepare_tables_with_memory(
            pool.clone(),
            detached_table_memory_context(),
            detached_metadata_memory_context(),
        )
        .expect("prepare recursive CTE tables");

        let payload = "recursive_spill_payload_".repeat(256);
        for chunk_idx in 0..24 {
            op.append_chunk_to_intermediate(make_unique_large_string_chunk(
                &payload,
                chunk_idx * 8,
                8,
            ))
            .expect("append distinct spill chunk");
            op.intermediate_table
                .reset()
                .expect("reset intermediate table");
        }

        assert!(op.distinct_table.row_count() > 0);
        assert!(
            pool.get_temporary_spill_metrics().write_bytes > 0,
            "expected recursive distinct state to spill"
        );

        let _ = pool.set_temporary_directory(String::new());
        let _ = fs::remove_dir_all(temp_dir);
    }
}
