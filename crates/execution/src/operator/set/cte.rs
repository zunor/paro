// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical CTE Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ Uses ExecutionContext allocator
//! - MetaPipeline: ✅ Uses for pipeline construction
//!
//! ## Known Limitations
//! - No parallel sink support for MVP
//! - No recursive CTE support
//!
//! ## Design Notes
//! CTE materializes the CTE query results into a shared ColumnDataCollection.
//! The CTE query is executed first (as a child pipeline), and the results are stored.
//! Then the main query can reference the CTE via CteScan operators.
//!
//! Pipeline structure:
//! - Child MetaPipeline: CTE query -> CTE (sink)
//! - Current Pipeline: CteScan (source) -> main query operators

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::types::LogicalType;
use paro_storage::buffer::BufferPool;
use paro_storage::column::{ChunkManagementState, ColumnDataAllocatorType, ColumnDataCollection};

use crate::execution_context::ExecutionContext;
use crate::memory_runtime::RetainedChunkVec;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{SinkCombineResultType, SinkResultType, SourceResultType};
use paro_planner::binder::ir::CTEMaterialize;

pub(crate) fn cte_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::ColumnData,
        MemoryAccountingClass::Revocable,
    )
}

/// Shared storage for CTE materialized results.
///
/// This is shared between CTE (writer) and CteScan (readers).
#[derive(Debug)]
pub struct CteWorkingTable {
    /// Column types of the CTE.
    pub types: Vec<LogicalType>,
    /// Buffer-managed materialized rows.
    collection: Mutex<Option<ColumnDataCollection>>,
}

impl CteWorkingTable {
    /// Create a new empty working table.
    pub fn new(types: Vec<LogicalType>) -> Self {
        Self {
            types,
            collection: Mutex::new(None),
        }
    }

    /// Prepare the working table for execution with the active buffer pool.
    pub fn prepare_for_execution(
        &self,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<()> {
        let mut guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;
        if guard.is_none() {
            *guard = Some(ColumnDataCollection::with_buffer_pool_and_memory(
                buffer_pool,
                self.types.clone(),
                MemoryTag::ColumnData,
                ColumnDataAllocatorType::BufferManagerAllocator,
                memory,
            ));
        } else if guard
            .as_ref()
            .map(|collection| collection.count() == 0)
            .unwrap_or(false)
        {
            guard
                .as_mut()
                .expect("collection should exist")
                .set_memory_context(memory)?;
        }
        Ok(())
    }

    /// Reset the table and bind future appends to a fresh execution memory owner.
    pub fn reset_with_memory(
        &self,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<()> {
        let mut guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;
        if guard.is_none() {
            *guard = Some(ColumnDataCollection::with_buffer_pool_and_memory(
                buffer_pool,
                self.types.clone(),
                MemoryTag::ColumnData,
                ColumnDataAllocatorType::BufferManagerAllocator,
                memory,
            ));
            return Ok(());
        }

        let collection = guard.as_mut().expect("collection should exist");
        collection.reset()?;
        collection.set_memory_context(memory)?;
        Ok(())
    }

    fn with_collection<T>(&self, f: impl FnOnce(&ColumnDataCollection) -> Result<T>) -> Result<T> {
        let guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;
        let collection = guard.as_ref().ok_or_else(|| {
            paro_error::internal("CTE working table has not been initialized".to_string())
        })?;
        f(collection)
    }

    fn with_collection_mut<T>(
        &self,
        f: impl FnOnce(&mut ColumnDataCollection) -> Result<T>,
    ) -> Result<T> {
        let mut guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;
        let collection = guard.as_mut().ok_or_else(|| {
            paro_error::internal("CTE working table has not been initialized".to_string())
        })?;
        f(collection)
    }

    /// Reset the working table (clear all data).
    pub fn reset(&self) -> Result<()> {
        let mut guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;
        if let Some(collection) = guard.as_mut() {
            collection.reset()?;
        }
        Ok(())
    }

    pub fn reclaim(&self, target_bytes: usize) -> Result<usize> {
        let Ok(guard) = self.collection.try_lock() else {
            return Ok(0);
        };
        let Some(collection) = guard.as_ref() else {
            return Ok(0);
        };
        collection.reclaim(target_bytes)
    }

    pub fn reclaimable_bytes(&self) -> usize {
        let Ok(guard) = self.collection.try_lock() else {
            return 0;
        };
        guard
            .as_ref()
            .map(ColumnDataCollection::resident_bytes)
            .unwrap_or(0)
    }

    /// Append a chunk to the working table.
    pub fn append(&self, chunk: &Chunk) -> Result<()> {
        let mut owned = chunk.clone();
        owned.try_flatten()?;
        self.with_collection_mut(|collection| collection.append_chunk(&owned))
    }

    /// Get the number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.collection
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(ColumnDataCollection::chunk_count))
            .unwrap_or(0)
    }

    /// Snapshot chunk storage indexes for source scans.
    pub fn storage_indexes(&self) -> Result<Vec<usize>> {
        self.with_collection(|collection| Ok(collection.chunk_storage_indexes()))
    }

    /// Fetch a chunk by storage index.
    pub fn fetch_chunk_by_storage_index(
        &self,
        storage_index: usize,
        state: &mut ChunkManagementState,
        output: &mut Chunk,
    ) -> Result<usize> {
        let column_ids = (0..self.types.len()).collect::<Vec<_>>();
        self.with_collection(|collection| {
            collection.fetch_chunk_by_storage_index(storage_index, &column_ids, state, output)
        })
    }

    /// Replace this table's contents with `other`, leaving `other` empty.
    pub fn replace_with(&self, other: &Self) -> Result<bool> {
        let self_ptr = self as *const Self as usize;
        let other_ptr = other as *const Self as usize;
        let (first, second, self_is_first) = if self_ptr <= other_ptr {
            (self, other, true)
        } else {
            (other, self, false)
        };

        let mut first_guard = first.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;
        let mut second_guard = second.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock CTE working table: {}", e))
        })?;

        let (dst_guard, src_guard) = if self_is_first {
            (&mut first_guard, &mut second_guard)
        } else {
            (&mut second_guard, &mut first_guard)
        };

        let dst = dst_guard.as_mut().ok_or_else(|| {
            paro_error::internal("Destination CTE working table is not initialized".to_string())
        })?;
        let src = src_guard.as_mut().ok_or_else(|| {
            paro_error::internal("Source CTE working table is not initialized".to_string())
        })?;

        if src.count() == 0 {
            return Ok(false);
        }

        dst.reset()?;
        std::mem::swap(dst, src);
        src.reset()?;
        Ok(dst.count() > 0)
    }

    /// Get total row count.
    pub fn row_count(&self) -> usize {
        self.collection
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(ColumnDataCollection::count))
            .unwrap_or(0)
    }
}

/// Physical CTE operator.
///
/// Materializes the CTE query results into a shared working table.
/// Acts as a Sink for the CTE query pipeline.
#[derive(Debug)]
pub struct CTE {
    /// CTE name (for debugging).
    pub cte_name: String,
    /// CTE index (unique identifier).
    pub cte_index: usize,
    /// Output types (same as main query child).
    pub types: Vec<LogicalType>,
    /// User-visible materialization strategy.
    pub materialization: CTEMaterialize,
    /// Number of references observed during binding.
    pub ref_count: usize,
    pub cte_query: Arc<dyn PhysicalOperator>,
    pub main_query: Arc<dyn PhysicalOperator>,
    /// Shared working table for CTE results.
    pub working_table: Arc<CteWorkingTable>,
}

impl CTE {
    /// Create a new CTE operator.
    pub fn new(
        cte_name: String,
        cte_index: usize,
        types: Vec<LogicalType>,
        materialization: CTEMaterialize,
        ref_count: usize,
        cte_query: Arc<dyn PhysicalOperator>,
        main_query: Arc<dyn PhysicalOperator>,
        working_table: Arc<CteWorkingTable>,
    ) -> Self {
        Self {
            cte_name,
            cte_index,
            types,
            materialization,
            ref_count,
            cte_query,
            main_query,
            working_table,
        }
    }
}

// ========== States ==========

/// Global sink state for CTE.
#[derive(Debug, Default)]
pub struct CteGlobalSinkState {
    /// Total rows materialized.
    pub total_rows: std::sync::atomic::AtomicUsize,
}

impl GlobalSinkState for CteGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local sink state for CTE.
#[derive(Debug)]
pub struct CteLocalSinkState {
    /// Local buffer for chunks before merging.
    pub local_chunks: RetainedChunkVec,
}

impl CteLocalSinkState {
    fn new(memory: MemoryAccountingContext) -> Self {
        Self {
            local_chunks: RetainedChunkVec::new(memory),
        }
    }
}

impl LocalSinkState for CteLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== PhysicalOperator Implementation ==========

impl PhysicalOperator for CTE {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Cte
    }

    fn explain_params(&self) -> Vec<String> {
        vec![
            format!("CTE Name: {}", self.cte_name),
            format!(
                "Materialization: {}",
                format_materialization(self.materialization)
            ),
            format!("Reference Count: {}", self.ref_count),
        ]
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn children_count(&self) -> usize {
        2
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        match index {
            0 => Some(self.cte_query.as_ref()),
            1 => Some(self.main_query.as_ref()),
            _ => None,
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        match index {
            0 => Some(self.cte_query.clone()),
            1 => Some(self.main_query.clone()),
            _ => None,
        }
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        // For MVP, we don't support parallel sink
        false
    }

    // ========== Sink Interface ==========

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        self.working_table
            .reset_with_memory(ctx.buffer_pool().clone(), cte_memory_context(ctx))?;
        Ok(Box::new(CteGlobalSinkState::default()))
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(CteLocalSinkState::new(cte_memory_context(ctx))))
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
            .downcast_mut::<CteLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        // Clone the chunk and store locally
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
            .downcast_mut::<CteLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<CteGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        // Merge local chunks into the working table
        let mut total_rows = 0;
        for chunk in lstate.local_chunks.drain_chunks() {
            total_rows += chunk.size();
            self.working_table.append(&chunk)?;
        }

        gstate
            .total_rows
            .fetch_add(total_rows, std::sync::atomic::Ordering::Relaxed);

        Ok(SinkCombineResultType::Finished)
    }

    // ========== Pipeline Construction ==========

    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        // Create a child MetaPipeline for the CTE query
        // The CTE query feeds into this operator (as sink)
        let child_meta = meta_pipeline.create_child_meta_pipeline(
            current,
            self_arc.clone(),
            MetaPipelineType::Regular,
        );

        // Build the CTE query into the child MetaPipeline
        child_meta.build(&self.cte_query, state);

        state.add_cte_dependency(self.cte_index, child_meta.base_pipeline());

        // Continue building the main query into the current pipeline.
        // This must dispatch through the child operator's own build_pipelines
        // implementation so nested MaterializedCTE nodes can install their
        // own CTE working-table context instead of falling back to the generic
        // sink+source builder.
        self.main_query
            .build_pipelines(&self.main_query, current, meta_pipeline, state);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Physical CTE Scan operator.
///
/// Scans the materialized CTE results from the shared working table.
/// Acts as a Source operator.
#[derive(Debug)]
pub struct CteScan {
    /// CTE name (for EXPLAIN / debugging).
    pub cte_name: String,
    /// CTE index (to identify which CTE to scan).
    pub cte_index: usize,
    /// Output types.
    pub types: Vec<LogicalType>,
    /// Shared working table to scan from.
    pub working_table: Arc<CteWorkingTable>,
    /// Whether this scan should wait for the materialization pipeline.
    pub register_dependency: bool,
}

impl CteScan {
    /// Create a new CteScan operator.
    pub fn new(
        cte_name: String,
        cte_index: usize,
        types: Vec<LogicalType>,
        working_table: Arc<CteWorkingTable>,
        register_dependency: bool,
    ) -> Self {
        Self {
            cte_name,
            cte_index,
            types,
            working_table,
            register_dependency,
        }
    }
}

// ========== CTE Scan States ==========

/// Global source state for CTE scan.
#[derive(Debug)]
pub struct CteScanGlobalSourceState {
    /// Snapshot of the storage indexes visible to this scan.
    pub storage_indexes: Vec<usize>,
    /// Current chunk slot being scanned.
    pub current_chunk: AtomicUsize,
}

impl GlobalSourceState for CteScanGlobalSourceState {
    fn max_threads(&self) -> usize {
        self.storage_indexes.len().max(1)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for CTE scan.
#[derive(Debug, Default)]
pub struct CteScanLocalSourceState {
    scan_state: ChunkManagementState,
}

impl LocalSourceState for CteScanLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for CteScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CteScan
    }

    fn explain_params(&self) -> Vec<String> {
        vec![format!("CTE Name: {}", self.cte_name)]
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        true
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        self.working_table
            .prepare_for_execution(ctx.buffer_pool().clone(), cte_memory_context(ctx))?;
        let storage_indexes = self.working_table.storage_indexes()?;
        Ok(Box::new(CteScanGlobalSourceState {
            storage_indexes,
            current_chunk: AtomicUsize::new(0),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(CteScanLocalSourceState::default()))
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<CteScanGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<CteScanLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        let chunk_idx = gstate.current_chunk.fetch_add(1, Ordering::Relaxed);
        if chunk_idx >= gstate.storage_indexes.len() {
            lstate.scan_state.clear();
            return Ok(SourceResultType::Finished);
        }

        let storage_index = gstate.storage_indexes[chunk_idx];
        self.working_table.fetch_chunk_by_storage_index(
            storage_index,
            &mut lstate.scan_state,
            chunk,
        )?;
        chunk.try_flatten()?;
        lstate.scan_state.clear();

        if chunk_idx + 1 >= gstate.storage_indexes.len() {
            Ok(SourceResultType::Finished)
        } else {
            Ok(SourceResultType::HaveMoreOutput)
        }
    }

    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        _meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        if self.register_dependency {
            if let Some(dependency) = state.get_cte_dependency(self.cte_index).cloned() {
                current.add_dependency(dependency);
            }
        }
        state.set_pipeline_source(current, self_arc.clone());
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn format_materialization(materialization: CTEMaterialize) -> &'static str {
    match materialization {
        CTEMaterialize::Default => "DEFAULT",
        CTEMaterialize::Materialized => "MATERIALIZED",
        CTEMaterialize::NotMaterialized => "NOT MATERIALIZED",
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_scheduler::task::InterruptState;

    use crate::execution_context::ExecutionContext;
    use crate::operator::state::OperatorSourceInput;
    use crate::operator::PhysicalOperator;
    use crate::thread_context::ThreadContext;

    use super::{cte_memory_context, CteScan, CteWorkingTable};

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn make_chunk(value: i32) -> Chunk {
        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 1);
        chunk.set_cardinality(1);
        chunk
            .column_mut(0)
            .expect("integer column should exist")
            .set_value(0, &Value::Integer(value));
        chunk
    }

    fn make_large_string_chunk(value: &str, rows: usize) -> Chunk {
        let mut chunk =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Varchar], rows);
        chunk.set_cardinality(rows);
        for row in 0..rows {
            chunk
                .column_mut(0)
                .expect("varchar column should exist")
                .set_string(row, value);
        }
        chunk
    }

    #[test]
    fn cte_scan_reports_parallelism_from_materialized_chunks() {
        let session = test_session();
        let table = Arc::new(CteWorkingTable::new(vec![LogicalType::Integer]));
        let thread_ctx = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session.clone(), &thread_ctx, None);
        table
            .prepare_for_execution(session.buffer_pool().clone(), cte_memory_context(&ctx))
            .expect("prepare CTE table");
        table.append(&make_chunk(1)).expect("append first chunk");
        table.append(&make_chunk(2)).expect("append second chunk");
        table.append(&make_chunk(3)).expect("append third chunk");

        let op = CteScan::new(
            "nums".to_string(),
            1,
            vec![LogicalType::Integer],
            table,
            false,
        );

        let gstate = op
            .get_global_source_state(&ctx, None)
            .expect("build global source state");

        assert!(op.parallel_source());
        assert_eq!(gstate.max_threads(), 3);
    }

    #[test]
    fn cte_scan_distributes_chunks_without_reuse() {
        let session = test_session();
        let table = Arc::new(CteWorkingTable::new(vec![LogicalType::Integer]));
        let thread_ctx = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session.clone(), &thread_ctx, None);
        table
            .prepare_for_execution(session.buffer_pool().clone(), cte_memory_context(&ctx))
            .expect("prepare CTE table");
        table.append(&make_chunk(11)).expect("append first chunk");
        table.append(&make_chunk(22)).expect("append second chunk");

        let op = CteScan::new(
            "nums".to_string(),
            1,
            vec![LogicalType::Integer],
            table,
            false,
        );

        let gstate = op
            .get_global_source_state(&ctx, None)
            .expect("build global source state");
        let mut lstate_1 = op
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("build local source state");
        let mut lstate_2 = op
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("build second local source state");
        let mut lstate_3 = op
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("build third local state");
        let interrupt = InterruptState::default();

        let mut chunk_1 = paro_common::test_utils::test_empty_chunk(op.types());
        let mut input_1 = OperatorSourceInput::new(gstate.as_ref(), lstate_1.as_mut(), &interrupt);
        let result_1 = op
            .get_data(&ctx, &mut chunk_1, &mut input_1)
            .expect("fetch first chunk");

        let mut chunk_2 = paro_common::test_utils::test_empty_chunk(op.types());
        let mut input_2 = OperatorSourceInput::new(gstate.as_ref(), lstate_2.as_mut(), &interrupt);
        let result_2 = op
            .get_data(&ctx, &mut chunk_2, &mut input_2)
            .expect("fetch second chunk");

        let mut chunk_3 = paro_common::test_utils::test_empty_chunk(op.types());
        let mut input_3 = OperatorSourceInput::new(gstate.as_ref(), lstate_3.as_mut(), &interrupt);
        let result_3 = op
            .get_data(&ctx, &mut chunk_3, &mut input_3)
            .expect("fetch exhausted chunk");

        let first = chunk_1.column(0).expect("first output column").get_value(0);
        let second = chunk_2
            .column(0)
            .expect("second output column")
            .get_value(0);

        assert_eq!(
            result_1,
            crate::result_type::SourceResultType::HaveMoreOutput
        );
        assert_eq!(result_2, crate::result_type::SourceResultType::Finished);
        assert_eq!(result_3, crate::result_type::SourceResultType::Finished);
        assert_eq!(first, Value::Integer(11));
        assert_eq!(second, Value::Integer(22));
    }

    #[test]
    fn working_table_spills_with_buffer_managed_storage() {
        let session = test_session();
        let table = Arc::new(CteWorkingTable::new(vec![LogicalType::Varchar]));
        let pool = session.buffer_pool().clone();
        let thread_ctx = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session.clone(), &thread_ctx, None);
        pool.set_memory_limit(256 * 1024)
            .expect("set small memory limit");

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("paro_cte_working_table_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .expect("set temp directory");

        table
            .prepare_for_execution(pool.clone(), cte_memory_context(&ctx))
            .expect("prepare working table");

        let payload = "spill_payload_".repeat(256);
        for _ in 0..128 {
            table
                .append(&make_large_string_chunk(&payload, 8))
                .expect("append spill chunk");
        }

        assert!(table.row_count() > 0);
        assert!(
            pool.get_temporary_spill_metrics().write_bytes > 0,
            "expected working table to spill via buffer-managed column storage"
        );

        let _ = pool.set_temporary_directory(String::new());
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn cte_scan_releases_spilled_pins_before_working_table_reset() {
        let session = test_session();
        let table = Arc::new(CteWorkingTable::new(vec![LogicalType::Varchar]));
        let pool = session.buffer_pool().clone();
        let thread_ctx = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session.clone(), &thread_ctx, None);
        pool.set_memory_limit(256 * 1024)
            .expect("set small memory limit");

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("paro_cte_scan_reset_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .expect("set temp directory");

        table
            .prepare_for_execution(pool.clone(), cte_memory_context(&ctx))
            .expect("prepare CTE table");

        let payload = "spill_payload_".repeat(256);
        for _ in 0..128 {
            table
                .append(&make_large_string_chunk(&payload, 8))
                .expect("append spill chunk");
        }
        assert!(
            pool.get_temporary_spill_metrics().write_bytes > 0,
            "expected working table to spill before scan/reset"
        );

        let op = CteScan::new(
            "spill_reset".to_string(),
            1,
            vec![LogicalType::Varchar],
            table.clone(),
            false,
        );

        let gstate = op
            .get_global_source_state(&ctx, None)
            .expect("build global source state");
        let mut lstate = op
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("build local source state");
        let interrupt = InterruptState::default();

        loop {
            let mut chunk = paro_common::test_utils::test_empty_chunk(op.types());
            let mut input = OperatorSourceInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
            let result = op
                .get_data(&ctx, &mut chunk, &mut input)
                .expect("scan working table");
            if result == crate::result_type::SourceResultType::Finished {
                break;
            }
        }

        table
            .reset()
            .expect("spilled working table reset should not keep scan pins");

        let _ = pool.set_temporary_directory(String::new());
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
