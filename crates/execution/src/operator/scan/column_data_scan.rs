// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Column Data Scan
//!
//! Thin scan wrapper over a materialized chunk collection.
//!
//!

use std::any::Any;
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::memory_runtime::RetainedChunkVec;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkInput,
    OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{SinkResultType, SourceResultType};

/// Shared materialized chunk collection.
#[derive(Debug)]
pub struct MaterializedChunkCollection {
    pub types: Vec<LogicalType>,
    chunks: Mutex<RetainedChunkVec>,
}

impl MaterializedChunkCollection {
    pub fn new(types: Vec<LogicalType>) -> Self {
        Self::with_memory(
            types,
            MemoryAccountingContext::detached(
                MemoryTag::ColumnData,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn with_memory(types: Vec<LogicalType>, memory: MemoryAccountingContext) -> Self {
        Self {
            types,
            chunks: Mutex::new(RetainedChunkVec::new(memory)),
        }
    }

    pub fn reset(&self) -> Result<()> {
        let mut chunks = self
            .chunks
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock chunk collection: {e}")))?;
        chunks.clear();
        Ok(())
    }

    pub fn append(&self, chunk: Chunk) -> Result<()> {
        let mut chunks = self
            .chunks
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock chunk collection: {e}")))?;
        chunks.push(chunk)?;
        Ok(())
    }

    pub fn chunk_count(&self) -> Result<usize> {
        self.chunks
            .lock()
            .map(|chunks| chunks.len())
            .map_err(|e| paro_error::internal(format!("Failed to lock chunk collection: {e}")))
    }

    pub fn snapshot(&self) -> Result<Arc<Vec<Chunk>>> {
        let chunks = self
            .chunks
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock chunk collection: {e}")))?;
        Ok(Arc::new(chunks.clone_chunks()))
    }

    pub fn reference_chunk(&self, chunk_idx: usize, output: &mut Chunk) -> Result<bool> {
        let chunks = self
            .chunks
            .lock()
            .map_err(|e| paro_error::internal(format!("Failed to lock chunk collection: {e}")))?;
        let Some(source) = chunks.as_slice().get(chunk_idx) else {
            return Ok(false);
        };
        output.reference(source);
        Ok(true)
    }
}

fn materialized_collection_memory_context(ctx: &ExecutionContext) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = ctx.operator_memory_account();
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::ColumnData,
        MemoryAccountingClass::Revocable,
    )
}

/// Shared binding for a materialized scan placeholder.
#[derive(Debug)]
pub struct ColumnDataScanBinding {
    dependency_id: Option<usize>,
    collection: Mutex<Option<Arc<MaterializedChunkCollection>>>,
}

impl ColumnDataScanBinding {
    pub fn new(dependency_id: Option<usize>) -> Self {
        Self {
            dependency_id,
            collection: Mutex::new(None),
        }
    }

    pub fn dependency_id(&self) -> Option<usize> {
        self.dependency_id
    }

    pub fn set_collection(&self, collection: Arc<MaterializedChunkCollection>) -> Result<()> {
        let mut guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock column-data scan binding: {e}"))
        })?;
        *guard = Some(collection);
        Ok(())
    }

    pub fn collection(&self) -> Result<Arc<MaterializedChunkCollection>> {
        let guard = self.collection.lock().map_err(|e| {
            paro_error::internal(format!("Failed to lock column-data scan binding: {e}"))
        })?;
        guard.clone().ok_or_else(|| {
            paro_error::internal("ColumnDataScan collection not initialized".to_string())
        })
    }
}

#[derive(Debug)]
pub struct PhysicalColumnDataScan {
    pub types: Vec<LogicalType>,
    pub binding: Arc<ColumnDataScanBinding>,
}

impl PhysicalColumnDataScan {
    pub fn new(types: Vec<LogicalType>, dependency_id: Option<usize>) -> Self {
        Self {
            types,
            binding: Arc::new(ColumnDataScanBinding::new(dependency_id)),
        }
    }

    pub fn with_binding(types: Vec<LogicalType>, binding: Arc<ColumnDataScanBinding>) -> Self {
        Self { types, binding }
    }

    pub fn binding(&self) -> Arc<ColumnDataScanBinding> {
        self.binding.clone()
    }
}

#[derive(Debug)]
struct ColumnDataSinkGlobalState {
    collection: Arc<MaterializedChunkCollection>,
}

impl GlobalSinkState for ColumnDataSinkGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct ColumnDataSinkLocalState;

impl LocalSinkState for ColumnDataSinkLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct ColumnDataCollectionSink {
    pub types: Vec<LogicalType>,
    pub binding: Arc<ColumnDataScanBinding>,
}

impl ColumnDataCollectionSink {
    pub fn new(types: Vec<LogicalType>, binding: Arc<ColumnDataScanBinding>) -> Self {
        Self { types, binding }
    }
}

#[derive(Debug)]
struct ColumnDataGlobalSourceState {
    collection: Arc<MaterializedChunkCollection>,
}

impl GlobalSourceState for ColumnDataGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct ColumnDataLocalSourceState {
    current_chunk: usize,
}

impl LocalSourceState for ColumnDataLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for PhysicalColumnDataScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ColumnDataScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let collection = self.binding.collection()?;
        Ok(Box::new(ColumnDataGlobalSourceState { collection }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(ColumnDataLocalSourceState::default()))
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
            .downcast_ref::<ColumnDataGlobalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid column-data global source state".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ColumnDataLocalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid column-data local source state".to_string())
            })?;

        let chunk_count = gstate.collection.chunk_count()?;
        if lstate.current_chunk >= chunk_count {
            return Ok(SourceResultType::Finished);
        }

        if !gstate
            .collection
            .reference_chunk(lstate.current_chunk, chunk)?
        {
            return Ok(SourceResultType::Finished);
        }
        lstate.current_chunk += 1;

        if lstate.current_chunk >= chunk_count {
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
        if let Some(dependency_id) = self.binding.dependency_id() {
            if let Some(dependency) = state.get_delim_join_dependency(dependency_id).cloned() {
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

impl PhysicalOperator for ColumnDataCollectionSink {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ColumnDataSink
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        false
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let collection = Arc::new(MaterializedChunkCollection::with_memory(
            self.types.clone(),
            materialized_collection_memory_context(ctx),
        ));
        self.binding.set_collection(collection.clone())?;
        Ok(Box::new(ColumnDataSinkGlobalState { collection }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(ColumnDataSinkLocalState))
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
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ColumnDataSinkGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid column-data sink state".to_string()))?;
        gstate
            .collection
            .append(chunk.try_deep_copy(chunk.allocator().clone())?)?;
        Ok(SinkResultType::NeedMoreInput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{MaterializedChunkCollection, PhysicalColumnDataScan};
    use crate::execution_context::ExecutionContext;
    use crate::operator::state::OperatorSourceInput;
    use crate::operator::PhysicalOperator;

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_scheduler::task::InterruptState;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[test]
    fn column_data_scan_reads_materialized_chunks() {
        let scan = PhysicalColumnDataScan::new(vec![LogicalType::Integer], Some(42));
        let collection = Arc::new(MaterializedChunkCollection::new(vec![LogicalType::Integer]));

        let mut stored =
            paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 2);
        stored.set_cardinality(2);
        stored
            .column_mut(0)
            .unwrap()
            .set_value(0, &Value::Integer(10));
        stored
            .column_mut(0)
            .unwrap()
            .set_value(1, &Value::Integer(20));
        collection.append(stored.clone()).expect("append");
        scan.binding()
            .set_collection(collection)
            .expect("set collection");

        let session = test_session();
        let thread = crate::thread_context::ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = scan
            .get_global_source_state(&ctx, None)
            .expect("global source");
        let mut lstate = scan
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("local source");
        let interrupt = InterruptState::default();
        let mut input = OperatorSourceInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        let mut out = paro_common::test_utils::test_chunk_with_capacity(&[LogicalType::Integer], 2);

        let result = scan.get_data(&ctx, &mut out, &mut input).expect("get data");
        assert_eq!(result, crate::result_type::SourceResultType::Finished);
        assert_eq!(out.size(), 2);
        assert_eq!(out.column(0).unwrap().get_value(0), Value::Integer(10));
        assert_eq!(out.column(0).unwrap().get_value(1), Value::Integer(20));
    }
}
