// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
    OperatorFinalizeInput, OperatorSinkCombineInput, OperatorSinkFinalizeInput, OperatorSinkInput,
    OperatorSourceInput, OperatorState, ProgressData,
};
use crate::operator::{OperatorPartitionInfo, OrderPreservationType, PhysicalOperator};
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{
    OperatorFinalResultType, OperatorFinalizeResultType, OperatorResultType, SinkCombineResultType,
    SinkFinalizeType, SinkNextBatchType, SinkResultType, SourceResultType,
};

use crate::explain::profiler::ExplainProfiler;
use crate::explain::types::{ExplainLogicalInfo, ExplainNodeId, ExplainSchema};

#[derive(Debug)]
pub struct ExplainAnnotatedOperator {
    node_id: ExplainNodeId,
    schema: ExplainSchema,
    logical: ExplainLogicalInfo,
    profiler: Option<Arc<ExplainProfiler>>,
    inner: Arc<dyn PhysicalOperator>,
}

impl ExplainAnnotatedOperator {
    pub fn new(
        node_id: ExplainNodeId,
        schema: ExplainSchema,
        logical: ExplainLogicalInfo,
        profiler: Option<Arc<ExplainProfiler>>,
        inner: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self {
            node_id,
            schema,
            logical,
            profiler,
            inner,
        }
    }

    pub fn inner(&self) -> &dyn PhysicalOperator {
        self.inner.as_ref()
    }

    pub fn logical(&self) -> &ExplainLogicalInfo {
        &self.logical
    }
}

impl PhysicalOperator for ExplainAnnotatedOperator {
    fn operator_type(&self) -> crate::operator_type::PhysicalOperatorType {
        self.inner.operator_type()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn explain_name(&self) -> String {
        self.inner.explain_name()
    }

    fn explain_params(&self) -> Vec<String> {
        self.inner.explain_params()
    }

    fn explain_node_id(&self) -> Option<ExplainNodeId> {
        Some(self.node_id)
    }

    fn explain_schema(&self) -> Option<&ExplainSchema> {
        Some(&self.schema)
    }

    fn explain_profiler(&self) -> Option<Arc<ExplainProfiler>> {
        self.profiler.clone()
    }

    fn explain_inner(&self) -> &dyn PhysicalOperator {
        self.inner.as_ref()
    }

    fn types(&self) -> &[LogicalType] {
        self.inner.types()
    }

    fn estimated_cardinality(&self) -> usize {
        self.inner.estimated_cardinality()
    }

    fn is_source(&self) -> bool {
        self.inner.is_source()
    }

    fn is_sink(&self) -> bool {
        self.inner.is_sink()
    }

    fn parallel_operator(&self) -> bool {
        self.inner.parallel_operator()
    }

    fn parallel_source(&self) -> bool {
        self.inner.parallel_source()
    }

    fn supports_partitioning(&self, partition_info: &OperatorPartitionInfo) -> bool {
        self.inner.supports_partitioning(partition_info)
    }

    fn parallel_sink(&self) -> bool {
        self.inner.parallel_sink()
    }

    fn operator_order(&self) -> OrderPreservationType {
        self.inner.operator_order()
    }

    fn source_order(&self) -> OrderPreservationType {
        self.inner.source_order()
    }

    fn requires_final_execute(&self) -> bool {
        self.inner.requires_final_execute()
    }

    fn requires_operator_finalize(&self) -> bool {
        self.inner.requires_operator_finalize()
    }

    fn get_operator_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        self.inner.get_operator_state(ctx)
    }

    fn get_global_operator_state(&self) -> Result<Box<dyn GlobalOperatorState>> {
        self.inner.get_global_operator_state()
    }

    fn get_local_source_state(
        &self,
        ctx: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        self.inner.get_local_source_state(ctx, gstate)
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        self.inner.get_global_source_state(ctx, sink_state)
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        self.inner.get_local_sink_state(ctx)
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        self.inner.get_global_sink_state(ctx)
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        self.inner.set_sink_state(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.inner.sink_state()
    }

    fn clear_sink_state(&self) {
        self.inner.clear_sink_state();
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorResultType> {
        self.inner.execute(ctx, input, chunk, gstate, state)
    }

    fn final_execute(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorFinalizeResultType> {
        self.inner.final_execute(ctx, chunk, gstate, state)
    }

    fn operator_finalize(&self, input: &OperatorFinalizeInput) -> Result<OperatorFinalResultType> {
        self.inner.operator_finalize(input)
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        self.inner.get_data(ctx, chunk, input)
    }

    fn get_progress(&self, gstate: &dyn GlobalSourceState) -> ProgressData {
        self.inner.get_progress(gstate)
    }

    fn get_sink_progress(
        &self,
        gstate: &dyn GlobalSinkState,
        source_progress: ProgressData,
    ) -> ProgressData {
        self.inner.get_sink_progress(gstate, source_progress)
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        self.inner.sink(ctx, chunk, input)
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        self.inner.combine(ctx, input)
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        self.inner.finalize(input)
    }

    fn prepare_finalize(&self, gstate: &dyn GlobalSinkState) -> Result<()> {
        self.inner.prepare_finalize(gstate)
    }

    fn next_batch(
        &self,
        ctx: &ExecutionContext,
        gstate: &dyn GlobalSinkState,
        lstate: &mut dyn LocalSinkState,
    ) -> Result<SinkNextBatchType> {
        self.inner.next_batch(ctx, gstate, lstate)
    }

    fn sink_order_dependent(&self) -> bool {
        self.inner.sink_order_dependent()
    }

    fn required_partition_info(&self) -> OperatorPartitionInfo {
        self.inner.required_partition_info()
    }

    fn children_count(&self) -> usize {
        self.inner.children_count()
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        self.inner.child(index)
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        self.inner.child_arc(index)
    }

    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        self.inner
            .build_pipelines(self_arc, current, meta_pipeline, state);
    }

    fn sink_state_name(&self) -> &str {
        self.inner.sink_state_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
