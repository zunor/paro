// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Core physical operator traits and shared execution state types.

pub mod aggregate;
pub mod ddl;
pub mod external;
pub mod filter;
pub mod graph;
pub mod helper;
pub mod join;
pub mod persistent;
pub mod projection;
pub mod result;
pub mod scan;
pub mod search;
pub mod set;
pub mod state;
pub mod window;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::execution_context::ExecutionContext;
use crate::explain::profiler::ExplainProfiler;
use crate::explain::types::{ExplainNodeId, ExplainRuntimeStats, ExplainSchema};
use crate::memory_runtime::OperatorMemoryScope;
use crate::operator::state::{
    EmptyGlobalOperatorState, EmptyGlobalSinkState, EmptyGlobalSourceState, EmptyLocalSinkState,
    EmptyLocalSourceState, EmptyOperatorState, GlobalOperatorState, GlobalSinkState,
    GlobalSourceState, LocalSinkState, LocalSourceState, OperatorFinalizeInput,
    OperatorSinkCombineInput, OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
    OperatorState, ProgressData,
};
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{
    OperatorFinalResultType, OperatorFinalizeResultType, OperatorResultType, SinkCombineResultType,
    SinkFinalizeType, SinkNextBatchType, SinkResultType, SourceResultType,
};

/// Order preservation type for operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OrderPreservationType {
    /// Order is preserved (insertion order)
    #[default]
    InsertionOrder,
    /// Order is not preserved
    NoOrder,
    /// Order is fixed (deterministic but different from input)
    FixedOrder,
}

/// Partitioning requirements that a source must satisfy for its sink.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OperatorPartitionInfo {
    /// Whether sink/source coordination requires a globally increasing batch index.
    pub batch_index: bool,
    /// Optional partition columns required by the sink.
    pub partition_columns: Vec<usize>,
}

impl OperatorPartitionInfo {
    /// No partitioning requirements.
    pub fn no_partition_info() -> Self {
        Self {
            batch_index: false,
            partition_columns: Vec::new(),
        }
    }

    /// Requires globally increasing batch index support.
    pub fn batch_index() -> Self {
        Self {
            batch_index: true,
            partition_columns: Vec::new(),
        }
    }

    /// Requires partition columns support.
    pub fn partition_columns(partition_columns: Vec<usize>) -> Self {
        Self {
            batch_index: false,
            partition_columns,
        }
    }

    /// Returns true if partition columns are required.
    pub fn requires_partition_columns(&self) -> bool {
        !self.partition_columns.is_empty()
    }

    /// Returns true if batch index support is required.
    pub fn requires_batch_index(&self) -> bool {
        self.batch_index
    }

    /// Returns true if any partitioning capability is required.
    pub fn any_required(&self) -> bool {
        self.requires_partition_columns() || self.requires_batch_index()
    }
}

/// Physical operator trait.
///
/// This is the base trait for all physical operators in the execution engine.
/// Operators can be:
/// - **Source**: Produces data without input (e.g., table scan)
/// - **Operator**: Transforms input data to output (e.g., filter, projection)
/// - **Sink**: Consumes data and produces side effects (e.g., insert, hash join build)
///
/// # Execution Model
/// Paro uses a pipeline-based push execution model:
/// 1. The executor calls `get_data()` on the source
/// 2. Data flows through operators via `execute()`
/// 3. Sinks consume data via `sink()`
///
/// # Example
/// ```ignore
/// // Implementing a simple projection operator
/// struct ProjectionOperator {
///     output_types: Vec<LogicalType>,
///     expressions: Vec<Expression>,
/// }
///
/// impl PhysicalOperator for ProjectionOperator {
///     fn execute(&self, ctx: &ExecutionContext, input: &Chunk,
///                chunk: &mut Chunk, gstate: &dyn GlobalOperatorState,
///                state: &mut dyn OperatorState) -> Result<OperatorResultType> {
///         // Evaluate expressions and write to output chunk
///         Ok(OperatorResultType::NeedMoreInput)
///     }
/// }
/// ```
pub trait PhysicalOperator: Send + Sync + fmt::Debug {
    // ========== Metadata ==========

    /// Get the operator type.
    fn operator_type(&self) -> PhysicalOperatorType;

    /// Get the operator name for display.
    fn name(&self) -> &str {
        self.operator_type().to_string()
    }

    /// Get the operator name for EXPLAIN output.
    fn explain_name(&self) -> String {
        self.name().to_string()
    }

    /// Get operator-specific parameter lines for EXPLAIN output.
    fn explain_params(&self) -> Vec<String> {
        vec![]
    }

    /// Stable EXPLAIN node id, available only when the plan is explain-annotated.
    fn explain_node_id(&self) -> Option<ExplainNodeId> {
        None
    }

    /// Explain schema annotation propagated from the logical plan.
    fn explain_schema(&self) -> Option<&ExplainSchema> {
        None
    }

    /// Shared EXPLAIN ANALYZE profiler for this annotated plan.
    fn explain_profiler(&self) -> Option<Arc<ExplainProfiler>> {
        None
    }

    /// Runtime memory statistics surfaced in EXPLAIN ANALYZE.
    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        ExplainRuntimeStats::default()
    }

    /// Underlying concrete operator used for explain-specific downcasts.
    fn explain_inner(&self) -> &dyn PhysicalOperator
    where
        Self: Sized,
    {
        self
    }

    /// Get the return types of this operator.
    fn types(&self) -> &[LogicalType];

    /// Get estimated cardinality.
    fn estimated_cardinality(&self) -> usize {
        0
    }

    /// Check if this operator is a source.
    fn is_source(&self) -> bool {
        false
    }

    /// Check if this operator is a sink.
    fn is_sink(&self) -> bool {
        false
    }

    /// Check if this operator supports parallel execution.
    ///
    /// Default is `true` - most operators can be executed in parallel.
    /// Override to return `false` for operators that cannot be parallelized
    /// (e.g., ORDER BY final merge, certain window functions).
    fn parallel_operator(&self) -> bool {
        true
    }

    /// Check if this operator supports parallel source.
    ///
    /// Default is `true` - most sources can be parallelized.
    /// Override to return `false` for sources that cannot be parallelized
    /// (e.g., single-row value scans, in-out table functions).
    fn parallel_source(&self) -> bool {
        true
    }

    /// Check whether this source can provide the requested partitioning guarantees.
    ///
    fn supports_partitioning(&self, partition_info: &OperatorPartitionInfo) -> bool {
        !partition_info.any_required()
    }

    /// Check if this operator supports parallel sink.
    ///
    /// Default is `true` - most sinks can be parallelized.
    /// Override to return `false` for sinks that require sequential execution.
    fn parallel_sink(&self) -> bool {
        true
    }

    /// Get the order preservation type for this operator.
    fn operator_order(&self) -> OrderPreservationType {
        OrderPreservationType::InsertionOrder
    }

    /// Get the source order preservation type.
    fn source_order(&self) -> OrderPreservationType {
        OrderPreservationType::InsertionOrder
    }

    /// Check if operator requires final execute.
    fn requires_final_execute(&self) -> bool {
        false
    }

    /// Check if operator requires a dedicated finalize call in PipelineFinishTask.
    ///
    fn requires_operator_finalize(&self) -> bool {
        false
    }

    // ========== State Management ==========

    /// Create thread-local operator state.
    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(EmptyOperatorState))
    }

    /// Create global operator state.
    fn get_global_operator_state(&self) -> Result<Box<dyn GlobalOperatorState>> {
        Ok(Box::new(EmptyGlobalOperatorState))
    }

    /// Create thread-local source state.
    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(EmptyLocalSourceState))
    }

    /// Create global source state.
    ///
    /// For blocking operators (both Sink and Source), the `sink_state` parameter
    /// provides access to the finalized sink state containing processed data.
    ///
    /// # Arguments
    /// * `sink_state` - For Sink+Source operators, the finalized GlobalSinkState;
    ///   `None` for pure Source operators.
    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Ok(Box::new(EmptyGlobalSourceState))
    }

    /// Create thread-local sink state.
    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(EmptyLocalSinkState))
    }

    /// Create global sink state.
    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(EmptyGlobalSinkState {
            finalize_state: SinkFinalizeType::Ready,
            _name: format!("EmptyGlobalSinkState for {}", self.operator_type()),
        }))
    }

    /// Set the global sink state for this operator (for blocking operators).
    fn set_sink_state(&self, _state: Arc<dyn GlobalSinkState>) {}

    /// Get the global sink state for this operator.
    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        None
    }

    /// Clear any shared sink state cached on the operator itself.
    fn clear_sink_state(&self) {}

    // ========== Operator Interface ==========

    /// Execute the operator on input data.
    ///
    /// Called for regular operators (non-source, non-sink).
    /// Takes input chunk and produces output chunk.
    ///
    /// # Returns
    /// - `NeedMoreInput`: Ready for more input
    /// - `HaveMoreOutput`: Call again with same input
    /// - `Finished`: Pipeline is complete
    fn execute(
        &self,
        _ctx: &ExecutionContext,
        _input: &Chunk,
        _chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        _state: &mut dyn OperatorState,
        _memory: OperatorMemoryScope<'_>,
    ) -> Result<OperatorResultType> {
        Err(paro_error::internal(
            "Execute called on non-operator".to_string(),
        ))
    }

    /// Final execute to flush cached results.
    ///
    /// Row-producing finalize can now return `Blocked` when the operator needs
    /// to suspend an asynchronous tail flush and resume later.
    fn final_execute(
        &self,
        _ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        _state: &mut dyn OperatorState,
        _memory: OperatorMemoryScope<'_>,
    ) -> Result<OperatorFinalizeResultType> {
        Ok(OperatorFinalizeResultType::Finished)
    }

    /// Finalize operator-level global state after all pipeline tasks complete.
    ///
    fn operator_finalize(&self, _input: &OperatorFinalizeInput) -> Result<OperatorFinalResultType> {
        Ok(OperatorFinalResultType::Finished)
    }

    // ========== Source Interface ==========

    /// Get data from this source operator.
    ///
    /// Called for source operators (leaf nodes like table scan).
    /// Fills the chunk with data from the source.
    ///
    /// # Returns
    /// - `HaveMoreOutput`: More data available
    /// - `Finished`: Source is exhausted
    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        Err(paro_error::internal(
            "GetData called on non-source".to_string(),
        ))
    }

    /// Get progress information from source.
    fn get_progress(&self, _gstate: &dyn GlobalSourceState) -> ProgressData {
        ProgressData::invalid()
    }

    /// Combine source progress with sink-side progress.
    ///
    /// Default behavior keeps source progress unchanged.
    fn get_sink_progress(
        &self,
        _gstate: &dyn GlobalSinkState,
        source_progress: ProgressData,
    ) -> ProgressData {
        source_progress
    }

    // ========== Sink Interface ==========

    /// Sink data into this operator.
    ///
    /// Called for sink operators (e.g., hash join build, insert).
    /// Can be called in parallel - use proper synchronization.
    ///
    /// # Returns
    /// - `NeedMoreInput`: Ready for more input
    /// - `Finished`: Sink is complete
    fn sink(
        &self,
        _ctx: &ExecutionContext,
        _chunk: &Chunk,
        _input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        Err(paro_error::internal("Sink called on non-sink".to_string()))
    }

    /// Combine thread-local sink state into global state.
    ///
    /// Called when a thread finishes its part of the pipeline.
    fn combine(
        &self,
        _ctx: &ExecutionContext,
        _input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        Ok(SinkCombineResultType::Finished)
    }

    /// Finalize the sink after all threads complete.
    ///
    /// Called once per pipeline, single-threaded.
    fn finalize(&self, _input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        Ok(SinkFinalizeType::Ready)
    }

    /// Prepare for finalization.
    fn prepare_finalize(&self, _gstate: &dyn GlobalSinkState) -> Result<()> {
        Ok(())
    }

    /// Move to next batch (for batch-aware sinks).
    fn next_batch(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSinkState,
        _lstate: &mut dyn LocalSinkState,
    ) -> Result<SinkNextBatchType> {
        Ok(SinkNextBatchType::Ready)
    }

    /// Check if sink is order dependent.
    fn sink_order_dependent(&self) -> bool {
        false
    }

    /// Partitioning requirements this sink imposes on upstream sources.
    fn required_partition_info(&self) -> OperatorPartitionInfo {
        OperatorPartitionInfo::no_partition_info()
    }

    // ========== Children ==========

    /// Get the number of children.
    fn children_count(&self) -> usize {
        0
    }

    /// Get a child operator by index.
    fn child(&self, _index: usize) -> Option<&dyn PhysicalOperator> {
        None
    }

    /// Get child operator as Arc.
    fn child_arc(&self, _index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        None
    }

    // ========== Pipeline Construction ==========

    /// Build pipelines for this operator.
    ///
    /// This method is called during pipeline construction to determine how
    /// this operator fits into the pipeline structure. The default implementation
    /// handles common cases:
    ///
    /// 1. **Source operators** (leaf nodes): Set as the pipeline source
    /// 2. **Sink operators**: Create a child MetaPipeline for the subtree
    /// 3. **Regular operators**: Add to the current pipeline and recurse
    ///
    /// Operators with special pipeline construction needs (e.g., Join, Union)
    /// should override this method.
    ///
    /// # Arguments
    /// * `self_arc` - Arc reference to this operator (needed for adding to pipeline)
    /// * `current` - The current pipeline being built
    /// * `meta_pipeline` - The MetaPipeline that owns the current pipeline
    /// * `state` - Build state for tracking dependencies
    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        // Default implementation delegates to build_pipelines_default
        crate::pipeline::build_pipelines::build_pipelines_default(
            self_arc,
            current,
            meta_pipeline,
            state,
        );
    }

    // ========== Utility ==========

    /// Name of the sink state.
    fn sink_state_name(&self) -> &str {
        "UnknownSinkState"
    }

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Downcast to mutable concrete type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Helper macro to implement common PhysicalOperator methods.
#[macro_export]
macro_rules! impl_physical_operator_common {
    ($type:ty, $op_type:expr, $types:expr) => {
        fn operator_type(&self) -> PhysicalOperatorType {
            $op_type
        }

        fn types(&self) -> &[LogicalType] {
            $types
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    };
}
