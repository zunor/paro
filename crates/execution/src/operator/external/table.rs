// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::explain::types::ExplainRuntimeStats;
use crate::memory_runtime::OperatorMemoryScope;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::physical_plan::plan_external_table::ExternalTablePlanBinding;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};

use super::batching::SubmissionBatchPolicy;
use super::runtime_bridge::{ExternalRuntimeBridge, RuntimeBridgeOutcome, TableSubmission};
use super::table_state::{
    ExternalTableFlowControl, ExternalTableGlobalSinkState, ExternalTableGlobalSourceState,
    ExternalTableLocalSinkState, ExternalTableLocalSourceState, ExternalTableSharedState,
    InflightTableBatch, TableOutputBatch,
};

#[derive(Debug)]
pub struct ExternalTable {
    output_types: Vec<LogicalType>,
    child: Arc<dyn PhysicalOperator>,
    binding: ExternalTablePlanBinding,
    bridge: Arc<ExternalRuntimeBridge>,
    batch_policy: SubmissionBatchPolicy,
    flow_control: ExternalTableFlowControl,
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl ExternalTable {
    pub fn new(
        binding: ExternalTablePlanBinding,
        child: Arc<dyn PhysicalOperator>,
        bridge: Arc<ExternalRuntimeBridge>,
    ) -> Self {
        let batch_policy = SubmissionBatchPolicy::from_dispatch_policy(bridge.dispatch_policy());
        Self {
            output_types: binding.emitted_output_types.clone(),
            child,
            binding,
            bridge,
            batch_policy,
            flow_control: ExternalTableFlowControl::default(),
            sink_state: Mutex::new(None),
        }
    }

    pub fn with_flow_control(mut self, flow_control: ExternalTableFlowControl) -> Self {
        self.flow_control = flow_control;
        self
    }

    pub fn explain_name(&self) -> &'static str {
        "EXTERNAL_TABLE"
    }

    fn shared_state(&self) -> Option<Arc<ExternalTableSharedState>> {
        let sink_state = self.sink_state()?;
        sink_state
            .as_any()
            .downcast_ref::<ExternalTableGlobalSinkState>()
            .map(|sink| sink.shared.clone())
    }

    fn runtime_stats(&self) -> Option<super::table_state::ExternalTableRuntimeStats> {
        self.shared_state().map(|shared| shared.runtime_stats())
    }

    fn runtime_allocator(memory: &OperatorMemoryScope<'_>) -> Arc<dyn Allocator> {
        memory.accounted_allocator_for(
            MemoryTag::ExternalRuntimeHost,
            MemoryAccountingClass::NonRevocable,
        )
    }

    fn take_prefix_batch(
        &self,
        state: &mut ExternalTableLocalSinkState,
        rows: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Chunk> {
        if rows == 0 || rows > state.accumulation.size() {
            return Err(paro_error::internal(
                "invalid external table batch split".to_string(),
            ));
        }

        let mut batch_view = state.accumulation.clone();
        batch_view.try_slice_range(0, rows)?;
        let batch = batch_view.try_deep_copy(allocator.clone())?;

        if rows == state.accumulation.size() {
            state.accumulation.try_reset(allocator.clone())?;
            state.accumulation_bytes = 0;
            return Ok(batch);
        }

        let mut remaining_view = state.accumulation.clone();
        remaining_view.try_slice_range(rows, state.accumulation.size() - rows)?;
        state.accumulation = remaining_view.try_deep_copy(allocator)?;
        state.accumulation_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&state.accumulation);
        Ok(batch)
    }

    fn stage_input(
        &self,
        shared: &Arc<ExternalTableSharedState>,
        state: &mut ExternalTableLocalSinkState,
        input: &Chunk,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<()> {
        if state.current_input_staged || input.is_empty() {
            return Ok(());
        }
        if !state.accumulation_uses_runtime_allocator {
            state.accumulation =
                Chunk::try_init_empty(self.child.types(), Self::runtime_allocator(memory))?;
            state.accumulation_uses_runtime_allocator = true;
        }
        state.accumulation.try_append(input)?;
        state.accumulation_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&state.accumulation);
        state.current_input_staged = true;
        shared.observe_accumulation_bytes(state.accumulation_bytes);
        Ok(())
    }

    fn enqueue_response_batches(
        &self,
        shared: &Arc<ExternalTableSharedState>,
        state: &mut ExternalTableLocalSinkState,
        response: &super::runtime_bridge::RuntimeBridgeResponse,
    ) {
        let partition_id = state.next_partition_id;
        state.next_partition_id = state.next_partition_id.saturating_add(1);

        let batches = response
            .output_batches
            .iter()
            .enumerate()
            .map(|(idx, batch)| TableOutputBatch {
                bytes: SubmissionBatchPolicy::estimate_chunk_bytes(batch),
                chunk: batch.clone(),
                partition_id,
                partition_end: idx + 1 == response.output_batches.len(),
            });
        shared.enqueue_output_batches(batches);
    }

    fn attach_passthrough_columns(
        &self,
        input: &Chunk,
        mut response: super::runtime_bridge::RuntimeBridgeResponse,
    ) -> Result<super::runtime_bridge::RuntimeBridgeResponse> {
        if !self.binding.parameterized || input.column_count() <= self.binding.argument_count {
            return Ok(response);
        }

        let passthrough_count = input.column_count() - self.binding.argument_count;
        let total_output_rows = response
            .output_batches
            .iter()
            .map(Chunk::size)
            .sum::<usize>();
        if total_output_rows != input.size() {
            return Err(paro_error::not_implemented(
                "parameterized external table routines currently require row-preserving output to retain lateral correlation columns",
            ));
        }

        let mut offset = 0;
        let merged_batches = response
            .output_batches
            .into_iter()
            .map(|batch| {
                let mut input_slice = input.clone();
                input_slice.try_slice_range(offset, batch.size())?;
                offset += batch.size();

                let mut columns = Vec::with_capacity(batch.column_count() + passthrough_count);
                for index in 0..batch.column_count() {
                    columns.push(
                        batch
                            .column(index)
                            .ok_or_else(|| {
                                paro_error::internal(format!(
                                    "missing worker output column {} for parameterized external table",
                                    index
                                ))
                            })?
                            .as_ref()
                            .clone(),
                    );
                }
                for index in self.binding.argument_count..input_slice.column_count() {
                    columns.push(
                        input_slice
                            .column(index)
                            .ok_or_else(|| {
                                paro_error::internal(format!(
                                    "missing passthrough column {} for parameterized external table",
                                    index
                                ))
                            })?
                            .as_ref()
                            .clone(),
                    );
                }
                Ok(Chunk::from_vectors(
                    columns,
                    input.allocator().clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let old_output_bytes = response.metrics.output_bytes;
        let new_output_bytes = merged_batches
            .iter()
            .map(SubmissionBatchPolicy::estimate_chunk_bytes)
            .sum();
        response.metrics.output_bytes = new_output_bytes;
        response.metrics.data_plane_bytes = response
            .metrics
            .data_plane_bytes
            .saturating_sub(old_output_bytes)
            .saturating_add(new_output_bytes);
        response.output_batches = merged_batches;
        Ok(response)
    }

    fn collect_inflight_if_needed(
        &self,
        shared: &Arc<ExternalTableSharedState>,
        state: &mut ExternalTableLocalSinkState,
    ) {
        let Some(inflight) = state.inflight.take() else {
            return;
        };
        self.enqueue_response_batches(shared, state, &inflight.response);
    }

    fn submit_if_ready(
        &self,
        ctx: &ExecutionContext,
        shared: &Arc<ExternalTableSharedState>,
        state: &mut ExternalTableLocalSinkState,
        tail_flush: bool,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<Option<SinkResultType>> {
        if !self.batch_policy.should_flush(
            state.accumulation.size(),
            state.accumulation_bytes,
            tail_flush,
            self.child.types(),
        ) {
            return Ok(None);
        }

        let batch_rows = self
            .batch_policy
            .suggest_batch_rows(self.child.types(), state.accumulation_bytes)
            .min(state.accumulation.size())
            .max(1);
        let batch = self.take_prefix_batch(state, batch_rows, Self::runtime_allocator(memory))?;
        shared.observe_accumulation_bytes(state.accumulation_bytes);
        let worker_input = self.extract_worker_input(&batch)?;

        let submission = TableSubmission {
            batch_id: state.next_batch_id,
            input: &worker_input,
            routine: &self.binding.routine,
            output_types: &self.binding.worker_output_types,
            lateral: self.binding.lateral,
            parameterized: self.binding.parameterized,
        };
        state.next_batch_id = state.next_batch_id.saturating_add(1);

        match self.bridge.execute_table(ctx, &submission, memory)? {
            RuntimeBridgeOutcome::Ready(response) => {
                let response = self.attach_passthrough_columns(&batch, response)?;
                shared.record_submission(batch.size(), false, &response);
                self.enqueue_response_batches(shared, state, &response);
                Ok(None)
            }
            RuntimeBridgeOutcome::Blocked(response) => {
                let response = self.attach_passthrough_columns(&batch, response)?;
                shared.record_submission(batch.size(), true, &response);
                state.inflight = Some(InflightTableBatch { response });
                let _ = ctx.interrupt_state().callback();
                Ok(Some(SinkResultType::Blocked))
            }
        }
    }

    fn extract_worker_input(&self, batch: &Chunk) -> Result<Chunk> {
        if self.binding.argument_count == 0 || batch.column_count() == self.binding.argument_count {
            return Ok(batch.clone());
        }
        if batch.column_count() < self.binding.argument_count {
            return Err(paro_error::internal(format!(
                "external table worker input expected {} argument columns, got {}",
                self.binding.argument_count,
                batch.column_count()
            )));
        }

        let columns = (0..self.binding.argument_count)
            .map(|index| {
                batch
                    .column(index)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "external table worker input is missing column {}",
                            index
                        ))
                    })
                    .map(|vector| vector.as_ref().clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Chunk::from_vectors(columns, batch.allocator().clone()))
    }

    fn run_sink_step(
        &self,
        ctx: &ExecutionContext,
        shared: &Arc<ExternalTableSharedState>,
        state: &mut ExternalTableLocalSinkState,
        input: Option<&Chunk>,
        tail_flush: bool,
        memory: &OperatorMemoryScope<'_>,
    ) -> Result<SinkResultType> {
        if let Some(input) = input {
            self.stage_input(shared, state, input, memory)?;
        }

        self.collect_inflight_if_needed(shared, state);

        if let Some(result) = self.submit_if_ready(ctx, shared, state, tail_flush, memory)? {
            return Ok(result);
        }

        if tail_flush {
            state.combined = true;
            state.current_input_staged = false;
            return Ok(SinkResultType::NeedMoreInput);
        }

        state.current_input_staged = false;
        Ok(SinkResultType::NeedMoreInput)
    }
}

impl PhysicalOperator for ExternalTable {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ExternalTable
    }

    fn explain_name(&self) -> String {
        ExternalTable::explain_name(self).to_string()
    }

    fn explain_params(&self) -> Vec<String> {
        let explain = self.bridge.explain();
        let mut params = vec![
            format!("Routine: {}", self.binding.routine.identity_label()),
            format!("Language: {}", explain.language),
            format!("Backend: {}", explain.backend),
            format!(
                "Batch Target: {} bytes (max accumulation {} bytes)",
                self.batch_policy.target_batch_bytes, self.batch_policy.max_accumulation_bytes
            ),
            format!(
                "Flow Control: visible_batches<={} visible_bytes<={} credit_step={}",
                self.flow_control.max_inflight_output_batches,
                self.flow_control.max_inflight_output_bytes,
                self.flow_control.credit_granularity_bytes
            ),
            format!(
                "Env Artifact: {}",
                explain
                    .env_artifact_id
                    .clone()
                    .unwrap_or_else(|| "unresolved".to_string())
            ),
            format!("Artifact Validation: {}", explain.artifact_validation_state),
            format!(
                "Correlation: lateral={} parameterized={} (outer batch + parameter columns)",
                self.binding.lateral, self.binding.parameterized
            ),
            format!(
                "Cost: startup={:.3}, per_row={:.3}, bytes={:.3}, queue_risk={:.3}",
                self.binding.cost.startup_cost,
                self.binding.cost.per_row_cost,
                self.binding.cost.bytes_cost,
                self.binding.cost.queue_risk
            ),
        ];

        if let Some(stats) = self.runtime_stats() {
            if stats.submissions > 0 {
                params.push(format!(
                    "Runtime: submissions={} blocked={} input_rows={} output_rows={}",
                    stats.submissions,
                    stats.blocked_submissions,
                    stats.total_input_rows,
                    stats.total_output_rows
                ));
                params.push(format!(
                    "Latency(us): acquire={} queue={} kernel={} encode_decode={}",
                    stats.worker_acquire_time_us,
                    stats.queue_wait_us,
                    stats.kernel_time_us,
                    stats.encode_decode_time_us
                ));
                params.push(format!(
                    "Data Plane: {} bytes, warm={} cold={} retired={}",
                    stats.data_plane_bytes,
                    stats.warm_batches,
                    stats.cold_batches,
                    stats.retired_count
                ));
                params.push(format!(
                    "Output Queue: peak_bytes={} peak_batches={} backlog_promotions={}",
                    stats.peak_visible_output_bytes,
                    stats.peak_visible_output_batches,
                    stats.promoted_backlog_batches
                ));
            }
        }

        params
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(shared) = self.shared_state() else {
            return ExplainRuntimeStats::default();
        };
        let stats = shared.runtime_stats();
        ExplainRuntimeStats {
            spilled: Some(false),
            peak_memory_bytes: Some(
                stats
                    .peak_accumulation_bytes
                    .saturating_add(stats.peak_visible_output_bytes),
            ),
            temp_storage_bytes: None,
            data_plane_bytes: (stats.data_plane_bytes > 0).then_some(stats.data_plane_bytes),
            output_buffer_bytes: (stats.peak_visible_output_bytes > 0)
                .then_some(stats.peak_visible_output_bytes),
            ..Default::default()
        }
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn estimated_cardinality(&self) -> usize {
        self.binding.estimated_cardinality
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(ExternalTableGlobalSinkState {
            shared: Arc::new(ExternalTableSharedState::new(self.flow_control.clone())),
        }))
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut sink_state = self.sink_state.lock();
        *sink_state = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().clone()
    }

    fn clear_sink_state(&self) {
        *self.sink_state.lock() = None;
    }

    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(ExternalTableLocalSinkState::new(
            self.child.types(),
            ctx.allocator(MemoryTag::Extension),
        )?))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExternalTableGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("invalid external table sink global state"))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ExternalTableLocalSinkState>()
            .ok_or_else(|| paro_error::internal("invalid external table local sink state"))?;
        self.run_sink_step(
            ctx,
            &gstate.shared,
            lstate,
            Some(chunk),
            false,
            &input.memory,
        )
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExternalTableGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("invalid external table sink global state"))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ExternalTableLocalSinkState>()
            .ok_or_else(|| paro_error::internal("invalid external table local sink state"))?;

        if lstate.combined {
            return Ok(SinkCombineResultType::Finished);
        }

        Ok(
            match self.run_sink_step(ctx, &gstate.shared, lstate, None, true, &input.memory)? {
                SinkResultType::NeedMoreInput | SinkResultType::Finished => {
                    SinkCombineResultType::Finished
                }
                SinkResultType::Blocked => SinkCombineResultType::Blocked,
                SinkResultType::Interrupted => SinkCombineResultType::Interrupted,
            },
        )
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExternalTableGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("invalid external table sink global state"))?;
        gstate.shared.mark_finalized();
        Ok(SinkFinalizeType::Ready)
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        if let Some(sink_state) = sink_state {
            let sink_state = sink_state
                .as_any()
                .downcast_ref::<ExternalTableGlobalSinkState>()
                .ok_or_else(|| {
                    paro_error::internal("invalid external table sink state for source")
                })?;
            return Ok(Box::new(ExternalTableGlobalSourceState {
                shared: sink_state.shared.clone(),
            }));
        }

        let stored = self.sink_state().ok_or_else(|| {
            paro_error::internal("external table requires sink state before source state")
        })?;
        let sink_state = stored
            .as_any()
            .downcast_ref::<ExternalTableGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("invalid stored external table sink state"))?;
        Ok(Box::new(ExternalTableGlobalSourceState {
            shared: sink_state.shared.clone(),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(ExternalTableLocalSourceState))
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
            .downcast_ref::<ExternalTableGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("invalid external table source global state"))?;

        gstate.shared.promote_backlog();
        let Some(batch) = gstate.shared.pop_visible_batch() else {
            chunk.try_reset(chunk.allocator().clone())?;
            return Ok(if gstate.shared.is_finalized() {
                SourceResultType::Finished
            } else {
                SourceResultType::Blocked
            });
        };
        *chunk = batch.chunk;

        Ok(if gstate.shared.has_visible_or_backlog_output() {
            SourceResultType::HaveMoreOutput
        } else if gstate.shared.is_finalized() {
            SourceResultType::Finished
        } else {
            SourceResultType::Blocked
        })
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
    use super::*;
    use crate::operator::external::runtime_bridge::{
        EvaluatingProjectKernel, ExternalRoutineDescriptor, RuntimeBridgeExplainInfo,
        RuntimeBridgeMetrics, RuntimeBridgeOutcome, RuntimeBridgeResponse, RuntimeWarmState,
        TableBridgeKernel,
    };
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::pipeline::build_state::PipelineBuildState;
    use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
    use crate::thread_context::ThreadContext;
    use paro_common::runtime_value::Value;

    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;
    use paro_routine::{
        RoutineCallIdentity, RoutineId, RoutineNullPolicy, RoutineSemantics, RoutineSideEffects,
        RoutineStability, RowSemantics,
    };
    use std::sync::Arc;

    fn test_ctx() -> ExecutionContext<'static> {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
    }

    fn binding() -> ExternalTablePlanBinding {
        ExternalTablePlanBinding {
            routine: ExternalRoutineDescriptor {
                label: "py_expand".to_string(),
                identity: RoutineCallIdentity::Catalog {
                    routine_id: RoutineId::from_raw(7001),
                    generation: 2,
                },
                semantics: RoutineSemantics {
                    stability: RoutineStability::Immutable,
                    null_policy: RoutineNullPolicy::CalledOnNullInput,
                    side_effects: RoutineSideEffects::None,
                    row_semantics: RowSemantics::RelationExpanding,
                    may_block: true,
                },
            },
            worker_output_types: vec![LogicalType::Integer],
            emitted_output_types: vec![LogicalType::Integer],
            argument_count: 1,
            lateral: true,
            parameterized: true,
            estimated_cardinality: 12,
            cost: paro_planner::operator::external_project::ExternalCostEstimate {
                startup_cost: 2.0,
                per_row_cost: 0.2,
                bytes_cost: 0.05,
                queue_risk: 0.4,
            },
        }
    }

    #[derive(Debug)]
    struct ReadyTableKernel;

    impl TableBridgeKernel for ReadyTableKernel {
        fn execute(
            &self,
            _ctx: &ExecutionContext,
            _submission: &crate::operator::external::runtime_bridge::TableSubmission<'_>,
            _memory: &crate::memory_runtime::OperatorMemoryScope<'_>,
        ) -> Result<RuntimeBridgeOutcome> {
            let batches = vec![
                Chunk::from_vectors(
                    vec![paro_common::test_utils::test_i32_vector_with_allocator(
                        &[10],
                        paro_common::test_utils::test_allocator(),
                    )],
                    paro_common::test_utils::test_allocator(),
                ),
                Chunk::from_vectors(
                    vec![paro_common::test_utils::test_i32_vector_with_allocator(
                        &[20],
                        paro_common::test_utils::test_allocator(),
                    )],
                    paro_common::test_utils::test_allocator(),
                ),
                Chunk::from_vectors(
                    vec![paro_common::test_utils::test_i32_vector_with_allocator(
                        &[30],
                        paro_common::test_utils::test_allocator(),
                    )],
                    paro_common::test_utils::test_allocator(),
                ),
            ];
            Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
                metrics: RuntimeBridgeMetrics {
                    output_rows: 3,
                    output_bytes: batches
                        .iter()
                        .map(SubmissionBatchPolicy::estimate_chunk_bytes)
                        .sum(),
                    data_plane_bytes: batches
                        .iter()
                        .map(SubmissionBatchPolicy::estimate_chunk_bytes)
                        .sum(),
                    kernel_time_us: 128,
                    warm_state: RuntimeWarmState::Warm,
                    ..Default::default()
                },
                output_batches: batches,
            }))
        }
    }

    fn bridge(table_kernel: Arc<dyn TableBridgeKernel>) -> Arc<ExternalRuntimeBridge> {
        Arc::new(ExternalRuntimeBridge::new(
            RuntimeBridgeExplainInfo::default_python_process(),
            ExternalDispatchPolicy {
                target_batch_bytes: 1024,
                max_accumulation_bytes: 1024,
                min_batch_rows: 2,
                max_batch_rows: 2,
                max_queue_depth_per_shard: 8,
                local_spin_budget_us: 50,
                worker_acquire_timeout_ms: 500,
                transport_retry_budget: 1,
            },
            Arc::new(EvaluatingProjectKernel),
            table_kernel,
        ))
    }

    #[test]
    fn external_table_promotes_backlog_between_source_reads() {
        let ctx = test_ctx();
        let operator = ExternalTable::new(
            binding(),
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer])),
            bridge(Arc::new(ReadyTableKernel)),
        )
        .with_flow_control(ExternalTableFlowControl {
            max_inflight_output_batches: 1,
            max_inflight_output_bytes: 32,
            credit_granularity_bytes: 16,
        });

        let g_sink: Arc<dyn GlobalSinkState> = operator
            .get_global_sink_state(&ctx)
            .expect("sink global state")
            .into();
        operator.set_sink_state(g_sink.clone());
        let mut l_sink = operator
            .get_local_sink_state(&ctx)
            .expect("local sink state");
        let input = input_chunk(&[1, 2]);
        let mut sink_input =
            OperatorSinkInput::new(g_sink.as_ref(), l_sink.as_mut(), ctx.interrupt_state());
        let sink_result = operator.sink(&ctx, &input, &mut sink_input).expect("sink");
        assert_eq!(sink_result, SinkResultType::NeedMoreInput);

        let mut combine_input =
            OperatorSinkCombineInput::new(g_sink.as_ref(), l_sink.as_mut(), ctx.interrupt_state());
        assert_eq!(
            operator.combine(&ctx, &mut combine_input).expect("combine"),
            SinkCombineResultType::Finished
        );
        assert_eq!(
            operator
                .finalize(&OperatorSinkFinalizeInput::new(
                    g_sink.as_ref(),
                    ctx.interrupt_state(),
                ))
                .expect("finalize"),
            SinkFinalizeType::Ready
        );

        let g_source = operator
            .get_global_source_state(&ctx, Some(g_sink.as_ref()))
            .expect("source state");
        let mut l_source = operator
            .get_local_source_state(&ctx, g_source.as_ref())
            .expect("local source state");
        let mut source_input =
            OperatorSourceInput::new(g_source.as_ref(), l_source.as_mut(), ctx.interrupt_state());
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let first = operator
            .get_data(&ctx, &mut output, &mut source_input)
            .expect("first source batch");
        assert_eq!(first, SourceResultType::HaveMoreOutput);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(10)));

        let second = operator
            .get_data(&ctx, &mut output, &mut source_input)
            .expect("second source batch");
        assert_eq!(second, SourceResultType::HaveMoreOutput);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(20)));

        let third = operator
            .get_data(&ctx, &mut output, &mut source_input)
            .expect("third source batch");
        assert_eq!(third, SourceResultType::Finished);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(30)));
        assert!(!operator.parallel_source());

        let explain = operator.explain_params().join("\n");
        assert!(explain.contains("Output Queue: peak_bytes="));
        let shared = operator.shared_state().expect("shared state");
        assert_eq!(shared.runtime_stats().peak_visible_output_batches, 1);
    }

    #[test]
    fn external_table_builds_pipeline_break() {
        let operator = Arc::new(ExternalTable::new(
            binding(),
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer])),
            bridge(Arc::new(ReadyTableKernel)),
        )) as Arc<dyn PhysicalOperator>;
        let meta = MetaPipeline::new(None, MetaPipelineType::Regular);
        let current = meta.base_pipeline();
        let mut state = PipelineBuildState::new();

        operator.build_pipelines(&operator, &current, &meta, &mut state);

        assert_eq!(
            current.source().expect("current source").operator_type(),
            PhysicalOperatorType::ExternalTable
        );
        let children = meta.children();
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0]
                .base_pipeline()
                .get_sink()
                .expect("child sink")
                .operator_type(),
            PhysicalOperatorType::ExternalTable
        );
    }

    #[test]
    fn parameterized_external_table_keeps_visible_output_before_hidden_correlation_columns() {
        let mut binding = binding();
        binding.emitted_output_types = vec![LogicalType::Integer, LogicalType::Integer];
        let operator = ExternalTable::new(
            binding,
            Arc::new(PhysicalDummyScan::with_types(vec![
                LogicalType::Integer,
                LogicalType::Integer,
            ])),
            bridge(Arc::new(ReadyTableKernel)),
        );

        let input = Chunk::from_vectors(
            vec![
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[7],
                    paro_common::test_utils::test_allocator(),
                ),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[99],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let worker_input = operator
            .extract_worker_input(&input)
            .expect("worker input should extract routine arguments");
        assert_eq!(worker_input.column_count(), 1);
        assert_eq!(worker_input.get_value(0, 0), Some(Value::Integer(7)));

        let response = RuntimeBridgeResponse {
            metrics: RuntimeBridgeMetrics {
                output_rows: 1,
                output_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(&Chunk::from_vectors(
                    vec![paro_common::test_utils::test_i32_vector_with_allocator(
                        &[70],
                        paro_common::test_utils::test_allocator(),
                    )],
                    paro_common::test_utils::test_allocator(),
                )),
                data_plane_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(
                    &Chunk::from_vectors(
                        vec![paro_common::test_utils::test_i32_vector_with_allocator(
                            &[70],
                            paro_common::test_utils::test_allocator(),
                        )],
                        paro_common::test_utils::test_allocator(),
                    ),
                ),
                ..Default::default()
            },
            output_batches: vec![Chunk::from_vectors(
                vec![paro_common::test_utils::test_i32_vector_with_allocator(
                    &[70],
                    paro_common::test_utils::test_allocator(),
                )],
                paro_common::test_utils::test_allocator(),
            )],
        };
        let attached = operator
            .attach_passthrough_columns(&input, response)
            .expect("correlation columns should be appended after worker output");
        assert_eq!(attached.output_batches.len(), 1);
        let batch = &attached.output_batches[0];
        assert_eq!(batch.column_count(), 2);
        assert_eq!(batch.get_value(0, 0), Some(Value::Integer(70)));
        assert_eq!(batch.get_value(1, 0), Some(Value::Integer(99)));
    }

    fn input_chunk(values: &[i32]) -> Chunk {
        Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                values,
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        )
    }
}
