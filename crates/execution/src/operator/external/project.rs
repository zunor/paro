// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::explain::types::ExplainRuntimeStats;
use crate::operator::state::{GlobalOperatorState, OperatorState};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::physical_plan::plan_external_project::ExternalProjectPlanBinding;
use crate::result_type::{OperatorFinalizeResultType, OperatorResultType};

use super::batching::SubmissionBatchPolicy;
use super::project_state::{
    ExternalProjectGlobalState, ExternalProjectSharedState, ExternalProjectState,
    InflightProjectBatch,
};
use super::result_cache::digest_chunk_abi_view;
use super::runtime_bridge::{
    ExternalRuntimeBridge, ProjectSubmission, RuntimeBridgeOutcome, RuntimeBridgeResponse,
};

#[derive(Debug)]
pub struct ExternalProject {
    output_types: Vec<LogicalType>,
    child: Arc<dyn PhysicalOperator>,
    binding: ExternalProjectPlanBinding,
    bridge: Arc<ExternalRuntimeBridge>,
    batch_policy: SubmissionBatchPolicy,
    shared_state: Mutex<Option<Arc<ExternalProjectSharedState>>>,
}

impl ExternalProject {
    pub fn new(
        binding: ExternalProjectPlanBinding,
        child: Arc<dyn PhysicalOperator>,
        bridge: Arc<ExternalRuntimeBridge>,
    ) -> Self {
        let mut output_types = child.types().to_vec();
        output_types.extend(
            binding
                .expressions
                .iter()
                .map(|expression| expression.expression.return_type()),
        );
        let batch_policy = SubmissionBatchPolicy::from_dispatch_policy(bridge.dispatch_policy());

        Self {
            output_types,
            child,
            binding,
            bridge,
            batch_policy,
            shared_state: Mutex::new(None),
        }
    }

    pub fn explain_name(&self) -> &'static str {
        "EXTERNAL_PROJECT"
    }

    fn cache_enabled(&self) -> bool {
        self.binding.routines.iter().all(|routine| {
            matches!(
                routine.semantics.stability,
                paro_routine::RoutineStability::Immutable | paro_routine::RoutineStability::Stable
            ) && matches!(
                routine.semantics.side_effects,
                paro_routine::RoutineSideEffects::None
            )
        })
    }

    fn cache_threshold_us(&self) -> u64 {
        64
    }

    fn shared_state(&self) -> Option<Arc<ExternalProjectSharedState>> {
        self.shared_state.lock().clone()
    }

    fn runtime_stats(&self) -> Option<super::project_state::ExternalProjectRuntimeStats> {
        self.shared_state().map(|shared| shared.runtime_stats())
    }

    fn cache_stats(&self) -> Option<super::result_cache::QueryLocalResultCacheStats> {
        self.shared_state().map(|shared| shared.cache_stats())
    }

    fn estimate_ready_output_bytes(ready_output: &std::collections::VecDeque<Chunk>) -> u64 {
        ready_output
            .iter()
            .map(SubmissionBatchPolicy::estimate_chunk_bytes)
            .sum()
    }

    fn ensure_shared_state(&self) -> Arc<ExternalProjectSharedState> {
        let mut shared_state = self.shared_state.lock();
        shared_state
            .get_or_insert_with(|| {
                Arc::new(ExternalProjectSharedState::new(
                    self.bridge
                        .dispatch_policy()
                        .max_accumulation_bytes
                        .saturating_mul(2),
                ))
            })
            .clone()
    }

    fn take_prefix_batch(
        &self,
        state: &mut ExternalProjectState,
        rows: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Chunk> {
        if rows == 0 || rows > state.accumulation.size() {
            return Err(paro_error::internal(
                "invalid external project batch split".to_string(),
            ));
        }

        let mut batch_view = state.accumulation.clone();
        batch_view.slice_range(0, rows);
        let batch = batch_view.deep_copy_with_allocator(allocator.clone());

        if rows == state.accumulation.size() {
            state.accumulation.reset();
            state.accumulation_bytes = 0;
            return Ok(batch);
        }

        let mut remaining_view = state.accumulation.clone();
        remaining_view.slice_range(rows, state.accumulation.size() - rows);
        state.accumulation = remaining_view.deep_copy_with_allocator(allocator);
        state.accumulation_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&state.accumulation);
        Ok(batch)
    }

    fn stage_input(
        &self,
        shared: &Arc<ExternalProjectSharedState>,
        state: &mut ExternalProjectState,
        input: &Chunk,
    ) {
        if state.current_input_staged || input.is_empty() {
            return;
        }
        state.accumulation.append(input);
        state.accumulation_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&state.accumulation);
        state.current_input_staged = true;
        shared.observe_accumulation_bytes(state.accumulation_bytes);
    }

    fn submit_if_ready(
        &self,
        ctx: &ExecutionContext,
        shared: &Arc<ExternalProjectSharedState>,
        state: &mut ExternalProjectState,
        tail_flush: bool,
    ) -> Result<Option<OperatorResultType>> {
        if !self.batch_policy.should_flush(
            state.accumulation.size(),
            state.accumulation_bytes,
            tail_flush,
            self.child.types(),
        ) {
            return Ok(None);
        }

        let allocator = ctx.allocator(MemoryTag::Extension);
        let target_rows = self
            .batch_policy
            .suggest_batch_rows(self.child.types(), state.accumulation_bytes)
            .min(state.accumulation.size())
            .max(1);
        let input_batch = self.take_prefix_batch(state, target_rows, allocator)?;
        shared.observe_accumulation_bytes(state.accumulation_bytes);

        let cache_key = self.cache_enabled().then(|| {
            digest_chunk_abi_view(
                &input_batch,
                self.binding
                    .routines
                    .iter()
                    .map(|routine| routine.identity.clone())
                    .collect(),
            )
        });

        if let Some(cache_key) = cache_key.as_ref() {
            if let Some(generated) = shared.cache_lookup(cache_key) {
                let response = RuntimeBridgeResponse {
                    metrics: super::runtime_bridge::RuntimeBridgeMetrics {
                        cache_hit: true,
                        output_rows: generated.size() as u64,
                        output_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(&generated),
                        data_plane_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(&generated),
                        ..Default::default()
                    },
                    output_batches: vec![generated],
                };
                self.finish_project_response(
                    ctx,
                    shared,
                    state,
                    input_batch,
                    response,
                    false,
                    false,
                )?;
                return Ok(None);
            }
        }

        let input_bytes = SubmissionBatchPolicy::estimate_chunk_bytes(&input_batch);
        let submission = ProjectSubmission {
            batch_id: state.next_batch_id,
            input: &input_batch,
            expressions: &self.binding.expressions,
            routines: &self.binding.routines,
            force_tail_flush: tail_flush,
            batch_policy: &self.batch_policy,
        };
        state.next_batch_id = state.next_batch_id.saturating_add(1);

        match self.bridge.execute_project(ctx, &submission)? {
            RuntimeBridgeOutcome::Ready(response) => {
                shared.record_submission(input_batch.size(), input_bytes, false, &response);
                self.finish_project_response(
                    ctx,
                    shared,
                    state,
                    input_batch,
                    response,
                    true,
                    false,
                )?;
                Ok(None)
            }
            RuntimeBridgeOutcome::Blocked(response) => {
                shared.record_submission(input_batch.size(), input_bytes, true, &response);
                state.inflight = Some(InflightProjectBatch {
                    input_batch,
                    input_bytes,
                    response,
                    cache_key,
                    cache_candidate: true,
                });
                let _ = ctx.interrupt_state().callback();
                Ok(Some(OperatorResultType::Blocked))
            }
        }
    }

    fn finish_project_response(
        &self,
        ctx: &ExecutionContext,
        shared: &Arc<ExternalProjectSharedState>,
        state: &mut ExternalProjectState,
        input_batch: Chunk,
        response: RuntimeBridgeResponse,
        allow_cache_insert: bool,
        blocked_path: bool,
    ) -> Result<()> {
        if response.output_batches.len() != 1 {
            return Err(paro_error::internal(
                "external project bridge must return exactly one generated batch".to_string(),
            ));
        }
        let generated = response
            .output_batches
            .first()
            .expect("generated batch should exist");
        if generated.size() != input_batch.size() {
            return Err(paro_error::internal(format!(
                "external project bridge returned {} rows for {} input rows",
                generated.size(),
                input_batch.size()
            )));
        }

        let cache_candidate = allow_cache_insert
            && self.cache_enabled()
            && !response.metrics.cache_hit
            && response
                .metrics
                .kernel_time_us
                .saturating_add(response.metrics.queue_wait_us)
                >= self.cache_threshold_us();

        if cache_candidate {
            let cache_key = digest_chunk_abi_view(
                &input_batch,
                self.binding
                    .routines
                    .iter()
                    .map(|routine| routine.identity.clone())
                    .collect(),
            );
            let _ = shared.cache_insert(
                cache_key,
                generated.deep_copy_with_allocator(ctx.allocator(MemoryTag::Extension)),
                SubmissionBatchPolicy::estimate_chunk_bytes(generated),
            );
        }

        let final_output =
            self.assemble_output(&input_batch, generated, ctx.allocator(MemoryTag::Extension))?;
        let mut ready_batches = self
            .batch_policy
            .rechunk_output(&final_output, ctx.allocator(MemoryTag::Extension));
        state.ready_output.append(&mut ready_batches);
        state.ready_output_bytes = Self::estimate_ready_output_bytes(&state.ready_output);
        shared.observe_ready_output_bytes(state.ready_output_bytes);

        if blocked_path {
            state.inflight = None;
        }
        Ok(())
    }

    fn collect_inflight_if_needed(
        &self,
        ctx: &ExecutionContext,
        shared: &Arc<ExternalProjectSharedState>,
        state: &mut ExternalProjectState,
    ) -> Result<()> {
        let Some(inflight) = state.inflight.take() else {
            return Ok(());
        };

        if inflight.cache_candidate && !inflight.response.metrics.cache_hit {
            if let Some(cache_key) = inflight.cache_key.clone() {
                if let Some(generated) = inflight.response.output_batches.first() {
                    let _ = shared.cache_insert(
                        cache_key,
                        generated.deep_copy_with_allocator(ctx.allocator(MemoryTag::Extension)),
                        SubmissionBatchPolicy::estimate_chunk_bytes(generated),
                    );
                }
            }
        }

        self.finish_project_response(
            ctx,
            shared,
            state,
            inflight.input_batch,
            inflight.response,
            false,
            true,
        )
    }

    fn assemble_output(
        &self,
        passthrough: &Chunk,
        generated: &Chunk,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Chunk> {
        if passthrough.size() != generated.size() {
            return Err(paro_error::internal(
                "external project passthrough/generated row mismatch".to_string(),
            ));
        }

        let mut passthrough_chunk =
            Chunk::init_empty_with_allocator(passthrough.types().as_slice(), allocator.clone());
        passthrough_chunk.reference(passthrough);
        let mut generated_chunk =
            Chunk::init_empty_with_allocator(generated.types().as_slice(), allocator);
        generated_chunk.reference(generated);
        passthrough_chunk.fuse(&mut generated_chunk);
        Ok(passthrough_chunk)
    }

    fn pop_ready_output(
        &self,
        shared: &Arc<ExternalProjectSharedState>,
        state: &mut ExternalProjectState,
        chunk: &mut Chunk,
        tail_flush: bool,
    ) -> Option<Result<OperatorFlow>> {
        let output = state.ready_output.pop_front()?;
        state.ready_output_bytes = Self::estimate_ready_output_bytes(&state.ready_output);
        shared.observe_ready_output_bytes(state.ready_output_bytes);
        *chunk = output;

        if !tail_flush
            && state.ready_output.is_empty()
            && state.inflight.is_none()
            && !self.batch_policy.should_flush(
                state.accumulation.size(),
                state.accumulation_bytes,
                false,
                self.child.types(),
            )
        {
            state.current_input_staged = false;
            return Some(Ok(OperatorFlow::NeedMoreInput));
        }

        if tail_flush
            && state.ready_output.is_empty()
            && state.inflight.is_none()
            && state.accumulation.is_empty()
        {
            return Some(Ok(OperatorFlow::Finished));
        }

        Some(Ok(OperatorFlow::HaveMoreOutput))
    }

    fn run_step(
        &self,
        ctx: &ExecutionContext,
        shared: &Arc<ExternalProjectSharedState>,
        state: &mut ExternalProjectState,
        input: Option<&Chunk>,
        chunk: &mut Chunk,
        tail_flush: bool,
    ) -> Result<OperatorFlow> {
        if let Some(input) = input {
            self.stage_input(shared, state, input);
        }

        loop {
            self.collect_inflight_if_needed(ctx, shared, state)?;

            if let Some(result) = self.pop_ready_output(shared, state, chunk, tail_flush) {
                return result;
            }

            if let Some(blocked) = self.submit_if_ready(ctx, shared, state, tail_flush)? {
                return Ok(match blocked {
                    OperatorResultType::Blocked => OperatorFlow::Blocked,
                    _ => unreachable!("submit_if_ready only returns blocked flow"),
                });
            }

            if !state.ready_output.is_empty() || state.inflight.is_some() {
                continue;
            }

            if tail_flush {
                return Ok(OperatorFlow::Finished);
            }

            state.current_input_staged = false;
            *chunk = Chunk::init_empty_with_allocator(
                &self.output_types,
                ctx.allocator(MemoryTag::Extension),
            );
            return Ok(OperatorFlow::NeedMoreInput);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorFlow {
    NeedMoreInput,
    HaveMoreOutput,
    Blocked,
    Finished,
}

impl PhysicalOperator for ExternalProject {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ExternalProject
    }

    fn explain_name(&self) -> String {
        self.explain_name().to_string()
    }

    fn explain_params(&self) -> Vec<String> {
        let explain = self.bridge.explain();
        let mut params = vec![
            format!(
                "Routines: {}",
                self.binding
                    .routines
                    .iter()
                    .map(|routine| routine.identity_label())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            format!("Language: {}", explain.language),
            format!("Backend: {}", explain.backend),
            format!(
                "Batch Target: {} bytes (max accumulation {} bytes)",
                self.batch_policy.target_batch_bytes, self.batch_policy.max_accumulation_bytes
            ),
            format!(
                "Env Artifact: {}",
                explain
                    .env_artifact_id
                    .clone()
                    .unwrap_or_else(|| "unresolved".to_string())
            ),
            format!("Artifact Validation: {}", explain.artifact_validation_state),
            "Batching Contract: child chunk -> runtime submission -> engine emission".to_string(),
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
            }
        }

        if let Some(cache_stats) = self.cache_stats() {
            if cache_stats.hits > 0
                || cache_stats.misses > 0
                || cache_stats.admissions > 0
                || cache_stats.evictions > 0
            {
                params.push(format!(
                    "Cache: hits={} misses={} admissions={} evictions={} resident={} bytes",
                    cache_stats.hits,
                    cache_stats.misses,
                    cache_stats.admissions,
                    cache_stats.evictions,
                    cache_stats.resident_bytes
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
        let cache = shared.cache_stats();
        ExplainRuntimeStats {
            spilled: Some(false),
            peak_memory_bytes: Some(
                stats
                    .peak_accumulation_bytes
                    .saturating_add(stats.peak_ready_output_bytes)
                    .saturating_add(cache.resident_bytes),
            ),
            temp_storage_bytes: None,
        }
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn estimated_cardinality(&self) -> usize {
        self.child.estimated_cardinality()
    }

    fn parallel_operator(&self) -> bool {
        true
    }

    fn requires_final_execute(&self) -> bool {
        true
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

    fn get_operator_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(ExternalProjectState::new(
            self.child.types(),
            ctx.allocator(MemoryTag::Extension),
        )))
    }

    fn get_global_operator_state(&self) -> Result<Box<dyn GlobalOperatorState>> {
        Ok(Box::new(ExternalProjectGlobalState {
            shared: self.ensure_shared_state(),
        }))
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorResultType> {
        let gstate = gstate
            .as_any()
            .downcast_ref::<ExternalProjectGlobalState>()
            .ok_or_else(|| paro_error::internal("invalid external project global state"))?;
        let state = state
            .as_any_mut()
            .downcast_mut::<ExternalProjectState>()
            .ok_or_else(|| paro_error::internal("invalid external project state"))?;

        Ok(
            match self.run_step(ctx, &gstate.shared, state, Some(input), chunk, false)? {
                OperatorFlow::NeedMoreInput => OperatorResultType::NeedMoreInput,
                OperatorFlow::HaveMoreOutput => OperatorResultType::HaveMoreOutput,
                OperatorFlow::Blocked => OperatorResultType::Blocked,
                OperatorFlow::Finished => OperatorResultType::Finished,
            },
        )
    }

    fn final_execute(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorFinalizeResultType> {
        let gstate = gstate
            .as_any()
            .downcast_ref::<ExternalProjectGlobalState>()
            .ok_or_else(|| paro_error::internal("invalid external project global state"))?;
        let state = state
            .as_any_mut()
            .downcast_mut::<ExternalProjectState>()
            .ok_or_else(|| paro_error::internal("invalid external project state"))?;

        Ok(
            match self.run_step(ctx, &gstate.shared, state, None, chunk, true)? {
                OperatorFlow::NeedMoreInput | OperatorFlow::Finished => {
                    OperatorFinalizeResultType::Finished
                }
                OperatorFlow::HaveMoreOutput => OperatorFinalizeResultType::HaveMoreOutput,
                OperatorFlow::Blocked => OperatorFinalizeResultType::Blocked,
            },
        )
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
        ExternalRoutineDescriptor, ProjectBridgeKernel, RuntimeBridgeMetrics, RuntimeBridgeOutcome,
        RuntimeBridgeResponse, RuntimeWarmState, UnboundTableKernel,
    };
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::thread_context::ThreadContext;
    use paro_common::runtime_value::Value;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_external_runtime::dispatch::policy::ExternalDispatchPolicy;
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{Expression, FunctionExpression, ReferenceExpression};
    use paro_planner::operator::external_project::{
        ExternalCostEstimate, ExternalProjectExpression,
    };
    use paro_routine::{
        BoundRoutineCallMeta, ExecutionBoundary, PlacementClass, RoutineCallIdentity, RoutineId,
        RoutineNullPolicy, RoutineSemantics, RoutineSideEffects, RoutineStability, RowSemantics,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    fn test_ctx() -> ExecutionContext<'static> {
        let session: Arc<StatementContext> = TestStatementContextBuilder::minimal().build();
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
    }

    fn stable_semantics() -> RoutineSemantics {
        RoutineSemantics {
            stability: RoutineStability::Immutable,
            null_policy: RoutineNullPolicy::CalledOnNullInput,
            side_effects: RoutineSideEffects::None,
            row_semantics: RowSemantics::RowPreserving,
            may_block: false,
        }
    }

    fn external_expression() -> ExternalProjectExpression {
        fn passthrough(
            input: &Chunk,
            _ctx: &dyn paro_function::scalar::FunctionExecContext,
            result: &mut paro_common::vector::Vector,
        ) -> paro_common::error::Result<()> {
            let column = input.column(0).expect("input column");
            for row_idx in 0..input.size() {
                result.set_i32(row_idx, column.get_i32(row_idx).expect("non-null"));
            }
            Ok(())
        }

        let semantics = stable_semantics();
        let routine_meta = BoundRoutineCallMeta {
            identity: RoutineCallIdentity::Catalog {
                routine_id: RoutineId::from_raw(9001),
                generation: 7,
            },
            semantics: semantics.clone(),
            boundary: ExecutionBoundary {
                placement: PlacementClass::External,
                may_block: false,
                row_semantics: RowSemantics::RowPreserving,
            },
            spec: None,
        };
        ExternalProjectExpression {
            output_name: "__ext".to_string(),
            expression: Expression::Function(
                FunctionExpression::new(
                    ScalarFunction::new(
                        "py_score".to_string(),
                        vec![LogicalType::Integer],
                        LogicalType::Integer,
                        passthrough,
                    ),
                    vec![Expression::Reference(ReferenceExpression::new(
                        0,
                        LogicalType::Integer,
                    ))],
                    LogicalType::Integer,
                )
                .with_routine_meta(routine_meta.clone()),
            ),
            routine_meta,
        }
    }

    fn binding() -> ExternalProjectPlanBinding {
        let expression = external_expression();
        ExternalProjectPlanBinding {
            routines: vec![ExternalRoutineDescriptor {
                label: "py_score".to_string(),
                identity: expression.routine_meta.identity.clone(),
                semantics: expression.routine_meta.semantics.clone(),
            }],
            expressions: vec![expression],
            cost: ExternalCostEstimate {
                startup_cost: 1.0,
                per_row_cost: 0.1,
                bytes_cost: 0.01,
                queue_risk: 0.2,
            },
        }
    }

    fn input_chunk(values: &[i32]) -> Chunk {
        Chunk::from_vectors(vec![Vector::from_i32(values)])
    }

    #[derive(Debug)]
    struct BlockingProjectKernel {
        seen_rows: Arc<StdMutex<Vec<usize>>>,
        blocked_once: AtomicBool,
        kernel_time_us: u64,
    }

    impl BlockingProjectKernel {
        fn new(seen_rows: Arc<StdMutex<Vec<usize>>>, kernel_time_us: u64) -> Self {
            Self {
                seen_rows,
                blocked_once: AtomicBool::new(false),
                kernel_time_us,
            }
        }

        fn generated_chunk(input: &Chunk) -> Chunk {
            let column = input.column(0).expect("input column");
            let values = (0..input.size())
                .map(|row_idx| column.get_i32(row_idx).expect("non-null") * 2)
                .collect::<Vec<_>>();
            Chunk::from_vectors(vec![Vector::from_i32(&values)])
        }
    }

    impl ProjectBridgeKernel for BlockingProjectKernel {
        fn execute(
            &self,
            _ctx: &ExecutionContext,
            submission: &crate::operator::external::runtime_bridge::ProjectSubmission<'_>,
        ) -> Result<RuntimeBridgeOutcome> {
            self.seen_rows
                .lock()
                .expect("rows lock")
                .push(submission.input.size());
            let output = Self::generated_chunk(submission.input);
            let response = RuntimeBridgeResponse {
                output_batches: vec![output.clone()],
                metrics: RuntimeBridgeMetrics {
                    kernel_time_us: self.kernel_time_us,
                    output_rows: output.size() as u64,
                    output_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(&output),
                    data_plane_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(submission.input)
                        .saturating_add(SubmissionBatchPolicy::estimate_chunk_bytes(&output)),
                    warm_state: RuntimeWarmState::Warm,
                    ..Default::default()
                },
            };
            if !self.blocked_once.swap(true, Ordering::SeqCst) {
                Ok(RuntimeBridgeOutcome::Blocked(response))
            } else {
                Ok(RuntimeBridgeOutcome::Ready(response))
            }
        }
    }

    fn test_bridge(project_kernel: Arc<dyn ProjectBridgeKernel>) -> Arc<ExternalRuntimeBridge> {
        Arc::new(ExternalRuntimeBridge::new(
            crate::operator::external::runtime_bridge::RuntimeBridgeExplainInfo::default_python_process(),
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
            project_kernel,
            Arc::new(UnboundTableKernel),
        ))
    }

    #[test]
    fn external_project_blocks_resumes_and_tail_flushes() {
        let ctx = test_ctx();
        let seen_rows = Arc::new(StdMutex::new(Vec::new()));
        let operator = ExternalProject::new(
            binding(),
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer])),
            test_bridge(Arc::new(BlockingProjectKernel::new(seen_rows.clone(), 128))),
        );
        let gstate = operator.get_global_operator_state().expect("global state");
        let mut state = operator.get_operator_state(&ctx).expect("operator state");

        let input = input_chunk(&[1, 2, 3]);
        let mut output = Chunk::new();

        let first = operator
            .execute(&ctx, &input, &mut output, gstate.as_ref(), state.as_mut())
            .expect("first execute");
        assert_eq!(first, OperatorResultType::Blocked);
        let blocked_state = state
            .as_any()
            .downcast_ref::<ExternalProjectState>()
            .expect("external project state");
        assert_eq!(blocked_state.accumulation.size(), 1);

        let second = operator
            .execute(&ctx, &input, &mut output, gstate.as_ref(), state.as_mut())
            .expect("resume execute");
        assert_eq!(second, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 2);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(1)));
        assert_eq!(output.get_value(1, 1), Some(Value::Integer(4)));
        let state_ref = state
            .as_any()
            .downcast_ref::<ExternalProjectState>()
            .expect("external project state");
        assert_eq!(state_ref.accumulation.size(), 1);

        let mut tail_output = Chunk::new();
        let final_result = operator
            .final_execute(&ctx, &mut tail_output, gstate.as_ref(), state.as_mut())
            .expect("tail flush");
        assert_eq!(final_result, OperatorFinalizeResultType::Finished);
        assert_eq!(tail_output.size(), 1);
        assert_eq!(tail_output.get_value(0, 0), Some(Value::Integer(3)));
        assert_eq!(tail_output.get_value(1, 0), Some(Value::Integer(6)));
        assert_eq!(*seen_rows.lock().expect("rows lock"), vec![2, 1]);
    }

    #[derive(Debug)]
    struct CountingProjectKernel {
        calls: Arc<AtomicUsize>,
        kernel_time_us: u64,
    }

    impl ProjectBridgeKernel for CountingProjectKernel {
        fn execute(
            &self,
            _ctx: &ExecutionContext,
            submission: &crate::operator::external::runtime_bridge::ProjectSubmission<'_>,
        ) -> Result<RuntimeBridgeOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let output = BlockingProjectKernel::generated_chunk(submission.input);
            Ok(RuntimeBridgeOutcome::Ready(RuntimeBridgeResponse {
                output_batches: vec![output.clone()],
                metrics: RuntimeBridgeMetrics {
                    kernel_time_us: self.kernel_time_us,
                    output_rows: output.size() as u64,
                    output_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(&output),
                    data_plane_bytes: SubmissionBatchPolicy::estimate_chunk_bytes(submission.input)
                        .saturating_add(SubmissionBatchPolicy::estimate_chunk_bytes(&output)),
                    warm_state: RuntimeWarmState::Warm,
                    ..Default::default()
                },
            }))
        }
    }

    #[test]
    fn external_project_query_local_cache_reuses_cached_result() {
        let ctx = test_ctx();
        let calls = Arc::new(AtomicUsize::new(0));
        let operator = ExternalProject::new(
            binding(),
            Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer])),
            test_bridge(Arc::new(CountingProjectKernel {
                calls: calls.clone(),
                kernel_time_us: 256,
            })),
        );
        let gstate = operator.get_global_operator_state().expect("global state");
        let mut state = operator.get_operator_state(&ctx).expect("operator state");

        let input = input_chunk(&[7, 8]);
        let mut output = Chunk::new();

        let first = operator
            .execute(&ctx, &input, &mut output, gstate.as_ref(), state.as_mut())
            .expect("first execute");
        assert_eq!(first, OperatorResultType::NeedMoreInput);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second = operator
            .execute(&ctx, &input, &mut output, gstate.as_ref(), state.as_mut())
            .expect("second execute");
        assert!(matches!(
            second,
            OperatorResultType::NeedMoreInput | OperatorResultType::HaveMoreOutput
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(output.get_value(1, 0), Some(Value::Integer(14)));

        if second == OperatorResultType::HaveMoreOutput {
            let mut followup = Chunk::new();
            let third = operator
                .execute(&ctx, &input, &mut followup, gstate.as_ref(), state.as_mut())
                .expect("drain followup");
            assert_eq!(third, OperatorResultType::NeedMoreInput);
            assert_eq!(followup.size(), 0);
        }

        let explain = operator.explain_params().join("\n");
        assert!(explain.contains("Cache: hits=1"));
    }
}
