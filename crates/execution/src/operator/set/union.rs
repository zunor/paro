// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Union Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ Uses ExecutionContext allocator
//! - MetaPipeline: ✅ Uses for pipeline construction
//!
//! ## Known Limitations
//! - No out-of-order execution optimization
//! - No batch index support
//! - Simple sequential execution for MVP
//!
//! ## Design Notes
//! Union is a special operator that combines results from multiple children.
//! Unlike most operators, it doesn't use the standard Source/Sink interface.
//! Instead, it creates multiple pipelines during BuildPipelines phase.
//!
//! For UNION ALL: Simply concatenate results from all children.
//! For UNION (distinct): Add a HashAggregate on top to remove duplicates.
//!
//! For MVP, we implement a simpler approach using a Source interface that
//! iterates through children sequentially.

use std::any::Any;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSourceState, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::SourceResultType;

/// Physical Union operator.
///
/// Combines results from multiple child operators (UNION ALL).
/// For UNION (distinct), a HashAggregate is added on top during plan generation.
#[derive(Debug)]
pub struct Union {
    /// Output types (same as children).
    pub types: Vec<LogicalType>,
    /// Materialized scan children used at execution time.
    pub children: Vec<Arc<dyn PhysicalOperator>>,
    /// Child plans that feed the materialization pipelines.
    pub materialized_inputs: Vec<Arc<dyn PhysicalOperator>>,
    /// Sink operators that materialize each child into a scan binding.
    pub materialized_sinks: Vec<Arc<dyn PhysicalOperator>>,
    /// Whether out-of-order execution is allowed.
    pub allow_out_of_order: bool,
}

impl Union {
    /// Create a new Union operator.
    ///
    /// # Arguments
    /// * `types` - Output types (must match all children)
    /// * `children` - Child operators to union
    /// * `allow_out_of_order` - Whether results can be returned out of order
    pub fn new(
        types: Vec<LogicalType>,
        children: Vec<Arc<dyn PhysicalOperator>>,
        materialized_inputs: Vec<Arc<dyn PhysicalOperator>>,
        materialized_sinks: Vec<Arc<dyn PhysicalOperator>>,
        allow_out_of_order: bool,
    ) -> Self {
        Self {
            types,
            children,
            materialized_inputs,
            materialized_sinks,
            allow_out_of_order,
        }
    }
}

// ========== States ==========

/// Global source state for union operation.
#[derive(Debug)]
pub struct UnionGlobalSourceState {
    /// Child source states (one per child).
    child_source_states: Mutex<Vec<Box<dyn GlobalSourceState>>>,
    /// Whether each child is finished.
    child_finished: Mutex<Vec<bool>>,
}

impl UnionGlobalSourceState {
    fn new(num_children: usize) -> Self {
        Self {
            child_source_states: Mutex::new(Vec::with_capacity(num_children)),
            child_finished: Mutex::new(vec![false; num_children]),
        }
    }
}

impl GlobalSourceState for UnionGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for union operation.
#[derive(Debug)]
pub struct UnionLocalSourceState {
    /// Current child index for this thread.
    current_child: usize,
    /// Child local source states.
    child_local_states: Vec<Box<dyn LocalSourceState>>,
}

impl LocalSourceState for UnionLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== PhysicalOperator Implementation ==========

impl PhysicalOperator for Union {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Union
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = vec!["Union Type: ALL".to_string()];
        if !self.allow_out_of_order {
            params.push("Order: PRESERVED".to_string());
        }
        params
    }

    fn children_count(&self) -> usize {
        self.children.len()
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        self.children.get(index).map(|c| c.as_ref())
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        self.children.get(index).cloned()
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        // For MVP, we don't support parallel source
        // In a full implementation, each child could be processed in parallel
        false
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let gstate = UnionGlobalSourceState::new(self.children.len());

        // Initialize child source states
        {
            let mut child_states = gstate
                .child_source_states
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;

            for child in &self.children {
                child_states.push(child.get_global_source_state(_ctx, None)?);
            }
        }

        Ok(Box::new(gstate))
    }

    fn get_local_source_state(
        &self,
        ctx: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        let gstate = gstate
            .as_any()
            .downcast_ref::<UnionGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;

        let child_gstates = gstate
            .child_source_states
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;

        let mut child_local_states = Vec::with_capacity(self.children.len());
        for (i, child) in self.children.iter().enumerate() {
            if i < child_gstates.len() {
                child_local_states
                    .push(child.get_local_source_state(ctx, child_gstates[i].as_ref())?);
            }
        }

        Ok(Box::new(UnionLocalSourceState {
            current_child: 0,
            child_local_states,
        }))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<UnionGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<UnionLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        // Try to get data from current child
        loop {
            let current_child = lstate.current_child;

            if current_child >= self.children.len() {
                // All children exhausted
                return Ok(SourceResultType::Finished);
            }

            let child = &self.children[current_child];

            // Check if this child is a source
            if !child.is_source() {
                // Skip non-source children (shouldn't happen in normal usage)
                lstate.current_child += 1;
                continue;
            }

            // Get child's global and local state
            let child_gstates = gstate
                .child_source_states
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;

            if current_child >= child_gstates.len()
                || current_child >= lstate.child_local_states.len()
            {
                lstate.current_child += 1;
                continue;
            }

            // Create input for child
            let mut child_input = OperatorSourceInput::with_memory(
                child_gstates[current_child].as_ref(),
                lstate.child_local_states[current_child].as_mut(),
                input.interrupt_state,
                input.memory.child_scope(),
            );

            // Get data from child
            let result = child.get_data(ctx, chunk, &mut child_input)?;

            match result {
                SourceResultType::HaveMoreOutput => {
                    return Ok(SourceResultType::HaveMoreOutput);
                }
                SourceResultType::Finished => {
                    // Move to next child
                    lstate.current_child += 1;

                    // Mark this child as finished
                    let mut finished = gstate
                        .child_finished
                        .lock()
                        .map_err(|e| paro_error::internal(e.to_string()))?;
                    if current_child < finished.len() {
                        finished[current_child] = true;
                    }

                    // If we have data in the chunk, we must return HaveMoreOutput
                    // even if this child is finished, so the caller can process the data.
                    if chunk.size() > 0 {
                        return Ok(SourceResultType::HaveMoreOutput);
                    }

                    // Otherwise continue to try next child
                    continue;
                }
                SourceResultType::Blocked => {
                    return Ok(SourceResultType::Blocked);
                }
            }
        }
    }

    // ========== Pipeline Construction ==========

    /// Build pipelines for union operator.
    ///
    /// For MVP, we use a simple approach where the union operator acts as a source
    /// that iterates through children sequentially.
    ///
    /// that feeds into the same sink, allowing parallel execution.
    fn build_pipelines(
        &self,
        self_arc: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        for (child, sink) in self
            .materialized_inputs
            .iter()
            .zip(self.materialized_sinks.iter())
        {
            let child_meta = meta_pipeline.create_child_meta_pipeline(
                current,
                sink.clone(),
                crate::pipeline::meta_pipeline::MetaPipelineType::Regular,
            );
            child_meta.build(child, state);
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
