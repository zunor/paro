// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Default pipeline construction for physical operators.

use std::sync::Arc;

use crate::operator::PhysicalOperator;

use super::meta_pipeline::{MetaPipeline, MetaPipelineType};
use super::pipeline::Pipeline;
use crate::pipeline::build_state::PipelineBuildState;

/// Build pipelines for a dynamic PhysicalOperator.
///
/// This is the main entry point for pipeline construction from a trait object.
///
/// # Pipeline Construction Rules
///
/// 1. **Source-only operators** (leaf nodes): Set as the pipeline source
/// 2. **Sink+Source operators** (e.g., HashAggregate):
///    - Becomes the source of the current pipeline
///    - Creates a child MetaPipeline where it acts as the sink
/// 3. **Regular operators**: Add to the current pipeline and recurse
///
/// Note: Sink-only operators (e.g., INSERT) are handled at the Executor level
/// by setting them as the root MetaPipeline's sink before building.
pub fn build_pipelines_default(
    op: &Arc<dyn PhysicalOperator>,
    current: &Arc<Pipeline>,
    meta_pipeline: &Arc<MetaPipeline>,
    state: &mut PipelineBuildState,
) {
    // Case 1: Source-only operator (leaf node, not a sink)
    if !op.is_sink() && op.children_count() == 0 {
        state.set_pipeline_source(current, op.clone());
        return;
    }

    // Case 2: Sink+Source operator (e.g., HashAggregate)
    // This operator acts as a pipeline breaker:
    // - It becomes the source of the current pipeline
    // - A child MetaPipeline is created where it acts as the sink
    if op.is_sink() && op.is_source() {
        // This operator becomes the source of the current pipeline
        state.set_pipeline_source(current, op.clone());

        // Create a child MetaPipeline for the subtree
        // The child MetaPipeline has this operator as its sink
        let child_meta = meta_pipeline.create_child_meta_pipeline(
            current,
            op.clone(),
            MetaPipelineType::Regular,
        );

        // Build the child into the child MetaPipeline
        if let Some(child) = op.child_arc(0) {
            child_meta.build(&child, state);
        }
        return;
    }

    // Case 3: Regular operator (not source, not sink)
    // Must have exactly one child for default implementation
    if op.children_count() != 1 {
        // Multi-child operators (Join, Union) need custom handling
        // For now, we panic - these should be handled by specific implementations
        panic!(
            "Operator {:?} has {} children, but default BuildPipelines only supports unary operators. \
             Multi-child operators need custom pipeline construction.",
            op.operator_type(),
            op.children_count()
        );
    }

    // Recurse into the child
    if let Some(child) = op.child_arc(0) {
        child.build_pipelines(&child, current, meta_pipeline, state);
    }

    // Add this operator to the pipeline after its child is already built
    current.add_operator(op.clone());
    meta_pipeline.propagate_operator_to_dependents(current, op.clone());
}
