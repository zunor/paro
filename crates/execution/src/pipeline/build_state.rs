// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # PipelineBuildState
//!
//! State maintained during pipeline construction.
//!
//!
//! ## Dependencies Check
//! - Pipeline: ✅ Using paro-execution::pipeline
//! - PhysicalOperator: ✅ Using paro-execution
//!
//! - Tracks global state during pipeline construction
//! - Manages dependencies for special operators (DelimJoin, CTE)
//! - Provides helper methods for setting pipeline source/sink/operators
//!
//! ## Progress
//! - Decoupled Pipeline states: Optimized for lazy initialization

use std::collections::HashMap;
use std::sync::Arc;

use crate::operator::PhysicalOperator;

use super::pipeline::Pipeline;

/// State maintained during pipeline construction.
///
/// PipelineBuildState tracks global information needed when building pipelines,
/// including dependencies for special operators like duplicate-eliminated joins
/// and materialized CTEs.
#[derive(Default)]
pub struct PipelineBuildState {
    /// Duplicate eliminated join scan dependencies.
    ///
    /// Maps a DelimJoin operator to the pipeline that produces its deduplicated data.
    /// The scan side of a DelimJoin must wait for this pipeline to complete.
    delim_join_dependencies: HashMap<usize, Arc<Pipeline>>,

    /// Materialized CTE scan dependencies.
    ///
    /// Maps a CTE scan operator to the pipeline that materializes the CTE.
    /// CTE scans must wait for materialization to complete.
    cte_dependencies: HashMap<usize, Arc<Pipeline>>,
}

impl PipelineBuildState {
    /// How much to increment batch indexes when multiple pipelines share the same source.
    /// This ensures batch indexes don't overlap across pipelines.
    pub const BATCH_INCREMENT: usize = 10_000_000_000_000;
    /// Create a new empty build state.
    pub fn new() -> Self {
        Self {
            delim_join_dependencies: HashMap::new(),
            cte_dependencies: HashMap::new(),
        }
    }

    // ========== Pipeline Configuration ==========

    /// Set the source operator for a pipeline.
    pub fn set_pipeline_source(&self, pipeline: &Pipeline, op: Arc<dyn PhysicalOperator>) {
        pipeline.set_source(op);
    }

    /// Set the sink operator for a pipeline.
    ///
    /// # Arguments
    /// * `pipeline` - The pipeline to configure
    /// * `op` - The sink operator (None for pipelines that output to result collector)
    /// * `sink_pipeline_count` - Number of pipelines sharing this sink (for batch index)
    pub fn set_pipeline_sink(
        &self,
        pipeline: &Pipeline,
        op: Option<Arc<dyn PhysicalOperator>>,
        sink_pipeline_count: usize,
    ) {
        if let Some(sink) = op {
            pipeline.set_sink(sink);
        } else {
            pipeline.clear_sink();
        }
        pipeline.set_batch_index(Self::BATCH_INCREMENT * sink_pipeline_count);
    }

    /// Add an operator to the pipeline's operator chain.
    pub fn add_pipeline_operator(&self, pipeline: &Pipeline, op: Arc<dyn PhysicalOperator>) {
        pipeline.add_operator(op);
    }

    /// Set all operators for a pipeline at once.
    pub fn set_pipeline_operators(
        &self,
        pipeline: &Pipeline,
        operators: Vec<Arc<dyn PhysicalOperator>>,
    ) {
        pipeline.set_operators(operators);
    }

    /// Get the source operator of a pipeline.
    pub fn get_pipeline_source(&self, pipeline: &Pipeline) -> Option<Arc<dyn PhysicalOperator>> {
        pipeline.source()
    }

    /// Get the sink operator of a pipeline.
    pub fn get_pipeline_sink(&self, pipeline: &Pipeline) -> Option<Arc<dyn PhysicalOperator>> {
        pipeline.get_sink()
    }

    /// Get the intermediate operators of a pipeline.
    pub fn get_pipeline_operators(&self, pipeline: &Pipeline) -> Vec<Arc<dyn PhysicalOperator>> {
        pipeline.get_operators()
    }

    // ========== Dependency Management ==========

    /// Register a duplicate-eliminated join dependency.
    ///
    /// The operator identified by `op_id` must wait for `pipeline` to complete
    /// before it can scan the deduplicated data.
    pub fn add_delim_join_dependency(&mut self, op_id: usize, pipeline: Arc<Pipeline>) {
        self.delim_join_dependencies.insert(op_id, pipeline);
    }

    /// Get the pipeline that a DelimJoin operator depends on.
    pub fn get_delim_join_dependency(&self, op_id: usize) -> Option<&Arc<Pipeline>> {
        self.delim_join_dependencies.get(&op_id)
    }

    /// Register a CTE materialization dependency.
    ///
    /// CTE scans with `op_id` must wait for `pipeline` to complete
    /// before they can read the materialized data.
    pub fn add_cte_dependency(&mut self, op_id: usize, pipeline: Arc<Pipeline>) {
        self.cte_dependencies.insert(op_id, pipeline);
    }

    /// Get the pipeline that a CTE scan depends on.
    pub fn get_cte_dependency(&self, op_id: usize) -> Option<&Arc<Pipeline>> {
        self.cte_dependencies.get(&op_id)
    }

    /// Check if there are any DelimJoin dependencies.
    pub fn has_delim_join_dependencies(&self) -> bool {
        !self.delim_join_dependencies.is_empty()
    }

    /// Check if there are any CTE dependencies.
    pub fn has_cte_dependencies(&self) -> bool {
        !self.cte_dependencies.is_empty()
    }
}

impl std::fmt::Debug for PipelineBuildState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineBuildState")
            .field("delim_join_deps", &self.delim_join_dependencies.len())
            .field("cte_deps", &self.cte_dependencies.len())
            .finish()
    }
}
