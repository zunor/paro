// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # MetaPipeline
//!
//! MetaPipeline represents a set of pipelines that all share the same sink.
//!
//!
//! ## Dependencies Check
//! - Pipeline: ✅ Using paro-execution::pipeline
//! - PhysicalOperator: ✅ Using paro-execution
//!
//! - MetaPipeline groups pipelines with the same sink
//! - Supports child MetaPipelines for Join builds
//! - Manages pipeline dependencies within and across MetaPipelines
//! - Build() delegates to PhysicalOperator::BuildPipelines()

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::operator::PhysicalOperator;
use parking_lot::RwLock;

use super::pipeline::Pipeline;
use crate::pipeline::build_state::PipelineBuildState;

/// Type of MetaPipeline.
///
/// Determines how the MetaPipeline is treated during scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MetaPipelineType {
    /// Regular pipeline (default)
    #[default]
    Regular,
    /// Join build side - must complete before probe side starts
    JoinBuild,
}

/// MetaPipeline represents a set of pipelines that all have the same sink.
///
/// 1. Group pipelines that share a sink (e.g., multiple sources feeding into a Union)
/// 2. Manage child MetaPipelines (e.g., Join build side)
/// 3. Track dependencies between pipelines
///
/// 1. For joins, build out the blocking side before going down the probe side
/// 2. Build child pipelines last (e.g., Hash Join becomes source after probe is done)
pub struct MetaPipeline {
    /// The shared sink operator for all pipelines in this MetaPipeline.
    /// None for the root MetaPipeline (which uses ResultCollector).
    sink: Option<Arc<dyn PhysicalOperator>>,

    /// Type of this MetaPipeline
    meta_type: MetaPipelineType,

    /// Whether this MetaPipeline is part of a recursive CTE
    recursive_cte: AtomicBool,

    /// All pipelines with different sources but the same sink
    pipelines: RwLock<Vec<Arc<Pipeline>>>,

    /// Dependencies between pipelines within this MetaPipeline
    /// Key: dependent pipeline, Value: pipelines it depends on
    pipeline_dependencies: RwLock<HashMap<usize, Vec<usize>>>,

    /// Child MetaPipelines (e.g., for Join build sides)
    children: RwLock<Vec<Arc<MetaPipeline>>>,

    /// Parent pipeline (the pipeline in the parent MetaPipeline that created this child)
    parent: RwLock<Option<Weak<Pipeline>>>,

    /// Next batch index for assigning to pipelines
    next_batch_index: RwLock<usize>,

    /// Pipelines (other than the base pipeline) that need an independent finish event chain.
    finish_pipelines: RwLock<HashSet<usize>>,

    /// Mapping from pipeline index to finish group root index.
    finish_map: RwLock<HashMap<usize, usize>>,
}

impl MetaPipeline {
    /// Create a new MetaPipeline with the given sink.
    ///
    /// # Arguments
    /// * `sink` - The shared sink operator (None for root MetaPipeline)
    /// * `meta_type` - Type of MetaPipeline (Regular or JoinBuild)
    pub fn new(sink: Option<Arc<dyn PhysicalOperator>>, meta_type: MetaPipelineType) -> Arc<Self> {
        let meta = Arc::new(Self {
            sink,
            meta_type,
            recursive_cte: AtomicBool::new(false),
            pipelines: RwLock::new(Vec::new()),
            pipeline_dependencies: RwLock::new(HashMap::new()),
            children: RwLock::new(Vec::new()),
            parent: RwLock::new(None),
            next_batch_index: RwLock::new(0),
            finish_pipelines: RwLock::new(HashSet::new()),
            finish_map: RwLock::new(HashMap::new()),
        });

        // Create the base pipeline
        meta.create_pipeline();

        meta
    }

    /// Get the sink operator.
    pub fn sink(&self) -> Option<&Arc<dyn PhysicalOperator>> {
        self.sink.as_ref()
    }

    /// Get the type of this MetaPipeline.
    pub fn meta_type(&self) -> MetaPipelineType {
        self.meta_type
    }

    /// Check if this MetaPipeline is part of a recursive CTE.
    pub fn has_recursive_cte(&self) -> bool {
        self.recursive_cte.load(Ordering::SeqCst)
    }

    /// Set the recursive CTE flag.
    pub fn set_recursive_cte(&self) {
        self.recursive_cte.store(true, Ordering::SeqCst);
    }

    /// Get the base (first) pipeline.
    pub fn base_pipeline(&self) -> Arc<Pipeline> {
        self.pipelines.read()[0].clone()
    }

    /// Get all pipelines in this MetaPipeline.
    pub fn pipelines(&self) -> Vec<Arc<Pipeline>> {
        self.pipelines.read().clone()
    }

    /// Get all pipelines recursively (including from child MetaPipelines).
    ///
    /// Returns pipelines in execution order: child MetaPipelines first, then this MetaPipeline.
    /// This ensures that dependencies (like HashAggregate build side) complete before
    /// the pipelines that depend on them.
    pub fn get_pipelines_recursive(&self) -> Vec<Arc<Pipeline>> {
        let mut result = Vec::new();
        // First, collect pipelines from child MetaPipelines (they must complete first)
        for child in self.children.read().iter() {
            result.extend(child.get_pipelines_recursive());
        }
        // Then, add this MetaPipeline's pipelines
        result.extend(self.pipelines.read().clone());
        result
    }

    /// Get child MetaPipelines.
    pub fn children(&self) -> Vec<Arc<MetaPipeline>> {
        self.children.read().clone()
    }

    /// Get all MetaPipelines recursively.
    ///
    /// If `include_self` is true, the first entry is this MetaPipeline.
    pub fn get_meta_pipelines_recursive(
        self: &Arc<Self>,
        include_self: bool,
    ) -> Vec<Arc<MetaPipeline>> {
        let mut result = Vec::new();
        if include_self {
            result.push(self.clone());
        }
        for child in self.children.read().iter() {
            result.extend(child.get_meta_pipelines_recursive(true));
        }
        result
    }

    /// Recursively get the last child MetaPipeline in depth-first insertion order.
    pub fn get_last_child(self: &Arc<Self>) -> Option<Arc<MetaPipeline>> {
        let last_child = self.children.read().last().cloned()?;
        last_child.get_last_child().or(Some(last_child))
    }

    /// Get the parent pipeline.
    pub fn parent(&self) -> Option<Arc<Pipeline>> {
        self.parent.read().as_ref().and_then(|w| w.upgrade())
    }

    /// Get pipeline dependencies.
    pub fn get_dependencies(&self) -> HashMap<usize, Vec<usize>> {
        self.pipeline_dependencies.read().clone()
    }

    /// Get explicit dependencies within this MetaPipeline.
    ///
    /// Returns pairs of `(dependent_pipeline, dependency_pipelines)` resolved from
    /// the internal index-based `pipeline_dependencies` map.
    pub fn explicit_dependencies(&self) -> Vec<(Arc<Pipeline>, Vec<Arc<Pipeline>>)> {
        let pipelines = self.pipelines.read();
        let deps = self.pipeline_dependencies.read();

        let mut entries: Vec<(usize, Vec<usize>)> =
            deps.iter().map(|(idx, dep)| (*idx, dep.clone())).collect();
        entries.sort_by_key(|(idx, _)| *idx);

        let mut result = Vec::new();
        for (dependent_idx, dependency_indices) in entries {
            let Some(dependent_pipeline) = pipelines.get(dependent_idx).cloned() else {
                continue;
            };

            let dependencies: Vec<Arc<Pipeline>> = dependency_indices
                .into_iter()
                .filter_map(|dep_idx| pipelines.get(dep_idx).cloned())
                .collect();

            if !dependencies.is_empty() {
                result.push((dependent_pipeline, dependencies));
            }
        }

        result
    }

    // ========== Pipeline Construction ==========

    /// Build the MetaPipeline starting from the given operator.
    ///
    /// This is the main entry point for pipeline construction.
    /// It delegates to the `build_pipelines_default` function.
    pub fn build(self: &Arc<Self>, op: &Arc<dyn PhysicalOperator>, state: &mut PipelineBuildState) {
        debug_assert_eq!(
            self.pipelines.read().len(),
            1,
            "Build should start with single base pipeline"
        );
        debug_assert!(
            self.children.read().is_empty(),
            "Build should start with no children"
        );

        let base = self.base_pipeline();
        op.build_pipelines(op, &base, self, state);
    }

    /// Mark all pipelines as ready for execution.
    pub fn ready(&self) {
        // First, wire internal dependencies
        {
            let pipelines = self.pipelines.read();
            let deps = self.pipeline_dependencies.read();
            for (&dependent_idx, dependency_indices) in deps.iter() {
                if let Some(dependent_pipeline) = pipelines.get(dependent_idx) {
                    for &dependency_idx in dependency_indices {
                        if let Some(dependency_pipeline) = pipelines.get(dependency_idx) {
                            dependent_pipeline.add_dependency(dependency_pipeline.clone());
                        }
                    }
                }
            }
        }

        // Mark all pipelines as ready
        for pipeline in self.pipelines.read().iter() {
            pipeline.set_ready();
        }

        // Recursively call ready on children
        for child in self.children.read().iter() {
            child.ready();
        }
    }

    /// Create a new empty pipeline within this MetaPipeline.
    ///
    /// The new pipeline shares the same sink as other pipelines in this MetaPipeline.
    pub fn create_pipeline(&self) -> Arc<Pipeline> {
        let mut batch_idx = self.next_batch_index.write();
        let pipeline = Arc::new(Pipeline::new_with_sink(self.sink.clone(), *batch_idx));
        *batch_idx += 1;
        self.pipelines.write().push(pipeline.clone());
        pipeline
    }

    /// Create a union pipeline (clone of current pipeline's operators).
    ///
    /// Used for UNION ALL where multiple sources feed into the same sink.
    ///
    /// # Arguments
    /// * `current` - The current pipeline to clone operators from
    /// * `order_matters` - If true, union_pipeline depends on current
    pub fn create_union_pipeline(
        &self,
        current: &Arc<Pipeline>,
        order_matters: bool,
    ) -> Arc<Pipeline> {
        let union_pipeline = self.create_pipeline();

        // Copy operators from current pipeline
        {
            let current_ops = current.get_operators();
            union_pipeline.set_operators(current_ops);
        }

        // Inherit dependencies from current
        for dep in current.get_dependencies() {
            union_pipeline.add_dependency(dep);
        }

        // Copy internal dependencies
        let current_idx = self.pipeline_index(current);
        let union_idx = self.pipeline_index(&union_pipeline);
        if let Some(current_idx) = current_idx {
            let current_deps = {
                let deps = self.pipeline_dependencies.read();
                deps.get(&current_idx).cloned()
            };
            if let Some(current_deps) = current_deps {
                let mut deps = self.pipeline_dependencies.write();
                deps.insert(union_idx.unwrap_or(0), current_deps);
            }
        }

        // If order matters, union depends on current
        if order_matters {
            if let (Some(union_idx), Some(current_idx)) = (union_idx, current_idx) {
                self.pipeline_dependencies
                    .write()
                    .entry(union_idx)
                    .or_default()
                    .push(current_idx);
            }
        }

        union_pipeline
    }

    /// Create a child pipeline starting at the given operator.
    ///
    /// Child pipelines are created when an operator needs to produce data
    /// after its main pipeline completes (e.g., FULL OUTER JOIN scanning HT).
    ///
    /// # Arguments
    /// * `current` - The current pipeline (must have source set)
    /// * `op` - The operator that becomes the source of the child pipeline
    /// * `last_pipeline` - The last pipeline added before building out current
    pub fn create_child_pipeline(
        &self,
        current: &Arc<Pipeline>,
        op: Arc<dyn PhysicalOperator>,
        last_pipeline: &Arc<Pipeline>,
    ) -> Arc<Pipeline> {
        // Rule 2: current must be fully built before creating child
        debug_assert!(
            current.has_source(),
            "Current pipeline must have source before creating child"
        );

        // Create child pipeline with same batch index as current
        let child = Arc::new(Pipeline::new_with_sink(
            self.sink.clone(),
            current.batch_index(),
        ));
        self.pipelines.write().push(child.clone());

        // Set the operator as source
        child.set_source(op);

        // Child depends on current and all pipelines added since last_pipeline
        let child_idx = self.pipeline_index(&child).unwrap_or(0);
        let current_idx = self.pipeline_index(current);
        let last_idx = self.pipeline_index(last_pipeline);

        let mut deps = self.pipeline_dependencies.write();
        let child_deps = deps.entry(child_idx).or_default();

        if let Some(idx) = current_idx {
            child_deps.push(idx);
        }

        // Add dependencies from pipelines added after last_pipeline
        if let Some(last_idx) = last_idx {
            let pipelines = self.pipelines.read();
            for (idx, _) in pipelines.iter().enumerate() {
                if idx > last_idx && Some(idx) != current_idx && idx != child_idx {
                    child_deps.push(idx);
                }
            }
        }

        child
    }

    /// Create a child MetaPipeline.
    ///
    /// Used for operators that need a separate MetaPipeline (e.g., Join build side).
    /// The child MetaPipeline must complete before the current pipeline can continue.
    ///
    /// # Arguments
    /// * `current` - The current pipeline that will depend on the child MetaPipeline
    /// * `op` - The operator that becomes the sink of the child MetaPipeline
    /// * `meta_type` - Type of the child MetaPipeline
    pub fn create_child_meta_pipeline(
        self: &Arc<Self>,
        current: &Arc<Pipeline>,
        op: Arc<dyn PhysicalOperator>,
        meta_type: MetaPipelineType,
    ) -> Arc<MetaPipeline> {
        let child = Arc::new(MetaPipeline {
            sink: Some(op),
            meta_type,
            recursive_cte: AtomicBool::new(self.has_recursive_cte()),
            pipelines: RwLock::new(Vec::new()),
            pipeline_dependencies: RwLock::new(HashMap::new()),
            children: RwLock::new(Vec::new()),
            parent: RwLock::new(Some(Arc::downgrade(current))),
            next_batch_index: RwLock::new(0),
            finish_pipelines: RwLock::new(HashSet::new()),
            finish_map: RwLock::new(HashMap::new()),
        });

        // Create base pipeline for child
        child.create_pipeline();

        // Current pipeline depends on child's base pipeline
        current.add_dependency(child.base_pipeline());

        self.children.write().push(child.clone());

        child
    }

    /// Add dependencies from pipelines created since `start`.
    ///
    /// Makes `dependant` depend on all pipelines added after (and optionally including) `start`.
    pub fn add_dependencies_from(
        &self,
        dependant: &Arc<Pipeline>,
        start: &Arc<Pipeline>,
        including: bool,
    ) -> Vec<Arc<Pipeline>> {
        let pipelines = self.pipelines.read();
        let dependant_idx = self.pipeline_index(dependant);
        let start_idx = self.pipeline_index(start);

        let (Some(dependant_idx), Some(start_idx)) = (dependant_idx, start_idx) else {
            return Vec::new();
        };

        let start_from = if including { start_idx } else { start_idx + 1 };

        let mut created = Vec::new();
        let mut deps = self.pipeline_dependencies.write();
        let dep_list = deps.entry(dependant_idx).or_default();

        for (idx, pipeline) in pipelines.iter().enumerate() {
            if idx >= start_from && idx != dependant_idx {
                dep_list.push(idx);
                created.push(pipeline.clone());
            }
        }

        created
    }

    /// Recursively make child MetaPipelines added after `last_child` depend on `new_dependencies`.
    ///
    pub fn add_recursive_dependencies(
        self: &Arc<Self>,
        new_dependencies: &[Arc<Pipeline>],
        last_child: &Arc<MetaPipeline>,
    ) {
        if self.has_recursive_cte() || new_dependencies.is_empty() {
            return;
        }

        let children = self.get_meta_pipelines_recursive(false);
        let Some(last_child_idx) = children
            .iter()
            .position(|child| Arc::ptr_eq(child, last_child))
        else {
            return;
        };

        for child_meta in children.iter().skip(last_child_idx + 1) {
            for pipeline in child_meta.pipelines() {
                let existing_deps = pipeline.get_dependencies();
                for new_dependency in new_dependencies {
                    if Arc::ptr_eq(&pipeline, new_dependency)
                        || existing_deps
                            .iter()
                            .any(|dependency| Arc::ptr_eq(dependency, new_dependency))
                        || Self::would_create_dependency_cycle(&pipeline, new_dependency)
                    {
                        continue;
                    }
                    pipeline.add_dependency(new_dependency.clone());
                }
            }
        }
    }

    /// Assign the next batch index to a pipeline.
    pub fn assign_next_batch_index(&self, pipeline: &Pipeline) {
        let mut idx = self.next_batch_index.write();
        pipeline.set_batch_index(*idx * PipelineBuildState::BATCH_INCREMENT);
        *idx += 1;
    }

    /// Ensure this pipeline has an independent prepare-finish/finish chain.
    ///
    pub fn add_finish_event(&self, pipeline: &Arc<Pipeline>) {
        let Some(finish_idx) = self.pipeline_index(pipeline) else {
            return;
        };

        self.finish_pipelines.write().insert(finish_idx);

        let pipeline_count = self.pipelines.read().len();
        let mut finish_map = self.finish_map.write();
        for idx in (finish_idx + 1)..pipeline_count {
            finish_map.insert(idx, finish_idx);
        }
    }

    /// Whether this pipeline requires its own finish event chain.
    pub fn has_finish_event(&self, pipeline: &Arc<Pipeline>) -> bool {
        let Some(idx) = self.pipeline_index(pipeline) else {
            return false;
        };
        self.finish_pipelines.read().contains(&idx)
    }

    /// Get the finish group root for the given pipeline.
    pub fn get_finish_group(&self, pipeline: &Arc<Pipeline>) -> Option<Arc<Pipeline>> {
        let pipelines = self.pipelines.read();
        let idx = pipelines.iter().position(|p| Arc::ptr_eq(p, pipeline))?;
        let group_idx = self.finish_map.read().get(&idx).copied()?;
        pipelines.get(group_idx).cloned()
    }

    /// Propagate an operator added to `current` into any dependent pipelines that
    /// reuse the same downstream path (e.g. source child pipelines for RIGHT/FULL joins).
    pub fn propagate_operator_to_dependents(
        &self,
        current: &Arc<Pipeline>,
        op: Arc<dyn PhysicalOperator>,
    ) {
        let Some(current_idx) = self.pipeline_index(current) else {
            return;
        };

        let deps = self.pipeline_dependencies.read().clone();
        let pipelines = self.pipelines.read().clone();
        let mut stack = vec![current_idx];
        let mut visited = HashSet::new();

        while let Some(dep_idx) = stack.pop() {
            if !visited.insert(dep_idx) {
                continue;
            }

            for (candidate_idx, candidate_deps) in &deps {
                if !candidate_deps.contains(&dep_idx) {
                    continue;
                }
                if let Some(pipeline) = pipelines.get(*candidate_idx) {
                    pipeline.add_operator(op.clone());
                    stack.push(*candidate_idx);
                }
            }
        }
    }

    // ========== Internal Helpers ==========

    /// Get the index of a pipeline within this MetaPipeline.
    fn pipeline_index(&self, pipeline: &Arc<Pipeline>) -> Option<usize> {
        let pipelines = self.pipelines.read();
        pipelines.iter().position(|p| Arc::ptr_eq(p, pipeline))
    }

    fn would_create_dependency_cycle(
        dependant: &Arc<Pipeline>,
        dependency: &Arc<Pipeline>,
    ) -> bool {
        let mut stack = vec![dependency.clone()];
        let mut visited = HashSet::new();

        while let Some(current) = stack.pop() {
            let key = Arc::as_ptr(&current) as usize;
            if !visited.insert(key) {
                continue;
            }
            if Arc::ptr_eq(&current, dependant) {
                return true;
            }
            stack.extend(current.get_dependencies());
        }

        false
    }
}

impl std::fmt::Debug for MetaPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPipeline")
            .field("sink", &self.sink.as_ref().map(|s| s.name()))
            .field("type", &self.meta_type)
            .field("pipelines", &self.pipelines.read().len())
            .field("children", &self.children.read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{MetaPipeline, MetaPipelineType};
    use crate::operator::PhysicalOperator;
    use crate::operator_type::PhysicalOperatorType;
    use paro_common::types::LogicalType;
    use std::any::Any;
    use std::sync::Arc;

    #[derive(Debug)]
    struct MockSinkOperator {
        types: Vec<LogicalType>,
    }

    impl MockSinkOperator {
        fn new() -> Self {
            Self { types: vec![] }
        }
    }

    impl PhysicalOperator for MockSinkOperator {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::HashJoin
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn is_sink(&self) -> bool {
            true
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    fn mock_sink() -> Arc<dyn PhysicalOperator> {
        Arc::new(MockSinkOperator::new())
    }

    #[test]
    fn finish_event_mapping_tracks_following_pipelines() {
        let meta = MetaPipeline::new(None, MetaPipelineType::Regular);

        let finish_root = meta.create_pipeline();
        let grouped = meta.create_pipeline();

        meta.add_finish_event(&finish_root);

        assert!(meta.has_finish_event(&finish_root));
        assert!(!meta.has_finish_event(&grouped));
        assert!(meta.get_finish_group(&finish_root).is_none());

        let group_root = meta
            .get_finish_group(&grouped)
            .expect("grouped pipeline should have finish group root");
        assert!(Arc::ptr_eq(&group_root, &finish_root));
    }

    #[test]
    fn add_recursive_dependencies_propagates_to_following_children() {
        let meta = MetaPipeline::new(Some(mock_sink()), MetaPipelineType::Regular);
        let current = meta.base_pipeline();
        let child1 =
            meta.create_child_meta_pipeline(&current, mock_sink(), MetaPipelineType::JoinBuild);
        let child2 =
            meta.create_child_meta_pipeline(&current, mock_sink(), MetaPipelineType::JoinBuild);

        let dependency = child1.base_pipeline();
        meta.add_recursive_dependencies(std::slice::from_ref(&dependency), &child1);

        let child2_dependencies = child2.base_pipeline().get_dependencies();
        assert!(
            child2_dependencies
                .iter()
                .any(|dep| Arc::ptr_eq(dep, &dependency)),
            "child2 should inherit recursive dependency from child1 build branch"
        );
    }

    #[test]
    fn get_last_child_returns_deepest_child() {
        let meta = MetaPipeline::new(Some(mock_sink()), MetaPipelineType::Regular);
        let current = meta.base_pipeline();
        let child =
            meta.create_child_meta_pipeline(&current, mock_sink(), MetaPipelineType::JoinBuild);
        let child_base = child.base_pipeline();
        let nested =
            child.create_child_meta_pipeline(&child_base, mock_sink(), MetaPipelineType::JoinBuild);

        let last_child = meta
            .get_last_child()
            .expect("meta pipeline should have at least one child");
        assert!(Arc::ptr_eq(&last_child, &nested));
    }
}
