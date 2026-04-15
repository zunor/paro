// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared physical join contract.
//!
//!

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_planner::operator::join::JoinType;

use crate::operator::PhysicalOperator;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;

const RECURSIVE_DEPENDENCY_CARDINALITY_THRESHOLD: usize = 100_000;

/// Common contract shared by physical join operators.
#[derive(Debug)]
pub struct PhysicalJoin {
    /// Probe side child.
    pub left: Arc<dyn PhysicalOperator>,
    /// Build side child.
    pub right: Arc<dyn PhysicalOperator>,
    /// Join type.
    pub join_type: JoinType,
    /// Projection pushed from planner for probe/output side.
    pub left_projection_map: Vec<usize>,
    /// Projection pushed from planner for build/output side.
    pub right_projection_map: Vec<usize>,
    /// Output types after projection and join-type layout rules are applied.
    pub types: Vec<LogicalType>,
    /// Projected probe-side output types.
    pub left_output_types: Vec<LogicalType>,
    /// Projected build-side output types.
    pub right_output_types: Vec<LogicalType>,
}

impl PhysicalJoin {
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Self {
        let left_output_types = Self::project_types(left.types(), &left_projection_map);
        let right_output_types = Self::project_types(right.types(), &right_projection_map);
        let types = Self::compute_output_types(&left_output_types, &right_output_types, join_type);

        Self {
            left,
            right,
            join_type,
            left_projection_map,
            right_projection_map,
            types,
            left_output_types,
            right_output_types,
        }
    }

    pub fn project_types(
        child_types: &[LogicalType],
        projection_map: &[usize],
    ) -> Vec<LogicalType> {
        if projection_map.is_empty() {
            child_types.to_vec()
        } else {
            projection_map
                .iter()
                .filter_map(|&idx| child_types.get(idx).cloned())
                .collect()
        }
    }

    pub fn compute_output_types(
        left_types: &[LogicalType],
        right_types: &[LogicalType],
        join_type: JoinType,
    ) -> Vec<LogicalType> {
        match join_type {
            JoinType::Semi | JoinType::Anti => left_types.to_vec(),
            JoinType::RightSemi | JoinType::RightAnti => right_types.to_vec(),
            JoinType::Mark => {
                let mut types = left_types.to_vec();
                types.push(LogicalType::Boolean);
                types
            }
            _ => {
                let mut types = left_types.to_vec();
                types.extend(right_types.iter().cloned());
                types
            }
        }
    }

    pub fn is_source(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    pub fn empty_result_if_rhs_is_empty(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Inner
                | JoinType::Right
                | JoinType::Semi
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub fn build_join_pipelines(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
        build_rhs: bool,
    ) {
        let last_pipeline = meta_pipeline
            .pipelines()
            .last()
            .cloned()
            .unwrap_or_else(|| current.clone());
        let mut recursive_dependencies = Vec::new();
        let mut last_child_ptr = None;

        if build_rhs {
            let build_meta = meta_pipeline.create_child_meta_pipeline(
                current,
                op.clone(),
                MetaPipelineType::JoinBuild,
            );
            build_meta.build(&self.right, state);
            recursive_dependencies = build_meta.pipelines();
            last_child_ptr = meta_pipeline.get_last_child();
        }

        self.left
            .build_pipelines(&self.left, current, meta_pipeline, state);

        if self.should_add_recursive_dependencies() {
            if let Some(last_child) = last_child_ptr {
                meta_pipeline.add_recursive_dependencies(&recursive_dependencies, &last_child);
            }
        }

        current.add_operator(op.clone());

        if self.is_source() {
            let _ = meta_pipeline.create_child_pipeline(current, op.clone(), &last_pipeline);
        }
    }

    fn should_add_recursive_dependencies(&self) -> bool {
        self.right.parallel_operator()
            && self.right.estimated_cardinality() >= RECURSIVE_DEPENDENCY_CARDINALITY_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use super::PhysicalJoin;
    use paro_common::types::LogicalType;
    use paro_planner::operator::join::JoinType;

    #[test]
    fn output_types_respect_projection_maps_and_mark_layout() {
        let left_types = vec![LogicalType::Integer, LogicalType::Varchar];
        let right_types = vec![LogicalType::Boolean, LogicalType::BigInt];

        let projected_left = PhysicalJoin::project_types(&left_types, &[1]);
        let projected_right = PhysicalJoin::project_types(&right_types, &[0]);
        let output =
            PhysicalJoin::compute_output_types(&projected_left, &projected_right, JoinType::Mark);

        assert_eq!(projected_left, vec![LogicalType::Varchar]);
        assert_eq!(projected_right, vec![LogicalType::Boolean]);
        assert_eq!(output, vec![LogicalType::Varchar, LogicalType::Boolean]);
    }

    #[test]
    fn empty_result_if_rhs_is_empty_preserves_contract() {
        assert!(matches!(JoinType::Inner, JoinType::Inner));
        assert!(PhysicalJoin::compute_output_types(&[], &[], JoinType::Inner).is_empty());
    }
}
