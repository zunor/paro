// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical plan generation for delim joins.
//!
//!

use std::sync::Arc;

use paro_common::error::Result;
use paro_planner::operator::join::ComparisonJoin;

use super::generator::PhysicalPlanGenerator;
use crate::operator::join::delim_join::DelimJoin;
use crate::operator::join::left_delim_join::LeftDelimJoin;
use crate::operator::join::right_delim_join::RightDelimJoin;
use crate::operator::scan::column_data_scan::{ColumnDataScanBinding, PhysicalColumnDataScan};
use crate::operator::scan::dummy_scan::PhysicalDummyScan;
use crate::operator::PhysicalOperator;

impl PhysicalPlanGenerator {
    pub(crate) fn create_plan_delim_join_from_children(
        &self,
        join: &ComparisonJoin,
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        if join.duplicate_eliminated_columns.is_empty() {
            return self.create_plan_regular_comparison_join_from_children(join, left, right);
        }

        if join.delim_flipped {
            let dummy_right = Arc::new(PhysicalDummyScan::with_types(right.types().to_vec()))
                as Arc<dyn PhysicalOperator>;
            let wrapped_join = self.create_plan_regular_comparison_join_from_children(
                join,
                left.clone(),
                dummy_right,
            )?;

            let mut delim_scans = Vec::new();
            Self::gather_delim_scan_bindings(&left, &mut delim_scans);
            if delim_scans.is_empty() {
                return self.create_plan_regular_comparison_join_from_children(join, left, right);
            }

            return Ok(Arc::new(RightDelimJoin::new(DelimJoin::new(
                right,
                wrapped_join,
                join.duplicate_eliminated_columns.clone(),
                Arc::new(ColumnDataScanBinding::new(None)),
                delim_scans,
                join.get_types(),
            ))));
        }

        let cached_left_scan = Arc::new(PhysicalColumnDataScan::new(left.types().to_vec(), None));
        let cached_left_binding = cached_left_scan.binding();
        let wrapped_join = self.create_plan_regular_comparison_join_from_children(
            join,
            cached_left_scan as Arc<dyn PhysicalOperator>,
            right.clone(),
        )?;

        let mut delim_scans = Vec::new();
        Self::gather_delim_scan_bindings(&right, &mut delim_scans);
        if delim_scans.is_empty() {
            return self.create_plan_regular_comparison_join_from_children(join, left, right);
        }

        Ok(Arc::new(LeftDelimJoin::new(DelimJoin::new(
            left,
            wrapped_join,
            join.duplicate_eliminated_columns.clone(),
            cached_left_binding,
            delim_scans,
            join.get_types(),
        ))))
    }

    fn gather_delim_scan_bindings(
        op: &Arc<dyn PhysicalOperator>,
        bindings: &mut Vec<Arc<ColumnDataScanBinding>>,
    ) {
        if let Some(scan) = op.as_any().downcast_ref::<PhysicalColumnDataScan>() {
            if scan.binding().dependency_id().is_some() {
                bindings.push(scan.binding());
            }
        }

        for idx in 0..op.children_count() {
            if let Some(child) = op.child_arc(idx) {
                Self::gather_delim_scan_bindings(&child, bindings);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::operator_type::PhysicalOperatorType;
    use crate::physical_plan::generator::PhysicalPlanGenerator;
    use crate::pipeline::build_state::PipelineBuildState;
    use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::join::{Join, JoinCondition, JoinType};
    use paro_planner::operator::{ColumnBinding, DelimGet, ExpressionGet};
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn test_generator() -> PhysicalPlanGenerator {
        PhysicalPlanGenerator::new(test_session())
    }

    fn expression_get(
        table_index: usize,
        column_count: usize,
        prefix: &str,
    ) -> paro_planner::operator::LogicalOperator {
        paro_planner::operator::LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![],
            (0..column_count)
                .map(|idx| format!("{prefix}{idx}"))
                .collect(),
            vec![LogicalType::Integer; column_count],
        ))
    }

    fn lp(op: paro_planner::operator::LogicalOperator) -> paro_planner::plan::LogicalPlan {
        paro_planner::plan::LogicalPlan::new(&paro_planner::binder::context::BindContext::new(), op)
    }

    #[test]
    fn duplicate_eliminated_comparison_join_uses_right_delim_join_when_flipped() {
        let left = paro_planner::operator::LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        ));
        let right = expression_get(1, 1, "r");
        let mut join = paro_planner::operator::join::ComparisonJoin::new(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(99, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                paro_planner::operator::join::JoinComparisonType::Equal,
            )],
        );
        join.delim_flipped = true;
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(1, 0),
            LogicalType::Integer,
        ))];
        let mut plan = paro_planner::operator::LogicalOperator::Join(Join::Comparison(join));

        let physical = test_generator()
            .plan_operator(&mut plan)
            .expect("plan should succeed");
        assert_eq!(
            physical.operator_type(),
            PhysicalOperatorType::RightDelimJoin
        );
    }

    #[test]
    fn right_delim_join_pipeline_uses_registered_delim_dependency() {
        let left = paro_planner::operator::LogicalOperator::DelimGet(DelimGet::new(
            99,
            vec![LogicalType::Integer],
        ));
        let right = expression_get(1, 1, "r");
        let mut join = paro_planner::operator::join::ComparisonJoin::new(
            JoinType::Inner,
            lp(left),
            lp(right),
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(99, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                paro_planner::operator::join::JoinComparisonType::Equal,
            )],
        );
        join.delim_flipped = true;
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(1, 0),
            LogicalType::Integer,
        ))];
        let mut logical = paro_planner::operator::LogicalOperator::Join(Join::Comparison(join));

        let physical = test_generator()
            .plan_operator(&mut logical)
            .expect("plan should succeed");
        let meta_pipeline = MetaPipeline::new(None, MetaPipelineType::Regular);
        let current = meta_pipeline.base_pipeline();
        let mut state = PipelineBuildState::new();

        physical.build_pipelines(&physical, &current, &meta_pipeline, &mut state);

        let dependency = state
            .get_delim_join_dependency(99)
            .cloned()
            .expect("dependency should be registered");
        let deps = current.get_dependencies();
        assert!(deps.iter().any(|dep| Arc::ptr_eq(dep, &dependency)));
        assert!(deps.iter().all(|dep| !Arc::ptr_eq(dep, &current)));
        assert_eq!(
            current.source().expect("pipeline source").operator_type(),
            PhysicalOperatorType::ColumnDataScan
        );
    }
}
