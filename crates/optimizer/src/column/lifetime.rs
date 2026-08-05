// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Analyze column lifetimes to build projection maps.

use std::collections::HashSet;

use paro_common::error::Result;
use paro_planner::expression::{ColumnRefExpression, Expression, ExpressionIterator};
use paro_planner::operator::{ColumnBinding, Join, LogicalOperator};
use paro_planner::plan::LogicalPlan;

pub struct ColumnLifetimeAnalyzer {
    column_references: HashSet<ColumnBinding>,
    everything_referenced: bool,
}

impl ColumnLifetimeAnalyzer {
    pub fn new(is_root: bool) -> Self {
        Self {
            column_references: HashSet::new(),
            everything_referenced: is_root,
        }
    }

    pub fn optimize(mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_plan(plan)
    }

    fn optimize_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let LogicalPlan {
            id,
            stats,
            operator,
        } = plan;
        let operator = match operator {
            LogicalOperator::Projection(mut proj) => {
                let mut child_analyzer = ColumnLifetimeAnalyzer::new(false);
                for expr in &proj.expressions {
                    child_analyzer.visit_expression(expr);
                }
                let child = *proj.child;
                proj.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::Projection(proj)
            }
            LogicalOperator::Filter(mut filter) => {
                for expr in &filter.expressions {
                    self.visit_expression(expr);
                }

                // Keep full filter outputs here.
                // Correlated-subquery plans can still contain stale column bindings before
                // ColumnBindingResolver runs; filter-level projection pruning is unsafe there.
                // We preserve correctness by deferring this optimization.
                filter.projection_map.clear();
                let child = *filter.child;
                filter.child = Box::new(self.optimize_plan(child)?);
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Aggregate(mut agg) => {
                let mut child_analyzer = ColumnLifetimeAnalyzer::new(false);
                for expr in &agg.groups {
                    child_analyzer.visit_expression(expr);
                }
                for expr in &agg.aggregates {
                    child_analyzer.visit_expression(expr);
                }
                let child = *agg.child;
                agg.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::Aggregate(agg)
            }
            LogicalOperator::Order(mut order) => {
                for order_expr in &order.orders {
                    self.visit_expression(&order_expr.expression);
                }

                let child = *order.child;
                let child_bindings = child.get_column_bindings();
                order.projection_map = if self.has_unknown_references(&child_bindings) {
                    Vec::new()
                } else {
                    self.generate_projection_map(&child_bindings)
                };

                order.child = Box::new(self.optimize_plan(child)?);
                LogicalOperator::Order(order)
            }
            LogicalOperator::Limit(mut limit) => {
                let child = *limit.child;
                limit.child = Box::new(self.optimize_plan(child)?);
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::TopN(mut topn) => {
                for order_expr in &topn.orders {
                    self.visit_expression(&order_expr.expression);
                }
                let child = *topn.child;
                topn.child = Box::new(self.optimize_plan(child)?);
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Distinct(mut distinct) => {
                self.everything_referenced = true;
                let child = *distinct.child;
                distinct.child = Box::new(self.optimize_plan(child)?);
                LogicalOperator::Distinct(distinct)
            }
            LogicalOperator::MaterializedCTE(mut cte) => {
                let mut cte_query_analyzer = ColumnLifetimeAnalyzer::new(true);
                let cte_query = *cte.cte_query;
                cte.cte_query = Box::new(cte_query_analyzer.optimize_plan(cte_query)?);

                let mut child_analyzer = ColumnLifetimeAnalyzer::new(self.everything_referenced);
                child_analyzer.column_references = self.column_references.clone();
                let child = *cte.child;
                cte.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::MaterializedCTE(cte)
            }
            LogicalOperator::RecursiveCTE(mut cte) => {
                let anchor = *cte.anchor;
                let recursive = *cte.recursive;
                cte.anchor = Box::new(ColumnLifetimeAnalyzer::new(true).optimize_plan(anchor)?);
                cte.recursive =
                    Box::new(ColumnLifetimeAnalyzer::new(true).optimize_plan(recursive)?);
                LogicalOperator::RecursiveCTE(cte)
            }
            LogicalOperator::CTERef(cte_ref) => LogicalOperator::CTERef(cte_ref),
            LogicalOperator::Join(join) => self.optimize_join(join)?,
            other => other,
        };
        Ok(LogicalPlan {
            id,
            stats,
            operator,
        })
    }

    fn optimize_join(&mut self, join: Join) -> Result<LogicalOperator> {
        match join {
            Join::Comparison(mut comp_join) => {
                for condition in &comp_join.conditions {
                    self.visit_expression(&condition.left);
                    self.visit_expression(&condition.right);
                }

                let left = *comp_join.left;
                let right = *comp_join.right;
                comp_join.left = Box::new(self.optimize_plan(left)?);
                comp_join.right = Box::new(self.optimize_plan(right)?);

                // Child joins may compact their outputs. Current projection
                // indexes must be derived from those final child layouts.
                let left_bindings = comp_join.left.get_column_bindings();
                let right_bindings = comp_join.right.get_column_bindings();
                let mut known_bindings =
                    Vec::with_capacity(left_bindings.len() + right_bindings.len());
                known_bindings.extend(left_bindings.iter().copied());
                known_bindings.extend(right_bindings.iter().copied());

                // Delim/correlated joins rely on duplicate-eliminated columns
                // carried through specialized pipelines. Their planner-provided
                // projection maps also distinguish visible subquery output from
                // internal correlation columns, so only replace those maps when
                // the complete binding set is safe to analyze.
                if comp_join.duplicate_eliminated_columns.is_empty()
                    && !self.has_unknown_references(&known_bindings)
                {
                    let left_unused = self.extract_unused_column_bindings(&left_bindings);
                    let right_unused = self.extract_unused_column_bindings(&right_bindings);

                    comp_join.left_projection_map =
                        Self::generate_projection_map_from_unused(&left_bindings, &left_unused);
                    comp_join.right_projection_map =
                        Self::generate_projection_map_from_unused(&right_bindings, &right_unused);
                }

                Ok(LogicalOperator::Join(Join::Comparison(comp_join)))
            }
            Join::Any(mut any_join) => {
                self.visit_expression(&any_join.condition);

                let left = *any_join.left;
                let right = *any_join.right;
                any_join.left = Box::new(self.optimize_plan(left)?);
                any_join.right = Box::new(self.optimize_plan(right)?);

                let left_bindings = any_join.left.get_column_bindings();
                let right_bindings = any_join.right.get_column_bindings();
                let mut known_bindings =
                    Vec::with_capacity(left_bindings.len() + right_bindings.len());
                known_bindings.extend(left_bindings.iter().copied());
                known_bindings.extend(right_bindings.iter().copied());

                let (left_unused, right_unused) = if self.has_unknown_references(&known_bindings) {
                    (HashSet::new(), HashSet::new())
                } else {
                    (
                        self.extract_unused_column_bindings(&left_bindings),
                        self.extract_unused_column_bindings(&right_bindings),
                    )
                };

                any_join.left_projection_map =
                    Self::generate_projection_map_from_unused(&left_bindings, &left_unused);
                any_join.right_projection_map =
                    Self::generate_projection_map_from_unused(&right_bindings, &right_unused);

                Ok(LogicalOperator::Join(Join::Any(any_join)))
            }
            Join::Cross(mut cross) => {
                let left = *cross.left;
                let right = *cross.right;
                cross.left = Box::new(self.optimize_plan(left)?);
                cross.right = Box::new(self.optimize_plan(right)?);
                Ok(LogicalOperator::Join(Join::Cross(cross)))
            }
        }
    }

    fn visit_expression(&mut self, expr: &Expression) {
        if let Expression::ColumnRef(column_ref) = expr {
            self.add_binding(column_ref);
            return;
        }

        ExpressionIterator::enumerate_children(expr, |child| self.visit_expression(child));
    }

    fn add_binding(&mut self, col_ref: &ColumnRefExpression) {
        self.column_references.insert(col_ref.binding);
    }

    fn extract_unused_column_bindings(&self, bindings: &[ColumnBinding]) -> HashSet<ColumnBinding> {
        if self.everything_referenced {
            return HashSet::new();
        }

        let mut unused = HashSet::new();
        for binding in bindings {
            if !self.column_references.contains(binding) {
                unused.insert(*binding);
            }
        }
        unused
    }

    /// Correlated-subquery flattening can temporarily leave stale table-index bindings.
    /// In that shape, projection-map pruning is unsafe and must be skipped.
    fn has_unknown_references(&self, known_bindings: &[ColumnBinding]) -> bool {
        if self.everything_referenced || self.column_references.is_empty() {
            return false;
        }
        self.column_references
            .iter()
            .any(|binding| !known_bindings.contains(binding))
    }

    fn generate_projection_map(&self, child_bindings: &[ColumnBinding]) -> Vec<usize> {
        if self.everything_referenced {
            return Vec::new();
        }

        let mut projection_map = Vec::new();
        for (idx, binding) in child_bindings.iter().enumerate() {
            if self.column_references.contains(binding) {
                projection_map.push(idx);
            }
        }

        if projection_map.len() == child_bindings.len() {
            Vec::new()
        } else {
            projection_map
        }
    }

    fn generate_projection_map_from_unused(
        bindings: &[ColumnBinding],
        unused: &HashSet<ColumnBinding>,
    ) -> Vec<usize> {
        if unused.is_empty() {
            return Vec::new();
        }

        let mut projection_map = Vec::new();
        for (idx, binding) in bindings.iter().enumerate() {
            if !unused.contains(binding) {
                projection_map.push(idx);
            }
        }

        if projection_map.len() == bindings.len() {
            Vec::new()
        } else {
            projection_map
        }
    }

    pub fn extract_column_bindings(expr: &Expression, bindings: &mut Vec<ColumnBinding>) {
        if let Expression::ColumnRef(column_ref) = expr {
            bindings.push(column_ref.binding);
            return;
        }

        ExpressionIterator::enumerate_children(expr, |child| {
            Self::extract_column_bindings(child, bindings);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnLifetimeAnalyzer;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ColumnRefExpression, Expression, WindowExpression, WindowFrame, WindowFrameBound,
        WindowFrameType,
    };
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, ExpressionGet, Join, JoinType, LogicalOperator,
    };
    use paro_planner::plan::LogicalPlan;

    #[test]
    fn extract_column_bindings_visits_window_frame_offsets() {
        let expected = ColumnBinding::new(7, 3);
        let expression = Expression::Window(WindowExpression {
            function: WindowFunction::row_number(),
            children: vec![],
            partitions: vec![],
            orders: vec![],
            frame: WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(Expression::ColumnRef(
                    ColumnRefExpression::new(expected, LogicalType::Integer),
                ))),
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            ignore_nulls: false,
            return_type: LogicalType::BigInt,
        });
        let mut bindings = Vec::new();

        ColumnLifetimeAnalyzer::extract_column_bindings(&expression, &mut bindings);

        assert_eq!(bindings, vec![expected]);
    }

    #[test]
    fn delim_join_preserves_visible_rhs_projection() {
        let ctx = BindContext::new();
        let left = LogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["outer_key".into()],
                vec![LogicalType::Integer],
            )),
        );
        let right = LogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["value".into(), "__corr_1".into()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        let mut join = ComparisonJoin::new(JoinType::Single, left, right, Vec::new());
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(10, 0),
            LogicalType::Integer,
        ))];
        join.right_projection_map = vec![0];
        let plan = LogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(join)));

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.right_projection_map, vec![0]);
        assert_eq!(join.get_types().len(), 2);
    }
}
