// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Analyze column lifetimes to build projection maps.

use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{ColumnBinding, Join, LogicalOperator, ProjectionMap};
use paro_planner::plan::OwnedLogicalPlan;

use crate::expression::traversal::visit_expression as traverse_expression;

pub struct ColumnLifetimeAnalyzer {
    column_references: HashSet<ColumnBinding>,
    everything_referenced: bool,
    preserve_unresolved_predicate_carriers: bool,
}

impl ColumnLifetimeAnalyzer {
    pub fn new(is_root: bool) -> Self {
        Self {
            column_references: HashSet::new(),
            everything_referenced: is_root,
            preserve_unresolved_predicate_carriers: false,
        }
    }

    /// Join-region extraction runs before every specialized correlation proof
    /// has consumed its planner carrier. Preserve a Filter/Order/TopN's full
    /// visible layout when its local expressions still reference bindings
    /// outside the current child layout; final executable settling uses
    /// [`Self::new`] and can narrow those predicate-only values safely.
    pub fn for_join_enumeration() -> Self {
        Self {
            column_references: HashSet::new(),
            everything_referenced: true,
            preserve_unresolved_predicate_carriers: true,
        }
    }

    fn child(&self, everything_referenced: bool) -> Self {
        Self {
            column_references: HashSet::new(),
            everything_referenced,
            preserve_unresolved_predicate_carriers: self.preserve_unresolved_predicate_carriers,
        }
    }

    pub fn optimize(mut self, plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        let plan = self.optimize_plan(plan)?;
        crate::statistics::unique_keys::refresh_unique_keys(plan)
    }

    fn optimize_plan(&mut self, plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        let (id, stats, operator) = plan.into_parts();
        let operator = match operator {
            LogicalOperator::Projection(mut proj) => {
                let mut child_analyzer = self.child(false);
                for expr in &proj.expressions {
                    child_analyzer.visit_expression(expr);
                }
                let child = *proj.child;
                proj.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::Projection(proj)
            }
            LogicalOperator::RowFetch(mut fetch) => {
                let child_bindings = fetch.child.get_column_bindings();
                let mut child_analyzer = self.child(self.everything_referenced);
                if !self.everything_referenced {
                    child_analyzer.column_references.extend(
                        self.column_references
                            .iter()
                            .filter(|binding| child_bindings.contains(binding))
                            .copied(),
                    );
                }
                for source in &fetch.sources {
                    child_analyzer.visit_expression(&source.rowid);
                }
                let child = *fetch.child;
                fetch.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::RowFetch(fetch)
            }
            LogicalOperator::Filter(mut filter) => {
                let output_references = self.column_references.clone();
                for expr in &filter.expressions {
                    self.visit_expression(expr);
                }

                let child = *filter.child;
                filter.child = Box::new(self.optimize_plan(child)?);
                let child_bindings = filter.child.get_column_bindings();
                // Correlated-subquery flattening can temporarily leave stale
                // predicate bindings before physical reference resolution.
                // Those are execution dependencies, not output demands: only
                // an unresolved parent-visible binding can require the full
                // carrier above this Filter.
                filter.projection_map = if self.everything_referenced
                    || Self::references_unknown_binding(&output_references, &child_bindings)
                    || (self.preserve_unresolved_predicate_carriers
                        && self.has_unknown_references(&child_bindings))
                {
                    ProjectionMap::all()
                } else {
                    self.generate_exact_projection_map(&child_bindings, &output_references)
                };
                LogicalOperator::Filter(filter)
            }
            LogicalOperator::Aggregate(mut agg) => {
                let mut child_analyzer = self.child(false);
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
                let output_references = self.column_references.clone();
                for order_expr in &order.orders {
                    self.visit_expression(&order_expr.expression);
                }

                let child = *order.child;
                order.child = Box::new(self.optimize_plan(child)?);
                let child_bindings = order.child.get_column_bindings();
                order.projection_map = if self.everything_referenced
                    || Self::references_unknown_binding(&output_references, &child_bindings)
                    || (self.preserve_unresolved_predicate_carriers
                        && self.has_unknown_references(&child_bindings))
                {
                    ProjectionMap::all()
                } else {
                    self.generate_exact_projection_map(&child_bindings, &output_references)
                };
                LogicalOperator::Order(order)
            }
            LogicalOperator::Limit(mut limit) => {
                let child = *limit.child;
                limit.child = Box::new(self.optimize_plan(child)?);
                LogicalOperator::Limit(limit)
            }
            LogicalOperator::TopN(mut topn) => {
                let output_references = self.column_references.clone();
                for order_expr in &topn.orders {
                    self.visit_expression(&order_expr.expression);
                }
                let child = *topn.child;
                topn.child = Box::new(self.optimize_plan(child)?);
                let child_bindings = topn.child.get_column_bindings();
                topn.projection_map = if self.everything_referenced
                    || Self::references_unknown_binding(&output_references, &child_bindings)
                    || (self.preserve_unresolved_predicate_carriers
                        && self.has_unknown_references(&child_bindings))
                {
                    ProjectionMap::all()
                } else {
                    self.generate_exact_projection_map(&child_bindings, &output_references)
                };
                LogicalOperator::TopN(topn)
            }
            LogicalOperator::Window(mut window) => {
                // A window appends its results but otherwise passes through
                // child columns. Keep only parent-visible child bindings plus
                // the partition/order/argument/frame dependencies needed to
                // compute retained window results. Window-result bindings
                // themselves must not leak into the child analyzer: they are
                // produced here and would look like unresolved correlated
                // references below, disabling join projection pruning.
                let child_bindings = window.child.get_column_bindings();
                let mut child_analyzer = self.child(self.everything_referenced);
                if !self.everything_referenced {
                    child_analyzer.column_references.extend(
                        self.column_references
                            .iter()
                            .filter(|binding| child_bindings.contains(binding))
                            .copied(),
                    );
                }
                for expression in &window.expressions {
                    paro_planner::expression::ExpressionIterator::enumerate_window_children(
                        expression,
                        |child| child_analyzer.visit_expression(child),
                    );
                }
                let child = *window.child;
                window.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::Window(window)
            }
            LogicalOperator::Distinct(mut distinct) => {
                let child = *distinct.child;
                // DISTINCT consumes its complete input row, but that demand is
                // local to this child. Mutating the current analyzer leaks the
                // full-width contract into sibling branches and every parent
                // above the DISTINCT, preventing their projection maps from
                // representing actual output demand.
                distinct.child = Box::new(self.child(true).optimize_plan(child)?);
                LogicalOperator::Distinct(distinct)
            }
            LogicalOperator::MaterializedCTE(mut cte) => {
                let mut cte_query_analyzer = self.child(true);
                let cte_query = *cte.cte_query;
                cte.cte_query = Box::new(cte_query_analyzer.optimize_plan(cte_query)?);

                let mut child_analyzer = self.child(self.everything_referenced);
                child_analyzer.column_references = self.column_references.clone();
                let child = *cte.child;
                cte.child = Box::new(child_analyzer.optimize_plan(child)?);
                LogicalOperator::MaterializedCTE(cte)
            }
            LogicalOperator::RecursiveCTE(mut cte) => {
                let anchor = *cte.anchor;
                let recursive = *cte.recursive;
                cte.anchor = Box::new(self.child(true).optimize_plan(anchor)?);
                cte.recursive = Box::new(self.child(true).optimize_plan(recursive)?);
                LogicalOperator::RecursiveCTE(cte)
            }
            LogicalOperator::CTERef(cte_ref) => LogicalOperator::CTERef(cte_ref),
            LogicalOperator::Join(join) => self.optimize_join(join)?,
            other => other,
        };
        Ok(OwnedLogicalPlan {
            id,
            stats,
            operator,
        })
    }

    fn optimize_join(&mut self, join: Join) -> Result<LogicalOperator> {
        match join {
            Join::Comparison(mut comp_join) => {
                // Projection maps describe values visible above the join. Join
                // conditions are execution requirements, not output demands:
                // mixing the two keeps dead key columns alive through every
                // downstream join in a chain.
                let output_references = self.column_references.clone();
                for condition in &comp_join.conditions {
                    self.visit_expression(&condition.left);
                    self.visit_expression(&condition.right);
                }

                let retained_left_bindings = Self::projected_bindings(
                    &comp_join.left.get_column_bindings(),
                    &comp_join.left_projection_map,
                    "comparison join left",
                )?;
                let retained_right_bindings = Self::projected_bindings(
                    &comp_join.right.get_column_bindings(),
                    &comp_join.right_projection_map,
                    "comparison join right",
                )?;
                let mut original_bindings = comp_join.left.get_column_bindings();
                original_bindings.extend(comp_join.right.get_column_bindings());
                let preserve_planner_layout = !comp_join.duplicate_eliminated_columns.is_empty()
                    || self.has_unknown_references(&original_bindings);
                if preserve_planner_layout {
                    self.column_references
                        .extend(retained_left_bindings.iter().copied());
                    self.column_references
                        .extend(retained_right_bindings.iter().copied());
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
                if !self.everything_referenced
                    && !preserve_planner_layout
                    && !self.has_unknown_references(&known_bindings)
                {
                    comp_join.left_projection_map = if Self::join_outputs_left(comp_join.join_type)
                    {
                        self.generate_exact_projection_map(&left_bindings, &output_references)
                    } else {
                        ProjectionMap::none()
                    };
                    comp_join.right_projection_map =
                        if Self::join_outputs_right(comp_join.join_type) {
                            self.generate_exact_projection_map(&right_bindings, &output_references)
                        } else {
                            ProjectionMap::none()
                        };
                } else {
                    // `everything` means every value already exposed by this
                    // operator, not every value its children can produce.
                    // Correlated and otherwise unresolved plans likewise own
                    // an explicit visible layout. Preserve both contracts by
                    // binding identity, never by stale child positions.
                    comp_join.left_projection_map =
                        Self::remap_projection(&left_bindings, &retained_left_bindings, "left")?;
                    comp_join.right_projection_map =
                        Self::remap_projection(&right_bindings, &retained_right_bindings, "right")?;
                }

                Ok(LogicalOperator::Join(Join::Comparison(comp_join)))
            }
            Join::Any(mut any_join) => {
                let output_references = self.column_references.clone();
                self.visit_expression(&any_join.condition);

                let retained_left_bindings = Self::projected_bindings(
                    &any_join.left.get_column_bindings(),
                    &any_join.left_projection_map,
                    "ANY join left",
                )?;
                let retained_right_bindings = Self::projected_bindings(
                    &any_join.right.get_column_bindings(),
                    &any_join.right_projection_map,
                    "ANY join right",
                )?;
                let mut original_bindings = any_join.left.get_column_bindings();
                original_bindings.extend(any_join.right.get_column_bindings());
                let preserve_planner_layout = self.has_unknown_references(&original_bindings);
                if preserve_planner_layout {
                    self.column_references
                        .extend(retained_left_bindings.iter().copied());
                    self.column_references
                        .extend(retained_right_bindings.iter().copied());
                }
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

                if !self.everything_referenced
                    && !preserve_planner_layout
                    && !self.has_unknown_references(&known_bindings)
                {
                    any_join.left_projection_map = if Self::join_outputs_left(any_join.join_type) {
                        self.generate_exact_projection_map(&left_bindings, &output_references)
                    } else {
                        ProjectionMap::none()
                    };
                    any_join.right_projection_map = if Self::join_outputs_right(any_join.join_type)
                    {
                        self.generate_exact_projection_map(&right_bindings, &output_references)
                    } else {
                        ProjectionMap::none()
                    };
                } else {
                    any_join.left_projection_map =
                        Self::remap_projection(&left_bindings, &retained_left_bindings, "left")?;
                    any_join.right_projection_map =
                        Self::remap_projection(&right_bindings, &retained_right_bindings, "right")?;
                }

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
        traverse_expression(expr, &mut |expression| {
            if let Expression::ColumnRef(column_ref) = expression {
                self.add_binding(column_ref);
            }
        });
    }

    fn add_binding(&mut self, col_ref: &ColumnRefExpression) {
        self.column_references.insert(col_ref.binding);
    }

    /// Correlated-subquery flattening can temporarily leave stale table-index bindings.
    /// In that shape, projection-map pruning is unsafe and must be skipped.
    fn has_unknown_references(&self, known_bindings: &[ColumnBinding]) -> bool {
        if self.everything_referenced || self.column_references.is_empty() {
            return false;
        }
        Self::references_unknown_binding(&self.column_references, known_bindings)
    }

    fn references_unknown_binding(
        references: &HashSet<ColumnBinding>,
        known_bindings: &[ColumnBinding],
    ) -> bool {
        references
            .iter()
            .any(|binding| !known_bindings.contains(binding))
    }

    fn generate_exact_projection_map(
        &self,
        bindings: &[ColumnBinding],
        output_references: &HashSet<ColumnBinding>,
    ) -> ProjectionMap {
        if self.everything_referenced {
            return ProjectionMap::all();
        }
        ProjectionMap::new(
            bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| output_references.contains(binding).then_some(index))
                .collect(),
        )
    }

    fn projected_bindings(
        bindings: &[ColumnBinding],
        projection: &ProjectionMap,
        label: &str,
    ) -> Result<Vec<ColumnBinding>> {
        let Some(indices) = projection.as_columns() else {
            return Ok(bindings.to_vec());
        };
        indices
            .iter()
            .map(|&index| {
                bindings.get(index).copied().ok_or_else(|| {
                    paro_error::internal(format!(
                        "{label} projection index {index} exceeds child width {}",
                        bindings.len()
                    ))
                })
            })
            .collect()
    }

    fn remap_projection(
        final_bindings: &[ColumnBinding],
        retained_bindings: &[ColumnBinding],
        side: &str,
    ) -> Result<ProjectionMap> {
        let indices = retained_bindings
            .iter()
            .map(|binding| {
                final_bindings
                    .iter()
                    .position(|item| item == binding)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "{side} join output binding {binding:?} was removed while rebuilding its projection"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ProjectionMap::new(indices))
    }

    fn join_outputs_left(join_type: paro_planner::operator::JoinType) -> bool {
        !matches!(
            join_type,
            paro_planner::operator::JoinType::RightSemi
                | paro_planner::operator::JoinType::RightAnti
        )
    }

    fn join_outputs_right(join_type: paro_planner::operator::JoinType) -> bool {
        !matches!(
            join_type,
            paro_planner::operator::JoinType::Semi
                | paro_planner::operator::JoinType::Anti
                | paro_planner::operator::JoinType::Mark
        )
    }

    pub fn extract_column_bindings(expr: &Expression, bindings: &mut Vec<ColumnBinding>) {
        traverse_expression(expr, &mut |expression| {
            if let Expression::ColumnRef(column_ref) = expression {
                bindings.push(column_ref.binding);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnLifetimeAnalyzer;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::OrderByNode;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, Expression, WindowExpression,
        WindowFrame, WindowFrameBound, WindowFrameType,
    };
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, CrossProduct, Distinct, ExpressionGet, Filter, Join,
        JoinComparisonType, JoinCondition, JoinType, LogicalOperator, Order, Projection, Window,
    };
    use paro_planner::plan::OwnedLogicalPlan;

    #[test]
    fn extract_column_bindings_visits_window_frame_offsets() {
        let expected = ColumnBinding::new(7, 3);
        let expression = Expression::Window(WindowExpression::native(
            WindowFunction::row_number(),
            vec![],
            vec![],
            vec![],
            WindowFrame {
                frame_type: WindowFrameType::Rows,
                start_bound: WindowFrameBound::Offset(Box::new(Expression::ColumnRef(
                    ColumnRefExpression::new(expected, LogicalType::Integer),
                ))),
                start_is_preceding: true,
                end_bound: WindowFrameBound::CurrentRow,
                end_is_preceding: false,
            },
            false,
        ));
        let mut bindings = Vec::new();

        ColumnLifetimeAnalyzer::extract_column_bindings(&expression, &mut bindings);

        assert_eq!(bindings, vec![expected]);
    }

    #[test]
    fn delim_join_preserves_visible_rhs_projection() {
        let ctx = BindContext::new();
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["outer_key".into()],
                vec![LogicalType::Integer],
            )),
        );
        let right = OwnedLogicalPlan::new(
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
        join.right_projection_map = vec![0].into();
        let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(join)));

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.right_projection_map.as_columns(), Some(&[0][..]));
        assert_eq!(join.get_types().len(), 2);
    }

    #[test]
    fn join_condition_columns_are_not_forced_into_join_output() {
        let ctx = BindContext::new();
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["left_key".into(), "payload".into()],
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
        );
        let right = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["right_key".into(), "dead_payload".into()],
                vec![LogicalType::Integer, LogicalType::Varchar],
            )),
        );
        let join = ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(20, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        let joined = OwnedLogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(join)));
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                30,
                joined,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 1),
                    LogicalType::BigInt,
                ))],
            )),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Projection(projection) = &optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &projection.child.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.left_projection_map.as_columns(), Some(&[1][..]));
        assert!(join.right_projection_map.is_none());
        assert_eq!(join.get_types(), vec![LogicalType::BigInt]);
    }

    #[test]
    fn root_analysis_preserves_an_explicit_join_output_contract() {
        let ctx = BindContext::new();
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["left_key".into(), "payload".into()],
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
        );
        let right = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["right_key".into(), "dead_payload".into()],
                vec![LogicalType::Integer, LogicalType::Varchar],
            )),
        );
        let mut join = ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(20, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        join.left_projection_map = vec![1].into();
        join.right_projection_map.clear();
        let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Join(Join::Comparison(join)));

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.left_projection_map.as_columns(), Some(&[1][..]));
        assert!(join.right_projection_map.is_none());
        assert_eq!(join.get_types(), vec![LogicalType::BigInt]);
    }

    #[test]
    fn window_dependencies_do_not_keep_unrelated_join_payload_alive() {
        let ctx = BindContext::new();
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["partition_key".into(), "payload".into(), "dead_left".into()],
                vec![
                    LogicalType::Integer,
                    LogicalType::BigInt,
                    LogicalType::Varchar,
                ],
            )),
        );
        let right = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["join_key".into(), "dead_right".into()],
                vec![LogicalType::Integer, LogicalType::Varchar],
            )),
        );
        let joined = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                left,
                right,
                vec![JoinCondition::new(
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(20, 0),
                        LogicalType::Integer,
                    )),
                    JoinComparisonType::Equal,
                )],
            ))),
        );
        let window = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Window(Window::new(
                40,
                vec![WindowExpression::native(
                    WindowFunction::row_number(),
                    vec![],
                    vec![Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    ))],
                    vec![],
                    WindowFrame {
                        frame_type: WindowFrameType::Rows,
                        start_bound: WindowFrameBound::Unbounded,
                        start_is_preceding: true,
                        end_bound: WindowFrameBound::Unbounded,
                        end_is_preceding: false,
                    },
                    false,
                )],
                joined,
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                50,
                window,
                vec![
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 1),
                        LogicalType::BigInt,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(40, 0),
                        LogicalType::BigInt,
                    )),
                ],
            )),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Projection(projection) = &optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Window(window) = &projection.child.operator else {
            panic!("expected window");
        };
        let LogicalOperator::Join(Join::Comparison(join)) = &window.child.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.left_projection_map.as_columns(), Some(&[0, 1][..]));
        assert!(join.right_projection_map.is_none());
        assert_eq!(
            join.get_types(),
            vec![LogicalType::Integer, LogicalType::BigInt]
        );
    }

    #[test]
    fn root_semi_join_never_exposes_filtering_side_columns() {
        let ctx = BindContext::new();
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["left".into()],
                vec![LogicalType::Integer],
            )),
        );
        let right = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["right".into()],
                vec![LogicalType::Integer],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Semi,
                left,
                right,
                Vec::new(),
            ))),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.left_projection_map.to_indices(1), vec![0]);
        assert!(join.right_projection_map.is_none());
        assert_eq!(join.get_types(), vec![LogicalType::Integer]);
    }

    #[test]
    fn order_key_is_an_execution_dependency_not_an_output_dependency() {
        let ctx = BindContext::new();
        let input = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["sort_key".into(), "payload".into()],
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
        );
        let order = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Order(Order::new(
                input,
                vec![OrderByNode {
                    expression: Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    )),
                    ascending: true,
                    nulls_first: false,
                }],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                30,
                order,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 1),
                    LogicalType::BigInt,
                ))],
            )),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Projection(projection) = &optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Order(order) = &projection.child.operator else {
            panic!("expected order");
        };
        assert_eq!(order.projection_map.as_columns(), Some(&[1][..]));
    }

    #[test]
    fn filter_key_is_an_execution_dependency_not_an_output_dependency() {
        let ctx = BindContext::new();
        let input = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["filter_key".into(), "payload".into()],
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
        );
        let filter = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Filter(Filter::new(
                input,
                vec![Expression::Comparison(ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    )),
                ))],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                30,
                filter,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 1),
                    LogicalType::BigInt,
                ))],
            )),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Projection(projection) = &optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Filter(filter) = &projection.child.operator else {
            panic!("expected filter");
        };
        assert_eq!(filter.projection_map.as_columns(), Some(&[1][..]));
    }

    #[test]
    fn unresolved_filter_key_does_not_expand_the_visible_output_contract() {
        let ctx = BindContext::new();
        let input = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["key".into(), "payload".into()],
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
        );
        let filter = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Filter(Filter::new(
                input,
                vec![Expression::Comparison(ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(99, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(99, 0),
                        LogicalType::Integer,
                    )),
                ))],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                30,
                filter,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 1),
                    LogicalType::BigInt,
                ))],
            )),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Projection(projection) = &optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Filter(filter) = &projection.child.operator else {
            panic!("expected filter");
        };
        assert_eq!(filter.projection_map.as_columns(), Some(&[1][..]));
    }

    #[test]
    fn distinct_full_row_demand_is_isolated_from_parent_filter_output() {
        let ctx = BindContext::new();
        let preserved = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                10,
                Vec::new(),
                vec!["filter_key".into(), "payload".into()],
                vec![LogicalType::Integer, LogicalType::BigInt],
            )),
        );
        let distinct_input = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                20,
                Vec::new(),
                vec!["distinct_key".into()],
                vec![LogicalType::Integer],
            )),
        );
        let distinct = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Distinct(Distinct::new(distinct_input)),
        );
        let cross = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Join(Join::Cross(CrossProduct::new(preserved, distinct))),
        );
        let filter = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Filter(Filter::new(
                cross,
                vec![Expression::Comparison(ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    )),
                    Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(10, 0),
                        LogicalType::Integer,
                    )),
                ))],
            )),
        );
        let plan = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::Projection(Projection::new(
                30,
                filter,
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(10, 1),
                    LogicalType::BigInt,
                ))],
            )),
        );

        let optimized = ColumnLifetimeAnalyzer::new(true).optimize(plan).unwrap();
        let LogicalOperator::Projection(projection) = &optimized.operator else {
            panic!("expected projection");
        };
        let LogicalOperator::Filter(filter) = &projection.child.operator else {
            panic!("expected filter");
        };
        assert_eq!(filter.projection_map.as_columns(), Some(&[1][..]));
    }
}
