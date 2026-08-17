// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Proof-grade alpha equivalence for deterministic relational inputs.

use std::collections::HashMap;

use paro_planner::expression::{Expression, ExpressionIterator, ExpressionVisitDecision};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Filter, Get, Join, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

use super::{clean_inner_join, is_movable};
use crate::aggregate::semantic_kernels::{
    aggregate_kernels_equal, cast_kernels_equal, scalar_kernels_equal,
};

#[derive(Default)]
pub(crate) struct AlphaBindings {
    forward: HashMap<ColumnBinding, ColumnBinding>,
    reverse: HashMap<ColumnBinding, ColumnBinding>,
}

impl AlphaBindings {
    pub(super) fn match_sources(grouped: &LogicalPlan, scalar: &LogicalPlan) -> Option<Self> {
        let mut bindings = Self::default();
        bindings.match_plan(grouped, scalar).then_some(bindings)
    }

    /// Pair two scans of the same catalog object through stable physical
    /// column ids. The detail scan may expose columns absent from the scalar
    /// branch, but every scalar column must have an exact detail counterpart.
    pub(crate) fn match_gets(detail: &Get, scalar: &Get) -> Option<Self> {
        let mut bindings = Self::default();
        bindings.match_get_pair(detail, scalar).then_some(bindings)
    }

    pub(super) fn bind(&mut self, grouped: ColumnBinding, scalar: ColumnBinding) -> bool {
        match (self.forward.get(&grouped), self.reverse.get(&scalar)) {
            (Some(existing), _) => *existing == scalar,
            (_, Some(existing)) => *existing == grouped,
            (None, None) => {
                self.forward.insert(grouped, scalar);
                self.reverse.insert(scalar, grouped);
                true
            }
        }
    }

    pub(crate) fn expressions_equal(&self, grouped: &Expression, scalar: &Expression) -> bool {
        if grouped.return_type() != scalar.return_type()
            || !is_movable(grouped)
            || !is_movable(scalar)
        {
            return false;
        }
        self.semantic_expression_equal(grouped, scalar)
    }

    /// Rebase a scalar-branch expression into the detail scan's binding
    /// domain. Unknown or unmapped expression domains fail closed.
    pub(crate) fn rebase_scalar(&self, expression: &Expression) -> Option<Expression> {
        let mut valid = true;
        ExpressionIterator::visit(expression, &mut |node| match node {
            Expression::ColumnRef(column) => {
                valid &= column.depth == 0 && self.reverse.contains_key(&column.binding);
                ExpressionVisitDecision::SkipChildren
            }
            Expression::Reference(_) | Expression::Subquery(_) | Expression::Window(_) => {
                valid = false;
                ExpressionVisitDecision::SkipChildren
            }
            _ => ExpressionVisitDecision::Descend,
        });
        valid.then(|| {
            expression.clone().replace_column_ref(&|column| {
                self.reverse.get(&column.binding).copied().map(|binding| {
                    Expression::ColumnRef(paro_planner::expression::ColumnRefExpression::new(
                        binding,
                        column.return_type.clone(),
                    ))
                })
            })
        })
    }

    /// Proof-grade equality for the deliberately small expression language
    /// admitted by this rewrite.  The general-purpose `Expression::equals`
    /// is suitable for common-subexpression heuristics, but intentionally
    /// omits execution details such as `TRY_CAST` and bound routine kernels.
    /// A post-aggregate reduction removes an entire execution, so an unknown
    /// node or uncomparable bind payload must decline the rewrite.
    fn semantic_expression_equal(&self, grouped: &Expression, scalar: &Expression) -> bool {
        match (grouped, scalar) {
            (Expression::Constant(left), Expression::Constant(right)) => {
                left.return_type == right.return_type && left.value == right.value
            }
            (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
                left.depth == 0
                    && right.depth == 0
                    && left.return_type == right.return_type
                    && self.forward.get(&left.binding) == Some(&right.binding)
            }
            (Expression::Function(left), Expression::Function(right)) => {
                left.return_type == right.return_type
                    && left.routine_meta == right.routine_meta
                    && scalar_kernels_equal(&left.function, &right.function)
                    && self.expression_slices_equal(&left.children, &right.children)
            }
            (Expression::Cast(left), Expression::Cast(right)) => {
                left.target_type == right.target_type
                    && left.try_cast == right.try_cast
                    && cast_kernels_equal(&left.cast_info, &right.cast_info)
                    && self.semantic_expression_equal(&left.child, &right.child)
            }
            (Expression::Conjunction(left), Expression::Conjunction(right)) => {
                left.conjunction_type == right.conjunction_type
                    && self.expression_slices_equal(&left.children, &right.children)
            }
            (Expression::Comparison(left), Expression::Comparison(right)) => {
                left.comparison_type == right.comparison_type
                    && self.semantic_expression_equal(&left.left, &right.left)
                    && self.semantic_expression_equal(&left.right, &right.right)
            }
            (Expression::Operator(left), Expression::Operator(right)) => {
                left.operator_type == right.operator_type
                    && left.return_type == right.return_type
                    && self.expression_slices_equal(&left.children, &right.children)
            }
            (Expression::Aggregate(left), Expression::Aggregate(right)) => {
                left.return_type == right.return_type
                    && left.aggr_type == right.aggr_type
                    && aggregate_kernels_equal(left, right)
                    && self.expression_slices_equal(&left.children, &right.children)
                    && match (&left.filter, &right.filter) {
                        (Some(left), Some(right)) => self.semantic_expression_equal(left, right),
                        (None, None) => true,
                        _ => false,
                    }
                    && left.order_bys.len() == right.order_bys.len()
                    && left
                        .order_bys
                        .iter()
                        .zip(&right.order_bys)
                        .all(|(left, right)| {
                            left.ascending == right.ascending
                                && left.nulls_first == right.nulls_first
                                && self
                                    .semantic_expression_equal(&left.expression, &right.expression)
                        })
            }
            // Case, Parameter, Reference, Subquery, and Window nodes are
            // intentionally outside the proof language. Admitting one
            // requires spelling out all of its bound semantics.
            _ => false,
        }
    }

    fn expression_slices_equal(&self, grouped: &[Expression], scalar: &[Expression]) -> bool {
        grouped.len() == scalar.len()
            && grouped
                .iter()
                .zip(scalar)
                .all(|(left, right)| self.semantic_expression_equal(left, right))
    }

    fn match_plan(&mut self, grouped: &LogicalPlan, scalar: &LogicalPlan) -> bool {
        match (&grouped.operator, &scalar.operator) {
            (LogicalOperator::Get(left), LogicalOperator::Get(right)) => {
                self.match_get_pair(left, right)
            }
            (LogicalOperator::Filter(left), LogicalOperator::Filter(right)) => {
                self.match_filters(left, right)
            }
            (LogicalOperator::Projection(left), LogicalOperator::Projection(right)) => {
                self.match_projections(left, right)
            }
            (
                LogicalOperator::Join(Join::Comparison(left)),
                LogicalOperator::Join(Join::Comparison(right)),
            ) => self.match_joins(left, right),
            _ => false,
        }
    }

    fn match_get_pair(&mut self, detail: &Get, scalar: &Get) -> bool {
        let (Some(detail_table), Some(scalar_table)) = (&detail.table, &scalar.table) else {
            return false;
        };
        if detail_table.base.base.catalog != scalar_table.base.base.catalog
            || detail_table.base.schema_name != scalar_table.base.schema_name
            || detail_table.base.base.object_id != scalar_table.base.base.object_id
            || detail.column_ids.len() != detail.column_types.len()
            || detail.column_ids.len() != detail.returned_types.len()
            || detail.column_ids.len() != detail.column_projections.len()
            || scalar.column_ids.len() != scalar.column_types.len()
            || scalar.column_ids.len() != scalar.returned_types.len()
            || scalar.column_ids.len() != scalar.column_projections.len()
            || detail.scan_order.is_some()
            || scalar.scan_order.is_some()
            || !detail.runtime_filter_expressions.is_empty()
            || !scalar.runtime_filter_expressions.is_empty()
        {
            return false;
        }

        scalar
            .column_ids
            .iter()
            .enumerate()
            .all(|(scalar_idx, id)| {
                let Some(detail_idx) = detail
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == id)
                else {
                    return false;
                };
                detail.column_types[detail_idx] == scalar.column_types[scalar_idx]
                    && detail.returned_types[detail_idx] == scalar.returned_types[scalar_idx]
                    && detail.column_projections[detail_idx]
                        == scalar.column_projections[scalar_idx]
                    && self.bind(
                        ColumnBinding::new(detail.table_index, detail_idx),
                        ColumnBinding::new(scalar.table_index, scalar_idx),
                    )
            })
    }

    fn match_filters(&mut self, grouped: &Filter, scalar: &Filter) -> bool {
        self.match_plan(&grouped.child, &scalar.child)
            && grouped
                .projection_map
                .is_identity(grouped.child.types().len())
            && scalar
                .projection_map
                .is_identity(scalar.child.types().len())
            && grouped.expressions.len() == scalar.expressions.len()
            && grouped
                .expressions
                .iter()
                .zip(&scalar.expressions)
                .all(|(left, right)| self.expressions_equal(left, right))
    }

    fn match_projections(&mut self, grouped: &Projection, scalar: &Projection) -> bool {
        if !self.match_plan(&grouped.child, &scalar.child)
            || scalar.expressions.len() > grouped.expressions.len()
            || grouped.returned_types.len() != grouped.expressions.len()
            || scalar.returned_types.len() != scalar.expressions.len()
        {
            return false;
        }
        let mut matched_grouped = vec![false; grouped.expressions.len()];
        for (scalar_idx, scalar_expression) in scalar.expressions.iter().enumerate() {
            let Some(grouped_idx) = grouped.expressions.iter().enumerate().position(
                |(grouped_idx, grouped_expression)| {
                    !matched_grouped[grouped_idx]
                        && grouped.returned_types[grouped_idx] == scalar.returned_types[scalar_idx]
                        && self.expressions_equal(grouped_expression, scalar_expression)
                },
            ) else {
                return false;
            };
            matched_grouped[grouped_idx] = true;
            if !self.bind(
                ColumnBinding::new(grouped.table_index, grouped_idx),
                ColumnBinding::new(scalar.table_index, scalar_idx),
            ) {
                return false;
            }
        }
        true
    }

    fn match_joins(&mut self, grouped: &ComparisonJoin, scalar: &ComparisonJoin) -> bool {
        if !clean_inner_join(grouped)
            || !clean_inner_join(scalar)
            || grouped.conditions.len() != scalar.conditions.len()
            || !self.match_plan(&grouped.left, &scalar.left)
            || !self.match_plan(&grouped.right, &scalar.right)
        {
            return false;
        }
        grouped
            .conditions
            .iter()
            .zip(&scalar.conditions)
            .all(|(left, right)| {
                left.comparison == right.comparison
                    && self.expressions_equal(&left.left, &right.left)
                    && self.expressions_equal(&left.right, &right.right)
            })
    }
}
