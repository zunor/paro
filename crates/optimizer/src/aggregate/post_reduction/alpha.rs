// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Proof-grade alpha equivalence for deterministic relational inputs.

use std::collections::HashMap;

use paro_planner::expression::Expression;
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Filter, Join, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

use super::{aggregate_kernels_equal, clean_inner_join, is_movable};
use crate::aggregate::semantic_kernels::{cast_kernels_equal, scalar_kernels_equal};

#[derive(Default)]
pub(super) struct AlphaBindings {
    forward: HashMap<ColumnBinding, ColumnBinding>,
    reverse: HashMap<ColumnBinding, ColumnBinding>,
}

impl AlphaBindings {
    pub(super) fn match_sources(grouped: &LogicalPlan, scalar: &LogicalPlan) -> Option<Self> {
        let mut bindings = Self::default();
        bindings.match_plan(grouped, scalar).then_some(bindings)
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

    pub(super) fn expressions_equal(&self, grouped: &Expression, scalar: &Expression) -> bool {
        if grouped.return_type() != scalar.return_type()
            || !is_movable(grouped)
            || !is_movable(scalar)
        {
            return false;
        }
        self.semantic_expression_equal(grouped, scalar)
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
            // Operator, Case, Parameter, Reference, Subquery, and Window
            // nodes are intentionally outside the proof language.  Q11's
            // common relational input needs none of them; admitting one in
            // the future requires spelling out all of its bound semantics.
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
                let (Some(left_table), Some(right_table)) = (&left.table, &right.table) else {
                    return false;
                };
                if left_table.base.base.catalog != right_table.base.base.catalog
                    || left_table.base.schema_name != right_table.base.schema_name
                    || left_table.base.base.object_id != right_table.base.base.object_id
                    || left.column_ids.len() != left.column_types.len()
                    || left.column_ids.len() != left.returned_types.len()
                    || right.column_ids.len() != right.column_types.len()
                    || right.column_ids.len() != right.returned_types.len()
                    || left.scan_order.is_some()
                    || right.scan_order.is_some()
                    || !left.runtime_filter_expressions.is_empty()
                    || !right.runtime_filter_expressions.is_empty()
                {
                    return false;
                }

                // The grouped branch may retain an additional grouping key.
                // Pair outputs through stable physical column ids, never
                // through branch-local scan ordinals.
                right.column_ids.iter().enumerate().all(|(right_idx, id)| {
                    let Some(left_idx) =
                        left.column_ids.iter().position(|candidate| candidate == id)
                    else {
                        return false;
                    };
                    left.column_types[left_idx] == right.column_types[right_idx]
                        && left.returned_types[left_idx] == right.returned_types[right_idx]
                        && self.bind(
                            ColumnBinding::new(left.table_index, left_idx),
                            ColumnBinding::new(right.table_index, right_idx),
                        )
                })
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
