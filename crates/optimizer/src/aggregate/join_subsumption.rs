// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reuse a filtered grouped aggregate across a later detail join.
//!
//! A common analytical shape first computes one row per detail key in a
//! reduction subquery and later joins the surviving keys back to the same
//! detail table to calculate the same distributive aggregate again. The
//! second scan is unnecessary: make the reduction join propagate its partial
//! aggregate and let the outer aggregate combine those partials.
//!
//! The rewrite is deliberately structural. It requires identical catalog
//! table identity and physical column ids on both scans, a one-row-per-key
//! reduction aggregate, and an aggregate implementation that explicitly
//! advertises its algebra. It never relies on catalog uniqueness statistics.

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::aggregate::AggregateAlgebra;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression,
};
use paro_planner::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, Get, Join, JoinComparisonType, JoinType,
    LogicalOperator, ProjectionMap,
};
use paro_planner::plan::LogicalPlan;

#[derive(Clone)]
struct OuterSum {
    input_binding: ColumnBinding,
    return_type: LogicalType,
}

struct DetailScan {
    table: Arc<TableCatalogEntry>,
    table_index: usize,
    key_column_id: usize,
    value_column_id: usize,
}

#[derive(Clone)]
enum ExposureMutation {
    None,
    AppendProjection {
        aggregate_binding: ColumnBinding,
        aggregate_type: LogicalType,
    },
}

struct ReductionExposure {
    output_binding: ColumnBinding,
    output_type: LogicalType,
    mutation: ExposureMutation,
}

/// Eliminate redundant detail scans covered by a filtered partial aggregate.
pub fn optimize_plan(plan: LogicalPlan) -> LogicalPlan {
    fn rewrite(plan: LogicalPlan) -> LogicalPlan {
        let mut plan = plan.map_children(rewrite);
        let LogicalOperator::Aggregate(aggregate) = &mut plan.operator else {
            return plan;
        };
        let Some(outer_sum) = AggregateJoinSubsumption::outer_sum(aggregate) else {
            return plan;
        };
        let Some(replacement) =
            AggregateJoinSubsumption::substitute_detail_join(aggregate.child.as_mut(), &outer_sum)
        else {
            return plan;
        };

        aggregate.aggregates[0] = replacement;
        aggregate.recompute_returned_types();
        plan
    }

    rewrite(plan)
}

struct AggregateJoinSubsumption;

impl AggregateJoinSubsumption {
    fn outer_sum(aggregate: &Aggregate) -> Option<OuterSum> {
        if aggregate.aggregates.len() != 1 || !aggregate.grouping_functions.is_empty() {
            return None;
        }
        let Expression::Aggregate(sum) = &aggregate.aggregates[0] else {
            return None;
        };
        if !Self::is_plain_sum(sum) {
            return None;
        }
        let [Expression::ColumnRef(input)] = sum.children.as_slice() else {
            return None;
        };
        if input.depth != 0
            || aggregate
                .groups
                .iter()
                .any(|group| Self::references_table(group, input.binding.table_index))
        {
            return None;
        }
        // Grouping sets do not change the proof: the rewrite preserves every
        // input contribution and only replaces each detail run by its additive
        // partial. Empty grouping sets therefore remain valid as well.
        Some(OuterSum {
            input_binding: input.binding,
            return_type: sum.return_type.clone(),
        })
    }

    fn substitute_detail_join(plan: &mut LogicalPlan, outer_sum: &OuterSum) -> Option<Expression> {
        if let Some(replacement) = Self::try_substitute_reduction_join(plan, outer_sum) {
            return Some(replacement);
        }
        if let Some(replacement) = Self::try_substitute_direct_join(plan, outer_sum) {
            return Some(replacement);
        }
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            return None;
        };
        if !Self::is_clean_inner_join(join) {
            return None;
        }

        // This pass runs before cost-based join ordering. Search a clean inner
        // region for the edge that directly joins the redundant detail scan;
        // removing it first lets join ordering cost the smaller graph.
        Self::substitute_detail_join(join.left.as_mut(), outer_sum)
            .or_else(|| Self::substitute_detail_join(join.right.as_mut(), outer_sum))
    }

    /// Rewrite the pre-join-order shape where a reduction SEMI join wraps an
    /// inner region that still contains the redundant detail scan.
    fn try_substitute_reduction_join(
        plan: &mut LogicalPlan,
        outer_sum: &OuterSum,
    ) -> Option<Expression> {
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            return None;
        };
        let (preserved, reduction, preserved_projection, reduction_projection) =
            match join.join_type {
                JoinType::Semi => (
                    join.left.as_mut(),
                    join.right.as_mut(),
                    &join.left_projection_map,
                    &mut join.right_projection_map,
                ),
                JoinType::RightSemi => (
                    join.right.as_mut(),
                    join.left.as_mut(),
                    &join.right_projection_map,
                    &mut join.left_projection_map,
                ),
                _ => return None,
            };
        if join.conditions.len() != 1
            || join.mark_index.is_some()
            || !join.duplicate_eliminated_columns.is_empty()
            || join.delim_flipped
            || !preserved_projection.is_all()
            || !reduction_projection.is_none()
        {
            return None;
        }
        let condition = &join.conditions[0];
        if condition.comparison != JoinComparisonType::Equal {
            return None;
        }
        let left = Self::column_binding(&condition.left)?;
        let right = Self::column_binding(&condition.right)?;
        let preserved_bindings = preserved.get_column_bindings();
        let reduction_bindings = reduction.get_column_bindings();
        let (preserved_key, reduction_key) =
            if preserved_bindings.contains(&left) && reduction_bindings.contains(&right) {
                (left, right)
            } else if preserved_bindings.contains(&right) && reduction_bindings.contains(&left) {
                (right, left)
            } else {
                return None;
            };

        let detail = Self::inspect_detail_edge(preserved, preserved_key, outer_sum)?;
        let exposure = Self::inspect_reduction(reduction, reduction_key, &detail)?;
        let replacement = Self::replacement_sum(&exposure, outer_sum)?;

        Self::apply_exposure(reduction, &exposure.mutation)?;
        if !Self::remove_detail_edge(preserved, preserved_key, &detail, outer_sum) {
            return None;
        }
        join.join_type = JoinType::Inner;
        *reduction_projection = ProjectionMap::all();
        Some(replacement)
    }

    fn inspect_detail_edge(
        plan: &LogicalPlan,
        preserved_key: ColumnBinding,
        outer_sum: &OuterSum,
    ) -> Option<DetailScan> {
        let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
            return None;
        };
        if !Self::is_clean_inner_join(join) {
            return None;
        }
        if join.conditions.len() == 1 {
            let detail_on_left = Self::direct_detail_get(join.left.as_ref(), outer_sum).is_some();
            let detail_on_right = Self::direct_detail_get(join.right.as_ref(), outer_sum).is_some();
            if detail_on_left != detail_on_right {
                let detail_get = if detail_on_left {
                    Self::direct_detail_get(join.left.as_ref(), outer_sum)
                } else {
                    Self::direct_detail_get(join.right.as_ref(), outer_sum)
                }?;
                let (detail_key, edge_preserved_key) =
                    Self::detail_join_keys(&join.conditions[0], detail_get.table_index)?;
                if edge_preserved_key == preserved_key {
                    return Some(DetailScan {
                        table: detail_get.table.as_ref()?.clone(),
                        table_index: detail_get.table_index,
                        key_column_id: *detail_get.column_ids.get(detail_key.column_index)?,
                        value_column_id: *detail_get
                            .column_ids
                            .get(outer_sum.input_binding.column_index)?,
                    });
                }
            }
        }
        Self::inspect_detail_edge(join.left.as_ref(), preserved_key, outer_sum)
            .or_else(|| Self::inspect_detail_edge(join.right.as_ref(), preserved_key, outer_sum))
    }

    fn remove_detail_edge(
        plan: &mut LogicalPlan,
        preserved_key: ColumnBinding,
        detail: &DetailScan,
        outer_sum: &OuterSum,
    ) -> bool {
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            return false;
        };
        if !Self::is_clean_inner_join(join) {
            return false;
        }
        if join.conditions.len() == 1 {
            // Revalidate the exact leaf shape used during inspection instead
            // of inferring it from output bindings. Apart from avoiding the
            // vacuous truth of `all()` on an empty projection, this keeps the
            // destructive mutation tied to the same table and value column
            // that justified the aggregate substitution.
            let detail_on_left = Self::direct_detail_get(join.left.as_ref(), outer_sum)
                .is_some_and(|get| get.table_index == detail.table_index);
            let detail_on_right = Self::direct_detail_get(join.right.as_ref(), outer_sum)
                .is_some_and(|get| get.table_index == detail.table_index);
            if detail_on_left != detail_on_right
                && Self::detail_join_keys(&join.conditions[0], detail.table_index)
                    .is_some_and(|(_, edge_preserved)| edge_preserved == preserved_key)
            {
                let replacement = if detail_on_left {
                    std::mem::replace(
                        &mut join.right,
                        Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
                    )
                } else {
                    std::mem::replace(
                        &mut join.left,
                        Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
                    )
                };
                *plan = *replacement;
                return true;
            }
        }
        Self::remove_detail_edge(join.left.as_mut(), preserved_key, detail, outer_sum)
            || Self::remove_detail_edge(join.right.as_mut(), preserved_key, detail, outer_sum)
    }

    fn try_substitute_direct_join(
        plan: &mut LogicalPlan,
        outer_sum: &OuterSum,
    ) -> Option<Expression> {
        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            return None;
        };
        if !Self::is_clean_inner_join(join) || join.conditions.len() != 1 {
            return None;
        }

        let detail_on_left = Self::direct_detail_get(join.left.as_ref(), outer_sum).is_some();
        let detail_on_right = Self::direct_detail_get(join.right.as_ref(), outer_sum).is_some();
        if detail_on_left == detail_on_right {
            return None;
        }

        let (detail_plan, preserved_plan) = if detail_on_left {
            (join.left.as_ref(), join.right.as_mut())
        } else {
            (join.right.as_ref(), join.left.as_mut())
        };
        let detail_get = Self::direct_detail_get(detail_plan, outer_sum)?;
        let (detail_key, preserved_key) =
            Self::detail_join_keys(&join.conditions[0], detail_get.table_index)?;
        let key_column_id = *detail_get.column_ids.get(detail_key.column_index)?;
        let value_column_id = *detail_get
            .column_ids
            .get(outer_sum.input_binding.column_index)?;
        let scan = DetailScan {
            table: detail_get.table.as_ref()?.clone(),
            table_index: detail_get.table_index,
            key_column_id,
            value_column_id,
        };

        let replacement =
            Self::expose_reduction_sum(preserved_plan, preserved_key, &scan, outer_sum)?;

        let replacement_plan = if detail_on_left {
            std::mem::replace(
                &mut join.right,
                Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
            )
        } else {
            std::mem::replace(
                &mut join.left,
                Box::new(LogicalPlan::synthetic(LogicalOperator::DummyScan)),
            )
        };
        *plan = *replacement_plan;
        Some(replacement)
    }

    fn expose_reduction_sum(
        plan: &mut LogicalPlan,
        preserved_key: ColumnBinding,
        detail: &DetailScan,
        outer_sum: &OuterSum,
    ) -> Option<Expression> {
        if let LogicalOperator::Filter(filter) = &mut plan.operator {
            if !filter.projection_map.is_all() {
                return None;
            }
            return Self::expose_reduction_sum(
                filter.child.as_mut(),
                preserved_key,
                detail,
                outer_sum,
            );
        }

        let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator else {
            return None;
        };

        if let Some(replacement) =
            Self::try_expose_from_reduction_join(join, preserved_key, detail, outer_sum)
        {
            return Some(replacement);
        }

        if !Self::is_clean_inner_join(join) {
            return None;
        }
        let left_has_key = join.left.get_column_bindings().contains(&preserved_key);
        let right_has_key = join.right.get_column_bindings().contains(&preserved_key);
        if left_has_key == right_has_key {
            return None;
        }
        if left_has_key {
            Self::expose_reduction_sum(join.left.as_mut(), preserved_key, detail, outer_sum)
        } else {
            Self::expose_reduction_sum(join.right.as_mut(), preserved_key, detail, outer_sum)
        }
    }

    fn try_expose_from_reduction_join(
        join: &mut ComparisonJoin,
        preserved_key: ColumnBinding,
        detail: &DetailScan,
        outer_sum: &OuterSum,
    ) -> Option<Expression> {
        let (preserved, reduction, preserved_projection, reduction_projection) =
            match join.join_type {
                JoinType::Semi => (
                    join.left.as_ref(),
                    join.right.as_mut(),
                    &join.left_projection_map,
                    &mut join.right_projection_map,
                ),
                JoinType::RightSemi => (
                    join.right.as_ref(),
                    join.left.as_mut(),
                    &join.right_projection_map,
                    &mut join.left_projection_map,
                ),
                _ => return None,
            };
        if join.conditions.len() != 1
            || join.mark_index.is_some()
            || !join.duplicate_eliminated_columns.is_empty()
            || join.delim_flipped
            || !preserved_projection.is_all()
            || !reduction_projection.is_none()
            || !preserved.get_column_bindings().contains(&preserved_key)
        {
            return None;
        }
        let condition = &join.conditions[0];
        if condition.comparison != JoinComparisonType::Equal {
            return None;
        }
        let left = Self::column_binding(&condition.left)?;
        let right = Self::column_binding(&condition.right)?;
        let reduction_key = if left == preserved_key {
            right
        } else if right == preserved_key {
            left
        } else {
            return None;
        };

        let exposure = Self::inspect_reduction(&*reduction, reduction_key, detail)?;
        let replacement = Self::replacement_sum(&exposure, outer_sum)?;
        Self::apply_exposure(reduction, &exposure.mutation)?;
        join.join_type = JoinType::Inner;
        *reduction_projection = ProjectionMap::all();

        Some(replacement)
    }

    fn replacement_sum(exposure: &ReductionExposure, outer_sum: &OuterSum) -> Option<Expression> {
        let (function, target_types) = get_sum_function()
            .bind(std::slice::from_ref(&exposure.output_type))
            .ok()?;
        // Rebinding the partial changes the return type for integer SUM, so it
        // is intentionally outside this rewrite's additive closure today.
        // DECIMAL retains its exact type. DOUBLE is admitted explicitly under
        // the same reassociation semantics already used by parallel combine;
        // aggregate results are not promised to be bitwise order-stable.
        if target_types != [exposure.output_type.clone()]
            || function.algebra != Some(AggregateAlgebra::Sum)
            || function.return_type != outer_sum.return_type
        {
            return None;
        }
        Some(Expression::Aggregate(AggregateExpression::new(
            function,
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                exposure.output_binding,
                exposure.output_type.clone(),
            ))],
            outer_sum.return_type.clone(),
        )))
    }

    fn inspect_reduction(
        plan: &LogicalPlan,
        reduction_key: ColumnBinding,
        detail: &DetailScan,
    ) -> Option<ReductionExposure> {
        if let LogicalOperator::Projection(projection) = &plan.operator {
            if reduction_key.table_index != projection.table_index {
                return None;
            }
            let projected_key =
                Self::column_binding(projection.expressions.get(reduction_key.column_index)?)?;
            let (aggregate_binding, aggregate_type) =
                Self::inspect_reduction_core(projection.child.as_ref(), projected_key, detail)?;
            if let Some((index, expression)) = projection
                .expressions
                .iter()
                .enumerate()
                .find(|(_, expression)| Self::column_binding(expression) == Some(aggregate_binding))
            {
                return Some(ReductionExposure {
                    output_binding: ColumnBinding::new(projection.table_index, index),
                    output_type: expression.return_type(),
                    mutation: ExposureMutation::None,
                });
            }
            return Some(ReductionExposure {
                output_binding: ColumnBinding::new(
                    projection.table_index,
                    projection.expressions.len(),
                ),
                output_type: aggregate_type.clone(),
                mutation: ExposureMutation::AppendProjection {
                    aggregate_binding,
                    aggregate_type,
                },
            });
        }

        let (aggregate_binding, aggregate_type) =
            Self::inspect_reduction_core(plan, reduction_key, detail)?;
        Some(ReductionExposure {
            output_binding: aggregate_binding,
            output_type: aggregate_type,
            mutation: ExposureMutation::None,
        })
    }

    fn inspect_reduction_core(
        plan: &LogicalPlan,
        reduction_key: ColumnBinding,
        detail: &DetailScan,
    ) -> Option<(ColumnBinding, LogicalType)> {
        if let LogicalOperator::Filter(filter) = &plan.operator {
            if !filter.projection_map.is_all() {
                return None;
            }
            return Self::inspect_reduction_core(filter.child.as_ref(), reduction_key, detail);
        }
        let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
            return None;
        };
        if aggregate.groups.len() != 1
            || !aggregate.grouping_sets.is_empty()
            || !aggregate.grouping_functions.is_empty()
            || reduction_key != ColumnBinding::new(aggregate.group_index, 0)
        {
            return None;
        }
        let Expression::ColumnRef(group_key) = &aggregate.groups[0] else {
            return None;
        };
        if group_key.depth != 0 || group_key.binding.table_index == detail.table_index {
            return None;
        }
        let LogicalOperator::Get(get) = &aggregate.child.operator else {
            return None;
        };
        if get.table_index != group_key.binding.table_index
            || !get.runtime_filter_expressions.is_empty()
            || !get
                .table
                .as_ref()
                .is_some_and(|table| Arc::ptr_eq(table, &detail.table))
            || *get.column_ids.get(group_key.binding.column_index)? != detail.key_column_id
        {
            return None;
        }

        aggregate
            .aggregates
            .iter()
            .enumerate()
            .find_map(|(index, expression)| {
                let Expression::Aggregate(sum) = expression else {
                    return None;
                };
                if !Self::is_plain_sum(sum) {
                    return None;
                }
                let [Expression::ColumnRef(value)] = sum.children.as_slice() else {
                    return None;
                };
                if value.depth != 0
                    || value.binding.table_index != get.table_index
                    || get.column_ids.get(value.binding.column_index).copied()
                        != Some(detail.value_column_id)
                {
                    return None;
                }
                Some((
                    ColumnBinding::new(aggregate.aggregate_index, index),
                    sum.return_type.clone(),
                ))
            })
    }

    fn apply_exposure(plan: &mut LogicalPlan, mutation: &ExposureMutation) -> Option<()> {
        let ExposureMutation::AppendProjection {
            aggregate_binding,
            aggregate_type,
        } = mutation
        else {
            return Some(());
        };
        let LogicalOperator::Projection(projection) = &mut plan.operator else {
            return None;
        };
        projection
            .expressions
            .push(Expression::ColumnRef(ColumnRefExpression::new(
                *aggregate_binding,
                aggregate_type.clone(),
            )));
        projection
            .output_names
            .push("partial_aggregate".to_string());
        projection.returned_types.push(aggregate_type.clone());
        Some(())
    }

    fn direct_detail_get<'a>(plan: &'a LogicalPlan, outer_sum: &OuterSum) -> Option<&'a Get> {
        let LogicalOperator::Get(get) = &plan.operator else {
            return None;
        };
        (get.table_index == outer_sum.input_binding.table_index
            && get.runtime_filter_expressions.is_empty()
            && get.table.is_some())
        .then_some(get)
    }

    fn detail_join_keys(
        condition: &paro_planner::operator::JoinCondition,
        detail_table_index: usize,
    ) -> Option<(ColumnBinding, ColumnBinding)> {
        if condition.comparison != JoinComparisonType::Equal {
            return None;
        }
        let left = Self::column_binding(&condition.left)?;
        let right = Self::column_binding(&condition.right)?;
        match (
            left.table_index == detail_table_index,
            right.table_index == detail_table_index,
        ) {
            (true, false) => Some((left, right)),
            (false, true) => Some((right, left)),
            _ => None,
        }
    }

    fn is_plain_sum(aggregate: &AggregateExpression) -> bool {
        aggregate.function.algebra == Some(AggregateAlgebra::Sum)
            && aggregate.aggr_type == AggregateType::NonDistinct
            && aggregate.filter.is_none()
            && aggregate.order_bys.is_empty()
            && aggregate.children.len() == 1
    }

    fn is_clean_inner_join(join: &ComparisonJoin) -> bool {
        join.join_type == JoinType::Inner
            && join.mark_index.is_none()
            && join.duplicate_eliminated_columns.is_empty()
            && !join.delim_flipped
            && join.left_projection_map.is_all()
            && join.right_projection_map.is_all()
    }

    fn column_binding(expression: &Expression) -> Option<ColumnBinding> {
        let Expression::ColumnRef(column) = expression else {
            return None;
        };
        (column.depth == 0).then_some(column.binding)
    }

    fn references_table(expression: &Expression, table_index: usize) -> bool {
        if matches!(expression, Expression::ColumnRef(column) if column.depth == 0 && column.binding.table_index == table_index)
        {
            return true;
        }
        let mut found = false;
        paro_planner::expression::ExpressionIterator::enumerate_children(expression, |child| {
            if !found {
                found = Self::references_table(child, table_index);
            }
        });
        found
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry,
    };
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_planner::expression::{AggregateExpression, ColumnRefExpression, Expression};
    use paro_planner::operator::{
        Aggregate, ColumnBinding, ExpressionGet, Get, Join, JoinCondition, JoinType,
        LogicalOperator, Projection,
    };
    use paro_planner::plan::LogicalPlan;
    use paro_storage::table::table_factory::TableFactory;

    use super::optimize_plan;

    const OUTER_DETAIL: usize = 10;
    const INNER_DETAIL: usize = 20;
    const PRESERVED: usize = 30;
    const INNER_GROUP: usize = 40;
    const INNER_AGGREGATE: usize = 41;
    const REDUCTION_PROJECTION: usize = 50;
    const OUTER_GROUP: usize = 60;
    const OUTER_AGGREGATE: usize = 61;
    const EXTRA_RELATION: usize = 70;

    fn decimal(precision: u8) -> LogicalType {
        LogicalType::Decimal {
            precision,
            scale: 2,
        }
    }

    fn column(table: usize, index: usize, ty: LogicalType) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table, index),
            ty,
        ))
    }

    fn sum(input: Expression) -> Expression {
        let input_type = input.return_type();
        let (function, targets) = get_sum_function()
            .bind(std::slice::from_ref(&input_type))
            .unwrap();
        assert_eq!(targets, [input_type]);
        let return_type = function.return_type.clone();
        Expression::Aggregate(AggregateExpression::new(function, vec![input], return_type))
    }

    fn detail_table(object_id: u64) -> Arc<TableCatalogEntry> {
        let types = vec![LogicalType::BigInt, decimal(15)];
        let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
        let info = CreateTableInfo::new(
            "paro".to_string(),
            "public".to_string(),
            format!("detail_{object_id}"),
            vec![
                ColumnDefinition::new("key".to_string(), types[0].clone()),
                ColumnDefinition::new("value".to_string(), types[1].clone()),
            ],
        );
        Arc::new(
            TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(object_id), 0)
                .unwrap(),
        )
    }

    fn get(table_index: usize, table: Arc<TableCatalogEntry>) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
            table_index,
            vec!["key".to_string(), "value".to_string()],
            vec![LogicalType::BigInt, decimal(15)],
            table,
        )))
    }

    fn preserved() -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            PRESERVED,
            vec![],
            vec!["key".to_string()],
            vec![LogicalType::BigInt],
        )))
    }

    fn q18_shape(
        outer_table: Arc<TableCatalogEntry>,
        inner_table: Arc<TableCatalogEntry>,
    ) -> LogicalPlan {
        let inner_sum = sum(column(INNER_DETAIL, 1, decimal(15)));
        let inner_aggregate = LogicalPlan::synthetic(LogicalOperator::Aggregate(Aggregate::new(
            INNER_GROUP,
            INNER_AGGREGATE,
            42,
            get(INNER_DETAIL, inner_table),
            vec![column(INNER_DETAIL, 0, LogicalType::BigInt)],
            vec![],
            vec![inner_sum],
            vec![],
        )));
        let reduction = LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            REDUCTION_PROJECTION,
            inner_aggregate,
            vec![column(INNER_GROUP, 0, LogicalType::BigInt)],
        )));
        let semi = LogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Semi,
            preserved(),
            reduction,
            vec![JoinCondition::equality(
                column(PRESERVED, 0, LogicalType::BigInt),
                column(REDUCTION_PROJECTION, 0, LogicalType::BigInt),
            )],
        )));
        let detail_join = LogicalPlan::synthetic(LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            semi,
            get(OUTER_DETAIL, outer_table),
            vec![JoinCondition::equality(
                column(PRESERVED, 0, LogicalType::BigInt),
                column(OUTER_DETAIL, 0, LogicalType::BigInt),
            )],
        )));
        LogicalPlan::synthetic(LogicalOperator::Aggregate(Aggregate::new(
            OUTER_GROUP,
            OUTER_AGGREGATE,
            62,
            detail_join,
            vec![column(PRESERVED, 0, LogicalType::BigInt)],
            vec![],
            vec![sum(column(OUTER_DETAIL, 1, decimal(15)))],
            vec![],
        )))
    }

    fn with_join_above_detail_edge(mut plan: LogicalPlan) -> LogicalPlan {
        let LogicalOperator::Aggregate(outer) = &mut plan.operator else {
            panic!("outer aggregate");
        };
        let detail_join = std::mem::replace(
            outer.child.as_mut(),
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
        );
        let extra = LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            EXTRA_RELATION,
            vec![],
            vec!["key".to_string()],
            vec![LogicalType::BigInt],
        )));
        outer.child = Box::new(LogicalPlan::synthetic(LogicalOperator::Join(
            Join::comparison(
                JoinType::Inner,
                detail_join,
                extra,
                vec![JoinCondition::equality(
                    column(PRESERVED, 0, LogicalType::BigInt),
                    column(EXTRA_RELATION, 0, LogicalType::BigInt),
                )],
            ),
        )));
        plan
    }

    #[test]
    fn reuses_filtered_partial_sum_without_catalog_uniqueness() {
        let table = detail_table(70_001);
        let optimized = optimize_plan(q18_shape(table.clone(), table));

        let LogicalOperator::Aggregate(outer) = &optimized.operator else {
            panic!("outer aggregate");
        };
        let Expression::Aggregate(outer_sum) = &outer.aggregates[0] else {
            panic!("outer sum");
        };
        assert_eq!(outer_sum.function.arguments, [decimal(38)]);
        assert_eq!(outer_sum.return_type, decimal(38));
        let [Expression::ColumnRef(partial)] = outer_sum.children.as_slice() else {
            panic!("partial sum reference");
        };
        assert_eq!(partial.binding, ColumnBinding::new(REDUCTION_PROJECTION, 1));

        let LogicalOperator::Join(Join::Comparison(reduction_join)) = &outer.child.operator else {
            panic!("reduction join");
        };
        assert_eq!(reduction_join.join_type, JoinType::Inner);
        let LogicalOperator::Projection(projection) = &reduction_join.right.operator else {
            panic!("reduction projection");
        };
        assert_eq!(projection.expressions.len(), 2);
    }

    #[test]
    fn different_catalog_tables_are_not_subsumed() {
        let optimized = optimize_plan(q18_shape(detail_table(70_002), detail_table(70_003)));

        let LogicalOperator::Aggregate(outer) = &optimized.operator else {
            panic!("outer aggregate");
        };
        assert!(matches!(outer.child.operator, LogicalOperator::Join(_)));
        let Expression::Aggregate(outer_sum) = &outer.aggregates[0] else {
            panic!("outer sum");
        };
        assert_eq!(outer_sum.function.arguments, [decimal(15)]);
    }

    #[test]
    fn finds_detail_edge_inside_clean_inner_join_region() {
        let table = detail_table(70_004);
        let optimized = optimize_plan(with_join_above_detail_edge(q18_shape(table.clone(), table)));

        let LogicalOperator::Aggregate(outer) = &optimized.operator else {
            panic!("outer aggregate");
        };
        let Expression::Aggregate(sum) = &outer.aggregates[0] else {
            panic!("outer sum");
        };
        let [Expression::ColumnRef(partial)] = sum.children.as_slice() else {
            panic!("partial sum reference");
        };
        assert_eq!(partial.binding, ColumnBinding::new(REDUCTION_PROJECTION, 1));
        let LogicalOperator::Join(Join::Comparison(wrapper)) = &outer.child.operator else {
            panic!("outer clean join should remain");
        };
        assert!(wrapper
            .left
            .get_column_bindings()
            .contains(&ColumnBinding::new(REDUCTION_PROJECTION, 1)));
    }
}
