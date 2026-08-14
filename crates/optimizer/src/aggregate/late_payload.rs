// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Delay functionally-dependent aggregate payload until after a bounded TopN.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::projection::{LateRowFetch, LateRowFetchSource};
use paro_planner::operator::{
    Aggregate, ColumnBinding, Get, Join, JoinType, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;

use crate::expression::traversal::visit_expression;

/// Rewrite every eligible subtree while preserving an ordinary narrow plan as
/// the fallback for shapes whose dependency or expression domain is unclear.
pub fn optimize_plan(plan: LogicalPlan, bind_context: &BindContext) -> (LogicalPlan, bool) {
    let mut changed = false;
    let plan = plan.map_children(|child| {
        let (child, child_changed) = optimize_plan(child, bind_context);
        changed |= child_changed;
        child
    });
    match prove_candidate(&plan) {
        Some(proof) => (apply_rewrite(plan, proof, bind_context), true),
        None => match prove_row_preserving_candidate(&plan) {
            Some(proof) => (
                apply_row_preserving_rewrite(plan, proof, bind_context),
                true,
            ),
            None => (plan, changed),
        },
    }
}

#[derive(Debug)]
struct RowPreservingCandidate {
    sources: Vec<RowPreservingSource>,
}

#[derive(Debug)]
struct RowPreservingSource {
    source_table_index: usize,
    table: Arc<TableCatalogEntry>,
    /// Output columns needed to evaluate TopN ordering. These are fetched after
    /// the selective relational child but before TopN builds its heap.
    ordered_catalog_columns: HashMap<usize, usize>,
    /// Output-only columns fetched after TopN has reduced the carrier to its
    /// bounded result cardinality.
    output_catalog_columns: HashMap<usize, usize>,
}

#[derive(Debug)]
struct Candidate {
    dependency: usize,
    source_table_index: usize,
    table: Arc<TableCatalogEntry>,
    dependent_catalog_columns: HashMap<usize, usize>,
}

fn prove_candidate(plan: &LogicalPlan) -> Option<Candidate> {
    let LogicalOperator::TopN(topn) = &plan.operator else {
        return None;
    };
    if topn.total_rows() == 0 || topn.total_rows() > 4096 {
        return None;
    }
    let LogicalOperator::Projection(output) = &topn.child.operator else {
        return None;
    };
    if output.late_row_fetch.is_some()
        || output
            .expressions
            .iter()
            .any(|expression| !expression.evaluation_properties().can_share_evaluation())
    {
        return None;
    }
    if output
        .expressions
        .iter()
        .any(|expression| !matches!(expression, Expression::ColumnRef(column) if column.depth == 0))
    {
        return None;
    }
    let LogicalOperator::Aggregate(aggregate) = &output.child.operator else {
        return None;
    };
    if aggregate.post_reduction.is_some()
        || !aggregate.grouping_functions.is_empty()
        || !is_plain_grouping_domain(aggregate)
    {
        return None;
    }

    if output
        .expressions
        .iter()
        .any(|expression| match expression {
            Expression::ColumnRef(column)
                if column.depth == 0 && column.binding.table_index == aggregate.group_index =>
            {
                column.binding.column_index >= aggregate.groups.len()
            }
            Expression::ColumnRef(column)
                if column.depth == 0 && column.binding.table_index == aggregate.aggregate_index =>
            {
                column.binding.column_index >= aggregate.aggregates.len()
            }
            _ => true,
        })
    {
        return None;
    }
    for order in &topn.orders {
        let Expression::ColumnRef(column) = &order.expression else {
            return None;
        };
        if column.depth != 0
            || column.binding.table_index != output.table_index
            || column.binding.column_index >= output.expressions.len()
        {
            return None;
        }
    }

    aggregate
        .group_dependencies
        .iter()
        .enumerate()
        .filter_map(|(dependency, proof)| {
            if !proof.is_valid_for(aggregate.groups.len()) {
                return None;
            }
            let first = *proof.dependents.first()?;
            let Expression::ColumnRef(first_column) = &aggregate.groups[first] else {
                return None;
            };
            if first_column.depth != 0 {
                return None;
            }
            let source_table_index = first_column.binding.table_index;
            let get = unique_get(aggregate.child.as_ref(), source_table_index)?;
            if source_path_preserves_non_null_rowid(aggregate.child.as_ref(), source_table_index)
                != Some(true)
            {
                return None;
            }
            let table = get.table.as_ref()?.clone();
            if table.get_storage().is_none() {
                return None;
            }
            let mut dependent_catalog_columns = HashMap::new();
            let mut has_wide_payload = false;
            for &group_index in proof.dependents.iter() {
                let Expression::ColumnRef(column) = &aggregate.groups[group_index] else {
                    return None;
                };
                if column.depth != 0 || column.binding.table_index != source_table_index {
                    return None;
                }
                let catalog_column = *get.column_ids.get(column.binding.column_index)?;
                if catalog_column >= table.columns.len() {
                    return None;
                }
                has_wide_payload |= matches!(column.return_type, LogicalType::Varchar);
                dependent_catalog_columns.insert(group_index, catalog_column);
            }
            has_wide_payload.then_some(Candidate {
                dependency,
                source_table_index,
                table,
                dependent_catalog_columns,
            })
        })
        .filter(|candidate| {
            topn.orders.iter().all(|order| {
                let Expression::ColumnRef(column) = &order.expression else {
                    return false;
                };
                let Expression::ColumnRef(output_column) =
                    &output.expressions[column.binding.column_index]
                else {
                    return false;
                };
                output_column.binding.table_index != aggregate.group_index
                    || !candidate
                        .dependent_catalog_columns
                        .contains_key(&output_column.binding.column_index)
            })
        })
        .max_by_key(|candidate| candidate.dependent_catalog_columns.len())
}

fn prove_row_preserving_candidate(plan: &LogicalPlan) -> Option<RowPreservingCandidate> {
    let LogicalOperator::TopN(topn) = &plan.operator else {
        return None;
    };
    if topn.total_rows() == 0 || topn.total_rows() > 4096 {
        return None;
    }
    let LogicalOperator::Projection(output) = &topn.child.operator else {
        return None;
    };
    if output.late_row_fetch.is_some()
        || output
            .expressions
            .iter()
            .any(|expression| !expression.evaluation_properties().can_share_evaluation())
    {
        return None;
    }
    if output
        .expressions
        .iter()
        .any(|expression| !matches!(expression, Expression::ColumnRef(column) if column.depth == 0))
    {
        return None;
    }
    let ordered_outputs = topn
        .orders
        .iter()
        .map(|order| {
            let Expression::ColumnRef(column) = &order.expression else {
                return None;
            };
            (column.depth == 0
                && column.binding.table_index == output.table_index
                && column.binding.column_index < output.expressions.len())
            .then_some(column.binding.column_index)
        })
        .collect::<Option<HashSet<_>>>()?;

    let mut by_source: HashMap<usize, RowPreservingSource> = HashMap::new();
    for (output_index, expression) in output.expressions.iter().enumerate() {
        let Expression::ColumnRef(column) = expression else {
            return None;
        };
        if column.depth != 0 {
            continue;
        }
        let get = unique_get(output.child.as_ref(), column.binding.table_index)?;
        let table = get.table.as_ref()?;
        if table.get_storage().is_none() {
            continue;
        }
        let catalog_column = *get.column_ids.get(column.binding.column_index)?;
        if catalog_column >= table.columns.len()
            || table.columns[catalog_column].logical_type != column.return_type
        {
            return None;
        }
        let source = by_source
            .entry(column.binding.table_index)
            .or_insert_with(|| RowPreservingSource {
                source_table_index: column.binding.table_index,
                table: table.clone(),
                ordered_catalog_columns: HashMap::new(),
                output_catalog_columns: HashMap::new(),
            });
        if ordered_outputs.contains(&output_index) {
            source
                .ordered_catalog_columns
                .insert(output_index, catalog_column);
        } else if matches!(column.return_type, LogicalType::Varchar) {
            source
                .output_catalog_columns
                .insert(output_index, catalog_column);
        }
    }

    let sources = by_source
        .into_values()
        .filter_map(|mut source| {
            // Each fetch frontier opens a table reader. Delay an ordering
            // frontier only when it displaces at least two columns including
            // variable-width payload; a lone fixed key is cheaper to carry.
            let ordered_worth_fetching = source.ordered_catalog_columns.len() >= 2
                && source.ordered_catalog_columns.keys().any(|&output_index| {
                    matches!(
                        output.expressions[output_index].return_type(),
                        LogicalType::Varchar
                    )
                });
            // Output-only varlen payload is consumed after a bounded TopN.
            // Even one such column is wider than the stable rowid that
            // replaces it across the relational path, while the sparse base
            // fetch is capped by `TopN::total_rows` above. Requiring two
            // columns retained a lone string through every join/breaker and
            // made join costing observe a needlessly wide intermediate.
            let output_worth_fetching = !source.output_catalog_columns.is_empty();

            if !ordered_worth_fetching {
                source.ordered_catalog_columns.clear();
            }
            if !output_worth_fetching {
                source.output_catalog_columns.clear();
            }

            (!source.ordered_catalog_columns.is_empty()
                || !source.output_catalog_columns.is_empty())
            .then_some(source)
        })
        .filter(|source| {
            row_preserving_source_path(output.child.as_ref(), source.source_table_index).is_some()
        })
        .collect::<Vec<_>>();
    (!sources.is_empty()).then_some(RowPreservingCandidate { sources })
}

fn row_preserving_source_path(plan: &LogicalPlan, source_table_index: usize) -> Option<()> {
    match &plan.operator {
        LogicalOperator::Get(get) => (get.table_index == source_table_index).then_some(()),
        LogicalOperator::Filter(filter) => {
            row_preserving_source_path(filter.child.as_ref(), source_table_index)
        }
        LogicalOperator::Window(window) => {
            row_preserving_source_path(window.child.as_ref(), source_table_index)
        }
        LogicalOperator::Order(order) => {
            row_preserving_source_path(order.child.as_ref(), source_table_index)
        }
        LogicalOperator::Limit(limit) => {
            row_preserving_source_path(limit.child.as_ref(), source_table_index)
        }
        LogicalOperator::EmptyResult(empty) => {
            row_preserving_source_path(empty.child.as_ref(), source_table_index)
        }
        LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Inner => {
            let left = unique_get(join.left.as_ref(), source_table_index).is_some();
            let right = unique_get(join.right.as_ref(), source_table_index).is_some();
            match (left, right) {
                (true, false) => row_preserving_source_path(join.left.as_ref(), source_table_index),
                (false, true) => {
                    row_preserving_source_path(join.right.as_ref(), source_table_index)
                }
                _ => None,
            }
        }
        LogicalOperator::Join(Join::Cross(join)) => {
            let left = unique_get(join.left.as_ref(), source_table_index).is_some();
            let right = unique_get(join.right.as_ref(), source_table_index).is_some();
            match (left, right) {
                (true, false) => row_preserving_source_path(join.left.as_ref(), source_table_index),
                (false, true) => {
                    row_preserving_source_path(join.right.as_ref(), source_table_index)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn is_plain_grouping_domain(aggregate: &Aggregate) -> bool {
    aggregate.grouping_sets.is_empty()
        || (aggregate.grouping_sets.len() == 1
            && aggregate.grouping_sets[0].expressions.len() == aggregate.groups.len()
            && aggregate.grouping_sets[0]
                .expressions
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                == (0..aggregate.groups.len()).collect())
}

/// Prove that the source rowid cannot be NULL at the aggregate input.
///
/// A base-table rowid is non-NULL, but an outer join may null-extend the side
/// that produced it. Grouping by that nullable rowid would merge every
/// unmatched row, so the late fetch is only valid when every join on the path
/// preserves the source side.
fn source_path_preserves_non_null_rowid(
    plan: &LogicalPlan,
    source_table_index: usize,
) -> Option<bool> {
    if let LogicalOperator::Get(get) = &plan.operator {
        return (get.table_index == source_table_index).then_some(true);
    }

    match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) => source_join_path_safety(
            join.join_type,
            join.left.as_ref(),
            join.right.as_ref(),
            source_table_index,
        ),
        LogicalOperator::Join(Join::Any(join)) => source_join_path_safety(
            join.join_type,
            join.left.as_ref(),
            join.right.as_ref(),
            source_table_index,
        ),
        LogicalOperator::Join(Join::Cross(join)) => {
            let left = source_path_preserves_non_null_rowid(join.left.as_ref(), source_table_index);
            let right =
                source_path_preserves_non_null_rowid(join.right.as_ref(), source_table_index);
            merge_unique_path(left, right)
        }
        _ => {
            let mut matched = None;
            for child in plan.children() {
                matched = merge_unique_path(
                    matched,
                    source_path_preserves_non_null_rowid(child, source_table_index),
                );
                if matched == Some(false) {
                    break;
                }
            }
            matched
        }
    }
}

fn source_join_path_safety(
    join_type: JoinType,
    left: &LogicalPlan,
    right: &LogicalPlan,
    source_table_index: usize,
) -> Option<bool> {
    let left_path = source_path_preserves_non_null_rowid(left, source_table_index).map(|safe| {
        safe && matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Mark
                | JoinType::Single
        )
    });
    let right_path = source_path_preserves_non_null_rowid(right, source_table_index).map(|safe| {
        safe && matches!(
            join_type,
            JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        )
    });
    merge_unique_path(left_path, right_path)
}

fn merge_unique_path(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (None, path) | (path, None) => path,
        // The candidate proof requires one unique source Get. Treat any
        // unexpected duplicate path as unsafe instead of choosing one.
        (Some(_), Some(_)) => Some(false),
    }
}

fn apply_rewrite(
    plan: LogicalPlan,
    candidate: Candidate,
    bind_context: &BindContext,
) -> LogicalPlan {
    let LogicalPlan {
        id: topn_id,
        stats: topn_stats,
        operator: LogicalOperator::TopN(mut topn),
    } = plan
    else {
        unreachable!("late payload proof requires TopN")
    };
    let output_plan = *topn.child;
    let LogicalPlan {
        id: output_id,
        operator: LogicalOperator::Projection(mut output),
        ..
    } = output_plan
    else {
        unreachable!("late payload proof requires Projection")
    };
    let aggregate_plan = *output.child;
    let LogicalPlan {
        id: aggregate_id,
        stats: aggregate_stats,
        operator: LogicalOperator::Aggregate(mut aggregate),
    } = aggregate_plan
    else {
        unreachable!("late payload proof requires Aggregate")
    };

    let required_output_bindings =
        collect_column_bindings(aggregate.groups.iter().chain(&aggregate.aggregates));
    let rowid_binding = append_virtual_rowid(
        aggregate.child.as_mut(),
        candidate.source_table_index,
        candidate.table.columns.len(),
        &required_output_bindings,
    )
    .expect("late payload proof established a unique source Get");
    let rowid_expression =
        Expression::ColumnRef(ColumnRefExpression::new(rowid_binding, LogicalType::BigInt));

    let dependent = aggregate.group_dependencies[candidate.dependency]
        .dependents
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let old_group_count = aggregate.groups.len();
    let mut old_to_new = vec![None; old_group_count];
    let mut groups = Vec::with_capacity(old_group_count - dependent.len() + 1);
    let mut group_stats = Vec::with_capacity(groups.capacity());
    for (old_index, (group, stats)) in aggregate
        .groups
        .drain(..)
        .zip(aggregate.group_stats.drain(..))
        .enumerate()
    {
        if dependent.contains(&old_index) {
            continue;
        }
        old_to_new[old_index] = Some(groups.len());
        groups.push(group);
        group_stats.push(stats);
    }
    let rowid_group_index = groups.len();
    groups.push(rowid_expression);
    group_stats.push(None);
    aggregate.groups = groups;
    aggregate.group_stats = group_stats;
    aggregate.grouping_sets.clear();
    aggregate.group_dependencies.clear();
    aggregate.recompute_returned_types();

    let carrier_table_index = bind_context.generate_table_index();
    let materialized_table_index = bind_context.generate_table_index();
    let mut carrier_expressions = Vec::new();
    let mut carrier_names = Vec::new();
    let mut output_to_carrier = vec![None; output.expressions.len()];
    let mut final_expressions = Vec::with_capacity(output.expressions.len());

    for (output_index, expression) in output.expressions.iter().enumerate() {
        let Expression::ColumnRef(column) = expression else {
            unreachable!("late payload proof accepts direct output columns only")
        };
        if column.binding.table_index == aggregate.group_index {
            let old_group = column.binding.column_index;
            if let Some(&catalog_column) = candidate.dependent_catalog_columns.get(&old_group) {
                final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(materialized_table_index, catalog_column),
                    column.return_type.clone(),
                )));
                continue;
            }
            let new_group = old_to_new[old_group]
                .expect("non-dependent aggregate group has a compacted ordinal");
            let carrier_index = carrier_expressions.len();
            carrier_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(aggregate.group_index, new_group),
                column.return_type.clone(),
            )));
            carrier_names.push(format!("late_group_{new_group}"));
            output_to_carrier[output_index] = Some(carrier_index);
            final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(carrier_table_index, carrier_index),
                column.return_type.clone(),
            )));
            continue;
        }
        if column.binding.table_index == aggregate.aggregate_index {
            let carrier_index = carrier_expressions.len();
            carrier_expressions.push(expression.clone());
            carrier_names.push(format!("late_aggregate_{}", column.binding.column_index));
            output_to_carrier[output_index] = Some(carrier_index);
            final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(carrier_table_index, carrier_index),
                column.return_type.clone(),
            )));
            continue;
        }
        unreachable!("late payload proof rejects non-aggregate output bindings")
    }

    for order in &mut topn.orders {
        let Expression::ColumnRef(column) = &mut order.expression else {
            unreachable!("late payload proof accepts direct TopN output keys only")
        };
        let carrier_index = output_to_carrier[column.binding.column_index]
            .expect("TopN ordering cannot depend on a delayed payload column");
        column.binding = ColumnBinding::new(carrier_table_index, carrier_index);
    }

    let rowid_carrier_index = carrier_expressions.len();
    carrier_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
        ColumnBinding::new(aggregate.group_index, rowid_group_index),
        LogicalType::BigInt,
    )));
    carrier_names.push("__late_rowid".to_string());

    let aggregate_plan = LogicalPlan {
        id: aggregate_id,
        stats: aggregate_stats.clone(),
        operator: LogicalOperator::Aggregate(aggregate),
    };
    let carrier = Projection::new(carrier_table_index, aggregate_plan, carrier_expressions)
        .with_output_names(carrier_names);
    topn.child = Box::new(LogicalPlan::synthetic(LogicalOperator::Projection(carrier)));
    let topn_plan = LogicalPlan {
        id: topn_id,
        stats: topn_stats.clone(),
        operator: LogicalOperator::TopN(topn),
    };
    output.child = Box::new(topn_plan);
    output.expressions = final_expressions;
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    output.late_row_fetch = Some(LateRowFetch {
        carrier_table_index,
        sources: vec![LateRowFetchSource {
            materialized_table_index,
            rowid: Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(carrier_table_index, rowid_carrier_index),
                LogicalType::BigInt,
            )),
            table: candidate.table,
        }],
        coalesce_input: false,
    });
    LogicalPlan {
        id: output_id,
        stats: topn_stats,
        operator: LogicalOperator::Projection(output),
    }
}

fn apply_row_preserving_rewrite(
    plan: LogicalPlan,
    candidate: RowPreservingCandidate,
    bind_context: &BindContext,
) -> LogicalPlan {
    let LogicalPlan {
        id: topn_id,
        stats: topn_stats,
        operator: LogicalOperator::TopN(mut topn),
    } = plan
    else {
        unreachable!("row-preserving late payload proof requires TopN")
    };
    let output_plan = *topn.child;
    let LogicalPlan {
        id: output_id,
        operator: LogicalOperator::Projection(mut output),
        ..
    } = output_plan
    else {
        unreachable!("row-preserving late payload proof requires Projection")
    };

    struct RewriteSource {
        source: RowPreservingSource,
        rowid_binding: ColumnBinding,
        ordered_table_index: Option<usize>,
        output_table_index: Option<usize>,
        narrow_rowid_index: usize,
        topn_rowid_index: Option<usize>,
    }

    let narrow_table_index = bind_context.generate_table_index();
    let topn_table_index = bind_context.generate_table_index();
    let mut sources = candidate
        .sources
        .into_iter()
        .map(|source| {
            let required_output_bindings = output
                .child
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let rowid_binding = append_virtual_rowid(
                output.child.as_mut(),
                source.source_table_index,
                source.table.columns.len(),
                &required_output_bindings,
            )
            .expect("row-preserving late payload proof established a unique source Get");
            let ordered_table_index = (!source.ordered_catalog_columns.is_empty())
                .then(|| bind_context.generate_table_index());
            let output_table_index = (!source.output_catalog_columns.is_empty())
                .then(|| bind_context.generate_table_index());
            RewriteSource {
                source,
                rowid_binding,
                ordered_table_index,
                output_table_index,
                narrow_rowid_index: usize::MAX,
                topn_rowid_index: None,
            }
        })
        .collect::<Vec<_>>();

    // The first carrier crosses joins/window/filter with only columns required
    // before either row-fetch frontier plus one stable rowid per source.
    let mut narrow_expressions = Vec::new();
    let mut narrow_names = Vec::new();
    let mut output_to_narrow = vec![None; output.expressions.len()];
    for (output_index, expression) in output.expressions.iter().enumerate() {
        if sources.iter().any(|source| {
            source
                .source
                .ordered_catalog_columns
                .contains_key(&output_index)
                || source
                    .source
                    .output_catalog_columns
                    .contains_key(&output_index)
        }) {
            continue;
        }
        let narrow_index = narrow_expressions.len();
        narrow_expressions.push(expression.clone());
        narrow_names.push(output.output_names[output_index].clone());
        output_to_narrow[output_index] = Some(narrow_index);
    }
    for source in &mut sources {
        source.narrow_rowid_index = narrow_expressions.len();
        narrow_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
            source.rowid_binding,
            LogicalType::BigInt,
        )));
        narrow_names.push(format!("__late_rowid_{}", source.source.source_table_index));
    }
    let narrow = Projection::new(narrow_table_index, *output.child, narrow_expressions)
        .with_output_names(narrow_names);

    // Fetch ordering payload only after the selective child has completed, but
    // before TopN needs those values. Output-only payload remains delayed.
    let mut topn_expressions = Vec::new();
    let mut topn_names = Vec::new();
    let mut output_to_topn = vec![None; output.expressions.len()];
    for (output_index, expression) in output.expressions.iter().enumerate() {
        if let Some(source) = sources.iter().find(|source| {
            source
                .source
                .ordered_catalog_columns
                .contains_key(&output_index)
        }) {
            let catalog_column = source.source.ordered_catalog_columns[&output_index];
            let materialized_table_index = source
                .ordered_table_index
                .expect("ordered payload source has a private namespace");
            let topn_index = topn_expressions.len();
            topn_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(materialized_table_index, catalog_column),
                expression.return_type(),
            )));
            topn_names.push(output.output_names[output_index].clone());
            output_to_topn[output_index] = Some(topn_index);
            continue;
        }
        if sources.iter().any(|source| {
            source
                .source
                .output_catalog_columns
                .contains_key(&output_index)
        }) {
            continue;
        }
        let topn_index = topn_expressions.len();
        topn_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(
                narrow_table_index,
                output_to_narrow[output_index]
                    .expect("ordinary output has a narrow carrier column"),
            ),
            expression.return_type(),
        )));
        topn_names.push(output.output_names[output_index].clone());
        output_to_topn[output_index] = Some(topn_index);
    }
    for source in &mut sources {
        if source.output_table_index.is_none() {
            continue;
        }
        source.topn_rowid_index = Some(topn_expressions.len());
        topn_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(narrow_table_index, source.narrow_rowid_index),
            LogicalType::BigInt,
        )));
        topn_names.push(format!("__late_rowid_{}", source.source.source_table_index));
    }
    let ordered_fetch_sources = sources
        .iter()
        .filter_map(|source| {
            source
                .ordered_table_index
                .map(|materialized_table_index| LateRowFetchSource {
                    materialized_table_index,
                    rowid: Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(narrow_table_index, source.narrow_rowid_index),
                        LogicalType::BigInt,
                    )),
                    table: source.source.table.clone(),
                })
        })
        .collect::<Vec<_>>();
    let mut topn_carrier = Projection::new(
        topn_table_index,
        LogicalPlan::synthetic(LogicalOperator::Projection(narrow)),
        topn_expressions,
    )
    .with_output_names(topn_names);
    if !ordered_fetch_sources.is_empty() {
        topn_carrier.late_row_fetch = Some(LateRowFetch {
            carrier_table_index: narrow_table_index,
            sources: ordered_fetch_sources,
            coalesce_input: true,
        });
    }

    for order in &mut topn.orders {
        let Expression::ColumnRef(column) = &mut order.expression else {
            unreachable!("row-preserving late payload proof accepts direct TopN keys")
        };
        column.binding = ColumnBinding::new(
            topn_table_index,
            output_to_topn[column.binding.column_index]
                .expect("ordered output is materialized before TopN"),
        );
    }
    topn.child = Box::new(LogicalPlan::synthetic(LogicalOperator::Projection(
        topn_carrier,
    )));
    let topn_plan = LogicalPlan {
        id: topn_id,
        stats: topn_stats.clone(),
        operator: LogicalOperator::TopN(topn),
    };

    let mut final_expressions = Vec::with_capacity(output.expressions.len());
    for (output_index, expression) in output.expressions.iter().enumerate() {
        if let Some(source) = sources.iter().find(|source| {
            source
                .source
                .output_catalog_columns
                .contains_key(&output_index)
        }) {
            let catalog_column = source.source.output_catalog_columns[&output_index];
            final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(
                    source
                        .output_table_index
                        .expect("output payload source has a private namespace"),
                    catalog_column,
                ),
                expression.return_type(),
            )));
        } else {
            final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(
                    topn_table_index,
                    output_to_topn[output_index]
                        .expect("ordinary output is produced by the TopN carrier"),
                ),
                expression.return_type(),
            )));
        }
    }
    let output_fetch_sources = sources
        .into_iter()
        .filter_map(|source| {
            source
                .output_table_index
                .map(|materialized_table_index| LateRowFetchSource {
                    materialized_table_index,
                    rowid: Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(
                            topn_table_index,
                            source
                                .topn_rowid_index
                                .expect("output payload source preserves its rowid through TopN"),
                        ),
                        LogicalType::BigInt,
                    )),
                    table: source.source.table,
                })
        })
        .collect::<Vec<_>>();
    output.child = Box::new(topn_plan);
    output.expressions = final_expressions;
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    output.late_row_fetch = (!output_fetch_sources.is_empty()).then_some(LateRowFetch {
        carrier_table_index: topn_table_index,
        sources: output_fetch_sources,
        coalesce_input: false,
    });
    LogicalPlan {
        id: output_id,
        stats: topn_stats,
        operator: LogicalOperator::Projection(output),
    }
}

fn append_virtual_rowid(
    plan: &mut LogicalPlan,
    table_index: usize,
    virtual_column_id: usize,
    required_output_bindings: &HashSet<ColumnBinding>,
) -> Option<ColumnBinding> {
    if let LogicalOperator::Get(get) = &mut plan.operator {
        if get.table_index != table_index {
            return None;
        }
        if let Some(output_index) = get
            .column_ids
            .iter()
            .position(|column_id| *column_id == virtual_column_id)
        {
            return Some(ColumnBinding::new(table_index, output_index));
        }
        let output_index = get.returned_types.len();
        get.names.push("rowid".to_string());
        get.returned_types.push(LogicalType::BigInt);
        get.column_ids.push(virtual_column_id);
        get.column_types.push(LogicalType::BigInt);
        return Some(ColumnBinding::new(table_index, output_index));
    }

    if let LogicalOperator::Filter(filter) = &mut plan.operator {
        let binding = append_virtual_rowid(
            &mut filter.child,
            table_index,
            virtual_column_id,
            required_output_bindings,
        )?;
        let child_index = filter
            .child
            .get_column_bindings()
            .iter()
            .position(|candidate| *candidate == binding)?;
        filter.projection_map.include(child_index);
        return Some(binding);
    }
    if let LogicalOperator::Join(join) = &mut plan.operator {
        match join {
            Join::Comparison(join) => {
                if unique_get(join.left.as_ref(), table_index).is_some() {
                    let binding = append_virtual_rowid(
                        &mut join.left,
                        table_index,
                        virtual_column_id,
                        required_output_bindings,
                    )?;
                    let child_index = join
                        .left
                        .get_column_bindings()
                        .iter()
                        .position(|candidate| *candidate == binding)?;
                    join.left_projection_map.include(child_index);
                    include_required_join_outputs(
                        join.left.as_ref(),
                        &mut join.left_projection_map,
                        required_output_bindings,
                    );
                    include_required_join_outputs(
                        join.right.as_ref(),
                        &mut join.right_projection_map,
                        required_output_bindings,
                    );
                    return Some(binding);
                }
                if unique_get(join.right.as_ref(), table_index).is_some() {
                    let binding = append_virtual_rowid(
                        &mut join.right,
                        table_index,
                        virtual_column_id,
                        required_output_bindings,
                    )?;
                    let child_index = join
                        .right
                        .get_column_bindings()
                        .iter()
                        .position(|candidate| *candidate == binding)?;
                    join.right_projection_map.include(child_index);
                    include_required_join_outputs(
                        join.left.as_ref(),
                        &mut join.left_projection_map,
                        required_output_bindings,
                    );
                    include_required_join_outputs(
                        join.right.as_ref(),
                        &mut join.right_projection_map,
                        required_output_bindings,
                    );
                    return Some(binding);
                }
                return None;
            }
            Join::Any(join) => {
                if unique_get(join.left.as_ref(), table_index).is_some() {
                    let binding = append_virtual_rowid(
                        &mut join.left,
                        table_index,
                        virtual_column_id,
                        required_output_bindings,
                    )?;
                    let child_index = join
                        .left
                        .get_column_bindings()
                        .iter()
                        .position(|candidate| *candidate == binding)?;
                    join.left_projection_map.include(child_index);
                    include_required_join_outputs(
                        join.left.as_ref(),
                        &mut join.left_projection_map,
                        required_output_bindings,
                    );
                    include_required_join_outputs(
                        join.right.as_ref(),
                        &mut join.right_projection_map,
                        required_output_bindings,
                    );
                    return Some(binding);
                }
                if unique_get(join.right.as_ref(), table_index).is_some() {
                    let binding = append_virtual_rowid(
                        &mut join.right,
                        table_index,
                        virtual_column_id,
                        required_output_bindings,
                    )?;
                    let child_index = join
                        .right
                        .get_column_bindings()
                        .iter()
                        .position(|candidate| *candidate == binding)?;
                    join.right_projection_map.include(child_index);
                    include_required_join_outputs(
                        join.left.as_ref(),
                        &mut join.left_projection_map,
                        required_output_bindings,
                    );
                    include_required_join_outputs(
                        join.right.as_ref(),
                        &mut join.right_projection_map,
                        required_output_bindings,
                    );
                    return Some(binding);
                }
                return None;
            }
            Join::Cross(join) => {
                if unique_get(join.left.as_ref(), table_index).is_some() {
                    return append_virtual_rowid(
                        &mut join.left,
                        table_index,
                        virtual_column_id,
                        required_output_bindings,
                    );
                }
                if unique_get(join.right.as_ref(), table_index).is_some() {
                    return append_virtual_rowid(
                        &mut join.right,
                        table_index,
                        virtual_column_id,
                        required_output_bindings,
                    );
                }
                return None;
            }
        }
    }
    let mut found = None;
    let _ = plan.operator.visit_children_mut(|child| {
        if let Some(binding) = append_virtual_rowid(
            child,
            table_index,
            virtual_column_id,
            required_output_bindings,
        ) {
            found = Some(binding);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    found
}

fn include_required_join_outputs(
    child: &LogicalPlan,
    projection: &mut paro_planner::operator::ProjectionMap,
    required: &HashSet<ColumnBinding>,
) {
    for (index, binding) in child.get_column_bindings().into_iter().enumerate() {
        if required.contains(&binding) {
            projection.include(index);
        }
    }
}

fn collect_column_bindings<'a>(
    expressions: impl IntoIterator<Item = &'a Expression>,
) -> HashSet<ColumnBinding> {
    let mut bindings = HashSet::new();
    for expression in expressions {
        visit_expression(expression, &mut |expression| {
            if let Expression::ColumnRef(column) = expression {
                bindings.insert(column.binding);
            }
        });
    }
    bindings
}

pub(super) fn unique_get(plan: &LogicalPlan, table_index: usize) -> Option<&Get> {
    let mut found = None;
    collect_get(plan, table_index, &mut found);
    found
}

fn collect_get<'a>(plan: &'a LogicalPlan, table_index: usize, found: &mut Option<&'a Get>) {
    if let LogicalOperator::Get(get) = &plan.operator {
        if get.table_index == table_index {
            if found.is_some() {
                *found = None;
                return;
            }
            *found = Some(get);
        }
    }
    for child in plan.children() {
        collect_get(child, table_index, found);
    }
}
