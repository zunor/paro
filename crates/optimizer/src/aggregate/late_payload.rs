// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Delay functionally-dependent aggregate payload until after a bounded TopN.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::ScalarPredicateProjection;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ColumnRefExpression, ConjunctionType, Expression, OperatorType};
use paro_planner::operator::{
    Aggregate, ColumnBinding, Get, GetColumnSource, Join, JoinType, LogicalOperator, Projection,
    RowFetch, RowFetchSource,
};
use paro_planner::plan::LogicalPlan;

use crate::aggregate::semantic_kernels::scalar_kernels_equal;
use crate::cost_model::CostModel;
use crate::expression::traversal::visit_expression;

/// Rewrite every eligible subtree while preserving an ordinary narrow plan as
/// the fallback for shapes whose dependency or expression domain is unclear.
pub fn optimize_plan(
    plan: LogicalPlan,
    bind_context: &BindContext,
    cost_model: &CostModel,
) -> Result<(LogicalPlan, bool)> {
    plan.try_fold_post_order(|plan, child_changes: Vec<bool>| {
        let (plan, node_changed) = rewrite_node(plan, bind_context, cost_model)?;
        Ok((
            plan,
            node_changed || child_changes.into_iter().any(|changed| changed),
        ))
    })
}

fn rewrite_node(
    plan: LogicalPlan,
    bind_context: &BindContext,
    cost_model: &CostModel,
) -> Result<(LogicalPlan, bool)> {
    if let Some(proof) = prove_selective_projection_candidate(&plan, cost_model) {
        return Ok((
            apply_selective_projection_rewrite(plan, proof, bind_context)?,
            true,
        ));
    }
    match prove_candidate(&plan, cost_model) {
        Some(proof) => Ok((apply_rewrite(plan, proof, bind_context)?, true)),
        None => match prove_row_preserving_candidate(&plan, cost_model) {
            Some(proof) => Ok((
                apply_row_preserving_rewrite(plan, proof, bind_context)?,
                true,
            )),
            None => Ok((plan, false)),
        },
    }
}

/// Lower exact leading-substring expressions to derived scan outputs. This is
/// a separate optimizer lifecycle from late row fetching: it neither creates
/// row IDs nor changes where stored payload is materialized.
pub fn optimize_matched_prefix_plan(plan: LogicalPlan) -> Result<(LogicalPlan, bool)> {
    plan.try_fold_post_order(|plan, child_changes: Vec<bool>| {
        let (plan, node_changed) = match prove_matched_prefix_candidate(&plan) {
            Some(proof) => (apply_matched_prefix_rewrite(plan, proof)?, true),
            None => (plan, false),
        };
        Ok((
            plan,
            node_changed || child_changes.into_iter().any(|changed| changed),
        ))
    })
}

#[derive(Debug)]
struct MatchedPrefixCandidate {
    source_table_index: usize,
    source_binding: ColumnBinding,
    byte_width: usize,
    output_indices: Vec<usize>,
}

/// Replace a leading substring result with a scan-produced prefix only when
/// the same bound kernel is an exact ASCII membership predicate below every
/// row-producing operator.  The predicate is the semantic witness: arbitrary
/// UTF-8 rows cannot be byte-truncated, but every emitted matching row begins
/// with the proven ASCII bytes.
fn prove_matched_prefix_candidate(plan: &LogicalPlan) -> Option<MatchedPrefixCandidate> {
    let LogicalOperator::Projection(output) = &plan.operator else {
        return None;
    };
    let mut candidate: Option<MatchedPrefixCandidate> = None;
    for (output_index, expression) in output.expressions.iter().enumerate() {
        let Expression::Function(function) = expression else {
            continue;
        };
        let Some(ScalarPredicateProjection::Utf8Substring {
            source_argument,
            start: 1,
            length: Some(length),
        }) = function.function.predicate_projection.as_ref()
        else {
            continue;
        };
        let byte_width = usize::try_from(*length).ok().filter(|width| *width > 0)?;
        let Expression::ColumnRef(source) = function.children.get(*source_argument)? else {
            continue;
        };
        if source.depth != 0 || source.return_type != LogicalType::Varchar {
            continue;
        }
        let get = unique_get(output.child.as_ref(), source.binding.table_index)?;
        let column_id = get.stored_column(source.binding.column_index)?;
        if column_id >= get.table.as_ref()?.columns.len()
            || !prove_matched_prefix_use_path(
                output.child.as_ref(),
                source.binding,
                &function.function,
                byte_width,
            )?
        {
            continue;
        }
        match &mut candidate {
            Some(candidate)
                if candidate.source_binding == source.binding
                    && candidate.byte_width == byte_width =>
            {
                candidate.output_indices.push(output_index);
            }
            None => {
                candidate = Some(MatchedPrefixCandidate {
                    source_table_index: source.binding.table_index,
                    source_binding: source.binding,
                    byte_width,
                    output_indices: vec![output_index],
                });
            }
            Some(_) => return None,
        }
    }
    candidate
}

fn prove_matched_prefix_use_path(
    plan: &LogicalPlan,
    source_binding: ColumnBinding,
    kernel: &paro_function::scalar::BoundScalarFunction,
    byte_width: usize,
) -> Option<bool> {
    match &plan.operator {
        LogicalOperator::Get(get) => {
            (get.table_index == source_binding.table_index).then_some(false)
        }
        LogicalOperator::Filter(filter) => {
            if matches!(&filter.child.operator, LogicalOperator::Get(get)
                if get.table_index == source_binding.table_index)
            {
                return Some(filter.expressions.iter().any(|expression| {
                    prove_prefix_filter_expression(expression, source_binding, kernel, byte_width)
                }));
            }
            prove_matched_prefix_use_path(filter.child.as_ref(), source_binding, kernel, byte_width)
        }
        LogicalOperator::Window(window) => {
            prove_matched_prefix_use_path(window.child.as_ref(), source_binding, kernel, byte_width)
        }
        LogicalOperator::Order(order) => {
            prove_matched_prefix_use_path(order.child.as_ref(), source_binding, kernel, byte_width)
        }
        LogicalOperator::Limit(limit) => {
            prove_matched_prefix_use_path(limit.child.as_ref(), source_binding, kernel, byte_width)
        }
        LogicalOperator::TopN(topn) => {
            prove_matched_prefix_use_path(topn.child.as_ref(), source_binding, kernel, byte_width)
        }
        LogicalOperator::EmptyResult(empty) => {
            prove_matched_prefix_use_path(empty.child.as_ref(), source_binding, kernel, byte_width)
        }
        LogicalOperator::Join(Join::Comparison(join)) => prove_prefix_join_child(
            join.left.as_ref(),
            join.right.as_ref(),
            source_binding,
            kernel,
            byte_width,
        ),
        LogicalOperator::Join(Join::Any(join)) => prove_prefix_join_child(
            join.left.as_ref(),
            join.right.as_ref(),
            source_binding,
            kernel,
            byte_width,
        ),
        LogicalOperator::Join(Join::Cross(join)) => prove_prefix_join_child(
            join.left.as_ref(),
            join.right.as_ref(),
            source_binding,
            kernel,
            byte_width,
        ),
        _ => None,
    }
}

fn prove_prefix_join_child(
    left: &LogicalPlan,
    right: &LogicalPlan,
    source_binding: ColumnBinding,
    kernel: &paro_function::scalar::BoundScalarFunction,
    byte_width: usize,
) -> Option<bool> {
    match (
        count_source_gets(left, source_binding.table_index),
        count_source_gets(right, source_binding.table_index),
    ) {
        (1, 0) => prove_matched_prefix_use_path(left, source_binding, kernel, byte_width),
        (0, 1) => prove_matched_prefix_use_path(right, source_binding, kernel, byte_width),
        _ => None,
    }
}

fn prove_prefix_filter_expression(
    expression: &Expression,
    source_binding: ColumnBinding,
    kernel: &paro_function::scalar::BoundScalarFunction,
    byte_width: usize,
) -> bool {
    if let Expression::Conjunction(conjunction) = expression {
        if conjunction.conjunction_type != ConjunctionType::And {
            return false;
        }
        return conjunction.children.iter().any(|child| {
            prove_prefix_filter_expression(child, source_binding, kernel, byte_width)
        });
    }
    let Expression::Operator(operator) = expression else {
        return false;
    };
    if operator.operator_type != OperatorType::In || operator.children.len() < 2 {
        return false;
    }
    let Expression::Function(projected) = &operator.children[0] else {
        return false;
    };
    let Some(ScalarPredicateProjection::Utf8Substring {
        source_argument,
        start: 1,
        length: Some(length),
    }) = projected.function.predicate_projection.as_ref()
    else {
        return false;
    };
    if usize::try_from(*length).ok() != Some(byte_width)
        || !scalar_kernels_equal(&projected.function, kernel)
        || !matches!(
            projected.children.get(*source_argument),
            Some(Expression::ColumnRef(column))
                if column.depth == 0 && column.binding == source_binding
        )
        || !operator.children[1..].iter().all(|child| {
            matches!(child, Expression::Constant(constant)
                if matches!(&constant.value, Value::Varchar(value)
                    if value.is_ascii() && value.len() == byte_width))
        })
    {
        return false;
    }
    true
}

fn apply_matched_prefix_rewrite(
    mut plan: LogicalPlan,
    candidate: MatchedPrefixCandidate,
) -> Result<LogicalPlan> {
    let LogicalOperator::Projection(output) = &mut plan.operator else {
        return Err(rewrite_invariant("matched-prefix root is not Projection"));
    };
    let derived_binding = append_get_matched_prefix_projection(
        output.child.as_mut(),
        candidate.source_table_index,
        candidate.source_binding,
        candidate.byte_width,
    )?;
    for output_index in candidate.output_indices {
        let expression = output
            .expressions
            .get_mut(output_index)
            .ok_or_else(|| rewrite_invariant("matched-prefix output ordinal is stale"))?;
        *expression = Expression::ColumnRef(ColumnRefExpression::new(
            derived_binding,
            LogicalType::Varchar,
        ));
    }
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    Ok(plan)
}

fn append_get_matched_prefix_projection(
    plan: &mut LogicalPlan,
    table_index: usize,
    source_binding: ColumnBinding,
    byte_width: usize,
) -> Result<ColumnBinding> {
    if let LogicalOperator::Get(get) = &mut plan.operator {
        if get.table_index != table_index {
            return Err(rewrite_invariant(
                "matched-prefix source Get no longer matches its proof",
            ));
        }
        let column_id = get
            .stored_column(source_binding.column_index)
            .ok_or_else(|| rewrite_invariant("matched-prefix source is no longer stored"))?;
        let column_type = get
            .column_types
            .get(source_binding.column_index)
            .cloned()
            .ok_or_else(|| rewrite_invariant("matched-prefix source type is missing"))?;
        return Ok(get.append_output(
            format!("__ascii_prefix_{column_id}_{byte_width}"),
            LogicalType::Varchar,
            column_type,
            GetColumnSource::MatchedUtf8Prefix {
                source_column: column_id,
                byte_width,
            },
        ));
    }
    append_prefix_through_operator(plan, table_index, source_binding, byte_width)
}

fn append_prefix_through_operator(
    plan: &mut LogicalPlan,
    table_index: usize,
    source_binding: ColumnBinding,
    byte_width: usize,
) -> Result<ColumnBinding> {
    match &mut plan.operator {
        LogicalOperator::Filter(filter) => {
            let binding = append_get_matched_prefix_projection(
                filter.child.as_mut(),
                table_index,
                source_binding,
                byte_width,
            )?;
            let child_index = filter
                .child
                .get_column_bindings()
                .iter()
                .position(|candidate| *candidate == binding)
                .ok_or_else(|| rewrite_invariant("Filter hid an appended prefix binding"))?;
            filter.projection_map.include(child_index);
            Ok(binding)
        }
        LogicalOperator::Window(window) => append_get_matched_prefix_projection(
            window.child.as_mut(),
            table_index,
            source_binding,
            byte_width,
        ),
        LogicalOperator::Order(order) => {
            let binding = append_get_matched_prefix_projection(
                order.child.as_mut(),
                table_index,
                source_binding,
                byte_width,
            )?;
            let child_index = order
                .child
                .get_column_bindings()
                .iter()
                .position(|candidate| *candidate == binding)
                .ok_or_else(|| rewrite_invariant("Order hid an appended prefix binding"))?;
            order.projection_map.include(child_index);
            Ok(binding)
        }
        LogicalOperator::Limit(limit) => append_get_matched_prefix_projection(
            limit.child.as_mut(),
            table_index,
            source_binding,
            byte_width,
        ),
        LogicalOperator::TopN(topn) => append_get_matched_prefix_projection(
            topn.child.as_mut(),
            table_index,
            source_binding,
            byte_width,
        ),
        LogicalOperator::EmptyResult(empty) => append_get_matched_prefix_projection(
            empty.child.as_mut(),
            table_index,
            source_binding,
            byte_width,
        ),
        LogicalOperator::Join(Join::Comparison(join)) => append_prefix_join_child(
            join.left.as_mut(),
            join.right.as_mut(),
            Some(&mut join.left_projection_map),
            Some(&mut join.right_projection_map),
            table_index,
            source_binding,
            byte_width,
        ),
        LogicalOperator::Join(Join::Any(join)) => append_prefix_join_child(
            join.left.as_mut(),
            join.right.as_mut(),
            Some(&mut join.left_projection_map),
            Some(&mut join.right_projection_map),
            table_index,
            source_binding,
            byte_width,
        ),
        LogicalOperator::Join(Join::Cross(join)) => append_prefix_join_child(
            join.left.as_mut(),
            join.right.as_mut(),
            None,
            None,
            table_index,
            source_binding,
            byte_width,
        ),
        _ => Err(rewrite_invariant(
            "matched-prefix source path no longer matches its proof",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn append_prefix_join_child(
    left: &mut LogicalPlan,
    right: &mut LogicalPlan,
    left_projection: Option<&mut paro_planner::operator::ProjectionMap>,
    right_projection: Option<&mut paro_planner::operator::ProjectionMap>,
    table_index: usize,
    source_binding: ColumnBinding,
    byte_width: usize,
) -> Result<ColumnBinding> {
    let (child, projection) = match (
        count_source_gets(left, table_index),
        count_source_gets(right, table_index),
    ) {
        (1, 0) => (left, left_projection),
        (0, 1) => (right, right_projection),
        _ => {
            return Err(rewrite_invariant(
                "matched-prefix join side no longer matches its proof",
            ));
        }
    };
    let binding =
        append_get_matched_prefix_projection(child, table_index, source_binding, byte_width)?;
    if let Some(projection) = projection {
        let child_index = child
            .get_column_bindings()
            .iter()
            .position(|candidate| *candidate == binding)
            .ok_or_else(|| rewrite_invariant("Join hid an appended prefix binding"))?;
        projection.include(child_index);
    }
    Ok(binding)
}

#[derive(Debug)]
struct SelectiveProjectionCandidate {
    sources: Vec<SelectiveProjectionSource>,
}

#[derive(Debug)]
struct SelectiveProjectionSource {
    source_table_index: usize,
    table: Arc<TableCatalogEntry>,
    delayed_bindings: HashMap<ColumnBinding, usize>,
    rowid_path: RowIdPath,
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
    benefit: f64,
    rowid_path: RowIdPath,
}

#[derive(Debug)]
struct Candidate {
    dependency: usize,
    source_table_index: usize,
    table: Arc<TableCatalogEntry>,
    dependent_catalog_columns: HashMap<usize, usize>,
    benefit: f64,
    rowid_path: RowIdPath,
}

/// Prove that output expressions can fetch base-table payload after a
/// selective row-preserving subtree.
///
/// This is the non-TopN counterpart of the existing late-payload rewrite. It
/// is especially useful across blocking operators: a stable rowid and narrow
/// predicate inputs cross the blocker, while wide output-only columns are
/// gathered for the surviving rows immediately below their final projection.
fn prove_selective_projection_candidate(
    plan: &LogicalPlan,
    cost_model: &CostModel,
) -> Option<SelectiveProjectionCandidate> {
    let LogicalOperator::Projection(output) = &plan.operator else {
        return None;
    };
    if matches!(output.child.operator, LogicalOperator::RowFetch(_))
        || output
            .expressions
            .iter()
            .any(|expression| !expression.evaluation_properties().can_share_evaluation())
    {
        return None;
    }
    // A late fetch is paid after the whole rowid path, where join fanout and
    // correlated selectivity errors have already accumulated. Planning it
    // from the expected cardinality makes the decision optimistically
    // irreversible: an underestimated join creates one random lookup per
    // actual row. Charge the interval's upper bound for that lookup frontier,
    // while the eager side below remains based on observed source work.
    let fetched_rows = output.child.stats.estimated_cardinality?.max;
    let mut by_source = HashMap::<usize, SelectiveProjectionSource>::new();
    for expression in &output.expressions {
        let mut valid = true;
        visit_expression(expression, &mut |candidate| {
            let Expression::ColumnRef(column) = candidate else {
                return;
            };
            if column.depth != 0 {
                valid = false;
                return;
            }
            let Some(get) = unique_get(output.child.as_ref(), column.binding.table_index) else {
                return;
            };
            let Some(table) = get
                .table
                .as_ref()
                .filter(|table| table.get_storage().is_some())
            else {
                valid = false;
                return;
            };
            let Some(catalog_column) = get.stored_column(column.binding.column_index) else {
                valid = false;
                return;
            };
            if table
                .columns
                .get(catalog_column)
                .is_none_or(|definition| definition.logical_type != column.return_type)
            {
                valid = false;
                return;
            }
            by_source
                .entry(column.binding.table_index)
                .or_insert_with(|| SelectiveProjectionSource {
                    source_table_index: column.binding.table_index,
                    table: Arc::clone(table),
                    delayed_bindings: HashMap::new(),
                    rowid_path: RowIdPath::Get,
                })
                .delayed_bindings
                .insert(column.binding, catalog_column);
        });
        if !valid {
            return None;
        }
    }

    let sources = by_source
        .into_values()
        .filter_map(|mut source| {
            let rowid_path = prove_rowid_path(
                output.child.as_ref(),
                source.source_table_index,
                RowIdPathPolicy::RowPreserving,
            )?;
            // The current sparse fetch frontier has a direct-scan locality
            // model. Once a rowid crosses a join, physical lookups are paid
            // after fanout, while join cardinality intervals do not yet carry
            // a uniqueness/locality bound strong enough to price that work.
            // Keep semantic rowid propagation broader than lowering
            // eligibility, but require a cost proof before enabling post-join
            // fetch rather than treating an optimistic estimate as exact.
            if rowid_path.crosses_join() {
                return None;
            }
            let carrier_rows =
                source_estimated_rows(output.child.as_ref(), source.source_table_index)?;
            if fetched_rows >= carrier_rows {
                return None;
            }
            cost_model.late_row_fetch_benefit(
                carrier_rows,
                fetched_rows,
                source.delayed_bindings.values().filter_map(|&column| {
                    source
                        .table
                        .columns
                        .get(column)
                        .map(|definition| definition.logical_type.clone())
                }),
                rowid_path.stages(),
            )?;
            source.rowid_path = rowid_path;
            Some(source)
        })
        .collect::<Vec<_>>();
    (!sources.is_empty()).then_some(SelectiveProjectionCandidate { sources })
}

fn source_estimated_rows(plan: &LogicalPlan, table_index: usize) -> Option<u64> {
    if matches!(&plan.operator, LogicalOperator::Get(get) if get.table_index == table_index) {
        return plan
            .stats
            .estimated_cardinality
            .map(|estimate| estimate.expected);
    }
    plan.operator
        .children()
        .into_iter()
        .find_map(|child| source_estimated_rows(child, table_index))
}

#[derive(Debug)]
enum RowIdPath {
    Get,
    Filter(Box<RowIdPath>),
    Window(Box<RowIdPath>),
    Order(Box<RowIdPath>),
    Limit(Box<RowIdPath>),
    EmptyResult(Box<RowIdPath>),
    Join {
        kind: RowIdJoinKind,
        side: RowIdJoinSide,
        child: Box<RowIdPath>,
    },
}

#[derive(Debug, Clone, Copy)]
enum RowIdJoinKind {
    Comparison,
    Any,
    Cross,
}

#[derive(Debug, Clone, Copy)]
enum RowIdJoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy)]
enum RowIdPathPolicy {
    /// The row survives each operator one-for-one; used when payload is
    /// removed from an ordinary detail stream.
    RowPreserving,
    /// The source may be filtered or duplicated, but its rowid cannot be
    /// null-extended; used as a functional-dependency group key.
    NonNull,
}

impl RowIdPath {
    fn stages(&self) -> usize {
        match self {
            Self::Get => 1,
            Self::Filter(child)
            | Self::Window(child)
            | Self::Order(child)
            | Self::Limit(child)
            | Self::EmptyResult(child)
            | Self::Join { child, .. } => child.stages() + 1,
        }
    }

    fn crosses_join(&self) -> bool {
        match self {
            Self::Get => false,
            Self::Filter(child)
            | Self::Window(child)
            | Self::Order(child)
            | Self::Limit(child)
            | Self::EmptyResult(child) => child.crosses_join(),
            Self::Join { .. } => true,
        }
    }
}

fn prove_candidate(plan: &LogicalPlan, cost_model: &CostModel) -> Option<Candidate> {
    let LogicalOperator::TopN(topn) = &plan.operator else {
        return None;
    };
    if topn.total_rows() == 0 {
        return None;
    }
    let LogicalOperator::Projection(output) = &topn.child.operator else {
        return None;
    };
    if matches!(output.child.operator, LogicalOperator::RowFetch(_))
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
            let rowid_path = prove_rowid_path(
                aggregate.child.as_ref(),
                source_table_index,
                RowIdPathPolicy::NonNull,
            )?;
            let get = unique_get(aggregate.child.as_ref(), source_table_index)?;
            let table = get.table.as_ref()?.clone();
            if table.get_storage().is_none() {
                return None;
            }
            let mut dependent_catalog_columns = HashMap::new();
            let mut payload_types = Vec::with_capacity(proof.dependents.len());
            for &group_index in proof.dependents.iter() {
                let Expression::ColumnRef(column) = &aggregate.groups[group_index] else {
                    return None;
                };
                if column.depth != 0 || column.binding.table_index != source_table_index {
                    return None;
                }
                let catalog_column = get.stored_column(column.binding.column_index)?;
                if catalog_column >= table.columns.len() {
                    return None;
                }
                payload_types.push(column.return_type.clone());
                dependent_catalog_columns.insert(group_index, catalog_column);
            }
            let carrier_rows = aggregate.child.stats.estimated_cardinality?.expected;
            let topn_rows = u64::try_from(topn.total_rows()).ok()?;
            let fetched_rows = topn_rows.min(
                output
                    .child
                    .stats
                    .estimated_cardinality
                    .map_or(carrier_rows, |estimate| estimate.expected),
            );
            let benefit = cost_model.late_row_fetch_benefit(
                carrier_rows,
                fetched_rows,
                payload_types,
                rowid_path.stages(),
            )?;
            Some(Candidate {
                dependency,
                source_table_index,
                table,
                dependent_catalog_columns,
                benefit,
                rowid_path,
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
        .max_by(|left, right| left.benefit.total_cmp(&right.benefit))
}

fn prove_row_preserving_candidate(
    plan: &LogicalPlan,
    cost_model: &CostModel,
) -> Option<RowPreservingCandidate> {
    let LogicalOperator::TopN(topn) = &plan.operator else {
        return None;
    };
    if topn.total_rows() == 0 {
        return None;
    }
    let LogicalOperator::Projection(output) = &topn.child.operator else {
        return None;
    };
    if matches!(output.child.operator, LogicalOperator::RowFetch(_))
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
        let catalog_column = get.stored_column(column.binding.column_index)?;
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
                benefit: 0.0,
                rowid_path: RowIdPath::Get,
            });
        if ordered_outputs.contains(&output_index) {
            source
                .ordered_catalog_columns
                .insert(output_index, catalog_column);
        } else {
            source
                .output_catalog_columns
                .insert(output_index, catalog_column);
        }
    }

    let carrier_rows = output.child.stats.estimated_cardinality?.expected;
    let fetched_rows = u64::try_from(topn.total_rows()).ok()?.min(carrier_rows);
    let sources = by_source
        .into_values()
        .filter_map(|mut source| {
            let rowid_path = prove_rowid_path(
                output.child.as_ref(),
                source.source_table_index,
                RowIdPathPolicy::RowPreserving,
            )?;
            let carrier_stages = rowid_path.stages();
            let ordered_benefit = cost_model.late_row_fetch_benefit(
                carrier_rows,
                carrier_rows,
                source
                    .ordered_catalog_columns
                    .keys()
                    .map(|&index| output.expressions[index].return_type()),
                carrier_stages,
            );
            let output_benefit = cost_model.late_row_fetch_benefit(
                carrier_rows,
                fetched_rows,
                source
                    .output_catalog_columns
                    .keys()
                    .map(|&index| output.expressions[index].return_type()),
                carrier_stages,
            );

            if ordered_benefit.is_none() {
                source.ordered_catalog_columns.clear();
            }
            if output_benefit.is_none() {
                source.output_catalog_columns.clear();
            }
            source.benefit =
                ordered_benefit.unwrap_or_default() + output_benefit.unwrap_or_default();
            source.rowid_path = rowid_path;

            (!source.ordered_catalog_columns.is_empty()
                || !source.output_catalog_columns.is_empty())
            .then_some(source)
        })
        .collect::<Vec<_>>();
    (!sources.is_empty()).then_some(RowPreservingCandidate { sources })
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

/// Return a structural witness from this operator to one unique base-table
/// scan. The proof and mutation sides intentionally share this exact operator
/// whitelist; an unrecognized node is a declined optimization, never a blind
/// recursive rewrite.
fn prove_rowid_path(
    plan: &LogicalPlan,
    source_table_index: usize,
    policy: RowIdPathPolicy,
) -> Option<RowIdPath> {
    if count_source_gets(plan, source_table_index) != 1 {
        return None;
    }
    prove_unique_rowid_path(plan, source_table_index, policy)
}

#[cfg(test)]
pub(super) fn proves_row_preserving_path(plan: &LogicalPlan, source_table_index: usize) -> bool {
    prove_rowid_path(plan, source_table_index, RowIdPathPolicy::RowPreserving).is_some()
}

fn prove_unique_rowid_path(
    plan: &LogicalPlan,
    source_table_index: usize,
    policy: RowIdPathPolicy,
) -> Option<RowIdPath> {
    match &plan.operator {
        LogicalOperator::Get(get) => {
            (get.table_index == source_table_index).then_some(RowIdPath::Get)
        }
        LogicalOperator::Filter(filter) => {
            prove_unique_rowid_path(filter.child.as_ref(), source_table_index, policy)
                .map(|path| RowIdPath::Filter(Box::new(path)))
        }
        LogicalOperator::Window(window) => {
            prove_unique_rowid_path(window.child.as_ref(), source_table_index, policy)
                .map(|path| RowIdPath::Window(Box::new(path)))
        }
        LogicalOperator::Order(order) => {
            prove_unique_rowid_path(order.child.as_ref(), source_table_index, policy)
                .map(|path| RowIdPath::Order(Box::new(path)))
        }
        LogicalOperator::Limit(limit) => {
            prove_unique_rowid_path(limit.child.as_ref(), source_table_index, policy)
                .map(|path| RowIdPath::Limit(Box::new(path)))
        }
        LogicalOperator::EmptyResult(empty) => {
            prove_unique_rowid_path(empty.child.as_ref(), source_table_index, policy)
                .map(|path| RowIdPath::EmptyResult(Box::new(path)))
        }
        LogicalOperator::Join(Join::Comparison(join)) => prove_join_rowid_path(
            RowIdJoinKind::Comparison,
            join.join_type,
            join.left.as_ref(),
            join.right.as_ref(),
            source_table_index,
            policy,
        ),
        LogicalOperator::Join(Join::Any(join)) => prove_join_rowid_path(
            RowIdJoinKind::Any,
            join.join_type,
            join.left.as_ref(),
            join.right.as_ref(),
            source_table_index,
            policy,
        ),
        LogicalOperator::Join(Join::Cross(join)) => prove_join_rowid_path(
            RowIdJoinKind::Cross,
            JoinType::Inner,
            join.left.as_ref(),
            join.right.as_ref(),
            source_table_index,
            policy,
        ),
        _ => None,
    }
}

fn prove_join_rowid_path(
    kind: RowIdJoinKind,
    join_type: JoinType,
    left: &LogicalPlan,
    right: &LogicalPlan,
    source_table_index: usize,
    policy: RowIdPathPolicy,
) -> Option<RowIdPath> {
    let left_count = count_source_gets(left, source_table_index);
    let right_count = count_source_gets(right, source_table_index);
    let (side, child_plan) = match (left_count, right_count) {
        (1, 0) => (RowIdJoinSide::Left, left),
        (0, 1) => (RowIdJoinSide::Right, right),
        _ => return None,
    };
    let allowed = match (policy, kind, side) {
        (RowIdPathPolicy::RowPreserving, RowIdJoinKind::Comparison, RowIdJoinSide::Left) => {
            matches!(
                join_type,
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::Semi
                    | JoinType::Anti
                    | JoinType::Mark
                    | JoinType::Single
            )
        }
        (RowIdPathPolicy::RowPreserving, RowIdJoinKind::Comparison, RowIdJoinSide::Right) => {
            matches!(
                join_type,
                JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
            )
        }
        (RowIdPathPolicy::RowPreserving, RowIdJoinKind::Cross, _) => true,
        (RowIdPathPolicy::RowPreserving, RowIdJoinKind::Any, _) => false,
        (RowIdPathPolicy::NonNull, RowIdJoinKind::Cross, _) => true,
        (RowIdPathPolicy::NonNull, _, RowIdJoinSide::Left) => matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Mark
                | JoinType::Single
        ),
        (RowIdPathPolicy::NonNull, _, RowIdJoinSide::Right) => matches!(
            join_type,
            JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        ),
    };
    if !allowed {
        return None;
    }
    let child = prove_unique_rowid_path(child_plan, source_table_index, policy)?;
    Some(RowIdPath::Join {
        kind,
        side,
        child: Box::new(child),
    })
}

fn count_source_gets(plan: &LogicalPlan, source_table_index: usize) -> usize {
    let here = usize::from(matches!(
        &plan.operator,
        LogicalOperator::Get(get) if get.table_index == source_table_index
    ));
    plan.children().into_iter().fold(here, |count, child| {
        count
            .saturating_add(count_source_gets(child, source_table_index))
            .min(2)
    })
}

fn apply_rewrite(
    plan: LogicalPlan,
    candidate: Candidate,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalPlan {
        id: topn_id,
        stats: topn_stats,
        operator: LogicalOperator::TopN(mut topn),
    } = plan
    else {
        return Err(rewrite_invariant("aggregate candidate root is not TopN"));
    };
    let output_plan = *topn.child;
    let LogicalPlan {
        id: output_id,
        operator: LogicalOperator::Projection(mut output),
        ..
    } = output_plan
    else {
        return Err(rewrite_invariant(
            "aggregate candidate child is not Projection",
        ));
    };
    let aggregate_plan = *output.child;
    let LogicalPlan {
        id: aggregate_id,
        stats: aggregate_stats,
        operator: LogicalOperator::Aggregate(mut aggregate),
    } = aggregate_plan
    else {
        return Err(rewrite_invariant(
            "aggregate candidate payload child is not Aggregate",
        ));
    };

    let required_output_bindings =
        collect_column_bindings(aggregate.groups.iter().chain(&aggregate.aggregates));
    let rowid_binding = append_virtual_rowid(
        aggregate.child.as_mut(),
        &candidate.rowid_path,
        candidate.source_table_index,
        &required_output_bindings,
    )
    .ok_or_else(|| rewrite_invariant("rowid witness no longer matches aggregate source"))?;
    let rowid_expression =
        Expression::ColumnRef(ColumnRefExpression::new(rowid_binding, LogicalType::BigInt));

    let dependent = aggregate
        .group_dependencies
        .get(candidate.dependency)
        .ok_or_else(|| rewrite_invariant("group dependency ordinal is stale"))?
        .dependents
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let old_group_count = aggregate.groups.len();
    let compacted_group_count = old_group_count
        .checked_sub(dependent.len())
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| rewrite_invariant("dependent group set exceeds aggregate groups"))?;
    let mut old_to_new = vec![None; old_group_count];
    let mut groups = Vec::with_capacity(compacted_group_count);
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
            return Err(rewrite_invariant(
                "aggregate output is no longer a direct column",
            ));
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
            let new_group = old_to_new
                .get(old_group)
                .copied()
                .flatten()
                .ok_or_else(|| rewrite_invariant("aggregate group remap is incomplete"))?;
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
        return Err(rewrite_invariant(
            "aggregate output binding escaped the proven domains",
        ));
    }

    for order in &mut topn.orders {
        let Expression::ColumnRef(column) = &mut order.expression else {
            return Err(rewrite_invariant("TopN key is no longer a direct column"));
        };
        let carrier_index = output_to_carrier
            .get(column.binding.column_index)
            .copied()
            .flatten()
            .ok_or_else(|| rewrite_invariant("TopN key depends on delayed payload"))?;
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
    output.child = Box::new(LogicalPlan::synthetic(LogicalOperator::RowFetch(
        RowFetch::new(
            carrier_table_index,
            vec![RowFetchSource {
                materialized_table_index,
                rowid: Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(carrier_table_index, rowid_carrier_index),
                    LogicalType::BigInt,
                )),
                table: candidate.table,
                needed_columns: candidate
                    .dependent_catalog_columns
                    .values()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            }],
            topn_plan,
        ),
    )));
    output.expressions = final_expressions;
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    Ok(LogicalPlan {
        id: output_id,
        stats: topn_stats,
        operator: LogicalOperator::Projection(output),
    })
}

fn apply_row_preserving_rewrite(
    plan: LogicalPlan,
    candidate: RowPreservingCandidate,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalPlan {
        id: topn_id,
        stats: topn_stats,
        operator: LogicalOperator::TopN(mut topn),
    } = plan
    else {
        return Err(rewrite_invariant(
            "row-preserving candidate root is not TopN",
        ));
    };
    let output_plan = *topn.child;
    let LogicalPlan {
        id: output_id,
        operator: LogicalOperator::Projection(mut output),
        ..
    } = output_plan
    else {
        return Err(rewrite_invariant(
            "row-preserving candidate child is not Projection",
        ));
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
        .map(|source| -> Result<RewriteSource> {
            let required_output_bindings = output
                .child
                .get_column_bindings()
                .into_iter()
                .collect::<HashSet<_>>();
            let rowid_binding = append_virtual_rowid(
                output.child.as_mut(),
                &source.rowid_path,
                source.source_table_index,
                &required_output_bindings,
            )
            .ok_or_else(|| rewrite_invariant("rowid witness no longer matches detail source"))?;
            let ordered_table_index = (!source.ordered_catalog_columns.is_empty())
                .then(|| bind_context.generate_table_index());
            let output_table_index = (!source.output_catalog_columns.is_empty())
                .then(|| bind_context.generate_table_index());
            Ok(RewriteSource {
                source,
                rowid_binding,
                ordered_table_index,
                output_table_index,
                narrow_rowid_index: usize::MAX,
                topn_rowid_index: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;

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
            let catalog_column = *source
                .source
                .ordered_catalog_columns
                .get(&output_index)
                .ok_or_else(|| rewrite_invariant("ordered payload mapping disappeared"))?;
            let materialized_table_index = source
                .ordered_table_index
                .ok_or_else(|| rewrite_invariant("ordered payload namespace is missing"))?;
            let topn_index = topn_expressions.len();
            topn_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(materialized_table_index, catalog_column),
                expression.return_type(),
            )));
            topn_names.push(
                output
                    .output_names
                    .get(output_index)
                    .cloned()
                    .ok_or_else(|| rewrite_invariant("projection output name is missing"))?,
            );
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
        let narrow_output = output_to_narrow
            .get(output_index)
            .copied()
            .flatten()
            .ok_or_else(|| rewrite_invariant("ordinary output has no narrow carrier column"))?;
        topn_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(narrow_table_index, narrow_output),
            expression.return_type(),
        )));
        topn_names.push(
            output
                .output_names
                .get(output_index)
                .cloned()
                .ok_or_else(|| rewrite_invariant("projection output name is missing"))?,
        );
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
                .map(|materialized_table_index| RowFetchSource {
                    materialized_table_index,
                    rowid: Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(narrow_table_index, source.narrow_rowid_index),
                        LogicalType::BigInt,
                    )),
                    table: source.source.table.clone(),
                    needed_columns: source
                        .source
                        .ordered_catalog_columns
                        .values()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                })
        })
        .collect::<Vec<_>>();
    let narrow_plan = LogicalPlan::synthetic(LogicalOperator::Projection(narrow));
    let topn_carrier_child = if ordered_fetch_sources.is_empty() {
        narrow_plan
    } else {
        LogicalPlan::synthetic(LogicalOperator::RowFetch(RowFetch::new(
            narrow_table_index,
            ordered_fetch_sources,
            narrow_plan,
        )))
    };
    let topn_carrier = Projection::new(topn_table_index, topn_carrier_child, topn_expressions)
        .with_output_names(topn_names);

    for order in &mut topn.orders {
        let Expression::ColumnRef(column) = &mut order.expression else {
            return Err(rewrite_invariant("TopN key is no longer a direct column"));
        };
        let topn_output = output_to_topn
            .get(column.binding.column_index)
            .copied()
            .flatten()
            .ok_or_else(|| rewrite_invariant("ordered output is not materialized before TopN"))?;
        column.binding = ColumnBinding::new(topn_table_index, topn_output);
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
            let catalog_column = *source
                .source
                .output_catalog_columns
                .get(&output_index)
                .ok_or_else(|| rewrite_invariant("output payload mapping disappeared"))?;
            let output_table_index = source
                .output_table_index
                .ok_or_else(|| rewrite_invariant("output payload namespace is missing"))?;
            final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(output_table_index, catalog_column),
                expression.return_type(),
            )));
        } else {
            let topn_output = output_to_topn
                .get(output_index)
                .copied()
                .flatten()
                .ok_or_else(|| rewrite_invariant("ordinary output is absent from TopN carrier"))?;
            final_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(topn_table_index, topn_output),
                expression.return_type(),
            )));
        }
    }
    let mut output_fetch_sources = Vec::new();
    for source in sources {
        let Some(materialized_table_index) = source.output_table_index else {
            continue;
        };
        let rowid_index = source
            .topn_rowid_index
            .ok_or_else(|| rewrite_invariant("output payload rowid is absent from TopN carrier"))?;
        output_fetch_sources.push(RowFetchSource {
            materialized_table_index,
            rowid: Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(topn_table_index, rowid_index),
                LogicalType::BigInt,
            )),
            table: source.source.table,
            needed_columns: source
                .source
                .output_catalog_columns
                .values()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
    }
    output.child = Box::new(if output_fetch_sources.is_empty() {
        topn_plan
    } else {
        LogicalPlan::synthetic(LogicalOperator::RowFetch(RowFetch::new(
            topn_table_index,
            output_fetch_sources,
            topn_plan,
        )))
    });
    output.expressions = final_expressions;
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();
    Ok(LogicalPlan {
        id: output_id,
        stats: topn_stats,
        operator: LogicalOperator::Projection(output),
    })
}

fn apply_selective_projection_rewrite(
    plan: LogicalPlan,
    candidate: SelectiveProjectionCandidate,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Projection(mut output),
    } = plan
    else {
        return Err(rewrite_invariant(
            "selective late-payload candidate root is not Projection",
        ));
    };

    struct SourceRewrite {
        source: SelectiveProjectionSource,
        rowid_binding: ColumnBinding,
        materialized_table_index: usize,
    }

    let required_output_bindings = output
        .child
        .get_column_bindings()
        .into_iter()
        .collect::<HashSet<_>>();
    let mut sources = candidate
        .sources
        .into_iter()
        .map(|source| -> Result<SourceRewrite> {
            let rowid_binding = append_virtual_rowid(
                output.child.as_mut(),
                &source.rowid_path,
                source.source_table_index,
                &required_output_bindings,
            )
            .ok_or_else(|| {
                rewrite_invariant("selective payload rowid path no longer matches its source")
            })?;
            Ok(SourceRewrite {
                source,
                rowid_binding,
                materialized_table_index: bind_context.generate_table_index(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut delayed = HashMap::<ColumnBinding, (usize, usize, LogicalType)>::new();
    for source in &sources {
        for (&binding, &catalog_column) in &source.source.delayed_bindings {
            let ty = source
                .source
                .table
                .columns
                .get(catalog_column)
                .ok_or_else(|| rewrite_invariant("delayed catalog column disappeared"))?
                .logical_type
                .clone();
            delayed.insert(
                binding,
                (source.materialized_table_index, catalog_column, ty),
            );
        }
    }

    let mut ordinary = Vec::<(ColumnBinding, LogicalType)>::new();
    let mut ordinary_index = HashMap::<ColumnBinding, usize>::new();
    for expression in &output.expressions {
        visit_expression(expression, &mut |candidate| {
            let Expression::ColumnRef(column) = candidate else {
                return;
            };
            if column.depth != 0
                || delayed.contains_key(&column.binding)
                || ordinary_index.contains_key(&column.binding)
            {
                return;
            }
            ordinary_index.insert(column.binding, ordinary.len());
            ordinary.push((column.binding, column.return_type.clone()));
        });
    }

    let carrier_table_index = bind_context.generate_table_index();
    let mut carrier_expressions = ordinary
        .iter()
        .map(|(binding, ty)| Expression::ColumnRef(ColumnRefExpression::new(*binding, ty.clone())))
        .collect::<Vec<_>>();
    let mut carrier_names = (0..carrier_expressions.len())
        .map(|index| format!("late_carrier_{index}"))
        .collect::<Vec<_>>();
    let mut rowid_indices = Vec::with_capacity(sources.len());
    for source in &sources {
        rowid_indices.push(carrier_expressions.len());
        carrier_expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
            source.rowid_binding,
            LogicalType::BigInt,
        )));
        carrier_names.push(format!("__late_rowid_{}", source.source.source_table_index));
    }

    for expression in &mut output.expressions {
        *expression = expression.clone().replace_column_ref(&|column| {
            if column.depth != 0 {
                return None;
            }
            if let Some((table_index, catalog_column, ty)) = delayed.get(&column.binding) {
                return Some(Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(*table_index, *catalog_column),
                    ty.clone(),
                )));
            }
            ordinary_index.get(&column.binding).map(|&index| {
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(carrier_table_index, index),
                    column.return_type.clone(),
                ))
            })
        });
    }
    output.returned_types = output
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();

    let child = std::mem::replace(
        &mut *output.child,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let child_stats = child.stats.clone();
    let carrier = Projection::new(carrier_table_index, child, carrier_expressions)
        .with_output_names(carrier_names);
    let mut carrier = LogicalPlan::synthetic(LogicalOperator::Projection(carrier));
    carrier.stats = child_stats;
    let fetch_sources = sources
        .drain(..)
        .zip(rowid_indices)
        .map(|(source, rowid_index)| RowFetchSource {
            materialized_table_index: source.materialized_table_index,
            rowid: Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(carrier_table_index, rowid_index),
                LogicalType::BigInt,
            )),
            table: source.source.table,
            needed_columns: source
                .source
                .delayed_bindings
                .values()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
        .collect::<Vec<_>>();
    output.child = Box::new(LogicalPlan::synthetic(LogicalOperator::RowFetch(
        RowFetch::new(carrier_table_index, fetch_sources, carrier),
    )));
    Ok(LogicalPlan {
        id,
        stats,
        operator: LogicalOperator::Projection(output),
    })
}

fn append_virtual_rowid(
    plan: &mut LogicalPlan,
    path: &RowIdPath,
    table_index: usize,
    required_output_bindings: &HashSet<ColumnBinding>,
) -> Option<ColumnBinding> {
    match path {
        RowIdPath::Get => {
            let LogicalOperator::Get(get) = &mut plan.operator else {
                return None;
            };
            if get.table_index != table_index {
                return None;
            }
            Some(get.append_virtual_rowid("rowid"))
        }
        RowIdPath::Filter(child_path) => {
            let LogicalOperator::Filter(filter) = &mut plan.operator else {
                return None;
            };
            let binding = append_virtual_rowid(
                &mut filter.child,
                child_path,
                table_index,
                required_output_bindings,
            )?;
            let child_index = filter
                .child
                .get_column_bindings()
                .iter()
                .position(|candidate| *candidate == binding)?;
            filter.projection_map.include(child_index);
            Some(binding)
        }
        RowIdPath::Window(child_path) => {
            let LogicalOperator::Window(window) = &mut plan.operator else {
                return None;
            };
            append_virtual_rowid(
                &mut window.child,
                child_path,
                table_index,
                required_output_bindings,
            )
        }
        RowIdPath::Order(child_path) => {
            let LogicalOperator::Order(order) = &mut plan.operator else {
                return None;
            };
            let binding = append_virtual_rowid(
                &mut order.child,
                child_path,
                table_index,
                required_output_bindings,
            )?;
            let child_index = order
                .child
                .get_column_bindings()
                .iter()
                .position(|candidate| *candidate == binding)?;
            order.projection_map.include(child_index);
            Some(binding)
        }
        RowIdPath::Limit(child_path) => {
            let LogicalOperator::Limit(limit) = &mut plan.operator else {
                return None;
            };
            append_virtual_rowid(
                &mut limit.child,
                child_path,
                table_index,
                required_output_bindings,
            )
        }
        RowIdPath::EmptyResult(child_path) => {
            let LogicalOperator::EmptyResult(empty) = &mut plan.operator else {
                return None;
            };
            append_virtual_rowid(
                &mut empty.child,
                child_path,
                table_index,
                required_output_bindings,
            )
        }
        RowIdPath::Join { kind, side, child } => {
            let LogicalOperator::Join(join) = &mut plan.operator else {
                return None;
            };
            match (kind, join) {
                (RowIdJoinKind::Comparison, Join::Comparison(join)) => append_projected_join_rowid(
                    &mut join.left,
                    &mut join.right,
                    &mut join.left_projection_map,
                    &mut join.right_projection_map,
                    *side,
                    child,
                    table_index,
                    required_output_bindings,
                ),
                (RowIdJoinKind::Any, Join::Any(join)) => append_projected_join_rowid(
                    &mut join.left,
                    &mut join.right,
                    &mut join.left_projection_map,
                    &mut join.right_projection_map,
                    *side,
                    child,
                    table_index,
                    required_output_bindings,
                ),
                (RowIdJoinKind::Cross, Join::Cross(join)) => match side {
                    RowIdJoinSide::Left => append_virtual_rowid(
                        &mut join.left,
                        child,
                        table_index,
                        required_output_bindings,
                    ),
                    RowIdJoinSide::Right => append_virtual_rowid(
                        &mut join.right,
                        child,
                        table_index,
                        required_output_bindings,
                    ),
                },
                _ => None,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn append_projected_join_rowid(
    left: &mut Box<LogicalPlan>,
    right: &mut Box<LogicalPlan>,
    left_projection: &mut paro_planner::operator::ProjectionMap,
    right_projection: &mut paro_planner::operator::ProjectionMap,
    side: RowIdJoinSide,
    path: &RowIdPath,
    table_index: usize,
    required_output_bindings: &HashSet<ColumnBinding>,
) -> Option<ColumnBinding> {
    let binding = match side {
        RowIdJoinSide::Left => {
            let binding =
                append_virtual_rowid(left.as_mut(), path, table_index, required_output_bindings)?;
            let child_index = left
                .get_column_bindings()
                .iter()
                .position(|candidate| *candidate == binding)?;
            left_projection.include(child_index);
            binding
        }
        RowIdJoinSide::Right => {
            let binding =
                append_virtual_rowid(right.as_mut(), path, table_index, required_output_bindings)?;
            let child_index = right
                .get_column_bindings()
                .iter()
                .position(|candidate| *candidate == binding)?;
            right_projection.include(child_index);
            binding
        }
    };
    include_required_join_outputs(left.as_ref(), left_projection, required_output_bindings);
    include_required_join_outputs(right.as_ref(), right_projection, required_output_bindings);
    Some(binding)
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

fn rewrite_invariant(detail: &str) -> paro_common::error::ParoError {
    paro_error::internal(format!("late row-fetch proof/rewrite mismatch: {detail}"))
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
    let mut duplicate = false;
    collect_get(plan, table_index, &mut found, &mut duplicate);
    (!duplicate).then_some(found).flatten()
}

fn collect_get<'a>(
    plan: &'a LogicalPlan,
    table_index: usize,
    found: &mut Option<&'a Get>,
    duplicate: &mut bool,
) {
    if let LogicalOperator::Get(get) = &plan.operator {
        if get.table_index == table_index {
            if found.replace(get).is_some() {
                *duplicate = true;
            }
        }
    }
    for child in plan.children() {
        collect_get(child, table_index, found, duplicate);
    }
}
