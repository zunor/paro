// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Partition a shared `UNION ALL` CTE by a constant discriminator.
//!
//! When every consumer fixes the same output column to one branch constant,
//! materializing the full union couples unrelated producers and makes every
//! reader scan every partition. This rewrite creates one independent sharing
//! owner per branch, pushes the consumers' predicates into each producer, and
//! materializes every partition as one bounded compound recipe. Ordinary
//! whole-CTE sharing and inlining remain independent Memo alternatives.
//!
//! The discriminator substitution is justified by a structural lineage
//! witness: every rewritten CTE reference must remain outside null-supplying
//! join inputs. If that proof or the compound materialization recipe cannot be
//! completed, this advisory rule publishes no alternative.

use std::collections::HashMap;
use std::ops::ControlFlow;

use paro_common::runtime_value::Value;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{ComparisonType, ConjunctionType, Expression};
use paro_planner::operator::{
    ColumnBinding, DependentJoinKind, JoinType, LogicalOperator, MaterializedCTE, SetOpType,
};
use paro_planner::plan::LogicalPlan;

#[derive(Debug)]
struct Partitioning {
    discriminator_ordinal: usize,
    branch_values: Vec<Value>,
    reference_partitions: HashMap<usize, usize>,
    reference_counts: Vec<usize>,
}

pub(crate) struct CTEPartitioner<'a> {
    bind_context: &'a BindContext,
}

impl<'a> CTEPartitioner<'a> {
    pub(crate) fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    /// Produce a root-local partitioned sharing alternative.
    ///
    /// Restricting the input to DEFAULT `UNION ALL` and producing single-branch
    /// owners makes the rule structurally idempotent.
    pub(crate) fn optimize_default_root(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let Some(partitioning) = recognize(&plan) else {
            return None;
        };
        self.apply(plan, partitioning)
    }

    /// Materialize a recognized partitioning recipe.
    ///
    /// This is an advisory optimizer alternative, so a stale or incomplete
    /// recognition witness rejects the alternative rather than turning an
    /// otherwise valid user query into an error.
    fn apply(&self, plan: LogicalPlan, partitioning: Partitioning) -> Option<LogicalPlan> {
        let LogicalOperator::MaterializedCTE(cte) = plan.into_operator() else {
            return None;
        };
        let mut branches = Vec::new();
        consume_union_all(*cte.cte_query, &mut branches);
        if branches.len() != partitioning.branch_values.len() {
            return None;
        }

        let partition_indices = (0..branches.len())
            .map(|_| self.bind_context.generate_table_index())
            .collect::<Vec<_>>();
        let mut child = *cte.child;
        rewrite_partition_constants(
            &mut child.operator,
            partitioning.discriminator_ordinal,
            &partitioning.reference_partitions,
            &partitioning.branch_values,
        );
        if !rewrite_references(
            &mut child.operator,
            cte.cte_index,
            &partitioning.reference_partitions,
            &partition_indices,
        ) {
            return None;
        }

        for (partition, branch) in branches.into_iter().enumerate().rev() {
            let name = format!("{}$partition{}", cte.cte_name, partition + 1);
            let partition_index = *partition_indices.get(partition)?;
            let reference_count = *partitioning.reference_counts.get(partition)?;
            child = LogicalPlan::new(
                self.bind_context,
                LogicalOperator::MaterializedCTE(
                    MaterializedCTE::new(
                        partition_index,
                        name,
                        cte.column_names.clone(),
                        cte.column_types.clone(),
                        CTEMaterialize::Default,
                        branch,
                        child,
                    )
                    .with_ref_count(reference_count),
                ),
            );
        }

        // A partitioned owner is useful only together with producer-side
        // filtering. Publish that complete physical sharing strategy as one
        // Memo expression instead of exposing intermediate DEFAULT owners
        // whose independently chosen inline/materialize decisions multiply
        // the search space and can create a partially shared hybrid.
        let (child, pushed) =
            super::filter_pusher::CTEFilterPusher::new().optimize_plan_with_change(child);
        if !pushed || !partition_owners_are_materialized(&child.operator, &partition_indices) {
            return None;
        }
        Some(child)
    }
}

fn recognize(plan: &LogicalPlan) -> Option<Partitioning> {
    let LogicalOperator::MaterializedCTE(cte) = &plan.operator else {
        return None;
    };
    if cte.materialized != CTEMaterialize::Default {
        return None;
    }

    let mut branches = Vec::new();
    collect_union_all(cte.cte_query.as_ref(), &mut branches)?;
    if branches.len() < 2 {
        return None;
    }

    // Replacing a reference binding by its branch constant is sound only
    // while every output row carrying that binding originated at the CTE
    // scan. A null-supplying outer-join input violates that invariant: an
    // unmatched row carries SQL NULL, not the branch constant. Build the
    // reference set and its structural safety witness together, even when a
    // projection currently hides the binding above the join, so correctness
    // cannot depend on a separate projection-elimination decision.
    let mut references = Vec::new();
    if !collect_rewrite_safe_reference_tables(
        &cte.child.operator,
        cte.cte_index,
        false,
        &mut references,
    ) {
        return None;
    }
    if references.len() < 2 {
        return None;
    }
    references.sort_unstable();
    references.dedup();

    // Filter pushing can make the compound recipe complete only when every
    // reference has a directly attached filter. Reject broader shapes here;
    // another normalization rule must first expose that local operand.
    let mut directly_filtered = Vec::new();
    collect_directly_filtered_reference_tables(
        &cte.child.operator,
        cte.cte_index,
        &mut directly_filtered,
    );
    directly_filtered.sort_unstable();
    directly_filtered.dedup();
    if directly_filtered != references {
        return None;
    }

    let mut equalities = HashMap::<ColumnBinding, Vec<Value>>::new();
    collect_filter_equalities(&cte.child.operator, &mut equalities);

    for ordinal in 0..cte.column_types.len() {
        let Some(branch_values) = branches
            .iter()
            .map(|branch| constant_output(branch, ordinal))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        if branch_values
            .iter()
            .enumerate()
            .any(|(index, value)| branch_values[..index].contains(value))
        {
            continue;
        }

        let mut reference_partitions = HashMap::new();
        let mut reference_counts = vec![0usize; branches.len()];
        let mut all_constrained = true;
        for table_index in &references {
            let binding = ColumnBinding::new(*table_index, ordinal);
            let Some(values) = equalities.get(&binding) else {
                all_constrained = false;
                break;
            };
            let mut values = values.iter();
            let Some(value) = values.next() else {
                all_constrained = false;
                break;
            };
            if values.any(|candidate| candidate != value) {
                all_constrained = false;
                break;
            }
            let Some(partition) = branch_values
                .iter()
                .position(|candidate| candidate == value)
            else {
                all_constrained = false;
                break;
            };
            reference_partitions.insert(*table_index, partition);
            reference_counts[partition] += 1;
        }
        // Do not suppress a producer branch. Besides keeping error behavior
        // stable, requiring every partition to be live makes this a pure
        // sharing-layout choice rather than predicate simplification.
        if all_constrained && reference_counts.iter().all(|count| *count > 0) {
            return Some(Partitioning {
                discriminator_ordinal: ordinal,
                branch_values,
                reference_partitions,
                reference_counts,
            });
        }
    }
    None
}

fn rewrite_partition_constants(
    operator: &mut LogicalOperator,
    discriminator_ordinal: usize,
    reference_partitions: &HashMap<usize, usize>,
    branch_values: &[Value],
) {
    paro_planner::visitor::enumerate_expressions(operator, |expression| {
        *expression = expression.clone().replace_column_ref(&|column| {
            if column.depth != 0 || column.binding.column_index != discriminator_ordinal {
                return None;
            }
            let partition = reference_partitions.get(&column.binding.table_index)?;
            Some(Expression::Constant(
                paro_planner::expression::ConstantExpression::new(
                    branch_values.get(*partition)?.clone(),
                    column.return_type.clone(),
                ),
            ))
        });
    });
    let _ = operator.visit_children_mut(|child| {
        rewrite_partition_constants(
            &mut child.operator,
            discriminator_ordinal,
            reference_partitions,
            branch_values,
        );
        ControlFlow::Continue(())
    });
}

fn collect_union_all<'a>(plan: &'a LogicalPlan, branches: &mut Vec<&'a LogicalPlan>) -> Option<()> {
    match &plan.operator {
        LogicalOperator::SetOperation(set)
            if set.setop_type == SetOpType::Union && set.setop_all =>
        {
            collect_union_all(set.left.as_ref(), branches)?;
            collect_union_all(set.right.as_ref(), branches)?;
        }
        _ => branches.push(plan),
    }
    Some(())
}

fn consume_union_all(plan: LogicalPlan, branches: &mut Vec<LogicalPlan>) {
    let (id, stats, operator) = plan.into_parts();
    match operator {
        LogicalOperator::SetOperation(set)
            if set.setop_type == SetOpType::Union && set.setop_all =>
        {
            consume_union_all(*set.left, branches);
            consume_union_all(*set.right, branches);
        }
        operator => branches.push(LogicalPlan {
            id,
            stats,
            operator,
        }),
    }
}

fn constant_output(plan: &LogicalPlan, ordinal: usize) -> Option<Value> {
    let binding = *plan.get_column_bindings().get(ordinal)?;
    match &plan.operator {
        LogicalOperator::Projection(projection)
            if binding.table_index == projection.table_index =>
        {
            constant_expression(
                projection.expressions.get(binding.column_index)?,
                &projection.child,
            )
        }
        LogicalOperator::Aggregate(aggregate) if binding.table_index == aggregate.group_index => {
            constant_expression(
                aggregate.groups.get(binding.column_index)?,
                &aggregate.child,
            )
        }
        _ => None,
    }
}

fn constant_expression(expression: &Expression, child: &LogicalPlan) -> Option<Value> {
    match expression {
        Expression::Constant(constant) if !constant.value.is_null() => Some(constant.value.clone()),
        Expression::ColumnRef(column) if column.depth == 0 => {
            let ordinal = child
                .get_column_bindings()
                .iter()
                .position(|binding| *binding == column.binding)?;
            constant_output(child, ordinal)
        }
        _ => None,
    }
}

/// Collect references to `cte_index` only when none occurs below a boundary
/// that can synthesize NULLs for that input's columns.
///
/// `in_null_supplying_input` is inherited through the subtree because once an
/// ancestor can replace an input row by a null-extended row, no descendant
/// binding may be treated as an unconditional value at expressions above that
/// ancestor. Join predicates themselves are evaluated before null extension,
/// but this rule rewrites the whole consumer tree, including those ancestors.
fn collect_rewrite_safe_reference_tables(
    operator: &LogicalOperator,
    cte_index: usize,
    in_null_supplying_input: bool,
    refs: &mut Vec<usize>,
) -> bool {
    if let LogicalOperator::CTERef(reference) = operator {
        if reference.cte_index != cte_index {
            return true;
        }
        if in_null_supplying_input {
            return false;
        }
        refs.push(reference.table_index);
        return true;
    }

    match operator {
        LogicalOperator::Join(join) => {
            let (left_is_null_supplying, right_is_null_supplying) =
                join_null_supplying_inputs(join.join_type());
            collect_rewrite_safe_reference_tables(
                &join.left().operator,
                cte_index,
                in_null_supplying_input || left_is_null_supplying,
                refs,
            ) && collect_rewrite_safe_reference_tables(
                &join.right().operator,
                cte_index,
                in_null_supplying_input || right_is_null_supplying,
                refs,
            )
        }
        LogicalOperator::DependentJoin(join) => {
            let (left_is_null_supplying, right_is_null_supplying) = match &join.kind {
                // A scalar subquery produces one NULL-valued RHS row when its
                // input is empty, which is the same lineage boundary as the
                // right input of a LEFT/SINGLE join.
                DependentJoinKind::Scalar { .. } => (false, true),
                DependentJoinKind::Mark { .. } => (false, false),
                DependentJoinKind::Lateral { join_type, .. } => {
                    join_null_supplying_inputs(*join_type)
                }
            };
            collect_rewrite_safe_reference_tables(
                &join.left.operator,
                cte_index,
                in_null_supplying_input || left_is_null_supplying,
                refs,
            ) && collect_rewrite_safe_reference_tables(
                &join.right.operator,
                cte_index,
                in_null_supplying_input || right_is_null_supplying,
                refs,
            )
        }
        _ => operator.children().into_iter().all(|child| {
            collect_rewrite_safe_reference_tables(
                &child.operator,
                cte_index,
                in_null_supplying_input,
                refs,
            )
        }),
    }
}

fn join_null_supplying_inputs(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Left | JoinType::Single => (false, true),
        JoinType::Right => (true, false),
        JoinType::Outer => (true, true),
        JoinType::Invalid
        | JoinType::Inner
        | JoinType::Semi
        | JoinType::Anti
        | JoinType::Mark
        | JoinType::RightSemi
        | JoinType::RightAnti => (false, false),
    }
}

fn collect_directly_filtered_reference_tables(
    operator: &LogicalOperator,
    cte_index: usize,
    refs: &mut Vec<usize>,
) {
    if let LogicalOperator::Filter(filter) = operator {
        if let LogicalOperator::CTERef(reference) = &filter.child.operator {
            if reference.cte_index == cte_index {
                refs.push(reference.table_index);
            }
            return;
        }
    }
    for child in operator.children() {
        collect_directly_filtered_reference_tables(&child.operator, cte_index, refs);
    }
}

fn collect_filter_equalities(
    operator: &LogicalOperator,
    equalities: &mut HashMap<ColumnBinding, Vec<Value>>,
) {
    if let LogicalOperator::Filter(filter) = operator {
        for expression in &filter.expressions {
            collect_conjunctive_equalities(expression, equalities);
        }
    }
    for child in operator.children() {
        collect_filter_equalities(&child.operator, equalities);
    }
}

fn collect_conjunctive_equalities(
    expression: &Expression,
    equalities: &mut HashMap<ColumnBinding, Vec<Value>>,
) {
    match expression {
        Expression::Conjunction(conjunction)
            if conjunction.conjunction_type == ConjunctionType::And =>
        {
            for child in &conjunction.children {
                collect_conjunctive_equalities(child, equalities);
            }
        }
        Expression::Comparison(comparison)
            if matches!(
                comparison.comparison_type,
                ComparisonType::Equal | ComparisonType::NotDistinctFrom
            ) =>
        {
            let pair = match (comparison.left.as_ref(), comparison.right.as_ref()) {
                (Expression::ColumnRef(column), Expression::Constant(constant))
                | (Expression::Constant(constant), Expression::ColumnRef(column))
                    if column.depth == 0 && !constant.value.is_null() =>
                {
                    Some((column.binding, constant.value.clone()))
                }
                _ => None,
            };
            if let Some((binding, value)) = pair {
                let values = equalities.entry(binding).or_default();
                if !values.contains(&value) {
                    values.push(value);
                }
            }
        }
        _ => {}
    }
}

fn rewrite_references(
    operator: &mut LogicalOperator,
    old_cte_index: usize,
    reference_partitions: &HashMap<usize, usize>,
    partition_indices: &[usize],
) -> bool {
    if let LogicalOperator::CTERef(reference) = operator {
        if reference.cte_index == old_cte_index {
            let Some(partition) = reference_partitions.get(&reference.table_index) else {
                return false;
            };
            let Some(partition_index) = partition_indices.get(*partition) else {
                return false;
            };
            reference.cte_index = *partition_index;
        }
        return true;
    }
    let mut complete = true;
    let _ = operator.visit_children_mut(|child| {
        complete &= rewrite_references(
            &mut child.operator,
            old_cte_index,
            reference_partitions,
            partition_indices,
        );
        if complete {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    });
    complete
}

fn partition_owners_are_materialized(
    operator: &LogicalOperator,
    partition_indices: &[usize],
) -> bool {
    let mut materialized = vec![false; partition_indices.len()];
    fn visit(operator: &LogicalOperator, partition_indices: &[usize], materialized: &mut [bool]) {
        if let LogicalOperator::MaterializedCTE(cte) = operator {
            if let Some(partition) = partition_indices
                .iter()
                .position(|index| *index == cte.cte_index)
            {
                materialized[partition] = cte.materialized == CTEMaterialize::Materialized;
            }
        }
        for child in operator.children() {
            visit(&child.operator, partition_indices, materialized);
        }
    }
    visit(operator, partition_indices, &mut materialized);
    materialized.into_iter().all(|present| present)
}

#[cfg(test)]
#[path = "partitioned_materialization/tests.rs"]
mod tests;
