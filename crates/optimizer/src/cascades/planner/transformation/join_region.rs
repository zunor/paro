// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact identities for associative join-graph enumeration problems.

use super::*;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use paro_planner::expression::{ComparisonExpression, ComparisonType};
use paro_planner::operator::{
    ComparisonJoin, CrossProduct, Join, JoinBuildSideConstraint, JoinComparisonType, JoinCondition,
    JoinType, LogicalOperator, LogicalOutputLayout, MarkJoinSemantics, ProjectionMap,
};
use paro_planner::plan::{CardinalityEstimate, CardinalityProvenance, NodeStats};
use paro_storage::statistics::ColumnStatistics;

use crate::join_order::optimizer::JoinOrderOptimizer;
use crate::join_order::query_graph::{JoinEdgeOrientation, JoinPredicateSet};
use crate::join_order::relation_manager::{
    DistinctCount, ExtractedFilter, RelationManager, RelationStats,
};

/// Internal join order is not an input to the graph enumerator. Atomic
/// boundaries, predicates and consumed facts are. Retain exact bytes rather
/// than using a digest as evidence that two enumeration problems are equal.
#[cfg(test)]
pub(super) fn identity(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<Option<Box<[u8]>>> {
    identity_with_facts(binding, memo, state, None)
}

pub(super) fn identity_with_facts(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: Option<&boundary::BoundarySnapshot>,
) -> Result<Option<Box<[u8]>>> {
    struct Graph {
        atoms: Vec<Box<[u8]>>,
        predicates: Vec<u64>,
        joins: usize,
        inputs: BTreeSet<GroupId>,
    }
    fn atom(
        operand: &PatternOperand,
        memo: &Memo,
        output: &mut StableFingerprintBuilder,
    ) -> Result<()> {
        match operand {
            PatternOperand::Group(group) => {
                output.write_u64(0);
                output.write_u64(memo.canonical_group(*group).0 as u64);
            }
            PatternOperand::Expression {
                group,
                expression,
                children,
            } => {
                let logical = memo
                    .logical_expr(*expression)
                    .ok_or_else(|| paro_error::internal("join atom lost its expression"))?;
                let encoding = logical.operator_encoding.as_deref().ok_or_else(|| {
                    paro_error::internal("join atom has no exact operator encoding")
                })?;
                output.write_u64(1);
                output.write_u64(memo.canonical_group(*group).0 as u64);
                output.write_u64(encoding.len() as u64);
                output.write_bytes(encoding);
                output.write_u64(logical.key.scalars.len() as u64);
                for scalar in &logical.key.scalars {
                    output.write_u64(scalar.0 as u64);
                }
                output.write_u64(children.len() as u64);
                for child in children {
                    atom(child, memo, output)?;
                }
            }
        }
        Ok(())
    }
    fn visit(
        operand: &PatternOperand,
        memo: &Memo,
        state: &PlannerTransformState,
        graph: &mut Graph,
    ) -> Result<()> {
        if let PatternOperand::Expression {
            expression,
            children,
            ..
        } = operand
        {
            let logical = memo
                .logical_expr(*expression)
                .ok_or_else(|| paro_error::internal("join binding lost its expression"))?;
            let operator = &state
                .payloads
                .logical
                .get(logical.payload.index())
                .ok_or_else(|| paro_error::internal("join binding lost its operator"))?
                .semantic_template
                .operator;
            match operator {
                LogicalOperator::Join(join @ Join::Comparison(comparison))
                    if comparison.join_type == JoinType::Inner && crate::join_order::relation_manager::RelationManager::join_shell_is_reorderable(join)
                        && logical.key.scalars.len() == comparison.conditions.len() => {
                    graph.predicates.extend(logical.key.scalars.iter().map(|scalar| scalar.0 as u64));
                    graph.joins += 1;
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                LogicalOperator::Join(join @ Join::Cross(_)) if crate::join_order::relation_manager::RelationManager::join_shell_is_reorderable(join) => {
                    graph.joins += 1;
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                LogicalOperator::Filter(filter) if filter.expressions.iter().all(|expression| !expression.evaluation_properties().is_reorder_fence()) => {
                    graph.predicates.extend(logical.key.scalars.iter().map(|scalar| scalar.0 as u64));
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                _ => {}
            }
        }
        let mut encoded = StableFingerprintBuilder::recording();
        atom(operand, memo, &mut encoded)?;
        graph.atoms.push(encoded.finish_recording().1);
        let owner = match operand {
            PatternOperand::Group(group) | PatternOperand::Expression { group, .. } => *group,
        };
        graph.inputs.insert(memo.canonical_group(owner));
        Ok(())
    }
    let mut graph = Graph {
        atoms: Vec::new(),
        predicates: Vec::new(),
        joins: 0,
        inputs: BTreeSet::new(),
    };
    visit(binding, memo, state, &mut graph)?;
    if graph.joins == 0 {
        return Ok(None);
    }
    graph.atoms.sort();
    graph.predicates.sort();
    let mut encoder = StableFingerprintBuilder::recording();
    encoder.write_bytes(b"paro.memo.join-graph-input.v1");
    encoder.write_u64(graph.atoms.len() as u64);
    for atom in &graph.atoms {
        encoder.write_u64(atom.len() as u64);
        encoder.write_bytes(atom);
    }
    encoder.write_u64(graph.predicates.len() as u64);
    for predicate in &graph.predicates {
        encoder.write_u64(*predicate);
    }
    // JoinGraph computes internal cardinalities from its atomic inputs. The
    // old binary tree's intermediate estimates are not enumeration inputs.
    let root = match binding {
        PatternOperand::Group(group) | PatternOperand::Expression { group, .. } => *group,
    };
    graph.inputs.insert(memo.canonical_group(root));
    for group in graph.inputs {
        let read = PatternRead::facts_from_group(memo, group)?;
        encoder.write_u64(group.0 as u64);
        if let Some(facts) = facts {
            // The reader already resolved inherited and producer evidence.
            // Recipe ids and input-list growth are invalidation cursors, not
            // inputs to enumeration. Equal fact values retain the same graph
            // problem even after its evidence DAG gains another derivation.
            facts.encode_group(group, &mut encoder)?;
        } else {
            encoder.write_fingerprint(read.logical_fact_fingerprint);
            encoder.write_fingerprint(read.statistics_snapshot_fingerprint);
        }
    }
    Ok(Some(encoder.finish_recording().1))
}

/// Enumerate the reorderable INNER/CROSS part of a binding directly from its
/// native shell. The legacy join-order implementation remains the fallback
/// for reductions, projection-sensitive joins, and opaque operators; this
/// path is intentionally conservative so a missing native capability cannot
/// turn into a semantic no-op.
pub(super) fn try_native_enumeration(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Vec<NativeShell>> {
    // Avoid even allocating native plan identities for a binary join. The
    // preflight only counts structurally reorderable children; the complete
    // shell guard below remains authoritative for projections and opaque
    // boundaries.
    if native_pattern_atom_count(binding, memo, state)? < 3 {
        return Ok(Vec::new());
    }
    let Some(shell) = NativeShell::from_pattern(memo, state, binding, facts)? else {
        return Ok(Vec::new());
    };
    // A native join graph does not yet carry the control-region facet closure
    // through its reordered group holes. Keep CTE/recursive/dependent owners on
    // the legacy path so a valid physical candidate cannot publish a
    // JointCostProof whose owner falls outside the region scope. Check the
    // actual opaque boundary references rather than the root's non-relational
    // fact observation, which is conservatively marked as control by design.
    if shell_contains_control_boundary(&shell) {
        return Ok(Vec::new());
    }
    let layouts = shell.layouts()?;
    let mut input = NativeJoinInput {
        atoms: Vec::new(),
        filters: Vec::new(),
        root_projection: None,
    };
    if !collect_native_join_input(&shell, shell.root, &layouts, true, &mut input)? {
        return Ok(Vec::new());
    }
    // A binary join has no alternative join order to enumerate. Keeping it on
    // the existing path also avoids spending native node identities for a
    // transformation that cannot reduce search work.
    if input.atoms.len() < 3 {
        return Ok(Vec::new());
    }

    let mut relation_manager = RelationManager::new();
    let mut column_stats = HashMap::new();
    let mut seen_tables = BTreeSet::new();
    for atom in &input.atoms {
        if !seen_tables.is_disjoint(&atom.tables) {
            // Repeated table bindings are not independent relation vertices.
            // Decline the native path instead of letting a partial mapping
            // silently cost or reconstruct the wrong graph.
            return Ok(Vec::new());
        }
        seen_tables.extend(atom.tables.iter().copied());
        let stats = native_relation_stats(atom, &mut column_stats);
        relation_manager.add_relation_shape(atom.tables.iter().copied(), stats);
    }
    let region_outputs = layouts
        .get(shell.root)
        .ok_or_else(|| paro_error::internal("native join shell has no root layout"))?
        .bindings()
        .iter()
        .copied()
        .zip(
            layouts
                .get(shell.root)
                .ok_or_else(|| paro_error::internal("native join shell lost root types"))?
                .types()
                .iter()
                .cloned(),
        )
        .collect::<HashMap<_, _>>();
    let mut optimizer = JoinOrderOptimizer::new(state.cost_model.defaults.clone());
    optimizer = optimizer.with_search_budget(memo.budget());
    let Some(graph) = optimizer.enumerate_relation_graph(
        input.filters,
        region_outputs,
        relation_manager,
        column_stats,
    )?
    else {
        return Ok(Vec::new());
    };

    let mut shells = Vec::with_capacity(graph.final_plans.len());
    for plan in graph.final_plans {
        let mut nodes = shell.nodes.to_vec();
        let mut used_filters = HashSet::new();
        let root_child =
            rebuild_native_join(&plan, &input.atoms, &mut used_filters, &mut nodes, state)?;
        let mut root = match root_child {
            NativeChild::Node(index) => index,
            NativeChild::MemoGroup { .. } | NativeChild::Group { .. } => {
                return Err(paro_error::internal(
                    "native join reconstruction produced a group root",
                ));
            }
        };

        let mut remaining = graph.root_filters.clone();
        remaining.extend(
            graph
                .filter_infos
                .iter()
                .filter(|filter| !used_filters.contains(&filter.filter_index))
                .map(|filter| filter.filter.clone()),
        );
        let projection_map = input.root_projection.clone();
        // INNER/CROSS reconstruction preserves the complete output width: the
        // native collector rejects every child projection that could drop or
        // duplicate a column.  The projection guard only needs that width,
        // not a second layout walk over the freshly rebuilt node graph.
        let root_width = layouts
            .get(shell.root)
            .ok_or_else(|| paro_error::internal("native join shell has no root layout"))?
            .len();
        let keep_filter = !remaining.is_empty()
            || projection_map
                .as_ref()
                .is_some_and(|projection| !projection.is_identity(root_width));
        if keep_filter {
            let index = nodes.len();
            nodes.push(NativeNode {
                id: state.bind_context.next_plan_id(),
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                    expressions: remaining,
                    child: NativeChild::Node(root),
                    projection_map: projection_map.unwrap_or_else(ProjectionMap::all),
                }),
            });
            root = index;
        }
        shells.push(compact_native_shell(NativeShell {
            nodes: nodes.into_boxed_slice(),
            root,
        })?);
    }
    Ok(shells)
}

fn shell_contains_control_boundary(shell: &NativeShell) -> bool {
    shell.nodes.iter().any(|node| {
        let mut control = false;
        node.operator.visit_child_links(&mut |child| {
            if matches!(
                child,
                NativeChild::MemoGroup { reference, .. }
                    | NativeChild::Group { reference, .. }
                    if reference.facts.contains_control_region
            ) {
                control = true;
            }
        });
        control
    })
}

fn native_pattern_atom_count(
    operand: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<usize> {
    let PatternOperand::Expression {
        expression,
        children,
        ..
    } = operand
    else {
        return Ok(1);
    };
    let logical = memo
        .logical_expr(*expression)
        .ok_or_else(|| paro_error::internal("native join preflight lost its expression"))?;
    let operator = &state
        .payloads
        .logical
        .get(logical.payload.index())
        .ok_or_else(|| paro_error::internal("native join preflight lost its operator"))?
        .semantic_template
        .operator;
    let reorderable = match operator {
        LogicalOperator::Filter(filter) => filter
            .expressions
            .iter()
            .all(|expression| !expression.evaluation_properties().is_reorder_fence()),
        LogicalOperator::Join(join) => RelationManager::join_shell_is_reorderable(join),
        _ => false,
    };
    if !reorderable {
        return Ok(1);
    }
    children.iter().try_fold(0usize, |count, child| {
        native_pattern_atom_count(child, memo, state).map(|atoms| count.saturating_add(atoms))
    })
}

struct NativeJoinAtom {
    child: NativeChild,
    layout: LogicalOutputLayout,
    stats: NodeStats,
    tables: BTreeSet<usize>,
}

struct NativeJoinInput {
    atoms: Vec<NativeJoinAtom>,
    filters: Vec<ExtractedFilter>,
    root_projection: Option<ProjectionMap>,
}

fn collect_native_join_input(
    shell: &NativeShell,
    index: usize,
    layouts: &[LogicalOutputLayout],
    at_root: bool,
    input: &mut NativeJoinInput,
) -> Result<bool> {
    let node = shell
        .nodes
        .get(index)
        .ok_or_else(|| paro_error::internal("native join shell references an unknown node"))?;
    match &node.operator {
        LogicalOperator::Filter(filter) => {
            if filter
                .expressions
                .iter()
                .any(|expression| expression.evaluation_properties().is_reorder_fence())
            {
                return Ok(false);
            }
            if !at_root && !filter.projection_map.is_identity(layouts[index].len()) {
                return Ok(false);
            }
            if at_root {
                input.root_projection = Some(filter.projection_map.clone());
            }
            input.filters.extend(
                filter
                    .expressions
                    .iter()
                    .cloned()
                    .map(ExtractedFilter::inner),
            );
            collect_native_join_child(shell, &filter.child, layouts, false, input)
        }
        LogicalOperator::Join(Join::Comparison(join))
            if join.join_type == JoinType::Inner
                && join.build_side_constraint == JoinBuildSideConstraint::Either
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped
                && !crate::expression::comparison_join_has_evaluation_fence(join)
                && join
                    .left_projection_map
                    .is_identity(native_child_layout(&join.left, layouts)?.len())
                && join
                    .right_projection_map
                    .is_identity(native_child_layout(&join.right, layouts)?.len()) =>
        {
            input
                .filters
                .extend(join.conditions.iter().map(|condition| {
                    ExtractedFilter::inner(Expression::Comparison(
                        ComparisonExpression::new(
                            comparison_type(condition.comparison),
                            condition.left.clone(),
                            condition.right.clone(),
                        )
                        .into(),
                    ))
                }));
            {
                let left_ok = collect_native_join_child(shell, &join.left, layouts, false, input)?;
                let right_ok =
                    collect_native_join_child(shell, &join.right, layouts, false, input)?;
                Ok(left_ok && right_ok)
            }
        }
        LogicalOperator::Join(Join::Cross(join))
            if join.build_side_constraint == JoinBuildSideConstraint::Either =>
        {
            let left_ok = collect_native_join_child(shell, &join.left, layouts, false, input)?;
            let right_ok = collect_native_join_child(shell, &join.right, layouts, false, input)?;
            Ok(left_ok && right_ok)
        }
        _ => {
            let layout = layouts
                .get(index)
                .cloned()
                .ok_or_else(|| paro_error::internal("native join atom has no layout"))?;
            let tables = layout
                .bindings()
                .iter()
                .map(|binding| binding.table_index)
                .collect::<BTreeSet<_>>();
            if tables.is_empty() {
                return Ok(false);
            }
            input.atoms.push(NativeJoinAtom {
                child: NativeChild::Node(index),
                layout,
                stats: node.stats.clone(),
                tables,
            });
            Ok(true)
        }
    }
}

fn collect_native_join_child(
    shell: &NativeShell,
    child: &NativeChild,
    layouts: &[LogicalOutputLayout],
    _at_root: bool,
    input: &mut NativeJoinInput,
) -> Result<bool> {
    match child {
        NativeChild::Node(index) => collect_native_join_input(shell, *index, layouts, false, input),
        NativeChild::MemoGroup { layout, stats, .. } => {
            let tables = layout
                .bindings()
                .iter()
                .map(|binding| binding.table_index)
                .collect::<BTreeSet<_>>();
            if tables.is_empty() {
                return Ok(false);
            }
            input.atoms.push(NativeJoinAtom {
                child: child.clone(),
                layout: layout.clone(),
                stats: stats.clone(),
                tables,
            });
            Ok(true)
        }
        NativeChild::Group { .. } => Ok(false),
    }
}

fn native_child_layout(
    child: &NativeChild,
    layouts: &[LogicalOutputLayout],
) -> Result<LogicalOutputLayout> {
    match child {
        NativeChild::Node(index) => layouts
            .get(*index)
            .cloned()
            .ok_or_else(|| paro_error::internal("native join child has no layout")),
        NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
            Ok(layout.clone())
        }
    }
}

fn comparison_type(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
    }
}

fn native_relation_stats(
    atom: &NativeJoinAtom,
    column_stats: &mut HashMap<paro_planner::operator::ColumnBinding, Arc<ColumnStatistics>>,
) -> RelationStats {
    let cardinality = atom
        .stats
        .estimated_cardinality
        .map(|estimate| usize::try_from(estimate.expected.max(1)).unwrap_or(usize::MAX))
        .unwrap_or(1)
        .max(1);
    let mut stats = RelationStats::with_cardinality(cardinality);
    stats.risk_cardinality = atom
        .stats
        .materialization_risk_cardinality
        .unwrap_or(cardinality as u64)
        .try_into()
        .unwrap_or(usize::MAX)
        .max(cardinality);
    stats.materialization_cardinality = stats.risk_cardinality;
    stats.estimated_payload_width =
        crate::join::build_probe_side::estimate_row_payload_width(atom.layout.types());
    stats.unique_keys = atom
        .stats
        .unique_keys
        .iter()
        .map(|key| key.columns.iter().map(|column| column.binding).collect())
        .collect();

    let Some(reference) = (match &atom.child {
        NativeChild::MemoGroup { reference, .. } => Some(reference),
        NativeChild::Node(_) | NativeChild::Group { .. } => None,
    }) else {
        return stats;
    };
    let columns = reference.column_statistics();
    let mut distinct = HashMap::new();
    for (binding, column) in atom.layout.bindings().iter().copied().zip(columns) {
        column_stats.insert(binding, column.clone());
        let evidence = column.distinct_evidence();
        let known = evidence.is_known();
        let count = DistinctCount::from_evidence(evidence, known);
        distinct.insert(binding, count);
    }
    stats.materialization_distinct_count = distinct.clone();
    stats.column_distinct_count = distinct;
    stats.contains_control_region = reference.facts.contains_control_region;
    stats
}

fn rebuild_native_join(
    node: &crate::join_order::cost_model::DPJoinNode,
    atoms: &[NativeJoinAtom],
    used_filters: &mut HashSet<usize>,
    nodes: &mut Vec<NativeNode>,
    state: &PlannerTransformState,
) -> Result<NativeChild> {
    if node.is_leaf {
        return atoms
            .get(node.set.relations()[0])
            .map(|atom| atom.child.clone())
            .ok_or_else(|| paro_error::internal("native join leaf is out of range"));
    }
    let left = rebuild_native_join(
        node.left_plan
            .as_deref()
            .ok_or_else(|| paro_error::internal("native join lost left frontier child"))?,
        atoms,
        used_filters,
        nodes,
        state,
    )?;
    let right = rebuild_native_join(
        node.right_plan
            .as_deref()
            .ok_or_else(|| paro_error::internal("native join lost right frontier child"))?,
        atoms,
        used_filters,
        nodes,
        state,
    )?;
    let flip_for_build = node.build_side == crate::join::build_probe_side::JoinBuildSide::Left;
    let (left, right) = if flip_for_build {
        (right, left)
    } else {
        (left, right)
    };
    let mut conditions = Vec::new();
    if let Some(predicates) = &node.predicates {
        for predicate in predicates.predicates() {
            let filter = predicate.filter();
            let start_len = conditions.len();
            let append = |comparison: &paro_planner::expression::ComparisonExpression,
                          conditions: &mut Vec<JoinCondition>| {
                let Some(orientation) = predicate.orientation() else {
                    return;
                };
                let comparison_type = match comparison.comparison_type {
                    ComparisonType::Equal => JoinComparisonType::Equal,
                    ComparisonType::NotEqual => JoinComparisonType::NotEqual,
                    ComparisonType::LessThan => JoinComparisonType::LessThan,
                    ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
                    ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
                    ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
                    ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
                    ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
                };
                let inverted = orientation == JoinEdgeOrientation::Inverted;
                conditions.push(JoinCondition::new(
                    if inverted {
                        (*comparison.right).clone()
                    } else {
                        (*comparison.left).clone()
                    },
                    if inverted {
                        (*comparison.left).clone()
                    } else {
                        (*comparison.right).clone()
                    },
                    if inverted {
                        comparison_type.flip()
                    } else {
                        comparison_type
                    },
                ));
            };
            match &filter.filter {
                Expression::Comparison(comparison) => append(comparison, &mut conditions),
                Expression::Conjunction(conjunction) => {
                    for child in &conjunction.children {
                        if let Expression::Comparison(comparison) = child {
                            append(comparison, &mut conditions);
                        }
                    }
                }
                _ => {}
            }
            if conditions.len() > start_len && predicate.orientation().is_some() {
                used_filters.insert(filter.filter_index);
            }
        }
    }
    if flip_for_build {
        for condition in &mut conditions {
            std::mem::swap(&mut condition.left, &mut condition.right);
            condition.comparison = condition.comparison.flip();
        }
    }
    let operator = if conditions.is_empty() {
        LogicalOperator::Join(Join::Cross(CrossProduct {
            left,
            right,
            build_side_constraint: JoinBuildSideConstraint::Either,
        }))
    } else {
        let join_type = node
            .predicates
            .as_ref()
            .map_or(JoinType::Inner, JoinPredicateSet::join_type);
        if join_type != JoinType::Inner {
            return Err(paro_error::internal(
                "native join reconstruction encountered a reduction",
            ));
        }
        LogicalOperator::Join(Join::Comparison(ComparisonJoin {
            join_type,
            anti_join_mode: Default::default(),
            left,
            right,
            conditions,
            mark_index: None,
            mark_semantics: MarkJoinSemantics::for_join_type(join_type),
            duplicate_eliminated_columns: Vec::new(),
            delim_flipped: false,
            build_side_constraint: JoinBuildSideConstraint::Either,
            left_projection_map: ProjectionMap::all(),
            right_projection_map: ProjectionMap::all(),
        }))
    };
    let mut stats = NodeStats::default();
    stats.set_cardinality(
        CardinalityEstimate::exact(quantize_native_cardinality(node.cardinality)),
        CardinalityProvenance::JoinGraph,
        Some(quantize_native_cardinality(
            node.materialization_cardinality,
        )),
    );
    let index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats,
        operator,
    });
    Ok(NativeChild::Node(index))
}

fn quantize_native_cardinality(cardinality: f64) -> u64 {
    if !cardinality.is_finite() || cardinality >= u64::MAX as f64 {
        u64::MAX
    } else {
        cardinality.max(1.0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{ComparisonJoin, Get, JoinCondition};

    fn scan(table: usize) -> OwnedLogicalPlan {
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
            Get::new_without_table(table, vec!["k".into()], vec![LogicalType::BigInt]),
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100));
        plan
    }
    fn join(
        left: OwnedLogicalPlan,
        right: OwnedLogicalPlan,
        a: usize,
        b: usize,
    ) -> OwnedLogicalPlan {
        let column = |table| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::BigInt).into(),
            )
        };
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                left,
                right,
                vec![JoinCondition::new(
                    column(a),
                    column(b),
                    JoinComparisonType::Equal,
                )],
            ),
        )))
    }

    #[test]
    fn associative_graph_identity_preserves_predicates_and_input_statistics() {
        let left = join(join(scan(0), scan(1), 0, 1), scan(2), 1, 2);
        let right = join(scan(0), join(scan(1), scan(2), 2, 1), 1, 0);
        let mut input = MemoBuilder::build_alternatives(
            vec![
                LogicalAlternative {
                    plan: left,
                    source: AlternativeOrigin::Baseline,
                    column_stats: Arc::new(HashMap::new()),
                },
                LogicalAlternative {
                    plan: right,
                    source: AlternativeOrigin::Specialized {
                        rule: JOIN_REGION_ENUMERATION_RULE,
                    },
                    column_stats: Arc::new(HashMap::new()),
                },
            ],
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let state = input.planner_state.read().unwrap();
        let mut scans = BTreeMap::new();
        let leaves = input
            .memo
            .groups()
            .filter_map(|group| {
                let logical = input.memo.logical_expr(group.logical_exprs()[0])?;
                let LogicalOperator::Get(get) = &state.payloads.logical[logical.payload.index()]
                    .semantic_template
                    .operator
                else {
                    return None;
                };
                Some((get.table_index, group.id))
            })
            .collect::<Vec<_>>();
        for (table, group) in leaves {
            if let Some(previous) = scans.insert(table, group) {
                input.memo.merge_groups(previous, group).unwrap();
            }
        }
        let expressions = input
            .memo
            .group(input.root)
            .unwrap()
            .logical_exprs()
            .to_vec();
        assert_eq!(expressions.len(), 2);
        let bindings = expressions
            .iter()
            .map(|expression| {
                matching::scoped_pattern_bindings(
                    PlannerTransformation::JoinRegionEnumeration,
                    input.root,
                    *expression,
                    &input.memo,
                    &state,
                    None,
                    BudgetDimension::RuleWorkPerGroup,
                )
                .unwrap()
                .bindings[0]
                    .clone()
            })
            .collect::<Vec<_>>();
        let first = identity(&bindings[0].root, &input.memo, &state)
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            identity(&bindings[1].root, &input.memo, &state)
                .unwrap()
                .unwrap()
        );
        let leaf = input
            .memo
            .groups()
            .find(|group| {
                group.logical_exprs().iter().any(|expr| {
                    let logical = input.memo.logical_expr(*expr).unwrap();
                    matches!(
                        state.payloads.logical[logical.payload.index()]
                            .semantic_template
                            .operator,
                        LogicalOperator::Get(_)
                    )
                })
            })
            .unwrap()
            .id;
        fn native_identity(
            memo: &mut Memo,
            state: &PlannerTransformState,
            root: GroupId,
            binding: &PatternOperand,
        ) -> Box<[u8]> {
            let mut ctx = TransformContext::new(memo, root);
            let facts = boundary::BoundarySnapshot::read(
                &mut ctx,
                state,
                binding,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .unwrap();
            identity_with_facts(binding, ctx.memo(), state, Some(&facts))
                .unwrap()
                .unwrap()
        }
        let native = native_identity(&mut input.memo, &state, input.root, &bindings[0].root);
        input.memo.group_mut(leaf).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(776),
            CardinalityRecipeKind::Statistics,
            100,
            100,
            100,
        );
        assert_eq!(
            native,
            native_identity(&mut input.memo, &state, input.root, &bindings[0].root),
            "changing a recipe without changing its value is not a new graph problem"
        );
        input.memo.group_mut(leaf).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(777),
            CardinalityRecipeKind::Statistics,
            0,
            80,
            100,
        );
        assert_ne!(
            first,
            identity(&bindings[0].root, &input.memo, &state)
                .unwrap()
                .unwrap()
        );
        assert_ne!(
            native,
            native_identity(&mut input.memo, &state, input.root, &bindings[0].root)
        );
    }

    #[test]
    fn native_enumeration_rebuilds_join_graph_without_owned_bridge() {
        let plan = join(join(scan(0), scan(1), 0, 1), scan(2), 1, 2);
        let mut input = MemoBuilder::build_alternatives(
            vec![LogicalAlternative {
                plan,
                source: AlternativeOrigin::Baseline,
                column_stats: Arc::new(HashMap::new()),
            }],
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let state = input.planner_state.read().unwrap();
        let expression = input
            .memo
            .group(input.root)
            .unwrap()
            .logical_exprs()
            .first()
            .copied()
            .unwrap();
        let binding = matching::scoped_pattern_bindings(
            PlannerTransformation::JoinRegionEnumeration,
            input.root,
            expression,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .bindings
        .first()
        .cloned()
        .unwrap();
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let shells = try_native_enumeration(&binding.root, context.memo(), &state, &facts).unwrap();
        assert!(!shells.is_empty());
        for shell in shells {
            assert!(shell.nodes.len() >= 2);
            assert!(matches!(
                shell.root_operator(),
                LogicalOperator::Join(_) | LogicalOperator::Filter(_)
            ));
            assert_eq!(shell.root_layout().unwrap().len(), 3);
        }
    }
}
