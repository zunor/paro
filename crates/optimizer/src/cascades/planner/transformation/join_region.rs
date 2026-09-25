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
use paro_planner::plan::{CardinalityProvenance, NodeStats};
use paro_storage::statistics::ColumnStatistics;

use crate::region::join::optimizer::JoinOrderOptimizer;
use crate::region::join::query_graph::{FilterInfo, JoinEdgeOrientation, JoinPredicateSet};
use crate::region::join::relation_manager::{
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
                    if comparison.join_type == JoinType::Inner && crate::region::join::relation_manager::RelationManager::join_shell_is_reorderable(join)
                        && logical.key.scalars.len() == comparison.conditions.len() => {
                    graph.predicates.extend(logical.key.scalars.iter().map(|scalar| scalar.0 as u64));
                    graph.joins += 1;
                    for child in children { visit(child, memo, state, graph)?; }
                    return Ok(());
                }
                LogicalOperator::Join(join @ Join::Cross(_)) if crate::region::join::relation_manager::RelationManager::join_shell_is_reorderable(join) => {
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
#[cfg(test)]
pub(super) fn try_native_enumeration(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Vec<NativeShell>> {
    let cache_key = identity_with_facts(binding, memo, state, Some(facts))?.map(|identity| {
        (
            memo.canonical_group(match binding {
                PatternOperand::Group(group) | PatternOperand::Expression { group, .. } => *group,
            }),
            identity,
        )
    });
    try_native_enumeration_with_cache_key(binding, memo, state, facts, cache_key.as_ref())
}

/// Native enumeration with an identity that was already computed by the
/// transformation task.  `apply_binding` needs the same identity as a
/// publication guard before it enters this producer; carrying that exact
/// value through avoids a second graph/fact walk.  The caller must provide an
/// identity from the same Memo/fact snapshot and before any mutation of either
/// one.  The public wrapper above remains useful for direct tests and callers
/// which do not already own that proof.
pub(super) fn try_native_enumeration_with_cache_key(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
    cache_key: Option<&(GroupId, Box<[u8]>)>,
) -> Result<Vec<NativeShell>> {
    // Avoid even allocating native plan identities for a binary join. The
    // preflight only counts structurally reorderable children; the complete
    // shell guard below remains authoritative for projections and opaque
    // boundaries.
    if native_pattern_atom_count(binding, memo, state)? < 3 {
        return Ok(Vec::new());
    }
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
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

    // Relation ids are local to one DP invocation. Canonicalize atom order
    // before either building or reading the cache so an equivalent
    // associative tree cannot map a cached relation-set id onto a different
    // native child. The uniqueness check below still rejects repeated table
    // bindings; table-index order is therefore a total order here.
    input
        .atoms
        .sort_unstable_by(|left, right| left.tables.cmp(&right.tables));

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
        let stats = native_relation_stats(atom, memo, facts, state, &mut column_stats);
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
    let graph = if let Some(key) = cache_key {
        let cached = {
            let mut cache = state
                .join_region_cache
                .lock()
                .expect("join-region cache poisoned");
            let cached = cache.entries.get(key).cloned();
            if cached.is_some() {
                cache.hits = cache.hits.saturating_add(1);
            }
            cached
        };
        if let Some(graph) = cached {
            graph
        } else {
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
            let graph = Arc::new(graph);
            let mut cache = state
                .join_region_cache
                .lock()
                .expect("join-region cache poisoned");
            if let Some(existing) = cache.entries.get(key).cloned() {
                cache.hits = cache.hits.saturating_add(1);
                existing
            } else {
                cache.entries.insert(key.clone(), Arc::clone(&graph));
                cache.builds = cache.builds.saturating_add(1);
                graph
            }
        }
    } else {
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
        Arc::new(graph)
    };

    let mut shells = Vec::with_capacity(graph.final_plans.len());
    for plan in &graph.final_plans {
        let mut nodes = shell.nodes.to_vec();
        let mut used_filters = HashSet::new();
        let root_child = rebuild_native_join(
            plan,
            &input.atoms,
            &graph.filter_infos,
            &mut used_filters,
            &mut nodes,
            state,
        )?;
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
                source_proofs: Box::new([]),
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
            // Positional projection indices belong to the input namespace,
            // not the already projected output of this Filter. A narrowing
            // filter is a region boundary, even if its output is contiguous.
            if !at_root
                && !filter
                    .projection_map
                    .is_identity(native_child_layout(&filter.child, layouts)?.len())
            {
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
                && !crate::rewrite::expr::comparison_join_has_evaluation_fence(join)
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
    memo: &Memo,
    facts: &boundary::BoundarySnapshot,
    state: &PlannerTransformState,
    column_stats: &mut HashMap<paro_planner::operator::ColumnBinding, Arc<ColumnStatistics>>,
) -> RelationStats {
    // Memo holes are read from the same resolved snapshot used by the graph
    // cache identity and its ReadSet. A stale transport annotation may not
    // override the relation owner, nor substitute for an unknown owner fact.
    let estimate = match &atom.child {
        NativeChild::MemoGroup { group, .. } => facts.cardinality(memo, *group),
        _ if atom.stats.cardinality_provenance != CardinalityProvenance::Unknown => {
            atom.stats.estimated_cardinality
        }
        _ => None,
    };
    let cardinality = estimate.map_or_else(
        || crate::estimate::gathering::default_table_cardinality(state.session.as_deref()),
        |estimate| usize::try_from(estimate.expected).unwrap_or(usize::MAX),
    );
    let mut stats = RelationStats::with_cardinality(cardinality);
    stats.cardinality_provenance = if estimate.is_some() {
        CardinalityProvenance::Statistics
    } else {
        CardinalityProvenance::Unknown
    };
    stats.risk_cardinality = estimate
        .map(|rows| usize::try_from(rows.max).unwrap_or(usize::MAX))
        .unwrap_or(cardinality)
        .max(cardinality);
    // A locally constructed atom can carry a separate materialization-risk
    // witness. Preserve it; only a Memo hole replaces transport annotations
    // with the authoritative boundary snapshot.
    if !matches!(atom.child, NativeChild::MemoGroup { .. }) {
        if let Some(risk) = atom.stats.materialization_risk_cardinality {
            stats.risk_cardinality = stats
                .risk_cardinality
                .max(usize::try_from(risk).unwrap_or(usize::MAX));
        }
    }
    stats.materialization_cardinality = stats.risk_cardinality;
    stats.estimated_payload_width =
        crate::cost::join_layout::estimate_row_payload_width(atom.layout.types());
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
    node: &crate::region::join::cost_model::DPJoinNode,
    atoms: &[NativeJoinAtom],
    filters: &[Arc<FilterInfo>],
    used_filters: &mut HashSet<usize>,
    nodes: &mut Vec<NativeNode>,
    state: &PlannerTransformState,
) -> Result<NativeChild> {
    if node.is_leaf {
        let child = atoms
            .get(node.set.relations()[0])
            .map(|atom| atom.child.clone())
            .ok_or_else(|| paro_error::internal("native join leaf is out of range"))?;
        return Ok(attach_native_filters(
            child,
            &node.set,
            filters,
            used_filters,
            nodes,
            state,
        ));
    }
    let left = rebuild_native_join(
        node.left_plan
            .as_deref()
            .ok_or_else(|| paro_error::internal("native join lost left frontier child"))?,
        atoms,
        filters,
        used_filters,
        nodes,
        state,
    )?;
    let right = rebuild_native_join(
        node.right_plan
            .as_deref()
            .ok_or_else(|| paro_error::internal("native join lost right frontier child"))?,
        atoms,
        filters,
        used_filters,
        nodes,
        state,
    )?;
    let flip_for_build = node.build_side == crate::cost::join_layout::JoinBuildSide::Left;
    let (left, right) = if flip_for_build {
        (right, left)
    } else {
        (left, right)
    };
    let mut conditions = Vec::new();
    if let Some(predicates) = &node.predicates {
        for predicate in predicates.predicates() {
            let filter = predicate.filter();
            let append = |comparison: &paro_planner::expression::ComparisonExpression,
                          conditions: &mut Vec<JoinCondition>| {
                let Some(orientation) = predicate.orientation() else {
                    return false;
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
                true
            };
            // A join predicate may carry a conjunction with a residual
            // expression that the native join operator cannot represent.  Do
            // not consume the filter unless every conjunct was translated;
            // otherwise the residual would disappear when root_filters are
            // assembled below.  The conservative fallback keeps the complete
            // original filter at its enclosing node.
            let mut translated = Vec::new();
            let fully_translated = match &filter.filter {
                Expression::Comparison(comparison) => append(comparison, &mut translated),
                Expression::Conjunction(conjunction) if !conjunction.children.is_empty() => {
                    conjunction.children.iter().all(|child| {
                        let Expression::Comparison(comparison) = child else {
                            return false;
                        };
                        append(comparison, &mut translated)
                    })
                }
                _ => false,
            };
            if fully_translated && !translated.is_empty() {
                conditions.extend(translated);
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
    let index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        // DP estimates include every applicable predicate, including filters
        // attached below. They are not exact facts for the bare join. Native
        // staging derives facts from the constructed operator and its children.
        stats: NodeStats::default(),
        operator,
        source_proofs: Box::new([]),
    });
    Ok(attach_native_filters(
        NativeChild::Node(index),
        &node.set,
        filters,
        used_filters,
        nodes,
        state,
    ))
}

/// Mirror the graph's predicate consumption at the earliest complete support.
/// In particular, DP prices relation-local filters at leaves. Reattaching them
/// only at the region root would execute unfiltered builds using filtered
/// cardinalities, and can turn a selective join into a quota-exhausting plan.
fn attach_native_filters(
    child: NativeChild,
    relations: &crate::region::join::relation::JoinRelationSet,
    filters: &[Arc<FilterInfo>],
    used: &mut HashSet<usize>,
    nodes: &mut Vec<NativeNode>,
    state: &PlannerTransformState,
) -> NativeChild {
    let expressions = filters
        .iter()
        .filter_map(|filter| {
            if filter.join_type() == JoinType::Inner
                && filter.set.count() != 0
                && relations.contains_all(&filter.set)
                && used.insert(filter.filter_index)
            {
                Some(filter.filter.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if expressions.is_empty() {
        child
    } else {
        let index = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                expressions,
                child,
                projection_map: ProjectionMap::all(),
            }),
            source_proofs: Box::new([]),
        });
        NativeChild::Node(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{ComparisonJoin, Get, JoinCondition};
    use paro_planner::plan::CardinalityEstimate;

    #[test]
    fn reconstruction_consumes_local_and_multirelation_filters_at_first_support() {
        use crate::region::join::relation::JoinRelationSet;
        use paro_planner::expression::{
            ConjunctionExpression, ConjunctionType, ConstantExpression,
        };
        let input =
            MemoBuilder::build(scan(0), BindContext::new(), SearchBudget::default()).unwrap();
        let state = input.planner_state.read().unwrap();
        let column = |table| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::BigInt).into(),
            )
        };
        let local = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                column(0),
                Expression::Constant(
                    ConstantExpression::new(
                        paro_common::runtime_value::Value::BigInt(1),
                        LogicalType::BigInt,
                    )
                    .into(),
                ),
            )
            .into(),
        );
        let residual = Expression::Conjunction(
            ConjunctionExpression::new(
                ConjunctionType::Or,
                vec![
                    Expression::Comparison(
                        ComparisonExpression::new(ComparisonType::Equal, column(0), column(2))
                            .into(),
                    ),
                    local.clone(),
                ],
            )
            .into(),
        );
        let set = |relations: Vec<usize>| Arc::new(JoinRelationSet::new(relations));
        let filters = vec![
            Arc::new(FilterInfo::new_inner(local.clone(), set(vec![0]), 0)),
            Arc::new(FilterInfo::new_inner(residual.clone(), set(vec![0, 2]), 1)),
        ];
        // Leaf placeholders are sufficient here: this routine is forbidden
        // to inspect/expand a child's Memo group to discover predicate support.
        let mut nodes = Vec::new();
        let mut used = HashSet::new();
        let leaf = attach_native_filters(
            NativeChild::Node(99),
            &set(vec![0]),
            &filters,
            &mut used,
            &mut nodes,
            &state,
        );
        assert!(matches!(leaf, NativeChild::Node(0)));
        assert_eq!(used, HashSet::from([0]));
        let LogicalOperator::Filter(filter) = &nodes[0].operator else {
            panic!("leaf filter lost")
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(filter.expressions[0].equals(&local));
        assert!(nodes[0].stats.estimated_cardinality.is_none());
        let partial = attach_native_filters(
            NativeChild::Node(98),
            &set(vec![0, 1]),
            &filters,
            &mut used,
            &mut nodes,
            &state,
        );
        assert!(matches!(partial, NativeChild::Node(98)));
        assert_eq!(nodes.len(), 1, "incomplete support must not consume OR");
        let root = attach_native_filters(
            NativeChild::Node(97),
            &set(vec![0, 1, 2]),
            &filters,
            &mut used,
            &mut nodes,
            &state,
        );
        assert!(matches!(root, NativeChild::Node(1)));
        let LogicalOperator::Filter(filter) = &nodes[1].operator else {
            panic!("residual lost")
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(filter.expressions[0].equals(&residual));
        assert_eq!(used, HashSet::from([0, 1]));
        attach_native_filters(
            root,
            &set(vec![0, 1, 2]),
            &filters,
            &mut used,
            &mut nodes,
            &state,
        );
        assert_eq!(nodes.len(), 2, "each predicate is consumed once");
    }

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
    fn memo_atom_uses_owner_evidence_not_a_stale_shell_or_one_row_default() {
        for known in [false, true] {
            let plan = scan(0);
            let layout = Arc::new(plan.output_layout());
            let mut input =
                MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
            input.memo.group_mut(input.root).unwrap().cardinality = if known {
                GroupCardinality::new(
                    Fingerprint(991),
                    CardinalityRecipeKind::Statistics,
                    271,
                    271,
                    271,
                )
            } else {
                GroupCardinality::unknown(Fingerprint(991), CardinalityRecipeKind::Statistics)
            };
            let state = input.planner_state.read().unwrap();
            let mut ctx = TransformContext::new(&mut input.memo, input.root);
            let facts = boundary::BoundarySnapshot::read(
                &mut ctx,
                &state,
                &PatternOperand::Group(input.root),
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .unwrap();
            let child = NativeChild::memo_group(
                ctx.memo(),
                &state,
                &facts,
                input.root,
                &layout,
                Arc::from(["k".to_owned()]),
            )
            .unwrap();
            let atom = NativeJoinAtom {
                child,
                layout: layout.as_ref().clone(),
                tables: [0].into_iter().collect(),
                stats: NodeStats {
                    estimated_cardinality: Some(CardinalityEstimate::exact(1)),
                    ..Default::default()
                },
            };
            let stats =
                native_relation_stats(&atom, ctx.memo(), &facts, &state, &mut HashMap::new());
            assert_eq!(stats.cardinality, if known { 271 } else { 1000 });
            assert_eq!(
                stats.cardinality_provenance,
                if known {
                    CardinalityProvenance::Statistics
                } else {
                    CardinalityProvenance::Unknown
                }
            );
        }
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

        // The DP result, rather than the reconstructed shell, is the shared
        // value. A second request must reuse that immutable graph while still
        // allocating fresh native node identities for the caller.
        let second = try_native_enumeration(&binding.root, context.memo(), &state, &facts).unwrap();
        assert_eq!(second.len(), 1);
        let cache = state.join_region_cache.lock().unwrap();
        assert_eq!(cache.builds, 1);
        assert_eq!(cache.hits, 1);
    }
}
