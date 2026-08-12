// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reuse an outer detail stream for a correlated full-partition aggregate.
//!
//! A decorrelated scalar aggregate normally scans its correlated source a
//! second time and attaches one result with a `Single` delim join. This pass
//! removes that duplicate graph only when the detail stream contains an exact
//! alpha-equivalent copy of the aggregate source. Extra detail relations must
//! be joined through a declared key, proving that they neither duplicate nor
//! discard an aggregate-source row that reaches the detail stream.

use std::collections::{HashMap, HashSet};

use paro_catalog::entry::ConstraintType;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, Expression, ExpressionIterator,
    ExpressionVisitDecision, WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType,
};
use paro_planner::operator::{
    AntiJoinMode, ColumnBinding, ComparisonJoin, Get, Join, JoinComparisonType, JoinType,
    LogicalOperator, MarkJoinSemantics, Window,
};
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

use crate::aggregate::semantic_kernels::{cast_kernels_equal, scalar_kernels_equal};

/// Rewrite eligible correlated scalar aggregates into full-partition windows.
pub struct CorrelatedPartitionAggregate {
    bind_context: BindContext,
}

impl CorrelatedPartitionAggregate {
    pub fn new(bind_context: BindContext) -> Self {
        Self { bind_context }
    }

    pub fn optimize_plan(&mut self, mut plan: LogicalPlan) -> LogicalPlan {
        let mut uses = BindingUseCounter::default();
        uses.visit_logical_plan(&mut plan);
        self.optimize_node(plan, false, &uses.uses)
    }

    fn optimize_node(
        &self,
        plan: LogicalPlan,
        output_hidden_by_projection: bool,
        binding_uses: &HashMap<ColumnBinding, usize>,
    ) -> LogicalPlan {
        let hides_child_bindings =
            output_hidden_by_projection || matches!(&plan.operator, LogicalOperator::Projection(_));
        let plan = plan
            .map_children(|child| self.optimize_node(child, hides_child_bindings, binding_uses));
        self.rewrite_filter(plan, output_hidden_by_projection, binding_uses)
    }

    fn rewrite_filter(
        &self,
        plan: LogicalPlan,
        output_hidden_by_projection: bool,
        binding_uses: &HashMap<ColumnBinding, usize>,
    ) -> LogicalPlan {
        let Some(rewrite) = recognize_filter(&plan, output_hidden_by_projection, binding_uses)
        else {
            return plan;
        };
        let fallback = paro_planner::binder::deep_copy::duplicate_plan_preserving_indices(
            &plan,
            self.bind_context.shared().as_ref(),
        );
        apply_rewrite(plan, rewrite, &self.bind_context).unwrap_or(fallback)
    }
}

#[derive(Default)]
struct BindingUseCounter {
    uses: HashMap<ColumnBinding, usize>,
}

impl LogicalOperatorVisitor for BindingUseCounter {
    fn visit_replace_column_ref(&mut self, column: &mut ColumnRefExpression) -> Option<Expression> {
        *self.uses.entry(column.binding).or_default() += 1;
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RelationId(usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableIdentity {
    catalog: String,
    schema: String,
    object_id: paro_catalog::entry::CatalogObjectId,
}

struct Relation<'a> {
    id: RelationId,
    get: &'a Get,
    filters: Vec<&'a Expression>,
}

struct EquiEdge<'a> {
    left: &'a Expression,
    right: &'a Expression,
    comparison: JoinComparisonType,
}

struct JoinGraph<'a> {
    relations: Vec<Relation<'a>>,
    edges: Vec<EquiEdge<'a>>,
    binding_relations: HashMap<ColumnBinding, RelationId>,
}

#[derive(Default)]
struct BindingMap {
    inner_to_outer: HashMap<ColumnBinding, ColumnBinding>,
    outer_to_inner: HashMap<ColumnBinding, ColumnBinding>,
}

impl BindingMap {
    fn bind(&mut self, inner: ColumnBinding, outer: ColumnBinding) -> bool {
        match (
            self.inner_to_outer.get(&inner),
            self.outer_to_inner.get(&outer),
        ) {
            (Some(existing), _) => *existing == outer,
            (_, Some(existing)) => *existing == inner,
            (None, None) => {
                self.inner_to_outer.insert(inner, outer);
                self.outer_to_inner.insert(outer, inner);
                true
            }
        }
    }

    fn outer(&self, inner: ColumnBinding) -> Option<ColumnBinding> {
        self.inner_to_outer.get(&inner).copied()
    }
}

struct Rewrite {
    scalar_binding: ColumnBinding,
    scalar_source_binding: ColumnBinding,
    scalar_expression: Expression,
    aggregate: AggregateExpression,
    partitions: Vec<Expression>,
}

struct ScalarBranch<'a> {
    projection: &'a paro_planner::operator::Projection,
    aggregate: &'a paro_planner::operator::Aggregate,
    aggregate_expression: &'a AggregateExpression,
    scalar_binding: ColumnBinding,
    scalar_expression: &'a Expression,
}

fn recognize_filter(
    plan: &LogicalPlan,
    output_hidden_by_projection: bool,
    binding_uses: &HashMap<ColumnBinding, usize>,
) -> Option<Rewrite> {
    let LogicalOperator::Filter(filter) = &plan.operator else {
        return None;
    };
    if filter.expressions.len() != 1
        || !filter
            .projection_map
            .is_identity(filter.child.types().len())
        || !filter.expressions.iter().all(is_movable)
    {
        return None;
    }
    let LogicalOperator::Join(Join::Comparison(join)) = &filter.child.operator else {
        return None;
    };
    if !canonical_scalar_delim_join(join) {
        return None;
    }
    let scalar = peel_scalar_branch(&join.right)?;
    // Removing the Single join removes its scalar binding. A projection above
    // this subtree is therefore a correctness boundary, not merely a pruning
    // opportunity: it proves that an unreferenced scalar cannot escape as an
    // implicit output column.
    if !output_hidden_by_projection
        || binding_uses.get(&scalar.scalar_binding).copied() != Some(1)
        || !filter
            .expressions
            .iter()
            .any(|expression| expression_mentions_binding(expression, scalar.scalar_binding))
    {
        return None;
    }
    let delim = find_only_delim_get(&scalar.aggregate.child)?;
    if delim.chunk_types.len() != join.duplicate_eliminated_columns.len()
        || scalar.aggregate.groups.len() != delim.chunk_types.len()
        || !validate_delim_binding_contract(join, &scalar, delim)
    {
        return None;
    }
    let correlation = match_correlation_keys(
        &scalar.aggregate.child,
        delim.table_index,
        &join.duplicate_eliminated_columns,
    )?;
    let inner_graph = JoinGraph::extract_correlated(&scalar.aggregate.child, delim.table_index)?;
    let outer_graph = JoinGraph::extract(&join.left)?;
    let (bindings, mapped_relations) = match_common_graph(&inner_graph, &outer_graph)?;
    let mut partitions = Vec::with_capacity(correlation.inner_keys.len());
    for inner_key in &correlation.inner_keys {
        partitions.push(rebase_expression(inner_key, &bindings)?);
    }
    prove_partition_keyed_extension(
        &outer_graph,
        &mapped_relations,
        &partitions,
        &join.duplicate_eliminated_columns,
    )?;
    let aggregate = rebase_aggregate(scalar.aggregate_expression, &bindings)?;
    Some(Rewrite {
        scalar_binding: scalar.scalar_binding,
        scalar_source_binding: ColumnBinding::new(scalar.aggregate.aggregate_index, 0),
        scalar_expression: scalar.scalar_expression.clone(),
        aggregate,
        partitions,
    })
}

fn canonical_scalar_delim_join(join: &ComparisonJoin) -> bool {
    join.join_type == JoinType::Single
        && join.anti_join_mode == AntiJoinMode::Regular
        && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && !join.delim_flipped
        && !join.duplicate_eliminated_columns.is_empty()
        && join
            .left_projection_map
            .is_identity(join.left.types().len())
        && join.right_projection_map.as_columns() == Some(&[0])
        && join.conditions.len() == join.duplicate_eliminated_columns.len()
        && join.conditions.iter().all(|condition| {
            condition.comparison == JoinComparisonType::NotDistinctFrom
                && is_movable(&condition.left)
                && is_movable(&condition.right)
        })
}

fn peel_scalar_branch(plan: &LogicalPlan) -> Option<ScalarBranch<'_>> {
    let LogicalOperator::Projection(projection) = &plan.operator else {
        return None;
    };
    if projection.expressions.len() != 2
        || projection.returned_types.len() != 2
        || projection.output_names.len() != 2
    {
        return None;
    }
    let LogicalOperator::Aggregate(aggregate) = &projection.child.operator else {
        return None;
    };
    if aggregate.groups.is_empty()
        || !(aggregate.grouping_sets.is_empty()
            || (aggregate.grouping_sets.len() == 1
                && aggregate.grouping_sets[0].expressions.as_slice() == [0]))
        || !aggregate.grouping_functions.is_empty()
        || aggregate.aggregates.len() != 1
        || aggregate.post_reduction.is_some()
    {
        return None;
    }
    let Expression::Aggregate(aggregate_expression) = &aggregate.aggregates[0] else {
        return None;
    };
    if aggregate_expression.aggr_type != AggregateType::NonDistinct
        // The partition-window spill path can discard a partially built hash
        // table after replaying its stable input payload. Aggregate states
        // with explicit destruction cannot participate until state teardown
        // itself is allocation-free and fallible destruction is surfaced.
        || aggregate_expression.function.destructor.is_some()
        || aggregate_expression.filter.is_some()
        || !aggregate_expression.order_bys.is_empty()
        || !is_movable(&aggregate.aggregates[0])
        || !is_movable(&projection.expressions[0])
    {
        return None;
    }
    let Expression::ColumnRef(group_output) = &projection.expressions[1] else {
        return None;
    };
    if group_output.depth != 0
        || group_output.binding != ColumnBinding::new(aggregate.group_index, 0)
        || aggregate.groups.len() != 1
        || !expression_uses_only_binding(
            &projection.expressions[0],
            ColumnBinding::new(aggregate.aggregate_index, 0),
        )
    {
        return None;
    }
    Some(ScalarBranch {
        projection,
        aggregate,
        aggregate_expression,
        scalar_binding: ColumnBinding::new(projection.table_index, 0),
        scalar_expression: &projection.expressions[0],
    })
}

fn validate_delim_binding_contract(
    join: &ComparisonJoin,
    scalar: &ScalarBranch<'_>,
    delim: &paro_planner::operator::DelimGet,
) -> bool {
    let key_count = join.duplicate_eliminated_columns.len();
    if scalar.aggregate.groups.len() != key_count
        || scalar.projection_group_count() != key_count
        || join.conditions.len() != key_count
    {
        return false;
    }
    for ordinal in 0..key_count {
        let expected_delim = ColumnBinding::new(delim.table_index, ordinal);
        if !matches!(
            scalar.aggregate.groups.get(ordinal),
            Some(Expression::ColumnRef(column))
                if column.depth == 0
                    && column.binding == expected_delim
                    && delim.chunk_types.get(ordinal) == Some(&column.return_type)
        ) {
            return false;
        }
        let expected_group_output = ColumnBinding::new(scalar.aggregate.group_index, ordinal);
        let projection_expression = match ordinal.checked_add(1) {
            Some(index) => scalar.projection_expression(index),
            None => return false,
        };
        if !matches!(projection_expression, Some(Expression::ColumnRef(column))
            if column.depth == 0
                && column.binding == expected_group_output
                && scalar.aggregate.groups[ordinal].return_type() == column.return_type)
        {
            return false;
        }
        let Some(condition) = join.conditions.get(ordinal) else {
            return false;
        };
        let expected_rhs = ColumnBinding::new(scalar.projection_table_index(), ordinal + 1);
        let matches = |outer: &Expression, right: &Expression| {
            same_column_expression(outer, &join.duplicate_eliminated_columns[ordinal])
                && matches!(right, Expression::ColumnRef(column)
                    if column.depth == 0
                        && column.binding == expected_rhs
                        && projection_expression.is_some_and(|projected|
                            projected.return_type() == column.return_type))
        };
        if !(matches(&condition.left, &condition.right)
            || matches(&condition.right, &condition.left))
        {
            return false;
        }
    }
    true
}

impl ScalarBranch<'_> {
    fn projection_table_index(&self) -> usize {
        self.scalar_binding.table_index
    }

    fn projection_group_count(&self) -> usize {
        // The scalar value is the first projection output; all remaining
        // outputs carry delimiter-group keys in ordinal order.
        self.projection.expressions.len().saturating_sub(1)
    }

    fn projection_expression(&self, index: usize) -> Option<&Expression> {
        self.projection.expressions.get(index)
    }
}

struct Correlation {
    inner_keys: Vec<Expression>,
}

fn match_correlation_keys(
    plan: &LogicalPlan,
    delim_table_index: usize,
    outer_keys: &[Expression],
) -> Option<Correlation> {
    let mut inner_keys = Vec::new();
    collect_correlation_keys(plan, delim_table_index, outer_keys.len(), &mut inner_keys)?;
    if inner_keys.len() != outer_keys.len() {
        return None;
    }
    inner_keys.sort_by_key(|(ordinal, _)| *ordinal);
    Some(Correlation {
        inner_keys: inner_keys.into_iter().map(|(_, key)| key).collect(),
    })
}

fn collect_correlation_keys(
    plan: &LogicalPlan,
    delim_table_index: usize,
    key_count: usize,
    keys: &mut Vec<(usize, Expression)>,
) -> Option<()> {
    match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) if clean_inner_join(join) => {
            for condition in &join.conditions {
                let left_delim = direct_delim_column(&condition.left, delim_table_index);
                let right_delim = direct_delim_column(&condition.right, delim_table_index);
                if left_delim.is_some() || right_delim.is_some() {
                    if condition.comparison != JoinComparisonType::Equal
                        || left_delim.is_some() == right_delim.is_some()
                    {
                        return None;
                    }
                    let (delim, inner) = if let Some(delim) = left_delim {
                        (delim, &condition.right)
                    } else {
                        (right_delim?, &condition.left)
                    };
                    if delim >= key_count
                        || expression_references_table(inner, delim_table_index)
                        || keys.iter().any(|(ordinal, _)| *ordinal == delim)
                    {
                        return None;
                    }
                    keys.push((delim, inner.clone()));
                } else if expression_references_table(&condition.left, delim_table_index)
                    || expression_references_table(&condition.right, delim_table_index)
                {
                    return None;
                }
            }
        }
        LogicalOperator::Filter(filter) => {
            if filter
                .expressions
                .iter()
                .any(|expression| expression_references_table(expression, delim_table_index))
            {
                return None;
            }
        }
        LogicalOperator::Get(_)
        | LogicalOperator::DelimGet(_)
        | LogicalOperator::Join(Join::Cross(_)) => {}
        _ => return None,
    }
    for child in plan.children() {
        collect_correlation_keys(child, delim_table_index, key_count, keys)?;
    }
    Some(())
}

fn direct_delim_column(expression: &Expression, table_index: usize) -> Option<usize> {
    match expression {
        Expression::ColumnRef(column)
            if column.depth == 0 && column.binding.table_index == table_index =>
        {
            Some(column.binding.column_index)
        }
        _ => None,
    }
}

impl<'a> JoinGraph<'a> {
    fn extract(plan: &'a LogicalPlan) -> Option<Self> {
        Self::extract_internal(plan, None)
    }

    fn extract_correlated(plan: &'a LogicalPlan, delim_table_index: usize) -> Option<Self> {
        Self::extract_internal(plan, Some(delim_table_index))
    }

    fn extract_internal(plan: &'a LogicalPlan, ignored_delim: Option<usize>) -> Option<Self> {
        let mut graph = Self {
            relations: Vec::new(),
            edges: Vec::new(),
            binding_relations: HashMap::new(),
        };
        graph.extract_node(plan, Vec::new(), ignored_delim)?;
        (!graph.relations.is_empty()).then_some(graph)
    }

    fn extract_node(
        &mut self,
        plan: &'a LogicalPlan,
        mut pending_filters: Vec<&'a Expression>,
        ignored_delim: Option<usize>,
    ) -> Option<HashSet<RelationId>> {
        match &plan.operator {
            LogicalOperator::Get(get) => {
                if get.table.is_none()
                    || get.scan_order.is_some()
                    || !get.runtime_filter_expressions.iter().all(is_movable)
                    || get.column_ids.len() != get.column_types.len()
                    || get.column_ids.len() != get.returned_types.len()
                    || !pending_filters.iter().all(|filter| is_movable(filter))
                {
                    return None;
                }
                let id = RelationId(self.relations.len());
                for column_index in 0..get.column_ids.len() {
                    self.binding_relations
                        .insert(ColumnBinding::new(get.table_index, column_index), id);
                }
                self.relations.push(Relation {
                    id,
                    get,
                    filters: std::mem::take(&mut pending_filters),
                });
                Some(HashSet::from([id]))
            }
            LogicalOperator::DelimGet(delim) if ignored_delim == Some(delim.table_index) => {
                Some(HashSet::new())
            }
            LogicalOperator::Filter(filter)
                if filter
                    .projection_map
                    .is_identity(filter.child.types().len()) =>
            {
                pending_filters.extend(&filter.expressions);
                let relations = self.extract_node(&filter.child, pending_filters, ignored_delim)?;
                // A pushed local filter must belong to exactly one scan. Mixed
                // predicates are represented as join conditions before this pass.
                (relations.len() == 1).then_some(relations)
            }
            LogicalOperator::Join(Join::Comparison(join)) if clean_inner_join(join) => {
                if !pending_filters.is_empty() {
                    return None;
                }
                let left = self.extract_node(&join.left, Vec::new(), ignored_delim)?;
                let right = self.extract_node(&join.right, Vec::new(), ignored_delim)?;
                if !left.is_disjoint(&right) {
                    return None;
                }
                for condition in &join.conditions {
                    if ignored_delim.is_some_and(|table_index| {
                        expression_references_table(&condition.left, table_index)
                            || expression_references_table(&condition.right, table_index)
                    }) {
                        continue;
                    }
                    let left_relation =
                        single_expression_relation(&condition.left, &self.binding_relations)?;
                    let right_relation =
                        single_expression_relation(&condition.right, &self.binding_relations)?;
                    if left_relation == right_relation
                        || !((left.contains(&left_relation) && right.contains(&right_relation))
                            || (left.contains(&right_relation) && right.contains(&left_relation)))
                    {
                        return None;
                    }
                    self.edges.push(EquiEdge {
                        left: &condition.left,
                        right: &condition.right,
                        comparison: condition.comparison,
                    });
                }
                Some(left.union(&right).copied().collect())
            }
            LogicalOperator::Join(Join::Cross(cross)) => {
                if !pending_filters.is_empty() {
                    return None;
                }
                let left = self.extract_node(&cross.left, Vec::new(), ignored_delim)?;
                let right = self.extract_node(&cross.right, Vec::new(), ignored_delim)?;
                if !left.is_disjoint(&right) {
                    return None;
                }
                Some(left.union(&right).copied().collect())
            }
            _ => None,
        }
    }
}

fn match_common_graph(
    inner: &JoinGraph<'_>,
    outer: &JoinGraph<'_>,
) -> Option<(BindingMap, HashSet<RelationId>)> {
    if inner.relations.len() > outer.relations.len() {
        return None;
    }
    let mut candidates = Vec::with_capacity(inner.relations.len());
    for inner_relation in &inner.relations {
        let identity = table_identity(inner_relation.get)?;
        let matches = outer
            .relations
            .iter()
            .filter(|outer_relation| table_identity(outer_relation.get).as_ref() == Some(&identity))
            .map(|relation| relation.id)
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return None;
        }
        candidates.push(matches);
    }
    let mut order = (0..inner.relations.len()).collect::<Vec<_>>();
    order.sort_by_key(|index| candidates[*index].len());
    let mut assignment = vec![None; inner.relations.len()];
    let mut used = HashSet::new();
    find_graph_embedding(
        0,
        &order,
        &candidates,
        &mut assignment,
        &mut used,
        inner,
        outer,
    )
}

fn find_graph_embedding(
    depth: usize,
    order: &[usize],
    candidates: &[Vec<RelationId>],
    assignment: &mut [Option<RelationId>],
    used: &mut HashSet<RelationId>,
    inner: &JoinGraph<'_>,
    outer: &JoinGraph<'_>,
) -> Option<(BindingMap, HashSet<RelationId>)> {
    if depth == order.len() {
        let mut bindings = BindingMap::default();
        for (inner_idx, outer_id) in assignment.iter().enumerate() {
            let outer_id = (*outer_id)?;
            bind_scan_columns(
                inner.relations.get(inner_idx)?.get,
                outer.relations.get(outer_id.0)?.get,
                &mut bindings,
            )?;
        }
        graph_semantics_equal(inner, outer, assignment, &bindings).then(|| (bindings, used.clone()))
    } else {
        let inner_idx = order[depth];
        for outer_id in &candidates[inner_idx] {
            if !used.insert(*outer_id) {
                continue;
            }
            assignment[inner_idx] = Some(*outer_id);
            if let Some(result) =
                find_graph_embedding(depth + 1, order, candidates, assignment, used, inner, outer)
            {
                return Some(result);
            }
            assignment[inner_idx] = None;
            used.remove(outer_id);
        }
        None
    }
}

fn graph_semantics_equal(
    inner: &JoinGraph<'_>,
    outer: &JoinGraph<'_>,
    assignment: &[Option<RelationId>],
    bindings: &BindingMap,
) -> bool {
    for (inner_idx, outer_id) in assignment.iter().enumerate() {
        let Some(outer_relation) = outer_id.and_then(|id| outer.relations.get(id.0)) else {
            return false;
        };
        if !unordered_expressions_equal(
            &inner.relations[inner_idx].filters,
            &outer_relation.filters,
            bindings,
        ) {
            return false;
        }
    }
    let mapped_ids = assignment.iter().flatten().copied().collect::<HashSet<_>>();
    let outer_common_edges = outer
        .edges
        .iter()
        .filter(|edge| {
            edge_relations(edge, &outer.binding_relations).is_some_and(|(left, right)| {
                mapped_ids.contains(&left) && mapped_ids.contains(&right)
            })
        })
        .collect::<Vec<_>>();
    if inner.edges.len() != outer_common_edges.len() {
        return false;
    }
    let mut matched = vec![false; outer_common_edges.len()];
    inner.edges.iter().all(|inner_edge| {
        outer_common_edges
            .iter()
            .enumerate()
            .position(|(idx, outer_edge)| {
                !matched[idx] && edge_semantics_equal(inner_edge, outer_edge, bindings)
            })
            .is_some_and(|idx| {
                matched[idx] = true;
                true
            })
    })
}

/// Prove that the only extra relation is a lookup keyed by the complete
/// partition tuple. Merely proving `dimension.key` unique is insufficient:
/// a detail-side filter on a dimension selected by some other common column
/// could remove only part of a partition and change the aggregate input.
///
/// Requiring one ordinary `=` edge for every partition component also proves
/// that every row reaching the Window has a non-NULL partition key. That is a
/// semantic requirement: the original correlated `inner.key = outer.key`
/// sees an empty input for an outer NULL, while SQL grouping would otherwise
/// combine all NULL keys into one partition.
fn prove_partition_keyed_extension(
    outer: &JoinGraph<'_>,
    common: &HashSet<RelationId>,
    partitions: &[Expression],
    capture_keys: &[Expression],
) -> Option<()> {
    if capture_keys.len() != partitions.len() {
        return None;
    }
    let dimensions = outer
        .relations
        .iter()
        .map(|relation| relation.id)
        .filter(|id| !common.contains(id))
        .collect::<Vec<_>>();
    let [dimension] = dimensions.as_slice() else {
        return None;
    };
    let relation = outer.relations.get(dimension.0)?;
    let mut equality_columns = HashSet::new();
    let mut key_pairs = Vec::new();
    for edge in &outer.edges {
        let (left_relation, right_relation) = edge_relations(edge, &outer.binding_relations)?;
        let incident = left_relation == *dimension || right_relation == *dimension;
        if !incident {
            continue;
        }
        // Every predicate incident on the lookup relation is part of the
        // proof. Silently ignoring an additional NDF/equality predicate would
        // allow it to discard only a subset of a partition.
        if edge.comparison != JoinComparisonType::Equal {
            return None;
        }
        let (dimension_expression, common_expression) =
            if left_relation == *dimension && common.contains(&right_relation) {
                (edge.left, edge.right)
            } else if right_relation == *dimension && common.contains(&left_relation) {
                (edge.right, edge.left)
            } else {
                return None;
            };
        let Expression::ColumnRef(column) = dimension_expression else {
            return None;
        };
        if column.depth != 0 || column.binding.table_index != relation.get.table_index {
            return None;
        }
        equality_columns.insert(*relation.get.column_ids.get(column.binding.column_index)?);
        key_pairs.push((common_expression, dimension_expression));
    }
    if key_pairs.len() != partitions.len() {
        return None;
    }
    let mut matched = vec![false; partitions.len()];
    if !key_pairs
        .iter()
        .all(|(common_expression, dimension_expression)| {
            partitions
                .iter()
                .enumerate()
                .position(|(idx, partition)| {
                    !matched[idx]
                        && same_column_expression(common_expression, partition)
                        && (same_column_expression(&capture_keys[idx], dimension_expression)
                            || same_column_expression(&capture_keys[idx], partition))
                })
                .is_some_and(|idx| {
                    matched[idx] = true;
                    true
                })
        })
    {
        return None;
    }
    relation
        .get
        .table
        .as_ref()
        .is_some_and(|table| {
            table.constraints().iter().any(|constraint| {
                matches!(
                    constraint.constraint_type,
                    ConstraintType::Unique | ConstraintType::PrimaryKey
                ) && !constraint.columns.is_empty()
                    && constraint
                        .columns
                        .iter()
                        .all(|column| equality_columns.contains(column))
            })
        })
        .then_some(())
}

fn same_column_expression(left: &Expression, right: &Expression) -> bool {
    matches!((left, right), (Expression::ColumnRef(left), Expression::ColumnRef(right))
        if left.depth == 0
            && right.depth == 0
            && left.binding == right.binding
            && left.return_type == right.return_type)
}

fn apply_rewrite(
    plan: LogicalPlan,
    rewrite: Rewrite,
    bind_context: &BindContext,
) -> Option<LogicalPlan> {
    let LogicalOperator::Filter(mut filter) = plan.operator else {
        return None;
    };
    let LogicalOperator::Join(Join::Comparison(mut join)) = filter.child.operator else {
        return None;
    };
    let detail = std::mem::replace(
        &mut *join.left,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    let window_index = bind_context.generate_table_index();
    let window_binding = ColumnBinding::new(window_index, 0);
    let window_type = rewrite.aggregate.return_type.clone();
    let frame = WindowFrame {
        frame_type: WindowFrameType::Rows,
        start_bound: WindowFrameBound::Unbounded,
        start_is_preceding: true,
        end_bound: WindowFrameBound::Unbounded,
        end_is_preceding: false,
    };
    let window_expression =
        WindowExpression::aggregate(rewrite.aggregate, rewrite.partitions, Vec::new(), frame);
    window_expression.verify_bound_contract().ok()?;
    let window = LogicalPlan::new(
        bind_context,
        LogicalOperator::Window(Window::new(window_index, vec![window_expression], detail)),
    );
    let scalar = rewrite.scalar_expression.replace_column_ref(&|column| {
        (column.depth == 0 && column.binding == rewrite.scalar_source_binding).then(|| {
            Expression::ColumnRef(ColumnRefExpression::new(
                window_binding,
                window_type.clone(),
            ))
        })
    });
    if !expression_uses_only_binding(&scalar, window_binding) {
        return None;
    }
    filter.expressions = filter
        .expressions
        .into_iter()
        .map(|expression| {
            expression.replace_column_ref(&|column| {
                (column.binding == rewrite.scalar_binding).then(|| scalar.clone())
            })
        })
        .collect();
    filter.child = Box::new(window);
    filter.projection_map = paro_planner::operator::ProjectionMap::all();
    Some(LogicalPlan::new(
        bind_context,
        LogicalOperator::Filter(filter),
    ))
}

fn table_identity(get: &Get) -> Option<TableIdentity> {
    let table = get.table.as_ref()?;
    Some(TableIdentity {
        catalog: table.base.base.catalog.clone(),
        schema: table.base.schema_name.clone(),
        object_id: table.base.base.object_id,
    })
}

fn bind_scan_columns(inner: &Get, outer: &Get, bindings: &mut BindingMap) -> Option<()> {
    if table_identity(inner)? != table_identity(outer)? {
        return None;
    }
    for (inner_idx, physical_id) in inner.column_ids.iter().enumerate() {
        let outer_idx = outer
            .column_ids
            .iter()
            .position(|candidate| candidate == physical_id)?;
        if inner.column_types.get(inner_idx) != outer.column_types.get(outer_idx)
            || inner.returned_types.get(inner_idx) != outer.returned_types.get(outer_idx)
            || !bindings.bind(
                ColumnBinding::new(inner.table_index, inner_idx),
                ColumnBinding::new(outer.table_index, outer_idx),
            )
        {
            return None;
        }
    }
    Some(())
}

fn unordered_expressions_equal(
    inner: &[&Expression],
    outer: &[&Expression],
    bindings: &BindingMap,
) -> bool {
    if inner.len() != outer.len() {
        return false;
    }
    let mut matched = vec![false; outer.len()];
    inner.iter().all(|inner_expression| {
        outer
            .iter()
            .enumerate()
            .position(|(idx, outer_expression)| {
                !matched[idx]
                    && semantic_expression_equal(inner_expression, outer_expression, bindings)
            })
            .is_some_and(|idx| {
                matched[idx] = true;
                true
            })
    })
}

fn edge_semantics_equal(inner: &EquiEdge<'_>, outer: &EquiEdge<'_>, bindings: &BindingMap) -> bool {
    inner.comparison == outer.comparison
        && ((semantic_expression_equal(inner.left, outer.left, bindings)
            && semantic_expression_equal(inner.right, outer.right, bindings))
            || (semantic_expression_equal(inner.left, outer.right, bindings)
                && semantic_expression_equal(inner.right, outer.left, bindings)))
}

fn semantic_expression_equal(
    inner: &Expression,
    outer: &Expression,
    bindings: &BindingMap,
) -> bool {
    if inner.return_type() != outer.return_type() || !is_movable(inner) || !is_movable(outer) {
        return false;
    }
    match (inner, outer) {
        (Expression::ColumnRef(inner), Expression::ColumnRef(outer)) => {
            inner.depth == 0
                && outer.depth == 0
                && bindings.outer(inner.binding) == Some(outer.binding)
        }
        (Expression::Constant(inner), Expression::Constant(outer)) => inner.value == outer.value,
        (Expression::Function(inner), Expression::Function(outer)) => {
            inner.return_type == outer.return_type
                && inner.routine_meta == outer.routine_meta
                && scalar_kernels_equal(&inner.function, &outer.function)
                && expression_slices_equal(&inner.children, &outer.children, bindings)
        }
        (Expression::Cast(inner), Expression::Cast(outer)) => {
            inner.target_type == outer.target_type
                && inner.try_cast == outer.try_cast
                && cast_kernels_equal(&inner.cast_info, &outer.cast_info)
                && semantic_expression_equal(&inner.child, &outer.child, bindings)
        }
        (Expression::Comparison(inner), Expression::Comparison(outer)) => {
            inner.comparison_type == outer.comparison_type
                && semantic_expression_equal(&inner.left, &outer.left, bindings)
                && semantic_expression_equal(&inner.right, &outer.right, bindings)
        }
        (Expression::Conjunction(inner), Expression::Conjunction(outer)) => {
            inner.conjunction_type == outer.conjunction_type
                && expression_slices_equal(&inner.children, &outer.children, bindings)
        }
        _ => false,
    }
}

fn expression_slices_equal(
    inner: &[Expression],
    outer: &[Expression],
    bindings: &BindingMap,
) -> bool {
    inner.len() == outer.len()
        && inner
            .iter()
            .zip(outer)
            .all(|(inner, outer)| semantic_expression_equal(inner, outer, bindings))
}

fn rebase_aggregate(
    aggregate: &AggregateExpression,
    bindings: &BindingMap,
) -> Option<AggregateExpression> {
    if aggregate.aggr_type != AggregateType::NonDistinct
        || aggregate.filter.is_some()
        || !aggregate.order_bys.is_empty()
    {
        return None;
    }
    let mut rebased = aggregate.clone();
    rebased.children = aggregate
        .children
        .iter()
        .map(|expression| rebase_expression(expression, bindings))
        .collect::<Option<Vec<_>>>()?;
    Some(rebased)
}

fn rebase_expression(expression: &Expression, bindings: &BindingMap) -> Option<Expression> {
    let valid = std::cell::Cell::new(true);
    let rebased = expression.clone().replace_column_ref(&|column| {
        if column.depth != 0 {
            valid.set(false);
            return None;
        }
        let Some(binding) = bindings.outer(column.binding) else {
            valid.set(false);
            return None;
        };
        Some(Expression::ColumnRef(ColumnRefExpression::new(
            binding,
            column.return_type.clone(),
        )))
    });
    (valid.get() && is_movable(&rebased)).then_some(rebased)
}

fn clean_inner_join(join: &ComparisonJoin) -> bool {
    join.join_type == JoinType::Inner
        && join.anti_join_mode == AntiJoinMode::Regular
        && join.mark_index.is_none()
        && join.mark_semantics == MarkJoinSemantics::NotMark
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && join
            .left_projection_map
            .is_identity(join.left.types().len())
        && join
            .right_projection_map
            .is_identity(join.right.types().len())
        && !join.conditions.is_empty()
        && join.conditions.iter().all(|condition| {
            matches!(
                condition.comparison,
                JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
            ) && is_movable(&condition.left)
                && is_movable(&condition.right)
        })
}

fn find_only_delim_get(plan: &LogicalPlan) -> Option<&paro_planner::operator::DelimGet> {
    let mut found = Vec::new();
    collect_delim_gets(plan, &mut found);
    (found.len() == 1).then(|| found[0])
}

fn collect_delim_gets<'a>(
    plan: &'a LogicalPlan,
    found: &mut Vec<&'a paro_planner::operator::DelimGet>,
) {
    if let LogicalOperator::DelimGet(delim) = &plan.operator {
        found.push(delim);
    }
    for child in plan.children() {
        collect_delim_gets(child, found);
    }
}

fn single_expression_relation(
    expression: &Expression,
    relations: &HashMap<ColumnBinding, RelationId>,
) -> Option<RelationId> {
    let mut found = None;
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            let Some(relation) = relations.get(&column.binding).copied() else {
                valid = false;
                return ExpressionVisitDecision::SkipChildren;
            };
            if found.is_some_and(|existing| existing != relation) {
                valid = false;
            } else {
                found = Some(relation);
            }
            ExpressionVisitDecision::SkipChildren
        }
        Expression::Aggregate(_)
        | Expression::Reference(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {
            valid = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    valid.then_some(found?)
}

fn edge_relations(
    edge: &EquiEdge<'_>,
    relations: &HashMap<ColumnBinding, RelationId>,
) -> Option<(RelationId, RelationId)> {
    Some((
        single_expression_relation(edge.left, relations)?,
        single_expression_relation(edge.right, relations)?,
    ))
}

fn expression_mentions_binding(expression: &Expression, binding: ColumnBinding) -> bool {
    let mut found = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if matches!(node, Expression::ColumnRef(column) if column.binding == binding) {
            found = true;
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    found
}

fn expression_references_table(expression: &Expression, table_index: usize) -> bool {
    let mut found = false;
    ExpressionIterator::visit(expression, &mut |node| {
        if matches!(node, Expression::ColumnRef(column)
            if column.binding.table_index == table_index)
        {
            found = true;
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    found
}

fn expression_uses_only_binding(expression: &Expression, binding: ColumnBinding) -> bool {
    let mut saw_binding = false;
    let mut valid = true;
    ExpressionIterator::visit(expression, &mut |node| match node {
        Expression::ColumnRef(column) => {
            saw_binding |= column.depth == 0 && column.binding == binding;
            valid &= column.depth == 0 && column.binding == binding;
            ExpressionVisitDecision::SkipChildren
        }
        Expression::Aggregate(_)
        | Expression::Reference(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {
            valid = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    valid && saw_binding
}

fn is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}
