// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Remove duplicate work from correlated full-partition aggregates.
//!
//! A decorrelated scalar aggregate normally scans its correlated source a
//! second time and attaches one result with a `Single` delim join. This pass
//! either reuses an alpha-equivalent outer detail stream through a partition
//! window, or pulls a null-rejected aggregate into an ordinary grouped join
//! when a declared key proves the preserved correlation tuple is unique.
//! Every rewrite is proof-driven: extra detail relations must be keyed, and
//! unmatched or duplicate-producing cases keep the original delim plan.

use std::collections::{HashMap, HashSet};

use paro_catalog::entry::ConstraintType;
use paro_common::error::{self as paro_error, Result};
use paro_function::aggregate::AggregateEmptyInput;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, AggregateType, ColumnRefExpression, ComparisonType, Expression,
    ExpressionIterator, ExpressionVisitDecision, WindowExpression, WindowFrame, WindowFrameBound,
    WindowFrameType,
};
use paro_planner::operator::{
    Aggregate, AntiJoinMode, ColumnBinding, ComparisonJoin, Get, Join, JoinComparisonType,
    JoinCondition, JoinType, LogicalOperator, MarkJoinSemantics, Projection, Window,
};
use paro_planner::plan::LogicalPlan;

use crate::aggregate::semantic_kernels::{cast_kernels_equal, scalar_kernels_equal};
use crate::statistics::unique_keys::{declared_unique_keys, NullRejectedKeyProof};

/// Rewrite eligible correlated scalar aggregates into partition windows or
/// keyed grouped joins.
pub struct CorrelatedPartitionAggregate {
    bind_context: BindContext,
}

impl CorrelatedPartitionAggregate {
    pub fn new(bind_context: BindContext) -> Self {
        Self { bind_context }
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_node(plan, None)
    }

    fn optimize_node(
        &self,
        plan: LogicalPlan,
        output_contract: Option<OutputContract>,
    ) -> Result<LogicalPlan> {
        let child_contracts = child_output_contracts(&plan.operator, output_contract.as_ref());
        let mut child_ordinal = 0;
        let plan = plan.try_map_children(|child| {
            let contract = child_contracts.get(child_ordinal).cloned().flatten();
            child_ordinal += 1;
            self.optimize_node(child, contract)
        })?;
        self.rewrite_filter(plan, output_contract.as_ref())
    }

    fn rewrite_filter(
        &self,
        plan: LogicalPlan,
        output_contract: Option<&OutputContract>,
    ) -> Result<LogicalPlan> {
        if let Some(rewrite) = recognize_filter(&plan, output_contract) {
            return apply_rewrite(plan, rewrite, &self.bind_context);
        }
        if let Some(rewrite) = recognize_grouped_join_filter(&plan, output_contract) {
            return apply_grouped_join_rewrite(plan, rewrite, &self.bind_context);
        }
        Ok(plan)
    }
}

/// Bindings consumed between a subtree and the projection that hides the rest
/// of its implicit output.  The contract is propagated only across operators
/// whose preserved child is addressed by binding; positional or implicit
/// consumers deliberately terminate it.
#[derive(Clone, Default)]
struct OutputContract {
    referenced: HashSet<ColumnBinding>,
}

impl OutputContract {
    fn from_expressions<'a>(expressions: impl IntoIterator<Item = &'a Expression>) -> Self {
        let mut contract = Self::default();
        contract.extend(expressions);
        contract
    }

    fn extend<'a>(&mut self, expressions: impl IntoIterator<Item = &'a Expression>) {
        for expression in expressions {
            ExpressionIterator::visit(expression, &mut |candidate| {
                if let Expression::ColumnRef(column) = candidate {
                    self.referenced.insert(column.binding);
                }
                ExpressionVisitDecision::Descend
            });
        }
    }

    fn references(&self, binding: ColumnBinding) -> bool {
        self.referenced.contains(&binding)
    }
}

fn child_output_contracts(
    operator: &LogicalOperator,
    inherited: Option<&OutputContract>,
) -> Vec<Option<OutputContract>> {
    match operator {
        LogicalOperator::Projection(projection) => vec![Some(OutputContract::from_expressions(
            projection.expressions.iter(),
        ))],
        LogicalOperator::Aggregate(aggregate) => vec![Some(OutputContract::from_expressions(
            aggregate.groups.iter().chain(aggregate.aggregates.iter()),
        ))],
        LogicalOperator::Filter(filter)
            if inherited.is_some() && filter.projection_map.is_all() =>
        {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(filter.expressions.iter());
            vec![Some(contract)]
        }
        LogicalOperator::Order(order) if inherited.is_some() && order.projection_map.is_all() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(order.orders.iter().map(|order| &order.expression));
            vec![Some(contract)]
        }
        LogicalOperator::Limit(limit) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(limit.limit.iter().chain(limit.offset.iter()));
            vec![Some(contract)]
        }
        LogicalOperator::TopN(topn) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(topn.orders.iter().map(|order| &order.expression));
            vec![Some(contract)]
        }
        LogicalOperator::Join(Join::Comparison(join)) if inherited.is_some() => {
            let mut contract = inherited.cloned().unwrap_or_default();
            contract.extend(
                join.conditions
                    .iter()
                    .flat_map(|condition| [&condition.left, &condition.right])
                    .chain(join.duplicate_eliminated_columns.iter()),
            );
            match join.join_type {
                JoinType::Semi | JoinType::Anti if join.left_projection_map.is_all() => {
                    vec![Some(contract), None]
                }
                JoinType::RightSemi | JoinType::RightAnti if join.right_projection_map.is_all() => {
                    vec![None, Some(contract)]
                }
                _ => vec![None, None],
            }
        }
        _ => operator.children().into_iter().map(|_| None).collect(),
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

struct GroupedJoinRewrite {
    scalar_binding: ColumnBinding,
    scalar_source_binding: ColumnBinding,
    scalar_expression: Expression,
    aggregate: AggregateExpression,
    delim_table_index: usize,
    null_rejection: NullRejectedKeyProof,
    outer_bindings: Vec<ColumnBinding>,
    outer_types: Vec<paro_common::types::LogicalType>,
    group_ordinals: Vec<usize>,
    output_width: usize,
    direct_source_side: DirectSourceSide,
}

struct ScalarBranch<'a> {
    projection: &'a paro_planner::operator::Projection,
    aggregate: &'a paro_planner::operator::Aggregate,
    aggregate_expression: &'a AggregateExpression,
    scalar_binding: ColumnBinding,
    scalar_expression: &'a Expression,
}

struct DelimShape<'a> {
    filter: &'a paro_planner::operator::Filter,
    join: &'a ComparisonJoin,
    scalar: ScalarBranch<'a>,
    delim: &'a paro_planner::operator::DelimGet,
    correlation: Correlation,
}

#[derive(Clone, Copy)]
enum DirectSourceSide {
    LeftDelim,
    RightDelim,
}

fn recognize_delim_shape<'a>(
    plan: &'a LogicalPlan,
    output_contract: Option<&OutputContract>,
) -> Option<DelimShape<'a>> {
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
    // The nearest projection and every operator between it and this filter
    // have explicitly enumerated their binding dependencies.  Only that
    // binding-transparent path proves the scalar is an implementation detail.
    if output_contract?.references(scalar.scalar_binding)
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
    Some(DelimShape {
        filter,
        join,
        scalar,
        delim,
        correlation,
    })
}

fn recognize_filter(
    plan: &LogicalPlan,
    output_contract: Option<&OutputContract>,
) -> Option<Rewrite> {
    let shape = recognize_delim_shape(plan, output_contract)?;
    let inner_graph =
        JoinGraph::extract_correlated(&shape.scalar.aggregate.child, shape.delim.table_index)?;
    let outer_graph = JoinGraph::extract(&shape.join.left)?;
    let (bindings, mapped_relations) = match_common_graph(&inner_graph, &outer_graph)?;
    let mut partitions = Vec::with_capacity(shape.correlation.inner_keys.len());
    for inner_key in &shape.correlation.inner_keys {
        partitions.push(rebase_expression(inner_key, &bindings)?);
    }
    prove_partition_keyed_extension(
        &outer_graph,
        &mapped_relations,
        &partitions,
        &shape.join.duplicate_eliminated_columns,
    )?;
    let aggregate = rebase_aggregate(shape.scalar.aggregate_expression, &bindings)?;
    Some(Rewrite {
        scalar_binding: shape.scalar.scalar_binding,
        scalar_source_binding: ColumnBinding::new(shape.scalar.aggregate.aggregate_index, 0),
        scalar_expression: shape.scalar.scalar_expression.clone(),
        aggregate,
        partitions,
    })
}

/// Pull a null-rejected correlated aggregate into the outer detail stream.
///
/// The canonical delim plan builds the detail rows, captures their unique
/// correlation keys, scans those keys to aggregate the inner relation, then
/// scans them again to attach the scalar.  When the correlation tuple is a
/// declared unique key of the preserved detail relation, the same semantics
/// are represented by one ordinary join followed by a grouped HAVING.  The
/// aggregate reproduces the outer bindings verbatim, so consumers above the
/// hiding projection do not observe an implementation-only binding domain.
fn recognize_grouped_join_filter(
    plan: &LogicalPlan,
    output_contract: Option<&OutputContract>,
) -> Option<GroupedJoinRewrite> {
    let shape = recognize_delim_shape(plan, output_contract)?;
    let output_contract = output_contract?;
    if !filter_rejects_null_scalar(&shape.filter.expressions[0], shape.scalar.scalar_binding) {
        return None;
    }
    // A strict predicate rejects the NULL produced by an empty correlated
    // aggregate. Aggregates with a non-NULL empty-input value (COUNT) cannot
    // replace the original outer-preserving scalar join with INNER.
    if shape.scalar.aggregate_expression.function.empty_input != AggregateEmptyInput::Null {
        return None;
    }
    let direct_source_side = direct_delim_join_source(
        &shape.scalar.aggregate.child,
        shape.delim.table_index,
        shape.correlation.inner_keys.len(),
    )?;
    let outer_bindings = shape.join.left.get_column_bindings();
    let outer_types = shape.join.left.types();
    let first = *outer_bindings.first()?;
    if outer_bindings.len() != outer_types.len()
        || !outer_bindings.iter().enumerate().all(|(ordinal, binding)| {
            binding.table_index == first.table_index && binding.column_index == ordinal
        })
    {
        return None;
    }
    let join_conditions = shape
        .correlation
        .inner_keys
        .iter()
        .cloned()
        .zip(shape.join.duplicate_eliminated_columns.iter().cloned())
        .map(|(inner, outer)| JoinCondition::new(inner, outer, JoinComparisonType::Equal))
        .collect::<Vec<_>>();
    let null_rejection = NullRejectedKeyProof::from_equal_right_keys(&join_conditions)?;
    // Prove row uniqueness before grouping the bindings required by the
    // filter and explicit ancestor contract.
    prove_null_rejected_preserved_unique_key(&shape.join.left, &null_rejection)?;
    let mut grouped_bindings = output_contract.referenced.clone();
    for expression in shape
        .filter
        .expressions
        .iter()
        .chain(shape.join.duplicate_eliminated_columns.iter())
    {
        ExpressionIterator::visit(expression, &mut |candidate| {
            if let Expression::ColumnRef(column) = candidate {
                grouped_bindings.insert(column.binding);
            }
            ExpressionVisitDecision::Descend
        });
    }
    let output_width = outer_bindings
        .iter()
        .rposition(|binding| output_contract.referenced.contains(binding))
        .map_or(0, |ordinal| ordinal + 1);
    // Projection output bindings are ordinal and contiguous. Every position
    // before the last visible binding therefore carries its real value even
    // when the current contract does not reference it. This converts a future
    // contract-analysis omission into extra grouping work, never a silent
    // NULL substitution.
    grouped_bindings.extend(outer_bindings.iter().take(output_width).copied());
    let group_ordinals = outer_bindings
        .iter()
        .enumerate()
        .filter_map(|(ordinal, binding)| grouped_bindings.contains(binding).then_some(ordinal))
        .collect::<Vec<_>>();
    if group_ordinals.is_empty() {
        return None;
    }
    Some(GroupedJoinRewrite {
        scalar_binding: shape.scalar.scalar_binding,
        scalar_source_binding: ColumnBinding::new(shape.scalar.aggregate.aggregate_index, 0),
        scalar_expression: shape.scalar.scalar_expression.clone(),
        aggregate: shape.scalar.aggregate_expression.clone(),
        delim_table_index: shape.delim.table_index,
        null_rejection,
        outer_bindings,
        outer_types,
        group_ordinals,
        output_width,
        direct_source_side,
    })
}

fn filter_rejects_null_scalar(expression: &Expression, scalar: ColumnBinding) -> bool {
    let Expression::Comparison(comparison) = expression else {
        return false;
    };
    if !matches!(
        comparison.comparison_type,
        ComparisonType::Equal
            | ComparisonType::NotEqual
            | ComparisonType::LessThan
            | ComparisonType::LessThanOrEqual
            | ComparisonType::GreaterThan
            | ComparisonType::GreaterThanOrEqual
    ) {
        return false;
    }
    matches!(comparison.left.as_ref(), Expression::ColumnRef(column)
        if column.depth == 0 && column.binding == scalar)
        ^ matches!(comparison.right.as_ref(), Expression::ColumnRef(column)
            if column.depth == 0 && column.binding == scalar)
}

fn direct_delim_join_source(
    plan: &LogicalPlan,
    delim_table_index: usize,
    correlation_key_count: usize,
) -> Option<DirectSourceSide> {
    let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
        return None;
    };
    // Every condition is consumed when the DelimGet side is removed. Requiring
    // exact cardinality, together with correlation matching, proves there is
    // no residual predicate to lose.
    if !clean_inner_join(join) || join.conditions.len() != correlation_key_count {
        return None;
    }
    let direct = |child: &LogicalPlan| {
        matches!(&child.operator, LogicalOperator::DelimGet(delim)
            if delim.table_index == delim_table_index)
    };
    match (
        direct(&join.left) && !plan_references_delim(&join.right, delim_table_index),
        direct(&join.right) && !plan_references_delim(&join.left, delim_table_index),
    ) {
        (true, false) => Some(DirectSourceSide::LeftDelim),
        (false, true) => Some(DirectSourceSide::RightDelim),
        _ => None,
    }
}

fn plan_references_delim(plan: &LogicalPlan, delim_table_index: usize) -> bool {
    matches!(&plan.operator, LogicalOperator::DelimGet(delim)
        if delim.table_index == delim_table_index)
        || plan
            .children()
            .iter()
            .any(|child| plan_references_delim(child, delim_table_index))
}

/// Follow cardinality-preserving unary/reduction operators to the base scan
/// that owns the complete correlation tuple and prove a declared key.
fn prove_null_rejected_preserved_unique_key(
    plan: &LogicalPlan,
    null_rejection: &NullRejectedKeyProof,
) -> Option<()> {
    fn recurse(plan: &LogicalPlan, null_rejection: &NullRejectedKeyProof) -> Option<()> {
        match &plan.operator {
            LogicalOperator::Get(get) => {
                if null_rejection
                    .bindings()
                    .any(|key| key.table_index != get.table_index)
                {
                    return None;
                }
                declared_unique_keys(get)
                    .iter()
                    .any(|key| key.is_unique_with_nulls_rejected(null_rejection))
                    .then_some(())
            }
            LogicalOperator::Filter(filter) => recurse(&filter.child, null_rejection),
            LogicalOperator::Join(Join::Comparison(join))
                if matches!(join.join_type, JoinType::Semi | JoinType::Anti)
                    && join
                        .left_projection_map
                        .is_identity(join.left.types().len()) =>
            {
                recurse(&join.left, null_rejection)
            }
            _ => None,
        }
    }

    // The opaque witness was extracted from the same Equal conditions carried
    // into apply, so nullable UNIQUE rows never reach the aggregate. This is a
    // row-uniqueness proof only; it must not become a GROUP BY FD.
    recurse(plan, null_rejection)
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
    let LogicalOperator::Aggregate(aggregate) = &projection.child.operator else {
        return None;
    };
    let expected_projection_width = aggregate.groups.len().checked_add(1)?;
    if projection.expressions.len() != expected_projection_width
        || projection.returned_types.len() != expected_projection_width
        || projection.output_names.len() != expected_projection_width
    {
        return None;
    }
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
    if !projection
        .expressions
        .iter()
        .skip(1)
        .enumerate()
        .all(|(ordinal, expression)| {
            matches!(expression, Expression::ColumnRef(group_output)
            if group_output.depth == 0
                && group_output.binding == ColumnBinding::new(aggregate.group_index, ordinal))
        })
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
) -> Result<LogicalPlan> {
    let LogicalOperator::Filter(mut filter) = plan.operator else {
        return Err(paro_error::internal(
            "partition-aggregate witness no longer points to a Filter",
        ));
    };
    let LogicalOperator::Join(Join::Comparison(mut join)) = filter.child.operator else {
        return Err(paro_error::internal(
            "partition-aggregate witness no longer points to a comparison join",
        ));
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
    window_expression.verify_bound_contract()?;
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
        return Err(paro_error::internal(
            "partition-aggregate scalar witness contains an unexpected binding",
        ));
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
    Ok(LogicalPlan::new(
        bind_context,
        LogicalOperator::Filter(filter),
    ))
}

fn apply_grouped_join_rewrite(
    plan: LogicalPlan,
    rewrite: GroupedJoinRewrite,
    bind_context: &BindContext,
) -> Result<LogicalPlan> {
    let LogicalOperator::Filter(mut filter) = plan.operator else {
        return Err(paro_error::internal(
            "grouped-join witness no longer points to a Filter",
        ));
    };
    let LogicalOperator::Join(Join::Comparison(mut delim_join)) = filter.child.operator else {
        return Err(paro_error::internal(
            "grouped-join witness no longer points to a comparison join",
        ));
    };
    let outer = std::mem::replace(
        &mut *delim_join.left,
        LogicalPlan::synthetic(LogicalOperator::DummyScan),
    );
    // Give the consumed outer source fresh internal bindings so the final
    // projection can safely reintroduce its original binding domain above the
    // new aggregate. This is a structural move, not a second runtime scan: the
    // original source is discarded and only its re-indexed copy enters the
    // new join.
    let copied_outer =
        paro_planner::binder::deep_copy::deep_copy_plan(&outer, bind_context.shared().as_ref());
    let mut copied_outer_bindings = copied_outer.get_column_bindings();
    if copied_outer_bindings.len() < rewrite.outer_bindings.len() {
        return Err(paro_error::internal(
            "grouped-join copied outer source lost witnessed bindings",
        ));
    }
    copied_outer_bindings.truncate(rewrite.outer_bindings.len());
    let outer_binding_map = rewrite
        .outer_bindings
        .iter()
        .copied()
        .zip(copied_outer_bindings.iter().copied())
        .collect::<HashMap<_, _>>();
    let conditions = rewrite
        .null_rejection
        .right_keys()
        .map(|(left, right)| -> Result<_> {
            let binding = outer_binding_map
                .get(&right.binding)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal("grouped-join copied source lost a witnessed key binding")
                })?;
            Ok(JoinCondition::new(
                left.clone(),
                Expression::ColumnRef(ColumnRefExpression::new(binding, right.return_type.clone())),
                JoinComparisonType::Equal,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let LogicalOperator::Projection(scalar_projection) = delim_join.right.operator else {
        return Err(paro_error::internal(
            "grouped-join scalar branch lost its projection",
        ));
    };
    let LogicalOperator::Aggregate(mut scalar_aggregate) = scalar_projection.child.operator else {
        return Err(paro_error::internal(
            "grouped-join scalar branch lost its aggregate",
        ));
    };
    let inner = take_direct_delim_join_source(
        std::mem::replace(
            &mut *scalar_aggregate.child,
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
        ),
        rewrite.delim_table_index,
        rewrite.direct_source_side,
    )?;
    let joined = LogicalPlan::new(
        bind_context,
        LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            inner,
            copied_outer,
            conditions,
        ))),
    );
    let groups = rewrite
        .group_ordinals
        .iter()
        .map(|&ordinal| {
            let binding = copied_outer_bindings.get(ordinal).copied().ok_or_else(|| {
                paro_error::internal("grouped-join copied outer source lost a grouped binding")
            })?;
            let ty = rewrite.outer_types.get(ordinal).cloned().ok_or_else(|| {
                paro_error::internal("grouped-join witness lost a grouped binding type")
            })?;
            Ok(Expression::ColumnRef(ColumnRefExpression::new(binding, ty)))
        })
        .collect::<Result<Vec<_>>>()?;
    let group_index = bind_context.generate_table_index();
    let group_binding_map = rewrite
        .group_ordinals
        .iter()
        .enumerate()
        .map(|(group_ordinal, &outer_ordinal)| {
            (
                rewrite.outer_bindings[outer_ordinal],
                ColumnBinding::new(group_index, group_ordinal),
            )
        })
        .collect::<HashMap<_, _>>();
    let aggregate_index = scalar_aggregate.aggregate_index;
    let aggregate = Aggregate::new(
        group_index,
        aggregate_index,
        scalar_aggregate.groupings_index,
        joined,
        groups,
        Vec::new(),
        vec![Expression::Aggregate(rewrite.aggregate.clone())],
        Vec::new(),
    );
    let aggregate = LogicalPlan::new(bind_context, LogicalOperator::Aggregate(aggregate));
    let aggregate_binding = ColumnBinding::new(aggregate_index, 0);
    let aggregate_type = rewrite.aggregate.return_type.clone();
    let scalar = rewrite.scalar_expression.replace_column_ref(&|column| {
        (column.depth == 0 && column.binding == rewrite.scalar_source_binding).then(|| {
            Expression::ColumnRef(ColumnRefExpression::new(
                aggregate_binding,
                aggregate_type.clone(),
            ))
        })
    });
    if !expression_uses_only_binding(&scalar, aggregate_binding) {
        return Err(paro_error::internal(
            "grouped-join scalar witness contains an unexpected binding",
        ));
    }
    filter.expressions = filter
        .expressions
        .into_iter()
        .map(|expression| {
            let expression = expression.replace_column_ref(&|column| {
                (column.depth == 0 && column.binding == rewrite.scalar_binding)
                    .then(|| scalar.clone())
            });
            expression.replace_column_ref(&|column| {
                (column.depth == 0)
                    .then(|| group_binding_map.get(&column.binding).copied())
                    .flatten()
                    .map(|binding| {
                        Expression::ColumnRef(ColumnRefExpression::new(
                            binding,
                            column.return_type.clone(),
                        ))
                    })
            })
        })
        .collect();
    filter.child = Box::new(aggregate);
    filter.projection_map = (0..rewrite.group_ordinals.len()).collect::<Vec<_>>().into();
    let filter = LogicalPlan::new(bind_context, LogicalOperator::Filter(filter));

    // A binding-aware ancestor that references no outer column is independent
    // of this subtree's physical width. Keep the grouped filter directly and
    // avoid manufacturing a semantically meaningful zero-column projection.
    if rewrite.output_width == 0 {
        return Ok(filter);
    }

    let output_expressions = (0..rewrite.output_width)
        .map(|outer_ordinal| {
            let outer_binding = rewrite.outer_bindings[outer_ordinal];
            let ty = rewrite.outer_types[outer_ordinal].clone();
            let binding = group_binding_map
                .get(&outer_binding)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal("grouped-join output prefix contains an ungrouped binding")
                })?;
            Ok(Expression::ColumnRef(ColumnRefExpression::new(binding, ty)))
        })
        .collect::<Result<Vec<_>>>()?;
    let output_table_index = rewrite
        .outer_bindings
        .first()
        .ok_or_else(|| paro_error::internal("grouped-join witness has no outer bindings"))?
        .table_index;
    Ok(LogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(Projection::new(
            output_table_index,
            filter,
            output_expressions,
        )),
    ))
}

fn take_direct_delim_join_source(
    plan: LogicalPlan,
    delim_table_index: usize,
    source_side: DirectSourceSide,
) -> Result<LogicalPlan> {
    let LogicalOperator::Join(Join::Comparison(join)) = plan.operator else {
        return Err(paro_error::internal(
            "direct DelimGet witness no longer points to a comparison join",
        ));
    };
    let left_is_delim = matches!(&join.left.operator, LogicalOperator::DelimGet(delim)
        if delim.table_index == delim_table_index);
    let right_is_delim = matches!(&join.right.operator, LogicalOperator::DelimGet(delim)
        if delim.table_index == delim_table_index);
    match (source_side, left_is_delim, right_is_delim) {
        (DirectSourceSide::LeftDelim, true, false) => Ok(*join.right),
        (DirectSourceSide::RightDelim, false, true) => Ok(*join.left),
        _ => Err(paro_error::internal(
            "direct DelimGet source side no longer matches its witness",
        )),
    }
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

#[cfg(test)]
mod proof_tests {
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::operator::{DelimGet, Filter, ProjectionMap};

    use super::*;

    #[test]
    fn direct_delim_source_rejects_unconsumed_residual_condition() {
        let context = BindContext::new();
        let delim_index = 10;
        let delim = LogicalPlan::new(
            &context,
            LogicalOperator::DelimGet(DelimGet::new(delim_index, vec![LogicalType::BigInt])),
        );
        let inner = LogicalPlan::dummy_scan(&context);
        let column = |table_index, column_index| {
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, column_index),
                LogicalType::BigInt,
            ))
        };
        let plan = LogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                delim,
                inner,
                vec![
                    JoinCondition::new(
                        column(delim_index, 0),
                        column(20, 0),
                        JoinComparisonType::Equal,
                    ),
                    JoinCondition::new(column(20, 0), column(20, 1), JoinComparisonType::Equal),
                ],
            ))),
        );

        assert!(direct_delim_join_source(&plan, delim_index, 1).is_none());
    }

    #[test]
    fn exact_identity_projection_terminates_output_contract() {
        let context = BindContext::new();
        let mut filter = Filter::new(LogicalPlan::dummy_scan(&context), Vec::new());
        filter.projection_map = ProjectionMap::new(vec![0]);

        let contracts = child_output_contracts(
            &LogicalOperator::Filter(filter),
            Some(&OutputContract::default()),
        );

        assert_eq!(contracts.len(), 1);
        assert!(contracts[0].is_none());
    }

    #[test]
    fn layout_relative_projection_propagates_output_contract() {
        let context = BindContext::new();
        let filter = Filter::new(LogicalPlan::dummy_scan(&context), Vec::new());

        let contracts = child_output_contracts(
            &LogicalOperator::Filter(filter),
            Some(&OutputContract::default()),
        );

        assert_eq!(contracts.len(), 1);
        assert!(contracts[0].is_some());
    }
}
