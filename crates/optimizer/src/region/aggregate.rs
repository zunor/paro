// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded joint join/grain planning. Each state owns one local operator and
//! immutable child choices, not a copy of a relation tree. The same local
//! statistics equations and physical response kernel price every transition.
//! Unsupported aggregate laws and join boundaries remain committed barriers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use paro_common::error::Result;
use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ConjunctionType, Expression,
};
use paro_planner::operator::bound_reference::{BoundRelationFactValues, BoundRelationFacts};
use paro_planner::operator::{
    Aggregate, BoundReference, BoundReferenceId, ColumnBinding, ComparisonJoin, Join,
    JoinBuildSideConstraint, JoinCondition, JoinType, LogicalOperator, LogicalOutputLayout,
};
use paro_planner::plan::{arena::LogicalPlanNode, OwnedLogicalPlan};

use crate::context::OptimizationContext;
use crate::estimate::gathering::StatisticsGathering;
use crate::physical::{select, ObjectiveProfile, PhysicalImplementationFlavor, ResourceGrantClass};
use crate::rewrite::aggregate::dimension_deferral::{
    inline_projections, join_region::is_plain_inner_equi_join, partial_merge,
};
use crate::rewrite::expr::traversal::{into_associative_terms, visit_expression};
use crate::rewrite::join::mixed_predicates::{join_comparison_type, movable};

type Mask = u16;

/// `None` is raw SQL rows. A partial state identifies the exact input subset
/// at which aggregate states were formed; its boundary keys define its grain.
/// Distinct grains must never compete as interchangeable rows.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StateKey {
    relations: Mask,
    partial: Option<Mask>,
}

struct Node {
    shell: LogicalPlanNode<()>,
    children: Vec<Arc<Node>>,
    leaf: Option<usize>,
    layout: LogicalOutputLayout,
    columns: Box<
        [(
            ColumnBinding,
            Arc<paro_storage::statistics::ColumnStatistics>,
        )],
    >,
    response: select::PhysicalResponse,
    implementation: PhysicalImplementationFlavor,
    /// Original SQL bindings carried by the current partial grain.
    rebind: BTreeMap<ColumnBinding, ColumnBinding>,
    partial_aggregate_index: Option<usize>,
    boundary: BoundReference,
}

impl Node {
    fn boundary(&self, ordinal: usize) -> Result<BoundReference> {
        let mut reference = self.boundary.clone();
        reference.reference_id = BoundReferenceId::input_ordinal(ordinal);
        Ok(reference)
    }
}

fn boundary(
    plan: &OwnedLogicalPlan,
    layout: &LogicalOutputLayout,
    response: &select::PhysicalResponse,
) -> Result<BoundReference> {
    BoundReference::new(
        BoundReferenceId::input_ordinal(0),
        layout.bindings().to_vec(),
        layout.types().to_vec(),
    )
    .with_facts(Arc::new(BoundRelationFacts::new(
        BoundRelationFactValues {
            cardinality: plan.stats.estimated_cardinality,
            maximum_cardinality: response.hard_rows,
            unique_keys: plan.stats.unique_keys.clone(),
            finite_domains: plan.stats.finite_domains.clone(),
            source_lineage: crate::physical::implementation::planner_source_lineage(plan),
            contains_control_region: crate::cost::join_layout::contains_control_region_boundary(
                plan,
            ),
            ..Default::default()
        },
        layout.types().to_vec(),
    )))
}

#[derive(Default)]
pub(crate) struct Work {
    pub regions: u64,
    pub transitions: u64,
    pub partial_states: u64,
    pub budget_fallbacks: u64,
    pub selected_partial: u64,
    pub borrowed_cuts: u64,
    pub completed_outputs: u64,
}

struct Region<'a> {
    aggregate: Option<Aggregate<()>>,
    leaves: Vec<&'a OwnedLogicalPlan>,
    conditions: Vec<JoinCondition>,
    condition_support: Box<[(Mask, Mask)]>,
    owners: BTreeMap<ColumnBinding, Mask>,
    types: BTreeMap<ColumnBinding, paro_common::types::LogicalType>,
    arguments: Mask,
    residuals: Vec<(Mask, Expression)>,
    output: LogicalOutputLayout,
}

fn columns(expression: &Expression) -> Option<BTreeSet<ColumnBinding>> {
    let mut result = BTreeSet::new();
    let mut valid = true;
    visit_expression(expression, &mut |e| {
        if let Expression::ColumnRef(c) = e {
            valid &= c.depth == 0;
            result.insert(c.binding);
        }
        valid &= !matches!(e, Expression::Reference(_));
    });
    valid.then_some(result)
}

fn movable_inner<Child>(join: &ComparisonJoin<Child>) -> bool {
    join.join_type == JoinType::Inner
        && join.build_side_constraint == JoinBuildSideConstraint::Either
        && !join.conditions.is_empty()
        && join.mark_index.is_none()
        && join.duplicate_eliminated_columns.is_empty()
        && !join.delim_flipped
        && join
            .conditions
            .iter()
            .all(|c| movable(&c.left) && movable(&c.right))
}

fn condition_supports(
    conditions: &[JoinCondition],
    owners: &BTreeMap<ColumnBinding, Mask>,
) -> Option<Box<[(Mask, Mask)]>> {
    let support = |expression: &Expression| {
        columns(expression)?
            .iter()
            .try_fold(0, |mask, b| Some(mask | owners.get(b)?))
    };
    conditions
        .iter()
        .map(|c| Some((support(&c.left)?, support(&c.right)?)))
        .collect()
}

fn flatten<'a>(
    plan: &'a OwnedLogicalPlan,
    leaves: &mut Vec<&'a OwnedLogicalPlan>,
    conditions: &mut Vec<JoinCondition>,
) {
    match &plan.operator {
        LogicalOperator::Join(Join::Comparison(join)) if is_plain_inner_equi_join(join) => {
            flatten(&join.left, leaves, conditions);
            flatten(&join.right, leaves, conditions);
            conditions.extend(join.conditions.iter().cloned());
        }
        LogicalOperator::Join(Join::Cross(join))
            if join.build_side_constraint == JoinBuildSideConstraint::Either =>
        {
            flatten(&join.left, leaves, conditions);
            flatten(&join.right, leaves, conditions);
        }
        _ => leaves.push(plan),
    }
}

impl<'a> Region<'a> {
    fn recognize(plan: &'a OwnedLogicalPlan) -> Option<Self> {
        let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
            return None;
        };
        if !crate::rewrite::aggregate::dimension_deferral::root_eligible(aggregate)
            || aggregate.groups.is_empty()
        {
            return None;
        }
        let mut projections = Vec::new();
        let mut input = aggregate.child.as_ref();
        while let LogicalOperator::Projection(projection) = &input.operator {
            if projection.expressions.iter().any(|e| !movable(e)) {
                return None;
            }
            projections.push(projection);
            input = &projection.child;
        }
        if let LogicalOperator::Join(join) = &input.operator {
            if crate::rewrite::expr::join_tree_has_evaluation_fence(join) {
                return None;
            }
        }
        let mut leaves = Vec::new();
        let mut conditions = Vec::new();
        flatten(input, &mut leaves, &mut conditions);
        // This is an explicit local search domain, not an unbounded second
        // optimizer. Larger regions retain the ordinary join-DP path.
        if !(2..=8).contains(&leaves.len()) {
            return None;
        }
        let mut owners = BTreeMap::new();
        let mut types = BTreeMap::new();
        for (index, leaf) in leaves.iter().enumerate() {
            for (binding, ty) in leaf.get_column_bindings().into_iter().zip(leaf.types()) {
                if owners.insert(binding, 1 << index).is_some() {
                    return None;
                }
                types.insert(binding, ty);
            }
        }
        // A partial grain retains each crossing key verbatim. Computed and
        // non-equality predicates need their own transition law, not a guess.
        if conditions.iter().any(|condition| {
            !matches!((&condition.left, &condition.right), (Expression::ColumnRef(l), Expression::ColumnRef(r))
                if l.depth == 0 && r.depth == 0 && owners.contains_key(&l.binding) && owners.contains_key(&r.binding))
        }) { return None }
        let groups = aggregate
            .groups
            .iter()
            .map(|e| inline_projections(e, &projections))
            .collect::<Option<Vec<_>>>()?;
        let aggregates = aggregate
            .aggregates
            .iter()
            .map(|e| inline_projections(e, &projections))
            .collect::<Option<Vec<_>>>()?;
        if groups.iter().any(|e| !movable(e)) {
            return None;
        }
        let mut arguments = 0;
        for expression in &aggregates {
            partial_merge(expression)?;
            for binding in columns(expression)? {
                arguments |= *owners.get(&binding)?;
            }
        }
        for group in &groups {
            for binding in columns(group)? {
                owners.get(&binding)?;
            }
        }
        let aggregate = Aggregate {
            group_index: aggregate.group_index,
            aggregate_index: aggregate.aggregate_index,
            groupings_index: aggregate.groupings_index,
            child: (),
            group_stats: vec![None; groups.len()],
            groups,
            aggregates,
            grouping_sets: vec![],
            grouping_functions: vec![],
            group_dependencies: vec![],
            group_input_multiplicity: Default::default(),
            post_reduction: None,
            returned_types: aggregate.returned_types.clone(),
        };
        let condition_support = condition_supports(&conditions, &owners)?;
        Some(Self {
            aggregate: Some(aggregate),
            leaves,
            conditions,
            condition_support,
            owners,
            types,
            arguments,
            residuals: vec![],
            output: plan.output_layout(),
        })
    }

    fn recognize_joins(plan: &'a OwnedLogicalPlan) -> Option<Self> {
        fn collect<'a>(
            plan: &'a OwnedLogicalPlan,
            leaves: &mut Vec<&'a OwnedLogicalPlan>,
            conditions: &mut Vec<JoinCondition>,
            residuals: &mut Vec<Expression>,
        ) {
            match &plan.operator {
                LogicalOperator::Filter(f) if join_root(plan) => {
                    residuals.extend(f.expressions.iter().cloned());
                    collect(&f.child, leaves, conditions, residuals);
                }
                LogicalOperator::Join(Join::Comparison(j)) if movable_inner(j) => {
                    collect(&j.left, leaves, conditions, residuals);
                    collect(&j.right, leaves, conditions, residuals);
                    conditions.extend(j.conditions.iter().cloned());
                }
                LogicalOperator::Join(Join::Cross(j))
                    if j.build_side_constraint == JoinBuildSideConstraint::Either =>
                {
                    collect(&j.left, leaves, conditions, residuals);
                    collect(&j.right, leaves, conditions, residuals);
                }
                _ => leaves.push(plan),
            }
        }
        if !join_root(plan)
            || crate::rewrite::expr::join_region_has_evaluation_fence(&plan.operator)
        {
            return None;
        }
        let mut leaves = vec![];
        let mut conditions = vec![];
        let mut predicates = vec![];
        collect(plan, &mut leaves, &mut conditions, &mut predicates);
        if !(2..=12).contains(&leaves.len()) {
            return None;
        }
        let mut owners = BTreeMap::new();
        let mut types = BTreeMap::new();
        for (index, leaf) in leaves.iter().enumerate() {
            for (binding, ty) in leaf.get_column_bindings().into_iter().zip(leaf.types()) {
                if owners.insert(binding, 1 << index).is_some() {
                    return None;
                }
                types.insert(binding, ty);
            }
        }
        for c in &conditions {
            let mut sides = Vec::new();
            for expression in [&c.left, &c.right] {
                let used = columns(expression)?;
                if used.is_empty() || used.iter().any(|b| !owners.contains_key(b)) {
                    return None;
                }
                sides.push(used.iter().fold(0_u16, |mask, b| mask | owners[b]));
            }
            // A computed scalar on one relation is still a binary edge.
            // General overlapping operands need residual-hyperedge routing;
            // do not flatten and silently drop them at a non-oriented cut.
            if sides.iter().any(|s| !s.is_power_of_two()) || sides[0] == sides[1] {
                return None;
            }
        }
        // Predicate ownership is decided once, before constructing the graph.
        // Otherwise an equality can affect cardinality only in a late Filter
        // while neither connecting its relations nor becoming a hash key.
        // The binary domain requires each operand to belong to one atomic
        // input. General multi-input expressions remain explicit residuals.
        let mut residuals = Vec::new();
        for expression in predicates
            .into_iter()
            .flat_map(|e| into_associative_terms(e, ConjunctionType::And))
        {
            if !movable(&expression) {
                return None;
            }
            let support = |e: &Expression| {
                columns(e)?
                    .iter()
                    .try_fold(0_u16, |mask, b| Some(mask | owners.get(b)?))
            };
            let mask = support(&expression)?;
            if let Expression::Comparison(c) = &expression {
                let left = support(&c.left)?;
                let right = support(&c.right)?;
                if left.is_power_of_two() && right.is_power_of_two() && left != right {
                    conditions.push(JoinCondition::new(
                        (*c.left).clone(),
                        (*c.right).clone(),
                        join_comparison_type(c.comparison_type),
                    ));
                    continue;
                }
            }
            residuals.push((mask, expression));
        }
        let condition_support = condition_supports(&conditions, &owners)?;
        Some(Self {
            aggregate: None,
            leaves,
            conditions,
            condition_support,
            owners,
            types,
            arguments: 0,
            residuals,
            output: plan.output_layout(),
        })
    }

    fn cut(&self, left: Mask, right: Mask) -> Vec<JoinCondition> {
        self.conditions
            .iter()
            .zip(self.condition_support.iter().copied())
            .filter_map(|(c, (l, r))| {
                if l & left == l && r & right == r {
                    Some(c.clone())
                } else if r & left == r && l & right == l {
                    let mut c = c.clone();
                    std::mem::swap(&mut c.left, &mut c.right);
                    c.comparison = c.comparison.flip();
                    Some(c)
                } else {
                    None
                }
            })
            .collect()
    }
}

struct Planner<'a> {
    context: OptimizationContext,
    gathering: StatisticsGathering,
    grant: ResourceGrantClass,
    calibration: &'a crate::cost::calibration::MachineCalibrationBundle,
    work: &'a mut Work,
    remaining: usize,
    exhausted: bool,
}

/// A borrowed lookup over disjoint, already completed relation outputs. No
/// per-cut hash table or statistics clone is needed for response evaluation.
struct InputColumns<'a>(&'a [Arc<Node>]);

impl crate::estimate::ColumnStatisticsLookup for InputColumns<'_> {
    fn get(
        &self,
        binding: &ColumnBinding,
    ) -> Option<&Arc<paro_storage::statistics::ColumnStatistics>> {
        self.0.iter().find_map(|node| {
            node.columns
                .iter()
                .find(|(key, _)| key == binding)
                .map(|(_, value)| value)
        })
    }
}

// A cut is stack-local scratch, never a resident candidate archive. Boxing
// its large arm would add an allocation to every immediately rejected cut.
#[allow(clippy::large_enum_variant)]
enum CutCandidate {
    Ready(Arc<Node>),
    Priced {
        operator: LogicalOperator<BoundReference>,
        children: Vec<Arc<Node>>,
        selection: select::LocalSelection,
    },
}

impl CutCandidate {
    fn cost(&self) -> &crate::physical::SearchCost {
        match self {
            Self::Ready(node) => &node.response.cost,
            Self::Priced { selection, .. } => &selection.response.cost,
        }
    }
}

impl Planner<'_> {
    fn reserve_transition(&mut self) -> Result<bool> {
        self.context.session.cancellation.check()?;
        if self.remaining == 0 {
            self.exhausted = true;
            return Ok(false);
        }
        self.remaining -= 1;
        self.work.transitions += 1;
        Ok(true)
    }

    fn price_cut(
        &mut self,
        operator: LogicalOperator<BoundReference>,
        children: Vec<Arc<Node>>,
    ) -> Result<Option<CutCandidate>> {
        use crate::estimate::ColumnStatisticsLookup;
        // Equality narrowing changes value bounds, not known marginal NDVs.
        // Delay output propagation only in this explicit estimator domain.
        // Unknown domains and residual comparisons retain full settlement.
        let columns = InputColumns(&children);
        let eligible = matches!(&operator, LogicalOperator::Join(Join::Comparison(join))
            if join.conditions.iter().all(|c| c.comparison == paro_planner::operator::JoinComparisonType::Equal
                && [&c.left, &c.right].iter().all(|e| matches!(e, Expression::ColumnRef(column)
                    if columns.get(&column.binding).is_some_and(|s| s.distinct_evidence().point > 0)))));
        if !eligible {
            return self
                .emit(operator, children)
                .map(|node| node.map(CutCandidate::Ready));
        }
        if !self.reserve_transition()? {
            return Ok(None);
        }
        self.work.borrowed_cuts += 1;
        let layouts = children
            .iter()
            .map(|c| c.layout.clone())
            .collect::<Vec<_>>();
        let stats = paro_planner::plan::NodeStats {
            estimated_cardinality: self.gathering.estimate_native_cardinality(
                &operator,
                &layouts,
                &columns,
                &mut self.context,
            ),
            ..Default::default()
        };
        let layout = operator.output_layout_from_children(&layouts);
        let responses = children.iter().map(|c| &c.response).collect::<Vec<_>>();
        Ok(select::select_native(
            &operator,
            &stats,
            &layout,
            &layouts,
            &columns,
            &responses,
            select::SelectionEnvironment {
                grant: self.grant,
                calibration: self.calibration,
                session: &self.context.session,
            },
        )?
        .map(|selection| CutCandidate::Priced {
            operator,
            children,
            selection,
        }))
    }

    fn complete_cut(&mut self, candidate: CutCandidate) -> Result<Option<Arc<Node>>> {
        match candidate {
            CutCandidate::Ready(node) => Ok(Some(node)),
            CutCandidate::Priced {
                operator,
                children,
                selection,
            } => {
                let node = self.complete(operator, children)?;
                if let Some(node) = &node {
                    debug_assert_eq!(node.implementation, selection.implementation);
                    debug_assert_eq!(node.response.cost, selection.response.cost);
                }
                Ok(node)
            }
        }
    }

    fn emit(
        &mut self,
        operator: LogicalOperator<BoundReference>,
        children: Vec<Arc<Node>>,
    ) -> Result<Option<Arc<Node>>> {
        if !self.reserve_transition()? {
            return Ok(None);
        }
        self.complete(operator, children)
    }

    fn complete(
        &mut self,
        operator: LogicalOperator<BoundReference>,
        children: Vec<Arc<Node>>,
    ) -> Result<Option<Arc<Node>>> {
        self.work.completed_outputs += 1;
        // One region-owned scratch table. Candidate outputs retain aligned
        // immutable column handles, not one hash table per candidate.
        let mut columns = std::mem::take(self.context.column_stats_mut());
        columns.clear();
        for child in &children {
            columns.extend(child.columns.iter().map(|(k, v)| (*k, v.clone())));
        }
        let mut propagator =
            crate::estimate::propagator::StatisticsPropagator::with_statistics_map(columns);
        let operator = propagator.propagate_native_operator(&self.context.session, operator);
        self.context.column_stats = Arc::new(propagator.take_statistics_map());
        let layouts = children
            .iter()
            .map(|c| c.layout.clone())
            .collect::<Vec<_>>();
        let bounds = children
            .iter()
            .map(|c| c.response.hard_rows)
            .collect::<Vec<_>>();
        let (stats, operator, layout, _) = self.gathering.gather_native_local(
            operator,
            Default::default(),
            &layouts,
            &bounds,
            self.context.column_stats.clone(),
            &mut self.context,
        );
        let responses = children.iter().map(|c| &c.response).collect::<Vec<_>>();
        let Some(selected) = select::select_native(
            &operator,
            &stats,
            &layout,
            &layouts,
            &self.context.column_stats,
            &responses,
            select::SelectionEnvironment {
                grant: self.grant,
                calibration: self.calibration,
                session: &self.context.session,
            },
        )?
        else {
            return Ok(None);
        };
        let columns = layout
            .bindings()
            .iter()
            .filter_map(|binding| {
                self.context
                    .column_stats
                    .get(binding)
                    .map(|s| (*binding, s.clone()))
            })
            .collect();
        let source_lineage = match &operator {
            LogicalOperator::Filter(_) | LogicalOperator::Join(Join::Comparison(_)) => layout
                .bindings()
                .iter()
                .map(|binding| {
                    children
                        .iter()
                        .find_map(|child| {
                            child
                                .layout
                                .bindings()
                                .iter()
                                .position(|b| b == binding)
                                .and_then(|i| child.boundary.facts.source_lineage.get(i).cloned())
                        })
                        .flatten()
                })
                .collect(),
            _ => vec![None; layout.len()],
        };
        let boundary = BoundReference::new(
            BoundReferenceId::input_ordinal(0),
            layout.bindings().to_vec(),
            layout.types().to_vec(),
        )
        .with_facts(Arc::new(BoundRelationFacts::new(
            BoundRelationFactValues {
                cardinality: stats.estimated_cardinality,
                maximum_cardinality: selected.response.hard_rows,
                unique_keys: stats.unique_keys.clone(),
                finite_domains: stats.finite_domains.clone(),
                source_lineage,
                contains_control_region: children
                    .iter()
                    .any(|c| c.boundary.facts.contains_control_region),
                ..Default::default()
            },
            layout.types().to_vec(),
        )))?;
        let shell = LogicalPlanNode {
            id: self.context.bind_context.next_plan_id(),
            stats,
            operator: operator.try_map_child_links(&mut |_| -> Result<_> { Ok(()) })?,
        };
        let mut rebind = BTreeMap::new();
        let mut partial_aggregate_index = None;
        for child in &children {
            rebind.extend(child.rebind.iter().map(|(k, v)| (*k, *v)));
            partial_aggregate_index = partial_aggregate_index.or(child.partial_aggregate_index);
        }
        Ok(Some(Arc::new(Node {
            shell,
            children,
            leaf: None,
            layout,
            columns,
            response: selected.response,
            implementation: selected.implementation,
            rebind,
            partial_aggregate_index,
            boundary,
        })))
    }

    fn partial(
        &mut self,
        region: &Region<'_>,
        mask: Mask,
        input: Arc<Node>,
    ) -> Result<Option<Arc<Node>>> {
        let mut keys = BTreeSet::new();
        for c in &region.conditions {
            let (Expression::ColumnRef(l), Expression::ColumnRef(r)) = (&c.left, &c.right) else {
                unreachable!()
            };
            if (region.owners[&l.binding] & mask != 0) != (region.owners[&r.binding] & mask != 0) {
                keys.insert(if region.owners[&l.binding] & mask != 0 {
                    l.binding
                } else {
                    r.binding
                });
            }
        }
        let aggregate = region
            .aggregate
            .as_ref()
            .expect("partial transition owns a merge law");
        for group in &aggregate.groups {
            for binding in columns(group).expect("recognized group") {
                if region.owners[&binding] & mask != 0 {
                    keys.insert(binding);
                }
            }
        }
        if keys.is_empty() {
            return Ok(None);
        }
        let group_index = self.context.bind_context.generate_table_index();
        let aggregate_index = self.context.bind_context.generate_table_index();
        let groupings_index = self.context.bind_context.generate_table_index();
        let groups: Vec<_> = keys
            .iter()
            .map(|binding| {
                Expression::ColumnRef(
                    ColumnRefExpression::new(*binding, region.types[binding].clone()).into(),
                )
            })
            .collect();
        let mut aggregate = Aggregate {
            group_index,
            aggregate_index,
            groupings_index,
            child: input.boundary(0)?,
            group_stats: vec![None; groups.len()],
            groups,
            grouping_sets: vec![],
            aggregates: aggregate.aggregates.clone(),
            grouping_functions: vec![],
            group_dependencies: vec![],
            group_input_multiplicity: Default::default(),
            post_reduction: None,
            returned_types: vec![],
        };
        aggregate.recompute_returned_types();
        let Some(mut output) =
            self.emit(LogicalOperator::Aggregate(Box::new(aggregate)), vec![input])?
        else {
            return Ok(None);
        };
        let node = Arc::get_mut(&mut output).expect("unpublished partial state");
        node.rebind = keys
            .into_iter()
            .enumerate()
            .map(|(i, k)| (k, ColumnBinding::new(group_index, i)))
            .collect();
        node.partial_aggregate_index = Some(aggregate_index);
        self.work.partial_states += 1;
        Ok(Some(output))
    }

    fn finish(&mut self, region: &Region<'_>, input: Arc<Node>) -> Result<Option<Arc<Node>>> {
        let Some(mut aggregate) = region.aggregate.clone() else {
            if input.layout.bindings() == region.output.bindings() {
                return Ok(Some(input));
            }
            let indices = region
                .output
                .bindings()
                .iter()
                .map(|b| input.layout.bindings().iter().position(|c| c == b))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    paro_common::error::internal("region lost a promised output binding")
                })?;
            let mut filter = filter(input.boundary(0)?, vec![]);
            filter.projection_map = paro_planner::operator::ProjectionMap::new(indices);
            return self.emit(LogicalOperator::Filter(filter), vec![input]);
        };
        aggregate.groups = aggregate
            .groups
            .into_iter()
            .map(|e| rebind(e, &input.rebind))
            .collect();
        if let Some(index) = input.partial_aggregate_index {
            aggregate.aggregates = aggregate
                .aggregates
                .into_iter()
                .enumerate()
                .map(|(ordinal, e)| {
                    let Expression::Aggregate(a) = e else {
                        unreachable!()
                    };
                    let merge = a
                        .function
                        .partial_merge_function()
                        .expect("recognized merge law");
                    Expression::Aggregate(
                        AggregateExpression::new(
                            merge,
                            vec![Expression::ColumnRef(
                                ColumnRefExpression::new(
                                    ColumnBinding::new(index, ordinal),
                                    a.return_type.clone(),
                                )
                                .into(),
                            )],
                            a.return_type.clone(),
                        )
                        .into(),
                    )
                })
                .collect();
        }
        let operator = LogicalOperator::Aggregate(Box::new(aggregate))
            .try_map_child_links(&mut |_| input.boundary(0))?;
        self.emit(operator, vec![input])
    }
}

fn rebind(expression: Expression, bindings: &BTreeMap<ColumnBinding, ColumnBinding>) -> Expression {
    expression.replace_column_ref(&|c| {
        bindings.get(&c.binding).map(|binding| {
            Expression::ColumnRef(ColumnRefExpression::new(*binding, c.return_type.clone()).into())
        })
    })
}

fn filter(
    child: BoundReference,
    expressions: Vec<Expression>,
) -> paro_planner::operator::Filter<BoundReference> {
    paro_planner::operator::Filter {
        child,
        expressions,
        projection_map: paro_planner::operator::ProjectionMap::all(),
    }
}

fn retain(states: &mut [BTreeMap<Option<Mask>, Arc<Node>>], key: StateKey, node: Arc<Node>) {
    debug_assert_eq!(
        key.partial.is_some(),
        node.partial_aggregate_index.is_some()
    );
    let frontier = &mut states[key.relations as usize];
    if frontier.get(&key.partial).is_none_or(|old| {
        ObjectiveProfile::Latency
            .compare(&node.response.cost, &old.response.cost)
            .is_lt()
    }) {
        frontier.insert(key.partial, node);
    }
}

fn reconstruct(
    node: &Node,
    region: &Region<'_>,
    context: &OptimizationContext,
) -> Result<OwnedLogicalPlan> {
    if let Some(index) = node.leaf {
        return Ok(duplicate_plan_preserving_indices(
            region.leaves[index],
            context.bind_context.shared().as_ref(),
        ));
    }
    let children = node
        .children
        .iter()
        .map(|c| reconstruct(c, region, context).map(Box::new))
        .collect::<Result<Vec<_>>>()?;
    let mut plan = node.shell.clone().assemble(children)?;
    // The selected build orientation is a physical decision, not a second
    // opportunity to reinterpret expected rows as a worst-case row envelope.
    if let LogicalOperator::Join(Join::Comparison(join)) = &mut plan.operator {
        join.build_side_constraint = if matches!(
            node.implementation,
            PhysicalImplementationFlavor::HashJoinBuildLeft
                | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
        ) {
            JoinBuildSideConstraint::Left
        } else {
            JoinBuildSideConstraint::Right
        };
        // Keep logical output positions stable. A physical build decision
        // must not swap a child's layout under an already priced projection.
    }
    Ok(plan)
}

pub(crate) struct Selection {
    pub plan: OwnedLogicalPlan,
    pub columns: HashMap<ColumnBinding, Arc<paro_storage::statistics::ColumnStatistics>>,
    #[cfg(test)]
    cost: crate::physical::SearchCost,
}

pub(crate) fn optimize(
    plan: &OwnedLogicalPlan,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cost::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
) -> Result<Option<Selection>> {
    let Some(region) = Region::recognize(plan) else {
        return Ok(None);
    };
    optimize_region(region, context, grant, calibration, max_transitions, work)
}

pub(crate) fn join_root(plan: &OwnedLogicalPlan) -> bool {
    match &plan.operator {
        LogicalOperator::Join(Join::Comparison(j)) => movable_inner(j),
        LogicalOperator::Join(Join::Cross(j)) => {
            j.build_side_constraint == JoinBuildSideConstraint::Either
        }
        LogicalOperator::Filter(f) => f.expressions.iter().all(movable) && join_root(&f.child),
        _ => false,
    }
}

pub(crate) fn optimize_joins(
    plan: &OwnedLogicalPlan,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cost::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
) -> Result<Option<Selection>> {
    let Some(region) = Region::recognize_joins(plan) else {
        return Ok(None);
    };
    optimize_region(region, context, grant, calibration, max_transitions, work)
}

/// Grain-aware receiver for the same connected-subgraph traversal used by
/// ordinary join planning. Traversal owns connectivity; this receiver owns
/// estimates, exact child choices and the explicit one-winner grain policy.
struct RegionEnumeration<'a, 'b> {
    region: Region<'a>,
    planner: Planner<'b>,
    states: Vec<BTreeMap<Option<Mask>, Arc<Node>>>,
    graph: crate::region::join::query_graph::QueryGraphEdges,
    sets: crate::region::join::relation::JoinRelationSetManager,
    allow_partial: bool,
    error: Option<paro_common::error::ParoError>,
}

fn relation_mask(set: &crate::region::join::relation::JoinRelationSet) -> Mask {
    set.relations()
        .iter()
        .fold(0, |mask, index| mask | (1 << index))
}

impl RegionEnumeration<'_, '_> {
    fn add_partial(&mut self, mask: Mask) -> Result<()> {
        let full = self.states.len() as Mask - 1;
        if self.allow_partial
            && self.region.aggregate.is_some()
            && mask != full
            && self.region.arguments & mask == self.region.arguments
        {
            if let Some(raw) = self.states[mask as usize].get(&None).cloned() {
                if let Some(partial) = self.planner.partial(&self.region, mask, raw)? {
                    retain(
                        &mut self.states,
                        StateKey {
                            relations: mask,
                            partial: Some(mask),
                        },
                        partial,
                    );
                }
            }
        }
        Ok(())
    }

    fn price_pair(&mut self, left: Mask, right: Mask) -> Result<()> {
        let mask = left | right;
        let full = self.states.len() as Mask - 1;
        let conditions = self.region.cut(left, right);
        if conditions.is_empty() {
            return Ok(());
        }
        let left_states = self.states[left as usize]
            .iter()
            .map(|(k, n)| (*k, n.clone()))
            .collect::<Vec<_>>();
        let right_states = self.states[right as usize]
            .iter()
            .map(|(k, n)| (*k, n.clone()))
            .collect::<Vec<_>>();
        let residuals = self
            .region
            .residuals
            .iter()
            .filter(|(support, _)| {
                if *support == 0 {
                    mask == full
                } else {
                    *support & mask == *support
                        && *support & left != *support
                        && *support & right != *support
                }
            })
            .map(|(_, e)| e.clone())
            .collect::<Vec<_>>();
        let mut changed_raw = false;
        for (lk, l) in &left_states {
            for (rk, r) in &right_states {
                if lk.is_some() && rk.is_some() {
                    continue;
                }
                let grain = lk.or(*rk);
                let conditions = conditions
                    .iter()
                    .cloned()
                    .map(|mut c| {
                        c.left = rebind(c.left, &l.rebind);
                        c.right = rebind(c.right, &r.rebind);
                        c
                    })
                    .collect();
                let join = ComparisonJoin {
                    join_type: JoinType::Inner,
                    anti_join_mode: Default::default(),
                    left: l.boundary(0)?,
                    right: r.boundary(1)?,
                    conditions,
                    mark_index: None,
                    mark_semantics: paro_planner::operator::join::MarkJoinSemantics::NotMark,
                    duplicate_eliminated_columns: vec![],
                    delim_flipped: false,
                    build_side_constraint: JoinBuildSideConstraint::Either,
                    left_projection_map: paro_planner::operator::ProjectionMap::all(),
                    right_projection_map: paro_planner::operator::ProjectionMap::all(),
                };
                let operator = LogicalOperator::Join(Join::Comparison(join));
                let children = vec![l.clone(), r.clone()];
                let candidate = if residuals.is_empty() {
                    self.planner.price_cut(operator, children)?
                } else if let Some(node) = self.planner.emit(operator, children)? {
                    let filter = filter(node.boundary(0)?, residuals.clone());
                    self.planner
                        .emit(LogicalOperator::Filter(filter), vec![node])?
                        .map(CutCandidate::Ready)
                } else {
                    None
                };
                let Some(candidate) = candidate else {
                    continue;
                };
                if self.states[mask as usize].get(&grain).is_some_and(|old| {
                    !ObjectiveProfile::Latency
                        .compare(candidate.cost(), &old.response.cost)
                        .is_lt()
                }) {
                    continue;
                }
                if let Some(node) = self.planner.complete_cut(candidate)? {
                    retain(
                        &mut self.states,
                        StateKey {
                            relations: mask,
                            partial: grain,
                        },
                        node,
                    );
                    changed_raw |= grain.is_none();
                }
            }
        }
        if changed_raw {
            self.add_partial(mask)?;
        }
        Ok(())
    }
}

impl crate::region::join::connected::ConnectedRegion for RegionEnumeration<'_, '_> {
    fn relations(&self) -> usize {
        self.region.leaves.len()
    }
    fn graph(&self) -> &crate::region::join::query_graph::QueryGraphEdges {
        &self.graph
    }
    fn sets(&mut self) -> &mut crate::region::join::relation::JoinRelationSetManager {
        &mut self.sets
    }
    fn contains(&self, set: &Arc<crate::region::join::relation::JoinRelationSet>) -> bool {
        !self.states[relation_mask(set) as usize].is_empty()
    }
    fn emit(
        &mut self,
        left: &Arc<crate::region::join::relation::JoinRelationSet>,
        right: &Arc<crate::region::join::relation::JoinRelationSet>,
        _connections: &[crate::region::join::query_graph::NeighborInfo],
    ) -> crate::region::join::enumerator::EnumerationOutcome {
        use crate::region::join::enumerator::EnumerationOutcome as Outcome;
        if let Err(error) = self.price_pair(relation_mask(left), relation_mask(right)) {
            self.error = Some(error);
            return Outcome::Ineligible;
        }
        if self.planner.exhausted {
            Outcome::PairBudgetExhausted
        } else {
            Outcome::Complete
        }
    }
}

fn optimize_region(
    region: Region<'_>,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cost::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
) -> Result<Option<Selection>> {
    optimize_region_with(
        region,
        context,
        grant,
        calibration,
        max_transitions,
        work,
        |region| crate::region::join::connected::enumerate(region),
    )
}

fn optimize_region_with(
    region: Region<'_>,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cost::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
    enumerate: impl FnOnce(
        &mut RegionEnumeration<'_, '_>,
    ) -> crate::region::join::enumerator::EnumerationOutcome,
) -> Result<Option<Selection>> {
    let allow_partial = context.session.settings.optimizer_aggregate_strategy()?
        == paro_context::OptimizerAggregateStrategy::Joint;
    let mut planner = Planner {
        context: context.fork_for_candidate(Arc::new(HashMap::new())),
        gathering: StatisticsGathering::new(),
        grant,
        calibration,
        work,
        remaining: max_transitions,
        exhausted: false,
    };
    let mut states = vec![BTreeMap::new(); 1 << region.leaves.len()];
    for (index, leaf) in region.leaves.iter().enumerate() {
        let Some(response) = select::estimate_response(
            leaf,
            &context.column_stats,
            grant,
            calibration,
            &context.session,
        )?
        else {
            return Ok(None);
        };
        let layout = leaf.output_layout();
        let boundary = boundary(leaf, &layout, &response)?;
        let columns = layout
            .bindings()
            .iter()
            .filter_map(|b| context.column_stats.get(b).map(|s| (*b, s.clone())))
            .collect();
        let shell = LogicalPlanNode {
            id: leaf.id,
            stats: leaf.stats.clone(),
            operator: LogicalOperator::DummyScan,
        };
        let mut node = Arc::new(Node {
            shell,
            children: vec![],
            leaf: Some(index),
            layout,
            columns,
            response,
            implementation: PhysicalImplementationFlavor::Structural,
            rebind: BTreeMap::new(),
            partial_aggregate_index: None,
            boundary,
        });
        let residuals = region
            .residuals
            .iter()
            .filter(|(support, _)| *support == 1 << index)
            .map(|(_, e)| e.clone())
            .collect::<Vec<_>>();
        if !residuals.is_empty() {
            let filter = filter(node.boundary(0)?, residuals);
            let Some(filtered) = planner.emit(LogicalOperator::Filter(filter), vec![node])? else {
                planner.work.budget_fallbacks += u64::from(planner.exhausted);
                return Ok(None);
            };
            node = filtered;
        }
        states[1 << index].insert(None, node);
    }
    let full: Mask = (1 << region.leaves.len()) - 1;
    let mut sets = crate::region::join::relation::JoinRelationSetManager::new();
    let mut graph = crate::region::join::query_graph::QueryGraphEdges::new();
    let mut make_set = |mask: Mask| {
        sets.get_relation_from_vec(
            (0..region.leaves.len())
                .filter(|i| mask & (1 << i) != 0)
                .collect(),
        )
    };
    for (left, right) in region.condition_support.iter().copied() {
        let left = make_set(left);
        let right = make_set(right);
        graph.create_edge(&left, right.clone(), None);
        graph.create_edge(&right, left, None);
    }
    let mut enumeration = RegionEnumeration {
        region,
        planner,
        states,
        graph,
        sets,
        allow_partial,
        error: None,
    };
    for i in 0..enumeration.region.leaves.len() {
        enumeration.add_partial(1 << i)?;
    }
    let _outcome = enumerate(&mut enumeration);
    let RegionEnumeration {
        region,
        mut planner,
        states,
        error,
        ..
    } = enumeration;
    if let Some(error) = error {
        return Err(error);
    }
    let mut best: Option<Arc<Node>> = None;
    for input in states[full as usize].values() {
        if let Some(candidate) = planner.finish(&region, input.clone())? {
            if best.as_ref().is_none_or(|old| {
                ObjectiveProfile::Latency
                    .compare(&candidate.response.cost, &old.response.cost)
                    .is_lt()
            }) {
                best = Some(candidate);
            }
        }
    }
    if planner.exhausted {
        planner.work.budget_fallbacks += 1;
        return Ok(None);
    }
    let Some(best) = best else { return Ok(None) };
    planner.work.regions += 1;
    planner.work.selected_partial += u64::from(best.partial_aggregate_index.is_some());
    let mut columns = HashMap::new();
    let mut pending = vec![best.as_ref()];
    while let Some(node) = pending.pop() {
        columns.extend(node.columns.iter().map(|(k, v)| (*k, v.clone())));
        pending.extend(node.children.iter().map(Arc::as_ref));
    }
    Ok(Some(Selection {
        plan: reconstruct(&best, &region, context)?,
        columns,
        #[cfg(test)]
        cost: best.response.cost,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::Optimizer;
    use paro_planner::planner::Planner;

    #[test]
    fn filter_comparisons_connect_region_and_become_exact_cut_conditions() {
        use paro_common::types::LogicalType;
        use paro_planner::expression::{
            ComparisonExpression, ComparisonType, ConjunctionExpression,
        };
        use paro_planner::operator::{ExpressionGet, Filter, JoinComparisonType};
        let column = |table, index| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, index), LogicalType::Integer)
                    .into(),
            )
        };
        let input = |table| {
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                table,
                vec![],
                vec!["key".into(), "value".into()],
                vec![LogicalType::Integer; 2],
            )))
        };
        let equality = |l, r| JoinCondition::new(l, r, JoinComparisonType::Equal);
        let ab = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                input(1),
                input(2),
                vec![equality(column(1, 0), column(2, 0))],
            ),
        )));
        let abc = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                ab,
                input(3),
                vec![equality(column(1, 1), column(3, 0))],
            ),
        )));
        let expressions = [
            Expression::Comparison(
                ComparisonExpression::new(ComparisonType::Equal, column(2, 1), column(3, 1)).into(),
            ),
            Expression::Comparison(
                ComparisonExpression::new(ComparisonType::LessThan, column(1, 1), column(2, 1))
                    .into(),
            ),
        ];
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            abc,
            vec![Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::And, expressions.to_vec()).into(),
            )],
        )));
        let region = Region::recognize_joins(&plan).unwrap();
        assert_eq!(region.conditions.len(), 4);
        assert!(region.residuals.is_empty());
        // B-C is now a real edge, not a residual waiting for A-B-C. Joining
        // A with B-C owns two equality keys and the range residual exactly once.
        assert_eq!(region.cut(2, 4).len(), 1);
        let cut = region.cut(1, 6);
        assert_eq!(cut.len(), 3);
        assert_eq!(
            cut.iter()
                .filter(|c| c.comparison == JoinComparisonType::Equal)
                .count(),
            2
        );
        assert_eq!(
            region.cut(6, 1).last().unwrap().comparison,
            JoinComparisonType::GreaterThan
        );

        let LogicalOperator::Filter(mut filter) = plan.into_parts().2 else {
            unreachable!()
        };
        filter.expressions = vec![Expression::Conjunction(
            ConjunctionExpression::new(ConjunctionType::Or, expressions.to_vec()).into(),
        )];
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter));
        let region = Region::recognize_joins(&plan).unwrap();
        assert_eq!(region.conditions.len(), 2);
        assert_eq!(
            region.residuals.len(),
            1,
            "OR must remain one predicate, not two hash keys"
        );
    }

    fn exercise(sql: &str, budget: usize) -> Work {
        exercise_domain(sql, budget, false)
    }

    fn exercise_domain(sql: &str, budget: usize, ordinary: bool) -> Work {
        exercise_with_statistics(sql, budget, ordinary, false)
    }

    fn exercise_with_statistics(sql: &str, budget: usize, ordinary: bool, known: bool) -> Work {
        let session = crate::rewrite::subquery::partition_aggregate_tests::setup_session();
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(paro_parser::parse_one(sql).unwrap().stmt)
            .unwrap();
        let query = planner.take_plan().unwrap();
        let (plan, mut context, calibration) = Optimizer::new(planner.binder.clone(), session)
            .prepare_region_for_test(query)
            .unwrap();
        if known {
            for statistic in context.column_stats_mut().values_mut() {
                let value = paro_storage::statistics::ColumnStatistics::with_estimated_distinct(
                    statistic.statistics().clone(),
                    Some(10),
                );
                *statistic = Arc::new(value);
            }
        }
        let grant = ResourceGrantClass {
            id: crate::physical::ResourceGrantClassId(0),
            hard_memory_bytes: 2 * 1024 * 1024 * 1024,
            spill_policy: crate::physical::SpillPolicy::Allowed,
            max_parallel_tasks: 4,
        };
        let mut work = Work::default();
        plan.try_visit_pre_order(|plan| {
            let optimize = if ordinary { optimize_joins } else { optimize };
            if let Some(selected) =
                optimize(plan, &context, grant, &calibration, budget, &mut work)?
            {
                if known {
                    let oracle_region = if ordinary {
                        Region::recognize_joins(plan)
                    } else {
                        Region::recognize(plan)
                    };
                    if let Some(region) = oracle_region.filter(|r| r.leaves.len() <= 4) {
                        let oracle = optimize_region_with(
                            region,
                            &context,
                            grant,
                            &calibration,
                            budget,
                            &mut Work::default(),
                            |enumeration| {
                                let full = enumeration.states.len() as Mask - 1;
                                for size in 2..=enumeration.region.leaves.len() {
                                    for mask in 1..=full {
                                        if mask.count_ones() as usize != size {
                                            continue;
                                        }
                                        let mut left = (mask - 1) & mask;
                                        while left != 0 {
                                            let right = mask ^ left;
                                            if left < right {
                                                enumeration.price_pair(left, right).unwrap();
                                            }
                                            left = (left - 1) & mask;
                                        }
                                    }
                                }
                                crate::region::join::enumerator::EnumerationOutcome::Complete
                            },
                        )?
                        .expect("independent subset oracle has a plan");
                        let actual = selected.cost.score.range.expected;
                        let expected = oracle.cost.score.range.expected;
                        assert!(
                            (actual - expected).abs() <= expected.abs().max(1.0) * 1e-10,
                            "connected traversal {actual} != exhaustive subset oracle {expected}"
                        );
                    }
                }
                assert_eq!(
                    selected.plan.get_column_bindings(),
                    plan.get_column_bindings()
                );
                assert_eq!(selected.plan.types(), plan.types());
                let mut joins = 0;
                selected.plan.try_visit_pre_order(|node| {
                    assert!(
                        !matches!(node.operator, LogicalOperator::BoundReference(_)),
                        "no pricing boundary may escape reconstruction"
                    );
                    assert!(
                        !matches!(node.operator, LogicalOperator::Join(Join::Cross(_))),
                        "connected test regions must not retain an avoidable Cartesian product"
                    );
                    if let LogicalOperator::Join(Join::Comparison(join)) = &node.operator {
                        joins += 1;
                        let left = join.left.get_column_bindings();
                        let right = join.right.get_column_bindings();
                        for condition in &join.conditions {
                            assert!(columns(&condition.left)
                                .unwrap()
                                .iter()
                                .all(|c| left.contains(c)));
                            assert!(columns(&condition.right)
                                .unwrap()
                                .iter()
                                .all(|c| right.contains(c)));
                        }
                    }
                    Ok(())
                })?;
                assert!(joins > 0);
            }
            Ok(())
        })
        .unwrap();
        work
    }

    const QUERY: &str = "SELECT n_name, r_name, sum(s_acctbal) FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey GROUP BY n_name,r_name";

    #[test]
    fn borrowed_cut_response_equals_full_settlement_and_discards_losing_outputs() {
        // Complete every retained response with the independent full native
        // settlement path. complete_cut asserts exact cost/implementation
        // agreement, not just matching the final SQL layout.
        let work = exercise_with_statistics(
            "SELECT a.s_acctbal FROM supplier a JOIN supplier b ON a.s_nationkey=b.s_nationkey JOIN supplier c ON b.s_nationkey=c.s_nationkey JOIN supplier d ON c.s_nationkey=d.s_nationkey",
            10000, true, true,
        );
        assert!(work.borrowed_cuts > 0);
        assert!(work.completed_outputs < work.transitions);
    }

    #[test]
    fn borrowed_join_pricing_retains_aggregate_grain_contract() {
        let work = exercise_with_statistics(QUERY, 10000, false, true);
        assert!(work.borrowed_cuts > 0);
        assert!(work.partial_states > 0);
        assert_eq!(work.budget_fallbacks, 0);
    }

    #[test]
    fn ordinary_regions_use_physical_response_and_preserve_output_bindings() {
        let work = exercise_domain("SELECT r_name,n_name,s_acctbal FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey WHERE s_acctbal > n_nationkey", 10000, true);
        assert!(work.regions > 0);
        assert!(work.transitions > 0);
        assert_eq!(work.partial_states, 0);
        assert_eq!(work.budget_fallbacks, 0);
    }

    #[test]
    fn mixed_equality_and_range_conditions_share_the_connected_traversal() {
        let work = exercise_domain(
            "SELECT a.s_acctbal FROM supplier a JOIN supplier b ON a.s_nationkey=b.s_nationkey AND a.s_acctbal < b.s_acctbal JOIN nation ON b.s_nationkey=n_nationkey",
            10000, true,
        );
        assert!(work.regions > 0);
        assert_eq!(work.budget_fallbacks, 0);
    }

    #[test]
    fn ordinary_region_is_not_limited_to_eight_relations() {
        let mut query = "SELECT a0.s_acctbal FROM supplier a0".to_string();
        for i in 1..9 {
            query.push_str(&format!(
                " JOIN supplier a{i} ON a{}.s_nationkey=a{i}.s_nationkey",
                i - 1
            ));
        }
        let work = exercise_with_statistics(&query, 65536, true, true);
        assert!(work.regions > 0);
        assert_eq!(work.budget_fallbacks, 0);
    }

    #[test]
    fn connected_region_opens_cartesian_input_before_committing_physical_choices() {
        // The third relation connects the first two. Treating their syntactic
        // CROSS PRODUCT as an atomic leaf locks in an arbitrarily large
        // intermediate, even though the complete region is connected.
        let work = exercise_domain("SELECT s_acctbal,r_name,n_name FROM supplier CROSS JOIN region JOIN nation ON s_nationkey=n_nationkey AND r_regionkey=n_regionkey", 10000, true);
        assert!(work.regions > 0);
        assert_eq!(work.budget_fallbacks, 0);
    }

    #[test]
    fn genuinely_disconnected_region_keeps_the_explicit_fallback_domain() {
        let work = exercise_domain(
            "SELECT s_acctbal,r_name FROM supplier CROSS JOIN region",
            10000,
            true,
        );
        assert_eq!(work.regions, 0);
    }

    #[test]
    fn joint_states_explore_multiple_aggregation_cuts_without_tree_copies() {
        let work = exercise(QUERY, 10000);
        assert_eq!(work.regions, 1);
        assert!(
            work.partial_states >= 2,
            "must consider more than a single dimension-deferral tree"
        );
        assert!(work.transitions > work.partial_states);
        assert_eq!(work.budget_fallbacks, 0);
    }

    #[test]
    fn joint_budget_retains_original_instead_of_claiming_infeasibility() {
        let work = exercise(QUERY, 0);
        assert_eq!(work.regions, 0);
        assert_eq!(work.transitions, 0);
        assert_eq!(work.budget_fallbacks, 1);
    }

    #[test]
    fn unsupported_aggregate_laws_do_not_enter_joint_search() {
        let work = exercise("SELECT n_name,r_name,count(DISTINCT s_acctbal) FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey GROUP BY n_name,r_name", 10000);
        assert_eq!(work.regions, 0);
        assert_eq!(work.transitions, 0);
    }
}
