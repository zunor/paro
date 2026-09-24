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
use paro_planner::expression::{AggregateExpression, ColumnRefExpression, Expression};
use paro_planner::operator::bound_reference::{BoundRelationFactValues, BoundRelationFacts};
use paro_planner::operator::{
    Aggregate, BoundReference, BoundReferenceId, ColumnBinding, ComparisonJoin, Join,
    JoinBuildSideConstraint, JoinCondition, JoinType, LogicalOperator, LogicalOutputLayout,
};
use paro_planner::plan::{arena::LogicalPlanNode, OwnedLogicalPlan};

use crate::aggregate::dimension_deferral::{
    inline_projections, join_region::is_plain_inner_equi_join, partial_merge,
};
use crate::context::{OptimizationContext, SharedColumnStatistics};
use crate::expression::traversal::visit_expression;
use crate::physical::{direct, ObjectiveProfile, PhysicalImplementationFlavor, ResourceGrantClass};
use crate::statistics::gathering::StatisticsGathering;

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
    columns: SharedColumnStatistics,
    response: direct::Completed,
    implementation: PhysicalImplementationFlavor,
    /// Original SQL bindings carried by the current partial grain.
    rebind: BTreeMap<ColumnBinding, ColumnBinding>,
    partial_aggregate_index: Option<usize>,
    boundary: BoundReference,
}

impl Node {
    fn boundary(&self, ordinal: usize) -> Result<Box<OwnedLogicalPlan>> {
        let mut reference = self.boundary.clone();
        reference.reference_id = BoundReferenceId::input_ordinal(ordinal);
        Ok(Box::new(OwnedLogicalPlan {
            id: self.shell.id,
            stats: self.shell.stats.clone(),
            operator: LogicalOperator::BoundReference(reference),
        }))
    }
}

fn boundary(
    plan: &OwnedLogicalPlan,
    layout: &LogicalOutputLayout,
    response: &direct::Completed,
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
            contains_control_region:
                crate::join::build_probe_side::contains_control_region_boundary(plan),
            ..Default::default()
        },
        layout.types().to_vec(),
    )))
}

#[derive(Default)]
pub(super) struct Work {
    pub regions: u64,
    pub transitions: u64,
    pub partial_states: u64,
    pub budget_fallbacks: u64,
    pub selected_partial: u64,
}

struct Region<'a> {
    aggregate: Option<Aggregate<()>>,
    leaves: Vec<&'a OwnedLogicalPlan>,
    conditions: Vec<JoinCondition>,
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
    });
    valid.then_some(result)
}

fn movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
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
        _ => leaves.push(plan),
    }
}

impl<'a> Region<'a> {
    fn recognize(plan: &'a OwnedLogicalPlan) -> Option<Self> {
        let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
            return None;
        };
        if !crate::aggregate::dimension_deferral::root_eligible(aggregate)
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
            if crate::expression::join_tree_has_evaluation_fence(join) {
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
        Some(Self {
            aggregate: Some(aggregate),
            leaves,
            conditions,
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
                LogicalOperator::Join(Join::Comparison(j)) if is_plain_inner_equi_join(j) => {
                    collect(&j.left, leaves, conditions, residuals);
                    collect(&j.right, leaves, conditions, residuals);
                    conditions.extend(j.conditions.iter().cloned());
                }
                _ => leaves.push(plan),
            }
        }
        if !join_root(plan) {
            return None;
        }
        let mut leaves = vec![];
        let mut conditions = vec![];
        let mut predicates = vec![];
        collect(plan, &mut leaves, &mut conditions, &mut predicates);
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
        if conditions.iter().any(|c| !matches!((&c.left, &c.right), (Expression::ColumnRef(l), Expression::ColumnRef(r))
            if l.depth == 0 && r.depth == 0 && owners.contains_key(&l.binding) && owners.contains_key(&r.binding))) { return None; }
        let residuals = predicates
            .into_iter()
            .map(|expression| {
                if !movable(&expression) {
                    return None;
                }
                let mut support = 0;
                for binding in columns(&expression)? {
                    support |= *owners.get(&binding)?;
                }
                Some((support, expression))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            aggregate: None,
            leaves,
            conditions,
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
            .filter_map(|c| {
                let (Expression::ColumnRef(l), Expression::ColumnRef(r)) = (&c.left, &c.right)
                else {
                    return None;
                };
                let l = self.owners[&l.binding];
                let r = self.owners[&r.binding];
                if l & left != 0 && r & right != 0 {
                    Some(c.clone())
                } else if r & left != 0 && l & right != 0 {
                    let mut c = c.clone();
                    std::mem::swap(&mut c.left, &mut c.right);
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
    calibration: &'a crate::cascades::calibration::MachineCalibrationBundle,
    work: &'a mut Work,
    remaining: usize,
    exhausted: bool,
}

impl Planner<'_> {
    fn emit(
        &mut self,
        operator: LogicalOperator,
        children: Vec<Arc<Node>>,
    ) -> Result<Option<Arc<Node>>> {
        self.context.session.cancellation.check()?;
        if self.remaining == 0 {
            self.exhausted = true;
            return Ok(None);
        }
        self.remaining -= 1;
        self.work.transitions += 1;
        let mut columns = HashMap::new();
        for child in &children {
            columns.extend(child.columns.iter().map(|(k, v)| (*k, v.clone())));
        }
        let mut propagator =
            crate::statistics::propagator::StatisticsPropagator::with_statistics_map(columns);
        let operator = propagator.propagate_operator(&self.context.session, operator);
        self.context.column_stats = Arc::new(propagator.take_statistics_map());
        let layouts = children
            .iter()
            .map(|c| c.layout.clone())
            .collect::<Vec<_>>();
        let bounds = children
            .iter()
            .map(|c| c.response.hard_rows)
            .collect::<Vec<_>>();
        let plan = OwnedLogicalPlan::new(&self.context.bind_context, operator);
        let (plan, layout, _) = self.gathering.gather_local(
            plan,
            &layouts,
            &bounds,
            self.context.column_stats.clone(),
            &mut self.context,
        );
        let responses = children.iter().map(|c| &c.response).collect::<Vec<_>>();
        let Some(selected) = direct::select_local(
            &plan,
            &self.context.column_stats,
            &responses,
            self.grant,
            self.calibration,
            &self.context.session,
        )?
        else {
            return Ok(None);
        };
        let columns = Arc::new(
            layout
                .bindings()
                .iter()
                .filter_map(|binding| {
                    self.context
                        .column_stats
                        .get(binding)
                        .map(|s| (*binding, s.clone()))
                })
                .collect(),
        );
        let boundary = boundary(&plan, &layout, &selected.response)?;
        let (shell, _) = LogicalPlanNode::detach(plan);
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
        let groups = keys
            .iter()
            .map(|binding| {
                Expression::ColumnRef(
                    ColumnRefExpression::new(*binding, region.types[binding].clone()).into(),
                )
            })
            .collect();
        let aggregate = Aggregate::new(
            group_index,
            aggregate_index,
            groupings_index,
            *input.boundary(0)?,
            groups,
            vec![],
            aggregate.aggregates.clone(),
            vec![],
        );
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
            let mut filter = paro_planner::operator::Filter::new(*input.boundary(0)?, vec![]);
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

pub(super) struct Selection {
    pub plan: OwnedLogicalPlan,
    pub columns: HashMap<ColumnBinding, Arc<paro_storage::statistics::ColumnStatistics>>,
}

pub(super) fn optimize(
    plan: &OwnedLogicalPlan,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cascades::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
) -> Result<Option<Selection>> {
    let Some(region) = Region::recognize(plan) else {
        return Ok(None);
    };
    optimize_region(region, context, grant, calibration, max_transitions, work)
}

pub(super) fn join_root(plan: &OwnedLogicalPlan) -> bool {
    match &plan.operator {
        LogicalOperator::Join(Join::Comparison(j)) => is_plain_inner_equi_join(j),
        LogicalOperator::Filter(f) => f.expressions.iter().all(movable) && join_root(&f.child),
        _ => false,
    }
}

pub(super) fn optimize_joins(
    plan: &OwnedLogicalPlan,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cascades::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
) -> Result<Option<Selection>> {
    let Some(region) = Region::recognize_joins(plan) else {
        return Ok(None);
    };
    optimize_region(region, context, grant, calibration, max_transitions, work)
}

fn optimize_region(
    region: Region<'_>,
    context: &OptimizationContext,
    grant: ResourceGrantClass,
    calibration: &crate::cascades::calibration::MachineCalibrationBundle,
    max_transitions: usize,
    work: &mut Work,
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
    let start = planner.work.transitions;
    let mut states = vec![BTreeMap::new(); 1 << region.leaves.len()];
    for (index, leaf) in region.leaves.iter().enumerate() {
        let Some(response) = direct::estimate_response(
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
        let columns = Arc::new(
            layout
                .bindings()
                .iter()
                .filter_map(|b| context.column_stats.get(b).map(|s| (*b, s.clone())))
                .collect(),
        );
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
            let filter = paro_planner::operator::Filter::new(*node.boundary(0)?, residuals);
            let Some(filtered) = planner.emit(LogicalOperator::Filter(filter), vec![node])? else {
                planner.work.budget_fallbacks += u64::from(planner.exhausted);
                return Ok(None);
            };
            node = filtered;
        }
        states[1 << index].insert(None, node);
    }
    let full: Mask = (1 << region.leaves.len()) - 1;
    for size in 1..=region.leaves.len() {
        for mask in 1..=full {
            if mask.count_ones() as usize != size {
                continue;
            }
            if size > 1 {
                let mut left = (mask - 1) & mask;
                while left != 0 {
                    let right = mask ^ left;
                    if left < right {
                        let conditions = region.cut(left, right);
                        if !conditions.is_empty() {
                            let left_states = states[left as usize]
                                .iter()
                                .map(|(k, n)| (*k, n.clone()))
                                .collect::<Vec<_>>();
                            let right_states = states[right as usize]
                                .iter()
                                .map(|(k, n)| (*k, n.clone()))
                                .collect::<Vec<_>>();
                            for (lk, l) in &left_states {
                                for (rk, r) in &right_states {
                                    if lk.is_some() && rk.is_some() {
                                        continue;
                                    }
                                    if planner.work.transitions - start >= max_transitions as u64 {
                                        planner.work.budget_fallbacks += 1;
                                        return Ok(None);
                                    }
                                    let conditions = conditions
                                        .iter()
                                        .cloned()
                                        .map(|mut c| {
                                            c.left = rebind(c.left, &l.rebind);
                                            c.right = rebind(c.right, &r.rebind);
                                            c
                                        })
                                        .collect();
                                    let join = ComparisonJoin::new(
                                        JoinType::Inner,
                                        *l.boundary(0)?,
                                        *r.boundary(1)?,
                                        conditions,
                                    );
                                    if let Some(mut node) = planner.emit(
                                        LogicalOperator::Join(Join::Comparison(join)),
                                        vec![l.clone(), r.clone()],
                                    )? {
                                        let residuals = region
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
                                        if !residuals.is_empty() {
                                            let filter = paro_planner::operator::Filter::new(
                                                *node.boundary(0)?,
                                                residuals,
                                            );
                                            let Some(filtered) = planner.emit(
                                                LogicalOperator::Filter(filter),
                                                vec![node],
                                            )?
                                            else {
                                                continue;
                                            };
                                            node = filtered;
                                        }
                                        retain(
                                            &mut states,
                                            StateKey {
                                                relations: mask,
                                                partial: lk.or(*rk),
                                            },
                                            node,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    left = (left - 1) & mask;
                }
            }
            if allow_partial
                && region.aggregate.is_some()
                && mask != full
                && region.arguments & mask == region.arguments
            {
                if let Some(raw) = states[mask as usize].get(&None).cloned() {
                    if let Some(partial) = planner.partial(&region, mask, raw)? {
                        states[mask as usize].insert(Some(mask), partial);
                    }
                }
            }
        }
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
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::Optimizer;
    use paro_planner::planner::Planner;

    fn exercise(sql: &str, budget: usize) -> Work {
        exercise_domain(sql, budget, false)
    }

    fn exercise_domain(sql: &str, budget: usize, ordinary: bool) -> Work {
        let session = crate::subquery::partition_aggregate_tests::setup_session();
        let mut planner = Planner::new(session.clone());
        planner
            .create_plan(paro_parser::parse_one(sql).unwrap().stmt)
            .unwrap();
        let query = planner.take_plan().unwrap();
        let mut optimizer = Optimizer::new(planner.binder.clone(), session);
        let query = optimizer.canonicalize_query(query).unwrap();
        let candidate = optimizer.settle_relational_baseline(query).unwrap();
        optimizer.ctx.column_stats = candidate.column_stats;
        let grant = ResourceGrantClass {
            id: crate::physical::ResourceGrantClassId(0),
            hard_memory_bytes: 2 * 1024 * 1024 * 1024,
            spill_policy: crate::physical::SpillPolicy::Allowed,
            max_parallel_tasks: 4,
        };
        let mut work = Work::default();
        candidate
            .plan
            .try_visit_pre_order(|plan| {
                let optimize = if ordinary { optimize_joins } else { optimize };
                if let Some(selected) = optimize(
                    plan,
                    &optimizer.ctx,
                    grant,
                    &optimizer.calibration,
                    budget,
                    &mut work,
                )? {
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
    fn ordinary_regions_use_physical_response_and_preserve_output_bindings() {
        let work = exercise_domain("SELECT r_name,n_name,s_acctbal FROM supplier JOIN nation ON s_nationkey=n_nationkey JOIN region ON n_regionkey=r_regionkey WHERE s_acctbal > n_nationkey", 10000, true);
        assert!(work.regions > 0);
        assert!(work.transitions > 0);
        assert_eq!(work.partial_states, 0);
        assert_eq!(work.budget_fallbacks, 0);
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
