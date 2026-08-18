// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cost-based join-order optimization.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_catalog::entry::ConstraintType;
use paro_common::error::Result;
use paro_context::StatementContext;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::{
    duplicate_operator_preserving_indices, duplicate_plan_preserving_indices,
};
use paro_planner::expression::{
    ComparisonType, ConjunctionExpression, ConjunctionType, Expression,
};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, CrossProduct, Filter, Join, JoinComparisonType, JoinCondition,
    JoinType, LogicalOperator,
};
use paro_planner::plan::{CardinalityEstimate, CardinalityProvenance, LogicalPlan};
use paro_storage::statistics::{ColumnStatistics, NumericStats};

use crate::cost_model::CostModel as LogicalCostModel;
use crate::join_order::cost_model::{CostModel, DPJoinNode};
use crate::join_order::enumerator::PlanEnumerator;
use crate::join_order::query_graph::{FilterInfo, JoinEdgeOrientation, QueryGraphEdges};
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::join_order::relation_manager::{
    DistinctCount, ExtractedFilter, RelationManager, RelationStats,
};

/// Tight integral-domain upper bound derived from correctness-safe min/max.
///
/// Join ordering uses this only as an NDV estimate. The paired bounds API
/// prevents a partially known statistic from being promoted into a domain.
fn integral_domain_cardinality(stats: &ColumnStatistics) -> Option<usize> {
    let (minimum, maximum) = NumericStats::guaranteed_bounds(stats.statistics())?;
    let minimum = integral_ordinal(&minimum)?;
    let maximum = integral_ordinal(&maximum)?;
    usize::try_from(maximum.checked_sub(minimum)?.checked_add(1)?).ok()
}

fn declared_unique_keys(plan: &LogicalPlan) -> Vec<Vec<ColumnBinding>> {
    let get = match &plan.operator {
        LogicalOperator::Get(get) => get,
        LogicalOperator::Filter(filter) => return declared_unique_keys(&filter.child),
        _ => return Vec::new(),
    };
    let Some(table) = &get.table else {
        return Vec::new();
    };

    table
        .constraints()
        .iter()
        .filter(|constraint| {
            matches!(
                constraint.constraint_type,
                ConstraintType::Unique | ConstraintType::PrimaryKey
            ) && !constraint.columns.is_empty()
        })
        .filter_map(|constraint| {
            constraint
                .columns
                .iter()
                .map(|column_id| {
                    get.column_sources
                        .iter()
                        .position(|source| {
                            matches!(
                                source,
                                paro_planner::operator::GetColumnSource::Stored {
                                    column_id: candidate
                                } if candidate == column_id
                            )
                        })
                        .map(|column_index| ColumnBinding::new(get.table_index, column_index))
                })
                .collect::<Option<Vec<_>>>()
        })
        .collect()
}

fn integral_ordinal(value: &paro_common::runtime_value::Value) -> Option<u128> {
    use paro_common::runtime_value::Value;

    match value {
        Value::Boolean(value) => Some(u128::from(*value)),
        Value::TinyInt(value) => Some(u128::from((*value as u8) ^ (1 << 7))),
        Value::SmallInt(value) => Some(u128::from((*value as u16) ^ (1 << 15))),
        Value::Integer(value) | Value::Date(value) => Some(u128::from((*value as u32) ^ (1 << 31))),
        Value::BigInt(value)
        | Value::Timestamp(value)
        | Value::TimestampTz(value)
        | Value::Time(value) => Some(u128::from((*value as u64) ^ (1 << 63))),
        Value::HugeInt(value) | Value::Decimal(value, ..) => Some((*value as u128) ^ (1 << 127)),
        Value::UTinyInt(value) => Some(u128::from(*value)),
        Value::USmallInt(value) => Some(u128::from(*value)),
        Value::UInteger(value) => Some(u128::from(*value)),
        Value::UBigInt(value) => Some(u128::from(*value)),
        Value::UHugeInt(value) => Some(*value),
        Value::Null(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::Varchar(_)
        | Value::Blob(_)
        | Value::Uuid(_)
        | Value::Interval(_, _, _)
        | Value::List(_, _)
        | Value::Array(_, _, _)
        | Value::Struct(_, _) => None,
    }
}

/// The JoinOrderOptimizer performs cost-based join order optimization.
///
pub struct JoinOrderOptimizer {
    /// The relation manager for tracking relations.
    relation_manager: RelationManager,
    /// The set manager for creating relation sets.
    set_manager: JoinRelationSetManager,
    /// The query graph for storing edges.
    query_graph: QueryGraphEdges,
    /// The cost model for evaluating join costs.
    cost_model: CostModel,
    /// Filter metadata extracted from the original join tree.
    filter_infos: Vec<Arc<FilterInfo>>,
    /// DP plans keyed by relation-set string for recursive reconstruction.
    plans: HashMap<Arc<JoinRelationSet>, DPJoinNode>,
    /// Output-column statistics gathered earlier in the pipeline.
    column_stats: HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    /// Original base-relation subplans keyed by relation id for reconstruction.
    relation_plans: Vec<LogicalPlan>,
}

impl JoinOrderOptimizer {
    /// Create a new JoinOrderOptimizer.
    pub fn new() -> Self {
        Self {
            relation_manager: RelationManager::new(),
            set_manager: JoinRelationSetManager::new(),
            query_graph: QueryGraphEdges::new(),
            cost_model: CostModel::new(),
            filter_infos: Vec::new(),
            plans: HashMap::new(),
            column_stats: HashMap::new(),
            relation_plans: Vec::new(),
        }
    }

    /// Optimize the join order of a logical plan.
    ///
    /// This is the main entry point for join order optimization.
    /// For now, this is a simplified implementation that doesn't traverse
    /// the tree recursively. It only optimizes if the root is a join.
    pub fn optimize(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.optimize_plan(
            ctx,
            LogicalPlan::synthetic(plan),
            &HashMap::new(),
            bind_context,
        )
        .map(|plan| plan.operator)
    }

    pub fn optimize_plan(
        &mut self,
        ctx: &StatementContext,
        plan: LogicalPlan,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
        bind_context: &BindContext,
    ) -> Result<LogicalPlan> {
        self.column_stats = column_stats.clone();
        plan.try_fold_post_order(|plan, child_states: Vec<bool>| {
            let contains_control_region_reference = child_states.into_iter().any(|state| state)
                || matches!(plan.operator, LogicalOperator::CTERef(_));
            self.optimize_current_plan(ctx, bind_context, plan, contains_control_region_reference)
        })
        .map(|(plan, _)| plan)
    }

    /// Keep join-graph extraction and reconstruction isolated from the
    /// explicit traversal state. Those routines own several large planner
    /// values and should not be folded back into the post-order driver.
    fn optimize_current_plan(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: LogicalPlan,
        contains_control_region_reference: bool,
    ) -> Result<(LogicalPlan, bool)> {
        if self.can_optimize_join(&plan.operator, contains_control_region_reference) {
            if let Some(mut optimized) = self.optimize_join_tree(
                ctx,
                bind_context,
                duplicate_plan_preserving_indices(&plan, bind_context.shared().as_ref()),
            )? {
                optimized.id = plan.id;
                return Ok((optimized, contains_control_region_reference));
            }
        }

        Ok((plan, contains_control_region_reference))
    }

    /// Check if a join can be optimized.
    fn can_optimize_join(
        &self,
        plan: &LogicalOperator,
        contains_control_region_reference: bool,
    ) -> bool {
        // A CTE reference is bound to the control region that owns its runtime
        // state. It is therefore a join-reordering boundary, not an ordinary
        // relation leaf.
        if contains_control_region_reference {
            return false;
        }
        match plan {
            LogicalOperator::Join(join) => RelationManager::join_is_reorderable(join),
            // SQL comma joins arrive here as Filter(CrossProduct). The filter
            // contains the actual join edge, so optimizing only the child
            // leaves a Cartesian product followed by an equality filter.
            LogicalOperator::Filter(filter) => {
                !filter
                    .expressions
                    .iter()
                    .any(|expression| expression.evaluation_properties().is_reorder_fence())
                    && self.can_optimize_join(&filter.child.operator, false)
            }
            _ => false,
        }
    }

    /// Optimize a join tree.
    fn optimize_join_tree(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: LogicalPlan,
    ) -> Result<Option<LogicalPlan>> {
        // Reset state
        self.relation_manager = RelationManager::new();
        self.set_manager = JoinRelationSetManager::new();
        self.query_graph = QueryGraphEdges::new();
        self.cost_model.reset();
        self.filter_infos.clear();
        self.plans.clear();
        self.relation_plans.clear();

        // Extract relations and filters from the join tree
        let mut filters = Vec::new();
        self.extract_join_relations(ctx, bind_context, &plan, &mut filters, true)?;

        // Check if we have enough relations to optimize
        if self.relation_manager.num_relations() < 2 {
            return Ok(None);
        }

        // Extract edges from filters
        let extracted_predicates = self
            .relation_manager
            .extract_edges(&filters, &mut self.set_manager);
        let Some(extracted_predicates) = extracted_predicates else {
            return Ok(None);
        };
        let filter_infos = extracted_predicates.graph_filters;
        if filter_infos
            .iter()
            .any(|filter| !filter.has_valid_reduction_roles())
        {
            // The source tree remains available to the post-order driver.
            // Invalid reduction metadata makes this region ineligible for
            // reordering; it must never turn a valid statement into an
            // optimizer-internal query failure.
            return Ok(None);
        }
        self.apply_relation_local_selectivity(&filter_infos);
        self.filter_infos = filter_infos.clone();

        // Initialize cardinality estimator
        self.cost_model
            .cardinality_estimator
            .init_equivalent_relations(&filter_infos);

        // Build query graph
        for filter_info in &filter_infos {
            // Get the left and right sets from the filter
            if let (Some(left_set), Some(right_set)) = (
                filter_info.left_set.as_ref(),
                filter_info.right_set.as_ref(),
            ) {
                self.query_graph.create_edge(
                    left_set,
                    right_set.clone(),
                    Some(filter_info.clone()),
                );
                self.query_graph.create_edge(
                    right_set,
                    left_set.clone(),
                    Some(filter_info.clone()),
                );
            } else if filter_info.set.count() > 1 {
                // Multi-relation filter without explicit left/right
                // Create edges between all pairs
                let relations = filter_info.set.relations();
                for i in 0..relations.len() {
                    for j in (i + 1)..relations.len() {
                        let left = self.set_manager.get_relation(relations[i]);
                        let right = self.set_manager.get_relation(relations[j]);
                        self.query_graph.create_edge(
                            &left,
                            right.clone(),
                            Some(filter_info.clone()),
                        );
                        self.query_graph
                            .create_edge(&right, left, Some(filter_info.clone()));
                    }
                }
            }
        }

        // Initialize cost model
        let stats = self.relation_manager.get_relation_stats();
        self.cost_model
            .init_cost_model(&mut self.set_manager, &stats);

        // Create plan enumerator
        let mut enumerator = PlanEnumerator::new(
            &self.query_graph,
            &mut self.set_manager,
            &mut self.cost_model,
            self.relation_manager.num_relations(),
        );

        // Initialize leaf plans
        enumerator.init_leaf_plans();

        // Solve join order
        if !enumerator.solve_join_order() {
            // Timed out or failed
            return Ok(None);
        }

        // Get the final plan (clone it to avoid borrow issues)
        let final_plan = match enumerator.get_final_plan() {
            Some(plan) => plan.clone(),
            None => return Ok(None),
        };
        self.plans = enumerator.get_plans().clone();

        // Drop enumerator to release mutable borrow
        drop(enumerator);

        // Reconstruct the logical plan
        let Some(reconstructed) =
            self.reconstruct_plan(bind_context, &final_plan, &mut HashSet::new())?
        else {
            return Ok(None);
        };

        Ok(Some(self.attach_filter_expressions(
            reconstructed,
            extracted_predicates.root_filters,
        )))
    }

    /// Fold relation-local predicates into the leaf statistics consumed by DP.
    ///
    /// Filter extraction deliberately separates predicates from their scans so
    /// they can be reattached to the reconstructed tree. Without this step the
    /// enumerator still costs every filtered scan at its base-table cardinality,
    /// hiding selective date/range predicates from join ordering.
    ///
    /// These mutations are private to `RelationManager`, the DP estimator's
    /// cost domain. They are never copied into the retained logical leaf plans:
    /// reconstruction reattaches each predicate exactly once, and the later
    /// statistics-propagation pass remains authoritative for plan annotations.
    fn apply_relation_local_selectivity(&mut self, filters: &[Arc<FilterInfo>]) {
        let mut filters_by_relation = HashMap::<usize, Vec<Expression>>::new();
        for filter in filters {
            if filter.join_type == JoinType::Inner && filter.set.count() == 1 {
                filters_by_relation
                    .entry(filter.set.relations()[0])
                    .or_default()
                    .push(filter.filter.clone());
            }
        }

        let cost_model = LogicalCostModel::default();
        for (relation_id, expressions) in filters_by_relation {
            let Some(relation) = self.relation_manager.get_relation_mut(relation_id) else {
                continue;
            };
            let estimate = cost_model.estimate_filter_cardinality(
                relation.stats.cardinality as u64,
                &expressions,
                &self.column_stats,
            );
            relation.stats.cardinality = estimate.expected.max(1) as usize;
            for distinct in relation.stats.column_distinct_count.values_mut() {
                // Both observed HLL values and synthetic NDV upper bounds are
                // domains of the filtered relation. Neither can exceed its
                // surviving row count.
                distinct.distinct_count = distinct
                    .distinct_count
                    .min(relation.stats.cardinality.max(1));
            }
        }
    }

    /// Extract relations and filters from a join tree.
    fn extract_join_relations(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: &LogicalPlan,
        filters: &mut Vec<ExtractedFilter>,
        at_region_root: bool,
    ) -> Result<()> {
        match &plan.operator {
            LogicalOperator::Join(Join::Comparison(join))
                if matches!(join.join_type, JoinType::Semi | JoinType::Anti) =>
            {
                if at_region_root {
                    if matches!(
                        join.left.operator,
                        LogicalOperator::Join(Join::Comparison(ref child))
                            if matches!(child.join_type, JoinType::Semi | JoinType::Anti)
                    ) {
                        // Consecutive reductions over the same preserved side
                        // are commutative filters. Expose only that cascade as
                        // a join-order region; the preserved input beneath it
                        // has already been optimized recursively and remains
                        // atomic here.
                        self.extract_reduction_cascade(ctx, bind_context, join, filters);
                    } else {
                        // A single reduction retains the established behavior:
                        // its preserved inner-join region may be reordered
                        // around the reduction edge.
                        self.extract_join_relations(ctx, bind_context, &join.left, filters, false)?;
                        self.add_relation_plan(ctx, bind_context, &join.right);
                        Self::extract_comparison_join_filters(join, filters);
                    }
                } else {
                    self.add_relation_plan(ctx, bind_context, plan);
                }
            }
            LogicalOperator::Join(join @ Join::Comparison(_))
                if RelationManager::join_is_reorderable(join) =>
            {
                // Recursively extract from children first so table-index mappings exist
                self.extract_join_relations(ctx, bind_context, join.left(), filters, false)?;
                self.extract_join_relations(ctx, bind_context, join.right(), filters, false)?;
                if let Join::Comparison(join) = join {
                    Self::extract_comparison_join_filters(join, filters);
                }
            }
            LogicalOperator::Join(join @ Join::Cross(_))
                if RelationManager::join_is_reorderable(join) =>
            {
                self.extract_join_relations(ctx, bind_context, join.left(), filters, false)?;
                self.extract_join_relations(ctx, bind_context, join.right(), filters, false)?;
            }
            LogicalOperator::Filter(filter) => {
                // Continue with child
                self.extract_join_relations(
                    ctx,
                    bind_context,
                    filter.child.as_ref(),
                    filters,
                    at_region_root,
                )?;
                filters.extend(
                    filter
                        .expressions
                        .iter()
                        .cloned()
                        .map(ExtractedFilter::inner),
                );
            }
            LogicalOperator::Join(_) => {
                // Outer, semi, anti, and expression joins are not associative
                // members of an inner-join region. Preserve the complete
                // subtree as one relation; its children have already been
                // optimized by `optimize_plan_recursive`.
                self.add_relation_plan(ctx, bind_context, plan);
            }
            _ => {
                // This is a base relation
                if RelationManager::operator_needs_relation(plan.operator.op_type()) {
                    self.add_relation_plan(ctx, bind_context, plan);
                }
            }
        }

        Ok(())
    }

    fn extract_reduction_cascade(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        join: &ComparisonJoin,
        filters: &mut Vec<ExtractedFilter>,
    ) {
        if let LogicalOperator::Join(Join::Comparison(child)) = &join.left.operator {
            if matches!(child.join_type, JoinType::Semi | JoinType::Anti) {
                self.extract_reduction_cascade(ctx, bind_context, child, filters);
            } else {
                self.add_relation_plan(ctx, bind_context, &join.left);
            }
        } else {
            self.add_relation_plan(ctx, bind_context, &join.left);
        }
        self.add_relation_plan(ctx, bind_context, &join.right);
        Self::extract_comparison_join_filters(join, filters);
    }

    fn extract_comparison_join_filters(join: &ComparisonJoin, filters: &mut Vec<ExtractedFilter>) {
        let expressions = join.conditions.iter().map(|condition| {
            let expression =
                Expression::Comparison(paro_planner::expression::ComparisonExpression {
                    left: Box::new(condition.left.clone()),
                    right: Box::new(condition.right.clone()),
                    comparison_type: Self::to_comparison_type(condition.comparison),
                });
            expression
        });
        if matches!(join.join_type, JoinType::Semi | JoinType::Anti) {
            let expressions = expressions.collect::<Vec<_>>();
            let expression = match expressions.as_slice() {
                [] => return,
                [expression] => expression.clone(),
                _ => Expression::Conjunction(ConjunctionExpression::new(
                    ConjunctionType::And,
                    expressions,
                )),
            };
            filters.push(ExtractedFilter::new(
                expression,
                join.join_type,
                join.anti_join_mode,
            ));
        } else {
            filters.extend(expressions.map(ExtractedFilter::inner));
        }
    }

    fn add_relation_plan(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: &LogicalPlan,
    ) {
        let cardinality = self.estimate_cardinality(ctx, plan);
        let mut stats = RelationStats::with_cardinality(cardinality);
        stats.estimated_payload_width =
            crate::join::build_probe_side::estimate_row_payload_width(&plan.types());
        stats.contains_control_region =
            crate::join::build_probe_side::contains_control_region_boundary(plan);
        stats.unique_keys = declared_unique_keys(plan);
        stats.column_distinct_count = plan
            .get_column_bindings()
            .into_iter()
            .map(|binding| {
                let column_stats = self.column_stats.get(&binding);
                let distinct = column_stats
                    .map(|stats| stats.get_distinct_count())
                    .unwrap_or(0);
                let from_hll = distinct > 0;
                let distinct = if from_hll {
                    distinct
                } else {
                    column_stats
                        .and_then(|stats| integral_domain_cardinality(stats))
                        .unwrap_or(cardinality.max(1))
                };
                (
                    binding,
                    DistinctCount::new(
                        // A filter can reduce the relation cardinality without
                        // rewriting base-column HLL or min/max statistics. The
                        // surviving domain cannot contain more values than rows.
                        distinct.min(cardinality.max(1)),
                        from_hll,
                    ),
                )
            })
            .collect();
        self.relation_manager.add_relation(
            duplicate_operator_preserving_indices(&plan.operator, bind_context.shared().as_ref()),
            None,
            stats,
        );
        self.relation_plans.push(duplicate_plan_preserving_indices(
            plan,
            bind_context.shared().as_ref(),
        ));
    }

    /// Estimate the cardinality of a base relation.
    fn estimate_cardinality(&self, ctx: &StatementContext, plan: &LogicalPlan) -> usize {
        if let Some(estimate) = plan.stats.estimated_cardinality {
            return estimate.expected.max(1) as usize;
        }

        match &plan.operator {
            LogicalOperator::Get(get) => {
                if let Some(table) = &get.table {
                    if let Some(storage) = table.get_storage() {
                        let rows = storage.total_rows();
                        if rows == 0 {
                            return 1;
                        }
                        return rows;
                    }
                }

                match ctx.get_setting("default_table_cardinality") {
                    Some(paro_common::runtime_value::Value::BigInt(v)) if *v > 0 => *v as usize,
                    Some(paro_common::runtime_value::Value::Integer(v)) if *v > 0 => *v as usize,
                    _ => 1000,
                }
            }
            LogicalOperator::ExpressionGet(get) => get.expressions.len().max(1),
            LogicalOperator::TableFunctionGet(_) => {
                // Table functions may have cardinality estimates
                // For now, use a default
                100
            }
            _ => {
                // Default estimate
                1000
            }
        }
    }

    /// Reconstruct a logical plan from a DP join node.
    fn reconstruct_plan(
        &self,
        bind_context: &BindContext,
        node: &DPJoinNode,
        used_filters: &mut HashSet<usize>,
    ) -> Result<Option<LogicalPlan>> {
        if node.is_leaf {
            // This is a base relation
            let relation_id = node.set.relations()[0];
            let relation = self.relation_plans.get(relation_id).ok_or_else(|| {
                paro_common::error::internal(format!("Relation {} not found", relation_id))
            })?;

            Ok(Some(self.attach_remaining_filters(
                duplicate_plan_preserving_indices(relation, bind_context.shared().as_ref()),
                &node.set,
                used_filters,
            )))
        } else {
            let left_node = self.lookup_plan(&node.left_set)?;
            let right_node = self.lookup_plan(&node.right_set)?;
            let mut left_set = node.left_set.clone();
            let mut right_set = node.right_set.clone();
            let Some(mut left_plan) =
                self.reconstruct_plan(bind_context, left_node, used_filters)?
            else {
                return Ok(None);
            };
            let Some(mut right_plan) =
                self.reconstruct_plan(bind_context, right_node, used_filters)?
            else {
                return Ok(None);
            };

            let result = if let Some(predicates) = &node.predicates {
                let chosen_join_type = predicates.join_type();
                if let Some(orientation) = predicates.reduction_orientation() {
                    if orientation == JoinEdgeOrientation::Inverted {
                        std::mem::swap(&mut left_plan, &mut right_plan);
                        std::mem::swap(&mut left_set, &mut right_set);
                    }
                }

                let mut join = ComparisonJoin::new(chosen_join_type, left_plan, right_plan, vec![]);
                join.anti_join_mode = predicates.anti_join_mode();
                for predicate in predicates.predicates() {
                    let appended = self.append_join_conditions(&mut join, predicate);
                    if appended {
                        used_filters.insert(predicate.filter().filter_index);
                    } else if predicates.reduction_orientation().is_some() {
                        // The original logical tree is still owned by the
                        // caller. A graph witness that no longer reconstructs
                        // makes this region ineligible for reordering; it must
                        // never turn a valid statement into an internal error.
                        return Ok(None);
                    }
                }

                if join.conditions.is_empty() {
                    debug_assert!(predicates.reduction_orientation().is_none());
                    let mut plan =
                        LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct {
                            left: join.left,
                            right: join.right,
                        })));
                    plan.stats.estimated_cardinality =
                        Some(Self::join_cardinality_estimate(node.cardinality));
                    plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
                    plan
                } else {
                    let mut plan =
                        LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
                    plan.stats.estimated_cardinality =
                        Some(Self::join_cardinality_estimate(node.cardinality));
                    plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
                    plan
                }
            } else {
                let mut plan =
                    LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                    })));
                plan.stats.estimated_cardinality =
                    Some(Self::join_cardinality_estimate(node.cardinality));
                plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
                plan
            };

            Ok(Some(self.attach_remaining_filters(
                duplicate_plan_preserving_indices(&result, bind_context.shared().as_ref()),
                &node.set,
                used_filters,
            )))
        }
    }

    fn lookup_plan(&self, set: &Arc<JoinRelationSet>) -> Result<&DPJoinNode> {
        self.plans.get(set).ok_or_else(|| {
            paro_common::error::internal(format!(
                "Join order optimizer could not find plan for set {}",
                set
            ))
        })
    }

    fn join_cardinality_estimate(cardinality: f64) -> CardinalityEstimate {
        let expected = if !cardinality.is_finite() || cardinality >= u64::MAX as f64 {
            u64::MAX
        } else {
            cardinality.max(1.0) as u64
        };
        CardinalityEstimate::exact(expected)
    }

    fn attach_remaining_filters(
        &self,
        mut result: LogicalPlan,
        result_set: &Arc<JoinRelationSet>,
        used_filters: &mut HashSet<usize>,
    ) -> LogicalPlan {
        let logical_cost_model = LogicalCostModel::default();
        let mut expressions = Vec::new();
        let mut filter_indexes = Vec::new();
        for filter in &self.filter_infos {
            if used_filters.contains(&filter.filter_index) {
                continue;
            }
            if filter.set.count() > 0 && result_set.contains_all(&filter.set) {
                expressions.push(filter.filter.clone());
                filter_indexes.push(filter.filter_index);
            }
        }
        result = self.attach_filter_expressions_with_cost_model(
            result,
            expressions,
            &logical_cost_model,
        );
        used_filters.extend(filter_indexes);
        result
    }

    fn attach_filter_expressions(
        &self,
        result: LogicalPlan,
        expressions: Vec<Expression>,
    ) -> LogicalPlan {
        self.attach_filter_expressions_with_cost_model(
            result,
            expressions,
            &LogicalCostModel::default(),
        )
    }

    fn attach_filter_expressions_with_cost_model(
        &self,
        mut result: LogicalPlan,
        expressions: Vec<Expression>,
        cost_model: &LogicalCostModel,
    ) -> LogicalPlan {
        if expressions.is_empty() {
            return result;
        }
        let estimated_cardinality = result.stats.estimated_cardinality.map(|estimate| {
            cost_model.estimate_filter_cardinality(
                estimate.expected,
                &expressions,
                &self.column_stats,
            )
        });
        result = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(result, expressions)));
        result.stats.estimated_cardinality = estimated_cardinality;
        result.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
        result
    }

    fn append_join_conditions(
        &self,
        join: &mut ComparisonJoin,
        predicate: &crate::join_order::query_graph::OrientedJoinPredicate,
    ) -> bool {
        let filter = predicate.filter();
        let start_len = join.conditions.len();
        match &filter.filter {
            Expression::Comparison(comp) => {
                if let Some(condition) = Self::comparison_to_join_condition(comp, predicate) {
                    join.conditions.push(condition);
                }
            }
            Expression::Conjunction(conj) => {
                for child in &conj.children {
                    let Expression::Comparison(comp) = child else {
                        continue;
                    };
                    if let Some(condition) = Self::comparison_to_join_condition(comp, predicate) {
                        join.conditions.push(condition);
                    }
                }
            }
            _ => {}
        }
        join.conditions.len() > start_len
    }

    fn comparison_to_join_condition(
        comparison: &paro_planner::expression::ComparisonExpression,
        predicate: &crate::join_order::query_graph::OrientedJoinPredicate,
    ) -> Option<JoinCondition> {
        let invert = predicate.orientation()? == JoinEdgeOrientation::Inverted;
        let comparison_type = Self::to_join_comparison_type(comparison.comparison_type)?;
        Some(JoinCondition::new(
            if invert {
                (*comparison.right).clone()
            } else {
                (*comparison.left).clone()
            },
            if invert {
                (*comparison.left).clone()
            } else {
                (*comparison.right).clone()
            },
            if invert {
                comparison_type.flip()
            } else {
                comparison_type
            },
        ))
    }

    fn to_comparison_type(comparison: JoinComparisonType) -> ComparisonType {
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

    fn to_join_comparison_type(comparison: ComparisonType) -> Option<JoinComparisonType> {
        Some(match comparison {
            ComparisonType::Equal => JoinComparisonType::Equal,
            ComparisonType::NotEqual => JoinComparisonType::NotEqual,
            ComparisonType::LessThan => JoinComparisonType::LessThan,
            ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
            ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
            ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
            ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
            ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
        })
    }
}

impl Default for JoinOrderOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::join::build_probe_side::BuildProbeSideOptimizer;
    use crate::join_order::cardinality::CardinalityEstimator;
    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
    };
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_function::scalar::FunctionStability;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ColumnRefExpression, ConstantExpression, FunctionExpression, ReferenceExpression,
    };
    use paro_planner::operator::{
        AntiJoinMode, AnyJoin, ColumnBinding, ExpressionGet, Get, Projection,
    };
    use paro_planner::plan::{CardinalityEstimate, NodeStats};
    use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
    use paro_storage::statistics::{BaseStatistics, ColumnStatistics};
    use paro_storage::table::table_factory::TableFactory;

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn create_scan(table_index: usize) -> LogicalOperator {
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            Vec::new(),
            vec!["id".to_string()],
            vec![LogicalType::Integer],
        ))
    }

    fn column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: ColumnBinding::new(table_index, column_index),
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn join_condition(
        comparison: JoinComparisonType,
        left_table: usize,
        right_table: usize,
    ) -> JoinCondition {
        JoinCondition::new(
            column_ref(left_table, 0),
            column_ref(right_table, 0),
            comparison,
        )
    }

    fn cross_product(left_table: usize, right_table: usize) -> LogicalPlan {
        LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
            LogicalPlan::synthetic(create_scan(left_table)),
            LogicalPlan::synthetic(create_scan(right_table)),
        ))))
    }

    fn volatile_boolean() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload")
            .with_stability(FunctionStability::Volatile);
        Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::Function(FunctionExpression::new(
                function,
                Vec::new(),
                LogicalType::Double,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Double(0.5),
                LogicalType::Double,
            )),
        ))
    }

    fn count_cross_products(plan: &LogicalOperator) -> usize {
        let self_count = matches!(plan, LogicalOperator::Join(Join::Cross(_))) as usize;
        self_count
            + plan
                .children()
                .into_iter()
                .map(|child| count_cross_products(&child.operator))
                .sum::<usize>()
    }

    fn projection_relation(
        bind_context: &BindContext,
        input_table_index: usize,
        output_table_index: usize,
        rows: u64,
    ) -> LogicalPlan {
        let input = LogicalPlan {
            id: bind_context.next_plan_id(),
            stats: NodeStats {
                estimated_cardinality: Some(CardinalityEstimate::exact(rows)),
                ..NodeStats::default()
            },
            operator: LogicalOperator::ExpressionGet(ExpressionGet::new(
                input_table_index,
                Vec::new(),
                vec!["id".to_string()],
                vec![LogicalType::Integer],
            )),
        };

        LogicalPlan {
            id: bind_context.next_plan_id(),
            stats: NodeStats {
                estimated_cardinality: Some(CardinalityEstimate::exact(rows)),
                ..NodeStats::default()
            },
            operator: LogicalOperator::Projection(Projection::new(
                output_table_index,
                input,
                vec![column_ref(input_table_index, 0)],
            )),
        }
    }

    #[test]
    fn optimize_reconstructs_comparison_join_with_original_predicate() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::GreaterThan, 0, 1)],
        )));

        let bind_context = BindContext::new();
        let optimized = optimizer.optimize(&session, &bind_context, plan).unwrap();

        match optimized {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Inner);
                assert_eq!(join.conditions.len(), 1);
                assert_eq!(
                    join.conditions[0].comparison,
                    JoinComparisonType::GreaterThan
                );
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn optimize_converts_filtered_cross_product_to_comparison_join() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let cross = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
        ))));
        let equality = Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
            ComparisonType::Equal,
            column_ref(0, 0),
            column_ref(1, 0),
        ));
        let plan = LogicalOperator::Filter(Filter::new(cross, vec![equality]));

        let bind_context = BindContext::new();
        let optimized = optimizer.optimize(&session, &bind_context, plan).unwrap();

        let LogicalOperator::Join(Join::Comparison(join)) = optimized else {
            panic!("expected filtered cross product to become a comparison join");
        };
        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(join.conditions.len(), 1);
        assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);
    }

    #[test]
    fn filtered_relation_hll_domain_is_bounded_by_its_cardinality() {
        use std::hash::{DefaultHasher, Hash, Hasher};

        let session = make_test_session();
        let bind_context = BindContext::new();
        let plan = projection_relation(&bind_context, 100, 0, 9);
        let hashes = (0_u64..100)
            .map(|value| {
                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                hasher.finish()
            })
            .collect::<Vec<_>>();
        let mut column_stats = ColumnStatistics::new(BaseStatistics::new(LogicalType::Integer));
        column_stats.update_distinct_statistics(&hashes, hashes.len());
        assert!(column_stats.get_distinct_count() > 9);

        let mut optimizer = JoinOrderOptimizer::new();
        optimizer
            .column_stats
            .insert(ColumnBinding::new(0, 0), Arc::new(column_stats));
        optimizer.add_relation_plan(&session, &bind_context, &plan);

        let stats = optimizer.relation_manager.get_relation_stats();
        assert_eq!(stats[0].cardinality, 9);
        let distinct_count = stats[0]
            .column_distinct_count
            .get(&ColumnBinding::new(0, 0))
            .expect("projection column should retain its binding-keyed statistics");
        assert_eq!(distinct_count.distinct_count, 9);
        assert!(distinct_count.from_hll);
    }

    #[test]
    fn filtered_relation_synthetic_domain_is_bounded_by_its_cardinality() {
        let session = make_test_session();
        let bind_context = BindContext::new();
        let plan = projection_relation(&bind_context, 100, 0, 100);
        let mut optimizer = JoinOrderOptimizer::new();
        optimizer.add_relation_plan(&session, &bind_context, &plan);

        let predicate =
            Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
                ComparisonType::LessThan,
                column_ref(0, 0),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(10),
                    LogicalType::Integer,
                )),
            ));
        let filter = Arc::new(FilterInfo::new_inner(
            predicate,
            Arc::new(JoinRelationSet::single(0)),
            0,
        ));
        optimizer.apply_relation_local_selectivity(&[filter]);

        let stats = optimizer.relation_manager.get_relation_stats();
        let distinct_count = stats[0]
            .column_distinct_count
            .get(&ColumnBinding::new(0, 0))
            .expect("projection column should retain synthetic statistics");
        assert!(!distinct_count.from_hll);
        assert_eq!(distinct_count.distinct_count, stats[0].cardinality);
        assert!(stats[0].cardinality < 100);
    }

    #[test]
    fn integral_min_max_domain_is_used_without_hll() {
        let session = make_test_session();
        let bind_context = BindContext::new();
        let plan = projection_relation(&bind_context, 100, 0, 1_000);
        let mut base = NumericStats::create_unknown(LogicalType::Integer);
        NumericStats::set_guaranteed_min(&mut base, &Value::Integer(-12));
        NumericStats::set_guaranteed_max(&mut base, &Value::Integer(12));

        let mut optimizer = JoinOrderOptimizer::new();
        optimizer.column_stats.insert(
            ColumnBinding::new(0, 0),
            Arc::new(ColumnStatistics::new(base)),
        );
        optimizer.add_relation_plan(&session, &bind_context, &plan);

        let stats = optimizer.relation_manager.get_relation_stats();
        let distinct_count = stats[0]
            .column_distinct_count
            .get(&ColumnBinding::new(0, 0))
            .expect("projection column should retain its min/max domain");
        assert_eq!(distinct_count.distinct_count, 25);
        assert!(!distinct_count.from_hll);
    }

    #[test]
    fn integral_domain_cardinality_rejects_unrepresentable_full_u128_range() {
        let mut base = NumericStats::create_unknown(LogicalType::UHugeInt);
        NumericStats::set_guaranteed_min(&mut base, &Value::UHugeInt(0));
        NumericStats::set_guaranteed_max(&mut base, &Value::UHugeInt(u128::MAX));
        let stats = ColumnStatistics::new(base);

        assert_eq!(integral_domain_cardinality(&stats), None);
    }

    #[test]
    fn persisted_composite_key_reaches_joint_domain_estimation() {
        static NEXT_META_ROOT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "paro_optimizer_unique_key_{}_{}",
            std::process::id(),
            NEXT_META_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(root.join("meta")).unwrap());
        let meta_manager = Arc::new(TabletMetaManager::with_store_and_data_root(store, &root));
        let types = vec![LogicalType::Integer; 3];
        let columns = (0..3)
            .map(|index| ColumnDefinition::new(format!("c{index}"), LogicalType::Integer))
            .collect::<Vec<_>>();
        let info = CreateTableInfo::new(
            "main".to_string(),
            "public".to_string(),
            "composite_key".to_string(),
            columns,
        )
        .with_constraints(vec![Constraint::unique(vec![0, 2])]);
        let entry = TableCatalogEntry::from_info(
            info,
            Arc::new(
                TableFactory::new(Some(Arc::clone(&meta_manager)))
                    .create_table(&types)
                    .unwrap(),
            ),
            CatalogObjectId::from_raw(42),
            0,
        )
        .unwrap();
        let restored = Arc::new(
            TableCatalogEntry::deserialize(
                &entry.serialize().unwrap(),
                "main".to_string(),
                Some(meta_manager),
            )
            .unwrap(),
        );

        // Project columns in a different order to verify that catalog column
        // IDs become output bindings before entering relation statistics.
        let mut get = Get::new(
            40,
            vec!["c2".to_string(), "c0".to_string(), "c1".to_string()],
            types.clone(),
            restored,
        );
        get.column_sources = vec![2, 0, 1]
            .into_iter()
            .map(|column_id| paro_planner::operator::GetColumnSource::Stored { column_id })
            .collect();
        let bind_context = BindContext::new();
        let plan = LogicalPlan {
            id: bind_context.next_plan_id(),
            stats: NodeStats {
                estimated_cardinality: Some(CardinalityEstimate::exact(100)),
                ..NodeStats::default()
            },
            operator: LogicalOperator::Get(get),
        };

        let mut optimizer = JoinOrderOptimizer::new();
        optimizer.add_relation_plan(&make_test_session(), &bind_context, &plan);
        let mut left_stats = optimizer.relation_manager.get_relation_stats()[0].clone();
        assert_eq!(
            left_stats.unique_keys,
            vec![vec![ColumnBinding::new(40, 1), ColumnBinding::new(40, 0)]]
        );
        left_stats.column_distinct_count = HashMap::from([
            (ColumnBinding::new(40, 0), DistinctCount::new(10, true)),
            (ColumnBinding::new(40, 1), DistinctCount::new(10, true)),
        ]);

        let mut set_manager = JoinRelationSetManager::new();
        let filters = [(1, 0), (0, 1)]
            .into_iter()
            .enumerate()
            .map(|(filter_index, (left_column, right_column))| {
                let left_binding = ColumnBinding::new(40, left_column);
                let right_binding = ColumnBinding::new(50, right_column);
                let expression =
                    Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
                        ComparisonType::Equal,
                        Expression::ColumnRef(ColumnRefExpression::new(
                            left_binding,
                            LogicalType::Integer,
                        )),
                        Expression::ColumnRef(ColumnRefExpression::new(
                            right_binding,
                            LogicalType::Integer,
                        )),
                    ));
                let mut filter = FilterInfo::new_inner(
                    expression,
                    set_manager.get_relation_from_vec(vec![0, 1]),
                    filter_index,
                );
                filter.set_left_set(set_manager.get_relation(0));
                filter.set_right_set(set_manager.get_relation(1));
                filter.set_left_binding(left_binding, 0);
                filter.set_right_binding(right_binding, 1);
                Arc::new(filter)
            })
            .collect::<Vec<_>>();
        let mut right_stats = RelationStats::with_cardinality(100);
        right_stats.column_distinct_count = HashMap::from([
            (ColumnBinding::new(50, 0), DistinctCount::new(10, true)),
            (ColumnBinding::new(50, 1), DistinctCount::new(10, true)),
        ]);

        let mut estimator = CardinalityEstimator::new();
        estimator.init_equivalent_relations(&filters);
        estimator.init_cardinality_estimator_props(&set_manager.get_relation(0), &left_stats);
        estimator.init_cardinality_estimator_props(&set_manager.get_relation(1), &right_stats);

        // Marginal statistics alone retain only one NDV=10 factor for the
        // correlated pair. The persisted composite key supplies the exact
        // joint domain of 100, yielding 100 * 100 / 100 rows.
        assert_eq!(
            estimator.estimate_cardinality(&set_manager.get_relation_from_vec(vec![0, 1])),
            100.0
        );
    }

    #[test]
    fn reconstructed_join_cardinality_is_never_quantized_to_zero() {
        let estimate = JoinOrderOptimizer::join_cardinality_estimate(0.125);
        assert_eq!(estimate, CardinalityEstimate::exact(1));
    }

    #[test]
    fn optimize_preserves_relation_independent_filter_above_reordered_join() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let equality = Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
            ComparisonType::Equal,
            column_ref(0, 0),
            column_ref(1, 0),
        ));
        let constant_false = Expression::Constant(ConstantExpression::new(
            Value::Boolean(false),
            LogicalType::Boolean,
        ));
        let plan = LogicalOperator::Filter(Filter::new(
            cross_product(0, 1),
            vec![equality, constant_false.clone()],
        ));

        let optimized = optimizer
            .optimize(&session, &BindContext::new(), plan)
            .unwrap();

        let LogicalOperator::Filter(filter) = optimized else {
            panic!("relation-independent predicate must remain a filter");
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(filter.expressions[0].equals(&constant_false));
        assert!(matches!(
            filter.child.operator,
            LogicalOperator::Join(Join::Comparison(_))
        ));
    }

    #[test]
    fn optimizer_keeps_original_tree_for_unmapped_or_bound_references() {
        let session = make_test_session();
        let plans = [
            Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(99, 0),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                )),
            )),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean)),
        ];

        for predicate in plans {
            let original = predicate.clone();
            let plan = LogicalOperator::Filter(Filter::new(cross_product(0, 1), vec![predicate]));
            let optimized = JoinOrderOptimizer::new()
                .optimize(&session, &BindContext::new(), plan)
                .unwrap();
            let LogicalOperator::Filter(filter) = optimized else {
                panic!("unsafe predicate must keep its filter wrapper");
            };
            assert_eq!(filter.expressions.len(), 1);
            assert!(filter.expressions[0].equals(&original));
        }
    }

    #[test]
    fn volatile_filter_is_a_join_reordering_fence() {
        let predicate = volatile_boolean();
        let plan =
            LogicalOperator::Filter(Filter::new(cross_product(0, 1), vec![predicate.clone()]));
        assert!(!JoinOrderOptimizer::new().can_optimize_join(&plan, false));

        let optimized = JoinOrderOptimizer::new()
            .optimize(&make_test_session(), &BindContext::new(), plan)
            .unwrap();
        let LogicalOperator::Filter(filter) = optimized else {
            panic!("volatile predicate must keep its evaluation boundary");
        };
        assert!(filter.expressions[0].equals(&predicate));

        let nested_filter = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            LogicalPlan::synthetic(create_scan(0)),
            vec![volatile_boolean()],
        )));
        let surrounding_join = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            nested_filter,
            LogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        )));
        assert!(!JoinOrderOptimizer::new().can_optimize_join(&surrounding_join, false));
    }

    #[test]
    fn filtered_cte_join_is_not_reordered_out_of_its_control_region() {
        let cte_ref = LogicalPlan::synthetic(LogicalOperator::CTERef(
            paro_planner::operator::CTERef::new(
                12,
                30,
                vec!["id".to_string()],
                vec![LogicalType::Integer],
            ),
        ));
        let cross = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
            cte_ref,
            LogicalPlan::synthetic(create_scan(31)),
        ))));
        let plan = LogicalOperator::Filter(Filter::new(
            cross,
            vec![Expression::Comparison(
                paro_planner::expression::ComparisonExpression::new(
                    ComparisonType::Equal,
                    column_ref(30, 0),
                    column_ref(31, 0),
                ),
            )],
        ));

        assert!(!JoinOrderOptimizer::new().can_optimize_join(&plan, true));
    }

    #[test]
    fn optimize_coalesces_single_relation_filters_after_join_reordering() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let cross = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
        ))));
        let compare = |comparison_type, left, right| {
            Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
                comparison_type,
                left,
                right,
            ))
        };
        let constant = |value| {
            Expression::Constant(ConstantExpression::new(
                Value::Integer(value),
                LogicalType::Integer,
            ))
        };
        let plan = LogicalOperator::Filter(Filter::new(
            cross,
            vec![
                compare(ComparisonType::Equal, column_ref(0, 0), column_ref(1, 0)),
                compare(
                    ComparisonType::GreaterThanOrEqual,
                    column_ref(0, 0),
                    constant(10),
                ),
                compare(ComparisonType::LessThan, column_ref(0, 0), constant(20)),
            ],
        ));

        let bind_context = BindContext::new();
        let optimized = optimizer.optimize(&session, &bind_context, plan).unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = optimized else {
            panic!("expected comparison join");
        };
        let filters = [&join.left.operator, &join.right.operator]
            .into_iter()
            .find_map(|operator| match operator {
                LogicalOperator::Filter(filter) => Some(filter),
                _ => None,
            })
            .expect("single-relation filter");
        assert_eq!(filters.expressions.len(), 2);
        assert!(matches!(
            filters.child.operator,
            LogicalOperator::ExpressionGet(_)
        ));
    }

    #[test]
    fn optimize_three_way_join_reconstructs_nested_join_tree() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let join_ab = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        )));
        let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            LogicalPlan::synthetic(join_ab),
            LogicalPlan::synthetic(create_scan(2)),
            vec![join_condition(JoinComparisonType::Equal, 1, 2)],
        )));

        let bind_context = BindContext::new();
        let optimized = optimizer.optimize(&session, &bind_context, plan).unwrap();

        assert_eq!(count_cross_products(&optimized), 0);
        match optimized {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(!join.conditions.is_empty());
                assert!(matches!(
                    join.left.operator,
                    LogicalOperator::Join(Join::Comparison(_)) | LogicalOperator::ExpressionGet(_)
                ));
                assert!(matches!(
                    join.right.operator,
                    LogicalOperator::Join(Join::Comparison(_)) | LogicalOperator::ExpressionGet(_)
                ));
            }
            other => panic!("expected nested comparison join tree, got {other:?}"),
        }
    }

    #[test]
    fn semi_join_optimization_preserves_join_semantics() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        )));

        assert!(optimizer.can_optimize_join(&plan, false));

        let bind_context = BindContext::new();
        let optimized = optimizer.optimize(&session, &bind_context, plan).unwrap();

        match optimized {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Semi);
                assert_eq!(join.conditions.len(), 1);
                let Expression::ColumnRef(left) = &join.conditions[0].left else {
                    panic!("expected column ref on left side");
                };
                assert_eq!(left.binding.table_index, 0);
            }
            other => panic!("expected semi comparison join, got {other:?}"),
        }
    }

    #[test]
    fn null_aware_anti_join_semantics_survive_reconstruction() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let mut join = ComparisonJoin::new(
            JoinType::Anti,
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        );
        join.anti_join_mode = AntiJoinMode::NullAware;

        let bind_context = BindContext::new();
        let optimized = optimizer
            .optimize(
                &session,
                &bind_context,
                LogicalOperator::Join(Join::Comparison(join)),
            )
            .unwrap();

        let LogicalOperator::Join(Join::Comparison(join)) = optimized else {
            panic!("expected anti comparison join");
        };
        assert_eq!(join.join_type, JoinType::Anti);
        assert_eq!(join.anti_join_mode, AntiJoinMode::NullAware);
        assert_eq!(join.conditions.len(), 1);
    }

    fn assert_nested_join_is_atomic(boundary_type: JoinType) {
        let session = make_test_session();
        let bind_context = BindContext::new();
        let boundary = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                boundary_type,
                LogicalPlan::synthetic(create_scan(0)),
                LogicalPlan::synthetic(create_scan(1)),
                vec![join_condition(JoinComparisonType::Equal, 0, 1)],
            ),
        )));
        let plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                boundary,
                LogicalPlan::synthetic(create_scan(2)),
                vec![join_condition(JoinComparisonType::Equal, 0, 2)],
            ),
        )));
        let mut optimizer = JoinOrderOptimizer::new();
        let mut filters = Vec::new();

        optimizer
            .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
            .unwrap();

        assert_eq!(optimizer.relation_manager.num_relations(), 2);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].join_type, JoinType::Inner);
        let LogicalOperator::Join(Join::Comparison(join)) = &optimizer.relation_plans[0].operator
        else {
            panic!("join boundary must remain an atomic relation");
        };
        assert_eq!(join.join_type, boundary_type);
    }

    #[test]
    fn extraction_keeps_nested_non_associative_joins_as_atomic_relations() {
        assert_nested_join_is_atomic(JoinType::Left);
        assert_nested_join_is_atomic(JoinType::Semi);
        assert_nested_join_is_atomic(JoinType::Anti);
    }

    #[test]
    fn root_semi_join_reorders_preserved_side_but_keeps_rhs_atomic() {
        let session = make_test_session();
        let bind_context = BindContext::new();
        let preserved = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                LogicalPlan::synthetic(create_scan(0)),
                LogicalPlan::synthetic(create_scan(1)),
                vec![join_condition(JoinComparisonType::Equal, 0, 1)],
            ),
        )));
        let plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Semi,
                preserved,
                LogicalPlan::synthetic(create_scan(2)),
                vec![join_condition(JoinComparisonType::Equal, 0, 2)],
            ),
        )));
        let mut optimizer = JoinOrderOptimizer::new();
        let mut filters = Vec::new();

        optimizer
            .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
            .unwrap();

        assert_eq!(optimizer.relation_manager.num_relations(), 3);
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].join_type, JoinType::Inner);
        assert_eq!(filters[1].join_type, JoinType::Semi);
        assert!(matches!(
            optimizer.relation_plans[2].operator,
            LogicalOperator::ExpressionGet(_)
        ));
    }

    #[test]
    fn extraction_treats_any_join_as_an_atomic_relation() {
        let session = make_test_session();
        let bind_context = BindContext::new();
        let boundary =
            LogicalPlan::synthetic(LogicalOperator::Join(Join::Any(Box::new(AnyJoin::new(
                JoinType::Inner,
                LogicalPlan::synthetic(create_scan(0)),
                LogicalPlan::synthetic(create_scan(1)),
                Expression::Constant(ConstantExpression::new(
                    Value::Boolean(true),
                    LogicalType::Boolean,
                )),
            )))));
        let plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(
                JoinType::Inner,
                boundary,
                LogicalPlan::synthetic(create_scan(2)),
                vec![join_condition(JoinComparisonType::Equal, 0, 2)],
            ),
        )));
        let mut optimizer = JoinOrderOptimizer::new();
        let mut filters = Vec::new();

        optimizer
            .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
            .unwrap();

        assert_eq!(optimizer.relation_manager.num_relations(), 2);
        assert!(matches!(
            optimizer.relation_plans[0].operator,
            LogicalOperator::Join(Join::Any(_))
        ));
    }

    #[test]
    fn optimize_plan_uses_build_width_when_intermediate_cardinalities_tie() {
        let session = make_test_session();
        let bind_context = BindContext::new();

        let rel_a = projection_relation(&bind_context, 100, 0, 1_000);
        let rel_b = projection_relation(&bind_context, 101, 1, 10);
        let rel_c = projection_relation(&bind_context, 102, 2, 10);

        let join_ab = LogicalPlan {
            id: bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                rel_a,
                rel_b,
                vec![join_condition(JoinComparisonType::Equal, 0, 1)],
            ))),
        };
        let plan = LogicalPlan {
            id: bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                JoinType::Inner,
                join_ab,
                rel_c,
                vec![join_condition(JoinComparisonType::Equal, 1, 2)],
            ))),
        };

        let mut column_stats = HashMap::new();
        for table_index in [0usize, 1, 2] {
            let mut base = BaseStatistics::new(LogicalType::Integer);
            base.set_distinct_count(10);
            column_stats.insert(
                ColumnBinding::new(table_index, 0),
                Arc::new(ColumnStatistics::new(base)),
            );
        }

        let mut optimizer = JoinOrderOptimizer::new();
        let optimized = optimizer
            .optimize_plan(&session, plan, &column_stats, &bind_context)
            .expect("join order optimization should succeed");

        let enumerated_build_tables = match &optimized.operator {
            LogicalOperator::Join(Join::Comparison(root)) => root
                .right
                .get_column_bindings()
                .into_iter()
                .map(|binding| binding.table_index)
                .collect::<HashSet<_>>(),
            other => panic!("expected comparison join root, got {other:?}"),
        };
        let physically_oriented = BuildProbeSideOptimizer::new(Arc::clone(&session)).optimize_plan(
            duplicate_plan_preserving_indices(&optimized, bind_context.shared().as_ref()),
        );
        let physical_build_tables = match &physically_oriented.operator {
            LogicalOperator::Join(Join::Comparison(root)) => root
                .right
                .get_column_bindings()
                .into_iter()
                .map(|binding| binding.table_index)
                .collect::<HashSet<_>>(),
            other => panic!("expected comparison join root, got {other:?}"),
        };
        assert_eq!(
            physical_build_tables, enumerated_build_tables,
            "final build/probe orientation must retain the side priced by DP"
        );

        let LogicalOperator::Join(Join::Comparison(root)) = optimized.operator else {
            panic!("expected comparison join root");
        };

        let nested = match (&root.left.operator, &root.right.operator) {
            (LogicalOperator::Join(Join::Comparison(join)), _) => join,
            (_, LogicalOperator::Join(Join::Comparison(join))) => join,
            other => panic!("expected one nested comparison join, got {other:?}"),
        };

        let nested_tables: HashSet<_> = nested
            .conditions
            .iter()
            .flat_map(|cond| {
                let mut tables = Vec::new();
                if let Expression::ColumnRef(left) = &cond.left {
                    tables.push(left.binding.table_index);
                }
                if let Expression::ColumnRef(right) = &cond.right {
                    tables.push(right.binding.table_index);
                }
                tables
            })
            .collect();
        // Both first joins are estimated at ten rows. Joining A-B first leaves
        // the single-column C relation as the final hash build instead of the
        // wider B-C intermediate.
        assert_eq!(nested_tables, HashSet::from([0usize, 1usize]));
    }
}
