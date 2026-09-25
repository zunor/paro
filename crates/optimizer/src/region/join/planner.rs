// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cost-based join-order optimization.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_common::error::Result;
use paro_common::logging::targets;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::{
    duplicate_operator_preserving_indices, duplicate_plan_preserving_indices,
};
use paro_planner::expression::{
    ComparisonType, ConjunctionExpression, ConjunctionType, Expression, ExpressionIterator,
    ExpressionVisitDecision, OperatorType,
};
use paro_planner::logical::operator::{
    ColumnBinding, ComparisonJoin, CrossProduct, Filter, Join, JoinComparisonType, JoinCondition,
    JoinType, LogicalOperator,
};
use paro_planner::logical::plan::{CardinalityEstimate, OwnedLogicalPlan};
use paro_storage::statistics::{ColumnStatistics, DistinctProvenance, NumericStats};
use tracing::debug;

use crate::cost::region::RegionCostModel;
use crate::estimate::selectivity::{SelectivityDefaults, SelectivityModel as LogicalCostModel};
use crate::region::join::candidate::DPJoinNode;
use crate::region::join::enumerator::{EnumerationOutcome, PlanEnumerator};
use crate::region::join::predicate_inference::infer_equality_constants;
use crate::region::join::query_graph::{
    FilterInfo, JoinEdgeOrientation, JoinPredicateSet, QueryGraphEdges,
};
use crate::region::join::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::region::join::relation_manager::{
    DistinctCount, ExtractedFilter, RelationManager, RelationStats,
};
use crate::rewrite::column::lifetime::ColumnLifetimeAnalyzer;

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

/// Whether a predicate's selectivity has no bounded frequency model.
///
/// A wildcard LIKE/ILIKE estimate describes pattern shape, not the value
/// distribution of the stored strings. Join enumeration therefore keeps its
/// point estimate for annotations but prices the predicate with a wider risk
/// cardinality. Exact patterns remain equality-shaped and do not need this
/// widening.
fn has_open_ended_selectivity(expression: &Expression) -> bool {
    if let Expression::Operator(operator) = expression {
        if matches!(
            operator.operator_type,
            OperatorType::Like | OperatorType::ILike
        ) {
            return !matches!(
                operator.children.get(1),
                Some(Expression::Constant(constant))
                    if matches!(&constant.value, Value::Varchar(pattern) if !pattern.contains('%') && !pattern.contains('_') && !pattern.contains('\\'))
            );
        }
    }

    let mut found = false;
    ExpressionIterator::enumerate_children(expression, |child| {
        found |= has_open_ended_selectivity(child);
    });
    found
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

/// The JoinRegionPlanner performs cost-based join order optimization.
///
pub struct JoinRegionPlanner {
    /// The relation manager for tracking relations.
    relation_manager: RelationManager,
    /// The set manager for creating relation sets.
    set_manager: JoinRelationSetManager,
    /// The query graph for storing edges.
    query_graph: QueryGraphEdges,
    /// The cost model for evaluating join costs.
    cost_model: RegionCostModel,
    /// Filter metadata extracted from the original join tree.
    filter_infos: Vec<Arc<FilterInfo>>,
    /// DP plans keyed by relation-set string for recursive reconstruction.
    plans: HashMap<Arc<JoinRelationSet>, Vec<DPJoinNode>>,
    /// Output-column statistics gathered earlier in the pipeline.
    column_stats: HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    /// Original base-relation subplans keyed by relation id for reconstruction.
    relation_plans: Vec<OwnedLogicalPlan>,
    exact_relation_limit: usize,
    max_pairs: usize,
    max_frontier_size: usize,
}

/// The graph side of join enumeration is independent from the representation
/// used to reconstruct a candidate. Native rule producers use this result to
/// rebuild a `NativeShell` directly, while the compatibility entry point
/// below still reconstructs `OwnedLogicalPlan` values for legacy callers.
///
/// The value is safe to share between equal, fact-keyed Memo requests: it
/// contains only immutable relation sets, predicate descriptions and DP
/// nodes. Native plan-node identities are allocated later while rebuilding a
/// shell, so sharing this graph result cannot alias an executable arena.
#[derive(Debug, Clone)]
pub(crate) struct JoinGraphEnumeration {
    pub(crate) final_plans: Vec<DPJoinNode>,
    pub(crate) root_filters: Vec<Expression>,
    /// Whether this graph exhausted the declared DP domain. Approximate
    /// graphs are still useful as prioritized seeds, but callers must not use
    /// them as proof evidence.
    pub(crate) completion: EnumerationOutcome,
}

impl JoinRegionPlanner {
    /// Create a new JoinRegionPlanner.
    pub fn new(selectivity_defaults: SelectivityDefaults) -> Self {
        Self {
            relation_manager: RelationManager::new(),
            set_manager: JoinRelationSetManager::new(),
            query_graph: QueryGraphEdges::new(),
            cost_model: RegionCostModel::new(selectivity_defaults),
            filter_infos: Vec::new(),
            plans: HashMap::new(),
            column_stats: HashMap::new(),
            relation_plans: Vec::new(),
            exact_relation_limit: 12,
            max_pairs: 10_000,
            max_frontier_size: 4,
        }
    }

    pub(crate) fn with_limits(mut self, limits: &crate::region::limits::RegionLimits) -> Self {
        self.exact_relation_limit = usize::from(limits.exact_relations);
        self.max_pairs = usize::try_from(limits.connected_pairs).unwrap_or(usize::MAX);
        self.max_frontier_size = usize::from(limits.candidates_per_subset).max(1);
        self
    }

    /// A committed region selects its physical hash orientation with the
    /// statement's calibration. Later lowering consumes that decision rather
    /// than silently reranking it under a different cardinality envelope.
    pub(crate) fn with_physical_pricing(
        mut self,
        calibration: &crate::cost::calibration::MachineCalibrationBundle,
    ) -> Result<Self> {
        self.cost_model.regional_pricing =
            Some(crate::cost::join::JoinWorkPricing::new(calibration)?);
        Ok(self)
    }

    /// Optimize the join order of a logical plan.
    ///
    /// This is the main entry point for join order optimization.
    /// For now, this is a simplified implementation that doesn't traverse
    /// the tree recursively. It only optimizes if the root is a join.
    #[cfg(test)]
    pub fn optimize(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        self.optimize_plan(
            ctx,
            OwnedLogicalPlan::synthetic(plan),
            &HashMap::new(),
            bind_context,
        )
        .map(OwnedLogicalPlan::into_operator)
    }

    #[cfg(test)]
    pub fn optimize_plan(
        &mut self,
        ctx: &StatementContext,
        plan: OwnedLogicalPlan,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
        bind_context: &BindContext,
    ) -> Result<OwnedLogicalPlan> {
        self.column_stats = column_stats.clone();
        // Join costing needs the semantic live-column set, not the canonical
        // `ProjectionMap::all()` payload retained by Memo identities. This
        // prepass derives that view before enumeration; final candidate
        // settling still recomputes executable projection maps after the join
        // tree has been reconstructed.
        let plan = ColumnLifetimeAnalyzer::for_join_enumeration().optimize(plan)?;
        plan.try_map_post_order(|plan| self.optimize_current_plan(ctx, bind_context, plan))
    }

    /// Optimize maximal legal regions once. Nested joins of the same region
    /// belong to its DP table, not to another invocation of the enumerator.
    pub(crate) fn optimize_regions(
        &mut self,
        ctx: &StatementContext,
        plan: OwnedLogicalPlan,
        column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
        bind_context: &BindContext,
    ) -> Result<OwnedLogicalPlan> {
        self.column_stats = column_stats.clone();
        let plan = ColumnLifetimeAnalyzer::for_join_enumeration().optimize(plan)?;
        let mut roots = HashSet::new();
        let mut pending = vec![(&plan, false)];
        while let Some((node, inside)) = pending.pop() {
            ctx.cancellation.check()?;
            let region = self.can_optimize_join(&node.operator);
            if region && !inside {
                roots.insert(node.id);
            }
            pending.extend(node.children().into_iter().map(|child| (child, region)));
        }
        plan.try_map_post_order(|plan| {
            ctx.cancellation.check()?;
            if roots.contains(&plan.id) {
                self.optimize_current_plan(ctx, bind_context, plan)
            } else {
                Ok(plan)
            }
        })
    }

    /// Keep join-graph extraction and reconstruction isolated from the
    /// explicit traversal state. Those routines own several large planner
    /// values and should not be folded back into the post-order driver.
    fn optimize_current_plan(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: OwnedLogicalPlan,
    ) -> Result<OwnedLogicalPlan> {
        if self.can_optimize_join(&plan.operator) {
            if let Some(mut optimized) = self
                .optimize_join_tree(
                    ctx,
                    bind_context,
                    duplicate_plan_preserving_indices(&plan, bind_context.shared().as_ref()),
                )?
                .into_iter()
                .next()
            {
                optimized.id = plan.id;
                return Ok(optimized);
            }
        }

        Ok(plan)
    }

    /// Check if a join can be optimized.
    fn can_optimize_join(&self, plan: &LogicalOperator) -> bool {
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
                    && self.can_optimize_join(&filter.child.operator)
            }
            _ => false,
        }
    }

    /// Optimize a join tree.
    fn optimize_join_tree(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: OwnedLogicalPlan,
    ) -> Result<Vec<OwnedLogicalPlan>> {
        // Reset state
        self.relation_manager = RelationManager::new();
        self.set_manager = JoinRelationSetManager::new();
        self.query_graph = QueryGraphEdges::new();
        self.cost_model.reset();
        self.filter_infos.clear();
        self.plans.clear();
        self.relation_plans.clear();

        // Preserve the columns this region promises to its parent before its
        // predicates are detached into the query graph.
        let region_outputs = plan
            .get_column_bindings()
            .into_iter()
            .zip(plan.types())
            .collect::<HashMap<_, _>>();

        // Extract relations and filters from the join tree
        let mut filters = Vec::new();
        self.extract_join_relations(ctx, bind_context, &plan, &mut filters, true)?;

        let relation_manager = std::mem::take(&mut self.relation_manager);
        let column_stats = std::mem::take(&mut self.column_stats);
        let Some(graph) =
            self.enumerate_relation_graph(filters, region_outputs, relation_manager, column_stats)?
        else {
            return Ok(Vec::new());
        };

        let mut result = Vec::with_capacity(graph.final_plans.len());
        for final_plan in graph.final_plans {
            debug!(
                target: targets::OPTIMIZER,
                shape = %final_plan.compact_shape(),
                completion = ?graph.completion,
                cardinality = final_plan.cardinality,
                cost = final_plan.cost,
                peak_build_bytes = final_plan.peak_build_bytes,
                "reconstructing join-order frontier member"
            );
            let Some(reconstructed) =
                self.reconstruct_plan(bind_context, &final_plan, &mut HashSet::new())?
            else {
                continue;
            };
            result.push(self.attach_filter_expressions(reconstructed, graph.root_filters.clone()));
        }
        Ok(result)
    }

    /// Enumerate a prepared relation graph without requiring an owned plan for
    /// each atomic relation. This is the shared graph kernel for the legacy
    /// reconstruction path and native transformation producers.
    pub(crate) fn enumerate_relation_graph(
        &mut self,
        filters: Vec<ExtractedFilter>,
        region_outputs: HashMap<ColumnBinding, LogicalType>,
        relation_manager: RelationManager,
        column_stats: HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    ) -> Result<Option<JoinGraphEnumeration>> {
        self.relation_manager = relation_manager;
        self.column_stats = column_stats;
        self.set_manager = JoinRelationSetManager::new();
        self.query_graph = QueryGraphEdges::new();
        self.cost_model.reset();
        self.filter_infos.clear();
        self.plans.clear();

        if self.relation_manager.num_relations() < 2 {
            return Ok(None);
        }

        let Some(extracted_predicates) = self
            .relation_manager
            .extract_edges(&filters, &mut self.set_manager)
        else {
            return Ok(None);
        };
        let filter_infos = extracted_predicates.graph_filters;
        let inferred_filters =
            infer_equality_constants(&filter_infos, &self.relation_manager, &mut self.set_manager);
        self.apply_relation_local_selectivity(&filter_infos);
        self.apply_relation_payload_widths(&region_outputs, &filter_infos);
        self.filter_infos = filter_infos
            .iter()
            .cloned()
            .chain(inferred_filters)
            .collect();

        // A computed multi-relation expression is not a direct-key NDV
        // equality. Keep its whole support and price it when that support is
        // available; never discard it merely because it has no key binding.
        let estimator_filters = if self.cost_model.regional_pricing.is_some() {
            let estimator = LogicalCostModel {
                defaults: self.cost_model.selectivity_defaults.clone(),
                #[cfg(test)]
                scan_access: Default::default(),
            };
            filter_infos
                .iter()
                .filter(|filter| {
                    let residual = filter.join_type() == JoinType::Inner
                        && filter.set.count() > 1
                        && (filter.left_binding.is_none() || filter.right_binding.is_none());
                    if residual {
                        self.cost_model.residual_selectivities.push((
                            Arc::clone(&filter.set),
                            estimator
                                .estimate_selectivity(&filter.filter, &self.column_stats)
                                .clamp(0.0, 1.0),
                        ));
                    }
                    !residual
                })
                .cloned()
                .collect::<Vec<_>>()
        } else {
            filter_infos.clone()
        };
        self.cost_model
            .init_equivalent_relations(&estimator_filters);
        for filter_info in &filter_infos {
            if let (Some(left_set), Some(right_set)) =
                (filter_info.left_set(), filter_info.right_set())
            {
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

        let stats = self.relation_manager.get_relation_stats();
        self.cost_model
            .init_cost_model(&mut self.set_manager, &stats);
        let mut enumerator = PlanEnumerator::with_budget(
            &self.query_graph,
            &mut self.set_manager,
            &mut self.cost_model,
            self.relation_manager.num_relations(),
            self.exact_relation_limit,
            self.max_pairs,
            self.max_frontier_size,
        );
        enumerator.init_leaf_plans();
        let completion = enumerator.solve_join_order();
        if !matches!(
            completion,
            EnumerationOutcome::Complete | EnumerationOutcome::Approximate
        ) {
            return Ok(None);
        }
        let final_plans = enumerator.get_final_plans().to_vec();
        if final_plans.is_empty() {
            return Ok(None);
        }
        self.plans = enumerator.get_plans().clone();
        drop(enumerator);
        Ok(Some(JoinGraphEnumeration {
            final_plans,
            root_filters: extracted_predicates.root_filters,
            completion,
        }))
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
            if filter.join_type() == JoinType::Inner && filter.set.count() == 1 {
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
            let base_cardinality = relation.stats.cardinality as u64;
            let estimate = cost_model.estimate_filter_cardinality(
                base_cardinality,
                &expressions,
                &self.column_stats,
            );
            relation.stats.cardinality = estimate.expected.max(1) as usize;
            let stable_expressions = expressions
                .iter()
                .filter(|expression| !has_open_ended_selectivity(expression))
                .cloned()
                .collect::<Vec<_>>();
            let risk_upper = if stable_expressions.len() == expressions.len() {
                estimate.expected
            } else {
                cost_model
                    .estimate_filter_cardinality(
                        base_cardinality,
                        &stable_expressions,
                        &self.column_stats,
                    )
                    .expected
            };
            // The expected estimate remains the annotation-facing estimate.
            // Join enumeration must price the complete stable-predicate
            // envelope: interpolating back toward an uncalibrated wildcard
            // estimate has no statistical meaning and can make a many-to-many
            // derived subtree look safe to materialize as a hash build.
            relation.stats.risk_cardinality = risk_upper.max(estimate.expected).max(1) as usize;
            // Marginal NDV and default predicate selectivity do not bound
            // skew. Until a predicate has a frequency/constraint proof, its
            // filtered estimate may influence order but cannot justify an
            // irreversible materialization decision.
            relation.stats.materialization_cardinality = if estimate.max == 0 {
                0
            } else {
                base_cardinality.max(estimate.expected) as usize
            };
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

    /// Estimate the payload that can cross a join-region cut.
    ///
    /// A leaf can read columns solely to evaluate its own local predicates.
    /// Those values are consumed before the leaf enters any hash build and
    /// must not be carried through every intermediate join. Retain only the
    /// region's public outputs and columns used by predicates spanning more
    /// than one relation. Keeping the union of all cross-relation keys is
    /// conservative: a key may remain costed beyond the join that consumes it,
    /// but a local-only value can no longer distort build orientation.
    fn apply_relation_payload_widths(
        &mut self,
        region_outputs: &HashMap<ColumnBinding, LogicalType>,
        filters: &[Arc<FilterInfo>],
    ) {
        let mut live_columns = region_outputs.clone();
        for filter in filters.iter().filter(|filter| filter.set.count() > 1) {
            ExpressionIterator::visit(&filter.filter, &mut |expression| {
                if let Expression::ColumnRef(column) = expression {
                    live_columns
                        .entry(column.binding)
                        .or_insert_with(|| column.return_type.clone());
                    ExpressionVisitDecision::SkipChildren
                } else {
                    ExpressionVisitDecision::Descend
                }
            });
        }

        let mut relation_columns = vec![HashMap::new(); self.relation_manager.num_relations()];
        for (binding, logical_type) in live_columns {
            let Some(relation_id) = self.relation_manager.get_relation_id(binding.table_index)
            else {
                continue;
            };
            relation_columns[relation_id].insert(binding, logical_type);
        }

        for (relation_id, columns) in relation_columns.into_iter().enumerate() {
            let Some(relation) = self.relation_manager.get_relation_mut(relation_id) else {
                continue;
            };
            relation.stats.estimated_payload_width =
                crate::cost::join_layout::estimate_row_payload_width(
                    &columns.into_values().collect::<Vec<_>>(),
                );
        }
    }

    /// Extract relations and filters from a join tree.
    fn extract_join_relations(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: &OwnedLogicalPlan,
        filters: &mut Vec<ExtractedFilter>,
        at_region_root: bool,
    ) -> Result<()> {
        match &plan.operator {
            LogicalOperator::Join(Join::Comparison(join))
                if RelationManager::reduction_join_is_reorderable(join) =>
            {
                if at_region_root {
                    if matches!(
                        join.left.operator,
                        LogicalOperator::Join(Join::Comparison(ref child))
                            if matches!(child.join_type, JoinType::Semi | JoinType::Anti)
                    ) {
                        // Consecutive reductions over the same preserved side
                        // are commutative filters. Keep the reorderable inner
                        // region beneath them in the same graph: a reduction
                        // may then shrink the preserved key domain before a
                        // wide or expensive dimension is joined. Reduction
                        // role edges still prevent a filter from running
                        // before the relation that owns its preserved key.
                        self.extract_reduction_cascade(ctx, bind_context, join, filters)?;
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
    ) -> Result<()> {
        // Every entry is validated by the root match or the recursive child
        // guard below. Invalid reductions remain atomic relations at their
        // caller, preserving existential multiplicity in every build mode.
        match &join.left.operator {
            LogicalOperator::Join(Join::Comparison(child))
                if RelationManager::reduction_join_is_reorderable(child) =>
            {
                self.extract_reduction_cascade(ctx, bind_context, child, filters)?;
            }
            LogicalOperator::Join(Join::Comparison(child))
                if matches!(child.join_type, JoinType::Semi | JoinType::Anti) =>
            {
                // A reduction whose predicate does not identify both inputs
                // (for example `5 = rhs.key`) is valid SQL but not a graph
                // edge. Keep the complete subtree atomic so its existential
                // semantics survive while outer reductions remain reorderable.
                self.add_relation_plan(ctx, bind_context, &join.left);
            }
            _ => {
                self.extract_join_relations(ctx, bind_context, &join.left, filters, false)?;
            }
        }
        self.add_relation_plan(ctx, bind_context, &join.right);
        Self::extract_comparison_join_filters(join, filters);
        Ok(())
    }

    fn extract_comparison_join_filters(join: &ComparisonJoin, filters: &mut Vec<ExtractedFilter>) {
        let expressions = join.conditions.iter().map(|condition| {
            let expression = Expression::Comparison(
                paro_planner::expression::ComparisonExpression::new(
                    Self::to_comparison_type(condition.comparison),
                    condition.left.clone(),
                    condition.right.clone(),
                )
                .into(),
            );
            expression
        });
        if matches!(join.join_type, JoinType::Semi | JoinType::Anti) {
            let expressions = expressions.collect::<Vec<_>>();
            let expression = match expressions.as_slice() {
                [] => return,
                [expression] => expression.clone(),
                _ => Expression::Conjunction(
                    ConjunctionExpression::new(ConjunctionType::And, expressions).into(),
                ),
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
        plan: &OwnedLogicalPlan,
    ) {
        let cardinality = crate::cost::join_layout::estimate_plan_cardinality(ctx, plan);
        let mut stats = RelationStats::with_cardinality(cardinality);
        stats.estimated_payload_width =
            crate::cost::join_layout::estimate_row_payload_width(&plan.types());
        stats.contains_control_region =
            crate::cost::join_layout::contains_control_region_boundary(plan);
        stats.unique_keys = crate::estimate::unique_keys::proven_unique_keys(plan);
        let memo_domain_observation = matches!(plan.operator, LogicalOperator::BoundReference(_));
        // Opaque Memo inputs own their occurrence's domain. The binding map
        // belongs to the rule's original shell and may describe another CTE
        // restriction or an earlier equivalent expression with the same
        // column names. Never use it to override the input group's facts.
        let boundary_columns = match &plan.operator {
            LogicalOperator::BoundReference(reference) => Some(reference.column_statistics()),
            LogicalOperator::Get(get) => get
                .table
                .as_ref()
                .and_then(|table| table.get_storage())
                .map(|storage| {
                    get.returned_types
                        .iter()
                        .enumerate()
                        .map(|(ordinal, ty)| {
                            get.stored_column(ordinal)
                                .and_then(|column| storage.column_statistics(column))
                                .map(Arc::new)
                                .unwrap_or_else(|| ColumnStatistics::create_unknown(ty.clone()))
                        })
                        .collect::<Vec<_>>()
                }),
            _ => None,
        };
        if let Some(columns) = &boundary_columns {
            self.column_stats.extend(
                plan.get_column_bindings()
                    .into_iter()
                    .zip(columns.iter().cloned()),
            );
        }
        let distinct_counts = plan
            .get_column_bindings()
            .into_iter()
            .enumerate()
            .map(|(ordinal, binding)| {
                let column_stats = match &boundary_columns {
                    Some(columns) => columns.get(ordinal),
                    None => self.column_stats.get(&binding),
                };
                let evidence = column_stats.map(|stats| stats.distinct_evidence());
                let has_evidence = evidence.is_some_and(|evidence| evidence.is_known());
                let storage_observation = column_stats
                    .is_some_and(|stats| stats.is_storage_observation())
                    && evidence.is_some_and(|evidence| evidence.is_complete_observation());
                let distinct = if has_evidence {
                    evidence.map_or(0, |evidence| evidence.point as usize)
                } else {
                    column_stats
                        .and_then(|stats| integral_domain_cardinality(stats))
                        .unwrap_or(cardinality.max(1))
                };
                let distinct = distinct.min(cardinality.max(1));
                let provenance =
                    evidence.map_or(DistinctProvenance::Unknown, |evidence| evidence.provenance);
                (
                    binding,
                    DistinctCount::from_evidence(
                        paro_storage::statistics::DistinctEvidence {
                            point: distinct as u64,
                            lower: evidence
                                .map_or(0, |evidence| evidence.lower.min(distinct as u64)),
                            upper: evidence.and_then(|evidence| evidence.upper),
                            provenance,
                        },
                        // Provenance, not the root operator shape, decides
                        // whether an HLL is an observed domain.  Filters,
                        // projections, search scans, and CTE boundaries can
                        // preserve a storage observation without being a
                        // bare Get; derived expressions remain estimates.
                        has_evidence
                            && (storage_observation
                                || (memo_domain_observation
                                    && !matches!(
                                        provenance,
                                        DistinctProvenance::ObservedPartial { .. }
                                    ))),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        stats.materialization_distinct_count = distinct_counts.clone();
        stats.column_distinct_count = distinct_counts;
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

    /// Reconstruct a logical plan from a DP join node.
    fn reconstruct_plan(
        &mut self,
        bind_context: &BindContext,
        node: &DPJoinNode,
        used_filters: &mut HashSet<usize>,
    ) -> Result<Option<OwnedLogicalPlan>> {
        if node.is_leaf {
            // This is a base relation
            let relation_id = node.set.relations()[0];
            let relation = self.relation_plans.get(relation_id).ok_or_else(|| {
                paro_common::error::internal(format!("Relation {} not found", relation_id))
            })?;

            let mut result = self.attach_remaining_filters(
                duplicate_plan_preserving_indices(relation, bind_context.shared().as_ref()),
                &node.set,
                used_filters,
            );
            result.stats.materialization_risk_cardinality =
                Some(Self::quantize_cardinality(node.materialization_cardinality));
            Ok(Some(result))
        } else {
            let left_node = node.left_plan.as_deref().ok_or_else(|| {
                paro_common::error::internal("join frontier member lost its left child")
            })?;
            let right_node = node.right_plan.as_deref().ok_or_else(|| {
                paro_common::error::internal("join frontier member lost its right child")
            })?;
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

            // DP costing chooses a materialized build input independently of
            // the arbitrary pair order used to enumerate a relation set.
            // Ordinary INNER/CROSS joins are commutative, so place that input
            // on the executable join's right side before binding conditions.
            // Reduction joins use their explicit oriented inverse below.
            let reduction_orientation = node
                .predicates
                .as_ref()
                .and_then(JoinPredicateSet::reduction_orientation);
            let flip_for_build = reduction_orientation.is_none()
                && node.build_side == crate::cost::join_layout::JoinBuildSide::Left;
            if flip_for_build {
                std::mem::swap(&mut left_plan, &mut right_plan);
                std::mem::swap(&mut left_set, &mut right_set);
            }

            let result = if let Some(predicates) = &node.predicates {
                let chosen_join_type = predicates.join_type();
                if let Some(orientation) = reduction_orientation {
                    if orientation == JoinEdgeOrientation::Inverted {
                        std::mem::swap(&mut left_plan, &mut right_plan);
                        std::mem::swap(&mut left_set, &mut right_set);
                    }
                }

                let mut join = ComparisonJoin::new(chosen_join_type, left_plan, right_plan, vec![]);
                if self.cost_model.regional_pricing.is_some() && reduction_orientation.is_none() {
                    join.build_side_constraint =
                        paro_planner::logical::operator::JoinBuildSideConstraint::Right;
                }
                join.anti_join_mode = predicates.anti_join_mode();
                for predicate in predicates.predicates() {
                    let appended = self.append_join_conditions(&mut join, predicate);
                    if appended {
                        used_filters.insert(predicate.filter().filter_index);
                    } else if predicate.orientation().is_some() {
                        // The original logical tree is still owned by the
                        // caller. An oriented graph witness that no longer
                        // reconstructs makes this region ineligible for
                        // reordering; it must never silently become a cross
                        // product in release builds.
                        return Ok(None);
                    }
                }
                if flip_for_build {
                    for condition in &mut join.conditions {
                        std::mem::swap(&mut condition.left, &mut condition.right);
                        condition.comparison = condition.comparison.flip();
                    }
                }

                if join.conditions.is_empty() {
                    if predicates.has_join_conditions() {
                        return Ok(None);
                    }
                    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
                        CrossProduct {
                            left: join.left,
                            right: join.right,
                            build_side_constraint: Default::default(),
                        },
                    )));
                    Self::set_reconstructed_cardinality(&mut plan, node);
                    plan
                } else {
                    debug_assert!(predicates.has_join_conditions());
                    let mut plan =
                        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
                    Self::set_reconstructed_cardinality(&mut plan, node);
                    plan
                }
            } else {
                let mut plan =
                    OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        build_side_constraint: Default::default(),
                    })));
                Self::set_reconstructed_cardinality(&mut plan, node);
                plan
            };

            // `result` is freshly reconstructed for this DP frontier member.
            // The old path deep-copied the complete subtree here only to give
            // every node fresh occurrence ids, repeating the physical search's
            // largest allocation. Re-label the already-owned nodes in one
            // post-order pass instead; relation plans were copied at their
            // ownership boundary above, so no sibling candidate is aliased.
            let mut result = result;
            if self.cost_model.regional_pricing.is_some() && reduction_orientation.is_none() {
                let before = self.cost_model.cardinality_before_activation(
                    &node.set,
                    &node.left_set,
                    &node.right_set,
                );
                result.stats.estimated_cardinality = Some(Self::join_cardinality_estimate(before));
            }
            let mut result = self.attach_remaining_filters(result, &node.set, used_filters);
            if self.cost_model.regional_pricing.is_some() {
                // DP already accounts for every whole-support residual once.
                // Reconstructing its Filter must not discount that count again.
                Self::set_reconstructed_cardinality(&mut result, node);
            }
            let (result, ()) = result.try_fold_post_order(|mut plan, _| {
                plan.id = bind_context.next_plan_id();
                Ok((plan, ()))
            })?;
            Ok(Some(result))
        }
    }

    fn join_cardinality_estimate(cardinality: f64) -> CardinalityEstimate {
        CardinalityEstimate::exact(Self::quantize_cardinality(cardinality))
    }

    fn set_reconstructed_cardinality(plan: &mut OwnedLogicalPlan, node: &DPJoinNode) {
        plan.stats.set_cardinality(
            Self::join_cardinality_estimate(node.cardinality),
            node.cardinality_provenance,
            Some(Self::quantize_cardinality(node.materialization_cardinality)),
        );
    }

    fn quantize_cardinality(cardinality: f64) -> u64 {
        if !cardinality.is_finite() || cardinality >= u64::MAX as f64 {
            u64::MAX
        } else {
            cardinality.max(1.0) as u64
        }
    }

    fn attach_remaining_filters(
        &self,
        mut result: OwnedLogicalPlan,
        result_set: &Arc<JoinRelationSet>,
        used_filters: &mut HashSet<usize>,
    ) -> OwnedLogicalPlan {
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
        result: OwnedLogicalPlan,
        expressions: Vec<Expression>,
    ) -> OwnedLogicalPlan {
        self.attach_filter_expressions_with_cost_model(
            result,
            expressions,
            &LogicalCostModel::default(),
        )
    }

    fn attach_filter_expressions_with_cost_model(
        &self,
        mut result: OwnedLogicalPlan,
        expressions: Vec<Expression>,
        cost_model: &LogicalCostModel,
    ) -> OwnedLogicalPlan {
        if expressions.is_empty() {
            return result;
        }
        let child_stats = result.stats.clone();
        let estimated_cardinality = child_stats.estimated_cardinality.map(|estimate| {
            cost_model.estimate_filter_cardinality(
                estimate.expected,
                &expressions,
                &self.column_stats,
            )
        });
        result =
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(result, expressions)));
        result.stats.inherit_cardinality_from(&child_stats);
        result.stats.estimated_cardinality = estimated_cardinality;
        result.stats.cardinality_provenance = child_stats.cardinality_provenance;
        result
    }

    fn append_join_conditions(
        &self,
        join: &mut ComparisonJoin,
        predicate: &crate::region::join::query_graph::OrientedJoinPredicate,
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
        predicate: &crate::region::join::query_graph::OrientedJoinPredicate,
    ) -> Option<JoinCondition> {
        let invert = predicate.orientation()? == JoinEdgeOrientation::Inverted;
        let comparison_type = crate::rewrite::join::mixed_predicates::join_comparison_type(
            comparison.comparison_type,
        );
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
}

#[cfg(test)]
mod tests;
