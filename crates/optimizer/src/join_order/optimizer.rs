// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cost-based join-order optimization.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_common::error::Result;
use paro_context::StatementContext;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::deep_copy::{
    duplicate_operator_preserving_indices, duplicate_plan_preserving_indices,
};
use paro_planner::expression::{ComparisonType, Expression};
use paro_planner::operator::{
    AntiJoinMode, ColumnBinding, ComparisonJoin, CrossProduct, Filter, Join, JoinComparisonType,
    JoinCondition, JoinType, LogicalOperator,
};
use paro_planner::plan::{CardinalityEstimate, LogicalPlan};
use paro_storage::statistics::ColumnStatistics;

use crate::cost_model::CostModel as LogicalCostModel;
use crate::join_order::cost_model::{CostModel, DPJoinNode, JoinPredicateSet};
use crate::join_order::enumerator::PlanEnumerator;
use crate::join_order::query_graph::{FilterInfo, QueryGraphEdges};
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::join_order::relation_manager::{
    DistinctCount, ExtractedFilter, RelationManager, RelationStats,
};

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
    plans: HashMap<String, DPJoinNode>,
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
        self.optimize_plan_recursive(ctx, bind_context, plan)
    }

    fn optimize_plan_recursive(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        self.optimize_plan_recursive_with_properties(ctx, bind_context, plan)
            .map(|(plan, _)| plan)
    }

    fn optimize_plan_recursive_with_properties(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool)> {
        let mut child_contains_control_region_reference = false;
        let plan = plan.try_map_children(|child| {
            let (child, contains_control_region_reference) =
                self.optimize_plan_recursive_with_properties(ctx, bind_context, child)?;
            child_contains_control_region_reference |= contains_control_region_reference;
            Ok(child)
        })?;
        let contains_control_region_reference = child_contains_control_region_reference
            || matches!(plan.operator, LogicalOperator::CTERef(_));

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
        self.cost_model = CostModel::new();
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
        let reconstructed =
            self.reconstruct_plan(bind_context, &final_plan, &mut HashSet::new())?;

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
                if distinct.from_hll {
                    distinct.distinct_count = distinct
                        .distinct_count
                        .min(relation.stats.cardinality.max(1));
                }
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
                    // The preserved side remains visible after a reduction
                    // join, so its inner-join region may be reordered around
                    // the SEMI/ANTI edge. The non-preserved side must remain
                    // atomic: extracting any of its relations would allow the
                    // enumerator to move them past the reduction point, after
                    // which their bindings no longer exist.
                    self.extract_join_relations(ctx, bind_context, &join.left, filters, false)?;
                    self.add_relation_plan(ctx, bind_context, &join.right);
                    Self::extract_comparison_join_filters(join, filters);
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

    fn extract_comparison_join_filters(join: &ComparisonJoin, filters: &mut Vec<ExtractedFilter>) {
        filters.extend(join.conditions.iter().map(|condition| {
            let expression =
                Expression::Comparison(paro_planner::expression::ComparisonExpression {
                    left: Box::new(condition.left.clone()),
                    right: Box::new(condition.right.clone()),
                    comparison_type: Self::to_comparison_type(condition.comparison),
                });
            ExtractedFilter::new(expression, join.join_type, join.anti_join_mode)
        }));
    }

    fn add_relation_plan(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: &LogicalPlan,
    ) {
        let cardinality = self.estimate_cardinality(ctx, plan);
        let mut stats = RelationStats::with_cardinality(cardinality);
        stats.column_distinct_count = plan
            .get_column_bindings()
            .into_iter()
            .map(|binding| {
                let distinct = self
                    .column_stats
                    .get(&binding)
                    .map(|stats| stats.get_distinct_count())
                    .unwrap_or(0);
                let from_hll = distinct > 0;
                (
                    binding,
                    DistinctCount::new(
                        if from_hll {
                            // A filter can reduce the relation cardinality without
                            // rewriting base-column HLL statistics. The filtered
                            // domain cannot contain more distinct values than rows.
                            distinct.min(cardinality.max(1))
                        } else {
                            cardinality.max(1)
                        },
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
    ) -> Result<LogicalPlan> {
        if node.is_leaf {
            // This is a base relation
            let relation_id = node.set.relations()[0];
            let relation = self.relation_plans.get(relation_id).ok_or_else(|| {
                paro_common::error::internal(format!("Relation {} not found", relation_id))
            })?;

            Ok(self.attach_remaining_filters(
                duplicate_plan_preserving_indices(relation, bind_context.shared().as_ref()),
                &node.set,
                used_filters,
            ))
        } else {
            let left_node = self.lookup_plan(&node.left_set)?;
            let right_node = self.lookup_plan(&node.right_set)?;
            let mut left_set = node.left_set.clone();
            let mut right_set = node.right_set.clone();
            let mut left_plan = self.reconstruct_plan(bind_context, left_node, used_filters)?;
            let mut right_plan = self.reconstruct_plan(bind_context, right_node, used_filters)?;

            let result = if let Some(predicates) = &node.predicates {
                if predicates.filters.is_empty() {
                    {
                        let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
                            CrossProduct {
                                left: Box::new(left_plan),
                                right: Box::new(right_plan),
                            },
                        )));
                        plan.stats.estimated_cardinality =
                            Some(Self::join_cardinality_estimate(node.cardinality));
                        plan
                    }
                } else {
                    let (chosen_join_type, chosen_anti_join_mode) =
                        Self::choose_join_semantics(predicates);
                    if matches!(chosen_join_type, JoinType::Semi | JoinType::Anti)
                        && Self::edge_is_inverted(
                            &left_set,
                            &right_set,
                            predicates
                                .filters
                                .first()
                                .and_then(|filter| filter.left_set.as_ref()),
                            predicates
                                .filters
                                .first()
                                .and_then(|filter| filter.right_set.as_ref()),
                        )
                    {
                        std::mem::swap(&mut left_plan, &mut right_plan);
                        std::mem::swap(&mut left_set, &mut right_set);
                    }

                    let mut join =
                        ComparisonJoin::new(chosen_join_type, left_plan, right_plan, vec![]);
                    join.anti_join_mode = chosen_anti_join_mode;
                    for filter in &predicates.filters {
                        if self.append_join_conditions(&mut join, filter, &left_set, &right_set) {
                            used_filters.insert(filter.filter_index);
                        }
                    }

                    if join.conditions.is_empty() {
                        let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
                            CrossProduct {
                                left: join.left,
                                right: join.right,
                            },
                        )));
                        plan.stats.estimated_cardinality =
                            Some(Self::join_cardinality_estimate(node.cardinality));
                        plan
                    } else {
                        let mut plan =
                            LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
                        plan.stats.estimated_cardinality =
                            Some(Self::join_cardinality_estimate(node.cardinality));
                        plan
                    }
                }
            } else {
                let mut plan =
                    LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                    })));
                plan.stats.estimated_cardinality =
                    Some(Self::join_cardinality_estimate(node.cardinality));
                plan
            };

            Ok(self.attach_remaining_filters(
                duplicate_plan_preserving_indices(&result, bind_context.shared().as_ref()),
                &node.set,
                used_filters,
            ))
        }
    }

    fn lookup_plan(&self, set: &Arc<JoinRelationSet>) -> Result<&DPJoinNode> {
        self.plans.get(&set.to_string()).ok_or_else(|| {
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
            if filter.set.count() > 0 && JoinRelationSet::is_subset(result_set, &filter.set) {
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
        result
    }

    fn append_join_conditions(
        &self,
        join: &mut ComparisonJoin,
        filter: &FilterInfo,
        left_set: &Arc<JoinRelationSet>,
        right_set: &Arc<JoinRelationSet>,
    ) -> bool {
        let start_len = join.conditions.len();
        match &filter.filter {
            Expression::Comparison(comp) => {
                if let Some(condition) =
                    Self::comparison_to_join_condition(comp, filter, left_set, right_set)
                {
                    join.conditions.push(condition);
                }
            }
            Expression::Conjunction(conj) => {
                for child in &conj.children {
                    let Expression::Comparison(comp) = child else {
                        continue;
                    };
                    if let Some(condition) =
                        Self::comparison_to_join_condition(comp, filter, left_set, right_set)
                    {
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
        filter: &FilterInfo,
        left_set: &Arc<JoinRelationSet>,
        right_set: &Arc<JoinRelationSet>,
    ) -> Option<JoinCondition> {
        let filter_left = filter.left_set.as_ref()?;
        let filter_right = filter.right_set.as_ref()?;
        let invert =
            Self::edge_is_inverted(left_set, right_set, Some(filter_left), Some(filter_right));
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

    fn choose_join_semantics(predicates: &JoinPredicateSet) -> (JoinType, AntiJoinMode) {
        let filter = predicates
            .filters
            .iter()
            .find(|filter| matches!(filter.join_type, JoinType::Semi | JoinType::Anti))
            .or_else(|| {
                predicates
                    .filters
                    .iter()
                    .find(|filter| filter.join_type != JoinType::Invalid)
            })
            .map(Arc::as_ref);
        filter.map_or((JoinType::Inner, AntiJoinMode::Regular), |filter| {
            (filter.join_type, filter.anti_join_mode)
        })
    }

    fn edge_is_inverted(
        left_set: &Arc<JoinRelationSet>,
        right_set: &Arc<JoinRelationSet>,
        filter_left: Option<&Arc<JoinRelationSet>>,
        filter_right: Option<&Arc<JoinRelationSet>>,
    ) -> bool {
        let (Some(filter_left), Some(filter_right)) = (filter_left, filter_right) else {
            return false;
        };
        JoinRelationSet::is_subset(left_set, filter_right)
            && JoinRelationSet::is_subset(right_set, filter_left)
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

    use super::*;
    use paro_common::{runtime_value::Value, types::LogicalType};
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_function::scalar::FunctionStability;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        ColumnRefExpression, ConstantExpression, FunctionExpression, ReferenceExpression,
    };
    use paro_planner::operator::{AnyJoin, ColumnBinding, ExpressionGet, Projection};
    use paro_planner::plan::{CardinalityEstimate, NodeStats};
    use paro_storage::statistics::{BaseStatistics, ColumnStatistics};

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
        column_stats.update_distinct_statistics_full(&hashes, hashes.len());
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
    fn optimize_plan_prefers_smaller_intermediate_when_plan_stats_are_available() {
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
        assert_eq!(nested_tables, HashSet::from([1usize, 2usize]));
    }
}
