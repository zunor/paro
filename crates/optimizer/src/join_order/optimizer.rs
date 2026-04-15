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
    ColumnBinding, ComparisonJoin, CrossProduct, Filter, Join, JoinComparisonType, JoinCondition,
    JoinType, LogicalOperator,
};
use paro_planner::plan::{CardinalityEstimate, LogicalPlan};
use paro_storage::statistics::ColumnStatistics;

use crate::cost_model::CostModel as LogicalCostModel;
use crate::join_order::cost_model::{CostModel, DPJoinNode};
use crate::join_order::enumerator::PlanEnumerator;
use crate::join_order::query_graph::{FilterInfo, NeighborInfo, QueryGraphEdges};
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
        let plan =
            plan.try_map_children(|child| self.optimize_plan_recursive(ctx, bind_context, child))?;

        if self.can_optimize_join(&plan.operator) {
            if let Some(mut optimized) = self.optimize_join_tree(
                ctx,
                bind_context,
                duplicate_plan_preserving_indices(&plan, bind_context.shared().as_ref()),
            )? {
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
        self.extract_join_relations(ctx, bind_context, &plan, &mut filters)?;

        // Check if we have enough relations to optimize
        if self.relation_manager.num_relations() < 2 {
            return Ok(None);
        }

        // Extract edges from filters
        let filter_infos = self
            .relation_manager
            .extract_edges(&filters, &mut self.set_manager);
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

        Ok(Some(reconstructed))
    }

    /// Extract relations and filters from a join tree.
    fn extract_join_relations(
        &mut self,
        ctx: &StatementContext,
        bind_context: &BindContext,
        plan: &LogicalPlan,
        filters: &mut Vec<ExtractedFilter>,
    ) -> Result<()> {
        match &plan.operator {
            LogicalOperator::Join(join) => {
                // Recursively extract from children first so table-index mappings exist
                self.extract_join_relations(ctx, bind_context, join.left(), filters)?;
                self.extract_join_relations(ctx, bind_context, join.right(), filters)?;

                match join {
                    Join::Comparison(cj) => {
                        for cond in &cj.conditions {
                            // Create comparison expression
                            let filter = Expression::Comparison(
                                paro_planner::expression::ComparisonExpression {
                                    left: Box::new(cond.left.clone()),
                                    right: Box::new(cond.right.clone()),
                                    comparison_type: Self::to_comparison_type(cond.comparison),
                                },
                            );
                            filters.push(ExtractedFilter::new(filter, cj.join_type));
                        }
                    }
                    Join::Cross(_) => {
                        // No conditions for cross product
                    }
                    Join::Any(_) => {
                        // Any join is not reorderable
                        return Ok(());
                    }
                }
            }
            LogicalOperator::Filter(filter) => {
                // Continue with child
                self.extract_join_relations(ctx, bind_context, filter.child.as_ref(), filters)?;
                filters.extend(
                    filter
                        .expressions
                        .iter()
                        .cloned()
                        .map(ExtractedFilter::inner),
                );
            }
            _ => {
                // This is a base relation
                if RelationManager::operator_needs_relation(plan.operator.op_type()) {
                    // Get cardinality estimate
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
                            DistinctCount::new(
                                if from_hll {
                                    distinct
                                } else {
                                    cardinality.max(1)
                                },
                                from_hll,
                            )
                        })
                        .collect();
                    // Add to relation manager
                    self.relation_manager.add_relation(
                        duplicate_operator_preserving_indices(
                            &plan.operator,
                            bind_context.shared().as_ref(),
                        ),
                        None,
                        stats,
                    );
                    self.relation_plans.push(duplicate_plan_preserving_indices(
                        plan,
                        bind_context.shared().as_ref(),
                    ));
                }
            }
        }

        Ok(())
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

            let result = if let Some(info) = &node.info {
                if info.filters.is_empty() {
                    {
                        let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(
                            CrossProduct {
                                left: Box::new(left_plan),
                                right: Box::new(right_plan),
                            },
                        )));
                        plan.stats.estimated_cardinality =
                            Some(CardinalityEstimate::exact(node.cardinality as u64));
                        plan
                    }
                } else {
                    let chosen_join_type = Self::choose_join_type(info);
                    if matches!(chosen_join_type, JoinType::Semi | JoinType::Anti)
                        && Self::edge_is_inverted(
                            &left_set,
                            &right_set,
                            info.filters
                                .first()
                                .and_then(|filter| filter.left_set.as_ref()),
                            info.filters
                                .first()
                                .and_then(|filter| filter.right_set.as_ref()),
                        )
                    {
                        std::mem::swap(&mut left_plan, &mut right_plan);
                        std::mem::swap(&mut left_set, &mut right_set);
                    }

                    let mut join =
                        ComparisonJoin::new(chosen_join_type, left_plan, right_plan, vec![]);
                    for filter in &info.filters {
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
                            Some(CardinalityEstimate::exact(node.cardinality as u64));
                        plan
                    } else {
                        let mut plan =
                            LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
                        plan.stats.estimated_cardinality =
                            Some(CardinalityEstimate::exact(node.cardinality as u64));
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
                    Some(CardinalityEstimate::exact(node.cardinality as u64));
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

    fn attach_remaining_filters(
        &self,
        mut result: LogicalPlan,
        result_set: &Arc<JoinRelationSet>,
        used_filters: &mut HashSet<usize>,
    ) -> LogicalPlan {
        let logical_cost_model = LogicalCostModel::default();
        for filter in &self.filter_infos {
            if used_filters.contains(&filter.filter_index) {
                continue;
            }
            if filter.set.count() > 0 && JoinRelationSet::is_subset(result_set, &filter.set) {
                let child_estimate = result.stats.estimated_cardinality;
                let estimated_cardinality = child_estimate.map(|estimate| {
                    logical_cost_model.estimate_filter_cardinality(
                        estimate.expected,
                        std::slice::from_ref(&filter.filter),
                        &self.column_stats,
                    )
                });
                result = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                    result,
                    vec![filter.filter.clone()],
                )));
                result.stats.estimated_cardinality = estimated_cardinality;
                used_filters.insert(filter.filter_index);
            }
        }
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

    fn choose_join_type(info: &NeighborInfo) -> JoinType {
        info.filters
            .iter()
            .find(|filter| matches!(filter.join_type, JoinType::Semi | JoinType::Anti))
            .or_else(|| {
                info.filters
                    .iter()
                    .find(|filter| filter.join_type != JoinType::Invalid)
            })
            .map(|filter| filter.join_type)
            .unwrap_or(JoinType::Inner)
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
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{ColumnBinding, ExpressionGet, Projection};
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
    fn optimize_semi_join_keeps_join_type() {
        let session = make_test_session();
        let mut optimizer = JoinOrderOptimizer::new();
        let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            LogicalPlan::synthetic(create_scan(0)),
            LogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        )));

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
