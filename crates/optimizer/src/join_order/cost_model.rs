// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join-order cost model based on cardinality estimates.

use std::sync::Arc;

use crate::join_order::cardinality::CardinalityEstimator;
use crate::join_order::query_graph::FilterInfo;
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::join_order::relation_manager::RelationStats;

/// Predicates evaluated at one cut in the chosen join tree.
///
/// Query-graph adjacency can expose several independent edges across the same
/// cut. The physical join must evaluate all of them, so the DP plan stores a
/// predicate set rather than one arbitrarily selected graph neighbor.
#[derive(Debug, Clone)]
pub struct JoinPredicateSet {
    pub filters: Vec<Arc<FilterInfo>>,
}

/// A node in the dynamic programming join plan.
///
#[derive(Debug, Clone)]
pub struct DPJoinNode {
    /// The set of relations in this node.
    pub set: Arc<JoinRelationSet>,
    /// The selected query-graph edge that connects the left and right children.
    pub predicates: Option<JoinPredicateSet>,
    /// Whether this is a leaf node (single relation).
    pub is_leaf: bool,
    /// The left child set (for non-leaf nodes).
    pub left_set: Arc<JoinRelationSet>,
    /// The right child set (for non-leaf nodes).
    pub right_set: Arc<JoinRelationSet>,
    /// The cost of this join node.
    pub cost: f64,
    /// The estimated cardinality of this node. Keep the fractional estimate
    /// throughout DP enumeration; logical plans quantize it only once when the
    /// chosen tree is reconstructed.
    pub cardinality: f64,
}

impl DPJoinNode {
    /// Create a leaf node (single relation).
    ///
    /// Leaf nodes have cost 0 since they represent base tables.
    pub fn leaf(set: Arc<JoinRelationSet>) -> Self {
        Self {
            set: set.clone(),
            predicates: None,
            is_leaf: true,
            left_set: set.clone(),
            right_set: set,
            cost: 0.0,
            cardinality: 0.0,
        }
    }

    /// Create an intermediate node (join of two relations).
    pub fn intermediate(
        set: Arc<JoinRelationSet>,
        predicates: Option<JoinPredicateSet>,
        left_set: Arc<JoinRelationSet>,
        right_set: Arc<JoinRelationSet>,
        cost: f64,
        cardinality: f64,
    ) -> Self {
        Self {
            set,
            predicates,
            is_leaf: false,
            left_set,
            right_set,
            cost,
            cardinality,
        }
    }
}

/// The CostModel computes the cost of join plans.
///
#[derive(Debug, Default)]
pub struct CostModel {
    /// Cardinality estimator used to calculate cost.
    pub cardinality_estimator: CardinalityEstimator,
    relation_widths: Vec<usize>,
}

impl CostModel {
    /// Create a new CostModel.
    pub fn new() -> Self {
        Self {
            cardinality_estimator: CardinalityEstimator::new(),
            relation_widths: Vec::new(),
        }
    }

    /// Initialize the cost model with relation statistics.
    ///
    /// This should be called after all relations have been added to the
    /// relation manager and before computing any costs.
    pub fn init_cost_model(
        &mut self,
        set_manager: &mut JoinRelationSetManager,
        relation_stats: &[RelationStats],
    ) {
        for (i, stats) in relation_stats.iter().enumerate() {
            let set = set_manager.get_relation(i);
            self.cardinality_estimator
                .init_cardinality_estimator_props(&set, stats);
        }
        self.relation_widths = relation_stats
            .iter()
            .map(|stats| stats.estimated_payload_width)
            .collect();
    }

    /// Compute the cost of joining two nodes.
    ///
    /// The cost is computed as:
    /// cost = cardinality(left ⋈ right) * row_width + cost(left) + cost(right)
    ///
    /// The row width is the combined projected payload plus one intermediate
    /// row header, so wide retained values are charged without duplicating the
    /// container overhead for every base relation.
    pub fn compute_cost(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        set_manager: &mut JoinRelationSetManager,
    ) -> f64 {
        // Get the combined relation set
        let combination = set_manager.union(&left.set, &right.set);

        // Estimate the cardinality of the join
        let join_cardinality = self
            .cardinality_estimator
            .estimate_cardinality(&combination);

        // Materializing an intermediate is proportional to both its row count
        // and carried width. Cardinality-only costing treats an integer key and
        // a wide row with retained strings as identical, encouraging early
        // dimension joins whose payload is repeatedly serialized by later
        // breakers.
        let payload_width = combination
            .relations()
            .iter()
            .map(|relation| self.relation_widths.get(*relation).copied().unwrap_or(0))
            .sum::<usize>();
        let row_width = 8usize.saturating_add(payload_width).max(1);
        join_cardinality * row_width as f64 + left.cost + right.cost
    }

    /// Compute the cost and create a new DPJoinNode.
    pub fn compute_cost_and_create_node(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        set_manager: &mut JoinRelationSetManager,
        predicates: Option<JoinPredicateSet>,
    ) -> DPJoinNode {
        let combination = set_manager.union(&left.set, &right.set);
        let cost = self.compute_cost(left, right, set_manager);
        let cardinality = self
            .cardinality_estimator
            .estimate_cardinality(&combination);

        DPJoinNode::intermediate(
            combination,
            predicates,
            left.set.clone(),
            right.set.clone(),
            cost,
            cardinality,
        )
    }

    /// Get the estimated cardinality for a relation set.
    pub fn get_cardinality(&mut self, set: &JoinRelationSet) -> f64 {
        self.cardinality_estimator.estimate_cardinality(set)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::join_order::query_graph::FilterInfo;
    use crate::join_order::relation_manager::DistinctCount;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, ComparisonExpression, ComparisonType};
    use paro_planner::operator::ColumnBinding;

    fn column_distinct_counts(
        table_index: usize,
        counts: impl IntoIterator<Item = DistinctCount>,
    ) -> HashMap<ColumnBinding, DistinctCount> {
        counts
            .into_iter()
            .enumerate()
            .map(|(column_index, count)| (ColumnBinding::new(table_index, column_index), count))
            .collect()
    }

    fn create_column_ref(
        table_index: usize,
        column_index: usize,
    ) -> paro_planner::expression::Expression {
        paro_planner::expression::Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn create_equality_filter(
        set_manager: &mut JoinRelationSetManager,
        left_table: usize,
        left_col: usize,
        right_table: usize,
        right_col: usize,
        filter_index: usize,
    ) -> Arc<FilterInfo> {
        let expr = paro_planner::expression::Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(left_table, left_col)),
            right: Box::new(create_column_ref(right_table, right_col)),
            comparison_type: ComparisonType::Equal,
        });

        let set = set_manager.get_relation_from_vec(vec![left_table, right_table]);
        let left_set = set_manager.get_relation(left_table);
        let right_set = set_manager.get_relation(right_table);

        let mut filter = FilterInfo::new_inner(expr, set, filter_index);
        filter.set_left_set(left_set);
        filter.set_right_set(right_set);
        filter.set_left_binding(ColumnBinding::new(left_table, left_col), left_table);
        filter.set_right_binding(ColumnBinding::new(right_table, right_col), right_table);

        Arc::new(filter)
    }

    #[test]
    fn test_cost_model_new() {
        let mut cost_model = CostModel::new();
        // Just verify it can be created
        assert_eq!(cost_model.get_cardinality(&JoinRelationSet::empty()), 1.0);
    }

    #[test]
    fn test_dp_join_node_leaf() {
        let mut set_manager = JoinRelationSetManager::new();
        let set = set_manager.get_relation(0);

        let node = DPJoinNode::leaf(set.clone());

        assert!(node.is_leaf);
        assert_eq!(node.cost, 0.0);
        assert_eq!(node.cardinality, 0.0);
        assert!(Arc::ptr_eq(&node.set, &set));
    }

    #[test]
    fn test_dp_join_node_intermediate() {
        let mut set_manager = JoinRelationSetManager::new();
        let left = set_manager.get_relation(0);
        let right = set_manager.get_relation(1);
        let combined = set_manager.union(&left, &right);

        let node = DPJoinNode::intermediate(
            combined.clone(),
            None,
            left.clone(),
            right.clone(),
            100.0,
            50.0,
        );

        assert!(!node.is_leaf);
        assert!(node.predicates.is_none());
        assert_eq!(node.cost, 100.0);
        assert_eq!(node.cardinality, 50.0);
        assert!(Arc::ptr_eq(&node.left_set, &left));
        assert!(Arc::ptr_eq(&node.right_set, &right));
    }

    #[test]
    fn test_init_cost_model() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        let stats = vec![
            RelationStats::with_cardinality(1000),
            RelationStats::with_cardinality(500),
        ];

        cost_model.init_cost_model(&mut set_manager, &stats);

        // Check that cardinalities were initialized
        let set0 = set_manager.get_relation(0);
        let card0 = cost_model.get_cardinality(&set0);
        assert_eq!(card0, 1000.0);

        let set1 = set_manager.get_relation(1);
        let card1 = cost_model.get_cardinality(&set1);
        assert_eq!(card1, 500.0);
    }

    #[test]
    fn test_compute_cost_leaf_nodes() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Initialize with join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model
            .cardinality_estimator
            .init_equivalent_relations(&[filter]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        // Create leaf nodes
        let left = DPJoinNode::leaf(set_manager.get_relation(0));
        let right = DPJoinNode::leaf(set_manager.get_relation(1));

        // Compute cost
        let cost = cost_model.compute_cost(&left, &right, &mut set_manager);

        // Cost should be join cardinality + 0 + 0 (leaf costs are 0)
        // Join cardinality = (1000 * 500) / max(100, 50) = 5000
        assert!(cost > 0.0);
    }

    #[test]
    fn test_compute_cost_with_existing_costs() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Initialize with join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model
            .cardinality_estimator
            .init_equivalent_relations(&[filter]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        // Create nodes with existing costs
        let left_set = set_manager.get_relation(0);
        let right_set = set_manager.get_relation(1);

        let left = DPJoinNode {
            set: left_set.clone(),
            predicates: None,
            is_leaf: false,
            left_set: left_set.clone(),
            right_set: left_set.clone(),
            cost: 100.0,
            cardinality: 1000.0,
        };

        let right = DPJoinNode {
            set: right_set.clone(),
            predicates: None,
            is_leaf: false,
            left_set: right_set.clone(),
            right_set: right_set.clone(),
            cost: 50.0,
            cardinality: 500.0,
        };

        // Compute cost
        let cost = cost_model.compute_cost(&left, &right, &mut set_manager);

        // Cost should include left.cost + right.cost
        assert!(cost >= 150.0); // At least the sum of child costs
    }

    #[test]
    fn test_compute_cost_and_create_node() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Initialize with join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model
            .cardinality_estimator
            .init_equivalent_relations(&[filter]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        // Create leaf nodes
        let left = DPJoinNode::leaf(set_manager.get_relation(0));
        let right = DPJoinNode::leaf(set_manager.get_relation(1));

        // Create join node
        let join_node =
            cost_model.compute_cost_and_create_node(&left, &right, &mut set_manager, None);

        assert!(!join_node.is_leaf);
        assert!(join_node.cost > 0.0);
        assert!(join_node.cardinality > 0.0);
        assert_eq!(join_node.set.count(), 2);
    }

    #[test]
    fn test_get_cardinality() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        let stats = vec![RelationStats::with_cardinality(1000)];
        cost_model.init_cost_model(&mut set_manager, &stats);

        let set = set_manager.get_relation(0);
        let card = cost_model.get_cardinality(&set);
        assert_eq!(card, 1000.0);
    }

    #[test]
    fn test_three_way_join_cost() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Create filters for A-B and B-C joins
        let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        let filter_bc = create_equality_filter(&mut set_manager, 1, 1, 2, 0, 1);
        cost_model
            .cardinality_estimator
            .init_equivalent_relations(&[filter_ab, filter_bc]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(
            1,
            [DistinctCount::new(50, true), DistinctCount::new(25, true)],
        );

        let mut stats2 = RelationStats::with_cardinality(200);
        stats2.column_distinct_count = column_distinct_counts(2, [DistinctCount::new(20, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1, stats2]);

        // Create leaf nodes
        let a = DPJoinNode::leaf(set_manager.get_relation(0));
        let b = DPJoinNode::leaf(set_manager.get_relation(1));
        let c = DPJoinNode::leaf(set_manager.get_relation(2));

        // Compare two join orders: (A ⋈ B) ⋈ C vs A ⋈ (B ⋈ C)
        let ab = cost_model.compute_cost_and_create_node(&a, &b, &mut set_manager, None);
        let ab_c = cost_model.compute_cost_and_create_node(&ab, &c, &mut set_manager, None);

        let bc = cost_model.compute_cost_and_create_node(&b, &c, &mut set_manager, None);
        let a_bc = cost_model.compute_cost_and_create_node(&a, &bc, &mut set_manager, None);

        // Both should have valid costs
        assert!(ab_c.cost > 0.0);
        assert!(a_bc.cost > 0.0);
    }

    #[test]
    fn selective_snowflake_path_beats_unfiltered_fact_dimension_path() {
        // Customer/order/lineitem/supplier/nation/region shape. The redundant
        // customer=nation edge is the transitive equality that makes the
        // selective dimension path representable without a Cartesian product.
        let mut sets = JoinRelationSetManager::new();
        let filters = vec![
            create_equality_filter(&mut sets, 0, 0, 1, 1, 0),
            create_equality_filter(&mut sets, 1, 0, 2, 0, 1),
            create_equality_filter(&mut sets, 2, 1, 3, 0, 2),
            create_equality_filter(&mut sets, 0, 1, 3, 1, 3),
            create_equality_filter(&mut sets, 3, 1, 4, 0, 4),
            create_equality_filter(&mut sets, 0, 1, 4, 0, 5),
            create_equality_filter(&mut sets, 4, 1, 5, 0, 6),
        ];
        let stats = vec![
            RelationStats {
                cardinality: 150_000,
                column_distinct_count: column_distinct_counts(
                    0,
                    [
                        DistinctCount::new(150_000, false),
                        DistinctCount::new(25, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 135_000,
                column_distinct_count: column_distinct_counts(
                    1,
                    [
                        DistinctCount::new(1_500_000, false),
                        DistinctCount::new(150_000, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 6_001_215,
                column_distinct_count: column_distinct_counts(
                    2,
                    [
                        DistinctCount::new(1_500_000, false),
                        DistinctCount::new(10_000, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 10_000,
                column_distinct_count: column_distinct_counts(
                    3,
                    [
                        DistinctCount::new(10_000, false),
                        DistinctCount::new(25, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 25,
                column_distinct_count: column_distinct_counts(
                    4,
                    [DistinctCount::new(25, false), DistinctCount::new(5, false)],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 1,
                column_distinct_count: column_distinct_counts(5, [DistinctCount::new(5, false)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
        ];
        let mut model = CostModel::new();
        model
            .cardinality_estimator
            .init_equivalent_relations(&filters);
        model.init_cost_model(&mut sets, &stats);

        let customer_nation_region = sets.get_relation_from_vec(vec![0, 4, 5]);
        let customer_orders = sets.get_relation_from_vec(vec![0, 1]);
        let customer_supplier = sets.get_relation_from_vec(vec![0, 3]);
        let filtered_orders = sets.get_relation_from_vec(vec![0, 1, 4, 5]);
        let customer_supplier_nation_region = sets.get_relation_from_vec(vec![0, 3, 4, 5]);
        let full_join = sets.get_relation_from_vec(vec![0, 1, 2, 3, 4, 5]);

        assert_eq!(model.get_cardinality(&customer_nation_region), 30_000.0);
        assert_eq!(model.get_cardinality(&customer_orders), 135_000.0);
        // The nation equality class is bounded by the 25-row nation domain,
        // even when nation itself is not yet part of this DP subset. Ignoring
        // that class-wide bound makes this explosive join look selective.
        assert_eq!(model.get_cardinality(&customer_supplier), 60_000_000.0);
        assert_eq!(model.get_cardinality(&filtered_orders), 27_000.0);
        assert_eq!(
            model.get_cardinality(&customer_supplier_nation_region),
            12_000_000.0
        );
        assert!((model.get_cardinality(&full_join) - 4_320.874_8).abs() < 1e-6);
    }

    #[test]
    fn equality_class_uses_each_observed_domain_once() {
        let mut sets = JoinRelationSetManager::new();
        let filters = vec![
            create_equality_filter(&mut sets, 0, 0, 1, 0, 0),
            create_equality_filter(&mut sets, 1, 0, 2, 0, 1),
        ];
        let stats = vec![
            RelationStats {
                cardinality: 10,
                column_distinct_count: column_distinct_counts(0, [DistinctCount::new(10, true)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 100,
                column_distinct_count: column_distinct_counts(1, [DistinctCount::new(100, true)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 1_000,
                column_distinct_count: column_distinct_counts(2, [DistinctCount::new(1_000, true)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
        ];
        let mut model = CostModel::new();
        model
            .cardinality_estimator
            .init_equivalent_relations(&filters);
        model.init_cost_model(&mut sets, &stats);

        let full_join = sets.get_relation_from_vec(vec![0, 1, 2]);
        assert_eq!(model.get_cardinality(&full_join), 10.0);
    }

    #[test]
    fn test_cross_product_cost() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // No join filters - this will be a cross product
        let stats = vec![
            RelationStats::with_cardinality(100),
            RelationStats::with_cardinality(50),
        ];
        cost_model.init_cost_model(&mut set_manager, &stats);

        let left = DPJoinNode::leaf(set_manager.get_relation(0));
        let right = DPJoinNode::leaf(set_manager.get_relation(1));

        let cost = cost_model.compute_cost(&left, &right, &mut set_manager);

        // Cross product cost should be high (100 * 50 = 5000)
        assert!(cost >= 5000.0);
    }
}
