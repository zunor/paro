// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Enumerate candidate join orders with DPccp and a greedy fallback.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_common::logging::targets;
use tracing::debug;

use crate::join_order::cost_model::{CostModel, DPJoinNode};
use crate::join_order::query_graph::{
    CutPredicateResolution, JoinPredicateSet, NeighborInfo, QueryGraphEdges,
};
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};

/// Threshold to switch from exact to approximate join order optimization.
pub const THRESHOLD_TO_SWAP_TO_APPROXIMATE: usize = 12;

/// Maximum number of pairs to consider before switching to greedy algorithm.
const MAX_PAIRS: usize = 10000;

/// The PlanEnumerator performs join order optimization using dynamic programming.
///
pub(crate) struct PlanEnumerator<'a> {
    /// The query graph containing edges between relations.
    query_graph: &'a QueryGraphEdges,
    /// The set manager for creating/looking up relation sets.
    set_manager: &'a mut JoinRelationSetManager,
    /// The cost model for evaluating join costs.
    cost_model: &'a mut CostModel,
    /// Number of relations in the query.
    num_relations: usize,
    /// The optimal plans found for each relation set.
    plans: HashMap<Arc<JoinRelationSet>, DPJoinNode>,
    /// The total number of join pairs considered.
    pairs: usize,
}

impl<'a> PlanEnumerator<'a> {
    /// Create a new PlanEnumerator.
    pub fn new(
        query_graph: &'a QueryGraphEdges,
        set_manager: &'a mut JoinRelationSetManager,
        cost_model: &'a mut CostModel,
        num_relations: usize,
    ) -> Self {
        Self {
            query_graph,
            set_manager,
            cost_model,
            num_relations,
            plans: HashMap::new(),
            pairs: 0,
        }
    }

    /// Initialize leaf plans (single relations).
    pub fn init_leaf_plans(&mut self) {
        for i in 0..self.num_relations {
            let set = self.set_manager.get_relation(i);
            let cardinality = self.cost_model.get_cardinality(&set);
            let node = DPJoinNode::leaf(
                set.clone(),
                self.cost_model.payload_width(set.as_ref()),
                cardinality,
            );

            self.plans.insert(set, node);
        }
    }

    /// Solve the join order using dynamic programming.
    ///
    /// Returns true if completed successfully (did not time out).
    pub fn solve_join_order(&mut self) -> bool {
        // For small graphs, try exact algorithm first
        if self.num_relations < THRESHOLD_TO_SWAP_TO_APPROXIMATE && self.solve_join_order_exactly()
        {
            // Check if we got a final plan
            let mut all_relations = HashSet::new();
            for i in 0..self.num_relations {
                all_relations.insert(i);
            }
            let total_set = self.set_manager.get_relation_from_set(&all_relations);

            if let Some(final_plan) = self.plans.get(&total_set) {
                debug!(
                    target: targets::OPTIMIZER,
                    relations = self.num_relations,
                    pairs = self.pairs,
                    left = %final_plan.left_set,
                    right = %final_plan.right_set,
                    cardinality = final_plan.cardinality,
                    cost = final_plan.cost,
                    "Completed exact join-order enumeration"
                );
                return true;
            }
        }

        // Fall back to approximate algorithm
        debug!(
            target: targets::OPTIMIZER,
            relations = self.num_relations,
            exact_pairs = self.pairs,
            "Falling back to greedy join-order enumeration"
        );
        self.solve_join_order_approximately()
    }

    /// Get the optimal plans.
    pub fn get_plans(&self) -> &HashMap<Arc<JoinRelationSet>, DPJoinNode> {
        &self.plans
    }

    /// Get the final plan for all relations.
    pub fn get_final_plan(&mut self) -> Option<&DPJoinNode> {
        let mut all_relations = HashSet::new();
        for i in 0..self.num_relations {
            all_relations.insert(i);
        }
        let total_set = self.set_manager.get_relation_from_set(&all_relations);
        self.plans.get(&total_set)
    }

    // Private methods

    /// Solve join order exactly using dynamic programming.
    fn solve_join_order_exactly(&mut self) -> bool {
        // Enumerate over all possible pairs in the neighborhood
        for i in (1..=self.num_relations).rev() {
            let start_node = self.set_manager.get_relation(i - 1);

            // Emit the start node
            if !self.emit_csg(&start_node) {
                return false;
            }

            // Initialize exclusion set as all nodes with number below this
            let mut exclusion_set = HashSet::new();
            for j in 0..i {
                exclusion_set.insert(j);
            }

            // Recursively search for neighbors not in exclusion set
            if !self.enumerate_csg_recursive(&start_node, &mut exclusion_set) {
                return false;
            }
        }
        true
    }

    /// Emit a connected subgraph (CSG).
    fn emit_csg(&mut self, node: &Arc<JoinRelationSet>) -> bool {
        if node.count() == self.num_relations {
            return true;
        }

        // Create exclusion set as everything inside the subgraph and anything below it
        let mut exclusion_set = HashSet::new();
        for i in 0..node.relations()[0] {
            exclusion_set.insert(i);
        }
        for &rel in node.relations() {
            exclusion_set.insert(rel);
        }

        // Find neighbors given this exclusion set
        let neighbors = self.query_graph.get_neighbors(node, &exclusion_set);
        if neighbors.is_empty() {
            return true;
        }

        // Neighbors should be in reverse order
        let mut neighbors = neighbors;
        neighbors.sort_by(|a, b| b.cmp(a));

        // Add neighbors to exclusion set for recursive calls
        let mut new_exclusion_set = exclusion_set.clone();
        for &neighbor in &neighbors {
            new_exclusion_set.insert(neighbor);
        }

        for neighbor_idx in neighbors {
            let neighbor_relation = self.set_manager.get_relation(neighbor_idx);

            // Check if connected
            let connections = self.query_graph.get_connections(node, &neighbor_relation);
            if !connections.is_empty()
                && !self.try_emit_pair(node, &neighbor_relation, &connections)
            {
                return false;
            }

            if !self.enumerate_cmp_recursive(node, &neighbor_relation, &mut new_exclusion_set) {
                return false;
            }

            new_exclusion_set.remove(&neighbor_idx);
        }

        true
    }

    /// Enumerate connected subgraphs recursively.
    fn enumerate_csg_recursive(
        &mut self,
        node: &Arc<JoinRelationSet>,
        exclusion_set: &mut HashSet<usize>,
    ) -> bool {
        // Find neighbors of S under the exclusion set
        let neighbors = self.query_graph.get_neighbors(node, exclusion_set);
        if neighbors.is_empty() {
            return true;
        }

        let all_subsets = get_all_neighbor_sets(neighbors.clone());
        let mut union_sets = Vec::new();

        for rel_set in &all_subsets {
            let neighbor = self.set_manager.get_relation_from_set(rel_set);
            let new_set = self.set_manager.union(node, &neighbor);

            if new_set.count() > node.count()
                && self.plans.contains_key(&new_set)
                && !self.emit_csg(&new_set)
            {
                return false;
            }
            union_sets.push(new_set);
        }

        let mut new_exclusion_set = exclusion_set.clone();
        for &neighbor in &neighbors {
            new_exclusion_set.insert(neighbor);
        }

        for union_set in union_sets {
            if !self.enumerate_csg_recursive(&union_set, &mut new_exclusion_set) {
                return false;
            }
        }

        true
    }

    /// Enumerate complement pairs recursively.
    fn enumerate_cmp_recursive(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        exclusion_set: &mut HashSet<usize>,
    ) -> bool {
        // Get neighbors of the second relation under the exclusion set
        let neighbors = self.query_graph.get_neighbors(right, exclusion_set);
        if neighbors.is_empty() {
            return true;
        }

        let all_subsets = get_all_neighbor_sets(neighbors.clone());
        let mut union_sets = Vec::new();

        for rel_set in &all_subsets {
            let neighbor = self.set_manager.get_relation_from_set(rel_set);
            let combined_set = self.set_manager.union(right, &neighbor);

            debug_assert!(combined_set.count() > right.count());

            if self.plans.contains_key(&combined_set) {
                let connections = self.query_graph.get_connections(left, &combined_set);
                if !connections.is_empty() && !self.try_emit_pair(left, &combined_set, &connections)
                {
                    return false;
                }
            }
            union_sets.push(combined_set);
        }

        let mut new_exclusion_set = exclusion_set.clone();
        for &neighbor in &neighbors {
            new_exclusion_set.insert(neighbor);
        }

        for union_set in union_sets {
            if !self.enumerate_cmp_recursive(left, &union_set, &mut new_exclusion_set) {
                return false;
            }
        }

        true
    }

    /// Try to emit a pair of relations.
    ///
    /// Returns false if too many pairs have been emitted.
    fn try_emit_pair(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        connections: &[NeighborInfo],
    ) -> bool {
        self.pairs += 1;
        if self.pairs >= MAX_PAIRS {
            return false;
        }

        self.emit_pair(left, right, connections)
    }

    /// Emit a pair of relations and create a join node.
    fn emit_pair(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        connections: &[NeighborInfo],
    ) -> bool {
        // Get the left and right plans
        let left_plan = match self.plans.get(left) {
            Some(plan) => plan.clone(),
            None => return true,
        };

        let right_plan = match self.plans.get(right) {
            Some(plan) => plan.clone(),
            None => return true,
        };

        // Create the join node
        let new_set = self.set_manager.union(left, right);
        let Some(new_node) = self.create_join_tree(&left_plan, &right_plan, connections) else {
            return false;
        };

        // Check if this is the best plan for this set
        let should_update = if let Some(existing) = self.plans.get(&new_set) {
            new_node.cost < existing.cost
        } else {
            true
        };

        if should_update {
            self.plans.insert(new_set, new_node);
        }
        true
    }

    fn create_join_tree(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        connections: &[NeighborInfo],
    ) -> Option<DPJoinNode> {
        let predicates = match Self::collect_cut_predicates(connections, &left.set, &right.set) {
            CutPredicateResolution::Resolved(predicates) => predicates,
            CutPredicateResolution::Ineligible => return None,
        };

        Some(self.cost_model.compute_cost_and_create_node(
            left,
            right,
            self.set_manager,
            predicates,
        ))
    }

    fn collect_cut_predicates(
        connections: &[NeighborInfo],
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> CutPredicateResolution {
        JoinPredicateSet::from_filters(
            connections
                .iter()
                .flat_map(|connection| &connection.filters),
            left,
            right,
        )
    }

    /// Solve join order approximately using a greedy algorithm.
    fn solve_join_order_approximately(&mut self) -> bool {
        // Start with all base relations
        let mut join_relations: Vec<Arc<JoinRelationSet>> = (0..self.num_relations)
            .map(|i| self.set_manager.get_relation(i))
            .collect();

        while join_relations.len() > 1 {
            let mut best_left = 0;
            let mut best_right = 0;
            let mut best_cost = f64::MAX;
            let mut found_connection = false;

            // Find the best pair to join
            for i in 0..join_relations.len() {
                for j in (i + 1)..join_relations.len() {
                    let connections = self
                        .query_graph
                        .get_connections(&join_relations[i], &join_relations[j]);

                    if !connections.is_empty() {
                        if !self.emit_pair(&join_relations[i], &join_relations[j], &connections) {
                            return false;
                        }

                        let combined = self
                            .set_manager
                            .union(&join_relations[i], &join_relations[j]);
                        if let Some(node) = self.plans.get(&combined) {
                            if node.cost < best_cost {
                                best_cost = node.cost;
                                best_left = i;
                                best_right = j;
                                found_connection = true;
                            }
                        }
                    }
                }
            }

            if !found_connection {
                // Fallback: just pick first two
                best_left = 0;
                best_right = 1;
                if !self.emit_pair(&join_relations[best_left], &join_relations[best_right], &[]) {
                    return false;
                }
            }

            // Ensure best_right > best_left for removal
            if best_left > best_right {
                std::mem::swap(&mut best_left, &mut best_right);
            }

            // Update join_relations
            let new_set = self
                .set_manager
                .union(&join_relations[best_left], &join_relations[best_right]);
            join_relations.remove(best_right);
            join_relations.remove(best_left);
            join_relations.push(new_set);
        }
        true
    }
}

/// Get all non-empty subsets of a set of neighbors.
///
/// This generates all 2^n - 1 subsets of the input set.
fn get_all_neighbor_sets(mut neighbors: Vec<usize>) -> Vec<HashSet<usize>> {
    neighbors.sort();

    let mut result = Vec::new();
    let mut added: Vec<HashSet<usize>> = neighbors
        .iter()
        .map(|&n| {
            let mut set = HashSet::new();
            set.insert(n);
            set
        })
        .collect();

    result.extend(added.clone());

    loop {
        added = add_super_sets(&added, &neighbors);
        if added.is_empty() {
            break;
        }
        result.extend(added.clone());
    }

    result
}

/// Add supersets by adding one more neighbor to each existing set.
fn add_super_sets(current: &[HashSet<usize>], all_neighbors: &[usize]) -> Vec<HashSet<usize>> {
    let mut result = Vec::new();

    for neighbor_set in current {
        let max_val = neighbor_set.iter().max().copied().unwrap_or(0);

        for &neighbor in all_neighbors {
            if neighbor <= max_val {
                continue;
            }
            if !neighbor_set.contains(&neighbor) {
                let mut new_set = neighbor_set.clone();
                new_set.insert(neighbor);
                result.push(new_set);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::join_order::query_graph::FilterInfo;
    use crate::join_order::relation_manager::{DistinctCount, RelationStats};
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
    fn join_cut_collects_all_crossing_predicates_once() {
        let mut set_manager = JoinRelationSetManager::new();
        let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        let filter_ac = create_equality_filter(&mut set_manager, 0, 1, 2, 0, 1);
        let connections = vec![
            NeighborInfo {
                neighbor: set_manager.get_relation(1),
                filters: vec![Arc::clone(&filter_ab)],
            },
            NeighborInfo {
                neighbor: set_manager.get_relation(2),
                filters: vec![filter_ab, filter_ac],
            },
        ];

        let left = set_manager.get_relation(0);
        let right = set_manager.get_relation_from_vec(vec![1, 2]);
        let CutPredicateResolution::Resolved(Some(predicates)) =
            PlanEnumerator::collect_cut_predicates(&connections, &left, &right)
        else {
            panic!("join cut should contain valid predicates")
        };

        assert_eq!(predicates.predicates().len(), 2);
        assert_eq!(predicates.predicates()[0].filter().filter_index, 0);
        assert_eq!(predicates.predicates()[1].filter().filter_index, 1);
    }

    #[test]
    fn test_get_all_neighbor_sets() {
        let neighbors = vec![1, 2, 3];
        let sets = get_all_neighbor_sets(neighbors);

        // Should have 2^3 - 1 = 7 subsets
        assert_eq!(sets.len(), 7);
    }

    #[test]
    fn test_get_all_neighbor_sets_single() {
        let neighbors = vec![1];
        let sets = get_all_neighbor_sets(neighbors);

        assert_eq!(sets.len(), 1);
        assert!(sets[0].contains(&1));
    }

    #[test]
    fn test_add_super_sets() {
        let mut set1 = HashSet::new();
        set1.insert(1);

        let current = vec![set1];
        let all_neighbors = vec![1, 2, 3];

        let result = add_super_sets(&current, &all_neighbors);

        // Should add {1,2} and {1,3}
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_plan_enumerator_init_leaf_plans() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();
        let query_graph = QueryGraphEdges::new();

        // Initialize cost model
        let stats = vec![
            RelationStats::with_cardinality(1000),
            RelationStats::with_cardinality(500),
        ];
        cost_model.init_cost_model(&mut set_manager, &stats);

        let mut enumerator =
            PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
        enumerator.init_leaf_plans();

        assert_eq!(enumerator.plans.len(), 2);

        // Get the key before borrowing enumerator
        let set0_key = enumerator.set_manager.get_relation(0);

        let plan0 = enumerator.plans.get(&set0_key).unwrap();
        assert!(plan0.is_leaf);
        assert_eq!(plan0.cardinality, 1000.0);
    }

    #[test]
    fn test_plan_enumerator_two_relations() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();
        let mut query_graph = QueryGraphEdges::new();

        // Create join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model
            .cardinality_estimator
            .init_equivalent_relations(&[filter.clone()]);

        // Add edge to query graph
        let left = set_manager.get_relation(0);
        let right = set_manager.get_relation(1);
        query_graph.create_edge(&left, right.clone(), Some(filter.clone()));
        query_graph.create_edge(&right, left, Some(filter));

        // Initialize cost model
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        let mut enumerator =
            PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
        enumerator.init_leaf_plans();
        assert!(enumerator.solve_join_order());

        // Should have plans for both single relations and the join
        assert!(enumerator.plans.len() >= 2);

        // Check final plan exists
        let final_plan = enumerator.get_final_plan();
        assert!(final_plan.is_some());
        assert!(final_plan.unwrap().predicates.is_some());
    }

    #[test]
    fn test_plan_enumerator_three_relations_chain() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();
        let mut query_graph = QueryGraphEdges::new();

        // Create chain: A - B - C
        let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        let filter_bc = create_equality_filter(&mut set_manager, 1, 1, 2, 0, 1);

        cost_model
            .cardinality_estimator
            .init_equivalent_relations(&[filter_ab.clone(), filter_bc.clone()]);

        // Add edges
        let r0 = set_manager.get_relation(0);
        let r1 = set_manager.get_relation(1);
        let r2 = set_manager.get_relation(2);

        query_graph.create_edge(&r0, r1.clone(), Some(filter_ab.clone()));
        query_graph.create_edge(&r1, r0, Some(filter_ab));
        query_graph.create_edge(&r1, r2.clone(), Some(filter_bc.clone()));
        query_graph.create_edge(&r2, r1, Some(filter_bc));

        // Initialize cost model
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

        let mut enumerator =
            PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 3);
        enumerator.init_leaf_plans();
        assert!(enumerator.solve_join_order());

        // Check final plan exists
        let final_plan = enumerator.get_final_plan();
        assert!(final_plan.is_some());

        let plan = final_plan.unwrap();
        assert_eq!(plan.set.count(), 3);
        assert!(plan.cost > 0.0);
        assert!(plan.predicates.is_some());
    }

    #[test]
    fn test_plan_enumerator_cross_product() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();
        let query_graph = QueryGraphEdges::new();

        // No join conditions - will need cross product
        let stats = vec![
            RelationStats::with_cardinality(100),
            RelationStats::with_cardinality(50),
        ];
        cost_model.init_cost_model(&mut set_manager, &stats);

        let mut enumerator =
            PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
        enumerator.init_leaf_plans();
        assert!(enumerator.solve_join_order());

        // Should still produce a final plan (via cross product)
        let final_plan = enumerator.get_final_plan();
        assert!(
            final_plan.is_some(),
            "Final plan should exist for cross product"
        );
    }

    #[test]
    fn test_plan_enumerator_approximate() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();
        let query_graph = QueryGraphEdges::new();

        // Create many relations to trigger approximate algorithm
        let num_relations = THRESHOLD_TO_SWAP_TO_APPROXIMATE + 1;
        let stats: Vec<_> = (0..num_relations)
            .map(|_| RelationStats::with_cardinality(1000))
            .collect();

        cost_model.init_cost_model(&mut set_manager, &stats);

        let mut enumerator = PlanEnumerator::new(
            &query_graph,
            &mut set_manager,
            &mut cost_model,
            num_relations,
        );
        enumerator.init_leaf_plans();

        let result = enumerator.solve_join_order();
        assert!(result);
    }
}
