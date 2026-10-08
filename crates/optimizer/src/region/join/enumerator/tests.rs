// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::*;
use crate::region::join::query_graph::FilterInfo;
use crate::region::join::relation_manager::{DistinctCount, RelationStats};
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, ComparisonExpression, ComparisonType};
use paro_planner::logical::operator::{AntiJoinMode, ColumnBinding, JoinType};

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
    paro_planner::expression::Expression::ColumnRef(
        ColumnRefExpression {
            binding: paro_planner::logical::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        }
        .into(),
    )
}

fn create_equality_filter(
    set_manager: &mut JoinRelationSetManager,
    left_table: usize,
    left_col: usize,
    right_table: usize,
    right_col: usize,
    filter_index: usize,
) -> Arc<FilterInfo> {
    let expr = paro_planner::expression::Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(left_table, left_col)),
            right: Box::new(create_column_ref(right_table, right_col)),
            comparison_type: ComparisonType::Equal,
        }
        .into(),
    );

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

fn create_reduction_filter(
    set_manager: &mut JoinRelationSetManager,
    preserved: usize,
    filtering: usize,
    filter_index: usize,
) -> Arc<FilterInfo> {
    let expression = paro_planner::expression::Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(preserved, 0)),
            right: Box::new(create_column_ref(filtering, 0)),
            comparison_type: ComparisonType::Equal,
        }
        .into(),
    );
    let set =
        set_manager.get_relation_from_vec(vec![preserved.min(filtering), preserved.max(filtering)]);
    let mut filter = FilterInfo::new(
        expression,
        set,
        filter_index,
        JoinType::Semi,
        AntiJoinMode::Regular,
    );
    filter.set_left_set(set_manager.get_relation(preserved));
    filter.set_right_set(set_manager.get_relation(filtering));
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
fn ineligible_exact_cut_does_not_fall_through_to_greedy_state() {
    let mut set_manager = JoinRelationSetManager::new();
    let forward = create_reduction_filter(&mut set_manager, 0, 1, 0);
    let inverted = create_reduction_filter(&mut set_manager, 1, 0, 1);
    let left = set_manager.get_relation(0);
    let right = set_manager.get_relation(1);
    let mut query_graph = QueryGraphEdges::new();
    for filter in [forward, inverted] {
        query_graph.create_edge(&left, Arc::clone(&right), Some(Arc::clone(&filter)));
        query_graph.create_edge(&right, Arc::clone(&left), Some(filter));
    }
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    cost_model.init_cost_model(
        &mut set_manager,
        &[
            RelationStats::with_cardinality(10),
            RelationStats::with_cardinality(10),
        ],
    );
    let mut enumerator = PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
    enumerator.init_leaf_plans();

    assert_eq!(
        enumerator.solve_join_order(),
        EnumerationOutcome::Ineligible
    );
    assert_eq!(
        enumerator.plans.len(),
        2,
        "an ineligible exact region must leave only authoritative leaf plans"
    );
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
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    let query_graph = QueryGraphEdges::new();

    // Initialize cost model
    let stats = vec![
        RelationStats::with_cardinality(1000),
        RelationStats::with_cardinality(500),
    ];
    cost_model.init_cost_model(&mut set_manager, &stats);

    let mut enumerator = PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
    enumerator.init_leaf_plans();

    assert_eq!(enumerator.plans.len(), 2);

    // Get the key before borrowing enumerator
    let set0_key = enumerator.set_manager.get_relation(0);

    let plan0 = enumerator.plans.get(&set0_key).unwrap();
    assert!(plan0[0].is_leaf);
    assert_eq!(plan0[0].cardinality, 1000.0);
}

#[test]
fn greedy_missing_input_is_not_reported_as_semantic_ineligibility() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    let query_graph = QueryGraphEdges::new();
    cost_model.init_cost_model(
        &mut set_manager,
        &[
            RelationStats::with_cardinality(10),
            RelationStats::with_cardinality(10),
        ],
    );
    let mut enumerator = PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
    enumerator.init_leaf_plans();
    let missing = enumerator.set_manager.get_relation(1);
    enumerator.plans.remove(&missing);

    assert_eq!(
        enumerator.solve_join_order_approximately(),
        EnumerationOutcome::MissingSubplan
    );
}

#[test]
fn calibrated_region_matches_independent_exhaustive_bipartitions() {
    // Enumerate every labelled binary tree independently of DPccp's
    // neighbor traversal and frontier admission. The cost kernel is the
    // common contract, not a second implementation of physical costing.
    fn exhaustive(
        mask: u8,
        leaves: &[DPJoinNode],
        filters: &[Arc<FilterInfo>],
        sets: &mut JoinRelationSetManager,
        costs: &mut RegionCostModel,
    ) -> Vec<DPJoinNode> {
        if mask.count_ones() == 1 {
            return vec![leaves[mask.trailing_zeros() as usize].clone()];
        }
        let mut result = Vec::new();
        let mut left_mask = (mask - 1) & mask;
        while left_mask != 0 {
            let right_mask = mask ^ left_mask;
            if left_mask < right_mask {
                let left = exhaustive(left_mask, leaves, filters, sets, costs);
                let right = exhaustive(right_mask, leaves, filters, sets, costs);
                for a in &left {
                    for b in &right {
                        if let CutPredicateResolution::Resolved(Some(predicates)) =
                            JoinPredicateSet::from_filters(filters, &a.set, &b.set)
                        {
                            result.push(costs.compute_cost_and_create_node(
                                a,
                                b,
                                sets,
                                Some(predicates),
                            ));
                        }
                    }
                }
            }
            left_mask = (left_mask - 1) & mask;
        }
        result
    }
    for rows in [[10, 200, 3, 800], [1000, 2, 400, 30]] {
        let mut sets = JoinRelationSetManager::new();
        let mut costs = RegionCostModel::new(Default::default());
        costs.regional_pricing = Some(
            crate::cost::join::JoinWorkPricing::new(
                &crate::cost::calibration::MachineCalibrationBundle::builtin_production(),
            )
            .unwrap(),
        );
        let mut graph = QueryGraphEdges::new();
        let mut filters = Vec::new();
        for a in 0..4 {
            for b in a + 1..4 {
                let filter = create_equality_filter(&mut sets, a, 0, b, 0, filters.len());
                let left = sets.get_relation(a);
                let right = sets.get_relation(b);
                graph.create_edge(&left, right.clone(), Some(filter.clone()));
                graph.create_edge(&right, left, Some(filter.clone()));
                filters.push(filter);
            }
        }
        costs.init_equivalent_relations(&filters);
        costs.init_cost_model(&mut sets, &rows.map(RelationStats::with_cardinality));
        let mut dp =
            PlanEnumerator::with_budget(&graph, &mut sets, &mut costs, 4, 12, 10_000, 1024);
        dp.init_leaf_plans();
        let leaves = (0..4)
            .map(|i| dp.plans[&dp.set_manager.get_relation(i)][0].clone())
            .collect::<Vec<_>>();
        assert_eq!(dp.solve_join_order(), EnumerationOutcome::Complete);
        let selected = dp.get_final_plan().unwrap().cost;
        drop(dp);
        let oracle = exhaustive(15, &leaves, &filters, &mut sets, &mut costs)
            .into_iter()
            .map(|node| node.cost)
            .min_by(f64::total_cmp)
            .unwrap();
        assert!((selected - oracle).abs() <= oracle.abs() * 1e-12);
    }
}

#[test]
fn test_plan_enumerator_two_relations() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    let mut query_graph = QueryGraphEdges::new();

    // Create join filter
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    cost_model.init_equivalent_relations(&[filter.clone()]);

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

    let mut enumerator = PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
    enumerator.init_leaf_plans();
    assert_eq!(enumerator.solve_join_order(), EnumerationOutcome::Complete);

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
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    let mut query_graph = QueryGraphEdges::new();

    // Create chain: A - B - C
    let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    let filter_bc = create_equality_filter(&mut set_manager, 1, 1, 2, 0, 1);

    cost_model.init_equivalent_relations(&[filter_ab.clone(), filter_bc.clone()]);

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

    let mut enumerator = PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 3);
    enumerator.init_leaf_plans();
    assert_eq!(enumerator.solve_join_order(), EnumerationOutcome::Complete);

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
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    let query_graph = QueryGraphEdges::new();

    // No join conditions - will need cross product
    let stats = vec![
        RelationStats::with_cardinality(100),
        RelationStats::with_cardinality(50),
    ];
    cost_model.init_cost_model(&mut set_manager, &stats);

    let mut enumerator = PlanEnumerator::new(&query_graph, &mut set_manager, &mut cost_model, 2);
    enumerator.init_leaf_plans();
    assert_eq!(enumerator.solve_join_order(), EnumerationOutcome::Complete);

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
    let mut cost_model =
        RegionCostModel::new(crate::estimate::selectivity::SelectivityDefaults::default());
    let query_graph = QueryGraphEdges::new();

    // Create many relations to trigger approximate algorithm
    let num_relations = 13;
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
    assert_eq!(result, EnumerationOutcome::Approximate);
}

/// Cheapest complete plan of a region with `rows` relations joined along
/// `edges`. `exact_relation_limit` 0 forces the greedy-seeded path;
/// `greedy_only` stops after the seed.
fn plan_cost(
    edges: &[(usize, usize)],
    rows: &[u64],
    exact_relation_limit: usize,
    greedy_only: bool,
) -> f64 {
    let mut sets = JoinRelationSetManager::new();
    let mut costs = RegionCostModel::new(Default::default());
    costs.regional_pricing = Some(
        crate::cost::join::JoinWorkPricing::new(
            &crate::cost::calibration::MachineCalibrationBundle::builtin_production(),
        )
        .unwrap(),
    );
    let mut graph = QueryGraphEdges::new();
    let mut filters = Vec::new();
    for &(a, b) in edges {
        let filter = create_equality_filter(&mut sets, a, 0, b, 0, filters.len());
        let left = sets.get_relation(a);
        let right = sets.get_relation(b);
        graph.create_edge(&left, right.clone(), Some(filter.clone()));
        graph.create_edge(&right, left, Some(filter.clone()));
        filters.push(filter);
    }
    costs.init_equivalent_relations(&filters);
    let stats = rows
        .iter()
        .map(|&rows| RelationStats::with_cardinality(rows as usize))
        .collect::<Vec<_>>();
    costs.init_cost_model(&mut sets, &stats);
    let mut dp = PlanEnumerator::with_budget(
        &graph,
        &mut sets,
        &mut costs,
        rows.len(),
        exact_relation_limit,
        1_000_000,
        16,
    );
    dp.init_leaf_plans();
    if greedy_only {
        assert_eq!(
            dp.solve_join_order_approximately(),
            EnumerationOutcome::Approximate
        );
    } else {
        dp.solve_join_order();
    }
    dp.get_final_plans()
        .iter()
        .map(|plan| plan.cost)
        .min_by(f64::total_cmp)
        .unwrap()
}

#[test]
fn linearized_refinement_never_loses_to_its_greedy_seed() {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        1 + state % bound
    };
    let mut improved = 0;
    for case in 0..24 {
        let relations = 7 + case % 5;
        // Alternate chains, stars and chains with a hub branch.
        let edges = (1..relations)
            .map(|relation| match case % 3 {
                0 => (relation - 1, relation),
                1 => (0, relation),
                _ if relation % 3 == 0 => (relation / 3, relation),
                _ => (relation - 1, relation),
            })
            .collect::<Vec<_>>();
        let rows = (0..relations)
            .map(|_| next(10) * 10_u64.pow(next(5) as u32))
            .collect::<Vec<_>>();
        let greedy = plan_cost(&edges, &rows, 0, true);
        let refined = plan_cost(&edges, &rows, 0, false);
        let exact = plan_cost(&edges, &rows, 12, false);
        assert!(
            refined <= greedy * (1.0 + 1e-12),
            "case {case}: {refined} > {greedy}"
        );
        assert!(
            exact <= refined * (1.0 + 1e-12),
            "case {case}: {exact} > {refined}"
        );
        if refined < greedy * (1.0 - 1e-9) {
            improved += 1;
        }
    }
    assert!(improved > 0, "linearized DP never improved a greedy seed");
}
