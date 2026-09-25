// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::*;
use crate::region::join::query_graph::{CutPredicateResolution, FilterInfo, JoinPredicateSet};
use crate::region::join::relation_manager::DistinctCount;
use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, OperatorExpression, OperatorType,
};
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
    create_comparison_filter(
        set_manager,
        left_table,
        left_col,
        right_table,
        right_col,
        filter_index,
        ComparisonType::Equal,
    )
}

fn create_comparison_filter(
    set_manager: &mut JoinRelationSetManager,
    left_table: usize,
    left_col: usize,
    right_table: usize,
    right_col: usize,
    filter_index: usize,
    comparison_type: ComparisonType,
) -> Arc<FilterInfo> {
    let expr = paro_planner::expression::Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(left_table, left_col)),
            right: Box::new(create_column_ref(right_table, right_col)),
            comparison_type,
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
    preserved_table: usize,
    preserved_col: usize,
    filtering_table: usize,
    filtering_col: usize,
    filter_index: usize,
    join_type: JoinType,
) -> Arc<FilterInfo> {
    assert!(matches!(join_type, JoinType::Semi | JoinType::Anti));
    let expr = paro_planner::expression::Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(preserved_table, preserved_col)),
            right: Box::new(create_column_ref(filtering_table, filtering_col)),
            comparison_type: ComparisonType::Equal,
        }
        .into(),
    );
    let set = set_manager.get_relation_from_vec(vec![preserved_table, filtering_table]);
    let preserved_set = set_manager.get_relation(preserved_table);
    let filtering_set = set_manager.get_relation(filtering_table);
    let mut filter = FilterInfo::new(expr, set, filter_index, join_type, AntiJoinMode::Regular);
    filter.set_left_set(preserved_set);
    filter.set_right_set(filtering_set);
    filter.set_left_binding(
        ColumnBinding::new(preserved_table, preserved_col),
        preserved_table,
    );
    filter.set_right_binding(
        ColumnBinding::new(filtering_table, filtering_col),
        filtering_table,
    );
    Arc::new(filter)
}

fn predicate_set(filters: &[Arc<FilterInfo>]) -> JoinPredicateSet {
    let filter = filters.first().expect("non-empty join predicate set");
    let crate::region::join::query_graph::CutPredicateResolution::Resolved(Some(predicates)) =
        JoinPredicateSet::from_filters(
            filters.iter(),
            filter.left_set().map_or(filter.set.as_ref(), Arc::as_ref),
            filter.right_set().map_or(filter.set.as_ref(), Arc::as_ref),
        )
    else {
        panic!("expected a valid non-empty join predicate set")
    };
    predicates
}

fn leaf(model: &mut RegionCostModel, set: Arc<JoinRelationSet>) -> DPJoinNode {
    let cardinality = model.get_cardinality(set.as_ref());
    let risk_cardinality = model.get_risk_cardinality(set.as_ref());
    DPJoinNode::leaf(
        set.clone(),
        model.payload_width(set.as_ref()),
        cardinality,
        risk_cardinality,
        model.get_materialization_cardinality(set.as_ref()),
    )
}

#[test]
fn test_cost_model_new() {
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());
    // Just verify it can be created
    assert_eq!(cost_model.get_cardinality(&JoinRelationSet::empty()), 1.0);
}

#[test]
fn test_dp_join_node_leaf() {
    let mut set_manager = JoinRelationSetManager::new();
    let set = set_manager.get_relation(0);

    let node = DPJoinNode::leaf(set.clone(), 7, 0.0, 0.0, 0.0);

    assert!(node.is_leaf);
    assert_eq!(node.cost, 0.0);
    assert_eq!(node.cardinality, 0.0);
    assert_eq!(node.output_payload_width, 7);
    assert!(Arc::ptr_eq(&node.set, &set));
}

#[test]
fn test_dp_join_node_intermediate() {
    let mut set_manager = JoinRelationSetManager::new();
    let left = set_manager.get_relation(0);
    let right = set_manager.get_relation(1);
    let combined = set_manager.union(&left, &right);

    let node = DPJoinNode::intermediate(
        None,
        &DPJoinNode::leaf(left.clone(), 5, 10.0, 10.0, 10.0),
        &DPJoinNode::leaf(right.clone(), 6, 5.0, 5.0, 5.0),
        CostedJoin {
            combination: combined.clone(),
            cardinality: 50.0,
            risk_cardinality: 50.0,
            materialization_cardinality: 50.0,
            materialization_is_reduction_bound: false,
            output_payload_width: 11,
            breakdown: JoinCostBreakdown {
                build: 25.0,
                probe: 25.0,
                match_output: 25.0,
                children: 25.0,
            },
            build_side: JoinBuildSide::Right,
            peak_build_bytes: 30,
        },
    );

    assert!(!node.is_leaf);
    assert!(node.predicates.is_none());
    assert_eq!(node.cost, 100.0);
    assert_eq!(node.cardinality, 50.0);
    assert_eq!(node.output_payload_width, 11);
    assert!(Arc::ptr_eq(&node.left_set, &left));
    assert!(Arc::ptr_eq(&node.right_set, &right));
}

#[test]
fn test_init_cost_model() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

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
fn leaf_risk_cardinality_does_not_replace_its_expected_estimate() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());
    let mut stats = RelationStats::with_cardinality(100);
    stats.risk_cardinality = 500;
    cost_model.init_cost_model(&mut set_manager, &[stats]);

    let set = set_manager.get_relation(0);
    assert_eq!(cost_model.get_cardinality(&set), 100.0);
    assert_eq!(cost_model.get_risk_cardinality(&set), 500.0);
}

#[test]
fn hash_orientation_uses_materialization_risk_without_repricing_cpu_work() {
    let mut sets = JoinRelationSetManager::new();
    let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
    let predicates = predicate_set(std::slice::from_ref(&filter));
    let mut left_stats = RelationStats::with_cardinality(10);
    left_stats.risk_cardinality = 10;
    left_stats.materialization_cardinality = 10_000;
    left_stats.estimated_payload_width = 8;
    left_stats.column_distinct_count =
        column_distinct_counts(0, [DistinctCount::new(10_000, true)]);
    let mut right_stats = RelationStats::with_cardinality(100);
    right_stats.risk_cardinality = 100;
    right_stats.materialization_cardinality = 100;
    right_stats.estimated_payload_width = 8;
    right_stats.column_distinct_count =
        column_distinct_counts(1, [DistinctCount::new(10_000, true)]);

    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_equivalent_relations(std::slice::from_ref(&filter));
    model.init_cost_model(&mut sets, &[left_stats, right_stats]);
    let combined = sets.get_relation_from_vec(vec![0, 1]);
    assert_eq!(model.materialization_cardinality(&combined), 10_000.0);
    let left = leaf(&mut model, sets.get_relation(0));
    let right = leaf(&mut model, sets.get_relation(1));
    let node = model.compute_cost_and_create_node(&left, &right, &mut sets, Some(predicates));

    assert_eq!(node.build_side, JoinBuildSide::Right);
    let conditions = RegionCostModel::condition_profile(node.predicates.as_ref());
    let expected_right_build = HashInputEstimate {
        rows: right.risk_cardinality,
        projected_payload_width: right.output_payload_width,
        condition_payload_width: conditions.right_payload_width,
        contains_control_region: false,
    }
    .execution_build_work();
    let breakdown =
        model.compute_cost_breakdown(&left, &right, &mut sets, node.predicates.as_ref());
    assert_eq!(breakdown.build, expected_right_build);
}

#[test]
fn committed_region_uses_expected_work_without_erasing_resource_risk() {
    let mut sets = JoinRelationSetManager::new();
    let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
    let mut left_stats = RelationStats::with_cardinality(10);
    left_stats.materialization_cardinality = 1_000_000;
    left_stats.estimated_payload_width = 8;
    let mut right_stats = RelationStats::with_cardinality(100_000);
    right_stats.estimated_payload_width = 128;
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.regional_pricing = Some(
        crate::cost::join::JoinWorkPricing::new(
            &crate::cost::calibration::MachineCalibrationBundle::builtin_production(),
        )
        .unwrap(),
    );
    model.init_equivalent_relations(std::slice::from_ref(&filter));
    model.init_cost_model(&mut sets, &[left_stats, right_stats]);
    let left = leaf(&mut model, sets.get_relation(0));
    let right = leaf(&mut model, sets.get_relation(1));
    let node = model.compute_cost_and_create_node(
        &left,
        &right,
        &mut sets,
        Some(predicate_set(std::slice::from_ref(&filter))),
    );
    assert_eq!(node.build_side, JoinBuildSide::Left);
    assert!(node.materialization_cardinality >= 1_000_000.0);
}

#[test]
fn residual_activation_requires_full_support_and_is_not_multiplied_on_revisit() {
    let mut sets = JoinRelationSetManager::new();
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_cost_model(
        &mut sets,
        &[
            RelationStats::with_cardinality(10),
            RelationStats::with_cardinality(10),
            RelationStats::with_cardinality(10),
        ],
    );
    let pair = sets.get_relation_from_vec(vec![0, 1]);
    let all = sets.get_relation_from_vec(vec![0, 1, 2]);
    let pair_before = model.get_cardinality(&pair);
    let all_before = model.get_cardinality(&all);
    model.residual_selectivities.push((all.clone(), 0.25));
    assert_eq!(model.get_cardinality(&pair), pair_before);
    assert_eq!(model.get_cardinality(&all), all_before * 0.25);
    assert_eq!(model.get_cardinality(&all), all_before * 0.25);
    let last = sets.get_relation(2);
    assert_eq!(
        model.cardinality_before_activation(&all, &pair, &last),
        all_before
    );
    model.residual_selectivities.push((pair.clone(), 0.5));
    assert_eq!(
        model.cardinality_before_activation(&all, &pair, &last),
        all_before * 0.5
    );
    assert_eq!(model.get_cardinality(&all), all_before * 0.125);
}

#[test]
fn materialization_rows_keep_their_own_distinct_domains() {
    let mut sets = JoinRelationSetManager::new();
    let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
    let mut left_stats = RelationStats::with_cardinality(10);
    left_stats.materialization_cardinality = 1_000_000;
    left_stats.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(10, false)]);
    left_stats.materialization_distinct_count =
        column_distinct_counts(0, [DistinctCount::new(1_000_000, false)]);
    let mut right_stats = RelationStats::with_cardinality(10);
    right_stats.materialization_cardinality = 1_000_000;
    right_stats.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(10, false)]);
    right_stats.materialization_distinct_count =
        column_distinct_counts(1, [DistinctCount::new(1_000_000, false)]);

    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_equivalent_relations(std::slice::from_ref(&filter));
    model.init_cost_model(&mut sets, &[left_stats, right_stats]);

    let joined = sets.get_relation_from_vec(vec![0, 1]);
    assert_eq!(model.materialization_cardinality(&joined), 1_000_000.0);
}

#[test]
fn test_compute_cost_leaf_nodes() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

    // Initialize with join filter
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    cost_model.init_equivalent_relations(&[filter]);

    // Initialize relation stats
    let mut stats0 = RelationStats::with_cardinality(1000);
    stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

    let mut stats1 = RelationStats::with_cardinality(500);
    stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

    cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

    // Create leaf nodes
    let left = leaf(&mut cost_model, set_manager.get_relation(0));
    let right = leaf(&mut cost_model, set_manager.get_relation(1));

    // Compute cost
    let cost = cost_model.compute_cost(&left, &right, &mut set_manager, None);

    // Cost should be join cardinality + 0 + 0 (leaf costs are 0)
    // Join cardinality = (1000 * 500) / max(100, 50) = 5000
    assert!(cost > 0.0);
}

#[test]
fn test_compute_cost_with_existing_costs() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

    // Initialize with join filter
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    cost_model.init_equivalent_relations(&[filter]);

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
        left_plan: None,
        right_plan: None,
        build_side: JoinBuildSide::Right,
        cost: 100.0,
        cardinality: 1000.0,
        cardinality_provenance: CardinalityProvenance::Statistics,
        risk_cardinality: 1000.0,
        materialization_cardinality: 1000.0,
        materialization_is_reduction_bound: false,
        output_payload_width: cost_model.payload_width(&left_set),
        peak_build_bytes: 0,
        shape: Arc::from("left"),
    };

    let right = DPJoinNode {
        set: right_set.clone(),
        predicates: None,
        is_leaf: false,
        left_set: right_set.clone(),
        right_set: right_set.clone(),
        left_plan: None,
        right_plan: None,
        build_side: JoinBuildSide::Right,
        cost: 50.0,
        cardinality: 500.0,
        cardinality_provenance: CardinalityProvenance::Statistics,
        risk_cardinality: 500.0,
        materialization_cardinality: 500.0,
        materialization_is_reduction_bound: false,
        output_payload_width: cost_model.payload_width(&right_set),
        peak_build_bytes: 0,
        shape: Arc::from("right"),
    };

    // Compute cost
    let cost = cost_model.compute_cost(&left, &right, &mut set_manager, None);

    // Cost should include left.cost + right.cost
    assert!(cost >= 150.0); // At least the sum of child costs
}

#[test]
fn test_compute_cost_and_create_node() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

    // Initialize with join filter
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    cost_model.init_equivalent_relations(&[filter]);

    // Initialize relation stats
    let mut stats0 = RelationStats::with_cardinality(1000);
    stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

    let mut stats1 = RelationStats::with_cardinality(500);
    stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

    cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

    // Create leaf nodes
    let left = leaf(&mut cost_model, set_manager.get_relation(0));
    let right = leaf(&mut cost_model, set_manager.get_relation(1));

    // Create join node
    let join_node = cost_model.compute_cost_and_create_node(&left, &right, &mut set_manager, None);

    assert!(!join_node.is_leaf);
    assert!(join_node.cost > 0.0);
    assert!(join_node.cardinality > 0.0);
    assert_eq!(join_node.set.count(), 2);
}

#[test]
fn unknown_ranking_prior_is_not_promoted_by_join_composition() {
    let mut sets = JoinRelationSetManager::new();
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    let mut unknown = RelationStats::with_cardinality(1000);
    unknown.cardinality_provenance = CardinalityProvenance::Unknown;
    model.init_cost_model(&mut sets, &[unknown, RelationStats::with_cardinality(50)]);
    let mut left = leaf(&mut model, sets.get_relation(0));
    left.cardinality_provenance = model.relation_provenance(0);
    let right = leaf(&mut model, sets.get_relation(1));
    let join = model.compute_cost_and_create_node(&left, &right, &mut sets, None);
    assert!(join.cardinality > 1.0);
    assert_eq!(join.cardinality_provenance, CardinalityProvenance::Unknown);
}

#[test]
fn test_get_cardinality() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

    let stats = vec![RelationStats::with_cardinality(1000)];
    cost_model.init_cost_model(&mut set_manager, &stats);

    let set = set_manager.get_relation(0);
    let card = cost_model.get_cardinality(&set);
    assert_eq!(card, 1000.0);
}

#[test]
fn test_three_way_join_cost() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

    // Create filters for A-B and B-C joins
    let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    let filter_bc = create_equality_filter(&mut set_manager, 1, 1, 2, 0, 1);
    cost_model.init_equivalent_relations(&[filter_ab, filter_bc]);

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
    let a = leaf(&mut cost_model, set_manager.get_relation(0));
    let b = leaf(&mut cost_model, set_manager.get_relation(1));
    let c = leaf(&mut cost_model, set_manager.get_relation(2));

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
fn weak_anti_reduction_follows_selective_inner_join() {
    // TPC-H Q16 shape: partsupp is first narrowed by a selective part
    // predicate; a tiny supplier exclusion then probes only those rows.
    // ANTI-first barely reduces the fact input and must not win merely on
    // the smaller cardinality of that first intermediate.
    let mut sets = JoinRelationSetManager::new();
    let part_filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
    let supplier_exclusion = create_reduction_filter(&mut sets, 0, 1, 2, 0, 1, JoinType::Anti);
    let filters = vec![part_filter.clone(), supplier_exclusion.clone()];

    let mut partsupp_stats = RelationStats::with_cardinality(800_000);
    partsupp_stats.estimated_payload_width = 16;
    partsupp_stats.column_distinct_count = column_distinct_counts(
        0,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    let mut part_stats = RelationStats::with_cardinality(30_000);
    part_stats.estimated_payload_width = 40;
    part_stats.column_distinct_count =
        column_distinct_counts(1, [DistinctCount::new(30_000, true)]);
    let mut supplier_stats = RelationStats::with_cardinality(112);
    supplier_stats.estimated_payload_width = 8;
    supplier_stats.column_distinct_count =
        column_distinct_counts(2, [DistinctCount::new(112, true)]);

    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_equivalent_relations(&filters);
    model.init_cost_model(&mut sets, &[partsupp_stats, part_stats, supplier_stats]);

    let mut partsupp = leaf(&mut model, sets.get_relation(0));
    partsupp.cardinality = model.get_cardinality(&partsupp.set);
    let mut part = leaf(&mut model, sets.get_relation(1));
    part.cardinality = model.get_cardinality(&part.set);
    let mut supplier = leaf(&mut model, sets.get_relation(2));
    supplier.cardinality = model.get_cardinality(&supplier.set);

    let anti_first = model.compute_cost_and_create_node(
        &partsupp,
        &supplier,
        &mut sets,
        Some(predicate_set(&[supplier_exclusion.clone()])),
    );
    let anti_then_part = model.compute_cost_and_create_node(
        &anti_first,
        &part,
        &mut sets,
        Some(predicate_set(&[part_filter.clone()])),
    );

    let part_first = model.compute_cost_and_create_node(
        &partsupp,
        &part,
        &mut sets,
        Some(predicate_set(&[part_filter])),
    );
    let part_then_anti = model.compute_cost_and_create_node(
        &part_first,
        &supplier,
        &mut sets,
        Some(predicate_set(&[supplier_exclusion])),
    );

    assert!(
        part_then_anti.cost < anti_then_part.cost,
        "selective inner first: {}, weak anti first: {}",
        part_then_anti.cost,
        anti_then_part.cost
    );
}

#[test]
fn hash_input_orientation_matches_width_aware_build_side_for_every_join() {
    let mut sets = JoinRelationSetManager::new();
    let inner_filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
    let reduction_filter = create_reduction_filter(&mut sets, 0, 0, 1, 0, 1, JoinType::Anti);
    let mut left_stats = RelationStats::with_cardinality(10);
    left_stats.estimated_payload_width = 1_024;
    let mut right_stats = RelationStats::with_cardinality(100);
    right_stats.estimated_payload_width = 8;
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_cost_model(&mut sets, &[left_stats, right_stats]);
    let mut left = leaf(&mut model, sets.get_relation(0));
    left.cardinality = 10.0;
    let mut right = leaf(&mut model, sets.get_relation(1));
    right.cardinality = 100.0;

    let inner = predicate_set(&[inner_filter]);
    let inner_cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&inner));
    let integer_key_width = estimate_row_payload_width(&[LogicalType::Integer]);
    assert_eq!(
        inner_cost.build,
        100.0
            * (estimate_hash_build_row_width(8, integer_key_width) + HASH_BUCKET_CACHE_LINE_BYTES)
                as f64,
        "the wider ten-row input costs more to serialize than the narrow hundred-row input"
    );
    assert_eq!(
        inner_cost.probe,
        10.0 * estimate_hash_probe_row_width(integer_key_width) as f64
    );

    let reduction = predicate_set(&[reduction_filter]);
    let reduction_cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&reduction));
    assert_eq!(reduction_cost.build, inner_cost.build);
    assert_eq!(reduction_cost.probe, inner_cost.probe);
    assert!(reduction_cost.match_output < inner_cost.match_output);
    assert!(reduction_cost.total() < inner_cost.total());

    let reduction_node =
        model.compute_cost_and_create_node(&left, &right, &mut sets, Some(reduction));
    assert_eq!(reduction_node.output_payload_width, 1_024);
}

#[test]
fn reduction_materialization_risk_is_bounded_by_the_preserved_child() {
    let mut sets = JoinRelationSetManager::new();
    let reduction_filter = create_reduction_filter(&mut sets, 0, 0, 1, 0, 0, JoinType::Semi);
    let predicates = predicate_set(std::slice::from_ref(&reduction_filter));
    let mut preserved_stats = RelationStats::with_cardinality(64);
    preserved_stats.risk_cardinality = 1_092;
    preserved_stats.materialization_cardinality = 1_092;
    let mut filtering_stats = RelationStats::with_cardinality(291_606);
    filtering_stats.risk_cardinality = 291_606;
    filtering_stats.materialization_cardinality = 291_606;
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_cost_model(&mut sets, &[preserved_stats, filtering_stats]);

    let preserved = leaf(&mut model, sets.get_relation(0));
    let filtering = leaf(&mut model, sets.get_relation(1));
    let forward = model.compute_cost_and_create_node(
        &preserved,
        &filtering,
        &mut sets,
        Some(predicates.clone()),
    );
    let CutPredicateResolution::Resolved(Some(inverted_predicates)) =
        JoinPredicateSet::from_filters(
            [&reduction_filter],
            filtering.set.as_ref(),
            preserved.set.as_ref(),
        )
    else {
        panic!("expected an inverted reduction predicate set")
    };
    let inverted = model.compute_cost_and_create_node(
        &filtering,
        &preserved,
        &mut sets,
        Some(inverted_predicates),
    );

    assert_eq!(forward.materialization_cardinality, 1_092.0);
    assert_eq!(inverted.materialization_cardinality, 1_092.0);
    assert_eq!(forward.output_payload_width, preserved.output_payload_width);
    assert_eq!(
        inverted.output_payload_width,
        preserved.output_payload_width
    );
}

#[test]
fn hash_probe_work_does_not_assume_runtime_filter_pushdown() {
    let mut sets = JoinRelationSetManager::new();
    let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
    let mut build_stats = RelationStats::with_cardinality(10);
    build_stats.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(10, true)]);
    let mut probe_stats = RelationStats::with_cardinality(1_000);
    probe_stats.column_distinct_count =
        column_distinct_counts(1, [DistinctCount::new(1_000, true)]);
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_equivalent_relations(&[Arc::clone(&filter)]);
    model.init_cost_model(&mut sets, &[build_stats, probe_stats]);
    let build = leaf(&mut model, sets.get_relation(0));
    let probe = leaf(&mut model, sets.get_relation(1));
    let predicates = predicate_set(&[filter]);

    let cost = model.compute_cost_breakdown(&build, &probe, &mut sets, Some(&predicates));
    let integer_key_width = estimate_row_payload_width(&[LogicalType::Integer]);
    assert_eq!(
        cost.probe,
        1_000.0 * estimate_hash_probe_row_width(integer_key_width) as f64,
        "runtime-filter eligibility belongs to physical pushdown, not the join graph"
    );
}

#[test]
fn range_join_does_not_claim_an_equality_runtime_filter() {
    let mut sets = JoinRelationSetManager::new();
    let filter = create_comparison_filter(&mut sets, 0, 0, 1, 0, 0, ComparisonType::LessThan);
    let left_stats = RelationStats::with_cardinality(10);
    let right_stats = RelationStats::with_cardinality(1_000);
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_cost_model(&mut sets, &[left_stats, right_stats]);
    let left = leaf(&mut model, sets.get_relation(0));
    let right = leaf(&mut model, sets.get_relation(1));
    let predicates = predicate_set(&[filter]);
    let combination = sets.union(&left.set, &right.set);
    let join_rows = model.get_cardinality(&combination);

    let cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&predicates));
    let condition_width = estimate_row_payload_width(&[LogicalType::Integer]);
    assert_eq!(cost.build, 10.0 * estimate_row_width_from_payload(1) as f64);
    assert_eq!(
        cost.match_output,
        join_rows * estimate_row_width_from_payload(2) as f64
    );
    assert_eq!(
        cost.probe,
        10.0 * 1_000.0 * (2 * condition_width + NESTED_LOOP_CURSOR_BYTES) as f64,
        "range joins materialize one side, evaluate every pair, and copy accepted rows"
    );
}

#[test]
fn reduction_control_region_remains_on_the_build_side() {
    let mut sets = JoinRelationSetManager::new();
    let reduction_filter = create_reduction_filter(&mut sets, 0, 0, 1, 0, 0, JoinType::Anti);
    let mut preserved_stats = RelationStats::with_cardinality(10);
    preserved_stats.estimated_payload_width = 8;
    let mut filtering_stats = RelationStats::with_cardinality(100);
    filtering_stats.estimated_payload_width = 1_024;
    filtering_stats.contains_control_region = true;
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_cost_model(&mut sets, &[preserved_stats, filtering_stats]);
    let mut preserved = leaf(&mut model, sets.get_relation(0));
    preserved.cardinality = 10.0;
    let mut filtering = leaf(&mut model, sets.get_relation(1));
    filtering.cardinality = 100.0;

    let predicates = predicate_set(&[reduction_filter]);
    let cost = model.compute_cost_breakdown(&preserved, &filtering, &mut sets, Some(&predicates));
    let key_width = estimate_row_payload_width(&[LogicalType::Integer]);
    assert_eq!(
        cost.build,
        100.0
            * (estimate_hash_build_row_width(1_024, key_width) + HASH_BUCKET_CACHE_LINE_BYTES)
                as f64
    );
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
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_equivalent_relations(&filters);
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
    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_equivalent_relations(&filters);
    model.init_cost_model(&mut sets, &stats);

    let full_join = sets.get_relation_from_vec(vec![0, 1, 2]);
    assert_eq!(model.get_cardinality(&full_join), 10.0);
}

#[test]
fn test_cross_product_cost() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut cost_model = RegionCostModel::new(SelectivityDefaults::default());

    // No join filters - this will be a cross product
    let stats = vec![
        RelationStats::with_cardinality(100),
        RelationStats::with_cardinality(50),
    ];
    cost_model.init_cost_model(&mut set_manager, &stats);

    let left = leaf(&mut cost_model, set_manager.get_relation(0));
    let right = leaf(&mut cost_model, set_manager.get_relation(1));

    let cost = cost_model.compute_cost(&left, &right, &mut set_manager, None);

    assert_eq!(
        cost,
        50.0 * estimate_row_width_from_payload(1) as f64
            + 100.0 * 50.0 * CROSS_PRODUCT_SELECTION_BYTES as f64,
        "cross product materializes the smaller input and emits one repeated-row ordinal per pair"
    );
}

#[test]
fn unoriented_graph_predicate_uses_cross_product_cost_contract() {
    let mut sets = JoinRelationSetManager::new();
    let left_set = sets.get_relation(0);
    let right_set = sets.get_relation(1);
    let full_set = sets.union(&left_set, &right_set);
    let residual = Arc::new(FilterInfo::new_inner(
        Expression::Operator(
            OperatorExpression::new(
                OperatorType::Coalesce,
                vec![create_column_ref(0, 0), create_column_ref(1, 0)],
                LogicalType::Boolean,
            )
            .into(),
        ),
        full_set,
        0,
    ));
    let CutPredicateResolution::Resolved(Some(predicates)) =
        JoinPredicateSet::from_filters([&residual], &left_set, &right_set)
    else {
        panic!("multi-relation residual should form a cut predicate set")
    };
    assert!(!predicates.has_join_conditions());

    let mut model = RegionCostModel::new(SelectivityDefaults::default());
    model.init_cost_model(
        &mut sets,
        &[
            RelationStats::with_cardinality(100),
            RelationStats::with_cardinality(50),
        ],
    );
    let left = leaf(&mut model, left_set);
    let right = leaf(&mut model, right_set);
    let cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&predicates));

    assert_eq!(cost.build, 50.0 * estimate_row_width_from_payload(1) as f64);
    assert_eq!(
        cost.probe,
        100.0 * 50.0 * CROSS_PRODUCT_SELECTION_BYTES as f64
    );
    assert_eq!(cost.match_output, 0.0);
}
