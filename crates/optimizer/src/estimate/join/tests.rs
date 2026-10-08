// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::region::join::relation::JoinRelationSetManager;
use crate::region::join::relation_manager::DistinctCount;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
    ConjunctionType, ConstantExpression,
};

fn create_column_ref(table_index: usize, column_index: usize) -> Expression {
    Expression::ColumnRef(
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

fn create_constant(value: i64) -> Expression {
    Expression::Constant(
        ConstantExpression {
            value: Value::BigInt(value),
            return_type: LogicalType::BigInt,
        }
        .into(),
    )
}

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
    let expr = Expression::Comparison(
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

fn create_semi_anti_filter(
    set_manager: &mut JoinRelationSetManager,
    join_type: JoinType,
) -> Arc<FilterInfo> {
    let expr = Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::Equal,
        }
        .into(),
    );
    let set = set_manager.get_relation_from_vec(vec![0, 1]);
    let left_set = set_manager.get_relation(0);
    let right_set = set_manager.get_relation(1);
    let mut filter = FilterInfo::new(
        expr,
        set,
        0,
        join_type,
        paro_planner::logical::operator::AntiJoinMode::Regular,
    );
    filter.set_left_set(left_set);
    filter.set_right_set(right_set);
    filter.set_left_binding(ColumnBinding::new(0, 0), 0);
    filter.set_right_binding(ColumnBinding::new(1, 0), 1);
    Arc::new(filter)
}

fn create_single_column_filter(
    set_manager: &mut JoinRelationSetManager,
    table: usize,
    col: usize,
    filter_index: usize,
) -> Arc<FilterInfo> {
    let expr = Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(table, col)),
            right: Box::new(create_constant(10)),
            comparison_type: ComparisonType::GreaterThan,
        }
        .into(),
    );

    let set = set_manager.get_relation(table);
    let left_set = set_manager.get_relation(table);

    let mut filter = FilterInfo::new_inner(expr, set, filter_index);
    filter.set_left_set(left_set);
    filter.set_left_binding(ColumnBinding::new(table, col), table);

    Arc::new(filter)
}

#[test]
fn test_cardinality_estimator_new() {
    let mut defaults = SelectivityDefaults::default();
    defaults.range = 0.42;
    let estimator = CardinalityEstimator::new(defaults);
    assert!(estimator.relation_set_stats.is_empty());
    assert!(estimator.relation_set_2_cardinality.is_empty());
    assert_eq!(estimator.selectivity_defaults.range, 0.42);
}

#[test]
fn test_init_equivalent_relations_single_filter() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    let filter = create_single_column_filter(&mut set_manager, 0, 0, 0);
    estimator.init_equivalent_relations(&[filter]);

    assert_eq!(estimator.relation_set_stats.len(), 1);
    assert!(estimator.relation_set_stats[0]
        .equivalent_relations
        .contains(&ColumnBinding::new(0, 0)));
}

#[test]
fn test_init_equivalent_relations_join_filter() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    estimator.init_equivalent_relations(&[filter]);

    assert_eq!(estimator.relation_set_stats.len(), 1);
    assert!(estimator.relation_set_stats[0]
        .equivalent_relations
        .contains(&ColumnBinding::new(0, 0)));
    assert!(estimator.relation_set_stats[0]
        .equivalent_relations
        .contains(&ColumnBinding::new(1, 0)));
}

#[test]
fn test_init_equivalent_relations_transitive() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    // A.x = B.y and B.y = C.z should create one equivalence class
    let filter1 = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    let filter2 = create_equality_filter(&mut set_manager, 1, 0, 2, 0, 1);

    estimator.init_equivalent_relations(&[filter1, filter2]);

    // Should have one equivalence class with all three bindings
    assert_eq!(estimator.relation_set_stats.len(), 1);
    assert!(estimator.relation_set_stats[0]
        .equivalent_relations
        .contains(&ColumnBinding::new(0, 0)));
    assert!(estimator.relation_set_stats[0]
        .equivalent_relations
        .contains(&ColumnBinding::new(1, 0)));
    assert!(estimator.relation_set_stats[0]
        .equivalent_relations
        .contains(&ColumnBinding::new(2, 0)));
}

#[test]
fn merging_equivalence_classes_retains_all_join_edges() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    let filter_cd = create_equality_filter(&mut set_manager, 2, 0, 3, 0, 1);
    let filter_bc = create_equality_filter(&mut set_manager, 1, 0, 2, 0, 2);

    estimator.init_equivalent_relations(&[filter_ab, filter_cd, filter_bc]);

    assert_eq!(estimator.relation_set_stats.len(), 1);
    assert_eq!(estimator.relation_set_stats[0].filters.len(), 3);
}

#[test]
fn test_init_cardinality_estimator_props() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    // Initialize with a filter first
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    estimator.init_equivalent_relations(&[filter]);

    // Initialize props for relation 0
    let set0 = set_manager.get_relation(0);
    let mut stats0 = RelationStats::with_cardinality(1000);
    stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);
    estimator.init_cardinality_estimator_props(&set0, &stats0);

    // Check that cardinality was stored
    assert!(estimator
        .relation_set_2_cardinality
        .contains_key(set0.as_ref()));
    let helper = &estimator.relation_set_2_cardinality[set0.as_ref()];
    assert_eq!(helper.cardinality_before_filters, 1000.0);
}

#[test]
fn test_estimate_cardinality_single_relation() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    // Initialize relation 0
    let set0 = set_manager.get_relation(0);
    let stats0 = RelationStats::with_cardinality(1000);
    estimator.init_cardinality_estimator_props(&set0, &stats0);

    // Estimate cardinality for single relation
    let cardinality = estimator.estimate_cardinality(&set0);
    assert_eq!(cardinality, 1000.0);
}

#[test]
fn test_estimate_cardinality_join() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    // Create join filter
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    estimator.init_equivalent_relations(&[filter]);

    // Initialize relation 0 with cardinality 1000, distinct count 100
    let set0 = set_manager.get_relation(0);
    let mut stats0 = RelationStats::with_cardinality(1000);
    stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);
    estimator.init_cardinality_estimator_props(&set0, &stats0);

    // Initialize relation 1 with cardinality 500, distinct count 50
    let set1 = set_manager.get_relation(1);
    let mut stats1 = RelationStats::with_cardinality(500);
    stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);
    estimator.init_cardinality_estimator_props(&set1, &stats1);

    // Estimate cardinality for join
    let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
    let cardinality = estimator.estimate_cardinality(&join_set);

    // Expected: (1000 * 500) / max(100, 50) = 500000 / 100 = 5000
    assert!(cardinality > 0.0);
}

#[test]
fn range_join_selectivity_does_not_depend_on_marginal_ndv() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let range =
        create_comparison_filter(&mut set_manager, 0, 0, 1, 0, 0, ComparisonType::GreaterThan);
    estimator.init_equivalent_relations(&[range]);

    for relation in 0..=1 {
        let set = set_manager.get_relation(relation);
        let mut stats = RelationStats::with_cardinality(1_000);
        stats.column_distinct_count =
            column_distinct_counts(relation, [DistinctCount::new(1_000, true)]);
        estimator.init_cardinality_estimator_props(&set, &stats);
    }

    let join = set_manager.get_relation_from_vec(vec![0, 1]);
    assert_eq!(estimator.estimate_cardinality(&join), 300_000.0);
}

#[test]
fn range_residual_refines_equality_join_with_distribution_free_prior() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
        create_comparison_filter(&mut set_manager, 0, 1, 1, 1, 1, ComparisonType::GreaterThan),
    ];
    estimator.init_equivalent_relations(&filters);

    for relation in 0..=1 {
        let set = set_manager.get_relation(relation);
        let mut stats = RelationStats::with_cardinality(1_000);
        stats.column_distinct_count = column_distinct_counts(
            relation,
            [
                DistinctCount::new(10, true),
                DistinctCount::new(1_000, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&set, &stats);
    }

    let join = set_manager.get_relation_from_vec(vec![0, 1]);
    assert!((estimator.estimate_cardinality(&join) - 30_000.0).abs() < 1e-6);
}

#[test]
fn parallel_equality_classes_do_not_assume_composite_key_independence() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
        create_equality_filter(&mut set_manager, 0, 1, 1, 1, 1),
    ];
    estimator.init_equivalent_relations(&filters);

    let left = set_manager.get_relation(0);
    let mut left_stats = RelationStats::with_cardinality(6_001_215);
    left_stats.column_distinct_count = column_distinct_counts(
        0,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&left, &left_stats);

    let right = set_manager.get_relation(1);
    let mut right_stats = RelationStats::with_cardinality(800_000);
    right_stats.column_distinct_count = column_distinct_counts(
        1,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&right, &right_stats);

    let join = set_manager.get_relation_from_vec(vec![0, 1]);
    // Marginal NDVs cannot establish that the two key columns are
    // independent. Use the strongest known single-column selectivity
    // instead of turning a 24M estimate into 2.4K.
    assert_eq!(estimator.estimate_cardinality(&join), 24_004_860.0);
}

#[test]
fn declared_composite_key_provides_a_joint_equality_domain() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
        create_equality_filter(&mut set_manager, 0, 1, 1, 1, 1),
    ];
    estimator.init_equivalent_relations(&filters);

    let probe = set_manager.get_relation(0);
    let mut probe_stats = RelationStats::with_cardinality(6_001_215);
    probe_stats.column_distinct_count = column_distinct_counts(
        0,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&probe, &probe_stats);

    let build = set_manager.get_relation(1);
    let mut build_stats = RelationStats::with_cardinality(800_000);
    build_stats.column_distinct_count = column_distinct_counts(
        1,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    build_stats.unique_keys = vec![vec![ColumnBinding::new(1, 0), ColumnBinding::new(1, 1)]];
    estimator.init_cardinality_estimator_props(&build, &build_stats);

    let join = set_manager.get_relation_from_vec(vec![0, 1]);
    assert_eq!(estimator.estimate_cardinality(&join), 6_001_215.0);
}

#[test]
fn parallel_key_correlation_survives_a_larger_equivalence_scope() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        // part.partkey = lineitem.partkey = partsupp.partkey
        create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
        create_equality_filter(&mut set_manager, 1, 0, 2, 0, 1),
        // lineitem.suppkey = partsupp.suppkey
        create_equality_filter(&mut set_manager, 1, 1, 2, 1, 2),
    ];
    estimator.init_equivalent_relations(&filters);

    let part = set_manager.get_relation(0);
    let mut part_stats = RelationStats::with_cardinality(150_000);
    part_stats.column_distinct_count =
        column_distinct_counts(0, [DistinctCount::new(150_000, true)]);
    estimator.init_cardinality_estimator_props(&part, &part_stats);

    let lineitem = set_manager.get_relation(1);
    let mut lineitem_stats = RelationStats::with_cardinality(6_001_215);
    lineitem_stats.column_distinct_count = column_distinct_counts(
        1,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&lineitem, &lineitem_stats);

    let partsupp = set_manager.get_relation(2);
    let mut partsupp_stats = RelationStats::with_cardinality(800_000);
    partsupp_stats.column_distinct_count = column_distinct_counts(
        2,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&partsupp, &partsupp_stats);

    let join = set_manager.get_relation_from_vec(vec![0, 1, 2]);
    assert_eq!(estimator.estimate_cardinality(&join), 18_003_645.0);
}

#[test]
fn filtered_dimension_and_composite_key_keep_fact_join_cardinality() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        create_equality_filter(&mut set_manager, 0, 0, 2, 0, 0),
        create_equality_filter(&mut set_manager, 1, 0, 2, 0, 1),
        create_equality_filter(&mut set_manager, 1, 1, 2, 1, 2),
    ];
    estimator.init_equivalent_relations(&filters);

    let part = set_manager.get_relation(0);
    let mut part_stats = RelationStats::with_cardinality(10_000);
    part_stats.column_distinct_count =
        column_distinct_counts(0, [DistinctCount::new(10_000, true)]);
    estimator.init_cardinality_estimator_props(&part, &part_stats);

    let lineitem = set_manager.get_relation(1);
    let mut lineitem_stats = RelationStats::with_cardinality(6_001_215);
    lineitem_stats.column_distinct_count = column_distinct_counts(
        1,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&lineitem, &lineitem_stats);

    let partsupp = set_manager.get_relation(2);
    let mut partsupp_stats = RelationStats::with_cardinality(800_000);
    partsupp_stats.column_distinct_count = column_distinct_counts(
        2,
        [
            DistinctCount::new(200_000, true),
            DistinctCount::new(10_000, true),
        ],
    );
    partsupp_stats.unique_keys = vec![vec![ColumnBinding::new(2, 0), ColumnBinding::new(2, 1)]];
    estimator.init_cardinality_estimator_props(&partsupp, &partsupp_stats);

    let join = set_manager.get_relation_from_vec(vec![0, 1, 2]);
    assert_eq!(estimator.estimate_cardinality(&join), 300_060.75);
}

#[test]
fn composite_fact_join_stays_large_across_transitive_dimensions() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        create_equality_filter(&mut set_manager, 0, 0, 2, 0, 0),
        create_equality_filter(&mut set_manager, 1, 0, 2, 1, 1),
        create_equality_filter(&mut set_manager, 2, 0, 3, 0, 2),
        create_equality_filter(&mut set_manager, 2, 1, 3, 1, 3),
    ];
    estimator.init_equivalent_relations(&filters);

    let relation_stats = [
        (10_000, vec![DistinctCount::new(10_000, true)]),
        (10_000, vec![DistinctCount::new(9_955, true)]),
        (
            6_001_215,
            vec![
                DistinctCount::new(207_507, true),
                DistinctCount::new(9_955, true),
            ],
        ),
        (
            800_000,
            vec![
                DistinctCount::new(207_507, true),
                DistinctCount::new(9_955, true),
            ],
        ),
    ];
    for (relation, (cardinality, distinct)) in relation_stats.into_iter().enumerate() {
        let set = set_manager.get_relation(relation);
        let mut stats = RelationStats::with_cardinality(cardinality);
        stats.column_distinct_count = column_distinct_counts(relation, distinct);
        estimator.init_cardinality_estimator_props(&set, &stats);
    }

    let join = set_manager.get_relation_from_vec(vec![0, 1, 2, 3]);
    let estimate = estimator.estimate_cardinality(&join);
    assert!(estimate > 100_000.0, "unexpected estimate {estimate}");
}

#[test]
fn transitive_triangles_align_composite_key_domains() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        // part_key equivalence class. The dimension-to-partsupp edge is
        // inferred transitively and creates a triangle.
        create_equality_filter(&mut set_manager, 0, 0, 2, 0, 0),
        create_equality_filter(&mut set_manager, 0, 0, 3, 0, 1),
        create_equality_filter(&mut set_manager, 2, 0, 3, 0, 2),
        // supp_key equivalence class with the same inferred shape.
        create_equality_filter(&mut set_manager, 1, 0, 2, 1, 3),
        create_equality_filter(&mut set_manager, 1, 0, 3, 1, 4),
        create_equality_filter(&mut set_manager, 2, 1, 3, 1, 5),
    ];
    estimator.init_equivalent_relations(&filters);

    let relation_stats = [
        (10_000, vec![DistinctCount::new(10_000, true)]),
        (10_000, vec![DistinctCount::new(9_955, true)]),
        (
            6_001_215,
            vec![
                DistinctCount::new(207_507, true),
                DistinctCount::new(9_955, true),
            ],
        ),
        (
            800_000,
            vec![
                DistinctCount::new(207_507, true),
                DistinctCount::new(9_955, true),
            ],
        ),
    ];
    for (relation, (cardinality, distinct)) in relation_stats.into_iter().enumerate() {
        let set = set_manager.get_relation(relation);
        let mut stats = RelationStats::with_cardinality(cardinality);
        stats.column_distinct_count = column_distinct_counts(relation, distinct);
        if relation == 3 {
            stats.unique_keys = vec![vec![ColumnBinding::new(3, 0), ColumnBinding::new(3, 1)]];
        }
        estimator.init_cardinality_estimator_props(&set, &stats);
    }

    let join = set_manager.get_relation_from_vec(vec![0, 1, 2, 3]);
    assert_eq!(estimator.estimate_cardinality(&join), 290_512.73168791726);
}

#[test]
fn star_correlation_only_removes_owned_denominator_factors() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
        create_equality_filter(&mut set_manager, 0, 0, 2, 0, 1),
        create_equality_filter(&mut set_manager, 0, 1, 1, 1, 2),
        create_equality_filter(&mut set_manager, 0, 1, 2, 1, 3),
    ];
    estimator.init_equivalent_relations(&filters);

    for (relation, cardinality, distinct) in [(0, 100, 100), (1, 10, 10), (2, 10, 10)] {
        let set = set_manager.get_relation(relation);
        let mut stats = RelationStats::with_cardinality(cardinality);
        stats.column_distinct_count = column_distinct_counts(
            relation,
            [
                DistinctCount::new(distinct, true),
                DistinctCount::new(distinct, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&set, &stats);
    }

    let join = set_manager.get_relation_from_vec(vec![0, 1, 2]);
    estimator.ensure_equality_graphs();
    let (denominator, _) = estimator.equality_denominator(&join);

    // Each star contributes X(100) * one leaf(10). The second,
    // correlated star removes exactly those two owned factors.
    assert_eq!(denominator, 1_000.0);
}

#[test]
fn parallel_self_join_edges_retain_every_owned_factor() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filters = vec![
        // One equality class has three binding vertices but only two
        // relation aliases. Both tree edges therefore belong to pair
        // (0, 1), and both factors must survive pair-level correlation.
        create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
        create_equality_filter(&mut set_manager, 0, 1, 1, 0, 1),
        // A second equality class connects the same relation pair.
        create_equality_filter(&mut set_manager, 0, 2, 1, 1, 2),
    ];
    estimator.init_equivalent_relations(&filters);

    let left = set_manager.get_relation(0);
    let mut left_stats = RelationStats::with_cardinality(1_000);
    left_stats.column_distinct_count = column_distinct_counts(
        0,
        [
            DistinctCount::new(5, true),
            DistinctCount::new(5, true),
            DistinctCount::new(20, true),
        ],
    );
    estimator.init_cardinality_estimator_props(&left, &left_stats);

    let right = set_manager.get_relation(1);
    let mut right_stats = RelationStats::with_cardinality(1_000);
    right_stats.column_distinct_count = column_distinct_counts(
        1,
        [DistinctCount::new(1, true), DistinctCount::new(20, true)],
    );
    estimator.init_cardinality_estimator_props(&right, &right_stats);

    let join = set_manager.get_relation_from_vec(vec![0, 1]);
    estimator.ensure_equality_graphs();
    assert_eq!(
        estimator
            .equality_graphs
            .iter()
            .map(|graph| graph.vertices.len())
            .max(),
        Some(3)
    );
    let (denominator, _) = estimator.equality_denominator(&join);

    // The first class owns 5 * 5 = 25 for pair (0, 1); the second owns
    // 20. Correlation keeps the stronger whole-class domain, 25.
    assert_eq!(denominator, 25.0);
}

#[test]
fn filtered_relation_domains_produce_a_nonzero_join_estimate() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
    estimator.init_equivalent_relations(&[filter]);

    let set0 = set_manager.get_relation(0);
    let mut stats0 = RelationStats::with_cardinality(9);
    stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(9, true)]);
    estimator.init_cardinality_estimator_props(&set0, &stats0);

    let set1 = set_manager.get_relation(1);
    let mut stats1 = RelationStats::with_cardinality(19);
    stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(19, true)]);
    estimator.init_cardinality_estimator_props(&set1, &stats1);

    let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
    assert_eq!(estimator.estimate_cardinality(&join_set), 9.0);
}

#[test]
fn semi_join_uses_the_build_side_distinct_domain() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filter = create_semi_anti_filter(&mut set_manager, JoinType::Semi);
    estimator.init_equivalent_relations(&[filter]);

    let left = set_manager.get_relation(0);
    let mut left_stats = RelationStats::with_cardinality(1_500_000);
    left_stats.column_distinct_count =
        column_distinct_counts(0, [DistinctCount::new(1_500_000, false)]);
    estimator.init_cardinality_estimator_props(&left, &left_stats);

    let right = set_manager.get_relation(1);
    let mut right_stats = RelationStats::with_cardinality(735);
    right_stats.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(735, false)]);
    estimator.init_cardinality_estimator_props(&right, &right_stats);

    let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
    assert_eq!(estimator.estimate_cardinality(&join_set), 735.0);
}

#[test]
fn inherited_domain_does_not_inflate_semi_join_coverage_prior() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filter = create_semi_anti_filter(&mut set_manager, JoinType::Semi);
    estimator.init_equivalent_relations(&[filter]);

    let left = set_manager.get_relation(0);
    let mut left_stats = RelationStats::with_cardinality(1_000);
    left_stats.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(1_000, true)]);
    estimator.init_cardinality_estimator_props(&left, &left_stats);

    let right = set_manager.get_relation(1);
    let mut right_stats = RelationStats::with_cardinality(900);
    right_stats.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(900, false)]);
    estimator.init_cardinality_estimator_props(&right, &right_stats);

    let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
    assert_eq!(
        estimator.estimate_cardinality(&join_set),
        200.0,
        "an upper-bound domain may tighten but never inflate the 20% match prior"
    );
}

#[test]
fn anti_join_estimates_the_unmatched_preserved_rows() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
    let filter = create_semi_anti_filter(&mut set_manager, JoinType::Anti);
    estimator.init_equivalent_relations(&[filter]);

    let left = set_manager.get_relation(0);
    let mut left_stats = RelationStats::with_cardinality(1_000);
    left_stats.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(1_000, true)]);
    estimator.init_cardinality_estimator_props(&left, &left_stats);

    let right = set_manager.get_relation(1);
    let mut right_stats = RelationStats::with_cardinality(100);
    right_stats.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(100, true)]);
    estimator.init_cardinality_estimator_props(&right, &right_stats);

    let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
    assert_eq!(estimator.estimate_cardinality(&join_set), 900.0);
}

#[test]
fn test_estimate_cardinality_cached() {
    let mut set_manager = JoinRelationSetManager::new();
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    let set0 = set_manager.get_relation(0);
    let stats0 = RelationStats::with_cardinality(1000);
    estimator.init_cardinality_estimator_props(&set0, &stats0);

    // First call
    let card1 = estimator.estimate_cardinality(&set0);
    // Second call should return cached value
    let card2 = estimator.estimate_cardinality(&set0);

    assert_eq!(card1, card2);
}

#[test]
fn test_relations_set_to_stats() {
    let mut bindings = HashSet::new();
    bindings.insert(ColumnBinding::new(0, 0));
    bindings.insert(ColumnBinding::new(1, 0));

    let stats = RelationsSetToStats::new(bindings);
    assert_eq!(stats.equivalent_relations.len(), 2);
    assert!(!stats.has_distinct_count_hll);
    assert_eq!(stats.distinct_count_no_hll, usize::MAX);
}

#[test]
fn test_relations_set_to_stats_get_distinct_count() {
    let mut stats = RelationsSetToStats::new(HashSet::new());

    // Without HLL
    stats.distinct_count_no_hll = 100;
    assert_eq!(stats.get_distinct_count(), 100);

    // With HLL
    stats.has_distinct_count_hll = true;
    stats.distinct_count_hll = 150;
    assert_eq!(stats.get_distinct_count(), 150);
}

#[test]
fn test_filter_info_with_total_domains() {
    let mut set_manager = JoinRelationSetManager::new();
    let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);

    let mut stats = RelationsSetToStats::new(HashSet::new());
    stats.distinct_count_hll = 100;
    stats.has_distinct_count_hll = true;

    let filter_with_domains = FilterInfoWithTotalDomains::new(filter, &stats);
    assert_eq!(filter_with_domains.distinct_count_hll, 100);
    assert!(filter_with_domains.has_distinct_count_hll);
    assert_eq!(filter_with_domains.get_distinct_count(), 100);
}

#[test]
fn partial_sketch_is_an_upper_bound_not_a_complete_equality_domain() {
    let estimate = DistinctDomainEstimate::from_evidence(
        DistinctEvidence {
            lower: 8,
            upper: None,
            point: 8,
            provenance: DistinctProvenance::ObservedPartial {
                observed_rows: 8,
                total_rows: 800,
            },
        },
        800,
    );

    assert!(!estimate.has_hll);
    assert_eq!(estimate.value(), 800);
}

#[test]
fn unknown_distinct_evidence_does_not_shrink_the_total_domain() {
    let estimate = DistinctDomainEstimate::from_evidence(DistinctEvidence::default(), 321);

    assert!(!estimate.has_hll);
    assert_eq!(estimate.value(), 321);
}

#[test]
fn derived_point_without_a_proof_stays_out_of_the_observed_domain_path() {
    let estimate = DistinctDomainEstimate::from_evidence(
        DistinctEvidence {
            point: 7,
            provenance: DistinctProvenance::Derived,
            ..DistinctEvidence::default()
        },
        321,
    );

    assert!(!estimate.has_hll);
    assert_eq!(estimate.value(), 7);
}

#[test]
fn test_cardinality_helper() {
    let helper = CardinalityHelper::new(1000.0);
    assert_eq!(helper.cardinality_before_filters, 1000.0);
}

#[test]
fn test_denom_info() {
    let set = Arc::new(JoinRelationSet::new(vec![0, 1]));
    let info = DenomInfo::new(set.clone(), 1.0, 100.0);

    assert_eq!(info.filter_strength, 1.0);
    assert_eq!(info.denominator, 100.0);
    assert_eq!(info.numerator_relations.count(), 2);
}

#[test]
fn test_remove_empty_total_domains() {
    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());

    // Add some stats
    let mut bindings1 = HashSet::new();
    bindings1.insert(ColumnBinding::new(0, 0));
    estimator
        .relation_set_stats
        .push(RelationsSetToStats::new(bindings1));

    // Add empty stats
    estimator
        .relation_set_stats
        .push(RelationsSetToStats::new(HashSet::new()));

    // Add more stats
    let mut bindings2 = HashSet::new();
    bindings2.insert(ColumnBinding::new(1, 0));
    estimator
        .relation_set_stats
        .push(RelationsSetToStats::new(bindings2));

    estimator.remove_empty_total_domains();

    assert_eq!(estimator.relation_set_stats.len(), 2);
}

#[test]
fn test_get_comparison_type() {
    // Equal comparison
    let eq_expr = Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::Equal,
        }
        .into(),
    );
    assert_eq!(
        CardinalityEstimator::get_comparison_type(&eq_expr),
        Some(ComparisonKind::Equal)
    );

    // Not equal comparison
    let ne_expr = Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::NotEqual,
        }
        .into(),
    );
    assert_eq!(
        CardinalityEstimator::get_comparison_type(&ne_expr),
        Some(ComparisonKind::NotEqual)
    );

    // Range comparison
    let lt_expr = Expression::Comparison(
        ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::LessThan,
        }
        .into(),
    );
    assert_eq!(
        CardinalityEstimator::get_comparison_type(&lt_expr),
        Some(ComparisonKind::Range)
    );

    // Constant (no comparison)
    let const_expr = create_constant(42);
    assert_eq!(CardinalityEstimator::get_comparison_type(&const_expr), None);

    let disjunction = Expression::Conjunction(
        ConjunctionExpression::new(ConjunctionType::Or, vec![eq_expr, create_constant(7)]).into(),
    );
    assert_eq!(
        CardinalityEstimator::get_comparison_type(&disjunction),
        None
    );
}
