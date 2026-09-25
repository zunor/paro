// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
use paro_common::types::LogicalType;
use paro_planner::expression::{
    ColumnRefExpression, ComparisonExpression, ConstantExpression, OperatorExpression,
};

use super::*;

#[test]
fn selectivity_uses_a_stack_safe_shared_dag_fold() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let model = SelectivityModel::default();
            for kind in [ConjunctionType::And, ConjunctionType::Or] {
                let mut expression = Expression::Comparison(
                    ComparisonExpression::new(
                        ComparisonType::Equal,
                        Expression::ColumnRef(
                            ColumnRefExpression::new(
                                ColumnBinding::new(1, 0),
                                LogicalType::Integer,
                            )
                            .into(),
                        ),
                        Expression::Constant(
                            ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                        ),
                    )
                    .into(),
                );
                for _ in 0..10_000 {
                    expression = Expression::Conjunction(
                        paro_planner::expression::ConjunctionExpression::new(
                            kind,
                            vec![expression.clone(), expression],
                        )
                        .into(),
                    );
                }
                let mut reads = 0;
                let stats = HashMap::new();
                let estimate = model
                    .estimate_selectivity_controlled(
                        &expression,
                        &StatisticsResolver::logical(&stats),
                        &mut SelectivityWork(|| {
                            reads += 1;
                            Ok(reads <= 150_000)
                        }),
                    )
                    .unwrap();
                assert_eq!(estimate.fraction, model.defaults.equality);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn shared_boolean_nodes_count_evaluations_without_confusing_them_with_domains() {
    use paro_planner::expression::{ConjunctionExpression, FunctionExpression};
    let model = SelectivityModel::default();
    let random = paro_function::scalar::math::get_random_function()
        .functions
        .into_iter()
        .next()
        .unwrap();
    let volatile = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::LessThan,
            Expression::Function(
                FunctionExpression::new(random, vec![], LogicalType::Double).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::Double(0.5), LogicalType::Double).into(),
            ),
        )
        .into(),
    );
    let stable = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            ),
        )
        .into(),
    );
    for kind in [ConjunctionType::And, ConjunctionType::Or] {
        for (leaf, occurrences) in [(stable.clone(), 1), (volatile.clone(), 8)] {
            let point = model.estimate_selectivity(&leaf, &HashMap::new());
            let mut root = leaf;
            for _ in 0..3 {
                root = Expression::Conjunction(
                    ConjunctionExpression::new(kind, vec![root.clone(), root]).into(),
                );
            }
            let estimate = model.estimate_selectivity(&root, &HashMap::new());
            let expected = match kind {
                ConjunctionType::And => point.powi(occurrences),
                ConjunctionType::Or => 1.0 - (1.0 - point).powi(occurrences),
            };
            assert!((estimate - expected).abs() < 1e-12);
        }
    }
}

#[test]
fn flat_and_dag_term_paths_agree_with_an_independent_occurrence_oracle() {
    use paro_planner::expression::ConjunctionExpression;
    let leaves = (0..24)
        .map(|value| {
            Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    Expression::ColumnRef(
                        ColumnRefExpression::new(
                            ColumnBinding::new(1, value),
                            LogicalType::Integer,
                        )
                        .into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(value as i32), LogicalType::Integer)
                            .into(),
                    ),
                )
                .into(),
            )
        })
        .collect::<Vec<_>>();
    let stats = HashMap::new();
    let view = StatisticsResolver::logical(&stats);
    for kind in [ConjunctionType::And, ConjunctionType::Or] {
        for count in 0..=leaves.len() {
            let flat: Vec<_> = leaves[..count]
                .iter()
                .chain(leaves[..count].iter().rev())
                .cloned()
                .collect();
            let shared =
                Expression::Conjunction(ConjunctionExpression::new(kind, flat.clone()).into());
            let nested = Expression::Conjunction(
                ConjunctionExpression::new(kind, vec![shared.clone(), shared]).into(),
            );
            let mut paths = vec![&nested];
            let mut oracle = HashMap::new();
            // Deliberately enumerate occurrences in this bounded test;
            // production analysis may not expand shared DAG paths.
            while let Some(node) = paths.pop() {
                if let Expression::Conjunction(conjunction) = node {
                    paths.extend(&conjunction.children);
                } else {
                    oracle.insert(node.allocation_identity(), 1u64);
                }
            }
            for (roots, expect_inline) in [
                (flat.iter().collect::<Vec<_>>(), count <= 8),
                (vec![&nested], count <= 8),
            ] {
                let actual =
                    flatten_associative(roots, &view, kind, &mut SelectivityWork(|| Ok(true)))
                        .unwrap();
                assert_eq!(!actual.spilled(), expect_inline);
                let actual: HashMap<_, _> = actual
                    .into_iter()
                    .map(|(node, occurrences)| (node.allocation_identity(), occurrences))
                    .collect();
                assert_eq!(actual, oracle);
            }
        }
    }
}

#[test]
fn sparse_fetch_requires_enough_work_to_amortize_its_frontier() {
    let model = SelectivityModel::default();
    assert!(model
        .late_row_fetch_benefit(1, 1, [LogicalType::Varchar], 8)
        .is_none());
    assert!(model
        .late_row_fetch_benefit(100_000, 100, [LogicalType::Varchar], 3)
        .is_some());
}

#[test]
fn estimate_filter_cardinality_preserves_empty_predicates() {
    let model = SelectivityModel::default();
    let estimate = model.estimate_filter_cardinality(42, &[], &HashMap::new());
    assert_eq!(estimate, CardinalityEstimate::exact(42));
}

#[test]
fn constant_false_selectivity_is_zero() {
    let model = SelectivityModel::default();
    let expr = Expression::Constant(
        ConstantExpression::new(Value::Boolean(false), LogicalType::Boolean).into(),
    );
    assert_eq!(model.estimate_selectivity(&expr, &HashMap::new()), 0.0);
}

#[test]
fn comparison_without_stats_uses_default_equality_selectivity() {
    let model = SelectivityModel::default();
    let expr = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(7), LogicalType::Integer).into(),
            ),
        )
        .into(),
    );

    assert_eq!(
        model.estimate_selectivity(&expr, &HashMap::new()),
        model.defaults.equality
    );
}

#[test]
fn equality_union_uses_one_domain_and_does_not_discount_it_repeatedly() {
    let model = SelectivityModel::default();
    let binding = ColumnBinding::new(1, 0);
    let equality = |binding, value| {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                Expression::ColumnRef(
                    ColumnRefExpression::new(binding, LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    };
    let either = |children| {
        Expression::Conjunction(
            paro_planner::expression::ConjunctionExpression::new(ConjunctionType::Or, children)
                .into(),
        )
    };
    let predicate = either(vec![
        equality(binding, 2001),
        equality(binding, 2002),
        equality(binding, 2001),
    ]);
    let statistics = |point| {
        HashMap::from([(
            binding,
            Arc::new(ColumnStatistics::with_estimated_distinct(
                paro_storage::statistics::BaseStatistics::create_unknown(LogicalType::Integer),
                Some(point),
            )),
        )])
    };
    assert_eq!(
        model.estimate_selectivity(&predicate, &statistics(200)),
        0.01
    );
    let in_values = |values: &[i32]| {
        let mut children = vec![Expression::ColumnRef(
            ColumnRefExpression::new(binding, LogicalType::Integer).into(),
        )];
        children.extend(values.iter().map(|value| {
            Expression::Constant(
                ConstantExpression::new(Value::Integer(*value), LogicalType::Integer).into(),
            )
        }));
        Expression::Operator(
            OperatorExpression::new(OperatorType::In, children, LogicalType::Boolean).into(),
        )
    };
    for expressions in [
        vec![predicate.clone(), in_values(&[2001, 2002, 2002])],
        vec![in_values(&[2002, 2001]), predicate.clone()],
    ] {
        let estimate = model.estimate_filter_cardinality(10_000, &expressions, &statistics(200));
        assert_eq!(estimate.expected, 100);
        assert!(estimate.max > estimate.expected, "NDV is not a hard bound");
    }
    assert_eq!(
        model
            .estimate_filter_cardinality(
                10_000,
                &[predicate.clone(), in_values(&[2002, 2003])],
                &statistics(200),
            )
            .expected,
        50
    );
    let filtered = statistics(2);
    let mut rows = 721;
    for _ in 0..8 {
        rows = model
            .estimate_filter_cardinality(rows, std::slice::from_ref(&predicate), &filtered)
            .expected;
        assert_eq!(rows, 721);
    }
    let resolver = StatisticsResolver::logical(&filtered);
    assert!(
        !model
            .estimate_selectivity_with_provenance(&predicate, &resolver)
            .proven
    );
    // Equalities on different columns may overlap; never add them as a
    // disjoint union, even when their type and NDV happen to match.
    let other = ColumnBinding::new(1, 1);
    let mut separate = filtered;
    separate.insert(other, separate[&binding].clone());
    assert_eq!(
        model.estimate_selectivity(
            &either(vec![equality(binding, 2001), equality(other, 2002)]),
            &separate
        ),
        0.75
    );
}

#[test]
fn integral_range_selectivity_uses_bounds_and_preserves_orientation() {
    let model = SelectivityModel::default();
    let binding = ColumnBinding::new(1, 0);
    let mut stats = ColumnStatistics::new(paro_storage::statistics::BaseStatistics::create_empty(
        LogicalType::Date,
    ));
    NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Date(0));
    NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Date(100));
    let column_stats = HashMap::from([(binding, Arc::new(stats))]);
    let column =
        || Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Date).into());
    let constant =
        || Expression::Constant(ConstantExpression::new(Value::Date(90), LogicalType::Date).into());
    let upper_bound = Expression::Comparison(
        ComparisonExpression::new(ComparisonType::LessThanOrEqual, column(), constant()).into(),
    );
    let reversed = Expression::Comparison(
        ComparisonExpression::new(ComparisonType::GreaterThanOrEqual, constant(), column()).into(),
    );

    let expected = 91.0 / 101.0;
    assert_eq!(
        model.estimate_selectivity(&upper_bound, &column_stats),
        expected
    );
    assert_eq!(
        model.estimate_selectivity(&reversed, &column_stats),
        expected
    );
}

#[test]
fn integral_range_selectivity_handles_domain_boundaries() {
    let mut stats = ColumnStatistics::new(paro_storage::statistics::BaseStatistics::create_empty(
        LogicalType::Integer,
    ));
    NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Integer(10));
    NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Integer(20));

    assert_eq!(
        estimate_range_selectivity(&stats, &Value::Integer(9), ComparisonType::LessThanOrEqual,),
        Some(0.0)
    );
    assert_eq!(
        estimate_range_selectivity(
            &stats,
            &Value::Integer(21),
            ComparisonType::GreaterThanOrEqual,
        ),
        Some(0.0)
    );
    assert_eq!(
        estimate_range_selectivity(&stats, &Value::Integer(20), ComparisonType::LessThanOrEqual,),
        Some(1.0)
    );
}

#[test]
fn conjunction_coalesces_bounds_on_the_same_integral_column() {
    let model = SelectivityModel::default();
    let binding = ColumnBinding::new(1, 0);
    let mut stats = ColumnStatistics::new(paro_storage::statistics::BaseStatistics::create_empty(
        LogicalType::Integer,
    ));
    NumericStats::set_guaranteed_min(stats.statistics_mut(), &Value::Integer(0));
    NumericStats::set_guaranteed_max(stats.statistics_mut(), &Value::Integer(9));
    let column_stats = HashMap::from([(binding, Arc::new(stats))]);
    let comparison = |comparison_type, value| {
        Expression::Comparison(
            ComparisonExpression::new(
                comparison_type,
                Expression::ColumnRef(
                    ColumnRefExpression::new(binding, LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    };
    let expression = Expression::Conjunction(
        paro_planner::expression::ConjunctionExpression::new(
            ConjunctionType::And,
            vec![
                comparison(ComparisonType::GreaterThanOrEqual, 2),
                comparison(ComparisonType::LessThan, 5),
            ],
        )
        .into(),
    );

    assert_eq!(model.estimate_selectivity(&expression, &column_stats), 0.3);
    assert_eq!(
        model
            .estimate_filter_cardinality(64, &[expression], &column_stats)
            .expected,
        19
    );
}

#[test]
fn conjunction_dampens_only_distinct_columns_within_one_relation() {
    let model = SelectivityModel::default();
    let size_binding = ColumnBinding::new(1, 0);
    let type_binding = ColumnBinding::new(1, 1);
    let mut size_stats = ColumnStatistics::new(
        paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
    );
    let hashes = (0..50).map(paro_common::hash::hash_u64).collect::<Vec<_>>();
    size_stats.update_distinct_statistics(&hashes, hashes.len());
    let distinct = size_stats.distinct_evidence().point;
    let column_stats = HashMap::from([(size_binding, Arc::new(size_stats))]);
    let equality = |value| {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::NotEqual,
                Expression::ColumnRef(
                    ColumnRefExpression::new(size_binding, LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    };
    let suffix = Expression::Operator(
        OperatorExpression::new(
            OperatorType::Like,
            vec![
                Expression::ColumnRef(
                    ColumnRefExpression::new(type_binding, LogicalType::Varchar).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(
                        Value::Varchar("%BRASS".to_string()),
                        LogicalType::Varchar,
                    )
                    .into(),
                ),
            ],
            LogicalType::Boolean,
        )
        .into(),
    );

    let same_column = (1.0 - 1.0 / distinct as f64).powi(2);
    let expected = model.defaults.like_contains * same_column.sqrt();
    let actual = model
        .estimate_filter_cardinality(
            200_000,
            &[equality(14), equality(16), suffix],
            &column_stats,
        )
        .expected;
    assert_eq!(actual, (200_000.0 * expected).round() as u64);
}

#[test]
fn conjunction_keeps_different_relations_independent() {
    let model = SelectivityModel::default();
    let equality = |table_index| {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                Expression::ColumnRef(
                    ColumnRefExpression::new(
                        ColumnBinding::new(table_index, 0),
                        LogicalType::Integer,
                    )
                    .into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    };

    assert_eq!(
        model
            .estimate_filter_cardinality(10_000, &[equality(1), equality(2)], &HashMap::new())
            .expected,
        100
    );
}

#[test]
fn integral_range_selectivity_rejects_mixed_types_and_decimal_scales() {
    let mut integer_stats = ColumnStatistics::new(
        paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
    );
    NumericStats::set_guaranteed_min(integer_stats.statistics_mut(), &Value::Integer(10));
    NumericStats::set_guaranteed_max(integer_stats.statistics_mut(), &Value::Integer(20));
    assert_eq!(
        estimate_range_selectivity(
            &integer_stats,
            &Value::BigInt(15),
            ComparisonType::LessThanOrEqual,
        ),
        None
    );

    let decimal_type = LogicalType::Decimal {
        precision: 10,
        scale: 2,
    };
    let mut decimal_stats = ColumnStatistics::new(
        paro_storage::statistics::BaseStatistics::create_empty(decimal_type),
    );
    NumericStats::set_guaranteed_min(
        decimal_stats.statistics_mut(),
        &Value::Decimal(1_000, 10, 2),
    );
    NumericStats::set_guaranteed_max(
        decimal_stats.statistics_mut(),
        &Value::Decimal(2_000, 10, 2),
    );
    assert_eq!(
        estimate_range_selectivity(
            &decimal_stats,
            &Value::Decimal(1_500, 10, 3),
            ComparisonType::LessThanOrEqual,
        ),
        None
    );
}

#[test]
fn like_selectivity_distinguishes_pattern_shapes() {
    let model = SelectivityModel::default();
    let like = |pattern: &str| {
        Expression::Operator(
            OperatorExpression::new(
                OperatorType::Like,
                vec![
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Varchar)
                            .into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar(pattern.to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        )
    };

    assert_eq!(
        model.estimate_selectivity(&like("green"), &HashMap::new()),
        0.1
    );
    assert_eq!(
        model.estimate_selectivity(&like("green%"), &HashMap::new()),
        0.02
    );
    assert_eq!(
        model.estimate_selectivity(&like("%green"), &HashMap::new()),
        0.05
    );
    assert_eq!(
        model.estimate_selectivity(&like("%green%"), &HashMap::new()),
        0.05
    );
    assert_eq!(
        model.estimate_selectivity(&like("%Customer%Complaints%"), &HashMap::new()),
        0.05f64.powf(1.5)
    );
    assert_eq!(
        model.estimate_selectivity(&like("%%green%"), &HashMap::new()),
        model.estimate_selectivity(&like("%green%"), &HashMap::new()),
        "consecutive percent wildcards do not change the pattern language"
    );
    assert!(
        model.estimate_selectivity(&like("%a%b"), &HashMap::new())
            <= model.estimate_selectivity(&like("%b"), &HashMap::new())
    );
    assert!(
        model.estimate_selectivity(&like("a%b%"), &HashMap::new())
            <= model.estimate_selectivity(&like("a%"), &HashMap::new())
    );
    assert!(
        model.estimate_selectivity(&like("a%b%c"), &HashMap::new())
            <= model.estimate_selectivity(&like("%b%"), &HashMap::new())
    );
    assert!(
        model.estimate_selectivity(&like("green%"), &HashMap::new())
            <= model.estimate_selectivity(&like("%green%"), &HashMap::new()),
        "adding a start anchor must not increase selectivity"
    );
    assert!(
        model.estimate_selectivity(&like("a%b%"), &HashMap::new())
            <= model.estimate_selectivity(&like("%a%b%"), &HashMap::new()),
        "anchor monotonicity must hold for multi-fragment patterns"
    );
    let mut misordered_calibration = model.clone();
    misordered_calibration.defaults.like_prefix = 0.5;
    assert!(
        misordered_calibration.estimate_selectivity(&like("green%"), &HashMap::new())
            <= misordered_calibration.estimate_selectivity(&like("%green%"), &HashMap::new()),
        "calibration cannot violate the anchor lattice"
    );
    assert_eq!(model.estimate_selectivity(&like("%"), &HashMap::new()), 1.0);
    assert_eq!(
        model.estimate_selectivity(&like("gr_en%"), &HashMap::new()),
        0.75
    );

    let not_like = Expression::Operator(
        OperatorExpression::new_unary(OperatorType::Not, like("%green%"), LogicalType::Boolean)
            .into(),
    );
    assert_eq!(model.estimate_selectivity(&not_like, &HashMap::new()), 0.95);

    let not_match_all = Expression::Operator(
        OperatorExpression::new_unary(OperatorType::Not, like("%"), LogicalType::Boolean).into(),
    );
    assert_eq!(
        model.estimate_selectivity(&not_match_all, &HashMap::new()),
        MIN_SELECTIVITY
    );
}

#[test]
fn only_proven_false_predicates_receive_zero_selectivity() {
    let model = SelectivityModel::default();
    let constant = |value| {
        Expression::Constant(
            ConstantExpression::new(Value::Boolean(value), LogicalType::Boolean).into(),
        )
    };
    let estimated_zero = Expression::Conjunction(
        paro_planner::expression::ConjunctionExpression::new(
            ConjunctionType::And,
            vec![
                Expression::Operator(
                    OperatorExpression::new_unary(
                        OperatorType::Not,
                        Expression::Operator(
                            OperatorExpression::new(
                                OperatorType::Like,
                                vec![
                                    Expression::ColumnRef(
                                        ColumnRefExpression::new(
                                            ColumnBinding::new(1, 0),
                                            LogicalType::Varchar,
                                        )
                                        .into(),
                                    ),
                                    Expression::Constant(
                                        ConstantExpression::new(
                                            Value::Varchar("%".to_string()),
                                            LogicalType::Varchar,
                                        )
                                        .into(),
                                    ),
                                ],
                                LogicalType::Boolean,
                            )
                            .into(),
                        ),
                        LogicalType::Boolean,
                    )
                    .into(),
                ),
                constant(true),
            ],
        )
        .into(),
    );
    let proven_zero = Expression::Conjunction(
        paro_planner::expression::ConjunctionExpression::new(
            ConjunctionType::And,
            vec![estimated_zero.clone(), constant(false)],
        )
        .into(),
    );

    assert_eq!(
        model.estimate_selectivity(&estimated_zero, &HashMap::new()),
        MIN_SELECTIVITY
    );
    assert_eq!(
        model.estimate_selectivity(&proven_zero, &HashMap::new()),
        0.0
    );
    assert_eq!(
        model
            .estimate_filter_cardinality(42, &[estimated_zero], &HashMap::new())
            .expected,
        1
    );
    assert_eq!(
        model.estimate_filter_cardinality(42, &[proven_zero], &HashMap::new()),
        CardinalityEstimate::exact(0)
    );
}

#[test]
fn exponential_damping_never_claims_a_proven_bound() {
    let estimate = column_aware_conjunction_estimate(
        [
            (
                SelectivityEstimate::proven(0.1),
                Some(ColumnBinding::new(7, 0)),
            ),
            (
                SelectivityEstimate::proven(0.2),
                Some(ColumnBinding::new(7, 1)),
            ),
        ]
        .into_iter(),
    );

    assert!(!estimate.proven);
    assert!((estimate.fraction - (0.1 * 0.2_f64.sqrt())).abs() < f64::EPSILON);
}

#[test]
fn boolean_selectivity_reductions_are_bitwise_permutation_invariant() {
    let estimates = [
        SelectivityEstimate::estimated(0.37),
        SelectivityEstimate::estimated(0.11),
        SelectivityEstimate::proven(0.83),
    ];
    let reversed = estimates.into_iter().rev();
    let and_forward = conjunction_estimate(estimates.into_iter());
    let and_reverse = conjunction_estimate(reversed);
    assert_eq!(
        and_forward.fraction.to_bits(),
        and_reverse.fraction.to_bits()
    );
    assert_eq!(and_forward.proven, and_reverse.proven);

    let or_forward = disjunction_estimate(estimates.into_iter());
    let or_reverse = disjunction_estimate(estimates.into_iter().rev());
    assert_eq!(or_forward.fraction.to_bits(), or_reverse.fraction.to_bits());
    assert_eq!(or_forward.proven, or_reverse.proven);

    let columns = [
        (estimates[0], Some(ColumnBinding::new(7, 2))),
        (estimates[1], Some(ColumnBinding::new(7, 0))),
        (estimates[2], None),
    ];
    let forward = column_aware_conjunction_estimate(columns.into_iter());
    let reverse = column_aware_conjunction_estimate(columns.into_iter().rev());
    assert_eq!(forward.fraction.to_bits(), reverse.fraction.to_bits());
    assert_eq!(forward.proven, reverse.proven);
}

#[test]
fn exact_like_uses_column_distinct_count() {
    let model = SelectivityModel::default();
    let binding = ColumnBinding::new(1, 0);
    let mut stats = ColumnStatistics::new(paro_storage::statistics::BaseStatistics::create_empty(
        LogicalType::Varchar,
    ));
    let hashes = (0..100)
        .map(paro_common::hash::hash_u64)
        .collect::<Vec<_>>();
    stats.update_distinct_statistics(&hashes, hashes.len());
    let distinct = stats.distinct_evidence().point;
    let column_stats = HashMap::from([(binding, Arc::new(stats))]);
    let expression = Expression::Operator(
        OperatorExpression::new(
            OperatorType::Like,
            vec![
                Expression::ColumnRef(
                    ColumnRefExpression::new(binding, LogicalType::Varchar).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(
                        Value::Varchar("green".to_string()),
                        LogicalType::Varchar,
                    )
                    .into(),
                ),
            ],
            LogicalType::Boolean,
        )
        .into(),
    );

    assert_eq!(
        model.estimate_selectivity(&expression, &column_stats),
        1.0 / distinct as f64
    );
}
