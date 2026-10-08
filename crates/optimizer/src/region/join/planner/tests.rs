// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

use crate::cost::join_layout::BuildProbeSideOptimizer;
use crate::estimate::join::CardinalityEstimator;
use paro_catalog::entry::{
    CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
};
use paro_common::{runtime_value::Value, types::LogicalType};
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_function::scalar::FunctionStability;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ColumnRefExpression, ConstantExpression, FunctionExpression, OperatorExpression, OperatorType,
    ReferenceExpression,
};
use paro_planner::logical::operator::{
    AntiJoinMode, AnyJoin, ColumnBinding, ExpressionGet, Get, Projection,
};
use paro_planner::logical::plan::{CardinalityEstimate, NodeStats};
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
    Expression::ColumnRef(
        ColumnRefExpression {
            binding: ColumnBinding::new(table_index, column_index),
            depth: 0,
            return_type: LogicalType::Integer,
        }
        .into(),
    )
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

fn cross_product(left_table: usize, right_table: usize) -> OwnedLogicalPlan {
    OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        OwnedLogicalPlan::synthetic(create_scan(left_table)),
        OwnedLogicalPlan::synthetic(create_scan(right_table)),
    ))))
}

fn volatile_boolean() -> Expression {
    let function = paro_function::scalar::math::get_random_function()
        .functions
        .into_iter()
        .next()
        .expect("random overload")
        .with_stability(FunctionStability::Volatile);
    Expression::Comparison(
        paro_planner::expression::ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::Function(
                FunctionExpression::new(function, Vec::new(), LogicalType::Double).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::Double(0.5), LogicalType::Double).into(),
            ),
        )
        .into(),
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
) -> OwnedLogicalPlan {
    let input = OwnedLogicalPlan {
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

    OwnedLogicalPlan {
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
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Inner,
        OwnedLogicalPlan::synthetic(create_scan(0)),
        OwnedLogicalPlan::synthetic(create_scan(1)),
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
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let cross = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        OwnedLogicalPlan::synthetic(create_scan(0)),
        OwnedLogicalPlan::synthetic(create_scan(1)),
    ))));
    let equality = Expression::Comparison(
        paro_planner::expression::ComparisonExpression::new(
            ComparisonType::Equal,
            column_ref(0, 0),
            column_ref(1, 0),
        )
        .into(),
    );
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
fn subplan_relation_uses_its_domain_instead_of_the_original_shell_snapshot() {
    use paro_planner::logical::operator::subplan_ref::{
        BoundColumnDomain, BoundRelationFactValues, BoundRelationFacts,
    };
    use paro_planner::logical::operator::SubplanRef;
    let session = make_test_session();
    let context = BindContext::new();
    let binding = ColumnBinding::new(0, 0);
    let mut reference = SubplanRef::new(
        paro_planner::logical::operator::SubplanRefId::input_ordinal(0),
        vec![binding],
        vec![LogicalType::Integer],
    );
    reference.facts = Arc::new(BoundRelationFacts::new(
        BoundRelationFactValues {
            cardinality: Some(CardinalityEstimate::exact(100)),
            column_domains: vec![BoundColumnDomain {
                expected_distinct: Some(17),
                guaranteed_distinct_upper: Some(20),
                provenance: DistinctProvenance::Derived,
            }],
            ..BoundRelationFactValues::default()
        },
        reference.types().to_vec(),
    ));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::SubplanRef(reference));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    optimizer.column_stats.insert(
        binding,
        Arc::new(ColumnStatistics::with_estimated_distinct(
            BaseStatistics::new(LogicalType::Integer),
            Some(900),
        )),
    );
    optimizer.add_relation_plan(&session, &context, &plan);
    let stats = optimizer.relation_manager.get_relation_stats();
    assert_eq!(stats[0].column_distinct_count[&binding].distinct_count, 17);
    assert!(stats[0].column_distinct_count[&binding].has_expected_distinct);
    assert_eq!(
        stats[0].column_distinct_count[&binding].evidence.provenance,
        DistinctProvenance::Derived
    );
}

#[test]
fn derived_relation_hll_domain_becomes_a_bounded_upper_estimate() {
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
    assert!(column_stats.distinct_evidence().point > 9);

    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
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
    assert!(!distinct_count.has_expected_distinct);
}

#[test]
fn filtered_relation_synthetic_domain_is_bounded_by_its_cardinality() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let plan = projection_relation(&bind_context, 100, 0, 100);
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    optimizer.add_relation_plan(&session, &bind_context, &plan);

    let predicate = Expression::Comparison(
        paro_planner::expression::ComparisonExpression::new(
            ComparisonType::LessThan,
            column_ref(0, 0),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(10), LogicalType::Integer).into(),
            ),
        )
        .into(),
    );
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
    assert!(!distinct_count.has_expected_distinct);
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

    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
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
    assert!(!distinct_count.has_expected_distinct);
}

#[test]
fn materialized_payload_excludes_leaf_local_columns() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let relation = |table_index| {
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            Vec::new(),
            vec!["join_key".to_string(), "local_text".to_string()],
            vec![LogicalType::Integer, LogicalType::Varchar],
        )))
    };
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    optimizer.add_relation_plan(&session, &bind_context, &relation(0));
    optimizer.add_relation_plan(&session, &bind_context, &relation(1));

    let relations = HashSet::from([0, 1]);
    let set = optimizer.set_manager.get_relation_from_set(&relations);
    let filter = Arc::new(FilterInfo::new(
        Expression::Comparison(
            paro_planner::expression::ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(0, 0),
                column_ref(1, 0),
            )
            .into(),
        ),
        set,
        0,
        JoinType::Inner,
        AntiJoinMode::Regular,
    ));
    let outputs = HashMap::from([(ColumnBinding::new(0, 0), LogicalType::Integer)]);

    optimizer.apply_relation_payload_widths(&outputs, &[filter]);

    let expected = crate::cost::join_layout::estimate_row_payload_width(&[LogicalType::Integer]);
    let stats = optimizer.relation_manager.get_relation_stats();
    assert_eq!(stats[0].estimated_payload_width, expected);
    assert_eq!(stats[1].estimated_payload_width, expected);
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
fn wildcard_string_predicates_are_open_ended_for_join_costing() {
    let string_column = Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(7, 0), LogicalType::Varchar).into(),
    );
    let predicate = |operator_type, pattern: &str| {
        Expression::Operator(
            OperatorExpression::new(
                operator_type,
                vec![
                    string_column.clone(),
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

    assert!(has_open_ended_selectivity(&predicate(
        OperatorType::ILike,
        "%needle%"
    )));
    assert!(has_open_ended_selectivity(&predicate(
        OperatorType::Like,
        "prefix%"
    )));
    assert!(!has_open_ended_selectivity(&predicate(
        OperatorType::Like,
        "exact"
    )));
}

#[test]
fn wildcard_filter_keeps_the_complete_unfiltered_risk_envelope() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let plan = projection_relation(&bind_context, 1_000, 0, 1_000);
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    optimizer.add_relation_plan(&session, &bind_context, &plan);

    let predicate = Expression::Operator(
        OperatorExpression::new(
            OperatorType::ILike,
            vec![
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Varchar).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(
                        Value::Varchar("%needle%".to_string()),
                        LogicalType::Varchar,
                    )
                    .into(),
                ),
            ],
            LogicalType::Boolean,
        )
        .into(),
    );
    let filter = Arc::new(FilterInfo::new_inner(
        predicate,
        Arc::new(JoinRelationSet::single(0)),
        0,
    ));

    optimizer.apply_relation_local_selectivity(&[filter]);

    let stats = optimizer.relation_manager.get_relation_stats();
    assert_eq!(stats[0].cardinality, 50);
    assert_eq!(stats[0].risk_cardinality, 1_000);
    assert_eq!(stats[0].materialization_cardinality, 1_000);
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
        .map(|column_id| paro_planner::logical::operator::GetColumnSource::Stored { column_id })
        .collect();
    let bind_context = BindContext::new();
    let plan = OwnedLogicalPlan {
        id: bind_context.next_plan_id(),
        stats: NodeStats {
            estimated_cardinality: Some(CardinalityEstimate::exact(100)),
            ..NodeStats::default()
        },
        operator: LogicalOperator::Get(Box::new(get)),
    };
    let plan = crate::estimate::unique_keys::refresh_unique_keys(plan).expect("cache unique keys");

    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    optimizer.add_relation_plan(&make_test_session(), &bind_context, &plan);
    let mut left_stats = optimizer.relation_manager.get_relation_stats()[0].clone();
    // Unique keys are sets. The shared proof layer canonicalizes them by
    // output position after resolving the catalog IDs through this Get's
    // reordered column sources.
    assert_eq!(
        left_stats.unique_keys,
        vec![vec![ColumnBinding::new(40, 0), ColumnBinding::new(40, 1)]]
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
            let expression = Expression::Comparison(
                paro_planner::expression::ComparisonExpression::new(
                    ComparisonType::Equal,
                    Expression::ColumnRef(
                        ColumnRefExpression::new(left_binding, LogicalType::Integer).into(),
                    ),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(right_binding, LogicalType::Integer).into(),
                    ),
                )
                .into(),
            );
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

    let mut estimator = CardinalityEstimator::new(SelectivityDefaults::default());
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
    let estimate = JoinRegionPlanner::join_cardinality_estimate(0.125);
    assert_eq!(estimate, CardinalityEstimate::exact(1));
}

#[test]
fn optimize_preserves_relation_independent_filter_above_reordered_join() {
    let session = make_test_session();
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let equality = Expression::Comparison(
        paro_planner::expression::ComparisonExpression::new(
            ComparisonType::Equal,
            column_ref(0, 0),
            column_ref(1, 0),
        )
        .into(),
    );
    let constant_false = Expression::Constant(
        ConstantExpression::new(Value::Boolean(false), LogicalType::Boolean).into(),
    );
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
fn multi_relation_residual_is_costed_and_rebuilt_as_filtered_cross_product() {
    let comparison = |table_index| {
        Expression::Comparison(
            paro_planner::expression::ComparisonExpression::new(
                ComparisonType::GreaterThan,
                column_ref(table_index, 0),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(0), LogicalType::Integer).into(),
                ),
            )
            .into(),
        )
    };
    let residual = Expression::Operator(
        OperatorExpression::new(
            OperatorType::Coalesce,
            vec![comparison(0), comparison(1)],
            LogicalType::Boolean,
        )
        .into(),
    );
    let plan = LogicalOperator::Filter(Filter::new(cross_product(0, 1), vec![residual.clone()]));

    let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
        .optimize(&make_test_session(), &BindContext::new(), plan)
        .unwrap();
    let LogicalOperator::Filter(filter) = optimized else {
        panic!("unoriented predicate must remain a residual filter")
    };
    assert_eq!(filter.expressions.len(), 1);
    assert!(filter.expressions[0].equals(&residual));
    assert!(matches!(
        filter.child.operator,
        LogicalOperator::Join(Join::Cross(_))
    ));
}

#[test]
fn optimizer_keeps_original_tree_for_unmapped_or_bound_references() {
    let session = make_test_session();
    let plans = [
        Expression::Comparison(
            paro_planner::expression::ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(99, 0),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                ),
            )
            .into(),
        ),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean).into()),
    ];

    for predicate in plans {
        let original = predicate.clone();
        let plan = LogicalOperator::Filter(Filter::new(cross_product(0, 1), vec![predicate]));
        let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
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
    let plan = LogicalOperator::Filter(Filter::new(cross_product(0, 1), vec![predicate.clone()]));
    assert!(!JoinRegionPlanner::new(SelectivityDefaults::default()).can_optimize_join(&plan));

    let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
        .optimize(&make_test_session(), &BindContext::new(), plan)
        .unwrap();
    let LogicalOperator::Filter(filter) = optimized else {
        panic!("volatile predicate must keep its evaluation boundary");
    };
    assert!(filter.expressions[0].equals(&predicate));

    let nested_filter = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(create_scan(0)),
        vec![volatile_boolean()],
    )));
    let surrounding_join = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Inner,
        nested_filter,
        OwnedLogicalPlan::synthetic(create_scan(1)),
        vec![join_condition(JoinComparisonType::Equal, 0, 1)],
    )));
    assert!(!JoinRegionPlanner::new(SelectivityDefaults::default())
        .can_optimize_join(&surrounding_join));

    let volatile_preserved = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(create_scan(0)),
        vec![volatile_boolean()],
    )));
    let reduction = ComparisonJoin::new(
        JoinType::Semi,
        volatile_preserved,
        OwnedLogicalPlan::synthetic(create_scan(1)),
        vec![join_condition(JoinComparisonType::Equal, 0, 1)],
    );
    assert!(
        !RelationManager::reduction_join_is_reorderable(&reduction),
        "specialized reduction extraction must honor the same subtree fence as general join ordering"
    );
}

#[test]
fn filtered_cte_join_is_reordered_as_an_atomic_relation() {
    let cte_ref = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(
        paro_planner::logical::operator::CTERef::new(
            12,
            30,
            "cte".to_string(),
            vec!["id".to_string()],
            vec![LogicalType::Integer],
        ),
    ));
    let cross = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        cte_ref,
        OwnedLogicalPlan::synthetic(create_scan(31)),
    ))));
    let plan = LogicalOperator::Filter(Filter::new(
        cross,
        vec![Expression::Comparison(
            paro_planner::expression::ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(30, 0),
                column_ref(31, 0),
            )
            .into(),
        )],
    ));

    let session = make_test_session();
    let bind_context = BindContext::new();
    let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
        .optimize(&session, &bind_context, plan)
        .unwrap();
    assert!(matches!(
        optimized,
        LogicalOperator::Join(Join::Comparison(_))
    ));
}

#[test]
fn optimize_coalesces_single_relation_filters_after_join_reordering() {
    let session = make_test_session();
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let cross = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
        OwnedLogicalPlan::synthetic(create_scan(0)),
        OwnedLogicalPlan::synthetic(create_scan(1)),
    ))));
    let compare = |comparison_type, left, right| {
        Expression::Comparison(
            paro_planner::expression::ComparisonExpression::new(comparison_type, left, right)
                .into(),
        )
    };
    let constant = |value| {
        Expression::Constant(
            ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
        )
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
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let join_ab = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Inner,
        OwnedLogicalPlan::synthetic(create_scan(0)),
        OwnedLogicalPlan::synthetic(create_scan(1)),
        vec![join_condition(JoinComparisonType::Equal, 0, 1)],
    )));
    let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Inner,
        OwnedLogicalPlan::synthetic(join_ab),
        OwnedLogicalPlan::synthetic(create_scan(2)),
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
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let plan = LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
        JoinType::Semi,
        OwnedLogicalPlan::synthetic(create_scan(0)),
        OwnedLogicalPlan::synthetic(create_scan(1)),
        vec![join_condition(JoinComparisonType::Equal, 0, 1)],
    )));

    assert!(optimizer.can_optimize_join(&plan));

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
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let mut join = ComparisonJoin::new(
        JoinType::Anti,
        OwnedLogicalPlan::synthetic(create_scan(0)),
        OwnedLogicalPlan::synthetic(create_scan(1)),
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
    let boundary = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            boundary_type,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ),
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            boundary,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![join_condition(JoinComparisonType::Equal, 0, 2)],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
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
    let preserved = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ),
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Semi,
            preserved,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![join_condition(JoinComparisonType::Equal, 0, 2)],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
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
fn reduction_cascade_shares_one_region_with_its_reorderable_preserved_joins() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let preserved = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ),
    )));
    let first = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Semi,
            preserved,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![join_condition(JoinComparisonType::Equal, 0, 2)],
        ),
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Semi,
            first,
            OwnedLogicalPlan::synthetic(create_scan(3)),
            vec![join_condition(JoinComparisonType::Equal, 0, 3)],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let mut filters = Vec::new();

    optimizer
        .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
        .unwrap();

    assert_eq!(optimizer.relation_manager.num_relations(), 4);
    assert_eq!(filters.len(), 3);
    assert_eq!(filters[0].join_type, JoinType::Inner);
    assert_eq!(filters[1].join_type, JoinType::Semi);
    assert_eq!(filters[2].join_type, JoinType::Semi);
}

#[test]
fn reduction_without_two_graph_roles_is_an_atomic_preserved_input() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let constant_key_reduction = OwnedLogicalPlan::synthetic(LogicalOperator::Join(
        Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            vec![paro_planner::logical::operator::JoinCondition::new(
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
                ),
                column_ref(1, 0),
                JoinComparisonType::Equal,
            )],
        )),
    ));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Semi,
            constant_key_reduction,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![join_condition(JoinComparisonType::Equal, 0, 2)],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let mut filters = Vec::new();

    optimizer
        .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
        .unwrap();

    assert_eq!(optimizer.relation_manager.num_relations(), 2);
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].join_type, JoinType::Semi);
    let LogicalOperator::Join(Join::Comparison(atomic)) = &optimizer.relation_plans[0].operator
    else {
        panic!("role-less reduction must remain an atomic relation")
    };
    assert_eq!(atomic.join_type, JoinType::Semi);
    assert!(matches!(atomic.conditions[0].left, Expression::Constant(_)));
}

#[test]
fn roleless_reduction_at_region_root_remains_fully_atomic() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let valid_inner_reduction = OwnedLogicalPlan::synthetic(LogicalOperator::Join(
        Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        )),
    ));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Semi,
            valid_inner_reduction,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![paro_planner::logical::operator::JoinCondition::new(
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
                ),
                column_ref(2, 0),
                JoinComparisonType::Equal,
            )],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let mut filters = Vec::new();

    assert!(
        !optimizer.can_optimize_join(&plan.operator),
        "a role-less root reduction must stop before graph extraction"
    );

    optimizer
        .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
        .unwrap();

    assert_eq!(optimizer.relation_manager.num_relations(), 1);
    assert!(filters.is_empty());
    let LogicalOperator::Join(Join::Comparison(root)) = &optimizer.relation_plans[0].operator
    else {
        panic!("role-less root reduction must remain one atomic relation")
    };
    assert_eq!(root.join_type, JoinType::Semi);
    assert!(matches!(root.conditions[0].left, Expression::Constant(_)));
    assert!(matches!(
        root.left.operator,
        LogicalOperator::Join(Join::Comparison(_))
    ));
}

#[test]
fn single_roleless_root_reduction_does_not_expose_its_preserved_join() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let preserved = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ),
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Anti,
            preserved,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![paro_planner::logical::operator::JoinCondition::new(
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(5), LogicalType::Integer).into(),
                ),
                column_ref(2, 0),
                JoinComparisonType::Equal,
            )],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
    let mut filters = Vec::new();

    optimizer
        .extract_join_relations(&session, &bind_context, &plan, &mut filters, true)
        .unwrap();

    assert_eq!(optimizer.relation_manager.num_relations(), 1);
    assert!(filters.is_empty());
    assert!(matches!(
        optimizer.relation_plans[0].operator,
        LogicalOperator::Join(Join::Comparison(_))
    ));
}

#[test]
fn extraction_treats_any_join_as_an_atomic_relation() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let boundary =
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Any(Box::new(AnyJoin::new(
            JoinType::Inner,
            OwnedLogicalPlan::synthetic(create_scan(0)),
            OwnedLogicalPlan::synthetic(create_scan(1)),
            Expression::Constant(
                ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
            ),
        )))));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            boundary,
            OwnedLogicalPlan::synthetic(create_scan(2)),
            vec![join_condition(JoinComparisonType::Equal, 0, 2)],
        ),
    )));
    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
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

    let join_ab = OwnedLogicalPlan {
        id: bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            rel_a,
            rel_b,
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ))),
    };
    let plan = OwnedLogicalPlan {
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

    let mut optimizer = JoinRegionPlanner::new(SelectivityDefaults::default());
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

    let LogicalOperator::Join(Join::Comparison(root)) = &optimized.operator else {
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

#[test]
fn reconstructed_inner_join_places_the_dp_build_input_on_the_right() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let small = projection_relation(&bind_context, 100, 0, 10);
    let large = projection_relation(&bind_context, 101, 1, 1_000_000);
    let plan = OwnedLogicalPlan {
        id: bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Inner,
            small,
            large,
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ))),
    };

    let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
        .optimize_plan(&session, plan, &HashMap::new(), &bind_context)
        .expect("join order optimization should succeed");
    let LogicalOperator::Join(Join::Comparison(root)) = &optimized.operator else {
        panic!("expected comparison join root")
    };

    assert_eq!(
        root.right.get_column_bindings()[0].table_index,
        0,
        "the input costed as the hash build must survive reconstruction on the right"
    );
    let condition = &root.conditions[0];
    let Expression::ColumnRef(left) = &condition.left else {
        panic!("expected a column join key")
    };
    let Expression::ColumnRef(right) = &condition.right else {
        panic!("expected a column join key")
    };
    assert_eq!(left.binding.table_index, 1);
    assert_eq!(right.binding.table_index, 0);
}

#[test]
fn reduction_control_region_orientation_survives_physical_side_selection() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let preserved = projection_relation(&bind_context, 100, 0, 1);
    let dependent_left = projection_relation(&bind_context, 101, 1, 64);
    let dependent_right = projection_relation(&bind_context, 102, 2, 64);
    let mut dependent = ComparisonJoin::new(
        JoinType::Inner,
        dependent_left,
        dependent_right,
        vec![join_condition(JoinComparisonType::Equal, 1, 2)],
    );
    dependent.duplicate_eliminated_columns = vec![column_ref(1, 0)];
    let dependent = OwnedLogicalPlan {
        id: bind_context.next_plan_id(),
        stats: NodeStats {
            estimated_cardinality: Some(CardinalityEstimate::exact(64)),
            ..NodeStats::default()
        },
        operator: LogicalOperator::Join(Join::Comparison(dependent)),
    };
    let plan = OwnedLogicalPlan {
        id: bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            preserved,
            dependent,
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ))),
    };

    let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
        .optimize_plan(&session, plan, &HashMap::new(), &bind_context)
        .expect("join-order optimization should succeed");
    let physical = BuildProbeSideOptimizer::new(Arc::clone(&session)).optimize_plan(optimized);
    let LogicalOperator::Join(Join::Comparison(root)) = &physical.operator else {
        panic!("expected reduction join root")
    };
    assert_eq!(root.join_type, JoinType::Semi);
    assert!(
        crate::cost::join_layout::contains_control_region_boundary(&root.right),
        "the filtering control region must remain the materialized build input"
    );
}

#[test]
fn reduction_work_orientation_survives_physical_side_selection() {
    let session = make_test_session();
    let bind_context = BindContext::new();
    let plan = OwnedLogicalPlan {
        id: bind_context.next_plan_id(),
        stats: NodeStats::default(),
        operator: LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
            JoinType::Semi,
            projection_relation(&bind_context, 100, 0, 1),
            projection_relation(&bind_context, 101, 1, 64),
            vec![join_condition(JoinComparisonType::Equal, 0, 1)],
        ))),
    };

    let optimized = JoinRegionPlanner::new(SelectivityDefaults::default())
        .optimize_plan(&session, plan, &HashMap::new(), &bind_context)
        .expect("join-order optimization should succeed");
    let physical = BuildProbeSideOptimizer::new(Arc::clone(&session)).optimize_plan(optimized);
    let LogicalOperator::Join(Join::Comparison(root)) = &physical.operator else {
        panic!("expected reduction join root")
    };
    assert_eq!(root.join_type, JoinType::RightSemi);
    assert_eq!(
        root.right.get_column_bindings()[0].table_index,
        0,
        "the smaller preserved input selected by DP must remain the physical build side"
    );
}
