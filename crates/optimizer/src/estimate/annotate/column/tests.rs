// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{
    CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
};
use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::window::WindowFunction;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, ComparisonExpression, WindowExpression, WindowFrame,
};
use paro_planner::logical::operator::{
    Aggregate, CTERef, ExpressionGet, Filter, Get, Limit, Projection, RecursiveCTE, Window,
};
use paro_storage::statistics::StringStats;
use paro_storage::table::table_factory::TableFactory;

use super::*;

fn make_test_session() -> Arc<StatementContext> {
    TestStatementContextBuilder::minimal().build()
}

fn keyed_group_aggregate(constraint: Constraint) -> Aggregate {
    let types = vec![
        LogicalType::BigInt,
        LogicalType::Varchar,
        LogicalType::Varchar,
    ];
    let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
    let info = CreateTableInfo::new(
        "paro".to_string(),
        "public".to_string(),
        "customer".to_string(),
        vec![
            ColumnDefinition::new("key".to_string(), LogicalType::BigInt),
            ColumnDefinition::new("name".to_string(), LogicalType::Varchar),
            ColumnDefinition::new("comment".to_string(), LogicalType::Varchar),
        ],
    )
    .with_constraints(vec![constraint]);
    let table = Arc::new(
        TableCatalogEntry::from_info(info, storage, CatalogObjectId::from_raw(20_001), 0).unwrap(),
    );
    let child = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
        7,
        vec!["key".to_string(), "name".to_string(), "comment".to_string()],
        types.clone(),
        table,
    ))));
    let groups = types
        .into_iter()
        .enumerate()
        .map(|(column_index, ty)| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(7, column_index), ty).into(),
            )
        })
        .collect();
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt).into(),
    );
    Aggregate::new(8, 9, 10, child, groups, Vec::new(), vec![count], Vec::new())
}

#[test]
fn primary_key_proves_group_dependencies_without_runtime_statistics() {
    let aggregate = keyed_group_aggregate(Constraint::primary_key(vec![0]));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate)));
    let propagated = StatisticsPropagator::new().propagate(make_test_session(), plan);
    let LogicalOperator::Aggregate(aggregate) = &propagated.operator else {
        panic!("expected aggregate root");
    };
    assert_eq!(
        aggregate.group_dependencies,
        [GroupDependency {
            determinants: Box::new([0]),
            dependents: Box::new([1, 2]),
        }]
    );
}

#[test]
fn nullable_unique_key_is_not_a_group_determinant() {
    let mut aggregate = keyed_group_aggregate(Constraint::unique(vec![0]));
    aggregate.group_stats[0] = Some(NumericStats::create_unknown(LogicalType::BigInt));
    assert!(derive_group_dependencies(&aggregate).is_empty());

    aggregate.group_stats[0] = Some(NumericStats::create_empty(LogicalType::BigInt));
    assert_eq!(derive_group_dependencies(&aggregate).len(), 1);
}

#[test]
fn memo_boundary_domains_do_not_leak_between_occurrences() {
    use paro_planner::logical::operator::{BoundReference, SetOperation};
    let input = |reference_id, value| {
        let reference = BoundReference::new(
            paro_planner::logical::operator::BoundReferenceId::group_hole(reference_id),
            vec![ColumnBinding::new(7, 0)],
            vec![LogicalType::Integer],
        );
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::BoundReference(reference)),
            vec![Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::Equal,
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(7, 0), LogicalType::Integer)
                            .into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
                    ),
                )
                .into(),
            )],
        )))
    };
    for values in [[2001, 2002], [2002, 2001]] {
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::union(
            9,
            input(1, values[0]),
            input(2, values[1]),
            true,
            vec![LogicalType::Integer],
        )));
        let plan = StatisticsPropagator::new().propagate(make_test_session(), plan);
        let LogicalOperator::SetOperation(union) = &plan.operator else {
            panic!("lost union")
        };
        assert!(matches!(union.left.operator, LogicalOperator::Filter(_)));
        assert!(matches!(union.right.operator, LogicalOperator::Filter(_)));
    }
}

#[test]
fn false_filter_becomes_schema_preserving_empty_result() {
    let bind_context = BindContext::new();
    let child = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            7,
            vec![vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(7, 0), LogicalType::Integer).into(),
            )]],
            vec!["quota".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let filter = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Filter(Filter::new(
            child,
            vec![Expression::Constant(
                ConstantExpression::new(Value::Boolean(false), LogicalType::Boolean).into(),
            )],
        )),
    );

    let optimized = StatisticsPropagator::new().propagate(make_test_session(), filter);

    match &optimized.operator {
        LogicalOperator::EmptyResult(empty) => {
            assert_eq!(empty.get_types(), vec![LogicalType::Integer]);
            assert_eq!(empty.child.output_names(), vec!["quota".to_string()]);
        }
        other => panic!("expected schema-preserving EmptyResult, got {other:?}"),
    }
}

#[test]
fn statistics_propagation_uses_a_heap_stack_for_deep_plans() {
    std::thread::Builder::new()
        .name("deep-statistics-propagation".to_string())
        .stack_size(512 * 1024)
        .spawn(|| {
            const DEPTH: usize = 10_000;
            let bind_context = BindContext::new();
            let mut plan = OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::ExpressionGet(ExpressionGet::new(
                    17,
                    vec![vec![Expression::Constant(
                        ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                    )]],
                    vec!["v".to_string()],
                    vec![LogicalType::Integer],
                )),
            );
            for _ in 0..DEPTH {
                plan = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::Limit(Box::new(Limit::new(plan, None, None))),
                );
            }

            let plan = StatisticsPropagator::new().propagate(make_test_session(), plan);
            let mut current = &plan;
            for _ in 0..DEPTH {
                let LogicalOperator::Limit(limit) = &current.operator else {
                    panic!("expected the deep limit chain to remain intact");
                };
                current = &limit.child;
            }
            assert!(matches!(
                &current.operator,
                LogicalOperator::ExpressionGet(_)
            ));
        })
        .expect("deep statistics thread should start")
        .join()
        .expect("deep statistics propagation should complete");
}

#[test]
fn group_dependency_collection_uses_a_heap_stack_for_deep_aggregate_inputs() {
    std::thread::Builder::new()
        .name("deep-group-dependency-collection".to_string())
        .stack_size(512 * 1024)
        .spawn(|| {
            const DEPTH: usize = 10_000;
            let mut aggregate = keyed_group_aggregate(Constraint::primary_key(vec![0]));
            let mut child = *aggregate.child;
            for _ in 0..DEPTH {
                child = OwnedLogicalPlan::synthetic(LogicalOperator::Limit(Box::new(Limit::new(
                    child, None, None,
                ))));
            }
            aggregate.child = Box::new(child);

            let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate)));
            let propagated = StatisticsPropagator::new().propagate(make_test_session(), plan);
            let LogicalOperator::Aggregate(aggregate) = &propagated.operator else {
                panic!("expected aggregate root");
            };
            assert_eq!(
                aggregate.group_dependencies,
                [GroupDependency {
                    determinants: Box::new([0]),
                    dependents: Box::new([1, 2]),
                }]
            );

            let mut child = aggregate.child.as_ref();
            for _ in 0..DEPTH {
                let LogicalOperator::Limit(limit) = &child.operator else {
                    panic!("expected deep aggregate input to retain its limit chain");
                };
                child = &limit.child;
            }
            assert!(matches!(&child.operator, LogicalOperator::Get(_)));
        })
        .expect("deep group-dependency thread should start")
        .join()
        .expect("deep group-dependency collection should complete");
}

#[test]
fn equality_filter_publishes_exact_singleton_domain() {
    let binding = ColumnBinding::new(7, 0);
    let mut original = ColumnStatistics::new(BaseStatistics::create_unknown(LogicalType::Integer));
    original.update_distinct_statistics(&[11, 29, 47], 3);
    let mut propagator = StatisticsPropagator::new();
    propagator
        .statistics_map
        .insert(binding, Arc::new(original));
    let mut predicate = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Integer).into()),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(2001), LogicalType::Integer).into(),
            ),
        )
        .into(),
    );

    assert_eq!(
        propagator.handle_filter(&mut predicate),
        FilterPropagateResult::NoPruningPossible
    );
    let statistics = propagator
        .statistics_map
        .get(&binding)
        .expect("filtered column statistics");
    assert_eq!(
        statistics.statistics().min_value(),
        Some(Value::Integer(2001))
    );
    assert_eq!(
        statistics.statistics().max_value(),
        Some(Value::Integer(2001))
    );
    assert_eq!(statistics.distinct_evidence().point, 0);
}

#[test]
fn recursive_reference_does_not_inherit_anchor_only_domain() {
    let bind_context = BindContext::new();
    let anchor = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            7,
            vec![vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )]],
            vec!["n".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let recursive_binding = ColumnBinding::new(8, 0);
    let recursive = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::CTERef(CTERef::new(
                    3,
                    8,
                    "counter".to_string(),
                    vec!["n".to_string()],
                    vec![LogicalType::Integer],
                )),
            ),
            vec![Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::LessThan,
                    Expression::ColumnRef(
                        ColumnRefExpression::new(recursive_binding, LogicalType::Integer).into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(4), LogicalType::Integer).into(),
                    ),
                )
                .into(),
            )],
        )),
    );
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::RecursiveCTE(RecursiveCTE {
            cte_index: 3,
            cte_name: "counter".to_string(),
            column_names: vec!["n".to_string()],
            column_types: vec![LogicalType::Integer],
            union_all: true,
            anchor: Box::new(anchor),
            recursive: Box::new(recursive),
        }),
    );

    let propagated = StatisticsPropagator::new().propagate(make_test_session(), plan);
    let LogicalOperator::RecursiveCTE(cte) = &propagated.operator else {
        panic!("expected recursive CTE root");
    };
    let LogicalOperator::Filter(filter) = &cte.recursive.operator else {
        panic!("recursive termination filter must remain in the plan");
    };
    assert_eq!(filter.expressions.len(), 1);
}

#[test]
fn window_outputs_keep_statistics_available_to_parent_projections() {
    let bind_context = BindContext::new();
    let input = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Projection(Projection::new(
            7,
            OwnedLogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
            vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(11), LogicalType::Integer).into(),
            )],
        )),
    );
    let function = WindowFunction::row_number();
    let window = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Window(Window::new(
            20,
            vec![WindowExpression::native(
                function.clone(),
                Vec::new(),
                vec![Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(7, 0), LogicalType::Integer).into(),
                )],
                Vec::new(),
                WindowFrame::get_default_frame(&function),
                false,
            )],
            input,
        )),
    );
    let root = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Projection(Projection::new(
            30,
            window,
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(20, 0), LogicalType::BigInt).into(),
            )],
        )),
    );

    let mut propagator = StatisticsPropagator::new();
    propagator.propagate(make_test_session(), root);

    for binding in [ColumnBinding::new(20, 0), ColumnBinding::new(30, 0)] {
        let stats = propagator
            .statistics_map()
            .get(&binding)
            .unwrap_or_else(|| panic!("missing statistics for {binding:?}"));
        assert_eq!(stats.statistics().get_type(), &LogicalType::BigInt);
        assert!(!stats.statistics().can_have_null());
        assert!(stats.statistics().can_have_no_null());
        assert_eq!(stats.statistics().min_value(), Some(Value::BigInt(1)));
        assert_eq!(
            stats.statistics().max_value(),
            Some(Value::BigInt(i64::MAX))
        );
    }
}

#[test]
fn aggregate_retains_group_statistics_for_physical_planning() {
    let bind_context = BindContext::new();
    let input = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Projection(Projection::new(
            7,
            OwnedLogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
            vec![Expression::Constant(
                ConstantExpression::new(Value::Varchar("R".to_string()), LogicalType::Varchar)
                    .into(),
            )],
        )),
    );
    let aggregate = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Aggregate(Box::new(Aggregate::new(
            20,
            21,
            22,
            input,
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(7, 0), LogicalType::Varchar).into(),
            )],
            vec![paro_planner::binder::ir::GroupingSet {
                expressions: vec![0],
            }],
            Vec::new(),
            Vec::new(),
        ))),
    );

    let optimized = StatisticsPropagator::new().propagate(make_test_session(), aggregate);
    let LogicalOperator::Aggregate(aggregate) = &optimized.operator else {
        panic!("expected aggregate");
    };
    let stats = aggregate.group_stats[0].as_ref().expect("group statistics");
    assert_eq!(stats.min_value(), Some(Value::Varchar("R".to_string())));
    assert_eq!(StringStats::max_string_length(stats), Some(1));
}

#[test]
fn aggregate_group_statistics_retain_hll_distinct_estimate() {
    let mut statistics = ColumnStatistics::new(BaseStatistics::create_empty(LogicalType::Integer));
    statistics.update_distinct_statistics(&[11, 22, 33, 44], 4);

    let group = aggregate_group_statistics(&statistics);

    assert!(statistics.distinct_evidence().point > 0);
    assert_eq!(
        group.get_distinct_count() as u64,
        statistics.distinct_evidence().point
    );
}

#[test]
fn window_statistics_only_publish_function_intrinsic_facts() {
    let cume_dist = WindowFunction::cume_dist();
    let cume_dist = WindowExpression::native(
        cume_dist.clone(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        WindowFrame::get_default_frame(&cume_dist),
        false,
    );
    let cume_dist_stats = window_output_statistics(&cume_dist);
    assert!(!cume_dist_stats.can_have_null());
    assert_eq!(cume_dist_stats.min_value(), Some(Value::Double(0.0)));
    assert_eq!(cume_dist_stats.max_value(), Some(Value::Double(1.0)));

    let ntile = WindowFunction::ntile();
    let ntile = WindowExpression::native(
        ntile.clone(),
        vec![Expression::Constant(
            ConstantExpression::new(Value::BigInt(4), LogicalType::BigInt).into(),
        )],
        Vec::new(),
        Vec::new(),
        WindowFrame::get_default_frame(&ntile),
        false,
    );
    let ntile_stats = window_output_statistics(&ntile);
    assert!(ntile_stats.can_have_null());
    assert!(ntile_stats.can_have_no_null());
    assert_eq!(ntile_stats.min_value(), None);
    assert_eq!(ntile_stats.max_value(), None);
}
