// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_catalog::entry::{EdgeTableInfo, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_context::test_support::TestStatementContextBuilder;
use paro_context::StatementContext;
use paro_planner::binder::context::BindContext;
use paro_planner::binder::ir::{CTEMaterialize, GroupingSet};
use paro_planner::expression::{ColumnRefExpression, ComparisonExpression, ConstantExpression};
use paro_planner::logical::operator::graph_expand::ExpandDirection;
use paro_planner::logical::operator::{
    Aggregate, CTERef, DelimGet, ExpressionGet, Filter, GraphExpand, GraphScan, Limit,
    MaterializedCTE, Projection,
};
use paro_storage::index::graph::GraphStatistics;

use super::*;
use crate::context::{GraphStatsCache, GraphStatsLoader, OptimizationContext};

fn make_test_session() -> Arc<StatementContext> {
    TestStatementContextBuilder::minimal().build()
}

fn column_ref(table_index: usize, column_index: usize) -> Expression {
    Expression::ColumnRef(
        ColumnRefExpression {
            binding: ColumnBinding::new(table_index, column_index),
            depth: 0,
            return_type: LogicalType::BigInt,
        }
        .into(),
    )
}

fn equality(
    left_table: usize,
    left_column: usize,
    right_table: usize,
    right_column: usize,
) -> JoinCondition {
    JoinCondition::new(
        column_ref(left_table, left_column),
        column_ref(right_table, right_column),
        JoinComparisonType::Equal,
    )
}

#[test]
fn finite_filter_domain_is_counted_once_and_is_not_a_frequency_proof() {
    use paro_planner::logical::plan::finite_domain::{DomainValue, FiniteDomains};
    let predicate = |table, value| {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(table, 0),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(value), LogicalType::BigInt).into(),
                ),
            )
            .into(),
        )
    };
    let domains = FiniteDomains::from([(
        ColumnBinding::new(1, 0),
        BTreeSet::from([DomainValue::Integer(1), DomainValue::Integer(2)]),
    )]);
    let selected = finite_filter_fraction(&[predicate(1, 1), predicate(1, 1)], &domains).unwrap();
    assert_eq!(selected.fraction, 0.5);
    assert!(!selected.impossible);
    assert!(selected.residuals.is_empty());
    assert!(
        finite_filter_fraction(&[predicate(1, 3)], &domains)
            .unwrap()
            .impossible
    );

    // Numerical underflow of an independence prior is not a proof that
    // a conjunction has no matching row.
    let domains = (0..1200)
        .map(|table| {
            (
                ColumnBinding::new(table, 0),
                BTreeSet::from([DomainValue::Integer(1), DomainValue::Integer(2)]),
            )
        })
        .collect();
    let predicates = (0..1200)
        .map(|table| predicate(table, 1))
        .collect::<Vec<_>>();
    let selected = finite_filter_fraction(&predicates, &domains).unwrap();
    assert_eq!(selected.fraction, 0.0);
    assert!(!selected.impossible);
}

/// Independent two-gather baseline: compare every relation property and
/// serialized column evidence, including producer-before-consumer state.
fn assert_query_settlement(plan: &OwnedLogicalPlan, context: &OptimizationContext) {
    use crate::estimate::annotate::column::StatisticsPropagator;
    use paro_planner::binder::deep_copy::duplicate_plan_preserving_indices;
    let duplicate =
        || duplicate_plan_preserving_indices(plan, context.bind_context.shared().as_ref());
    let mut old_context = context.fork_for_candidate(Arc::new(HashMap::new()));
    let mut new_context = context.fork_for_candidate(Arc::new(HashMap::new()));
    let old = StatisticsGathering::new()
        .gather(duplicate(), &mut old_context)
        .unwrap();
    let mut propagation = StatisticsPropagator::new();
    let old = propagation.propagate(old_context.session.clone(), old);
    old_context.column_stats = Arc::new(propagation.take_statistics_map());
    let old = StatisticsGathering::new()
        .gather(old, &mut old_context)
        .unwrap();
    let new = crate::estimate::annotate(duplicate(), &mut new_context).unwrap();
    let mut pending = vec![(&old, &new)];
    while let Some((old, new)) = pending.pop() {
        assert_eq!(
            std::mem::discriminant(&old.operator),
            std::mem::discriminant(&new.operator)
        );
        assert_eq!(old.stats, new.stats);
        assert_eq!(old.output_layout(), new.output_layout());
        let old_children = old.children();
        let new_children = new.children();
        assert_eq!(old_children.len(), new_children.len());
        pending.extend(old_children.into_iter().zip(new_children));
    }
    let columns = |context: &OptimizationContext| {
        context
            .column_stats
            .iter()
            .map(|(binding, column)| (*binding, column.to_bytes().unwrap()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(columns(&old_context), columns(&new_context));
}

fn values_relation(
    bind_context: &BindContext,
    table_index: usize,
    rows: usize,
) -> OwnedLogicalPlan {
    OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            vec![
                vec![Expression::Constant(
                    ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt,).into()
                )];
                rows
            ],
            vec!["v".to_string()],
            vec![LogicalType::BigInt],
        )),
    )
}

#[test]
fn statistics_gathering_sets_expression_get_and_limit_cardinality() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());

    let child = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![
                vec![Expression::Constant(
                    ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt,).into()
                )];
                10
            ],
            vec!["v".to_string()],
            vec![LogicalType::BigInt],
        )),
    );
    let projection = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Projection(Projection::new(
            2,
            child,
            vec![Expression::ColumnRef(
                ColumnRefExpression {
                    binding: ColumnBinding::new(1, 0),
                    depth: 0,
                    return_type: LogicalType::BigInt,
                }
                .into(),
            )],
        )),
    );
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Limit(Box::new(Limit::new(
            projection,
            Some(Expression::Constant(
                ConstantExpression::new(Value::BigInt(3), LogicalType::BigInt).into(),
            )),
            None,
        ))),
    );

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");

    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(3))
    );
    let LogicalOperator::Limit(limit) = &gathered.operator else {
        panic!("expected limit");
    };
    assert_eq!(
        limit.child.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(10))
    );
    assert!(ctx.column_stats.contains_key(&ColumnBinding::new(2, 0)));
}

#[test]
fn statistics_gathering_uses_a_heap_stack_for_deep_plans() {
    std::thread::Builder::new()
        .name("deep-statistics-gathering".to_string())
        .stack_size(512 * 1024)
        .spawn(|| {
            const DEPTH: usize = 10_000;
            let bind_context = BindContext::new();
            let session = make_test_session();
            let mut ctx = OptimizationContext::new(session, bind_context.clone());
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

            let plan = StatisticsGathering::new()
                .gather(plan, &mut ctx)
                .expect("deep gathering should not consume the native stack");
            assert!(ctx.column_stats.contains_key(&ColumnBinding::new(17, 0)));
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
        .expect("deep statistics gathering should complete");
}

#[test]
fn delimiter_collection_uses_a_heap_stack_for_a_deep_dependent_rhs() {
    std::thread::Builder::new()
        .name("deep-delimiter-collection".to_string())
        .stack_size(512 * 1024)
        .spawn(|| {
            const DEPTH: usize = 10_000;
            let bind_context = BindContext::new();
            let session = make_test_session();
            let mut ctx = OptimizationContext::new(session, bind_context.clone());
            let outer = values_relation(&bind_context, 1, 35);
            let mut dependent = OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::DelimGet(DelimGet::new(9, vec![LogicalType::BigInt])),
            );
            for _ in 0..DEPTH {
                dependent = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::Limit(Box::new(Limit::new(dependent, None, None))),
                );
            }
            let mut join = paro_planner::logical::operator::ComparisonJoin::new(
                JoinType::Inner,
                outer,
                dependent,
                vec![equality(1, 0, 9, 0)],
            );
            join.duplicate_eliminated_columns = vec![column_ref(1, 0)];
            let plan =
                OwnedLogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));

            let gathered = StatisticsGathering::new()
                .gather(plan, &mut ctx)
                .expect("deep delimiter gathering should succeed");
            let LogicalOperator::Join(Join::Comparison(join)) = &gathered.operator else {
                panic!("expected delimiter join root");
            };
            let mut dependent = join.right.as_ref();
            for _ in 0..DEPTH {
                let LogicalOperator::Limit(limit) = &dependent.operator else {
                    panic!("expected deep dependent RHS to retain its limit chain");
                };
                dependent = &limit.child;
            }
            assert!(matches!(&dependent.operator, LogicalOperator::DelimGet(_)));
            let estimate = dependent
                .stats
                .estimated_cardinality
                .expect("deep DelimGet should inherit the outer key domain");
            assert!(estimate.expected > 1 && estimate.expected < 35);
            assert!(ctx.column_stats.contains_key(&ColumnBinding::new(9, 0)));
        })
        .expect("deep delimiter thread should start")
        .join()
        .expect("deep delimiter collection should complete");
}

#[test]
fn graph_name_lookup_uses_a_heap_stack_for_a_deep_graph_chain() {
    std::thread::Builder::new()
        .name("deep-graph-name-lookup".to_string())
        .stack_size(512 * 1024)
        .spawn(|| {
            const DEPTH: usize = 10_000;

            struct StaticGraphStatsLoader;

            impl GraphStatsLoader for StaticGraphStatsLoader {
                fn load(&self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
                    (graph_name == "g").then(|| {
                        Arc::new(
                            GraphStatistics::default()
                                .with_vertex_count("v", 10)
                                .with_pattern_step_count("v", "e", "v", 10),
                        )
                    })
                }
            }

            let bind_context = BindContext::new();
            let mut ctx = OptimizationContext::new(make_test_session(), bind_context.clone());
            ctx.graph_stats = GraphStatsCache::with_loader(Arc::new(StaticGraphStatsLoader));
            let mut child = OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::GraphScan(Box::new(GraphScan::new(
                    VertexTableInfo {
                        table_name: "vertices".to_string(),
                        table_oid: 1,
                        key_column_ids: vec![0],
                        label: "v".to_string(),
                        property_column_ids: Vec::new(),
                    },
                    None,
                    0,
                    3,
                    "v".to_string(),
                    "g".to_string(),
                    "public".to_string(),
                ))),
            );
            for _ in 0..DEPTH {
                child = OwnedLogicalPlan::new(
                    &bind_context,
                    LogicalOperator::Filter(Filter::new(child, Vec::new())),
                );
            }
            let plan = OwnedLogicalPlan::new(
                &bind_context,
                LogicalOperator::GraphExpand(Box::new(GraphExpand::new(
                    EdgeTableInfo {
                        table_name: "edges".to_string(),
                        table_oid: 2,
                        key_column_ids: vec![0],
                        source_key_column_ids: vec![0],
                        source_vertex_table: "vertices".to_string(),
                        source_ref_column_ids: vec![0],
                        destination_key_column_ids: vec![0],
                        destination_vertex_table: "vertices".to_string(),
                        destination_ref_column_ids: vec![0],
                        label: "e".to_string(),
                        property_column_ids: Vec::new(),
                    },
                    ExpandDirection::Forward,
                    "v".to_string(),
                    0,
                    1,
                    2,
                    3,
                    "v".to_string(),
                    1,
                    1,
                    "vertices".to_string(),
                    child,
                ))),
            );

            let gathered = StatisticsGathering::new()
                .gather(plan, &mut ctx)
                .expect("deep graph gathering should succeed");
            assert_eq!(
                gathered.stats.estimated_cardinality,
                Some(CardinalityEstimate {
                    min: 5,
                    expected: 10,
                    max: 20,
                })
            );

            let LogicalOperator::GraphExpand(expand) = &gathered.operator else {
                panic!("expected graph-expand root");
            };
            let mut child = expand.child.as_ref();
            for _ in 0..DEPTH {
                let LogicalOperator::Filter(filter) = &child.operator else {
                    panic!("expected deep graph chain to retain its filters");
                };
                child = &filter.child;
            }
            let LogicalOperator::GraphScan(scan) = &child.operator else {
                panic!("expected graph-scan leaf");
            };
            assert_eq!(scan.graph_name, "g");
            assert_eq!(
                child.stats.estimated_cardinality,
                Some(CardinalityEstimate::exact(10))
            );
        })
        .expect("deep graph-name thread should start")
        .join()
        .expect("deep graph-name lookup should complete");
}

#[test]
fn materialized_cte_publishes_producer_cardinality_before_its_consumer() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let producer = values_relation(&bind_context, 1, 37);
    let consumer = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::CTERef(CTERef::new(
            9,
            2,
            "shared".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::BigInt],
        )),
    );
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::MaterializedCTE(MaterializedCTE::new(
            9,
            "shared".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::BigInt],
            CTEMaterialize::Materialized,
            producer,
            consumer,
        )),
    );

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");
    let LogicalOperator::MaterializedCTE(cte) = &gathered.operator else {
        panic!("expected materialized CTE");
    };

    assert_eq!(
        cte.child.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(37))
    );
    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(37))
    );
}

#[test]
fn detached_cte_reference_retains_its_owner_cardinality_summary() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let mut reference = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::CTERef(CTERef::new(
            9,
            2,
            "shared".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::BigInt],
        )),
    );
    reference.stats.estimated_cardinality = Some(CardinalityEstimate::exact(37));

    assert_query_settlement(&reference, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(reference, &mut ctx)
        .expect("detached reference should retain its summary");

    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(37))
    );
}

#[test]
fn delim_join_publishes_outer_key_domain_before_its_consumer() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let outer = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            (0..35)
                .map(|value| {
                    vec![Expression::Constant(
                        ConstantExpression::new(Value::BigInt(value % 7), LogicalType::BigInt)
                            .into(),
                    )]
                })
                .collect(),
            vec!["key".to_string()],
            vec![LogicalType::BigInt],
        )),
    );
    let delim = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::DelimGet(DelimGet::new(9, vec![LogicalType::BigInt])),
    );
    let mut join = paro_planner::logical::operator::ComparisonJoin::new(
        JoinType::Inner,
        outer,
        delim,
        vec![equality(1, 0, 9, 0)],
    );
    join.duplicate_eliminated_columns = vec![column_ref(1, 0)];
    let plan = OwnedLogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");
    let LogicalOperator::Join(Join::Comparison(join)) = &gathered.operator else {
        panic!("expected delim join")
    };
    let estimate = join
        .right
        .stats
        .estimated_cardinality
        .expect("DelimGet should inherit the outer key domain");
    assert!(estimate.expected > 1 && estimate.expected < 35);
    assert_eq!(estimate.max, estimate.expected * 2);
    assert!(ctx.column_stats.contains_key(&ColumnBinding::new(9, 0)));
}

#[test]
fn dummy_scan_is_the_exact_one_row_relation() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let plan = OwnedLogicalPlan::new(&bind_context, LogicalOperator::DummyScan);

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");

    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(1))
    );
}

#[test]
fn residual_filter_reestimates_from_current_input_not_stale_join_graph() {
    let bind_context = BindContext::new();
    let mut ctx = OptimizationContext::new(make_test_session(), bind_context.clone());
    let mut plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Filter(Filter::new(values_relation(&bind_context, 1, 10), vec![])),
    );
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1000));
    plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;
    let gathered = StatisticsGathering::new().gather(plan, &mut ctx).unwrap();
    assert_eq!(gathered.stats.estimated_cardinality.unwrap().expected, 10);
    assert_eq!(
        gathered.stats.cardinality_provenance,
        CardinalityProvenance::Statistics
    );
}

#[test]
fn statistics_gathering_preserves_join_graph_cardinality_provenance() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let join = paro_planner::logical::operator::ComparisonJoin::new(
        JoinType::Inner,
        values_relation(&bind_context, 1, 10),
        values_relation(&bind_context, 2, 20),
        vec![equality(1, 0, 2, 0)],
    );
    let mut plan =
        OwnedLogicalPlan::new(&bind_context, LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(73));
    plan.stats.cardinality_provenance = CardinalityProvenance::JoinGraph;

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");

    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(73))
    );
    assert_eq!(
        gathered.stats.cardinality_provenance,
        CardinalityProvenance::JoinGraph
    );
}

#[test]
fn grouped_aggregate_over_proven_empty_input_is_empty() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let child = values_relation(&bind_context, 1, 0);
    let aggregate = Aggregate::new(
        2,
        3,
        4,
        child,
        vec![column_ref(1, 0)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Aggregate(Box::new(aggregate)),
    );

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");

    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(0))
    );
}

#[test]
fn known_group_distinct_count_has_a_finite_uncertainty_envelope() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context);
    let binding = ColumnBinding::new(1, 0);
    let base = BaseStatistics::new(LogicalType::BigInt);
    let mut stats = ColumnStatistics::new(base);
    stats.update_distinct_statistics(&[11, 29], 2);
    ctx.column_stats_mut().insert(binding, Arc::new(stats));

    assert_eq!(
        estimate_group_distinct(&column_ref(1, 0), &ctx, 4_096, 4_096),
        (2, Some(4))
    );
}

#[test]
fn singleton_range_is_exact_without_forging_an_hll_observation() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context);
    let binding = ColumnBinding::new(1, 0);
    ctx.column_stats_mut().insert(
        binding,
        Arc::new(
            ColumnStatistics::new(BaseStatistics::from_constant(&Value::BigInt(2001)))
                .with_guaranteed_distinct_upper(1),
        ),
    );

    assert_eq!(
        estimate_group_distinct(&column_ref(1, 0), &ctx, 4_096, 4_096),
        (1, Some(1))
    );
}

#[test]
fn value_selecting_aggregate_preserves_its_input_domain_statistics() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context);
    let binding = ColumnBinding::new(1, 0);
    let mut input = ColumnStatistics::new(BaseStatistics::from_constant(&Value::BigInt(7)));
    input.update_distinct_statistics(&[11, 29, 47], 3);
    ctx.column_stats_mut().insert(binding, Arc::new(input));
    let (function, target_types) =
        paro_function::aggregate::distributive::first_last::get_first_function()
            .bind(&[LogicalType::BigInt])
            .expect("bind FIRST");
    assert_eq!(target_types, vec![LogicalType::BigInt]);
    let return_type = function.return_type.clone();
    let expression = Expression::Aggregate(
        paro_planner::expression::AggregateExpression::new(
            function,
            vec![column_ref(1, 0)],
            return_type,
        )
        .into(),
    );

    let unconstrained = aggregate_expression_statistics(&expression, &ctx, None);
    assert_eq!(unconstrained.distinct_evidence().point, 3);
    assert_eq!(unconstrained.guaranteed_distinct_upper(), None);

    let output = aggregate_expression_statistics(&expression, &ctx, Some(1));

    assert_eq!(output.distinct_evidence().point, 1);
    assert_eq!(output.statistics().min_value(), Some(Value::BigInt(7)));
    assert_eq!(output.statistics().max_value(), Some(Value::BigInt(7)));
}

#[test]
fn filter_output_replaces_storage_hll_with_exact_equality_domain() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let binding = ColumnBinding::new(1, 0);
    let mut storage = ColumnStatistics::new(BaseStatistics::create_unknown(LogicalType::BigInt));
    storage.update_distinct_statistics(&[11, 29, 47], 3);
    ctx.column_stats_mut().insert(binding, Arc::new(storage));
    let filter = Filter::new(
        values_relation(&bind_context, 1, 4),
        vec![Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(1, 0),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(2001), LogicalType::BigInt).into(),
                ),
            )
            .into(),
        )],
    );

    let output = filter_output_stats(&filter, &filter.child.output_layout(), &ctx);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].distinct_evidence().point, 1);
    assert_eq!(
        output[0].statistics().min_value(),
        Some(Value::BigInt(2001))
    );
    ctx.column_stats_mut().insert(binding, output[0].clone());
    assert_eq!(
        estimate_group_distinct(&column_ref(1, 0), &ctx, 4, 4),
        (1, Some(1))
    );
}

#[test]
fn filter_output_stats_follow_non_identity_projection_order() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let first = ColumnBinding::new(1, 0);
    let third = ColumnBinding::new(1, 2);
    ctx.column_stats_mut().insert(
        first,
        Arc::new(ColumnStatistics::new(BaseStatistics::from_constant(
            &Value::BigInt(7),
        ))),
    );
    ctx.column_stats_mut().insert(
        third,
        Arc::new(ColumnStatistics::new(BaseStatistics::from_constant(
            &Value::BigInt(99),
        ))),
    );
    let child = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![vec![
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(7), LogicalType::BigInt).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(8), LogicalType::BigInt).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(99), LogicalType::BigInt).into(),
                ),
            ]],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec![LogicalType::BigInt; 3],
        )),
    );
    let mut filter = Filter::new(
        child,
        vec![Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(1, 0),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(2001), LogicalType::BigInt).into(),
                ),
            )
            .into(),
        )],
    );
    filter.projection_map = vec![2, 0].into();

    let output = filter_output_stats(&filter, &filter.child.output_layout(), &ctx);

    assert_eq!(output.len(), 2);
    assert_eq!(output[0].statistics().min_value(), Some(Value::BigInt(99)));
    assert_eq!(
        output[1].statistics().min_value(),
        Some(Value::BigInt(2001))
    );
    assert_eq!(output[1].guaranteed_distinct_upper(), Some(1));
}

#[test]
fn finite_equality_disjunction_publishes_a_plan_invariant_domain() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let binding = ColumnBinding::new(1, 0);
    let mut storage = ColumnStatistics::new(BaseStatistics::create_unknown(LogicalType::BigInt));
    storage.update_distinct_statistics(&[11, 29, 47], 3);
    ctx.column_stats_mut().insert(binding, Arc::new(storage));
    let equality = |year| {
        Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                column_ref(1, 0),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(year), LogicalType::BigInt).into(),
                ),
            )
            .into(),
        )
    };
    let filter = Filter::new(
        values_relation(&bind_context, 1, 4),
        vec![Expression::Conjunction(
            paro_planner::expression::ConjunctionExpression::new(
                ConjunctionType::Or,
                vec![equality(2001), equality(2002), equality(2001)],
            )
            .into(),
        )],
    );

    let output = filter_output_stats(&filter, &filter.child.output_layout(), &ctx);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].distinct_evidence().point, 2);
    assert_eq!(output[0].guaranteed_distinct_upper(), Some(2));
    assert_eq!(
        output[0].statistics().min_value(),
        Some(Value::BigInt(2001))
    );
    assert_eq!(
        output[0].statistics().max_value(),
        Some(Value::BigInt(2002))
    );
    ctx.column_stats_mut().insert(binding, output[0].clone());
    assert_eq!(
        estimate_group_distinct(&column_ref(1, 0), &ctx, 4, 4),
        (2, Some(2))
    );
}

#[test]
fn same_domain_semi_join_does_not_count_duplicate_demand_rows_twice() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let mut shared = ColumnStatistics::new(BaseStatistics::from_constant(&Value::BigInt(7)));
    shared.update_distinct_statistics(&[11, 29, 47], 3);
    let shared = Arc::new(shared);
    ctx.column_stats_mut()
        .insert(ColumnBinding::new(1, 0), shared.clone());
    ctx.column_stats_mut()
        .insert(ColumnBinding::new(2, 0), shared);

    let join = paro_planner::logical::operator::ComparisonJoin::new(
        JoinType::Semi,
        values_relation(&bind_context, 1, 1),
        values_relation(&bind_context, 2, 1),
        vec![equality(1, 0, 2, 0)],
    );
    let estimate = estimate_same_domain_semi_join(
        &join,
        CardinalityEstimate {
            min: 36_000,
            expected: 73_049,
            max: 90_000,
        },
        CardinalityEstimate {
            min: 362,
            expected: 724,
            max: 1_086,
        },
        &[ColumnBinding::new(1, 0)],
        &[ColumnBinding::new(2, 0)],
        &CardinalityInputs {
            column_stats: &ctx.column_stats,
            cost_model: &ctx.cost_model,
            session: &ctx.session,
            graph_stats: &mut ctx.graph_stats,
        },
    )
    .expect("same-domain semi join estimate");

    assert_eq!(estimate.expected, 724);
    assert_eq!(estimate.max, 1_086);
}

#[test]
fn empty_grouping_set_emits_one_row_over_empty_input() {
    let bind_context = BindContext::new();
    let session = make_test_session();
    let mut ctx = OptimizationContext::new(session, bind_context.clone());
    let child = values_relation(&bind_context, 1, 0);
    let aggregate = Aggregate::new(
        2,
        3,
        4,
        child,
        vec![column_ref(1, 0)],
        vec![GroupingSet {
            expressions: Vec::new(),
        }],
        Vec::new(),
        Vec::new(),
    );
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Aggregate(Box::new(aggregate)),
    );

    assert_query_settlement(&plan, &ctx);
    let gathered = StatisticsGathering::new()
        .gather(plan, &mut ctx)
        .expect("gather should succeed");

    assert_eq!(
        gathered.stats.estimated_cardinality,
        Some(CardinalityEstimate::exact(1))
    );
}

#[test]
fn mark_and_single_joins_preserve_left_cardinality() {
    let left = CardinalityEstimate::exact(73);
    let right = CardinalityEstimate::exact(2);
    let inner = CardinalityEstimate::exact(1);

    assert_eq!(
        adjust_join_estimate(inner, left, right, JoinType::Mark),
        left
    );
    assert_eq!(
        adjust_join_estimate(inner, left, right, JoinType::Single),
        left
    );

    // This is the correlated-aggregate failure shape: the dependent side
    // currently estimates to zero, but MARK/SINGLE still emit one derived
    // value for every preserved row.
    let empty = CardinalityEstimate::exact(0);
    assert_eq!(
        adjust_join_estimate(empty, left, empty, JoinType::Mark),
        left
    );
    assert_eq!(
        adjust_join_estimate(empty, left, empty, JoinType::Single),
        left
    );
}

#[test]
fn composite_equalities_share_one_marginal_domain_per_relation_pair() {
    let conditions = [equality(0, 0, 1, 0), equality(0, 1, 1, 1)];
    let pair = equality_relation_pair(&conditions[0], &[], &[]);
    assert_eq!(pair, equality_relation_pair(&conditions[1], &[], &[]));
    let selectivity =
        correlate_join_condition_selectivities([(pair, 1.0 / 200_000.0), (pair, 1.0 / 10_000.0)]);
    assert_eq!(selectivity, 1.0 / 200_000.0);
}

#[test]
fn equalities_between_different_relation_pairs_remain_independent() {
    let first_pair = equality_relation_pair(&equality(0, 0, 1, 0), &[], &[]);
    let second_pair = equality_relation_pair(&equality(2, 0, 3, 0), &[], &[]);
    assert_ne!(first_pair, second_pair);
    let selectivity = correlate_join_condition_selectivities([
        (first_pair, 1.0 / 200_000.0),
        (first_pair, 1.0 / 10_000.0),
        (second_pair, 1.0 / 25.0),
    ]);
    assert_eq!(selectivity, 1.0 / 200_000.0 / 25.0);
}

#[test]
fn physical_references_recover_their_relation_pair_from_join_inputs() {
    let condition = JoinCondition::new(
        Expression::Reference(
            paro_planner::expression::ReferenceExpression::new(1, LogicalType::BigInt).into(),
        ),
        Expression::Reference(
            paro_planner::expression::ReferenceExpression::new(0, LogicalType::BigInt).into(),
        ),
        JoinComparisonType::Equal,
    );
    assert_eq!(
        equality_relation_pair(
            &condition,
            &[ColumnBinding::new(4, 0), ColumnBinding::new(2, 1)],
            &[ColumnBinding::new(7, 3)]
        ),
        Some((2, 7))
    );
}
