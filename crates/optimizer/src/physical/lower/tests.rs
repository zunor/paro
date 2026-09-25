// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_catalog::entry::{ColumnDefinition, EdgeTableInfo, TableCatalogEntry, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::window::WindowFunction;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    AggregateExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
    ConjunctionType, ConstantExpression, Expression, OperatorExpression, OperatorType,
    OrderByExpression, ReferenceExpression, WindowExpression, WindowFrame,
};
use paro_planner::logical::operator::aggregate::GroupDependency;
use paro_planner::logical::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::logical::operator::{
    Aggregate, ExplainSpec, ExpressionGet, Filter, Get, GraphExpand, GraphScan, Limit,
    LogicalOperator, Order, Projection, SetOperation, Window as LogicalWindow,
};
use paro_planner::logical::plan::OwnedLogicalPlan;
use paro_storage::index::PredicateTree;
use paro_storage::search::{
    CapabilityToken, FullTextIntent, FullTextQueryKind, FullTextQueryStats, FullTextScoreMode,
    NormalizedSearchRequest, ProjectionSpec, SearchCapabilityState, SearchIntent,
    SearchRequestMode,
};
use paro_storage::statistics::{NumericStats, StringStats};

use super::*;
use crate::physical::specs::GroupKeyEncoding;

#[path = "tests/aggregate_singleton.rs"]
mod aggregate_singleton;
#[path = "tests/order_window_lowering.rs"]
mod order_window_lowering;
#[path = "tests/window_arguments.rs"]
mod window_arguments;

#[test]
fn query_extraction_rejects_a_node_without_a_winner_contract() {
    let ctx = BindContext::new();
    let logical = OwnedLogicalPlan::new(&ctx, LogicalOperator::DummyScan);

    let error = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .requiring_implementation_contracts()
        .build(logical)
        .expect_err("query extraction must never invent an implicit physical implementation");

    assert!(error.to_string().contains("no verified winner contract"));
}

#[test]
fn physical_rewrite_composes_consecutive_projects() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".into(), "b".into(), "c".into()],
            vec![LogicalType::Integer; 3],
        )),
    );
    let inner = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            1,
            values,
            vec![
                Expression::Reference(ReferenceExpression::new(2, LogicalType::Integer).into()),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            ],
        )),
    );
    let outer = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            2,
            inner,
            vec![
                Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer).into()),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            ],
        )),
    );

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(outer)
        .unwrap();
    let PhysicalNodeKind::Project(project) = &plan.node(plan.root).kind else {
        panic!("expected project root");
    };
    let [child] = plan.node(plan.root).children.as_slice(&plan.children) else {
        panic!("expected unary project");
    };

    assert!(matches!(
        plan.node(*child).kind,
        PhysicalNodeKind::Values(_)
    ));
    assert!(matches!(
        &project.expressions[0],
        Expression::Reference(reference) if reference.index == 0
    ));
    assert!(matches!(
        &project.expressions[1],
        Expression::Reference(reference) if reference.index == 2
    ));
    assert_eq!(
        plan.nodes.len(),
        2,
        "folded projects must leave no arena orphans"
    );
}

#[test]
fn project_alias_does_not_rename_its_scan_input() {
    let ctx = BindContext::new();
    let get = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
    let project = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                1,
                get,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::Integer).into(),
                )],
            )
            .with_visible_names(vec!["renamed".to_string()]),
        ),
    );

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(project)
        .unwrap();
    let explain = plan.format_explain_text_with_spec(&ExplainSpec::default());

    assert!(explain.contains("Output: renamed"), "{explain}");
    assert!(explain.contains("Columns: a, b, c"), "{explain}");
    assert!(!explain.contains("Columns: renamed"), "{explain}");
}

#[test]
fn explain_size_is_bounded_for_deep_project_filter_chains() {
    std::thread::Builder::new()
        .name("deep-explain-plan".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let ctx = BindContext::new();
            let mut plan = OwnedLogicalPlan::new(
                &ctx,
                LogicalOperator::ExpressionGet(ExpressionGet::new(
                    0,
                    vec![],
                    vec!["flag".to_string()],
                    vec![LogicalType::Boolean],
                )),
            );
            for index in 0..12 {
                plan = OwnedLogicalPlan::new(
                    &ctx,
                    LogicalOperator::Projection(
                        Projection::new(
                            index * 2 + 1,
                            plan,
                            vec![Expression::Operator(
                                OperatorExpression::new_unary(
                                    OperatorType::Not,
                                    Expression::Reference(
                                        ReferenceExpression::new(0, LogicalType::Boolean).into(),
                                    ),
                                    LogicalType::Boolean,
                                )
                                .into(),
                            )],
                        )
                        .with_visible_names(Vec::new()),
                    ),
                );
                plan = OwnedLogicalPlan::new(
                    &ctx,
                    LogicalOperator::Filter(Filter::new(
                        plan,
                        vec![Expression::Reference(
                            ReferenceExpression::new(0, LogicalType::Boolean).into(),
                        )],
                    )),
                );
            }

            let physical = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
                .build(plan)
                .unwrap();
            let explain = physical.format_explain_text_with_spec(&ExplainSpec::default());

            assert!(
                explain.len() < 64 * 1024,
                "EXPLAIN grew to {} bytes",
                explain.len()
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn explain_parenthesizes_mixed_boolean_conjunctions() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["flag_a".into(), "flag_b".into(), "flag_c".into()],
            vec![LogicalType::Boolean; 3],
        )),
    );
    let disjunction = Expression::Conjunction(
        ConjunctionExpression {
            conjunction_type: ConjunctionType::Or,
            children: vec![
                ref_expr(0, LogicalType::Boolean),
                ref_expr(1, LogicalType::Boolean),
            ],
        }
        .into(),
    );
    let filter = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Filter(Filter::new(
            values,
            vec![Expression::Conjunction(
                ConjunctionExpression {
                    conjunction_type: ConjunctionType::And,
                    children: vec![disjunction, ref_expr(2, LogicalType::Boolean)],
                }
                .into(),
            )],
        )),
    );

    let physical = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(filter)
        .unwrap();
    let explain = physical.format_explain_text_with_spec(&ExplainSpec::default());

    assert!(
        explain.contains("Filter: (flag_a OR flag_b) AND flag_c"),
        "{explain}"
    );
}

#[test]
fn physical_rewrite_preserves_computed_expression_multiplicity() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".into()],
            vec![LogicalType::Integer],
        )),
    );
    let computed = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(7), LogicalType::Integer).into(),
            ),
        )
        .into(),
    );
    let inner = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(1, values, vec![computed])),
    );
    let outer = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(Projection::new(
            2,
            inner,
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean).into()),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean).into()),
            ],
        )),
    );

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(outer)
        .unwrap();
    let [child] = plan.node(plan.root).children.as_slice(&plan.children) else {
        panic!("expected unary project");
    };

    assert!(matches!(
        plan.node(*child).kind,
        PhysicalNodeKind::Project(_)
    ));
}

#[test]
fn arena_extractor_builds_streaming_subset_without_runtime_objects() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let filter = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(Filter::new(values, vec![])));
    let project_expr =
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into());
    let project = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(1, filter, vec![project_expr]).with_visible_names(vec!["a".into()]),
        ),
    );
    let limit = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Limit(Box::new(Limit::new(project, None, None))),
    );

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor.build(limit).expect("subset should lower");

    assert_eq!(plan.nodes.len(), 4);
    assert!(matches!(
        plan.node(plan.root).kind,
        PhysicalNodeKind::Limit(_)
    ));
    assert!(crate::physical::PhysicalPlanVerifier::verify(&plan).is_ok());
    assert!(plan.format_tree().contains("LIMIT"));
}

#[test]
fn arena_extractor_lowers_distinct_to_hash_aggregate() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["a".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let distinct = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Distinct(paro_planner::logical::operator::Distinct::new(values)),
    );

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor
        .build(distinct)
        .expect("DISTINCT should lower to typed aggregate");

    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("DISTINCT should lower as aggregate");
    };
    assert_eq!(spec.grouping_key_count, 1);
    assert!(spec.aggregates.is_empty());
    assert_eq!(spec.output_names.as_ref(), ["a"]);
    assert_eq!(plan.child_ids(&plan.node(plan.root).children).len(), 1);
    assert!(crate::physical::PhysicalPlanVerifier::verify(&plan).is_ok());
    let explain = plan.format_explain_text_with_spec(&ExplainSpec::default());
    assert!(explain.contains("Group Key: a"), "{explain}");
    assert!(!explain.contains("Group Key: #"), "{explain}");
}

#[test]
fn aggregate_uses_lossless_fixed_width_keys_for_bounded_strings() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["brand".to_string()],
            vec![LogicalType::Varchar],
        )),
    );
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
    );
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![ref_expr(0, LogicalType::Varchar)],
        vec![],
        vec![count],
        vec![],
    );
    let mut stats = StringStats::create_empty(LogicalType::Varchar);
    StringStats::update(&mut stats, "Brand45");
    aggregate.group_stats[0] = Some(stats);
    let aggregate = OwnedLogicalPlan::new(&ctx, LogicalOperator::Aggregate(Box::new(aggregate)));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor.build(aggregate).expect("aggregate should lower");
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("expected aggregate root");
    };
    assert_eq!(
        spec.group_key_encodings.as_ref(),
        [GroupKeyEncoding::PackedString {
            physical_type: LogicalType::UBigInt,
            max_length: 7,
        }]
    );
}

#[test]
fn aggregate_packs_inline_strings_when_fixed_keys_preserve_row_width() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["nation".to_string()],
            vec![LogicalType::Varchar],
        )),
    );
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
    );
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![ref_expr(0, LogicalType::Varchar)],
        vec![],
        vec![count],
        vec![],
    );
    let mut stats = StringStats::create_empty(LogicalType::Varchar);
    StringStats::update(&mut stats, "UNITED KINGDOM");
    aggregate.group_stats[0] = Some(stats);
    let aggregate = OwnedLogicalPlan::new(&ctx, LogicalOperator::Aggregate(Box::new(aggregate)));

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(aggregate)
        .expect("aggregate should lower");
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("expected aggregate root");
    };
    assert_eq!(
        spec.group_key_encodings.as_ref(),
        [GroupKeyEncoding::PackedString {
            physical_type: LogicalType::UHugeInt,
            max_length: 14,
        }]
    );
}

#[test]
fn aggregate_skips_offset_keys_that_only_replace_row_padding() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["size".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
    );
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![ref_expr(0, LogicalType::Integer)],
        vec![],
        vec![count],
        vec![],
    );
    let mut stats = paro_storage::statistics::NumericStats::create_empty(LogicalType::Integer);
    paro_storage::statistics::NumericStats::set_guaranteed_min(
        &mut stats,
        &paro_common::runtime_value::Value::Integer(-5),
    );
    paro_storage::statistics::NumericStats::set_guaranteed_max(
        &mut stats,
        &paro_common::runtime_value::Value::Integer(250),
    );
    aggregate.group_stats[0] = Some(stats);
    let aggregate = OwnedLogicalPlan::new(&ctx, LogicalOperator::Aggregate(Box::new(aggregate)));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor.build(aggregate).expect("aggregate should lower");
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("expected aggregate root");
    };
    assert_eq!(
        spec.group_key_encodings.as_ref(),
        [GroupKeyEncoding::Identity]
    );
}

#[test]
fn aggregate_requires_complete_bounds_for_offset_keys() {
    fn lower_with_stats(
        first_stats: paro_storage::statistics::BaseStatistics,
        second_stats: paro_storage::statistics::BaseStatistics,
    ) -> Box<[GroupKeyEncoding]> {
        let ctx = BindContext::new();
        let values = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![],
                vec!["first".to_string(), "second".to_string()],
                vec![LogicalType::BigInt, LogicalType::BigInt],
            )),
        );
        let count = Expression::Aggregate(
            AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
        );
        let mut aggregate = Aggregate::new(
            1,
            2,
            3,
            values,
            vec![
                ref_expr(0, LogicalType::BigInt),
                ref_expr(1, LogicalType::BigInt),
            ],
            vec![],
            vec![count],
            vec![],
        );
        aggregate.group_stats = vec![Some(first_stats), Some(second_stats)];
        let aggregate =
            OwnedLogicalPlan::new(&ctx, LogicalOperator::Aggregate(Box::new(aggregate)));

        let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
        let plan = extractor.build(aggregate).expect("aggregate should lower");
        let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
            panic!("expected aggregate root");
        };
        spec.group_key_encodings.clone()
    }

    let mut known = NumericStats::create_empty(LogicalType::BigInt);
    NumericStats::update_i64(&mut known, 10);
    NumericStats::update_i64(&mut known, 20);
    assert_eq!(
        lower_with_stats(known.copy(), known.copy()).as_ref(),
        [
            GroupKeyEncoding::OffsetInteger {
                physical_type: LogicalType::UTinyInt,
                minimum: 10,
            },
            GroupKeyEncoding::OffsetInteger {
                physical_type: LogicalType::UTinyInt,
                minimum: 10,
            },
        ]
    );

    let mut incomplete = known.copy();
    incomplete.merge(&NumericStats::create_unknown(LogicalType::BigInt));
    assert_eq!(
        lower_with_stats(incomplete, known).as_ref(),
        [GroupKeyEncoding::Identity, GroupKeyEncoding::Identity]
    );
}

#[test]
fn aggregate_materializes_proven_dependent_groups_as_states() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["key".to_string(), "name".to_string(), "comment".to_string()],
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
            ],
        )),
    );
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
    );
    let mut aggregate = Aggregate::new(
        1,
        2,
        3,
        values,
        vec![
            ref_expr(0, LogicalType::Varchar),
            ref_expr(1, LogicalType::Varchar),
            ref_expr(2, LogicalType::Varchar),
        ],
        vec![],
        vec![count],
        vec![],
    );
    aggregate.group_dependencies.push(GroupDependency {
        determinants: Box::new([0]),
        dependents: Box::new([1, 2]),
    });
    // Retained keys and dependent-state output are independent domains. A
    // proven compact key must survive removal of other grouping columns.
    let mut key_stats = StringStats::create_empty(LogicalType::Varchar);
    StringStats::update(&mut key_stats, "key");
    aggregate.group_stats[0] = Some(key_stats);
    let aggregate = OwnedLogicalPlan::new(&ctx, LogicalOperator::Aggregate(Box::new(aggregate)));

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(aggregate)
        .expect("aggregate should lower");
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("expected aggregate root");
    };

    assert_eq!(spec.grouping_key_count, 1);
    assert_eq!(spec.groups.len(), 1);
    assert_eq!(spec.aggregates.len(), 3);
    assert_eq!(spec.state_output_projection.as_ref(), [0, 2, 3, 1]);
    assert_eq!(
        spec.group_key_encodings.as_ref(),
        [GroupKeyEncoding::PackedString {
            physical_type: LogicalType::UInteger,
            max_length: 3,
        }]
    );
    assert_eq!(
        spec.output_types.as_ref(),
        [
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::Varchar,
            LogicalType::BigInt,
        ]
    );
    assert!(spec.perfect_hash.is_none());
}

#[test]
fn arena_extractor_fuses_aggregate_only_having_into_aggregate_emit() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["key".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
    );
    let aggregate = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(Box::new(Aggregate::new(
            1,
            2,
            3,
            values,
            vec![ref_expr(0, LogicalType::Integer)],
            vec![],
            vec![count],
            vec![],
        ))),
    );
    let having = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Filter(Filter::new(
            aggregate,
            vec![comparison(
                ComparisonType::GreaterThan,
                ref_expr(1, LogicalType::BigInt),
                Expression::Constant(
                    ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt).into(),
                ),
            )],
        )),
    );

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor.build(having).expect("HAVING should lower");

    let PhysicalNodeKind::Aggregate(spec) = &plan.node(plan.root).kind else {
        panic!("aggregate-only HAVING should be fused into aggregate emit");
    };
    assert_eq!(spec.having_filter.len(), 1);
    let Expression::Comparison(predicate) = &spec.having_filter[0] else {
        panic!("expected rebased HAVING comparison");
    };
    let Expression::Reference(reference) = predicate.left.as_ref() else {
        panic!("expected aggregate reference in HAVING");
    };
    assert_eq!(reference.index, 0);
    let explain: serde_json::Value =
        serde_json::from_str(&plan.format_explain_json(ExplainSpec::default())).unwrap();
    assert_eq!(
        explain["plan"]["properties"]["Having"], "count_star(*) > 1",
        "HAVING must name the aggregate result, not grouping column zero"
    );
}

#[test]
fn aggregate_having_fusion_preserves_an_independent_output_projection() {
    let ctx = BindContext::new();
    let values = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["key".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let count = Expression::Aggregate(
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt).into(),
    );
    let aggregate = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Aggregate(Box::new(Aggregate::new(
            1,
            2,
            3,
            values,
            vec![ref_expr(0, LogicalType::Integer)],
            vec![],
            vec![count],
            vec![],
        ))),
    );
    let mut filter = Filter::new(
        aggregate,
        vec![comparison(
            ComparisonType::GreaterThan,
            ref_expr(1, LogicalType::BigInt),
            Expression::Constant(
                ConstantExpression::new(Value::BigInt(10), LogicalType::BigInt).into(),
            ),
        )],
    );
    // COUNT is required by HAVING but not by the parent plan.
    filter.projection_map = vec![0].into();
    let having = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor.build(having).expect("HAVING should lower");

    let PhysicalNodeKind::Project(project) = &plan.node(plan.root).kind else {
        panic!("projected HAVING should retain an explicit output projection");
    };
    assert_eq!(project.expressions.len(), 1);
    assert!(matches!(
        &project.expressions[0],
        Expression::Reference(reference) if reference.index == 0
    ));
    let [aggregate_id] = plan.child_ids(&plan.node(plan.root).children) else {
        panic!("HAVING projection should have one aggregate child");
    };
    let PhysicalNodeKind::Aggregate(spec) = &plan.node(*aggregate_id).kind else {
        panic!("HAVING predicate should remain attached to the aggregate");
    };
    assert_eq!(spec.having_filter.len(), 1);
    assert!(plan
        .nodes
        .iter()
        .all(|node| !matches!(node.kind, PhysicalNodeKind::Filter(_))));
}

#[test]
fn arena_extractor_pushes_filter_predicates_into_rowset_scan() {
    let ctx = BindContext::new();
    let get = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
    let mut filter = Filter::new(
        get,
        vec![
            comparison(
                ComparisonType::GreaterThanOrEqual,
                ref_expr(0, LogicalType::Integer),
                int_const(10),
            ),
            Expression::Operator(
                OperatorExpression::new(
                    OperatorType::In,
                    vec![
                        ref_expr(1, LogicalType::Integer),
                        int_const(3),
                        int_const(7),
                    ],
                    LogicalType::Boolean,
                )
                .into(),
            ),
            Expression::Operator(
                OperatorExpression::new_unary(
                    OperatorType::IsNull,
                    ref_expr(2, LogicalType::Varchar),
                    LogicalType::Boolean,
                )
                .into(),
            ),
        ],
    );
    filter.projection_map = vec![0].into();
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let physical = extractor.build(plan).expect("filter should lower");

    let PhysicalNodeKind::RowsetScan(spec) = &physical.node(physical.root).kind else {
        panic!("fully pushed filter should lower to rowset scan root");
    };
    assert_eq!(spec.column_projection.columns(), [0].as_slice());
    assert!(spec.residual_predicates.is_empty());
    assert!(!spec.planned_materialization().is_late());
    let Some(PredicateTree::And(children)) = spec.predicate.as_ref() else {
        panic!("expected conjunctive storage predicate");
    };
    assert_eq!(children.len(), 3);
    let explain = physical.format_explain_text_with_spec(&ExplainSpec::default());
    assert!(
        explain.contains("Pushed Predicate: a >= 10 AND b IN (3, 7) AND c IS NULL"),
        "{explain}"
    );
    assert!(!explain.contains("col#"), "{explain}");
}

#[test]
fn zero_column_rowset_projection_never_enables_late_materialization() {
    let ctx = BindContext::new();
    let get = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
    let mut filter = Filter::new(
        get,
        vec![comparison(
            ComparisonType::Equal,
            ref_expr(0, LogicalType::Integer),
            int_const(42),
        )],
    );
    filter.projection_map = paro_planner::logical::operator::ProjectionMap::none();
    let mut plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));
    plan.stats.estimated_cardinality =
        Some(paro_planner::logical::plan::CardinalityEstimate::exact(1));

    let physical = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(plan)
        .expect("zero-column filter should lower");
    let PhysicalNodeKind::RowsetScan(spec) = &physical.node(physical.root).kind else {
        panic!("fully pushed zero-column filter should lower to rowset scan");
    };

    assert!(spec.column_projection.columns().is_empty());
    assert!(!spec.planned_materialization().is_late());
}

#[test]
fn rowset_scan_materialization_policy_uses_estimated_filter_density() {
    let build_scan = |filtered_rows: u64| {
        let ctx = BindContext::new();
        let mut get = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
        get.stats.estimated_cardinality = Some(
            paro_planner::logical::plan::CardinalityEstimate::exact(1_000_000),
        );
        let filter = Filter::new(
            get,
            vec![comparison(
                ComparisonType::LessThanOrEqual,
                ref_expr(0, LogicalType::Integer),
                int_const(42),
            )],
        );
        let mut plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));
        plan.stats.estimated_cardinality = Some(
            paro_planner::logical::plan::CardinalityEstimate::exact(filtered_rows),
        );
        let physical = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
            .build(plan)
            .expect("filter should lower");
        let PhysicalNodeKind::RowsetScan(spec) = &physical.node(physical.root).kind else {
            panic!("fully pushed filter should lower to rowset scan");
        };
        spec.planned_materialization().is_late()
    };

    assert!(!build_scan(990_000));
    assert!(build_scan(10_000));
}

#[test]
fn arena_extractor_can_disable_rowset_scan_pushdown() {
    let ctx = BindContext::new();
    let get = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
    let filter = Filter::new(
        get,
        vec![comparison(
            ComparisonType::Equal,
            ref_expr(0, LogicalType::Integer),
            int_const(42),
        )],
    );
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext {
        rowset_scan_pushdown: false,
        ..PhysicalBuildContext::default()
    });
    let physical = extractor.build(plan).expect("filter should lower");

    let PhysicalNodeKind::Filter(_) = &physical.node(physical.root).kind else {
        panic!("disabled pushdown should keep a filter root");
    };
    let [child] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("filter should have one rowset child");
    };
    let PhysicalNodeKind::RowsetScan(scan) = &physical.node(*child).kind else {
        panic!("filter child should be rowset scan");
    };
    assert!(scan.predicate.is_none());
    assert!(!scan.planned_materialization().is_late());
}

#[test]
fn arena_extractor_keeps_residual_filter_above_pushed_rowset_scan() {
    let ctx = BindContext::new();
    let get = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
    let filter = Filter::new(
        get,
        vec![Expression::Conjunction(
            ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: vec![
                    comparison(
                        ComparisonType::Equal,
                        ref_expr(0, LogicalType::Integer),
                        int_const(42),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
                    ),
                ],
            }
            .into(),
        )],
    );
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Filter(filter));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let physical = extractor.build(plan).expect("filter should lower");

    let PhysicalNodeKind::Filter(spec) = &physical.node(physical.root).kind else {
        panic!("residual expression should keep a filter root");
    };
    assert_eq!(spec.expressions.len(), 1);
    let [child] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("filter should have one rowset child");
    };
    let PhysicalNodeKind::RowsetScan(scan) = &physical.node(*child).kind else {
        panic!("filter child should be rowset scan");
    };
    assert!(scan.predicate.is_some());
    assert_eq!(scan.residual_predicates.len(), 1);
}

#[test]
fn arena_extractor_pushes_get_runtime_filters_into_rowset_scan() {
    let ctx = BindContext::new();
    let mut get = test_get();
    get.runtime_filter_expressions.push(comparison(
        ComparisonType::LessThanOrEqual,
        ref_expr(0, LogicalType::Integer),
        int_const(99),
    ));
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(get)));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let physical = extractor.build(plan).expect("get should lower");

    let PhysicalNodeKind::RowsetScan(spec) = &physical.node(physical.root).kind else {
        panic!("expected rowset scan");
    };
    assert!(spec.predicate.is_some());
    assert_eq!(spec.runtime_filter_expressions.len(), 1);
}

#[test]
fn arena_extractor_hands_graph_expand_filters_to_graph_project() {
    let ctx = BindContext::new();
    let scan = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "v".to_string(),
                property_column_ids: vec![],
            },
            None,
            0,
            3,
            "v".to_string(),
            "g".to_string(),
            "public".to_string(),
        ))),
    );
    let mut expand = GraphExpand::new(
        EdgeTableInfo {
            table_name: "edges".to_string(),
            table_oid: 2,
            key_column_ids: vec![0],
            source_key_column_ids: vec![0],
            source_vertex_table: "vertices".to_string(),
            source_ref_column_ids: vec![1],
            destination_key_column_ids: vec![0],
            destination_vertex_table: "vertices".to_string(),
            destination_ref_column_ids: vec![2],
            label: "e".to_string(),
            property_column_ids: vec![],
        },
        paro_planner::logical::operator::graph_expand::ExpandDirection::Forward,
        "v".to_string(),
        0,
        1,
        2,
        3,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        scan,
    );
    expand.edge_filter = Some(Expression::Constant(
        ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
    ));
    expand.target_filter = Some(Expression::Constant(
        ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
    ));
    let expand = OwnedLogicalPlan::new(&ctx, LogicalOperator::GraphExpand(Box::new(expand)));
    let project = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Projection(
            Projection::new(
                3,
                expand,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::UBigInt).into(),
                )],
            )
            .with_visible_names(vec!["src".to_string()]),
        ),
    );

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor
        .build(project)
        .expect("graph project should own graph expand filters");

    let PhysicalNodeKind::GraphProject(project_spec) = &plan.node(plan.root).kind else {
        panic!("expected graph project root");
    };
    assert_eq!(project_spec.filters.len(), 2);
    let [expand_id] = plan.child_ids(&plan.node(plan.root).children) else {
        panic!("graph project should have graph expand child");
    };
    let PhysicalNodeKind::GraphExpand(expand_spec) = &plan.node(*expand_id).kind else {
        panic!("graph project child should be graph expand");
    };
    assert!(expand_spec.edge_filter.is_none());
    assert!(expand_spec.target_filter.is_none());
    assert!(!expand_spec.has_path_functions);
    assert_eq!(expand_spec.output_types.len(), 5);
    assert!(crate::physical::PhysicalPlanVerifier::verify(&plan).is_ok());
}

#[test]
fn arena_extractor_lowers_graph_path_functions_with_path_history() {
    let ctx = BindContext::new();
    let scan = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "vertices".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "v".to_string(),
                property_column_ids: vec![],
            },
            None,
            0,
            3,
            "v".to_string(),
            "g".to_string(),
            "public".to_string(),
        ))),
    );
    let mut expand = GraphExpand::new(
        EdgeTableInfo {
            table_name: "edges".to_string(),
            table_oid: 2,
            key_column_ids: vec![0],
            source_key_column_ids: vec![0],
            source_vertex_table: "vertices".to_string(),
            source_ref_column_ids: vec![1],
            destination_key_column_ids: vec![0],
            destination_vertex_table: "vertices".to_string(),
            destination_ref_column_ids: vec![2],
            label: "e".to_string(),
            property_column_ids: vec![],
        },
        paro_planner::logical::operator::graph_expand::ExpandDirection::Forward,
        "v".to_string(),
        0,
        1,
        2,
        3,
        "v".to_string(),
        1,
        1,
        "vertices".to_string(),
        scan,
    );
    expand.has_path_functions = true;
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::GraphExpand(Box::new(expand)));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let physical = extractor
        .build(plan)
        .expect("path functions should lower with path history enabled");

    let PhysicalNodeKind::GraphExpand(spec) = &physical.node(physical.root).kind else {
        panic!(
            "expected GRAPH_EXPAND root, got {:?}",
            physical.node(physical.root).kind
        );
    };
    assert!(spec.has_path_functions);
}

#[test]
fn arena_extractor_lowers_single_join_to_typed_hash_path() {
    let ctx = BindContext::new();
    let left = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["l".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["r".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
    );
    let join = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Single,
            left,
            right,
            vec![condition],
        )),
    );
    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let plan = extractor
        .build(join)
        .expect("single join should lower to typed hash join");

    let PhysicalNodeKind::HashJoin(spec) = &plan.node(plan.root).kind else {
        panic!("single join should enter typed hash join after scalar semantics coverage");
    };
    assert_eq!(spec.join_type, JoinType::Single);
    assert_eq!(plan.child_ids(&plan.node(plan.root).children).len(), 2);
    assert!(crate::physical::PhysicalPlanVerifier::verify(&plan).is_ok());
    let explain = plan.format_explain_text_with_spec(&ExplainSpec::default());
    assert!(explain.contains("Join Condition: l = r"), "{explain}");
    assert!(!explain.contains("Join Condition: #"), "{explain}");
}

#[test]
fn auxiliary_runtime_filter_winner_emits_owned_physical_edge() {
    let ctx = BindContext::new();
    let left_get = test_get();
    let mut right_get = test_get();
    right_get.table_index = 1;
    let left = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(left_get)));
    let right = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(right_get)));
    let condition = JoinCondition::equality(
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                paro_planner::logical::operator::ColumnBinding::new(0, 0),
                LogicalType::Integer,
            )
            .into(),
        ),
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                paro_planner::logical::operator::ColumnBinding::new(1, 0),
                LogicalType::Integer,
            )
            .into(),
        ),
    );
    let mut join = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![condition],
        )),
    );
    crate::physical::slot_assignment::assign_expression_slots(&mut join.operator)
        .expect("runtime-filter extraction requires the selected positional ABI");
    let artifact = crate::physical::identity::Fingerprint(77);
    let contract = crate::physical::ImplementationContract {
        required: crate::physical::RequiredProperties::default(),
        provided: crate::physical::ProvidedProperties {
            ordering: crate::physical::requirements::ProvidedOrdering::Unordered,
            partitioning: crate::physical::requirements::ProvidedPartitioning::Singleton,
            materialization: crate::physical::requirements::ProvidedMaterialization::default(),
            mutation_safety: crate::physical::requirements::ProvidedMutationSafety::NotApplicable,
            representation: crate::physical::requirements::ProvidedRepresentation::Flat,
            replayability: crate::physical::requirements::ProvidedReplayability::OnePass,
            result_guarantee: crate::physical::requirements::ResultGuarantee::Exact,
        },
        cost: crate::physical::PhysicalCost::ZERO,
        grant: crate::physical::PhysicalGrantContract::Invariant,
        origin: crate::physical::PlanOrigin::SpecializedRegion(
            crate::physical::identity::Fingerprint(88),
        ),
        goal_fingerprint: artifact,
        physical_fingerprint: artifact,
        implementation: crate::physical::PhysicalImplementationFlavor::HashJoinRuntimeFilter,
        region_owner: Some(crate::physical::identity::Fingerprint(88)),
        owned_artifacts: vec![crate::physical::OwnedAuxiliaryArtifact {
            fingerprint: artifact,
            kind: crate::physical::AuxiliaryArtifactKind::RuntimeFilter,
        }]
        .into_boxed_slice(),
    };
    let mut contracts = HashMap::new();
    contracts.insert(join.id, contract);

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .with_implementation_contracts(Arc::new(contracts))
        .build(join)
        .expect("auxiliary runtime-filter winner should lower");
    let PhysicalNodeKind::HashJoin(spec) = &plan.node(plan.root).kind else {
        panic!("expected hash join root");
    };
    assert_eq!(spec.runtime_filter.as_ref().unwrap().artifact, artifact);
    let [probe, build] = plan.child_ids(&plan.node(plan.root).children) else {
        panic!("expected two hash join children");
    };
    let edge = plan.edges.iter().next().expect("runtime-filter edge");
    assert_eq!(edge.producer, *build);
    assert_eq!(edge.consumer, *probe);
    assert_eq!(
        edge.kind,
        crate::physical::PhysicalEdgeKind::RuntimeFilter(artifact)
    );
    assert_eq!(
        plan.properties
            .get(*probe)
            .unwrap()
            .auxiliary_dependencies
            .as_ref(),
        &[edge.id.0]
    );
}

#[test]
fn build_left_runtime_filter_keeps_artifact_ownership_on_the_hash_join() {
    let ctx = BindContext::new();
    let left = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(test_get())));
    let mut right_get = test_get();
    right_get.table_index = 1;
    let right = OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(right_get)));
    let condition = JoinCondition::equality(
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                paro_planner::logical::operator::ColumnBinding::new(0, 0),
                LogicalType::Integer,
            )
            .into(),
        ),
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                paro_planner::logical::operator::ColumnBinding::new(1, 0),
                LogicalType::Integer,
            )
            .into(),
        ),
    );
    let mut join = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![condition],
        )),
    );
    crate::physical::slot_assignment::assign_expression_slots(&mut join.operator)
        .expect("runtime-filter extraction requires the selected positional ABI");
    let artifact = crate::physical::identity::Fingerprint(177);
    let owner = crate::physical::identity::Fingerprint(188);
    let contract = crate::physical::ImplementationContract {
        required: crate::physical::RequiredProperties::default(),
        provided: crate::physical::ProvidedProperties {
            ordering: crate::physical::requirements::ProvidedOrdering::Unordered,
            partitioning: crate::physical::requirements::ProvidedPartitioning::Singleton,
            materialization: crate::physical::requirements::ProvidedMaterialization::default(),
            mutation_safety: crate::physical::requirements::ProvidedMutationSafety::NotApplicable,
            representation: crate::physical::requirements::ProvidedRepresentation::Flat,
            replayability: crate::physical::requirements::ProvidedReplayability::OnePass,
            result_guarantee: crate::physical::requirements::ResultGuarantee::Exact,
        },
        cost: crate::physical::PhysicalCost::ZERO,
        grant: crate::physical::PhysicalGrantContract::Invariant,
        origin: crate::physical::PlanOrigin::SpecializedRegion(owner),
        goal_fingerprint: artifact,
        physical_fingerprint: artifact,
        implementation:
            crate::physical::PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter,
        region_owner: Some(owner),
        owned_artifacts: vec![crate::physical::OwnedAuxiliaryArtifact {
            fingerprint: artifact,
            kind: crate::physical::AuxiliaryArtifactKind::RuntimeFilter,
        }]
        .into_boxed_slice(),
    };
    let mut contracts = HashMap::new();
    contracts.insert(join.id, contract);

    let plan = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .with_implementation_contracts(Arc::new(contracts))
        .build(join)
        .expect("build-left runtime-filter winner should lower");
    let PhysicalNodeKind::HashJoin(spec) = &plan.node(plan.root).kind else {
        panic!("build-left output layout should belong to the hash join");
    };
    assert_eq!(spec.output_permutation.len(), 6);
    assert_eq!(spec.output_permutation.destination_of(0), Some(3));
    assert_eq!(spec.output_permutation.destination_of(5), Some(2));
    assert_eq!(spec.output_permutation.natural_of(0), Some(3));
    assert!(plan
        .nodes
        .iter()
        .all(|node| node.label.display_name != "HASH_JOIN_OUTPUT_LAYOUT"));
    let owners = plan
        .properties
        .iter()
        .filter(|(_, properties)| {
            properties
                .owned_artifacts
                .iter()
                .any(|candidate| candidate.fingerprint == artifact)
        })
        .collect::<Vec<_>>();
    assert_eq!(owners.len(), 1);
    assert!(matches!(
        plan.node(owners[0].0).kind,
        PhysicalNodeKind::HashJoin(_)
    ));
    let region_owners = plan
        .properties
        .iter()
        .filter(|(_, properties)| properties.region_owner == Some(owner))
        .collect::<Vec<_>>();
    assert_eq!(region_owners.len(), 1);
    assert_eq!(region_owners[0].0, plan.root);
    crate::physical::PhysicalPlanVerifier::verify(&plan)
        .expect("the runtime-filter artifact should have exactly one physical owner");
}

#[test]
fn build_left_output_permutation_covers_every_reversible_join_type() {
    for (logical_join_type, physical_join_type) in [
        (JoinType::Inner, JoinType::Inner),
        (JoinType::Left, JoinType::Right),
        (JoinType::Right, JoinType::Left),
        (JoinType::Outer, JoinType::Outer),
    ] {
        let ctx = BindContext::new();
        let left = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![],
                vec!["left_key".to_string(), "left_payload".to_string()],
                vec![LogicalType::Integer, LogicalType::Varchar],
            )),
        );
        let right = OwnedLogicalPlan::new(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![],
                vec![
                    "right_key".to_string(),
                    "right_flag".to_string(),
                    "right_payload".to_string(),
                ],
                vec![
                    LogicalType::Integer,
                    LogicalType::Boolean,
                    LogicalType::BigInt,
                ],
            )),
        );
        let Join::Comparison(join) = Join::comparison(
            logical_join_type,
            left,
            right,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            )],
        ) else {
            unreachable!()
        };
        let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
        let join = join
            .try_map_child_links(&mut |child| PreparedNode::from_owned(*child))
            .unwrap();
        let (kind, children) = extractor
            .lower_comparison_hash_join_build_left(&join)
            .expect("every reversible join should support build-left lowering");
        let PhysicalNodeKind::HashJoin(spec) = kind else {
            panic!("build-left lowering should produce one hash join");
        };

        assert_eq!(children.len(), 2);
        assert_eq!(spec.join_type, physical_join_type);
        assert_eq!(spec.output_names[0], "left_key");
        assert_eq!(spec.output_names[2], "right_key");
        assert_eq!(
            spec.output_types.as_ref(),
            [
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Boolean,
                LogicalType::BigInt,
            ]
        );
        assert_eq!(spec.output_permutation.destination_of(0), Some(2));
        assert_eq!(spec.output_permutation.destination_of(2), Some(4));
        assert_eq!(spec.output_permutation.destination_of(3), Some(0));
        assert_eq!(spec.output_permutation.destination_of(4), Some(1));
        assert_eq!(spec.output_permutation.natural_of(0), Some(3));
        assert_eq!(spec.output_permutation.natural_of(4), Some(2));
    }
}

#[test]
fn join_qualifiers_survive_wrapped_scans() {
    let ctx = BindContext::new();
    let mut left_get = test_get();
    left_get.table_index = 0;
    left_get.relation_alias = Some("l".to_string());
    let mut right_get = test_get();
    right_get.table_index = 1;
    right_get.relation_alias = Some("r".to_string());
    let left = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(left_get))),
            vec![comparison(
                ComparisonType::GreaterThan,
                ref_expr(0, LogicalType::Integer),
                int_const(0),
            )],
        )),
    );
    let right = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::new(&ctx, LogicalOperator::Get(Box::new(right_get))),
            vec![comparison(
                ComparisonType::GreaterThan,
                ref_expr(0, LogicalType::Integer),
                int_const(0),
            )],
        )),
    );
    let join = OwnedLogicalPlan::new(
        &ctx,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::equality(
                ref_expr(0, LogicalType::Integer),
                ref_expr(0, LogicalType::Integer),
            )],
        )),
    );
    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext {
        rowset_scan_pushdown: false,
        ..PhysicalBuildContext::default()
    });
    let physical = extractor.build(join).unwrap();
    let explain = physical.format_explain_text_with_spec(&ExplainSpec::default());

    assert!(explain.contains("Join Condition: l.a = r.a"), "{explain}");
}

#[test]
fn arena_extractor_lowers_search_scan_with_planned_token() {
    let ctx = BindContext::new();
    let get = test_get();
    let intent = SearchIntent::FullText(FullTextIntent {
        column_id: 2,
        query: "graph".to_string(),
        query_kind: FullTextQueryKind::Legacy,
        query_stats: FullTextQueryStats::new(1),
        config: "simple".to_string(),
        score_mode: FullTextScoreMode::DocumentRankV1,
    });
    let token = CapabilityToken {
        definition_id: 42,
        generation_id: 7,
        root_version: 11,
        capability_state: SearchCapabilityState::Queryable,
    };
    let score_expr = document_rank_expression();
    let search = LogicalSearchScan::new(
        get,
        NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::TopK { limit: 5 },
            predicate: None,
            projections: ProjectionSpec {
                columns: vec![2],
                include_score: true,
            },
            intents: vec![intent.clone()],
            fusion: None,
        },
        SearchDecision::IndexScan {
            candidate: SearchCandidate {
                intent,
                token: token.clone(),
                kind: paro_storage::search::SearchIndexKind::FullText,
                estimated_cost: None,
                exact_filter_materialization: None,
            },
            confidence: paro_planner::logical::operator::Confidence::High,
        },
        vec![ref_expr(2, LogicalType::Varchar), score_expr.clone()],
        9,
        Vec::new(),
        Vec::new(),
        Some(1),
        score_expr,
        false,
        5,
    )
    .with_output_names(vec!["c".to_string(), "score".to_string()]);
    let mut wrong =
        OwnedLogicalPlan::new(&ctx, LogicalOperator::SearchScan(Box::new(search.clone())));
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::SearchScan(Box::new(search)));
    let LogicalOperator::SearchScan(scan) = &mut wrong.operator else {
        unreachable!()
    };
    let SearchDecision::IndexScan { candidate, .. } = &mut scan.decision else {
        unreachable!()
    };
    let SearchIntent::FullText(intent) = &mut candidate.intent else {
        unreachable!()
    };
    intent.score_mode = FullTextScoreMode::CorpusBm25V1;
    assert!(PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(wrong)
        .unwrap_err()
        .to_string()
        .contains("logical scoring contract"));

    let mut extractor = PhysicalPlanBuilder::new(PhysicalBuildContext::default());
    let physical = extractor.build(plan).expect("search scan should lower");

    let PhysicalNodeKind::FullTextSearch(spec) = &physical.node(physical.root).kind else {
        panic!("search scan should lower to fulltext source");
    };
    assert_eq!(spec.capability_token, token);
    assert_eq!(spec.column_id, 2);
    assert_eq!(spec.projected_columns.as_ref(), [2]);
    assert!(spec.emit_score);
    assert_eq!(spec.output_names.as_ref(), ["c", "score"]);
    let identity = physical.structural_identity_fingerprint().unwrap();
    assert_eq!(
        identity,
        physical.clone().structural_identity_fingerprint().unwrap()
    );
    let changes: [fn(&mut crate::physical::specs::FullTextSearchSpec); 5] = [
        |s| s.query.push_str(" changed"),
        |s| s.score_mode = FullTextScoreMode::CorpusBm25V1,
        |s| s.capability_token.root_version += 1,
        |s| s.emit_score = false,
        |s| s.projected_columns = Box::new([1]),
    ];
    for change in changes {
        let mut changed = physical.clone();
        let PhysicalNodeKind::FullTextSearch(spec) =
            &mut changed.nodes.get_mut(changed.root).unwrap().kind
        else {
            unreachable!()
        };
        change(spec);
        assert_ne!(identity, changed.structural_identity_fingerprint().unwrap());
    }
}

#[test]
fn arena_extractor_projects_derived_values_from_the_canonical_search_score() {
    let ctx = BindContext::new();
    let get = test_get();
    let intent = SearchIntent::FullText(FullTextIntent {
        column_id: 2,
        query: "graph".to_string(),
        query_kind: FullTextQueryKind::Legacy,
        query_stats: FullTextQueryStats::new(1),
        config: "simple".to_string(),
        score_mode: FullTextScoreMode::DocumentRankV1,
    });
    let score_expr = document_rank_expression();
    let derived_score = Expression::Operator(
        OperatorExpression::new(
            OperatorType::Coalesce,
            vec![
                score_expr.clone(),
                Expression::Constant(
                    ConstantExpression::new(Value::Float(0.0), LogicalType::Float).into(),
                ),
            ],
            LogicalType::Float,
        )
        .into(),
    );
    let search = LogicalSearchScan::new(
        get,
        NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::TopK { limit: 5 },
            predicate: None,
            projections: ProjectionSpec {
                columns: vec![2],
                include_score: true,
            },
            intents: vec![intent.clone()],
            fusion: None,
        },
        SearchDecision::IndexScan {
            candidate: SearchCandidate {
                intent,
                token: CapabilityToken {
                    definition_id: 42,
                    generation_id: 7,
                    root_version: 11,
                    capability_state: SearchCapabilityState::Queryable,
                },
                kind: paro_storage::search::SearchIndexKind::FullText,
                estimated_cost: None,
                exact_filter_materialization: None,
            },
            confidence: paro_planner::logical::operator::Confidence::High,
        },
        vec![
            ref_expr(2, LogicalType::Varchar),
            derived_score,
            score_expr.clone(),
        ],
        9,
        Vec::new(),
        Vec::new(),
        Some(2),
        score_expr,
        false,
        5,
    )
    .with_output_names(vec!["c".to_string(), "derived_score".to_string()]);
    let plan = OwnedLogicalPlan::new(&ctx, LogicalOperator::SearchScan(Box::new(search)));

    let physical = PhysicalPlanBuilder::new(PhysicalBuildContext::default())
        .build(plan)
        .expect("derived search score should lower through a projection");

    let PhysicalNodeKind::Project(project) = &physical.node(physical.root).kind else {
        panic!("derived search score should retain a project above the source");
    };
    assert_eq!(project.visible_count, 2);
    assert_eq!(
        project.output_names.as_ref(),
        ["c", "derived_score", "__paro_hidden_1"]
    );
    let Expression::Operator(derived) = &project.expressions[1] else {
        panic!("derived score expression should be preserved");
    };
    assert!(matches!(
        &derived.children[0],
        Expression::Reference(reference) if reference.index == 1
    ));

    let [source_id] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("derived search projection should have one source child");
    };
    let PhysicalNodeKind::FullTextSearch(source) = &physical.node(*source_id).kind else {
        panic!("derived search projection should read from the full-text source");
    };
    assert_eq!(source.projected_columns.as_ref(), [2]);
    assert_eq!(source.output_names.as_ref(), ["c", "__search_score"]);
    assert_eq!(
        source.output_types.as_ref(),
        [LogicalType::Varchar, LogicalType::Float]
    );
}

fn document_rank_expression() -> Expression {
    let function = paro_function::scalar::fulltext::get_bm25_functions()
        .functions
        .remove(0);
    Expression::Function(
        paro_planner::expression::FunctionExpression::new(
            function,
            vec![
                ref_expr(2, LogicalType::Varchar),
                Expression::Constant(
                    ConstantExpression::new(Value::Varchar("graph".into()), LogicalType::Varchar)
                        .into(),
                ),
            ],
            LogicalType::Float,
        )
        .into(),
    )
}

pub(super) fn test_get() -> Get {
    let storage = Arc::new(
        paro_storage::table::table_factory::TableFactory::default()
            .create_table(&[
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Varchar,
            ])
            .expect("table storage"),
    );
    let table = Arc::new(TableCatalogEntry::new(
        "paro".to_string(),
        "public".to_string(),
        "scan_t".to_string(),
        vec![
            ColumnDefinition::new("a".to_string(), LogicalType::Integer),
            ColumnDefinition::new("b".to_string(), LogicalType::Integer),
            ColumnDefinition::new("c".to_string(), LogicalType::Varchar),
        ],
        storage,
        paro_catalog::entry::CatalogObjectId::from_raw(10_001),
        0,
    ));
    Get {
        table_index: 0,
        returned_types: vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        names: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        relation_name: Some("scan_t".to_string()),
        relation_alias: None,
        column_sources: vec![
            paro_planner::logical::operator::GetColumnSource::Stored { column_id: 0 },
            paro_planner::logical::operator::GetColumnSource::Stored { column_id: 1 },
            paro_planner::logical::operator::GetColumnSource::Stored { column_id: 2 },
        ],
        column_types: vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        table: Some(table),
        scan_order: None,
        runtime_filter_expressions: Vec::new(),
    }
}

fn ref_expr(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty).into())
}

fn int_const(value: i32) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
    )
}

fn comparison(comparison_type: ComparisonType, left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpression::new(comparison_type, left, right).into())
}
