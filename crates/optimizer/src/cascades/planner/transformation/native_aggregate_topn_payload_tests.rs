// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::super::native_topn_payload::restore_root_output;
use super::super::{boundary, matching, PlannerTransformation, TransformContext};
use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use crate::cascades::rules::TransformationRule;
use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, CreateTableInfo, TableCatalogEntry};
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_planner::binder::{context::BindContext, ir::OrderByNode};
use paro_planner::expression::AggregateExpression;
use paro_planner::operator::aggregate::GroupDependency;
use paro_planner::operator::{Aggregate, Get, Projection, TopN};
use paro_planner::plan::{CardinalityEstimate, OwnedLogicalPlan};
use paro_storage::table::table_factory::TableFactory;
use std::sync::Arc;

fn fixture(map: Option<Vec<usize>>, payload_order: bool) -> OwnedLogicalPlan {
    let types = vec![
        LogicalType::BigInt,
        LogicalType::Varchar,
        LogicalType::Varchar,
        LogicalType::Integer,
    ];
    let columns = types
        .iter()
        .enumerate()
        .map(|(i, t)| ColumnDefinition::new(format!("c{i}"), t.clone()))
        .collect();
    let table = Arc::new(
        TableCatalogEntry::from_info(
            CreateTableInfo::new(
                "paro".into(),
                "public".into(),
                "aggregate_topn".into(),
                columns,
            ),
            Arc::new(TableFactory::default().create_table(&types).unwrap()),
            CatalogObjectId::from_raw(91_031),
            0,
        )
        .unwrap(),
    );
    let mut get = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
        10,
        (0..4).map(|i| format!("c{i}")).collect(),
        types,
        table,
    ))));
    get.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100_000));
    let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    let mut aggregate = Aggregate::new(
        20,
        21,
        22,
        get,
        vec![
            column(ColumnBinding::new(10, 0), LogicalType::BigInt),
            column(ColumnBinding::new(10, 1), LogicalType::Varchar),
            column(ColumnBinding::new(10, 2), LogicalType::Varchar),
        ],
        vec![],
        vec![Expression::Aggregate(
            AggregateExpression::new(
                sum,
                vec![column(ColumnBinding::new(10, 3), LogicalType::Integer)],
                LogicalType::BigInt,
            )
            .into(),
        )],
        vec![],
    );
    aggregate.group_dependencies.push(GroupDependency {
        determinants: vec![0].into_boxed_slice(),
        dependents: vec![1, 2].into_boxed_slice(),
    });
    let mut aggregate =
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate)));
    aggregate.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let projection = Projection::new(
        30,
        aggregate,
        vec![
            column(ColumnBinding::new(20, 0), LogicalType::BigInt),
            column(ColumnBinding::new(20, 1), LogicalType::Varchar),
            column(ColumnBinding::new(21, 0), LogicalType::BigInt),
            column(ColumnBinding::new(20, 2), LogicalType::Varchar),
        ],
    )
    .with_visible_names(vec![
        "key".into(),
        "name".into(),
        "sum".into(),
        "address".into(),
    ]);
    let mut projection = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(projection));
    projection.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut topn = TopN::new(
        projection,
        vec![OrderByNode {
            expression: column(
                ColumnBinding::new(30, if payload_order { 1 } else { 2 }),
                if payload_order {
                    LogicalType::Varchar
                } else {
                    LogicalType::BigInt
                },
            ),
            ascending: false,
            nulls_first: true,
        }],
        20,
        3,
    );
    if let Some(map) = map {
        topn.projection_map = ProjectionMap::new(map);
    }
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::TopN(topn));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    plan
}

fn check(plan: OwnedLogicalPlan, expected: bool, inspect: impl FnOnce(&NativeShell, &NativeShell)) {
    let mut input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
    input.planner_state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    let state = input.planner_state.read().unwrap();
    let expr = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let bindings = matching::scoped_pattern_bindings(
        PlannerTransformation::LatePayloadFetch,
        input.root,
        expr,
        &input.memo,
        &state,
        None,
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap();
    let binding = &bindings
        .bindings
        .first()
        .expect("aggregate TopN selected binding")
        .root;
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let facts = boundary::BoundarySnapshot::read(
        &mut context,
        &state,
        binding,
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let mut shell = NativeShell::from_pattern(context.memo(), &state, binding, &facts)
        .unwrap()
        .unwrap();
    restore_root_output(&mut shell, binding, context.memo(), &state).unwrap();
    let result = rewrite(shell.clone(), &state).unwrap();
    assert_eq!(result.is_some(), expected);
    if let Some(result) = result {
        inspect(&shell, &result);
    }
    let before = (
        context.memo().group_count(),
        state.columns.len(),
        state.scalars.len(),
        state.binding_ids.checkpoint(),
        state.payloads.logical.len(),
    );
    drop(state);
    let rule = super::super::PlannerTransformationRule {
        transformation: PlannerTransformation::LatePayloadFetch,
        planner_state: input.planner_state.clone(),
    };
    let bridges = super::super::semantic_plan::owned_binding_instantiation_count();
    let outputs = rule
        .apply_binding(&bindings.bindings[0], &mut context)
        .unwrap();
    assert_eq!(outputs.len(), usize::from(expected));
    if expected {
        assert_eq!(
            bridges,
            super::super::semantic_plan::owned_binding_instantiation_count()
        );
    }
    drop(outputs);
    context.rollback().unwrap();
    let state = input.planner_state.read().unwrap();
    assert_eq!(
        before,
        (
            input.memo.group_count(),
            state.columns.len(),
            state.scalars.len(),
            state.binding_ids.checkpoint(),
            state.payloads.logical.len()
        )
    );
}

#[test]
fn native_aggregate_topn_exact_group_remap_apply_and_rollback() {
    let (_, changed) = crate::aggregate::late_payload::rewrite_node(
        fixture(None, false),
        &BindContext::new(),
        &crate::cost_model::CostModel::default(),
    )
    .unwrap();
    assert!(changed);
    check(fixture(None, false), true, |original, result| {
        assert_eq!(
            original.root_layout().unwrap(),
            result.root_layout().unwrap()
        );
        let aggregate_index = result
            .nodes
            .iter()
            .position(|n| matches!(n.operator, LogicalOperator::Aggregate(_)))
            .unwrap();
        let LogicalOperator::Aggregate(aggregate) = &result.nodes[aggregate_index].operator else {
            unreachable!()
        };
        assert_eq!(aggregate.groups.len(), 2);
        assert_eq!(aggregate.group_stats.len(), 2);
        assert!(aggregate.group_dependencies.is_empty());
        assert!(aggregate.grouping_sets.is_empty());
        assert!(matches!(
            aggregate.group_input_multiplicity,
            GroupInputMultiplicity::Arbitrary
        ));
        assert_eq!(
            aggregate.returned_types,
            [
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt
            ]
        );
        let LogicalOperator::Projection(output) = result.root_operator() else {
            panic!("final projection")
        };
        assert_eq!(output.visible_names, ["key", "name", "sum", "address"]);
        let fetch_index = node(&output.child).unwrap();
        let LogicalOperator::RowFetch(fetch) = &result.nodes[fetch_index].operator else {
            panic!("post-TopN fetch")
        };
        assert_eq!(fetch.sources[0].needed_columns.as_ref(), &[1, 2]);
        let topn_index = node(&fetch.child).unwrap();
        let LogicalOperator::TopN(topn) = &result.nodes[topn_index].operator else {
            panic!("TopN")
        };
        assert_eq!((topn.limit, topn.offset), (20, 3));
        assert!(!topn.orders[0].ascending && topn.orders[0].nulls_first);
        assert_eq!(
            result.nodes[topn_index].stats.estimated_cardinality,
            original.nodes[original.root].stats.estimated_cardinality
        );
        assert_eq!(
            result.nodes[result.root].stats.estimated_cardinality,
            original.nodes[original.root].stats.estimated_cardinality
        );
        let LogicalOperator::Projection(carrier) =
            &result.nodes[node(&topn.child).unwrap()].operator
        else {
            panic!("carrier")
        };
        assert_eq!(carrier.visible_count, 0);
        assert_eq!(
            carrier.visible_names,
            ["late_group_0", "late_aggregate_0", "__late_rowid"]
        );
        assert!(
            result
                .nodes
                .iter()
                .filter(|n| matches!(n.operator, LogicalOperator::RowFetch(_)))
                .count()
                == 1
        );
    });
}

#[test]
fn native_aggregate_topn_omits_fetch_when_payload_not_projected() {
    check(fixture(Some(vec![0]), false), true, |_, result| {
        assert!(!result
            .nodes
            .iter()
            .any(|n| matches!(n.operator, LogicalOperator::RowFetch(_))));
        let LogicalOperator::Projection(output) = result.root_operator() else {
            unreachable!()
        };
        assert_eq!(output.visible_names, ["key"]);
    });
}

#[test]
fn native_aggregate_topn_shared_admission_rejects_payload_order() {
    let (_, changed) = crate::aggregate::late_payload::rewrite_node(
        fixture(None, true),
        &BindContext::new(),
        &crate::cost_model::CostModel::default(),
    )
    .unwrap();
    assert!(!changed);
    check(fixture(None, true), false, |_, _| unreachable!());
}

#[test]
fn native_aggregate_topn_nonprefix_map_never_changes_root_binding_identity() {
    // Native producer must not return a compressed [0] binding for original
    // output [1]. The outer integration may normalize/restore this separately.
    check(fixture(Some(vec![1]), false), false, |_, _| unreachable!());
}

#[test]
fn native_aggregate_topn_requires_dependency_and_non_null_selected_source() {
    for null_extended in [false, true] {
        let mut plan = fixture(None, false);
        let LogicalOperator::TopN(topn) = &mut plan.operator else {
            unreachable!()
        };
        let LogicalOperator::Projection(output) = &mut topn.child.operator else {
            unreachable!()
        };
        let LogicalOperator::Aggregate(aggregate) = &mut output.child.operator else {
            unreachable!()
        };
        if null_extended {
            let peer = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                Get::new_without_table(9, vec!["peer".into()], vec![LogicalType::BigInt]),
            )));
            let source = *std::mem::replace(
                &mut aggregate.child,
                Box::new(OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)),
            );
            let mut joined = OwnedLogicalPlan::synthetic(LogicalOperator::Join(
                paro_planner::operator::Join::comparison(
                    paro_planner::operator::JoinType::Left,
                    peer,
                    source,
                    vec![],
                ),
            ));
            joined.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100_000));
            aggregate.child = Box::new(joined);
        } else {
            aggregate.group_dependencies.clear();
        }
        check(plan, false, |_, _| unreachable!());
    }
}
