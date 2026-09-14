// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::super::{boundary, matching, PlannerTransformation, TransformContext};
use super::*;
use crate::cascades::budget::{BudgetDimension, SearchBudget};
use crate::cascades::planner::MemoBuilder;
use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, CreateTableInfo};
use paro_planner::binder::{context::BindContext, ir::OrderByNode};
use paro_planner::operator::{Filter, Get, JoinType, TopN};
use paro_planner::plan::{CardinalityEstimate, OwnedLogicalPlan};
use paro_storage::table::table_factory::TableFactory;

fn table() -> Arc<TableCatalogEntry> {
    let types = vec![
        LogicalType::BigInt,
        LogicalType::Varchar,
        LogicalType::Varchar,
    ];
    let columns = types
        .iter()
        .enumerate()
        .map(|(i, t)| ColumnDefinition::new(format!("c{i}"), t.clone()))
        .collect();
    Arc::new(
        TableCatalogEntry::from_info(
            CreateTableInfo::new(
                "paro".into(),
                "public".into(),
                "native_topn".into(),
                columns,
            ),
            Arc::new(TableFactory::default().create_table(&types).unwrap()),
            CatalogObjectId::from_raw(91_030),
            0,
        )
        .unwrap(),
    )
}

fn scan(index: usize) -> OwnedLogicalPlan {
    let table = table();
    let mut get = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
        index,
        table.columns.iter().map(|c| c.name.clone()).collect(),
        table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect(),
        table,
    ))));
    get.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100_000));
    get
}

// kind 0: both frontiers; 1: hidden ordering; 2: derived carrier;
// 3: comparison join, 4: cross, 5: duplicate selected source occurrence.
fn fixture(kind: usize) -> OwnedLogicalPlan {
    let mut input = scan(7);
    let mut exprs = vec![
        column(ColumnBinding::new(7, 1), LogicalType::Varchar),
        column(ColumnBinding::new(7, 2), LogicalType::Varchar),
    ];
    if kind == 2 {
        let LogicalOperator::Get(get) = &mut input.operator else {
            unreachable!()
        };
        let prefix = get.append_matched_utf8_prefix(1, 2, LogicalType::Varchar);
        exprs[0] = column(prefix, LogicalType::Varchar);
    }
    if kind >= 3 {
        let peer = scan(if kind == 5 { 7 } else { 9 });
        input = OwnedLogicalPlan::synthetic(LogicalOperator::Join(if kind == 4 {
            Join::Cross(paro_planner::operator::CrossProduct::new(input, peer))
        } else {
            Join::comparison(JoinType::Inner, input, peer, vec![])
        }));
    }
    input = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(input, vec![])));
    input.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100));
    if kind == 1 {
        exprs.push(column(ColumnBinding::new(7, 0), LogicalType::BigInt));
    }
    let projection =
        Projection::new(20, input, exprs).with_visible_names(vec!["name".into(), "address".into()]);
    let mut projection = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(projection));
    projection.stats.estimated_cardinality = Some(CardinalityEstimate::exact(100));
    let mut topn = TopN::new(
        projection,
        vec![OrderByNode {
            expression: column(
                ColumnBinding::new(20, if kind == 1 { 2 } else { 0 }),
                if kind == 1 {
                    LogicalType::BigInt
                } else {
                    LogicalType::Varchar
                },
            ),
            ascending: false,
            nulls_first: true,
        }],
        3,
        2,
    );
    if kind == 1 {
        topn.projection_map = ProjectionMap::new(vec![0, 1]);
    }
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::TopN(topn));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(3));
    plan
}

fn with_shell(plan: OwnedLogicalPlan, f: impl FnOnce(NativeShell, &PlannerTransformState)) {
    use crate::cascades::rules::TransformationRule;
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
        .expect("selected TopN binding")
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
    f(shell, &state);
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
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        super::super::semantic_plan::owned_binding_instantiation_count(),
        bridges
    );
    drop(outputs);
    context.rollback().unwrap();
    let state = input.planner_state.read().unwrap();
    assert_eq!(
        (
            input.memo.group_count(),
            state.columns.len(),
            state.scalars.len(),
            state.binding_ids.checkpoint(),
            state.payloads.logical.len()
        ),
        before
    );
}

fn fetch_columns(shell: &NativeShell) -> Vec<Vec<usize>> {
    shell
        .nodes
        .iter()
        .filter_map(|n| match &n.operator {
            LogicalOperator::RowFetch(f) => Some(
                f.sources
                    .iter()
                    .flat_map(|s| s.needed_columns.iter().copied())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[test]
fn native_detail_topn_preserves_two_fetch_frontiers_without_owned_bridge() {
    for kind in 0..5 {
        let (_, changed) = crate::aggregate::late_payload::rewrite_node(
            fixture(kind),
            &BindContext::new(),
            &crate::cost_model::CostModel::default(),
        )
        .unwrap();
        assert!(changed, "legacy fixture {kind}");
        with_shell(fixture(kind), |shell, state| {
            let before = super::super::semantic_plan::owned_binding_instantiation_count();
            let expected = shell.root_layout().unwrap();
            let result = rewrite(shell, state).unwrap().expect("native detail TopN");
            assert_eq!(
                before,
                super::super::semantic_plan::owned_binding_instantiation_count()
            );
            assert_eq!(result.root_layout().unwrap(), expected);
            let LogicalOperator::Projection(output) = result.root_operator() else {
                panic!("final projection")
            };
            assert_eq!(output.visible_names, ["name", "address"]);
            assert_eq!(output.visible_count, 2);
            let topn_index = result
                .nodes
                .iter()
                .position(|n| matches!(n.operator, LogicalOperator::TopN(_)))
                .unwrap();
            let LogicalOperator::TopN(topn) = &result.nodes[topn_index].operator else {
                unreachable!()
            };
            assert_eq!((topn.limit, topn.offset), (3, 2));
            assert!(!topn.orders[0].ascending && topn.orders[0].nulls_first);
            let fetches = result
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(i, n)| {
                    matches!(n.operator, LogicalOperator::RowFetch(_)).then_some(i)
                })
                .collect::<Vec<_>>();
            assert!(
                fetches.iter().any(|&i| i > topn_index),
                "output fetch after TopN"
            );
            if kind == 0 || kind == 3 || kind == 4 {
                assert_eq!(fetch_columns(&result), vec![vec![1], vec![2]]);
                assert!(
                    fetches.iter().any(|&i| i < topn_index),
                    "ordering fetch before TopN"
                );
            }
            if kind == 2 {
                assert_eq!(fetch_columns(&result), vec![vec![2]]);
            }
        });
    }
}

#[test]
fn native_detail_topn_guards_and_source_occurrences_fail_closed() {
    with_shell(fixture(0), |shell, state| {
        for case in 0..4 {
            let mut bad = shell.clone();
            let LogicalOperator::TopN(topn) = &mut bad.nodes[bad.root].operator else {
                unreachable!()
            };
            match case {
                0 => {
                    topn.limit = 0;
                    topn.offset = 0;
                }
                1 => topn.projection_map = ProjectionMap::new(vec![99]),
                2 => {
                    let Expression::ColumnRef(c) = &mut topn.orders[0].expression else {
                        unreachable!()
                    };
                    c.depth = 1;
                }
                _ => {
                    let Expression::ColumnRef(c) = &mut topn.orders[0].expression else {
                        unreachable!()
                    };
                    c.binding.table_index = 999;
                }
            }
            assert!(rewrite(bad, state).unwrap().is_none(), "guard {case}");
        }
        // A compact Projection must not silently renumber the original
        // namespace for non-prefix, reordered or repeated root outputs.
        for indices in [vec![1], vec![1, 0], vec![0, 0]] {
            let mut bad = shell.clone();
            let LogicalOperator::TopN(topn) = &mut bad.nodes[bad.root].operator else {
                unreachable!()
            };
            topn.projection_map = ProjectionMap::new(indices.clone());
            assert!(rewrite(bad, state).unwrap().is_none(), "map {indices:?}");
        }
    });
    with_shell(fixture(3), |mut shell, state| {
        let index = shell
            .nodes
            .iter()
            .position(|n| matches!(n.operator, LogicalOperator::Join(_)))
            .unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = &mut shell.nodes[index].operator else {
            unreachable!()
        };
        join.right = join.left.clone();
        assert_eq!(occurrences(&shell, &NativeChild::Node(index), 7), Some(2));
        assert!(rewrite(shell, state).unwrap().is_none());
    });
}

#[test]
fn native_detail_topn_respects_null_extension_and_any_join_fences() {
    with_shell(fixture(3), |shell, state| {
        let mut arbitrary = shell.clone();
        let join_node = arbitrary
            .nodes
            .iter_mut()
            .find(|n| matches!(n.operator, LogicalOperator::Join(_)))
            .unwrap();
        let LogicalOperator::Join(Join::Comparison(join)) = &join_node.operator else {
            unreachable!()
        };
        join_node.operator =
            LogicalOperator::Join(Join::Any(Box::new(paro_planner::operator::AnyJoin {
                join_type: join.join_type,
                left: join.left.clone(),
                right: join.right.clone(),
                condition: Expression::Constant(
                    paro_planner::expression::ConstantExpression::new(
                        paro_common::runtime_value::Value::Boolean(true),
                        LogicalType::Boolean,
                    )
                    .into(),
                ),
                mark_index: None,
                build_side_constraint: join.build_side_constraint,
                left_projection_map: join.left_projection_map.clone(),
                right_projection_map: join.right_projection_map.clone(),
            })));
        assert!(rewrite(arbitrary, state).unwrap().is_none());
        for (join_type, allowed) in [
            (JoinType::Left, true),
            (JoinType::Right, false),
            (JoinType::Outer, false),
            (JoinType::Semi, true),
            (JoinType::Anti, true),
        ] {
            let mut changed = shell.clone();
            let index = changed
                .nodes
                .iter()
                .position(|n| matches!(n.operator, LogicalOperator::Join(_)))
                .unwrap();
            let LogicalOperator::Join(Join::Comparison(join)) = &mut changed.nodes[index].operator
            else {
                unreachable!()
            };
            join.join_type = join_type;
            assert_eq!(
                rewrite(changed, state).unwrap().is_some(),
                allowed,
                "{join_type:?}"
            );
        }
    });
}
