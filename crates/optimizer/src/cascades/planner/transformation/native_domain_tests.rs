// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::types::LogicalType;
use paro_function::scalar::FunctionErrorMode;
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
    ConstantExpression, FunctionExpression,
};
use paro_planner::operator::bound_reference::{
    BoundRelationFactValues, BoundRelationFacts, BoundSourceColumn,
};

fn column(table: usize, ordinal: usize) -> Expression {
    Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(table, ordinal), LogicalType::Integer).into(),
    )
}
fn equal(table: usize, ordinal: usize) -> Expression {
    Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            column(table, ordinal),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(2), LogicalType::Integer).into(),
            ),
        )
        .into(),
    )
}
fn boundary(id: u32, table: usize, ordinals: &[usize]) -> OwnedLogicalPlan {
    let types = vec![LogicalType::Integer; ordinals.len()];
    let facts = Arc::new(BoundRelationFacts::new(
        BoundRelationFactValues {
            cardinality: Some(CardinalityEstimate::exact(10)),
            contains_control_region: false,
            source_lineage: ordinals
                .iter()
                .map(|ordinal| {
                    Some(vec![BoundSourceColumn {
                        source: table,
                        occurrence: id as usize,
                        column: *ordinal,
                        rows: Some(CardinalityEstimate::exact(10)),
                        distinct: Some(4),
                        unique: false,
                    }])
                })
                .collect(),
            ..BoundRelationFactValues::default()
        },
        types.clone(),
    ));
    let reference = BoundReference::new(
        BoundReferenceId::group_hole(id),
        ordinals
            .iter()
            .map(|ordinal| ColumnBinding::new(table, *ordinal))
            .collect(),
        types,
    )
    .with_facts(facts)
    .unwrap();
    OwnedLogicalPlan::synthetic(LogicalOperator::BoundReference(reference))
}
fn native(plan: OwnedLogicalPlan) -> NativeShell {
    let mut shell = NativeShell::from_owned(plan, &HashMap::new()).unwrap();
    for node in &mut shell.nodes {
        node.operator.visit_child_links_mut(&mut |child| {
            if let NativeChild::Group { id, stats, layout, names, reference } = child.clone() {
                *child = NativeChild::MemoGroup {
                    group: GroupId::new(reference.reference_id.group_hole_value().unwrap() as usize),
                    id, stats, layout, names, reference,
                };
            }
        });
    }
    shell
}
fn state() -> Arc<RwLock<PlannerTransformState>> {
    MemoBuilder::build(
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap()
    .planner_state
}

fn frozen_selected(
    plan: OwnedLogicalPlan,
) -> (
    CascadesEngine,
    Arc<RwLock<PlannerTransformState>>,
    Arc<FrozenCandidate>,
) {
    let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
    let state = input.planner_state.clone();
    let grants = super::super::super::tests::test_grant_classes();
    let classes = Arc::new(grants.into_iter().map(|grant| (grant.id, grant)).collect());
    let mut registry = ImplementationRegistry::default();
    implementation::register_implementations(
        &mut registry,
        state.clone(),
        classes,
        input.calibration,
        false,
    )
    .unwrap();
    let mut engine = CascadesEngine::new(input.memo, registry);
    let optimized = engine
        .optimize_for_grants(
            input.root,
            input.root_goal,
            AdmissibleGrantSetId(0),
            grants,
            input.mode,
        )
        .unwrap();
    let winner = optimized.winners.first().unwrap();
    let root = engine
        .memo()
        .freeze_candidate_tree(ChildWinnerRef {
            group: input.root,
            goal: winner.goal,
            candidate: winner.winner.candidate,
        })
        .unwrap();
    state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    (engine, state, root)
}
fn rebound_columns(shell: &NativeShell) -> Vec<Vec<ColumnBinding>> {
    shell
        .nodes
        .iter()
        .filter_map(|node| {
            let LogicalOperator::Filter(filter) = &node.operator else {
                return None;
            };
            let mut columns = Vec::new();
            for predicate in &filter.expressions {
                visit_expression(predicate, &mut |part| {
                    if let Expression::ColumnRef(column) = part {
                        columns.push(column.binding);
                    }
                });
            }
            Some(columns)
        })
        .collect()
}

#[test]
fn output_demand_preserves_row_facts_and_predicate_inputs() {
    use paro_planner::binder::ir::OrderByNode;
    use paro_planner::operator::{ProjectionMap, TopN};
    let mut filtered = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        boundary(1, 10, &[0, 1, 2]),
        vec![equal(10, 2)],
    )));
    filtered.stats.estimated_cardinality = Some(CardinalityEstimate::exact(7));
    let mut topn = TopN::new(
        filtered,
        vec![OrderByNode {
            expression: column(10, 1),
            ascending: true,
            nulls_first: false,
        }],
        2,
        0,
    );
    topn.projection_map = ProjectionMap::new(vec![0]);
    let shell = native(OwnedLogicalPlan::synthetic(LogicalOperator::TopN(topn)));
    let rows = shell
        .nodes
        .iter()
        .map(|node| node.stats.estimated_cardinality)
        .collect::<Vec<_>>();
    let input = MemoBuilder::build(
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap();
    let result = prune_output_demands(shell, &input.planner_state.read().unwrap(), &input.memo)
        .unwrap()
        .unwrap();
    assert_eq!(
        result
            .nodes
            .iter()
            .map(|node| node.stats.estimated_cardinality)
            .collect::<Vec<_>>(),
        rows
    );
    let LogicalOperator::Filter(filter) = &result.nodes[0].operator else {
        unreachable!()
    };
    assert_eq!(filter.projection_map.as_columns(), Some([0, 1].as_slice()));
    // The predicate-only input is still available; only the emitted row is narrower.
    let NativeChild::MemoGroup { layout, .. } = &filter.child else {
        unreachable!()
    };
    assert_eq!(
        layout.bindings(),
        &[
            ColumnBinding::new(10, 0),
            ColumnBinding::new(10, 1),
            ColumnBinding::new(10, 2)
        ]
    );
    assert_eq!(
        result.root_layout().unwrap().bindings(),
        &[ColumnBinding::new(10, 0)]
    );
}

#[test]
fn domain_union_routes_each_ordinal_and_preserves_duplicate_null_bags() {
    let state = state();
    let state = state.write().unwrap();
    for base in [0, 100] {
        let union = SetOperation::union(
            base + 30,
            boundary(1, base + 10, &[7, 2]),
            boundary(2, base + 20, &[9, 4]),
            true,
            vec![LogicalType::Integer; 2],
        );
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(union)),
            vec![equal(base + 30, 1)],
        )));
        let shell = native(plan);
        let output = shell.root_layout().unwrap();
        let result = transfer_shell(shell, &state).unwrap().unwrap();
        assert_eq!(result.root_layout().unwrap(), output);
        assert!(matches!(
            result.root_operator(),
            LogicalOperator::SetOperation(_)
        ));
        assert_eq!(
            rebound_columns(&result),
            vec![
                vec![ColumnBinding::new(base + 10, 2)],
                vec![ColumnBinding::new(base + 20, 4)]
            ]
        );
        // Independent bag-selection oracle, not a cost-composition replay.
        // The generated predicates above identify slot 1 in each exact input.
        let left = vec![
            (Some(1), Some(2)),
            (Some(1), Some(2)),
            (None, Some(2)),
            (Some(-3), None),
        ];
        let right = vec![(Some(1), Some(2)), (None, None), (Some(-3), Some(-4))];
        let original = left
            .iter()
            .chain(&right)
            .filter(|row| row.1 == Some(2))
            .copied()
            .collect::<Vec<_>>();
        let transformed = [left, right]
            .into_iter()
            .flat_map(|rows| rows.into_iter().filter(|row| row.1 == Some(2)))
            .collect::<Vec<_>>();
        assert_eq!(original, transformed);
        assert_eq!(transformed.len(), 4);
    }
}

#[test]
fn domain_group_transfer_keeps_aggregate_output_residual_and_original_layout() {
    let state = state();
    let state = state.read().unwrap();
    let mut subtract = paro_function::scalar::ScalarFunctionSet::new("-".into());
    paro_function::scalar::operators::arithmetic::register_arithmetic_functions(&mut subtract);
    let (subtract, _) = subtract
        .bind(&[LogicalType::Integer, LogicalType::Integer])
        .unwrap();
    let difference = Expression::Function(
        FunctionExpression::new(
            subtract,
            vec![column(0, 2), column(0, 9)],
            LogicalType::Integer,
        )
        .into(),
    );
    let (sum, _) = paro_function::aggregate::distributive::sum::get_sum_function()
        .bind(&[LogicalType::Integer])
        .unwrap();
    let sum = Expression::Aggregate(
        AggregateExpression::new(sum, vec![difference], LogicalType::BigInt).into(),
    );
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        boundary(1, 0, &[7, 2, 9]),
        vec![column(0, 7)],
        vec![],
        vec![sum.clone()],
        vec![],
    );
    let positive = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(11, 0), LogicalType::BigInt).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::BigInt(0), LogicalType::BigInt).into(),
            ),
        )
        .into(),
    );
    let mut filter = Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
        vec![equal(10, 0), positive],
    );
    filter.projection_map = paro_planner::operator::ProjectionMap::new(vec![1]);
    let shell = native(OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter)));
    let result = transfer_shell(shell, &state).unwrap().unwrap();
    let LogicalOperator::Filter(root) = result.root_operator() else {
        panic!("residual must remain above grouping");
    };
    assert_eq!(root.projection_map.to_indices(2), vec![1]);
    assert_eq!(
        rebound_columns(&result),
        vec![
            vec![ColumnBinding::new(0, 7)],
            vec![ColumnBinding::new(11, 0)]
        ]
    );
    let NativeChild::Node(aggregate) = root.child else {
        panic!("aggregate child");
    };
    let LogicalOperator::Aggregate(aggregate) = &result.nodes[aggregate].operator else {
        panic!("aggregate preserved");
    };
    assert!(aggregate.aggregates[0].equals(&sum));
    // Independent grouped SUM(x-y), with duplicate/NULL keys and amounts and
    // a negative contribution that must not be filtered before accumulation.
    let rows = [
        (Some(2), Some(5), Some(1)),
        (Some(2), Some(-5), Some(2)),
        (Some(2), None, Some(1)),
        (None, Some(99), Some(0)),
        (Some(1), Some(3), Some(1)),
    ];
    let group = |rows: Vec<(Option<i32>, Option<i32>, Option<i32>)>| {
        let mut sums = BTreeMap::<Option<i32>, Option<i32>>::new();
        for (key, x, y) in rows {
            let value = sums.entry(key).or_default();
            if let Some(amount) = x.zip(y).map(|(x, y)| x - y) {
                *value = Some(value.unwrap_or(0) + amount);
            }
        }
        sums.into_iter()
            .filter(|(key, sum)| *key == Some(2) && sum.is_some_and(|sum| sum > 0))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        group(rows.to_vec()),
        group(rows.into_iter().filter(|row| row.0 == Some(2)).collect())
    );
    assert!(
        group(rows.to_vec()).is_empty(),
        "pushing SUM > 0 to amounts would invent a result"
    );
}

#[test]
fn domain_projection_rebinds_transparent_columns_and_declines_missing_lineage() {
    let state = state();
    let state = state.read().unwrap();
    let fixture = || {
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                10,
                boundary(1, 0, &[7, 2]),
                vec![column(0, 2), column(0, 7)],
            ))),
            vec![equal(10, 0)],
        )))
    };
    let result = transfer_shell(native(fixture()), &state).unwrap().unwrap();
    assert_eq!(
        rebound_columns(&result),
        vec![vec![ColumnBinding::new(0, 2)]]
    );
    let mut shell = native(fixture());
    for node in &mut shell.nodes {
        node.operator.visit_child_links_mut(&mut |child| {
            if let NativeChild::MemoGroup { reference, .. } = child {
                let mut values = reference.facts.values().clone();
                values.source_lineage.clear();
                reference.facts =
                    Arc::new(BoundRelationFacts::new(values, reference.types().to_vec()));
            }
        });
    }
    assert!(transfer_shell(shell, &state).unwrap().is_none());

    let foreign_namespace = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            10,
            boundary(1, 0, &[7, 2]),
            vec![column(0, 2), column(0, 7)],
        ))),
        vec![equal(99, 0)],
    )));
    assert!(
        transfer_shell(native(foreign_namespace), &state)
            .unwrap()
            .is_none(),
        "a predicate from a foreign output namespace must not be rebound by ordinal"
    );
}

#[test]
fn domain_closure_reaches_an_exact_aggregate_behind_projection() {
    let state = state();
    let state = state.read().unwrap();
    let count = Expression::Aggregate(
        AggregateExpression::new(
            paro_function::aggregate::distributive::count::get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        )
        .into(),
    );
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        boundary(1, 0, &[7, 2]),
        vec![column(0, 7)],
        vec![],
        vec![count],
        vec![],
    );
    let projection = Projection::new(
        20,
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
        vec![column(10, 0)],
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(projection)),
        vec![equal(20, 0)],
    )));
    let result = transfer_shell_closure(native(plan), &state)
        .unwrap()
        .unwrap();
    let filters = result
        .nodes
        .iter()
        .filter_map(|node| match &node.operator {
            LogicalOperator::Filter(filter) => Some(filter),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        filters.len(),
        1,
        "one local filter should be inserted at the leaf"
    );
    assert_eq!(
        rebound_columns(&result),
        vec![vec![ColumnBinding::new(0, 7)]]
    );
    assert!(result
        .nodes
        .iter()
        .any(|node| { matches!(node.operator, LogicalOperator::Aggregate(_)) }));
}

#[test]
fn native_domain_journal_rolls_back_speculative_nodes_and_operators() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        boundary(1, 0, &[7]),
        vec![equal(10, 0)],
    )));
    let shell = native(plan);
    let original_root = shell.root;
    let original_operator = shell.nodes[original_root].operator.clone();
    let input = match &original_operator {
        LogicalOperator::Filter(filter) => filter.child.clone(),
        _ => panic!("test fixture root should be a filter"),
    };
    let mut layouts = shell.layouts().unwrap();
    let mut nodes = shell.nodes.into_vec();
    let mut journal = NativeRewriteJournal::default();
    let mut fixed_point = domain_transfer::DomainFixedPoint::default();
    let checkpoint = journal.checkpoint(&nodes, &layouts, &fixed_point);
    let relation = match &input {
        NativeChild::MemoGroup { group, .. } => *group,
        NativeChild::Node(_) | NativeChild::Group { .. } => fixed_point.root_relation(),
    };
    let predicate = equal(10, 0);
    fixed_point.record(relation, &[], &predicate);
    assert!(fixed_point.is_seen(relation, &[], &predicate));

    journal.record_operator(&nodes, original_root);
    nodes[original_root].operator = LogicalOperator::DummyScan;
    let state = state();
    let state = state.read().unwrap();
    let _speculative = add_native_filter(
        &mut nodes,
        &mut layouts,
        input,
        vec![equal(10, 0)],
        paro_planner::operator::ProjectionMap::all(),
        &state,
    )
    .unwrap();

    assert!(nodes.len() > checkpoint.node_len);
    journal.rollback(&mut nodes, &mut layouts, &mut fixed_point, checkpoint);
    assert_eq!(nodes.len(), checkpoint.node_len);
    assert_eq!(layouts.len(), checkpoint.layout_len);
    assert!(!fixed_point.is_seen(relation, &[], &predicate));
    assert!(matches!(
        nodes[original_root].operator,
        LogicalOperator::Filter(_)
    ));
}

#[test]
fn native_domain_fixed_point_does_not_reinstall_an_exact_landing_filter() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        boundary(1, 0, &[7]),
        vec![equal(10, 0)],
    )));
    let shell = native(plan);
    let root = shell.root;
    let input = match &shell.nodes[root].operator {
        LogicalOperator::Filter(filter) => filter.child.clone(),
        _ => panic!("test fixture root should be a filter"),
    };
    let mut layouts = shell.layouts().unwrap();
    let mut nodes = shell.nodes.into_vec();
    let mut journal = NativeRewriteJournal::default();
    let mut fixed_point = domain_transfer::DomainFixedPoint::default();
    let state = state();
    let state = state.read().unwrap();
    let predicate = equal(10, 0);

    let first = push_domain(
        (&mut nodes, &mut layouts),
        input.clone(),
        vec![predicate.clone()],
        &state,
        &mut journal,
        &mut fixed_point,
        &[],
    )
    .unwrap();
    assert!(first.moved);
    let node_count = nodes.len();

    let second = push_domain(
        (&mut nodes, &mut layouts),
        input,
        vec![predicate],
        &state,
        &mut journal,
        &mut fixed_point,
        &[],
    )
    .unwrap();
    assert!(second.moved);
    assert_eq!(nodes.len(), node_count);
}

#[test]
fn production_selected_binding_drives_native_closure_through_projection_and_aggregate() {
    let count = Expression::Aggregate(
        AggregateExpression::new(
            paro_function::aggregate::distributive::count::get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        )
        .into(),
    );
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
            paro_planner::operator::ExpressionGet::new(
                0,
                vec![],
                vec!["key0".into(), "key1".into()],
                vec![LogicalType::Integer; 2],
            ),
        )),
        vec![column(0, 1)],
        vec![],
        vec![count],
        vec![],
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            20,
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
            vec![column(10, 0)],
        ))),
        vec![equal(20, 0)],
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let bindings = rule
        .selected_quality_bindings(engine.memo(), &root)
        .unwrap();
    assert_eq!(bindings.len(), 1);
    let PatternOperand::Expression {
        children: filter_children,
        ..
    } = &bindings[0].root
    else {
        panic!("production binding root is not a filter");
    };
    let PatternOperand::Expression {
        children: projection_children,
        ..
    } = &filter_children[0]
    else {
        panic!("production binding omitted the projection");
    };
    assert!(matches!(
        &projection_children[0],
        PatternOperand::Expression { .. }
    ));

    let mut context = TransformContext::new(engine.memo_mut(), bindings[0].root_group());
    let outputs = rule.apply_binding(&bindings[0], &mut context).unwrap();
    assert_eq!(outputs.len(), 1, "selected binding did not reach staging");
    let state = state.read().unwrap();
    assert_eq!(
        state.metadata[&outputs[0].payload].output_columns,
        state.metadata[&root.logical.payload].output_columns,
        "native closure changed the selected output contract"
    );
}

#[test]
fn production_binding_records_a_resumable_continuation_at_a_memo_group_hole() {
    let count = Expression::Aggregate(
        AggregateExpression::new(
            paro_function::aggregate::distributive::count::get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        )
        .into(),
    );
    let inner = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        20,
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
            paro_planner::operator::ExpressionGet::new(
                0,
                vec![],
                vec!["key".into()],
                vec![LogicalType::Integer],
            ),
        )),
        vec![column(0, 0)],
    )));
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        inner,
        vec![column(20, 0)],
        vec![],
        vec![count],
        vec![],
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
        vec![equal(10, 0)],
    )));
    let mut input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
    let state = input.planner_state.clone();
    state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());

    let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let aggregate_group = input
        .memo
        .logical_expr(root_expression)
        .unwrap()
        .key
        .children[0];
    let aggregate_expression = input.memo.group(aggregate_group).unwrap().logical_exprs()[0];
    let inner_group = input
        .memo
        .logical_expr(aggregate_expression)
        .unwrap()
        .key
        .children[0];
    // Deliberately leave the inner group opaque.  This is the actual
    // production shape at the boundary: the native closure owns the Filter
    // and outer Aggregate while the next operator must be discovered from
    // Memo.
    let root = PatternOperand::Expression {
        group: input.root,
        expression: root_expression,
        children: Box::new([PatternOperand::Expression {
            group: aggregate_group,
            expression: aggregate_expression,
            children: Box::new([PatternOperand::Group(inner_group)]),
        }]),
    };
    let binding = PatternBinding {
        fingerprint: continuation_binding_fingerprint(
            &root,
            root_expression,
            OptimizationContextId(0),
        ),
        root,
    };
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let mut context = TransformContext::new(&mut input.memo, input.root);
    context.enable_domain_continuations();
    let outputs = rule.apply_binding(&binding, &mut context).unwrap();
    assert_eq!(outputs.len(), 1, "the selected binding must still stage");

    let continuations = context.take_domain_continuations();
    assert_eq!(continuations.len(), 1);
    let continuation = &continuations[0];
    assert_eq!(continuation.hole, inner_group);
    assert_eq!(continuation.occurrence, aggregate_expression);
    assert!(!continuation.predicates.is_empty());
    assert!(continuation
        .reads
        .iter()
        .any(|read| { read.group == inner_group && read.logical_frontier_revision.is_some() }));
    let PatternOperand::Expression {
        children: aggregate_children,
        ..
    } = &continuation.binding.root
    else {
        panic!("continuation lost the root filter");
    };
    let PatternOperand::Expression {
        children: inner_children,
        expression: continued_expression,
        ..
    } = &aggregate_children[0]
    else {
        panic!("continuation did not preserve the projection path");
    };
    assert_eq!(*continued_expression, aggregate_expression);
    assert!(matches!(
        inner_children[0],
        PatternOperand::Expression { .. }
    ));
}

#[test]
fn production_selected_binding_rebinds_each_union_all_branch_before_staging() {
    let left = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![],
            vec!["left_key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let right = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            30,
            vec![],
            vec!["right_key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let union = SetOperation::union(40, left, right, true, vec![LogicalType::Integer]);
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(union)),
        vec![equal(40, 0)],
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let bindings = rule
        .selected_quality_bindings(engine.memo(), &root)
        .unwrap();
    assert_eq!(bindings.len(), 1);
    let PatternOperand::Expression {
        children: filter_children,
        ..
    } = &bindings[0].root
    else {
        panic!("production binding root is not a filter");
    };
    let PatternOperand::Expression {
        children: union_children,
        ..
    } = &filter_children[0]
    else {
        panic!("production binding omitted the UNION ALL");
    };
    assert_eq!(union_children.len(), 2);

    let mut context = TransformContext::new(engine.memo_mut(), bindings[0].root_group());
    let outputs = rule.apply_binding(&bindings[0], &mut context).unwrap();
    assert_eq!(outputs.len(), 1, "UNION ALL binding did not reach staging");
    let state = state.read().unwrap();
    assert_eq!(
        state.metadata[&outputs[0].payload].output_columns,
        state.metadata[&root.logical.payload].output_columns,
        "branch rebinding changed the UNION output contract"
    );
}

#[test]
fn production_selected_binding_reaches_aggregate_input_on_a_join_side() {
    let count = Expression::Aggregate(
        AggregateExpression::new(
            paro_function::aggregate::distributive::count::get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        )
        .into(),
    );
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
            paro_planner::operator::ExpressionGet::new(
                0,
                vec![],
                vec!["key".into()],
                vec![LogicalType::Integer],
            ),
        )),
        vec![column(0, 0)],
        vec![],
        vec![count],
        vec![],
    );
    let join = Join::cross(
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
            paro_planner::operator::ExpressionGet::new(
                30,
                vec![],
                vec!["dimension_key".into()],
                vec![LogicalType::Integer],
            ),
        )),
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(join)),
        vec![equal(10, 0)],
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let bindings = rule
        .selected_quality_bindings(engine.memo(), &root)
        .unwrap();
    assert_eq!(bindings.len(), 1);
    let PatternOperand::Expression {
        children: filter_children,
        ..
    } = &bindings[0].root
    else {
        panic!("production binding root is not a filter");
    };
    let PatternOperand::Expression {
        children: join_children,
        ..
    } = &filter_children[0]
    else {
        panic!("production binding omitted the join");
    };
    assert_eq!(join_children.len(), 2);

    let mut context = TransformContext::new(engine.memo_mut(), bindings[0].root_group());
    let outputs = rule.apply_binding(&bindings[0], &mut context).unwrap();
    assert_eq!(outputs.len(), 1, "join-side binding did not reach staging");
    let state = state.read().unwrap();
    assert_eq!(
        state.metadata[&outputs[0].payload].output_columns,
        state.metadata[&root.logical.payload].output_columns,
        "join-side aggregate rebinding changed the output contract"
    );
}

#[test]
fn domain_union_declines_distinct_and_incomplete_layouts() {
    let state = state();
    let state = state.read().unwrap();
    for (all, width, predicate) in [
        (false, 2, equal(30, 1)),
        (true, 1, equal(30, 1)),
        (true, 2, equal(30, 3)),
        (true, 2, equal(99, 0)),
    ] {
        let union = SetOperation::union(
            30,
            boundary(1, 10, &[7, 2][..width]),
            boundary(2, 20, &[9, 4]),
            all,
            vec![LogicalType::Integer; 2],
        );
        let shell = native(OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
            Filter::new(
                OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(union)),
                vec![predicate],
            ),
        )));
        assert!(transfer_shell(shell, &state).unwrap().is_none());
    }
}

#[test]
fn domain_real_rule_preserves_narrow_root_and_does_not_import_into_settlement() {
    let key = |value| {
        Expression::Constant(
            ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
        )
    };
    let input = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![vec![key(1)], vec![key(2)], vec![key(3)]],
            vec!["key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let count = Expression::Aggregate(
        AggregateExpression::new(
            paro_function::aggregate::distributive::count::get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        )
        .into(),
    );
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        input,
        vec![column(0, 0)],
        vec![],
        vec![count],
        vec![],
    );
    let residual = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(11, 0), LogicalType::BigInt).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::BigInt(0), LogicalType::BigInt).into(),
            ),
        )
        .into(),
    );
    let mut filter = Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
        vec![equal(10, 0), residual],
    );
    filter.projection_map = paro_planner::operator::ProjectionMap::new(vec![1]);
    let mut input = MemoBuilder::build(
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter)),
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap();
    let state = input.planner_state.clone();
    state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    let root = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let child_group = input.memo.logical_expr(root).unwrap().key.children[0];
    let child = input.memo.group(child_group).unwrap().logical_exprs()[0];
    let grandchildren = input
        .memo
        .logical_expr(child)
        .unwrap()
        .key
        .children
        .iter()
        .copied()
        .map(PatternOperand::Group)
        .collect();
    let binding = PatternBinding {
        root: PatternOperand::Expression {
            group: input.root,
            expression: root,
            children: Box::new([PatternOperand::Expression {
                group: child_group,
                expression: child,
                children: grandchildren,
            }]),
        },
        fingerprint: Fingerprint(301),
    };
    let source_columns = state.read().unwrap().metadata
        [&input.memo.logical_expr(root).unwrap().payload]
        .output_columns
        .clone();
    let arena_before = state.read().unwrap().staging_arena.len();
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let outputs = rule.apply_binding(&binding, &mut context).unwrap();
    assert_eq!(
        outputs.len(),
        1,
        "native must not suppress a valid semantic alternative"
    );
    let state = state.read().unwrap();
    assert_eq!(
        state.staging_arena.len(),
        arena_before,
        "native binding must not instantiate/settle an owned operand tree"
    );
    assert_eq!(
        state.metadata[&outputs[0].payload].output_columns,
        source_columns
    );
    let LogicalOperator::Filter(residual) = &state.payloads.logical[outputs[0].payload.index()]
        .semantic_template
        .operator
    else {
        panic!("aggregate output predicate must remain in the returned expression");
    };
    assert!(residual.expressions.iter().all(|expression| {
        let mut group_key = false;
        visit_expression(expression, &mut |part| {
            if let Expression::ColumnRef(column) = part {
                group_key |= column.binding.table_index == 10;
            }
        });
        !group_key
    }));
}

#[test]
fn domain_refresh_prunes_child_filter_to_parent_aggregate_demand() {
    let state = state();
    state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    let mut state = state.write().unwrap();
    let memo = MemoBuilder::build(
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap()
    .memo;
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        boundary(1, 0, &[7, 2]),
        vec![column(0, 7)],
        vec![],
        vec![],
        vec![],
    );
    let shell = native(OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
        Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
            vec![equal(10, 0)],
        ),
    )));
    let shell = transfer_shell(shell, &state).unwrap().unwrap();
    let (shell, _, _) = refresh_statistics(shell, &mut state, &memo)
        .unwrap()
        .unwrap();
    let LogicalOperator::Aggregate(aggregate) = shell.root_operator() else {
        panic!("group-only aggregate must remain the root");
    };
    let NativeChild::Node(filter) = aggregate.child else {
        panic!("transferred predicate must have a local child filter");
    };
    assert!(matches!(
        shell.nodes[filter].operator,
        LogicalOperator::Filter(_)
    ));
    assert_eq!(
        shell.layouts().unwrap()[filter].bindings(),
        &[ColumnBinding::new(0, 7)],
        "child filter output must drop 0:2, which its parent aggregate does not read"
    );
}

#[test]
fn repeated_native_refresh_reuses_relation_facts_without_reusing_stale_stats() {
    let state = state();
    state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    let mut state = state.write().unwrap();
    let memo = MemoBuilder::build(
        OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap()
    .memo;
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        boundary(1, 0, &[7, 2]),
        vec![column(0, 7)],
        vec![],
        vec![],
        vec![],
    );
    let shell = native(OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
        Filter::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate))),
            vec![equal(10, 0)],
        ),
    )));
    let shell = transfer_shell(shell, &state).unwrap().unwrap();
    let (shell, _, _) = refresh_statistics(shell, &mut state, &memo)
        .unwrap()
        .unwrap();
    let evaluations = state.settlement_cache.native_relation_fact_evaluations;
    let hits = state.settlement_cache.native_relation_hits;
    let evidence_reuses = state
        .settlement_cache
        .native_relation_cached_evidence_reuses;
    let view_reuses = state
        .settlement_cache
        .native_relation_ordered_column_view_reuses;
    let (shell, _, _) = refresh_statistics(shell, &mut state, &memo)
        .unwrap()
        .unwrap();
    assert!(
        state.settlement_cache.native_relation_hits > hits,
        "the second occurrence should reuse an immutable relation fact entry"
    );
    assert_eq!(
        state.settlement_cache.native_relation_fact_evaluations, evaluations,
        "a repeated relation must not rerun propagation/gathering"
    );
    assert!(
        state
            .settlement_cache
            .native_relation_cached_evidence_reuses
            > evidence_reuses,
        "a cached relation must reuse its immutable completed evidence"
    );
    assert!(
        state
            .settlement_cache
            .native_relation_ordered_column_view_reuses
            > view_reuses,
        "a repeated parent edge must reuse the completed positional column view"
    );
    assert!(!shell.nodes.is_empty());
}

#[test]
fn production_selected_binding_derives_mixed_aggregate_domain_through_constant_projection() {
    use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
    let constant = |v| {
        Expression::Constant(
            ConstantExpression::new(Value::Integer(v), LogicalType::Integer).into(),
        )
    };
    let compare = |left, right| {
        Expression::Comparison(ComparisonExpression::new(ComparisonType::Equal, left, right).into())
    };
    let and = |children| {
        Expression::Conjunction(ConjunctionExpression::new(ConjunctionType::And, children).into())
    };
    let input = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![vec![constant(1)], vec![constant(2)], vec![constant(3)]],
            vec!["key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let count = Expression::Aggregate(
        AggregateExpression::new(
            paro_function::aggregate::distributive::count::get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        )
        .into(),
    );
    let aggregate =
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
            10,
            11,
            12,
            input,
            vec![column(0, 0)],
            vec![],
            vec![count],
            vec![],
        ))));
    let aggregate_column = Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(11, 0), LogicalType::BigInt).into(),
    );
    let projection = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        20,
        aggregate,
        vec![constant(7), column(10, 0), aggregate_column],
    )));
    let total = Expression::ColumnRef(
        ColumnRefExpression::new(ColumnBinding::new(20, 2), LogicalType::BigInt).into(),
    );
    let positive = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::GreaterThan,
            total,
            Expression::Constant(
                ConstantExpression::new(Value::BigInt(0), LogicalType::BigInt).into(),
            ),
        )
        .into(),
    );
    let predicate = and(vec![
        compare(column(20, 0), constant(7)),
        Expression::Conjunction(
            ConjunctionExpression::new(
                ConjunctionType::Or,
                vec![
                    compare(column(20, 1), constant(2)),
                    and(vec![compare(column(20, 1), constant(1)), positive]),
                ],
            )
            .into(),
        ),
    ]);
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        projection,
        vec![predicate],
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let bindings = rule
        .selected_quality_bindings(engine.memo(), &root)
        .unwrap();
    assert_eq!(
        bindings.len(),
        1,
        "constant output must not block the selected route"
    );
    let before = state.read().unwrap().payloads.logical.len();
    let mut context = TransformContext::new(engine.memo_mut(), bindings[0].root_group());
    let outputs = rule.apply_binding(&bindings[0], &mut context).unwrap();
    assert_eq!(
        outputs.len(),
        1,
        "mixed predicate must produce a staged input restriction"
    );
    let state = state.read().unwrap();
    let mut input_domain = false;
    let mut output_residual = false;
    for payload in &state.payloads.logical[before..] {
        if let LogicalOperator::Filter(filter) = &payload.semantic_template.operator {
            for predicate in &filter.expressions {
                let mut tables = BTreeSet::new();
                visit_expression(predicate, &mut |part| {
                    if let Expression::ColumnRef(column) = part {
                        tables.insert(column.binding.table_index);
                    }
                });
                input_domain |= tables == BTreeSet::from([0]);
                output_residual |= tables.contains(&11);
            }
        }
    }
    assert!(
        input_domain,
        "native publication omitted the necessary source-key restriction"
    );
    assert!(
        output_residual,
        "aggregate result predicate must remain above aggregation"
    );
}

#[test]
fn production_selected_two_domains_publish_one_filter_at_original_hole() {
    use crate::rewrite::expr::traversal::associative_terms;
    use paro_planner::expression::ConjunctionType;

    let value = |value: Option<i32>| {
        Expression::Constant(
            ConstantExpression::new(
                value.map_or(Value::Null(LogicalType::Integer), Value::Integer),
                LogicalType::Integer,
            )
            .into(),
        )
    };
    let source_rows = [
        (Some(2), Some(2)),
        (Some(2), Some(2)),
        (Some(2), Some(3)),
        (Some(3), Some(2)),
        (None, Some(2)),
        (Some(2), None),
        (None, None),
    ];
    let rows = source_rows
        .into_iter()
        .map(|(x, y)| vec![value(x), value(y)])
        .collect();
    let input = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            rows,
            vec!["x".into(), "y".into()],
            vec![LogicalType::Integer; 2],
        ),
    ));
    let aggregate =
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
            10,
            11,
            12,
            input,
            vec![column(0, 0), column(0, 1)],
            vec![],
            vec![],
            vec![],
        ))));
    let projection = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        20,
        aggregate,
        vec![column(10, 0), column(10, 1)],
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        projection,
        vec![equal(20, 0), equal(20, 1)],
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let original_hole = root.children[0].children[0].children[0].reference.group;
    let original_aggregate = root.children[0].children[0].reference.group;
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::PredicateTransfer,
        planner_state: state.clone(),
    };
    let bindings = rule
        .selected_quality_bindings(engine.memo(), &root)
        .unwrap();
    assert_eq!(bindings.len(), 1);
    let mut operand = &bindings[0].root;
    for selected in [&root, &root.children[0], &root.children[0].children[0]] {
        let PatternOperand::Expression {
            expression,
            children,
            ..
        } = operand
        else {
            panic!("selected production binding must retain Filter -> Projection -> Aggregate");
        };
        assert_eq!(*expression, selected.logical.id);
        assert_eq!(children.len(), 1);
        operand = &children[0];
    }
    assert_eq!(*operand, PatternOperand::Group(original_hole));

    let arena_before = state.read().unwrap().staging_arena.len();
    let mut context = TransformContext::new(engine.memo_mut(), bindings[0].root_group());
    let mut outputs = rule
        .apply_binding(&bindings[0], &mut context)
        .unwrap()
        .into_vec();
    assert_eq!(
        outputs.len(),
        1,
        "both domains must land in the same publication"
    );
    let output = outputs.pop().unwrap();
    let published = context
        .memo_mut()
        .insert_logical_with_operator_encoding(
            output.target_group,
            output.key,
            output.payload,
            output.proof,
            output.operator_encoding.unwrap(),
        )
        .unwrap();
    context.commit().unwrap();

    // Follow the returned root's exact edges, not unrelated new payloads or
    // an independently selected physical winner which could hide an extra hop.
    let memo = engine.memo();
    let state = state.read().unwrap();
    assert_eq!(
        state.staging_arena.len(),
        arena_before,
        "the selected long closure must not fall back to owned settlement"
    );
    let projection = memo.logical_expr(published).unwrap();
    assert!(matches!(
        state.payloads.logical[projection.payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::Projection(_)
    ));
    assert_eq!(
        state.metadata[&projection.payload].output_columns,
        state.metadata[&root.logical.payload].output_columns
    );
    let aggregate_group = projection.key.children[0];
    assert_ne!(aggregate_group, original_aggregate);
    let [aggregate_id] = memo.group(aggregate_group).unwrap().logical_exprs() else {
        panic!("new aggregate group must identify the published closure unambiguously");
    };
    let aggregate = memo.logical_expr(*aggregate_id).unwrap();
    assert!(matches!(
        state.payloads.logical[aggregate.payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::Aggregate(_)
    ));
    let [filter_id] = memo
        .group(aggregate.key.children[0])
        .unwrap()
        .logical_exprs()
    else {
        panic!("aggregate must reference one newly published input filter");
    };
    let filter = memo.logical_expr(*filter_id).unwrap();
    let LogicalOperator::Filter(predicate_filter) = &state.payloads.logical[filter.payload.index()]
        .semantic_template
        .operator
    else {
        panic!("aggregate input must be a Filter");
    };
    assert_eq!(
        filter.key.children.as_ref(),
        &[original_hole],
        "one publication must land both domains directly on the original hole, without Filter -> Filter"
    );
    let terms = predicate_filter
        .expressions
        .iter()
        .flat_map(|expression| associative_terms(expression, ConjunctionType::And))
        .collect::<Vec<_>>();
    assert_eq!(terms.len(), 2);
    for ordinal in [0, 1] {
        assert!(
            terms.iter().any(|term| term.equals(&equal(0, ordinal))),
            "published input Filter lost the domain on column {ordinal}"
        );
    }
    // Independently evaluate the actual published predicates on the source
    // bag. Grouping before output filtering and filtering before grouping
    // must agree, including duplicate and NULL input keys.
    let scalar = |expression: &Expression, row: (Option<i32>, Option<i32>)| match expression {
        Expression::ColumnRef(column) => {
            assert_eq!(column.binding.table_index, 0);
            [row.0, row.1][column.binding.column_index]
        }
        Expression::Constant(constant) => match constant.value {
            Value::Integer(value) => Some(value),
            Value::Null(_) => None,
            _ => panic!("unexpected fixture literal"),
        },
        _ => panic!("unexpected fixture scalar"),
    };
    let actual = source_rows
        .into_iter()
        .filter(|row| {
            terms.iter().all(|term| {
                let Expression::Comparison(comparison) = term else {
                    panic!("unexpected published predicate");
                };
                assert_eq!(comparison.comparison_type, ComparisonType::Equal);
                scalar(&comparison.left, *row)
                    .zip(scalar(&comparison.right, *row))
                    .is_some_and(|(left, right)| left == right)
            })
        })
        .collect::<BTreeSet<_>>();
    let expected = source_rows
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|row| *row == (Some(2), Some(2)))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn production_limit_binding_uses_native_shell_without_owned_settlement() {
    let input = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )]],
            vec!["value".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let projection = Projection::new(10, input, vec![column(0, 0)]);
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Limit(Box::new(
        paro_planner::operator::Limit::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::Projection(projection)),
            Some(Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )),
            None,
        ),
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::LimitPushdown,
        planner_state: state.clone(),
    };
    let bindings = rule
        .bindings(
            root.logical.id,
            &RuleContext {
                memo: engine.memo(),
                group: root.reference.group,
            },
        )
        .unwrap();
    assert_eq!(bindings.bindings.len(), 1);
    let arena_before = state.read().unwrap().staging_arena.len();
    let mut context = TransformContext::new(engine.memo_mut(), root.reference.group);
    let outputs = rule
        .apply_binding(&bindings.bindings[0], &mut context)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        state.read().unwrap().staging_arena.len(),
        arena_before,
        "native limit pushdown must not round-trip through the owned arena"
    );
    let state = state.read().unwrap();
    assert!(matches!(
        state.payloads.logical[outputs[0].payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::Projection(_)
    ));
}

#[test]
fn production_topn_binding_uses_native_shell_without_owned_settlement() {
    let input = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )]],
            vec!["value".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let order = paro_planner::operator::Order::new(
        input,
        vec![paro_planner::binder::ir::OrderByNode {
            expression: column(0, 0),
            ascending: true,
            nulls_first: false,
        }],
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Limit(Box::new(
        paro_planner::operator::Limit::new(
            OwnedLogicalPlan::synthetic(LogicalOperator::Order(order)),
            Some(Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )),
            None,
        ),
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let root_expr = {
        let candidates = engine
            .memo()
            .group(root.reference.group)
            .unwrap()
            .logical_exprs()
            .to_vec();
        let state = state.read().unwrap();
        candidates
            .into_iter()
            .find(|expression| {
                let logical = engine.memo().logical_expr(*expression).unwrap();
                matches!(
                    state.payloads.logical[logical.payload.index()]
                        .semantic_template
                        .operator,
                    LogicalOperator::Limit(_)
                )
            })
            .expect("the source LIMIT must remain in the Memo")
    };
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::TopNIntroduction,
        planner_state: state.clone(),
    };
    let bindings = rule
        .bindings(
            root_expr,
            &RuleContext {
                memo: engine.memo(),
                group: root.reference.group,
            },
        )
        .unwrap();
    assert_eq!(bindings.bindings.len(), 1);
    let arena_before = state.read().unwrap().staging_arena.len();
    let mut context = TransformContext::new(engine.memo_mut(), root.reference.group);
    let outputs = rule
        .apply_binding(&bindings.bindings[0], &mut context)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        state.read().unwrap().staging_arena.len(),
        arena_before,
        "native TopN introduction must not round-trip through the owned arena"
    );
    let state = state.read().unwrap();
    assert!(matches!(
        state.payloads.logical[outputs[0].payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::TopN(_)
    ));
}

#[test]
fn production_input_materialization_uses_native_shell_without_owned_settlement() {
    let mut arithmetic = paro_function::scalar::ScalarFunctionSet::new("-".into());
    paro_function::scalar::operators::arithmetic::register_arithmetic_functions(&mut arithmetic);
    let (subtract, _) = arithmetic
        .bind(&[LogicalType::Integer, LogicalType::Integer])
        .unwrap();
    let subtract = paro_function::scalar::BoundScalarFunction::from(subtract)
        .with_error_mode(FunctionErrorMode::Infallible);
    let difference = Expression::Function(
        FunctionExpression::new(
            subtract.clone(),
            vec![column(0, 1), column(0, 2)],
            LogicalType::Integer,
        )
        .into(),
    );
    let right_difference = Expression::Function(
        FunctionExpression::new(
            subtract,
            vec![column(1, 1), column(1, 2)],
            LogicalType::Integer,
        )
        .into(),
    );
    let (sum, _) = paro_function::aggregate::distributive::sum::get_sum_function()
        .bind(&[LogicalType::Integer])
        .unwrap();
    let left = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![],
            vec!["key".into(), "x".into(), "y".into()],
            vec![LogicalType::Integer; 3],
        ),
    ));
    let right = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            1,
            vec![],
            vec!["key".into(), "u".into(), "v".into()],
            vec![LogicalType::Integer; 3],
        ),
    ));
    let join = Join::comparison(
        JoinType::Inner,
        left,
        right,
        vec![paro_planner::operator::JoinCondition::equality(
            column(0, 0),
            column(1, 0),
        )],
    );
    let aggregate = Aggregate::new(
        10,
        11,
        12,
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(join)),
        vec![column(1, 0)],
        vec![],
        vec![
            Expression::Aggregate(
                AggregateExpression::new(sum.clone(), vec![difference], LogicalType::BigInt).into(),
            ),
            Expression::Aggregate(
                AggregateExpression::new(sum, vec![right_difference], LogicalType::BigInt).into(),
            ),
        ],
        vec![],
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(aggregate)));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::AggregateInputMaterialization,
        planner_state: state.clone(),
    };
    let bindings = rule
        .bindings(
            root.logical.id,
            &RuleContext {
                memo: engine.memo(),
                group: root.reference.group,
            },
        )
        .unwrap();
    assert_eq!(bindings.bindings.len(), 1);
    let arena_before = state.read().unwrap().staging_arena.len();
    let mut context = TransformContext::new(engine.memo_mut(), root.reference.group);
    let outputs = rule
        .apply_binding(&bindings.bindings[0], &mut context)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        state.read().unwrap().staging_arena.len(),
        arena_before,
        "aggregate input materialization must not round-trip through the owned arena"
    );
    let state = state.read().unwrap();
    let LogicalOperator::Aggregate(aggregate) = &state.payloads.logical[outputs[0].payload.index()]
        .semantic_template
        .operator
    else {
        panic!("native materialization must preserve the aggregate root");
    };
    assert_eq!(
        aggregate
            .aggregates
            .iter()
            .filter(|expression| {
                let Expression::Aggregate(aggregate) = *expression else {
                    return false;
                };
                matches!(aggregate.children.as_slice(), [Expression::ColumnRef(column)] if column.binding.column_index == 3)
            })
            .count(),
        2,
        "each independent join side must receive its own native materialization"
    );
}

#[test]
fn production_key_domain_binding_rewrites_the_exact_probe_edge_natively() {
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![vec![
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(2), LogicalType::Integer).into(),
                ),
            ]],
            vec!["key".into(), "payload".into()],
            vec![LogicalType::Integer; 2],
        ),
    ));
    let projection = Projection::new(10, probe, vec![column(0, 0), column(0, 1)]);
    let reduction = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            20,
            vec![vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )]],
            vec!["key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let semi = Join::comparison(
        JoinType::Semi,
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(projection)),
        reduction,
        vec![paro_planner::operator::JoinCondition::equality(
            column(10, 0),
            column(20, 0),
        )],
    );
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(semi));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::KeyDomainTransfer,
        planner_state: state.clone(),
    };
    let bindings = rule
        .bindings(
            root.logical.id,
            &RuleContext {
                memo: engine.memo(),
                group: root.reference.group,
            },
        )
        .unwrap();
    assert_eq!(bindings.bindings.len(), 1);
    let arena_before = state.read().unwrap().staging_arena.len();
    let mut context = TransformContext::new(engine.memo_mut(), root.reference.group);
    let outputs = rule
        .apply_binding(&bindings.bindings[0], &mut context)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        state.read().unwrap().staging_arena.len(),
        arena_before,
        "native key-domain transfer must not round-trip through the owned arena"
    );
    let state = state.read().unwrap();
    assert!(matches!(
        state.payloads.logical[outputs[0].payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::Projection(_)
    ));
}

#[test]
fn production_mark_filter_binding_rewrites_without_owned_settlement() {
    let left = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            0,
            vec![vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )]],
            vec!["left_key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let right = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
        paro_planner::operator::ExpressionGet::new(
            1,
            vec![vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            )]],
            vec!["right_key".into()],
            vec![LogicalType::Integer],
        ),
    ));
    let mut mark_join = Join::comparison(
        JoinType::Mark,
        left,
        right,
        vec![paro_planner::operator::JoinCondition::equality(
            column(0, 0),
            column(1, 0),
        )],
    );
    let Join::Comparison(mark) = &mut mark_join else {
        panic!("comparison MARK join");
    };
    mark.mark_index = Some(30);
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(mark_join)),
        vec![Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(30, 0), LogicalType::Boolean).into(),
        )],
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        40,
        plan,
        vec![column(0, 0)],
    )));
    let (mut engine, state, root) = frozen_selected(plan);
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::MarkJoinToSemi,
        planner_state: state.clone(),
    };
    let bindings = rule
        .bindings(
            root.logical.id,
            &RuleContext {
                memo: engine.memo(),
                group: root.reference.group,
            },
        )
        .unwrap();
    assert_eq!(bindings.bindings.len(), 1);
    let arena_before = state.read().unwrap().staging_arena.len();
    let mut context = TransformContext::new(engine.memo_mut(), root.reference.group);
    let outputs = rule
        .apply_binding(&bindings.bindings[0], &mut context)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        state.read().unwrap().staging_arena.len(),
        arena_before,
        "native MARK rewrite must not round-trip through the owned arena"
    );
    let state = state.read().unwrap();
    assert!(matches!(
        state.payloads.logical[outputs[0].payload.index()]
            .semantic_template
            .operator,
        LogicalOperator::Projection(_)
    ));
}
