// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! T0: the same CTE restriction must not acquire different facts merely by
//! crossing the native rather than owned transport boundary.
use super::*;
use paro_common::{runtime_value::Value, types::LogicalType};
use paro_planner::expression::{ComparisonExpression, ConstantExpression};
use paro_planner::operator::{CrossProduct, ExpressionGet, Filter};

fn literal(value: i32) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
    )
}

fn consumer(table: usize) -> OwnedLogicalPlan {
    let reference = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
        9,
        table,
        "shared".into(),
        vec!["key".into()],
        vec![LogicalType::Integer],
    )));
    let predicate = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer).into(),
            ),
            literal(1),
        )
        .into(),
    );
    OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        reference,
        vec![predicate],
    )))
}

fn assert_same_column(
    actual: &SharedColumnStatistics,
    expected: &SharedColumnStatistics,
    binding: ColumnBinding,
    label: &str,
    failures: &mut Vec<String>,
) {
    let expected = expected
        .get(&binding)
        .expect("owned oracle must contain the consumed column");
    let Some(actual) = actual.get(&binding) else {
        failures.push(format!("{label}: missing scoped column {binding:?}"));
        return;
    };
    if actual.to_bytes().unwrap() != expected.to_bytes().unwrap()
        || actual.distinct_evidence() != expected.distinct_evidence()
        || actual.estimated_numeric_distribution() != expected.estimated_numeric_distribution()
        || actual.is_storage_observation() != expected.is_storage_observation()
    {
        failures.push(format!(
            "{label}: column {binding:?}: native {:?}, owned {:?}",
            actual.distinct_evidence(),
            expected.distinct_evidence()
        ));
    }
}

#[test]
fn production_cte_filter_transport_preserves_producer_scoped_facts() {
    let session = paro_context::TestStatementContextBuilder::minimal().build();
    let bind = BindContext::new();
    for _ in 0..32 {
        bind.generate_table_index();
    }
    let producer = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        0,
        (0..40).map(|row| vec![literal(row % 4)]).collect(),
        vec!["key".into()],
        vec![LogicalType::Integer],
    )));
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
        9,
        "shared".into(),
        vec!["key".into()],
        vec![LogicalType::Integer],
        CTEMaterialize::Default,
        producer,
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
            consumer(10),
            consumer(11),
        )))),
    )));
    let mut optimization = crate::context::OptimizationContext::new(session.clone(), bind.clone());
    let plan = StatisticsGathering::new()
        .gather(plan, &mut optimization)
        .unwrap();
    // ExpressionGet cardinality is gathered, but its literal NDV is not a
    // storage observation. Supply the fixture's independently known input
    // fact rather than relying on the generic unknown-column default.
    for table in [0, 10, 11] {
        optimization.column_stats_mut().insert(
            ColumnBinding::new(table, 0),
            Arc::new(ColumnStatistics::with_estimated_distinct(
                paro_storage::statistics::BaseStatistics::create_unknown(LogicalType::Integer),
                Some(4),
            )),
        );
    }
    let LogicalOperator::MaterializedCTE(owner) = &plan.operator else {
        unreachable!()
    };
    assert_eq!(
        owner
            .cte_query
            .stats
            .estimated_cardinality
            .unwrap()
            .expected,
        40
    );
    assert!(
        optimization.column_stats[&ColumnBinding::new(0, 0)]
            .distinct_evidence()
            .point
            > 1,
        "fixture must start with a nontrivial producer domain"
    );
    let mut input = MemoBuilder::build_alternatives(
        vec![LogicalAlternative {
            plan,
            source: AlternativeOrigin::Baseline,
            column_stats: optimization.column_stats.clone(),
        }],
        bind,
        SearchBudget::default(),
    )
    .unwrap();
    let planner_state = input.planner_state.clone();
    planner_state.write().unwrap().session = Some(session.clone());
    let binding = {
        let state = planner_state.read().unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let bindings = matching::scoped_pattern_bindings(
            PlannerTransformation::CteFilterPushdown,
            input.root,
            expression,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap();
        assert_eq!(bindings.completion, PatternEnumerationCompletion::Complete);
        assert_eq!(bindings.bindings.len(), 1);
        bindings.bindings[0].clone()
    };
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let (native, oracle, scopes) = {
        let mut state = planner_state.write().unwrap();
        let facts = boundary::BoundarySnapshot::read(
            &mut context,
            &state,
            &binding.root,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let requirement = CteRequirement::from_binding(&binding, context.memo(), &state).unwrap();
        assert_eq!(requirement.occurrences.len(), 2);
        let instantiated = semantic_plan::instantiate_bound_plan_with_group_holes(
            context.memo(),
            &state,
            &binding.root,
            Some(&facts),
        )
        .unwrap()
        .unwrap();
        let (shell, layouts) =
            NativeShell::from_pattern_with_layouts(context.memo(), &state, &binding.root, &facts)
                .unwrap()
                .unwrap();
        let (native, native_proof) = requirement
            .native_filter_domain(shell, &layouts, context.memo(), &mut state)
            .unwrap()
            .unwrap();
        let (restricted, proof) = requirement
            .restrict_predicate_domain(instantiated.plan, context.memo(), &state)
            .unwrap()
            .unwrap();
        assert_eq!(native_proof.fingerprint, proof.fingerprint);
        let restricted = requirement
            .close_domain(restricted, &proof, context.memo(), &mut state)
            .unwrap();
        let environment = PlannerRuleEnvironment {
            control: context.memo().control().clone(),
            bind_context: state.bind_context.clone(),
            session,
            cost_model: state.cost_model.clone(),
            budget: context.memo().budget().clone(),
            verify_enabled: false,
        };
        // Separate oracle storage: do not seed the production settlement cache
        // or staging arena with the answer before apply_binding is exercised.
        let mut cache = settlement::SettlementCache::default();
        let mut arena = LogicalPlanArena::default();
        let settled = cache
            .settle_arena_test_in(restricted, &environment, &mut arena)
            .unwrap()
            .unwrap();
        (native, arena.export(settled.plan).unwrap(), settled.scopes)
    };
    let LogicalOperator::MaterializedCTE(owned_owner) = &oracle.operator else {
        unreachable!()
    };
    let LogicalOperator::MaterializedCTE(native_owner) = native.root_operator() else {
        unreachable!()
    };
    assert_eq!(native_owner.cte_index, owned_owner.cte_index);
    let producer_rows = owned_owner.cte_query.stats.estimated_cardinality.unwrap();
    assert!(
        producer_rows.expected > 0 && producer_rows.expected < 40,
        "oracle must actually narrow the known producer: {producer_rows:?}"
    );
    assert!(matches!(
        owned_owner.child.operator,
        LogicalOperator::Join(_)
    ));
    let mut oracle_refs = BTreeMap::new();
    let mut pending = vec![owned_owner.child.as_ref()];
    while let Some(node) = pending.pop() {
        if let LogicalOperator::CTERef(reference) = &node.operator {
            assert_eq!(reference.cte_index, owned_owner.cte_index);
            assert_eq!(
                node.stats.estimated_cardinality,
                Some(producer_rows),
                "owned CTERef must consume its lexical producer fact"
            );
            oracle_refs.insert(reference.table_index, node);
        }
        pending.extend(node.children());
    }
    assert_eq!(oracle_refs.len(), 2);
    // Raw producer output is diagnostic only: a future common bridge may
    // legitimately refresh it between construction and production staging.
    let mut construction_differences = Vec::new();
    let NativeChild::Node(producer_index) = &native_owner.cte_query else {
        unreachable!()
    };
    if native.nodes[*producer_index].stats.estimated_cardinality != Some(producer_rows) {
        construction_differences.push(format!(
            "producer rows: native {:?}, owned {:?}",
            native.nodes[*producer_index].stats.estimated_cardinality, producer_rows
        ));
    }
    for node in &native.nodes {
        if let LogicalOperator::CTERef(reference) = &node.operator {
            if node.stats != oracle_refs[&reference.table_index].stats {
                construction_differences.push(format!(
                    "CTERef {} retained pre-restriction stats: {:?}",
                    reference.table_index, node.stats
                ));
            }
        }
    }
    let NativeChild::Node(consumer_index) = &native_owner.child else {
        unreachable!()
    };
    if native.nodes[*consumer_index].stats != owned_owner.child.stats {
        construction_differences
            .push("consumer join derived stats differ from owned settlement".into());
    }

    // Actual production entry and newly staged logical payloads, not a
    // hand-written StagingRequest. Follow only this returned alternative.
    let rule = PlannerTransformationRule {
        transformation: PlannerTransformation::CteFilterPushdown,
        planner_state: planner_state.clone(),
    };
    let outputs = rule.apply_binding(&binding, &mut context).unwrap();
    assert_eq!(outputs.len(), 1);
    let mut failures = Vec::new();
    let state = planner_state.read().unwrap();
    let producer_group = outputs[0].key.children[0];
    let mut pending = vec![(
        input.root,
        outputs[0].payload,
        outputs[0].key.children.to_vec(),
    )];
    let mut seen = BTreeSet::new();
    let mut references_checked = 0;
    let mut joins_checked = 0;
    let mut producers_checked = 0;
    while let Some((group_id, payload_id, children)) = pending.pop() {
        if !seen.insert(payload_id) {
            continue;
        }
        let payload = &state.payloads.logical[payload_id.index()];
        match &payload.semantic_template.operator {
            LogicalOperator::CTERef(reference) if reference.cte_index == owned_owner.cte_index => {
                let expected = oracle_refs[&reference.table_index];
                let rows = context.memo().cardinality_estimate(group_id).unwrap().1;
                if rows != expected.stats.estimated_cardinality.unwrap().expected {
                    failures.push(format!(
                        "CTERef {} staged rows {rows}, owned {:?}",
                        reference.table_index, expected.stats.estimated_cardinality
                    ));
                }
                assert_same_column(
                    &payload.column_stats,
                    &scopes[&expected.id],
                    ColumnBinding::new(reference.table_index, 0),
                    "CTERef payload",
                    &mut failures,
                );
                references_checked += 1;
            }
            LogicalOperator::Join(_) => {
                let rows = context.memo().cardinality_estimate(group_id).unwrap().1;
                if rows
                    != owned_owner
                        .child
                        .stats
                        .estimated_cardinality
                        .unwrap()
                        .expected
                {
                    failures.push(format!(
                        "consumer join staged rows {rows}, owned {:?}",
                        owned_owner.child.stats.estimated_cardinality
                    ));
                }
                for table in [10, 11] {
                    assert_same_column(
                        &payload.column_stats,
                        &scopes[&owned_owner.child.id],
                        ColumnBinding::new(table, 0),
                        "consumer join payload",
                        &mut failures,
                    );
                }
                joins_checked += 1;
            }
            LogicalOperator::Filter(_) if group_id == producer_group => {
                let rows = context.memo().cardinality_estimate(group_id).unwrap().1;
                if rows != producer_rows.expected {
                    failures.push(format!(
                        "producer staged rows {rows}, owned {producer_rows:?}"
                    ));
                }
                assert_same_column(
                    &payload.column_stats,
                    &scopes[&owned_owner.cte_query.id],
                    ColumnBinding::new(0, 0),
                    "producer Filter payload",
                    &mut failures,
                );
                producers_checked += 1;
            }
            _ => {}
        }
        for group in children {
            let expressions = context.memo().group(group).unwrap().logical_exprs();
            assert_eq!(
                expressions.len(),
                1,
                "fixture path must identify an exact staged child"
            );
            let expression = context.memo().logical_expr(expressions[0]).unwrap();
            pending.push((group, expression.payload, expression.key.children.to_vec()));
        }
    }
    assert_eq!(
        (producers_checked, references_checked, joins_checked),
        (1, 2, 1),
        "unexpected staged path: {failures:?}"
    );
    drop(state);
    context.rollback().unwrap();
    assert!(
        failures.is_empty(),
        "transport-dependent CTE facts:\n{}\nconstruction diagnostics:\n{}",
        failures.join("\n"),
        construction_differences.join("\n")
    );
}
