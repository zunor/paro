// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_common::types::LogicalType;
use paro_planner::expression::{ColumnRefExpression, ConstantExpression};
use paro_planner::operator::{Projection, SetOpType, SetOperation};

fn source(table: usize) -> OwnedLogicalPlan {
    let mut plan = super::super::tests::test_base_get(table, table as u64 + 1, "facts", 10);
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10));
    plan
}

fn project(plan: OwnedLogicalPlan, table: usize) -> OwnedLogicalPlan {
    let binding = plan.get_column_bindings()[0];
    OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        table,
        plan,
        vec![Expression::ColumnRef(
            ColumnRefExpression::new(binding, LogicalType::Integer).into(),
        )],
    )))
}

fn input(plan: OwnedLogicalPlan, budget: SearchBudget) -> OptimizationInput {
    MemoBuilder::build(plan, BindContext::new(), budget).unwrap()
}

#[test]
fn native_predicate_statistics_follow_observed_input_facts_not_payload_snapshots() {
    use paro_planner::expression::{ComparisonExpression, ComparisonType};
    use paro_planner::operator::{Filter, Get};
    let binding = ColumnBinding::new(0, 0);
    let predicate = Expression::Comparison(
        ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Integer).into()),
            Expression::Constant(
                ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
            ),
        )
        .into(),
    );
    let mut source = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
        Get::new_without_table(0, vec!["k".into()], vec![LogicalType::Integer]),
    )));
    source.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10));
    let mut input = input(
        OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            source,
            vec![predicate],
        ))),
        SearchBudget::default(),
    );
    let root_expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let expression = input.memo.logical_expr(root_expression).unwrap();
    let child = expression.key.children[0];
    let scalar = expression.key.scalars[0];
    let payload = expression.payload;
    let column = input.memo.group(child).unwrap().schema.columns()[0].id;
    let pattern = PatternOperand::Expression {
        group: input.root,
        expression: root_expression,
        children: Box::new([PatternOperand::Group(child)]),
    };
    // This stale sidecar describes neither observation below. Reading it
    // instead of the subscribed Memo input would always estimate 1/100.
    input.planner_state.write().unwrap().payloads.logical[payload.index()].column_stats =
        Arc::new(HashMap::from([(
            binding,
            Arc::new(ColumnStatistics::with_estimated_distinct(
                paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
                Some(100),
            )),
        )]));
    let state = input.planner_state.read().unwrap();
    let mut previous_reads = Vec::<PatternRead>::new();
    for point in [2, 5] {
        input
            .memo
            .group_mut(child)
            .unwrap()
            .logical_properties
            .column_domains
            .insert(column, GroupColumnDomain::new(Some(point), None).unwrap());
        if !previous_reads.is_empty() {
            assert!(previous_reads
                .iter()
                .any(|read| !read.is_current(&input.memo).unwrap()));
        }
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let snapshot = BoundarySnapshot::read(
            &mut context,
            &state,
            &pattern,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let columns = &snapshot.groups[&context.memo().canonical_group(child)];
        let native = state
            .cost_model
            .estimate_native_selectivity(
                scalar,
                &state.scalars,
                &state.binding_ids,
                |column| {
                    Some(crate::cost_model::ColumnPredicateEvidence {
                        point: columns
                            .column_domains
                            .get(&column)
                            .and_then(|domain| domain.expected()),
                        values: columns
                            .column_values
                            .get(&column)
                            .map(|values| values.statistics()),
                        distribution: columns
                            .column_values
                            .get(&column)
                            .and_then(|values| values.distribution()),
                    })
                },
                || Ok(true),
            )
            .unwrap()
            .unwrap();
        assert_eq!(native, 1.0 / point as f64);
        previous_reads = context.take_fact_reads();
        assert!(previous_reads.iter().any(|read| read.group == child));
    }
    assert!(!BoundarySnapshot::default().groups.contains_key(&child));
}

#[test]
fn boundary_transports_are_typed_immutable_views_of_one_fact_value() {
    let mut input = input(project(source(0), 9), SearchBudget::default());
    let expr = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let logical = input.memo.logical_expr(expr).unwrap();
    let child = logical.key.children[0];
    let column = input.memo.group(child).unwrap().schema.columns()[0].id;
    let state = input.planner_state.read().unwrap();
    let layout = state.metadata[&logical.payload].child_layouts[0].clone();
    let binding = PatternOperand::Expression {
        group: input.root,
        expression: expr,
        children: Box::new([PatternOperand::Group(child)]),
    };
    let mut prior = None;
    for point in [3, 7] {
        input
            .memo
            .group_mut(child)
            .unwrap()
            .logical_properties
            .column_domains
            .insert(column, GroupColumnDomain::new(Some(point), None).unwrap());
        let mut context = TransformContext::new(&mut input.memo, input.root);
        let snapshot = BoundarySnapshot::read(
            &mut context,
            &state,
            &binding,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let transport = snapshot
            .transport(context.memo(), &state, child, &layout)
            .unwrap();
        let same_layout = Arc::new(layout.as_ref().clone());
        let reused = snapshot
            .transport(context.memo(), &state, child, &same_layout)
            .unwrap();
        assert!(
            Arc::ptr_eq(&transport, &reused),
            "layout value, not its allocation, owns transport identity"
        );
        assert!(Arc::ptr_eq(
            &transport.column_statistics()[0],
            &reused.column_statistics()[0]
        ));
        assert_eq!(
            transport.column_statistics()[0].distinct_evidence().point,
            point
        );
        if let Some(prior) = prior {
            assert!(!Arc::ptr_eq(&transport, &prior));
            assert_eq!(prior.column_statistics()[0].distinct_evidence().point, 3);
        }
        prior = Some(transport);
    }
}

#[test]
fn native_column_evidence_keeps_distribution_without_an_ndv_point() {
    use paro_storage::statistics::{BaseStatistics, EstimatedNumericDistribution};
    let distribution = EstimatedNumericDistribution::normal(30.0, 4.0).unwrap();
    let column = ColumnStatistics::with_estimated_distinct(
        BaseStatistics::create_empty(LogicalType::Double),
        None,
    )
    .with_estimated_numeric_distribution(Some(distribution));
    let id = ColumnId(3);
    let facts = GroupFacts::from(GroupFactValue {
        column_values: BTreeMap::from([(
            id,
            paro_planner::operator::bound_reference::BoundColumnValues::from_column(&column)
                .unwrap(),
        )]),
        ..Default::default()
    });
    assert!(!facts.column_domains.contains_key(&id));
    let values = &facts.column_values[&id];
    assert_eq!(values.statistics().get_type(), &LogicalType::Double);
    assert_eq!(values.distribution(), Some(distribution));
    assert!(!facts.column_values.contains_key(&ColumnId(4)));
}

#[test]
fn boundary_value_identity_retains_which_operand_owns_each_fact() {
    let mut memo = Memo::new(SearchBudget::default());
    let groups = (0..2)
        .map(|_| {
            memo.create_group(
                GroupSchema::new([]).unwrap(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            )
        })
        .collect::<Vec<_>>();
    let binding = PatternOperand::Expression {
        group: groups[0],
        expression: LogicalExprId(0),
        children: vec![PatternOperand::Group(groups[1])].into_boxed_slice(),
    };
    let facts = |rows| {
        Arc::new(GroupFacts::from(GroupFactValue {
            cardinality: Some(CardinalityEnvelope {
                lower: rows,
                expected_lower: rows,
                expected_upper: rows,
                upper: rows,
            }),
            ..Default::default()
        }))
    };
    let (small, large) = (facts(1), facts(100));
    let before = BoundarySnapshot {
        groups: BTreeMap::from([(groups[0], small.clone()), (groups[1], large.clone())]),
    };
    let after = BoundarySnapshot {
        groups: BTreeMap::from([(groups[0], large), (groups[1], small)]),
    };
    assert_ne!(
        before.binding_value_fingerprint(&memo, &binding).unwrap(),
        after.binding_value_fingerprint(&memo, &binding).unwrap()
    );
}

#[test]
fn metadata_clones_share_the_aligned_child_schema() {
    let input = input(project(source(0), 1), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let metadata = &state.metadata[&input.memo.logical_expr(expression).unwrap().payload];
    let cloned = metadata.clone();
    assert!(Arc::ptr_eq(
        &metadata.child_layouts[0],
        &cloned.child_layouts[0]
    ));
    assert_eq!(
        cloned.child_layouts[0].bindings(),
        &[ColumnBinding::new(0, 0)]
    );
    assert_eq!(cloned.child_layouts[0].types(), &[LogicalType::Integer]);
}

#[test]
fn narrow_key_facet_preserves_null_domains_when_outputs_are_permuted_or_pruned() {
    let facts = GroupFacts::from(GroupFactValue {
        unique_keys: BTreeSet::from([
            vec![ColumnId(1)].into_boxed_slice(),
            vec![ColumnId(1), ColumnId(2)].into_boxed_slice(),
        ]),
        grouping_unique_keys: BTreeSet::from([vec![ColumnId(2)].into_boxed_slice()]),
        ..Default::default()
    });
    for columns in [
        vec![ColumnId(1), ColumnId(2)],
        vec![ColumnId(2), ColumnId(1)],
        vec![ColumnId(1)],
        vec![],
    ] {
        let layout = paro_planner::operator::LogicalOutputLayout::new(
            vec![LogicalType::Integer; columns.len()],
            columns
                .iter()
                .map(|column| ColumnBinding::new(7, column.index()))
                .collect(),
        );
        let keys = facts.keys_in_layout(&layout, &columns);
        assert_eq!(
            keys.len(),
            match columns.len() {
                2 => 3,
                1 => 1,
                _ => 0,
            }
        );
        for key in keys {
            let referenced = key
                .columns
                .iter()
                .map(|column| {
                    assert_eq!(layout.bindings()[column.output_index], column.binding);
                    column.binding.column_index
                })
                .collect::<Vec<_>>();
            assert_eq!(
                key.null_semantics,
                if referenced == [2] {
                    UniqueKeyNullSemantics::NullsEqual
                } else {
                    UniqueKeyNullSemantics::NullsDistinct
                }
            );
        }
    }
}

#[test]
fn boundary_value_identity_includes_grouping_proofs_and_finite_domains() {
    let group = GroupId(0);
    let encoded = |facts| {
        let snapshot = BoundarySnapshot {
            groups: BTreeMap::from([(group, Arc::new(GroupFacts::from(facts)))]),
        };
        let mut encoder = StableFingerprintBuilder::recording();
        snapshot.encode_group(group, &mut encoder).unwrap();
        encoder.finish_recording().1
    };
    let unknown = encoded(GroupFactValue::default());
    let grouping = encoded(GroupFactValue {
        grouping_unique_keys: BTreeSet::from([vec![ColumnId(1)].into_boxed_slice()]),
        ..Default::default()
    });
    let domain = encoded(GroupFactValue {
        grouping_domains: BTreeMap::from([(
            ColumnId(1),
            BTreeSet::from([SafeGroupingValue::Integer(5)]),
        )]),
        ..Default::default()
    });
    assert_ne!(unknown, grouping);
    assert_ne!(unknown, domain);
    assert_ne!(grouping, domain);
}

#[test]
fn immutable_fact_identity_is_computed_once_and_keeps_the_exact_encoding_contract() {
    let facts = GroupFacts::from(GroupFactValue {
        grouping_domains: BTreeMap::from([(
            ColumnId(1),
            BTreeSet::from([SafeGroupingValue::Varchar("known".into())]),
        )]),
        ..Default::default()
    });
    assert!(facts.fingerprint.get().is_none());
    let mut direct = StableFingerprintBuilder::default();
    direct.write_bytes(b"paro.memo.boundary-value.v1");
    facts.value.encode(&mut direct);
    let expected = direct.finish();
    for _ in 0..1_000 {
        assert_eq!(facts.fingerprint(), expected);
        assert_eq!(facts.fingerprint.get(), Some(&expected));
    }
}

fn grouped_branch_with_tag(
    source_table: usize,
    output_table: usize,
    tag: &str,
) -> OwnedLogicalPlan {
    let aggregate = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(
        paro_planner::operator::Aggregate::new(
            output_table + 10,
            output_table + 11,
            output_table + 12,
            source(source_table),
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(source_table, 0), LogicalType::Integer)
                    .into(),
            )],
            vec![],
            vec![],
            vec![],
        ),
    )));
    OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        output_table,
        aggregate,
        vec![
            Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(output_table + 10, 0),
                    LogicalType::Integer,
                )
                .into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::Varchar(tag.to_string()), LogicalType::Varchar)
                    .into(),
            ),
        ],
    )))
}

#[test]
fn native_boundary_retains_alias_lineage_and_records_inherited_statistics() {
    let mut input = input(project(source(0), 1), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        snapshot
            .cardinality(context.memo(), input.root)
            .unwrap()
            .expected,
        10
    );
    let sources = snapshot.groups[&input.root]
        .lineage
        .values()
        .next()
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].source, 0);
    let source_group = GroupId::new(sources[0].occurrence);
    let first_work = context
        .memo()
        .group(input.root)
        .unwrap()
        .ledger
        .consumed(BudgetDimension::RuleWorkPerGroup);
    let reused = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(Arc::ptr_eq(
        &snapshot.groups[&input.root],
        &reused.groups[&input.root]
    ));
    let second_work = context
        .memo()
        .group(input.root)
        .unwrap()
        .ledger
        .consumed(BudgetDimension::RuleWorkPerGroup)
        - first_work;
    assert!(
        second_work < first_work,
        "a current read log must not rebuild the evidence recipes: {first_work} -> {second_work}"
    );
    let reads = context.take_fact_reads();
    assert!(reads.iter().any(|read| read.group == source_group));
    let root_read = PatternRead::facts_from_group(context.memo(), input.root).unwrap();
    drop(context);
    input.memo.group_mut(source_group).unwrap().cardinality = GroupCardinality::new(
        Fingerprint(9),
        CardinalityRecipeKind::Statistics,
        20,
        20,
        20,
    );
    assert!(root_read.is_current(&input.memo).unwrap());
    assert!(reads
        .iter()
        .any(|read| !read.is_current(&input.memo).unwrap()));
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let refreshed = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(!Arc::ptr_eq(
        &snapshot.groups[&input.root],
        &refreshed.groups[&input.root]
    ));
    assert_eq!(
        refreshed
            .cardinality(context.memo(), input.root)
            .unwrap()
            .expected,
        20
    );
}

#[test]
fn projection_domains_and_lineage_consume_native_operands_not_extraction_scalars() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        1,
        source(0),
        vec![
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
            ),
            Expression::Constant(
                ConstantExpression::new(Value::Varchar("native".into()), LogicalType::Varchar)
                    .into(),
            ),
        ],
    )));
    let mut input = input(plan, SearchBudget::default());
    let mut state = input.planner_state.write().unwrap();
    let expr = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let payload = input.memo.logical_expr(expr).unwrap().payload;
    let output_columns = state.metadata[&payload].output_columns.clone();
    // Deliberately poison the extraction-only scalar carrier. These two fact
    // recipes must use the immutable ColumnId/ScalarExprId operands instead.
    let LogicalOperator::Projection(projection) = &mut state.payloads.logical[payload.index()]
        .semantic_template
        .operator
    else {
        panic!("projection fixture")
    };
    projection.expressions = vec![
        Expression::Constant(
            ConstantExpression::new(Value::Integer(99), LogicalType::Integer).into(),
        ),
        Expression::Constant(
            ConstantExpression::new(Value::Varchar("carrier".into()), LogicalType::Varchar).into(),
        ),
    ];
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let facts = &snapshot.groups[&input.root];
    assert_eq!(
        facts.grouping_domains[&output_columns[1]],
        BTreeSet::from([SafeGroupingValue::Varchar("native".into())])
    );
    let lineage = facts.lineage[&output_columns[0]].as_ref().unwrap();
    assert_eq!(lineage.len(), 1);
    assert_eq!(lineage[0].source, 0);
}

#[test]
fn outer_column_with_a_matching_binding_is_not_a_local_lineage_proof() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        1,
        source(0),
        vec![Expression::ColumnRef(
            ColumnRefExpression::with_depth(ColumnBinding::new(0, 0), LogicalType::Integer, 1)
                .into(),
        )],
    )));
    let mut input = input(plan, SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let facts = &snapshot.groups[&input.root];
    assert!(facts.lineage.values().all(Option::is_none));
    assert!(facts.grouping_domains.is_empty());
}

#[test]
fn value_domain_change_invalidates_the_fact_value_not_only_the_read_cursor() {
    use paro_planner::operator::bound_reference::BoundColumnValues;
    use paro_storage::statistics::BaseStatistics;
    let mut input = input(source(0), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let column = *input
        .memo
        .group(input.root)
        .unwrap()
        .schema
        .ids()
        .first()
        .unwrap();
    let mut values = Vec::new();
    for constant in [2001, 2002, 2001] {
        input
            .memo
            .group_mut(input.root)
            .unwrap()
            .logical_properties
            .column_values
            .insert(
                column,
                BoundColumnValues::new(BaseStatistics::from_constant(&Value::Integer(constant)))
                    .unwrap(),
            );
        let mut ctx = TransformContext::new(&mut input.memo, input.root);
        let snapshot = BoundarySnapshot::read(
            &mut ctx,
            &state,
            &PatternOperand::Group(input.root),
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap()
        .unwrap();
        let mut fingerprint = StableFingerprintBuilder::default();
        snapshot.encode_group(input.root, &mut fingerprint).unwrap();
        values.push(fingerprint.finish());
    }
    assert_ne!(values[0], values[1]);
    assert_eq!(values[0], values[2]);
}

#[test]
fn finite_replay_proof_survives_a_recursive_identity_alternative() {
    let mut input = input(source(0), SearchBudget::default());
    let mut state = input.planner_state.write().unwrap();
    let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
    let original = input.memo.logical_expr(expression).unwrap().payload;
    let mut metadata = state.metadata[&original].clone();
    metadata.operator_type = paro_planner::operator::LogicalOperatorType::Filter;
    metadata.child_layouts =
        Box::new([Arc::new(paro_planner::operator::LogicalOutputLayout::new(
            vec![LogicalType::Integer],
            vec![ColumnBinding::new(0, 0)],
        ))]);
    let column_stats = state.payloads.logical[original.index()]
        .column_stats
        .clone();
    let (payload, _) = state.payloads.push_logical(PlannerLogicalPayload {
        scalar_facts: Default::default(),
        semantic_template: paro_planner::plan::arena::LogicalPlanNode::from_shell(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(
                paro_planner::operator::Filter::new(source(0), vec![]),
            )),
        ),
        operator_encoding: Box::new([]),
        column_stats,
    });
    state.metadata.insert(payload, metadata);
    input
        .memo
        .insert_logical(
            input.root,
            LogicalExprKey {
                operator: Fingerprint(9191),
                scalars: Box::new([]),
                children: Box::new([input.root]),
            },
            payload,
            EquivalenceProof::Normalization { rule: RuleId(9191) },
        )
        .unwrap();
    let mut ctx = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut ctx,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(
        snapshot.groups[&input.root].can_replay,
        "unknown evidence from an identity cycle cannot refute a finite proof"
    );
}

#[test]
fn aggregate_key_is_derived_from_native_shell_without_cached_plan_statistics() {
    let aggregate = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(
        paro_planner::operator::Aggregate::new(
            1,
            2,
            3,
            source(0),
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
            )],
            vec![],
            vec![],
            vec![],
        ),
    )));
    assert!(aggregate.stats.unique_keys.is_empty());
    let mut input = input(project(aggregate, 4), SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let layout = Arc::new(paro_planner::operator::LogicalOutputLayout::new(
        vec![LogicalType::Integer],
        vec![ColumnBinding::new(4, 0)],
    ));
    let transported = snapshot
        .transport(context.memo(), &state, input.root, &layout)
        .unwrap();
    assert_eq!(transported.unique_keys.len(), 1);
    assert_eq!(
        transported.unique_keys[0].columns[0].binding,
        layout.bindings()[0]
    );
    assert_eq!(
        transported.unique_keys[0].provenance,
        UniqueKeyProvenance::Structural
    );
    assert_eq!(
        transported.unique_keys[0].null_semantics,
        UniqueKeyNullSemantics::NullsEqual
    );
}

#[test]
fn native_group_hole_does_not_publish_null_extended_grouping_keys() {
    use paro_planner::operator::{Aggregate, ComparisonJoin, Join, JoinCondition, JoinType};
    let aggregate = |table| {
        OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
            table + 1,
            table + 2,
            table + 3,
            source(table),
            vec![Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Integer).into(),
            )],
            vec![],
            vec![],
            vec![],
        ))))
    };
    let join = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Left,
            aggregate(0),
            aggregate(10),
            vec![JoinCondition::equality(
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Integer).into(),
                ),
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(11, 0), LogicalType::Integer)
                        .into(),
                ),
            )],
        ),
    )));
    let mut input = input(join, SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut ctx = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut ctx,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let transported = snapshot
        .transport(
            ctx.memo(),
            &state,
            input.root,
            &Arc::new(paro_planner::operator::LogicalOutputLayout::new(
                vec![LogicalType::Integer, LogicalType::Integer],
                vec![ColumnBinding::new(1, 0), ColumnBinding::new(11, 0)],
            )),
        )
        .unwrap();
    assert_eq!(transported.grouping_unique_keys.len(), 1);
    assert_eq!(
        transported.grouping_unique_keys[0].columns[0].binding,
        ColumnBinding::new(1, 0)
    );
    assert!(transported
        .unique_keys
        .iter()
        .any(|key| key.columns[0].binding == ColumnBinding::new(11, 0)
            && key.null_semantics == UniqueKeyNullSemantics::NullsDistinct));
}

#[test]
fn group_boundary_never_unions_coverage_from_different_alternatives() {
    let mut input = MemoBuilder::build_alternatives(
        vec![
            LogicalAlternative {
                plan: project(source(0), 2),
                source: AlternativeOrigin::Baseline,
                column_stats: Arc::new(HashMap::new()),
            },
            LogicalAlternative {
                plan: project(source(1), 2),
                source: AlternativeOrigin::Specialized {
                    rule: TOP_N_INTRODUCTION_RULE,
                },
                column_stats: Arc::new(HashMap::new()),
            },
        ],
        BindContext::new(),
        SearchBudget::default(),
    )
    .unwrap();
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(snapshot.groups[&input.root]
        .lineage
        .values()
        .all(Option::is_none));
}

#[test]
fn repeated_union_occurrences_cannot_claim_one_source_work_identity() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
        1,
        source(0),
        source(0),
        SetOpType::Union,
        true,
        vec![LogicalType::Integer],
    )));
    let mut input = input(plan, SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert!(snapshot.groups[&input.root]
        .lineage
        .values()
        .all(Option::is_none));
}

#[test]
fn disjoint_finite_grouping_domains_make_union_all_key_composable() {
    let plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
        30,
        grouped_branch_with_tag(0, 20, "store"),
        grouped_branch_with_tag(1, 21, "web"),
        SetOpType::Union,
        true,
        vec![LogicalType::Integer, LogicalType::Varchar],
    )));
    let mut input = input(plan, SearchBudget::default());
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    let output = input.memo.group(input.root).unwrap().schema.columns();
    assert!(snapshot.groups[&input.root]
        .unique_keys
        .contains(&Box::from([output[0].id, output[1].id])));
    assert!(snapshot.groups[&input.root]
        .grouping_unique_keys
        .contains(&Box::from([output[0].id, output[1].id])));
}

#[test]
fn exhausted_fact_read_preserves_baseline_and_reports_incomplete_work() {
    let mut budget = SearchBudget::default();
    budget.max_rule_work_units_per_group = 2;
    let mut input = input(project(source(0), 1), budget);
    let before = input.memo.logical_expr_count();
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    assert!(BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup
    )
    .unwrap()
    .is_none());
    assert_eq!(context.memo().logical_expr_count(), before);
    assert!(context
        .memo()
        .group(input.root)
        .unwrap()
        .ledger
        .exhaustion_events()
        .any(|(dimension, _)| *dimension == BudgetDimension::RuleWorkPerGroup));
}

#[test]
fn cte_registry_is_observed_even_before_a_producer_exists_and_rolls_back() {
    let mut input = input(project(source(0), 1), SearchBudget::default());
    let column = input.memo.group(input.root).unwrap().schema.columns()[0].id;
    input
        .memo
        .group_mut(input.root)
        .unwrap()
        .logical_properties
        .cte_references
        .insert(CteReferenceDomain {
            cte_index: 7,
            columns: BTreeMap::from([(paro_planner::operator::cte::CteColumnId(0), column)]),
        });
    let read = PatternRead::facts_from_group(&input.memo, input.root).unwrap();
    let savepoint = input.memo.transformation_savepoint();
    input
        .memo
        .register_cte_producer(
            7,
            input.root,
            BTreeMap::from([(paro_planner::operator::cte::CteColumnId(0), column)]),
        )
        .unwrap();
    assert!(!read.is_current(&input.memo).unwrap());
    assert_eq!(input.memo.take_changed_cte_readers(), vec![input.root]);
    input.memo.rollback_transformation(savepoint).unwrap();
    assert!(read.is_current(&input.memo).unwrap());
    assert!(input.memo.take_changed_cte_readers().is_empty());
}

#[test]
fn shared_dag_facts_do_not_expand_bag_occurrences() {
    let bind = BindContext::new();
    let mut plan = source(0);
    for table in 1..9 {
        let right = duplicate_plan_preserving_indices(&plan, bind.shared().as_ref());
        plan = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::new(
            table,
            plan,
            right,
            SetOpType::Union,
            true,
            vec![LogicalType::Integer],
        )));
    }
    let mut budget = SearchBudget::default();
    budget.max_rule_work_units_per_group = 128;
    let mut input = input(plan, budget);
    // Import is occurrence-preserving. Explicitly establish the shared DAG
    // whose evidence reader is under test, independently of import policy.
    let groups = input
        .memo
        .groups()
        .map(|group| (group.id, group.schema.columns().to_vec()))
        .collect::<Vec<_>>();
    let mut canonical = Vec::new();
    for (group, schema) in groups {
        if let Some((previous, _)) = canonical.iter().find(|(_, existing)| existing == &schema) {
            input.memo.merge_groups(*previous, group).unwrap();
        } else {
            canonical.push((group, schema));
        }
    }
    input.root = input.memo.canonical_group(input.root);
    assert_eq!(input.memo.groups().count(), 9);
    let state = input.planner_state.read().unwrap();
    let mut context = TransformContext::new(&mut input.memo, input.root);
    let snapshot = BoundarySnapshot::read(
        &mut context,
        &state,
        &PatternOperand::Group(input.root),
        BudgetDimension::RuleWorkPerGroup,
    )
    .unwrap()
    .unwrap();
    assert_eq!(snapshot.groups.len(), 9);
    assert_eq!(context.take_fact_reads().len(), 9);
}
