// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
