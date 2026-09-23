// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner integration and contract tests.

use std::sync::Arc;

use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, TableCatalogEntry, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_planner::binder::ir::CTEMaterialize;
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ConstantExpression, Expression, ReferenceExpression,
    WindowExpression, WindowFrame,
};
use paro_planner::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::operator::{
    CTERef, ComparisonJoin, EmptyResult, ExpressionGet, Filter, Get, GraphScan, MaterializedCTE,
    Projection, SetOperation, TopN, Window as LogicalWindow,
};
use paro_planner::plan::CardinalityEstimate;
use paro_storage::table::table_factory::TableFactory;

use super::super::memo::LogicalExpr;
use super::*;

mod native_runtime_filter;

#[test]
fn cte_domain_quality_inspects_selected_predicates_without_rule_provenance() {
    use paro_planner::expression::{ComparisonExpression, ComparisonType};
    fn fixture(normalized: bool) -> OwnedLogicalPlan {
        let producer =
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![vec![Expression::Constant(
                    ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                )]],
                vec!["k".into()],
                vec![LogicalType::Integer],
            )));
        let consumer = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            9,
            10,
            "c".into(),
            vec!["k".into()],
            vec![LogicalType::Integer],
        )));
        let predicate = Expression::Comparison(
            ComparisonExpression::new(
                ComparisonType::Equal,
                Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(10, 0), LogicalType::Integer)
                        .into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                ),
            )
            .into(),
        );
        let consumer = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            consumer,
            vec![predicate],
        )));
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                9,
                "c".into(),
                vec!["k".into()],
                vec![LogicalType::Integer],
                CTEMaterialize::Materialized,
                producer,
                consumer,
            )));
        if normalized {
            crate::cte::normalize::normalize(plan).unwrap()
        } else {
            plan
        }
    }
    for normalized in [false, true] {
        let mut input = MemoBuilder::build(
            fixture(normalized),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        input.root_goal.grant = GrantGoalKey::Class(test_grant_classes()[0].id);
        let state = input.planner_state.clone();
        let mut registry = ImplementationRegistry::default();
        implementation::register_implementations(
            &mut registry,
            state.clone(),
            Arc::new(
                test_grant_classes()
                    .into_iter()
                    .map(|class| (class.id, class))
                    .collect(),
            ),
            input.calibration.clone(),
            input.force_spill,
        )
        .unwrap();
        let mut engine = CascadesEngine::new(input.memo, registry);
        engine.prime_grant_context(test_grant_classes()).unwrap();
        let winner = engine
            .optimize(input.root, input.root_goal, input.mode)
            .unwrap();
        let reference = ChildWinnerRef {
            group: input.root,
            goal: input.root_goal,
            candidate: winner.candidate,
        };
        let state = state.read().unwrap();
        let mut properties = quality_properties::SelectedQualityProperties::default();
        let inspected =
            inspect_quality_candidate(engine.memo(), reference, &state, &mut properties)
                .unwrap()
                .unwrap();
        assert_eq!(inspected.cte_producer_witnesses.contains(&9), normalized);
        // Initial/normalization provenance alone never supplies the property.
        assert!(inspected.rules.is_empty());
        let frozen = engine.memo().freeze_candidate_tree(reference).unwrap();
        let expected =
            frozen_quality_evidence(engine.memo(), reference, &frozen, input.root_goal, &state)
                .unwrap()
                .unwrap();
        let actual = planner_quality_evidence(
            engine.memo(),
            reference,
            &winner,
            &state,
            &mut properties,
            &super::super::quality::QualityBundleRegistry::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(actual.evidence, expected, "normalized={normalized}");
        let builds = properties.builds;
        let domain_builds = properties.cte_domains.builds;
        let region_before = properties
            .region_fact_fingerprint(
                engine.memo(),
                reference,
                input.root_goal,
                &quality_node_map(&actual.nodes),
            )
            .unwrap();
        let mut other_goal = input.root_goal;
        other_goal.row_goal = super::super::memo::RowGoal::AtMost(2);
        assert_ne!(
            region_before,
            properties
                .region_fact_fingerprint(
                    engine.memo(),
                    reference,
                    other_goal,
                    &quality_node_map(&actual.nodes),
                )
                .unwrap(),
            "a region certificate belongs to its exact goal"
        );
        let again = planner_quality_evidence(
            engine.memo(),
            reference,
            &winner,
            &state,
            &mut properties,
            &super::super::quality::QualityBundleRegistry::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(again.evidence, expected);
        assert_eq!(properties.builds, builds);
        assert_eq!(properties.cte_domains.builds, domain_builds);
        assert!(properties.reuses > 0);
        assert!(properties.cte_domains.reuses > 0);
        assert!(
            Arc::ptr_eq(&actual.nodes, &again.nodes),
            "same selected root must share its view"
        );
        // The same exact candidate must refresh after a real fact mutation.
        // Only that node and its ancestors are re-derived, not its siblings.
        let changed = actual
            .nodes
            .iter()
            .find(|node| node.children.is_empty())
            .unwrap()
            .reference;
        let unchanged: Vec<_> = actual
            .nodes
            .iter()
            .filter(|node| node.children.is_empty() && node.reference.group != changed.group)
            .map(|node| {
                (
                    node.reference.candidate,
                    properties.revision(node.reference.candidate),
                )
            })
            .collect();
        engine
            .memo_mut()
            .group_mut(changed.group)
            .unwrap()
            .logical_properties
            .maximum_cardinality = Some(17);
        let refreshed = planner_quality_evidence(
            engine.memo(),
            reference,
            &winner,
            &state,
            &mut properties,
            &super::super::quality::QualityBundleRegistry::default(),
        )
        .unwrap()
        .unwrap();
        let frozen = engine.memo().freeze_candidate_tree(reference).unwrap();
        assert_eq!(
            refreshed.evidence,
            frozen_quality_evidence(engine.memo(), reference, &frozen, input.root_goal, &state)
                .unwrap()
                .unwrap()
        );
        assert!(properties.builds > builds);
        let region_after = properties
            .region_fact_fingerprint(
                engine.memo(),
                reference,
                input.root_goal,
                &quality_node_map(&refreshed.nodes),
            )
            .unwrap();
        assert_ne!(
            region_before, region_after,
            "a changed descendant invalidates its region certificate"
        );
        assert!(
            Arc::ptr_eq(&actual.nodes, &refreshed.nodes),
            "fact refresh must not reconstruct immutable choices"
        );
        assert!(properties.builds - builds < actual.nodes.len() as u64);
        for (candidate, revision) in unchanged {
            assert_eq!(properties.revision(candidate), revision);
        }
        // A truncated or cyclic choice graph cannot borrow a cached success.
        let mut invalid = actual.nodes.to_vec();
        let root_node = invalid
            .iter_mut()
            .find(|node| node.reference == reference)
            .unwrap();
        root_node.children = Arc::from([reference]);
        assert!(selected_dag::SelectedDag::from_nodes(reference, invalid.into()).is_none());
    }
}

pub(super) fn test_grant_classes() -> [ResourceGrantClass; 1] {
    [ResourceGrantClass {
        id: super::super::ids::ResourceGrantClassId(0),
        hard_memory_bytes: u64::MAX,
        spill_policy: crate::physical::SpillPolicy::Allowed,
        max_parallel_tasks: 1,
    }]
}

#[test]
fn quality_rule_evidence_comes_from_selected_proofs_not_apply_audit() {
    let logical = LogicalExpr {
        id: LogicalExprId::new(0),
        key: LogicalExprKey {
            operator: Fingerprint(1),
            scalars: Box::new([]),
            children: Box::new([]),
        },
        operator_encoding: None,
        operator_tag: None,
        payload: LogicalPayloadId::new(0),
        proofs: [EquivalenceProof::Initial].into_iter().collect(),
        // This is deliberately populated as if a rule reached the apply gate
        // but returned empty or was rejected by budget admission.
        applied_rules: [RuleId(100), RuleId(101)].into_iter().collect(),
    };
    assert!(selected_rule_proofs(&logical, None).is_empty());

    let mut transformed = logical.clone();
    transformed.proofs.insert(EquivalenceProof::Transformation {
        rule: RuleId(100),
        source: LogicalExprId::new(0),
        premise: Fingerprint(2),
    });
    assert_eq!(
        selected_rule_proofs(&transformed, Some(RuleId(100)))
            .into_iter()
            .collect::<Vec<_>>(),
        vec![RuleId(100)]
    );
    // A proof from another expression/branch cannot certify this payload's
    // origin merely because the rule was applied somewhere in the group.
    assert!(selected_rule_proofs(&transformed, Some(RuleId(101))).is_empty());
}

#[test]
fn persistent_region_scope_visits_shared_arena_nodes_once() {
    let mut memo = Memo::new(SearchBudget::default());
    let mut scope = None;
    let mut expected = BTreeSet::new();
    for _ in 0..40 {
        let group = memo.create_group(
            GroupSchema::new([]).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        expected.insert(group);
        scope = Some(PlannerRegionScope::new(
            group,
            scope
                .into_iter()
                .flat_map(|prior: PlannerRegionScope| [prior.clone(), prior]),
        ));
    }
    let scope = scope.unwrap();
    let (groups, overflow) = scope.materialize_bounded(&memo, 40);
    assert!(!overflow);
    assert_eq!(groups, expected);
    let (groups, overflow) = scope.materialize_bounded(&memo, 8);
    assert!(overflow);
    assert_eq!(groups.len(), 9);
}

#[test]
fn cross_product_memory_tracks_only_the_materialized_build_side() {
    let mut left = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        0,
        Vec::new(),
        vec!["left".to_string()],
        vec![LogicalType::BigInt],
    )));
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_000_000_000));
    let mut right =
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            Vec::new(),
            vec!["right".to_string()],
            vec![LogicalType::BigInt],
        )));
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(3));
    let mut product = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::cross(left, right)));
    product.stats.estimated_cardinality = Some(CardinalityEstimate::exact(3_000_000_000));

    let cost = planner_operator_cost(
        &product,
        2,
        Some(3_000_000_000),
        &[Some(1_000_000_000), Some(3)],
        Default::default(),
    )
    .expect("cross-product cost");

    assert_eq!(cost.peak_memory_upper, 3 * (8 + 8));
}

#[test]
fn materialized_cte_cost_tracks_the_producer_write() {
    let cost_for_rows = |rows| {
        let mut producer =
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                Vec::new(),
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
            )));
        producer.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        let mut consumer = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            1,
            1,
            "shared".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::BigInt],
        )));
        consumer.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1));
        let mut plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                1,
                "shared".to_string(),
                vec!["v".to_string()],
                vec![LogicalType::BigInt],
                CTEMaterialize::Materialized,
                producer,
                consumer,
            )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1));
        planner_operator_cost(
            &plan,
            2,
            Some(1),
            &[Some(rows), Some(1)],
            Default::default(),
        )
        .expect("materialized CTE cost")
    };

    let one_row = cost_for_rows(1);
    let hundred_rows = cost_for_rows(100);
    assert!(hundred_rows.work_latency.expected > one_row.work_latency.expected + 90.0);
    assert!(hundred_rows.peak_memory_upper > one_row.peak_memory_upper);
}

#[test]
fn materialized_cte_cost_is_monotone_in_producer_width() {
    let cost_for_types = |types: Vec<LogicalType>| {
        let names = (0..types.len())
            .map(|index| format!("v{index}"))
            .collect::<Vec<_>>();
        let mut producer = OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(
            ExpressionGet::new(0, Vec::new(), names.clone(), types.clone()),
        ));
        producer.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_000));
        let mut consumer = OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            1,
            1,
            "shared".to_string(),
            names.clone(),
            types.clone(),
        )));
        consumer.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_000));
        let mut plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                1,
                "shared".to_string(),
                names,
                types,
                CTEMaterialize::Materialized,
                producer,
                consumer,
            )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_000));
        planner_operator_cost(
            &plan,
            2,
            Some(1_000),
            &[Some(1_000), Some(1_000)],
            Default::default(),
        )
        .unwrap()
    };

    let narrow = cost_for_types(vec![LogicalType::BigInt]);
    let wide = cost_for_types(vec![LogicalType::Varchar; 8]);
    assert!(wide.work_latency.expected > narrow.work_latency.expected);
    assert!(wide.peak_memory_upper > narrow.peak_memory_upper);
}

#[test]
fn calibrated_tuple_work_distinguishes_narrow_and_wide_intermediates() {
    let facts = |width| ResolvedPlannerCostFacts {
        output_rows: CompactRange::point(1_000.0).unwrap(),
        child_rows: vec![CompactRange::point(1_000.0).unwrap()].into_boxed_slice(),
        output_rows_hard_upper: Some(1_000),
        child_rows_hard_upper: vec![Some(1_000)].into_boxed_slice(),
        child_row_widths: vec![width].into_boxed_slice(),
        child_materialization_risk_rows: vec![1_000].into_boxed_slice(),
        output_row_width: width,
        hash_key_width: None,
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_identity: None,
        runtime_filter_build_left_domain_identity: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_key_types: Box::new([]),
    };
    let calibrated_cost = |facts: &ResolvedPlannerCostFacts| {
        let mut work = LocalOperatorWork::default();
        add_tuple_byte_work(&mut work, facts).unwrap();
        MachineCalibrationBundle::default().fold(&work).unwrap()
    };

    let narrow = calibrated_cost(&facts(16));
    let wide = calibrated_cost(&facts(128));
    assert!(wide.score.risk_adjusted > narrow.score.risk_adjusted);
    assert!(
        wide.resources_expected[ResourceDimension::MemoryRead as usize]
            > narrow.resources_expected[ResourceDimension::MemoryRead as usize]
    );
}

#[test]
fn non_source_width_cannot_manufacture_pipeline_tasks() {
    let facts = |rows: f64| ResolvedPlannerCostFacts {
        output_rows: CompactRange::point(rows).unwrap(),
        child_rows: vec![CompactRange::point(rows).unwrap()].into_boxed_slice(),
        output_rows_hard_upper: None,
        child_rows_hard_upper: vec![None].into_boxed_slice(),
        child_row_widths: vec![32].into_boxed_slice(),
        child_materialization_risk_rows: vec![rows as u64].into_boxed_slice(),
        output_row_width: 32,
        hash_key_width: None,
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_identity: None,
        runtime_filter_build_left_domain_identity: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_key_types: Box::new([]),
    };

    assert_eq!(
        super::costing::useful_parallel_tasks_for_facts(&facts(1_000.0), 10),
        1
    );
    assert_eq!(
        super::costing::useful_parallel_tasks_for_facts(&facts(2_000_000.0), 10),
        1
    );
}

#[test]
fn scan_parallelism_uses_pre_predicate_physical_work() {
    let facts = ResolvedPlannerCostFacts {
        output_rows: CompactRange::point(1.0).unwrap(),
        child_rows: Box::new([]),
        output_rows_hard_upper: None,
        child_rows_hard_upper: Box::new([]),
        child_row_widths: Box::new([]),
        child_materialization_risk_rows: Box::new([]),
        output_row_width: 8,
        hash_key_width: None,
        scan_access_width: Some(8),
        scan_physical_rows: Some(4_000_000),
        scan_work_source: Some(WorkSourceId(0)),
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_identity: None,
        runtime_filter_build_left_domain_identity: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_key_types: Box::new([]),
    };

    assert_eq!(
        super::costing::useful_parallel_tasks_for_facts(&facts, 10),
        10
    );
}

#[test]
fn scan_work_evidence_does_not_require_analyze_catalog_statistics() {
    let scan = test_base_get(0, 24_101, "unanalysed_source", 10_000);
    let LogicalOperator::Get(get) = &scan.operator else {
        unreachable!()
    };
    assert!(get
        .table
        .as_ref()
        .unwrap()
        .statistics()
        .is_none_or(|statistics| statistics.row_count == 0));
    let facts = planner_cost_facts(
        &scan,
        &HashMap::new(),
        &BindingCatalog::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(facts.scan_physical_rows, Some(10_000));
    // Unknown and an observed empty source are different evidence states.
    let empty = test_base_get(1, 24_102, "empty_source", 0);
    let facts = planner_cost_facts(
        &empty,
        &HashMap::new(),
        &BindingCatalog::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(facts.scan_physical_rows, Some(0));
}

#[test]
fn replaceable_runtime_filter_work_is_serial_until_bound_to_a_source() {
    let facts = ResolvedPlannerCostFacts {
        output_rows: CompactRange::point(10.0).unwrap(),
        child_rows: vec![
            CompactRange::point(2_000_000.0).unwrap(),
            CompactRange::point(100.0).unwrap(),
        ]
        .into_boxed_slice(),
        output_rows_hard_upper: None,
        child_rows_hard_upper: vec![None, None].into_boxed_slice(),
        child_row_widths: vec![32, 8].into_boxed_slice(),
        child_materialization_risk_rows: vec![2_000_000, 100].into_boxed_slice(),
        output_row_width: 32,
        hash_key_width: Some(8),
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        // The replaceable term is bound to the traced source only after child
        // winners expose that source pipeline's task supply.
        runtime_filter_probe_source_rows: Some(CompactRange::point(20_000.0).unwrap()),
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_identity: None,
        runtime_filter_build_left_domain_identity: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_key_types: vec![LogicalType::BigInt].into_boxed_slice(),
    };

    let apply = super::costing::runtime_filter_apply_cost(
        &facts,
        PhysicalImplementationFlavor::HashJoinRuntimeFilter,
        &MachineCalibrationBundle::default(),
        10,
    )
    .unwrap()
    .unwrap();

    assert_eq!(apply.max_parallel_tasks, 1);
    assert_eq!(apply.output_pipeline_tasks, 1);
}

#[test]
fn runtime_filter_tuple_work_counts_only_rows_delivered_to_the_join() {
    let facts = ResolvedPlannerCostFacts {
        output_rows: CompactRange::point(10.0).unwrap(),
        child_rows: vec![
            CompactRange::point(1_000_000.0).unwrap(),
            CompactRange::point(100.0).unwrap(),
        ]
        .into_boxed_slice(),
        output_rows_hard_upper: Some(10),
        child_rows_hard_upper: vec![Some(1_000_000), Some(100)].into_boxed_slice(),
        child_row_widths: vec![128, 8].into_boxed_slice(),
        child_materialization_risk_rows: vec![1_000_000, 100].into_boxed_slice(),
        output_row_width: 128,
        hash_key_width: Some(8),
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_identity: None,
        runtime_filter_build_left_domain_identity: None,
        runtime_filter_build_left_distinct_expected: None,
        runtime_filter_key_types: vec![LogicalType::BigInt].into_boxed_slice(),
    };
    let calibrated =
        |work: &LocalOperatorWork| MachineCalibrationBundle::default().fold(work).unwrap();
    let mut unfiltered = LocalOperatorWork::default();
    add_tuple_byte_work(&mut unfiltered, &facts).unwrap();
    let mut filtered = LocalOperatorWork::default();
    add_tuple_byte_work_for_children(
        &mut filtered,
        &facts,
        &[
            CompactRange::point(100.0).unwrap(),
            CompactRange::point(100.0).unwrap(),
        ],
    )
    .unwrap();

    assert!(
        calibrated(&filtered).score.risk_adjusted < calibrated(&unfiltered).score.risk_adjusted
    );
}

#[test]
fn calibrated_hash_work_distinguishes_integral_and_wide_keys() {
    let calibrated_cost = |key_width| {
        let mut work = LocalOperatorWork::default();
        add_hash_key_byte_work(
            &mut work,
            CompactRange::point(10_000.0).unwrap(),
            Some(key_width),
        )
        .unwrap();
        MachineCalibrationBundle::default().fold(&work).unwrap()
    };

    let integral = calibrated_cost(8);
    let wide = calibrated_cost(8 + 4 * 32);
    assert!(wide.score.risk_adjusted > integral.score.risk_adjusted);
    assert!(
        wide.resources_expected[ResourceDimension::Cpu as usize]
            > integral.resources_expected[ResourceDimension::Cpu as usize]
    );
}

#[test]
fn expression_cost_facts_read_current_group_cardinality() {
    let schema = GroupSchema::new([super::super::column::ColumnDesc {
        id: ColumnId::new(0),
        logical_type: LogicalType::BigInt,
        nullable: false,
        origin: ColumnOrigin::Derived {
            key: Fingerprint(1),
        },
        visibility: ColumnVisibility::Visible,
        name_hint: None,
    }])
    .unwrap();
    let mut memo = Memo::new(SearchBudget::default());
    let mut child_properties = LogicalProperties::default();
    child_properties.column_domains.insert(
        ColumnId::new(0),
        GroupColumnDomain::new(Some(25), Some(40)).unwrap(),
    );
    let child = memo.create_group(
        schema.clone(),
        child_properties,
        GroupCardinality::new(
            Fingerprint(1),
            CardinalityRecipeKind::Statistics,
            80,
            100,
            120,
        ),
    );
    let parent = memo.create_group(
        schema,
        LogicalProperties::default(),
        GroupCardinality::new(Fingerprint(2), CardinalityRecipeKind::Statistics, 8, 10, 12),
    );
    let template = PlannerCostFacts {
        child_row_widths: vec![16].into_boxed_slice(),
        child_materialization_risk_rows: vec![120].into_boxed_slice(),
        output_row_width: 16,
        hash_key_width: None,
        scan_access_width: None,
        scan_physical_rows: None,
        scan_work_source: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_build_left_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_build_left_probe_source_rows: None,
        runtime_filter_probe_sources: Box::new([]),
        runtime_filter_build_left_probe_sources: Box::new([]),
        runtime_filter_build_distinct_expected: None,
        runtime_filter_build_domain_column: None,
        runtime_filter_build_key: None,
        runtime_filter_build_left_distinct_expected: Some(100),
        runtime_filter_build_left_domain_column: Some(ColumnId::new(0)),
        runtime_filter_build_left_key: Some(Fingerprint(42)),
        runtime_filter_key_types: Box::new([]),
    };

    let initial = expression_cost_facts(&memo, parent, &[child], &template).unwrap();
    assert_eq!(initial.child_rows[0].expected, 100.0);
    assert_eq!(
        initial.runtime_filter_build_left_distinct_expected,
        Some(25)
    );
    let initial_domain_identity = initial
        .runtime_filter_build_left_domain_identity
        .expect("the build column must have a semantic domain identity");

    memo.group_mut(child).unwrap().cardinality =
        GroupCardinality::new(Fingerprint(3), CardinalityRecipeKind::JoinRegion, 4, 5, 6);
    memo.group_mut(child)
        .unwrap()
        .logical_properties
        .column_domains
        .insert(
            ColumnId::new(0),
            GroupColumnDomain::new(Some(5), Some(6)).unwrap(),
        );
    let refined = expression_cost_facts(&memo, parent, &[child], &template).unwrap();
    assert_eq!(refined.child_rows[0].expected, 5.0);
    assert_eq!(refined.runtime_filter_build_left_distinct_expected, Some(5));
    assert_ne!(
        refined.runtime_filter_build_left_domain_identity,
        Some(initial_domain_identity),
        "fact changes must invalidate the runtime-filter domain proof"
    );
}

#[test]
fn graph_relation_identity_is_part_of_the_query_ir_fingerprint() {
    let scan = |label: &str, table_oid: u64| {
        OwnedLogicalPlan::synthetic(LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: label.to_ascii_lowercase(),
                table_oid,
                key_column_ids: vec![0],
                label: label.to_string(),
                property_column_ids: vec![1],
            },
            None,
            1,
            2,
            label.to_string(),
            "g".to_string(),
            "public".to_string(),
        ))))
    };
    let person = scan("Person", 11);
    let company = scan("Company", 12);
    let scalars = ScalarArena::default();
    let person = query_operator_fingerprint(&person, &[], &scalars).unwrap();
    let company = query_operator_fingerprint(&company, &[], &scalars).unwrap();
    assert_ne!(person, company);
}

#[test]
fn graph_variable_identity_is_part_of_the_query_ir_fingerprint() {
    let scan = |table_index: usize| {
        OwnedLogicalPlan::synthetic(LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "person".to_string(),
                table_oid: 11,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![1],
            },
            None,
            table_index,
            100,
            "Person".to_string(),
            "g".to_string(),
            "public".to_string(),
        ))))
    };
    let scalars = ScalarArena::default();
    let first = query_operator_fingerprint(&scan(7), &[], &scalars).unwrap();
    let second = query_operator_fingerprint(&scan(8), &[], &scalars).unwrap();
    assert_ne!(first, second);
}

#[test]
fn graph_filter_is_part_of_the_query_ir_fingerprint() {
    let scan = |value: bool| {
        OwnedLogicalPlan::synthetic(LogicalOperator::GraphScan(Box::new(GraphScan::new(
            VertexTableInfo {
                table_name: "person".to_string(),
                table_oid: 11,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![1],
            },
            Some(Expression::Constant(
                ConstantExpression {
                    value: Value::Boolean(value),
                    return_type: LogicalType::Boolean,
                }
                .into(),
            )),
            1,
            2,
            "Person".to_string(),
            "g".to_string(),
            "public".to_string(),
        ))))
    };
    let fingerprint = |plan: OwnedLogicalPlan| {
        let mut binding_ids = BindingCatalog::default();
        let mut columns = ColumnCatalog::default();
        let mut scalars = ScalarArena::default();
        let roots = intern_operator_scalars(
            &plan.operator,
            &[],
            &[] as &[&[ColumnId]],
            &mut binding_ids,
            &mut columns,
            &mut scalars,
        )
        .unwrap();
        query_operator_fingerprint(&plan, &roots, &scalars).unwrap()
    };

    assert_ne!(fingerprint(scan(true)), fingerprint(scan(false)));
}

#[test]
fn cte_owner_is_part_of_the_query_ir_fingerprint() {
    let reference = |cte_index| {
        OwnedLogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
            cte_index,
            7,
            "shared".to_string(),
            vec!["v".to_string()],
            vec![LogicalType::Integer],
        )))
    };
    let scalars = ScalarArena::default();
    let first = query_operator_fingerprint(&reference(1), &[], &scalars).unwrap();
    let second = query_operator_fingerprint(&reference(2), &[], &scalars).unwrap();
    assert_ne!(first, second);
}

#[test]
fn memo_round_trip_derives_layout_after_winner_selection() {
    let bind_context = BindContext::new();
    let leaf = OwnedLogicalPlan::dummy_scan(&bind_context);
    let wrapped = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::EmptyResult(EmptyResult::new(leaf)),
    );
    let input = MemoBuilder::build(wrapped, bind_context, SearchBudget::default()).unwrap();
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let optimized = &optimized.variants[0];
    assert!(matches!(
        optimized.plan.operator,
        LogicalOperator::EmptyResult(_)
    ));
    assert!(matches!(
        optimized.plan.children()[0].operator,
        LogicalOperator::DummyScan
    ));
}

#[test]
fn only_explicit_detail_collects_the_physical_diagnostic_matrix() {
    use paro_context::compile_diagnostics::{CaptureLevel, CompileCapture};
    let mut previous = None;
    for level in [
        None,
        Some(CaptureLevel::Summary),
        Some(CaptureLevel::Detail),
    ] {
        let bind = BindContext::new();
        let input = MemoBuilder::build(
            OwnedLogicalPlan::dummy_scan(&bind),
            bind,
            SearchBudget::default(),
        )
        .unwrap();
        let mut session = paro_context::TestStatementContextBuilder::minimal().build();
        Arc::get_mut(&mut session).unwrap().options.compile_capture =
            level.map(|level| CompileCapture::try_start_with_level(level).unwrap());
        input.planner_state.write().unwrap().session = Some(session);
        let grants = [ResourceGrantClass {
            id: ResourceGrantClassId(2),
            ..test_grant_classes()[0]
        }];
        let output = input.optimize(&grants).unwrap();
        let summary = output.search_summary;
        assert_eq!(
            summary.physical_search.is_some(),
            level == Some(CaptureLevel::Detail)
        );
        assert!(summary.work_counters["physical_goal_count"] > 0);
        let observed = (
            summary.is_complete(),
            summary.groups,
            summary.logical_expressions,
            summary.physical_expressions,
            summary.work_counters["physical_goal_count"],
            output.variants[0].physical_fingerprint,
            output.variants[0].cost,
        );
        if let Some(previous) = &previous {
            assert_eq!(&observed, previous, "collection must not change search");
        }
        previous = Some(observed);
    }
}

#[test]
fn planner_topn_retains_hidden_sort_operand_without_widening_output() {
    // Production Memo -> mandatory/optional -> frozen winner -> extraction.
    // The semantic ORDER template erases its projection map, but the target
    // group has only `id`. A direct native rewrite must restore that contract.
    for offset in [0, 3] {
        let session = crate::subquery::partition_aggregate_tests::setup_session();
        let binder = Binder::new(session.clone());
        let bind_context = binder.bind_context.clone();
        let constant = |n| {
            Expression::Constant(
                ConstantExpression::new(Value::Integer(n), LogicalType::Integer).into(),
            )
        };
        let leaf = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                (0..128)
                    .map(|n| vec![constant(n), constant(128 - n)])
                    .collect(),
                vec!["id".into(), "hidden_score".into()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        let mut order = paro_planner::operator::Order::new(
            leaf,
            vec![paro_planner::binder::ir::OrderByNode {
                expression: Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 1), LogicalType::Integer).into(),
                ),
                ascending: false,
                nulls_first: false,
            }],
        );
        order.projection_map = paro_planner::operator::ProjectionMap::new(vec![0]);
        let ordered = OwnedLogicalPlan::new(&bind_context, LogicalOperator::Order(order));
        let plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Limit(Box::new(paro_planner::operator::Limit::new(
                ordered,
                Some(constant(3)),
                Some(constant(offset)),
            ))),
        );
        let context = crate::context::OptimizationContext::new(session, bind_context);
        let input = MemoBuilder::build_with_search(
            vec![LogicalAlternative {
                plan,
                source: AlternativeOrigin::Baseline,
                column_stats: Arc::new(HashMap::new()),
            }],
            &binder,
            SearchBudget::default(),
            &context,
        )
        .unwrap();
        let grants = [0, 1, 2].map(|id| ResourceGrantClass {
            id: ResourceGrantClassId(id),
            hard_memory_bytes: 16 << 20,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        });
        let optimized = input.optimize(&grants).unwrap();
        assert!(
            optimized
                .rule_insertions
                .get(&TOP_N_INTRODUCTION_RULE)
                .copied()
                .unwrap_or(0)
                > 0,
            "{:#?}",
            optimized.rule_work_profile
        );
        let variant = optimized
            .variants
            .iter()
            .find(|v| v.class == ResourceGrantClassId(2))
            .unwrap();
        let winner = &variant.plan;
        assert_eq!(winner.get_column_bindings(), vec![ColumnBinding::new(0, 0)]);
        // Generation and selection are separate contracts. This VALUES
        // fixture can legitimately prefer sort; the server spill/search
        // regressions independently require actual TopN/TopK execution.
        if let LogicalOperator::TopN(topn) = &winner.operator {
            assert_eq!(topn.limit, 3);
            assert_eq!(topn.offset, offset as usize);
            assert_eq!(topn.child.types().len(), 2);
            assert!(!topn.orders[0].ascending);
            assert!(!topn.orders[0].nulls_first);
        } else {
            assert!(matches!(winner.operator, LogicalOperator::Limit(_)));
        }
        assert!(variant.contracts.contains_key(&winner.id));
    }
}

#[test]
fn planner_cross_product_keeps_verified_grant_when_another_is_unresolved() {
    // Exercise the production implementation registry, cost composition,
    // freeze and extraction, not a test-only Leaf implementation.
    let bind_context = BindContext::new();
    let input = |index| {
        let mut plan = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                index,
                vec![],
                vec!["value".to_owned()],
                vec![LogicalType::Integer],
            )),
        );
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10));
        plan
    };
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Join(Join::cross(input(0), input(1))),
    );
    let grants = [0, 2].map(|id| ResourceGrantClass {
        id: ResourceGrantClassId(id),
        hard_memory_bytes: if id == 2 { 1 } else { 16 << 20 },
        spill_policy: SpillPolicy::Forbidden,
        max_parallel_tasks: 1,
    });
    let input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
    // A frozen statement snapshot selects the production expected-grant
    // boundary; standalone Memo clients intentionally use eager coverage.
    input.planner_state.write().unwrap().session =
        Some(paro_context::TestStatementContextBuilder::minimal().build());
    let output = input.optimize(&grants).unwrap();
    assert_eq!(output.variants.len(), 1);
    assert_eq!(output.variants[0].class, ResourceGrantClassId(0));
    assert!(output.variants[0]
        .contracts
        .get(&output.variants[0].plan.id)
        .is_some());
    let coverage = output.grant_search.unwrap();
    assert_eq!(coverage.expected_class, Some(ResourceGrantClassId(2)));
    assert_eq!(
        coverage.unresolved_classes,
        BTreeSet::from([ResourceGrantClassId(2)])
    );
}

#[test]
fn memo_winner_names_the_hash_join_implementation() {
    let bind_context = BindContext::new();
    let left = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["left".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["right".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
    );
    let join = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            left,
            right,
            vec![condition],
        )),
    );
    let input = MemoBuilder::build(join, bind_context, SearchBudget::default()).unwrap();
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let optimized = &optimized.variants[0];
    let contract = optimized
        .contracts
        .get(&optimized.plan.id)
        .expect("root winner contract");
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::HashJoin
    );
}

#[test]
fn memo_hash_join_can_select_logical_left_as_physical_build() {
    let bind_context = BindContext::new();
    let mut left = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            integer_value_rows(8, 1),
            vec!["left".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(8));
    let mut right = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            integer_value_rows(4096, 1),
            vec!["right".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(4096));
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
    );
    let mut join = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Join(Join::comparison(
            JoinType::Left,
            left,
            right,
            vec![condition],
        )),
    );
    join.stats.estimated_cardinality = Some(CardinalityEstimate::exact(8));

    let input = MemoBuilder::build(join, bind_context, SearchBudget::default()).unwrap();
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let optimized = &optimized.variants[0];
    let contract = optimized
        .contracts
        .get(&optimized.plan.id)
        .expect("root winner contract");
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::HashJoinBuildLeft
    );
}

#[test]
fn memo_hash_join_does_not_materialize_a_selectivity_reduced_fact_subtree() {
    let bind_context = BindContext::new();
    let mut reduced_fact = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            integer_value_rows(8, 1),
            vec!["reduced_fact".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    reduced_fact.stats.estimated_cardinality = Some(CardinalityEstimate::exact(8));
    reduced_fact.stats.materialization_risk_cardinality = Some(1_000_000);

    let mut dimension = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            integer_value_rows(4096, 1),
            vec!["dimension".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    dimension.stats.estimated_cardinality = Some(CardinalityEstimate::exact(4096));
    dimension.stats.materialization_risk_cardinality = Some(4096);

    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
    );
    let mut join = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            reduced_fact,
            dimension,
            vec![condition],
        )),
    );
    join.stats.estimated_cardinality = Some(CardinalityEstimate::exact(8));

    let input = MemoBuilder::build(join, bind_context, SearchBudget::default()).unwrap();
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let optimized = &optimized.variants[0];
    let contract = optimized
        .contracts
        .get(&optimized.plan.id)
        .expect("root winner contract");
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::HashJoin
    );
}

#[test]
fn preserved_build_can_filter_a_direct_non_preserved_probe() {
    let mut preserved_build = test_base_get(0, 20_021, "preserved_build", 538);
    preserved_build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(538));
    let mut non_preserved_probe = test_base_get(1, 20_022, "non_preserved_probe", 719_384);
    non_preserved_probe.stats.estimated_cardinality = Some(CardinalityEstimate::exact(719_384));
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
    );
    let join = ComparisonJoin::new(
        JoinType::Left,
        preserved_build,
        non_preserved_probe,
        vec![condition],
    );
    assert!(supports_build_left_runtime_filter_auxiliary(&join, true));
    assert!(!supports_runtime_filter_auxiliary(&join, true));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(538));

    let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let variant = &optimized.variants[0];
    let contract = variant.contracts.get(&variant.plan.id).unwrap();
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
    );
    assert_eq!(contract.owned_artifacts.len(), 1);
}

#[test]
fn calibration_revision_can_change_the_selected_physical_algorithm() {
    fn coefficient(value: f64) -> super::super::calibration::CalibratedOpCost {
        let mut resources = [0.0; super::super::cost::RESOURCE_DIMS];
        resources[ResourceDimension::Cpu as usize] = value;
        super::super::calibration::CalibratedOpCost {
            expected_resources_per_unit: resources,
            risk_resources_per_unit: resources,
            latency_per_unit: CompactRange::point(value).unwrap(),
        }
    }

    fn selected(calibration: MachineCalibrationBundle) -> PhysicalImplementationFlavor {
        let bind_context = BindContext::new();
        let mut left = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                integer_value_rows(512, 2),
                vec!["lower".to_string(), "upper".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(512));
        let mut right = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                integer_value_rows(512, 1),
                vec!["point".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(512));
        let reference = |index| {
            Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer).into())
        };
        let mut join = OwnedLogicalPlan::new(
            &bind_context,
            LogicalOperator::Join(Join::comparison(
                JoinType::Inner,
                left,
                right,
                vec![
                    JoinCondition::new(
                        reference(0),
                        reference(0),
                        JoinComparisonType::LessThanOrEqual,
                    ),
                    JoinCondition::new(
                        reference(1),
                        reference(0),
                        JoinComparisonType::GreaterThanOrEqual,
                    ),
                ],
            )),
        );
        join.stats.estimated_cardinality = Some(CardinalityEstimate::exact(8_056));
        let optimized = MemoBuilder::build(join, bind_context, SearchBudget::default())
            .unwrap()
            .with_calibration(Arc::new(calibration))
            .optimize(&test_grant_classes())
            .unwrap();
        let variant = &optimized.variants[0];
        variant
            .contracts
            .get(&variant.plan.id)
            .expect("range-join root winner contract")
            .implementation
    }

    let mut prefer_sort_range = MachineCalibrationBundle::default();
    prefer_sort_range
        .set(OP_RANGE_JOIN_ROW, coefficient(0.01))
        .unwrap();
    prefer_sort_range
        .set(OP_IE_JOIN_ROW, coefficient(100.0))
        .unwrap();
    let mut prefer_ie = MachineCalibrationBundle::default();
    prefer_ie
        .set(OP_RANGE_JOIN_ROW, coefficient(100.0))
        .unwrap();
    prefer_ie.set(OP_IE_JOIN_ROW, coefficient(0.01)).unwrap();

    assert_eq!(
        selected(prefer_sort_range),
        PhysicalImplementationFlavor::SortRangeJoin
    );
    assert_eq!(
        selected(prefer_ie),
        PhysicalImplementationFlavor::ClassicIeJoin
    );
}

#[test]
fn memo_window_winner_is_the_node_lowered_by_the_physical_extractor() {
    let bind_context = BindContext::new();
    let mut values = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            integer_value_rows(1_024, 2),
            vec!["grp".to_string(), "value".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    values.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_024));
    let aggregate =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    let mut plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Window(LogicalWindow::new(
            1,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(
                    ReferenceExpression::new(0, LogicalType::Integer).into(),
                )],
                Vec::new(),
                WindowFrame::default(),
            )],
            values,
        )),
    );
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_024));

    let input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let optimized = optimized.variants.into_vec().remove(0);
    let contract = optimized.contracts.get(&optimized.plan.id).unwrap();
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::PartitionAggregateWindow
    );

    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(optimized.contracts)
            .with_enforcer_contracts(optimized.enforcers)
            .requiring_winner_contracts()
            .extract_selected(&optimized.plan)
            .unwrap();
    assert!(matches!(
        physical.node(physical.root).kind,
        crate::physical::PhysicalNodeKind::PartitionAggregateWindow(_)
    ));
}

#[test]
fn selected_plan_reads_native_operands_not_the_legacy_scalar_payload() {
    let bind_context = BindContext::new();
    let original = Expression::Constant(
        ConstantExpression::new(Value::Integer(42), LogicalType::Integer).into(),
    );
    let plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Projection(Projection::new(
            1,
            OwnedLogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
            vec![original.clone()],
        )),
    );
    let input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
    {
        let mut state = input.planner_state.write().unwrap();
        let mut overwritten = 0;
        for payload in &mut state.payloads.logical {
            if let LogicalOperator::Projection(projection) = &mut payload.semantic_template.operator
            {
                projection.expressions[0] = Expression::Constant(
                    ConstantExpression::new(Value::Integer(999), LogicalType::Integer).into(),
                );
                overwritten += 1;
            }
        }
        assert_eq!(overwritten, 1);
    }
    let output = input.optimize(&test_grant_classes()).unwrap();
    let mut found = false;
    output.variants[0]
        .plan
        .try_visit_pre_order(|plan| {
            if let LogicalOperator::Projection(projection) = &plan.operator {
                for expression in &projection.expressions {
                    if matches!(expression, Expression::Constant(_)) {
                        assert!(expression.equals(&original));
                        found = true;
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    assert!(found);
}

#[test]
fn mark_join_to_semi_is_an_explicit_isolatable_transformation() {
    fn plan(bind_context: &BindContext) -> OwnedLogicalPlan {
        let left = OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                integer_value_rows(1, 1),
                vec!["left".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let right = OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                integer_value_rows(1, 1),
                vec!["right".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let column = |table_index, logical_type| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table_index, 0), logical_type).into(),
            )
        };
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            left,
            right,
            vec![JoinCondition::equality(
                column(0, LogicalType::Integer),
                column(1, LogicalType::Integer),
            )],
        );
        let mark_index = 90;
        join.mark_index = Some(mark_index);
        let filter = OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Filter(Filter::new(
                OwnedLogicalPlan::new(bind_context, LogicalOperator::Join(Join::Comparison(join))),
                vec![column(mark_index, LogicalType::Boolean)],
            )),
        );
        OwnedLogicalPlan::new(
            bind_context,
            LogicalOperator::Projection(Projection::new(
                91,
                filter,
                vec![column(0, LogicalType::Integer)],
            )),
        )
    }

    fn optimize(mut budget: SearchBudget) -> OptimizationOutput {
        // test_grant_classes declares only class zero; do not derive class
        // two from a different session-side class domain.
        budget.max_grant_classes = 1;
        let session = crate::subquery::partition_aggregate_tests::setup_session();
        let binder = Binder::new(session.clone());
        let context =
            crate::context::OptimizationContext::new(session, binder.bind_context.clone());
        MemoBuilder::build_with_search(
            vec![LogicalAlternative {
                plan: plan(&binder.bind_context),
                source: AlternativeOrigin::Baseline,
                column_stats: Arc::new(HashMap::new()),
            }],
            &binder,
            budget,
            &context,
        )
        .unwrap()
        .optimize(&test_grant_classes())
        .unwrap()
    }

    fn selected_join_type(output: &OptimizationOutput) -> JoinType {
        fn find(plan: &crate::physical::selected::SelectedNode) -> Option<JoinType> {
            if let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator {
                return Some(join.join_type);
            }
            plan.children().into_iter().find_map(find)
        }
        find(&output.variants[0].plan).expect("optimized plan must retain the comparison join")
    }

    let enabled = optimize(SearchBudget::default());
    assert_eq!(selected_join_type(&enabled), JoinType::Semi);
    assert!(enabled
        .rule_attempts
        .get(&MARK_JOIN_TO_SEMI_RULE)
        .is_some_and(|attempts| *attempts > 0));
    assert!(enabled
        .rule_insertions
        .get(&MARK_JOIN_TO_SEMI_RULE)
        .is_some_and(|insertions| *insertions > 0));

    let mut disabled_budget = SearchBudget::default();
    disabled_budget.disable_transformation(MARK_JOIN_TO_SEMI_RULE);
    let disabled = optimize(disabled_budget);
    assert_eq!(selected_join_type(&disabled), JoinType::Mark);
    assert!(!disabled.rule_attempts.contains_key(&MARK_JOIN_TO_SEMI_RULE));
    assert!(!disabled
        .rule_insertions
        .contains_key(&MARK_JOIN_TO_SEMI_RULE));
}

fn integer_value_rows(rows: usize, columns: usize) -> Vec<Vec<Expression>> {
    (0..rows)
        .map(|_| {
            (0..columns)
                .map(|_| {
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(0), LogicalType::Integer).into(),
                    )
                })
                .collect()
        })
        .collect()
}

#[test]
fn composite_equality_runtime_filter_has_identity_without_single_column_ndv() {
    let bind_context = BindContext::new();
    let reference =
        |index| Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer).into());
    let two_columns = |index, oid, name, rows| {
        OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
            index + 2,
            test_base_get(index, oid, name, rows),
            vec![reference(0), reference(0)],
        )))
    };
    let mut plan = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::Join(Join::comparison(
            JoinType::Inner,
            two_columns(0, 991, "composite_build", 16),
            two_columns(1, 992, "composite_probe", 128),
            vec![
                JoinCondition::equality(reference(0), reference(0)),
                JoinCondition::equality(reference(1), reference(1)),
            ],
        )),
    );
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(16));
    let facts = planner_cost_facts(
        &plan,
        &HashMap::new(),
        &BindingCatalog::default(),
        paro_storage::rowset::scan_cost::ScanAccessCostModel::default(),
    )
    .unwrap();
    assert!(facts.runtime_filter_build_domain_column.is_none());
    assert!(facts.runtime_filter_build_left_domain_column.is_none());
    assert!(facts.runtime_filter_build_key.is_some());
    assert!(facts.runtime_filter_build_left_key.is_some());
    let input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
    // The optional build-left and build-right implementations must both be
    // constructible even though a joint NDV is deliberately not guessed.
    let output = input.optimize(&test_grant_classes()).unwrap();
    assert!(!output.variants.is_empty());
}

#[test]
fn runtime_filter_facet_does_not_alias_distinct_relation_owners() {
    let facet = |owner| {
        planner_region_facet(
            RegionFacetKind::RuntimeFilter,
            FacetCriticality::Optional,
            Fingerprint(77),
            Fingerprint(78),
            owner,
            std::iter::once(owner).collect(),
        )
    };
    let a = facet(GroupId(0));
    let b = facet(GroupId(1));
    assert_ne!(a.fingerprint, b.fingerprint);
    assert_eq!(a.fingerprint, facet(GroupId(0)).fingerprint);
    let forest = RegionForest::normalize([a.clone(), b.clone()], 1, 8).unwrap();
    assert!(forest.deferred_facets.is_empty());
    assert_ne!(
        forest.region_for_facet(a.fingerprint),
        forest.region_for_facet(b.fingerprint)
    );
}

pub(super) fn test_base_get(
    table_index: usize,
    oid: u64,
    name: &str,
    rows: usize,
) -> OwnedLogicalPlan {
    let storage = Arc::new(
        TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("table storage"),
    );
    let mut remaining = rows;
    while remaining > 0 {
        let chunk_rows = remaining.min(paro_common::vector::VECTOR_SIZE);
        let start = rows - remaining;
        let values = (start..start + chunk_rows)
            .map(|value| i32::try_from(value).expect("test key fits i32"))
            .collect::<Vec<_>>();
        storage
            .append(&paro_common::test_utils::test_chunk_from_vectors(vec![
                paro_common::test_utils::test_i32_vector(&values),
            ]))
            .expect("populate test table");
        remaining -= chunk_rows;
    }
    let table = Arc::new(TableCatalogEntry::new(
        "paro".to_string(),
        "public".to_string(),
        name.to_string(),
        vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )],
        storage,
        CatalogObjectId::from_raw(oid),
        0,
    ));
    OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
        table_index,
        vec!["id".to_string()],
        vec![LogicalType::Integer],
        table,
    ))))
}

#[test]
fn direct_rowset_reference_admits_and_selects_runtime_filter_region() {
    let mut left = test_base_get(0, 20_001, "probe", 20_000);
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_002, "build", 20);
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
    );
    let join = ComparisonJoin::new(JoinType::Inner, left, right, vec![condition]);
    assert!(supports_runtime_filter_auxiliary(&join, true));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    let input =
        MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).expect("build memo");
    let region = input
        .memo
        .regions()
        .nodes
        .iter()
        .find(|region| {
            region
                .facets
                .iter()
                .any(|facet| facet.kind == RegionFacetKind::RuntimeFilter)
        })
        .expect("runtime-filter AuxiliaryPlanRegion");
    let artifact = region
        .facets
        .iter()
        .find(|facet| facet.kind == RegionFacetKind::RuntimeFilter)
        .unwrap()
        .fingerprint;
    let optimized = input.optimize(&test_grant_classes()).expect("optimize");
    let variant = &optimized.variants[0];
    let contract = variant
        .contracts
        .get(&variant.plan.id)
        .expect("root winner contract");
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::HashJoinRuntimeFilter
    );
    assert_eq!(contract.owned_artifacts.len(), 1);
    // The region facet names a reusable definition; extraction instantiates
    // it in the root execution occurrence, so it is not the runtime handle.
    assert_ne!(contract.owned_artifacts[0].fingerprint, artifact);
    assert!(contract.region_owner.is_some());
    assert!(matches!(
        contract.origin,
        crate::physical::PlanOrigin::SpecializedRegion(_)
    ));
}

#[test]
fn oversized_runtime_filter_candidate_span_yields_to_the_baseline() {
    let mut left = test_base_get(0, 20_011, "probe", 20_000);
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_012, "build", 20);
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            )],
        ),
    )));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let mut budget = SearchBudget::default();
    budget.max_composite_region_groups = 2;

    let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
    assert_eq!(input.memo.regions().nodes.len(), 1);
    assert!(input.memo.regions().deferred_facets.is_empty());
    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let variant = &optimized.variants[0];
    let contract = variant.contracts.get(&variant.plan.id).unwrap();
    assert_eq!(
        contract.implementation,
        PhysicalImplementationFlavor::HashJoin
    );
    assert!(contract.owned_artifacts.is_empty());
    assert_eq!(contract.region_owner, None);
}

#[test]
fn fully_pushable_filter_probe_requires_the_pushdown_compile_capability() {
    let left = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        test_base_get(0, 20_003, "filtered_probe", 0),
        Vec::new(),
    )));
    let right = test_base_get(1, 20_004, "build", 0);
    let join = ComparisonJoin::new(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );

    assert!(supports_runtime_filter_auxiliary(&join, true));
    assert!(!supports_runtime_filter_auxiliary(&join, false));
}

#[test]
fn passthrough_projection_keeps_the_runtime_filter_consumer_lineage() {
    let mut probe = test_base_get(0, 20_005, "projected_probe", 20_000);
    probe.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut left = OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        2,
        probe,
        vec![Expression::Reference(
            ReferenceExpression::new(0, LogicalType::Integer).into(),
        )],
    )));
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_006, "build", 20);
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let join = ComparisonJoin::new(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    let mut budget = SearchBudget::default();
    budget.max_composite_region_groups = 4;
    let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
    assert!(input.memo.regions().nodes.iter().any(|region| {
        region
            .facets
            .iter()
            .any(|facet| facet.kind == RegionFacetKind::RuntimeFilter)
    }));
    let optimized = input
        .optimize(&test_grant_classes())
        .unwrap()
        .variants
        .into_vec()
        .remove(0);
    assert_eq!(
        optimized
            .contracts
            .get(&optimized.plan.id)
            .unwrap()
            .implementation,
        PhysicalImplementationFlavor::HashJoinRuntimeFilter
    );
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(optimized.contracts)
            .with_enforcer_contracts(optimized.enforcers)
            .requiring_winner_contracts()
            .extract_selected(&optimized.plan)
            .unwrap();
    crate::physical::PhysicalPlanVerifier::verify(&physical).unwrap();
    assert!(physical.edges.iter().any(|edge| matches!(
        physical.node(edge.consumer).kind,
        crate::physical::PhysicalNodeKind::RowsetScan(_)
    )));
}

#[test]
fn inner_join_probe_keeps_runtime_filter_consumer_lineage() {
    let fact = test_base_get(0, 20_031, "fact_probe", 20_000);
    let dimension = test_base_get(1, 20_032, "first_build", 20);
    let first_join = ComparisonJoin::new(
        JoinType::Inner,
        fact,
        dimension,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(first_join)));
    let build = test_base_get(2, 20_033, "second_build", 20);
    let join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));

    let logical = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .extract(logical)
            .unwrap();
    let [probe, _] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("outer hash join must be binary");
    };
    let lineage = crate::physical::lineage::trace_rowset_lineage(&physical, *probe, 0);
    assert_eq!(lineage.len(), 1);
    assert!(matches!(
        physical.node(lineage[0].0).kind,
        crate::physical::PhysicalNodeKind::RowsetScan(_)
    ));
}

#[test]
fn semi_join_preserved_probe_keeps_runtime_filter_consumer_lineage() {
    let fact = test_base_get(0, 20_044, "fact_probe", 20_000);
    let dimension = test_base_get(1, 20_045, "first_build", 20);
    let first_join = ComparisonJoin::new(
        JoinType::Semi,
        fact,
        dimension,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(first_join)));
    let build = test_base_get(2, 20_046, "second_build", 20);
    let join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));

    let logical = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .extract(logical)
            .unwrap();
    let [probe, _] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("outer hash join must be binary");
    };
    let lineage = crate::physical::lineage::trace_rowset_lineage(&physical, *probe, 0);
    assert_eq!(lineage.len(), 1);
    assert!(matches!(
        physical.node(lineage[0].0).kind,
        crate::physical::PhysicalNodeKind::RowsetScan(_)
    ));
}

#[test]
fn left_outer_preserved_probe_keeps_runtime_filter_consumer_lineage() {
    let preserved = test_base_get(0, 20_034, "preserved_probe", 20_000);
    let nullable_build = test_base_get(1, 20_035, "nullable_build", 2_000);
    let left_join = ComparisonJoin::new(
        JoinType::Left,
        preserved,
        nullable_build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(left_join)));
    let build = test_base_get(2, 20_036, "filter_build", 20);
    let join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));

    let logical = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .extract(logical)
            .unwrap();
    let [probe, _] = physical.child_ids(&physical.node(physical.root).children) else {
        panic!("outer hash join must be binary");
    };
    let lineage = crate::physical::lineage::trace_rowset_lineage(&physical, *probe, 0);
    assert_eq!(lineage.len(), 1);
    assert!(matches!(
        physical.node(lineage[0].0).kind,
        crate::physical::PhysicalNodeKind::RowsetScan(_)
    ));
}

#[test]
fn left_outer_nullable_build_output_stops_runtime_filter_lineage() {
    let preserved = test_base_get(0, 20_037, "preserved_probe", 20_000);
    let nullable_build = test_base_get(1, 20_038, "nullable_build", 2_000);
    let left_join = ComparisonJoin::new(
        JoinType::Left,
        preserved,
        nullable_build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    let probe = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(left_join)));
    let build = test_base_get(2, 20_039, "filter_build", 20);
    let join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );

    assert!(!supports_runtime_filter_auxiliary(&join, true));
}

fn nested_runtime_filter_input(
    build_side: paro_planner::operator::join::JoinBuildSideConstraint,
) -> OptimizationInput {
    let mut fact = test_base_get(0, 20_041, "fact_probe", 20_000);
    fact.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut first_build = test_base_get(1, 20_042, "first_build", 20);
    first_build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let mut first_join = ComparisonJoin::new(
        JoinType::Inner,
        fact,
        first_build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    first_join.build_side_constraint = build_side;
    let mut probe =
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(first_join)));
    probe.stats.estimated_cardinality = Some(CardinalityEstimate::exact(200));
    // With right builds, both RFs reach source 0 through the first join.
    // Without that constraint the outer join may instead filter source 2.
    let mut second_build = test_base_get(2, 20_043, "second_build", 500);
    second_build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(500));
    let mut second_join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        second_build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    second_join.build_side_constraint = build_side;
    let mut plan =
        OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(second_join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    MemoBuilder::build(plan, BindContext::new(), SearchBudget::default())
        .expect("build nested runtime-filter memo")
        .with_strong_incumbent_export(true)
}

/// Return (source identity, physical occurrence), after checking executable
/// contracts. Equal source IDs alone must not hide two separate scan nodes.
fn nested_runtime_filter_consumers(
    plan: crate::physical::selected::SelectedChild,
    contracts: WinnerPhysicalContracts,
    enforcers: ExtractedEnforcerContracts,
) -> Vec<(usize, usize)> {
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(contracts)
            .with_enforcer_contracts(enforcers)
            .requiring_winner_contracts()
            .extract_selected(&plan)
            .unwrap();
    crate::physical::PhysicalPlanVerifier::verify(&physical).unwrap();
    let mut consumers = physical
        .edges
        .iter()
        .filter(|edge| {
            matches!(
                edge.kind,
                crate::physical::PhysicalEdgeKind::RuntimeFilter(_)
            )
        })
        .map(|edge| {
            let crate::physical::PhysicalNodeKind::RowsetScan(scan) =
                &physical.node(edge.consumer).kind
            else {
                panic!("nested runtime filter must reach a rowset scan");
            };
            (scan.table_index, edge.consumer.index())
        })
        .collect::<Vec<_>>();
    consumers.sort_unstable();
    consumers
}

#[test]
fn nested_filters_share_one_ordered_source_work_lane() {
    // Price one fixed legal witness: baseline scans and two right-build RFs.
    // All candidate data/costs still come from the production providers. The
    // unrestricted companion test below retains the normal choice domain.
    struct FixedRfWitness {
        providers: Arc<ImplementationRegistry>,
        id: ImplementationId,
    }
    impl PhysicalImplementation for FixedRfWitness {
        fn id(&self) -> ImplementationId {
            self.id
        }

        fn grant_dependency(&self) -> GrantDependencyDescriptor {
            self.providers
                .implementation(self.id)
                .unwrap()
                .grant_dependency()
        }

        fn grant_dependency_for(
            &self,
            expr: &LogicalExpr,
            ctx: &ImplementationContext<'_>,
        ) -> GrantDependencyDescriptor {
            self.providers
                .implementation(self.id)
                .unwrap()
                .grant_dependency_for(expr, ctx)
        }

        fn matches(
            &self,
            expr: &LogicalExpr,
            goal: OptimizationGoal,
            ctx: &ImplementationContext<'_>,
        ) -> bool {
            (expr.key.children.is_empty() == (self.id == PLANNER_BASELINE_IMPLEMENTATION))
                && self
                    .providers
                    .implementation(self.id)
                    .unwrap()
                    .matches(expr, goal, ctx)
        }

        fn candidates(
            &self,
            expr: LogicalExprId,
            goal: OptimizationGoal,
            ctx: &ImplementationContext<'_>,
        ) -> Result<Box<[PhysicalCandidate]>> {
            self.providers
                .implementation(self.id)
                .unwrap()
                .candidates(expr, goal, ctx)
        }
    }

    let mut input =
        nested_runtime_filter_input(paro_planner::operator::join::JoinBuildSideConstraint::Right);
    input.mode = SearchMode::Direct; // Fixed physical witness, no mandatory baseline join phase.
    input.root_goal.grant = GrantGoalKey::Class(test_grant_classes()[0].id);
    let mut registry = ImplementationRegistry::default();
    implementation::register_implementations(
        &mut registry,
        input.planner_state.clone(),
        Arc::new(
            test_grant_classes()
                .into_iter()
                .map(|class| (class.id, class))
                .collect(),
        ),
        input.calibration.clone(),
        input.force_spill,
    )
    .unwrap();
    let providers = Arc::new(registry);
    let mut registry = ImplementationRegistry::default();
    for id in [
        PLANNER_BASELINE_IMPLEMENTATION,
        PLANNER_HASH_JOIN_RUNTIME_FILTER,
    ] {
        registry
            .register_implementation(FixedRfWitness {
                providers: providers.clone(),
                id,
            })
            .unwrap();
    }
    input.memo.set_calibration(input.calibration.clone());
    let mut engine = CascadesEngine::new(input.memo, registry);
    engine.prime_grant_context(test_grant_classes()).unwrap();
    let candidate = engine
        .optimize(input.root, input.root_goal, input.mode)
        .unwrap();
    let root = engine
        .memo()
        .freeze_candidate_tree(ChildWinnerRef {
            group: input.root,
            goal: input.root_goal,
            candidate: candidate.candidate,
        })
        .unwrap();
    let extracted = extract_frozen_planner_tree(
        engine.memo(),
        &input.planner_state.read().unwrap(),
        &input.bind_context,
        input.root,
        input.root_goal,
        root.clone(),
        input.mode,
    )
    .unwrap();
    let presented = enforce_result_presentation(
        extracted,
        &input.presentation,
        &input.bind_context,
        input.calibration.as_ref(),
        root.winner.physical_fingerprint,
        root.winner.cost,
    )
    .unwrap();
    let consumers = nested_runtime_filter_consumers(
        presented.plan,
        Arc::new(presented.contracts),
        Arc::new(presented.enforcers),
    );
    assert_eq!(consumers.len(), 2);
    assert_eq!(consumers[0].0, 0);
    assert_eq!(
        consumers[0], consumers[1],
        "both RF edges must reach the same scan"
    );
    let inner = &root.children[0];
    for node in [&root, inner] {
        assert_eq!(
            node.physical.key.implementation,
            PLANNER_HASH_JOIN_RUNTIME_FILTER
        );
        let CostComposition::SidewaysFilter {
            filtered_child,
            sources,
            ..
        } = &node.winner.cost_composition
        else {
            panic!("selected RF must retain its source-work composition");
        };
        assert_eq!(*filtered_child, 0);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source, WorkSourceId(0));
    }
    let lanes = &root.winner.source_work;
    assert_eq!(
        lanes
            .iter()
            .filter(|lane| lane.source == WorkSourceId(0))
            .count(),
        1
    );
    let lane = lanes
        .iter()
        .find(|lane| lane.source == WorkSourceId(0))
        .unwrap();
    assert_eq!(lane.source_rows, 20_000);
    assert_eq!(lane.retentions.len(), 2);
    assert_eq!(lane.filters.len(), 2);
    assert!(lanes
        .iter()
        .filter(|lane| lane.source != WorkSourceId(0))
        .all(|lane| lane.filters.is_empty()));
    let inner_lane = inner
        .winner
        .source_work
        .iter()
        .find(|lane| lane.source == WorkSourceId(0))
        .unwrap();
    assert_eq!(inner_lane.filters.len(), 1);
    assert!(lane.filters.contains(&inner_lane.filters[0]));
    let [a, b] = lane.filters.as_ref() else {
        unreachable!()
    };
    assert_ne!(a.domain, b.domain);
    assert_ne!(a.evaluation, b.evaluation);
    assert_eq!((a.evaluation_rows, b.evaluation_rows), (20_000, 20_000));

    // Independently price the specified selectivity-first order from each
    // evaluation's immutable full-source work, not from an already reduced
    // lane. Different RF representations need not have equal apply costs.
    let a_work = a.full_apply_cost.score.range.expected;
    let b_work = b.full_apply_cost.score.range.expected;
    assert!(a_work > 0.0 && b_work > 0.0);
    assert_ne!(a.expected_retained_ppm, b.expected_retained_ppm);
    let expected_apply = if a.expected_retained_ppm < b.expected_retained_ppm {
        a_work + f64::from(a.expected_retained_ppm) / 1_000_000.0 * b_work
    } else {
        b_work + f64::from(b.expected_retained_ppm) / 1_000_000.0 * a_work
    };
    assert!((lane.filter_apply_cost.score.range.expected - expected_apply).abs() < 1e-9);
    assert!(expected_apply < a_work + b_work);
}

#[test]
fn nested_filters_do_not_merge_distinct_build_domains() {
    let optimized =
        nested_runtime_filter_input(paro_planner::operator::join::JoinBuildSideConstraint::Either)
            .optimize(&test_grant_classes())
            .expect("optimize unconstrained nested filters");
    let root = optimized.strong_incumbent_plans[0].frozen();
    assert_eq!(
        root.physical.key.implementation,
        PLANNER_HASH_JOIN_RUNTIME_FILTER
    );
    assert_eq!(
        root.children[0].physical.key.implementation,
        PLANNER_HASH_JOIN_RUNTIME_FILTER
    );
    let lane = root
        .winner
        .source_work
        .iter()
        .find(|lane| lane.source == WorkSourceId(0))
        .expect("the two nested filters must reach the fact source");
    assert_eq!(lane.retentions.len(), 2);
    assert_eq!(lane.filters.len(), 2);
    assert_ne!(lane.retentions[0].domain, lane.retentions[1].domain);
    assert_ne!(lane.filters[0].evaluation, lane.filters[1].evaluation);

    let variant = optimized.variants.into_vec().remove(0);
    let consumers =
        nested_runtime_filter_consumers(variant.plan, variant.contracts, variant.enforcers);
    assert_eq!(consumers.len(), 2);
    assert_eq!((consumers[0].0, consumers[1].0), (0, 0));
}

#[test]
fn union_all_probe_owns_one_runtime_filter_with_two_scan_consumers() {
    let mut first = test_base_get(0, 20_021, "first_probe", 10_000);
    first.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut second = test_base_get(1, 20_022, "second_probe", 10_000);
    second.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut union = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(
        SetOperation::union(2, first, second, true, vec![LogicalType::Integer]),
    ));
    union.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut build = test_base_get(3, 20_023, "build", 20);
    build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let join = ComparisonJoin::new(
        JoinType::Inner,
        union,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    let mut budget = SearchBudget::default();
    budget.max_composite_region_groups = 8;
    let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
    let optimized = input
        .optimize(&test_grant_classes())
        .unwrap()
        .variants
        .into_vec()
        .remove(0);
    assert_eq!(
        optimized
            .contracts
            .get(&optimized.plan.id)
            .unwrap()
            .implementation,
        PhysicalImplementationFlavor::HashJoinRuntimeFilter
    );
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(optimized.contracts)
            .with_enforcer_contracts(optimized.enforcers)
            .requiring_winner_contracts()
            .extract_selected(&optimized.plan)
            .unwrap();
    crate::physical::PhysicalPlanVerifier::verify(&physical).unwrap();
    let edges = physical
        .edges
        .iter()
        .filter(|edge| {
            matches!(
                edge.kind,
                crate::physical::PhysicalEdgeKind::RuntimeFilter(_)
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(edges.len(), 2);
    assert!(edges.iter().all(|edge| matches!(
        physical.node(edge.consumer).kind,
        crate::physical::PhysicalNodeKind::RowsetScan(_)
    )));
}

#[test]
fn build_left_semi_join_filters_every_union_all_probe_source() {
    let mut first = test_base_get(0, 20_031, "first_probe", 10_000);
    first.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut second = test_base_get(1, 20_032, "second_probe", 10_000);
    second.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut union = OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(
        SetOperation::union(2, first, second, true, vec![LogicalType::Integer]),
    ));
    union.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut build = test_base_get(3, 20_033, "build", 20);
    build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let join = ComparisonJoin::new(
        JoinType::Semi,
        build,
        union,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        )],
    );
    assert!(supports_build_left_runtime_filter_auxiliary(&join, true));
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    let mut budget = SearchBudget::default();
    budget.max_composite_region_groups = 8;
    let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
    let optimized = input
        .optimize(&test_grant_classes())
        .unwrap()
        .variants
        .into_vec()
        .remove(0);
    assert_eq!(
        optimized
            .contracts
            .get(&optimized.plan.id)
            .unwrap()
            .implementation,
        PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
    );
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(optimized.contracts)
            .with_enforcer_contracts(optimized.enforcers)
            .requiring_winner_contracts()
            .extract_selected(&optimized.plan)
            .unwrap();
    crate::physical::PhysicalPlanVerifier::verify(&physical).unwrap();
    assert_eq!(
        physical
            .edges
            .iter()
            .filter(|edge| matches!(
                edge.kind,
                crate::physical::PhysicalEdgeKind::RuntimeFilter(_)
            ))
            .count(),
        2
    );
}

#[test]
fn global_sort_enforcer_is_extracted_as_an_executable_plan_node() {
    let bind_context = BindContext::new();
    let mut input = MemoBuilder::build(
        constant_projection(&bind_context, 7),
        bind_context,
        SearchBudget::default(),
    )
    .unwrap();
    let column = input.presentation.columns[0];
    let mut required = input
        .memo
        .required(input.root_goal.required)
        .cloned()
        .unwrap();
    required.ordering = OrderingRequirement::Ordered(RequiredOrdering {
        keys: vec![OrderingKey {
            column,
            direction: SortDirection::Asc,
            nulls: NullOrder::Last,
            collation: None,
        }]
        .into_boxed_slice(),
        scope: OrderingScope::Global,
    });
    input.root_goal.required = input.memo.intern_required(required).unwrap();

    let optimized = input.optimize(&test_grant_classes()).unwrap();
    let optimized = optimized.variants.into_vec().remove(0);
    assert!(matches!(
        optimized
            .enforcers
            .get(&optimized.plan.id)
            .and_then(|chain| chain.last())
            .unwrap()
            .contract
            .origin,
        crate::physical::PlanOrigin::Enforcer(_)
    ));
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(optimized.contracts)
            .with_enforcer_contracts(optimized.enforcers)
            .extract_selected(&optimized.plan)
            .unwrap();
    assert!(matches!(
        physical.node(physical.root).kind,
        crate::physical::PhysicalNodeKind::Sort(_)
    ));
}

fn constant_projection(bind_context: &BindContext, value: i32) -> OwnedLogicalPlan {
    OwnedLogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(Projection::new(
            9,
            OwnedLogicalPlan::dummy_scan(bind_context),
            vec![Expression::Constant(
                ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
            )],
        )),
    )
}

#[test]
fn fixed_membership_fingerprint_uses_set_semantics() {
    use paro_storage::index::{FixedMembership, FixedMembershipBuildPolicy, Predicate};

    let fingerprint = |values| {
        let mut fingerprint = StableFingerprintBuilder::default();
        encode_predicate(
            &mut fingerprint,
            &Predicate::FixedIn {
                column_id: 7,
                values,
            },
        );
        fingerprint.finish()
    };
    let dense = FixedMembership::i32_with_policy(
        vec![15, 10, 12, 12],
        FixedMembershipBuildPolicy::new(512, 256),
    );
    let sorted =
        FixedMembership::i32_with_policy(vec![12, 15, 10], FixedMembershipBuildPolicy::new(0, 0));
    let wider = FixedMembership::i64(vec![10, 12, 15]);

    assert_eq!(fingerprint(dense), fingerprint(sorted));
    assert_ne!(
        fingerprint(FixedMembership::i32(vec![10, 12, 15])),
        fingerprint(wider)
    );
}

#[test]
fn query_ir_identity_uses_scalar_semantics_not_planner_node_id() {
    let bind_context = BindContext::new();
    let first = MemoBuilder::build(
        constant_projection(&bind_context, 7),
        bind_context.clone(),
        SearchBudget::default(),
    )
    .unwrap();
    let second = MemoBuilder::build(
        constant_projection(&bind_context, 7),
        bind_context.clone(),
        SearchBudget::default(),
    )
    .unwrap();
    let different = MemoBuilder::build(
        constant_projection(&bind_context, 8),
        bind_context,
        SearchBudget::default(),
    )
    .unwrap();

    let key = |input: &OptimizationInput| {
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        input.memo.logical_expr(expression).unwrap().key.clone()
    };
    let first_key = key(&first);
    let second_key = key(&second);
    let different_key = key(&different);
    assert_eq!(first_key.operator, second_key.operator);
    assert_eq!(first_key.scalars, second_key.scalars);
    assert_ne!(first_key.operator, different_key.operator);
    assert_eq!(
        first
            .planner_state
            .read()
            .expect("planner state poisoned")
            .scalars
            .len(),
        1
    );
}

#[test]
fn query_ir_identity_excludes_positional_projection_layout() {
    let child = || {
        OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
            41,
            Vec::new(),
            vec!["a".to_string(), "b".to_string()],
            vec![LogicalType::Integer, LogicalType::BigInt],
        )))
    };
    let mut identity = Filter::new(child(), Vec::new());
    identity.projection_map = vec![0, 1].into();
    let mut permuted = Filter::new(child(), Vec::new());
    permuted.projection_map = vec![1, 0].into();
    let scalars = ScalarArena::default();

    let identity = query_operator_fingerprint(
        &OwnedLogicalPlan::synthetic(LogicalOperator::Filter(identity)),
        &[],
        &scalars,
    )
    .unwrap();
    let permuted = query_operator_fingerprint(
        &OwnedLogicalPlan::synthetic(LogicalOperator::Filter(permuted)),
        &[],
        &scalars,
    )
    .unwrap();

    assert_eq!(identity, permuted);
}

#[test]
fn exact_is_the_default_root_contract_and_approximate_requires_opt_in() {
    let bind_context = BindContext::new();
    let exact = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::TopN(TopN::new(
            OwnedLogicalPlan::dummy_scan(&bind_context),
            Vec::new(),
            1,
            0,
        )),
    );
    assert_eq!(required_result_guarantee(&exact), ResultGuarantee::Exact);

    let approximate = OwnedLogicalPlan::new(
        &bind_context,
        LogicalOperator::TopN(
            TopN::new(
                OwnedLogicalPlan::dummy_scan(&bind_context),
                Vec::new(),
                1,
                0,
            )
            .with_hnsw_options(paro_storage::index::hnsw::HnswQueryOptions {
                objective: paro_storage::index::hnsw::HnswSearchObjective::CostOptimized,
                ..Default::default()
            }),
        ),
    );
    assert_eq!(
        required_result_guarantee(&approximate),
        ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
    );
}
