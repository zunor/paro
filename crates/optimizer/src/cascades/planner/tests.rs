// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner integration and contract tests.

use std::sync::Arc;

use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, TableCatalogEntry, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_planner::expression::{
    AggregateExpression, ConstantExpression, Expression, ReferenceExpression, WindowExpression,
    WindowFrame,
};
use paro_planner::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::operator::{
    CTERef, ComparisonJoin, EmptyResult, ExpressionGet, Filter, Get, GraphScan, Projection, TopN,
    Window as LogicalWindow,
};
use paro_planner::plan::CardinalityEstimate;
use paro_storage::table::table_factory::TableFactory;

use super::*;

fn test_grant_classes() -> [ResourceGrantClass; 1] {
    [ResourceGrantClass {
        id: super::super::ids::ResourceGrantClassId(0),
        hard_memory_bytes: u64::MAX,
        spill_policy: crate::physical::SpillPolicy::Allowed,
        concurrency_class: 0,
    }]
}

#[test]
fn cross_product_memory_tracks_only_the_materialized_build_side() {
    let mut left = LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        0,
        Vec::new(),
        vec!["left".to_string()],
        vec![LogicalType::BigInt],
    )));
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_000_000_000));
    let mut right = LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
        1,
        Vec::new(),
        vec!["right".to_string()],
        vec![LogicalType::BigInt],
    )));
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(3));
    let mut product = LogicalPlan::synthetic(LogicalOperator::Join(Join::cross(left, right)));
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
fn calibrated_tuple_work_distinguishes_narrow_and_wide_intermediates() {
    let facts = |width| PlannerCostFacts {
        output_rows: CompactRange::point(1_000.0).unwrap(),
        child_rows: vec![CompactRange::point(1_000.0).unwrap()].into_boxed_slice(),
        output_rows_hard_upper: Some(1_000),
        child_rows_hard_upper: vec![Some(1_000)].into_boxed_slice(),
        child_row_widths: vec![width].into_boxed_slice(),
        output_row_width: width,
        perfect_hash_slots: None,
    };
    let calibrated_cost = |facts: &PlannerCostFacts| {
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
fn graph_relation_identity_is_part_of_the_query_ir_fingerprint() {
    let scan = |label: &str, table_oid: u64| {
        LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
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
        )))
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
        LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
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
        )))
    };
    let scalars = ScalarArena::default();
    let first = query_operator_fingerprint(&scan(7), &[], &scalars).unwrap();
    let second = query_operator_fingerprint(&scan(8), &[], &scalars).unwrap();
    assert_ne!(first, second);
}

#[test]
fn graph_filter_is_part_of_the_query_ir_fingerprint() {
    let scan = |value: bool| {
        LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
            VertexTableInfo {
                table_name: "person".to_string(),
                table_oid: 11,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![1],
            },
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(value),
                return_type: LogicalType::Boolean,
            })),
            1,
            2,
            "Person".to_string(),
            "g".to_string(),
            "public".to_string(),
        )))
    };
    let fingerprint = |mut plan: LogicalPlan| {
        let mut binding_ids = BTreeMap::new();
        let mut columns = ColumnCatalog::default();
        let mut scalars = ScalarArena::default();
        let roots = intern_operator_scalars(
            &mut plan.operator,
            &[],
            &[],
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
        LogicalPlan::synthetic(LogicalOperator::CTERef(CTERef::new(
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
fn memo_round_trip_preserves_tree_shape_without_positional_repair() {
    let bind_context = BindContext::new();
    let leaf = LogicalPlan::dummy_scan(&bind_context);
    let wrapped = LogicalPlan::new(
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
fn memo_winner_names_the_hash_join_implementation() {
    let bind_context = BindContext::new();
    let left = LogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["left".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let right = LogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            1,
            vec![],
            vec!["right".to_string()],
            vec![LogicalType::Integer],
        )),
    );
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = LogicalPlan::new(
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
        let mut left = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![],
                vec!["lower".to_string(), "upper".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(512));
        let mut right = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![],
                vec!["point".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(512));
        let reference =
            |index| Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer));
        let mut join = LogicalPlan::new(
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
    let mut values = LogicalPlan::new(
        &bind_context,
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![],
            vec!["grp".to_string(), "value".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        )),
    );
    values.stats.estimated_cardinality = Some(CardinalityEstimate::exact(1_024));
    let aggregate =
        AggregateExpression::new(get_count_star_function(), Vec::new(), LogicalType::BigInt);
    let mut plan = LogicalPlan::new(
        &bind_context,
        LogicalOperator::Window(LogicalWindow::new(
            1,
            vec![WindowExpression::aggregate(
                aggregate,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    LogicalType::Integer,
                ))],
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
            .extract(&optimized.plan)
            .unwrap();
    assert!(matches!(
        physical.node(physical.root).kind,
        crate::physical::PhysicalNodeKind::PartitionAggregateWindow(_)
    ));
}

fn test_base_get(table_index: usize, oid: u64, name: &str) -> LogicalPlan {
    let storage = Arc::new(
        TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("table storage"),
    );
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
    LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
        table_index,
        vec!["id".to_string()],
        vec![LogicalType::Integer],
        table,
    )))
}

#[test]
fn direct_rowset_reference_admits_and_selects_runtime_filter_region() {
    let mut left = test_base_get(0, 20_001, "probe");
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_002, "build");
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let condition = JoinCondition::equality(
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
    );
    let join = ComparisonJoin::new(JoinType::Inner, left, right, vec![condition]);
    assert!(supports_runtime_filter_auxiliary(&join, true));
    let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
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
    assert_eq!(contract.owned_artifacts[0].fingerprint, artifact);
    assert!(contract.region_owner.is_some());
    assert!(matches!(
        contract.origin,
        crate::physical::PlanOrigin::SpecializedRegion(_)
    ));
}

#[test]
fn oversized_optional_runtime_filter_facet_yields_to_the_baseline() {
    let mut left = test_base_get(0, 20_011, "probe");
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_012, "build");
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            left,
            right,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            )],
        ),
    )));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let mut budget = SearchBudget::default();
    budget.max_composite_region_groups = 2;

    let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
    assert!(input.memo.regions().nodes.is_empty());
    assert_eq!(input.memo.regions().dropped_optional_facets.len(), 1);
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
    let left = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
        test_base_get(0, 20_003, "filtered_probe"),
        Vec::new(),
    )));
    let right = test_base_get(1, 20_004, "build");
    let join = ComparisonJoin::new(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );

    assert!(supports_runtime_filter_auxiliary(&join, true));
    assert!(!supports_runtime_filter_auxiliary(&join, false));
}

#[test]
fn passthrough_projection_keeps_the_runtime_filter_consumer_lineage() {
    let mut left = LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        2,
        test_base_get(0, 20_005, "projected_probe"),
        vec![Expression::Reference(ReferenceExpression::new(
            0,
            LogicalType::Integer,
        ))],
    )));
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_006, "build");
    right.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let join = ComparisonJoin::new(
        JoinType::Inner,
        left,
        right,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));
    let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    let optimized = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default())
        .unwrap()
        .optimize(&test_grant_classes())
        .unwrap()
        .variants
        .into_vec()
        .remove(0);
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .with_winner_contracts(optimized.contracts)
            .with_enforcer_contracts(optimized.enforcers)
            .requiring_winner_contracts()
            .extract(&optimized.plan)
            .unwrap();
    crate::physical::PhysicalPlanVerifier::verify(&physical).unwrap();
    assert!(physical.edges.iter().any(|edge| matches!(
        physical.node(edge.consumer).kind,
        crate::physical::PhysicalNodeKind::RowsetScan(_)
    )));
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
            .extract(&optimized.plan)
            .unwrap();
    assert!(matches!(
        physical.node(physical.root).kind,
        crate::physical::PhysicalNodeKind::Sort(_)
    ));
}

fn constant_projection(bind_context: &BindContext, value: i32) -> LogicalPlan {
    LogicalPlan::new(
        bind_context,
        LogicalOperator::Projection(Projection::new(
            9,
            LogicalPlan::dummy_scan(bind_context),
            vec![Expression::Constant(ConstantExpression::new(
                Value::Integer(value),
                LogicalType::Integer,
            ))],
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
fn exact_is_the_default_root_contract_and_approximate_requires_opt_in() {
    let bind_context = BindContext::new();
    let exact = LogicalPlan::new(
        &bind_context,
        LogicalOperator::TopN(TopN::new(
            LogicalPlan::dummy_scan(&bind_context),
            Vec::new(),
            1,
            0,
        )),
    );
    assert_eq!(required_result_guarantee(&exact), ResultGuarantee::Exact);

    let approximate = LogicalPlan::new(
        &bind_context,
        LogicalOperator::TopN(
            TopN::new(LogicalPlan::dummy_scan(&bind_context), Vec::new(), 1, 0).with_hnsw_options(
                paro_storage::index::hnsw::HnswQueryOptions {
                    objective: paro_storage::index::hnsw::HnswSearchObjective::CostOptimized,
                    ..Default::default()
                },
            ),
        ),
    );
    assert_eq!(
        required_result_guarantee(&approximate),
        ResultGuarantee::ApproximateAllowed(COST_OPTIMIZED_SEARCH_POLICY)
    );
}
