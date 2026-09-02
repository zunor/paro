// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Planner integration and contract tests.

use std::sync::Arc;

use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, TableCatalogEntry, VertexTableInfo};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ConstantExpression, Expression, ReferenceExpression,
    WindowExpression, WindowFrame,
};
use paro_planner::operator::join::{Join, JoinCondition, JoinType};
use paro_planner::operator::{
    CTERef, ComparisonJoin, EmptyResult, ExpressionGet, Filter, Get, GraphScan, Projection,
    SetOperation, TopN, Window as LogicalWindow,
};
use paro_planner::plan::CardinalityEstimate;
use paro_storage::table::table_factory::TableFactory;

use super::*;

fn test_grant_classes() -> [ResourceGrantClass; 1] {
    [ResourceGrantClass {
        id: super::super::ids::ResourceGrantClassId(0),
        hard_memory_bytes: u64::MAX,
        spill_policy: crate::physical::SpillPolicy::Allowed,
        max_parallel_tasks: 1,
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
    let facts = |width| ResolvedPlannerCostFacts {
        output_rows: CompactRange::point(1_000.0).unwrap(),
        child_rows: vec![CompactRange::point(1_000.0).unwrap()].into_boxed_slice(),
        output_rows_hard_upper: Some(1_000),
        child_rows_hard_upper: vec![Some(1_000)].into_boxed_slice(),
        child_row_widths: vec![width].into_boxed_slice(),
        output_row_width: width,
        hash_key_width: None,
        scan_access_width: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_probe_is_direct: false,
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
    let child = memo.create_group(
        schema.clone(),
        LogicalProperties::default(),
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
        output_row_width: 16,
        hash_key_width: None,
        scan_access_width: None,
        perfect_hash: None,
        topn_capacity: None,
        runtime_filter_probe_multiplicity: RuntimeFilterProbeMultiplicity::Unknown,
        runtime_filter_probe_source_rows: None,
        runtime_filter_probe_is_direct: false,
        runtime_filter_key_types: Box::new([]),
    };

    let initial = expression_cost_facts(&memo, parent, &[child], &template).unwrap();
    assert_eq!(initial.child_rows[0].expected, 100.0);

    memo.group_mut(child).unwrap().cardinality =
        GroupCardinality::new(Fingerprint(3), CardinalityRecipeKind::JoinRegion, 4, 5, 6);
    let refined = expression_cost_facts(&memo, parent, &[child], &template).unwrap();
    assert_eq!(refined.child_rows[0].expected, 5.0);
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
        let mut binding_ids = BindingCatalog::default();
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
fn memo_round_trip_derives_layout_after_winner_selection() {
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
                integer_value_rows(512, 2),
                vec!["lower".to_string(), "upper".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        );
        left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(512));
        let mut right = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                integer_value_rows(512, 1),
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
            integer_value_rows(1_024, 2),
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

#[test]
fn mark_join_to_semi_is_an_explicit_isolatable_transformation() {
    fn plan(bind_context: &BindContext) -> LogicalPlan {
        let left = LogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                integer_value_rows(1, 1),
                vec!["left".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let right = LogicalPlan::new(
            bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                integer_value_rows(1, 1),
                vec!["right".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let column = |table_index, logical_type| {
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, 0),
                logical_type,
            ))
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
        let filter = LogicalPlan::new(
            bind_context,
            LogicalOperator::Filter(Filter::new(
                LogicalPlan::new(bind_context, LogicalOperator::Join(Join::Comparison(join))),
                vec![column(mark_index, LogicalType::Boolean)],
            )),
        );
        LogicalPlan::new(
            bind_context,
            LogicalOperator::Projection(Projection::new(
                91,
                filter,
                vec![column(0, LogicalType::Integer)],
            )),
        )
    }

    fn optimize(budget: SearchBudget) -> OptimizationOutput {
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
        fn find(plan: &LogicalPlan) -> Option<JoinType> {
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
                    Expression::Constant(ConstantExpression::new(
                        Value::Integer(0),
                        LogicalType::Integer,
                    ))
                })
                .collect()
        })
        .collect()
}

fn test_base_get(table_index: usize, oid: u64, name: &str, rows: usize) -> LogicalPlan {
    let storage = Arc::new(
        TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("table storage"),
    );
    let mut remaining = rows;
    while remaining > 0 {
        let chunk_rows = remaining.min(paro_common::vector::VECTOR_SIZE);
        storage
            .append(&paro_common::test_utils::test_chunk_from_vectors(vec![
                paro_common::test_utils::test_i32_vector(&vec![0; chunk_rows]),
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
    LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
        table_index,
        vec!["id".to_string()],
        vec![LogicalType::Integer],
        table,
    )))
}

#[test]
fn direct_rowset_reference_admits_and_selects_runtime_filter_region() {
    let mut left = test_base_get(0, 20_001, "probe", 20_000);
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_002, "build", 20);
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
    let mut left = test_base_get(0, 20_011, "probe", 20_000);
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_012, "build", 20);
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
        test_base_get(0, 20_003, "filtered_probe", 0),
        Vec::new(),
    )));
    let right = test_base_get(1, 20_004, "build", 0);
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
    let mut probe = test_base_get(0, 20_005, "projected_probe", 20_000);
    probe.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut left = LogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
        2,
        probe,
        vec![Expression::Reference(ReferenceExpression::new(
            0,
            LogicalType::Integer,
        ))],
    )));
    left.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut right = test_base_get(1, 20_006, "build", 20);
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
            .extract(&optimized.plan)
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
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    let probe = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(first_join)));
    let build = test_base_get(2, 20_033, "second_build", 20);
    let join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));

    let logical = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
    let physical =
        crate::physical::PhysicalPlanExtractor::new(crate::physical::ExtractionContext::default())
            .extract(&logical)
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
fn nested_filters_do_not_independently_discount_the_same_rowset() {
    let mut fact = test_base_get(0, 20_041, "fact_probe", 20_000);
    fact.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut first_build = test_base_get(1, 20_042, "first_build", 20);
    first_build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let first_join = ComparisonJoin::new(
        JoinType::Inner,
        fact,
        first_build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    let mut probe = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(first_join)));
    probe.stats.estimated_cardinality = Some(CardinalityEstimate::exact(200));
    // The second build is selective at the 20,000-row source, but its lineage
    // crosses the first join. Until one composite region jointly prices the
    // ordered predicate stages, independently discounting the same fact-scan
    // winner twice would invent work savings.
    let mut second_build = test_base_get(2, 20_043, "second_build", 500);
    second_build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(500));
    let second_join = ComparisonJoin::new(
        JoinType::Inner,
        probe,
        second_build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(second_join)));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));

    let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default())
        .expect("build nested runtime-filter memo");
    let optimized = input.optimize(&test_grant_classes()).expect("optimize");
    let runtime_filters = optimized.variants[0]
        .contracts
        .values()
        .filter(|contract| {
            contract.implementation == PhysicalImplementationFlavor::HashJoinRuntimeFilter
        })
        .count();

    assert_eq!(runtime_filters, 1);
}

#[test]
fn union_all_probe_owns_one_runtime_filter_with_two_scan_consumers() {
    let mut first = test_base_get(0, 20_021, "first_probe", 10_000);
    first.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut second = test_base_get(1, 20_022, "second_probe", 10_000);
    second.stats.estimated_cardinality = Some(CardinalityEstimate::exact(10_000));
    let mut union = LogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::union(
        2,
        first,
        second,
        true,
        vec![LogicalType::Integer],
    )));
    union.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20_000));
    let mut build = test_base_get(3, 20_023, "build", 20);
    build.stats.estimated_cardinality = Some(CardinalityEstimate::exact(20));
    let join = ComparisonJoin::new(
        JoinType::Inner,
        union,
        build,
        vec![JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        )],
    );
    assert!(supports_runtime_filter_auxiliary(&join, true));
    let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(join)));
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
            .extract(&optimized.plan)
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
fn query_ir_identity_excludes_positional_projection_layout() {
    let child = || {
        LogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
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
        &LogicalPlan::synthetic(LogicalOperator::Filter(identity)),
        &[],
        &scalars,
    )
    .unwrap();
    let permuted = query_operator_fingerprint(
        &LogicalPlan::synthetic(LogicalOperator::Filter(permuted)),
        &[],
        &scalars,
    )
    .unwrap();

    assert_eq!(identity, permuted);
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
