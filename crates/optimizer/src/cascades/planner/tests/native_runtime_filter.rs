// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Task-owned, storage-free fixtures for native/owned RF admission parity.
//! Positive cases were red with the operator-only native entry point.

use super::*;
use paro_planner::operator::bound_reference::{
    BoundColumnDomain, BoundReference, BoundReferenceId, BoundRelationFactValues,
    BoundRelationFacts, BoundSourceColumn,
};
use paro_storage::statistics::DistinctProvenance;

fn rf_source(
    source: usize,
    occurrence: usize,
    column: usize,
    rows: u64,
    distinct: u64,
) -> BoundSourceColumn {
    BoundSourceColumn {
        source,
        occurrence,
        column,
        rows: Some(CardinalityEstimate::exact(rows)),
        distinct: Some(distinct),
        unique: false,
    }
}

fn rf_boundary(
    table_index: usize,
    types: Vec<LogicalType>,
    rows: u64,
    distinct: &[u64],
    source_lineage: Vec<Option<Vec<BoundSourceColumn>>>,
) -> OwnedLogicalPlan {
    assert_eq!(types.len(), distinct.len());
    assert_eq!(types.len(), source_lineage.len());
    let bindings = (0..types.len())
        .map(|column| ColumnBinding::new(table_index, column))
        .collect();
    let facts = Arc::new(BoundRelationFacts::new(
        BoundRelationFactValues {
            cardinality: Some(CardinalityEstimate::exact(rows)),
            column_domains: distinct
                .iter()
                .map(|&keys| BoundColumnDomain {
                    expected_distinct: Some(keys),
                    provenance: DistinctProvenance::Derived,
                    ..BoundColumnDomain::default()
                })
                .collect(),
            source_lineage,
            contains_control_region: false,
            ..BoundRelationFactValues::default()
        },
        types.clone(),
    ));
    let reference = BoundReference::new(
        BoundReferenceId::input_ordinal(table_index),
        bindings,
        types,
    )
    .with_facts(facts)
    .unwrap();
    let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::BoundReference(reference));
    plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
    plan
}

fn rf_join(probe: OwnedLogicalPlan, probe_key: usize) -> OwnedLogicalPlan {
    let key_type = probe.types()[probe_key].clone();
    let build = rf_boundary(
        90,
        vec![key_type.clone()],
        20,
        &[20],
        vec![Some(vec![rf_source(97, 907, 0, 20, 20)])],
    );
    OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
        ComparisonJoin::new(
            JoinType::Inner,
            probe,
            build,
            vec![JoinCondition::equality(
                Expression::Reference(ReferenceExpression::new(probe_key, key_type.clone()).into()),
                Expression::Reference(ReferenceExpression::new(0, key_type).into()),
            )],
        ),
    )))
}

fn rf_native_implementations(plan: &OwnedLogicalPlan) -> PlannerImplementationSet {
    rf_native_with_rowset(plan, true)
}

fn rf_native_with_rowset(plan: &OwnedLogicalPlan, enabled: bool) -> PlannerImplementationSet {
    // The original BoundReference children retain both layout and facts; the
    // native shell has only input ordinals, not an owned descendant tree.
    let mut ordinal = 0;
    let shell = duplicate_plan_preserving_indices(plan, BindContext::new().shared().as_ref())
        .into_parts()
        .2
        .try_map_child_links(&mut |_| {
            let child = BoundReferenceId::input_ordinal(ordinal);
            ordinal += 1;
            Ok::<_, std::convert::Infallible>(child)
        })
        .unwrap();
    let children = plan.children();
    let layouts = children
        .iter()
        .map(|child| child.output_layout())
        .collect::<Vec<_>>();
    let inputs = children
        .iter()
        .zip(&layouts)
        .map(|(child, layout)| {
            let LogicalOperator::BoundReference(reference) = &child.operator else {
                panic!("fixture boundary must be explicit");
            };
            RuntimeFilterInput::Boundary {
                layout,
                facts: &reference.facts,
            }
        })
        .collect::<Vec<_>>();
    planner_native_implementation_set(&shell, enabled, &inputs)
}

fn rf_observed_sources(
    plan: &OwnedLogicalPlan,
) -> Option<Vec<(usize, CardinalityEstimate, RuntimeFilterProbeMultiplicity)>> {
    let LogicalOperator::Join(Join::Comparison(join)) = &plan.operator else {
        panic!("RF fixture must be a comparison join");
    };
    runtime_filter_probe_sources(join).map(|sources| {
        sources
            .iter()
            .map(|source| (source.source.0, source.rows, source.multiplicity))
            .collect()
    })
}

fn corresponding_cost_facts(plan: &OwnedLogicalPlan, id_offset: usize) -> PlannerCostFacts {
    let children = plan.children();
    let layouts = children
        .iter()
        .map(|child| child.output_layout())
        .collect::<Vec<_>>();
    let inputs = children
        .iter()
        .zip(&layouts)
        .map(|(child, layout)| {
            let LogicalOperator::BoundReference(reference) = &child.operator else {
                panic!("explicit boundary")
            };
            RuntimeFilterInput::Boundary {
                layout,
                facts: &reference.facts,
            }
        })
        .collect::<Vec<_>>();
    let mut bindings = BindingCatalog::default();
    let mut stats = HashMap::new();
    let mut id = id_offset;
    for (child, layout) in children.iter().zip(&layouts) {
        let LogicalOperator::BoundReference(reference) = &child.operator else {
            unreachable!()
        };
        for ((binding, ty), column) in layout
            .bindings()
            .iter()
            .zip(layout.types())
            .zip(reference.facts.column_statistics())
        {
            bindings
                .insert(
                    binding.table_index,
                    binding.column_index,
                    ty,
                    ColumnId::new(id),
                )
                .unwrap();
            stats.insert(*binding, column.clone());
            id += 1;
        }
    }
    let scan_cost = Default::default();
    let owned = planner_cost_facts(plan, &stats, &bindings, scan_cost).unwrap();
    let widths = layouts
        .iter()
        .map(|layout| planner_row_width_from_layout(layout, scan_cost))
        .collect::<Vec<_>>();
    let rows = children
        .iter()
        .map(|child| child.stats.estimated_cardinality.unwrap().max)
        .collect::<Vec<_>>();
    // This fixture copy constructs only the test operator shell. Production
    // staging already has the shell and directly borrows the boundary views.
    let shell = duplicate_plan_preserving_indices(plan, BindContext::new().shared().as_ref())
        .into_parts()
        .2
        .try_map_child_links(&mut |_| Ok::<_, std::convert::Infallible>(()))
        .unwrap();
    let native = planner_native_cost_facts(
        &shell,
        &rows,
        &widths,
        planner_row_width_from_layout(&plan.output_layout(), scan_cost),
        scan_cost,
        &inputs,
        &stats,
        &bindings,
    )
    .unwrap();
    assert_eq!(
        format!("{owned:?}"),
        format!("{native:?}"),
        "all adapter fields must agree, not only RF admission"
    );
    native
}

#[test]
fn native_runtime_filter_admits_typed_boundary_lineage() {
    let mut native_admissions = Vec::new();
    for key_type in [LogicalType::Integer, LogicalType::BigInt] {
        let plan = rf_join(
            rf_boundary(
                40,
                vec![key_type.clone()],
                20_000,
                &[2_000],
                vec![Some(vec![rf_source(17, 701, 3, 20_000, 2_000)])],
            ),
            0,
        );
        // Source 17/column 3 is not boundary 40/output 0. These constants
        // describe the fixture, independently of either admission function.
        assert_eq!(
            rf_observed_sources(&plan),
            Some(vec![(
                17,
                CardinalityEstimate::exact(20_000),
                RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys: 2_000 },
            )])
        );
        let owned = planner_implementation_set(&plan, true);
        let native = rf_native_implementations(&plan);
        let facts = corresponding_cost_facts(&plan, 0);
        assert_eq!(
            facts.runtime_filter_build_domain_column,
            Some(ColumnId::new(1))
        );
        assert_eq!(facts.runtime_filter_build_distinct_expected, Some(20));
        assert_eq!(
            facts.runtime_filter_probe_sources[0].source,
            WorkSourceId(17)
        );
        assert!(!rf_native_with_rowset(&plan, false).hash_join_runtime_filter);
        assert!(owned.hash_join_runtime_filter, "owned {key_type:?}");
        assert_eq!(owned.baseline, PhysicalImplementationFlavor::HashJoin);
        assert_eq!(native.baseline, PhysicalImplementationFlavor::HashJoin);
        native_admissions.push((key_type, native.hash_join_runtime_filter));
    }
    assert!(
        native_admissions.iter().all(|(_, admitted)| *admitted),
        "complete typed source lineage must also admit native RF: {native_admissions:?}"
    );
}

#[test]
fn native_runtime_filter_preserves_two_source_occurrences() {
    // A UNION ALL boundary with disjoint key domains: 20,000 + 10,000 rows,
    // 2,000 + 500 distinct keys. Each physical occurrence keeps its own NDV.
    let mut native_admissions = Vec::new();
    for reverse in [false, true] {
        let mut sources = vec![
            rf_source(17, 701, 3, 20_000, 2_000),
            rf_source(29, 702, 5, 10_000, 500),
        ];
        if reverse {
            sources.reverse();
        }
        let plan = rf_join(
            rf_boundary(
                40,
                vec![LogicalType::BigInt],
                30_000,
                &[2_500],
                vec![Some(sources)],
            ),
            0,
        );
        assert_eq!(
            rf_observed_sources(&plan),
            Some(vec![
                (
                    17,
                    CardinalityEstimate::exact(20_000),
                    RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys: 2_000 },
                ),
                (
                    29,
                    CardinalityEstimate::exact(10_000),
                    RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys: 500 },
                ),
            ])
        );
        assert!(planner_implementation_set(&plan, true).hash_join_runtime_filter);
        let facts = corresponding_cost_facts(&plan, 500);
        assert_eq!(
            facts.runtime_filter_probe_source_rows,
            Some(CardinalityEstimate::exact(30_000))
        );
        assert_eq!(facts.runtime_filter_probe_sources.len(), 2);
        assert_eq!(
            facts.runtime_filter_build_domain_column,
            Some(ColumnId::new(501))
        );
        native_admissions.push(rf_native_implementations(&plan).hash_join_runtime_filter);
    }
    assert!(
        native_admissions.iter().all(|admitted| *admitted),
        "both native source occurrences must survive either lineage order"
    );
}

#[test]
fn native_runtime_filter_costs_follow_the_supplied_fact_snapshot() {
    let build = |rows, distinct| {
        rf_join(
            rf_boundary(
                40,
                vec![LogicalType::Integer],
                rows,
                &[distinct],
                vec![Some(vec![rf_source(17, 701, 3, rows, distinct)])],
            ),
            0,
        )
    };
    let old = build(20_000, 2_000);
    let refined = build(100_000, 500);
    let before = corresponding_cost_facts(&old, 0);
    let after = corresponding_cost_facts(&refined, 200);
    assert_eq!(
        before.runtime_filter_probe_source_rows,
        Some(CardinalityEstimate::exact(20_000))
    );
    assert_eq!(
        after.runtime_filter_probe_source_rows,
        Some(CardinalityEstimate::exact(100_000))
    );
    assert_eq!(
        after.runtime_filter_probe_sources[0].multiplicity,
        RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys: 500 }
    );
    assert_eq!(
        format!("{before:?}"),
        format!("{:?}", corresponding_cost_facts(&old, 0)),
        "the immutable earlier evidence is unchanged"
    );
}

#[test]
fn native_runtime_filter_missing_source_rows_never_creates_source_work_credit() {
    let mut source = rf_source(17, 701, 3, 20_000, 2_000);
    source.rows = None;
    let plan = rf_join(
        rf_boundary(
            40,
            vec![LogicalType::Integer],
            20_000,
            &[2_000],
            vec![Some(vec![source])],
        ),
        0,
    );
    // Executable lineage and its price are different obligations.
    assert!(rf_native_implementations(&plan).hash_join_runtime_filter);
    let facts = corresponding_cost_facts(&plan, 0);
    assert!(facts.runtime_filter_probe_sources.is_empty());
    assert!(facts.runtime_filter_probe_source_rows.is_none());
}

#[test]
fn native_runtime_filter_rejects_missing_and_null_extended_lineage() {
    let missing = rf_boundary(40, vec![LogicalType::Integer], 20_000, &[2_000], vec![None]);
    // Artificial LEFT JOIN output snapshot: output 0 retains its stored
    // origin; output 1 is NULL-extended and therefore has no exact lineage.
    // This tests admission of supplied evidence, not lineage derivation.
    let nullable = rf_boundary(
        40,
        vec![LogicalType::Integer, LogicalType::Integer],
        20_000,
        &[2_000, 20],
        vec![Some(vec![rf_source(17, 701, 3, 20_000, 2_000)]), None],
    );
    assert!(
        planner_implementation_set(
            &rf_join(
                duplicate_plan_preserving_indices(&nullable, BindContext::new().shared().as_ref()),
                0
            ),
            true
        )
        .hash_join_runtime_filter
    );
    for (case, plan) in [
        ("missing", rf_join(missing, 0)),
        ("NULL-extended", rf_join(nullable, 1)),
    ] {
        assert_eq!(rf_observed_sources(&plan), None, "{case}");
        assert!(
            !planner_implementation_set(&plan, true).hash_join_runtime_filter,
            "owned must reject {case} probe lineage"
        );
        assert!(
            !rf_native_implementations(&plan).hash_join_runtime_filter,
            "native must reject {case}, even if another output has lineage"
        );
    }
}
