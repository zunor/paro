// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

// Exact-binding RF staging/contract oracle. These tests do not cover SQL
// execution, physical extraction, partial/merge aggregate equivalence, winner
// selection, or performance. The grouped build is a legal Aggregate boundary,
// not a manufactured AggregateDecomposition proof.

#[cfg(test)]
mod native_staging_rf_oracle {
    use super::*;
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::{
        Aggregate, BoundReference, Filter, LogicalOutputLayout, ProjectionMap,
    };
    use paro_storage::statistics::BaseStatistics;

    const ORACLE_RULE: RuleId = RuleId(990_731);
    const PROBE_ROWS: u64 = 20_000;
    const BUILD_ROWS: u64 = 20;
    // A fixed comparison operating point, not an extracted runtime handle.
    const EVALUATION: Fingerprint = Fingerprint(0x51a6e);

    fn col(table: usize, ordinal: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, ordinal), LogicalType::Integer)
                .into(),
        )
    }

    fn rows(mut plan: OwnedLogicalPlan, count: u64) -> OwnedLogicalPlan {
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(count));
        plan
    }

    fn get_two(table_index: usize, oid: u64, count: u64) -> OwnedLogicalPlan {
        // Real stored columns, as in the existing staging fixture. No rows are
        // appended or executed: count/NDV are explicitly fixture estimates.
        let types = vec![LogicalType::Integer, LogicalType::Integer];
        let names = vec!["key".to_string(), "other".to_string()];
        let storage = Arc::new(TableFactory::default().create_table(&types).unwrap());
        let table = Arc::new(TableCatalogEntry::new(
            "paro".into(),
            "public".into(),
            format!("rf_oracle_{table_index}"),
            names
                .iter()
                .map(|name| ColumnDefinition::new(name.clone(), LogicalType::Integer))
                .collect(),
            storage,
            CatalogObjectId::from_raw(oid),
            0,
        ));
        rows(
            OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
                table_index,
                names,
                types,
                table,
            )))),
            count,
        )
    }

    fn grouped(
        child: OwnedLogicalPlan,
        source: usize,
        output: usize,
        order: &[usize],
        count: u64,
    ) -> OwnedLogicalPlan {
        rows(
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
                output,
                output + 1,
                output + 2,
                child,
                order.iter().map(|index| col(source, *index)).collect(),
                vec![],
                vec![],
                vec![],
            )))),
            count,
        )
    }

    fn fixture(
        reversed: bool,
        aggregate_probe: bool,
    ) -> (OwnedLogicalPlan, SharedColumnStatistics) {
        let order = if reversed { vec![1, 0] } else { vec![0, 1] };
        let key_slot = usize::from(reversed);
        let mut filter = Filter::new(get_two(0, 91_001, PROBE_ROWS), vec![]);
        filter.projection_map = ProjectionMap::new(order.clone());
        let probe = rows(
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter)),
            PROBE_ROWS,
        );
        let probe = if aggregate_probe {
            grouped(probe, 0, 20, &order, PROBE_ROWS)
        } else {
            probe
        };
        let build = grouped(get_two(1, 91_002, BUILD_ROWS), 1, 10, &order, BUILD_ROWS);
        let probe_key = if aggregate_probe {
            col(20, key_slot)
        } else {
            col(0, 0)
        };
        let plan = rows(
            OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
                ComparisonJoin::new(
                    JoinType::Inner,
                    probe,
                    build,
                    vec![JoinCondition::equality(probe_key, col(10, key_slot))],
                ),
            ))),
            BUILD_ROWS,
        );

        // Different per-column domains make a wrong ordinal observable even
        // when both columns have the same type and WorkSourceId. These are
        // ranking estimates, not declared unique keys or capacity proofs.
        let mut stats = HashMap::new();
        let mut add = |table, index, ndv: usize| {
            stats.insert(
                ColumnBinding::new(table, index),
                Arc::new(ColumnStatistics::with_estimated_distinct(
                    BaseStatistics::new(LogicalType::Integer),
                    Some(ndv),
                )),
            );
        };
        add(0, 0, PROBE_ROWS as usize);
        add(0, 1, 7);
        add(1, 0, BUILD_ROWS as usize);
        add(1, 1, 3);
        for (output, input) in order.iter().copied().enumerate() {
            add(10, output, if input == 0 { BUILD_ROWS as usize } else { 3 });
            if aggregate_probe {
                add(20, output, if input == 0 { PROBE_ROWS as usize } else { 7 });
            }
        }
        (plan, Arc::new(stats))
    }

    fn check_boundary(layout: &LogicalOutputLayout, reference: &BoundReference, scan_probe: bool) {
        let facts = &reference.facts;
        assert_eq!(layout.types(), facts.types());
        assert!(
            facts.cardinality.is_some(),
            "must transport observed group facts"
        );
        assert_eq!(facts.source_lineage.len(), 2);
        if scan_probe {
            for (column, ndv) in [(0, PROBE_ROWS), (1, 7)] {
                let slot = layout
                    .bindings()
                    .iter()
                    .position(|binding| *binding == ColumnBinding::new(0, column))
                    .unwrap();
                let lineage = facts.source_lineage[slot]
                    .as_ref()
                    .expect("stored-column lineage");
                assert_eq!(lineage.len(), 1);
                assert_eq!(lineage[0].source, 0);
                assert_eq!(lineage[0].column, column);
                assert_eq!(lineage[0].rows.as_ref().unwrap().expected, PROBE_ROWS);
                assert_eq!(lineage[0].distinct, Some(ndv));
            }
        } else {
            // Aggregate retains real relation facts, but is not a rowset
            // passthrough. Never delete facts by hand to manufacture this case.
            assert!(facts.source_lineage.iter().all(Option::is_none));
        }
    }

    fn stage_once(
        memo: &mut Memo,
        state: Arc<RwLock<PlannerTransformState>>,
        target: GroupId,
        binding: &PatternBinding,
        source: &PlannerOperatorMetadata,
        column_stats: SharedColumnStatistics,
        shape: (bool, bool),
    ) -> StagedEquivalent {
        let (native, aggregate_probe) = shape;
        // Every attempt gets a new read context, before its first publication.
        let mut tx = TransformContext::new(memo, target);
        let facts = {
            let state = state.read().unwrap();
            boundary::BoundarySnapshot::read(
                &mut tx,
                &state,
                &binding.root,
                BudgetDimension::RuleWorkPerGroup,
            )
            .unwrap()
            .expect("fact work admitted")
        };
        tx.record_fact_value(
            facts
                .binding_value_fingerprint(tx.memo(), &binding.root)
                .unwrap(),
        );
        let required = tx
            .memo()
            .optimization_context(source.child_context)
            .unwrap()
            .required_region_facets()
            .to_vec()
            .into_boxed_slice();
        let staged = tx
            .with_sidecar_transaction(
                state,
                PlannerTransformState::savepoint,
                PlannerTransformState::rollback_to,
                |memo, state| {
                    let (input, holes, proofs) = if native {
                        let shell = NativeShell::from_pattern(memo, state, &binding.root, &facts)?
                            .expect("exact root shell");
                        assert_eq!(shell.nodes.len(), 1);
                        let LogicalOperator::Join(Join::Comparison(join)) = shell.root_operator()
                        else {
                            panic!("expected exact join binding");
                        };
                        for (child, scan_probe) in
                            [(&join.left, !aggregate_probe), (&join.right, false)]
                        {
                            let NativeChild::MemoGroup {
                                layout, reference, ..
                            } = child
                            else {
                                panic!(
                                    "native input must be a direct, fact-bearing Memo group hole"
                                );
                            };
                            check_boundary(layout, reference, scan_probe);
                        }
                        (
                            StagingInput::Native {
                                shell,
                                resident_nodes: HashMap::new(),
                            },
                            BTreeMap::new(),
                            HashMap::new(),
                        )
                    } else {
                        // Independent production materializer, not NativeShell::from_owned.
                        let owned = semantic_plan::instantiate_bound_plan_with_group_holes(
                            memo,
                            state,
                            &binding.root,
                            Some(&facts),
                        )?
                        .expect("owned exact binding");
                        assert_eq!(owned.group_holes.len(), 2);
                        let LogicalOperator::Join(Join::Comparison(join)) = &owned.plan.operator
                        else {
                            panic!("expected owned exact join binding");
                        };
                        for (child, scan_probe) in
                            [(&join.left, !aggregate_probe), (&join.right, false)]
                        {
                            let LogicalOperator::BoundReference(reference) = &child.operator else {
                                panic!("owned input must retain its observed group boundary");
                            };
                            check_boundary(&child.output_layout(), reference, scan_probe);
                        }
                        (
                            StagingInput::Arena(state.staging_arena.import(owned.plan)?),
                            owned.group_holes,
                            owned.selected_proofs,
                        )
                    };
                    stage_transformed_expression(
                        StagingRequest {
                            input,
                            input_facts: facts,
                            column_stats,
                            column_stat_scopes: HashMap::new(),
                            resident_nodes: HashMap::new(),
                            target: StagingTarget {
                                group: target,
                                rule: ORACLE_RULE,
                                budget_class: TransformationBudgetClass::Local,
                                input_context: source.input_context,
                                child_context: source.child_context,
                                refined_cardinality_kind: None,
                            },
                            regions: StagingRegionRequirements {
                                preserved_facet: source.required_region_facet,
                                extended_required_facets: required,
                                // Deliberately do not inherit the baseline root's RF
                                // capability: this fresh target must derive its own.
                                inherited_runtime_filter_facet: None,
                            },
                            nested_group_holes: holes,
                            selected_proofs: proofs,
                        },
                        memo,
                        state,
                    )
                },
            )
            .unwrap()
            .expect("staging should publish metadata");
        // A dropped TransformContext does not close its Memo savepoint. Each
        // successful attempt must commit before owned rediscovery starts.
        tx.commit().unwrap();
        staged
    }

    struct Observation {
        metadata: PlannerOperatorMetadata,
        composition: Option<CostComposition>,
        region: Option<RegionCandidateContract>,
        scan_access: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    }

    fn observe(
        memo: &Memo,
        state: &PlannerTransformState,
        target: GroupId,
        staged: &StagedEquivalent,
        aggregate_probe: bool,
    ) -> Observation {
        let metadata = state.metadata[&staged.payload].clone();
        assert_eq!(
            metadata.implementations.hash_join_runtime_filter,
            !aggregate_probe
        );
        assert!(
            !metadata.implementations.hash_join_build_left_runtime_filter,
            "the right Aggregate is not a scan probe for build-left RF"
        );
        assert!(metadata
            .cost_facts
            .runtime_filter_build_left_probe_sources
            .is_empty());
        assert!(
            metadata.required_region_facet.is_none(),
            "fixture has no required root owner"
        );
        let (region, composition) = if aggregate_probe {
            assert!(metadata.cost_facts.runtime_filter_probe_sources.is_empty());
            assert!(metadata.runtime_filter_region_facet.is_none());
            assert!(!memo
                .regions()
                .nodes
                .iter()
                .flat_map(|node| node.facets.iter())
                .any(|facet| facet.kind == RegionFacetKind::RuntimeFilter
                    && facet.scope.contains(&target)));
            (None, None)
        } else {
            let sources = &metadata.cost_facts.runtime_filter_probe_sources;
            assert_eq!(sources.len(), 1);
            assert_eq!(sources[0].source, WorkSourceId(0));
            assert_eq!(sources[0].rows.expected, PROBE_ROWS);
            assert_eq!(
                sources[0].multiplicity,
                RuntimeFilterProbeMultiplicity::EstimatedDistinct { keys: PROBE_ROWS }
            );
            assert_eq!(
                metadata.cost_facts.runtime_filter_build_distinct_expected,
                Some(BUILD_ROWS)
            );
            let facet = metadata
                .runtime_filter_region_facet
                .expect("RF ownership must be published");
            let published = memo
                .regions()
                .nodes
                .iter()
                .flat_map(|node| node.facets.iter())
                .find(|candidate| candidate.fingerprint == facet)
                .unwrap();
            published.validate_contract().unwrap();
            assert_eq!(published.kind, RegionFacetKind::RuntimeFilter);
            assert!(published.scope.contains(&target));
            let (build, probe) =
                runtime_filter_dependency_boundary(PLANNER_HASH_JOIN_RUNTIME_FILTER).unwrap();
            assert_eq!(
                (build, probe),
                (
                    RegionBoundaryEndpoint::Input(1),
                    RegionBoundaryEndpoint::Input(0)
                )
            );
            let region =
                planner_runtime_filter_region_contract(memo, Some(facet), build, probe).unwrap();
            assert_eq!(region.facets.as_ref(), &[facet]);
            assert_eq!(
                region.artifacts.as_ref(),
                &[RegionOwnedArtifact {
                    fingerprint: facet,
                    kind: RegionArtifactKind::RuntimeFilter,
                }]
            );
            assert_eq!(
                region.artifact_dependencies.as_ref(),
                &[RegionArtifactDependencyContract {
                    artifact: facet,
                    producer: RegionBoundaryEndpoint::Input(1),
                    consumer: RegionBoundaryEndpoint::Input(0),
                    kind: RegionDependencyKind::ControlWaitComplete,
                }]
            );
            let facts =
                expression_cost_facts(memo, target, &staged.key.children, &metadata.cost_facts)
                    .unwrap();
            let composition = planner_cost_composition(
                &metadata,
                PhysicalImplementationFlavor::HashJoinRuntimeFilter,
                &facts,
                1,
                EVALUATION,
            )
            .unwrap();
            let CostComposition::SidewaysFilter {
                filtered_child,
                sources,
                ..
            } = &composition
            else {
                panic!("admission flag alone must not masquerade as a source-work response");
            };
            assert_eq!(*filtered_child, 0);
            assert_eq!(sources.len(), 1);
            assert_eq!(sources[0].source, WorkSourceId(0));
            assert!(sources[0].expected_retained_ppm < 1_000_000);
            assert!(sources[0].expected_retained_ppm <= sources[0].upper_retained_ppm);
            assert!(sources[0].upper_retained_ppm <= 1_000_000);
            (Some(region), Some(composition))
        };
        Observation {
            metadata,
            region,
            composition,
            scan_access: state.scan_access_cost,
        }
    }

    fn assert_same(a: &Observation, b: &Observation) {
        assert_eq!(a.scan_access, b.scan_access);
        let (a_meta, b_meta) = (&a.metadata, &b.metadata);
        macro_rules! metadata_eq { ($($field:ident),+ $(,)?) => { $(
            assert_eq!(a_meta.$field, b_meta.$field, stringify!($field));
        )+ }; }
        metadata_eq!(
            operator_fingerprint,
            input_context,
            child_context,
            child_layouts,
            child_required,
            child_row_goals,
            output_columns,
            required_region_facet,
            runtime_filter_region_facet,
            grant_dependency,
            provided,
            local_cost
        );
        assert_eq!(
            a_meta.implementations.hash_join_runtime_filter,
            b_meta.implementations.hash_join_runtime_filter
        );
        assert_eq!(
            a_meta.implementations.hash_join_build_left_runtime_filter,
            b_meta.implementations.hash_join_build_left_runtime_filter
        );
        let (a_cost, b_cost) = (&a_meta.cost_facts, &b_meta.cost_facts);
        macro_rules! cost_eq { ($($field:ident),+ $(,)?) => { $(
            assert_eq!(a_cost.$field, b_cost.$field, stringify!($field));
        )+ }; }
        cost_eq!(
            child_row_widths,
            child_materialization_risk_rows,
            output_row_width,
            hash_key_width,
            scan_access_width,
            scan_physical_rows,
            scan_work_source,
            topn_capacity,
            runtime_filter_probe_multiplicity,
            runtime_filter_build_left_probe_multiplicity,
            runtime_filter_probe_source_rows,
            runtime_filter_build_left_probe_source_rows,
            runtime_filter_build_distinct_expected,
            runtime_filter_build_domain_column,
            runtime_filter_build_left_distinct_expected,
            runtime_filter_build_left_domain_column,
            runtime_filter_key_types
        );
        assert!(a_cost.perfect_hash.is_none() && b_cost.perfect_hash.is_none());
        let source_values = |sources: &[PlannerRuntimeFilterSource]| {
            sources
                .iter()
                .map(|source| (source.source, source.rows, source.multiplicity))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            source_values(&a_cost.runtime_filter_probe_sources),
            source_values(&b_cost.runtime_filter_probe_sources)
        );
        assert_eq!(
            source_values(&a_cost.runtime_filter_build_left_probe_sources),
            source_values(&b_cost.runtime_filter_build_left_probe_sources)
        );
        // Both runs seed the identical ordered fixture, so Memo column/region
        // identities are comparable. Do not compare these IDs across orders.
        assert_eq!(a.region, b.region);
        assert_eq!(a.composition, b.composition);
    }

    fn run_case(reversed: bool, aggregate_probe: bool, native_first: bool) -> Observation {
        let (plan, stats) = fixture(reversed, aggregate_probe);
        let mut budget = SearchBudget::default();
        budget.max_composite_region_groups = 16; // no optional-facet budget drop in this oracle
        let mut input = MemoBuilder::build_alternatives(
            vec![LogicalAlternative {
                plan,
                source: AlternativeOrigin::Baseline,
                column_stats: stats.clone(),
            }],
            BindContext::new(),
            budget,
        )
        .unwrap();
        let state = input.planner_state.clone();
        state.write().unwrap().session = Some(TestStatementContextBuilder::minimal().build());
        assert!(state.read().unwrap().rowset_scan_pushdown); // never change the RF flag in the test
        let root = input.root;
        let expression = input.memo.group(root).unwrap().logical_exprs()[0];
        let logical = input.memo.logical_expr(expression).unwrap();
        let binding = PatternBinding::root_only(root, expression, logical);
        let source = state.read().unwrap().metadata[&logical.payload].clone();
        let source_stats = state.read().unwrap().payloads.logical[logical.payload.index()]
            .column_stats
            .clone();
        assert!(!source_stats.is_empty());
        assert!(
            Arc::ptr_eq(&stats, &source_stats),
            "preserve the baseline's complete cost inputs"
        );
        let original = input.memo.group(root).unwrap();
        let (schema, properties, cardinality) = (
            original.schema.clone(),
            original.logical_properties.clone(),
            original.cardinality.clone(),
        );
        let target = input.memo.create_group(schema, properties, cardinality);
        assert!(input.memo.group(target).unwrap().logical_exprs().is_empty());
        let staged = stage_once(
            &mut input.memo,
            state.clone(),
            target,
            &binding,
            &source,
            source_stats.clone(),
            (native_first, aggregate_probe),
        );
        assert!(
            input
                .memo
                .logical_expr_for_structural_key(target, &staged.key, &staged.operator_encoding)
                .is_none(),
            "the first attempt must not reuse the baseline root's metadata"
        );
        assert!(input
            .memo
            .group(target)
            .unwrap()
            .logical_properties
            .same_contract(&staged.logical_properties));
        let observed = observe(
            &input.memo,
            &state.read().unwrap(),
            target,
            &staged,
            aggregate_probe,
        );
        assert_eq!(observed.metadata.input_context, source.input_context);
        assert_eq!(observed.metadata.child_context, source.child_context);
        let key_slot = usize::from(reversed);
        let probe_binding = if aggregate_probe {
            ColumnBinding::new(20, key_slot)
        } else {
            ColumnBinding::new(0, 0)
        };
        assert_eq!(
            observed.metadata.child_layouts[0].bindings()[key_slot],
            probe_binding
        );
        let expected_domain = state
            .read()
            .unwrap()
            .binding_ids
            .get(10, key_slot, &LogicalType::Integer)
            .copied()
            .unwrap();
        assert_eq!(
            observed
                .metadata
                .cost_facts
                .runtime_filter_build_domain_column,
            Some(expected_domain)
        );

        if native_first {
            // Staging alone does NOT insert its target root. Seed this exact
            // relation clone explicitly, otherwise a 'duplicate' test is fake.
            let published = input
                .memo
                .insert_logical_with_operator_encoding_and_tag(
                    target,
                    staged.key.clone(),
                    staged.payload,
                    EquivalenceProof::Initial,
                    staged.operator_encoding.clone(),
                    operator_tag(observed.metadata.operator_type),
                )
                .unwrap();
            state
                .write()
                .unwrap()
                .record_expression_group(staged.key.clone(), target, published);
            let forest_before = input.memo.regions().clone();
            let groups_before = input.memo.group_count();
            let metadata_before = state.read().unwrap().metadata.len();
            let repeated = stage_once(
                &mut input.memo,
                state.clone(),
                target,
                &binding,
                &source,
                source_stats,
                (false, aggregate_probe),
            );
            assert_eq!(
                repeated.payload, staged.payload,
                "owned rediscovery must retain the native contract"
            );
            assert_eq!(repeated.key, staged.key);
            assert_eq!(repeated.operator_encoding, staged.operator_encoding);
            assert_eq!(
                input.memo.group(target).unwrap().logical_exprs(),
                &[published]
            );
            assert_eq!(input.memo.group_count(), groups_before);
            assert_eq!(state.read().unwrap().metadata.len(), metadata_before);
            assert_eq!(input.memo.regions(), &forest_before);
            assert_same(
                &observed,
                &observe(
                    &input.memo,
                    &state.read().unwrap(),
                    target,
                    &repeated,
                    aggregate_probe,
                ),
            );
        }
        observed
    }

    #[test]
    fn native_staging_rf_matches_owned_and_survives_owned_rediscovery() {
        for reversed in [false, true] {
            // Each call constructs a fresh Memo, with identical nonempty
            // column statistics, session/cost policy and exact child facts.
            let native = run_case(reversed, false, true);
            let owned = run_case(reversed, false, false);
            assert_same(&native, &owned);
        }
    }

    #[test]
    fn native_staging_aggregate_probe_is_not_a_rowset_consumer() {
        for reversed in [false, true] {
            let native = run_case(reversed, true, true);
            let owned = run_case(reversed, true, false);
            assert_same(&native, &owned);
        }
    }
}
