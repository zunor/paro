// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transactional staging of planner rewrites into Memo groups.

use super::*;

pub(super) struct StagedEquivalent {
    pub(super) key: LogicalExprKey,
    pub(super) payload: LogicalPayloadId,
    pub(super) logical_properties: LogicalProperties,
    pub(super) cardinality: GroupCardinality,
}

pub(super) struct StagingRequest {
    plan: LogicalPlan,
    column_stats: Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
    target: GroupId,
    rule: RuleId,
    preserved_region_facet: Option<Fingerprint>,
    refined_cardinality_kind: Option<CardinalityRecipeKind>,
}

impl StagingRequest {
    pub(super) fn new(
        plan: LogicalPlan,
        column_stats: Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
        target: GroupId,
        rule: RuleId,
        preserved_region_facet: Option<Fingerprint>,
        refined_cardinality_kind: Option<CardinalityRecipeKind>,
    ) -> Self {
        Self {
            plan,
            column_stats,
            target,
            rule,
            preserved_region_facet,
            refined_cardinality_kind,
        }
    }
}

pub(super) fn stage_transformed_expression(
    request: StagingRequest,
    memo: &mut Memo,
    state: &mut PlannerTransformState,
) -> Result<StagedEquivalent> {
    let StagingRequest {
        plan,
        column_stats,
        target,
        rule,
        preserved_region_facet,
        refined_cardinality_kind,
    } = request;

    struct NodeState {
        group: GroupId,
        columns: Box<[ColumnId]>,
        region_scope: PlannerRegionScope,
    }

    struct StagingOptions<'a> {
        column_stats: &'a Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
        rule: RuleId,
    }

    fn stage_node(
        plan: LogicalPlan,
        target: Option<GroupId>,
        memo: &mut Memo,
        state: &mut PlannerTransformState,
        options: &StagingOptions<'_>,
        required_region_facet: Option<Fingerprint>,
        refined_cardinality_kind: Option<CardinalityRecipeKind>,
    ) -> Result<(LogicalPlan, NodeState, Option<StagedEquivalent>)> {
        let mut detached = Vec::new();
        let skeleton = plan.try_map_children(|child| {
            detached.push(child);
            Ok(LogicalPlan::synthetic(LogicalOperator::DummyScan))
        })?;
        // The payload recipe owns only this operator shell. Capturing it
        // before children are reattached avoids duplicating the entire
        // already-staged subtree at every ancestor (quadratic on chains).
        let semantic_template = semantic_plan::detach_template(duplicate_plan_preserving_indices(
            &skeleton,
            state.bind_context.shared().as_ref(),
        ));
        let mut child_states = Vec::with_capacity(detached.len());
        let mut children = Vec::with_capacity(detached.len());
        for child in detached {
            let (child, child_state, staged) =
                stage_node(child, None, memo, state, options, None, None)?;
            debug_assert!(staged.is_none());
            children.push(child);
            child_states.push(child_state);
        }
        let mut children = children.into_iter();
        let mut plan = skeleton.try_map_children(|_| {
            children
                .next()
                .ok_or_else(|| paro_error::internal("transformed planner tree lost a staged child"))
        })?;
        if children.next().is_some() {
            return Err(paro_error::internal(
                "transformed planner tree produced an extra staged child",
            ));
        }

        let output_bindings = plan.get_column_bindings();
        let output_types = plan.types();
        let output_names = plan.output_names();
        if output_bindings.len() != output_types.len() {
            return Err(paro_error::internal(
                "transformed plan output binding/type arity mismatch",
            ));
        }
        let mut output_columns = Vec::with_capacity(output_bindings.len());
        for (index, (binding, logical_type)) in output_bindings
            .iter()
            .copied()
            .zip(output_types.into_iter())
            .enumerate()
        {
            let type_domain = logical_type_fingerprint(&logical_type);
            let binding_key = (binding.table_index, binding.column_index, type_domain);
            let id = if let Some(id) = state.binding_ids.get(&binding_key).copied() {
                id
            } else {
                let id = state.columns.intern(
                    logical_type,
                    true,
                    ColumnOrigin::Derived {
                        key: typed_binding_fingerprint(binding, type_domain),
                    },
                    ColumnVisibility::Visible,
                    output_names.get(index).cloned(),
                )?;
                state.binding_ids.insert(binding_key, id)?;
                id
            };
            output_columns.push(id);
        }
        let unique_columns: BTreeSet<_> = output_columns.iter().copied().collect();
        let schema = GroupSchema::new(
            unique_columns
                .iter()
                .map(|id| {
                    state.columns.get(*id).cloned().ok_or_else(|| {
                        paro_error::internal("transformed plan lost a column descriptor")
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )?;
        let child_maximum_cardinalities = child_states
            .iter()
            .map(|child| {
                memo.group(child.group)
                    .and_then(|group| group.logical_properties.maximum_cardinality)
            })
            .collect::<Vec<_>>();
        let logical_properties =
            derive_logical_properties(&plan.operator, &child_maximum_cardinalities);
        let output_rows_hard_upper = logical_properties.maximum_cardinality;
        // Preserve binding semantics before Query IR interning replaces
        // operator expressions with scalar-arena references.
        let scalar_roots = intern_operator_scalars(
            &mut plan.operator,
            &output_columns,
            &child_states
                .iter()
                .map(|child| child.columns.clone())
                .collect::<Vec<_>>(),
            &mut state.binding_ids,
            &mut state.columns,
            &mut state.scalars,
        )?;
        let operator_fingerprint =
            query_operator_fingerprint(&plan, &scalar_roots, &state.scalars)?;
        let key = LogicalExprKey {
            operator: operator_fingerprint,
            scalars: scalar_roots,
            children: child_states
                .iter()
                .map(|child| child.group)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        };
        let mut cardinality = derive_group_cardinality(
            &plan.operator,
            &key.children,
            &plan.stats,
            key.stable_fingerprint(),
        );
        if let Some(target) = target {
            cardinality = if let Some(kind) = refined_cardinality_kind {
                cardinality.with_kind(kind)
            } else {
                let target = memo.canonical_group(target);
                memo.group(target)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "shape-only transformation targets an unknown cardinality group",
                        )
                    })?
                    .cardinality
                    .clone()
            };
        }

        if target.is_none() {
            if let Some((group, _)) = state.expression_groups.get(&key).and_then(|candidates| {
                candidates.iter().copied().find(|(group, _)| {
                    memo.group(*group).is_some_and(|existing| {
                        existing.schema == schema
                            && existing
                                .logical_properties
                                .same_contract(&logical_properties)
                    })
                })
            }) {
                return Ok((
                    plan,
                    NodeState {
                        group,
                        columns: output_columns.into_boxed_slice(),
                        region_scope: PlannerRegionScope::new(
                            group,
                            child_states.iter().map(|child| child.region_scope.clone()),
                        ),
                    },
                    None,
                ));
            }
        }

        let group = if let Some(target) = target {
            let target = memo.canonical_group(target);
            let contract = memo.group(target).ok_or_else(|| {
                paro_error::internal("transformation targets an unknown equivalence group")
            })?;
            if contract.schema != schema
                || !contract
                    .logical_properties
                    .same_contract(&logical_properties)
            {
                return Err(paro_error::internal(format!(
                    "transformation rule {} changed its target group logical contract: target_schema={:?}, output_schema={schema:?}, target_properties={:?}, output_properties={logical_properties:?}",
                    options.rule.0,
                    contract.schema, contract.logical_properties,
                )));
            }
            target
        } else {
            memo.create_group(schema, logical_properties.clone(), cardinality.clone())
        };
        let region_scope = PlannerRegionScope::new(
            group,
            child_states.iter().map(|child| child.region_scope.clone()),
        );

        if target.is_some() {
            if let Some(existing) = memo.logical_expr_for_key(group, &key) {
                return Ok((
                    plan,
                    NodeState {
                        group,
                        columns: output_columns.into_boxed_slice(),
                        region_scope,
                    },
                    Some(StagedEquivalent {
                        key,
                        payload: existing.payload,
                        logical_properties,
                        cardinality,
                    }),
                ));
            }
        }

        let (payload, baseline_payload) = state.payloads.push_logical(PlannerLogicalPayload {
            semantic_template,
            column_stats: options.column_stats.clone(),
        });
        let mut implementations = planner_implementation_set(&plan, state.rowset_scan_pushdown);
        let runtime_filter_candidate = target.is_none() && implementations.hash_join_runtime_filter;
        if !runtime_filter_candidate {
            implementations.hash_join_runtime_filter = false;
        }
        let metadata = PlannerOperatorMetadata {
            origin_rule: Some(options.rule),
            operator_type: plan.operator.op_type(),
            operator_fingerprint,
            provided: ProvidedProperties {
                ordering: derive_provided_ordering(
                    &plan.operator,
                    &output_columns,
                    child_states.first().map(|child| child.columns.as_ref()),
                    &state.binding_ids,
                ),
                partitioning: ProvidedPartitioning::Singleton,
                materialization: ProvidedMaterialization {
                    values: unique_columns,
                    locators: BTreeMap::new(),
                },
                mutation_safety: ProvidedMutationSafety::NotApplicable,
                representation: ProvidedRepresentation::Flat,
                replayability: ProvidedReplayability::OnePass,
                result_guarantee: provided_result_guarantee(&plan.operator),
            },
            local_cost: planner_operator_cost(
                &plan,
                child_states.len(),
                output_rows_hard_upper,
                &child_maximum_cardinalities,
                state.scan_access_cost,
            )?,
            implementations,
            grant_dependency: planner_grant_dependency(&plan.operator),
            spillable: planner_operator_spillable(&plan.operator),
            cost_facts: planner_cost_facts(&plan, state.scan_access_cost)?,
            output_columns: output_columns.clone().into_boxed_slice(),
            child_required: intern_child_requirements(
                memo,
                child_states.iter().map(|child| child.columns.as_ref()),
            )?,
            child_row_goals: child_row_goals(&plan.operator, child_states.len()),
            search: None,
            required_region_facet: target.and(required_region_facet),
            runtime_filter_region_facet: None,
            structural_retained_children: planner_structural_retained_children(&plan.operator),
            baseline_payload,
        };
        if state.metadata.insert(payload, metadata).is_some() {
            return Err(paro_error::internal(
                "transformed planner payload metadata was assigned twice",
            ));
        }

        let staged = if target.is_some() {
            Some(StagedEquivalent {
                key,
                payload,
                logical_properties,
                cardinality,
            })
        } else {
            let logical =
                memo.insert_logical(group, key.clone(), payload, EquivalenceProof::Initial)?;
            state.record_expression_group(key, group, logical);
            if runtime_filter_candidate {
                let (scope, _) = region_scope
                    .materialize_bounded(usize::from(memo.budget().max_composite_region_groups));
                let mut facet = planner_region_facet(
                    RegionFacetKind::RuntimeFilter,
                    FacetCriticality::Optional,
                    logical,
                    operator_fingerprint,
                    scope,
                );
                facet.priority = 2_000 + RegionFacetKind::RuntimeFilter as u16;
                let fingerprint = facet.fingerprint;
                // Publish the tentative ownership before normalization.  If
                // this optional facet makes the forest exceed its structural
                // bound, the common dropped-facet path can now disable both
                // the facet and its implementation atomically.  Leaving the
                // implementation enabled with `None` ownership would admit a
                // physical artifact that no region can prove.
                state
                    .metadata
                    .get_mut(&payload)
                    .ok_or_else(|| {
                        paro_error::internal("dynamic runtime-filter payload disappeared")
                    })?
                    .runtime_filter_region_facet = Some(fingerprint);
                let dropped = memo.upsert_region_facet(facet)?;
                disable_dropped_runtime_filter_facets(state, &dropped)?;
            }
            None
        };
        Ok((
            plan,
            NodeState {
                group,
                columns: output_columns.into_boxed_slice(),
                region_scope,
            },
            staged,
        ))
    }

    let options = StagingOptions {
        column_stats: &column_stats,
        rule,
    };
    let (_, root, staged) = stage_node(
        plan,
        Some(target),
        memo,
        state,
        &options,
        preserved_region_facet,
        refined_cardinality_kind,
    )?;
    if let Some(fingerprint) = preserved_region_facet {
        let mut facet = memo
            .regions()
            .nodes
            .iter()
            .flat_map(|region| region.facets.iter())
            .find(|facet| facet.fingerprint == fingerprint)
            .cloned()
            .ok_or_else(|| paro_error::internal("preserved planning facet disappeared"))?;
        let ceiling = match facet.criticality {
            FacetCriticality::Required => memo.budget().max_mandatory_region_groups as usize,
            FacetCriticality::Optional => usize::from(memo.budget().max_composite_region_groups),
        };
        let (scope, overflow) = root.region_scope.materialize_bounded(ceiling);
        if overflow && facet.criticality == FacetCriticality::Required {
            return Err(paro_error::internal(
                "required planning-region closure exceeds query complexity ceiling",
            ));
        }
        facet.scope.extend(scope);
        let dropped = memo.upsert_region_facet(facet)?;
        disable_dropped_runtime_filter_facets(state, &dropped)?;
    }
    staged.ok_or_else(|| paro_error::internal("transformation failed to stage its root"))
}

fn disable_dropped_runtime_filter_facets(
    state: &mut PlannerTransformState,
    dropped: &[Fingerprint],
) -> Result<()> {
    let dropped = dropped.iter().copied().collect::<BTreeSet<_>>();
    let payloads = state
        .metadata
        .iter()
        .filter_map(|(payload, metadata)| {
            metadata
                .runtime_filter_region_facet
                .is_some_and(|facet| dropped.contains(&facet))
                .then_some(*payload)
        })
        .collect::<Vec<_>>();
    for payload in payloads {
        state.disable_runtime_filter(payload)?;
    }
    Ok(())
}
