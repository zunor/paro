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
    pub(super) plan: LogicalPlan,
    pub(super) column_stats: SharedColumnStatistics,
    pub(super) target: StagingTarget,
    pub(super) regions: StagingRegionRequirements,
    /// Existing Memo groups referenced by the transformed root's child holes.
    /// `None` imports an owned tree; `Some` preserves every listed boundary
    /// and stages only the new operator shell.
    pub(super) root_child_groups: Option<Box<[GroupId]>>,
}

pub(super) struct StagingTarget {
    pub(super) group: GroupId,
    pub(super) rule: RuleId,
    pub(super) budget_class: TransformationBudgetClass,
    pub(super) input_context: OptimizationContextId,
    pub(super) child_context: OptimizationContextId,
    pub(super) refined_cardinality_kind: Option<CardinalityRecipeKind>,
}

pub(super) struct StagingRegionRequirements {
    pub(super) preserved_facet: Option<Fingerprint>,
    pub(super) extended_required_facets: Box<[Fingerprint]>,
    pub(super) inherited_runtime_filter_facet: Option<Fingerprint>,
}

pub(super) fn stage_transformed_expression(
    request: StagingRequest,
    memo: &mut Memo,
    state: &mut PlannerTransformState,
) -> Result<Option<StagedEquivalent>> {
    let StagingRequest {
        plan,
        column_stats,
        target:
            StagingTarget {
                group: target,
                rule,
                budget_class,
                input_context,
                child_context,
                refined_cardinality_kind,
            },
        regions:
            StagingRegionRequirements {
                preserved_facet: preserved_region_facet,
                extended_required_facets: extended_required_region_facets,
                inherited_runtime_filter_facet,
            },
        root_child_groups,
    } = request;

    struct NodeState {
        group: GroupId,
        columns: Box<[ColumnId]>,
        region_scope: PlannerRegionScope,
    }

    struct StagingOptions<'a> {
        column_stats: &'a Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
        search_context: Option<&'a crate::context::OptimizationContext>,
        rule: RuleId,
        group_budget: BudgetDimension,
    }

    struct StagingSession<'a> {
        memo: &'a mut Memo,
        state: &'a mut PlannerTransformState,
        options: StagingOptions<'a>,
        pending_runtime_filter_facets: Vec<RegionFacet>,
    }

    struct NodeStagingRequest {
        plan: LogicalPlan,
        target: Option<GroupId>,
        required_region_facet: Option<Fingerprint>,
        inherited_runtime_filter_facet: Option<Fingerprint>,
        node_context: OptimizationContextId,
        target_child_context: Option<OptimizationContextId>,
        refined_cardinality_kind: Option<CardinalityRecipeKind>,
        preserved_child_groups: Option<Box<[GroupId]>>,
    }

    fn referenced_group_scope(memo: &Memo, root: GroupId) -> PlannerRegionScope {
        let root = memo.canonical_group(root);
        let mut visited = BTreeSet::from([root]);
        let mut pending = vec![root];
        while let Some(group) = pending.pop() {
            let Some(group) = memo.group(group) else {
                continue;
            };
            for child in group
                .logical_exprs()
                .iter()
                .filter_map(|expression| memo.logical_expr(*expression))
                .flat_map(|expression| expression.key.children.iter().copied())
                .map(|child| memo.canonical_group(child))
            {
                if visited.insert(child) {
                    pending.push(child);
                }
            }
        }
        PlannerRegionScope::new(
            root,
            visited
                .into_iter()
                .filter(|group| *group != root)
                .map(|group| PlannerRegionScope::new(group, std::iter::empty())),
        )
    }

    fn stage_node(
        session: &mut StagingSession<'_>,
        request: NodeStagingRequest,
    ) -> Result<(LogicalPlan, NodeState, Option<StagedEquivalent>)> {
        let NodeStagingRequest {
            plan,
            target,
            required_region_facet,
            inherited_runtime_filter_facet,
            node_context,
            target_child_context,
            refined_cardinality_kind,
            preserved_child_groups,
        } = request;
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
            session.state.bind_context.shared().as_ref(),
        ));
        // Recursive staging interns every node's scalar expressions into the
        // Query IR.  Keep a second operator shell for the binding-semantic
        // tree returned to the parent: provider matching and later rule
        // staging must never observe a child's arena-local References.
        let semantic_skeleton = duplicate_plan_preserving_indices(
            &skeleton,
            session.state.bind_context.shared().as_ref(),
        );
        if preserved_child_groups
            .as_deref()
            .is_some_and(|groups| groups.len() != detached.len())
        {
            return Err(paro_error::internal(
                "transformation group-hole arity disagrees with its operator shell",
            ));
        }
        let preserved_child_groups = preserved_child_groups.as_deref();
        let mut child_states = Vec::with_capacity(detached.len());
        let mut children = Vec::with_capacity(detached.len());
        let descendant_context = target_child_context.unwrap_or(node_context);
        for (index, child) in detached.into_iter().enumerate() {
            if let Some(group) = preserved_child_groups
                .and_then(|groups| groups.get(index))
                .copied()
            {
                let bindings = child.get_column_bindings();
                let types = child.types();
                if bindings.len() != types.len() {
                    return Err(paro_error::internal(
                        "transformation group hole has inconsistent binding/type arity",
                    ));
                }
                let columns = bindings
                    .into_iter()
                    .zip(types)
                    .map(|(binding, logical_type)| {
                        session
                            .state
                            .binding_ids
                            .get(&(
                                binding.table_index,
                                binding.column_index,
                                logical_type_fingerprint(&logical_type),
                            ))
                            .copied()
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "transformation group hole references an unknown column",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let group = session.memo.canonical_group(group);
                let contract = session.memo.group(group).ok_or_else(|| {
                    paro_error::internal("transformation group hole references an unknown group")
                })?;
                if columns.iter().copied().collect::<BTreeSet<_>>() != contract.schema.ids() {
                    return Err(paro_error::internal(
                        "transformation group hole changes its referenced group schema",
                    ));
                }
                child_states.push(NodeState {
                    group,
                    columns: columns.into_boxed_slice(),
                    region_scope: referenced_group_scope(session.memo, group),
                });
                children.push(child);
            } else {
                let (child, child_state, staged) = stage_node(
                    session,
                    NodeStagingRequest {
                        plan: child,
                        target: None,
                        required_region_facet: None,
                        inherited_runtime_filter_facet: None,
                        node_context: descendant_context,
                        target_child_context: None,
                        refined_cardinality_kind: None,
                        preserved_child_groups: None,
                    },
                )?;
                debug_assert!(staged.is_none());
                children.push(child);
                child_states.push(child_state);
            }
        }
        let mut children = children.into_iter();
        let semantic_plan = semantic_skeleton.try_map_children(|_| {
            children
                .next()
                .ok_or_else(|| paro_error::internal("transformed planner tree lost a staged child"))
        })?;
        if children.next().is_some() {
            return Err(paro_error::internal(
                "transformed planner tree produced an extra staged child",
            ));
        }

        let memo = &mut *session.memo;
        let state = &mut *session.state;
        let options = &session.options;
        let pending_runtime_filter_facets = &mut session.pending_runtime_filter_facets;

        let output_bindings = semantic_plan.get_column_bindings();
        let output_types = semantic_plan.types();
        let output_names = semantic_plan.output_names();
        if output_bindings.len() != output_types.len() {
            return Err(paro_error::internal(
                "transformed plan output binding/type arity mismatch",
            ));
        }
        let mut output_columns = Vec::with_capacity(output_bindings.len());
        for (index, (binding, logical_type)) in output_bindings
            .iter()
            .copied()
            .zip(output_types.iter().cloned())
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
            derive_logical_properties(&semantic_plan.operator, &child_maximum_cardinalities);
        let output_rows_hard_upper = logical_properties.maximum_cardinality;
        let search_candidate = if let Some(search_context) = options.search_context.filter(|_| {
            matches!(
                &semantic_plan.operator,
                LogicalOperator::TopN(_) | LogicalOperator::Filter(_)
            )
        }) {
            crate::search::optimizer::SearchOptimizer::new()
                .physical_candidate_for_root(&semantic_plan, search_context)?
        } else {
            None
        };
        if search_candidate.is_some() {
            debug!(
                target: targets::OPTIMIZER,
                rule = options.rule.0,
                operator = ?semantic_plan.operator.op_type(),
                "attached search provider to transformed logical expression"
            );
        }
        let mut plan = skeleton;
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
        let logical_identity = key.stable_fingerprint();
        let mut cardinality = derive_group_cardinality(
            &semantic_plan.operator,
            &key.children,
            &semantic_plan.stats,
            logical_identity,
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
                candidates.iter().copied().find(|(group, logical)| {
                    let context_matches = memo
                        .logical_expr(*logical)
                        .and_then(|logical| state.metadata.get(&logical.payload))
                        .is_some_and(|metadata| metadata.input_context == node_context);
                    context_matches
                        && memo.group(*group).is_some_and(|existing| {
                            existing.schema == schema
                                && existing
                                    .logical_properties
                                    .same_contract(&logical_properties)
                        })
                })
            }) {
                let group = memo.canonical_group(group);
                let existing = memo.group_mut(group).ok_or_else(|| {
                    paro_error::internal("reused transformed expression lost its Memo group")
                })?;
                // Reusing identity must not discard facts derived in the new
                // semantic context. This is particularly important for a CTE
                // reference after predicate pushdown: its operator key is
                // unchanged, while the owner proves a much tighter row
                // domain. Equivalent facts intersect at the group boundary;
                // no payload-local snapshot is allowed to freeze the older
                // estimate.
                existing
                    .logical_properties
                    .merge_equivalent_facts(&logical_properties);
                existing.cardinality =
                    std::mem::take(&mut existing.cardinality).canonical_with(cardinality.clone());
                return Ok((
                    semantic_plan,
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
            memo.create_optional_group(
                options.group_budget,
                schema,
                logical_properties.clone(),
                cardinality.clone(),
            )?
        };
        let region_scope = PlannerRegionScope::new(
            group,
            child_states.iter().map(|child| child.region_scope.clone()),
        );

        if target.is_some() {
            if let Some(existing) = memo.logical_expr_for_key(group, &key) {
                let existing_context = state
                    .metadata
                    .get(&existing.payload)
                    .map(|metadata| metadata.input_context)
                    .ok_or_else(|| {
                        paro_error::internal("existing target expression lost planner metadata")
                    })?;
                if existing_context != node_context {
                    // Structural identity is not occurrence identity. Until
                    // the Memo index carries context as a first-class key, an
                    // advisory rewrite that collides with the same relational
                    // key in another expression-path context must decline.
                    // Returning `None` lets the outer TransformContext roll
                    // back every recursively staged child and sidecar write;
                    // this expected miss is not an optimizer corruption.
                    return Ok((
                        semantic_plan,
                        NodeState {
                            group,
                            columns: output_columns.into_boxed_slice(),
                            region_scope,
                        },
                        None,
                    ));
                }
                return Ok((
                    semantic_plan,
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
        let search = search_candidate
            .map(|search_plan| {
                stage_search_implementation(
                    SearchStagingRequest {
                        plan: search_plan,
                        expected_output_bindings: &output_bindings,
                        expected_output_types: &output_types,
                        output_columns: &output_columns,
                        materialized_columns: &unique_columns,
                        binding_ids: &state.binding_ids,
                        operator_fingerprint,
                        output_rows_hard_upper,
                        column_stats: options.column_stats.as_ref(),
                        scan_access_cost: state.scan_access_cost,
                    },
                    &mut state.payloads,
                )
            })
            .transpose()?;
        // Physical admission and costing need the recursively reattached
        // semantic tree. `plan` is deliberately only an interned operator
        // shell whose children are DummyScan placeholders; consulting it for
        // lineage, widths, or child statistics would make every transformed
        // join appear to have no rowset consumer.
        let implementations =
            planner_implementation_set(&semantic_plan, state.rowset_scan_pushdown);
        if let LogicalOperator::Join(Join::Comparison(join)) = &semantic_plan.operator {
            debug!(
                target: targets::OPTIMIZER,
                rule = options.rule.0,
                baseline = ?implementations.baseline,
                join_type = ?join.join_type,
                runtime_filter_candidate = implementations.hash_join_runtime_filter,
                build_left_runtime_filter_candidate = implementations.hash_join_build_left_runtime_filter,
                probe_operator = ?join.left.operator.op_type(),
                conditions = ?join.conditions,
                "staged transformed physical join implementation set"
            );
        }
        let runtime_filter_candidate = implementations.hash_join_runtime_filter
            || implementations.hash_join_build_left_runtime_filter;
        let runtime_filter_scope =
            runtime_filter_candidate.then(|| std::iter::once(group).collect::<BTreeSet<_>>());
        let metadata = PlannerOperatorMetadata {
            origin_rule: Some(options.rule),
            operator_type: semantic_plan.operator.op_type(),
            operator_fingerprint,
            provided: ProvidedProperties {
                ordering: derive_provided_ordering(
                    &semantic_plan.operator,
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
                result_guarantee: provided_result_guarantee(&semantic_plan.operator),
            },
            local_cost: planner_operator_cost(
                &semantic_plan,
                child_states.len(),
                output_rows_hard_upper,
                &child_maximum_cardinalities,
                state.scan_access_cost,
            )?,
            implementations,
            grant_dependency: planner_grant_dependency(&semantic_plan.operator),
            spillable: planner_operator_spillable(&semantic_plan.operator),
            cost_facts: planner_cost_facts(
                &semantic_plan,
                options.column_stats.as_ref(),
                state.scan_access_cost,
            )?,
            output_columns: output_columns.clone().into_boxed_slice(),
            child_required: intern_child_requirements(
                memo,
                child_states.iter().map(|child| child.columns.as_ref()),
            )?,
            child_row_goals: child_row_goals(&semantic_plan.operator, child_states.len()),
            search,
            input_context: node_context,
            child_context: target_child_context.unwrap_or(node_context),
            required_region_facet: target.and(required_region_facet),
            runtime_filter_region_facet: None,
            structural_retained_children: planner_structural_retained_children(
                &semantic_plan.operator,
            ),
            baseline_payload,
        };
        if state.metadata.insert(payload, metadata).is_some() {
            return Err(paro_error::internal(
                "transformed planner payload metadata was assigned twice",
            ));
        }

        let staged = if target.is_some() {
            if runtime_filter_candidate {
                let mut facet = if let Some(fingerprint) = inherited_runtime_filter_facet {
                    memo.regions()
                        .nodes
                        .iter()
                        .flat_map(|region| region.facets.iter())
                        .find(|facet| facet.fingerprint == fingerprint)
                        .cloned()
                        .ok_or_else(|| {
                            paro_error::internal(
                                "transformation inherited an unknown runtime-filter facet",
                            )
                        })?
                } else {
                    let mut facet = planner_region_facet(
                        RegionFacetKind::RuntimeFilter,
                        FacetCriticality::Optional,
                        logical_identity,
                        operator_fingerprint,
                        BTreeSet::new(),
                    );
                    facet.priority = 2_000 + RegionFacetKind::RuntimeFilter as u16;
                    facet
                };
                facet
                    .scope
                    .extend(runtime_filter_scope.clone().expect("candidate scope"));
                let fingerprint = facet.fingerprint;
                state
                    .metadata
                    .get_mut(&payload)
                    .ok_or_else(|| {
                        paro_error::internal("transformed runtime-filter payload disappeared")
                    })?
                    .runtime_filter_region_facet = Some(fingerprint);
                pending_runtime_filter_facets.push(facet);
            }
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
                let mut facet = planner_region_facet(
                    RegionFacetKind::RuntimeFilter,
                    FacetCriticality::Optional,
                    logical_identity,
                    operator_fingerprint,
                    runtime_filter_scope.expect("candidate scope"),
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
                pending_runtime_filter_facets.push(facet);
            }
            None
        };
        Ok((
            semantic_plan,
            NodeState {
                group,
                columns: output_columns.into_boxed_slice(),
                region_scope,
            },
            staged,
        ))
    }

    let search_context =
        if crate::search::optimizer::SearchOptimizer::contains_candidate_root(&plan) {
            let session_context = state
                .session
                .clone()
                .ok_or_else(|| paro_error::internal("search planning has no statement context"))?;
            let mut search_context = crate::context::OptimizationContext::new(
                session_context,
                state.bind_context.clone(),
            );
            search_context.column_stats = column_stats.clone();
            search_context.cost_model = state.cost_model.clone();
            search_context.verify_enabled = state.verify_enabled;
            Some(search_context)
        } else {
            None
        };

    let (root, staged, pending_runtime_filter_facets) = {
        let mut session = StagingSession {
            memo,
            state,
            options: StagingOptions {
                column_stats: &column_stats,
                search_context: search_context.as_ref(),
                rule,
                group_budget: budget_class.group_dimension(),
            },
            pending_runtime_filter_facets: Vec::new(),
        };
        let (_, root, staged) = stage_node(
            &mut session,
            NodeStagingRequest {
                plan,
                target: Some(target),
                required_region_facet: preserved_region_facet,
                inherited_runtime_filter_facet,
                node_context: input_context,
                target_child_context: Some(child_context),
                refined_cardinality_kind,
                preserved_child_groups: root_child_groups,
            },
        )?;
        (root, staged, session.pending_runtime_filter_facets)
    };
    let Some(staged) = staged else {
        return Ok(None);
    };
    let mut region_facets = Vec::with_capacity(
        extended_required_region_facets
            .len()
            .saturating_add(pending_runtime_filter_facets.len()),
    );
    for fingerprint in extended_required_region_facets {
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
        region_facets.push(facet);
    }
    // Optional facets created inside a transformed mandatory region are
    // normalized only after that region owns its complete rewritten scope.
    // Publishing them during recursive staging would compare them with the
    // stale pre-transformation scope and permanently drop otherwise nested
    // runtime filters as an apparent oversized overlap.
    region_facets.extend(pending_runtime_filter_facets);
    if !region_facets.is_empty() {
        if let Some(session) = &state.session {
            session.cancellation.check()?;
        }
        let dropped = memo.upsert_region_facets(region_facets)?;
        disable_dropped_runtime_filter_facets(state, &dropped)?;
    }
    Ok(Some(staged))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_catalog::entry::{CatalogObjectId, ColumnDefinition, TableCatalogEntry};
    use paro_common::types::LogicalType;
    use paro_context::TestStatementContextBuilder;
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_planner::operator::join::{Join, JoinCondition, JoinType};
    use paro_planner::operator::{ComparisonJoin, ExpressionGet, Get};
    use paro_planner::plan::CardinalityEstimate;
    use paro_storage::table::table_factory::TableFactory;

    use super::*;

    fn test_base_get(table_index: usize, object_id: u64, name: &str, rows: u64) -> LogicalPlan {
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
            CatalogObjectId::from_raw(object_id),
            0,
        ));
        let mut plan = LogicalPlan::synthetic(LogicalOperator::Get(Get::new(
            table_index,
            vec!["id".to_string()],
            vec![LogicalType::Integer],
            table,
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        plan
    }

    fn equality_join(left: LogicalPlan, right: LogicalPlan, rows: u64) -> LogicalPlan {
        let condition = JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
        );
        let mut plan = LogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(JoinType::Inner, left, right, vec![condition]),
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        plan
    }

    #[test]
    fn root_key_collision_in_another_context_declines_and_rolls_back() {
        let bind_context = BindContext::new();
        let plan = LogicalPlan::new(
            &bind_context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                Vec::new(),
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let staged_plan = duplicate_plan_preserving_indices(&plan, bind_context.shared().as_ref());
        let mut input = MemoBuilder::build(plan, bind_context, SearchBudget::default()).unwrap();
        let root = input.root;
        let (schema, properties, cardinality) = {
            let group = input.memo.group(root).unwrap();
            (
                group.schema.clone(),
                group.logical_properties.clone(),
                group.cardinality.clone(),
            )
        };
        let groups_before = input.memo.group_count();
        let state = input.planner_state.clone();
        state.write().unwrap().session = Some(TestStatementContextBuilder::minimal().build());
        let metadata_before = state.read().unwrap().metadata.len();
        let mut transaction = TransformContext::new(&mut input.memo, root);

        let outcome = transaction
            .with_sidecar_transaction(
                state.clone(),
                PlannerTransformState::savepoint,
                PlannerTransformState::rollback_to,
                |memo, state| {
                    // Model work performed while recursively staging a plan;
                    // a context collision at its root must cause all of it to
                    // be discarded by the common advisory-miss path.
                    memo.create_group(schema, properties, cardinality);
                    stage_transformed_expression(
                        StagingRequest {
                            plan: staged_plan,
                            column_stats: Arc::new(HashMap::new()),
                            target: StagingTarget {
                                group: root,
                                rule: RuleId(999),
                                budget_class: TransformationBudgetClass::Local,
                                input_context: OptimizationContextId(1),
                                child_context: OptimizationContextId(1),
                                refined_cardinality_kind: None,
                            },
                            regions: StagingRegionRequirements {
                                preserved_facet: None,
                                extended_required_facets: Box::new([]),
                                inherited_runtime_filter_facet: None,
                            },
                            root_child_groups: None,
                        },
                        memo,
                        state,
                    )
                },
            )
            .unwrap();

        assert!(outcome.is_none());
        assert_eq!(transaction.memo().group_count(), groups_before + 1);
        transaction.rollback().unwrap();
        assert_eq!(input.memo.group_count(), groups_before);
        assert_eq!(state.read().unwrap().metadata.len(), metadata_before);
    }

    #[test]
    fn transformed_join_uses_reattached_children_for_physical_facts() {
        let baseline = equality_join(
            equality_join(
                test_base_get(0, 30_001, "fact", 20_000),
                test_base_get(1, 30_002, "first_dimension", 20),
                20,
            ),
            test_base_get(2, 30_003, "second_dimension", 30),
            20,
        );
        let transformed = equality_join(
            equality_join(
                test_base_get(0, 30_001, "fact", 20_000),
                test_base_get(2, 30_003, "second_dimension", 30),
                30,
            ),
            test_base_get(1, 30_002, "first_dimension", 20),
            20,
        );
        let mut input =
            MemoBuilder::build(baseline, BindContext::new(), SearchBudget::default()).unwrap();
        let root = input.root;
        let state = input.planner_state.clone();
        state.write().unwrap().session = Some(TestStatementContextBuilder::minimal().build());
        let mut transaction = TransformContext::new(&mut input.memo, root);

        let staged = transaction
            .with_sidecar_transaction(
                state.clone(),
                PlannerTransformState::savepoint,
                PlannerTransformState::rollback_to,
                |memo, state| {
                    stage_transformed_expression(
                        StagingRequest {
                            plan: transformed,
                            column_stats: Arc::new(HashMap::new()),
                            target: StagingTarget {
                                group: root,
                                rule: JOIN_REGION_ENUMERATION_RULE,
                                budget_class: TransformationBudgetClass::Local,
                                input_context: OptimizationContextId(0),
                                child_context: OptimizationContextId(0),
                                refined_cardinality_kind: Some(
                                    CardinalityRecipeKind::ConstraintRefined,
                                ),
                            },
                            regions: StagingRegionRequirements {
                                preserved_facet: None,
                                extended_required_facets: Box::new([]),
                                inherited_runtime_filter_facet: None,
                            },
                            root_child_groups: None,
                        },
                        memo,
                        state,
                    )
                },
            )
            .unwrap()
            .expect("reordered join should stage");

        let planner_state = state.read().unwrap();
        let metadata = planner_state
            .metadata
            .get(&staged.payload)
            .expect("staged root metadata");
        assert!(metadata.implementations.hash_join_runtime_filter);
        assert_eq!(
            metadata
                .cost_facts
                .runtime_filter_probe_sources
                .iter()
                .map(|source| source.source)
                .collect::<Vec<_>>(),
            vec![WorkSourceId(0)]
        );
        assert!(metadata
            .cost_facts
            .child_row_widths
            .iter()
            .all(|width| *width > 8));
    }
}
