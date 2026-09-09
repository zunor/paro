// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transactional staging of planner rewrites into Memo groups.

use super::*;

pub(super) struct StagedEquivalent {
    pub(super) key: LogicalExprKey,
    pub(super) payload: LogicalPayloadId,
    pub(super) operator_encoding: Box<[u8]>,
    pub(super) logical_properties: LogicalProperties,
    pub(super) cardinality: GroupCardinality,
}

pub(super) struct StagingRequest {
    pub(super) plan: paro_planner::plan::arena::PlanIndex,
    pub(super) input_facts: boundary::BoundarySnapshot,
    pub(super) column_stats: SharedColumnStatistics,
    pub(super) column_stat_scopes: HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
    pub(super) target: StagingTarget,
    pub(super) regions: StagingRegionRequirements,
    /// Opaque Memo inputs retained by the transformed expression. Inputs
    /// legitimately discarded by a relational rewrite are removed before
    /// staging; every surviving transport node is consumed exactly once.
    pub(super) nested_group_holes: BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
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
        input_facts,
        column_stats,
        column_stat_scopes,
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
        nested_group_holes,
    } = request;

    #[derive(Clone)]
    struct NodeState {
        group: GroupId,
        columns: Box<[ColumnId]>,
        region_scope: PlannerRegionScope,
    }

    struct StagingOptions<'a> {
        column_stats: &'a Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
        column_stat_scopes: &'a HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
        rule: RuleId,
        group_budget: BudgetDimension,
    }

    struct StagingSession<'a> {
        memo: &'a mut Memo,
        state: &'a mut PlannerTransformState,
        options: StagingOptions<'a>,
        pending_runtime_filter_facets: Vec<RegionFacet>,
        nested_group_holes: BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        facts: boundary::BoundarySnapshot,
        search_candidates: HashMap<paro_planner::plan::PlanNodeId, OwnedLogicalPlan>,
    }

    struct NodeStagingRequest {
        plan: OwnedLogicalPlan,
        target: Option<GroupId>,
        required_region_facet: Option<Fingerprint>,
        inherited_runtime_filter_facet: Option<Fingerprint>,
        node_context: OptimizationContextId,
        target_child_context: Option<OptimizationContextId>,
        refined_cardinality_kind: Option<CardinalityRecipeKind>,
    }

    fn stage_node(
        session: &mut StagingSession<'_>,
        request: NodeStagingRequest,
        child_states: Vec<NodeState>,
    ) -> Result<Option<(OwnedLogicalPlan, NodeState, Option<StagedEquivalent>)>> {
        let NodeStagingRequest {
            plan,
            target,
            required_region_facet,
            inherited_runtime_filter_facet,
            node_context,
            target_child_context,
            refined_cardinality_kind,
        } = request;
        if let LogicalOperator::BoundReference(reference) = &plan.operator {
            if target.is_some()
                || !session
                    .nested_group_holes
                    .contains_key(&reference.reference_id)
            {
                return Err(paro_error::internal(
                    "staging reached an unregistered or root Memo group hole",
                ));
            }
        }
        if target.is_none() {
            let nested_reference = match &plan.operator {
                LogicalOperator::BoundReference(reference) => Some(reference.reference_id),
                _ => None,
            };
            if let Some(group) = nested_reference
                .and_then(|reference_id| session.nested_group_holes.remove(&reference_id))
            {
                let bindings = plan.get_column_bindings();
                let types = plan.types();
                if bindings.len() != types.len() {
                    return Err(paro_error::internal(
                        "nested group hole has inconsistent binding/type arity",
                    ));
                }
                let columns = bindings
                    .into_iter()
                    .zip(types)
                    .map(|(binding, logical_type)| {
                        session
                            .state
                            .binding_ids
                            .get(binding.table_index, binding.column_index, &logical_type)
                            .copied()
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "nested group hole references an unknown column",
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let group = session.memo.canonical_group(group);
                let contract = session.memo.group(group).ok_or_else(|| {
                    paro_error::internal("nested group hole references an unknown group")
                })?;
                if columns.iter().copied().collect::<BTreeSet<_>>() != contract.schema.ids() {
                    return Err(paro_error::internal(
                        "nested group hole changes its referenced group schema",
                    ));
                }
                return Ok(Some((
                    plan,
                    NodeState {
                        group,
                        columns: columns.into_boxed_slice(),
                        region_scope: PlannerRegionScope::group(group),
                    },
                    None,
                )));
            }
        }
        let (skeleton, children) = paro_planner::plan::arena::LogicalPlanNode::detach(plan);
        let semantic_template = semantic_plan::canonical_template(skeleton.clone());
        // The canonical extraction template deliberately has no output demand
        // or occurrence statistics. Derive the published schema/facts from the
        // settled occurrence, before erasing those annotations for storage.
        let semantic_plan = skeleton.clone().assemble(children)?;
        let memo = &mut *session.memo;
        let state = &mut *session.state;
        let options = &session.options;
        if semantic_plan.id.is_synthetic() && !options.column_stat_scopes.is_empty() {
            return Err(paro_error::internal(
                "synthetic plan id cannot select a column-statistics scope",
            ));
        }
        let column_stats = options
            .column_stat_scopes
            .get(&semantic_plan.id)
            .unwrap_or(options.column_stats);
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
            let id = if let Some(id) = state
                .binding_ids
                .get(binding.table_index, binding.column_index, &logical_type)
                .copied()
            {
                id
            } else {
                let id = state.columns.intern(
                    logical_type.clone(),
                    true,
                    ColumnOrigin::Derived {
                        key: typed_binding_fingerprint(binding, type_domain),
                    },
                    ColumnVisibility::Visible,
                    output_names.get(index).cloned(),
                )?;
                state.binding_ids.insert(
                    binding.table_index,
                    binding.column_index,
                    &logical_type,
                    id,
                )?;
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
        let mut logical_properties =
            derive_logical_properties(&semantic_plan.operator, &child_maximum_cardinalities);
        attach_group_column_domains(
            &mut logical_properties,
            &output_bindings,
            &output_columns,
            column_stats.as_ref(),
            &schema,
        )?;
        if let LogicalOperator::CTERef(reference) = &semantic_plan.operator {
            logical_properties
                .cte_references
                .insert(cte_reference_domain(reference, &output_columns)?);
        }
        if let LogicalOperator::MaterializedCTE(cte) = &semantic_plan.operator {
            if let Some(producer) = child_states.first() {
                memo.register_cte_producer(
                    cte.cte_index,
                    producer.group,
                    cte_producer_columns(cte, producer.group, memo, &state.binding_ids)?,
                )?;
            }
        }
        let output_rows_hard_upper = logical_properties.maximum_cardinality;
        let search_candidate = session.search_candidates.remove(&semantic_plan.id);
        if search_candidate.is_some() {
            debug!(
                target: targets::OPTIMIZER,
                rule = options.rule.0,
                operator = ?semantic_plan.operator.op_type(),
                "attached search provider to transformed logical expression"
            );
        }
        let plan = skeleton;
        // Preserve binding semantics before Query IR interning replaces
        // operator expressions with scalar-arena references.
        let scalar_roots = intern_operator_scalars(
            &plan.operator,
            &output_columns,
            &child_states
                .iter()
                .map(|child| child.columns.clone())
                .collect::<Vec<_>>(),
            &mut state.binding_ids,
            &mut state.columns,
            &mut state.scalars,
        )?;
        let (operator_fingerprint, operator_encoding) =
            query_operator_identity(&plan.operator, &scalar_roots, &state.scalars)?;
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
            // Group merges canonicalize child identities in the Memo without
            // rewriting this sidecar's historical keys. Compare children in
            // the current union-find domain so rediscovering the same shell
            // reuses its group instead of colliding in the allocation ledger.
            let equivalent_key =
                |candidate: &LogicalExprKey| {
                    candidate.operator == key.operator
                        && candidate.scalars == key.scalars
                        && candidate.children.len() == key.children.len()
                        && candidate.children.iter().zip(key.children.iter()).all(
                            |(left, right)| {
                                memo.canonical_group(*left) == memo.canonical_group(*right)
                            },
                        )
                };
            if let Some((group, _)) = state
                .expression_groups
                .range(
                    LogicalExprKey {
                        operator: key.operator,
                        scalars: Box::new([]),
                        children: Box::new([]),
                    }..,
                )
                .take_while(|(candidate, _)| candidate.operator == key.operator)
                .filter(|(candidate, _)| equivalent_key(candidate))
                .flat_map(|(_, candidates)| candidates.iter().copied())
                .find(|(group, logical)| {
                    let payload = memo.logical_expr(*logical).map(|logical| logical.payload);
                    let context_matches = payload
                        .and_then(|payload| state.metadata.get(&payload))
                        .is_some_and(|metadata| metadata.input_context == node_context);
                    let structure_matches = payload
                        .and_then(|payload| state.payloads.logical.get(payload.index()))
                        .is_some_and(|payload| {
                            payload.operator_encoding.as_ref() == operator_encoding.as_ref()
                        });
                    context_matches
                        && structure_matches
                        && memo.group(*group).is_some_and(|existing| {
                            existing.schema == schema
                                && existing
                                    .logical_properties
                                    .same_contract(&logical_properties)
                        })
                })
            {
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
                    .merge_equivalent_facts(&logical_properties)?;
                existing.cardinality =
                    std::mem::take(&mut existing.cardinality).canonical_with(cardinality.clone());
                return Ok(Some((
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
                )));
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
            let Some(group) = memo.create_optional_group(
                options.group_budget,
                {
                    let mut allocation = StableFingerprintBuilder::default();
                    allocation.write_bytes(b"paro.transformed-group.v2");
                    allocation.write_fingerprint(logical_identity);
                    allocation.write_u64(node_context.0 as u64);
                    // An operator shell can expose different output column
                    // identities (notably a freshly rebound Projection).
                    // Admission must name the same contract as group reuse.
                    allocation.write_bytes(&operator_encoding);
                    allocation.write_u64(schema.columns().len() as u64);
                    for column in schema.columns() {
                        allocation.write_u64(column.id.0 as u64);
                        allocation.write_u64(column.nullable as u64);
                    }
                    allocation.write_u64(logical_properties.unique_keys.len() as u64);
                    for key in &logical_properties.unique_keys {
                        allocation.write_u64(key.len() as u64);
                        for column in key {
                            allocation.write_u64(column.0 as u64);
                        }
                    }
                    allocation.write_u64(logical_properties.outer_references.len() as u64);
                    for column in &logical_properties.outer_references {
                        allocation.write_u64(column.0 as u64);
                    }
                    allocation.finish()
                },
                schema,
                logical_properties.clone(),
                cardinality.clone(),
            )?
            else {
                return Ok(None);
            };
            group
        };
        let region_scope = PlannerRegionScope::new(
            group,
            child_states.iter().map(|child| child.region_scope.clone()),
        );

        if target.is_some() {
            if let Some(existing) =
                memo.logical_expr_for_structural_key(group, &key, &operator_encoding)
            {
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
                    return Ok(Some((
                        semantic_plan,
                        NodeState {
                            group,
                            columns: output_columns.into_boxed_slice(),
                            region_scope,
                        },
                        None,
                    )));
                }
                return Ok(Some((
                    semantic_plan,
                    NodeState {
                        group,
                        columns: output_columns.into_boxed_slice(),
                        region_scope,
                    },
                    Some(StagedEquivalent {
                        key,
                        payload: existing.payload,
                        operator_encoding,
                        logical_properties,
                        cardinality,
                    }),
                )));
            }
        }

        let Some(scalar_facts) = super::super::scalar_facts::NativeScalarFacts::derive(
            &semantic_template.operator,
            &key.scalars,
            &state.scalars,
            &state.binding_ids,
            &state.columns,
            || memo.control().checkpoint(),
        )?
        else {
            return Ok(None);
        };
        let (payload, baseline_payload) = state.payloads.push_logical(PlannerLogicalPayload {
            scalar_facts,
            semantic_template,
            operator_encoding: operator_encoding.clone(),
            column_stats: column_stats.clone(),
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
                        column_stats: column_stats.as_ref(),
                        scan_access_cost: state.scan_access_cost,
                    },
                    &mut state.payloads,
                )
            })
            .transpose()?;
        // Physical admission consumes the bound semantic window. `plan` is
        // an interned scalar shell with no child ownership at all.
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
                column_stats.as_ref(),
                &state.binding_ids,
                state.scan_access_cost,
            )?,
            output_columns: output_columns.clone().into_boxed_slice(),
            child_layouts: semantic_plan
                .children()
                .into_iter()
                .map(|child| Arc::new(child.output_layout()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
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
                operator_encoding,
                logical_properties,
                cardinality,
            })
        } else {
            let logical = memo.insert_logical_with_operator_encoding(
                group,
                key.clone(),
                payload,
                EquivalenceProof::TransformationDescendant { rule: options.rule },
                operator_encoding,
            )?;
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
        Ok(Some((
            semantic_plan,
            NodeState {
                group,
                columns: output_columns.into_boxed_slice(),
                region_scope,
            },
            staged,
        )))
    }

    let plan_view = state.staging_arena.plan(plan)?;
    let provider_roots =
        crate::search::optimizer::SearchOptimizer::candidate_arena_roots(&plan_view)?;
    let search_context = if !provider_roots.is_empty() {
        let session_context = state
            .session
            .clone()
            .ok_or_else(|| paro_error::internal("search planning has no statement context"))?;
        let mut search_context =
            crate::context::OptimizationContext::new(session_context, state.bind_context.clone());
        search_context.column_stats = column_stats.clone();
        search_context.cost_model = state.cost_model.clone();
        search_context.verify_enabled = state.verify_enabled;
        Some(search_context)
    } else {
        None
    };

    // Search providers consume an explicit bounded Filter/TopN pattern. Read
    // that window before detaching its children; the rest of staging operates
    // on scalar shells and immutable group facts, never rebuilt descendants.
    let mut search_candidates = HashMap::new();
    if let Some(context) = &search_context {
        for index in provider_roots {
            if !memo.control().checkpoint()? {
                return Ok(None);
            }
            let node = state
                .staging_arena
                .export_checked(index, || context.session.cancellation.check())?;
            if let Some(candidate) = crate::search::optimizer::SearchOptimizer::new()
                .physical_candidate_for_root(&node, context)?
            {
                if node.id.is_synthetic() {
                    return Err(paro_error::internal(
                        "synthetic plan id cannot identify a search candidate",
                    ));
                }
                if search_candidates.insert(node.id, candidate).is_some() {
                    return Err(paro_error::internal(
                        "search provider has ambiguous occurrence identity",
                    ));
                }
            }
        }
    }
    let (root, staged, pending_runtime_filter_facets) = {
        let mut session = StagingSession {
            memo,
            state,
            options: StagingOptions {
                column_stats: &column_stats,
                column_stat_scopes: &column_stat_scopes,
                rule,
                group_budget: budget_class.group_dimension(),
            },
            pending_runtime_filter_facets: Vec::new(),
            nested_group_holes,
            facts: input_facts,
            search_candidates,
        };
        use paro_planner::plan::arena::{LogicalPlanNode, PlanIndex};
        let root_index = plan;
        session.state.staging_arena.get(root_index)?;
        let mut completed = BTreeMap::<PlanIndex, (LogicalPlanNode<()>, NodeState)>::new();
        let mut root_result = None;
        let Some(post_order) = session
            .state
            .staging_arena
            .post_order_controlled(root_index, || session.memo.control().checkpoint())?
        else {
            return Ok(None);
        };
        for index in post_order {
            if !session.memo.control().checkpoint()? {
                return Ok(None);
            }
            if let Some(statement) = &session.state.session {
                statement.cancellation.check()?;
            }
            let node = session.state.staging_arena.get(index)?.clone();
            let is_root = index == root_index;
            let mut child_states = Vec::new();
            let operator = node.operator.try_map_child_links(&mut |child| {
                let (transport, state) = completed
                    .get(&child)
                    .ok_or_else(|| paro_error::internal("staging lost an arena input"))?;
                child_states.push(state.clone());
                transport.instantiate(transport.id, []).map(Box::new)
            })?;
            let plan = OwnedLogicalPlan {
                id: node.id,
                stats: node.stats,
                operator,
            };
            let Some(result) = stage_node(
                &mut session,
                NodeStagingRequest {
                    plan,
                    target: is_root.then_some(target),
                    required_region_facet: is_root.then_some(preserved_region_facet).flatten(),
                    inherited_runtime_filter_facet: is_root
                        .then_some(inherited_runtime_filter_facet)
                        .flatten(),
                    node_context: if is_root {
                        input_context
                    } else {
                        child_context
                    },
                    target_child_context: is_root.then_some(child_context),
                    refined_cardinality_kind: is_root.then_some(refined_cardinality_kind).flatten(),
                },
                child_states,
            )?
            else {
                return Ok(None);
            };
            let (mut plan, node, staged) = result;
            if !is_root && !matches!(plan.operator, LogicalOperator::BoundReference(_)) {
                session
                    .facts
                    .settle_group(session.memo, session.state, node.group)?;
                let layout = Arc::new(plan.output_layout());
                let facts =
                    session
                        .facts
                        .transport(session.memo, session.state, node.group, &layout)?;
                let (types, bindings) = Arc::unwrap_or_clone(layout).into_parts();
                let reference = paro_planner::operator::BoundReference::new(
                    paro_planner::operator::BoundReferenceId::node_occurrence(plan.id.0),
                    bindings,
                    types,
                )
                .with_facts(facts);
                plan.operator = LogicalOperator::BoundReference(reference);
            }
            if is_root {
                root_result = Some((node, staged));
            } else {
                debug_assert!(staged.is_none());
                completed.insert(index, (LogicalPlanNode::from_shell(plan), node));
            }
        }
        let Some((root, staged)) = root_result else {
            return Err(paro_error::internal("staging has no completed root"));
        };
        if !session.nested_group_holes.is_empty() {
            return Err(paro_error::internal(
                "transformation rewrite discarded an opaque Memo group hole",
            ));
        }
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
        let (scope, overflow) = root.region_scope.materialize_bounded(memo, ceiling);
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

    fn test_base_get(
        table_index: usize,
        object_id: u64,
        name: &str,
        rows: u64,
    ) -> OwnedLogicalPlan {
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
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
            table_index,
            vec!["id".to_string()],
            vec![LogicalType::Integer],
            table,
        ))));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        plan
    }

    fn equality_join(
        left: OwnedLogicalPlan,
        right: OwnedLogicalPlan,
        rows: u64,
    ) -> OwnedLogicalPlan {
        let condition = JoinCondition::equality(
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into()),
        );
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Comparison(
            ComparisonJoin::new(JoinType::Inner, left, right, vec![condition]),
        )));
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(rows));
        plan
    }

    #[test]
    fn alias_projections_allocate_distinct_schema_contracts() {
        use paro_planner::expression::ColumnRefExpression;
        use paro_planner::operator::{Projection, SetOperation};
        let source = || test_base_get(0, 30_099, "shared_source", 100);
        let union = |left, right| {
            OwnedLogicalPlan::synthetic(LogicalOperator::SetOperation(SetOperation::union(
                10,
                left,
                right,
                true,
                vec![LogicalType::Integer],
            )))
        };
        let mut input = MemoBuilder::build(
            union(source(), source()),
            BindContext::new(),
            SearchBudget::default(),
        )
        .unwrap();
        let project = |table| {
            OwnedLogicalPlan::synthetic(LogicalOperator::Projection(Projection::new(
                table,
                source(),
                vec![Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
                )],
            )))
        };
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let staged = stage_transformed_expression(
            StagingRequest {
                input_facts: boundary::BoundarySnapshot::default(),
                plan: state
                    .staging_arena
                    .import(union(project(2), project(3)))
                    .unwrap(),
                column_stats: Arc::new(HashMap::new()),
                column_stat_scopes: HashMap::new(),
                target: StagingTarget {
                    group: input.root,
                    rule: RuleId(999),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap()
        .expect("alias projections must not collide in group allocation admission");
        let children = &staged.key.children;
        assert_eq!(children.len(), 2);
        assert_ne!(children[0], children[1]);
        assert_ne!(
            input.memo.group(children[0]).unwrap().schema,
            input.memo.group(children[1]).unwrap().schema
        );
    }

    #[test]
    fn staging_preserves_the_settled_root_projection_before_canonicalization() {
        use paro_planner::operator::{Filter, ProjectionMap};
        let make_plan = || {
            let source = equality_join(
                test_base_get(0, 70_001, "left_source", 10),
                test_base_get(1, 70_002, "right_source", 10),
                10,
            );
            let mut filter = Filter::new(source, vec![]);
            filter.projection_map = ProjectionMap::new(vec![1]);
            OwnedLogicalPlan::synthetic(LogicalOperator::Filter(filter))
        };
        let mut input =
            MemoBuilder::build(make_plan(), BindContext::new(), SearchBudget::default()).unwrap();
        let mut state = input.planner_state.write().unwrap();
        state.session = Some(TestStatementContextBuilder::minimal().build());
        let plan = state.staging_arena.import(make_plan()).unwrap();
        let staged = stage_transformed_expression(
            StagingRequest {
                plan,
                input_facts: boundary::BoundarySnapshot::default(),
                column_stats: Arc::new(HashMap::new()),
                column_stat_scopes: HashMap::new(),
                target: StagingTarget {
                    group: input.root,
                    rule: RuleId(999),
                    budget_class: TransformationBudgetClass::Local,
                    input_context: OptimizationContextId(0),
                    child_context: OptimizationContextId(0),
                    refined_cardinality_kind: None,
                },
                regions: StagingRegionRequirements {
                    preserved_facet: None,
                    extended_required_facets: Box::new([]),
                    inherited_runtime_filter_facet: None,
                },
                nested_group_holes: BTreeMap::new(),
            },
            &mut input.memo,
            &mut state,
        )
        .unwrap()
        .unwrap();
        let metadata = &state.metadata[&staged.payload];
        assert_eq!(metadata.output_columns.len(), 1);
        assert_eq!(
            input.memo.group(input.root).unwrap().schema.columns().len(),
            1
        );
        let LogicalOperator::Filter(template) = &state.payloads.logical[staged.payload.index()]
            .semantic_template
            .operator
        else {
            panic!("expected canonical filter template")
        };
        assert!(template.projection_map.is_all());
    }

    #[test]
    fn root_key_collision_in_another_context_declines_and_rolls_back() {
        let bind_context = BindContext::new();
        let plan = OwnedLogicalPlan::new(
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
                            plan: state.staging_arena.import(staged_plan).unwrap(),
                            input_facts: boundary::BoundarySnapshot::default(),
                            column_stats: Arc::new(HashMap::new()),
                            column_stat_scopes: HashMap::new(),
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
                            nested_group_holes: BTreeMap::new(),
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
                            plan: state.staging_arena.import(transformed).unwrap(),
                            input_facts: boundary::BoundarySnapshot::default(),
                            column_stats: Arc::new(HashMap::new()),
                            column_stat_scopes: HashMap::new(),
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
                            nested_group_holes: BTreeMap::new(),
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
