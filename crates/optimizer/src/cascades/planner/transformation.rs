// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical equivalence rules and transactional Memo staging.

use super::*;

mod matching;

pub(super) fn register_transformations(
    registry: &mut ImplementationRegistry,
    planner_state: Arc<RwLock<PlannerTransformState>>,
) -> Result<()> {
    for transformation in PlannerTransformation::ALL {
        registry.register_transformation(PlannerTransformationRule {
            transformation,
            planner_state: planner_state.clone(),
        })?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum PlannerTransformation {
    ExpensivePredicatePlacement,
    CteInline,
    CteFilterPushdown,
    AggregatePostReduction,
    JoinElimination,
    AggregateJoinPreaggregation,
    AggregateJoinSubsumption,
    AggregateNonNullInput,
    AggregateDimensionDeferral,
    AggregateInputMaterialization,
    LimitPushdown,
    LatePayloadFetch,
    ScalarAggregateWindow,
}

impl PlannerTransformation {
    const ALL: [Self; 13] = [
        Self::ExpensivePredicatePlacement,
        Self::CteInline,
        Self::CteFilterPushdown,
        Self::AggregatePostReduction,
        Self::JoinElimination,
        Self::AggregateJoinPreaggregation,
        Self::AggregateJoinSubsumption,
        Self::AggregateNonNullInput,
        Self::AggregateDimensionDeferral,
        Self::AggregateInputMaterialization,
        Self::LimitPushdown,
        Self::LatePayloadFetch,
        Self::ScalarAggregateWindow,
    ];

    const fn id(self) -> RuleId {
        match self {
            Self::ExpensivePredicatePlacement => EXPENSIVE_PREDICATE_PLACEMENT_RULE,
            Self::CteInline => CTE_INLINE_RULE,
            Self::CteFilterPushdown => CTE_FILTER_PUSHDOWN_RULE,
            Self::AggregatePostReduction => AGGREGATE_POST_REDUCTION_RULE,
            Self::JoinElimination => JOIN_ELIMINATION_RULE,
            Self::AggregateJoinPreaggregation => AGGREGATE_JOIN_PREAGGREGATION_RULE,
            Self::AggregateJoinSubsumption => AGGREGATE_JOIN_SUBSUMPTION_RULE,
            Self::AggregateNonNullInput => AGGREGATE_NON_NULL_INPUT_RULE,
            Self::AggregateDimensionDeferral => AGGREGATE_DIMENSION_DEFERRAL_RULE,
            Self::AggregateInputMaterialization => AGGREGATE_INPUT_MATERIALIZATION_RULE,
            Self::LimitPushdown => LIMIT_PUSHDOWN_RULE,
            Self::LatePayloadFetch => LATE_PAYLOAD_FETCH_RULE,
            Self::ScalarAggregateWindow => SCALAR_AGGREGATE_WINDOW_RULE,
        }
    }
}

#[derive(Debug)]
struct PlannerTransformationRule {
    transformation: PlannerTransformation,
    planner_state: Arc<RwLock<PlannerTransformState>>,
}

impl TransformationRule for PlannerTransformationRule {
    fn id(&self) -> RuleId {
        self.transformation.id()
    }

    fn promise(
        &self,
        _expr: &crate::cascades::memo::LogicalExpr,
        _ctx: &RuleContext<'_>,
    ) -> RulePromise {
        RulePromise::NORMAL
    }

    fn matches(&self, expr: &crate::cascades::memo::LogicalExpr, _ctx: &RuleContext<'_>) -> bool {
        let state = self
            .planner_state
            .read()
            .expect("planner transform state poisoned");
        state.binder.is_some()
            && matching::matches_transformation(self.transformation, expr, _ctx.memo, &state)
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let target_group = ctx.group();
        let (plan, source_stats, source_region, environment) =
            {
                let state = self
                    .planner_state
                    .read()
                    .expect("planner transform state poisoned");
                let plan = semantic_view::materialize(ctx.memo(), &state, expr)?;
                let logical = ctx.memo().logical_expr(expr).ok_or_else(|| {
                    paro_error::internal("planner rule lost its source expression")
                })?;
                let metadata = state
                    .metadata
                    .get(&logical.payload)
                    .ok_or_else(|| paro_error::internal("planner rule lost its source metadata"))?;
                let payload = state
                    .payloads
                    .logical
                    .get(logical.payload.index())
                    .ok_or_else(|| paro_error::internal("planner rule lost its source payload"))?;
                let binder = state.binder.clone().ok_or_else(|| {
                    paro_error::internal("planner rule has no binder environment")
                })?;
                (
                    plan,
                    payload.column_stats.clone(),
                    metadata
                        .required_region_facet
                        .map(|facet| (facet, metadata.operator_type)),
                    PlannerRuleEnvironment {
                        binder,
                        bind_context: state.bind_context.clone(),
                        session: state.session.clone().ok_or_else(|| {
                            paro_error::internal("planner rule has no statement context")
                        })?,
                        cost_model: state.cost_model.clone(),
                        verify_enabled: state.verify_enabled,
                    },
                )
            };
        let Some(plan) = rewrite_planner_expression(
            self.transformation,
            plan,
            source_stats.as_ref(),
            &environment,
        )?
        else {
            return Ok(Box::new([]));
        };
        let (plan, column_stats) = settle_transformed_expression(plan, &environment)?;
        let mut preserved_region_facet = None;
        if let Some((facet, source_operator)) = source_region {
            let kind = ctx
                .memo()
                .regions()
                .nodes
                .iter()
                .flat_map(|region| region.facets.iter())
                .find(|candidate| candidate.fingerprint == facet)
                .map(|facet| facet.kind)
                .ok_or_else(|| {
                    paro_error::internal("planner rule references an unknown required region facet")
                })?;
            let discharges_sharing =
                matches!(self.transformation, PlannerTransformation::CteInline)
                    && kind == RegionFacetKind::Sharing
                    && plan.operator.op_type() != source_operator;
            let preserves_sharing = matches!(
                self.transformation,
                PlannerTransformation::CteInline | PlannerTransformation::CteFilterPushdown
            ) && kind == RegionFacetKind::Sharing
                && plan.operator.op_type() == source_operator;
            if preserves_sharing {
                preserved_region_facet = Some(facet);
            } else if !discharges_sharing {
                debug!(
                    target: targets::OPTIMIZER,
                    rule = self.id().0,
                    group = target_group.index(),
                    ?kind,
                    "discarded local transformation that cannot preserve a required region"
                );
                return Ok(Box::new([]));
            }
        }
        let mut state = self
            .planner_state
            .write()
            .expect("planner transform state poisoned");
        if !transformed_plan_matches_group_contract(&plan, target_group, ctx.memo(), &state)? {
            debug!(
                target: targets::OPTIMIZER,
                rule = self.id().0,
                group = target_group.index(),
                "discarded optional transformation before staging an incompatible root contract"
            );
            return Ok(Box::new([]));
        }
        // Enlist planner-owned arenas and indexes in the engine's attempt
        // before the first staging write. The savepoint is only fixed-size
        // counts into append-only arenas and mutation journals.
        let savepoint = state.savepoint();
        let rollback_state = self.planner_state.clone();
        ctx.enlist_rollback(move || {
            let mut state = rollback_state.write().map_err(|_| {
                paro_error::internal("planner transform state poisoned during rollback")
            })?;
            state.rollback_to(savepoint)
        });
        let staged = stage_transformed_expression(
            plan,
            Arc::new(column_stats),
            target_group,
            self.id(),
            ctx.memo_mut(),
            &mut state,
            preserved_region_facet,
        )?;
        drop(state);
        let source = ctx
            .memo()
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("planner rule lost its source expression"))?;
        Ok(vec![EquivalentExpression {
            target_group,
            key: staged.key,
            payload: staged.payload,
            proof: EquivalenceProof::Transformation {
                rule: self.id(),
                source: expr,
                premise: source.key.stable_fingerprint(),
            },
        }]
        .into_boxed_slice())
    }
}

fn transformed_plan_matches_group_contract(
    plan: &LogicalPlan,
    target: GroupId,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<bool> {
    let bindings = plan.get_column_bindings();
    let types = plan.types();
    if bindings.len() != types.len() {
        return Ok(false);
    }
    let mut columns = BTreeSet::new();
    for (binding, logical_type) in bindings.into_iter().zip(types) {
        let domain = logical_type_fingerprint(&logical_type);
        let Some(column) = state
            .binding_ids
            .get(&(binding.table_index, binding.column_index, domain))
            .copied()
        else {
            return Ok(false);
        };
        columns.insert(column);
    }
    let schema = GroupSchema::new(
        columns
            .into_iter()
            .map(|column| {
                state.columns.get(column).cloned().ok_or_else(|| {
                    paro_error::internal("transformation contract references an unknown column")
                })
            })
            .collect::<Result<Vec<_>>>()?,
    )?;
    Ok(memo
        .group(target)
        .is_some_and(|group| group.schema == schema))
}

#[derive(Clone)]
struct PlannerRuleEnvironment {
    binder: Binder,
    bind_context: BindContext,
    session: Arc<paro_context::StatementContext>,
    cost_model: crate::cost_model::CostModel,
    verify_enabled: bool,
}

fn rewrite_planner_expression(
    transformation: PlannerTransformation,
    plan: LogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    environment: &PlannerRuleEnvironment,
) -> Result<Option<LogicalPlan>> {
    let rewritten = match transformation {
        PlannerTransformation::ExpensivePredicatePlacement => {
            let mut context = crate::context::OptimizationContext::new(
                environment.session.clone(),
                environment.bind_context.clone(),
            );
            context.column_stats = column_stats.clone();
            context.cost_model = environment.cost_model.clone();
            context.verify_enabled = environment.verify_enabled;
            let (plan, changed) = ReorderFilter::new().rewrite_with_change(plan, &context)?;
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::CteInline => {
            let (plan, changed) =
                CTEInlining::new(&environment.bind_context).optimize_plan_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::CteFilterPushdown => {
            let plan = FilterPullup::new().rewrite_plan(plan);
            let plan = FilterPushdown::new().rewrite_plan(plan);
            let (plan, changed) = CTEFilterPusher::new().optimize_plan_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregatePostReduction => {
            let (plan, changed) =
                post_reduction::optimize_plan_with_change(plan, &environment.bind_context);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::JoinElimination => {
            let (plan, changed) = JoinElimination::new().optimize_plan_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregateJoinPreaggregation => {
            let (plan, changed) =
                join_preaggregation::optimize_plan(plan, &environment.bind_context);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregateJoinSubsumption => {
            let plan = FilterPushdown::new().rewrite_plan(plan);
            let (plan, changed) = join_subsumption::optimize_plan_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregateNonNullInput => {
            let (plan, changed) = non_null_inputs::optimize_plan_with_change(plan, column_stats);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregateDimensionDeferral => {
            let (plan, changed) = dimension_deferral::optimize_plan(
                plan,
                &environment.bind_context,
                &environment.cost_model,
            )?;
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregateInputMaterialization => {
            let (plan, changed) =
                input_materialization::optimize_plan(plan, &environment.bind_context)?;
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::LimitPushdown => {
            let (plan, changed) = LimitPushdown::new().optimize_plan_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::LatePayloadFetch => {
            let (plan, prefix_changed) = late_payload::optimize_matched_prefix_plan(plan)?;
            let (plan, payload_changed) = late_payload::optimize_plan(
                plan,
                &environment.bind_context,
                &environment.cost_model,
            )?;
            if !prefix_changed && !payload_changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::ScalarAggregateWindow => {
            let (plan, changed) = scalar_aggregate_window::optimize_plan_with_change(
                plan,
                &environment.bind_context,
            )?;
            if !changed {
                return Ok(None);
            }
            plan
        }
    };
    Ok(Some(rewritten))
}

fn settle_transformed_expression(
    mut plan: LogicalPlan,
    environment: &PlannerRuleEnvironment,
) -> Result<(LogicalPlan, HashMap<ColumnBinding, Arc<ColumnStatistics>>)> {
    RemoveUnusedColumns::optimize(
        &mut plan,
        &environment.binder,
        environment.session.as_ref(),
        true,
    );
    let mut context = crate::context::OptimizationContext::new(
        environment.session.clone(),
        environment.bind_context.clone(),
    );
    context.cost_model = environment.cost_model.clone();
    context.verify_enabled = environment.verify_enabled;
    plan = StatisticsGathering::new().gather(plan, &mut context)?;
    let mut propagator = StatisticsPropagator::new();
    plan = propagator.propagate(environment.session.clone(), plan);
    context.column_stats = propagator.take_statistics_map();
    plan = StatisticsGathering::new().gather(plan, &mut context)?;
    plan = singleton_groups::optimize_plan(plan, &context.column_stats);
    plan = ColumnLifetimeAnalyzer::new(true).optimize(plan)?;
    if environment.verify_enabled {
        verify_logical_plan(&environment.bind_context, &plan)?;
    }
    Ok((plan, context.column_stats))
}

struct StagedEquivalent {
    key: LogicalExprKey,
    payload: LogicalPayloadId,
}

fn stage_transformed_expression(
    plan: LogicalPlan,
    column_stats: Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
    target: GroupId,
    rule: RuleId,
    memo: &mut Memo,
    state: &mut PlannerTransformState,
    preserved_region_facet: Option<Fingerprint>,
) -> Result<StagedEquivalent> {
    struct NodeState {
        group: GroupId,
        columns: Box<[ColumnId]>,
        subtree_groups: BTreeSet<GroupId>,
    }

    fn stage_node(
        plan: LogicalPlan,
        column_stats: &Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
        target: Option<GroupId>,
        rule: RuleId,
        memo: &mut Memo,
        state: &mut PlannerTransformState,
        required_region_facet: Option<Fingerprint>,
    ) -> Result<(LogicalPlan, NodeState, Option<StagedEquivalent>)> {
        let mut detached = Vec::new();
        let skeleton = plan.try_map_children(|child| {
            detached.push(child);
            Ok(LogicalPlan::synthetic(LogicalOperator::DummyScan))
        })?;
        let mut child_states = Vec::with_capacity(detached.len());
        let mut children = Vec::with_capacity(detached.len());
        for child in detached {
            let (child, child_state, staged) =
                stage_node(child, column_stats, None, rule, memo, state, None)?;
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

        if target.is_none() {
            if let Some((group, _)) = state.expression_groups.get(&key).and_then(|candidates| {
                candidates.iter().copied().find(|(group, _)| {
                    memo.group(*group).is_some_and(|existing| {
                        existing.schema == schema
                            && existing.logical_properties == logical_properties
                    })
                })
            }) {
                return Ok((
                    plan,
                    NodeState {
                        group,
                        columns: output_columns.into_boxed_slice(),
                        subtree_groups: child_states
                            .iter()
                            .flat_map(|child| child.subtree_groups.iter().copied())
                            .chain(std::iter::once(group))
                            .collect(),
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
                || contract.logical_properties.unique_keys != logical_properties.unique_keys
                || contract.logical_properties.outer_references
                    != logical_properties.outer_references
            {
                return Err(paro_error::internal(format!(
                    "transformation rule {} changed its target group logical contract: target_schema={:?}, output_schema={schema:?}, target_properties={:?}, output_properties={logical_properties:?}",
                    rule.0,
                    contract.schema, contract.logical_properties,
                )));
            }
            target
        } else {
            memo.create_group(schema, logical_properties)
        };
        let subtree_groups = child_states
            .iter()
            .flat_map(|child| child.subtree_groups.iter().copied())
            .chain(std::iter::once(group))
            .collect::<BTreeSet<_>>();

        if target.is_some() {
            if let Some(existing) = memo.logical_expr_for_key(group, &key) {
                return Ok((
                    plan,
                    NodeState {
                        group,
                        columns: output_columns.into_boxed_slice(),
                        subtree_groups,
                    },
                    Some(StagedEquivalent {
                        key,
                        payload: existing.payload,
                    }),
                ));
            }
        }

        let output_estimate = plan.stats.estimated_cardinality;
        let mut payload_skeleton =
            duplicate_plan_preserving_indices(&plan, state.bind_context.shared().as_ref())
                .map_children(|_| LogicalPlan::synthetic(LogicalOperator::DummyScan));
        payload_skeleton.stats = NodeStats::default();
        let payload = state.payloads.push(PlannerLogicalPayload {
            extraction_template: payload_skeleton,
            output_estimate,
            column_stats: column_stats.clone(),
        });
        let mut implementations = planner_implementation_set(&plan, state.rowset_scan_pushdown);
        let runtime_filter_candidate = target.is_none() && implementations.hash_join_runtime_filter;
        if !runtime_filter_candidate {
            implementations.hash_join_runtime_filter = false;
        }
        let metadata = PlannerOperatorMetadata {
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
            cost_facts: planner_cost_facts(
                &plan,
                output_rows_hard_upper,
                &child_maximum_cardinalities,
                state.scan_access_cost,
            )?,
            output_columns: output_columns.clone().into_boxed_slice(),
            search: None,
            required_region_facet: target.and(required_region_facet),
            runtime_filter_region_facet: None,
            structural_retained_children: planner_structural_retained_children(&plan.operator),
        };
        if state.metadata.insert(payload, metadata).is_some() {
            return Err(paro_error::internal(
                "transformed planner payload metadata was assigned twice",
            ));
        }

        let staged = if target.is_some() {
            Some(StagedEquivalent { key, payload })
        } else {
            let logical =
                memo.insert_logical(group, key.clone(), payload, EquivalenceProof::Initial)?;
            state.record_expression_group(key, group, logical);
            if runtime_filter_candidate {
                let mut facet = planner_region_facet(
                    RegionFacetKind::RuntimeFilter,
                    FacetCriticality::Optional,
                    logical,
                    operator_fingerprint,
                    subtree_groups.clone(),
                );
                // Preserve already-admitted baseline side inputs when a
                // transformed alternative overlaps them. The later dynamic
                // facet yields first if the composite ceiling is reached.
                facet.priority = 2_000 + RegionFacetKind::RuntimeFilter as u16;
                let fingerprint = facet.fingerprint;
                let dropped = memo.upsert_region_facet(facet)?;
                disable_dropped_runtime_filter_facets(state, &dropped)?;
                if !dropped.contains(&fingerprint) {
                    state
                        .metadata
                        .get_mut(&payload)
                        .ok_or_else(|| {
                            paro_error::internal("dynamic runtime-filter payload disappeared")
                        })?
                        .runtime_filter_region_facet = Some(fingerprint);
                }
            }
            None
        };
        Ok((
            plan,
            NodeState {
                group,
                columns: output_columns.into_boxed_slice(),
                subtree_groups,
            },
            staged,
        ))
    }

    let (_, root, staged) = stage_node(
        plan,
        &column_stats,
        Some(target),
        rule,
        memo,
        state,
        preserved_region_facet,
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
        facet.scope.extend(root.subtree_groups);
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
