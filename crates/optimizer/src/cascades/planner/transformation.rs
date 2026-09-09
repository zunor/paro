// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical equivalence rules and transactional Memo staging.

use super::*;

pub(super) mod cte;
mod join_region;
mod matching;
pub(super) mod settlement;
mod staging;

use staging::{
    stage_transformed_expression, NativeChild, NativeNode, NativeShell, StagingInput,
    StagingRegionRequirements, StagingRequest, StagingTarget,
};

fn settle_with_session_arena(
    state: &mut PlannerTransformState,
    plan: OwnedLogicalPlan,
    environment: &PlannerRuleEnvironment,
) -> Result<Option<settlement::SettledExpression>> {
    state
        .settlement_cache
        .settle_arena_in(plan, environment, &mut state.staging_arena)
}

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
    PredicateTransfer,
    KeyDomainTransfer,
    CtePartitionedMaterialization,
    CteInline,
    CteDemandPushdown,
    CteFilterPushdown,
    JoinRegionEnumeration,
    AggregatePostReduction,
    MarkJoinToSemi,
    JoinElimination,
    AggregateJoinPreaggregation,
    AggregateJoinSubsumption,
    AggregateNonNullInput,
    AggregateDimensionDeferral,
    AggregateDimensionSharing,
    AggregateInputMaterialization,
    TopNIntroduction,
    LimitPushdown,
    LatePayloadFetch,
    ScalarAggregateWindow,
}

impl PlannerTransformation {
    const ALL: [Self; 20] = [
        Self::PredicateTransfer,
        Self::KeyDomainTransfer,
        Self::CtePartitionedMaterialization,
        Self::CteInline,
        Self::CteDemandPushdown,
        Self::CteFilterPushdown,
        Self::JoinRegionEnumeration,
        Self::AggregatePostReduction,
        Self::MarkJoinToSemi,
        Self::JoinElimination,
        Self::AggregateJoinPreaggregation,
        Self::AggregateJoinSubsumption,
        Self::AggregateNonNullInput,
        Self::AggregateDimensionDeferral,
        Self::AggregateDimensionSharing,
        Self::AggregateInputMaterialization,
        Self::TopNIntroduction,
        Self::LimitPushdown,
        Self::LatePayloadFetch,
        Self::ScalarAggregateWindow,
    ];

    const fn id(self) -> RuleId {
        match self {
            Self::PredicateTransfer => PREDICATE_TRANSFER_RULE,
            Self::KeyDomainTransfer => KEY_DOMAIN_TRANSFER_RULE,
            Self::CtePartitionedMaterialization => CTE_PARTITIONED_MATERIALIZATION_RULE,
            Self::CteInline => CTE_INLINE_RULE,
            Self::CteDemandPushdown => CTE_DEMAND_PUSHDOWN_RULE,
            Self::CteFilterPushdown => CTE_FILTER_PUSHDOWN_RULE,
            Self::JoinRegionEnumeration => JOIN_REGION_ENUMERATION_RULE,
            Self::AggregatePostReduction => AGGREGATE_POST_REDUCTION_RULE,
            Self::MarkJoinToSemi => MARK_JOIN_TO_SEMI_RULE,
            Self::JoinElimination => JOIN_ELIMINATION_RULE,
            Self::AggregateJoinPreaggregation => AGGREGATE_JOIN_PREAGGREGATION_RULE,
            Self::AggregateJoinSubsumption => AGGREGATE_JOIN_SUBSUMPTION_RULE,
            Self::AggregateNonNullInput => AGGREGATE_NON_NULL_INPUT_RULE,
            Self::AggregateDimensionDeferral => AGGREGATE_DIMENSION_DEFERRAL_RULE,
            Self::AggregateDimensionSharing => AGGREGATE_DIMENSION_SHARING_RULE,
            Self::AggregateInputMaterialization => AGGREGATE_INPUT_MATERIALIZATION_RULE,
            Self::TopNIntroduction => TOP_N_INTRODUCTION_RULE,
            Self::LimitPushdown => LIMIT_PUSHDOWN_RULE,
            Self::LatePayloadFetch => LATE_PAYLOAD_FETCH_RULE,
            Self::ScalarAggregateWindow => SCALAR_AGGREGATE_WINDOW_RULE,
        }
    }

    /// Only proof-producing rewrites may replace the group's canonical
    /// statistics recipe. Shape-only alternatives keep the normalized recipe
    /// so enumeration cannot vote estimates up or down.
    const fn cardinality_recipe_kind(self) -> Option<CardinalityRecipeKind> {
        match self {
            Self::CteInline
            | Self::CteDemandPushdown
            | Self::CteFilterPushdown
            | Self::JoinRegionEnumeration
            | Self::AggregateJoinSubsumption
            | Self::JoinElimination => Some(CardinalityRecipeKind::ConstraintRefined),
            _ => None,
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

    fn binding_reads(
        &self,
        binding: &PatternBinding,
        _discovery_reads: &[PatternRead],
        ctx: &RuleContext<'_>,
    ) -> Result<Box<[PatternRead]>> {
        let mut groups = BTreeSet::new();
        let mut pending = vec![&binding.root];
        while let Some(operand) = pending.pop() {
            let group = match operand {
                PatternOperand::Group(group) => *group,
                PatternOperand::Expression {
                    group, children, ..
                } => {
                    pending.extend(children.iter());
                    *group
                }
            };
            groups.insert(ctx.memo.canonical_group(group));
        }
        groups
            .into_iter()
            .map(|group| PatternRead::facts_from_group(ctx.memo, group))
            .collect()
    }

    fn binding_fact_value(
        &self,
        binding: &PatternBinding,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Option<Fingerprint>> {
        let state = self
            .planner_state
            .read()
            .map_err(|_| paro_error::internal("planner transform state poisoned"))?;
        let Some(facts) = boundary::BoundarySnapshot::read(
            ctx,
            &state,
            &binding.root,
            self.budget_class().work_dimension(),
        )?
        else {
            return Ok(None);
        };
        Ok(Some(
            facts.binding_value_fingerprint(ctx.memo(), &binding.root)?,
        ))
    }

    fn output_bound(&self, binding: &PatternBinding, ctx: &RuleContext<'_>) -> usize {
        if matches!(
            self.transformation,
            PlannerTransformation::CtePartitionedMaterialization
        ) {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            let logical = ctx
                .memo
                .logical_expr(binding.root_expression())
                .expect("bound root disappeared");
            let operator = &state.payloads.logical[logical.payload.index()]
                .semantic_template
                .operator;
            if let LogicalOperator::MaterializedCTE(cte) = operator {
                // Each producer column can induce at most one partition.
                return cte.column_types.len().max(1);
            }
        }
        if matches!(
            self.transformation,
            PlannerTransformation::JoinRegionEnumeration
        ) {
            usize::from(ctx.memo.budget().max_multiway_join_candidates).max(1)
        } else {
            1
        }
    }

    fn output_saturates_observed_binding(&self) -> bool {
        matches!(
            self.transformation,
            PlannerTransformation::JoinRegionEnumeration
                | PlannerTransformation::AggregateDimensionDeferral
                | PlannerTransformation::AggregateDimensionSharing
                | PlannerTransformation::AggregateInputMaterialization
        )
    }

    fn matches_root(&self, expr: &crate::cascades::memo::LogicalExpr) -> bool {
        if self.output_saturates_observed_binding()
            && expression_is_only_rule_output(expr, self.id())
        {
            return false;
        }
        let state = self
            .planner_state
            .read()
            .expect("planner transform state poisoned");
        state.binder.is_some()
            && matching::matches_transformation_root(self.transformation, expr, &state)
    }

    fn promise(
        &self,
        _expr: &crate::cascades::memo::LogicalExpr,
        _ctx: &RuleContext<'_>,
    ) -> RulePromise {
        match self.transformation {
            // Sharing-preserving producer restrictions must be explored before
            // an inline alternative duplicates the producer.  Besides exposing
            // the cheaper shared shape sooner, this makes the global optional
            // group budget independent of the number of CTE references: an
            // expansive alternative cannot consume all staging capacity before
            // the bounded producer alternatives have been considered.
            PlannerTransformation::CteFilterPushdown | PlannerTransformation::PredicateTransfer => {
                RulePromise::HIGH
            }
            // Partitioning and inlining duplicate sharing-owner structure.
            // Explore them only after producer restriction and the ordinary
            // local rewrites of the restricted child groups have reached the
            // Memo, so a finite query-wide group budget cannot strand the
            // sharing-preserving composition.
            PlannerTransformation::CtePartitionedMaterialization
            | PlannerTransformation::CteInline => RulePromise::LOW,
            _ => RulePromise::NORMAL,
        }
    }

    fn budget_class(&self) -> TransformationBudgetClass {
        if matches!(
            self.transformation,
            PlannerTransformation::CtePartitionedMaterialization
                | PlannerTransformation::AggregateDimensionSharing
        ) {
            TransformationBudgetClass::Composition
        } else {
            TransformationBudgetClass::Local
        }
    }

    fn matches(&self, expr: &crate::cascades::memo::LogicalExpr, _ctx: &RuleContext<'_>) -> bool {
        self.matches_root(expr)
    }

    fn bindings(&self, expr: LogicalExprId, ctx: &RuleContext<'_>) -> Result<PatternBindingSet> {
        let state = self
            .planner_state
            .read()
            .map_err(|_| paro_error::internal("planner transform state poisoned"))?;
        let cancellation = state
            .session
            .as_ref()
            .map(|session| session.cancellation.clone());
        if matches!(
            self.transformation,
            PlannerTransformation::AggregateDimensionSharing
        ) {
            return matching::dimension_sharing_pattern_bindings(
                ctx.group,
                expr,
                ctx.memo,
                &state,
                ctx.memo.budget(),
                cancellation.as_ref(),
            );
        }
        let mut bindings = matching::scoped_pattern_bindings(
            self.transformation,
            ctx.group,
            expr,
            ctx.memo,
            &state,
            cancellation.as_ref(),
            self.budget_class().work_dimension(),
        )?;
        if matches!(
            self.transformation,
            PlannerTransformation::JoinRegionEnumeration
        ) {
            // Associative parenthesizations are not distinct inputs to the
            // region enumerator. Collapse them before fact derivation and rule
            // admission using the same exact graph transcript as apply.
            let mut graph_identities = BTreeSet::new();
            let mut unique = Vec::new();
            for binding in bindings.bindings.into_vec() {
                let identity =
                    join_region::identity_with_facts(&binding.root, ctx.memo, &state, None)?;
                if identity.is_none_or(|identity| graph_identities.insert(identity)) {
                    unique.push(binding);
                }
            }
            bindings.bindings = unique.into_boxed_slice();
        }
        Ok(bindings)
    }

    fn apply(
        &self,
        _expr: LogicalExprId,
        _ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        Err(paro_error::internal(
            "planner transformations require an explicit PatternBinding",
        ))
    }

    fn apply_binding(
        &self,
        binding: &PatternBinding,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        let expr = binding.root_expression();
        let target_group = ctx.group();
        let facts = {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            let Some(facts) = boundary::BoundarySnapshot::read(
                ctx,
                &state,
                &binding.root,
                self.budget_class().work_dimension(),
            )?
            else {
                return Ok(Box::new([]));
            };
            facts
        };
        ctx.record_fact_value(facts.binding_value_fingerprint(ctx.memo(), &binding.root)?);
        let region_identity = if matches!(
            self.transformation,
            PlannerTransformation::JoinRegionEnumeration
        ) {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            let identity =
                join_region::identity_with_facts(&binding.root, ctx.memo(), &state, Some(&facts))?
                    .map(|identity| (ctx.memo().canonical_group(target_group), identity));
            if identity
                .as_ref()
                .is_some_and(|key| state.enumerated_join_regions.contains(key))
            {
                return Ok(Box::new([]));
            }
            identity
        } else {
            None
        };
        // These two search rules can consume the exact matched shell directly
        // for their conservative native subsets. Keep the legacy owned-plan
        // path available for every shape that needs richer semantic handling.
        let direct_native = {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            match self.transformation {
                PlannerTransformation::PredicateTransfer => {
                    try_native_predicate_transfer(&binding.root, ctx.memo(), &state, &facts)?
                        .into_iter()
                        .collect()
                }
                PlannerTransformation::JoinRegionEnumeration => {
                    join_region::try_native_enumeration(&binding.root, ctx.memo(), &state, &facts)?
                }
                _ => Vec::new(),
            }
        };
        let (
            plan,
            source_stats,
            source_region,
            enclosing_required_region_facets,
            source_runtime_filter_facet,
            source_input_context,
            source_child_context,
            source_output_columns,
            mut nested_group_holes,
            environment,
        ) = {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            let (plan, nested_group_holes) = if direct_native.is_empty() {
                let Some(instantiated) = semantic_plan::instantiate_bound_plan_with_group_holes(
                    ctx.memo(),
                    &state,
                    &binding.root,
                    Some(&facts),
                )?
                else {
                    return Ok(Box::new([]));
                };
                (Some(instantiated.plan), instantiated.group_holes)
            } else {
                (None, BTreeMap::new())
            };
            let logical = ctx
                .memo()
                .logical_expr(expr)
                .ok_or_else(|| paro_error::internal("planner rule lost its source expression"))?;
            let metadata = state
                .metadata
                .get(&logical.payload)
                .ok_or_else(|| paro_error::internal("planner rule lost its source metadata"))?;
            let payload = state
                .payloads
                .logical
                .get(logical.payload.index())
                .ok_or_else(|| paro_error::internal("planner rule lost its source payload"))?;
            let enclosing_required_region_facets = ctx
                .memo()
                .optimization_context(metadata.child_context)
                .ok_or_else(|| {
                    paro_error::internal("planner expression has an unknown child context")
                })?
                .required_region_facets()
                .to_vec();
            (
                plan,
                payload.column_stats.clone(),
                metadata
                    .required_region_facet
                    .map(|facet| (facet, metadata.operator_type)),
                enclosing_required_region_facets,
                metadata.runtime_filter_region_facet,
                metadata.input_context,
                metadata.child_context,
                metadata.output_columns.clone(),
                nested_group_holes,
                PlannerRuleEnvironment {
                    control: ctx.memo().control().clone(),
                    bind_context: state.bind_context.clone(),
                    session: state.session.clone().ok_or_else(|| {
                        paro_error::internal("planner rule has no statement context")
                    })?,
                    cost_model: state.cost_model.clone(),
                    budget: ctx.memo().budget().clone(),
                    verify_enabled: state.verify_enabled,
                },
            )
        };
        let mut cte_restriction = None;
        let plans = if let Some(plan) = plan {
            if matches!(
                self.transformation,
                PlannerTransformation::CteInline
                    | PlannerTransformation::CteFilterPushdown
                    | PlannerTransformation::CtePartitionedMaterialization
                    | PlannerTransformation::CteDemandPushdown
            ) {
                let state = self
                    .planner_state
                    .read()
                    .expect("planner transform state poisoned");
                let requirement = cte::CteRequirement::from_binding(binding, ctx.memo(), &state)?;
                debug!(target: targets::OPTIMIZER, owner = requirement.owner.index(), producer = requirement.producer.index(), base_producer = requirement.base_producer.index(), "bound native CTE requirement");
                if requirement.owner != ctx.memo().canonical_group(target_group)
                    || requirement.sharing_owner != source_region.map(|(facet, _)| facet)
                {
                    return Err(paro_error::internal("CTE inline changed sharing ownership"));
                }
                if matches!(self.transformation, PlannerTransformation::CteInline) {
                    drop(state);
                    requirement
                        .inline(plan, &mut nested_group_holes, &environment.bind_context)?
                        .into_iter()
                        .collect()
                } else if matches!(
                    self.transformation,
                    PlannerTransformation::CtePartitionedMaterialization
                ) {
                    drop(state);
                    let mut state = self
                        .planner_state
                        .write()
                        .expect("planner transform state poisoned");
                    let (cte_partition_labels, staging_arena) = state.cte_partition_state_mut();
                    requirement.partitions(
                        plan,
                        &mut nested_group_holes,
                        &environment.bind_context,
                        cte_partition_labels,
                        staging_arena,
                    )?
                } else {
                    let restricted = if matches!(
                        self.transformation,
                        PlannerTransformation::CteDemandPushdown
                    ) {
                        requirement.restrict_key_domain(
                            plan,
                            &mut nested_group_holes,
                            ctx.memo(),
                            &state,
                            &facts,
                        )?
                    } else {
                        requirement.restrict_predicate_domain(plan, ctx.memo(), &state)?
                    };
                    if let Some((plan, proof)) = restricted {
                        drop(state);
                        let plan = requirement.close_domain(
                            plan,
                            &proof,
                            ctx.memo(),
                            &mut self
                                .planner_state
                                .write()
                                .expect("planner transform state poisoned"),
                        )?;
                        cte_restriction = Some((requirement.producer, proof));
                        vec![plan]
                    } else {
                        Vec::new()
                    }
                }
            } else {
                rewrite_planner_expressions(
                    self.transformation,
                    plan,
                    source_stats.as_ref(),
                    &environment,
                )?
            }
        } else {
            Vec::new()
        };
        if plans.is_empty() && direct_native.is_empty() {
            return Ok(Box::new([]));
        }

        // These rules produce a bounded shell whose leaves are opaque Memo
        // operands. Re-settling that shell only to turn it back into
        // GroupId/ScalarExprId edges is redundant. Keep the shell in the
        // session arena and let the native staging pass consume it. Rules
        // which introduce executable leaves or CTE ownership changes retain
        // the full settlement path below.
        enum PreparedPlan {
            Native {
                shell: NativeShell,
                column_stats: SharedColumnStatistics,
            },
            Settled {
                plan: paro_planner::plan::arena::PlanIndex,
                column_stats: SharedColumnStatistics,
                scopes: HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
            },
        }

        struct PreparedAlternative {
            plan: PreparedPlan,
            preserved_region_facet: Option<Fingerprint>,
            extended_required_region_facets: Box<[Fingerprint]>,
            input_context: OptimizationContextId,
            child_context: OptimizationContextId,
            nested_group_holes: BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
        }

        let use_native_shell = matches!(
            self.transformation,
            PlannerTransformation::PredicateTransfer
                | PlannerTransformation::JoinRegionEnumeration
                | PlannerTransformation::AggregateDimensionDeferral
                | PlannerTransformation::AggregateJoinSubsumption
        );
        enum PlanCandidate {
            Native(NativeShell),
            Owned(OwnedLogicalPlan),
        }

        let mut candidates = Vec::with_capacity(plans.len() + direct_native.len());
        candidates.extend(direct_native.into_iter().map(PlanCandidate::Native));
        candidates.extend(plans.into_iter().map(PlanCandidate::Owned));

        let mut prepared = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let (prepared_plan, root_operator, output_layout, retained_group_holes) =
                match candidate {
                    PlanCandidate::Native(shell) => {
                        let root_operator = shell.root_operator().op_type();
                        let output_layout = shell.root_layout()?;
                        (
                            PreparedPlan::Native {
                                shell,
                                column_stats: source_stats.clone(),
                            },
                            root_operator,
                            output_layout,
                            BTreeMap::new(),
                        )
                    }
                    PlanCandidate::Owned(plan) => {
                        let retained_group_holes =
                            retained_group_holes(&plan, &nested_group_holes)?;
                        let group_hole_guard = GroupHoleTransportGuard::capture(
                            &plan,
                            retained_group_holes.keys().copied(),
                            &environment.bind_context,
                        )?;
                        let use_native_plan = use_native_shell && native_shell_is_closed(&plan);
                        if use_native_plan {
                            let plan = refresh_native_shell_statistics(
                                plan,
                                source_stats.as_ref(),
                                &environment,
                            );
                            let root_operator = plan.operator.op_type();
                            let output_layout = plan.output_layout();
                            group_hole_guard.validate_owned(&plan)?;
                            let shell = NativeShell::from_owned(plan)?;
                            (
                                PreparedPlan::Native {
                                    shell,
                                    column_stats: source_stats.clone(),
                                },
                                root_operator,
                                output_layout,
                                retained_group_holes,
                            )
                        } else {
                            let settled = {
                                let mut planner_state = self
                                    .planner_state
                                    .write()
                                    .expect("planner transform state poisoned");
                                settle_with_session_arena(&mut planner_state, plan, &environment)?
                            };
                            let Some(settlement::SettledExpression {
                                plan,
                                statistics: column_stats,
                                scopes,
                            }) = settled
                            else {
                                return Ok(Box::new([]));
                            };
                            let mut state = self
                                .planner_state
                                .write()
                                .expect("planner transform state poisoned");
                            let view = state.staging_arena.plan(plan)?;
                            group_hole_guard.validate_arena(&view)?;
                            if environment.verify_enabled {
                                crate::verify::verify_arena_plan(&view, || {
                                    environment.session.cancellation.check()
                                })?;
                            }
                            // The target Memo group owns the output contract. Settlement
                            // may legitimately widen child carriers for predicates and
                            // ordering, but the transformed root must be frozen back to
                            // the group's exact binding layout before staging.
                            let plan = semantic_plan::freeze_arena_output_layout(
                                plan,
                                &source_output_columns,
                                &mut state,
                            )?;
                            let root_operator = state.staging_arena.get(plan)?.operator.op_type();
                            let output_layout = state.staging_arena.output_layout(plan)?.clone();
                            (
                                PreparedPlan::Settled {
                                    plan,
                                    column_stats,
                                    scopes,
                                },
                                root_operator,
                                output_layout,
                                retained_group_holes,
                            )
                        }
                    }
                };
            let mut preserved_region_facet = None;
            let mut extended_required_region_facets = enclosing_required_region_facets.clone();
            let output_input_context = source_input_context;
            let mut output_child_context = source_child_context;
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
                        paro_error::internal(
                            "planner rule references an unknown required region facet",
                        )
                    })?;
                let discharges_sharing =
                    matches!(self.transformation, PlannerTransformation::CteInline)
                        && kind == RegionFacetKind::Sharing
                        && root_operator != source_operator;
                let preserves_sharing = matches!(
                    self.transformation,
                    PlannerTransformation::CtePartitionedMaterialization
                        | PlannerTransformation::CteInline
                        | PlannerTransformation::CteDemandPushdown
                        | PlannerTransformation::CteFilterPushdown
                ) && kind == RegionFacetKind::Sharing
                    && root_operator == source_operator;
                if preserves_sharing {
                    preserved_region_facet = Some(facet);
                } else if discharges_sharing {
                    extended_required_region_facets.retain(|candidate| *candidate != facet);
                    output_child_context = source_input_context;
                } else {
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
            if !transformed_layout_matches_group_contract(
                &output_layout,
                target_group,
                ctx.memo(),
                &self
                    .planner_state
                    .read()
                    .expect("planner transform state poisoned"),
            )? {
                debug!(
                    target: targets::OPTIMIZER,
                    rule = self.id().0,
                    group = target_group.index(),
                    output_bindings = ?output_layout.bindings(),
                    output_types = ?output_layout.types(),
                    target_schema = ?ctx.memo().group(target_group).map(|group| &group.schema),
                    "discarded optional transformation before staging an incompatible root contract"
                );
                continue;
            }
            prepared.push(PreparedAlternative {
                plan: prepared_plan,
                preserved_region_facet,
                extended_required_region_facets: extended_required_region_facets.into_boxed_slice(),
                input_context: output_input_context,
                child_context: output_child_context,
                nested_group_holes: retained_group_holes,
            });
        }
        if prepared.is_empty() {
            return Ok(Box::new([]));
        }
        let staged = ctx.with_sidecar_transaction(
            self.planner_state.clone(),
            PlannerTransformState::savepoint,
            PlannerTransformState::rollback_to,
            |memo, state| {
                let mut staged = Vec::with_capacity(prepared.len());
                for prepared in prepared {
                    let PreparedAlternative {
                        plan,
                        preserved_region_facet,
                        extended_required_region_facets,
                        input_context,
                        child_context,
                        nested_group_holes,
                    } = prepared;
                    let (plan, column_stats, column_stat_scopes) = match plan {
                        PreparedPlan::Native {
                            shell,
                            column_stats,
                        } => (StagingInput::Native(shell), column_stats, HashMap::new()),
                        PreparedPlan::Settled {
                            plan,
                            column_stats,
                            scopes,
                        } => (StagingInput::Arena(plan), column_stats, scopes),
                    };
                    let Some(expression) = stage_transformed_expression(
                        StagingRequest {
                            input: plan,
                            input_facts: facts.clone(),
                            column_stats,
                            column_stat_scopes,
                            target: StagingTarget {
                                group: target_group,
                                rule: self.id(),
                                budget_class: self.budget_class(),
                                input_context,
                                child_context,
                                refined_cardinality_kind: self
                                    .transformation
                                    .cardinality_recipe_kind(),
                            },
                            regions: StagingRegionRequirements {
                                preserved_facet: preserved_region_facet,
                                extended_required_facets: extended_required_region_facets,
                                inherited_runtime_filter_facet: source_runtime_filter_facet,
                            },
                            nested_group_holes,
                        },
                        memo,
                        state,
                    )?
                    else {
                        return Ok(None);
                    };
                    if let Some((input, proof)) = &cte_restriction {
                        let producer = *expression.key.children.first().ok_or_else(|| {
                            paro_error::internal("restricted CTE lost its producer group")
                        })?;
                        state.cte_restrictions.push(cte::CteRestriction {
                            producer,
                            input: *input,
                            proof: proof.clone(),
                        });
                    }
                    staged.push(expression);
                }
                if let Some(key) = region_identity {
                    if state.enumerated_join_regions.insert(key.clone()) {
                        state.join_region_insertions.push(key);
                    }
                }
                Ok(Some(staged))
            },
        )?;
        let Some(staged) = staged else {
            // An expected occurrence-context collision is an advisory miss.
            // Returning no outputs delegates the already-enlisted Memo and
            // sidecar rollback to the common transformation transaction.
            return Ok(Box::new([]));
        };
        let source = ctx
            .memo()
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("planner rule lost its source expression"))?;
        let premise = source.key.stable_fingerprint();
        Ok(staged
            .into_iter()
            .map(|staged| EquivalentExpression {
                target_group,
                key: staged.key,
                payload: staged.payload,
                operator_encoding: Some(staged.operator_encoding),
                logical_properties: staged.logical_properties,
                cardinality: staged.cardinality,
                proof: EquivalenceProof::Transformation {
                    rule: self.id(),
                    source: expr,
                    premise,
                },
            })
            .collect())
    }
}

/// A bounded region enumerator already emits its complete candidate frontier
/// for one exact input binding. Its output trees are equivalent results, not
/// fresh region seeds. Re-enumerating those trees recursively turns a bounded
/// `N`-candidate enumeration into an `N^depth` closure without exposing a new
/// semantic input.
///
/// Initial expressions and expressions independently produced by another
/// rule remain seeds. Their pattern read cursors still wake when a child
/// frontier or fact changes, so this guard removes only enumerator self-feed.
fn expression_is_only_rule_output(expr: &crate::cascades::memo::LogicalExpr, rule: RuleId) -> bool {
    proofs_are_only_rule_output(&expr.proofs, rule)
}

fn proofs_are_only_rule_output(proofs: &BTreeSet<EquivalenceProof>, rule: RuleId) -> bool {
    let mut produced_by_rule = false;
    for proof in proofs {
        match proof {
            EquivalenceProof::Transformation {
                rule: proof_rule, ..
            }
            | EquivalenceProof::SpecializedEnumerator {
                rule: proof_rule, ..
            }
            | EquivalenceProof::TransformationDescendant { rule: proof_rule }
                if *proof_rule == rule =>
            {
                produced_by_rule = true
            }
            EquivalenceProof::Normalization { rule: proof_rule } if *proof_rule == rule => {
                produced_by_rule = true
            }
            EquivalenceProof::Initial
            | EquivalenceProof::TransformationDescendant { .. }
            | EquivalenceProof::Normalization { .. }
            | EquivalenceProof::Transformation { .. }
            | EquivalenceProof::SpecializedEnumerator { .. } => return false,
        }
    }
    produced_by_rule
}

fn transformed_layout_matches_group_contract(
    layout: &paro_planner::operator::LogicalOutputLayout,
    target: GroupId,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<bool> {
    let bindings = layout.bindings().to_vec();
    let types = layout.types().to_vec();
    if bindings.len() != types.len() {
        return Ok(false);
    }
    let mut columns = BTreeSet::new();
    for (binding, logical_type) in bindings.into_iter().zip(types) {
        let Some(column) = state
            .binding_ids
            .get(binding.table_index, binding.column_index, &logical_type)
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
    control: Arc<super::super::control::SearchControl>,
    bind_context: BindContext,
    session: Arc<paro_context::StatementContext>,
    cost_model: crate::cost_model::CostModel,
    budget: SearchBudget,
    verify_enabled: bool,
}

/// Apply the part of PredicateTransfer that only needs the matched operator
/// shell.  A pattern binding already contains the exact join/filter operators
/// and Memo group holes, so routing a side-local predicate does not require an
/// `OwnedLogicalPlan` or a representative subtree.
///
/// This deliberately implements the conservative side-local subset first:
/// predicates that reference one side of an INNER/CROSS join are safe to move
/// below that join; mixed-side, constant, fenced, and outer-join predicates
/// stay on the legacy path.  Declining those cases is important because a
/// native fast path must not turn missing semantic coverage into an accepted
/// no-op.
fn try_native_predicate_transfer(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some(shell) = NativeShell::from_pattern(memo, state, binding, facts)? else {
        return Ok(None);
    };
    let root = shell.root;
    let LogicalOperator::Filter(filter) = shell.nodes[root].operator.clone() else {
        return Ok(None);
    };
    let NativeChild::Node(join_index) = filter.child.clone() else {
        return Ok(None);
    };
    let join_operator = shell
        .nodes
        .get(join_index)
        .ok_or_else(|| paro_error::internal("native predicate shell lost its join child"))?
        .operator
        .clone();
    let (left, right, inner) = match join_operator.clone() {
        LogicalOperator::Join(Join::Comparison(join))
            if join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped
                && !crate::expression::comparison_join_has_evaluation_fence(&join) =>
        {
            (join.left, join.right, true)
        }
        LogicalOperator::Join(Join::Cross(join)) => (join.left, join.right, true),
        _ => return Ok(None),
    };
    if !inner {
        return Ok(None);
    }
    if filter
        .expressions
        .iter()
        .any(|expression| expression.evaluation_properties().is_reorder_fence())
    {
        return Ok(None);
    }

    let layouts = shell.layouts()?;
    let child_layout =
        |child: &NativeChild| -> Result<paro_planner::operator::LogicalOutputLayout> {
            match child {
                NativeChild::Node(index) => layouts.get(*index).cloned().ok_or_else(|| {
                    paro_error::internal("native predicate shell referenced an unknown node")
                }),
                NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
                    Ok(layout.clone())
                }
            }
        };
    let left_tables = child_layout(&left)?
        .bindings()
        .iter()
        .map(|binding| binding.table_index)
        .collect::<BTreeSet<_>>();
    let right_tables = child_layout(&right)?
        .bindings()
        .iter()
        .map(|binding| binding.table_index)
        .collect::<BTreeSet<_>>();
    let mut left_filters = Vec::new();
    let mut right_filters = Vec::new();
    let mut remaining = Vec::new();
    for expression in filter.expressions {
        if expression.evaluation_properties().is_reorder_fence() {
            remaining.push(expression);
            continue;
        }
        let mut tables = BTreeSet::new();
        crate::expression::traversal::visit_expression(&expression, &mut |candidate| {
            if let Expression::ColumnRef(column) = candidate {
                tables.insert(column.binding.table_index);
            }
        });
        if !tables.is_empty() && tables.is_subset(&left_tables) && tables.is_disjoint(&right_tables)
        {
            left_filters.push(expression);
        } else if !tables.is_empty()
            && tables.is_subset(&right_tables)
            && tables.is_disjoint(&left_tables)
        {
            right_filters.push(expression);
        } else {
            remaining.push(expression);
        }
    }
    if left_filters.is_empty() && right_filters.is_empty() {
        return Ok(None);
    }

    let join_stats = shell
        .nodes
        .get(join_index)
        .map(|node| node.stats.clone())
        .unwrap_or_default();
    let mut nodes = shell.nodes.into_vec();
    let add_filter =
        |nodes: &mut Vec<NativeNode>, child: NativeChild, expressions: Vec<Expression>| {
            let index = nodes.len();
            nodes.push(NativeNode {
                id: state.bind_context.next_plan_id(),
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                    expressions,
                    child,
                    projection_map: paro_planner::operator::ProjectionMap::all(),
                }),
            });
            index
        };
    let left = if left_filters.is_empty() {
        left
    } else {
        NativeChild::Node(add_filter(&mut nodes, left, left_filters))
    };
    let right = if right_filters.is_empty() {
        right
    } else {
        NativeChild::Node(add_filter(&mut nodes, right, right_filters))
    };
    let join = match join_operator {
        LogicalOperator::Join(Join::Comparison(mut join)) => {
            join.left = left;
            join.right = right;
            LogicalOperator::Join(Join::Comparison(join))
        }
        LogicalOperator::Join(Join::Cross(mut join)) => {
            join.left = left;
            join.right = right;
            LogicalOperator::Join(Join::Cross(join))
        }
        _ => return Ok(None),
    };
    let joined = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: join_stats,
        operator: join,
    });
    let root = if remaining.is_empty()
        && filter
            .projection_map
            .is_identity(layouts.get(join_index).map_or(0, |layout| layout.len()))
    {
        joined
    } else {
        let root = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: NodeStats::default(),
            operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                expressions: remaining,
                child: NativeChild::Node(joined),
                projection_map: filter.projection_map,
            }),
        });
        root
    };
    compact_native_shell(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })
    .map(Some)
}

fn compact_native_shell(shell: NativeShell) -> Result<NativeShell> {
    fn visit(
        index: usize,
        nodes: &[NativeNode],
        marks: &mut [u8],
        order: &mut Vec<usize>,
    ) -> Result<()> {
        let node = nodes
            .get(index)
            .ok_or_else(|| paro_error::internal("native shell references an unknown node"))?;
        match marks[index] {
            1 => return Err(paro_error::internal("native shell contains a child cycle")),
            2 => return Ok(()),
            _ => {}
        }
        marks[index] = 1;
        let mut children = Vec::new();
        node.operator
            .visit_child_links(&mut |child| children.push(child));
        for child in children {
            if let NativeChild::Node(index) = child {
                visit(*index, nodes, marks, order)?;
            }
        }
        marks[index] = 2;
        order.push(index);
        Ok(())
    }

    if shell.nodes.is_empty() || shell.root >= shell.nodes.len() {
        return Err(paro_error::internal("native shell has no compactable root"));
    }
    let root = shell.root;
    let nodes = shell.nodes.into_vec();
    let mut marks = vec![0_u8; nodes.len()];
    let mut order = Vec::new();
    visit(root, &nodes, &mut marks, &mut order)?;
    let mut remap = vec![usize::MAX; nodes.len()];
    let mut compacted = Vec::with_capacity(order.len());
    for old_index in order {
        let node = &nodes[old_index];
        let operator = node
            .operator
            .clone()
            .try_map_child_links(&mut |child| {
                Ok::<_, std::convert::Infallible>(match child {
                    NativeChild::Node(index) => NativeChild::Node(remap[index]),
                    other => other,
                })
            })
            .expect("native shell child remapping cannot fail");
        remap[old_index] = compacted.len();
        compacted.push(NativeNode {
            id: node.id,
            stats: node.stats.clone(),
            operator,
        });
    }
    Ok(NativeShell {
        nodes: compacted.into_boxed_slice(),
        root: remap[root],
    })
}

/// Recompute facts for one native transformation shell without entering the
/// general settlement cache.  Native rule outputs are closed over
/// BoundReference children, so a single local propagation/gather pass is
/// sufficient; walking and re-importing the same shell through a second arena
/// would only add ownership traffic.  The child references remain immutable
/// Memo contracts and are never exported as owned descendants.
fn refresh_native_shell_statistics(
    plan: OwnedLogicalPlan,
    source_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    environment: &PlannerRuleEnvironment,
) -> OwnedLogicalPlan {
    let children = plan.children();
    let child_layouts = children
        .iter()
        .map(|child| child.output_layout())
        .collect::<Vec<_>>();
    let child_maximum_cardinalities = children
        .iter()
        .map(|child| match &child.operator {
            LogicalOperator::BoundReference(reference) => reference.facts.maximum_cardinality,
            _ => child
                .stats
                .estimated_cardinality
                .map(|estimate| estimate.max),
        })
        .collect::<Vec<_>>();
    let mut context = crate::context::OptimizationContext::new(
        environment.session.clone(),
        environment.bind_context.clone(),
    );
    context.cost_model = environment.cost_model.clone();
    for child in &children {
        match &child.operator {
            LogicalOperator::BoundReference(reference) => {
                for (binding, statistics) in reference
                    .bindings
                    .iter()
                    .copied()
                    .zip(reference.column_statistics())
                {
                    context.column_stats_mut().insert(binding, statistics);
                }
            }
            _ => {
                for binding in child.get_column_bindings() {
                    if let Some(statistics) = source_stats.get(&binding) {
                        context
                            .column_stats_mut()
                            .insert(binding, statistics.clone());
                    }
                }
            }
        }
    }
    let input_column_stats = context.column_stats.clone();
    let mut propagator =
        StatisticsPropagator::with_statistics_map(context.column_stats.as_ref().clone());
    let plan = plan.map_operator(|operator| {
        propagator.propagate_operator(environment.session.as_ref(), operator)
    });
    context.column_stats = Arc::new(propagator.take_statistics_map());
    let mut gathering = StatisticsGathering::new();
    let (plan, _output, _maximum) = gathering.gather_local(
        plan,
        &child_layouts,
        &child_maximum_cardinalities,
        input_column_stats,
        &mut context,
    );
    plan
}

/// A native shell may contain transparent operator structure and native scalar
/// operands, but every leaf must remain a Memo group boundary. In particular,
/// never bypass settlement for a shell that still contains a real scan/get:
/// that would turn an owned planner tree into a second source of semantics.
fn native_shell_is_closed(plan: &OwnedLogicalPlan) -> bool {
    if matches!(plan.operator, LogicalOperator::BoundReference(_)) {
        return true;
    }
    let mut children = Vec::new();
    plan.operator
        .visit_child_links(&mut |child| children.push(child));
    !children.is_empty()
        && children
            .into_iter()
            .all(|child| native_shell_is_closed(&child))
}

fn rewrite_planner_expressions(
    transformation: PlannerTransformation,
    plan: OwnedLogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    environment: &PlannerRuleEnvironment,
) -> Result<Vec<OwnedLogicalPlan>> {
    if matches!(transformation, PlannerTransformation::JoinRegionEnumeration) {
        return crate::join_order::optimizer::JoinOrderOptimizer::new(
            environment.cost_model.defaults.clone(),
        )
        .with_search_budget(&environment.budget)
        .enumerate_region(
            environment.session.as_ref(),
            plan,
            column_stats,
            &environment.bind_context,
        );
    }
    Ok(
        rewrite_planner_expression(transformation, plan, column_stats, environment)?
            .into_iter()
            .collect(),
    )
}

fn rewrite_planner_expression(
    transformation: PlannerTransformation,
    plan: OwnedLogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    environment: &PlannerRuleEnvironment,
) -> Result<Option<OwnedLogicalPlan>> {
    let rewritten = match transformation {
        PlannerTransformation::PredicateTransfer => FilterPushdown::new().rewrite_plan(plan),
        PlannerTransformation::KeyDomainTransfer => {
            return crate::filter::domain_transfer::transfer(plan)
        }
        PlannerTransformation::CtePartitionedMaterialization => {
            unreachable!("CTE partitioning consumes a native occurrence requirement")
        }
        PlannerTransformation::CteInline => {
            unreachable!("CTE inlining consumes a native occurrence requirement")
        }
        PlannerTransformation::CteDemandPushdown => {
            unreachable!("CTE key domains consume a native occurrence requirement")
        }
        PlannerTransformation::CteFilterPushdown => {
            unreachable!("CTE filtering consumes a native occurrence requirement")
        }
        PlannerTransformation::JoinRegionEnumeration => {
            unreachable!("join-region enumeration returns a bounded expression frontier")
        }
        PlannerTransformation::AggregatePostReduction => {
            let (plan, changed) =
                post_reduction::optimize_plan_with_change(plan, &environment.bind_context)?;
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::MarkJoinToSemi => {
            let Some(plan) = rewrite_positive_consumed_mark_filter(plan) else {
                return Ok(None);
            };
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
            let (plan, changed) = join_subsumption::optimize_root_with_change(plan);
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
            let (plan, changed) =
                dimension_deferral::optimize_plan(plan, &environment.bind_context)?;
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::AggregateDimensionSharing => {
            let (plan, changed) =
                dimension_sharing::optimize_plan(plan, &environment.bind_context)?;
            if !changed {
                return Ok(None);
            }
            debug!(
                target: targets::OPTIMIZER,
                rule = AGGREGATE_DIMENSION_SHARING_RULE.0,
                "recognized shareable aggregate dimension branches"
            );
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
        PlannerTransformation::TopNIntroduction => {
            if !TopNOptimizer::can_optimize(&plan.operator) {
                return Ok(None);
            }
            TopNOptimizer::new().optimize_plan(plan)
        }
        PlannerTransformation::LimitPushdown => {
            let (plan, changed) = LimitPushdown::new().optimize_plan_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::LatePayloadFetch => {
            let (plan, prefix_changed) = late_payload::rewrite_matched_prefix_node(plan)?;
            let (plan, payload_changed) = late_payload::rewrite_node(
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

/// A positive mark filter is context-sensitive: its own relational output can
/// still expose the always-true marker, while an ancestor may no longer
/// require it. Rewrite the staged subtree here and let the ordinary group
/// contract check accept the smallest ancestor that preserves its full output.
fn rewrite_positive_consumed_mark_filter(plan: OwnedLogicalPlan) -> Option<OwnedLogicalPlan> {
    let mut changed = false;
    let (plan, ()) = plan
        .try_fold_post_order(|plan, _children: Vec<()>| {
            let is_match = matches!(
                &plan.operator,
                LogicalOperator::Filter(filter)
                    if matches!(filter.expressions.as_slice(), [Expression::ColumnRef(marker)]
                        if marker.depth == 0
                            && matches!(&filter.child.operator,
                                LogicalOperator::Join(Join::Comparison(join))
                                    if join.join_type == JoinType::Mark
                                        && join.mark_index.is_some_and(|index| {
                                            marker.binding == ColumnBinding::new(index, 0)
                                        })))
            );
            if !is_match {
                return Ok((plan, ()));
            }
            let (id, stats, operator) = plan.into_parts();
            let LogicalOperator::Filter(filter) = operator else {
                unreachable!("positive mark-filter shape was checked")
            };
            let LogicalOperator::Join(Join::Comparison(mut join)) = (*filter.child).into_operator()
            else {
                unreachable!("positive mark-filter child was checked")
            };
            join.join_type = JoinType::Semi;
            join.mark_index = None;
            join.mark_semantics = paro_planner::operator::MarkJoinSemantics::NotMark;
            join.left_projection_map = paro_planner::operator::ProjectionMap::all();
            join.right_projection_map = paro_planner::operator::ProjectionMap::none();
            changed = true;
            Ok((
                OwnedLogicalPlan {
                    id,
                    stats,
                    operator: LogicalOperator::Join(Join::Comparison(join)),
                },
                (),
            ))
        })
        .ok()?;
    changed.then_some(plan)
}

struct GroupHoleTransportGuard {
    templates: BTreeMap<paro_planner::operator::BoundReferenceId, GroupHoleTransportTemplate>,
}

/// Keep the exact Memo operands that survived a relational rewrite.
///
/// Eliminating an operator may legitimately eliminate one of its opaque
/// inputs. Surviving references must still be registered exactly once; an
/// introduced or duplicated reference is never accepted as equivalent.
fn retained_group_holes(
    plan: &OwnedLogicalPlan,
    available: &BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>,
) -> Result<BTreeMap<paro_planner::operator::BoundReferenceId, GroupId>> {
    let mut retained = BTreeMap::new();
    plan.try_visit_pre_order(|node| {
        let LogicalOperator::BoundReference(reference) = &node.operator else {
            return Ok(());
        };
        let group = available
            .get(&reference.reference_id)
            .copied()
            .ok_or_else(|| {
                paro_error::internal("transformation introduced an unregistered Memo group hole")
            })?;
        if retained.insert(reference.reference_id, group).is_some() {
            return Err(paro_error::internal(
                "transformation duplicated an opaque Memo group hole",
            ));
        }
        Ok(())
    })?;
    Ok(retained)
}

struct GroupHoleTransportTemplate {
    bindings: Vec<ColumnBinding>,
    types: Vec<paro_common::types::LogicalType>,
    facts: Arc<paro_planner::operator::bound_reference::BoundRelationFacts>,
}

impl GroupHoleTransportGuard {
    fn capture(
        plan: &OwnedLogicalPlan,
        hole_ids: impl IntoIterator<Item = paro_planner::operator::BoundReferenceId>,
        _bind_context: &BindContext,
    ) -> Result<Self> {
        let wanted = hole_ids.into_iter().collect::<BTreeSet<_>>();
        let mut templates = BTreeMap::new();
        plan.try_visit_pre_order(|node| {
            let LogicalOperator::BoundReference(reference) = &node.operator else {
                return Ok(());
            };
            if !wanted.contains(&reference.reference_id) {
                return Err(paro_error::internal(
                    "transformation contains an unregistered Memo group hole",
                ));
            }
            if wanted.contains(&reference.reference_id) {
                if templates
                    .insert(
                        reference.reference_id,
                        GroupHoleTransportTemplate {
                            bindings: reference.bindings.clone(),
                            types: reference.types().to_vec(),
                            facts: reference.facts.clone(),
                        },
                    )
                    .is_some()
                {
                    return Err(paro_error::internal(
                        "group-hole transport reused a non-synthetic plan identity",
                    ));
                }
            }
            Ok(())
        })?;
        debug_assert_eq!(templates.len(), wanted.len());
        Ok(Self { templates })
    }

    fn validate_arena(&self, plan: &paro_planner::plan::LogicalPlan) -> Result<()> {
        let mut seen = BTreeSet::new();
        for index in plan.arena().post_order(plan.root())? {
            let node = plan.arena().get(index)?;
            let LogicalOperator::BoundReference(reference) = &node.operator else {
                continue;
            };
            let template = self.templates.get(&reference.reference_id).ok_or_else(|| {
                paro_error::internal("settlement introduced an unregistered Memo group hole")
            })?;
            if !seen.insert(reference.reference_id) {
                return Err(paro_error::internal(
                    "optimizer duplicated an opaque Memo group-hole occurrence",
                ));
            }
            if reference.bindings != template.bindings
                || reference.types() != template.types
                || reference.facts != template.facts
            {
                return Err(paro_error::internal(
                    "settlement changed an immutable Memo boundary",
                ));
            }
        }
        if seen.len() != self.templates.len() {
            return Err(paro_error::internal(
                "settlement removed a protected Memo group hole",
            ));
        }
        Ok(())
    }

    fn validate_owned(&self, plan: &OwnedLogicalPlan) -> Result<()> {
        let mut seen = BTreeSet::new();
        plan.try_visit_pre_order(|node| {
            let LogicalOperator::BoundReference(reference) = &node.operator else {
                return Ok(());
            };
            let template = self.templates.get(&reference.reference_id).ok_or_else(|| {
                paro_error::internal("native staging introduced an unregistered Memo group hole")
            })?;
            if !seen.insert(reference.reference_id) {
                return Err(paro_error::internal(
                    "native staging duplicated an opaque Memo group-hole occurrence",
                ));
            }
            if reference.bindings != template.bindings
                || reference.types() != template.types
                || reference.facts != template.facts
            {
                return Err(paro_error::internal(
                    "native transformation changed an immutable Memo boundary",
                ));
            }
            Ok(())
        })?;
        if seen.len() != self.templates.len() {
            return Err(paro_error::internal(
                "native transformation removed a protected Memo group hole",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enumerator_outputs_do_not_feed_the_same_enumerator() {
        let rule = JOIN_REGION_ENUMERATION_RULE;
        let source = LogicalExprId::new(7);
        let premise = Fingerprint(11);
        let generated = BTreeSet::from([
            EquivalenceProof::Normalization { rule },
            EquivalenceProof::Transformation {
                rule,
                source,
                premise,
            },
        ]);
        assert!(proofs_are_only_rule_output(&generated, rule));

        let initial = BTreeSet::from([
            EquivalenceProof::Initial,
            EquivalenceProof::Transformation {
                rule,
                source,
                premise,
            },
        ]);
        assert!(!proofs_are_only_rule_output(&initial, rule));

        let independently_generated = BTreeSet::from([
            EquivalenceProof::Transformation {
                rule,
                source,
                premise,
            },
            EquivalenceProof::Transformation {
                rule: AGGREGATE_POST_REDUCTION_RULE,
                source,
                premise,
            },
        ]);
        assert!(!proofs_are_only_rule_output(&independently_generated, rule));

        let normalized_by_another_rule = BTreeSet::from([
            EquivalenceProof::Normalization {
                rule: AGGREGATE_POST_REDUCTION_RULE,
            },
            EquivalenceProof::Transformation {
                rule,
                source,
                premise,
            },
        ]);
        assert!(!proofs_are_only_rule_output(
            &normalized_by_another_rule,
            rule
        ));
    }
}
