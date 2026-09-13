// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical equivalence rules and transactional Memo staging.

use super::*;
use smallvec::SmallVec;
use std::collections::HashSet;

use paro_common::runtime_value::Value;
use paro_planner::operator::{Aggregate, Filter, Projection, SetOpType, SetOperation};

pub(super) mod cte;
mod join_region;
mod matching;
mod native_domain;
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

    fn quality_dependency(&self) -> Option<QualityDependency> {
        match self.transformation {
            // These rules publish the consumer-side demand/domain facts that
            // the producer and aggregate stages consume.  They are semantic
            // dependencies, not merely a higher numeric promise.
            PlannerTransformation::CteDemandPushdown => Some(QualityDependency::ConsumerDemand),
            PlannerTransformation::CteFilterPushdown
            | PlannerTransformation::PredicateTransfer
            | PlannerTransformation::KeyDomainTransfer => {
                Some(QualityDependency::DomainRestriction)
            }
            PlannerTransformation::CtePartitionedMaterialization => {
                Some(QualityDependency::ProducerDomain)
            }
            PlannerTransformation::AggregatePostReduction
            | PlannerTransformation::AggregateJoinPreaggregation
            | PlannerTransformation::AggregateJoinSubsumption
            | PlannerTransformation::AggregateNonNullInput
            | PlannerTransformation::AggregateDimensionDeferral
            | PlannerTransformation::AggregateInputMaterialization => {
                Some(QualityDependency::NarrowAggregate)
            }
            PlannerTransformation::AggregateDimensionSharing => {
                Some(QualityDependency::DimensionMerge)
            }
            PlannerTransformation::JoinRegionEnumeration => Some(QualityDependency::JoinSelection),
            // Inlining and the remaining rewrites are valid alternatives, but
            // they do not by themselves publish the quality-chain contract.
            PlannerTransformation::CteInline
            | PlannerTransformation::MarkJoinToSemi
            | PlannerTransformation::JoinElimination
            | PlannerTransformation::TopNIntroduction
            | PlannerTransformation::LimitPushdown
            | PlannerTransformation::LatePayloadFetch
            | PlannerTransformation::ScalarAggregateWindow => None,
        }
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

    fn selected_quality_bindings(
        &self,
        memo: &Memo,
        candidate: &FrozenCandidate,
    ) -> Result<Box<[PatternBinding]>> {
        if !matches!(
            self.transformation,
            PlannerTransformation::PredicateTransfer
        ) {
            return Ok(Box::new([]));
        }
        let state = self
            .planner_state
            .read()
            .map_err(|_| paro_error::internal("planner transform state poisoned"))?;
        Ok(super::quality_domain::selected_transfer_bindings(
            memo, candidate, &state,
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

    fn root_operator_tag_may_match(&self, operator_tag: Option<u64>) -> bool {
        let Some(operator_tag) = operator_tag else {
            // Untagged core Memo expressions are outside the planner's
            // accelerator contract. Let the authoritative metadata/root
            // dispatch decide their applicability.
            return true;
        };
        use paro_planner::operator::LogicalOperatorType as Op;
        let matches = |operator| operator_tag == super::identity::operator_tag(operator);
        match self.transformation {
            PlannerTransformation::KeyDomainTransfer
            | PlannerTransformation::JoinRegionEnumeration => matches(Op::ComparisonJoin),
            PlannerTransformation::CtePartitionedMaterialization
            | PlannerTransformation::CteInline
            | PlannerTransformation::CteDemandPushdown
            | PlannerTransformation::CteFilterPushdown => matches(Op::MaterializedCTE),
            PlannerTransformation::PredicateTransfer => matches(Op::Filter),
            PlannerTransformation::AggregatePostReduction => {
                matches(Op::MaterializedCTE) || matches(Op::Projection) || matches(Op::Filter)
            }
            PlannerTransformation::MarkJoinToSemi => matches(Op::Projection) || matches(Op::Filter),
            PlannerTransformation::JoinElimination => {
                matches(Op::Projection)
                    || matches(Op::Filter)
                    || matches(Op::Aggregate)
                    || matches(Op::Limit)
                    || matches(Op::Order)
                    || matches(Op::TopN)
            }
            PlannerTransformation::AggregateJoinPreaggregation
            | PlannerTransformation::AggregateJoinSubsumption
            | PlannerTransformation::AggregateNonNullInput
            | PlannerTransformation::AggregateDimensionDeferral
            | PlannerTransformation::AggregateInputMaterialization => matches(Op::Aggregate),
            PlannerTransformation::AggregateDimensionSharing => matches(Op::LogicalUnion),
            PlannerTransformation::TopNIntroduction | PlannerTransformation::LimitPushdown => {
                matches(Op::Limit)
            }
            PlannerTransformation::LatePayloadFetch => matches(Op::Projection) || matches(Op::TopN),
            PlannerTransformation::ScalarAggregateWindow => {
                matches(Op::ComparisonJoin) || matches(Op::Projection) || matches(Op::Filter)
            }
        }
    }

    fn root_dispatch(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        ctx: &RuleContext<'_>,
    ) -> Result<RootDispatch> {
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::Match);
        let state = self
            .planner_state
            .read()
            .map_err(|_| paro_error::internal("planner transform state poisoned"))?;
        if state.binder.is_none()
            || !matching::matches_transformation_root(self.transformation, expr, &state)
        {
            return Ok(RootDispatch::default());
        }
        if matches!(
            self.transformation,
            PlannerTransformation::AggregateDimensionSharing
        ) {
            return matching::dimension_sharing_root_dispatch(ctx.group, expr.id, ctx.memo, &state);
        }
        if let Some(reads) = matching::cached_negative_root_reads(
            self.transformation,
            ctx.group,
            expr.id,
            ctx.memo,
            &state,
        )? {
            return Ok(RootDispatch {
                matches: false,
                reads,
            });
        }
        Ok(RootDispatch {
            matches: true,
            reads: Box::new([]),
        })
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
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::Match);
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
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::Apply);
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
                crate::transformation_rejection::reject::<()>(&mut ctx.rejection_reasons, crate::transformation_rejection::TransformationRejectionGuard::BoundaryUnavailable);
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
        // These search rules can consume the exact matched shell directly
        // for their conservative native subsets. Keep the legacy owned-plan
        // path available for every shape that needs richer semantic handling.
        let mut native_domain_scopes = None;
        let direct_native = if matches!(
            self.transformation,
            PlannerTransformation::PredicateTransfer
                | PlannerTransformation::JoinRegionEnumeration
                | PlannerTransformation::AggregateDimensionDeferral
                | PlannerTransformation::AggregateDimensionSharing
        ) {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            match self.transformation {
                PlannerTransformation::PredicateTransfer => {
                    let domain = match native_domain::try_transfer(
                        &binding.root,
                        ctx.memo(),
                        &state,
                        &facts,
                    )? {
                        Some(shell) => {
                            native_domain::refresh_statistics(shell, &state, ctx.memo())?
                        }
                        None => None,
                    };
                    if let Some((shell, scopes)) = domain {
                        native_domain_scopes = Some(scopes);
                        vec![shell]
                    } else {
                        try_native_predicate_transfer(&binding.root, ctx.memo(), &state, &facts)?
                            .into_iter()
                            .collect()
                    }
                }
                PlannerTransformation::JoinRegionEnumeration => {
                    join_region::try_native_enumeration(&binding.root, ctx.memo(), &state, &facts)?
                }
                PlannerTransformation::AggregateDimensionDeferral => {
                    try_native_dimension_deferral(&binding.root, ctx.memo(), &state, &facts)?
                        .into_iter()
                        .collect()
                }
                PlannerTransformation::AggregateDimensionSharing => {
                    try_native_dimension_sharing(&binding.root, ctx.memo(), &state, &facts)?
                        .into_iter()
                        .collect()
                }
                _ => unreachable!("native dispatch guard changed"),
            }
        } else {
            Vec::new()
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
            selected_proofs,
            environment,
        ) = {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            // Native producers are deliberately conservative.  A non-empty
            // native result is not a completeness proof: it may cover only a
            // side-local predicate subset or only the reorderable part of a
            // join region.  Always retain the semantic rewrite as a peer
            // candidate so a partial native result cannot hide a valid
            // alternative from the Memo search.
            // JoinRegion's native enumerator has already validated a closed,
            // reorderable graph and emitted every bounded final plan.  Do not
            // first materialize the same binding as an OwnedLogicalPlan just
            // to convert it back into group references below.  Other native
            // paths (notably PredicateTransfer) may be only a partial
            // semantic subset, so they deliberately keep their owned peer.
            let native_direct_only = (native_domain_scopes.is_some()
                || matches!(
                    self.transformation,
                    PlannerTransformation::JoinRegionEnumeration
                        | PlannerTransformation::AggregateDimensionDeferral
                        | PlannerTransformation::AggregateDimensionSharing
                ))
                && !direct_native.is_empty();
            let (plan, nested_group_holes, selected_proofs) = if native_direct_only {
                (None, BTreeMap::new(), HashMap::new())
            } else {
                let Some(instantiated) = semantic_plan::instantiate_bound_plan_with_group_holes(
                    ctx.memo(),
                    &state,
                    &binding.root,
                    Some(&facts),
                )?
                else {
                    return Ok(Box::new([]));
                };
                (
                    Some(instantiated.plan),
                    instantiated.group_holes,
                    instantiated.selected_proofs,
                )
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
                selected_proofs,
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
                    &mut ctx.rejection_reasons,
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
                scopes: HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
            },
            Settled {
                plan: paro_planner::plan::arena::PlanIndex,
                column_stats: SharedColumnStatistics,
                scopes: HashMap<paro_planner::plan::PlanNodeId, SharedColumnStatistics>,
                selected_proofs: HashMap<paro_planner::plan::PlanNodeId, Box<[EquivalenceProof]>>,
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

        let use_native_shell = native_shell_staging_allowed(self.transformation);
        enum PlanCandidate {
            Native(NativeShell),
            Owned {
                plan: OwnedLogicalPlan,
                selected_proofs: HashMap<paro_planner::plan::PlanNodeId, Box<[EquivalenceProof]>>,
            },
        }

        let mut candidates = Vec::with_capacity(plans.len() + direct_native.len());
        candidates.extend(direct_native.into_iter().map(PlanCandidate::Native));
        candidates.extend(plans.into_iter().map(|plan| PlanCandidate::Owned {
            plan,
            selected_proofs: selected_proofs.clone(),
        }));

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
                                scopes: native_domain_scopes.take().unwrap_or_default(),
                            },
                            root_operator,
                            output_layout,
                            BTreeMap::new(),
                        )
                    }
                    PlanCandidate::Owned {
                        plan,
                        selected_proofs,
                    } => {
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
                            let shell = NativeShell::from_owned(plan, &selected_proofs)?;
                            (
                                PreparedPlan::Native {
                                    shell,
                                    column_stats: source_stats.clone(),
                                    scopes: HashMap::new(),
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
                                    selected_proofs,
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
                    let (plan, column_stats, column_stat_scopes, selected_proofs) = match plan {
                        PreparedPlan::Native {
                            shell,
                            column_stats,
                            scopes,
                        } => (
                            StagingInput::Native(shell),
                            column_stats,
                            scopes,
                            HashMap::new(),
                        ),
                        PreparedPlan::Settled {
                            plan,
                            column_stats,
                            scopes,
                            selected_proofs,
                        } => (
                            StagingInput::Arena(plan),
                            column_stats,
                            scopes,
                            selected_proofs,
                        ),
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
                            selected_proofs,
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

/// Native shell staging is an admission decision, not a generic conversion
/// shortcut.  PredicateTransfer's direct shell is allowed only for the
/// separately proven local subset; its complete semantic peer still needs the
/// settlement path because that path carries producer/consumer ownership and
/// residual predicate facts.  Treating every closed owned tree as a shell was
/// the Q11 quality regression: it preserved an executable plan while dropping
/// the narrow date-domain/partial-aggregate choice.
fn native_shell_staging_allowed(transformation: PlannerTransformation) -> bool {
    matches!(
        transformation,
        PlannerTransformation::JoinRegionEnumeration
            | PlannerTransformation::AggregateDimensionDeferral
            | PlannerTransformation::AggregateJoinSubsumption
            | PlannerTransformation::AggregateDimensionSharing
    )
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
    if !native_predicate_transfer_may_apply(binding, memo, state)? {
        return Ok(None);
    }
    let Some(shell) = NativeShell::from_pattern(memo, state, binding, facts)? else {
        return Ok(None);
    };
    // CTE references carry producer/consumer ownership and demand domains
    // that are not represented by a side-local Filter shell.  Moving a
    // predicate around such a boundary can be row-preserving yet still erase
    // the producer restriction that makes Q11's narrow partial aggregate
    // possible.  Leave those bindings to the CTE-aware semantic path until a
    // native ownership adapter supplies the complete proof.
    if native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
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
    let mut left_tables = child_layout(&left)?
        .bindings()
        .iter()
        .map(|binding| binding.table_index)
        .collect::<SmallVec<[_; 8]>>();
    let mut right_tables = child_layout(&right)?
        .bindings()
        .iter()
        .map(|binding| binding.table_index)
        .collect::<SmallVec<[_; 8]>>();
    left_tables.sort_unstable();
    left_tables.dedup();
    right_tables.sort_unstable();
    right_tables.dedup();
    let mut left_filters = Vec::new();
    let mut right_filters = Vec::new();
    let mut remaining = Vec::new();
    for expression in filter.expressions {
        if expression.evaluation_properties().is_reorder_fence() {
            remaining.push(expression);
            continue;
        }
        let mut tables = SmallVec::<[usize; 4]>::new();
        crate::expression::traversal::visit_expression(&expression, &mut |candidate| {
            if let Expression::ColumnRef(column) = candidate {
                if !tables.contains(&column.binding.table_index) {
                    tables.push(column.binding.table_index);
                }
            }
        });
        tables.sort_unstable();
        if is_side_local_tables(&tables, &left_tables, &right_tables) {
            left_filters.push(expression);
        } else if is_side_local_tables(&tables, &right_tables, &left_tables) {
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
    let child_stats = |nodes: &[NativeNode], child: &NativeChild| -> NodeStats {
        match child {
            NativeChild::Node(index) => nodes
                .get(*index)
                .map(|node| node.stats.clone())
                .unwrap_or_default(),
            NativeChild::MemoGroup { stats, .. } | NativeChild::Group { stats, .. } => {
                stats.clone()
            }
        }
    };
    let add_filter =
        |nodes: &mut Vec<NativeNode>, child: NativeChild, expressions: Vec<Expression>| {
            // A side-local filter may reduce rows, but a native shell has no
            // selectivity proof yet. Carry the child range as a conservative
            // upper/expected estimate instead of defaulting to one row; the
            // latter makes the direct candidate look artificially cheap and
            // can displace the complete legacy rewrite in physical search.
            let stats = child_stats(nodes, &child);
            let index = nodes.len();
            nodes.push(NativeNode {
                id: state.bind_context.next_plan_id(),
                stats,
                operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                    expressions,
                    child,
                    projection_map: paro_planner::operator::ProjectionMap::all(),
                }),
                source_proofs: Box::new([]),
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
        source_proofs: Box::new([]),
    });
    let root = if remaining.is_empty()
        && filter
            .projection_map
            .is_identity(layouts.get(join_index).map_or(0, |layout| layout.len()))
    {
        joined
    } else {
        let root = nodes.len();
        let stats = child_stats(&nodes, &NativeChild::Node(joined));
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats,
            operator: LogicalOperator::Filter(paro_planner::operator::Filter {
                expressions: remaining,
                child: NativeChild::Node(joined),
                projection_map: filter.projection_map,
            }),
            source_proofs: Box::new([]),
        });
        root
    };
    compact_native_shell(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })
    .map(Some)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeDimensionExpressionDomain {
    Constant,
    Fact,
    Dimension,
    Mixed,
    Invalid,
}

#[derive(Debug, Clone, Copy)]
struct NativeDimensionConditionRewrite {
    key_ordinal: usize,
    fact_on_left: bool,
}

#[derive(Debug, Clone)]
enum NativeDeferredOuterGroup {
    Partial { ordinal: usize },
    Dimension(Expression),
}

/// Apply the direct-child subset of AggregateDimensionDeferral without first
/// materializing an OwnedLogicalPlan.  The legacy rule can rotate a complete
/// multiway join region and inline projection spines; those shapes remain on
/// its owned path.  This path is intentionally narrower: one plain aggregate,
/// one plain inner equi-join, a direct Get dimension on the right, and no
/// projection or control boundary.  For that shape the old recognizer is
/// already a local rewrite, so the native result is authoritative and does not
/// need a second semantic peer.
fn try_native_dimension_deferral(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    // The general DimensionRegion matcher intentionally accepts projections,
    // opaque relations, and associative join shapes. Most of those bindings
    // cannot enter this direct-child subset. Reject them from the immutable
    // operator shells before allocating a native node vector; the legacy path
    // will still perform its complete recognizer for every rejected shape.
    if !native_dimension_direct_shape(binding, memo, state)? {
        return Ok(None);
    }
    let Some(shell) = NativeShell::from_pattern(memo, state, binding, facts)? else {
        return Ok(None);
    };
    if native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }
    let layouts = shell.layouts()?;
    let original_root_layout = shell.root_layout()?;
    let LogicalOperator::Aggregate(aggregate) = shell.root_operator().clone() else {
        return Ok(None);
    };
    if !dimension_deferral::root_eligible(&aggregate) {
        return Ok(None);
    }
    let NativeChild::Node(join_index) = aggregate.child.clone() else {
        return Ok(None);
    };
    let join_node = shell
        .nodes
        .get(join_index)
        .ok_or_else(|| paro_error::internal("native dimension shell lost its join child"))?;
    let LogicalOperator::Join(Join::Comparison(join)) = join_node.operator.clone() else {
        return Ok(None);
    };
    if join.join_type != JoinType::Inner
        || join.conditions.is_empty()
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || join.build_side_constraint != paro_planner::operator::JoinBuildSideConstraint::Either
        || join
            .conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
    {
        return Ok(None);
    }
    let NativeChild::Node(dimension_index) = join.right.clone() else {
        return Ok(None);
    };
    if !matches!(
        shell
            .nodes
            .get(dimension_index)
            .ok_or_else(|| paro_error::internal("native dimension shell lost its dimension"))?
            .operator,
        LogicalOperator::Get(_)
    ) {
        return Ok(None);
    }
    let left_layout = native_dimension_child_layout(&join.left, &layouts)?;
    let right_layout = native_dimension_child_layout(&join.right, &layouts)?;
    if !join.left_projection_map.is_identity(left_layout.len())
        || !join.right_projection_map.is_identity(right_layout.len())
    {
        return Ok(None);
    }
    let fact_bindings = left_layout
        .bindings()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let dimension_bindings = right_layout
        .bindings()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if fact_bindings.is_empty() || dimension_bindings.is_empty() {
        return Ok(None);
    }

    let mut partial_groups = Vec::with_capacity(join.conditions.len() + aggregate.groups.len());
    let mut condition_rewrites = Vec::with_capacity(join.conditions.len());
    for condition in &join.conditions {
        let (fact_key, fact_on_left) = match (
            native_dimension_expression_domain(
                &condition.left,
                &fact_bindings,
                &dimension_bindings,
            ),
            native_dimension_expression_domain(
                &condition.right,
                &fact_bindings,
                &dimension_bindings,
            ),
        ) {
            (NativeDimensionExpressionDomain::Fact, NativeDimensionExpressionDomain::Dimension) => {
                (&condition.left, true)
            }
            (NativeDimensionExpressionDomain::Dimension, NativeDimensionExpressionDomain::Fact) => {
                (&condition.right, false)
            }
            _ => return Ok(None),
        };
        if !native_dimension_expression_is_movable(fact_key) {
            return Ok(None);
        }
        let key_ordinal = native_dimension_insert_unique(&mut partial_groups, fact_key.clone());
        condition_rewrites.push(NativeDimensionConditionRewrite {
            key_ordinal,
            fact_on_left,
        });
    }

    let mut outer_groups = Vec::with_capacity(aggregate.groups.len());
    let mut has_deferred_payload = false;
    for group in &aggregate.groups {
        if !native_dimension_expression_is_movable(group) {
            return Ok(None);
        }
        match native_dimension_expression_domain(group, &fact_bindings, &dimension_bindings) {
            NativeDimensionExpressionDomain::Fact | NativeDimensionExpressionDomain::Constant => {
                let ordinal = native_dimension_insert_unique(&mut partial_groups, group.clone());
                outer_groups.push(NativeDeferredOuterGroup::Partial { ordinal });
            }
            NativeDimensionExpressionDomain::Dimension => {
                has_deferred_payload = true;
                outer_groups.push(NativeDeferredOuterGroup::Dimension(group.clone()));
            }
            NativeDimensionExpressionDomain::Mixed | NativeDimensionExpressionDomain::Invalid => {
                return Ok(None);
            }
        }
    }
    if !has_deferred_payload {
        return Ok(None);
    }

    let mut partial_aggregates = Vec::with_capacity(aggregate.aggregates.len());
    let mut merge_functions = Vec::with_capacity(aggregate.aggregates.len());
    for expression in &aggregate.aggregates {
        let Expression::Aggregate(partial) = expression else {
            return Ok(None);
        };
        if partial.aggr_type != paro_planner::expression::AggregateType::NonDistinct
            || !partial.order_bys.is_empty()
            || partial
                .children
                .iter()
                .any(|child| !native_dimension_expression_is_movable(child))
            || partial
                .filter
                .as_deref()
                .is_some_and(|filter| !native_dimension_expression_is_movable(filter))
            || !matches!(
                native_dimension_expression_domain(expression, &fact_bindings, &dimension_bindings),
                NativeDimensionExpressionDomain::Fact | NativeDimensionExpressionDomain::Constant
            )
        {
            return Ok(None);
        }
        let Some(merge) = partial.function.partial_merge_function() else {
            return Ok(None);
        };
        if merge.arguments != [partial.return_type.clone()]
            || merge.return_type != partial.return_type
        {
            return Ok(None);
        }
        partial_aggregates.push(expression.clone());
        merge_functions.push(merge);
    }

    let partial_group_index = state.bind_context.generate_table_index();
    let partial_aggregate_index = state.bind_context.generate_table_index();
    let partial_groupings_index = state.bind_context.generate_table_index();
    let mut outer_stats = shell.nodes[shell.root].stats.clone();
    outer_stats.unique_keys.clear();
    let mut final_join_stats = shell
        .nodes
        .get(join_index)
        .map(|node| node.stats.clone())
        .unwrap_or_default();
    final_join_stats.unique_keys.clear();
    let mut nodes = shell.nodes.into_vec();

    let child_stats = |nodes: &[NativeNode], child: &NativeChild| -> NodeStats {
        match child {
            NativeChild::Node(index) => nodes
                .get(*index)
                .map(|node| node.stats.clone())
                .unwrap_or_default(),
            NativeChild::MemoGroup { stats, .. } | NativeChild::Group { stats, .. } => {
                stats.clone()
            }
        }
    };

    let mut partial_operator = (*aggregate).clone();
    partial_operator.group_index = partial_group_index;
    partial_operator.aggregate_index = partial_aggregate_index;
    partial_operator.groupings_index = partial_groupings_index;
    partial_operator.child = join.left.clone();
    partial_operator.groups = partial_groups.clone();
    partial_operator.grouping_sets.clear();
    partial_operator.aggregates = partial_aggregates;
    reset_native_aggregate_output(&mut partial_operator);
    let mut partial_stats = child_stats(&nodes, &partial_operator.child);
    partial_stats.unique_keys.clear();
    let partial_index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: partial_stats,
        operator: LogicalOperator::Aggregate(Box::new(partial_operator)),
        source_proofs: Box::new([]),
    });

    let mut final_conditions = join.conditions.clone();
    for (condition, rewrite) in final_conditions.iter_mut().zip(&condition_rewrites) {
        let fact_expression = if rewrite.fact_on_left {
            &mut condition.left
        } else {
            &mut condition.right
        };
        let return_type = fact_expression.return_type();
        *fact_expression = Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                ColumnBinding::new(partial_group_index, rewrite.key_ordinal),
                return_type,
            )
            .into(),
        );
        if rewrite.fact_on_left {
            std::mem::swap(&mut condition.left, &mut condition.right);
            condition.comparison = condition.comparison.flip();
        }
    }
    let mut final_join = join.clone();
    final_join.left = join.right.clone();
    final_join.right = NativeChild::Node(partial_index);
    final_join.conditions = final_conditions;
    final_join.left_projection_map = paro_planner::operator::ProjectionMap::all();
    final_join.right_projection_map = paro_planner::operator::ProjectionMap::all();
    let final_join_index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: final_join_stats,
        operator: LogicalOperator::Join(Join::Comparison(final_join)),
        source_proofs: Box::new([]),
    });

    let outer_groups = outer_groups
        .into_iter()
        .map(|group| match group {
            NativeDeferredOuterGroup::Dimension(expression) => expression,
            NativeDeferredOuterGroup::Partial { ordinal } => Expression::ColumnRef(
                paro_planner::expression::ColumnRefExpression::new(
                    ColumnBinding::new(partial_group_index, ordinal),
                    partial_groups[ordinal].return_type(),
                )
                .into(),
            ),
        })
        .collect::<Vec<_>>();
    let outer_aggregates = merge_functions
        .into_iter()
        .enumerate()
        .map(|(aggregate_index, merge)| {
            let return_type = merge.return_type.clone();
            Expression::Aggregate(
                paro_planner::expression::AggregateExpression::new(
                    merge,
                    vec![Expression::ColumnRef(
                        paro_planner::expression::ColumnRefExpression::new(
                            ColumnBinding::new(partial_aggregate_index, aggregate_index),
                            return_type.clone(),
                        )
                        .into(),
                    )],
                    return_type,
                )
                .into(),
            )
        })
        .collect::<Vec<_>>();
    let mut outer_operator = (*aggregate).clone();
    outer_operator.child = NativeChild::Node(final_join_index);
    outer_operator.groups = outer_groups;
    outer_operator.aggregates = outer_aggregates;
    outer_operator.grouping_sets.clear();
    reset_native_aggregate_output(&mut outer_operator);
    let outer_index = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: outer_stats,
        operator: LogicalOperator::Aggregate(Box::new(outer_operator)),
        source_proofs: Box::new([]),
    });

    let shell = compact_native_shell(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root: outer_index,
    })?;
    let result_layout = shell.root_layout()?;
    if result_layout.bindings() != original_root_layout.bindings()
        || result_layout.types() != original_root_layout.types()
    {
        return Ok(None);
    }
    Ok(Some(shell))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeSharingOutputSlot {
    Group(usize),
    Aggregate(usize),
    Constant,
}

#[derive(Debug, Clone, Copy)]
struct NativeSharingBranch {
    projection: usize,
    filter: Option<usize>,
    outer: usize,
    join: usize,
    dimension: usize,
    partial: usize,
}

// This variant owns its output-slot vector in the native matcher.  The
// apply-time view cannot borrow a vector from a temporary shell, so keep the
// indices separate and derive the slots again while validating the shell.
#[derive(Debug, Clone)]
struct NativeSharingBranchView {
    branch: NativeSharingBranch,
    output_slots: Vec<NativeSharingOutputSlot>,
}

fn native_child_node(child: &NativeChild) -> Option<usize> {
    match child {
        NativeChild::Node(index) => Some(*index),
        NativeChild::MemoGroup { .. } | NativeChild::Group { .. } => None,
    }
}

fn native_collect_union_arms(
    shell: &NativeShell,
    index: usize,
    output_types: &[paro_common::types::LogicalType],
    arms: &mut Vec<usize>,
) -> bool {
    let Some(node) = shell.nodes.get(index) else {
        return false;
    };
    let LogicalOperator::SetOperation(setop) = &node.operator else {
        arms.push(index);
        return true;
    };
    if setop.setop_type != SetOpType::Union
        || !setop.setop_all
        || setop.column_count != output_types.len()
        || setop.types != output_types
    {
        return false;
    }
    let Some(left) = native_child_node(&setop.left) else {
        return false;
    };
    let Some(right) = native_child_node(&setop.right) else {
        return false;
    };
    native_collect_union_arms(shell, left, output_types, arms)
        && native_collect_union_arms(shell, right, output_types, arms)
}

fn native_expression_reads_only(expression: &Expression, allowed: &HashSet<ColumnBinding>) -> bool {
    let mut valid = true;
    let mut read = false;
    crate::expression::traversal::visit_expression(expression, &mut |expression| {
        if let Expression::ColumnRef(column) = expression {
            read = true;
            valid &= column.depth == 0 && allowed.contains(&column.binding);
        }
    });
    valid && read
}

fn native_sharing_output_slot(
    expression: &Expression,
    aggregate: &Aggregate<NativeChild>,
) -> Option<NativeSharingOutputSlot> {
    match expression {
        Expression::ColumnRef(column) if column.depth == 0 => {
            if column.binding.table_index == aggregate.group_index
                && column.binding.column_index < aggregate.groups.len()
            {
                Some(NativeSharingOutputSlot::Group(column.binding.column_index))
            } else if column.binding.table_index == aggregate.aggregate_index
                && column.binding.column_index < aggregate.aggregates.len()
            {
                Some(NativeSharingOutputSlot::Aggregate(
                    column.binding.column_index,
                ))
            } else {
                None
            }
        }
        Expression::Constant(_) => Some(NativeSharingOutputSlot::Constant),
        _ => None,
    }
}

fn native_sharing_merge_contract_matches(
    outer: &Aggregate<NativeChild>,
    partial: &Aggregate<NativeChild>,
) -> bool {
    outer.aggregates.iter().all(|expression| {
        let Expression::Aggregate(merge) = expression else {
            return false;
        };
        if merge.children.len() != 1 || merge.filter.is_some() || !merge.order_bys.is_empty() {
            return false;
        }
        let Expression::ColumnRef(column) = &merge.children[0] else {
            return false;
        };
        if column.depth != 0
            || column.binding.table_index != partial.aggregate_index
            || column.binding.column_index >= partial.aggregates.len()
        {
            return false;
        }
        let Expression::Aggregate(source) = &partial.aggregates[column.binding.column_index] else {
            return false;
        };
        source
            .function
            .partial_merge_function()
            .is_some_and(|expected| expected.execution_semantics_equal(&merge.function))
    })
}

fn native_extend_bindings(
    bindings: &mut HashMap<ColumnBinding, ColumnBinding>,
    from_table: usize,
    to_table: usize,
    count: usize,
) {
    native_extend_bindings_with_offset(bindings, from_table, to_table, 0, count);
}

fn native_extend_bindings_with_offset(
    bindings: &mut HashMap<ColumnBinding, ColumnBinding>,
    from_table: usize,
    to_table: usize,
    to_column_offset: usize,
    count: usize,
) {
    bindings.extend((0..count).map(|ordinal| {
        (
            ColumnBinding::new(from_table, ordinal),
            ColumnBinding::new(to_table, to_column_offset.saturating_add(ordinal)),
        )
    }));
}

fn native_remap_expression(
    expression: &Expression,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) -> Option<Expression> {
    let valid = std::cell::Cell::new(true);
    let expression = expression.clone().replace_column_ref(&|column| {
        if column.depth != 0 {
            valid.set(false);
            return None;
        }
        bindings.get(&column.binding).map_or_else(
            || {
                valid.set(false);
                None
            },
            |binding| {
                Some(Expression::ColumnRef(
                    paro_planner::expression::ColumnRefExpression::new(
                        *binding,
                        column.return_type.clone(),
                    )
                    .into(),
                ))
            },
        )
    });
    valid.get().then_some(expression)
}

fn native_replace_known_bindings(
    expression: &Expression,
    bindings: &HashMap<ColumnBinding, ColumnBinding>,
) -> Expression {
    expression.clone().replace_column_ref(&|column| {
        bindings.get(&column.binding).map(|binding| {
            Expression::ColumnRef(
                paro_planner::expression::ColumnRefExpression::new(
                    *binding,
                    column.return_type.clone(),
                )
                .into(),
            )
        })
    })
}

fn native_sharing_branch_view(
    shell: &NativeShell,
    layouts: &[paro_planner::operator::LogicalOutputLayout],
    projection_index: usize,
) -> Option<NativeSharingBranchView> {
    let projection = match &shell.nodes.get(projection_index)?.operator {
        LogicalOperator::Projection(projection) => projection,
        _ => return None,
    };
    let mut child = native_child_node(&projection.child)?;
    let filter = if matches!(shell.nodes.get(child)?.operator, LogicalOperator::Filter(_)) {
        let filter = child;
        child = native_child_node(match &shell.nodes.get(filter)?.operator {
            LogicalOperator::Filter(filter) => &filter.child,
            _ => unreachable!("filter shape was checked"),
        })?;
        Some(filter)
    } else {
        None
    };
    let outer = child;
    let outer_operator = match &shell.nodes.get(outer)?.operator {
        LogicalOperator::Aggregate(aggregate) => aggregate,
        _ => return None,
    };
    if outer_operator.post_reduction.is_some()
        || outer_operator.aggregates.is_empty()
        || !outer_operator.has_plain_grouping_domain()
    {
        return None;
    }
    let join = native_child_node(&outer_operator.child)?;
    let join_operator = match &shell.nodes.get(join)?.operator {
        LogicalOperator::Join(Join::Comparison(join)) => join,
        _ => return None,
    };
    if join_operator.join_type != JoinType::Inner
        || join_operator.conditions.is_empty()
        || join_operator.mark_index.is_some()
        || !join_operator.duplicate_eliminated_columns.is_empty()
        || join_operator.delim_flipped
        || join_operator.build_side_constraint
            != paro_planner::operator::JoinBuildSideConstraint::Either
        || join_operator
            .conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
        || !join_operator.left_projection_map.is_identity(
            native_dimension_child_layout(&join_operator.left, layouts)
                .ok()?
                .len(),
        )
        || !join_operator.right_projection_map.is_identity(
            native_dimension_child_layout(&join_operator.right, layouts)
                .ok()?
                .len(),
        )
    {
        return None;
    }
    let dimension = native_child_node(&join_operator.left)?;
    let partial = native_child_node(&join_operator.right)?;
    let dimension_operator = match &shell.nodes.get(dimension)?.operator {
        LogicalOperator::Get(get) if get.table.is_some() => get,
        _ => return None,
    };
    let partial_operator = match &shell.nodes.get(partial)?.operator {
        LogicalOperator::Aggregate(aggregate) => aggregate,
        _ => return None,
    };
    if partial_operator.post_reduction.is_some()
        || partial_operator.aggregates.is_empty()
        || !partial_operator.has_plain_grouping_domain()
        || !native_sharing_merge_contract_matches(outer_operator, partial_operator)
    {
        return None;
    }
    let dimension_layout = native_dimension_child_layout(&join_operator.left, layouts).ok()?;
    let dimension_bindings = dimension_layout
        .bindings()
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if !join_operator.conditions.iter().all(|condition| {
        native_expression_reads_only(&condition.left, &dimension_bindings)
            && matches!(
                &condition.right,
                Expression::ColumnRef(column)
                    if column.depth == 0
                        && column.binding.table_index == partial_operator.group_index
                        && column.binding.column_index < partial_operator.groups.len()
            )
    }) {
        return None;
    }
    let output_slots = projection
        .expressions
        .iter()
        .map(|expression| native_sharing_output_slot(expression, outer_operator))
        .collect::<Option<Vec<_>>>()?;
    if projection.returned_types.len() != projection.expressions.len()
        || projection.returned_types != layouts.get(projection_index)?.types()
    {
        return None;
    }
    let _ = dimension_operator;
    Some(NativeSharingBranchView {
        branch: NativeSharingBranch {
            projection: projection_index,
            filter,
            outer,
            join,
            dimension,
            partial,
        },
        output_slots,
    })
}

fn native_sharing_constants_are_disjoint(
    shell: &NativeShell,
    branches: &[NativeSharingBranchView],
    constant_outputs: &[usize],
) -> bool {
    if constant_outputs.is_empty() {
        return false;
    }
    branches.iter().enumerate().all(|(left_index, left)| {
        branches
            .iter()
            .enumerate()
            .skip(left_index + 1)
            .all(|(_, right)| {
                constant_outputs.iter().any(|ordinal| {
                    let left_expression = match &shell.nodes[left.branch.projection].operator {
                        LogicalOperator::Projection(projection) => {
                            projection.expressions.get(*ordinal)
                        }
                        _ => None,
                    };
                    let right_expression = match &shell.nodes[right.branch.projection].operator {
                        LogicalOperator::Projection(projection) => {
                            projection.expressions.get(*ordinal)
                        }
                        _ => None,
                    };
                    let (Some(Expression::Constant(left)), Some(Expression::Constant(right))) =
                        (left_expression, right_expression)
                    else {
                        return false;
                    };
                    native_grouping_constants_prove_distinct(left, right)
                })
            })
    })
}

fn native_grouping_constants_prove_distinct(
    left: &paro_planner::expression::ConstantExpression,
    right: &paro_planner::expression::ConstantExpression,
) -> bool {
    if left.return_type != right.return_type
        || left.value.is_null()
        || right.value.is_null()
        || matches!(left.value, Value::Float(_) | Value::Double(_))
        || matches!(right.value, Value::Float(_) | Value::Double(_))
        || matches!(
            left.return_type,
            paro_common::types::LogicalType::VarcharCollation(_)
        )
    {
        return false;
    }
    left.value != right.value
}

/// Native implementation of AggregateDimensionSharing. The matcher already
/// narrowed the pattern to the exact UNION/partial/merge shell. This function
/// only moves immutable Memo group references and scalar expressions; it
/// never exports the matched subtree to an OwnedLogicalPlan.
fn try_native_dimension_sharing(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some(shell) = NativeShell::from_pattern(memo, state, binding, facts)? else {
        return Ok(None);
    };
    let (output_types, allow_out_of_order, root_table_index, root_stats) =
        match shell.root_operator() {
            LogicalOperator::SetOperation(root_setop)
                if root_setop.setop_type == SetOpType::Union
                    && root_setop.setop_all
                    && root_setop.column_count == root_setop.types.len() =>
            {
                (
                    root_setop.types.clone(),
                    root_setop.allow_out_of_order,
                    root_setop.table_index,
                    shell.nodes[shell.root].stats.clone(),
                )
            }
            _ => return Ok(None),
        };
    let mut arm_indices = Vec::new();
    if !native_collect_union_arms(&shell, shell.root, &output_types, &mut arm_indices)
        || arm_indices.len() < 2
    {
        return Ok(None);
    }
    let layouts = shell.layouts()?;
    let branches = arm_indices
        .into_iter()
        .map(|index| native_sharing_branch_view(&shell, &layouts, index))
        .collect::<Option<Vec<_>>>();
    let Some(branches) = branches else {
        return Ok(None);
    };
    let first = branches
        .first()
        .cloned()
        .ok_or_else(|| paro_error::internal("native dimension sharing has no first branch"))?;
    if first.output_slots.len() != output_types.len()
        || !branches.iter().skip(1).all(|branch| {
            branch.output_slots == first.output_slots
                && match &shell.nodes[branch.branch.projection].operator {
                    LogicalOperator::Projection(projection) => {
                        projection.returned_types == output_types
                    }
                    _ => false,
                }
        })
        || !matches!(
            &shell.nodes[first.branch.projection].operator,
            LogicalOperator::Projection(projection) if projection.returned_types == output_types
        )
    {
        return Ok(None);
    }
    let first_outer = match &shell.nodes[first.branch.outer].operator {
        LogicalOperator::Aggregate(aggregate) => aggregate.clone(),
        _ => return Ok(None),
    };
    let first_join = match &shell.nodes[first.branch.join].operator {
        LogicalOperator::Join(Join::Comparison(join)) => join.clone(),
        _ => return Ok(None),
    };
    let first_partial = match &shell.nodes[first.branch.partial].operator {
        LogicalOperator::Aggregate(aggregate) => aggregate.clone(),
        _ => return Ok(None),
    };
    let first_projection = match &shell.nodes[first.branch.projection].operator {
        LogicalOperator::Projection(projection) => projection.clone(),
        _ => return Ok(None),
    };
    let first_filter = match first.branch.filter {
        None => None,
        Some(index) => match &shell.nodes[index].operator {
            LogicalOperator::Filter(filter) => Some(filter.clone()),
            _ => return Ok(None),
        },
    };
    let mut all_compatible = true;
    for branch in branches.iter().skip(1) {
        let right_outer = match &shell.nodes[branch.branch.outer].operator {
            LogicalOperator::Aggregate(aggregate) => aggregate,
            _ => {
                all_compatible = false;
                continue;
            }
        };
        let right_partial = match &shell.nodes[branch.branch.partial].operator {
            LogicalOperator::Aggregate(aggregate) => aggregate,
            _ => {
                all_compatible = false;
                continue;
            }
        };
        let right_join = match &shell.nodes[branch.branch.join].operator {
            LogicalOperator::Join(Join::Comparison(join)) => join,
            _ => {
                all_compatible = false;
                continue;
            }
        };
        let right_projection = match &shell.nodes[branch.branch.projection].operator {
            LogicalOperator::Projection(projection) => projection,
            _ => {
                all_compatible = false;
                continue;
            }
        };
        let Some(left_dimension) = (match &shell.nodes[first.branch.dimension].operator {
            LogicalOperator::Get(get) => Some(get),
            _ => None,
        }) else {
            all_compatible = false;
            continue;
        };
        let Some(right_dimension) = (match &shell.nodes[branch.branch.dimension].operator {
            LogicalOperator::Get(get) => Some(get),
            _ => None,
        }) else {
            all_compatible = false;
            continue;
        };
        if !crate::aggregate::dimension_sharing::equivalent_dimension_gets(
            left_dimension,
            right_dimension,
        ) || right_outer.groups.len() != first_outer.groups.len()
            || right_outer.aggregates.len() != first_outer.aggregates.len()
            || right_partial.groups.len() != first_partial.groups.len()
            || right_partial.aggregates.len() != first_partial.aggregates.len()
            || right_join.conditions.len() != first_join.conditions.len()
            || right_partial
                .groups
                .iter()
                .map(Expression::return_type)
                .ne(first_partial.groups.iter().map(Expression::return_type))
            || right_partial
                .aggregates
                .iter()
                .map(Expression::return_type)
                .ne(first_partial.aggregates.iter().map(Expression::return_type))
            || branch.branch.filter.is_some() != first.branch.filter.is_some()
        {
            all_compatible = false;
            continue;
        }
        let mut bindings = HashMap::new();
        native_extend_bindings(
            &mut bindings,
            right_dimension.table_index,
            left_dimension.table_index,
            right_dimension.returned_types.len(),
        );
        native_extend_bindings(
            &mut bindings,
            right_partial.group_index,
            first_partial.group_index,
            first_partial.groups.len(),
        );
        native_extend_bindings(
            &mut bindings,
            right_partial.aggregate_index,
            first_partial.aggregate_index,
            first_partial.aggregates.len(),
        );
        native_extend_bindings(
            &mut bindings,
            right_outer.group_index,
            first_outer.group_index,
            first_outer.groups.len(),
        );
        native_extend_bindings(
            &mut bindings,
            right_outer.aggregate_index,
            first_outer.aggregate_index,
            first_outer.aggregates.len(),
        );
        let filters_match = match (first.branch.filter, branch.branch.filter) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                let left = match &shell.nodes[left].operator {
                    LogicalOperator::Filter(filter) => filter,
                    _ => {
                        all_compatible = false;
                        continue;
                    }
                };
                let right = match &shell.nodes[right].operator {
                    LogicalOperator::Filter(filter) => filter,
                    _ => {
                        all_compatible = false;
                        continue;
                    }
                };
                right.expressions.len() == left.expressions.len()
                    && right
                        .expressions
                        .iter()
                        .zip(&left.expressions)
                        .all(|(right, left)| {
                            native_remap_expression(right, &bindings)
                                .is_some_and(|right| left.equals(&right))
                        })
            }
            _ => false,
        };
        if !filters_match
            || !right_outer
                .groups
                .iter()
                .zip(&first_outer.groups)
                .all(|(right, left)| {
                    native_remap_expression(right, &bindings)
                        .is_some_and(|right| left.equals(&right))
                })
            || !right_outer
                .aggregates
                .iter()
                .zip(&first_outer.aggregates)
                .all(|(right, left)| {
                    native_remap_expression(right, &bindings)
                        .is_some_and(|right| left.equals(&right))
                })
            || !right_join
                .conditions
                .iter()
                .zip(&first_join.conditions)
                .all(|(right, left)| {
                    right.comparison == left.comparison
                        && native_remap_expression(&right.left, &bindings)
                            .is_some_and(|right| left.left.equals(&right))
                        && native_remap_expression(&right.right, &bindings)
                            .is_some_and(|right| left.right.equals(&right))
                })
            || !right_projection
                .expressions
                .iter()
                .zip(&first_projection.expressions)
                .zip(&first.output_slots)
                .all(|((right, left), slot)| match slot {
                    NativeSharingOutputSlot::Constant => right.return_type() == left.return_type(),
                    _ => native_remap_expression(right, &bindings)
                        .is_some_and(|right| left.equals(&right)),
                })
        {
            all_compatible = false;
        }
    }
    if !all_compatible {
        return Ok(None);
    }
    let constant_outputs = first
        .output_slots
        .iter()
        .enumerate()
        .filter_map(|(ordinal, slot)| {
            matches!(slot, NativeSharingOutputSlot::Constant).then_some(ordinal)
        })
        .collect::<Vec<_>>();
    tracing::debug!(
        target: "paro::optimizer::native_sharing",
        first_groups = ?first_outer.groups,
        first_projection = ?first_projection.expressions,
        output_slots = ?first.output_slots,
        constant_outputs = ?constant_outputs,
        "native dimension sharing output mapping"
    );
    let needs_hidden_identity =
        !native_sharing_constants_are_disjoint(&shell, &branches, &constant_outputs);
    let mut nodes = shell.nodes.into_vec();
    let partial_group_count = first_partial.groups.len();
    let partial_aggregate_count = first_partial.aggregates.len();
    let mut union_types = first_partial
        .groups
        .iter()
        .map(Expression::return_type)
        .chain(first_partial.aggregates.iter().map(Expression::return_type))
        .chain(
            constant_outputs
                .iter()
                .map(|ordinal| first_projection.expressions[*ordinal].return_type()),
        )
        .collect::<Vec<_>>();
    if needs_hidden_identity {
        union_types.push(paro_common::types::LogicalType::UBigInt);
    }
    let mut partial_arm_indices = Vec::with_capacity(branches.len());
    for (branch_index, branch) in branches.iter().enumerate() {
        let partial = match &nodes[branch.branch.partial].operator {
            LogicalOperator::Aggregate(partial) => partial,
            _ => return Ok(None),
        };
        let mut expressions = partial
            .groups
            .iter()
            .enumerate()
            .map(|(ordinal, expression)| {
                Expression::ColumnRef(
                    paro_planner::expression::ColumnRefExpression::new(
                        ColumnBinding::new(partial.group_index, ordinal),
                        expression.return_type(),
                    )
                    .into(),
                )
            })
            .chain(
                partial
                    .aggregates
                    .iter()
                    .enumerate()
                    .map(|(ordinal, expression)| {
                        Expression::ColumnRef(
                            paro_planner::expression::ColumnRefExpression::new(
                                ColumnBinding::new(partial.aggregate_index, ordinal),
                                expression.return_type(),
                            )
                            .into(),
                        )
                    }),
            )
            .collect::<Vec<_>>();
        if let LogicalOperator::Projection(projection) = &nodes[branch.branch.projection].operator {
            expressions.extend(
                constant_outputs
                    .iter()
                    .map(|ordinal| projection.expressions[*ordinal].clone()),
            );
        } else {
            return Ok(None);
        }
        if needs_hidden_identity {
            expressions.push(Expression::Constant(
                paro_planner::expression::ConstantExpression::new(
                    Value::UBigInt(u64::try_from(branch_index).unwrap_or(u64::MAX)),
                    paro_common::types::LogicalType::UBigInt,
                )
                .into(),
            ));
        }
        let arm = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: nodes[branch.branch.partial].stats.clone(),
            operator: LogicalOperator::Projection(Projection {
                table_index: state.bind_context.generate_table_index(),
                expressions,
                visible_names: Vec::new(),
                visible_count: 0,
                visible_qualifier: None,
                child: NativeChild::Node(branch.branch.partial),
                returned_types: union_types.clone(),
            }),
            source_proofs: Box::new([]),
        });
        partial_arm_indices.push(arm);
    }
    let first_partial_group_index = first_partial.group_index;
    let first_partial_aggregate_index = first_partial.aggregate_index;
    let mut partial_union = *partial_arm_indices
        .first()
        .ok_or_else(|| paro_error::internal("native dimension sharing has no partial arm"))?;
    let mut union_table_index = None;
    for arm in partial_arm_indices.iter().skip(1).copied() {
        let table_index = state.bind_context.generate_table_index();
        let index = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: nodes[partial_union].stats.clone(),
            operator: LogicalOperator::SetOperation(SetOperation {
                table_index,
                column_count: union_types.len(),
                left: NativeChild::Node(partial_union),
                right: NativeChild::Node(arm),
                setop_type: SetOpType::Union,
                setop_all: true,
                allow_out_of_order,
                types: union_types.clone(),
            }),
            source_proofs: Box::new([]),
        });
        partial_union = index;
        union_table_index = Some(table_index);
    }
    let union_table_index = union_table_index.ok_or_else(|| {
        paro_error::internal("native dimension sharing requires at least two union arms")
    })?;
    let mut partial_to_union = HashMap::new();
    native_extend_bindings(
        &mut partial_to_union,
        first_partial_group_index,
        union_table_index,
        partial_group_count,
    );
    // The compact UNION layout is `[partial groups, partial aggregates,
    // branch constants, optional identity]`.  Aggregate references from the
    // original partial child therefore start after the group prefix; mapping
    // them at column zero produces a syntactically valid shell whose final
    // aggregate reads a grouping key (and is rejected later by type/layout
    // validation).
    native_extend_bindings_with_offset(
        &mut partial_to_union,
        first_partial_aggregate_index,
        union_table_index,
        partial_group_count,
        partial_aggregate_count,
    );
    let mut join = first_join.clone();
    join.left = NativeChild::Node(first.branch.dimension);
    join.right = NativeChild::Node(partial_union);
    join.conditions = join
        .conditions
        .iter()
        .map(|condition| paro_planner::operator::JoinCondition {
            left: native_replace_known_bindings(&condition.left, &partial_to_union),
            right: native_replace_known_bindings(&condition.right, &partial_to_union),
            comparison: condition.comparison,
        })
        .collect();
    join.left_projection_map = paro_planner::operator::ProjectionMap::all();
    join.right_projection_map = paro_planner::operator::ProjectionMap::all();
    let joined = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: nodes[first.branch.join].stats.clone(),
        operator: LogicalOperator::Join(Join::Comparison(join)),
        source_proofs: Box::new([]),
    });
    let original_outer_group_count = first_outer.groups.len();
    let original_outer_aggregate_count = first_outer.aggregates.len();
    let original_outer_group_index = first_outer.group_index;
    let original_outer_aggregate_index = first_outer.aggregate_index;
    let mut final_groups = first_outer
        .groups
        .iter()
        .map(|expression| {
            // A branch discriminator such as `sale_type` is commonly a
            // literal carried by the outer grouping key and exposed by the
            // projection.  Leaving the first arm's literal in place would
            // merge every UNION arm into that one value after sharing.  Read
            // the corresponding compact UNION column instead, while leaving
            // unrelated constants untouched.
            if let Some(constant_ordinal) = constant_outputs
                .iter()
                .position(|ordinal| first_projection.expressions[*ordinal].equals(expression))
            {
                Expression::ColumnRef(
                    paro_planner::expression::ColumnRefExpression::new(
                        ColumnBinding::new(
                            union_table_index,
                            partial_group_count
                                .saturating_add(partial_aggregate_count)
                                .saturating_add(constant_ordinal),
                        ),
                        union_types
                            [partial_group_count + partial_aggregate_count + constant_ordinal]
                            .clone(),
                    )
                    .into(),
                )
            } else {
                native_replace_known_bindings(expression, &partial_to_union)
            }
        })
        .collect::<Vec<_>>();
    final_groups.extend(constant_outputs.iter().enumerate().map(|(ordinal, _)| {
        Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                ColumnBinding::new(
                    union_table_index,
                    partial_group_count + partial_aggregate_count + ordinal,
                ),
                union_types[partial_group_count + partial_aggregate_count + ordinal].clone(),
            )
            .into(),
        )
    }));
    let branch_identity_ordinal = needs_hidden_identity.then_some(union_types.len() - 1);
    if let Some(ordinal) = branch_identity_ordinal {
        final_groups.push(Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                ColumnBinding::new(union_table_index, ordinal),
                paro_common::types::LogicalType::UBigInt,
            )
            .into(),
        ));
    }
    tracing::debug!(
        target: "paro::optimizer::native_sharing",
        union_table_index,
        partial_group_count,
        partial_aggregate_count,
        final_groups = ?final_groups,
        "native dimension sharing constructed final grouping"
    );
    let final_aggregates = first_outer
        .aggregates
        .iter()
        .map(|expression| native_replace_known_bindings(expression, &partial_to_union))
        .collect::<Vec<_>>();
    let final_group_index = state.bind_context.generate_table_index();
    let final_aggregate_index = state.bind_context.generate_table_index();
    let final_groupings_index = state.bind_context.generate_table_index();
    let mut final_aggregate = (*first_outer).clone();
    final_aggregate.group_index = final_group_index;
    final_aggregate.aggregate_index = final_aggregate_index;
    final_aggregate.groupings_index = final_groupings_index;
    final_aggregate.child = NativeChild::Node(joined);
    final_aggregate.groups = final_groups;
    final_aggregate.aggregates = final_aggregates;
    final_aggregate.grouping_sets.clear();
    reset_native_aggregate_output(&mut final_aggregate);
    let final_aggregate_index_in_shell = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: nodes[first.branch.outer].stats.clone(),
        operator: LogicalOperator::Aggregate(Box::new(final_aggregate)),
        source_proofs: Box::new([]),
    });
    let mut outer_to_final = HashMap::new();
    native_extend_bindings(
        &mut outer_to_final,
        original_outer_group_index,
        final_group_index,
        original_outer_group_count,
    );
    native_extend_bindings(
        &mut outer_to_final,
        original_outer_aggregate_index,
        final_aggregate_index,
        original_outer_aggregate_count,
    );
    let final_input = if let Some(filter) = first_filter {
        let expressions = filter
            .expressions
            .iter()
            .map(|expression| native_remap_expression(expression, &outer_to_final))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| paro_error::internal("native dimension sharing lost filter bindings"))?;
        let index = nodes.len();
        nodes.push(NativeNode {
            id: state.bind_context.next_plan_id(),
            stats: nodes[final_aggregate_index_in_shell].stats.clone(),
            operator: LogicalOperator::Filter(Filter {
                expressions,
                child: NativeChild::Node(final_aggregate_index_in_shell),
                projection_map: filter.projection_map,
            }),
            source_proofs: Box::new([]),
        });
        index
    } else {
        final_aggregate_index_in_shell
    };
    let mut constant_ordinal = 0usize;
    let output_expressions = first
        .output_slots
        .iter()
        .enumerate()
        .map(|(output_ordinal, slot)| match slot {
            // A constant output in one UNION arm is a grouping key in the
            // shared aggregate.  Re-emitting the first arm's literal here
            // would collapse all arms back to that value at the root.
            NativeSharingOutputSlot::Constant => {
                let group_ordinal = original_outer_group_count + constant_ordinal;
                constant_ordinal += 1;
                Some(Expression::ColumnRef(
                    paro_planner::expression::ColumnRefExpression::new(
                        ColumnBinding::new(final_group_index, group_ordinal),
                        output_types[output_ordinal].clone(),
                    )
                    .into(),
                ))
            }
            _ => native_remap_expression(
                &first_projection.expressions[output_ordinal],
                &outer_to_final,
            ),
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| paro_error::internal("native dimension sharing lost output bindings"))?;
    let root = nodes.len();
    nodes.push(NativeNode {
        id: state.bind_context.next_plan_id(),
        stats: root_stats,
        operator: LogicalOperator::Projection(Projection {
            table_index: root_table_index,
            expressions: output_expressions,
            visible_names: first_projection.visible_names,
            visible_count: first_projection.visible_count,
            visible_qualifier: first_projection.visible_qualifier,
            child: NativeChild::Node(final_input),
            returned_types: first_projection.returned_types,
        }),
        source_proofs: Box::new([]),
    });
    let result = compact_native_shell(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result.root_layout()?.types() != output_types {
        return Ok(None);
    }
    Ok(Some(result))
}

fn native_dimension_direct_shape(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<bool> {
    let PatternOperand::Expression {
        expression,
        children,
        ..
    } = binding
    else {
        return Ok(false);
    };
    let logical = memo
        .logical_expr(*expression)
        .ok_or_else(|| paro_error::internal("native dimension preflight lost its root"))?;
    let payload = state
        .payloads
        .logical
        .get(logical.payload.index())
        .ok_or_else(|| paro_error::internal("native dimension preflight lost its root payload"))?;
    if !matches!(
        payload.semantic_template.operator,
        LogicalOperator::Aggregate(_)
    ) || children.len() != 1
    {
        return Ok(false);
    }
    let PatternOperand::Expression {
        expression: child_expression,
        children: join_children,
        ..
    } = &children[0]
    else {
        return Ok(false);
    };
    let child_logical = memo.logical_expr(*child_expression).ok_or_else(|| {
        paro_error::internal("native dimension preflight lost its join expression")
    })?;
    let child_payload = state
        .payloads
        .logical
        .get(child_logical.payload.index())
        .ok_or_else(|| paro_error::internal("native dimension preflight lost its join payload"))?;
    let LogicalOperator::Join(Join::Comparison(join)) = &child_payload.semantic_template.operator
    else {
        return Ok(false);
    };
    if join.join_type != JoinType::Inner
        || join.conditions.is_empty()
        || join.mark_index.is_some()
        || !join.duplicate_eliminated_columns.is_empty()
        || join.delim_flipped
        || join.build_side_constraint != paro_planner::operator::JoinBuildSideConstraint::Either
        || join
            .conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
        || join_children.len() != 2
    {
        return Ok(false);
    }
    let PatternOperand::Expression {
        expression: dimension_expression,
        children: dimension_children,
        ..
    } = &join_children[1]
    else {
        return Ok(false);
    };
    let dimension_logical = memo.logical_expr(*dimension_expression).ok_or_else(|| {
        paro_error::internal("native dimension preflight lost its dimension expression")
    })?;
    let dimension_payload = state
        .payloads
        .logical
        .get(dimension_logical.payload.index())
        .ok_or_else(|| {
            paro_error::internal("native dimension preflight lost its dimension payload")
        })?;
    if !matches!(
        dimension_payload.semantic_template.operator,
        LogicalOperator::Get(_)
    ) || !dimension_children.is_empty()
    {
        return Ok(false);
    }
    let join_metadata = state
        .metadata
        .get(&child_logical.payload)
        .ok_or_else(|| paro_error::internal("native dimension preflight lost join metadata"))?;
    let [left_layout, right_layout] = join_metadata.child_layouts.as_ref() else {
        return Ok(false);
    };
    Ok(join
        .left_projection_map
        .is_identity(left_layout.bindings().len())
        && join
            .right_projection_map
            .is_identity(right_layout.bindings().len()))
}

fn native_dimension_child_layout(
    child: &NativeChild,
    layouts: &[paro_planner::operator::LogicalOutputLayout],
) -> Result<paro_planner::operator::LogicalOutputLayout> {
    match child {
        NativeChild::Node(index) => layouts
            .get(*index)
            .cloned()
            .ok_or_else(|| paro_error::internal("native dimension child has no layout")),
        NativeChild::MemoGroup { layout, .. } | NativeChild::Group { layout, .. } => {
            Ok(layout.clone())
        }
    }
}

fn reset_native_aggregate_output(aggregate: &mut paro_planner::operator::Aggregate<NativeChild>) {
    aggregate.group_stats = vec![None; aggregate.groups.len()];
    aggregate.group_dependencies.clear();
    aggregate.group_input_multiplicity = paro_planner::operator::GroupInputMultiplicity::Arbitrary;
    aggregate.returned_types = aggregate
        .groups
        .iter()
        .map(Expression::return_type)
        .chain(aggregate.aggregates.iter().map(Expression::return_type))
        .chain(
            aggregate
                .grouping_functions
                .iter()
                .map(|_| paro_common::types::LogicalType::BigInt),
        )
        .collect();
}

fn native_dimension_expression_domain(
    expression: &Expression,
    fact: &HashSet<ColumnBinding>,
    dimension: &HashSet<ColumnBinding>,
) -> NativeDimensionExpressionDomain {
    let mut domain = NativeDimensionExpressionDomain::Constant;
    crate::expression::traversal::visit_expression(expression, &mut |expression| {
        let Expression::ColumnRef(column) = expression else {
            return;
        };
        let current = if column.depth != 0 {
            NativeDimensionExpressionDomain::Invalid
        } else if fact.contains(&column.binding) {
            NativeDimensionExpressionDomain::Fact
        } else if dimension.contains(&column.binding) {
            NativeDimensionExpressionDomain::Dimension
        } else {
            NativeDimensionExpressionDomain::Invalid
        };
        domain = native_dimension_combine_domains(domain, current);
    });
    domain
}

fn native_dimension_combine_domains(
    left: NativeDimensionExpressionDomain,
    right: NativeDimensionExpressionDomain,
) -> NativeDimensionExpressionDomain {
    use NativeDimensionExpressionDomain::{Constant, Dimension, Fact, Invalid, Mixed};
    match (left, right) {
        (Invalid, _) | (_, Invalid) => Invalid,
        (Mixed, _) | (_, Mixed) => Mixed,
        (Constant, domain) | (domain, Constant) => domain,
        (Fact, Fact) => Fact,
        (Dimension, Dimension) => Dimension,
        (Fact, Dimension) | (Dimension, Fact) => Mixed,
    }
}

fn native_dimension_expression_is_movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

fn native_dimension_insert_unique(
    expressions: &mut Vec<Expression>,
    expression: Expression,
) -> usize {
    if let Some(ordinal) = expressions
        .iter()
        .position(|existing| existing.equals(&expression))
    {
        ordinal
    } else {
        let ordinal = expressions.len();
        expressions.push(expression);
        ordinal
    }
}

/// Prove that a predicate transfer can possibly be useful before allocating a
/// native shell.  Matching deliberately returns many filter bindings that are
/// not side-local (join predicates, constants, or predicates over an opaque
/// child).  The full native path remains authoritative; this preflight only
/// rejects cases for which the existing column metadata proves that no
/// single-side predicate can be moved.
fn native_predicate_transfer_may_apply(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<bool> {
    fn operand_tables(
        operand: &PatternOperand,
        memo: &Memo,
        state: &PlannerTransformState,
    ) -> Result<Option<SmallVec<[usize; 8]>>> {
        let columns = match operand {
            PatternOperand::Group(group) => {
                let Some(group) = memo.group(*group) else {
                    return Ok(None);
                };
                group.schema.ids()
            }
            PatternOperand::Expression { expression, .. } => {
                let Some(logical) = memo.logical_expr(*expression) else {
                    return Ok(None);
                };
                let Some(metadata) = state.metadata.get(&logical.payload) else {
                    return Ok(None);
                };
                metadata.output_columns.iter().copied().collect()
            }
        };
        let mut tables = SmallVec::<[usize; 8]>::new();
        for column in columns {
            let Some(binding) = state.binding_ids.relation_binding(column) else {
                return Ok(None);
            };
            if !tables.contains(&binding.table_index) {
                tables.push(binding.table_index);
            }
        }
        if tables.is_empty() {
            Ok(None)
        } else {
            tables.sort_unstable();
            Ok(Some(tables))
        }
    }

    let PatternOperand::Expression {
        expression,
        children,
        ..
    } = binding
    else {
        return Ok(false);
    };
    let logical = memo
        .logical_expr(*expression)
        .ok_or_else(|| paro_error::internal("native predicate preflight lost its expression"))?;
    let operator = &state
        .payloads
        .logical
        .get(logical.payload.index())
        .ok_or_else(|| paro_error::internal("native predicate preflight lost its operator"))?
        .semantic_template
        .operator;
    let LogicalOperator::Filter(filter) = operator else {
        return Ok(false);
    };
    if filter
        .expressions
        .iter()
        .any(|expression| expression.evaluation_properties().is_reorder_fence())
    {
        return Ok(false);
    }
    let [join_operand] = children.as_ref() else {
        return Ok(false);
    };
    let PatternOperand::Expression {
        expression: join_expression,
        children: join_children,
        ..
    } = join_operand
    else {
        return Ok(false);
    };
    let join_logical = memo
        .logical_expr(*join_expression)
        .ok_or_else(|| paro_error::internal("native predicate preflight lost its join"))?;
    let join_operator = &state
        .payloads
        .logical
        .get(join_logical.payload.index())
        .ok_or_else(|| paro_error::internal("native predicate preflight lost its join operator"))?
        .semantic_template
        .operator;
    let reorderable = match join_operator {
        LogicalOperator::Join(Join::Comparison(join)) => {
            join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped
                && !crate::expression::comparison_join_has_evaluation_fence(join)
        }
        LogicalOperator::Join(Join::Cross(_)) => true,
        _ => false,
    };
    if !reorderable {
        return Ok(false);
    }
    let [left, right] = join_children.as_ref() else {
        return Ok(false);
    };
    // The native shell currently proves only local predicate movement.  Do
    // not use it as a replacement for the complete semantic rewrite when a
    // child is already a derived relation: CTE demand, aggregate domains and
    // producer ownership are encoded by that relation's alternatives, not by
    // its output columns.  The first native tranche is therefore limited to
    // an unambiguous pair of base Get groups.  This keeps D2 enabled for the
    // small safe subset while preventing a partial shell from winning over
    // Q11's CTE-aware narrow-aggregate chain.
    if !native_predicate_base_relation_operand(left, memo, state)?
        || !native_predicate_base_relation_operand(right, memo, state)?
    {
        return Ok(false);
    }
    let mut visited_groups = BTreeSet::new();
    if operand_contains_control_boundary(left, memo, state, &mut visited_groups)?
        || operand_contains_control_boundary(right, memo, state, &mut visited_groups)?
    {
        // A CTE/recursive producer is not just another relation: its
        // consumer demand and sharing owner are part of the rewrite proof.
        // PredicateTransfer has no native ownership adapter yet.
        return Ok(false);
    }
    let Some(left_tables) = operand_tables(left, memo, state)? else {
        return Ok(false);
    };
    let Some(right_tables) = operand_tables(right, memo, state)? else {
        return Ok(false);
    };
    Ok(filter.expressions.iter().any(|expression| {
        let mut tables = SmallVec::<[usize; 4]>::new();
        crate::expression::traversal::visit_expression(expression, &mut |candidate| {
            if let Expression::ColumnRef(column) = candidate {
                if !tables.contains(&column.binding.table_index) {
                    tables.push(column.binding.table_index);
                }
            }
        });
        tables.sort_unstable();
        is_side_local_tables(&tables, &left_tables, &right_tables)
            || is_side_local_tables(&tables, &right_tables, &left_tables)
    }))
}

fn native_predicate_base_relation_operand(
    operand: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<bool> {
    let (group, expression, children) = match operand {
        PatternOperand::Group(group) => {
            let group = memo.canonical_group(*group);
            let Some(group_ref) = memo.group(group) else {
                return Ok(false);
            };
            let [expression] = group_ref.logical_exprs() else {
                return Ok(false);
            };
            (group, *expression, 0)
        }
        PatternOperand::Expression {
            group,
            expression,
            children,
        } => (*group, *expression, children.len()),
    };
    if children != 0 || memo.cardinality_dependencies(group).next().is_some() {
        return Ok(false);
    }
    let Some(logical) = memo.logical_expr(expression) else {
        return Ok(false);
    };
    let Some(payload) = state.payloads.logical.get(logical.payload.index()) else {
        return Ok(false);
    };
    Ok(matches!(
        payload.semantic_template.operator,
        LogicalOperator::Get(_)
    ))
}

fn operand_contains_control_boundary(
    operand: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    visited_groups: &mut BTreeSet<GroupId>,
) -> Result<bool> {
    fn operator_is_control<Child>(operator: &LogicalOperator<Child>) -> bool {
        matches!(
            operator,
            LogicalOperator::CTERef(_)
                | LogicalOperator::MaterializedCTE(_)
                | LogicalOperator::RecursiveCTE(_)
        )
    }

    match operand {
        PatternOperand::Group(group) => {
            let group = memo.canonical_group(*group);
            if !visited_groups.insert(group) {
                return Ok(false);
            }
            if memo.cardinality_dependencies(group).next().is_some() {
                return Ok(true);
            }
            let logical_ids = memo
                .group(group)
                .map(|group| group.logical_exprs().to_vec())
                .unwrap_or_default();
            for logical_id in logical_ids {
                let logical = memo.logical_expr(logical_id).ok_or_else(|| {
                    paro_error::internal("native predicate control scan lost an expression")
                })?;
                let payload = state
                    .payloads
                    .logical
                    .get(logical.payload.index())
                    .ok_or_else(|| {
                        paro_error::internal("native predicate control scan lost a payload")
                    })?;
                if operator_is_control(&payload.semantic_template.operator) {
                    return Ok(true);
                }
                for child in &logical.key.children {
                    if operand_contains_control_boundary(
                        &PatternOperand::Group(*child),
                        memo,
                        state,
                        visited_groups,
                    )? {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        PatternOperand::Expression {
            expression,
            children,
            ..
        } => {
            let logical = memo.logical_expr(*expression).ok_or_else(|| {
                paro_error::internal("native predicate control scan lost an expression")
            })?;
            let payload = state
                .payloads
                .logical
                .get(logical.payload.index())
                .ok_or_else(|| {
                    paro_error::internal("native predicate control scan lost a payload")
                })?;
            if operator_is_control(&payload.semantic_template.operator) {
                return Ok(true);
            }
            children.iter().try_fold(false, |found, child| {
                if found {
                    return Ok(true);
                }
                operand_contains_control_boundary(child, memo, state, visited_groups)
            })
        }
    }
}

fn is_side_local_tables(tables: &[usize], side_tables: &[usize], other_tables: &[usize]) -> bool {
    !tables.is_empty()
        && tables
            .iter()
            .all(|table| side_tables.binary_search(table).is_ok())
        && tables
            .iter()
            .all(|table| other_tables.binary_search(table).is_err())
}

fn native_shell_contains_control_boundary(shell: &NativeShell) -> bool {
    shell.nodes.iter().any(|node| {
        let mut contains = matches!(
            &node.operator,
            LogicalOperator::CTERef(_)
                | LogicalOperator::MaterializedCTE(_)
                | LogicalOperator::RecursiveCTE(_)
        );
        node.operator.visit_child_links(&mut |child| match child {
            NativeChild::MemoGroup { reference, .. } | NativeChild::Group { reference, .. } => {
                contains |= reference.facts.contains_control_region;
            }
            NativeChild::Node(_) => {}
        });
        contains
    })
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
            source_proofs: node.source_proofs.clone(),
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
    rejection_reasons: &mut Option<crate::transformation_rejection::RejectionReasons>,
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
    Ok(rewrite_planner_expression(
        transformation,
        plan,
        column_stats,
        environment,
        rejection_reasons,
    )?
    .into_iter()
    .collect())
}

fn rewrite_planner_expression(
    transformation: PlannerTransformation,
    plan: OwnedLogicalPlan,
    column_stats: &HashMap<ColumnBinding, Arc<ColumnStatistics>>,
    environment: &PlannerRuleEnvironment,
    rejection_reasons: &mut Option<crate::transformation_rejection::RejectionReasons>,
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
            if !prefix_changed {
                crate::transformation_rejection::reject::<()>(
                    rejection_reasons,
                    crate::transformation_rejection::TransformationRejectionGuard::PrefixNoWitness,
                );
            }
            let (plan, payload_changed) = late_payload::rewrite_node_profiled(
                plan,
                &environment.bind_context,
                &environment.cost_model,
                rejection_reasons,
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

    #[test]
    fn predicate_transfer_semantic_peer_keeps_ownership_settlement() {
        assert!(!native_shell_staging_allowed(
            PlannerTransformation::PredicateTransfer
        ));
        assert!(native_shell_staging_allowed(
            PlannerTransformation::JoinRegionEnumeration
        ));
    }
}
