// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical equivalence rules and transactional Memo staging.

use super::*;

mod matching;
mod staging;

use staging::{stage_transformed_expression, StagingRequest};

pub(super) fn register_transformations(
    registry: &mut ImplementationRegistry,
    planner_state: Arc<RwLock<PlannerTransformState>>,
) -> Result<()> {
    validate_semantic_dependencies()?;
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
    CteDemandPushdown,
    AggregatePostReduction,
    MarkJoinToSemi,
    JoinElimination,
    AggregateJoinPreaggregation,
    AggregateJoinSubsumption,
    AggregateNonNullInput,
    AggregateDimensionDeferral,
    AggregateInputMaterialization,
    TopNIntroduction,
    LimitPushdown,
    LatePayloadFetch,
    ScalarAggregateWindow,
}

impl PlannerTransformation {
    const ALL: [Self; 15] = [
        Self::ExpensivePredicatePlacement,
        Self::CteInline,
        Self::CteDemandPushdown,
        Self::AggregatePostReduction,
        Self::MarkJoinToSemi,
        Self::JoinElimination,
        Self::AggregateJoinPreaggregation,
        Self::AggregateJoinSubsumption,
        Self::AggregateNonNullInput,
        Self::AggregateDimensionDeferral,
        Self::AggregateInputMaterialization,
        Self::TopNIntroduction,
        Self::LimitPushdown,
        Self::LatePayloadFetch,
        Self::ScalarAggregateWindow,
    ];

    const fn id(self) -> RuleId {
        match self {
            Self::ExpensivePredicatePlacement => EXPENSIVE_PREDICATE_PLACEMENT_RULE,
            Self::CteInline => CTE_INLINE_RULE,
            Self::CteDemandPushdown => CTE_DEMAND_PUSHDOWN_RULE,
            Self::AggregatePostReduction => AGGREGATE_POST_REDUCTION_RULE,
            Self::MarkJoinToSemi => MARK_JOIN_TO_SEMI_RULE,
            Self::JoinElimination => JOIN_ELIMINATION_RULE,
            Self::AggregateJoinPreaggregation => AGGREGATE_JOIN_PREAGGREGATION_RULE,
            Self::AggregateJoinSubsumption => AGGREGATE_JOIN_SUBSUMPTION_RULE,
            Self::AggregateNonNullInput => AGGREGATE_NON_NULL_INPUT_RULE,
            Self::AggregateDimensionDeferral => AGGREGATE_DIMENSION_DEFERRAL_RULE,
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
            | Self::AggregateJoinSubsumption
            | Self::JoinElimination => Some(CardinalityRecipeKind::ConstraintRefined),
            _ => None,
        }
    }

    /// Earlier equivalence proofs whose semantic shape this rule is allowed
    /// to consume through a child group. Rules not listed here remain
    /// alternatives for costing; they cannot silently change this rule's
    /// input just because their numeric id happens to sort later.
    const fn semantic_dependencies(self) -> &'static [RuleId] {
        match self {
            // Subsumption recognizes the explicit semi-join produced when a
            // positive mark is consumed by its filter.
            Self::AggregateJoinSubsumption => &[MARK_JOIN_TO_SEMI_RULE],
            _ => &[],
        }
    }
}

fn validate_semantic_dependencies() -> Result<()> {
    fn visit(
        transformation: PlannerTransformation,
        visiting: &mut BTreeSet<RuleId>,
        visited: &mut BTreeSet<RuleId>,
    ) -> Result<()> {
        if visited.contains(&transformation.id()) {
            return Ok(());
        }
        if !visiting.insert(transformation.id()) {
            return Err(paro_error::internal(format!(
                "optimizer transformation dependency cycle contains rule {}",
                transformation.id().0
            )));
        }
        for dependency in transformation.semantic_dependencies() {
            let dependency = PlannerTransformation::ALL
                .iter()
                .copied()
                .find(|candidate| candidate.id() == *dependency)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "optimizer transformation {} depends on unregistered rule {}",
                        transformation.id().0,
                        dependency.0
                    ))
                })?;
            visit(dependency, visiting, visited)?;
        }
        visiting.remove(&transformation.id());
        visited.insert(transformation.id());
        Ok(())
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for transformation in PlannerTransformation::ALL {
        visit(transformation, &mut visiting, &mut visited)?;
    }
    Ok(())
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
                let plan = semantic_plan::materialize(
                    ctx.memo(),
                    &state,
                    expr,
                    self.transformation.semantic_dependencies(),
                )?;
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
                PlannerTransformation::CteInline | PlannerTransformation::CteDemandPushdown
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
        {
            let state = self
                .planner_state
                .read()
                .expect("planner transform state poisoned");
            if !transformed_plan_matches_group_contract(&plan, target_group, ctx.memo(), &state)? {
                debug!(
                    target: targets::OPTIMIZER,
                    rule = self.id().0,
                    group = target_group.index(),
                    output_bindings = ?plan.get_column_bindings(),
                    output_types = ?plan.types(),
                    target_schema = ?ctx.memo().group(target_group).map(|group| &group.schema),
                    "discarded optional transformation before staging an incompatible root contract"
                );
                return Ok(Box::new([]));
            }
        }
        let staged = ctx.with_sidecar_transaction(
            self.planner_state.clone(),
            PlannerTransformState::savepoint,
            PlannerTransformState::rollback_to,
            |memo, state| {
                stage_transformed_expression(
                    StagingRequest::new(
                        plan,
                        Arc::new(column_stats),
                        target_group,
                        self.id(),
                        preserved_region_facet,
                        self.transformation.cardinality_recipe_kind(),
                    ),
                    memo,
                    state,
                )
            },
        )?;
        let source = ctx
            .memo()
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("planner rule lost its source expression"))?;
        Ok(vec![EquivalentExpression {
            target_group,
            key: staged.key,
            payload: staged.payload,
            logical_properties: staged.logical_properties,
            cardinality: staged.cardinality,
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
            let (plan, changed) = ReorderFilter::new().reorder_node(plan, &context);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::CteInline => {
            let (plan, changed) =
                CTEInlining::new(&environment.bind_context).optimize_root_with_change(plan);
            if !changed {
                return Ok(None);
            }
            plan
        }
        PlannerTransformation::CteDemandPushdown => {
            let (plan, changed) = CTEDemandPusher::new(&environment.bind_context)
                .optimize_default_root_with_change(plan);
            if changed {
                plan
            } else {
                let (plan, changed) =
                    CTEFilterPusher::new().optimize_default_root_with_change(plan);
                if !changed {
                    return Ok(None);
                }
                plan
            }
        }
        PlannerTransformation::AggregatePostReduction => {
            let (plan, changed) =
                post_reduction::optimize_plan_with_change(plan, &environment.bind_context);
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

/// A positive mark filter is context-sensitive: its own relational output can
/// still expose the always-true marker, while an ancestor may no longer
/// require it. Rewrite the staged subtree here and let the ordinary group
/// contract check accept the smallest ancestor that preserves its full output.
fn rewrite_positive_consumed_mark_filter(plan: LogicalPlan) -> Option<LogicalPlan> {
    fn rewrite_subtree(plan: LogicalPlan) -> (LogicalPlan, bool) {
        let mut child_changed = false;
        let plan = plan.map_children(|child| {
            let (child, changed) = rewrite_subtree(child);
            child_changed |= changed;
            child
        });
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
            return (plan, child_changed);
        }
        let LogicalPlan {
            id,
            stats,
            operator: LogicalOperator::Filter(filter),
        } = plan
        else {
            unreachable!("positive mark-filter shape was checked")
        };
        let LogicalPlan {
            operator: LogicalOperator::Join(Join::Comparison(mut join)),
            ..
        } = *filter.child
        else {
            unreachable!("positive mark-filter child was checked")
        };
        join.join_type = JoinType::Semi;
        join.mark_index = None;
        join.mark_semantics = paro_planner::operator::MarkJoinSemantics::NotMark;
        join.left_projection_map = paro_planner::operator::ProjectionMap::all();
        join.right_projection_map = paro_planner::operator::ProjectionMap::none();
        (
            LogicalPlan {
                id,
                stats,
                operator: LogicalOperator::Join(Join::Comparison(join)),
            },
            true,
        )
    }

    let (plan, changed) = rewrite_subtree(plan);
    changed.then_some(plan)
}

fn settle_transformed_expression(
    mut plan: LogicalPlan,
    environment: &PlannerRuleEnvironment,
) -> Result<(LogicalPlan, HashMap<ColumnBinding, Arc<ColumnStatistics>>)> {
    // A group-local rewrite such as CTE substitution can expose a fresh
    // Filter(CrossProduct) boundary after the root canonicalization pass.
    // Stage only canonical join semantics so the equivalent expression is
    // never costed as an accidental Cartesian product.
    plan = JoinPredicateNormalizer::new(&environment.bind_context).optimize_plan(plan)?;
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
