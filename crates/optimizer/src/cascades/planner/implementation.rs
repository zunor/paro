// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical implementation registry and candidate construction.

use super::*;

fn parallel_tasks_for_goal(
    goal: OptimizationGoal,
    classes: &BTreeMap<crate::cascades::ids::ResourceGrantClassId, ResourceGrantClass>,
) -> Result<u16> {
    match goal.grant {
        GrantGoalKey::Class(class) => classes
            .get(&class)
            .map(|class| class.max_parallel_tasks.max(1))
            .ok_or_else(|| {
                paro_error::internal(
                    "physical implementation references an unknown resource grant class",
                )
            }),
        // Grant-invariant implementations own no task-scaled blocking state.
        // Keeping their local contract single-task avoids smuggling a session
        // setting into an otherwise shareable winner.
        GrantGoalKey::Invariant(_) => Ok(1),
    }
}

pub(super) fn register_implementations(
    registry: &mut ImplementationRegistry,
    planner_state: Arc<RwLock<PlannerTransformState>>,
    grant_classes: Arc<BTreeMap<crate::cascades::ids::ResourceGrantClassId, ResourceGrantClass>>,
    calibration: Arc<MachineCalibrationBundle>,
    force_spill: bool,
) -> Result<()> {
    registry.register_implementation(PlannerBaselineImplementation {
        planner_state: planner_state.clone(),
        grant_classes: grant_classes.clone(),
        calibration: calibration.clone(),
        force_spill,
    })?;
    for (id, flavor) in [
        (
            PLANNER_PERFECT_HASH_AGGREGATE,
            PhysicalImplementationFlavor::PerfectHashAggregate,
        ),
        (
            PLANNER_SORT_RANGE_JOIN,
            PhysicalImplementationFlavor::SortRangeJoin,
        ),
        (
            PLANNER_CLASSIC_IE_JOIN,
            PhysicalImplementationFlavor::ClassicIeJoin,
        ),
        (
            PLANNER_HASH_JOIN_RUNTIME_FILTER,
            PhysicalImplementationFlavor::HashJoinRuntimeFilter,
        ),
        (
            PLANNER_HASH_JOIN_BUILD_LEFT,
            PhysicalImplementationFlavor::HashJoinBuildLeft,
        ),
        (
            PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER,
            PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter,
        ),
        (
            PLANNER_PARTITION_AGGREGATE_WINDOW,
            PhysicalImplementationFlavor::PartitionAggregateWindow,
        ),
        (
            PLANNER_SINGLETON_AGGREGATE_PROJECTION,
            PhysicalImplementationFlavor::SingletonAggregateProjection,
        ),
        (
            PLANNER_EXTERNAL_CROSS_PRODUCT,
            PhysicalImplementationFlavor::CrossProductExternal,
        ),
    ] {
        registry.register_implementation(AlternativeImplementation {
            id,
            flavor,
            planner_state: planner_state.clone(),
            grant_classes: grant_classes.clone(),
            calibration: calibration.clone(),
            force_spill,
        })?;
    }
    registry.register_implementation(PlannerSearchImplementation { planner_state })?;
    Ok(())
}

#[derive(Debug)]
struct PlannerBaselineImplementation {
    planner_state: Arc<RwLock<PlannerTransformState>>,
    grant_classes: Arc<BTreeMap<crate::cascades::ids::ResourceGrantClassId, ResourceGrantClass>>,
    calibration: Arc<MachineCalibrationBundle>,
    force_spill: bool,
}

impl PhysicalImplementation for PlannerBaselineImplementation {
    fn id(&self) -> ImplementationId {
        PLANNER_BASELINE_IMPLEMENTATION
    }

    fn grant_dependency_for(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        self.planner_state
            .read()
            .expect("planner transform state poisoned")
            .metadata
            .get(&expr.payload)
            .map(|metadata| metadata.grant_dependency)
            .unwrap_or(GrantDependencyDescriptor::Sensitive)
    }

    fn matches(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        goal: OptimizationGoal,
        _ctx: &ImplementationContext<'_>,
    ) -> bool {
        self.planner_state
            .read()
            .expect("planner transform state poisoned")
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| metadata.input_context == goal.context)
    }

    fn candidates(
        &self,
        expr: crate::cascades::ids::LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("baseline implementation lost logical expr"))?;
        let planner_state = self
            .planner_state
            .read()
            .expect("planner transform state poisoned");
        let metadata = planner_state
            .metadata
            .get(&logical.payload)
            .ok_or_else(|| paro_error::internal("baseline implementation lost metadata"))?;
        let children = logical.key.children.clone();
        let cost_facts =
            expression_cost_facts(ctx.memo, ctx.group, &children, &metadata.cost_facts)?;
        let child_goals = children
            .iter()
            .copied()
            .zip(metadata.child_required.iter().copied())
            .zip(metadata.child_row_goals.iter().copied())
            .map(|((child, required), row_goal)| {
                (
                    child,
                    OptimizationGoal {
                        required,
                        row_goal: match row_goal {
                            PlannerChildRowGoal::All => RowGoal::All,
                            PlannerChildRowGoal::Parent => goal.row_goal,
                        },
                        context: metadata.child_context,
                        ..goal
                    },
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_u64(self.id().0 as u64);
        fingerprint.write_fingerprint(metadata.operator_fingerprint);
        append_grant_fingerprint(&mut fingerprint, metadata.grant_dependency, goal.grant);
        fingerprint.write_u64(
            (self.force_spill
                && implementation_spillable(metadata, metadata.implementations.baseline))
                as u64,
        );
        let local_cost = implementation_cost(
            metadata,
            &cost_facts,
            metadata.implementations.baseline,
            self.calibration.as_ref(),
            parallel_tasks_for_goal(goal, &self.grant_classes)?,
        )?;
        let spillable = implementation_spillable(metadata, metadata.implementations.baseline);
        let estimated_peak_memory = local_cost.peak_memory_upper;
        let Some(local_cost) = cost_for_grant(
            local_cost,
            metadata.grant_dependency,
            spillable,
            goal.grant,
            &self.grant_classes,
            self.force_spill,
        )?
        else {
            debug!(
                target: targets::OPTIMIZER,
                logical_expression = expr.index(),
                payload = logical.payload.0,
                operator = ?metadata.operator_type,
                implementation = ?metadata.implementations.baseline,
                estimated_peak_memory,
                spillable,
                grant = ?goal.grant,
                "mandatory baseline is infeasible for the resource grant"
            );
            return Ok(Box::new([]));
        };
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children,
                payload_fingerprint: metadata.operator_fingerprint,
            },
            payload: metadata.baseline_payload,
            provided: metadata.provided.clone(),
            child_goals,
            local_cost,
            cost_composition: planner_cost_composition(
                metadata,
                metadata.implementations.baseline,
                &cost_facts,
            )?,
            spillable,
            enforcer_cost_input: planner_enforcer_cost_input(
                &cost_facts,
                goal.grant,
                &self.grant_classes,
            )?,
            physical_fingerprint: fingerprint.finish(),
            region: planner_region_contract(ctx.memo, metadata.required_region_facet)?,
            mandatory: true,
        }]
        .into_boxed_slice())
    }
}

#[derive(Debug)]
struct AlternativeImplementation {
    id: ImplementationId,
    flavor: PhysicalImplementationFlavor,
    planner_state: Arc<RwLock<PlannerTransformState>>,
    grant_classes: Arc<BTreeMap<crate::cascades::ids::ResourceGrantClassId, ResourceGrantClass>>,
    calibration: Arc<MachineCalibrationBundle>,
    force_spill: bool,
}

impl PhysicalImplementation for AlternativeImplementation {
    fn id(&self) -> ImplementationId {
        self.id
    }

    fn grant_dependency_for(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        if self
            .planner_state
            .read()
            .expect("planner transform state poisoned")
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| metadata.implementations.supports(self.flavor))
        {
            GrantDependencyDescriptor::Sensitive
        } else {
            GrantDependencyDescriptor::Invariant
        }
    }

    fn matches(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        goal: OptimizationGoal,
        _ctx: &ImplementationContext<'_>,
    ) -> bool {
        self.planner_state
            .read()
            .expect("planner transform state poisoned")
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| {
                metadata.input_context == goal.context
                    && metadata.implementations.supports(self.flavor)
            })
    }

    fn candidates(
        &self,
        expr: crate::cascades::ids::LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("physical implementation lost logical expr"))?;
        let planner_state = self
            .planner_state
            .read()
            .expect("planner transform state poisoned");
        let metadata = planner_state
            .metadata
            .get(&logical.payload)
            .ok_or_else(|| paro_error::internal("physical implementation lost metadata"))?;
        if !metadata.implementations.supports(self.flavor) {
            return Ok(Box::new([]));
        }
        let children = logical.key.children.clone();
        if self.flavor == PhysicalImplementationFlavor::HashJoinRuntimeFilter
            && children.len().saturating_add(1)
                > usize::from(ctx.memo.budget().max_composite_region_groups)
        {
            return Ok(Box::new([]));
        }
        let cost_facts =
            expression_cost_facts(ctx.memo, ctx.group, &children, &metadata.cost_facts)?;
        let child_goals = children
            .iter()
            .copied()
            .zip(metadata.child_required.iter().copied())
            .zip(metadata.child_row_goals.iter().copied())
            .map(|((child, required), row_goal)| {
                (
                    child,
                    OptimizationGoal {
                        required,
                        row_goal: match row_goal {
                            PlannerChildRowGoal::All => RowGoal::All,
                            PlannerChildRowGoal::Parent => goal.row_goal,
                        },
                        context: metadata.child_context,
                        ..goal
                    },
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_u64(self.id.0 as u64);
        fingerprint.write_fingerprint(metadata.operator_fingerprint);
        append_grant_fingerprint(&mut fingerprint, metadata.grant_dependency, goal.grant);
        fingerprint.write_u64(
            (self.force_spill && implementation_spillable(metadata, self.flavor)) as u64,
        );
        let implementation_cost = implementation_cost(
            metadata,
            &cost_facts,
            self.flavor,
            self.calibration.as_ref(),
            parallel_tasks_for_goal(goal, &self.grant_classes)?,
        )?;
        let Some(local_cost) = cost_for_grant(
            implementation_cost,
            GrantDependencyDescriptor::Sensitive,
            implementation_spillable(metadata, self.flavor),
            goal.grant,
            &self.grant_classes,
            self.force_spill,
        )?
        else {
            return Ok(Box::new([]));
        };
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id,
                logical: expr,
                children,
                payload_fingerprint: metadata.operator_fingerprint,
            },
            payload: metadata.baseline_payload,
            provided: metadata.provided.clone(),
            child_goals,
            local_cost,
            cost_composition: planner_cost_composition(metadata, self.flavor, &cost_facts)?,
            spillable: implementation_spillable(metadata, self.flavor),
            enforcer_cost_input: planner_enforcer_cost_input(
                &cost_facts,
                goal.grant,
                &self.grant_classes,
            )?,
            physical_fingerprint: fingerprint.finish(),
            region: if let Some((producer, consumer)) = runtime_filter_dependency_boundary(self.id)
            {
                Some(planner_runtime_filter_region_contract(
                    ctx.memo,
                    metadata.runtime_filter_region_facet,
                    producer,
                    consumer,
                )?)
            } else {
                planner_region_contract(ctx.memo, metadata.required_region_facet)?
            },
            mandatory: false,
        }]
        .into_boxed_slice())
    }
}

#[derive(Debug)]
struct PlannerSearchImplementation {
    planner_state: Arc<RwLock<PlannerTransformState>>,
}

impl PhysicalImplementation for PlannerSearchImplementation {
    fn id(&self) -> ImplementationId {
        PLANNER_SEARCH_PROVIDER
    }

    fn grant_dependency_for(
        &self,
        _expr: &crate::cascades::memo::LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        GrantDependencyDescriptor::Invariant
    }

    fn matches(
        &self,
        expr: &crate::cascades::memo::LogicalExpr,
        goal: OptimizationGoal,
        _ctx: &ImplementationContext<'_>,
    ) -> bool {
        self.planner_state
            .read()
            .expect("planner transform state poisoned")
            .metadata
            .get(&expr.payload)
            .is_some_and(|metadata| {
                metadata.input_context == goal.context && metadata.search.is_some()
            })
    }

    fn candidates(
        &self,
        expr: crate::cascades::ids::LogicalExprId,
        _goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("search implementation lost logical expr"))?;
        let planner_state = self
            .planner_state
            .read()
            .expect("planner transform state poisoned");
        let Some(search) = planner_state
            .metadata
            .get(&logical.payload)
            .and_then(|metadata| metadata.search.as_ref())
        else {
            return Ok(Box::new([]));
        };
        let cost_facts = expression_cost_facts(ctx.memo, ctx.group, &[], &search.cost_facts)?;
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_u64(self.id().0 as u64);
        fingerprint.write_fingerprint(search.payload_fingerprint);
        Ok(vec![PhysicalCandidate {
            key: PhysicalExprKey {
                implementation: self.id(),
                logical: expr,
                children: Box::new([]),
                payload_fingerprint: search.payload_fingerprint,
            },
            payload: search.payload,
            provided: search.provided.clone(),
            child_goals: Box::new([]),
            local_cost: search.local_cost,
            cost_composition: CostComposition::Sequential,
            spillable: false,
            enforcer_cost_input: crate::cascades::engine::EnforcerCostInput::unbounded(
                cost_facts.output_rows,
                cost_facts.output_row_width,
            ),
            physical_fingerprint: fingerprint.finish(),
            region: planner_region_contract(ctx.memo, None)?,
            mandatory: false,
        }]
        .into_boxed_slice())
    }
}
