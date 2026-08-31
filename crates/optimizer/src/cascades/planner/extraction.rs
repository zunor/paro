// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Winner extraction from Memo expressions into verified planner trees.

use super::*;

pub(super) fn extract_planner_tree(
    memo: &Memo,
    state: &PlannerTransformState,
    bind_context: &BindContext,
    root: GroupId,
    goal: OptimizationGoal,
    mode: SearchMode,
) -> Result<ExtractedWinnerTree> {
    #[derive(Debug)]
    struct BuildTask {
        payload: PhysicalPayloadId,
        child_count: usize,
        output_columns: Box<[ColumnId]>,
        enforcers: Box<[crate::cascades::enforcer::EnforcerStep]>,
        enforcer_cost_input: crate::cascades::engine::EnforcerCostInput,
        base_contract: WinnerPhysicalContract,
        final_contract: WinnerPhysicalContract,
        output_estimate: Option<paro_planner::plan::CardinalityEstimate>,
    }

    #[derive(Debug)]
    enum Task {
        Visit(GroupId, OptimizationGoal),
        Build(Box<BuildTask>),
    }

    let mut tasks = vec![Task::Visit(root, goal)];
    let mut plans = Vec::new();
    let mut contracts = std::collections::HashMap::new();
    let mut extracted_enforcers = std::collections::HashMap::new();
    while let Some(task) = tasks.pop() {
        match task {
            Task::Visit(group, goal) => {
                let winner = memo
                    .group(group)
                    .and_then(|group| group.winner(goal))
                    .ok_or_else(|| paro_error::internal("extraction found no group winner"))?;
                let physical = memo.physical_expr(winner.expression).ok_or_else(|| {
                    paro_error::internal("winner physical expression disappeared")
                })?;
                let logical = memo.logical_expr(physical.key.logical).ok_or_else(|| {
                    paro_error::internal("winner extraction lost logical expression")
                })?;
                let operator_metadata = state.metadata.get(&logical.payload).ok_or_else(|| {
                    paro_error::internal("winner extraction lost implementation metadata")
                })?;
                let implementation = selected_implementation_flavor(
                    physical.key.implementation,
                    operator_metadata.implementations,
                )?;
                if matches!(
                    implementation,
                    PhysicalImplementationFlavor::HashJoin
                        | PhysicalImplementationFlavor::HashJoinRuntimeFilter
                        | PhysicalImplementationFlavor::NestedLoopJoin
                        | PhysicalImplementationFlavor::SortRangeJoin
                        | PhysicalImplementationFlavor::ClassicIeJoin
                ) {
                    debug!(
                        target: targets::OPTIMIZER,
                        group = group.index(),
                        logical_expression = physical.key.logical.index(),
                        implementation = ?implementation,
                        winner_score = winner.cost.score.risk_adjusted,
                        "extracted physical join winner"
                    );
                }
                let required = memo.required(goal.required).cloned().ok_or_else(|| {
                    paro_error::internal("winner extraction lost required properties")
                })?;
                let grant = match goal.grant {
                    GrantGoalKey::Invariant(set) => {
                        crate::physical::properties::PhysicalGrantContract::Invariant(set)
                    }
                    GrantGoalKey::Class(class) => {
                        crate::physical::properties::PhysicalGrantContract::Class(class)
                    }
                };
                let origin = if let Some(proof) = &winner.joint_cost_proof {
                    let region = memo.regions().node(proof.region).ok_or_else(|| {
                        paro_error::internal("winner extraction lost its planning region")
                    })?;
                    crate::physical::properties::PlanOrigin::SpecializedRegion(
                        region.stable_fingerprint(),
                    )
                } else {
                    match mode {
                        SearchMode::Direct => crate::physical::properties::PlanOrigin::Direct,
                        SearchMode::Memo => crate::physical::properties::PlanOrigin::Memo,
                    }
                };
                let (region_owner, owned_artifacts) = extracted_region_ownership(memo, winner)?;
                let mut child_costs = Vec::with_capacity(winner.child_goals.len());
                for (child, child_goal) in &winner.child_goals {
                    let child_cost = memo
                        .group(*child)
                        .and_then(|group| group.winner(*child_goal))
                        .ok_or_else(|| {
                            paro_error::internal("winner extraction lost a child winner")
                        })?
                        .cost;
                    child_costs.push(child_cost);
                }
                let base_cost = crate::cascades::engine::compose_candidate_cost(
                    winner.local_cost,
                    &child_costs,
                    winner.cost_composition,
                )?;
                let base_contract = WinnerPhysicalContract {
                    required: RequiredProperties {
                        result_guarantee: physical.provided.result_guarantee,
                        ..RequiredProperties::default()
                    },
                    provided: physical.provided.clone(),
                    cost: base_cost,
                    grant,
                    origin,
                    goal_fingerprint: physical.key.stable_fingerprint(),
                    physical_fingerprint: physical.key.stable_fingerprint(),
                    implementation,
                    region_owner,
                    owned_artifacts: owned_artifacts.clone(),
                };
                let final_contract = WinnerPhysicalContract {
                    required,
                    provided: winner.provided.clone(),
                    cost: winner.cost,
                    grant,
                    origin,
                    goal_fingerprint: optimization_goal_fingerprint(goal),
                    physical_fingerprint: winner.physical_fingerprint,
                    implementation,
                    region_owner,
                    owned_artifacts,
                };
                tasks.push(Task::Build(Box::new(BuildTask {
                    payload: physical.payload,
                    child_count: winner.child_goals.len(),
                    output_columns: operator_metadata.output_columns.clone(),
                    enforcers: winner.enforcers.clone(),
                    enforcer_cost_input: winner.enforcer_cost_input,
                    base_contract,
                    final_contract,
                    output_estimate: memo.cardinality_estimate(group).map(
                        |(min, expected, max)| paro_planner::plan::CardinalityEstimate {
                            min,
                            expected,
                            max,
                        },
                    ),
                })));
                for (child, child_goal) in winner.child_goals.iter().rev() {
                    tasks.push(Task::Visit(*child, *child_goal));
                }
            }
            Task::Build(task) => {
                let BuildTask {
                    payload,
                    child_count,
                    output_columns,
                    enforcers,
                    enforcer_cost_input,
                    base_contract,
                    final_contract,
                    output_estimate,
                } = *task;
                if plans.len() < child_count {
                    return Err(paro_error::internal(
                        "physical extraction child stack underflow",
                    ));
                }
                let children = plans.split_off(plans.len() - child_count);
                let payload = state
                    .payloads
                    .get_physical(payload)
                    .ok_or_else(|| paro_error::internal("unknown planner physical payload"))?;
                let mut children = children.into_iter();
                let template = match &payload.template {
                    PlannerPhysicalTemplate::Logical(logical) => {
                        &state
                            .payloads
                            .logical
                            .get(logical.index())
                            .ok_or_else(|| {
                                paro_error::internal("physical payload lost its logical semantics")
                            })?
                            .semantic_template
                    }
                    PlannerPhysicalTemplate::Executable(template) => template.as_ref(),
                };
                let mut plan =
                    duplicate_plan_preserving_indices(template, bind_context.shared().as_ref())
                        .try_map_children(|_| {
                            children.next().ok_or_else(|| {
                                paro_error::internal("physical extraction lost a child plan")
                            })
                        })?;
                if children.next().is_some() {
                    return Err(paro_error::internal(
                        "physical extraction produced excess child plans",
                    ));
                }
                if matches!(&payload.template, PlannerPhysicalTemplate::Logical(_)) {
                    plan = semantic_plan::freeze_extraction_layout(plan, &output_columns, state)?;
                }
                anchor_output_cardinality(&mut plan, output_estimate);
                contracts.insert(plan.id, base_contract.clone());
                let mut provided = base_contract.provided;
                let mut cumulative_cost = base_contract.cost;
                let enforcer_cost = crate::cascades::engine::enforcer_cost(
                    enforcers.as_ref(),
                    enforcer_cost_input,
                    memo.calibration(),
                )?
                .ok_or_else(|| {
                    paro_error::internal("extracted enforcer exceeds its verified resource grant")
                })?;
                let expected_final_cost = cumulative_cost.sequential(enforcer_cost)?;
                if !enforcers.is_empty() && expected_final_cost != final_contract.cost {
                    return Err(paro_error::internal(
                        "extracted enforcer chain cost disagrees with the verified winner",
                    ));
                }
                let mut physical_enforcers = Vec::with_capacity(enforcers.len());
                for (index, enforcer) in enforcers.iter().enumerate() {
                    let is_final = index + 1 == enforcers.len();
                    provided = enforcer.apply(provided, &final_contract.required)?;
                    let single_cost = crate::cascades::engine::enforcer_cost(
                        std::slice::from_ref(enforcer),
                        enforcer_cost_input,
                        memo.calibration(),
                    )?
                    .ok_or_else(|| {
                        paro_error::internal(
                            "extracted enforcer step exceeds its verified resource grant",
                        )
                    })?;
                    cumulative_cost = cumulative_cost.sequential(single_cost)?;
                    let mut contract = final_contract.clone();
                    contract.required = if is_final {
                        final_contract.required.clone()
                    } else {
                        RequiredProperties {
                            result_guarantee: provided.result_guarantee,
                            ..RequiredProperties::default()
                        }
                    };
                    contract.provided = provided.clone();
                    contract.cost = if is_final {
                        final_contract.cost
                    } else {
                        cumulative_cost
                    };
                    contract.origin = crate::physical::properties::PlanOrigin::Enforcer(
                        enforcer.stable_fingerprint(),
                    );
                    contract.implementation = PhysicalImplementationFlavor::Structural;
                    contract.region_owner = None;
                    contract.owned_artifacts = Box::new([]);
                    physical_enforcers.push(ExtractedEnforcerContract {
                        enforcer: extract_physical_enforcer(
                            &plan,
                            enforcer,
                            &output_columns,
                            &final_contract.required,
                        )?,
                        contract,
                    });
                }
                if enforcers.is_empty() {
                    contracts.insert(plan.id, final_contract);
                } else if extracted_enforcers
                    .insert(plan.id, physical_enforcers.into_boxed_slice())
                    .is_some()
                {
                    return Err(paro_error::internal(
                        "physical extraction assigned two enforcer chains to one plan node",
                    ));
                }
                plans.push(plan);
            }
        }
    }
    if plans.len() != 1 {
        return Err(paro_error::internal(
            "physical extraction did not produce exactly one root",
        ));
    }
    Ok((plans.pop().unwrap(), contracts, extracted_enforcers))
}

/// A physical winner may use a different equivalent child expression from
/// the one named by the group's canonical estimation recipe. Anchor the
/// selected row-preserving chain to the group estimate so EXPLAIN and later
/// physical lowering cannot publish contradictory cardinalities for nodes
/// that provably emit the same row domain.
fn anchor_output_cardinality(
    plan: &mut LogicalPlan,
    estimate: Option<paro_planner::plan::CardinalityEstimate>,
) {
    plan.stats.estimated_cardinality = estimate;
    let passthrough_child = match &plan.operator {
        LogicalOperator::Projection(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_)
        | LogicalOperator::Order(_)
        | LogicalOperator::Window(_) => Some(0),
        LogicalOperator::MaterializedCTE(_) => Some(1),
        _ => None,
    };
    let Some(target) = passthrough_child else {
        return;
    };
    let mut ordinal = 0;
    let _ = plan.visit_children_mut(|child| {
        if ordinal == target {
            anchor_output_cardinality(child, estimate);
            std::ops::ControlFlow::Break(())
        } else {
            ordinal += 1;
            std::ops::ControlFlow::Continue(())
        }
    });
}

pub(super) fn extract_physical_enforcer(
    child: &LogicalPlan,
    enforcer: &crate::cascades::enforcer::EnforcerStep,
    output_columns: &[ColumnId],
    required: &RequiredProperties,
) -> Result<ExtractedPhysicalEnforcer> {
    match enforcer {
        crate::cascades::enforcer::EnforcerStep::Sort(ordering) => {
            if ordering.scope != OrderingScope::Global {
                return Err(paro_error::not_implemented(
                    "partition-local sort requires an exchange-aware physical ABI",
                ));
            }
            let child_types = child.types();
            let mut orders = Vec::with_capacity(ordering.keys.len());
            for key in &ordering.keys {
                if key.collation.is_some() {
                    return Err(paro_error::not_implemented(
                        "collation-aware sort enforcer is not executable yet",
                    ));
                }
                let index = output_columns
                    .iter()
                    .position(|column| *column == key.column)
                    .ok_or_else(|| {
                        paro_error::internal(
                            "sort enforcer key is absent from the extracted row layout",
                        )
                    })?;
                let logical_type = child_types.get(index).cloned().ok_or_else(|| {
                    paro_error::internal(
                        "sort enforcer key position exceeds the extracted row layout",
                    )
                })?;
                orders.push(OrderByNode {
                    expression: Expression::Reference(ReferenceExpression::new(
                        index,
                        logical_type,
                    )),
                    ascending: key.direction == SortDirection::Asc,
                    nulls_first: key.nulls == NullOrder::First,
                });
            }
            Ok(ExtractedPhysicalEnforcer::Sort {
                orders: orders.into_boxed_slice(),
            })
        }
        crate::cascades::enforcer::EnforcerStep::MutationInputSpool { barrier } => {
            let MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } =
                &required.mutation_safety
            else {
                return Err(paro_error::internal(
                    "mutation input spool has no stable-read requirement",
                ));
            };
            Ok(ExtractedPhysicalEnforcer::MutationInputSpool {
                barrier: *barrier,
                targets: targets.clone(),
                snapshot: *snapshot,
            })
        }
        _ => Err(paro_error::not_implemented(format!(
            "physical extraction has no executable ABI for enforcer {enforcer:?}"
        ))),
    }
}

pub(super) fn extracted_region_ownership(
    memo: &Memo,
    winner: &crate::cascades::memo::Winner,
) -> Result<(
    Option<Fingerprint>,
    Box<[crate::physical::OwnedAuxiliaryArtifact]>,
)> {
    let Some(proof) = &winner.joint_cost_proof else {
        return Ok((None, Box::new([])));
    };
    let region = memo
        .regions()
        .node(proof.region)
        .ok_or_else(|| paro_error::internal("winner region ownership disappeared"))?;
    let artifacts = proof
        .owned_artifacts
        .iter()
        .map(|artifact| crate::physical::OwnedAuxiliaryArtifact {
            fingerprint: artifact.fingerprint,
            kind: match artifact.kind {
                RegionArtifactKind::RuntimeFilter => {
                    crate::physical::AuxiliaryArtifactKind::RuntimeFilter
                }
                RegionArtifactKind::SharedSpool => {
                    crate::physical::AuxiliaryArtifactKind::SharedSpool
                }
                RegionArtifactKind::WorkTable => crate::physical::AuxiliaryArtifactKind::WorkTable,
                RegionArtifactKind::ExactRowset => {
                    crate::physical::AuxiliaryArtifactKind::ExactRowset
                }
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok((Some(region.stable_fingerprint()), artifacts))
}
