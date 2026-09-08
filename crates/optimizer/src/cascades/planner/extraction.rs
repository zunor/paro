// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Winner extraction from Memo expressions into verified planner trees.

use super::*;
use paro_planner::expression::ReferenceExpression;
use paro_planner::operator::Projection as LogicalProjection;

use crate::cascades::calibration::{LocalOperatorWork, OP_ENFORCER_STREAM_ROW};

type WinnerContractMap =
    std::collections::HashMap<paro_planner::plan::PlanNodeId, WinnerPhysicalContract>;
type WinnerEnforcerMap =
    std::collections::HashMap<paro_planner::plan::PlanNodeId, Box<[ExtractedEnforcerContract]>>;
pub(super) struct ExtractedWinnerTree {
    plan: OwnedLogicalPlan,
    contracts: WinnerContractMap,
    enforcers: WinnerEnforcerMap,
    output_columns: Box<[ColumnId]>,
}

pub(super) struct PresentedWinnerTree {
    pub(super) plan: OwnedLogicalPlan,
    pub(super) contracts: WinnerContractMap,
    pub(super) enforcers: WinnerEnforcerMap,
    pub(super) physical_fingerprint: Fingerprint,
    pub(super) cost: SearchCost,
}

pub(super) fn extract_planner_tree(
    memo: &Memo,
    state: &PlannerTransformState,
    bind_context: &BindContext,
    root: GroupId,
    goal: OptimizationGoal,
    candidate: super::super::ids::CandidateId,
    mode: SearchMode,
) -> Result<ExtractedWinnerTree> {
    super::super::verifier::WinnerVerifier::verify_candidate_tree(
        memo,
        ChildWinnerRef {
            group: root,
            goal,
            candidate,
        },
    )?;
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
        Visit {
            group: GroupId,
            goal: OptimizationGoal,
            candidate: Option<ChildWinnerRef>,
            occurrence: Fingerprint,
        },
        Build(Box<BuildTask>),
    }

    let mut tasks = vec![Task::Visit {
        group: root,
        goal,
        candidate: Some(ChildWinnerRef {
            group: root,
            goal,
            candidate,
        }),
        occurrence: Fingerprint(0),
    }];
    let mut plans = Vec::new();
    let mut contracts = std::collections::HashMap::new();
    let mut extracted_enforcers = std::collections::HashMap::new();
    let mut root_output_columns = None;
    while let Some(task) = tasks.pop() {
        match task {
            Task::Visit {
                group,
                goal,
                candidate,
                occurrence,
            } => {
                let winner = candidate
                    .map_or_else(
                        || memo.group(group).and_then(|group| group.winner(goal)),
                        |candidate| memo.resolve_child_winner(candidate),
                    )
                    .ok_or_else(|| {
                        paro_error::internal("extraction found no exact group winner")
                    })?;
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
                if memo
                    .group(group)
                    .is_some_and(|group| group.logical_exprs().len() > 1)
                {
                    debug!(
                        target: targets::OPTIMIZER,
                        group = group.index(),
                        logical_expression = physical.key.logical.index(),
                        operator = ?operator_metadata.operator_type,
                        origin_rule = ?operator_metadata.origin_rule.map(|rule| rule.0),
                        implementation = ?implementation,
                        winner_score = winner.cost.score.risk_adjusted,
                        "extracted winner from an equivalence group"
                    );
                }
                if matches!(
                    implementation,
                    PhysicalImplementationFlavor::HashJoin
                        | PhysicalImplementationFlavor::HashJoinBuildLeft
                        | PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter
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
                let (region_owner, mut owned_artifacts) = extracted_region_ownership(memo, winner)?;
                // A Memo candidate is a reusable definition, not one execution
                // occurrence. Two CTE domains can choose the same producer
                // candidate while owning independent runtime-filter state.
                for artifact in &mut owned_artifacts {
                    artifact.fingerprint = artifact_instance(artifact.fingerprint, occurrence);
                }
                let mut child_costs = Vec::with_capacity(winner.children.len());
                let mut child_source_work = Vec::with_capacity(winner.children.len());
                for child in &winner.children {
                    let child_winner = memo.resolve_child_winner(*child).ok_or_else(|| {
                        paro_error::internal("winner extraction lost its exact child candidate")
                    })?;
                    child_costs.push(child_winner.cost);
                    child_source_work.push(child_winner.source_work.as_ref());
                }
                let base_cost = crate::cascades::engine::constrain_composed_cost_to_grant(
                    crate::cascades::engine::compose_candidate_cost_with_sources_at(
                        winner.local_cost,
                        winner.source_filter_apply_cost,
                        &child_costs,
                        &child_source_work,
                        winner.cost_composition.clone(),
                        memo.calibration(),
                    )?
                    .cost,
                    winner.enforcer_cost_input,
                )?
                .ok_or_else(|| {
                    paro_error::internal("winner extraction exceeds its resource grant")
                })?;
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
                    child_count: winner.children.len(),
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
                for (ordinal, child) in winner.children.iter().enumerate().rev() {
                    tasks.push(Task::Visit {
                        group: child.group,
                        goal: child.goal,
                        candidate: Some(*child),
                        occurrence: child_occurrence(occurrence, ordinal),
                    });
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
                let mut plan = match &payload.template {
                    PlannerPhysicalTemplate::Logical(logical) => state
                        .payloads
                        .logical
                        .get(logical.index())
                        .ok_or_else(|| {
                            paro_error::internal("physical payload lost its logical semantics")
                        })?
                        .semantic_template
                        .instantiate(bind_context.next_plan_id(), &mut children)?,
                    PlannerPhysicalTemplate::Executable(template) => {
                        duplicate_plan_preserving_indices(template, bind_context.shared().as_ref())
                            .try_map_children(|_| {
                                children.next().ok_or_else(|| {
                                    paro_error::internal("physical extraction lost a child plan")
                                })
                            })?
                    }
                };
                if children.next().is_some() {
                    return Err(paro_error::internal(
                        "physical extraction produced excess child plans",
                    ));
                }
                if matches!(&payload.template, PlannerPhysicalTemplate::Logical(_)) {
                    plan = semantic_plan::freeze_output_layout(plan, &output_columns, state)?;
                }
                anchor_output_cardinality(&mut plan, output_estimate);
                contracts.insert(plan.id, base_contract.clone());
                let mut provided = base_contract.provided;
                let mut cumulative_cost = base_contract.cost;
                let enforcer_phase = crate::cascades::engine::enforcer_cost(
                    enforcers.as_ref(),
                    enforcer_cost_input,
                    memo.calibration(),
                )?
                .ok_or_else(|| {
                    paro_error::internal("extracted enforcer exceeds its verified resource grant")
                })?;
                let expected_final_cost = enforcer_phase.compose_after(cumulative_cost)?;
                if !enforcers.is_empty() && expected_final_cost != final_contract.cost {
                    return Err(paro_error::internal(
                        "extracted enforcer chain cost disagrees with the verified winner",
                    ));
                }
                let mut physical_enforcers = Vec::with_capacity(enforcers.len());
                for (index, enforcer) in enforcers.iter().enumerate() {
                    let is_final = index + 1 == enforcers.len();
                    provided = enforcer.apply(provided, &final_contract.required)?;
                    let single_phase = crate::cascades::engine::enforcer_cost(
                        std::slice::from_ref(enforcer),
                        enforcer_cost_input,
                        memo.calibration(),
                    )?
                    .ok_or_else(|| {
                        paro_error::internal(
                            "extracted enforcer step exceeds its verified resource grant",
                        )
                    })?;
                    cumulative_cost = single_phase.compose_after(cumulative_cost)?;
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
                if tasks.is_empty() {
                    root_output_columns = Some(output_columns);
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
    // Winner extraction substitutes equivalent child expressions and freezes
    // physical projection maps. Rebuild every positional uniqueness witness
    // once, on that final tree, before physical lowering is allowed to turn a
    // catalog proof into an execution contract.
    let plan = crate::statistics::unique_keys::refresh_unique_keys(plans.pop().unwrap())?;
    Ok(ExtractedWinnerTree {
        plan,
        contracts,
        enforcers: extracted_enforcers,
        output_columns: root_output_columns
            .ok_or_else(|| paro_error::internal("physical extraction lost root output columns"))?,
    })
}

fn child_occurrence(parent: Fingerprint, ordinal: usize) -> Fingerprint {
    let mut identity = StableFingerprintBuilder::default();
    identity.write_bytes(b"paro.physical-occurrence.v1");
    identity.write_fingerprint(parent);
    identity.write_u64(ordinal as u64);
    identity.finish()
}

fn artifact_instance(definition: Fingerprint, occurrence: Fingerprint) -> Fingerprint {
    let mut identity = StableFingerprintBuilder::default();
    identity.write_bytes(b"paro.auxiliary-artifact-instance.v1");
    identity.write_fingerprint(definition);
    identity.write_fingerprint(occurrence);
    identity.finish()
}

#[cfg(test)]
#[path = "extraction/occurrence_tests.rs"]
mod occurrence_tests;

/// A physical winner may use a different equivalent child expression from
/// the one named by the group's canonical estimation recipe. Anchor the
/// selected row-preserving chain to the group estimate so EXPLAIN and later
/// physical lowering cannot publish contradictory cardinalities for nodes
/// that provably emit the same row domain.
fn anchor_output_cardinality(
    plan: &mut OwnedLogicalPlan,
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
    child: &OwnedLogicalPlan,
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

pub(super) fn enforce_result_presentation(
    extracted: ExtractedWinnerTree,
    presentation: &ResultPresentation,
    bind_context: &BindContext,
    calibration: &MachineCalibrationBundle,
    child_fingerprint: Fingerprint,
    child_cost: SearchCost,
) -> Result<PresentedWinnerTree> {
    let ExtractedWinnerTree {
        plan: child,
        mut contracts,
        enforcers,
        output_columns,
    } = extracted;
    if presentation.columns.len() != presentation.names.len() {
        return Err(paro_error::internal(
            "result presentation columns and names are not aligned",
        ));
    }
    // Aliases are presentation metadata owned by the compiler result schema;
    // they do not require a runtime data movement. Equal ColumnId order is a
    // proved no-op presentation enforcer. A physical projection is mandatory
    // only when equivalence search changed the executable layout.
    if output_columns.as_ref() == presentation.columns.as_ref() {
        return Ok(PresentedWinnerTree {
            plan: child,
            contracts,
            enforcers,
            physical_fingerprint: child_fingerprint,
            cost: child_cost,
        });
    }
    let child_types = child.types();
    let expressions = presentation
        .columns
        .iter()
        .map(|column| {
            let ordinal = output_columns
                .iter()
                .position(|candidate| candidate == column)
                .ok_or_else(|| {
                    paro_error::internal("result presentation references a missing root column")
                })?;
            let logical_type = child_types.get(ordinal).cloned().ok_or_else(|| {
                paro_error::internal("result presentation column lost its physical type")
            })?;
            Ok(Expression::Reference(ReferenceExpression::new(
                ordinal,
                logical_type,
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    let child_id = child.id;
    let projection =
        LogicalProjection::new(bind_context.generate_table_index(), child, expressions)
            .with_visible_names(presentation.names.to_vec());
    let mut plan = OwnedLogicalPlan::new(bind_context, LogicalOperator::Projection(projection));
    if let Some(child_stats) = plan.children().first().map(|child| child.stats.clone()) {
        plan.stats.inherit_cardinality_from(&child_stats);
    }

    let child_contract = enforcers
        .get(&child_id)
        .and_then(|chain| chain.last())
        .map(|enforcer| enforcer.contract.clone())
        .or_else(|| contracts.get(&child_id).cloned())
        .ok_or_else(|| {
            paro_error::internal("result presentation child lost its winner contract")
        })?;
    let rows = match plan.stats.estimated_cardinality {
        Some(cardinality) => CompactRange::new(
            cardinality.min as f64,
            cardinality.expected as f64,
            cardinality.max as f64,
        )?,
        None => CompactRange::new(0.0, 1.0, 4.0)?,
    };
    let mut work = LocalOperatorWork::default();
    work.add(OP_ENFORCER_STREAM_ROW, rows)?;
    let cost = child_cost.sequential(calibration.fold(&work)?)?;
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.result-presentation.v1");
    fingerprint.write_fingerprint(child_fingerprint);
    for column in presentation.columns.iter().copied() {
        fingerprint.write_u64(column.0 as u64);
    }
    for name in presentation.names.iter() {
        fingerprint.write_bytes(name.as_bytes());
    }
    let physical_fingerprint = fingerprint.finish();
    contracts.insert(
        plan.id,
        WinnerPhysicalContract {
            required: child_contract.required.clone(),
            provided: child_contract.provided,
            cost,
            grant: child_contract.grant,
            origin: crate::physical::properties::PlanOrigin::Enforcer(physical_fingerprint),
            goal_fingerprint: physical_fingerprint,
            physical_fingerprint,
            implementation: PhysicalImplementationFlavor::Structural,
            region_owner: None,
            owned_artifacts: Box::new([]),
        },
    );
    Ok(PresentedWinnerTree {
        plan,
        contracts,
        enforcers,
        physical_fingerprint,
        cost,
    })
}
