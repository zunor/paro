// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Independently recomputed Memo and winner invariants.

use std::collections::BTreeSet;

use paro_common::error::{self as paro_error, Result};

use super::column::ColumnCatalog;
use super::enforcer::replay_enforcer_chain;
use super::ids::{Fingerprint, GroupId, OptimizationContextId};
use super::memo::{EquivalenceProof, Memo, OptimizationContext};
use super::region::{
    FacetCriticality, RegionArtifactKind, RegionBoundaryEndpoint, RegionDependencyEdge,
    RegionDependencyKind, RegionFacet, RegionFacetKind, RegionScopeContract,
};

pub struct MemoVerifier;

impl MemoVerifier {
    pub fn verify(memo: &Memo, columns: Option<&ColumnCatalog>) -> Result<()> {
        verify_region_forest(memo)?;
        for group in memo.groups() {
            if let Some(columns) = columns {
                group.schema.validate_against(columns)?;
            }
            if group.logical_exprs().is_empty() {
                return Err(paro_error::internal(
                    "canonical Memo group has no logical expression",
                ));
            }
            for &logical_id in group.logical_exprs() {
                let logical = memo
                    .logical_expr(logical_id)
                    .ok_or_else(|| paro_error::internal("group contains unknown logical ID"))?;
                if memo.logical_owner(logical_id) != Some(group.id) {
                    return Err(paro_error::internal(
                        "logical expression is indexed by the wrong Memo group",
                    ));
                }
                if logical
                    .key
                    .children
                    .iter()
                    .any(|child| memo.group(*child).is_none())
                {
                    return Err(paro_error::internal(
                        "logical expression references an unknown child group",
                    ));
                }
                if logical.proofs.is_empty() {
                    return Err(paro_error::internal(
                        "logical expression has no equivalence proof",
                    ));
                }
                for proof in &logical.proofs {
                    match proof {
                        EquivalenceProof::Initial | EquivalenceProof::Normalization { .. } => {}
                        EquivalenceProof::Transformation { source, .. } => {
                            if memo.logical_expr(*source).is_none() {
                                return Err(paro_error::internal(
                                    "transformation proof references unknown source",
                                ));
                            }
                        }
                        EquivalenceProof::SpecializedEnumerator { region, .. } => {
                            if *region == Default::default() {
                                return Err(paro_error::internal(
                                    "specialized-enumerator proof has no region identity",
                                ));
                            }
                        }
                    }
                }
            }
            for &physical_id in group.physical_exprs() {
                let physical = memo
                    .physical_expr(physical_id)
                    .ok_or_else(|| paro_error::internal("group contains unknown physical ID"))?;
                if memo.physical_owner(physical_id) != Some(group.id)
                    || memo.logical_owner(physical.key.logical) != Some(group.id)
                {
                    return Err(paro_error::internal(
                        "physical expression does not implement its owning group",
                    ));
                }
                physical.provided.validate()?;
            }
        }
        WinnerVerifier::verify(memo)
    }
}

pub struct WinnerVerifier;

impl WinnerVerifier {
    pub fn verify(memo: &Memo) -> Result<()> {
        for group in memo.groups() {
            for (goal, frontier) in group.winner_frontiers() {
                verify_optimization_context(memo, group.id, goal.context)?;
                if frontier.candidates().is_empty() {
                    return Err(paro_error::internal("winner frontier is empty"));
                }
                for winner in frontier.candidates() {
                    winner.local_cost.validate()?;
                    winner.cost.validate()?;
                    let physical = memo.physical_expr(winner.expression).ok_or_else(|| {
                        paro_error::internal("winner references unknown physical expression")
                    })?;
                    if memo.physical_owner(winner.expression) != Some(group.id) {
                        return Err(paro_error::internal(
                            "winner references a physical expression from another group",
                        ));
                    }
                    let required = memo.required(goal.required).ok_or_else(|| {
                        paro_error::internal("winner references unknown required properties")
                    })?;
                    let provided = replay_enforcer_chain(
                        physical.provided.clone(),
                        required,
                        &winner.enforcers,
                    )?;
                    if provided != winner.provided || !provided.satisfies(required) {
                        return Err(paro_error::internal(
                            "winner enforcer replay does not satisfy its goal",
                        ));
                    }
                    let mut child_costs = Vec::with_capacity(winner.child_goals.len());
                    let mut child_source_work = Vec::with_capacity(winner.child_goals.len());
                    for (child, child_goal) in &winner.child_goals {
                        verify_optimization_context(memo, *child, child_goal.context)?;
                        let Some(child_winner) = memo
                            .group(*child)
                            .and_then(|group| group.winner(*child_goal))
                        else {
                            return Err(paro_error::internal(
                                "winner child goal has no verified child winner",
                            ));
                        };
                        child_costs.push(child_winner.cost);
                        child_source_work.push(child_winner.source_work.as_ref());
                    }
                    let recomposed = super::engine::compose_candidate_cost_with_sources(
                        winner.local_cost,
                        winner.source_filter_apply_cost,
                        &child_costs,
                        &child_source_work,
                        winner.cost_composition,
                    )?;
                    if recomposed.source_work != winner.source_work {
                        return Err(paro_error::internal(
                            "winner source-work evidence failed independent composition replay",
                        ));
                    }
                    let mut recomputed_cost = super::engine::constrain_composed_cost_to_grant(
                        recomposed.cost,
                        winner.enforcer_cost_input,
                    )?
                    .ok_or_else(|| {
                        paro_error::internal("winner composition exceeds its resource grant")
                    })?;
                    let enforcer_cost = super::engine::enforcer_cost(
                        &winner.enforcers,
                        winner.enforcer_cost_input,
                        memo.calibration(),
                    )?
                    .ok_or_else(|| {
                        paro_error::internal("winner enforcer exceeds its resource grant")
                    })?;
                    recomputed_cost = super::engine::constrain_composed_cost_to_grant(
                        recomputed_cost.sequential(enforcer_cost)?,
                        winner.enforcer_cost_input,
                    )?
                    .ok_or_else(|| {
                        paro_error::internal("winner enforcers exceed its resource grant")
                    })?;
                    if recomputed_cost != winner.cost {
                        return Err(paro_error::internal(
                            "winner cumulative cost failed independent composition replay",
                        ));
                    }
                    verify_joint_cost_proof(memo, group.id, *goal, winner, physical)?;
                }
            }
        }
        Ok(())
    }
}

fn verify_region_forest(memo: &Memo) -> Result<()> {
    let forest = memo.regions();
    let mut facets = BTreeSet::new();
    let dropped: BTreeSet<_> = forest.dropped_optional_facets.iter().copied().collect();
    for node in forest.nodes.iter() {
        if node.scope.is_empty()
            || node
                .scope
                .iter()
                .any(|group| memo.group(*group).is_none() || memo.canonical_group(*group) != *group)
        {
            return Err(paro_error::internal(
                "planning region owns an empty, unknown, or non-canonical group scope",
            ));
        }
        for facet in node.facets.iter() {
            if !facets.insert(facet.fingerprint) || dropped.contains(&facet.fingerprint) {
                return Err(paro_error::internal(
                    "planning facet does not have exactly one admitted region owner",
                ));
            }
            if facet.scope.is_empty() || !facet.scope.is_subset(&node.scope) {
                return Err(paro_error::internal(
                    "planning facet scope escapes its owning composite region",
                ));
            }
            facet.validate_contract()?;
        }
        if let Some(parent) = node.parent {
            let parent = forest
                .node(parent)
                .ok_or_else(|| paro_error::internal("planning region has an unknown parent"))?;
            if node.scope == parent.scope || !node.scope.is_subset(&parent.scope) {
                return Err(paro_error::internal(
                    "planning region parent relation is not strict containment",
                ));
            }
        }
    }
    Ok(())
}

fn region_facet(memo: &Memo, fingerprint: Fingerprint) -> Option<&RegionFacet> {
    let region = memo.regions().region_for_facet(fingerprint)?;
    memo.regions()
        .node(region)?
        .facets
        .iter()
        .find(|facet| facet.fingerprint == fingerprint)
}

fn verify_optimization_context(
    memo: &Memo,
    group: GroupId,
    context: OptimizationContextId,
) -> Result<&OptimizationContext> {
    let group = memo.canonical_group(group);
    let context = memo
        .optimization_context(context)
        .ok_or_else(|| paro_error::internal("winner references an unknown optimization context"))?;
    for fingerprint in context.required_region_facets() {
        let facet = region_facet(memo, *fingerprint).ok_or_else(|| {
            paro_error::internal("optimization context references an unowned planning facet")
        })?;
        if facet.criticality != FacetCriticality::Required
            || facet.scope_contract != RegionScopeContract::Exact
            || !facet.scope.contains(&group)
        {
            return Err(paro_error::internal(
                "optimization context disagrees with its required region scope",
            ));
        }
    }
    Ok(context)
}

fn verify_joint_cost_proof(
    memo: &Memo,
    owner_group: super::ids::GroupId,
    owner_goal: super::memo::OptimizationGoal,
    winner: &super::memo::Winner,
    physical: &super::memo::PhysicalExpr,
) -> Result<()> {
    let Some(proof) = &winner.joint_cost_proof else {
        return Ok(());
    };
    let owner_group = memo.canonical_group(owner_group);
    if memo.canonical_group(proof.owner_group) != owner_group {
        return Err(paro_error::internal(
            "region JointCostProof owner disagrees with its canonical winner group",
        ));
    }
    if memo.physical_owner(winner.expression) != Some(owner_group) {
        return Err(paro_error::internal(
            "region JointCostProof winner expression has a different canonical owner",
        ));
    }
    if proof.local_cost != winner.local_cost {
        return Err(paro_error::internal(
            "region JointCostProof local cost disagrees with its winner",
        ));
    }
    if proof.source_filter_apply_cost != winner.source_filter_apply_cost {
        return Err(paro_error::internal(
            "region JointCostProof source-filter cost disagrees with its winner",
        ));
    }
    if proof.cost_composition != winner.cost_composition {
        return Err(paro_error::internal(
            "region JointCostProof composition disagrees with its winner",
        ));
    }
    let canonical_winner_boundary = winner
        .child_goals
        .iter()
        .map(|(child, goal)| (memo.canonical_group(*child), *goal))
        .collect::<Vec<_>>();
    if proof.boundary_goals.as_ref() != canonical_winner_boundary.as_slice()
        || proof
            .boundary_goals
            .iter()
            .any(|(child, _)| memo.canonical_group(*child) != *child)
    {
        return Err(paro_error::internal(
            "region JointCostProof boundary goals disagree with its canonical winner boundary",
        ));
    }
    let region = memo
        .regions()
        .node(proof.region)
        .ok_or_else(|| paro_error::internal("JointCostProof references an unknown region"))?;
    if !region.scope.contains(&owner_group) {
        return Err(paro_error::internal(
            "JointCostProof owner is outside its region scope",
        ));
    }
    let mut proof_facets = BTreeSet::new();
    for facet in proof.facets.iter().copied() {
        if !proof_facets.insert(facet)
            || memo.regions().region_for_facet(facet) != Some(proof.region)
        {
            return Err(paro_error::internal(
                "JointCostProof facet is duplicated or owned by another region",
            ));
        }
    }
    if proof_facets.is_empty() {
        return Err(paro_error::internal(
            "region-owned winner has no active facet",
        ));
    }
    let runtime_filter_facets = region
        .facets
        .iter()
        .filter(|facet| {
            proof_facets.contains(&facet.fingerprint)
                && facet.kind == RegionFacetKind::RuntimeFilter
        })
        .map(|facet| facet.fingerprint)
        .collect::<BTreeSet<_>>();
    let mut artifacts = BTreeSet::new();
    let mut runtime_filter_artifacts = BTreeSet::new();
    for artifact in proof.owned_artifacts.iter().copied() {
        if !artifacts.insert(artifact.fingerprint) || !proof_facets.contains(&artifact.fingerprint)
        {
            return Err(paro_error::internal(
                "region artifact has no unique active-facet owner",
            ));
        }
        let facet = region
            .facets
            .iter()
            .find(|facet| facet.fingerprint == artifact.fingerprint)
            .ok_or_else(|| paro_error::internal("region artifact lost its facet"))?;
        match artifact.kind {
            RegionArtifactKind::RuntimeFilter if facet.kind == RegionFacetKind::RuntimeFilter => {
                if physical.key.children.len() != 2 {
                    return Err(paro_error::internal(
                        "runtime-filter region candidate is not a binary producer/consumer plan",
                    ));
                }
                if facet.scope_contract != RegionScopeContract::OwnerWithImmediateInputs
                    || !facet.scope.contains(&owner_group)
                {
                    return Err(paro_error::internal(
                        "runtime-filter candidate is not anchored by its declared facet scope",
                    ));
                }
                runtime_filter_artifacts.insert(facet.fingerprint);
            }
            RegionArtifactKind::ExactRowset if facet.kind == RegionFacetKind::ExactRowset => {}
            RegionArtifactKind::SharedSpool if facet.kind == RegionFacetKind::Sharing => {}
            RegionArtifactKind::WorkTable if facet.kind == RegionFacetKind::Recursion => {}
            _ => {
                return Err(paro_error::internal(
                    "region artifact kind disagrees with its owning facet",
                ));
            }
        }
    }
    if runtime_filter_facets != runtime_filter_artifacts || runtime_filter_facets.len() > 1 {
        return Err(paro_error::internal(
            "runtime-filter facet and artifact ownership are not one-to-one",
        ));
    }
    let owns_runtime_filter = !runtime_filter_artifacts.is_empty();
    if physical.key.children.len() != winner.child_goals.len()
        || physical
            .key
            .children
            .iter()
            .zip(winner.child_goals.iter())
            .any(|(physical_child, (goal_child, _))| {
                memo.canonical_group(*physical_child) != memo.canonical_group(*goal_child)
            })
    {
        return Err(paro_error::internal(
            "winner child goals disagree with its physical expression",
        ));
    }
    let candidate_scope = std::iter::once(owner_group)
        .chain(
            physical
                .key
                .children
                .iter()
                .map(|child| memo.canonical_group(*child)),
        )
        .collect::<BTreeSet<_>>();
    if owns_runtime_filter {
        let logical = memo.logical_expr(physical.key.logical).ok_or_else(|| {
            paro_error::internal("runtime-filter physical expression lost its logical owner")
        })?;
        let logical_children = logical
            .key
            .children
            .iter()
            .map(|child| memo.canonical_group(*child))
            .collect::<Vec<_>>();
        let physical_children = physical
            .key
            .children
            .iter()
            .map(|child| memo.canonical_group(*child))
            .collect::<Vec<_>>();
        if logical_children != physical_children {
            return Err(paro_error::internal(
                "runtime-filter candidate boundary disagrees with its logical join inputs",
            ));
        }
        if candidate_scope.len() > usize::from(memo.budget().max_composite_region_groups) {
            return Err(paro_error::internal(
                "runtime-filter candidate span exceeds the composite-region bound",
            ));
        }
        // The region forest independently declares this facet's dynamic
        // owner-plus-input boundary above. Expression-path context completes
        // that contract: a semantic group may occur under a shared owner and
        // in an equivalent inline sibling, so GroupId alone cannot establish
        // whether the selected inputs cross a required region boundary.
        if winner
            .child_goals
            .iter()
            .any(|(_, child_goal)| child_goal.context != owner_goal.context)
        {
            return Err(paro_error::internal(
                "runtime-filter candidate crosses a required planning-region boundary",
            ));
        }
    } else if physical
        .key
        .children
        .iter()
        .any(|child| !region.scope.contains(&memo.canonical_group(*child)))
    {
        return Err(paro_error::internal(
            "JointCostProof dependency endpoint escapes its owned region scope",
        ));
    }
    let mut expected_dependencies = winner
        .child_goals
        .iter()
        .map(|(child, _)| RegionDependencyEdge {
            producer: memo.canonical_group(*child),
            consumer: owner_group,
            kind: RegionDependencyKind::Data,
        })
        .collect::<Vec<_>>();
    let mut dependency_count_by_artifact = std::collections::BTreeMap::new();
    let expected_runtime_filter_boundary =
        super::planner::runtime_filter_dependency_boundary(physical.key.implementation);
    for dependency in &proof.artifact_dependencies {
        let Some(artifact) = proof
            .owned_artifacts
            .iter()
            .find(|artifact| artifact.fingerprint == dependency.artifact)
        else {
            return Err(paro_error::internal(
                "JointCostProof artifact dependency has no owned artifact",
            ));
        };
        if artifact.kind == RegionArtifactKind::RuntimeFilter {
            let Some((expected_producer, expected_consumer)) = expected_runtime_filter_boundary
            else {
                return Err(paro_error::internal(
                    "runtime-filter artifact is attached to a non-runtime-filter implementation",
                ));
            };
            if dependency.producer != expected_producer
                || dependency.consumer != expected_consumer
                || dependency.kind != RegionDependencyKind::ControlWaitComplete
            {
                return Err(paro_error::internal(
                    "runtime-filter artifact boundary disagrees with its physical implementation",
                ));
            }
        }
        let producer = resolve_region_boundary_endpoint(
            memo,
            owner_group,
            &physical.key.children,
            dependency.producer,
        )?;
        let consumer = resolve_region_boundary_endpoint(
            memo,
            owner_group,
            &physical.key.children,
            dependency.consumer,
        )?;
        expected_dependencies.push(RegionDependencyEdge {
            producer,
            consumer,
            kind: dependency.kind,
        });
        *dependency_count_by_artifact
            .entry(dependency.artifact)
            .or_insert(0usize) += 1;
    }
    for artifact in &proof.owned_artifacts {
        if artifact.kind == RegionArtifactKind::RuntimeFilter
            && dependency_count_by_artifact
                .get(&artifact.fingerprint)
                .copied()
                != Some(1)
        {
            return Err(paro_error::internal(
                "runtime-filter artifact must have exactly one verified boundary",
            ));
        }
    }
    expected_dependencies.sort_unstable();
    if proof.dependencies.as_ref() != expected_dependencies.as_slice()
        || proof.dependencies.iter().any(|dependency| {
            let producer = memo.canonical_group(dependency.producer);
            let consumer = memo.canonical_group(dependency.consumer);
            let endpoints_owned = if owns_runtime_filter {
                candidate_scope.contains(&producer) && candidate_scope.contains(&consumer)
            } else {
                region.scope.contains(&producer) && region.scope.contains(&consumer)
            };
            dependency.producer != producer
                || dependency.consumer != consumer
                || !endpoints_owned
                || dependency.producer == dependency.consumer
        })
    {
        return Err(paro_error::internal(
            "JointCostProof dependency DAG disagrees with its canonical fragment",
        ));
    }
    if !region_dependency_dag_is_acyclic(&proof.dependencies) {
        return Err(paro_error::internal(
            "JointCostProof data/control dependencies contain a cycle",
        ));
    }
    Ok(())
}

fn resolve_region_boundary_endpoint(
    memo: &Memo,
    owner_group: GroupId,
    physical_children: &[GroupId],
    endpoint: RegionBoundaryEndpoint,
) -> Result<GroupId> {
    match endpoint {
        RegionBoundaryEndpoint::Owner => Ok(owner_group),
        RegionBoundaryEndpoint::Input(ordinal) => physical_children
            .get(usize::from(ordinal))
            .map(|child| memo.canonical_group(*child))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "JointCostProof artifact dependency references missing physical input {ordinal}"
                ))
            }),
    }
}

fn region_dependency_dag_is_acyclic(dependencies: &[RegionDependencyEdge]) -> bool {
    let edges = dependencies
        .iter()
        .filter(|edge| edge.kind != RegionDependencyKind::Feedback)
        .copied()
        .collect::<Vec<_>>();
    let mut nodes = BTreeSet::new();
    let mut indegree = std::collections::BTreeMap::new();
    for edge in &edges {
        nodes.insert(edge.producer);
        nodes.insert(edge.consumer);
        indegree.entry(edge.producer).or_insert(0usize);
        *indegree.entry(edge.consumer).or_insert(0usize) += 1;
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(node, degree)| (*degree == 0).then_some(*node))
        .collect::<BTreeSet<_>>();
    let mut visited = 0usize;
    while let Some(node) = ready.pop_first() {
        visited += 1;
        for edge in edges.iter().filter(|edge| edge.producer == node) {
            let degree = indegree
                .get_mut(&edge.consumer)
                .expect("dependency consumer was indexed");
            *degree -= 1;
            if *degree == 0 {
                ready.insert(edge.consumer);
            }
        }
    }
    visited == nodes.len()
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;

    use super::*;
    use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility, GroupSchema};
    use crate::cascades::cost::{CompactRange, SearchCost};
    use crate::cascades::ids::{
        AdmissibleGrantSetId, ColumnId, LogicalPayloadId, ObjectiveProfileId, PhysicalPayloadId,
    };
    use crate::cascades::memo::{
        GrantGoalKey, GroupCardinality, LogicalExprKey, LogicalProperties, PhysicalExprKey,
        RowGoal, Winner,
    };
    use crate::cascades::properties::{
        ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning,
        ProvidedProperties, ProvidedReplayability, ProvidedRepresentation, RequiredProperties,
        ResultGuarantee,
    };
    use crate::cascades::region::{
        RegionArtifactDependencyContract, RegionForest, RegionOwnedArtifact,
    };
    use crate::cascades::rules::CostComposition;

    fn schema(column: u32) -> GroupSchema {
        GroupSchema::new([ColumnDesc {
            id: ColumnId(column),
            logical_type: LogicalType::Integer,
            nullable: false,
            origin: ColumnOrigin::Derived {
                key: Fingerprint(column as u128),
            },
            visibility: ColumnVisibility::Visible,
            name_hint: None,
        }])
        .unwrap()
    }

    fn add_logical_group(
        memo: &mut Memo,
        operator: u128,
        children: impl IntoIterator<Item = GroupId>,
    ) -> (GroupId, crate::cascades::ids::LogicalExprId) {
        let group = memo.create_group(
            schema(operator as u32),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let logical = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(operator),
                    scalars: Box::new([]),
                    children: children.into_iter().collect::<Vec<_>>().into_boxed_slice(),
                },
                LogicalPayloadId(operator as u32),
                EquivalenceProof::Initial,
            )
            .unwrap();
        (group, logical)
    }

    fn provided() -> ProvidedProperties {
        ProvidedProperties {
            ordering: ProvidedOrdering::Unordered,
            partitioning: ProvidedPartitioning::Singleton,
            materialization: ProvidedMaterialization::default(),
            mutation_safety: ProvidedMutationSafety::NotApplicable,
            representation: ProvidedRepresentation::Flat,
            replayability: ProvidedReplayability::OnePass,
            result_guarantee: ResultGuarantee::Exact,
        }
    }

    fn verify_runtime_filter_boundary(
        implementation: crate::cascades::ids::ImplementationId,
        declared_producer: RegionBoundaryEndpoint,
        declared_consumer: RegionBoundaryEndpoint,
        proof_producer_ordinal: usize,
        proof_consumer_ordinal: usize,
    ) -> Result<()> {
        let mut memo = Memo::new(Default::default());
        let (first, _) = add_logical_group(&mut memo, 10, []);
        let (second, _) = add_logical_group(&mut memo, 11, []);
        let children = [first, second];
        let (owner, logical) = add_logical_group(&mut memo, 12, children);
        let facet = Fingerprint(210);
        memo.set_regions(
            RegionForest::normalize(
                [RegionFacet {
                    fingerprint: facet,
                    kind: RegionFacetKind::RuntimeFilter,
                    criticality: FacetCriticality::Optional,
                    priority: 1,
                    scope_contract: RegionScopeContract::OwnerWithImmediateInputs,
                    scope: [owner].into_iter().collect(),
                }],
                8,
                8,
            )
            .unwrap(),
        );
        let physical = memo
            .insert_physical(
                owner,
                PhysicalExprKey {
                    implementation,
                    logical,
                    children: children.into(),
                    payload_fingerprint: Fingerprint(211),
                },
                PhysicalPayloadId(0),
                provided(),
            )
            .unwrap();
        let required = memo.intern_required(RequiredProperties::default()).unwrap();
        let goal = crate::cascades::memo::OptimizationGoal {
            required,
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let child_goals = Box::new([(first, goal), (second, goal)]);
        let mut dependencies = vec![
            RegionDependencyEdge {
                producer: first,
                consumer: owner,
                kind: RegionDependencyKind::Data,
            },
            RegionDependencyEdge {
                producer: second,
                consumer: owner,
                kind: RegionDependencyKind::Data,
            },
            RegionDependencyEdge {
                producer: children[proof_producer_ordinal],
                consumer: children[proof_consumer_ordinal],
                kind: RegionDependencyKind::ControlWaitComplete,
            },
        ];
        dependencies.sort_unstable();
        let winner = Winner {
            expression: physical,
            child_goals: child_goals.clone(),
            enforcers: Box::new([]),
            enforcer_cost_input: crate::cascades::engine::EnforcerCostInput::unbounded(
                CompactRange::point(1.0).unwrap(),
                8,
            ),
            provided: provided(),
            local_cost: SearchCost::ZERO,
            source_filter_apply_cost: None,
            cost_composition: CostComposition::Sequential,
            cost: SearchCost::ZERO,
            source_work: Box::new([]),
            physical_fingerprint: Fingerprint(212),
            joint_cost_proof: Some(crate::cascades::region::JointCostProof {
                region: memo.regions().region_for_facet(facet).unwrap(),
                facets: Box::new([facet]),
                owner_group: owner,
                boundary_goals: child_goals,
                owned_artifacts: Box::new([RegionOwnedArtifact {
                    fingerprint: facet,
                    kind: RegionArtifactKind::RuntimeFilter,
                }]),
                artifact_dependencies: Box::new([RegionArtifactDependencyContract {
                    artifact: facet,
                    producer: declared_producer,
                    consumer: declared_consumer,
                    kind: RegionDependencyKind::ControlWaitComplete,
                }]),
                dependencies: dependencies.into_boxed_slice(),
                local_cost: SearchCost::ZERO,
                source_filter_apply_cost: None,
                cost_composition: CostComposition::Sequential,
            }),
        };

        verify_joint_cost_proof(
            &memo,
            owner,
            goal,
            &winner,
            memo.physical_expr(physical).unwrap(),
        )
    }

    #[test]
    fn build_right_runtime_filter_accepts_build_to_probe_boundary() {
        verify_runtime_filter_boundary(
            super::super::planner::PLANNER_HASH_JOIN_RUNTIME_FILTER,
            RegionBoundaryEndpoint::Input(1),
            RegionBoundaryEndpoint::Input(0),
            1,
            0,
        )
        .unwrap();
    }

    #[test]
    fn build_right_runtime_filter_rejects_reversed_boundary() {
        let error = verify_runtime_filter_boundary(
            super::super::planner::PLANNER_HASH_JOIN_RUNTIME_FILTER,
            RegionBoundaryEndpoint::Input(0),
            RegionBoundaryEndpoint::Input(1),
            0,
            1,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("boundary disagrees with its physical implementation"));
    }

    #[test]
    fn build_left_runtime_filter_accepts_build_to_probe_boundary() {
        verify_runtime_filter_boundary(
            super::super::planner::PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER,
            RegionBoundaryEndpoint::Input(0),
            RegionBoundaryEndpoint::Input(1),
            0,
            1,
        )
        .unwrap();
    }

    #[test]
    fn build_left_runtime_filter_rejects_reversed_boundary() {
        let error = verify_runtime_filter_boundary(
            super::super::planner::PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER,
            RegionBoundaryEndpoint::Input(1),
            RegionBoundaryEndpoint::Input(0),
            1,
            0,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("boundary disagrees with its physical implementation"));
    }

    #[test]
    fn optimization_context_is_checked_against_required_region_membership() {
        let mut memo = Memo::new(Default::default());
        let (inside, _) = add_logical_group(&mut memo, 1, []);
        let (outside, _) = add_logical_group(&mut memo, 2, []);
        let fingerprint = Fingerprint(100);
        memo.set_regions(
            RegionForest::normalize(
                [RegionFacet {
                    fingerprint,
                    kind: RegionFacetKind::Parameterization,
                    criticality: FacetCriticality::Required,
                    priority: 1,
                    scope_contract: RegionScopeContract::Exact,
                    scope: [inside].into_iter().collect(),
                }],
                8,
                8,
            )
            .unwrap(),
        );
        let context = memo
            .intern_optimization_context(OptimizationContext::new([fingerprint]))
            .unwrap();

        verify_optimization_context(&memo, inside, context).unwrap();
        let error = verify_optimization_context(&memo, outside, context).unwrap_err();
        assert!(error
            .to_string()
            .contains("disagrees with its required region scope"));
    }

    #[test]
    fn runtime_filter_boundary_must_match_the_logical_join_inputs() {
        let mut memo = Memo::new(Default::default());
        let (probe, _) = add_logical_group(&mut memo, 1, []);
        let (build, _) = add_logical_group(&mut memo, 2, []);
        let (unrelated, _) = add_logical_group(&mut memo, 3, []);
        let (owner, logical) = add_logical_group(&mut memo, 4, [probe, build]);
        let facet = Fingerprint(200);
        memo.set_regions(
            RegionForest::normalize(
                [RegionFacet {
                    fingerprint: facet,
                    kind: RegionFacetKind::RuntimeFilter,
                    criticality: FacetCriticality::Optional,
                    priority: 1,
                    scope_contract: RegionScopeContract::OwnerWithImmediateInputs,
                    scope: [owner].into_iter().collect(),
                }],
                8,
                8,
            )
            .unwrap(),
        );
        let physical = memo
            .insert_physical(
                owner,
                PhysicalExprKey {
                    implementation: super::super::planner::PLANNER_HASH_JOIN_RUNTIME_FILTER,
                    logical,
                    children: Box::new([probe, unrelated]),
                    payload_fingerprint: Fingerprint(300),
                },
                PhysicalPayloadId(0),
                provided(),
            )
            .unwrap();
        let required = memo.intern_required(RequiredProperties::default()).unwrap();
        let goal = crate::cascades::memo::OptimizationGoal {
            required,
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let child_goals = Box::new([(probe, goal), (unrelated, goal)]);
        let mut dependencies = vec![
            RegionDependencyEdge {
                producer: probe,
                consumer: owner,
                kind: RegionDependencyKind::Data,
            },
            RegionDependencyEdge {
                producer: unrelated,
                consumer: owner,
                kind: RegionDependencyKind::Data,
            },
            RegionDependencyEdge {
                producer: unrelated,
                consumer: probe,
                kind: RegionDependencyKind::ControlWaitComplete,
            },
        ];
        dependencies.sort_unstable();
        let winner = Winner {
            expression: physical,
            child_goals: child_goals.clone(),
            enforcers: Box::new([]),
            enforcer_cost_input: crate::cascades::engine::EnforcerCostInput::unbounded(
                CompactRange::point(1.0).unwrap(),
                8,
            ),
            provided: provided(),
            local_cost: SearchCost::ZERO,
            source_filter_apply_cost: None,
            cost_composition: CostComposition::Sequential,
            cost: SearchCost::ZERO,
            source_work: Box::new([]),
            physical_fingerprint: Fingerprint(400),
            joint_cost_proof: Some(crate::cascades::region::JointCostProof {
                region: memo.regions().region_for_facet(facet).unwrap(),
                facets: Box::new([facet]),
                owner_group: owner,
                boundary_goals: child_goals,
                owned_artifacts: Box::new([RegionOwnedArtifact {
                    fingerprint: facet,
                    kind: RegionArtifactKind::RuntimeFilter,
                }]),
                artifact_dependencies: Box::new([
                    crate::cascades::region::RegionArtifactDependencyContract {
                        artifact: facet,
                        producer: RegionBoundaryEndpoint::Input(1),
                        consumer: RegionBoundaryEndpoint::Input(0),
                        kind: RegionDependencyKind::ControlWaitComplete,
                    },
                ]),
                dependencies: dependencies.into_boxed_slice(),
                local_cost: SearchCost::ZERO,
                source_filter_apply_cost: None,
                cost_composition: CostComposition::Sequential,
            }),
        };

        let error = verify_joint_cost_proof(
            &memo,
            owner,
            goal,
            &winner,
            memo.physical_expr(physical).unwrap(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("boundary disagrees with its logical join inputs"));
    }
}
