// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Independently recomputed Memo and winner invariants.

use std::collections::BTreeSet;

use paro_common::error::{self as paro_error, Result};

use super::column::ColumnCatalog;
use super::enforcer::replay_enforcer_chain;
use super::memo::{EquivalenceProof, Memo};
use super::region::{
    RegionArtifactKind, RegionDependencyEdge, RegionDependencyKind, RegionFacetKind,
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
                    for (child, child_goal) in &winner.child_goals {
                        let Some(child_winner) = memo
                            .group(*child)
                            .and_then(|group| group.winner(*child_goal))
                        else {
                            return Err(paro_error::internal(
                                "winner child goal has no verified child winner",
                            ));
                        };
                        child_costs.push(child_winner.cost);
                    }
                    let mut recomputed_cost = super::engine::constrain_composed_cost_to_grant(
                        super::engine::compose_candidate_cost(
                            winner.local_cost,
                            &child_costs,
                            winner.cost_composition,
                        )?,
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
                    verify_joint_cost_proof(memo, group.id, winner, physical)?;
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

fn verify_joint_cost_proof(
    memo: &Memo,
    owner_group: super::ids::GroupId,
    winner: &super::memo::Winner,
    physical: &super::memo::PhysicalExpr,
) -> Result<()> {
    let Some(proof) = &winner.joint_cost_proof else {
        return Ok(());
    };
    let owner_group = memo.canonical_group(owner_group);
    if memo.canonical_group(proof.owner_group) != owner_group
        || memo.physical_owner(winner.expression) != Some(owner_group)
        || proof.local_cost != winner.local_cost
        || proof.cost_composition != winner.cost_composition
        || proof.boundary_goals.as_ref() != winner.child_goals.as_ref()
    {
        return Err(paro_error::internal(
            "region JointCostProof disagrees with canonical winner ownership or cost inputs",
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
    let mut artifacts = BTreeSet::new();
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
    if physical
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
    if proof
        .owned_artifacts
        .iter()
        .any(|artifact| artifact.kind == RegionArtifactKind::RuntimeFilter)
    {
        let [(probe, _), (build, _)] = winner.child_goals.as_ref() else {
            return Err(paro_error::internal(
                "runtime-filter JointCostProof is not a binary join",
            ));
        };
        expected_dependencies.push(RegionDependencyEdge {
            producer: memo.canonical_group(*build),
            consumer: memo.canonical_group(*probe),
            kind: RegionDependencyKind::ControlWaitComplete,
        });
    }
    expected_dependencies.sort_unstable();
    if proof.dependencies.as_ref() != expected_dependencies.as_slice()
        || proof.dependencies.iter().any(|dependency| {
            !region
                .scope
                .contains(&memo.canonical_group(dependency.producer))
                || !region
                    .scope
                    .contains(&memo.canonical_group(dependency.consumer))
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
