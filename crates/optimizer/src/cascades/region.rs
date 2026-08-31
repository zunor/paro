// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Laminar planning-region ownership and optional-facet admission.

use std::collections::BTreeSet;

use paro_common::error::{self as paro_error, Result};

use super::cost::SearchCost;
use super::ids::{Fingerprint, GroupId, RegionId, StableFingerprintBuilder};
use super::memo::OptimizationGoal;
use super::rules::CostComposition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegionFacetKind {
    Parameterization,
    Sharing,
    Recursion,
    RuntimeFilter,
    ExactRowset,
    Auxiliary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FacetCriticality {
    Required,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionFacet {
    pub fingerprint: Fingerprint,
    pub kind: RegionFacetKind,
    pub criticality: FacetCriticality,
    pub priority: u16,
    pub scope: BTreeSet<GroupId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionNode {
    pub id: RegionId,
    pub scope: BTreeSet<GroupId>,
    pub facets: Box<[RegionFacet]>,
    pub parent: Option<RegionId>,
}

impl RegionNode {
    pub fn stable_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.planning-region.v1");
        for group in &self.scope {
            fingerprint.write_u64(group.0 as u64);
        }
        for facet in &self.facets {
            fingerprint.write_fingerprint(facet.fingerprint);
        }
        fingerprint.finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegionForest {
    pub nodes: Box<[RegionNode]>,
    pub dropped_optional_facets: Box<[Fingerprint]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegionArtifactKind {
    RuntimeFilter,
    SharedSpool,
    WorkTable,
    ExactRowset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegionOwnedArtifact {
    pub fingerprint: Fingerprint,
    pub kind: RegionArtifactKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegionDependencyKind {
    Data,
    ControlWaitComplete,
    Feedback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegionDependencyEdge {
    pub producer: GroupId,
    pub consumer: GroupId,
    pub kind: RegionDependencyKind,
}

/// Region identity attached by an implementation rule before costing. The
/// engine, rather than the implementation, turns this declaration into a
/// complete [`JointCostProof`] from the canonical physical expression and its
/// resolved child goals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionCandidateContract {
    pub region: RegionId,
    pub facets: Box<[Fingerprint]>,
    pub artifacts: Box<[RegionOwnedArtifact]>,
}

/// Extraction-time record for a non-local candidate. Every field is copied
/// from canonical Memo state or independently checked by WinnerVerifier; a
/// specialized enumerator cannot submit an opaque aggregate-cost assertion.
#[derive(Debug, Clone, PartialEq)]
pub struct JointCostProof {
    pub region: RegionId,
    pub facets: Box<[Fingerprint]>,
    pub owner_group: GroupId,
    pub boundary_goals: Box<[(GroupId, OptimizationGoal)]>,
    pub owned_artifacts: Box<[RegionOwnedArtifact]>,
    pub dependencies: Box<[RegionDependencyEdge]>,
    pub local_cost: SearchCost,
    pub cost_composition: CostComposition,
    pub candidate_fingerprint: Fingerprint,
}

impl RegionForest {
    pub fn normalize(
        facets: impl IntoIterator<Item = RegionFacet>,
        max_composite_region_groups: usize,
        mandatory_complexity_ceiling: usize,
    ) -> Result<Self> {
        if max_composite_region_groups == 0 || mandatory_complexity_ceiling == 0 {
            return Err(paro_error::internal(
                "region group ceilings must be greater than zero",
            ));
        }
        let mut required = Vec::new();
        let mut optional = Vec::new();
        for facet in facets {
            if facet.scope.is_empty() {
                return Err(paro_error::internal(
                    "planning region facet has empty scope",
                ));
            }
            match facet.criticality {
                FacetCriticality::Required => required.push(facet),
                FacetCriticality::Optional => optional.push(facet),
            }
        }
        required.sort_by_key(facet_stable_key);
        optional.sort_by_key(facet_stable_key);

        let mut working = required
            .into_iter()
            .map(WorkingRegion::from_facet)
            .collect::<Vec<_>>();
        normalize_overlaps(&mut working);
        if working
            .iter()
            .any(|region| region.scope.len() > mandatory_complexity_ceiling)
        {
            return Err(paro_error::internal(
                "required planning-region closure exceeds query complexity ceiling",
            ));
        }

        let mut dropped = Vec::new();
        for facet in optional {
            let fingerprint = facet.fingerprint;
            let mut trial = working.clone();
            trial.push(WorkingRegion::from_facet(facet));
            normalize_overlaps(&mut trial);
            let admitted_scope = trial
                .iter()
                .find(|region| {
                    region
                        .facets
                        .iter()
                        .any(|candidate| candidate.fingerprint == fingerprint)
                })
                .map(|region| region.scope.len())
                .expect("trial must contain the optional facet");
            if admitted_scope > max_composite_region_groups {
                dropped.push(fingerprint);
            } else {
                working = trial;
            }
        }

        working.sort_by(|left, right| {
            left.scope
                .len()
                .cmp(&right.scope.len())
                .then_with(|| left.scope.cmp(&right.scope))
                .then_with(|| left.facet_fingerprints().cmp(&right.facet_fingerprints()))
        });
        ensure_laminar(&working)?;

        let mut nodes = working
            .iter()
            .enumerate()
            .map(|(index, region)| RegionNode {
                id: RegionId::new(index),
                scope: region.scope.clone(),
                facets: region.facets.clone().into_boxed_slice(),
                parent: None,
            })
            .collect::<Vec<_>>();
        for child_index in 0..nodes.len() {
            let child_scope = &nodes[child_index].scope;
            let parent = nodes
                .iter()
                .filter(|candidate| {
                    child_scope != &candidate.scope && child_scope.is_subset(&candidate.scope)
                })
                .min_by(|left, right| {
                    left.scope
                        .len()
                        .cmp(&right.scope.len())
                        .then_with(|| left.id.cmp(&right.id))
                })
                .map(|node| node.id);
            nodes[child_index].parent = parent;
        }
        dropped.sort_unstable();
        Ok(Self {
            nodes: nodes.into_boxed_slice(),
            dropped_optional_facets: dropped.into_boxed_slice(),
        })
    }

    pub fn owner_of(&self, group: GroupId) -> Option<RegionId> {
        self.nodes
            .iter()
            .filter(|region| region.scope.contains(&group))
            .min_by(|left, right| {
                left.scope
                    .len()
                    .cmp(&right.scope.len())
                    .then_with(|| left.id.cmp(&right.id))
            })
            .map(|region| region.id)
    }

    pub fn node(&self, id: RegionId) -> Option<&RegionNode> {
        self.nodes.get(id.index()).filter(|node| node.id == id)
    }

    pub fn region_for_facet(&self, fingerprint: Fingerprint) -> Option<RegionId> {
        self.nodes
            .iter()
            .find(|node| {
                node.facets
                    .iter()
                    .any(|facet| facet.fingerprint == fingerprint)
            })
            .map(|node| node.id)
    }
}

#[derive(Debug, Clone)]
struct WorkingRegion {
    scope: BTreeSet<GroupId>,
    facets: Vec<RegionFacet>,
}

impl WorkingRegion {
    fn from_facet(facet: RegionFacet) -> Self {
        Self {
            scope: facet.scope.clone(),
            facets: vec![facet],
        }
    }

    fn facet_fingerprints(&self) -> Vec<Fingerprint> {
        self.facets.iter().map(|facet| facet.fingerprint).collect()
    }
}

fn facet_stable_key(facet: &RegionFacet) -> (u16, Fingerprint) {
    (facet.priority, facet.fingerprint)
}

fn normalize_overlaps(regions: &mut Vec<WorkingRegion>) {
    loop {
        let mut merge_pair = None;
        'outer: for left in 0..regions.len() {
            for right in (left + 1)..regions.len() {
                if regions[left].scope == regions[right].scope
                    || partially_overlaps(&regions[left].scope, &regions[right].scope)
                {
                    merge_pair = Some((left, right));
                    break 'outer;
                }
            }
        }
        let Some((left, right)) = merge_pair else {
            break;
        };
        let mut merged = regions.remove(right);
        regions[left].scope.extend(merged.scope);
        regions[left].facets.append(&mut merged.facets);
        regions[left].facets.sort_by_key(facet_stable_key);
        regions[left].facets.dedup_by_key(|facet| facet.fingerprint);
    }
}

fn partially_overlaps(left: &BTreeSet<GroupId>, right: &BTreeSet<GroupId>) -> bool {
    !left.is_disjoint(right) && !left.is_subset(right) && !right.is_subset(left)
}

fn ensure_laminar(regions: &[WorkingRegion]) -> Result<()> {
    for (index, left) in regions.iter().enumerate() {
        for right in &regions[(index + 1)..] {
            if !left.scope.is_disjoint(&right.scope)
                && !left.scope.is_subset(&right.scope)
                && !right.scope.is_subset(&left.scope)
            {
                return Err(paro_error::internal(
                    "region normalization failed to produce a laminar forest",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facet(id: u128, criticality: FacetCriticality, groups: &[u32]) -> RegionFacet {
        RegionFacet {
            fingerprint: Fingerprint(id),
            kind: if criticality == FacetCriticality::Required {
                RegionFacetKind::Parameterization
            } else {
                RegionFacetKind::RuntimeFilter
            },
            criticality,
            priority: id as u16,
            scope: groups.iter().copied().map(GroupId).collect(),
        }
    }

    #[test]
    fn required_partial_overlap_closes_transitively() {
        let forest = RegionForest::normalize(
            [
                facet(1, FacetCriticality::Required, &[1, 2]),
                facet(2, FacetCriticality::Required, &[2, 3]),
                facet(3, FacetCriticality::Required, &[3, 4]),
            ],
            2,
            8,
        )
        .unwrap();
        assert_eq!(forest.nodes.len(), 1);
        assert_eq!(forest.nodes[0].scope.len(), 4);
        assert_eq!(forest.nodes[0].facets.len(), 3);
    }

    #[test]
    fn optional_facet_yields_before_collapsing_decomposition() {
        let optional = facet(9, FacetCriticality::Optional, &[2, 3]);
        let forest = RegionForest::normalize(
            [
                facet(1, FacetCriticality::Required, &[1, 2]),
                facet(2, FacetCriticality::Required, &[3, 4]),
                optional,
            ],
            3,
            8,
        )
        .unwrap();
        assert_eq!(forest.nodes.len(), 2);
        assert_eq!(forest.dropped_optional_facets.as_ref(), &[Fingerprint(9)]);
    }

    #[test]
    fn contained_regions_form_parent_child_and_have_unique_lowest_owner() {
        let forest = RegionForest::normalize(
            [
                facet(1, FacetCriticality::Required, &[1, 2, 3]),
                facet(2, FacetCriticality::Required, &[1, 2]),
            ],
            8,
            8,
        )
        .unwrap();
        assert_eq!(forest.nodes.len(), 2);
        let owner = forest.owner_of(GroupId(1)).unwrap();
        let node = &forest.nodes[owner.index()];
        assert_eq!(node.scope.len(), 2);
        assert!(node.parent.is_some());
    }
}
