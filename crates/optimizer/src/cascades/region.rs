// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Laminar planning-region ownership and optional-facet admission.

use std::collections::{BTreeMap, BTreeSet, HashMap};

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

/// How a facet's admitted scope constrains a selected physical candidate.
///
/// Most facets own the complete set of groups recorded in [`RegionFacet::scope`].
/// Runtime filters are different: their probe/build groups are chosen by the
/// winning join expression, so eagerly unioning every alternative's children
/// would collapse otherwise independent regions. Their declared scope is an
/// anchor set and the verifier expands exactly one selected owner to its
/// immediate inputs, under the owner's expression-path context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegionScopeContract {
    Exact,
    OwnerWithImmediateInputs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionFacet {
    pub fingerprint: Fingerprint,
    pub kind: RegionFacetKind,
    pub criticality: FacetCriticality,
    pub priority: u16,
    pub scope_contract: RegionScopeContract,
    pub scope: BTreeSet<GroupId>,
}

impl RegionFacet {
    /// Validate the invariant carried by the facet itself, independently of
    /// the forest node that eventually owns it.
    ///
    /// A runtime filter is an optional physical capability whose concrete
    /// producer/consumer boundary is selected with the winning join. Every
    /// other current facet describes an exact, eagerly materialized scope.
    pub fn validate_contract(&self) -> Result<()> {
        if self.scope.is_empty() {
            return Err(paro_error::internal(
                "planning region facet has empty scope",
            ));
        }
        match self.kind {
            RegionFacetKind::RuntimeFilter
                if self.criticality == FacetCriticality::Optional
                    && self.scope_contract == RegionScopeContract::OwnerWithImmediateInputs =>
            {
                Ok(())
            }
            RegionFacetKind::RuntimeFilter => Err(paro_error::internal(
                "runtime-filter facet must be optional and owner-with-immediate-inputs scoped",
            )),
            _ if self.scope_contract == RegionScopeContract::Exact => Ok(()),
            _ => Err(paro_error::internal(
                "non-runtime planning facet must have an exact scope contract",
            )),
        }
    }
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

/// One endpoint of a candidate-local dependency before Memo groups are
/// resolved. Input ordinals are stable in the [`PhysicalCandidate`](crate::cascades::rules::PhysicalCandidate)
/// contract and deliberately do not imply left/right or build/probe semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegionBoundaryEndpoint {
    Owner,
    Input(u16),
}

impl RegionBoundaryEndpoint {
    pub const fn stable_tag(self) -> (u64, u64) {
        match self {
            Self::Owner => (0, 0),
            Self::Input(ordinal) => (1, ordinal as u64),
        }
    }
}

/// A typed artifact edge in candidate-local coordinates.
///
/// The engine resolves this declaration through child goals. WinnerVerifier
/// independently replays it through the physical expression's child list, so
/// a proof cannot silently reverse build and probe while remaining self-valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegionArtifactDependencyContract {
    pub artifact: Fingerprint,
    pub producer: RegionBoundaryEndpoint,
    pub consumer: RegionBoundaryEndpoint,
    pub kind: RegionDependencyKind,
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
    pub artifact_dependencies: Box<[RegionArtifactDependencyContract]>,
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
    pub artifact_dependencies: Box<[RegionArtifactDependencyContract]>,
    pub dependencies: Box<[RegionDependencyEdge]>,
    pub local_cost: SearchCost,
    pub source_filter_apply_cost: Option<SearchCost>,
    pub cost_composition: CostComposition,
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
        // A singleton cannot partially overlap any set. Its only closure
        // partner is the identical singleton, independently of admission
        // order. Keep these anchor regions out of the overlap algorithm:
        // runtime-filter anchors are numerous, but do not make the search
        // space of composite ownership any larger.
        let mut singletons = BTreeMap::<GroupId, WorkingRegion>::new();
        for facet in facets {
            facet.validate_contract()?;
            if facet.scope.len() == 1 {
                let group = *facet.scope.first().expect("validated nonempty scope");
                match singletons.entry(group) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().facets.push(facet);
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(WorkingRegion::from_facet(facet));
                    }
                }
                continue;
            }
            match facet.criticality {
                FacetCriticality::Required => required.push(facet),
                FacetCriticality::Optional => optional.push(facet),
            }
        }
        required.sort_by_key(facet_stable_key);
        optional.sort_by_key(facet_stable_key);

        let mut working = required.into_iter().map(WorkingRegion::from_facet).fold(
            Vec::new(),
            |mut working, candidate| {
                insert_overlap_closure(&mut working, candidate);
                working
            },
        );
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
            let candidate = overlap_closure(&working, WorkingRegion::from_facet(facet));
            if candidate.region.scope.len() > max_composite_region_groups {
                dropped.push(fingerprint);
            } else {
                commit_overlap_closure(&mut working, candidate);
            }
        }

        for mut region in singletons.into_values() {
            region.facets.sort_by_key(facet_stable_key);
            region.facets.dedup_by_key(|facet| facet.fingerprint);
            working.push(region);
        }

        working.sort_by(|left, right| {
            left.scope
                .len()
                .cmp(&right.scope.len())
                .then_with(|| left.scope.cmp(&right.scope))
                .then_with(|| left.facet_fingerprints().cmp(&right.facet_fingerprints()))
        });
        let parents = laminar_parents(&working)?;

        let nodes = working
            .into_iter()
            .enumerate()
            .map(|(index, region)| RegionNode {
                id: RegionId::new(index),
                scope: region.scope,
                facets: region.facets.into_boxed_slice(),
                parent: parents[index],
            })
            .collect::<Vec<_>>();
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

struct OverlapClosure {
    region: WorkingRegion,
    merged_indices: Vec<usize>,
}

/// Compute only the connected overlap component touched by one new facet.
/// Contained and disjoint regions remain independent, so cloning the complete
/// forest for every optional facet is unnecessary and made normalization grow
/// quadratically in both facets and bytes copied.
fn overlap_closure(regions: &[WorkingRegion], mut region: WorkingRegion) -> OverlapClosure {
    let mut merged = BTreeSet::new();
    loop {
        let mut changed = false;
        for (index, candidate) in regions.iter().enumerate() {
            if merged.contains(&index) {
                continue;
            }
            if region.scope == candidate.scope
                || partially_overlaps(&region.scope, &candidate.scope)
            {
                merged.insert(index);
                region.scope.extend(candidate.scope.iter().copied());
                region.facets.extend(candidate.facets.iter().cloned());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    region.facets.sort_by_key(facet_stable_key);
    region.facets.dedup_by_key(|facet| facet.fingerprint);
    OverlapClosure {
        region,
        merged_indices: merged.into_iter().collect(),
    }
}

fn commit_overlap_closure(regions: &mut Vec<WorkingRegion>, closure: OverlapClosure) {
    for index in closure.merged_indices.into_iter().rev() {
        regions.remove(index);
    }
    regions.push(closure.region);
}

fn insert_overlap_closure(regions: &mut Vec<WorkingRegion>, region: WorkingRegion) {
    let closure = overlap_closure(regions, region);
    commit_overlap_closure(regions, closure);
}

fn partially_overlaps(left: &BTreeSet<GroupId>, right: &BTreeSet<GroupId>) -> bool {
    !left.is_disjoint(right) && !left.is_subset(right) && !right.is_subset(left)
}

/// Scopes are distinct and sorted by increasing cardinality. Visiting them in
/// reverse leaves the innermost already-seen owner of each group in `owners`.
/// A laminar child must see the same parent at every member; disagreement is
/// exactly a partial overlap. Thus validation and parent construction require
/// one pass over memberships, not two quadratic passes over all region pairs.
fn laminar_parents(regions: &[WorkingRegion]) -> Result<Vec<Option<RegionId>>> {
    let mut owners = HashMap::<GroupId, RegionId>::new();
    let mut parents = vec![None; regions.len()];
    for (index, region) in regions.iter().enumerate().rev() {
        let parent = region
            .scope
            .first()
            .and_then(|group| owners.get(group))
            .copied();
        if region
            .scope
            .iter()
            .any(|group| owners.get(group).copied() != parent)
        {
            return Err(paro_error::internal(
                "region normalization failed to produce a laminar forest",
            ));
        }
        parents[index] = parent;
        for group in &region.scope {
            owners.insert(*group, RegionId::new(index));
        }
    }
    Ok(parents)
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
            scope_contract: if criticality == FacetCriticality::Required {
                RegionScopeContract::Exact
            } else {
                RegionScopeContract::OwnerWithImmediateInputs
            },
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

    #[test]
    fn indexed_laminar_parents_reject_partial_overlap() {
        let regions = [
            WorkingRegion::from_facet(facet(1, FacetCriticality::Required, &[1, 2])),
            WorkingRegion::from_facet(facet(2, FacetCriticality::Required, &[2, 3])),
        ];
        assert!(laminar_parents(&regions).is_err());
    }

    #[test]
    fn singleton_admission_and_parent_links_match_independent_set_oracle() {
        // Replay ordered admission with a copied set model and exhaustive
        // parent selection. Closure follows the stable facet schedule: an
        // arbitrary global pairwise reduction may choose a different (also
        // laminar) ownership decomposition and is not this contract's oracle.
        fn close(regions: &mut Vec<WorkingRegion>) {
            let mut incoming = regions.pop().unwrap();
            let mut consumed = BTreeSet::new();
            loop {
                let before = consumed.len();
                for (index, existing) in regions.iter().enumerate() {
                    if consumed.contains(&index) {
                        continue;
                    }
                    let common = incoming.scope.intersection(&existing.scope).count();
                    if incoming.scope == existing.scope
                        || (common != 0
                            && common < incoming.scope.len()
                            && common < existing.scope.len())
                    {
                        incoming.scope.extend(existing.scope.iter().copied());
                        incoming.facets.extend(existing.facets.iter().cloned());
                        consumed.insert(index);
                    }
                }
                if consumed.len() == before {
                    break;
                }
            }
            let mut ordinal = 0;
            regions.retain(|_| {
                let keep = !consumed.contains(&ordinal);
                ordinal += 1;
                keep
            });
            regions.push(incoming);
        }
        fn oracle(mut facets: Vec<RegionFacet>, limit: usize) -> RegionForest {
            facets.sort_by_key(|facet| (facet.criticality, facet_stable_key(facet)));
            let mut regions = Vec::new();
            let mut dropped = Vec::new();
            for facet in facets {
                let optional = facet.criticality == FacetCriticality::Optional;
                let fingerprint = facet.fingerprint;
                let mut trial = regions.clone();
                trial.push(WorkingRegion::from_facet(facet));
                close(&mut trial);
                let own = trial
                    .iter()
                    .find(|region| {
                        region
                            .facets
                            .iter()
                            .any(|facet| facet.fingerprint == fingerprint)
                    })
                    .unwrap();
                if optional && own.scope.len() > limit {
                    dropped.push(fingerprint);
                } else {
                    regions = trial;
                }
            }
            for region in &mut regions {
                region.facets.sort_by_key(facet_stable_key);
            }
            regions.sort_by(|a, b| {
                a.scope
                    .len()
                    .cmp(&b.scope.len())
                    .then(a.scope.cmp(&b.scope))
            });
            let nodes = regions
                .iter()
                .enumerate()
                .map(|(index, region)| {
                    let parent = regions
                        .iter()
                        .enumerate()
                        .filter(|(_, other)| {
                            region.scope.len() < other.scope.len()
                                && region.scope.is_subset(&other.scope)
                        })
                        .min_by_key(|(index, other)| (other.scope.len(), *index))
                        .map(|(index, _)| RegionId::new(index));
                    RegionNode {
                        id: RegionId::new(index),
                        scope: region.scope.clone(),
                        facets: region.facets.clone().into_boxed_slice(),
                        parent,
                    }
                })
                .collect();
            dropped.sort_unstable();
            RegionForest {
                nodes,
                dropped_optional_facets: dropped.into_boxed_slice(),
            }
        }
        // Includes nested, disjoint, equal and partially overlapping scopes;
        // budgets reject optional bridges, never required scopes/singletons.
        for seed in 0..512_u64 {
            let mut random = seed + 1;
            let mut facets = Vec::new();
            for id in 0..9 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let mask = 1 + ((random >> 32) % 63);
                let scope = (0..6)
                    .filter(|bit| mask & (1 << bit) != 0)
                    .collect::<Vec<_>>();
                let criticality = if random & 4 == 0 {
                    FacetCriticality::Required
                } else {
                    FacetCriticality::Optional
                };
                facets.push(facet(id + 1, criticality, &scope));
                // Add a same-group required/optional anchor pair as well.
                facets.push(facet(id + 20, criticality, &[id as u32 % 6]));
            }
            for limit in 1..=6 {
                let expected = oracle(facets.clone(), limit);
                assert_eq!(
                    RegionForest::normalize(facets.clone(), limit, 6).unwrap(),
                    expected,
                    "seed {seed}, limit {limit}"
                );
                facets.reverse();
                assert_eq!(
                    RegionForest::normalize(facets.clone(), limit, 6).unwrap(),
                    expected,
                    "reversed seed {seed}, limit {limit}"
                );
            }
        }
    }

    #[test]
    fn many_anchor_regions_do_not_enter_composite_overlap_closure() {
        let groups = (0..8192).collect::<Vec<_>>();
        let mut facets = vec![facet(1, FacetCriticality::Required, &groups)];
        facets.extend(groups.iter().map(|group| {
            facet(
                u128::from(*group) + 2,
                FacetCriticality::Optional,
                &[*group],
            )
        }));
        let forest = RegionForest::normalize(facets, 1, groups.len()).unwrap();
        assert_eq!(forest.nodes.len(), groups.len() + 1);
        assert!(forest.dropped_optional_facets.is_empty());
        for node in &forest.nodes[..groups.len()] {
            assert_eq!(node.parent, Some(RegionId::new(groups.len())));
        }
        assert_eq!(forest.nodes[groups.len()].parent, None);
    }

    #[test]
    fn incremental_overlap_closure_matches_batch_oracle_for_every_insertion_order() {
        fn reference(regions: &mut Vec<WorkingRegion>) {
            loop {
                let mut pair = None;
                'outer: for left in 0..regions.len() {
                    for right in (left + 1)..regions.len() {
                        if regions[left].scope == regions[right].scope
                            || partially_overlaps(&regions[left].scope, &regions[right].scope)
                        {
                            pair = Some((left, right));
                            break 'outer;
                        }
                    }
                }
                let Some((left, right)) = pair else {
                    break;
                };
                let mut merged = regions.remove(right);
                regions[left].scope.extend(merged.scope);
                regions[left].facets.append(&mut merged.facets);
                regions[left].facets.sort_by_key(facet_stable_key);
                regions[left].facets.dedup_by_key(|facet| facet.fingerprint);
            }
        }

        fn summary(mut regions: Vec<WorkingRegion>) -> Vec<(Vec<GroupId>, Vec<Fingerprint>)> {
            let mut summary = regions
                .drain(..)
                .map(|region| {
                    (
                        region.scope.into_iter().collect(),
                        region
                            .facets
                            .into_iter()
                            .map(|facet| facet.fingerprint)
                            .collect(),
                    )
                })
                .collect::<Vec<_>>();
            summary.sort();
            summary
        }

        fn advance_permutation(order: &mut [usize]) -> bool {
            let Some(pivot) = (0..order.len().saturating_sub(1))
                .rev()
                .find(|index| order[*index] < order[*index + 1])
            else {
                return false;
            };
            let successor = ((pivot + 1)..order.len())
                .rev()
                .find(|index| order[*index] > order[pivot])
                .expect("permutation pivot has a successor");
            order.swap(pivot, successor);
            order[(pivot + 1)..].reverse();
            true
        }

        let scopes = [&[1, 2][..], &[2, 3], &[3, 4], &[5], &[1], &[4, 5]];
        let mut canonical = (0..scopes.len())
            .map(|index| {
                WorkingRegion::from_facet(facet(
                    index as u128 + 1,
                    FacetCriticality::Required,
                    scopes[index],
                ))
            })
            .collect::<Vec<_>>();
        reference(&mut canonical);
        let expected = summary(canonical);
        let mut order = (0..scopes.len()).collect::<Vec<_>>();
        loop {
            let mut actual = Vec::new();
            for index in order.iter().copied() {
                insert_overlap_closure(
                    &mut actual,
                    WorkingRegion::from_facet(facet(
                        index as u128 + 1,
                        FacetCriticality::Required,
                        scopes[index],
                    )),
                );
            }
            assert_eq!(summary(actual), expected, "insertion order {order:?}");
            if !advance_permutation(&mut order) {
                break;
            }
        }
    }
}
