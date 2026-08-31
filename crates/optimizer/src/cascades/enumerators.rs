//! Bounded domain enumerators. They emit stable candidates; Memo remains the
//! sole owner of equivalence, properties, costing and winners.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::budget::SearchBudget;
use super::ids::{FactorizationSpecId, Fingerprint, GroupId, StableFingerprintBuilder};
use super::properties::{ProvidedRepresentation, ResultGuarantee};

pub type AtomSet = BTreeSet<usize>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateEdge {
    pub atoms: AtomSet,
    pub fingerprint: Fingerprint,
    /// Deterministic coarse rank only; final selection belongs to SearchCost.
    pub heuristic_rank: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinFence {
    /// Atoms whose non-inner semantics form one indivisible scope when joined
    /// with atoms outside the fence.
    pub scope: AtomSet,
    /// Reorderable partitions inside the scope. A candidate may not split a
    /// partition or interleave a partial partition with an outside atom.
    pub partitions: Box<[AtomSet]>,
    pub ordered: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JoinLegalityConstraints {
    pub fences: Box<[JoinFence]>,
}

impl JoinLegalityConstraints {
    pub fn allows(&self, left: &AtomSet, right: &AtomSet) -> bool {
        let union = left.union(right).copied().collect::<AtomSet>();
        self.fences
            .iter()
            .all(|fence| fence_allows(fence, left, right, &union))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRegion {
    pub atoms: Box<[GroupId]>,
    pub predicates: Box<[PredicateEdge]>,
    pub legality: JoinLegalityConstraints,
}

impl JoinRegion {
    pub fn validate(&self) -> Result<()> {
        if self.atoms.len() < 2 {
            return Err(paro_error::internal(
                "join region requires at least two atoms",
            ));
        }
        let unique = self.atoms.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != self.atoms.len() {
            return Err(paro_error::internal("join region contains duplicate atoms"));
        }
        let atom_count = self.atoms.len();
        if self
            .predicates
            .iter()
            .any(|edge| edge.atoms.len() < 2 || edge.atoms.iter().any(|atom| *atom >= atom_count))
        {
            return Err(paro_error::internal(
                "join predicate edge references an invalid atom set",
            ));
        }
        for fence in &self.legality.fences {
            if fence.scope.is_empty()
                || fence.scope.iter().any(|atom| *atom >= atom_count)
                || fence.partitions.is_empty()
            {
                return Err(paro_error::internal("join fence has an invalid scope"));
            }
            let mut covered = AtomSet::new();
            for partition in &fence.partitions {
                if partition.is_empty()
                    || !partition.is_subset(&fence.scope)
                    || !covered.is_disjoint(partition)
                {
                    return Err(paro_error::internal(
                        "join fence partitions must be non-empty and disjoint",
                    ));
                }
                covered.extend(partition);
            }
            if covered != fence.scope {
                return Err(paro_error::internal(
                    "join fence partitions must cover the complete scope",
                ));
            }
        }
        Ok(())
    }

    fn connected(&self, subset: &AtomSet) -> bool {
        if subset.len() <= 1 {
            return true;
        }
        let Some(&start) = subset.iter().next() else {
            return false;
        };
        let mut reached = [start].into_iter().collect::<AtomSet>();
        loop {
            let before = reached.len();
            for edge in &self.predicates {
                if !edge.atoms.is_disjoint(&reached) {
                    reached.extend(edge.atoms.intersection(subset).copied());
                }
            }
            if reached.len() == before {
                return reached == *subset;
            }
        }
    }

    fn cut_edges(&self, left: &AtomSet, right: &AtomSet) -> Box<[Fingerprint]> {
        self.predicates
            .iter()
            .filter(|edge| !edge.atoms.is_disjoint(left) && !edge.atoms.is_disjoint(right))
            .map(|edge| edge.fingerprint)
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }

    fn rank(&self, subset: &AtomSet) -> u64 {
        let atom_work = subset.len() as u64 * 1_000;
        let predicate_credit: u64 = self
            .predicates
            .iter()
            .filter(|edge| edge.atoms.is_subset(subset))
            .map(|edge| u64::from(edge.heuristic_rank))
            .sum();
        atom_work.saturating_sub(predicate_credit.min(atom_work))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinEnumerationStrategy {
    ExactDpccp,
    DeterministicBeam,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinDecomposition {
    pub union: AtomSet,
    pub left: AtomSet,
    pub right: AtomSet,
    pub predicates: Box<[Fingerprint]>,
    pub stable_fingerprint: Fingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEnumeration {
    pub strategy: JoinEnumerationStrategy,
    pub candidates: Box<[JoinDecomposition]>,
    pub considered_pairs: u32,
    pub pair_budget_exhausted: bool,
}

pub struct JoinRegionEnumerator<'a> {
    budget: &'a SearchBudget,
}

impl<'a> JoinRegionEnumerator<'a> {
    pub fn new(budget: &'a SearchBudget) -> Self {
        Self { budget }
    }

    pub fn enumerate(&self, region: &JoinRegion) -> Result<JoinEnumeration> {
        region.validate()?;
        if region.atoms.len() <= usize::from(self.budget.max_join_exact_relations)
            && region.atoms.len() <= 63
        {
            let exact = self.enumerate_exact(region)?;
            if !exact.pair_budget_exhausted {
                return Ok(exact);
            }
        }
        self.enumerate_beam(region)
    }

    fn enumerate_exact(&self, region: &JoinRegion) -> Result<JoinEnumeration> {
        let atom_count = region.atoms.len();
        let all_mask = (1_u64 << atom_count) - 1;
        let mut connected = BTreeMap::<u64, AtomSet>::new();
        for mask in 1..=all_mask {
            let subset = set_from_mask(mask, atom_count);
            if region.connected(&subset) {
                connected.insert(mask, subset);
            }
        }
        let mut candidates = Vec::new();
        let mut considered = 0_u32;
        for (&union_mask, union) in &connected {
            if union.len() < 2 {
                continue;
            }
            let anchor = union_mask.trailing_zeros();
            let mut left_mask = (union_mask - 1) & union_mask;
            while left_mask != 0 {
                if left_mask & (1_u64 << anchor) != 0 {
                    let right_mask = union_mask ^ left_mask;
                    if right_mask != 0 {
                        considered = considered.saturating_add(1);
                        if considered > self.budget.max_join_connected_pairs {
                            return Ok(JoinEnumeration {
                                strategy: JoinEnumerationStrategy::ExactDpccp,
                                candidates: candidates.into_boxed_slice(),
                                considered_pairs: considered - 1,
                                pair_budget_exhausted: true,
                            });
                        }
                        if let (Some(left), Some(right)) =
                            (connected.get(&left_mask), connected.get(&right_mask))
                        {
                            if region.legality.allows(left, right) {
                                if let Some(candidate) = decomposition(region, union, left, right) {
                                    candidates.push(candidate);
                                }
                            }
                        }
                    }
                }
                left_mask = (left_mask - 1) & union_mask;
            }
        }
        candidates.sort_by_key(|candidate| candidate.stable_fingerprint);
        Ok(JoinEnumeration {
            strategy: JoinEnumerationStrategy::ExactDpccp,
            candidates: candidates.into_boxed_slice(),
            considered_pairs: considered,
            pair_budget_exhausted: false,
        })
    }

    fn enumerate_beam(&self, region: &JoinRegion) -> Result<JoinEnumeration> {
        let all = (0..region.atoms.len()).collect::<AtomSet>();
        let mut previous = (0..region.atoms.len())
            .map(|atom| [atom].into_iter().collect::<AtomSet>())
            .collect::<Vec<_>>();
        let mut candidates = Vec::new();
        let mut considered = 0_u32;
        let mut exhausted = false;
        for target_size in 2..=region.atoms.len() {
            let mut next = BTreeMap::<AtomSet, JoinDecomposition>::new();
            'outer: for left in &previous {
                for atom in all.difference(left).copied() {
                    considered = considered.saturating_add(1);
                    if considered > self.budget.max_join_connected_pairs {
                        exhausted = true;
                        break 'outer;
                    }
                    let right = [atom].into_iter().collect::<AtomSet>();
                    let mut union = left.clone();
                    union.insert(atom);
                    if union.len() != target_size
                        || !region.connected(&union)
                        || !region.legality.allows(left, &right)
                    {
                        continue;
                    }
                    let Some(candidate) = decomposition(region, &union, left, &right) else {
                        continue;
                    };
                    next.entry(union).or_insert(candidate);
                }
            }
            let mut ranked = next.into_values().collect::<Vec<_>>();
            ranked.sort_by(|left, right| {
                region
                    .rank(&left.union)
                    .cmp(&region.rank(&right.union))
                    .then_with(|| left.stable_fingerprint.cmp(&right.stable_fingerprint))
            });
            ranked.truncate(usize::from(self.budget.join_beam_width.max(1)));
            previous = ranked
                .iter()
                .map(|candidate| candidate.union.clone())
                .collect();
            candidates.extend(ranked);
            if previous.is_empty() || exhausted {
                break;
            }
        }
        if !previous.iter().any(|subset| subset == &all) {
            return Err(paro_error::internal(
                "join region has no connected, legality-preserving decomposition",
            ));
        }
        candidates.sort_by_key(|candidate| candidate.stable_fingerprint);
        Ok(JoinEnumeration {
            strategy: JoinEnumerationStrategy::DeterministicBeam,
            candidates: candidates.into_boxed_slice(),
            considered_pairs: considered.min(self.budget.max_join_connected_pairs),
            pair_budget_exhausted: exhausted,
        })
    }
}

fn decomposition(
    region: &JoinRegion,
    union: &AtomSet,
    left: &AtomSet,
    right: &AtomSet,
) -> Option<JoinDecomposition> {
    let predicates = region.cut_edges(left, right);
    if predicates.is_empty() {
        return None;
    }
    let mut builder = StableFingerprintBuilder::default();
    encode_set(&mut builder, union);
    encode_set(&mut builder, left);
    encode_set(&mut builder, right);
    for predicate in &predicates {
        builder.write_fingerprint(*predicate);
    }
    Some(JoinDecomposition {
        union: union.clone(),
        left: left.clone(),
        right: right.clone(),
        predicates,
        stable_fingerprint: builder.finish(),
    })
}

fn fence_allows(fence: &JoinFence, left: &AtomSet, right: &AtomSet, union: &AtomSet) -> bool {
    let inside = union
        .intersection(&fence.scope)
        .copied()
        .collect::<AtomSet>();
    let has_outside = union.iter().any(|atom| !fence.scope.contains(atom));
    if has_outside && !inside.is_empty() && inside != fence.scope {
        return false;
    }
    if has_outside && fence.scope.is_subset(union) {
        // The complete fenced result must be built before it can join an
        // outside atom; an outside atom cannot be interleaved with one side of
        // an outer/semi/anti/dependent boundary.
        if !fence.scope.is_subset(left) && !fence.scope.is_subset(right) {
            return false;
        }
    }
    if !fence.scope.is_subset(union) {
        return true;
    }
    let left_scope = left
        .intersection(&fence.scope)
        .copied()
        .collect::<AtomSet>();
    let right_scope = right
        .intersection(&fence.scope)
        .copied()
        .collect::<AtomSet>();
    let partition_aligned = |side: &AtomSet| {
        fence
            .partitions
            .iter()
            .all(|partition| partition.is_subset(side) || partition.is_disjoint(side))
    };
    if !partition_aligned(&left_scope) || !partition_aligned(&right_scope) {
        return false;
    }
    if fence.ordered && !left_scope.is_empty() && !right_scope.is_empty() {
        let left_max = fence
            .partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| partition.is_subset(&left_scope))
            .map(|(index, _)| index)
            .max();
        let right_min = fence
            .partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| partition.is_subset(&right_scope))
            .map(|(index, _)| index)
            .min();
        return left_max
            .zip(right_min)
            .is_some_and(|(left, right)| left < right);
    }
    true
}

fn set_from_mask(mask: u64, atom_count: usize) -> AtomSet {
    (0..atom_count)
        .filter(|atom| mask & (1_u64 << atom) != 0)
        .collect()
}

fn encode_set(builder: &mut StableFingerprintBuilder, set: &AtomSet) {
    builder.write_u64(set.len() as u64);
    for atom in set {
        builder.write_u64(*atom as u64);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphDirection {
    Forward,
    Reverse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphExpansion {
    pub source: usize,
    pub target: usize,
    pub direction: GraphDirection,
    pub conditional_degree_rank: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPatternRegion {
    pub atoms: Box<[GroupId]>,
    pub expansions: Box<[GraphExpansion]>,
    pub path_semantics: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFrontierCandidate {
    pub start_atom: usize,
    pub visit_order: Box<[usize]>,
    pub representation: ProvidedRepresentation,
    pub stable_fingerprint: Fingerprint,
}

pub fn enumerate_graph_frontiers(
    region: &GraphPatternRegion,
    budget: &SearchBudget,
) -> Result<Box<[GraphFrontierCandidate]>> {
    if region.path_semantics {
        return Err(paro_error::internal(
            "path-semantic regions require the dedicated path enumerator",
        ));
    }
    if region.atoms.is_empty()
        || region.expansions.iter().any(|edge| {
            edge.source >= region.atoms.len()
                || edge.target >= region.atoms.len()
                || edge.source == edge.target
        })
    {
        return Err(paro_error::internal("graph pattern region is malformed"));
    }
    let mut candidates = Vec::new();
    for start in 0..region.atoms.len() {
        let mut visited = [start].into_iter().collect::<AtomSet>();
        let mut order = vec![start];
        while visited.len() < region.atoms.len() {
            let next = region
                .expansions
                .iter()
                .filter_map(|edge| {
                    let target = if visited.contains(&edge.source)
                        && !visited.contains(&edge.target)
                    {
                        Some(edge.target)
                    } else if visited.contains(&edge.target) && !visited.contains(&edge.source) {
                        Some(edge.source)
                    } else {
                        None
                    }?;
                    Some((edge.conditional_degree_rank, target, edge.direction))
                })
                .min();
            let Some((_, next, _)) = next else {
                break;
            };
            visited.insert(next);
            order.push(next);
        }
        if visited.len() != region.atoms.len() {
            continue;
        }
        for representation in [
            ProvidedRepresentation::Flat,
            ProvidedRepresentation::Factorized(FactorizationSpecId(start as u32)),
        ] {
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_u64(start as u64);
            for atom in &order {
                fingerprint.write_u64(*atom as u64);
            }
            fingerprint.write_u64(match representation {
                ProvidedRepresentation::Flat => 0,
                ProvidedRepresentation::Factorized(spec) => 1 + u64::from(spec.0),
            });
            candidates.push(GraphFrontierCandidate {
                start_atom: start,
                visit_order: order.clone().into_boxed_slice(),
                representation,
                stable_fingerprint: fingerprint.finish(),
            });
        }
    }
    candidates.sort_by_key(|candidate| candidate.stable_fingerprint);
    candidates.truncate(budget.max_graph_frontiers as usize);
    Ok(candidates.into_boxed_slice())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversalContract {
    StrictMonotone,
    RelaxedMonotone,
    FixedTopKOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchImplementationKind {
    SequentialExact,
    ScalarRowset,
    RankedProvider,
    ExactRowsetFilteredRanked,
    SearchDrivenFusion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProviderCandidate {
    pub fingerprint: Fingerprint,
    pub kind: SearchImplementationKind,
    pub guarantee: ResultGuarantee,
    pub traversal: TraversalContract,
    pub accepts_exact_rowset: bool,
    pub supports_continuation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchSemantics {
    pub required_guarantee: ResultGuarantee,
    pub downstream_may_remove_rows: bool,
    pub downstream_may_change_multiplicity: bool,
    pub exact_rowset_available: bool,
}

pub fn enumerate_search_candidates(
    providers: impl IntoIterator<Item = SearchProviderCandidate>,
    semantics: SearchSemantics,
    budget: &SearchBudget,
) -> Box<[SearchProviderCandidate]> {
    let mut candidates = providers
        .into_iter()
        .filter(|candidate| {
            candidate.guarantee.satisfies(semantics.required_guarantee)
                && match candidate.kind {
                    SearchImplementationKind::SequentialExact
                    | SearchImplementationKind::ScalarRowset => true,
                    SearchImplementationKind::ExactRowsetFilteredRanked => {
                        semantics.exact_rowset_available && candidate.accepts_exact_rowset
                    }
                    SearchImplementationKind::RankedProvider => true,
                    SearchImplementationKind::SearchDrivenFusion => {
                        (!semantics.downstream_may_remove_rows
                            && !semantics.downstream_may_change_multiplicity)
                            || (candidate.supports_continuation
                                && candidate.traversal != TraversalContract::FixedTopKOnly)
                    }
                }
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| candidate.fingerprint);
    candidates.dedup_by_key(|candidate| candidate.fingerprint);
    candidates.truncate(usize::from(budget.max_search_candidates));
    candidates.into_boxed_slice()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AggregateImplementationKind {
    Hash,
    SpillableHash,
    PerfectHash,
    OrderedStreaming,
    LocalGlobal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateRegionFeatures {
    pub input_ordered_by_group: bool,
    pub input_partitioned_by_group: bool,
    pub bounded_integral_domain: bool,
    pub spill_allowed: bool,
    pub parallel_input: bool,
}

pub fn enumerate_aggregate_candidates(
    features: AggregateRegionFeatures,
) -> Box<[AggregateImplementationKind]> {
    let mut candidates = vec![AggregateImplementationKind::Hash];
    if features.spill_allowed {
        candidates.push(AggregateImplementationKind::SpillableHash);
    }
    if features.bounded_integral_domain {
        candidates.push(AggregateImplementationKind::PerfectHash);
    }
    if features.input_ordered_by_group {
        candidates.push(AggregateImplementationKind::OrderedStreaming);
    }
    if features.parallel_input || features.input_partitioned_by_group {
        candidates.push(AggregateImplementationKind::LocalGlobal);
    }
    candidates.sort();
    candidates.dedup();
    candidates.into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(atoms: &[usize], id: u128) -> PredicateEdge {
        PredicateEdge {
            atoms: atoms.iter().copied().collect(),
            fingerprint: Fingerprint(id),
            heuristic_rank: 100,
        }
    }

    #[test]
    fn exact_join_enumerator_only_emits_connected_pairs() {
        let region = JoinRegion {
            atoms: [GroupId(0), GroupId(1), GroupId(2)].into(),
            predicates: [edge(&[0, 1], 1), edge(&[1, 2], 2)].into(),
            legality: JoinLegalityConstraints::default(),
        };
        let result = JoinRegionEnumerator::new(&SearchBudget::default())
            .enumerate(&region)
            .unwrap();
        assert_eq!(result.strategy, JoinEnumerationStrategy::ExactDpccp);
        assert!(result.candidates.iter().all(|candidate| {
            !candidate.predicates.is_empty()
                && region.connected(&candidate.left)
                && region.connected(&candidate.right)
        }));
        assert!(result
            .candidates
            .iter()
            .any(|candidate| candidate.union.len() == 3));
    }

    #[test]
    fn ordered_fence_rejects_outer_join_reversal() {
        let fence = JoinFence {
            scope: [0, 1].into_iter().collect(),
            partitions: vec![[0].into_iter().collect(), [1].into_iter().collect()]
                .into_boxed_slice(),
            ordered: true,
        };
        let legality = JoinLegalityConstraints {
            fences: [fence].into(),
        };
        assert!(legality.allows(&[0].into_iter().collect(), &[1].into_iter().collect()));
        assert!(!legality.allows(&[1].into_iter().collect(), &[0].into_iter().collect()));
        assert!(!legality.allows(&[0, 2].into_iter().collect(), &[1].into_iter().collect()));
    }

    #[test]
    fn pair_exhaustion_falls_back_to_deterministic_beam() {
        let mut budget = SearchBudget::default();
        budget.max_join_connected_pairs = 1;
        budget.join_beam_width = 8;
        let region = JoinRegion {
            atoms: [GroupId(0), GroupId(1), GroupId(2)].into(),
            predicates: [edge(&[0, 1], 1), edge(&[1, 2], 2), edge(&[0, 2], 3)].into(),
            legality: JoinLegalityConstraints::default(),
        };
        let result = JoinRegionEnumerator::new(&budget).enumerate(&region);
        assert!(result.is_err() || result.unwrap().pair_budget_exhausted);
    }

    #[test]
    fn fixed_topk_cannot_cross_a_row_eliminating_stage() {
        let providers = [SearchProviderCandidate {
            fingerprint: Fingerprint(1),
            kind: SearchImplementationKind::SearchDrivenFusion,
            guarantee: ResultGuarantee::Exact,
            traversal: TraversalContract::FixedTopKOnly,
            accepts_exact_rowset: false,
            supports_continuation: false,
        }];
        let candidates = enumerate_search_candidates(
            providers,
            SearchSemantics {
                required_guarantee: ResultGuarantee::Exact,
                downstream_may_remove_rows: true,
                downstream_may_change_multiplicity: false,
                exact_rowset_available: false,
            },
            &SearchBudget::default(),
        );
        assert!(candidates.is_empty());
    }

    #[test]
    fn aggregate_baseline_is_never_optional() {
        let candidates = enumerate_aggregate_candidates(AggregateRegionFeatures {
            input_ordered_by_group: false,
            input_partitioned_by_group: false,
            bounded_integral_domain: false,
            spill_allowed: false,
            parallel_input: false,
        });
        assert_eq!(&*candidates, &[AggregateImplementationKind::Hash]);
    }
}
