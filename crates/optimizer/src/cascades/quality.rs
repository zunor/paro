// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Capability-driven quality decision packages.
//!
//! A quality bundle is a bounded request for native facts/choices.  It does
//! not own a subtree or select a logical winner; its result is consumed by
//! the same Memo/task protocol as every other candidate.  The registry keeps
//! the lifecycle states explicit so missing evidence cannot be relabelled as
//! NotApplicable and accidentally certify P_ready.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::ids::{
    CandidateId, Fingerprint, LogicalExprId, PhysicalExprId, QualityPolicyId, RuleId,
};
use super::memo::{ChildWinnerRef, Memo, OptimizationGoal, Winner};
use super::rules::PatternBinding;
use super::tasks::{ReadSet, ReadSetId};

macro_rules! quality_id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u32);

        impl $name {
            pub const fn new(index: usize) -> Self {
                Self(index as u32)
            }
        }
    };
}

quality_id_type!(BundleId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BundleCapability {
    ScanPredicate,
    SmallJoin,
    SharedAggregate,
    CorrelatedSubquery,
    LargeJoin,
    Ordering,
    GraphProvider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BundleFact {
    OutputDemand,
    PredicateDomain,
    JoinRegion,
    AggregateDecomposition,
    CteConsumerDemand,
    NullSemantics,
    OrderingDemand,
    ProviderCapability,
}

impl BundleFact {
    pub const fn stable_tag(self) -> u64 {
        match self {
            Self::OutputDemand => 0,
            Self::PredicateDomain => 1,
            Self::JoinRegion => 2,
            Self::AggregateDecomposition => 3,
            Self::CteConsumerDemand => 4,
            Self::NullSemantics => 5,
            Self::OrderingDemand => 6,
            Self::ProviderCapability => 7,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityBundleSpec {
    pub id: BundleId,
    pub revision: u32,
    pub required_capabilities: Box<[BundleCapability]>,
    pub required_facts: Box<[BundleFact]>,
    pub priority: u16,
}

impl QualityBundleSpec {
    pub fn new(
        id: BundleId,
        revision: u32,
        capabilities: impl IntoIterator<Item = BundleCapability>,
        facts: impl IntoIterator<Item = BundleFact>,
        priority: u16,
    ) -> Self {
        let mut capabilities = capabilities.into_iter().collect::<Vec<_>>();
        capabilities.sort_unstable();
        capabilities.dedup();
        let mut facts = facts.into_iter().collect::<Vec<_>>();
        facts.sort_unstable();
        facts.dedup();
        Self {
            id,
            revision,
            required_capabilities: capabilities.into_boxed_slice(),
            required_facts: facts.into_boxed_slice(),
            priority,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleInput {
    pub capabilities: BTreeSet<BundleCapability>,
    pub facts: BTreeSet<BundleFact>,
    pub reads: ReadSetId,
    pub region: Fingerprint,
    /// A correctness/provenance witness supplied by the fact producer.  The
    /// registry never synthesizes one from a query name or benchmark label.
    pub applicability_proof: Fingerprint,
    /// Native choice identities are supplied by the fact producer. The
    /// registry does not derive a plan identity from CandidateId or retain an
    /// owned logical tree as a quality result.
    pub choices: Box<[Fingerprint]>,
    pub candidate: Option<CandidateId>,
    /// Per-region aggregate witnesses. A global AggregateDecomposition fact
    /// is not sufficient for a shared-aggregate bundle: every relevant
    /// selected region must be covered by the same candidate and fact
    /// snapshot.
    pub aggregate_regions: Box<[AggregateRegionWitness]>,
}

/// Evidence that one exact aggregate region of a frozen candidate satisfies
/// the decomposition contract. The fact fingerprint is deliberately carried
/// by the producer instead of being reconstructed by the quality registry;
/// changing statistics, logical facts, or the selected region invalidates the
/// witness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateRegionWitness {
    pub region: Fingerprint,
    pub candidate: CandidateId,
    pub anchor: CandidateId,
    pub fact_fingerprint: Fingerprint,
    /// The exact selected anchor already names immutable child CandidateIds.
    /// Membership in the root choice manifest therefore binds its entire
    /// selected subtree; expanding that subtree again for every region adds
    /// no identity evidence. Fact revisions remain a separate witness.
    pub anchor_choice: Fingerprint,
    pub covered: bool,
}

/// Native evidence for one exact frozen candidate. This is a compact
/// producer result, not a second Memo or an owned logical tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeQualityEvidence {
    pub capabilities: BTreeSet<BundleCapability>,
    pub facts: BTreeSet<BundleFact>,
    pub region: Fingerprint,
    pub applicability_proof: Fingerprint,
    pub choices: Box<[Fingerprint]>,
    pub aggregate_regions: Box<[AggregateRegionWitness]>,
    /// Exact selected filter choices with a still-transferable cheap domain.
    /// Scheduling hints only; they never discharge a quality obligation.
    pub pending_domain_transfers: Box<[CandidateId]>,
    /// Rule witnesses are extracted from the selected logical expressions'
    /// equivalence proofs.  They are audit/attribution data only; an applied
    /// rule set is never accepted as a substitute for this selected-DAG
    /// witness.
    pub selected_rules: Box<[RuleId]>,
    /// Shape facts observed while walking the exact frozen DAG. These are
    /// diagnostic evidence only; the policy never treats an operator count as
    /// a substitute for a semantic or physical contract.
    pub shape: NativeQualityShape,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeQualityShape {
    pub nodes: u32,
    pub aggregates: u32,
    pub joins: u32,
    pub runtime_filter_joins: u32,
    pub aggregate_witness_nodes: u32,
    pub join_region_witness_nodes: u32,
}

/// A borrowed-candidate inspection result.  It contains only immutable Memo
/// identities and exact child references; it deliberately does not own
/// logical/physical payloads or a cloned candidate tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityCandidateNode {
    pub reference: ChildWinnerRef,
    pub logical: LogicalExprId,
    pub physical: PhysicalExprId,
    pub children: std::sync::Arc<[ChildWinnerRef]>,
}

/// Exact selected-choice properties and the fact dependencies used to derive
/// them. Missing facts request production work; absence means the graph could
/// not be certified. There is no second frozen-tree quality implementation.
#[derive(Debug, Clone)]
pub struct SelectedQualityEvidence {
    pub nodes: std::sync::Arc<[QualityCandidateNode]>,
    pub reads: ReadSet,
    pub evidence: NativeQualityEvidence,
}

/// The provider observes immutable selected choices. It never changes a
/// candidate or substitutes a different frontier winner. The engine performs
/// executable verification and freezing only after policy certification.
#[derive(Debug, Default, Clone, Copy)]
pub struct QualityPropertyWork {
    pub node_builds: u64,
    pub node_reuses: u64,
    pub cte_builds: u64,
    pub cte_reuses: u64,
}

pub trait QualityEvidenceProvider: std::fmt::Debug {
    fn property_work(&self) -> Option<QualityPropertyWork> {
        None
    }

    fn evaluate(
        &self,
        memo: &Memo,
        reference: ChildWinnerRef,
        winner: &Winner,
        goal: OptimizationGoal,
        policy: &QualityBundleRegistry,
    ) -> Result<Option<SelectedQualityEvidence>>;

    /// Construct transport only for the missing domain request chosen by the
    /// existing production scheduler, not for every candidate inspection.
    fn selected_domain_bindings(
        &self,
        _memo: &Memo,
        _reference: ChildWinnerRef,
        _winner: &Winner,
        _nodes: &[QualityCandidateNode],
        _goal: OptimizationGoal,
    ) -> Result<Box<[PatternBinding]>> {
        Ok(Box::new([]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BundleResult {
    Completed {
        bundle: BundleId,
        revision: u32,
        reads: ReadSetId,
        region: Fingerprint,
        choices: Box<[Fingerprint]>,
        candidate: Option<CandidateId>,
        aggregate_regions: Box<[AggregateRegionWitness]>,
    },
    NotApplicable {
        bundle: BundleId,
        revision: u32,
        proof: Fingerprint,
    },
    MissingEvidence {
        bundle: BundleId,
        revision: u32,
        missing: Box<[BundleFact]>,
    },
    Suspended {
        bundle: BundleId,
        revision: u32,
        cursor: u64,
    },
}

impl BundleResult {
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::NotApplicable { .. })
    }

    pub const fn bundle(&self) -> BundleId {
        match self {
            Self::Completed { bundle, .. }
            | Self::NotApplicable { bundle, .. }
            | Self::MissingEvidence { bundle, .. }
            | Self::Suspended { bundle, .. } => *bundle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BundleState {
    Pending,
    Completed,
    NotApplicable,
    MissingEvidence,
    Suspended,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QualityEvaluationSummary {
    pub completed: u64,
    pub not_applicable: u64,
    pub missing_evidence: u64,
    pub suspended: u64,
    pub missing_facts: u64,
    pub missing_bundles: Box<[BundleId]>,
    pub missing_fact_kinds: Box<[BundleFact]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegisteredBundle {
    spec: QualityBundleSpec,
    result: Option<BundleResult>,
    state: BundleState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PReadyCertificate {
    pub policy: QualityPolicyId,
    pub bundles: Box<[(BundleId, u32)]>,
    /// All completed bundles must identify the same root candidate.
    pub candidate: CandidateId,
    /// Exact selected-DAG identities shared by every completed bundle.
    pub choices: Box<[Fingerprint]>,
    pub regions: Box<[Fingerprint]>,
    pub reads: Box<[ReadSetId]>,
    pub candidates: Box<[CandidateId]>,
    /// The exact per-region witnesses consumed by the certificate.
    pub aggregate_regions: Box<[AggregateRegionWitness]>,
}

impl PReadyCertificate {
    pub fn is_complete(&self, registry: &QualityBundleRegistry) -> bool {
        self.bundles.iter().all(|(id, revision)| {
            let Some(bundle) = registry.bundles.get(id) else {
                return false;
            };
            if bundle.spec.revision != *revision || !bundle.result_is_ready() {
                return false;
            }
            match bundle.result.as_ref() {
                Some(BundleResult::Completed {
                    candidate,
                    choices,
                    region,
                    reads,
                    aggregate_regions,
                    ..
                }) => {
                    *candidate == Some(self.candidate)
                        && choices.as_ref() == self.choices.as_ref()
                        && self.regions.as_ref() == [*region]
                        && self.reads.as_ref() == [*reads]
                        && aggregate_regions.as_ref() == self.aggregate_regions.as_ref()
                }
                Some(BundleResult::NotApplicable { .. }) => true,
                _ => false,
            }
        })
    }
}

impl RegisteredBundle {
    fn result_is_ready(&self) -> bool {
        self.result.as_ref().is_some_and(BundleResult::is_ready)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualityPolicyStatus {
    NotSatisfied,
    Satisfied(PReadyCertificate),
}

/// Query-local, capability-driven bundle registry.  There is no benchmark or
/// SQL identity in this type; callers provide the already-derived capability
/// and fact sets from the native planner.
#[derive(Debug, Default)]
pub struct QualityBundleRegistry {
    bundles: BTreeMap<BundleId, RegisteredBundle>,
}

impl QualityBundleRegistry {
    pub fn register(&mut self, spec: QualityBundleSpec) -> Result<()> {
        if self.bundles.contains_key(&spec.id) {
            return Err(paro_error::internal(
                "quality bundle id was registered twice",
            ));
        }
        self.bundles.insert(
            spec.id,
            RegisteredBundle {
                spec,
                result: None,
                state: BundleState::Pending,
            },
        );
        Ok(())
    }

    pub fn register_builtin_f1_f4(&mut self) -> Result<()> {
        self.register(QualityBundleSpec::new(
            BundleId(1),
            1,
            [BundleCapability::ScanPredicate],
            [BundleFact::OutputDemand, BundleFact::PredicateDomain],
            10,
        ))?;
        self.register(QualityBundleSpec::new(
            BundleId(2),
            1,
            [BundleCapability::SmallJoin],
            [BundleFact::JoinRegion, BundleFact::OutputDemand],
            20,
        ))?;
        self.register(QualityBundleSpec::new(
            BundleId(3),
            1,
            [BundleCapability::SharedAggregate],
            [
                BundleFact::AggregateDecomposition,
                BundleFact::CteConsumerDemand,
                BundleFact::NullSemantics,
            ],
            30,
        ))?;
        self.register(QualityBundleSpec::new(
            BundleId(4),
            1,
            [BundleCapability::CorrelatedSubquery],
            [BundleFact::PredicateDomain, BundleFact::NullSemantics],
            40,
        ))?;
        Ok(())
    }

    pub fn state(&self, id: BundleId) -> Option<BundleState> {
        self.bundles.get(&id).map(|bundle| bundle.state)
    }

    /// Whether all applicable bundles have their local facts. This is only
    /// demand for the complete selected-choice manifest, never a certificate:
    /// `evaluate_native_candidate` must still validate each region against it.
    /// Keep applicability here rather than duplicating built-in policy in the
    /// planner or constructing a root manifest for a known blocked candidate.
    pub fn claims_are_ready(
        &self,
        capabilities: &BTreeSet<BundleCapability>,
        facts: &BTreeSet<BundleFact>,
    ) -> bool {
        self.bundles.values().all(|bundle| {
            !bundle
                .spec
                .required_capabilities
                .iter()
                .all(|c| capabilities.contains(c))
                || bundle.spec.required_facts.iter().all(|f| facts.contains(f))
        })
    }

    pub fn result(&self, id: BundleId) -> Option<&BundleResult> {
        self.bundles
            .get(&id)
            .and_then(|bundle| bundle.result.as_ref())
    }

    /// Clear all bundle results before evaluating another root candidate. A
    /// previous candidate's completed package is not valid evidence for a
    /// later candidate, even when the query facts did not change.
    pub fn clear_results(&mut self) {
        for bundle in self.bundles.values_mut() {
            bundle.result = None;
            bundle.state = BundleState::Pending;
        }
    }

    /// Stable identity used when a caller wants to mirror bundle lifecycle in
    /// the TaskRegistry/governor. The revision is part of the identity.
    pub fn bundle_identity(&self, id: BundleId) -> Option<Fingerprint> {
        let bundle = self.bundles.get(&id)?;
        let mut fingerprint = super::ids::StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.quality-bundle.v2");
        fingerprint.write_u64(bundle.spec.id.0 as u64);
        fingerprint.write_u64(bundle.spec.revision as u64);
        Some(fingerprint.finish())
    }

    /// Evaluate every registered package against one native candidate. The
    /// exact candidate and choice vector are installed in every input, so
    /// independently completed packages cannot certify a mixed root.
    pub fn evaluate_native_candidate(
        &mut self,
        policy: QualityPolicyId,
        candidate: CandidateId,
        reads: ReadSetId,
        evidence: &NativeQualityEvidence,
        budget: u32,
    ) -> Result<Option<PReadyCertificate>> {
        self.clear_results();
        let input = BundleInput {
            capabilities: evidence.capabilities.clone(),
            facts: evidence.facts.clone(),
            reads,
            region: evidence.region,
            applicability_proof: evidence.applicability_proof,
            choices: evidence.choices.clone(),
            candidate: Some(candidate),
            aggregate_regions: evidence.aggregate_regions.clone(),
        };
        let ids = self.bundles.keys().copied().collect::<Vec<_>>();
        for id in ids {
            self.evaluate(id, &input, budget)?;
        }
        Ok(self.p_ready_certificate(policy))
    }

    pub fn evaluation_summary(&self) -> QualityEvaluationSummary {
        let mut summary = QualityEvaluationSummary::default();
        let mut missing_bundles = Vec::new();
        let mut missing_fact_kinds = BTreeSet::new();
        for bundle in self.bundles.values() {
            match bundle.result.as_ref() {
                Some(BundleResult::Completed { .. }) => summary.completed += 1,
                Some(BundleResult::NotApplicable { .. }) => summary.not_applicable += 1,
                Some(BundleResult::MissingEvidence { missing, .. }) => {
                    summary.missing_evidence += 1;
                    summary.missing_facts += missing.len() as u64;
                    missing_bundles.push(bundle.spec.id);
                    missing_fact_kinds.extend(missing.iter().copied());
                }
                Some(BundleResult::Suspended { .. }) => summary.suspended += 1,
                None => {}
            }
        }
        summary.missing_bundles = missing_bundles.into_boxed_slice();
        summary.missing_fact_kinds = missing_fact_kinds.into_iter().collect();
        summary
    }

    pub fn evaluate(
        &mut self,
        id: BundleId,
        input: &BundleInput,
        budget: u32,
    ) -> Result<BundleResult> {
        let registered = self
            .bundles
            .get(&id)
            .ok_or_else(|| paro_error::internal("quality bundle was not registered"))?;
        if registered.spec.id != id {
            return Err(paro_error::internal(
                "quality bundle registry identity is corrupt",
            ));
        }
        let spec = registered.spec.clone();
        if !spec
            .required_capabilities
            .iter()
            .all(|capability| input.capabilities.contains(capability))
        {
            let result = BundleResult::NotApplicable {
                bundle: id,
                revision: spec.revision,
                proof: input.applicability_proof,
            };
            self.publish_result(id, result.clone());
            return Ok(result);
        }
        let missing = spec
            .required_facts
            .iter()
            .copied()
            .filter(|fact| !input.facts.contains(fact))
            .collect::<Vec<_>>();
        let mut missing = missing;
        if spec
            .required_facts
            .contains(&BundleFact::AggregateDecomposition)
            && !aggregate_coverage_is_complete(input)
        {
            if !missing.contains(&BundleFact::AggregateDecomposition) {
                missing.push(BundleFact::AggregateDecomposition);
            }
        }
        if !missing.is_empty() {
            let result = BundleResult::MissingEvidence {
                bundle: id,
                revision: spec.revision,
                missing: missing.into_boxed_slice(),
            };
            self.publish_result(id, result.clone());
            return Ok(result);
        }
        if budget == 0 {
            let result = BundleResult::Suspended {
                bundle: id,
                revision: spec.revision,
                cursor: 0,
            };
            self.publish_result(id, result.clone());
            return Ok(result);
        }
        let result = BundleResult::Completed {
            bundle: id,
            revision: spec.revision,
            reads: input.reads,
            region: input.region,
            // A bundle only publishes native choice identities supplied by
            // its caller. It never constructs or owns a plan tree, and it
            // never manufactures a digest from a candidate integer.
            choices: input.choices.clone(),
            candidate: input.candidate,
            aggregate_regions: input.aggregate_regions.clone(),
        };
        self.publish_result(id, result.clone());
        Ok(result)
    }

    fn publish_result(&mut self, id: BundleId, result: BundleResult) {
        if let Some(bundle) = self.bundles.get_mut(&id) {
            bundle.state = match result {
                BundleResult::Completed { .. } => BundleState::Completed,
                BundleResult::NotApplicable { .. } => BundleState::NotApplicable,
                BundleResult::MissingEvidence { .. } => BundleState::MissingEvidence,
                BundleResult::Suspended { .. } => BundleState::Suspended,
            };
            bundle.result = Some(result);
        }
    }

    pub fn p_ready_certificate(&self, policy: QualityPolicyId) -> Option<PReadyCertificate> {
        if self
            .bundles
            .values()
            .any(|bundle| !bundle.result_is_ready())
        {
            return None;
        }
        let mut bundles = Vec::with_capacity(self.bundles.len());
        let mut regions = BTreeSet::new();
        let mut reads = BTreeSet::new();
        let mut candidates = BTreeSet::new();
        let mut certificate_candidate = None;
        let mut certificate_choices: Option<Box<[Fingerprint]>> = None;
        let mut certificate_aggregate_regions: Option<Box<[AggregateRegionWitness]>> = None;
        for (id, bundle) in &self.bundles {
            bundles.push((*id, bundle.spec.revision));
            if let Some(BundleResult::Completed {
                region,
                reads: read_set,
                choices,
                candidate,
                aggregate_regions,
                ..
            }) = &bundle.result
            {
                let candidate = (*candidate)?;
                if let Some(previous) = certificate_candidate {
                    if previous != candidate {
                        return None;
                    }
                } else {
                    certificate_candidate = Some(candidate);
                }
                if let Some(previous) = &certificate_choices {
                    if previous.as_ref() != choices.as_ref() {
                        return None;
                    }
                } else {
                    certificate_choices = Some(choices.clone());
                }
                if let Some(previous) = &certificate_aggregate_regions {
                    if previous.as_ref() != aggregate_regions.as_ref() {
                        return None;
                    }
                } else {
                    certificate_aggregate_regions = Some(aggregate_regions.clone());
                }
                if regions
                    .iter()
                    .next()
                    .is_some_and(|previous| previous != region)
                {
                    return None;
                }
                if reads
                    .iter()
                    .next()
                    .is_some_and(|previous| previous != read_set)
                {
                    return None;
                }
                regions.insert(*region);
                reads.insert(*read_set);
                candidates.insert(candidate);
            }
        }
        // A policy containing only NotApplicable packages is not a quality
        // handoff. At least one native producer must have completed.
        let candidate = certificate_candidate?;
        let choices = certificate_choices?;
        let aggregate_regions = certificate_aggregate_regions.unwrap_or_default();
        Some(PReadyCertificate {
            policy,
            bundles: bundles.into_boxed_slice(),
            candidate,
            choices,
            regions: regions.into_iter().collect(),
            reads: reads.into_iter().collect(),
            candidates: candidates.into_iter().collect(),
            aggregate_regions,
        })
    }
}

fn aggregate_coverage_is_complete(input: &BundleInput) -> bool {
    let Some(candidate) = input.candidate else {
        return false;
    };
    !input.aggregate_regions.is_empty()
        && input.aggregate_regions.iter().all(|witness| {
            witness.covered
                && witness.candidate == candidate
                && witness.anchor.is_valid()
                && input.choices.contains(&witness.anchor_choice)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregate_region(candidate: CandidateId, choices: &[Fingerprint]) -> AggregateRegionWitness {
        AggregateRegionWitness {
            region: Fingerprint(10),
            candidate,
            anchor: CandidateId(8),
            fact_fingerprint: Fingerprint(11),
            anchor_choice: choices[0],
            covered: true,
        }
    }

    fn input(
        capabilities: impl IntoIterator<Item = BundleCapability>,
        facts: impl IntoIterator<Item = BundleFact>,
    ) -> BundleInput {
        BundleInput {
            capabilities: capabilities.into_iter().collect(),
            facts: facts.into_iter().collect(),
            reads: ReadSetId::new(1),
            region: Fingerprint(2),
            applicability_proof: Fingerprint(3),
            choices: Box::new([Fingerprint(5)]),
            candidate: Some(CandidateId(4)),
            aggregate_regions: Box::new([aggregate_region(CandidateId(4), &[Fingerprint(5)])]),
        }
    }

    #[test]
    fn claim_readiness_uses_registered_demand_but_never_certifies_coverage() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let mut value = input(
            [BundleCapability::SharedAggregate],
            [
                BundleFact::AggregateDecomposition,
                BundleFact::NullSemantics,
            ],
        );
        assert!(!registry.claims_are_ready(&value.capabilities, &value.facts));
        value.facts.insert(BundleFact::CteConsumerDemand);
        assert!(registry.claims_are_ready(&value.capabilities, &value.facts));
        value.choices = Box::new([]);
        assert!(matches!(
            registry.evaluate(BundleId(3), &value, 1).unwrap(),
            BundleResult::MissingEvidence { .. }
        ));
        assert!(registry
            .p_ready_certificate(QualityPolicyId::new(1))
            .is_none());
        registry
            .register(QualityBundleSpec::new(
                BundleId(9),
                1,
                [BundleCapability::SharedAggregate],
                [BundleFact::OrderingDemand],
                50,
            ))
            .unwrap();
        assert!(!registry.claims_are_ready(&value.capabilities, &value.facts));
        value.facts.insert(BundleFact::OrderingDemand);
        assert!(registry.claims_are_ready(&value.capabilities, &value.facts));
        value.capabilities.clear();
        value.facts.clear();
        assert!(registry.claims_are_ready(&value.capabilities, &value.facts));
    }

    #[test]
    fn missing_evidence_is_not_not_applicable_and_cannot_certify_ready() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let result = registry
            .evaluate(
                BundleId(3),
                &input(
                    [BundleCapability::SharedAggregate],
                    [BundleFact::AggregateDecomposition],
                ),
                1,
            )
            .unwrap();
        assert!(matches!(result, BundleResult::MissingEvidence { .. }));
        assert_eq!(
            registry.state(BundleId(3)),
            Some(BundleState::MissingEvidence)
        );
        assert!(registry.p_ready_certificate(QualityPolicyId(1)).is_none());
    }

    #[test]
    fn capability_guard_can_prove_not_applicable_before_fact_construction() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let result = registry
            .evaluate(
                BundleId(3),
                &input([BundleCapability::ScanPredicate], []),
                0,
            )
            .unwrap();
        assert!(matches!(
            result,
            BundleResult::NotApplicable {
                proof: Fingerprint(3),
                ..
            }
        ));
        assert_eq!(
            registry.state(BundleId(3)),
            Some(BundleState::NotApplicable)
        );
    }

    #[test]
    fn all_f1_f4_ready_results_form_a_versioned_certificate() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        registry
            .evaluate(
                BundleId(1),
                &input(
                    [BundleCapability::ScanPredicate],
                    [BundleFact::OutputDemand, BundleFact::PredicateDomain],
                ),
                1,
            )
            .unwrap();
        registry
            .evaluate(
                BundleId(2),
                &input(
                    [BundleCapability::ScanPredicate],
                    [BundleFact::OutputDemand, BundleFact::PredicateDomain],
                ),
                1,
            )
            .unwrap();
        registry
            .evaluate(
                BundleId(3),
                &input(
                    [BundleCapability::SharedAggregate],
                    [
                        BundleFact::AggregateDecomposition,
                        BundleFact::CteConsumerDemand,
                        BundleFact::NullSemantics,
                    ],
                ),
                1,
            )
            .unwrap();
        registry
            .evaluate(
                BundleId(4),
                &input(
                    [BundleCapability::CorrelatedSubquery],
                    [BundleFact::PredicateDomain, BundleFact::NullSemantics],
                ),
                1,
            )
            .unwrap();
        let certificate = registry.p_ready_certificate(QualityPolicyId(1)).unwrap();
        assert_eq!(certificate.bundles.len(), 4);
        assert_eq!(certificate.candidate, CandidateId(4));
        assert_eq!(certificate.choices.as_ref(), &[Fingerprint(5)]);
        assert!(certificate.is_complete(&registry));
    }

    #[test]
    fn mixed_candidate_choices_cannot_form_a_certificate() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        for id in [BundleId(1), BundleId(2), BundleId(3), BundleId(4)] {
            let (capabilities, facts) = match id {
                BundleId(1) => (
                    vec![BundleCapability::ScanPredicate],
                    vec![BundleFact::OutputDemand, BundleFact::PredicateDomain],
                ),
                BundleId(2) => (
                    vec![BundleCapability::SmallJoin],
                    vec![BundleFact::JoinRegion, BundleFact::OutputDemand],
                ),
                BundleId(3) => (
                    vec![BundleCapability::SharedAggregate],
                    vec![
                        BundleFact::AggregateDecomposition,
                        BundleFact::CteConsumerDemand,
                        BundleFact::NullSemantics,
                    ],
                ),
                _ => (
                    vec![BundleCapability::CorrelatedSubquery],
                    vec![BundleFact::PredicateDomain, BundleFact::NullSemantics],
                ),
            };
            let mut value = input(capabilities, facts);
            if id == BundleId(4) {
                value.choices = Box::new([Fingerprint(99)]);
            }
            registry.evaluate(id, &value, 1).unwrap();
        }
        assert!(registry.p_ready_certificate(QualityPolicyId(1)).is_none());
    }

    #[test]
    fn native_evaluation_replaces_previous_candidate_evidence() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let evidence = |candidate, choice| NativeQualityEvidence {
            pending_domain_transfers: Box::new([]),
            capabilities: [
                BundleCapability::ScanPredicate,
                BundleCapability::SmallJoin,
                BundleCapability::SharedAggregate,
                BundleCapability::CorrelatedSubquery,
            ]
            .into_iter()
            .collect(),
            facts: [
                BundleFact::OutputDemand,
                BundleFact::PredicateDomain,
                BundleFact::JoinRegion,
                BundleFact::AggregateDecomposition,
                BundleFact::CteConsumerDemand,
                BundleFact::NullSemantics,
            ]
            .into_iter()
            .collect(),
            region: Fingerprint(2),
            applicability_proof: Fingerprint(3),
            choices: Box::new([Fingerprint(choice)]),
            aggregate_regions: Box::new([aggregate_region(candidate, &[Fingerprint(choice)])]),
            selected_rules: Box::new([]),
            shape: NativeQualityShape::default(),
        };
        let first = registry
            .evaluate_native_candidate(
                QualityPolicyId(1),
                CandidateId(4),
                ReadSetId::new(1),
                &evidence(CandidateId(4), 5),
                1,
            )
            .unwrap()
            .unwrap();
        assert_eq!(first.candidate, CandidateId(4));
        let second = registry
            .evaluate_native_candidate(
                QualityPolicyId(1),
                CandidateId(7),
                ReadSetId::new(2),
                &evidence(CandidateId(7), 6),
                1,
            )
            .unwrap()
            .unwrap();
        assert_eq!(second.candidate, CandidateId(7));
        assert_eq!(second.choices.as_ref(), &[Fingerprint(6)]);
        assert!(!first.is_complete(&registry));
        assert!(registry.result(BundleId(1)).is_some_and(|result| matches!(
            result,
            BundleResult::Completed {
                candidate: Some(CandidateId(7)),
                choices,
                ..
            } if choices.as_ref() == [Fingerprint(6)]
        )));
    }

    #[test]
    fn partial_shared_aggregate_evidence_cannot_certify_pready() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let value = input(
            vec![BundleCapability::SharedAggregate],
            vec![
                BundleFact::AggregateDecomposition,
                BundleFact::NullSemantics,
            ],
        );
        registry.evaluate(BundleId(3), &value, 1).unwrap();
        assert!(matches!(
            registry.result(BundleId(3)),
            Some(BundleResult::MissingEvidence { missing, .. })
                if missing.contains(&BundleFact::CteConsumerDemand)
        ));
        assert!(registry.p_ready_certificate(QualityPolicyId(1)).is_none());
    }

    #[test]
    fn mixed_read_sets_or_regions_cannot_form_a_certificate() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        for id in [BundleId(1), BundleId(2), BundleId(3), BundleId(4)] {
            let (capabilities, facts) = match id {
                BundleId(1) => (
                    vec![BundleCapability::ScanPredicate],
                    vec![BundleFact::OutputDemand, BundleFact::PredicateDomain],
                ),
                BundleId(2) => (
                    vec![BundleCapability::SmallJoin],
                    vec![BundleFact::JoinRegion, BundleFact::OutputDemand],
                ),
                BundleId(3) => (
                    vec![BundleCapability::SharedAggregate],
                    vec![
                        BundleFact::AggregateDecomposition,
                        BundleFact::CteConsumerDemand,
                        BundleFact::NullSemantics,
                    ],
                ),
                _ => (
                    vec![BundleCapability::CorrelatedSubquery],
                    vec![BundleFact::PredicateDomain, BundleFact::NullSemantics],
                ),
            };
            let mut value = input(capabilities, facts);
            if id == BundleId(4) {
                value.reads = ReadSetId::new(2);
                value.region = Fingerprint(99);
            }
            registry.evaluate(id, &value, 1).unwrap();
        }
        assert!(registry.p_ready_certificate(QualityPolicyId(1)).is_none());
    }

    #[test]
    fn all_not_applicable_packages_do_not_certify_pready() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let value = input(Vec::<BundleCapability>::new(), Vec::<BundleFact>::new());
        for id in [BundleId(1), BundleId(2), BundleId(3), BundleId(4)] {
            registry.evaluate(id, &value, 1).unwrap();
        }
        assert!(registry.p_ready_certificate(QualityPolicyId(1)).is_none());
    }

    #[test]
    fn budget_suspension_preserves_an_obligation() {
        let mut registry = QualityBundleRegistry::default();
        registry
            .register(QualityBundleSpec::new(
                BundleId(9),
                2,
                [BundleCapability::SmallJoin],
                [BundleFact::JoinRegion],
                1,
            ))
            .unwrap();
        let result = registry
            .evaluate(
                BundleId(9),
                &input([BundleCapability::SmallJoin], [BundleFact::JoinRegion]),
                0,
            )
            .unwrap();
        assert!(matches!(result, BundleResult::Suspended { cursor: 0, .. }));
        assert!(registry.p_ready_certificate(QualityPolicyId(1)).is_none());
    }

    #[test]
    fn aggregate_bundle_requires_every_selected_region() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let mut value = input(
            [BundleCapability::SharedAggregate],
            [
                BundleFact::AggregateDecomposition,
                BundleFact::CteConsumerDemand,
                BundleFact::NullSemantics,
            ],
        );
        let mut second = aggregate_region(CandidateId(4), &[Fingerprint(5)]);
        second.region = Fingerprint(12);
        second.covered = false;
        value.aggregate_regions = Box::new([value.aggregate_regions[0].clone(), second]);
        let result = registry.evaluate(BundleId(3), &value, 1).unwrap();
        assert!(matches!(
            result,
            BundleResult::MissingEvidence { missing, .. }
                if missing.contains(&BundleFact::AggregateDecomposition)
        ));
    }

    #[test]
    fn aggregate_bundle_rejects_witness_from_another_candidate_or_choice_set() {
        let mut registry = QualityBundleRegistry::default();
        registry.register_builtin_f1_f4().unwrap();
        let mut value = input(
            [BundleCapability::SharedAggregate],
            [
                BundleFact::AggregateDecomposition,
                BundleFact::CteConsumerDemand,
                BundleFact::NullSemantics,
            ],
        );
        value.aggregate_regions[0].candidate = CandidateId(99);
        assert!(matches!(
            registry.evaluate(BundleId(3), &value, 1).unwrap(),
            BundleResult::MissingEvidence { missing, .. }
                if missing.contains(&BundleFact::AggregateDecomposition)
        ));

        value.aggregate_regions[0].candidate = CandidateId(4);
        value.aggregate_regions[0].anchor_choice = Fingerprint(99);
        assert!(matches!(
            registry.evaluate(BundleId(3), &value, 1).unwrap(),
            BundleResult::MissingEvidence { missing, .. }
                if missing.contains(&BundleFact::AggregateDecomposition)
        ));
    }
}
