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

use super::ids::{CandidateId, Fingerprint, QualityPolicyId};
use super::tasks::ReadSetId;

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
    pub candidate: Option<CandidateId>,
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
    pub regions: Box<[Fingerprint]>,
    pub reads: Box<[ReadSetId]>,
    pub candidates: Box<[CandidateId]>,
}

impl PReadyCertificate {
    pub fn is_complete(&self, registry: &QualityBundleRegistry) -> bool {
        self.bundles.iter().all(|(id, revision)| {
            registry
                .bundles
                .get(id)
                .is_some_and(|bundle| bundle.spec.revision == *revision && bundle.result_is_ready())
        })
    }
}

impl RegisteredBundle {
    fn result_is_ready(&self) -> bool {
        self.result.as_ref().is_some_and(BundleResult::is_ready)
    }
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

    pub fn result(&self, id: BundleId) -> Option<&BundleResult> {
        self.bundles
            .get(&id)
            .and_then(|bundle| bundle.result.as_ref())
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
            // its caller.  It never constructs or owns a plan tree.
            choices: input
                .candidate
                .into_iter()
                .map(|candidate| Fingerprint(candidate.0 as u128))
                .collect(),
            candidate: input.candidate,
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
        for (id, bundle) in &self.bundles {
            bundles.push((*id, bundle.spec.revision));
            if let Some(BundleResult::Completed {
                region,
                reads: read_set,
                candidate,
                ..
            }) = &bundle.result
            {
                regions.insert(*region);
                reads.insert(*read_set);
                if let Some(candidate) = candidate {
                    candidates.insert(*candidate);
                }
            }
        }
        Some(PReadyCertificate {
            policy,
            bundles: bundles.into_boxed_slice(),
            regions: regions.into_iter().collect(),
            reads: reads.into_iter().collect(),
            candidates: candidates.into_iter().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            candidate: Some(CandidateId(4)),
        }
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
        assert!(certificate.is_complete(&registry));
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
}
