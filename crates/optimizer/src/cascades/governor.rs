// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Milestone and stop-policy bookkeeping for interactive Cascades search.
//!
//! The governor is intentionally conservative.  It can choose a structural
//! or resource stop, but an economic stop is inert until a versioned,
//! in-scope calibration is supplied.  In particular, missing execution
//! benefit evidence is not converted to zero benefit.

use std::collections::BTreeSet;
use std::time::Duration;

use paro_common::error::{self as paro_error, Result};

use super::budget::BudgetDimension;
use super::ids::{CalibrationRevisionId, CandidateId, Fingerprint, QualityPolicyId};
use super::tasks::{BoundProofId, StopReason};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlanMilestone {
    None,
    PSafe,
    PReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GovernorStop {
    ProofStop(BoundProofId),
    EconomicStop(CalibrationRevisionId),
    ResourceStop(BudgetDimension),
    CalibrationUnavailable,
}

impl GovernorStop {
    pub const fn is_search_complete(self) -> bool {
        matches!(self, Self::ProofStop(_))
    }

    pub const fn as_task_reason(self) -> Option<StopReason> {
        match self {
            Self::ProofStop(proof) => Some(StopReason::ProofStop { proof }),
            Self::EconomicStop(calibration) => Some(StopReason::EconomicStop { calibration }),
            Self::ResourceStop(dimension) => Some(StopReason::ResourceStop { dimension }),
            Self::CalibrationUnavailable => Some(StopReason::CalibrationUnavailable),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalibrationScope {
    pub revision: CalibrationRevisionId,
    pub scope: Fingerprint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanningPolicy {
    pub id: QualityPolicyId,
    pub revision: u32,
    pub economic_stop_enabled: bool,
    pub calibration: Option<CalibrationScope>,
    pub optional_time_limit: Option<Duration>,
    /// Small, explicitly calibrated margin in milliseconds.  It is a policy
    /// input, not a claim that optimizer cost units are milliseconds.
    pub minimum_economic_margin_ms: f64,
}

impl Default for PlanningPolicy {
    fn default() -> Self {
        Self {
            id: QualityPolicyId::default(),
            revision: 1,
            economic_stop_enabled: false,
            calibration: None,
            optional_time_limit: None,
            minimum_economic_margin_ms: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EconomicSignal {
    pub incremental_compile_ms: f64,
    pub current_first_execution_ms: f64,
    pub after_first_execution_upper_ms: f64,
}

impl EconomicSignal {
    pub fn validate(self) -> Result<()> {
        let values = [
            self.incremental_compile_ms,
            self.current_first_execution_ms,
            self.after_first_execution_upper_ms,
        ];
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(paro_error::internal(
                "economic signal must be finite and non-negative",
            ));
        }
        Ok(())
    }

    pub fn possible_savings_upper_ms(self) -> f64 {
        (self.current_first_execution_ms - self.after_first_execution_upper_ms).max(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EconomicDecision {
    Continue,
    Stop,
    CalibrationUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernorSnapshot {
    pub milestone: PlanMilestone,
    pub safe_candidate: Option<CandidateId>,
    pub ready_bundles: usize,
    pub pending_bundles: usize,
    pub obligations: usize,
    pub stop: Option<GovernorStop>,
    pub omitted_work: bool,
}

#[derive(Debug, Clone)]
pub struct Governor {
    policy: PlanningPolicy,
    milestone: PlanMilestone,
    safe_candidate: Option<CandidateId>,
    required_bundles: BTreeSet<Fingerprint>,
    ready_bundles: BTreeSet<Fingerprint>,
    obligations: BTreeSet<Fingerprint>,
    stop: Option<GovernorStop>,
    last_calibration_status: Option<GovernorStop>,
}

impl Governor {
    pub fn new(policy: PlanningPolicy) -> Result<Self> {
        if !policy.minimum_economic_margin_ms.is_finite() || policy.minimum_economic_margin_ms < 0.0
        {
            return Err(paro_error::internal(
                "planning policy margin must be finite and non-negative",
            ));
        }
        Ok(Self {
            policy,
            milestone: PlanMilestone::None,
            safe_candidate: None,
            required_bundles: BTreeSet::new(),
            ready_bundles: BTreeSet::new(),
            obligations: BTreeSet::new(),
            stop: None,
            last_calibration_status: None,
        })
    }

    pub fn policy(&self) -> &PlanningPolicy {
        &self.policy
    }

    pub fn milestone(&self) -> PlanMilestone {
        self.milestone
    }

    pub fn register_bundle(&mut self, bundle: Fingerprint) -> Result<()> {
        if !self.required_bundles.insert(bundle) {
            return Err(paro_error::internal(
                "quality bundle was registered twice in one policy",
            ));
        }
        Ok(())
    }

    pub fn mark_safe(&mut self, candidate: CandidateId) {
        self.safe_candidate = Some(candidate);
        self.milestone = self.milestone.max(PlanMilestone::PSafe);
    }

    pub fn mark_bundle_complete(&mut self, bundle: Fingerprint) -> Result<()> {
        if !self.required_bundles.contains(&bundle) {
            return Err(paro_error::internal(
                "quality bundle completion was not registered",
            ));
        }
        self.ready_bundles.insert(bundle);
        self.refresh_ready_milestone();
        Ok(())
    }

    pub fn add_obligation(&mut self, obligation: Fingerprint) {
        self.obligations.insert(obligation);
    }

    pub fn discharge_obligation(&mut self, obligation: Fingerprint) {
        self.obligations.remove(&obligation);
        self.refresh_ready_milestone();
    }

    fn refresh_ready_milestone(&mut self) {
        if self.safe_candidate.is_some()
            && self.ready_bundles == self.required_bundles
            && self.obligations.is_empty()
        {
            self.milestone = PlanMilestone::PReady;
        }
    }

    pub fn proof_stop(&mut self, proof: BoundProofId) {
        self.stop = Some(GovernorStop::ProofStop(proof));
    }

    pub fn resource_stop(&mut self, dimension: BudgetDimension) {
        if !matches!(self.stop, Some(GovernorStop::ProofStop(_))) {
            self.stop = Some(GovernorStop::ResourceStop(dimension));
        }
    }

    pub fn consider_economic(
        &mut self,
        signal: EconomicSignal,
        calibration: Option<CalibrationScope>,
    ) -> Result<EconomicDecision> {
        signal.validate()?;
        if !self.policy.economic_stop_enabled {
            return Ok(EconomicDecision::Continue);
        }
        let Some(expected) = self.policy.calibration else {
            self.last_calibration_status = Some(GovernorStop::CalibrationUnavailable);
            return Ok(EconomicDecision::CalibrationUnavailable);
        };
        if calibration != Some(expected) {
            self.last_calibration_status = Some(GovernorStop::CalibrationUnavailable);
            return Ok(EconomicDecision::CalibrationUnavailable);
        }
        let savings = signal.possible_savings_upper_ms();
        if savings <= signal.incremental_compile_ms + self.policy.minimum_economic_margin_ms {
            self.stop = Some(GovernorStop::EconomicStop(expected.revision));
            return Ok(EconomicDecision::Stop);
        }
        Ok(EconomicDecision::Continue)
    }

    pub fn stop(&self) -> Option<GovernorStop> {
        self.stop
    }

    pub fn last_calibration_status(&self) -> Option<GovernorStop> {
        self.last_calibration_status
    }

    pub fn is_search_complete(&self) -> bool {
        self.stop.is_some_and(GovernorStop::is_search_complete)
    }

    pub fn snapshot(&self) -> GovernorSnapshot {
        GovernorSnapshot {
            milestone: self.milestone,
            safe_candidate: self.safe_candidate,
            ready_bundles: self.ready_bundles.len(),
            pending_bundles: self
                .required_bundles
                .len()
                .saturating_sub(self.ready_bundles.len()),
            obligations: self.obligations.len(),
            stop: self.stop,
            omitted_work: !self.obligations.is_empty()
                || self.stop.is_some_and(|stop| !stop.is_search_complete()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAFE: CandidateId = CandidateId(3);
    const BUNDLE: Fingerprint = Fingerprint(7);

    #[test]
    fn ready_requires_safe_candidate_and_all_obligations() {
        let mut governor = Governor::new(PlanningPolicy::default()).unwrap();
        governor.register_bundle(BUNDLE).unwrap();
        governor.add_obligation(Fingerprint(9));
        governor.mark_bundle_complete(BUNDLE).unwrap();
        assert_eq!(governor.milestone(), PlanMilestone::None);
        governor.mark_safe(SAFE);
        assert_eq!(governor.milestone(), PlanMilestone::PSafe);
        governor.discharge_obligation(Fingerprint(9));
        assert_eq!(governor.milestone(), PlanMilestone::PReady);
    }

    #[test]
    fn missing_calibration_does_not_turn_unknown_savings_into_zero() {
        let mut policy = PlanningPolicy::default();
        policy.economic_stop_enabled = true;
        let mut governor = Governor::new(policy).unwrap();
        let decision = governor
            .consider_economic(
                EconomicSignal {
                    incremental_compile_ms: 2.0,
                    current_first_execution_ms: 100.0,
                    after_first_execution_upper_ms: 0.0,
                },
                None,
            )
            .unwrap();
        assert_eq!(decision, EconomicDecision::CalibrationUnavailable);
        assert_eq!(governor.stop(), None);
        assert_eq!(
            governor.last_calibration_status(),
            Some(GovernorStop::CalibrationUnavailable)
        );
    }

    #[test]
    fn economic_stop_requires_matching_versioned_scope() {
        let scope = CalibrationScope {
            revision: CalibrationRevisionId(4),
            scope: Fingerprint(11),
        };
        let mut policy = PlanningPolicy::default();
        policy.economic_stop_enabled = true;
        policy.calibration = Some(scope);
        let mut governor = Governor::new(policy).unwrap();
        let signal = EconomicSignal {
            incremental_compile_ms: 4.0,
            current_first_execution_ms: 10.0,
            after_first_execution_upper_ms: 9.0,
        };
        assert_eq!(
            governor
                .consider_economic(
                    signal,
                    Some(CalibrationScope {
                        revision: CalibrationRevisionId(5),
                        ..scope
                    })
                )
                .unwrap(),
            EconomicDecision::CalibrationUnavailable
        );
        assert_eq!(
            governor.consider_economic(signal, Some(scope)).unwrap(),
            EconomicDecision::Stop
        );
        assert_eq!(
            governor.stop(),
            Some(GovernorStop::EconomicStop(CalibrationRevisionId(4)))
        );
    }

    #[test]
    fn proof_stop_is_the_only_complete_stop() {
        let mut governor = Governor::new(PlanningPolicy::default()).unwrap();
        governor.resource_stop(BudgetDimension::Group);
        assert!(!governor.is_search_complete());
        governor.proof_stop(BoundProofId(2));
        assert!(governor.is_search_complete());
        assert!(!governor.snapshot().omitted_work);
    }
}
