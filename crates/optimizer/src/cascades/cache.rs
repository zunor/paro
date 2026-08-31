//! Dependency-complete cache identity and bounded plan-stability hysteresis.

use super::ids::Fingerprint;
pub use crate::physical::PlanDependencies;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StabilityDriftLedger {
    pub rejected_normalized_improvement: f64,
    pub consecutive_retains: u32,
}

impl Default for StabilityDriftLedger {
    fn default() -> Self {
        Self {
            rejected_normalized_improvement: 0.0,
            consecutive_retains: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IncumbentPlan {
    pub fingerprint: Fingerprint,
    pub dependencies: PlanDependencies,
    pub ledger: StabilityDriftLedger,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompileStabilityMode {
    Cold { ignore_pin: bool },
    Hysteretic { incumbent: Box<IncumbentPlan> },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StabilityPolicy {
    pub absolute_switch_margin: f64,
    pub relative_switch_margin: f64,
    pub max_accumulated_drift: f64,
    pub max_consecutive_retains: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReanchorReason {
    ExplicitCold,
    CandidateSpaceRevision,
    IncumbentInfeasible,
    AccumulatedDrift,
    ConsecutiveRetainLimit,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StabilityDecision {
    ChooseChallenger { reanchor: Option<ReanchorReason> },
    RetainIncumbent { ledger: StabilityDriftLedger },
}

impl StabilityPolicy {
    pub fn decide(
        self,
        mode: &CompileStabilityMode,
        current_dependencies: &PlanDependencies,
        incumbent_score: Option<f64>,
        challenger_score: f64,
    ) -> StabilityDecision {
        let CompileStabilityMode::Hysteretic { incumbent } = mode else {
            return StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::ExplicitCold),
            };
        };
        if !incumbent
            .dependencies
            .hysteresis_space_matches(current_dependencies)
        {
            return StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::CandidateSpaceRevision),
            };
        }
        let Some(incumbent_score) = incumbent_score.filter(|score| score.is_finite()) else {
            return StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::IncumbentInfeasible),
            };
        };
        let absolute = (incumbent_score - challenger_score).max(0.0);
        let normalized = absolute / incumbent_score.abs().max(1.0);
        let switch =
            absolute > self.absolute_switch_margin && normalized > self.relative_switch_margin;
        if switch {
            return StabilityDecision::ChooseChallenger { reanchor: None };
        }
        let ledger = StabilityDriftLedger {
            rejected_normalized_improvement: incumbent.ledger.rejected_normalized_improvement
                + normalized,
            consecutive_retains: incumbent.ledger.consecutive_retains.saturating_add(1),
        };
        if ledger.rejected_normalized_improvement > self.max_accumulated_drift {
            return StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::AccumulatedDrift),
            };
        }
        if ledger.consecutive_retains > self.max_consecutive_retains {
            return StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::ConsecutiveRetainLimit),
            };
        }
        StabilityDecision::RetainIncumbent { ledger }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn dependencies(revision: u128) -> PlanDependencies {
        PlanDependencies {
            catalog_versions: BTreeMap::new(),
            statistics_compatibility: BTreeMap::new(),
            graph_generations: BTreeMap::new(),
            provider_capabilities: BTreeMap::new(),
            search_index_generations: BTreeMap::new(),
            machine_calibration_revision: Fingerprint(revision),
            estimator_revision: Fingerprint(1),
            routine_artifacts: BTreeMap::new(),
            external_runtime_profiles: BTreeMap::new(),
            model_artifacts: BTreeMap::new(),
            quality_policy_revision: None,
            rule_set_revision: Fingerprint(1),
            plan_stability_policy_revision: Fingerprint(1),
            optimizer_config_fingerprint: Fingerprint(1),
            physical_abi_revision: Fingerprint(1),
        }
    }

    fn policy() -> StabilityPolicy {
        StabilityPolicy {
            absolute_switch_margin: 5.0,
            relative_switch_margin: 0.1,
            max_accumulated_drift: 0.2,
            max_consecutive_retains: 2,
        }
    }

    #[test]
    fn calibration_change_forces_reanchor() {
        let mode = CompileStabilityMode::Hysteretic {
            incumbent: Box::new(IncumbentPlan {
                fingerprint: Fingerprint(9),
                dependencies: dependencies(1),
                ledger: Default::default(),
            }),
        };
        assert!(matches!(
            policy().decide(&mode, &dependencies(2), Some(100.0), 99.0),
            StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::CandidateSpaceRevision)
            }
        ));
    }

    #[test]
    fn repeated_near_ties_are_automatically_reanchored() {
        let mut incumbent = IncumbentPlan {
            fingerprint: Fingerprint(9),
            dependencies: dependencies(1),
            ledger: Default::default(),
        };
        for _ in 0..2 {
            let decision = policy().decide(
                &CompileStabilityMode::Hysteretic {
                    incumbent: Box::new(incumbent.clone()),
                },
                &dependencies(1),
                Some(100.0),
                99.0,
            );
            let StabilityDecision::RetainIncumbent { ledger } = decision else {
                panic!("near tie should initially retain incumbent")
            };
            incumbent.ledger = ledger;
        }
        assert!(matches!(
            policy().decide(
                &CompileStabilityMode::Hysteretic {
                    incumbent: Box::new(incumbent),
                },
                &dependencies(1),
                Some(100.0),
                99.0,
            ),
            StabilityDecision::ChooseChallenger {
                reanchor: Some(ReanchorReason::ConsecutiveRetainLimit)
            }
        ));
    }
}
