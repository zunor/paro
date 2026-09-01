// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Versioned dependencies that make an immutable physical plan reusable.

use std::collections::BTreeMap;

use crate::physical::identity::Fingerprint;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanDependencies {
    pub catalog_versions: BTreeMap<Fingerprint, u64>,
    pub statistics_compatibility: BTreeMap<Fingerprint, u64>,
    pub graph_generations: BTreeMap<Fingerprint, u64>,
    pub provider_capabilities: BTreeMap<Fingerprint, u64>,
    pub search_index_generations: BTreeMap<Fingerprint, u64>,
    pub machine_calibration_revision: Fingerprint,
    pub estimator_revision: Fingerprint,
    pub routine_artifacts: BTreeMap<Fingerprint, u64>,
    pub external_runtime_profiles: BTreeMap<Fingerprint, u64>,
    pub model_artifacts: BTreeMap<Fingerprint, u64>,
    pub quality_policy_revision: Option<Fingerprint>,
    pub rule_set_revision: Fingerprint,
    pub plan_stability_policy_revision: Fingerprint,
    pub optimizer_config_fingerprint: Fingerprint,
    pub physical_abi_revision: Fingerprint,
}

impl PlanDependencies {
    pub fn exact_match(&self, current: &Self) -> bool {
        self == current
    }

    /// Revisions that change the candidate or score space force a cold anchor.
    /// Ordinary compatible statistics movement is intentionally excluded.
    pub fn hysteresis_space_matches(&self, current: &Self) -> bool {
        self.machine_calibration_revision == current.machine_calibration_revision
            && self.estimator_revision == current.estimator_revision
            && self.routine_artifacts == current.routine_artifacts
            && self.external_runtime_profiles == current.external_runtime_profiles
            && self.model_artifacts == current.model_artifacts
            && self.rule_set_revision == current.rule_set_revision
            && self.plan_stability_policy_revision == current.plan_stability_policy_revision
            && self.optimizer_config_fingerprint == current.optimizer_config_fingerprint
            && self.physical_abi_revision == current.physical_abi_revision
    }
}
