// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Versioned dependencies that make an immutable physical plan reusable.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

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

    /// Form the conservative dependency set for an artifact containing more
    /// than one executable variant. Static revisions must describe one common
    /// candidate space; object-level maps are unioned and conflicting versions
    /// are rejected instead of allowing an ambiguous cache contract.
    pub fn merge_artifact(&mut self, other: &Self) -> Result<()> {
        merge_versions(
            &mut self.catalog_versions,
            &other.catalog_versions,
            "catalog",
        )?;
        merge_versions(
            &mut self.statistics_compatibility,
            &other.statistics_compatibility,
            "statistics compatibility",
        )?;
        merge_versions(
            &mut self.graph_generations,
            &other.graph_generations,
            "graph",
        )?;
        merge_versions(
            &mut self.provider_capabilities,
            &other.provider_capabilities,
            "provider capability",
        )?;
        merge_versions(
            &mut self.search_index_generations,
            &other.search_index_generations,
            "search index",
        )?;
        merge_versions(
            &mut self.routine_artifacts,
            &other.routine_artifacts,
            "routine artifact",
        )?;
        merge_versions(
            &mut self.external_runtime_profiles,
            &other.external_runtime_profiles,
            "external runtime profile",
        )?;
        merge_versions(
            &mut self.model_artifacts,
            &other.model_artifacts,
            "model artifact",
        )?;
        for (name, left, right) in [
            (
                "machine calibration",
                self.machine_calibration_revision,
                other.machine_calibration_revision,
            ),
            (
                "estimator",
                self.estimator_revision,
                other.estimator_revision,
            ),
            ("rule set", self.rule_set_revision, other.rule_set_revision),
            (
                "plan stability policy",
                self.plan_stability_policy_revision,
                other.plan_stability_policy_revision,
            ),
            (
                "optimizer config",
                self.optimizer_config_fingerprint,
                other.optimizer_config_fingerprint,
            ),
            (
                "physical ABI",
                self.physical_abi_revision,
                other.physical_abi_revision,
            ),
        ] {
            if left != right {
                return Err(paro_error::internal(format!(
                    "physical portfolio mixes {name} revisions"
                )));
            }
        }
        if self.quality_policy_revision != other.quality_policy_revision {
            return Err(paro_error::internal(
                "physical portfolio mixes result-quality policy revisions",
            ));
        }
        Ok(())
    }
}

fn merge_versions(
    target: &mut BTreeMap<Fingerprint, u64>,
    source: &BTreeMap<Fingerprint, u64>,
    domain: &str,
) -> Result<()> {
    for (object, version) in source {
        if let Some(existing) = target.insert(*object, *version) {
            if existing != *version {
                return Err(paro_error::internal(format!(
                    "physical portfolio contains conflicting {domain} versions"
                )));
            }
        }
    }
    Ok(())
}
