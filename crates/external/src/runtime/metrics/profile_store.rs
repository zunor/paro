// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::runtime::dispatch::policy::ExternalDispatchPolicy;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RoutinePerfProfileKey {
    pub tenant_or_security_domain: String,
    pub env_artifact_id: String,
    pub routine_generation: u64,
    pub backend_kind: String,
    pub runtime_contract: String,
    pub shape_class: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RoutinePerfProfile {
    pub preferred_target_batch_bytes: Option<u64>,
    pub preferred_local_spin_budget_us: Option<u64>,
    pub stable_cache_enable_threshold_us: Option<u64>,
    pub output_expansion_factor_p50: Option<f64>,
    pub output_expansion_factor_p95: Option<f64>,
    pub queue_wait_p50_us: Option<u64>,
    pub queue_wait_p95_us: Option<u64>,
    pub kernel_time_p50_us: Option<u64>,
    pub kernel_time_p95_us: Option<u64>,
    pub warm_hit_ratio: Option<f64>,
    pub gil_bound: Option<bool>,
    pub releases_gil: Option<bool>,
    pub library_parallelism_enabled: Option<bool>,
    pub observed_batches: usize,
}

impl RoutinePerfProfile {
    pub fn cold_start_policy(&self, defaults: &ExternalDispatchPolicy) -> ExternalDispatchPolicy {
        let mut policy = defaults.clone();
        if let Some(batch_bytes) = self.preferred_target_batch_bytes {
            policy.target_batch_bytes = batch_bytes;
        }
        if let Some(local_spin_budget_us) = self.preferred_local_spin_budget_us {
            policy.local_spin_budget_us = local_spin_budget_us;
        }
        policy
    }
}

#[derive(Debug, Default)]
pub struct InMemoryProfileStore {
    profiles: BTreeMap<RoutinePerfProfileKey, RoutinePerfProfile>,
}

impl InMemoryProfileStore {
    pub fn get_or_default(&mut self, key: &RoutinePerfProfileKey) -> &mut RoutinePerfProfile {
        self.profiles.entry(key.clone()).or_default()
    }
}
