// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::runtime::metrics::profile_store::RoutinePerfProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapWindow {
    pub warmup_batches: usize,
    pub canary_batches: usize,
}

impl BootstrapWindow {
    pub fn total_batches(self) -> usize {
        self.warmup_batches + self.canary_batches
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PerfObservation {
    pub target_batch_bytes: u64,
    pub queue_wait_us: u64,
    pub kernel_time_us: u64,
    pub output_expansion_factor: f64,
    pub warm_hit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AutotuningPolicy {
    pub bootstrap: BootstrapWindow,
    pub ewma_alpha: f64,
}

impl Default for AutotuningPolicy {
    fn default() -> Self {
        Self {
            bootstrap: BootstrapWindow {
                warmup_batches: 4,
                canary_batches: 4,
            },
            ewma_alpha: 0.25,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Autotuner;

impl Autotuner {
    pub fn observe(
        &self,
        profile: &mut RoutinePerfProfile,
        observation: PerfObservation,
        policy: &AutotuningPolicy,
    ) {
        profile.observed_batches += 1;
        if profile.observed_batches <= policy.bootstrap.total_batches() {
            return;
        }

        profile.preferred_target_batch_bytes = Some(update_ewma_u64(
            profile.preferred_target_batch_bytes,
            observation.target_batch_bytes,
            policy.ewma_alpha,
        ));
        profile.queue_wait_p50_us = Some(update_ewma_u64(
            profile.queue_wait_p50_us,
            observation.queue_wait_us,
            policy.ewma_alpha,
        ));
        profile.kernel_time_p50_us = Some(update_ewma_u64(
            profile.kernel_time_p50_us,
            observation.kernel_time_us,
            policy.ewma_alpha,
        ));
        profile.output_expansion_factor_p50 = Some(update_ewma_f64(
            profile.output_expansion_factor_p50,
            observation.output_expansion_factor,
            policy.ewma_alpha,
        ));
        profile.warm_hit_ratio = Some(update_ewma_f64(
            profile.warm_hit_ratio,
            if observation.warm_hit { 1.0 } else { 0.0 },
            policy.ewma_alpha,
        ));

        profile.queue_wait_p95_us = Some(
            profile
                .queue_wait_p50_us
                .unwrap()
                .max(observation.queue_wait_us),
        );
        profile.kernel_time_p95_us = Some(
            profile
                .kernel_time_p50_us
                .unwrap()
                .max(observation.kernel_time_us),
        );
        profile.output_expansion_factor_p95 = Some(
            profile
                .output_expansion_factor_p50
                .unwrap()
                .max(observation.output_expansion_factor),
        );
        profile.preferred_local_spin_budget_us =
            Some((profile.queue_wait_p50_us.unwrap() / 4).clamp(10, 1_000));
        profile.stable_cache_enable_threshold_us = Some(
            profile
                .kernel_time_p50_us
                .unwrap()
                .saturating_add(profile.queue_wait_p50_us.unwrap()),
        );
    }
}

fn update_ewma_u64(current: Option<u64>, sample: u64, alpha: f64) -> u64 {
    match current {
        Some(current) => {
            ((current as f64) * (1.0 - alpha) + (sample as f64) * alpha).round() as u64
        }
        None => sample,
    }
}

fn update_ewma_f64(current: Option<f64>, sample: f64, alpha: f64) -> f64 {
    match current {
        Some(current) => current * (1.0 - alpha) + sample * alpha,
        None => sample,
    }
}
