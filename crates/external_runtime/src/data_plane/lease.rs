// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReclaimHint {
    None,
    MadvCold,
    MadvPageOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrossDomainReuseStrategy {
    TrustSameDomain,
    ZeroFill,
    GenerationBump,
    ZeroFillAndGenerationBump,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighSecurityArenaMode {
    Disabled,
    PreferMemfdSecret,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseReclaimPolicy {
    pub zero_fill_before_reuse: bool,
    pub bump_generation_before_cross_domain_reuse: bool,
    pub reclaim_hint: ReclaimHint,
    pub high_security_mode: HighSecurityArenaMode,
}

impl Default for LeaseReclaimPolicy {
    fn default() -> Self {
        Self {
            zero_fill_before_reuse: true,
            bump_generation_before_cross_domain_reuse: true,
            reclaim_hint: ReclaimHint::None,
            high_security_mode: HighSecurityArenaMode::Disabled,
        }
    }
}

impl LeaseReclaimPolicy {
    pub fn reuse_strategy(&self, cross_security_domain: bool) -> CrossDomainReuseStrategy {
        if !cross_security_domain {
            return if self.zero_fill_before_reuse {
                CrossDomainReuseStrategy::ZeroFill
            } else {
                CrossDomainReuseStrategy::TrustSameDomain
            };
        }

        match (
            self.zero_fill_before_reuse,
            self.bump_generation_before_cross_domain_reuse,
        ) {
            (true, true) => CrossDomainReuseStrategy::ZeroFillAndGenerationBump,
            (true, false) => CrossDomainReuseStrategy::ZeroFill,
            (false, true) => CrossDomainReuseStrategy::GenerationBump,
            (false, false) => CrossDomainReuseStrategy::TrustSameDomain,
        }
    }
}
