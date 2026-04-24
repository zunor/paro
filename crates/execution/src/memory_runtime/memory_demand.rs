// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Operator memory demand hints.

use paro_common::memory::MemoryDomain;

/// Demand for a single memory domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryDomainDemand {
    pub domain: MemoryDomain,
    pub min_bytes: usize,
    pub desired_bytes: usize,
    pub max_bytes: Option<usize>,
}

impl MemoryDomainDemand {
    pub fn new(domain: MemoryDomain, min_bytes: usize, desired_bytes: usize) -> Self {
        Self {
            domain,
            min_bytes,
            desired_bytes: desired_bytes.max(min_bytes),
            max_bytes: None,
        }
    }

    pub fn update_in_place(&mut self, new_desired: usize) {
        let capped = self
            .max_bytes
            .map(|max| new_desired.min(max))
            .unwrap_or(new_desired);
        self.desired_bytes = capped.max(self.min_bytes);
    }
}

/// Multi-domain memory demand.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryDemand {
    domains: Vec<MemoryDomainDemand>,
}

impl MemoryDemand {
    pub fn new(domains: Vec<MemoryDomainDemand>) -> Self {
        Self { domains }
    }

    pub fn host(min_bytes: usize, desired_bytes: usize) -> Self {
        Self::new(vec![MemoryDomainDemand::new(
            MemoryDomain::Host,
            min_bytes,
            desired_bytes,
        )])
    }

    pub fn domains(&self) -> &[MemoryDomainDemand] {
        &self.domains
    }

    pub fn update_in_place(&mut self, domain: MemoryDomain, new_desired: usize) {
        if let Some(demand) = self.domains.iter_mut().find(|d| d.domain == domain) {
            demand.update_in_place(new_desired);
        } else {
            self.domains
                .push(MemoryDomainDemand::new(domain, 0, new_desired));
        }
    }
}
