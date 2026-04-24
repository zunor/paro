// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Memory domains.

/// Number of memory domains exposed in runtime snapshots.
pub const MEMORY_DOMAIN_COUNT: usize = 4;

/// Physical memory domain for grant accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MemoryDomain {
    /// Ordinary host memory.
    #[default]
    Host,
    /// Pinned host memory for device transfers.
    PinnedHost,
    /// Device-local memory.
    Device,
    /// Unified memory with runtime migration.
    Unified,
}

impl MemoryDomain {
    pub fn all() -> &'static [MemoryDomain] {
        &[
            MemoryDomain::Host,
            MemoryDomain::PinnedHost,
            MemoryDomain::Device,
            MemoryDomain::Unified,
        ]
    }

    #[inline]
    pub fn as_index(self) -> usize {
        match self {
            MemoryDomain::Host => 0,
            MemoryDomain::PinnedHost => 1,
            MemoryDomain::Device => 2,
            MemoryDomain::Unified => 3,
        }
    }
}
