// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statement-local bounds for connected-region enumeration.

#[derive(Debug, Clone, Copy)]
pub(crate) struct RegionLimits {
    pub exact_relations: u16,
    pub connected_pairs: u32,
    pub candidates_per_subset: u16,
}

impl Default for RegionLimits {
    fn default() -> Self {
        Self {
            exact_relations: 12,
            connected_pairs: 65_536,
            candidates_per_subset: 16,
        }
    }
}
