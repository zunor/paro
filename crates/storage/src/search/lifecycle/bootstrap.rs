// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search generation bootstrap reporting.

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchBootstrapReport {
    pub definitions_considered: usize,
    pub definitions_updated: usize,
    pub rowsets_materialized: usize,
}
