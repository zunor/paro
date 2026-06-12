// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Generation coverage projection for external storage callers.

use std::collections::BTreeSet;

use crate::search::capability::CoverageState;
use crate::search::tail::TailPendingSet;

use super::view::SearchDefinitionState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchGenerationCoverage {
    pub visible_version: i64,
    pub indexed_through_ts: u64,
    pub visible_segment_count: usize,
    pub indexed_segment_count: usize,
    pub coverage: CoverageState,
}

impl SearchGenerationCoverage {
    pub fn is_complete(&self) -> bool {
        self.coverage.is_complete()
    }
}

pub(crate) fn search_generation_coverage_for_state(
    state: &SearchDefinitionState,
) -> Option<SearchGenerationCoverage> {
    let generation = state.generation.as_ref()?;
    let manifest = state.manifest.as_ref();
    let indexed_segment_count = manifest
        .map(|manifest| {
            manifest
                .artifacts
                .artifacts
                .iter()
                .map(|artifact| artifact.segment)
                .collect::<BTreeSet<_>>()
                .len()
        })
        .unwrap_or_default();
    let tail_pending = TailPendingSet {
        entries: manifest
            .map(|manifest| manifest.tail_pending_entries.clone())
            .unwrap_or_default(),
    };
    Some(SearchGenerationCoverage {
        visible_version: generation.build_snapshot_version,
        indexed_through_ts: generation.indexed_through_ts,
        visible_segment_count: indexed_segment_count + tail_pending.coverage_segments(),
        indexed_segment_count,
        coverage: generation.coverage.clone(),
    })
}
