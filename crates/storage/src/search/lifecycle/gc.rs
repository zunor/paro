// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::search::artifact::{ArtifactGcContext, ArtifactGcPolicy, GcDecision};
use crate::search::capability::SearchIndexKind;

struct HnswGcPolicy;
struct SparseGcPolicy;
struct FullTextGcPolicy;

impl ArtifactGcPolicy for HnswGcPolicy {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
        let tombstone_ratio = context.tombstone_ratio.unwrap_or_default();
        if tombstone_ratio >= 0.15 {
            GcDecision::Rebuild
        } else if tombstone_ratio >= 0.05 && context.query_pressure.unwrap_or_default() >= 0.7 {
            GcDecision::Heal
        } else if context.bytes_on_disk >= 256 * 1024 * 1024 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

impl ArtifactGcPolicy for SparseGcPolicy {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
        let tombstone_ratio = context.tombstone_ratio.unwrap_or_default();
        if tombstone_ratio >= 0.35 {
            GcDecision::Rebuild
        } else if context.bytes_on_disk >= 128 * 1024 * 1024 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

impl ArtifactGcPolicy for FullTextGcPolicy {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
        let tombstone_ratio = context.tombstone_ratio.unwrap_or_default();
        if tombstone_ratio >= 0.25 {
            GcDecision::Rebuild
        } else if tombstone_ratio >= 0.1 && context.query_pressure.unwrap_or_default() >= 0.6 {
            GcDecision::Heal
        } else if context.bytes_on_disk >= 128 * 1024 * 1024 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

pub(crate) fn gc_policy_for_kind(kind: SearchIndexKind) -> &'static dyn ArtifactGcPolicy {
    static HNSW: HnswGcPolicy = HnswGcPolicy;
    static SPARSE: SparseGcPolicy = SparseGcPolicy;
    static FULLTEXT: FullTextGcPolicy = FullTextGcPolicy;
    match kind {
        SearchIndexKind::Hnsw => &HNSW,
        SearchIndexKind::Sparse => &SPARSE,
        SearchIndexKind::FullText => &FULLTEXT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_gc_policies_keep_distinct_thresholds() {
        assert_eq!(
            gc_policy_for_kind(SearchIndexKind::Hnsw).should_gc(&ArtifactGcContext {
                tombstone_ratio: Some(0.16),
                ..ArtifactGcContext::default()
            }),
            GcDecision::Rebuild
        );
        assert_eq!(
            gc_policy_for_kind(SearchIndexKind::Sparse).should_gc(&ArtifactGcContext {
                bytes_on_disk: 129 * 1024 * 1024,
                ..ArtifactGcContext::default()
            }),
            GcDecision::CompactOnly
        );
        assert_eq!(
            gc_policy_for_kind(SearchIndexKind::FullText).should_gc(&ArtifactGcContext {
                tombstone_ratio: Some(0.11),
                query_pressure: Some(0.7),
                ..ArtifactGcContext::default()
            }),
            GcDecision::Heal
        );
    }
}
