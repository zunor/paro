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
        } else if hnsw_generation_needs_compaction(context) {
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
        } else if context.artifact_count >= 32 {
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
        } else if context.artifact_count >= 32 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

/// HNSW compaction is a levelled fan-out policy. Absolute artifact size is
/// not garbage: rebuilding one already-coarse graph because it exceeds a byte
/// threshold creates an endless rewrite loop. Compact when accumulated small
/// partitions form a meaningful next level, or when fan-out itself reaches a
/// hard ceiling.
fn hnsw_generation_needs_compaction(context: &ArtifactGcContext) -> bool {
    if context.artifact_count <= 1 {
        return false;
    }
    let rows_outside_largest = context
        .indexed_rows
        .saturating_sub(context.largest_artifact_rows);
    let level_target = context.largest_artifact_rows.div_ceil(4).max(32_768);
    context.artifact_count >= 32
        || (context.artifact_count >= 8 && rows_outside_largest >= level_target)
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
                artifact_count: 32,
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

    #[test]
    fn hnsw_compaction_is_levelled_and_stable_after_coalescing() {
        let policy = gc_policy_for_kind(SearchIndexKind::Hnsw);
        assert_eq!(
            policy.should_gc(&ArtifactGcContext {
                bytes_on_disk: 2 * 1024 * 1024 * 1024,
                artifact_count: 1,
                indexed_rows: 500_000,
                largest_artifact_rows: 500_000,
                ..ArtifactGcContext::default()
            }),
            GcDecision::Skip
        );
        assert_eq!(
            policy.should_gc(&ArtifactGcContext {
                artifact_count: 26,
                indexed_rows: 500_000,
                largest_artifact_rows: 400_000,
                ..ArtifactGcContext::default()
            }),
            GcDecision::CompactOnly
        );
    }
}
