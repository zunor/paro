// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::search::artifact::{
    ArtifactCompactionLayout, ArtifactGcContext, ArtifactGcPolicy, GcDecision,
};
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
    let Some(ArtifactCompactionLayout::HnswLevelled {
        artifact_row_counts,
        target_rows,
        fanout,
    }) = &context.compaction_layout
    else {
        return false;
    };
    hnsw_compaction_level(artifact_row_counts, *target_rows, *fanout).is_some()
}

/// Return the lowest size tier containing one complete deterministic merge
/// quantum. A tier covers `[target * fanout^level, target * fanout^(level+1))`;
/// every merge therefore promotes one artifact and keeps query fan-out
/// logarithmically bounded without rewriting an unrelated large base graph.
pub(crate) fn hnsw_compaction_level(
    artifact_rows: &[u64],
    target_rows: u64,
    fanout: u32,
) -> Option<u32> {
    if target_rows == 0 || fanout < 2 {
        return None;
    }
    let mut counts = std::collections::BTreeMap::<u32, usize>::new();
    for &rows in artifact_rows {
        let level = hnsw_artifact_compaction_level(rows, target_rows, fanout);
        let count = counts.entry(level).or_default();
        *count = count.saturating_add(1);
    }
    let merge_width = usize::try_from(fanout).unwrap_or(usize::MAX);
    counts
        .into_iter()
        .find_map(|(level, count)| (count >= merge_width).then_some(level))
}

pub(crate) fn hnsw_artifact_compaction_level(rows: u64, target_rows: u64, fanout: u32) -> u32 {
    let mut units = rows.div_ceil(target_rows.max(1)).max(1);
    let fanout = u64::from(fanout.max(2));
    let mut level = 0u32;
    while units >= fanout {
        units = units.div_ceil(fanout);
        level = level.saturating_add(1);
    }
    level
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
                artifact_count: 5,
                indexed_rows: 500_000,
                largest_artifact_rows: 400_000,
                compaction_layout: Some(ArtifactCompactionLayout::HnswLevelled {
                    artifact_row_counts: vec![400_000, 25_000, 25_000, 25_000, 25_000],
                    target_rows: 50_000,
                    fanout: 4,
                }),
                ..ArtifactGcContext::default()
            }),
            GcDecision::CompactOnly
        );
        assert_eq!(
            policy.should_gc(&ArtifactGcContext {
                artifact_count: 2,
                indexed_rows: 500_000,
                largest_artifact_rows: 400_000,
                compaction_layout: Some(ArtifactCompactionLayout::HnswLevelled {
                    artifact_row_counts: vec![400_000, 100_000],
                    target_rows: 50_000,
                    fanout: 4,
                }),
                ..ArtifactGcContext::default()
            }),
            GcDecision::Skip
        );
    }

    #[test]
    fn hnsw_compaction_selects_the_lowest_eligible_tier_independent_of_input_order() {
        let rows = [
            400_000, 400_000, 400_000, 400_000, 25_000, 25_000, 25_000, 25_000,
        ];
        assert_eq!(hnsw_compaction_level(&rows, 50_000, 4), Some(0));
    }
}
