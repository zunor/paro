// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use super::super::capability::{SearchArtifactRef, SearchIndexDefinition, SearchIndexKind};
use super::super::inline_sink::{
    FullTextStatsDelta, HnswStatsDelta, SearchStatsDelta, SparseStatsDelta,
};
use super::super::stats::{
    FullTextProviderStats, GenerationStats, HnswProviderStats, SearchProviderStats,
    SparseProviderStats, StatsSubtractOutcome,
};

pub(crate) fn generation_stats_from_artifacts(
    definition: &SearchIndexDefinition,
    artifacts: &[SearchArtifactRef],
) -> paro_common::error::Result<GenerationStats> {
    let mut stats = empty_generation_stats_for_definition(definition)?;
    let mut indexed_segments = BTreeSet::new();
    for artifact in artifacts {
        if indexed_segments.insert((artifact.segment.rowset_id, artifact.segment.segment_id)) {
            stats.indexed_rows = stats.indexed_rows.saturating_add(artifact.stats.row_count);
        }
        stats.artifact_count = stats.artifact_count.saturating_add(1);
        if let Some(provider_stats) = artifact.stats.provider_stats.as_ref().cloned() {
            merge_provider_stats_into_generation(&mut stats, std::iter::once(provider_stats));
        }
    }
    Ok(stats)
}

pub(crate) fn generation_stats_after_artifact_replacement(
    definition: &SearchIndexDefinition,
    current: &GenerationStats,
    removed_artifacts: &[SearchArtifactRef],
    added_artifacts: &[SearchArtifactRef],
    materialized_artifacts: &[SearchArtifactRef],
) -> paro_common::error::Result<GenerationStats> {
    let mut next = current.clone();
    let removed_stats = generation_stats_from_artifacts(definition, removed_artifacts)?;
    let subtract_outcome = next.try_subtract_assign(&removed_stats)?;
    let added_stats = generation_stats_from_artifacts(definition, added_artifacts)?;
    next.merge_assign(&added_stats);
    if matches!(
        subtract_outcome,
        StatsSubtractOutcome::NeedsShardSummaryRebuild
    ) {
        return generation_stats_from_artifacts(definition, materialized_artifacts);
    }
    Ok(next)
}

pub(crate) fn empty_generation_stats_for_definition(
    definition: &SearchIndexDefinition,
) -> paro_common::error::Result<GenerationStats> {
    let provider_stats = match definition.kind {
        SearchIndexKind::FullText => {
            let config = definition.fulltext_provider_config()?;
            Some(SearchProviderStats::FullText(
                FullTextProviderStats::empty_for_config(&config.config),
            ))
        }
        SearchIndexKind::Sparse => {
            Some(SearchProviderStats::Sparse(SparseProviderStats::default()))
        }
        SearchIndexKind::Hnsw => Some(SearchProviderStats::Hnsw(HnswProviderStats::default())),
    };
    Ok(GenerationStats {
        indexed_rows: 0,
        artifact_count: 0,
        provider_stats,
    })
}

pub(crate) fn merge_provider_stats_into_generation<I>(
    generation_stats: &mut GenerationStats,
    provider_stats: I,
) where
    I: IntoIterator<Item = SearchProviderStats>,
{
    for provider_stats in provider_stats {
        generation_stats.merge_assign(&GenerationStats {
            indexed_rows: 0,
            artifact_count: 0,
            provider_stats: Some(provider_stats),
        });
    }
}

pub(crate) fn stats_deltas_from_generation_stats(
    generation_stats: &GenerationStats,
) -> Vec<SearchStatsDelta> {
    if generation_stats.artifact_count == 0 {
        return Vec::new();
    }
    match &generation_stats.provider_stats {
        Some(SearchProviderStats::FullText(stats)) => {
            vec![SearchStatsDelta::FullText(FullTextStatsDelta {
                stats: stats.clone(),
            })]
        }
        Some(SearchProviderStats::Sparse(stats)) => {
            vec![SearchStatsDelta::Sparse(SparseStatsDelta {
                row_count: stats.row_count,
                nnz: stats.nnz,
                posting_fanout: stats.posting_fanout,
                unique_dimensions: stats.unique_dimensions,
                l2_norm_sum: stats.l2_norm_sum,
                max_l2_norm: stats.max_l2_norm,
            })]
        }
        Some(SearchProviderStats::Hnsw(stats)) => {
            vec![SearchStatsDelta::Hnsw(HnswStatsDelta {
                vector_count: stats.vector_count,
                dimension: stats.dimension,
                max_level: stats.max_level,
                m: stats.m,
                ef_construction: stats.ef_construction,
                graph_memory_bytes: stats.graph_memory_bytes,
                vector_storage_bytes: stats.vector_storage_bytes,
                total_graph_links: stats.total_graph_links,
                level0_graph_links: stats.level0_graph_links,
                avg_level0_degree: stats.avg_level0_degree,
                max_level0_degree: stats.max_level0_degree,
            })]
        }
        None => Vec::new(),
    }
}
