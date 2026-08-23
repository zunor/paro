// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::statistics::{FullTextIndexStatistics, HnswIndexStatistics, SparseIndexStatistics};
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};

pub type TableId = u64;
pub type SearchDefinitionId = u64;
pub type SearchGenerationId = u64;
pub type BuildEpoch = u64;
pub type ProviderVariantId = u32;
pub type ConfigFingerprint = u64;
pub type SegmentId = u32;
pub type SearchSourceId = u32;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "stats", rename_all = "snake_case")]
pub enum SearchProviderStats {
    FullText(FullTextProviderStats),
    Sparse(SparseProviderStats),
    Hnsw(HnswProviderStats),
}

impl SearchProviderStats {
    pub fn as_fulltext(&self) -> Option<&FullTextProviderStats> {
        match self {
            Self::FullText(stats) => Some(stats),
            Self::Sparse(_) | Self::Hnsw(_) => None,
        }
    }

    pub fn as_sparse(&self) -> Option<&SparseProviderStats> {
        match self {
            Self::Sparse(stats) => Some(stats),
            Self::FullText(_) | Self::Hnsw(_) => None,
        }
    }

    pub fn as_hnsw(&self) -> Option<&HnswProviderStats> {
        match self {
            Self::Hnsw(stats) => Some(stats),
            Self::FullText(_) | Self::Sparse(_) => None,
        }
    }

    pub fn merge_assign(&mut self, incoming: &Self) {
        match (self, incoming) {
            (Self::FullText(existing), Self::FullText(incoming)) => existing.merge_assign(incoming),
            (Self::Sparse(existing), Self::Sparse(incoming)) => existing.merge_assign(incoming),
            (Self::Hnsw(existing), Self::Hnsw(incoming)) => existing.merge_assign(incoming),
            _ => {}
        }
    }

    pub fn try_subtract_assign(&mut self, outgoing: &Self) -> Result<StatsSubtractOutcome> {
        match (self, outgoing) {
            (Self::FullText(existing), Self::FullText(outgoing)) => {
                existing.try_subtract_assign(outgoing)
            }
            (Self::Sparse(existing), Self::Sparse(outgoing)) => {
                existing.try_subtract_assign(outgoing)
            }
            (Self::Hnsw(existing), Self::Hnsw(outgoing)) => existing.try_subtract_assign(outgoing),
            _ => Err(paro_error::invalid_input(
                "SearchProviderStats kind mismatch",
            )),
        }
    }

    pub fn rebuild_from_shard_summaries<'a, I>(summaries: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = &'a Self>,
    {
        let mut fulltext = Vec::new();
        let mut sparse = Vec::new();
        let mut hnsw = Vec::new();
        for summary in summaries {
            match summary {
                Self::FullText(stats) => fulltext.push(stats),
                Self::Sparse(stats) => sparse.push(stats),
                Self::Hnsw(stats) => hnsw.push(stats),
            }
        }
        let provider_kinds = [!fulltext.is_empty(), !sparse.is_empty(), !hnsw.is_empty()]
            .into_iter()
            .filter(|present| *present)
            .count();
        if provider_kinds > 1 {
            return Err(paro_error::invalid_input(
                "mixed SearchProviderStats shard summaries",
            ));
        }
        if !sparse.is_empty() {
            return Ok(SparseProviderStats::rebuild_from_shard_summaries(sparse)?
                .map(SearchProviderStats::Sparse));
        }
        if !hnsw.is_empty() {
            return Ok(HnswProviderStats::rebuild_from_shard_summaries(hnsw)?
                .map(SearchProviderStats::Hnsw));
        }
        Ok(
            FullTextProviderStats::rebuild_from_shard_summaries(fulltext)?
                .map(SearchProviderStats::FullText),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsSubtractOutcome {
    Exact,
    NeedsShardSummaryRebuild,
}

impl StatsSubtractOutcome {
    pub const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Exact, Self::Exact) => Self::Exact,
            _ => Self::NeedsShardSummaryRebuild,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullTextProviderStats {
    pub total_docs: u32,
    pub total_terms: u64,
    pub avg_doc_length: f32,
    pub unique_terms: u32,
    pub total_postings: u64,
    pub max_posting_list_len: u32,
    pub min_posting_list_len: u32,
    pub bm25_k1: f32,
    pub bm25_b: f32,
    pub tokenizer: String,
}

impl Default for FullTextProviderStats {
    fn default() -> Self {
        Self {
            total_docs: 0,
            total_terms: 0,
            avg_doc_length: 0.0,
            unique_terms: 0,
            total_postings: 0,
            max_posting_list_len: 0,
            min_posting_list_len: 0,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: TokenizerKind::Default.config_name().to_string(),
        }
    }
}

impl FullTextProviderStats {
    pub fn empty_for_config(config: &str) -> Self {
        let tokenizer = TokenizerKind::from_config(config)
            .map(|kind| kind.config_name().to_string())
            .unwrap_or_else(|_| TokenizerKind::Default.config_name().to_string());
        Self {
            tokenizer,
            ..Self::default()
        }
    }

    pub fn merge_assign(&mut self, incoming: &Self) {
        self.total_docs = self.total_docs.saturating_add(incoming.total_docs);
        self.total_terms = self.total_terms.saturating_add(incoming.total_terms);
        self.unique_terms = self.unique_terms.saturating_add(incoming.unique_terms);
        self.total_postings = self.total_postings.saturating_add(incoming.total_postings);
        self.max_posting_list_len = self.max_posting_list_len.max(incoming.max_posting_list_len);
        if self.min_posting_list_len == 0 {
            self.min_posting_list_len = incoming.min_posting_list_len;
        } else if incoming.min_posting_list_len > 0 {
            self.min_posting_list_len =
                self.min_posting_list_len.min(incoming.min_posting_list_len);
        }
        self.avg_doc_length = if self.total_docs == 0 {
            0.0
        } else {
            self.total_terms as f32 / self.total_docs as f32
        };
    }

    pub fn try_subtract_assign(&mut self, outgoing: &Self) -> Result<StatsSubtractOutcome> {
        self.ensure_compatible(outgoing)?;
        if outgoing.total_docs > self.total_docs
            || outgoing.total_terms > self.total_terms
            || outgoing.unique_terms > self.unique_terms
            || outgoing.total_postings > self.total_postings
        {
            return Err(paro_error::data_corrupted(
                "FullTextProviderStats subtract would underflow",
            ));
        }

        self.total_docs -= outgoing.total_docs;
        self.total_terms -= outgoing.total_terms;
        self.avg_doc_length = if self.total_docs == 0 {
            0.0
        } else {
            self.total_terms as f32 / self.total_docs as f32
        };

        let needs_rebuild = outgoing.unique_terms > 0
            || outgoing.total_postings > 0
            || outgoing.max_posting_list_len > 0
            || outgoing.min_posting_list_len > 0;
        if needs_rebuild {
            self.unique_terms -= outgoing.unique_terms;
            self.total_postings -= outgoing.total_postings;
            self.max_posting_list_len = 0;
            self.min_posting_list_len = 0;
            Ok(StatsSubtractOutcome::NeedsShardSummaryRebuild)
        } else {
            Ok(StatsSubtractOutcome::Exact)
        }
    }

    pub fn rebuild_from_shard_summaries<'a, I>(summaries: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = &'a Self>,
    {
        let mut iter = summaries.into_iter();
        let Some(first) = iter.next() else {
            return Ok(None);
        };
        let mut rebuilt = Self::empty_for_config(&first.tokenizer);
        rebuilt.bm25_k1 = first.bm25_k1;
        rebuilt.bm25_b = first.bm25_b;
        rebuilt.merge_assign(first);
        for summary in iter {
            rebuilt.ensure_compatible(summary)?;
            rebuilt.merge_assign(summary);
        }
        Ok(Some(rebuilt))
    }

    fn ensure_compatible(&self, other: &Self) -> Result<()> {
        if self.tokenizer != other.tokenizer
            || self.bm25_k1.to_bits() != other.bm25_k1.to_bits()
            || self.bm25_b.to_bits() != other.bm25_b.to_bits()
        {
            return Err(paro_error::invalid_input(
                "FullTextProviderStats config mismatch",
            ));
        }
        Ok(())
    }

    pub fn global_stats(&self) -> GlobalFullTextStats {
        GlobalFullTextStats::from_totals_with_bm25(
            self.total_docs,
            self.total_terms,
            self.bm25_k1,
            self.bm25_b,
        )
    }

    pub fn index_statistics(&self) -> FullTextIndexStatistics {
        let tokenizer_kind =
            TokenizerKind::from_config(&self.tokenizer).unwrap_or(TokenizerKind::Default);
        FullTextIndexStatistics {
            total_docs: self.total_docs,
            total_terms: self.total_terms,
            avg_doc_length: self.avg_doc_length,
            unique_terms: self.unique_terms,
            total_postings: self.total_postings,
            max_posting_list_len: self.max_posting_list_len,
            min_posting_list_len: self.min_posting_list_len,
            bm25_k1: self.bm25_k1,
            bm25_b: self.bm25_b,
            tokenizer_kind,
        }
    }
}

impl From<&FullTextIndexStatistics> for FullTextProviderStats {
    fn from(value: &FullTextIndexStatistics) -> Self {
        Self {
            total_docs: value.total_docs,
            total_terms: value.total_terms,
            avg_doc_length: value.avg_doc_length,
            unique_terms: value.unique_terms,
            total_postings: value.total_postings,
            max_posting_list_len: value.max_posting_list_len,
            min_posting_list_len: value.min_posting_list_len,
            bm25_k1: value.bm25_k1,
            bm25_b: value.bm25_b,
            tokenizer: value.tokenizer_kind.config_name().to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SparseProviderStats {
    pub row_count: u64,
    pub nnz: u64,
    pub posting_fanout: u64,
    pub unique_dimensions: u64,
    pub avg_vector_nnz: f32,
    pub l2_norm_sum: f64,
    pub max_l2_norm: f32,
}

impl Default for SparseProviderStats {
    fn default() -> Self {
        Self {
            row_count: 0,
            nnz: 0,
            posting_fanout: 0,
            unique_dimensions: 0,
            avg_vector_nnz: 0.0,
            l2_norm_sum: 0.0,
            max_l2_norm: 0.0,
        }
    }
}

impl SparseProviderStats {
    pub fn merge_assign(&mut self, incoming: &Self) {
        self.row_count = self.row_count.saturating_add(incoming.row_count);
        self.nnz = self.nnz.saturating_add(incoming.nnz);
        self.posting_fanout = self.posting_fanout.saturating_add(incoming.posting_fanout);
        self.unique_dimensions = self
            .unique_dimensions
            .saturating_add(incoming.unique_dimensions);
        self.l2_norm_sum += incoming.l2_norm_sum;
        self.max_l2_norm = self.max_l2_norm.max(incoming.max_l2_norm);
        self.avg_vector_nnz = if self.row_count == 0 {
            0.0
        } else {
            self.nnz as f32 / self.row_count as f32
        };
    }

    pub fn try_subtract_assign(&mut self, outgoing: &Self) -> Result<StatsSubtractOutcome> {
        if outgoing.row_count > self.row_count
            || outgoing.nnz > self.nnz
            || outgoing.posting_fanout > self.posting_fanout
            || outgoing.unique_dimensions > self.unique_dimensions
            || outgoing.l2_norm_sum > self.l2_norm_sum + f64::EPSILON
        {
            return Err(paro_error::data_corrupted(
                "SparseProviderStats subtract would underflow",
            ));
        }

        self.row_count -= outgoing.row_count;
        self.nnz -= outgoing.nnz;
        self.posting_fanout -= outgoing.posting_fanout;
        self.l2_norm_sum -= outgoing.l2_norm_sum;
        self.avg_vector_nnz = if self.row_count == 0 {
            0.0
        } else {
            self.nnz as f32 / self.row_count as f32
        };

        let needs_rebuild = outgoing.unique_dimensions > 0 || outgoing.max_l2_norm > 0.0;
        if needs_rebuild {
            self.unique_dimensions -= outgoing.unique_dimensions;
            self.max_l2_norm = 0.0;
            Ok(StatsSubtractOutcome::NeedsShardSummaryRebuild)
        } else {
            Ok(StatsSubtractOutcome::Exact)
        }
    }

    pub fn rebuild_from_shard_summaries<'a, I>(summaries: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = &'a Self>,
    {
        let mut rebuilt = Self::default();
        let mut saw_any = false;
        for summary in summaries {
            saw_any = true;
            rebuilt.merge_assign(summary);
        }
        Ok(saw_any.then_some(rebuilt))
    }
}

impl From<&SparseIndexStatistics> for SparseProviderStats {
    fn from(value: &SparseIndexStatistics) -> Self {
        Self {
            row_count: value.num_indexed_vectors as u64,
            nnz: value.total_postings as u64,
            posting_fanout: value.total_postings as u64,
            unique_dimensions: value.num_unique_dimensions as u64,
            avg_vector_nnz: value.avg_vector_nnz,
            l2_norm_sum: value.l2_norm_sum,
            max_l2_norm: value.max_l2_norm,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswProviderStats {
    pub vector_count: u64,
    pub dimension: u32,
    pub max_level: u32,
    pub m: u32,
    pub ef_construction: u32,
    pub graph_memory_bytes: u64,
    pub vector_storage_bytes: u64,
    pub total_graph_links: u64,
    pub level0_graph_links: u64,
    pub avg_level0_degree: f32,
    pub max_level0_degree: u32,
}

impl Default for HnswProviderStats {
    fn default() -> Self {
        Self {
            vector_count: 0,
            dimension: 0,
            max_level: 0,
            m: 0,
            ef_construction: 0,
            graph_memory_bytes: 0,
            vector_storage_bytes: 0,
            total_graph_links: 0,
            level0_graph_links: 0,
            avg_level0_degree: 0.0,
            max_level0_degree: 0,
        }
    }
}

impl HnswProviderStats {
    pub fn merge_assign(&mut self, incoming: &Self) {
        if self.dimension == 0 {
            self.dimension = incoming.dimension;
        }
        if self.m == 0 {
            self.m = incoming.m;
        }
        if self.ef_construction == 0 {
            self.ef_construction = incoming.ef_construction;
        }
        self.vector_count = self.vector_count.saturating_add(incoming.vector_count);
        self.max_level = self.max_level.max(incoming.max_level);
        self.graph_memory_bytes = self
            .graph_memory_bytes
            .saturating_add(incoming.graph_memory_bytes);
        self.vector_storage_bytes = self
            .vector_storage_bytes
            .saturating_add(incoming.vector_storage_bytes);
        self.total_graph_links = self
            .total_graph_links
            .saturating_add(incoming.total_graph_links);
        self.level0_graph_links = self
            .level0_graph_links
            .saturating_add(incoming.level0_graph_links);
        self.max_level0_degree = self.max_level0_degree.max(incoming.max_level0_degree);
        self.avg_level0_degree = if self.vector_count == 0 {
            0.0
        } else {
            self.level0_graph_links as f32 / self.vector_count as f32
        };
    }

    pub fn try_subtract_assign(&mut self, outgoing: &Self) -> Result<StatsSubtractOutcome> {
        self.ensure_compatible(outgoing)?;
        if outgoing.vector_count > self.vector_count
            || outgoing.graph_memory_bytes > self.graph_memory_bytes
            || outgoing.vector_storage_bytes > self.vector_storage_bytes
            || outgoing.total_graph_links > self.total_graph_links
            || outgoing.level0_graph_links > self.level0_graph_links
        {
            return Err(paro_error::data_corrupted(
                "HnswProviderStats subtract would underflow",
            ));
        }

        self.vector_count -= outgoing.vector_count;
        self.graph_memory_bytes -= outgoing.graph_memory_bytes;
        self.vector_storage_bytes -= outgoing.vector_storage_bytes;
        self.total_graph_links -= outgoing.total_graph_links;
        self.level0_graph_links -= outgoing.level0_graph_links;
        self.avg_level0_degree = if self.vector_count == 0 {
            0.0
        } else {
            self.level0_graph_links as f32 / self.vector_count as f32
        };

        let needs_rebuild = outgoing.max_level > 0 || outgoing.max_level0_degree > 0;
        if needs_rebuild {
            self.max_level = 0;
            self.max_level0_degree = 0;
            Ok(StatsSubtractOutcome::NeedsShardSummaryRebuild)
        } else {
            Ok(StatsSubtractOutcome::Exact)
        }
    }

    pub fn rebuild_from_shard_summaries<'a, I>(summaries: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = &'a Self>,
    {
        let mut iter = summaries.into_iter();
        let Some(first) = iter.next() else {
            return Ok(None);
        };
        let mut rebuilt = Self::default();
        rebuilt.merge_assign(first);
        for summary in iter {
            rebuilt.ensure_compatible(summary)?;
            rebuilt.merge_assign(summary);
        }
        Ok(Some(rebuilt))
    }

    fn ensure_compatible(&self, other: &Self) -> Result<()> {
        if (self.dimension != 0 && other.dimension != 0 && self.dimension != other.dimension)
            || (self.m != 0 && other.m != 0 && self.m != other.m)
            || (self.ef_construction != 0
                && other.ef_construction != 0
                && self.ef_construction != other.ef_construction)
        {
            return Err(paro_error::invalid_input(
                "HnswProviderStats config mismatch",
            ));
        }
        Ok(())
    }

    pub const fn estimated_total_memory_bytes(&self) -> u64 {
        self.graph_memory_bytes
            .saturating_add(self.vector_storage_bytes)
    }
}

impl From<&HnswIndexStatistics> for HnswProviderStats {
    fn from(value: &HnswIndexStatistics) -> Self {
        Self {
            vector_count: value.num_indexed_vectors as u64,
            dimension: value.dimension as u32,
            max_level: value.max_level as u32,
            m: value.m as u32,
            ef_construction: value.ef_construction as u32,
            graph_memory_bytes: value.graph_size_bytes,
            vector_storage_bytes: value.storage_size_bytes,
            total_graph_links: value.total_graph_links,
            level0_graph_links: value.level0_graph_links,
            avg_level0_degree: value.avg_level0_degree,
            max_level0_degree: value.max_level0_degree,
        }
    }
}

/// Per-artifact local stats carried by a manifest entry.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SearchArtifactStats {
    pub row_count: u64,
    pub bytes_on_disk: u64,
    pub provider_stats: Option<SearchProviderStats>,
}

/// Generation-level aggregated stats consumed by capability/costing.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct GenerationStats {
    pub indexed_rows: u64,
    pub artifact_count: usize,
    pub provider_stats: Option<SearchProviderStats>,
}

impl GenerationStats {
    pub fn merge_assign(&mut self, incoming: &Self) {
        self.indexed_rows = self.indexed_rows.saturating_add(incoming.indexed_rows);
        self.artifact_count = self.artifact_count.saturating_add(incoming.artifact_count);
        match (&mut self.provider_stats, &incoming.provider_stats) {
            (Some(existing), Some(incoming)) => existing.merge_assign(incoming),
            (None, Some(incoming)) => self.provider_stats = Some(incoming.clone()),
            _ => {}
        }
    }

    pub fn try_subtract_assign(&mut self, outgoing: &Self) -> Result<StatsSubtractOutcome> {
        if outgoing.indexed_rows > self.indexed_rows
            || outgoing.artifact_count > self.artifact_count
        {
            return Err(paro_error::data_corrupted(
                "GenerationStats subtract would underflow",
            ));
        }

        self.indexed_rows -= outgoing.indexed_rows;
        self.artifact_count -= outgoing.artifact_count;

        match (&mut self.provider_stats, &outgoing.provider_stats) {
            (Some(existing), Some(outgoing)) => existing.try_subtract_assign(outgoing),
            (_, None) => Ok(StatsSubtractOutcome::Exact),
            (None, Some(_)) => Err(paro_error::data_corrupted(
                "GenerationStats missing provider stats for subtract",
            )),
        }
    }

    pub fn fulltext_provider_stats(&self) -> Option<&FullTextProviderStats> {
        self.provider_stats.as_ref()?.as_fulltext()
    }

    pub fn sparse_provider_stats(&self) -> Option<&SparseProviderStats> {
        self.provider_stats.as_ref()?.as_sparse()
    }

    pub fn hnsw_provider_stats(&self) -> Option<&HnswProviderStats> {
        self.provider_stats.as_ref()?.as_hnsw()
    }

    pub fn fulltext_global_stats(&self) -> Option<GlobalFullTextStats> {
        Some(self.fulltext_provider_stats()?.global_stats())
    }

    pub fn fulltext_index_statistics(&self) -> Option<FullTextIndexStatistics> {
        Some(self.fulltext_provider_stats()?.index_statistics())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum CatchUpBacklogTier {
    #[default]
    Healthy,
    Elevated,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum MaintenancePriority {
    #[default]
    Idle,
    Opportunistic,
    Elevated,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BuildWatermarks {
    pub snapshot_version: i64,
    pub replay_watermark: i64,
    pub cutover_watermark: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GenerationRecoveryState {
    pub catch_up_build_epoch: Option<BuildEpoch>,
    pub superseded_build_epochs: Vec<BuildEpoch>,
    pub tail_pending_rowsets: usize,
    pub tail_pending_rows: u64,
    pub backlog_tier: CatchUpBacklogTier,
    pub priority: MaintenancePriority,
    pub rowset_rate_limit: usize,
    pub row_rate_limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GenerationMaintenanceState {
    pub build_watermarks: BuildWatermarks,
    pub recovery: GenerationRecoveryState,
    pub tombstone_rows: u64,
    pub tombstone_ratio_millis: u32,
}

/// Coarse planning-time cost summary.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct SearchCostEstimate {
    pub score: f64,
    pub estimated_rows: Option<u64>,
    pub estimated_total_rows: Option<u64>,
}

impl SearchCostEstimate {
    pub const fn new(score: f64) -> Self {
        Self {
            score,
            estimated_rows: None,
            estimated_total_rows: None,
        }
    }

    pub const fn with_rows(mut self, estimated_rows: u64) -> Self {
        self.estimated_rows = Some(estimated_rows);
        self
    }

    pub const fn with_total_rows(mut self, estimated_total_rows: u64) -> Self {
        self.estimated_total_rows = Some(estimated_total_rows);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PreferHint {
    Latency,
    Recall,
    WarmCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SearchExecutionMode {
    Exact,
    ExactTailMerge,
    ApproxTopK,
    ExactFallback,
}

/// Supported execution modes for a queryable generation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExecutionModes {
    modes: BTreeSet<SearchExecutionMode>,
}

impl ExecutionModes {
    pub fn new<I>(modes: I) -> Self
    where
        I: IntoIterator<Item = SearchExecutionMode>,
    {
        Self {
            modes: modes.into_iter().collect(),
        }
    }

    pub fn exact_only() -> Self {
        Self::new([SearchExecutionMode::Exact])
    }

    pub fn insert(&mut self, mode: SearchExecutionMode) {
        self.modes.insert(mode);
    }

    pub fn contains(&self, mode: SearchExecutionMode) -> bool {
        self.modes.contains(&mode)
    }

    pub fn iter(&self) -> impl Iterator<Item = &SearchExecutionMode> {
        self.modes.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BuildWatermarks, ExecutionModes, FullTextProviderStats, GenerationMaintenanceState,
        GenerationRecoveryState, GenerationStats, HnswProviderStats, MaintenancePriority,
        SearchExecutionMode, SearchProviderStats, SparseProviderStats, StatsSubtractOutcome,
    };
    use crate::index::fulltext::tokenizer::TokenizerKind;

    #[test]
    fn execution_modes_track_support_without_duplicates() {
        let mut modes = ExecutionModes::exact_only();
        modes.insert(SearchExecutionMode::ExactTailMerge);
        modes.insert(SearchExecutionMode::ExactTailMerge);

        let collected = modes.iter().copied().collect::<Vec<_>>();
        assert_eq!(
            collected,
            vec![
                SearchExecutionMode::Exact,
                SearchExecutionMode::ExactTailMerge
            ]
        );
        assert!(modes.contains(SearchExecutionMode::ExactTailMerge));
        assert!(!modes.contains(SearchExecutionMode::ApproxTopK));
    }

    #[test]
    fn fulltext_provider_stats_round_trip_to_search_cost_views() {
        let stats = FullTextProviderStats {
            total_docs: 7,
            total_terms: 21,
            avg_doc_length: 3.0,
            unique_terms: 5,
            total_postings: 12,
            max_posting_list_len: 4,
            min_posting_list_len: 1,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: TokenizerKind::Chinese.config_name().to_string(),
        };

        let global = stats.global_stats();
        assert_eq!(global.total_docs, 7);
        assert_eq!(global.total_terms, 21);

        let index_stats = stats.index_statistics();
        assert_eq!(index_stats.unique_terms, 5);
        assert_eq!(index_stats.tokenizer_kind, TokenizerKind::Chinese);
    }

    #[test]
    fn generation_stats_merge_provider_specific_fulltext_totals() {
        let mut stats = GenerationStats {
            indexed_rows: 3,
            artifact_count: 1,
            provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                total_docs: 3,
                total_terms: 9,
                avg_doc_length: 3.0,
                unique_terms: 4,
                total_postings: 5,
                max_posting_list_len: 3,
                min_posting_list_len: 1,
                bm25_k1: 1.2,
                bm25_b: 0.75,
                tokenizer: "simple".to_string(),
            })),
        };

        stats.merge_assign(&GenerationStats {
            indexed_rows: 2,
            artifact_count: 1,
            provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                total_docs: 2,
                total_terms: 4,
                avg_doc_length: 2.0,
                unique_terms: 3,
                total_postings: 6,
                max_posting_list_len: 4,
                min_posting_list_len: 2,
                bm25_k1: 1.2,
                bm25_b: 0.75,
                tokenizer: "simple".to_string(),
            })),
        });

        let fulltext = stats
            .fulltext_provider_stats()
            .expect("fulltext provider stats");
        assert_eq!(stats.indexed_rows, 5);
        assert_eq!(stats.artifact_count, 2);
        assert_eq!(fulltext.total_docs, 5);
        assert_eq!(fulltext.total_terms, 13);
        assert_eq!(fulltext.unique_terms, 7);
        assert_eq!(fulltext.total_postings, 11);
        assert_eq!(fulltext.max_posting_list_len, 4);
        assert_eq!(fulltext.min_posting_list_len, 1);
    }

    #[test]
    fn fulltext_provider_stats_try_subtract_marks_irreversible_summary_rebuild() {
        let mut stats = FullTextProviderStats {
            total_docs: 10,
            total_terms: 30,
            avg_doc_length: 3.0,
            unique_terms: 7,
            total_postings: 20,
            max_posting_list_len: 6,
            min_posting_list_len: 1,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: "simple".to_string(),
        };
        let outgoing = FullTextProviderStats {
            total_docs: 4,
            total_terms: 8,
            avg_doc_length: 2.0,
            unique_terms: 3,
            total_postings: 9,
            max_posting_list_len: 5,
            min_posting_list_len: 1,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: "simple".to_string(),
        };

        let outcome = stats.try_subtract_assign(&outgoing).unwrap();
        assert_eq!(outcome, StatsSubtractOutcome::NeedsShardSummaryRebuild);
        assert_eq!(stats.total_docs, 6);
        assert_eq!(stats.total_terms, 22);
        assert!((stats.avg_doc_length - (22.0 / 6.0)).abs() < 1e-6);
        assert_eq!(stats.unique_terms, 4);
        assert_eq!(stats.total_postings, 11);
        assert_eq!(stats.max_posting_list_len, 0);
        assert_eq!(stats.min_posting_list_len, 0);
    }

    #[test]
    fn fulltext_provider_stats_rebuilds_from_shard_summaries() {
        let left = FullTextProviderStats {
            total_docs: 2,
            total_terms: 4,
            avg_doc_length: 2.0,
            unique_terms: 3,
            total_postings: 4,
            max_posting_list_len: 2,
            min_posting_list_len: 1,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: "simple".to_string(),
        };
        let right = FullTextProviderStats {
            total_docs: 3,
            total_terms: 9,
            avg_doc_length: 3.0,
            unique_terms: 5,
            total_postings: 7,
            max_posting_list_len: 4,
            min_posting_list_len: 2,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: "simple".to_string(),
        };

        let rebuilt = FullTextProviderStats::rebuild_from_shard_summaries([&left, &right])
            .unwrap()
            .expect("rebuilt stats");
        assert_eq!(rebuilt.total_docs, 5);
        assert_eq!(rebuilt.total_terms, 13);
        assert!((rebuilt.avg_doc_length - (13.0 / 5.0)).abs() < 1e-6);
        assert_eq!(rebuilt.unique_terms, 8);
        assert_eq!(rebuilt.total_postings, 11);
        assert_eq!(rebuilt.max_posting_list_len, 4);
        assert_eq!(rebuilt.min_posting_list_len, 1);
    }

    #[test]
    fn generation_stats_try_subtract_delegates_provider_outcome() {
        let mut stats = GenerationStats {
            indexed_rows: 10,
            artifact_count: 3,
            provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                total_docs: 10,
                total_terms: 30,
                avg_doc_length: 3.0,
                unique_terms: 7,
                total_postings: 20,
                max_posting_list_len: 6,
                min_posting_list_len: 1,
                bm25_k1: 1.2,
                bm25_b: 0.75,
                tokenizer: "simple".to_string(),
            })),
        };
        let outgoing = GenerationStats {
            indexed_rows: 4,
            artifact_count: 1,
            provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                total_docs: 4,
                total_terms: 8,
                avg_doc_length: 2.0,
                unique_terms: 3,
                total_postings: 9,
                max_posting_list_len: 5,
                min_posting_list_len: 1,
                bm25_k1: 1.2,
                bm25_b: 0.75,
                tokenizer: "simple".to_string(),
            })),
        };

        let outcome = stats.try_subtract_assign(&outgoing).unwrap();
        assert_eq!(outcome, StatsSubtractOutcome::NeedsShardSummaryRebuild);
        assert_eq!(stats.indexed_rows, 6);
        assert_eq!(stats.artifact_count, 2);
        assert_eq!(stats.fulltext_provider_stats().unwrap().total_docs, 6);
    }

    #[test]
    fn sparse_provider_stats_merge_subtract_and_rebuild_summary() {
        let mut stats = SparseProviderStats {
            row_count: 10,
            nnz: 32,
            posting_fanout: 32,
            unique_dimensions: 8,
            avg_vector_nnz: 3.2,
            l2_norm_sum: 18.0,
            max_l2_norm: 4.0,
        };
        let outgoing = SparseProviderStats {
            row_count: 4,
            nnz: 12,
            posting_fanout: 12,
            unique_dimensions: 3,
            avg_vector_nnz: 3.0,
            l2_norm_sum: 7.0,
            max_l2_norm: 3.0,
        };

        let outcome = stats.try_subtract_assign(&outgoing).unwrap();
        assert_eq!(outcome, StatsSubtractOutcome::NeedsShardSummaryRebuild);
        assert_eq!(stats.row_count, 6);
        assert_eq!(stats.nnz, 20);
        assert!((stats.avg_vector_nnz - (20.0 / 6.0)).abs() < 1e-6);
        assert_eq!(stats.unique_dimensions, 5);
        assert_eq!(stats.max_l2_norm, 0.0);

        let shard_a = SparseProviderStats {
            row_count: 6,
            nnz: 20,
            posting_fanout: 20,
            unique_dimensions: 5,
            avg_vector_nnz: 20.0 / 6.0,
            l2_norm_sum: 11.0,
            max_l2_norm: 4.0,
        };
        let rebuilt = SparseProviderStats::rebuild_from_shard_summaries([&shard_a, &outgoing])
            .unwrap()
            .expect("rebuilt sparse stats");
        assert_eq!(rebuilt.row_count, 10);
        assert_eq!(rebuilt.nnz, 32);
        assert_eq!(rebuilt.posting_fanout, 32);
        assert_eq!(rebuilt.unique_dimensions, 8);
        assert_eq!(rebuilt.max_l2_norm, 4.0);
    }

    #[test]
    fn generation_stats_exposes_sparse_provider_stats() {
        let stats = GenerationStats {
            indexed_rows: 3,
            artifact_count: 1,
            provider_stats: Some(SearchProviderStats::Sparse(SparseProviderStats {
                row_count: 3,
                nnz: 7,
                posting_fanout: 7,
                unique_dimensions: 4,
                avg_vector_nnz: 7.0 / 3.0,
                l2_norm_sum: 5.0,
                max_l2_norm: 2.5,
            })),
        };

        let sparse = stats
            .sparse_provider_stats()
            .expect("sparse provider stats");
        assert_eq!(sparse.row_count, 3);
        assert_eq!(sparse.nnz, 7);
        assert!(stats.fulltext_provider_stats().is_none());
    }

    #[test]
    fn hnsw_provider_stats_merge_subtract_and_rebuild_summary() {
        let mut stats = HnswProviderStats {
            vector_count: 10,
            dimension: 128,
            max_level: 3,
            m: 16,
            ef_construction: 100,
            graph_memory_bytes: 4096,
            vector_storage_bytes: 5120,
            total_graph_links: 180,
            level0_graph_links: 120,
            avg_level0_degree: 12.0,
            max_level0_degree: 32,
        };
        let outgoing = HnswProviderStats {
            vector_count: 4,
            dimension: 128,
            max_level: 2,
            m: 16,
            ef_construction: 100,
            graph_memory_bytes: 1024,
            vector_storage_bytes: 2048,
            total_graph_links: 70,
            level0_graph_links: 44,
            avg_level0_degree: 11.0,
            max_level0_degree: 24,
        };

        let outcome = stats.try_subtract_assign(&outgoing).unwrap();
        assert_eq!(outcome, StatsSubtractOutcome::NeedsShardSummaryRebuild);
        assert_eq!(stats.vector_count, 6);
        assert_eq!(stats.graph_memory_bytes, 3072);
        assert_eq!(stats.vector_storage_bytes, 3072);
        assert_eq!(stats.total_graph_links, 110);
        assert_eq!(stats.level0_graph_links, 76);
        assert!((stats.avg_level0_degree - (76.0 / 6.0)).abs() < 1e-6);
        assert_eq!(stats.max_level, 0);
        assert_eq!(stats.max_level0_degree, 0);

        let shard_a = HnswProviderStats {
            max_level: 3,
            max_level0_degree: 32,
            ..stats
        };
        let rebuilt = HnswProviderStats::rebuild_from_shard_summaries([&shard_a, &outgoing])
            .unwrap()
            .expect("rebuilt hnsw stats");
        assert_eq!(rebuilt.vector_count, 10);
        assert_eq!(rebuilt.dimension, 128);
        assert_eq!(rebuilt.max_level, 3);
        assert_eq!(rebuilt.max_level0_degree, 32);
        assert_eq!(rebuilt.total_graph_links, 180);
        assert_eq!(rebuilt.level0_graph_links, 120);
        assert!((rebuilt.avg_level0_degree - 12.0).abs() < 1e-6);
        assert_eq!(rebuilt.estimated_total_memory_bytes(), 9216);
    }

    #[test]
    fn generation_stats_exposes_hnsw_provider_stats() {
        let stats = GenerationStats {
            indexed_rows: 5,
            artifact_count: 1,
            provider_stats: Some(SearchProviderStats::Hnsw(HnswProviderStats {
                vector_count: 5,
                dimension: 64,
                max_level: 2,
                m: 16,
                ef_construction: 100,
                graph_memory_bytes: 2048,
                vector_storage_bytes: 1280,
                total_graph_links: 80,
                level0_graph_links: 56,
                avg_level0_degree: 11.2,
                max_level0_degree: 24,
            })),
        };

        let hnsw = stats.hnsw_provider_stats().expect("hnsw provider stats");
        assert_eq!(hnsw.vector_count, 5);
        assert_eq!(hnsw.dimension, 64);
        assert_eq!(hnsw.estimated_total_memory_bytes(), 3328);
        assert!(stats.fulltext_provider_stats().is_none());
        assert!(stats.sparse_provider_stats().is_none());
    }

    #[test]
    fn maintenance_state_defaults_to_idle_watermarks() {
        let state = GenerationMaintenanceState::default();
        assert_eq!(
            state,
            GenerationMaintenanceState {
                build_watermarks: BuildWatermarks::default(),
                recovery: GenerationRecoveryState {
                    priority: MaintenancePriority::Idle,
                    ..GenerationRecoveryState::default()
                },
                tombstone_rows: 0,
                tombstone_ratio_millis: 0,
            }
        );
    }
}
