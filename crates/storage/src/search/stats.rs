// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use crate::index::fulltext::text_index::GlobalFullTextStats;
use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::statistics::FullTextIndexStatistics;
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
}

impl SearchProviderStats {
    pub fn as_fulltext(&self) -> Option<&FullTextProviderStats> {
        match self {
            Self::FullText(stats) => Some(stats),
        }
    }

    pub fn merge_assign(&mut self, incoming: &Self) {
        match (self, incoming) {
            (Self::FullText(existing), Self::FullText(incoming)) => existing.merge_assign(incoming),
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

    pub fn global_stats(&self) -> GlobalFullTextStats {
        GlobalFullTextStats::from_totals(self.total_docs, self.total_terms)
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

    pub fn fulltext_provider_stats(&self) -> Option<&FullTextProviderStats> {
        self.provider_stats.as_ref()?.as_fulltext()
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
}

impl SearchCostEstimate {
    pub const fn new(score: f64) -> Self {
        Self {
            score,
            estimated_rows: None,
        }
    }

    pub const fn with_rows(mut self, estimated_rows: u64) -> Self {
        self.estimated_rows = Some(estimated_rows);
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
        GenerationRecoveryState, GenerationStats, MaintenancePriority, SearchExecutionMode,
        SearchProviderStats,
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
