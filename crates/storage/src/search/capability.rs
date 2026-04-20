// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;

use super::artifact::ArtifactLocation;
use super::stats::{
    BuildEpoch, ConfigFingerprint, ExecutionModes, GenerationStats, PreferHint, ProviderVariantId,
    SearchArtifactStats, SearchCostEstimate, SearchDefinitionId, SearchGenerationId, SegmentId,
    TableId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SearchIndexKind {
    Hnsw,
    Sparse,
    FullText,
}

/// User-declared search intent. Definition alone does not imply queryability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchIndexDefinition {
    pub definition_id: SearchDefinitionId,
    pub table_id: TableId,
    pub name: String,
    pub kind: SearchIndexKind,
    pub column_ids: Vec<ColumnId>,
    pub expression: Option<String>,
    pub provider_config: Value,
    pub config_fingerprint: ConfigFingerprint,
}

/// Physical segment address for a durable artifact in the current storage layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArtifactSegmentRef {
    pub rowset_id: RowsetId,
    pub segment_id: SegmentId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchArtifactRef {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub segment: ArtifactSegmentRef,
    pub column_id: ColumnId,
    pub kind: SearchIndexKind,
    pub provider_variant: ProviderVariantId,
    pub artifact_format_version: u32,
    pub location: ArtifactLocation,
    pub stats: SearchArtifactStats,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoverageState {
    Complete,
    TailPending {
        pending_rowsets: usize,
        pending_segments: usize,
        pending_rows: u64,
        exact_tail_merge: bool,
    },
}

impl CoverageState {
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    pub const fn supports_exact_tail_merge(&self) -> bool {
        match self {
            Self::Complete => true,
            Self::TailPending {
                exact_tail_merge, ..
            } => *exact_tail_merge,
        }
    }
}

/// Queryable generation head exposed to planners/providers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchGeneration {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    pub build_snapshot_version: i64,
    pub coverage: CoverageState,
    pub manifest_location: Option<ArtifactLocation>,
    pub generation_stats: GenerationStats,
    pub execution_modes: ExecutionModes,
    pub config_fingerprint: ConfigFingerprint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchCapability {
    pub definition_id: SearchDefinitionId,
    pub table_id: TableId,
    pub kind: SearchIndexKind,
    pub generation_id: SearchGenerationId,
    pub coverage: CoverageState,
    pub config_fingerprint: ConfigFingerprint,
    pub generation_stats: GenerationStats,
    pub execution_modes: ExecutionModes,
    pub estimated_cost: Option<SearchCostEstimate>,
    pub prefer_hint: Option<PreferHint>,
}

impl SearchCapability {
    pub fn from_generation(
        definition: &SearchIndexDefinition,
        generation: &SearchGeneration,
    ) -> Self {
        Self {
            definition_id: definition.definition_id,
            table_id: definition.table_id,
            kind: definition.kind,
            generation_id: generation.generation_id,
            coverage: generation.coverage.clone(),
            config_fingerprint: generation.config_fingerprint,
            generation_stats: generation.generation_stats.clone(),
            execution_modes: generation.execution_modes.clone(),
            estimated_cost: None,
            prefer_hint: None,
        }
    }

    pub fn with_estimated_cost(mut self, estimated_cost: SearchCostEstimate) -> Self {
        self.estimated_cost = Some(estimated_cost);
        self
    }

    pub fn with_prefer_hint(mut self, prefer_hint: PreferHint) -> Self {
        self.prefer_hint = Some(prefer_hint);
        self
    }

    pub fn is_queryable(&self) -> bool {
        if self.coverage.is_complete() {
            return true;
        }
        self.coverage.supports_exact_tail_merge()
            && self
                .execution_modes
                .contains(super::stats::SearchExecutionMode::ExactTailMerge)
    }
}

impl SearchIndexDefinition {
    pub fn compute_config_fingerprint(
        kind: SearchIndexKind,
        column_ids: &[ColumnId],
        expression: Option<&str>,
        provider_config: &Value,
    ) -> u64 {
        let mut payload = Vec::new();
        payload.extend_from_slice(format!("{kind:?}|").as_bytes());
        for column_id in column_ids {
            payload.extend_from_slice(column_id.to_string().as_bytes());
            payload.push(b',');
        }
        payload.push(b'|');
        if let Some(expression) = expression {
            payload.extend_from_slice(expression.as_bytes());
        }
        payload.push(b'|');
        payload.extend_from_slice(provider_config.to_string().as_bytes());
        seahash::hash(&payload)
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SequentialCapability {
    pub table_id: TableId,
    pub estimated_cost: Option<SearchCostEstimate>,
}

impl SequentialCapability {
    pub fn with_estimated_cost(mut self, estimated_cost: SearchCostEstimate) -> Self {
        self.estimated_cost = Some(estimated_cost);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchPlanCandidate {
    QueryableIndex(SearchCapability),
    ExactScanFallback(SequentialCapability),
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CoverageState, SearchCapability, SearchGeneration, SearchIndexDefinition, SearchIndexKind,
    };
    use crate::search::artifact::ArtifactLocation;
    use crate::search::stats::{
        ExecutionModes, FullTextProviderStats, GenerationStats, SearchExecutionMode,
        SearchProviderStats,
    };

    #[test]
    fn coverage_state_knows_exact_tail_merge_contract() {
        let complete = CoverageState::Complete;
        let tail_pending = CoverageState::TailPending {
            pending_rowsets: 1,
            pending_segments: 2,
            pending_rows: 3,
            exact_tail_merge: true,
        };

        assert!(complete.is_complete());
        assert!(complete.supports_exact_tail_merge());
        assert!(!tail_pending.is_complete());
        assert!(tail_pending.supports_exact_tail_merge());
    }

    #[test]
    fn capability_is_derived_from_definition_and_generation() {
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![3],
            expression: Some("to_tsvector('simple', body)".to_string()),
            provider_config: json!({"tokenizer": "simple"}),
            config_fingerprint: 99,
        };
        let generation = SearchGeneration {
            definition_id: 7,
            generation_id: 13,
            build_epoch: 2,
            build_snapshot_version: 42,
            coverage: CoverageState::Complete,
            manifest_location: Some(ArtifactLocation::SidecarArtifactFile {
                relative_path: "artifact/fts.manifest".into(),
                byte_offset: 0,
                byte_length: 64,
            }),
            generation_stats: GenerationStats {
                indexed_rows: 100,
                artifact_count: 4,
                provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                    total_docs: 100,
                    total_terms: 300,
                    avg_doc_length: 3.0,
                    unique_terms: 10,
                    total_postings: 25,
                    max_posting_list_len: 5,
                    min_posting_list_len: 1,
                    bm25_k1: 1.2,
                    bm25_b: 0.75,
                    tokenizer: "simple".to_string(),
                })),
            },
            execution_modes: ExecutionModes::new([
                SearchExecutionMode::Exact,
                SearchExecutionMode::ExactTailMerge,
            ]),
            config_fingerprint: 99,
        };

        let capability = SearchCapability::from_generation(&definition, &generation);
        assert_eq!(capability.table_id, 11);
        assert_eq!(capability.definition_id, 7);
        assert_eq!(capability.generation_id, 13);
        assert_eq!(capability.kind, SearchIndexKind::FullText);
        assert!(capability
            .execution_modes
            .contains(SearchExecutionMode::ExactTailMerge));
    }
}
