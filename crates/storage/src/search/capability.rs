// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;

use super::artifact::ArtifactLocation;
use super::stats::{
    BuildEpoch, CatchUpBacklogTier, ConfigFingerprint, ExecutionModes, GenerationStats,
    MaintenancePriority, PreferHint, ProviderVariantId, SearchArtifactStats, SearchCostEstimate,
    SearchDefinitionId, SearchGenerationId, SegmentId, TableId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    pub freshness_policy: SearchFreshnessPolicy,
    pub config_fingerprint: ConfigFingerprint,
}

impl SearchIndexDefinition {
    /// Decode the durable HNSW provider contract. HNSW consumers use this
    /// method instead of inspecting JSON fields or supplying local defaults.
    pub fn hnsw_provider_config(&self) -> paro_common::error::Result<super::HnswProviderConfig> {
        if self.kind != SearchIndexKind::Hnsw {
            return Err(paro_common::error::invalid_input(format!(
                "search definition '{}' is not HNSW",
                self.name
            )));
        }
        super::HnswProviderConfig::from_value(&self.provider_config)
    }

    pub fn fulltext_provider_config(
        &self,
    ) -> paro_common::error::Result<super::FullTextProviderConfig> {
        if self.kind != SearchIndexKind::FullText {
            return Err(paro_common::error::invalid_input(format!(
                "search definition '{}' is not FullText",
                self.name
            )));
        }
        super::FullTextProviderConfig::from_value(&self.provider_config)
    }

    pub fn sparse_provider_config(
        &self,
    ) -> paro_common::error::Result<super::SparseProviderConfig> {
        if self.kind != SearchIndexKind::Sparse {
            return Err(paro_common::error::invalid_input(format!(
                "search definition '{}' is not Sparse",
                self.name
            )));
        }
        super::SparseProviderConfig::from_value(&self.provider_config)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchDefinitionOrigin {
    CatalogIndex { index_id: u64 },
    SchemaSeed { column_id: ColumnId },
}

impl SearchDefinitionOrigin {
    pub const fn catalog(index_id: u64) -> Self {
        Self::CatalogIndex { index_id }
    }

    pub const fn schema_seed(column_id: ColumnId) -> Self {
        Self::SchemaSeed { column_id }
    }

    pub const fn is_catalog_index(self) -> bool {
        matches!(self, Self::CatalogIndex { .. })
    }

    pub fn is_schema_seed_for(self, column_id: ColumnId) -> bool {
        matches!(self, Self::SchemaSeed { column_id: seed_column } if seed_column == column_id)
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchTailSummary {
    pub pending_rowsets: usize,
    pub pending_segments: usize,
    pub pending_rows: u64,
    pub pending_bytes: u64,
    pub delete_rows: u64,
    pub exact_tail_merge: bool,
    pub backlog_tier: CatchUpBacklogTier,
    pub maintenance_priority: MaintenancePriority,
}

impl SearchTailSummary {
    pub const fn complete() -> Self {
        Self {
            pending_rowsets: 0,
            pending_segments: 0,
            pending_rows: 0,
            pending_bytes: 0,
            delete_rows: 0,
            exact_tail_merge: true,
            backlog_tier: CatchUpBacklogTier::Healthy,
            maintenance_priority: MaintenancePriority::Idle,
        }
    }

    pub const fn has_pending_rows(&self) -> bool {
        self.pending_rows > 0
    }
}

impl Default for SearchTailSummary {
    fn default() -> Self {
        Self::complete()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchFreshnessPolicy {
    Required,
    BoundedLag {
        max_tail_rows: u64,
        max_lag_millis: u64,
    },
    Opportunistic,
}

impl SearchFreshnessPolicy {
    pub const fn bounded_by_tail_rows(max_tail_rows: u64) -> Self {
        Self::BoundedLag {
            max_tail_rows,
            max_lag_millis: 0,
        }
    }

    pub const fn default_for_kind(kind: SearchIndexKind) -> Self {
        match kind {
            SearchIndexKind::Hnsw => Self::bounded_by_tail_rows(4_096),
            SearchIndexKind::Sparse => Self::bounded_by_tail_rows(32_768),
            SearchIndexKind::FullText => Self::bounded_by_tail_rows(16_384),
        }
    }
}

/// Queryable generation head exposed to planners/providers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchGeneration {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub root_version: u64,
    pub build_epoch: BuildEpoch,
    pub build_snapshot_version: i64,
    pub indexed_through_ts: u64,
    pub coverage: CoverageState,
    pub tail_summary: SearchTailSummary,
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
    pub root_version: u64,
    pub indexed_through_ts: u64,
    pub coverage: CoverageState,
    pub tail_summary: SearchTailSummary,
    pub freshness_policy: SearchFreshnessPolicy,
    pub config_fingerprint: ConfigFingerprint,
    pub generation_stats: GenerationStats,
    pub execution_modes: ExecutionModes,
    pub estimated_cost: Option<SearchCostEstimate>,
    pub prefer_hint: Option<PreferHint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchCapabilityState {
    Queryable,
    NotQueryable { reason: SearchNotQueryableReason },
}

impl SearchCapabilityState {
    pub const fn is_queryable(&self) -> bool {
        matches!(self, Self::Queryable)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchNotQueryableReason {
    CoverageIncomplete,
    TailOverBudget,
    FreshnessRequired,
    ProviderDisabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityToken {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub root_version: u64,
    pub capability_state: SearchCapabilityState,
}

impl CapabilityToken {
    pub fn from_capability(capability: &SearchCapability) -> Self {
        Self {
            definition_id: capability.definition_id,
            generation_id: capability.generation_id,
            root_version: capability.root_version,
            capability_state: capability.capability_state(),
        }
    }

    pub const fn is_generation_stale(&self, current_generation_id: SearchGenerationId) -> bool {
        self.generation_id != current_generation_id
    }

    pub const fn is_queryable(&self) -> bool {
        self.capability_state.is_queryable()
    }
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
            root_version: generation.root_version,
            indexed_through_ts: generation.indexed_through_ts,
            coverage: generation.coverage.clone(),
            tail_summary: generation.tail_summary,
            freshness_policy: definition.freshness_policy,
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
        if !self.coverage.supports_exact_tail_merge()
            || !self
                .execution_modes
                .contains(super::stats::SearchExecutionMode::ExactTailMerge)
        {
            return false;
        }
        match self.freshness_policy {
            SearchFreshnessPolicy::Required => false,
            SearchFreshnessPolicy::BoundedLag { max_tail_rows, .. } => {
                self.tail_summary.pending_rows <= max_tail_rows
            }
            SearchFreshnessPolicy::Opportunistic => false,
        }
    }

    pub fn capability_state(&self) -> SearchCapabilityState {
        if self.is_queryable() {
            SearchCapabilityState::Queryable
        } else if self.tail_summary.has_pending_rows()
            && matches!(self.freshness_policy, SearchFreshnessPolicy::Required)
        {
            SearchCapabilityState::NotQueryable {
                reason: SearchNotQueryableReason::FreshnessRequired,
            }
        } else if self.tail_summary.has_pending_rows()
            && (!self.tail_summary.exact_tail_merge
                || matches!(
                    self.freshness_policy,
                    SearchFreshnessPolicy::BoundedLag { max_tail_rows, .. }
                        if self.tail_summary.pending_rows > max_tail_rows
                ))
        {
            SearchCapabilityState::NotQueryable {
                reason: SearchNotQueryableReason::TailOverBudget,
            }
        } else {
            SearchCapabilityState::NotQueryable {
                reason: SearchNotQueryableReason::CoverageIncomplete,
            }
        }
    }

    pub fn capability_token(&self) -> CapabilityToken {
        CapabilityToken::from_capability(self)
    }
}

impl SearchIndexDefinition {
    pub fn try_compute_config_fingerprint(
        kind: SearchIndexKind,
        column_ids: &[ColumnId],
        expression: Option<&str>,
        provider_config: &Value,
    ) -> paro_common::error::Result<u64> {
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
        match kind {
            SearchIndexKind::Hnsw => {
                let config = super::HnswProviderConfig::from_value(provider_config)?;
                payload.extend_from_slice(config.dimension.to_string().as_bytes());
                payload.push(b'|');
                payload.extend_from_slice(
                    serde_json::to_string(&config.build_contract())
                        .map_err(|error| {
                            paro_common::error::serialization_error(format!(
                                "serialize HNSW build contract fingerprint: {error}"
                            ))
                        })?
                        .as_bytes(),
                );
            }
            SearchIndexKind::FullText => {
                let config = super::FullTextProviderConfig::from_value(provider_config)?;
                payload.extend_from_slice(
                    serde_json::to_string(&config)
                        .map_err(|error| {
                            paro_common::error::serialization_error(format!(
                                "serialize FullText provider fingerprint: {error}"
                            ))
                        })?
                        .as_bytes(),
                );
            }
            SearchIndexKind::Sparse => {
                let config = super::SparseProviderConfig::from_value(provider_config)?;
                payload.extend_from_slice(
                    serde_json::to_string(&config)
                        .map_err(|error| {
                            paro_common::error::serialization_error(format!(
                                "serialize Sparse provider fingerprint: {error}"
                            ))
                        })?
                        .as_bytes(),
                );
            }
        }
        Ok(seahash::hash(&payload))
    }

    #[cfg(test)]
    pub fn compute_config_fingerprint(
        kind: SearchIndexKind,
        column_ids: &[ColumnId],
        expression: Option<&str>,
        provider_config: &Value,
    ) -> u64 {
        Self::try_compute_config_fingerprint(kind, column_ids, expression, provider_config)
            .expect("test provider configuration is valid")
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
        CoverageState, SearchCapability, SearchCapabilityState, SearchFreshnessPolicy,
        SearchGeneration, SearchIndexDefinition, SearchIndexKind, SearchNotQueryableReason,
        SearchTailSummary,
    };
    use crate::index::hnsw::DistanceMetric;
    use crate::search::artifact::{ArtifactFileId, ArtifactLocation};
    use crate::search::stats::{
        ExecutionModes, FullTextProviderStats, GenerationStats, SearchExecutionMode,
        SearchProviderStats,
    };
    use crate::search::{HnswInlineConfig, HnswProviderConfig, HNSW_PROVIDER_CONFIG_VERSION};

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
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 99,
        };
        let generation = SearchGeneration {
            definition_id: 7,
            generation_id: 13,
            root_version: 5,
            build_epoch: 2,
            build_snapshot_version: 42,
            indexed_through_ts: 42,
            coverage: CoverageState::Complete,
            tail_summary: SearchTailSummary::complete(),
            manifest_location: Some(ArtifactLocation::SidecarArtifactFile {
                file_id: ArtifactFileId {
                    definition_id: 7,
                    generation_id: 13,
                    package_index: 0,
                },
                offset: 0,
                len: 64,
                checksum: 1234,
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
        assert_eq!(capability.root_version, 5);
        assert_eq!(capability.indexed_through_ts, 42);
        assert_eq!(capability.tail_summary, SearchTailSummary::complete());
        assert_eq!(capability.kind, SearchIndexKind::FullText);
        assert!(capability
            .execution_modes
            .contains(SearchExecutionMode::ExactTailMerge));
    }

    #[test]
    fn capability_token_tracks_generation_staleness_not_root_version_churn() {
        let capability = SearchCapability {
            definition_id: 7,
            table_id: 11,
            kind: SearchIndexKind::FullText,
            generation_id: 13,
            root_version: 5,
            indexed_through_ts: 42,
            coverage: CoverageState::TailPending {
                pending_rowsets: 1,
                pending_segments: 1,
                pending_rows: 16,
                exact_tail_merge: false,
            },
            tail_summary: SearchTailSummary {
                pending_rowsets: 1,
                pending_segments: 1,
                pending_rows: 16,
                pending_bytes: 256,
                delete_rows: 0,
                exact_tail_merge: false,
                ..SearchTailSummary::complete()
            },
            freshness_policy: SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 8,
                max_lag_millis: 0,
            },
            config_fingerprint: 99,
            generation_stats: GenerationStats::default(),
            execution_modes: ExecutionModes::exact_only(),
            estimated_cost: None,
            prefer_hint: None,
        };

        let token = capability.capability_token();

        assert_eq!(token.definition_id, 7);
        assert_eq!(token.generation_id, 13);
        assert_eq!(token.root_version, 5);
        assert!(capability.tail_summary.has_pending_rows());
        assert!(!token.is_generation_stale(13));
        assert!(token.is_generation_stale(14));
        assert_eq!(
            token.capability_state,
            SearchCapabilityState::NotQueryable {
                reason: SearchNotQueryableReason::TailOverBudget,
            }
        );
        assert!(!token.is_queryable());
    }

    #[test]
    fn hnsw_artifact_fingerprint_excludes_search_and_placement_policy() {
        let base = HnswProviderConfig {
            version: HNSW_PROVIDER_CONFIG_VERSION,
            dimension: 16,
            distance: DistanceMetric::Cosine,
            m: 16,
            ef_construct: 96,
            ef_search: 64,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 0,
            build_seed: 7,
            inline_threshold: HnswInlineConfig {
                enabled: true,
                max_vector_count: 4_096,
                max_graph_memory_bytes: 64 * 1024 * 1024,
                max_dimension: 128,
            },
        }
        .validated()
        .unwrap();
        let mut policy_tuned = base.clone();
        policy_tuned.ef_search = 240;
        policy_tuned.plain_scan_threshold = 20_000;
        policy_tuned.inline_threshold.max_vector_count = 8_192;

        let fingerprint = |config: &HnswProviderConfig| {
            SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[3],
                None,
                &config.to_value().unwrap(),
            )
        };
        assert_eq!(fingerprint(&base), fingerprint(&policy_tuned));

        let mut rebuilt = base.clone();
        rebuilt.m = 24;
        rebuilt.ef_construct = 120;
        assert_ne!(fingerprint(&base), fingerprint(&rebuilt));
    }

    #[test]
    fn production_fingerprint_rejects_invalid_provider_config() {
        let invalid = serde_json::json!({
            "version": 1,
            "dimension": 16,
            "distance": "cosine",
            "m": 16,
            "ef_construct": 96,
            "ef_search": 64,
            "plain_scan_threshold": 10000,
            "filtered_plain_scan_threshold": 0,
            "build_seed": 7,
            "inline_threshold": {
                "enabled": false,
                "max_vector_count": 0,
                "max_graph_memory_bytes": 0,
                "max_dimension": 0
            },
            "unknown": true
        });
        assert!(SearchIndexDefinition::try_compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &invalid,
        )
        .is_err());
    }
}
