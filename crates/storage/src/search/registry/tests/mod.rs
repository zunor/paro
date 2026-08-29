// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::index::fulltext::text_index::FullTextIndex;
use crate::index::hnsw::{DistanceMetric, SearchParams};
use crate::meta::{FileMetadataStore, GlobalSchemaMap, MetadataStore, TabletMetaManager};
use crate::rowset::{ColumnData, RowsetWriter, RowsetWriterContext, SparseVector};
use crate::search::artifact::{ArtifactLocation, SegmentPagePointer};
use crate::search::capability::{ArtifactSegmentRef, ArtifactSegmentSpan, SearchPartitionCoverage};
use crate::search::definition::origin::SCHEMA_SEED_BIT;
use crate::search::maintenance::ProviderMaintenanceRequest;
use crate::search::manifest::{ManifestDelta, ManifestDeltaEntry, DELTA_COUNT_SOFT_LIMIT};
use crate::search::stats::{
    ExecutionModes, FullTextProviderStats, GenerationMaintenanceState, GenerationStats,
    HnswProviderStats, SearchArtifactStats, SearchProviderStats, SparseProviderStats,
};
use crate::search::tail::{TailMutationKind, TailPendingEntry, TailRowImageRef};
use crate::search::{ArtifactFileId, SearchFreshnessPolicy, SearchStatsDelta};
use crate::search::{
    CoverageState, FlushSearchMode, OpenSearchCursorResult, SearchCapabilityState,
    SearchMaintenanceAction, SearchNotQueryableReason,
};
use crate::search::{OpenedSearchCursor, ResourceBudget, SearchBatchConfig, SearchBatchState};
use crate::table::table_factory::TableFactory;
use crate::table::table_handle::TableHandle;
use crate::tablet::{KeysType, Tablet, TabletColumn, TabletSchema, Version};
use crate::test_utils::*;
use paro_common::allocator::default_allocator;
use paro_common::chunk::Chunk;
use paro_common::effect::{SearchGenerationPublication, TabletMutation};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_scheduler::scheduler::TaskScheduler;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

fn create_table_with_root(
    root: &std::path::Path,
    types: &[LogicalType],
) -> crate::table::table_handle::TableHandle {
    TableFactory::new(Some(meta_manager(root)))
        .with_storage_root(root)
        .create_table(types)
        .expect("create table")
}

fn create_table_without_default_indexes(
    root: &std::path::Path,
    types: &[LogicalType],
) -> TableHandle {
    let columns = types
        .iter()
        .enumerate()
        .map(|(idx, logical_type)| {
            TabletColumn::new(idx as u32, format!("col_{idx}"), logical_type.clone())
        })
        .collect();
    let schema = Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap());
    let tablet = Tablet::new(10_001, 10_001, 0, schema, root.join("tablet"), None).unwrap();
    tablet.init().unwrap();
    TableHandle::from_runtime_tablet(tablet, types.to_vec())
}

#[test]
fn retiring_definition_invalidates_running_and_queued_builds() {
    let temp = TempDir::new().unwrap();
    let table = create_table_without_default_indexes(
        temp.path(),
        &[LogicalType::Integer, LogicalType::embedding(2)],
    );
    let registry = table.search_registry();
    let token = registry
        .begin_definition_build(77)
        .unwrap()
        .expect("active definition accepts provider work");
    assert!(!token.should_stop());

    registry.retire_definition_builds(77).unwrap();

    assert!(token.should_stop());
    assert!(registry.begin_definition_build(77).unwrap().is_none());
    assert!(registry.begin_definition_build(78).unwrap().is_some());

    registry.activate_definition_builds(77).unwrap();
    let replacement = registry
        .begin_definition_build(77)
        .unwrap()
        .expect("replacement definition accepts fresh provider work");
    assert!(!replacement.should_stop());
    assert!(token.should_stop(), "reactivation must not revive old work");
}

fn singleton_artifact_segment(artifact: &SearchArtifactRef) -> ArtifactSegmentRef {
    artifact
        .coverage
        .singleton_segment()
        .expect("test artifact must cover one segment")
}

fn create_schema_seeded_hnsw_table(
    root: &std::path::Path,
    types: &[LogicalType],
    vector_column: usize,
) -> TableHandle {
    let columns = types
        .iter()
        .enumerate()
        .map(|(idx, logical_type)| {
            let column = TabletColumn::new(idx as u32, format!("col_{idx}"), logical_type.clone());
            if idx == vector_column {
                column.with_hnsw_index(16, 64, 0)
            } else {
                column
            }
        })
        .collect();
    let schema = Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap());
    let tablet_id = 10_002;
    let tablet = Tablet::new(
        tablet_id,
        tablet_id,
        0,
        schema,
        root.join("tablet"),
        Some(meta_manager(root)),
    )
    .unwrap();
    tablet.init().unwrap();
    tablet.save_meta().unwrap();
    TableHandle::from_runtime_tablet(tablet, types.to_vec())
}

fn test_sparse_blob_vector(values: &[SparseVector]) -> Vector {
    let mut vector = Vector::try_new(LogicalType::Blob, values.len(), test_allocator())
        .expect("blob vector allocation");
    for (idx, value) in values.iter().enumerate() {
        vector.set_blob(idx, &value.to_row_image_v1().expect("sparse row image"));
    }
    vector.set_count(values.len());
    vector
}

fn reopen_table_with_root(
    root: &std::path::Path,
    types: &[LogicalType],
    descriptor: &crate::table::storage_descriptor::TableStorageDescriptor,
) -> crate::table::table_handle::TableHandle {
    TableFactory::new(Some(meta_manager(root)))
        .with_storage_root(root)
        .open_from_descriptor(types, descriptor)
        .expect("open table")
}

fn meta_manager(root: &std::path::Path) -> Arc<TabletMetaManager> {
    let metadata_store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(root.join("meta")).expect("meta store"));
    Arc::new(TabletMetaManager::new(
        metadata_store,
        Arc::new(GlobalSchemaMap::default()),
    ))
}

fn encode_varlen(values: &[&str]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes
}

fn drain_search_cursor(
    table: &TableHandle,
    opened: OpenedSearchCursor,
    projected_columns: &[usize],
    emit_score: bool,
    row_limit: usize,
) -> paro_common::error::Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    let mut cursor = opened.cursor;
    let snapshot = opened.snapshot;
    let batch_config = SearchBatchConfig {
        row_limit: row_limit.max(1),
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, row_limit.max(1024), 1);

    loop {
        match cursor.next_batch(&batch_config, &mut budget)? {
            SearchBatchState::Ready(batch) if batch.is_empty() => continue,
            SearchBatchState::Ready(batch) => chunks.push(table.materialize_search_batch(
                &snapshot,
                batch,
                projected_columns,
                emit_score,
                Arc::new(default_allocator()),
            )?),
            SearchBatchState::Exhausted => return Ok(chunks),
        }
    }
}

fn load_manifest_delta_entries(
    table: &crate::table::table_handle::TableHandle,
    definition_id: u64,
) -> Vec<ManifestDeltaEntry> {
    let current = table.search_registry().view.load();
    let state = current
        .definitions
        .get(&definition_id)
        .expect("definition state");
    let manifest = state.manifest.as_ref().expect("manifest");
    let delta_files = manifest.root.recent_delta_files.clone();
    let definition_dir = table
        .search_registry()
        .manifests
        .generation_dir(definition_id, manifest.root.generation_id);
    drop(current);

    delta_files
        .iter()
        .flat_map(|delta_file| {
            let bytes =
                std::fs::read(definition_dir.join(&delta_file.file_name)).expect("read delta");
            serde_json::from_slice::<ManifestDelta>(&bytes)
                .expect("decode delta")
                .entries
        })
        .collect()
}

fn fulltext_test_definition(definition_id: u64) -> SearchIndexDefinition {
    let provider_config = json!({"version": 1, "config": "simple"});
    SearchIndexDefinition {
        definition_id,
        table_id: 10,
        name: format!("fts_{definition_id}"),
        kind: SearchIndexKind::FullText,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::Required,
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    }
}

fn fulltext_test_artifact(
    definition_id: u64,
    rowset_id: u64,
    total_docs: u32,
    total_terms: u64,
    unique_terms: u32,
    total_postings: u64,
    max_posting_list_len: u32,
) -> SearchArtifactRef {
    SearchArtifactRef {
        definition_id,
        generation_id: 1,
        coverage: SearchPartitionCoverage::singleton(
            ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            u64::from(total_docs),
        )
        .unwrap(),
        column_id: 0,
        kind: SearchIndexKind::FullText,
        provider_variant: 1,
        artifact_format_version: 1,
        location: ArtifactLocation::Inline {
            page: SegmentPagePointer {
                rowset_id,
                segment_id: 0,
                column_id: 0,
                page_offset: rowset_id * 100,
                page_len: 64,
                checksum: rowset_id,
            },
        },
        stats: SearchArtifactStats {
            row_count: u64::from(total_docs),
            bytes_on_disk: 64,
            provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                total_docs,
                total_terms,
                avg_doc_length: if total_docs == 0 {
                    0.0
                } else {
                    total_terms as f32 / total_docs as f32
                },
                unique_terms,
                total_postings,
                max_posting_list_len,
                min_posting_list_len: 1,
                bm25_k1: 1.2,
                bm25_b: 0.75,
                tokenizer: "simple".to_string(),
            })),
        },
        checksum: rowset_id,
    }
}

fn sparse_test_definition(definition_id: u64) -> SearchIndexDefinition {
    let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
    SearchIndexDefinition {
        definition_id,
        table_id: 1,
        name: format!("sparse_{definition_id}"),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Sparse,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    }
}

fn sparse_test_artifact(
    definition_id: u64,
    rowset_id: u64,
    row_count: u64,
    nnz: u64,
    unique_dimensions: u64,
    max_l2_norm: f32,
) -> SearchArtifactRef {
    SearchArtifactRef {
        definition_id,
        generation_id: 1,
        coverage: SearchPartitionCoverage::singleton(
            ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            row_count,
        )
        .unwrap(),
        column_id: 0,
        kind: SearchIndexKind::Sparse,
        provider_variant: 1,
        artifact_format_version: 1,
        location: ArtifactLocation::Inline {
            page: SegmentPagePointer {
                rowset_id,
                segment_id: 0,
                column_id: 0,
                page_offset: rowset_id * 100,
                page_len: 64,
                checksum: rowset_id,
            },
        },
        stats: SearchArtifactStats {
            row_count,
            bytes_on_disk: 64,
            provider_stats: Some(SearchProviderStats::Sparse(SparseProviderStats {
                row_count,
                nnz,
                posting_fanout: nnz,
                unique_dimensions,
                avg_vector_nnz: if row_count == 0 {
                    0.0
                } else {
                    nnz as f32 / row_count as f32
                },
                l2_norm_sum: max_l2_norm as f64 * row_count as f64,
                max_l2_norm,
            })),
        },
        checksum: rowset_id,
    }
}

fn test_hnsw_provider_config(
    dimension: u32,
    m: usize,
    ef_construct: usize,
    inline_max_vector_count: u64,
) -> serde_json::Value {
    crate::search::HnswProviderConfig {
        version: crate::search::HNSW_PROVIDER_CONFIG_VERSION,
        dimension,
        distance: DistanceMetric::Euclidean,
        build_vector_encoding: crate::index::hnsw::HnswBuildVectorEncoding::symmetric_i16(
            dimension.min(128),
        )
        .unwrap(),
        m: m as u32,
        ef_construct: ef_construct as u32,
        ef_search: ef_construct as u32,
        rerank_policy: crate::index::hnsw::HnswRerankPolicy::default_for_encoding(
            crate::index::hnsw::HnswBuildVectorEncoding::symmetric_i16(dimension.min(128)).unwrap(),
        ),
        distance_cost: crate::index::hnsw::HnswDistanceCostProfile::default(),
        generation_layout: crate::search::HnswGenerationLayout::default(),
        maintenance: crate::search::HnswMaintenancePolicy::default(),
        build_seed: 1,
        proposal_wave_max_size: crate::search::DEFAULT_HNSW_PROPOSAL_WAVE_MAX_SIZE,
        warmup_point_count: crate::search::DEFAULT_HNSW_WARMUP_POINT_COUNT,
        filter_columns: Vec::new(),
        filter_block_rows: crate::search::DEFAULT_HNSW_FILTER_BLOCK_ROWS,
        filter_m: crate::search::DEFAULT_HNSW_FILTER_M,
        inline_threshold: crate::search::HnswInlineConfig {
            enabled: inline_max_vector_count != 0,
            max_vector_count: inline_max_vector_count,
            max_graph_memory_bytes: if inline_max_vector_count == 0 {
                0
            } else {
                64 * 1024 * 1024
            },
            max_dimension: if inline_max_vector_count == 0 {
                0
            } else {
                1_536
            },
        },
    }
    .validated()
    .unwrap()
    .to_value()
    .unwrap()
}

fn hnsw_test_definition(definition_id: u64) -> SearchIndexDefinition {
    let provider_config = test_hnsw_provider_config(128, 16, 100, 4_096);
    SearchIndexDefinition {
        definition_id,
        table_id: 1,
        name: format!("hnsw_{definition_id}"),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![0],
        expression: None,
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[0],
            None,
            &provider_config,
        ),
        provider_config,
    }
}

fn hnsw_test_artifact(
    definition_id: u64,
    rowset_id: u64,
    vector_count: u64,
    max_level: u32,
    max_level0_degree: u32,
) -> SearchArtifactRef {
    SearchArtifactRef {
        definition_id,
        generation_id: 1,
        coverage: SearchPartitionCoverage::singleton(
            ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            vector_count,
        )
        .unwrap(),
        column_id: 0,
        kind: SearchIndexKind::Hnsw,
        provider_variant: 1,
        artifact_format_version: 1,
        location: ArtifactLocation::Inline {
            page: SegmentPagePointer {
                rowset_id,
                segment_id: 0,
                column_id: 0,
                page_offset: rowset_id * 100,
                page_len: 64,
                checksum: rowset_id,
            },
        },
        stats: SearchArtifactStats {
            row_count: vector_count,
            bytes_on_disk: 64,
            provider_stats: Some(SearchProviderStats::Hnsw(HnswProviderStats {
                vector_count,
                dimension: 128,
                max_level,
                m: 16,
                ef_construction: 100,
                graph_memory_bytes: vector_count * 256,
                vector_storage_bytes: vector_count * 512,
                total_graph_links: vector_count * 18,
                level0_graph_links: vector_count * 12,
                avg_level0_degree: if vector_count == 0 { 0.0 } else { 12.0 },
                max_level0_degree,
            })),
        },
        checksum: rowset_id,
    }
}

mod definition_lifecycle;
mod generation_publication;
mod maintenance;
