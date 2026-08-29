// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Instant;

use crate::index::hnsw::ScoredPoint;
use crate::index::sparse::SparseVectorIndex;
use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::search::artifact::ArtifactLocation;
use crate::search::capability::SearchIndexKind;
use crate::search::cursor::{
    OpenedSearchCursor, SearchBatchState, SearchCursor, SearchReadSnapshot, VisibleSegment,
};
use crate::search::providers::sparse::row_image::decode_sparse_runtime_value;
use crate::search::row_fetch::snapshot_epoch;
use crate::search::segment_dispatch::{dispatch_segments, SegmentDispatchResult};
use crate::search::sidecar::{
    DecodedSidecarReaderRequest, SearchReaderRuntime, SidecarIntegrityPolicy, SidecarReaderRequest,
    SIDECAR_PACKAGE_CODEC,
};
use crate::search::tail::exact_merge::{ensure_tail_exact_merge_budget, TailExactMergeQueryShape};
use crate::search::tail_merge::{resolve_logical_rows, visible_row_ids};
use crate::search::telemetry::{
    GenerationTelemetryEvent, NoopSearchTelemetryCollector, QueryTelemetryEvent,
    SearchTelemetryCollector,
};
use crate::search::topk_merge::{ranked_rows_to_batch, RankedRow, TopKCollector};
use crate::tablet::TabletRef;
use paro_common::error::{self as paro_error, Result};

use crate::search::budget::{ResourceBudget, SearchBatchConfig};
use crate::search::cursor::PhysicalRowRef;

pub(crate) struct SparseSearchProvider {
    tablet: TabletRef,
    column_id: usize,
    query: SparseVector,
    k: usize,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
}

impl SparseSearchProvider {
    pub(crate) fn new(
        tablet: TabletRef,
        column_id: usize,
        query: &SparseVector,
        k: usize,
        predicate: Option<PredicateTree>,
    ) -> Self {
        Self {
            tablet,
            column_id,
            query: query.clone(),
            k,
            predicate,
            telemetry: Arc::new(NoopSearchTelemetryCollector),
        }
    }

    pub(crate) fn open(self, snapshot: SearchReadSnapshot) -> Result<OpenedSearchCursor> {
        if self.k == 0 {
            return Ok(OpenedSearchCursor {
                snapshot,
                cursor: Box::new(ExhaustedSparseCursor),
            });
        }

        ensure_tail_exact_merge_budget(
            &snapshot,
            SearchIndexKind::Sparse,
            self.column_id as u32,
            TailExactMergeQueryShape::Sparse {
                query_terms: self.query.len(),
                top_k: self.k,
            },
        )?;

        let reader_runtime = Arc::clone(&snapshot.reader_runtime);
        Ok(OpenedSearchCursor {
            snapshot: snapshot.clone(),
            cursor: Box::new(SparseSearchCursor {
                reader_runtime,
                snapshot,
                tablet: self.tablet,
                storage_col_id: self.column_id as u32,
                query: self.query,
                k: self.k,
                predicate: self.predicate,
                telemetry: self.telemetry,
                state: SparseCursorState::Pending,
            }),
        })
    }
}

#[derive(Debug)]
struct ExhaustedSparseCursor;

impl SearchCursor for ExhaustedSparseCursor {
    fn next_batch(
        &mut self,
        _batch: &SearchBatchConfig,
        _budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        Ok(SearchBatchState::Exhausted)
    }
}

#[derive(Debug)]
enum SparseCursorState {
    Pending,
    Ready { rows: Vec<RankedRow>, offset: usize },
    Exhausted,
}

struct SparseSearchCursor {
    reader_runtime: Arc<SearchReaderRuntime>,
    snapshot: SearchReadSnapshot,
    tablet: TabletRef,
    storage_col_id: u32,
    query: SparseVector,
    k: usize,
    predicate: Option<PredicateTree>,
    telemetry: Arc<dyn SearchTelemetryCollector>,
    state: SparseCursorState,
}

impl std::fmt::Debug for SparseSearchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparseSearchCursor")
            .field("storage_col_id", &self.storage_col_id)
            .field("k", &self.k)
            .field("state", &self.state)
            .finish()
    }
}

fn open_sidecar_sparse_index(
    snapshot: &SearchReadSnapshot,
    runtime: &SearchReaderRuntime,
    visible_segment: &VisibleSegment,
    column_id: u32,
) -> Result<Option<Arc<SparseVectorIndex>>> {
    let Some(artifact) =
        snapshot.artifact_for_segment(SearchIndexKind::Sparse, column_id, visible_segment)
    else {
        return Ok(None);
    };
    if !matches!(
        artifact.location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ) {
        return Ok(None);
    }

    let request = DecodedSidecarReaderRequest {
        sidecar: SidecarReaderRequest {
            location: &artifact.location,
            artifact_format_version: artifact.artifact_format_version,
            provider: SearchIndexKind::Sparse,
            codec: SIDECAR_PACKAGE_CODEC,
            integrity: SidecarIntegrityPolicy::EnvelopeChecksum,
        },
    };
    runtime.get_or_try_open_decoded(request, |cached| {
        SparseVectorIndex::deserialize(cached.bytes()).map(Some)
    })
}

fn sparse_ranked_rows_from_points(
    snapshot: &SearchReadSnapshot,
    visible_segment: &VisibleSegment,
    points: Vec<ScoredPoint>,
) -> Vec<RankedRow> {
    if snapshot.has_overlay_delete_vectors() {
        points
            .into_iter()
            .filter_map(|point| {
                let row = PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(point.idx as u32),
                );
                (!snapshot.is_overlay_deleted(row)).then(|| RankedRow::new(row, point.score))
            })
            .collect()
    } else {
        points
            .into_iter()
            .map(|point| {
                RankedRow::new(
                    PhysicalRowRef::new(
                        visible_segment.rowset_id,
                        visible_segment.segment_id,
                        crate::rowset::SegmentRowId::from_raw(point.idx as u32),
                    ),
                    point.score,
                )
            })
            .collect()
    }
}

impl SparseSearchCursor {
    fn build_ranked_rows(&self, budget: &ResourceBudget) -> Result<Vec<RankedRow>> {
        let started_at = Instant::now();
        self.telemetry.record_generation(GenerationTelemetryEvent {
            kind: SearchIndexKind::Sparse,
            definition_id: self.snapshot.generation.definition_id,
            generation_id: self.snapshot.generation.generation_id,
            build_epoch: self.snapshot.generation.build_epoch,
            coverage: self.snapshot.generation.coverage.clone(),
            artifact_count: self.snapshot.generation.artifacts.artifacts.len(),
        });
        let per_segment = dispatch_segments(
            SearchIndexKind::Sparse,
            self.snapshot.table_lease.visible_segments(),
            budget.parallelism_slots.max(1),
            self.telemetry.as_ref(),
            |_, segment| {
                let (rows, degraded) = self.search_segment(segment)?;
                Ok(SegmentDispatchResult {
                    candidates_produced: rows.len(),
                    degraded,
                    output: (rows, degraded),
                })
            },
        )?;

        let mut collector = TopKCollector::new(self.k);
        let mut candidates_produced = 0usize;
        let mut degraded_segments = 0usize;
        for (rows, degraded) in per_segment {
            candidates_produced += rows.len();
            degraded_segments += usize::from(degraded);
            collector.extend(rows);
        }
        let peak_heap_items = collector.len();
        let ranked_rows = collector.into_sorted_rows();
        self.telemetry.record_query(QueryTelemetryEvent {
            kind: SearchIndexKind::Sparse,
            segments_searched: self.snapshot.table_lease.visible_segment_count(),
            candidates_produced,
            rows_returned: ranked_rows.len(),
            peak_heap_items,
            degraded_segments,
            degraded_score_reasons: Vec::new(),
            elapsed: started_at.elapsed(),
        });
        Ok(ranked_rows)
    }

    fn search_segment(&self, visible_segment: &VisibleSegment) -> Result<(Vec<RankedRow>, bool)> {
        let snapshot_version = snapshot_epoch(self.snapshot.table.visible_version);
        if visible_segment
            .segment
            .sparse_index(self.storage_col_id)
            .is_some()
        {
            return visible_segment
                .segment
                .sparse_vector_search_with_epoch(
                    self.storage_col_id,
                    &self.query,
                    self.k,
                    snapshot_version,
                    self.predicate.as_ref(),
                )
                .map(|rows| {
                    let ranked_rows =
                        sparse_ranked_rows_from_points(&self.snapshot, visible_segment, rows);
                    (ranked_rows, false)
                });
        }
        if let Some(index) = open_sidecar_sparse_index(
            &self.snapshot,
            self.reader_runtime.as_ref(),
            visible_segment,
            self.storage_col_id,
        )? {
            let filter_bitmap = visible_segment
                .segment
                .build_filter_bitmap_with_epoch(snapshot_version, self.predicate.as_ref())?;
            if filter_bitmap
                .as_ref()
                .is_some_and(|bitmap| bitmap.is_empty())
            {
                return Ok((Vec::new(), false));
            }
            let rows = index.search(&self.query, self.k, filter_bitmap.as_ref())?;
            return Ok((
                sparse_ranked_rows_from_points(&self.snapshot, visible_segment, rows),
                false,
            ));
        }

        let row_ids = visible_row_ids(&self.snapshot, visible_segment, self.predicate.as_ref())?;
        if row_ids.is_empty() {
            return Ok((Vec::new(), true));
        }
        let resolved = resolve_logical_rows(
            &self.tablet,
            &self.snapshot,
            visible_segment,
            &row_ids,
            self.storage_col_id,
        )?;
        let column = resolved
            .column(0)
            .ok_or_else(|| paro_error::internal("resolved sparse tail chunk missing column"))?;
        let mut collector = TopKCollector::new(self.k);
        for (offset, row_id) in row_ids.iter().copied().enumerate() {
            let Some(vector) = decode_sparse_runtime_value(column.get_value(offset))? else {
                continue;
            };
            let Some(score) = self.query.dot(&vector) else {
                continue;
            };
            collector.push(RankedRow::new(
                PhysicalRowRef::new(
                    visible_segment.rowset_id,
                    visible_segment.segment_id,
                    crate::rowset::SegmentRowId::from_raw(row_id),
                ),
                score,
            ));
        }
        Ok((collector.into_sorted_rows(), true))
    }
}

impl SearchCursor for SparseSearchCursor {
    fn next_batch(
        &mut self,
        batch: &SearchBatchConfig,
        budget: &mut ResourceBudget,
    ) -> Result<SearchBatchState> {
        if matches!(self.state, SparseCursorState::Pending) {
            let ranked_rows = self.build_ranked_rows(budget)?;
            self.state = if ranked_rows.is_empty() {
                SparseCursorState::Exhausted
            } else {
                SparseCursorState::Ready {
                    rows: ranked_rows,
                    offset: 0,
                }
            };
        }

        match &mut self.state {
            SparseCursorState::Pending => Err(paro_error::internal(
                "sparse cursor did not transition out of pending state",
            )),
            SparseCursorState::Exhausted => Ok(SearchBatchState::Exhausted),
            SparseCursorState::Ready { rows, offset } => {
                let row_limit = batch.row_limit.max(1);
                let end = (*offset + row_limit).min(rows.len());
                let candidate_batch = ranked_rows_to_batch(rows[*offset..end].to_vec());
                *offset = end;
                if *offset >= rows.len() {
                    self.state = SparseCursorState::Exhausted;
                }
                Ok(SearchBatchState::Ready(candidate_batch))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::sparse::SparseIndexBuilder;
    use crate::metrics::storage_metrics;
    use crate::search::capability::{ArtifactSegmentRef, CoverageState, SearchArtifactRef};
    use crate::search::cursor::{
        GenerationArtifactSet, GenerationReadLease, GenerationReadSnapshot, TableReadLease,
    };
    use crate::search::providers::sparse::row_image::encode_sparse_row_image;
    use crate::search::stats::{GenerationMaintenanceState, GenerationStats, SearchArtifactStats};
    use crate::search::{SearchReaderRuntime, SidecarArtifactStore};
    use crate::table::table_factory::TableFactory;
    use crate::test_utils::{test_allocator, test_chunk_from_vectors};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    fn test_sparse_blob_vector(values: &[SparseVector]) -> Vector {
        let mut vector = Vector::try_new(LogicalType::Blob, values.len(), test_allocator())
            .expect("blob vector allocation");
        for (idx, value) in values.iter().enumerate() {
            vector.set_blob(
                idx,
                &encode_sparse_row_image(value).expect("sparse row image"),
            );
        }
        vector.set_count(values.len());
        vector
    }

    #[test]
    #[serial_test::serial]
    fn sparse_cursor_reads_sidecar_artifact_through_reader_cache() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        storage_metrics().reset_for_tests();
        let table = TableFactory::default()
            .create_table(&[LogicalType::Blob])
            .expect("create table");
        let first = SparseVector::parse("{1:1.0,2:0.5}").unwrap();
        let second = SparseVector::parse("{2:1.0}").unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
                first.clone(),
                second.clone(),
            ])]))
            .expect("append rows");

        let (table_snapshot, table_lease) = TableReadLease::open(
            &table.tablet(),
            table.tablet_id(),
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("open table lease");
        let visible_segment = table_lease
            .visible_segments()
            .first()
            .expect("visible segment");
        assert!(visible_segment.segment.sparse_index(0).is_none());

        let query = SparseVector::parse("{1:1.0}").unwrap();
        let mut builder = SparseIndexBuilder::new();
        builder.add(0, &first).unwrap();
        builder.add(1, &second).unwrap();
        let index = builder.build();
        let bytes = index.serialize().unwrap();

        let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
        let file_id = SidecarArtifactStore::default_shard_file_id(8, 1);
        let mut writer = store.create_package_writer(file_id).unwrap();
        let location = writer.append_artifact(&bytes).unwrap();
        writer.finalize().unwrap();

        let artifact = SearchArtifactRef {
            definition_id: 8,
            generation_id: 1,
            coverage: crate::search::SearchPartitionCoverage::singleton(
                ArtifactSegmentRef {
                    rowset_id: visible_segment.rowset_id,
                    segment_id: visible_segment.segment_id,
                },
                2,
            )
            .unwrap(),
            column_id: 0,
            kind: SearchIndexKind::Sparse,
            provider_variant: 1,
            artifact_format_version: 1,
            location,
            stats: SearchArtifactStats {
                row_count: 2,
                bytes_on_disk: bytes.len() as u64,
                provider_stats: None,
            },
            checksum: seahash::hash(&bytes),
        };
        let generation = GenerationReadSnapshot {
            definition_id: 8,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: table.max_version(),
            indexed_through_ts: table.max_version() as u64,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            provider_config: Arc::new(serde_json::Value::Null),
            hnsw_provider_config: None,
            hnsw_query_activity: None,
            artifacts: Arc::new(GenerationArtifactSet {
                artifacts: vec![artifact],
            }),
            tail_pending_entries: Arc::from([]),
        };
        let generation_lease = GenerationReadLease::from_snapshot(&generation);
        let snapshot = SearchReadSnapshot::new(
            table_snapshot,
            SearchIndexKind::Sparse,
            generation,
            table_lease,
            generation_lease,
            Arc::new(SearchReaderRuntime::new(SidecarArtifactStore::new(
                table.tablet().data_dir().clone(),
            ))),
        );

        let opened = SparseSearchProvider::new(table.tablet(), 0, &query, 10, None)
            .open(snapshot)
            .unwrap();
        let mut cursor = opened.cursor;
        let mut budget = ResourceBudget::default();
        let batch = match cursor
            .next_batch(&SearchBatchConfig::default(), &mut budget)
            .unwrap()
        {
            SearchBatchState::Ready(batch) => batch,
            SearchBatchState::Exhausted => panic!("expected sidecar result"),
        };

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row_offset, 0);
        let metrics = storage_metrics().snapshot();
        let sidecar = metrics
            .search_sidecar_reader_by_key
            .iter()
            .find(|series| series.key.provider == SearchIndexKind::Sparse)
            .expect("sparse sidecar reader metrics");
        assert_eq!(sidecar.counters.open_count_total, 1);
        assert_eq!(sidecar.counters.cache_misses_total, 1);
        assert!(sidecar.counters.format_dispatch_total >= 1);
    }

    #[test]
    fn sparse_tail_exact_fallback_reads_typed_binary_row_image() {
        let table = TableFactory::default()
            .create_table(&[LogicalType::Blob])
            .expect("create table");
        let first =
            encode_sparse_row_image(&SparseVector::new(vec![1, 4], vec![1.0, 0.5]).unwrap())
                .unwrap();
        let second =
            encode_sparse_row_image(&SparseVector::new(vec![2], vec![3.0]).unwrap()).unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_blob_vector(&[
                first, second,
            ])]))
            .expect("append rows");

        let (table_snapshot, table_lease) = TableReadLease::open(
            &table.tablet(),
            table.tablet_id(),
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("open table lease");
        let visible_segment = table_lease
            .visible_segments()
            .first()
            .expect("visible segment");
        let generation = GenerationReadSnapshot {
            definition_id: 9,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: table.max_version(),
            indexed_through_ts: table.max_version() as u64,
            coverage: CoverageState::TailPending {
                pending_rowsets: 1,
                pending_segments: 1,
                pending_rows: 2,
                exact_tail_merge: true,
            },
            generation_stats: GenerationStats::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            provider_config: Arc::new(serde_json::Value::Null),
            hnsw_provider_config: None,
            hnsw_query_activity: None,
            artifacts: Arc::new(GenerationArtifactSet {
                artifacts: Vec::new(),
            }),
            tail_pending_entries: Arc::from([crate::search::TailPendingEntry {
                entry_id: crate::search::TailEntryId(1),
                rowset_id: visible_segment.rowset_id,
                segment_ids: vec![visible_segment.segment_id],
                mutation: crate::search::TailMutationKind::Append,
                row_count: 2,
                byte_count: visible_segment.segment.file_size(),
                row_image_ref: Some(crate::search::TailRowImageRef::WholeRowset),
            }]),
        };
        let generation_lease = GenerationReadLease::from_snapshot(&generation);
        let snapshot = SearchReadSnapshot::new(
            table_snapshot,
            SearchIndexKind::Sparse,
            generation,
            table_lease,
            generation_lease,
            Arc::new(SearchReaderRuntime::new(SidecarArtifactStore::new(
                table.tablet().data_dir().clone(),
            ))),
        );
        assert_eq!(snapshot.tail_window().committed_rows, 2);
        let query = SparseVector::new(vec![1], vec![1.0]).unwrap();
        let opened = SparseSearchProvider::new(table.tablet(), 0, &query, 10, None)
            .open(snapshot)
            .unwrap();
        let mut cursor = opened.cursor;
        let mut budget = ResourceBudget::default();
        let batch = match cursor
            .next_batch(&SearchBatchConfig::default(), &mut budget)
            .unwrap()
        {
            SearchBatchState::Ready(batch) => batch,
            SearchBatchState::Exhausted => panic!("expected typed sparse tail result"),
        };

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row_offset, 0);
    }

    fn test_blob_vector(values: &[Vec<u8>]) -> Vector {
        let mut vector = Vector::try_new(LogicalType::Blob, values.len(), test_allocator())
            .expect("blob vector allocation");
        for (idx, value) in values.iter().enumerate() {
            vector.set_value(idx, &Value::Blob(value.clone()));
        }
        vector.set_count(values.len());
        vector
    }
}
