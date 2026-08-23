// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment::Segment;
use super::segment_format::{ColumnMeta, SegmentFooter};
use crate::index::art::ART;
use crate::index::fulltext::text_index::FullTextIndex;
use crate::index::sparse::SparseVectorIndex;
use crate::index::{
    BitmapIndex, BloomFilterIndex, BoundIndex, HnswIndex, PredicateIndexBinding,
    SegmentLocalComplete,
};
use crate::rowset::page::{CompressionType, PagePointer};
use crate::statistics::{
    FullTextIndexStatistics, HnswIndexStatistics, IndexStatistics, IndexType,
    SegmentIndexStatistics, SparseIndexStatistics,
};
use crate::tablet::ColumnId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub(super) enum DeferredHnswState {
    Unloaded,
    Ready {
        index: Arc<HnswIndex>,
        statistics: HnswIndexStatistics,
    },
    Unsupported {
        reason: String,
    },
    Failed(paro_common::error::ParoError),
}

/// Structural pointer to an inline HNSW artifact. Its page is read and the
/// index is materialized only when a vector-search capability asks for it.
/// Unsupported durable versions are a recoverable capability state; current
/// format corruption remains an error at that capability boundary.
pub(super) struct DeferredHnswIndex {
    pub(super) page_pointer: PagePointer,
    pub(super) compression: CompressionType,
    pub(super) vector_data_offset: u64,
    pub(super) vector_data_len: u64,
    pub(super) dimension: usize,
    pub(super) state: Mutex<DeferredHnswState>,
}

#[derive(Default)]
pub struct SegmentPredicateIndexes {
    pub(super) bloom_filters: HashMap<ColumnId, Arc<BloomFilterIndex>>,
    pub(super) bitmap_indexes: HashMap<ColumnId, Arc<BitmapIndex>>,
    /// One catalog scalar index can expose ordered ART and low-cardinality
    /// bitmap representations. They share one completeness credential and are
    /// published/replaced atomically under a single lock.
    pub(super) runtime_scalar_indexes: RwLock<HashMap<ColumnId, ScalarAccessPaths>>,
}

pub(super) struct ScalarAccessPaths {
    art: Arc<ART>,
    bitmap: Option<Arc<BitmapIndex>>,
    completeness: SegmentLocalComplete,
}

impl SegmentPredicateIndexes {
    pub fn bloom_filter_index(&self, column_id: ColumnId) -> Option<Arc<BloomFilterIndex>> {
        self.bloom_filters.get(&column_id).cloned()
    }

    pub fn bitmap_index(&self, column_id: ColumnId) -> Option<Arc<BitmapIndex>> {
        self.runtime_scalar_indexes
            .read()
            .ok()
            .and_then(|guard| {
                guard
                    .get(&column_id)
                    .and_then(|paths| paths.bitmap.as_ref().cloned())
            })
            .or_else(|| self.bitmap_indexes.get(&column_id).cloned())
    }

    pub fn art_index(&self, column_id: ColumnId) -> Option<Arc<ART>> {
        self.runtime_scalar_indexes
            .read()
            .ok()
            .and_then(|guard| guard.get(&column_id).map(|paths| Arc::clone(&paths.art)))
    }

    pub(crate) fn predicate_indexes(
        &self,
        segment_rows: u64,
    ) -> paro_common::error::Result<Vec<PredicateIndexBinding>> {
        let mut results = Vec::with_capacity(
            self.bloom_filters
                .len()
                .saturating_add(self.bitmap_indexes.len())
                .saturating_add(
                    self.runtime_scalar_indexes
                        .read()
                        .map(|guard| guard.len().saturating_mul(2))
                        .unwrap_or(0),
                ),
        );
        for idx in self.bloom_filters.values() {
            results.push(PredicateIndexBinding::candidate(
                Arc::clone(idx) as Arc<dyn BoundIndex>
            ));
        }
        for idx in self.bitmap_indexes.values() {
            let completeness = SegmentLocalComplete::prove(idx.indexed_row_count(), segment_rows)?;
            results.push(PredicateIndexBinding::complete_scalar(
                Arc::clone(idx) as Arc<dyn BoundIndex>,
                completeness,
            ));
        }
        if let Ok(guard) = self.runtime_scalar_indexes.read() {
            for paths in guard.values() {
                if !paths.completeness.covers(segment_rows) {
                    return Err(paro_common::error::data_corrupted(format!(
                        "runtime scalar index coverage {} does not match segment rows {segment_rows}",
                        paths.completeness.row_count()
                    )));
                }
                if let Some(bitmap) = &paths.bitmap {
                    results.push(PredicateIndexBinding::complete_scalar(
                        Arc::clone(bitmap) as Arc<dyn BoundIndex>,
                        paths.completeness,
                    ));
                }
                results.push(PredicateIndexBinding::complete_scalar(
                    Arc::clone(&paths.art) as Arc<dyn BoundIndex>,
                    paths.completeness,
                ));
            }
        }
        Ok(results)
    }
}

#[derive(Default)]
pub struct SegmentSearchIndexes {
    pub(super) hnsw_indexes: HashMap<ColumnId, Arc<DeferredHnswIndex>>,
    pub(super) sparse_indexes: HashMap<ColumnId, Arc<SparseVectorIndex>>,
    pub(super) fulltext_indexes: HashMap<ColumnId, Arc<FullTextIndex>>,
    pub(super) runtime_fulltext_indexes: RwLock<HashMap<ColumnId, Arc<FullTextIndex>>>,
}

#[derive(Default)]
pub struct SegmentIndexes {
    pub(super) predicate: SegmentPredicateIndexes,
    pub(super) search: SegmentSearchIndexes,
}

#[derive(Default)]
pub struct SegmentIndexStats {
    pub(super) hnsw_stats: HashMap<ColumnId, HnswIndexStatistics>,
    pub(super) sparse_stats: HashMap<ColumnId, SparseIndexStatistics>,
    pub(super) fulltext_stats: HashMap<ColumnId, FullTextIndexStatistics>,
    pub(super) runtime_fulltext_stats: RwLock<HashMap<ColumnId, FullTextIndexStatistics>>,
}

impl SegmentIndexStats {
    fn footer_pointer_size(pointer_size: Option<u32>) -> u64 {
        pointer_size.unwrap_or_default() as u64
    }

    fn fulltext_runtime_size(
        runtime_fulltext_indexes: &RwLock<HashMap<ColumnId, Arc<FullTextIndex>>>,
        column_id: ColumnId,
    ) -> Option<u64> {
        let index = runtime_fulltext_indexes
            .read()
            .ok()
            .and_then(|guard| guard.get(&column_id).cloned())?;
        index.serialize().ok().map(|bytes| bytes.len() as u64)
    }

    fn collect_column_stats(
        &self,
        meta: &ColumnMeta,
        runtime_fulltext_indexes: &RwLock<HashMap<ColumnId, Arc<FullTextIndex>>>,
    ) -> Vec<IndexStatistics> {
        let mut stats = Vec::new();

        if meta.zonemap_index_pointer.size > 0 {
            stats.push(IndexStatistics::new(
                IndexType::ZoneMap,
                meta.zonemap_index_pointer.size as u64,
                0,
            ));
        }

        if let Some(ptr) = meta.bloom_filter_pointer.filter(|ptr| ptr.size > 0) {
            stats.push(IndexStatistics::new(IndexType::Bloom, ptr.size as u64, 0));
        }

        if let Some(ptr) = meta.bitmap_index_pointer.filter(|ptr| ptr.size > 0) {
            stats.push(IndexStatistics::new(IndexType::Bitmap, ptr.size as u64, 0));
        }

        if let Some(hnsw_stats) = self.hnsw_stats.get(&meta.column_id) {
            stats.push(IndexStatistics::new(
                IndexType::HNSW,
                meta.hnsw_index_pointer
                    .map(|ptr| ptr.size as u64)
                    .unwrap_or(hnsw_stats.graph_size_bytes + hnsw_stats.storage_size_bytes),
                hnsw_stats.num_indexed_vectors as u64,
            ));
        } else if let Some(ptr) = meta.hnsw_index_pointer.filter(|ptr| ptr.size > 0) {
            stats.push(IndexStatistics::new(IndexType::HNSW, ptr.size as u64, 0));
        }

        if let Some(sparse_stats) = self.sparse_stats.get(&meta.column_id) {
            stats.push(IndexStatistics::new(
                IndexType::Sparse,
                Self::footer_pointer_size(meta.sparse_index_pointer.map(|ptr| ptr.size)),
                sparse_stats.total_postings as u64,
            ));
        } else if let Some(ptr) = meta.sparse_index_pointer.filter(|ptr| ptr.size > 0) {
            stats.push(IndexStatistics::new(IndexType::Sparse, ptr.size as u64, 0));
        }

        if let Ok(runtime_stats) = self.runtime_fulltext_stats.read() {
            if let Some(fulltext_stats) = runtime_stats.get(&meta.column_id) {
                stats.push(IndexStatistics::new(
                    IndexType::FullText,
                    Self::fulltext_runtime_size(runtime_fulltext_indexes, meta.column_id)
                        .unwrap_or_else(|| {
                            Self::footer_pointer_size(
                                meta.fulltext_index_pointer.map(|ptr| ptr.size),
                            )
                        }),
                    fulltext_stats.total_postings,
                ));
                return stats;
            }
        }

        if let Some(fulltext_stats) = self.fulltext_stats.get(&meta.column_id) {
            stats.push(IndexStatistics::new(
                IndexType::FullText,
                Self::footer_pointer_size(meta.fulltext_index_pointer.map(|ptr| ptr.size)),
                fulltext_stats.total_postings,
            ));
        } else if let Some(ptr) = meta.fulltext_index_pointer.filter(|ptr| ptr.size > 0) {
            stats.push(IndexStatistics::new(
                IndexType::FullText,
                ptr.size as u64,
                0,
            ));
        }

        stats
    }

    pub fn segment_index_statistics(
        &self,
        footer: &SegmentFooter,
        runtime_fulltext_indexes: &RwLock<HashMap<ColumnId, Arc<FullTextIndex>>>,
    ) -> SegmentIndexStatistics {
        let mut stats = SegmentIndexStatistics::new();
        for meta in &footer.column_metas {
            stats.add_column(
                meta.column_id,
                self.collect_column_stats(meta, runtime_fulltext_indexes),
            );
        }
        stats
    }
}

impl Segment {
    /// Whether the segment footer advertises an inline HNSW artifact. This is
    /// a metadata-only check and never opens the artifact.
    pub fn has_hnsw_artifact(&self, column_id: ColumnId) -> bool {
        self.indexes.search.hnsw_indexes.contains_key(&column_id)
    }

    /// Materialize the HNSW capability on first use.
    pub fn open_hnsw_index(
        &self,
        column_id: ColumnId,
    ) -> paro_common::error::Result<Option<Arc<HnswIndex>>> {
        let Some(deferred) = self.indexes.search.hnsw_indexes.get(&column_id) else {
            return Ok(None);
        };
        self.materialize_hnsw_index(deferred)
    }

    /// Test-only convenience for assertions over freshly written segments.
    #[cfg(test)]
    pub fn hnsw_index(&self, column_id: ColumnId) -> Option<Arc<HnswIndex>> {
        self.open_hnsw_index(column_id).ok().flatten()
    }

    /// Get HNSW index statistics for a column.
    pub fn hnsw_index_statistics(&self, column_id: ColumnId) -> Option<HnswIndexStatistics> {
        if let Some(stats) = self.index_stats.hnsw_stats.get(&column_id) {
            return Some(stats.clone());
        }
        let deferred = self.indexes.search.hnsw_indexes.get(&column_id)?;
        let state = deferred.state.lock().ok()?;
        match &*state {
            DeferredHnswState::Ready { statistics, .. } => Some(statistics.clone()),
            _ => None,
        }
    }

    pub fn hnsw_rebuild_reason(&self, column_id: ColumnId) -> Option<String> {
        let deferred = self.indexes.search.hnsw_indexes.get(&column_id)?;
        let state = deferred.state.lock().ok()?;
        match &*state {
            DeferredHnswState::Unsupported { reason } => Some(reason.clone()),
            _ => None,
        }
    }

    /// Get sparse index for a column.
    pub fn sparse_index(&self, column_id: ColumnId) -> Option<Arc<SparseVectorIndex>> {
        self.indexes.search.sparse_indexes.get(&column_id).cloned()
    }

    /// Get sparse index statistics for a column.
    pub fn sparse_index_statistics(&self, column_id: ColumnId) -> Option<&SparseIndexStatistics> {
        self.index_stats.sparse_stats.get(&column_id)
    }

    /// Get full-text index for a column.
    pub fn fulltext_index(&self, column_id: ColumnId) -> Option<Arc<FullTextIndex>> {
        if let Ok(guard) = self.indexes.search.runtime_fulltext_indexes.read() {
            if let Some(index) = guard.get(&column_id) {
                return Some(Arc::clone(index));
            }
        }
        self.indexes
            .search
            .fulltext_indexes
            .get(&column_id)
            .cloned()
    }

    /// Get full-text index statistics for a column.
    pub fn fulltext_index_statistics(
        &self,
        column_id: ColumnId,
    ) -> Option<FullTextIndexStatistics> {
        if let Ok(guard) = self.index_stats.runtime_fulltext_stats.read() {
            if let Some(stats) = guard.get(&column_id) {
                return Some(stats.clone());
            }
        }
        self.index_stats.fulltext_stats.get(&column_id).cloned()
    }

    /// Register a runtime full-text index built after segment open.
    pub fn register_runtime_fulltext_index(&self, column_id: ColumnId, index: Arc<FullTextIndex>) {
        let stats = FullTextIndexStatistics::collect(index.as_ref());
        if let Ok(mut guard) = self.indexes.search.runtime_fulltext_indexes.write() {
            guard.insert(column_id, Arc::clone(&index));
        }
        if let Ok(mut guard) = self.index_stats.runtime_fulltext_stats.write() {
            guard.insert(column_id, stats);
        }
    }

    /// Get bloom filter index for a column.
    pub fn bloom_filter_index(&self, column_id: ColumnId) -> Option<Arc<BloomFilterIndex>> {
        self.indexes.predicate.bloom_filter_index(column_id)
    }

    /// Get bitmap index for a column.
    pub fn bitmap_index(&self, column_id: ColumnId) -> Option<Arc<BitmapIndex>> {
        self.indexes.predicate.bitmap_index(column_id)
    }

    /// Get runtime ART index for a column.
    pub fn art_index(&self, column_id: ColumnId) -> Option<Arc<ART>> {
        self.indexes.predicate.art_index(column_id)
    }

    /// Atomically publish every physical representation of one complete
    /// segment-local scalar index.
    pub(crate) fn register_runtime_scalar_index(
        &self,
        column_id: ColumnId,
        art: Arc<ART>,
        bitmap: Option<Arc<BitmapIndex>>,
        completeness: SegmentLocalComplete,
    ) {
        if let Ok(mut guard) = self.indexes.predicate.runtime_scalar_indexes.write() {
            guard.insert(
                column_id,
                ScalarAccessPaths {
                    art,
                    bitmap,
                    completeness,
                },
            );
        }
    }

    /// Remove a runtime ART index previously registered on this segment.
    pub fn drop_art_index(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.indexes.predicate.runtime_scalar_indexes.write() {
            guard.remove(&column_id);
        }
    }

    /// Get predicate indexes for evaluator use.
    pub(crate) fn predicate_indexes(
        &self,
    ) -> paro_common::error::Result<Vec<PredicateIndexBinding>> {
        self.indexes.predicate.predicate_indexes(self.num_rows())
    }

    /// Get per-column index statistics for this segment.
    pub fn index_statistics(&self) -> SegmentIndexStatistics {
        self.index_stats
            .segment_index_statistics(&self.footer, &self.indexes.search.runtime_fulltext_indexes)
    }
}
