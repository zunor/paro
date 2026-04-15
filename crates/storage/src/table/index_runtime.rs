use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::buffer::{BufferManager, StandardBufferManager};
use crate::codec::vector_decoder;
use crate::index::art::{ARTConflictType, ARTKey, ART};
use crate::index::fulltext::text_index::{FullTextIndex, FullTextIndexConfig};
use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::index::{BoundIndex, IndexAppendMode, IndexConstraintType};
use crate::rowset::{RowsetSharedPtr, SegmentSharedPtr};
use crate::statistics::{FullTextIndexStatistics, HnswIndexStatistics, SparseIndexStatistics};
use crate::table::table_handle::FullTextIndexCoverage;
use crate::tablet::{ColumnId, TabletRef};
use paro_common::allocator::{default_allocator, Allocator, ArenaAllocator};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::index_set::IndexSet;

const ART_BACKFILL_BATCH_SIZE: usize = 1024;

fn merge_fulltext_index_stats(
    agg: Option<FullTextIndexStatistics>,
    incoming: FullTextIndexStatistics,
) -> Option<FullTextIndexStatistics> {
    Some(match agg {
        None => incoming,
        Some(mut merged) => {
            merged.total_docs = merged.total_docs.saturating_add(incoming.total_docs);
            merged.total_terms = merged.total_terms.saturating_add(incoming.total_terms);
            merged.unique_terms = merged.unique_terms.saturating_add(incoming.unique_terms);
            merged.total_postings = merged
                .total_postings
                .saturating_add(incoming.total_postings);
            merged.max_posting_list_len = merged
                .max_posting_list_len
                .max(incoming.max_posting_list_len);
            if merged.min_posting_list_len == 0 {
                merged.min_posting_list_len = incoming.min_posting_list_len;
            } else if incoming.min_posting_list_len > 0 {
                merged.min_posting_list_len = merged
                    .min_posting_list_len
                    .min(incoming.min_posting_list_len);
            }
            merged.avg_doc_length = if merged.total_docs == 0 {
                0.0
            } else {
                merged.total_terms as f32 / merged.total_docs as f32
            };
            merged
        }
    })
}

fn is_runtime_art_supported_type(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Date
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
    )
}

#[derive(Debug)]
pub(crate) struct IndexRuntime {
    indexes: IndexSet,
    declared_art_indexes: RwLock<HashSet<ColumnId>>,
    declared_vector_indexes: RwLock<HashSet<ColumnId>>,
    declared_sparse_indexes: RwLock<HashSet<ColumnId>>,
    declared_fulltext_indexes: RwLock<HashMap<ColumnId, String>>,
}

impl IndexRuntime {
    pub(crate) fn new() -> Self {
        Self {
            indexes: IndexSet::new(),
            declared_art_indexes: RwLock::new(HashSet::new()),
            declared_vector_indexes: RwLock::new(HashSet::new()),
            declared_sparse_indexes: RwLock::new(HashSet::new()),
            declared_fulltext_indexes: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn index_count(&self) -> usize {
        self.indexes.len()
    }

    pub(crate) fn has_index(&self, name: &str) -> bool {
        self.indexes.has_index(name)
    }

    pub(crate) fn get_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.indexes.find_by_name(name)
    }

    pub(crate) fn get_indexes(&self) -> Vec<Arc<dyn BoundIndex>> {
        self.indexes.get_all()
    }

    pub(crate) fn add_index(&self, index: Arc<dyn BoundIndex>) -> Result<()> {
        self.indexes.add_index(index)
    }

    pub(crate) fn remove_index(&self, name: &str) -> Option<Arc<dyn BoundIndex>> {
        self.indexes.remove_index(name)
    }

    pub(crate) fn mark_declared_art_index(&self, tablet: &TabletRef, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_art_indexes.write() {
            guard.insert(column_id);
        }
        tablet.mark_declared_art_column(column_id);
    }

    pub(crate) fn unmark_declared_art_index(&self, tablet: &TabletRef, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_art_indexes.write() {
            guard.remove(&column_id);
        }
        tablet.unmark_declared_art_column(column_id);
    }

    pub(crate) fn mark_declared_vector_index(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_vector_indexes.write() {
            guard.insert(column_id);
        }
    }

    pub(crate) fn unmark_declared_vector_index(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_vector_indexes.write() {
            guard.remove(&column_id);
        }
    }

    pub(crate) fn mark_declared_sparse_index(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_sparse_indexes.write() {
            guard.insert(column_id);
        }
    }

    pub(crate) fn unmark_declared_sparse_index(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_sparse_indexes.write() {
            guard.remove(&column_id);
        }
    }

    pub(crate) fn mark_declared_fulltext_index(&self, column_id: ColumnId) {
        self.mark_declared_fulltext_index_with_config(column_id, "simple");
    }

    pub(crate) fn mark_declared_fulltext_index_with_config(
        &self,
        column_id: ColumnId,
        config: &str,
    ) {
        if let Ok(mut guard) = self.declared_fulltext_indexes.write() {
            guard.insert(column_id, config.to_string());
        }
    }

    pub(crate) fn unmark_declared_fulltext_index(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_fulltext_indexes.write() {
            guard.remove(&column_id);
        }
    }

    pub(crate) fn build_runtime_fulltext_index(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
    ) -> Result<()> {
        let config = self
            .declared_fulltext_indexes
            .read()
            .ok()
            .and_then(|guard| guard.get(&column_id).cloned())
            .unwrap_or_else(|| "simple".to_string());
        self.build_runtime_fulltext_index_with_config(tablet, column_id, &config)
    }

    pub(crate) fn build_runtime_fulltext_index_with_config(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
        config: &str,
    ) -> Result<()> {
        let visible = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible)?;
        let fulltext_columns = vec![(column_id, config.to_string())];

        for rowset in rowsets {
            Self::build_runtime_fulltext_indexes_for_rowset(&rowset, &fulltext_columns)?;
        }

        Ok(())
    }

    pub(crate) fn build_runtime_art_index(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
    ) -> Result<()> {
        let visible = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible)?;
        let art_columns = [column_id];

        for rowset in rowsets {
            Self::build_runtime_art_indexes_for_rowset(&rowset, &art_columns)?;
        }

        Ok(())
    }

    pub(crate) fn declared_art_columns(&self) -> Vec<ColumnId> {
        self.declared_art_indexes
            .read()
            .map(|guard| {
                let mut columns = guard.iter().copied().collect::<Vec<_>>();
                columns.sort_unstable();
                columns
            })
            .unwrap_or_default()
    }

    pub(crate) fn recovery_runtime_index_count(&self, tablet: &TabletRef) -> usize {
        let mut art_columns = self
            .declared_art_indexes
            .read()
            .map(|guard| guard.iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        let mut vector_columns = self
            .declared_vector_indexes
            .read()
            .map(|guard| guard.iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        let mut sparse_columns = self
            .declared_sparse_indexes
            .read()
            .map(|guard| guard.iter().copied().collect::<HashSet<_>>())
            .unwrap_or_default();
        let mut fulltext_columns = self
            .declared_fulltext_indexes
            .read()
            .map(|guard| guard.keys().copied().collect::<HashSet<_>>())
            .unwrap_or_default();

        let visible = tablet.max_version();
        if let Ok(rowsets) = tablet.capture_consistent_rowsets(visible) {
            for rowset in rowsets {
                if rowset.load().is_err() {
                    continue;
                }
                for segment in rowset.segments() {
                    for meta in segment.column_metas() {
                        let column_id = meta.column_id;
                        if segment.art_index(column_id).is_some() {
                            art_columns.insert(column_id);
                        }
                        if segment.hnsw_index(column_id).is_some() {
                            vector_columns.insert(column_id);
                        }
                        if segment.sparse_index(column_id).is_some() {
                            sparse_columns.insert(column_id);
                        }
                        if segment.fulltext_index(column_id).is_some() {
                            fulltext_columns.insert(column_id);
                        }
                    }
                }
            }
        }

        self.indexes
            .len()
            .saturating_add(art_columns.len())
            .saturating_add(vector_columns.len())
            .saturating_add(sparse_columns.len())
            .saturating_add(fulltext_columns.len())
    }

    pub(crate) fn declared_fulltext_columns_with_config(&self) -> Vec<(ColumnId, String)> {
        self.declared_fulltext_indexes
            .read()
            .map(|guard| {
                let mut columns: Vec<(ColumnId, String)> = guard
                    .iter()
                    .map(|(&column_id, cfg)| (column_id, cfg.clone()))
                    .collect();
                columns.sort_unstable_by_key(|(column_id, _)| *column_id);
                columns
            })
            .unwrap_or_default()
    }

    pub(crate) fn build_runtime_art_indexes_for_rowset(
        rowset: &RowsetSharedPtr,
        art_columns: &[ColumnId],
    ) -> Result<()> {
        if art_columns.is_empty() {
            return Ok(());
        }

        rowset.load()?;
        let buffer_manager: Arc<dyn BufferManager> = Arc::new(StandardBufferManager::default());
        for segment in rowset.segments() {
            for &column_id in art_columns {
                Self::build_runtime_art_index_for_segment_with_buffer(
                    &segment,
                    column_id,
                    Arc::clone(&buffer_manager),
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn build_runtime_fulltext_indexes_for_rowset(
        rowset: &RowsetSharedPtr,
        fulltext_columns: &[(ColumnId, String)],
    ) -> Result<()> {
        if fulltext_columns.is_empty() {
            return Ok(());
        }

        rowset.load()?;
        for segment in rowset.segments() {
            for (column_id, config) in fulltext_columns {
                Self::build_runtime_fulltext_index_for_segment(&segment, *column_id, config)?;
            }
        }
        Ok(())
    }

    fn build_runtime_fulltext_index_for_segment(
        segment: &SegmentSharedPtr,
        column_id: ColumnId,
        config: &str,
    ) -> Result<()> {
        if segment.fulltext_index(column_id).is_some() {
            return Ok(());
        }

        let mut iter = segment.new_column_iterator(column_id)?;
        let tokenizer_kind = TokenizerKind::from_config(config)?;
        let mut index =
            FullTextIndex::new_with_tokenizer_kind(tokenizer_kind, FullTextIndexConfig::default());

        for row_id in 0..segment.num_rows() {
            iter.seek_to_ordinal(row_id)?;
            let (_rowids, batch) = iter.next_batch(1)?;
            let Some(text_bytes) = batch.varlen_row(0)? else {
                continue;
            };
            let text = std::str::from_utf8(text_bytes.as_ref()).map_err(|err| {
                paro_error::data_corrupted(format!(
                    "FullText backfill decode error at column {} row {}: {}",
                    column_id, row_id, err
                ))
            })?;
            index.add_document(row_id as u32, text)?;
        }

        segment.register_runtime_fulltext_index(column_id, Arc::new(index));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn build_runtime_art_index_for_segment(
        segment: &SegmentSharedPtr,
        column_id: ColumnId,
    ) -> Result<()> {
        let buffer_manager: Arc<dyn BufferManager> = Arc::new(StandardBufferManager::default());
        Self::build_runtime_art_index_for_segment_with_buffer(segment, column_id, buffer_manager)
    }

    fn build_runtime_art_index_for_segment_with_buffer(
        segment: &SegmentSharedPtr,
        column_id: ColumnId,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> Result<()> {
        if segment.art_index(column_id).is_some() {
            return Ok(());
        }

        let schema_column = segment
            .schema()
            .column_by_id(column_id)
            .ok_or_else(|| paro_error::column_not_found(format!("column {}", column_id)))?;
        let logical_type = schema_column.logical_type.clone();
        if !is_runtime_art_supported_type(&logical_type) {
            return Err(paro_error::not_supported(format!(
                "ART runtime backfill does not support column {} type {:?}",
                column_id, logical_type
            )));
        }

        let mut iter = segment.new_column_iterator(column_id)?;
        let vector_allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let arena_allocator: Arc<dyn Allocator> = Arc::new(default_allocator());
        let mut arena = ArenaAllocator::new(arena_allocator);
        let mut art = ART::new(
            format!("art_segment_{}_col_{}", segment.segment_id(), column_id),
            IndexConstraintType::None,
            column_id,
            logical_type.clone(),
            buffer_manager,
        );
        let mut row_id_base = 0u64;

        loop {
            let (count, batch) = iter.next_batch(ART_BACKFILL_BATCH_SIZE)?;
            if count == 0 {
                break;
            }

            let vector = vector_decoder::decode_column_batch(
                &logical_type,
                &batch,
                count,
                Arc::clone(&vector_allocator),
                None,
            )?;

            for row_idx in 0..count {
                if vector.is_null(row_idx) {
                    continue;
                }
                let key = ARTKey::from_vector_value(&vector, row_idx, &logical_type, &mut arena)?;
                let row_id = row_id_base
                    .checked_add(row_idx as u64)
                    .ok_or_else(|| paro_error::data_corrupted("ART row id overflow"))?;
                match art.insert_key(&mut arena, &key, row_id as i64, IndexAppendMode::Default) {
                    ARTConflictType::NoConflict => {}
                    ARTConflictType::Constraint => {
                        return Err(paro_error::internal(format!(
                            "Duplicate key violation while building runtime ART for column {}",
                            column_id
                        )))
                    }
                    ARTConflictType::Transaction => {
                        return Err(paro_error::serialization_failure(
                            "Transaction conflict while building runtime ART index",
                        ))
                    }
                }
            }

            row_id_base = row_id_base
                .checked_add(count as u64)
                .ok_or_else(|| paro_error::data_corrupted("ART batch row count overflow"))?;
        }

        segment.register_runtime_art_index(column_id, Arc::new(art));
        Ok(())
    }

    pub(crate) fn hnsw_index_statistics(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
    ) -> Option<HnswIndexStatistics> {
        let visible = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible).ok()?;

        let mut agg: Option<HnswIndexStatistics> = None;
        for rowset in rowsets {
            if rowset.load().is_err() {
                continue;
            }
            for segment in rowset.segments() {
                let Some(stats) = segment.hnsw_index_statistics(column_id) else {
                    continue;
                };
                agg = Some(match agg {
                    None => stats.clone(),
                    Some(mut merged) => {
                        merged.num_indexed_vectors = merged
                            .num_indexed_vectors
                            .saturating_add(stats.num_indexed_vectors);
                        merged.dimension = merged.dimension.max(stats.dimension);
                        merged.max_level = merged.max_level.max(stats.max_level);
                        merged.m = merged.m.max(stats.m);
                        merged.ef_construction = merged.ef_construction.max(stats.ef_construction);
                        merged.graph_size_bytes = merged
                            .graph_size_bytes
                            .saturating_add(stats.graph_size_bytes);
                        merged.storage_size_bytes = merged
                            .storage_size_bytes
                            .saturating_add(stats.storage_size_bytes);
                        merged
                    }
                });
            }
        }

        agg
    }

    pub(crate) fn sparse_index_statistics(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
    ) -> Option<SparseIndexStatistics> {
        let visible = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible).ok()?;

        let mut agg: Option<SparseIndexStatistics> = None;
        for rowset in rowsets {
            if rowset.load().is_err() {
                continue;
            }
            for segment in rowset.segments() {
                let Some(stats) = segment.sparse_index_statistics(column_id) else {
                    continue;
                };
                agg = Some(match agg {
                    None => stats.clone(),
                    Some(mut merged) => {
                        merged.num_indexed_vectors = merged
                            .num_indexed_vectors
                            .saturating_add(stats.num_indexed_vectors);
                        merged.num_unique_dimensions = merged
                            .num_unique_dimensions
                            .max(stats.num_unique_dimensions);
                        merged.num_posting_lists = merged
                            .num_posting_lists
                            .saturating_add(stats.num_posting_lists);
                        merged.total_postings =
                            merged.total_postings.saturating_add(stats.total_postings);
                        merged
                    }
                });
            }
        }

        if let Some(mut merged) = agg {
            if merged.num_indexed_vectors == 0 {
                merged.avg_vector_nnz = 0.0;
            } else {
                merged.avg_vector_nnz =
                    merged.total_postings as f32 / merged.num_indexed_vectors as f32;
            }
            return Some(merged);
        }

        None
    }

    pub(crate) fn fulltext_index_statistics(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
    ) -> Option<FullTextIndexStatistics> {
        let visible = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible).ok()?;

        let mut agg: Option<FullTextIndexStatistics> = None;
        for rowset in rowsets {
            if let Ok(rowset_stats) = rowset.statistics() {
                if let Some(stats) = rowset_stats.fulltext_index(column_id) {
                    agg = merge_fulltext_index_stats(agg, stats.clone());
                    continue;
                }
            }

            if rowset.load().is_err() {
                continue;
            }

            for segment in rowset.segments() {
                let Some(stats) = segment.fulltext_index_statistics(column_id) else {
                    continue;
                };
                agg = merge_fulltext_index_stats(agg, stats);
            }
        }
        agg
    }

    pub(crate) fn has_vector_index(&self, tablet: &TabletRef, column_id: ColumnId) -> bool {
        let visible = tablet.max_version();
        if let Ok(rowsets) = tablet.capture_consistent_rowsets(visible) {
            for rowset in rowsets {
                if rowset.load().is_err() {
                    continue;
                }
                for segment in rowset.segments() {
                    if segment.hnsw_index(column_id).is_some() {
                        return true;
                    }
                }
            }
        }
        if self
            .declared_vector_indexes
            .read()
            .map(|s| s.contains(&column_id))
            .unwrap_or(false)
        {
            return true;
        }
        if let Some(schema) = tablet.schema() {
            if let Some(col) = schema.column_by_id(column_id) {
                return col.index_hnsw;
            }
        }
        false
    }

    pub(crate) fn has_fulltext_index(&self, tablet: &TabletRef, column_id: ColumnId) -> bool {
        if Self::has_fulltext_index_on_segments(tablet, column_id) {
            return true;
        }
        self.declared_fulltext_indexes
            .read()
            .map(|m| m.contains_key(&column_id))
            .unwrap_or(false)
    }

    pub(crate) fn has_fulltext_index_with_config(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
        config: &str,
    ) -> bool {
        let expected_kind = match TokenizerKind::from_config(config) {
            Ok(kind) => kind,
            Err(_) => return false,
        };
        if Self::has_fulltext_index_on_segments_with_kind(tablet, column_id, expected_kind) {
            return true;
        }
        self.declared_fulltext_indexes
            .read()
            .ok()
            .and_then(|m| m.get(&column_id).cloned())
            .map(|declared| declared.eq_ignore_ascii_case(config))
            .unwrap_or(false)
    }

    pub(crate) fn fulltext_index_coverage(
        &self,
        tablet: &TabletRef,
        column_id: ColumnId,
    ) -> Result<FullTextIndexCoverage> {
        let visible_version = tablet.max_version();
        let rowsets = tablet.capture_consistent_rowsets(visible_version)?;
        let mut visible_segment_count = 0usize;
        let mut indexed_segment_count = 0usize;

        for rowset in rowsets {
            rowset.load()?;
            for segment in rowset.segments() {
                visible_segment_count += 1;
                if segment.fulltext_index(column_id).is_some() {
                    indexed_segment_count += 1;
                }
            }
        }

        Ok(FullTextIndexCoverage {
            visible_version,
            visible_segment_count,
            indexed_segment_count,
        })
    }

    fn has_fulltext_index_on_segments(tablet: &TabletRef, column_id: ColumnId) -> bool {
        let visible = tablet.max_version();
        if let Ok(rowsets) = tablet.capture_consistent_rowsets(visible) {
            for rowset in rowsets {
                if rowset.load().is_err() {
                    continue;
                }
                for segment in rowset.segments() {
                    if segment.fulltext_index(column_id).is_some() {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn has_fulltext_index_on_segments_with_kind(
        tablet: &TabletRef,
        column_id: ColumnId,
        kind: TokenizerKind,
    ) -> bool {
        let visible = tablet.max_version();
        if let Ok(rowsets) = tablet.capture_consistent_rowsets(visible) {
            for rowset in rowsets {
                if rowset.load().is_err() {
                    continue;
                }
                for segment in rowset.segments() {
                    if let Some(index) = segment.fulltext_index(column_id) {
                        if index.tokenizer().kind() == kind {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    pub(crate) fn has_sparse_index(&self, tablet: &TabletRef, column_id: ColumnId) -> bool {
        let visible = tablet.max_version();
        if let Ok(rowsets) = tablet.capture_consistent_rowsets(visible) {
            for rowset in rowsets {
                if rowset.load().is_err() {
                    continue;
                }
                for segment in rowset.segments() {
                    if segment.sparse_index(column_id).is_some() {
                        return true;
                    }
                }
            }
        }
        self.declared_sparse_indexes
            .read()
            .map(|s| s.contains(&column_id))
            .unwrap_or(false)
    }
}
