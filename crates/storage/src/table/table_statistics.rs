// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::statistics::{
    ColumnStatistics, FullTextIndexStatistics, HnswIndexStatistics, SparseIndexStatistics,
};
use crate::tablet::ColumnId;

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

impl TableHandle {
    pub fn column_statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
        let stats = self.tablet().statistics().ok()?;
        let col_stats = stats.column(column_index as u32)?;
        Some(Self::visible_column_statistics(
            &col_stats.stats,
            col_stats.null_count,
            col_stats.num_rows,
        ))
    }

    pub fn all_column_statistics(&self) -> Vec<ColumnStatistics> {
        let stats = self.tablet().statistics().ok();
        let mut result = Vec::with_capacity(self.types().len());
        for (idx, ty) in self.types().iter().enumerate() {
            let entry = stats
                .as_ref()
                .and_then(|s| s.column(idx as u32))
                .map(|col_stats| {
                    Self::visible_column_statistics(
                        &col_stats.stats,
                        col_stats.null_count,
                        col_stats.num_rows,
                    )
                })
                .unwrap_or_else(|| {
                    ColumnStatistics::new(crate::statistics::BaseStatistics::create_empty(
                        ty.clone(),
                    ))
                });
            result.push(entry);
        }
        result
    }

    /// Aggregate HNSW index statistics across visible rowsets.
    pub fn hnsw_index_statistics(&self, column_id: ColumnId) -> Option<HnswIndexStatistics> {
        let visible = self.max_version();
        let rowsets = self.tablet().capture_consistent_rowsets(visible).ok()?;

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
                        merged.total_graph_links = merged
                            .total_graph_links
                            .saturating_add(stats.total_graph_links);
                        merged.level0_graph_links = merged
                            .level0_graph_links
                            .saturating_add(stats.level0_graph_links);
                        merged.max_level0_degree =
                            merged.max_level0_degree.max(stats.max_level0_degree);
                        merged.avg_level0_degree = if merged.num_indexed_vectors == 0 {
                            0.0
                        } else {
                            merged.level0_graph_links as f32 / merged.num_indexed_vectors as f32
                        };
                        merged
                    }
                });
            }
        }

        agg
    }

    /// Aggregate sparse vector index statistics across visible rowsets.
    pub fn sparse_index_statistics(&self, column_id: ColumnId) -> Option<SparseIndexStatistics> {
        let visible = self.max_version();
        let rowsets = self.tablet().capture_consistent_rowsets(visible).ok()?;

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

        agg.map(|mut merged| {
            merged.avg_vector_nnz = if merged.num_indexed_vectors == 0 {
                0.0
            } else {
                merged.total_postings as f32 / merged.num_indexed_vectors as f32
            };
            merged
        })
    }

    /// Aggregate full-text index statistics across visible rowsets.
    pub fn fulltext_index_statistics(
        &self,
        column_id: ColumnId,
    ) -> Option<FullTextIndexStatistics> {
        let visible = self.max_version();
        let rowsets = self.tablet().capture_consistent_rowsets(visible).ok()?;

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

    fn visible_column_statistics(
        column_stats: &ColumnStatistics,
        null_count: u64,
        num_rows: u64,
    ) -> ColumnStatistics {
        let mut stats = column_stats.copy();
        if null_count > 0 {
            stats.statistics_mut().set_has_null_fast();
        }
        if num_rows > null_count {
            stats.statistics_mut().set_has_no_null_fast();
        }
        stats
    }

    /// Number of rows currently marked deleted in tablet statistics.
    pub fn deleted_row_count(&self) -> usize {
        self.tablet()
            .statistics()
            .map(|s| s.delete_stats.num_deleted_rows as usize)
            .unwrap_or(0)
    }
}
