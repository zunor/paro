// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Rowset Statistics
//!
//! Aggregated statistics across all segments in a rowset.

use crate::rowset::segment_statistics::{ColumnSegmentStatistics, SegmentStatistics};
use crate::rowset::SegmentSharedPtr;
use crate::statistics::{ColumnStatistics, DeleteStatistics, FullTextIndexStatistics};
use crate::tablet::ColumnId;
use std::collections::HashMap;

/// Aggregated statistics for a column across a rowset.
#[derive(Debug, Clone)]
pub struct RowsetColumnStatistics {
    /// Column ID
    pub column_id: ColumnId,
    /// Column-level statistics (min/max, distinct, etc.)
    pub stats: ColumnStatistics,
    /// Total NULL count across segments
    pub null_count: u64,
    /// Total rows across segments
    pub num_rows: u64,
}

impl RowsetColumnStatistics {
    pub fn new(
        column_id: ColumnId,
        stats: ColumnStatistics,
        null_count: u64,
        num_rows: u64,
    ) -> Self {
        Self {
            column_id,
            stats,
            null_count,
            num_rows,
        }
    }

    pub fn has_nulls(&self) -> bool {
        self.null_count > 0
    }
}

/// Rowset-level statistics aggregated from segments.
#[derive(Debug, Clone, Default)]
pub struct RowsetStatistics {
    /// Total rows in the rowset
    pub num_rows: u64,
    /// Number of segments
    pub num_segments: u32,
    /// Delete statistics (for primary key tables)
    pub delete_stats: DeleteStatistics,
    /// Per-column aggregated statistics
    pub columns: Vec<RowsetColumnStatistics>,
    /// Per-column full-text global statistics metadata.
    pub fulltext_indexes: HashMap<ColumnId, FullTextIndexStatistics>,
}

impl RowsetStatistics {
    /// Create empty statistics for a rowset.
    pub fn new(num_rows: u64, num_segments: u32) -> Self {
        Self {
            num_rows,
            num_segments,
            delete_stats: DeleteStatistics::from_counts(num_rows, 0),
            columns: Vec::new(),
            fulltext_indexes: HashMap::new(),
        }
    }

    /// Add statistics from a single segment.
    pub fn add_segment_stats(&mut self, segment_stats: &SegmentStatistics) {
        let mut index = HashMap::<ColumnId, usize>::new();
        for (idx, col) in self.columns.iter().enumerate() {
            index.insert(col.column_id, idx);
        }

        for col_stats in segment_stats.columns() {
            match index.get(&col_stats.column_id) {
                Some(&idx) => {
                    let entry = &mut self.columns[idx];
                    entry.stats.merge(&col_stats.stats);
                    entry.null_count += col_stats.null_count;
                    entry.num_rows += col_stats.num_rows;
                }
                None => {
                    self.columns.push(RowsetColumnStatistics::new(
                        col_stats.column_id,
                        col_stats.stats.clone(),
                        col_stats.null_count,
                        col_stats.num_rows,
                    ));
                    index.insert(col_stats.column_id, self.columns.len() - 1);
                }
            }
        }
    }

    /// Merge another rowset statistics into this one.
    pub fn merge(&mut self, other: &RowsetStatistics) {
        let total_rows = self.num_rows + other.num_rows;
        let total_deleted =
            self.delete_stats.num_deleted_rows + other.delete_stats.num_deleted_rows;
        self.num_rows = total_rows;
        self.num_segments += other.num_segments;
        self.delete_stats = DeleteStatistics::from_counts(total_rows, total_deleted);

        let mut index = HashMap::<ColumnId, usize>::new();
        for (idx, col) in self.columns.iter().enumerate() {
            index.insert(col.column_id, idx);
        }

        for col_stats in other.columns.iter() {
            match index.get(&col_stats.column_id) {
                Some(&idx) => {
                    let entry = &mut self.columns[idx];
                    entry.stats.merge(&col_stats.stats);
                    entry.null_count += col_stats.null_count;
                    entry.num_rows += col_stats.num_rows;
                }
                None => {
                    self.columns.push(col_stats.clone());
                    index.insert(col_stats.column_id, self.columns.len() - 1);
                }
            }
        }

        for (column_id, stats) in &other.fulltext_indexes {
            merge_fulltext_stats_into(&mut self.fulltext_indexes, *column_id, stats.clone());
        }
    }

    /// Aggregate statistics from a set of segments.
    pub fn from_segments(segments: &[SegmentSharedPtr]) -> Self {
        let mut num_rows = 0u64;
        let mut num_segments = 0u32;
        let mut columns_map: HashMap<ColumnId, RowsetColumnStatistics> = HashMap::new();
        let mut fulltext_indexes: HashMap<ColumnId, FullTextIndexStatistics> = HashMap::new();

        for segment in segments {
            num_segments += 1;
            num_rows += segment.num_rows();

            let Some(stats) = segment.statistics() else {
                continue;
            };

            for col in stats.columns() {
                let entry = columns_map.entry(col.column_id).or_insert_with(|| {
                    RowsetColumnStatistics::new(col.column_id, col.stats.clone(), 0, 0)
                });
                entry.stats.merge(&col.stats);
                entry.null_count += col.null_count;
                entry.num_rows += col.num_rows;
            }

            for meta in segment.column_metas() {
                let Some(stats) = segment.fulltext_index_statistics(meta.column_id) else {
                    continue;
                };
                merge_fulltext_stats_into(&mut fulltext_indexes, meta.column_id, stats);
            }
        }

        let mut columns: Vec<RowsetColumnStatistics> = columns_map.into_values().collect();
        columns.sort_by_key(|c| c.column_id);

        Self {
            num_rows,
            num_segments,
            delete_stats: DeleteStatistics::from_counts(num_rows, 0),
            columns,
            fulltext_indexes,
        }
    }

    /// Get statistics for a specific column ID.
    pub fn column(&self, column_id: ColumnId) -> Option<&RowsetColumnStatistics> {
        self.columns.iter().find(|c| c.column_id == column_id)
    }

    /// Iterate over all column statistics.
    pub fn columns(&self) -> &[RowsetColumnStatistics] {
        &self.columns
    }

    /// Get full-text statistics metadata for a specific indexed column.
    pub fn fulltext_index(&self, column_id: ColumnId) -> Option<&FullTextIndexStatistics> {
        self.fulltext_indexes.get(&column_id)
    }

    /// Update delete statistics for this rowset.
    pub fn set_delete_stats(&mut self, delete_stats: DeleteStatistics) {
        self.delete_stats = delete_stats;
    }
}

impl From<ColumnSegmentStatistics> for RowsetColumnStatistics {
    fn from(stats: ColumnSegmentStatistics) -> Self {
        RowsetColumnStatistics {
            column_id: stats.column_id,
            stats: stats.stats,
            null_count: stats.null_count,
            num_rows: stats.num_rows,
        }
    }
}

fn merge_fulltext_stats_into(
    target: &mut HashMap<ColumnId, FullTextIndexStatistics>,
    column_id: ColumnId,
    incoming: FullTextIndexStatistics,
) {
    use std::collections::hash_map::Entry;

    match target.entry(column_id) {
        Entry::Vacant(entry) => {
            entry.insert(incoming);
        }
        Entry::Occupied(mut entry) => {
            let merged = entry.get_mut();
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::tokenizer::TokenizerKind;

    fn make_ft_stats(total_docs: u32, total_terms: u64) -> FullTextIndexStatistics {
        FullTextIndexStatistics {
            total_docs,
            total_terms,
            avg_doc_length: if total_docs == 0 {
                0.0
            } else {
                total_terms as f32 / total_docs as f32
            },
            unique_terms: 10,
            total_postings: 20,
            max_posting_list_len: 4,
            min_posting_list_len: 1,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer_kind: TokenizerKind::Default,
        }
    }

    #[test]
    fn rowset_statistics_merge_fulltext_metadata() {
        let mut left = RowsetStatistics::new(0, 0);
        left.fulltext_indexes.insert(1, make_ft_stats(2, 5));

        let mut right = RowsetStatistics::new(0, 0);
        right.fulltext_indexes.insert(1, make_ft_stats(3, 9));

        left.merge(&right);
        let merged = left.fulltext_index(1).expect("merged fulltext stats");
        assert_eq!(merged.total_docs, 5);
        assert_eq!(merged.total_terms, 14);
        assert!((merged.avg_doc_length - 2.8).abs() < 1e-6);
    }
}
