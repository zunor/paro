//! # Index Statistics
//!
//! Index-level statistics for segment metadata.

use crate::tablet::ColumnId;

/// Supported index types for statistics reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    ZoneMap,
    Bloom,
    Bitmap,
    ShortKey,
    HNSW,
    Sparse,
    FullText,
    ART,
}

/// Statistics for a single index.
#[derive(Debug, Clone, Copy)]
pub struct IndexStatistics {
    /// Index type
    pub index_type: IndexType,
    /// Index size in bytes
    pub index_size_bytes: u64,
    /// Number of entries in the index
    pub entry_count: u64,
}

impl IndexStatistics {
    pub fn new(index_type: IndexType, index_size_bytes: u64, entry_count: u64) -> Self {
        Self {
            index_type,
            index_size_bytes,
            entry_count,
        }
    }
}

/// Segment-level index statistics grouped by column.
#[derive(Debug, Clone, Default)]
pub struct SegmentIndexStatistics {
    /// Per-column index statistics
    pub columns: Vec<(ColumnId, Vec<IndexStatistics>)>,
}

impl SegmentIndexStatistics {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column_id: ColumnId, indexes: Vec<IndexStatistics>) {
        if indexes.is_empty() {
            return;
        }
        self.columns.push((column_id, indexes));
    }

    pub fn column(&self, column_id: ColumnId) -> Option<&[IndexStatistics]> {
        self.columns
            .iter()
            .find(|(id, _)| *id == column_id)
            .map(|(_, stats)| stats.as_slice())
    }
}
