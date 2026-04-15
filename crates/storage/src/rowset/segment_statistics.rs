// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment Statistics (Rowset)
//!
//! Segment-level column statistics collected during rowset writes.
//! This wraps per-column `ColumnStatistics` plus row/null counts.

use crate::statistics::ColumnStatistics;
use crate::tablet::ColumnId;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use std::io::{Read, Write};

/// Statistics for a single column within a segment.
#[derive(Debug, Clone)]
pub struct ColumnSegmentStatistics {
    /// Column ID
    pub column_id: ColumnId,
    /// Column-level statistics (min/max, distinct, etc.)
    pub stats: ColumnStatistics,
    /// Number of NULL values
    pub null_count: u64,
    /// Number of rows in this column
    pub num_rows: u64,
}

impl ColumnSegmentStatistics {
    /// Create column statistics for a segment column.
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

    /// Whether this column contains NULL values.
    pub fn has_nulls(&self) -> bool {
        self.null_count > 0
    }

    /// Serialize column statistics to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.column_id.to_le_bytes())?;
        w.write_all(&self.num_rows.to_le_bytes())?;
        w.write_all(&self.null_count.to_le_bytes())?;

        let mut type_buf = Vec::new();
        self.stats
            .statistics()
            .get_type()
            .serialize(&mut type_buf)?;
        w.write_all(&(type_buf.len() as u32).to_le_bytes())?;
        w.write_all(&type_buf)?;

        self.stats.serialize(w)?;
        Ok(())
    }

    /// Deserialize column statistics from a reader.
    pub fn deserialize<R: Read>(r: &mut R) -> Result<Self> {
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        r.read_exact(&mut buf4)?;
        let column_id = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf8)?;
        let num_rows = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let null_count = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf4)?;
        let type_len = u32::from_le_bytes(buf4) as usize;
        let mut type_bytes = vec![0u8; type_len];
        r.read_exact(&mut type_bytes)?;
        let mut type_cursor = std::io::Cursor::new(type_bytes);
        let logical_type = LogicalType::deserialize(&mut type_cursor)?;

        let stats = ColumnStatistics::deserialize(r, logical_type)?;

        Ok(Self {
            column_id,
            stats,
            null_count,
            num_rows,
        })
    }
}

/// Segment-level statistics (per-column).
#[derive(Debug, Clone, Default)]
pub struct SegmentStatistics {
    /// Number of rows in this segment
    pub num_rows: u64,
    /// Per-column statistics
    pub columns: Vec<ColumnSegmentStatistics>,
}

impl SegmentStatistics {
    /// Create empty statistics for a segment.
    pub fn new(num_rows: u64) -> Self {
        Self {
            num_rows,
            columns: Vec::new(),
        }
    }

    /// Add column statistics.
    pub fn add_column(&mut self, stats: ColumnSegmentStatistics) {
        self.columns.push(stats);
    }

    /// Get statistics for a specific column ID.
    pub fn column(&self, column_id: ColumnId) -> Option<&ColumnSegmentStatistics> {
        self.columns.iter().find(|c| c.column_id == column_id)
    }

    /// Build segment statistics from column metadata collected in a footer.
    pub fn from_column_metas(
        column_metas: &[crate::rowset::segment::ColumnMeta],
        num_rows: u64,
    ) -> Option<Self> {
        let mut stats = Self::new(num_rows);
        for meta in column_metas {
            if let (Some(col_stats), Some(null_count)) = (&meta.column_stats, meta.null_count) {
                stats.add_column(ColumnSegmentStatistics::new(
                    meta.column_id,
                    col_stats.clone(),
                    null_count,
                    meta.num_rows,
                ));
            }
        }
        if stats.columns().is_empty() {
            None
        } else {
            Some(stats)
        }
    }

    /// Iterate over all column statistics.
    pub fn columns(&self) -> &[ColumnSegmentStatistics] {
        &self.columns
    }

    /// Serialize segment statistics to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.num_rows.to_le_bytes())?;
        w.write_all(&(self.columns.len() as u32).to_le_bytes())?;
        for col in &self.columns {
            col.serialize(w)?;
        }
        Ok(())
    }

    /// Deserialize segment statistics from a reader.
    pub fn deserialize<R: Read>(r: &mut R) -> Result<Self> {
        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];

        r.read_exact(&mut buf8)?;
        let num_rows = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf4)?;
        let count = u32::from_le_bytes(buf4) as usize;

        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(ColumnSegmentStatistics::deserialize(r)?);
        }

        Ok(Self { num_rows, columns })
    }

    /// Serialize to a byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut buf)?;
        Ok(buf)
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor)
    }
}
