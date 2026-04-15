// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statistics merge helpers for compaction.

use crate::rowset::{RowsetSharedPtr, RowsetStatistics};
use paro_common::error::Result;

/// Merge rowset-level statistics from a list of input rowsets.
///
/// This is safe for compaction paths that preserve all rows (e.g. duplicate keys).
pub fn merge_rowset_statistics(rowsets: &[RowsetSharedPtr]) -> Result<RowsetStatistics> {
    let mut merged = RowsetStatistics::default();
    for rowset in rowsets {
        let stats = rowset.statistics()?;
        merged.merge(&stats);
    }
    Ok(merged)
}
