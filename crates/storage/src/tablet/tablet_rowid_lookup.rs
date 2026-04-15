// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::tablet_reader::TabletReader;
use crate::rowid_resolver;
use crate::tablet::ColumnId;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

impl TabletReader {
    /// Bulk read by tablet RowIDs. Handles cross-segment routing.
    pub fn get_by_rowids(&self, rowids: &[u64], column_ids: &[ColumnId]) -> Result<Chunk> {
        self.get_by_rowids_internal(rowids, column_ids, 0)
    }

    pub(super) fn get_by_rowids_internal(
        &self,
        rowids: &[u64],
        column_ids: &[ColumnId],
        depth: usize,
    ) -> Result<Chunk> {
        if !self.is_prepared {
            return Err(paro_error::internal("TabletReader not prepared"));
        }

        let col_types: Vec<LogicalType> = column_ids
            .iter()
            .map(|&cid| {
                self.schema
                    .column_by_id(cid)
                    .map(|column| column.logical_type.clone())
                    .ok_or_else(|| {
                        paro_error::invalid_input(format!("Column ID {} not found in schema", cid))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        rowid_resolver::read_chunk_by_rowids_recursive(
            &self.tablet,
            column_ids,
            &col_types,
            rowids,
            self.allocator.clone(),
            depth,
            &|rowset_id| {
                if let Some(rowset) = self.rowsets.iter().find(|r| r.rowset_id() == rowset_id) {
                    return Ok(rowset.clone());
                }
                self.tablet.find_rowset_by_id(rowset_id).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Rowset {} not found while resolving row ids",
                        rowset_id
                    ))
                })
            },
        )
    }
}
