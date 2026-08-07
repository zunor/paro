// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Read-only probes between serialized aggregate hash-table rows.
//!
//! Parallel DISTINCT finalization keeps worker-local key tables immutable and
//! probes them directly for the uncommon cross-worker duplicate. This avoids
//! copying every unique tuple into another hash table merely to obtain one
//! globally unique stream.

use super::*;

/// A validated lookup from rows in `source` into `target`.
///
/// Construction verifies the complete tuple layout once. Subsequent probes
/// can therefore compare serialized rows without repeating schema checks or
/// exposing raw row pointers to callers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SerializedGroupLookup<'a> {
    target: &'a GroupedAggregateHashTable,
    source: &'a GroupedAggregateHashTable,
}

impl<'a> SerializedGroupLookup<'a> {
    pub(crate) fn try_new(
        target: &'a GroupedAggregateHashTable,
        source: &'a GroupedAggregateHashTable,
    ) -> Result<Self> {
        target.ensure_compatible(source)?;
        target.ensure_lookup_storage_available()?;
        Ok(Self { target, source })
    }

    /// Return whether the selected source row already exists in the target.
    pub(crate) fn contains(&self, source_row_idx: usize) -> Result<bool> {
        if source_row_idx >= self.source.count {
            return Err(paro_error::internal(format!(
                "Serialized source row out of bounds during lookup: row={source_row_idx}, count={}",
                self.source.count
            )));
        }
        let source_row = self.source.row_ptr(source_row_idx);
        let hash = self.source.layout.load_hash(source_row);
        let inline_key = self
            .target
            .inline_key_layout
            .as_ref()
            .map(|layout| unsafe { layout.encode_serialized_row(&self.source.layout, source_row) })
            .transpose()?;
        let inline_keys = match (inline_key, self.target.inline_keys.as_ref()) {
            (Some(_), Some(keys)) if keys.len() == self.target.capacity => Some(keys),
            (None, None) => None,
            _ => {
                return Err(paro_error::internal(
                    "Aggregate inline-key lookup storage is inconsistent",
                ));
            }
        };

        let mut slot = self.target.slot_for_hash(hash);
        loop {
            let entry = self.target.entries[slot];
            if !entry.is_occupied() {
                return Ok(false);
            }
            let keys_match = if let Some(inline_key) = inline_key {
                let target_key = inline_keys.ok_or_else(|| {
                    paro_error::internal("Aggregate inline-key lookup storage disappeared")
                })?[slot];
                entry.matches_hash(hash) && target_key == inline_key
            } else {
                entry.matches_hash(hash)
                    && unsafe {
                        self.target.layout.compare_serialized_groups(
                            self.target.row_ptr(entry.row_idx()),
                            &self.target.varlen_heap,
                            source_row,
                            &self.source.varlen_heap,
                        )?
                    }
            };
            if keys_match {
                return Ok(true);
            }
            slot = (slot + 1) & self.target.bitmask;
        }
    }
}

impl GroupedAggregateHashTable {
    /// Read the full-key hash stored alongside a serialized group row.
    pub(crate) fn serialized_group_hash(&self, row_idx: usize) -> Result<u64> {
        if row_idx >= self.count {
            return Err(paro_error::internal(format!(
                "Serialized group hash row out of bounds: row={row_idx}, count={}",
                self.count
            )));
        }
        Ok(self.layout.load_hash(self.row_ptr(row_idx)))
    }
}
