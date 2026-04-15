// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::primary_key::{DeleteVector, RowID};
use crate::table::table_handle::TableHandle;
use crate::tablet::{KeysType, PhysicalRowRef};
use crate::transaction::txn::Transaction;
use crate::wal::wal_entry::WalEntry;
use crate::wal::wal_type::WalType;
use crate::wal::wal_writer::{WalInitState, WalWriter};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

pub(crate) fn delete(
    table: &TableHandle,
    row_ids: &[u64],
    txn: Option<Arc<Transaction>>,
) -> Result<usize> {
    if row_ids.is_empty() {
        return Ok(0);
    }

    let tablet = table.tablet();
    let mut dedup = HashSet::with_capacity(row_ids.len());
    let mut locations = Vec::with_capacity(row_ids.len());
    for raw in row_ids {
        let row_id = RowID::from_raw(*raw);
        let loc = tablet.decode_row_id(row_id)?;
        if dedup.insert(loc) {
            locations.push(loc);
        }
    }

    if locations.is_empty() {
        return Ok(0);
    }

    let mut segment_row_count = HashMap::new();
    for location in &locations {
        let key = (location.rowset_id, location.segment_id);
        let rows_in_segment = if let Some(rows) = segment_row_count.get(&key) {
            *rows
        } else {
            let rowset = tablet
                .find_rowset_by_id(location.rowset_id)
                .ok_or_else(|| {
                    paro_error::invalid_input(format!("Rowset {} not found", location.rowset_id))
                })?;
            rowset.load()?;
            let segment = rowset.get_segment(location.segment_id).ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "Segment {} not found in rowset {}",
                    location.segment_id, location.rowset_id
                ))
            })?;
            let rows = segment.num_rows() as u32;
            segment_row_count.insert(key, rows);
            rows
        };
        if location.row_offset >= rows_in_segment {
            return Err(paro_error::invalid_input(format!(
                "Row offset {} out of range for rowset {} segment {} (rows={})",
                location.row_offset, location.rowset_id, location.segment_id, rows_in_segment
            )));
        }
    }

    let encoded_locations: Vec<_> = locations
        .iter()
        .map(|loc| (loc.rowset_id, loc.segment_id, loc.row_offset))
        .collect();
    let deleted = locations.len();

    if let Some(txn) = txn {
        txn.add_pending_row_id_delete(tablet, locations)?;
        return Ok(deleted);
    }

    tablet.apply_row_id_delete_refs(&locations)?;

    let wal = WalWriter::new(
        tablet.data_dir().join("tablet.wal"),
        WalInitState::Uninitialized,
    );
    let entry = WalEntry::RowIdDelete {
        locations: encoded_locations,
    };
    wal.write_entry(WalType::RowIdDelete, &entry.serialize_data())?;
    wal.flush()?;

    Ok(deleted)
}

pub(crate) fn delete_all(table: &TableHandle, txn: Option<Arc<Transaction>>) -> Result<usize> {
    let tablet = table.tablet();
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    if schema.keys_type() == KeysType::PrimaryKeys {
        let entries = tablet.snapshot_primary_index_entries()?;
        if entries.is_empty() {
            return Ok(0);
        }
        let removed = entries.len();
        let keys: Vec<Vec<u8>> = entries.into_iter().map(|(key, _)| key).collect();
        if let Some(txn) = txn {
            txn.add_pending_primary_delete(tablet, keys)?;
        } else {
            tablet.apply_primary_delete(keys)?;
        }
        return Ok(removed);
    }

    let visible_version = tablet.max_version();
    let rowsets = tablet.capture_consistent_rowsets(visible_version)?;
    let mut locations = Vec::new();

    for rowset in rowsets {
        rowset.load()?;
        let rowset_id = rowset.rowset_id();
        for segment in rowset.segments() {
            let segment_id = segment.segment_id();
            let num_rows = segment.num_rows() as u32;
            if num_rows == 0 {
                continue;
            }

            let existing = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                segment_id,
                visible_version,
            )?;
            if let Some(delete_vector) = existing {
                for row_id in 0..num_rows {
                    if !delete_vector.is_deleted(row_id) {
                        locations.push(PhysicalRowRef::new(rowset_id, segment_id, row_id));
                    }
                }
            } else {
                for row_id in 0..num_rows {
                    locations.push(PhysicalRowRef::new(rowset_id, segment_id, row_id));
                }
            }
        }
    }

    if locations.is_empty() {
        return Ok(0);
    }

    let deleted = locations.len();
    if let Some(txn) = txn {
        txn.add_pending_row_id_delete(tablet.clone(), locations)?;
        return Ok(deleted);
    }

    tablet.apply_row_id_delete_refs(&locations)?;
    let wal_locations: Vec<_> = locations.into_iter().map(Into::into).collect();
    let wal = WalWriter::new(
        tablet.data_dir().join("tablet.wal"),
        WalInitState::Uninitialized,
    );
    let entry = WalEntry::RowIdDelete {
        locations: wal_locations,
    };
    wal.write_entry(WalType::RowIdDelete, &entry.serialize_data())?;
    wal.flush()?;

    Ok(deleted)
}

pub(crate) fn delete_by_primary_keys(
    table: &TableHandle,
    keys: &Chunk,
    txn: Option<Arc<Transaction>>,
) -> Result<usize> {
    let tablet = table.tablet();
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    if schema.keys_type() != KeysType::PrimaryKeys {
        return Err(paro_error::invalid_input(
            "delete_by_primary_keys requires PRIMARY_KEYS tablet",
        ));
    }

    let serializer = crate::primary_key::PrimaryKeySerializer::from_schema_ref(&schema)?;
    let mut removed = 0usize;
    let mut removed_keys = Vec::new();
    for row in 0..keys.size() {
        let key = serializer.encode_row(keys, row)?;
        if tablet.lookup_primary_key(&key)?.is_some() {
            removed += 1;
            removed_keys.push(key);
        }
    }

    if removed == 0 {
        return Ok(0);
    }

    if let Some(txn) = txn {
        txn.add_pending_primary_delete(tablet, removed_keys)?;
        return Ok(removed);
    }

    tablet.apply_primary_delete(removed_keys)?;
    Ok(removed)
}
