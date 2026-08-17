// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use super::MutationTarget;
use crate::primary_key::{primary_key_hash, DeleteVector, PrimaryKeySerializer, RowID};
use crate::rowset::SegmentRowId;
use crate::table::table_handle::TableHandle;
use crate::tablet::{KeysType, PhysicalRowRef, TabletReaderParams};
use crate::transaction::overlay_reader::TxnOverlayReader;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_transaction::{CommitTs, TableId, TransactionView};

pub(crate) fn delete(
    view: &TransactionView,
    table: &TableHandle,
    row_ids: &[u64],
    target: MutationTarget,
) -> Result<usize> {
    if row_ids.is_empty() {
        return Ok(0);
    }

    let table_id = storage_table_id(table);
    let read_version = view.visible_version_i64();
    let tablet = table.tablet();
    let mut dedup = HashSet::with_capacity(row_ids.len());
    let mut locations = Vec::with_capacity(row_ids.len());
    let mut segment_row_count = HashMap::new();

    view.read_tracker()
        .record_row_reads(table_id, row_ids.iter().copied());
    for raw in row_ids {
        if !dedup.insert(*raw) {
            continue;
        }

        let row_id = RowID::from_raw(*raw);
        let location = tablet.decode_row_id(row_id)?;
        let rowset = tablet
            .find_rowset_by_id(location.rowset_id)
            .ok_or_else(|| {
                paro_error::invalid_input(format!("Rowset {} not found", location.rowset_id))
            })?;
        if !rowset.is_visible() || rowset.end_version() > read_version {
            continue;
        }

        let key = (location.rowset_id, location.segment_id);
        let rows_in_segment = if let Some(rows) = segment_row_count.get(&key) {
            *rows
        } else {
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
        if location.row_offset.get() >= rows_in_segment {
            return Err(paro_error::invalid_input(format!(
                "Row offset {} out of range for rowset {} segment {} (rows={})",
                location.row_offset, location.rowset_id, location.segment_id, rows_in_segment
            )));
        }

        let existing = DeleteVector::load_from_dir_at_version(
            rowset.rowset_path(),
            location.segment_id,
            read_version,
        )?;
        if existing
            .as_ref()
            .is_some_and(|delete_vector| delete_vector.is_deleted(location.row_offset.get()))
        {
            continue;
        }

        locations.push(location);
    }

    if locations.is_empty() {
        return Ok(0);
    }

    let deleted = locations.len();

    if let MutationTarget::Transaction(txn) = target {
        txn.add_pending_row_id_delete(view.command_id(), tablet, locations)?;
        return Ok(deleted);
    }

    tablet.apply_row_id_delete_refs(&locations, immediate_commit_ts(view)?)?;

    Ok(deleted)
}

pub(crate) fn delete_all(
    view: &TransactionView,
    table: &TableHandle,
    target: MutationTarget,
) -> Result<usize> {
    let table_id = storage_table_id(table);
    view.read_tracker().record_predicate(table_id, 0);

    let tablet = table.tablet();
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    if schema.keys_type() == KeysType::PrimaryKeys {
        let rows = visible_primary_key_rows(view, table, None)?;
        if rows.is_empty() {
            return Ok(0);
        }
        let removed = rows.len();
        let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
        if let MutationTarget::Transaction(txn) = target {
            let locations = rows.iter().map(|row| row.location).collect();
            txn.add_pending_primary_delete(view.command_id(), tablet, keys, locations)?;
        } else {
            tablet.apply_primary_delete_at_version(keys, immediate_commit_ts(view)?)?;
        }
        return Ok(removed);
    }

    let visible_version = view.visible_version_i64();
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
                        locations.push(PhysicalRowRef::new(
                            rowset_id,
                            segment_id,
                            SegmentRowId::from_raw(row_id),
                        ));
                    }
                }
            } else {
                for row_id in 0..num_rows {
                    locations.push(PhysicalRowRef::new(
                        rowset_id,
                        segment_id,
                        SegmentRowId::from_raw(row_id),
                    ));
                }
            }
        }
    }

    if locations.is_empty() {
        return Ok(0);
    }

    let deleted = locations.len();
    if let MutationTarget::Transaction(txn) = target {
        txn.add_pending_row_id_delete(view.command_id(), tablet.clone(), locations)?;
        return Ok(deleted);
    }

    tablet.apply_row_id_delete_refs(&locations, immediate_commit_ts(view)?)?;

    Ok(deleted)
}

pub(crate) fn delete_by_primary_keys(
    view: &TransactionView,
    table: &TableHandle,
    keys: &Chunk,
    target: MutationTarget,
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

    let serializer = PrimaryKeySerializer::from_schema_ref(&schema)?;
    let requested_keys = serializer.encode_chunk(keys)?;
    if requested_keys.is_empty() {
        return Ok(0);
    }

    let target_set: HashSet<Vec<u8>> = requested_keys.into_iter().collect();
    let rows = visible_primary_key_rows(view, table, Some(&target_set))?;
    if rows.is_empty() {
        return Ok(0);
    }

    let removed = rows.len();
    let removed_keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
    if let MutationTarget::Transaction(txn) = target {
        let locations = rows.iter().map(|row| row.location).collect();
        txn.add_pending_primary_delete(view.command_id(), tablet, removed_keys, locations)?;
        return Ok(removed);
    }

    tablet.apply_primary_delete_at_version(removed_keys, immediate_commit_ts(view)?)?;
    Ok(removed)
}

pub(crate) fn visible_primary_key_row_ids(
    view: &TransactionView,
    table: &TableHandle,
    encoded_keys: &[Vec<u8>],
) -> Result<HashMap<Vec<u8>, u64>> {
    if encoded_keys.is_empty() {
        return Ok(HashMap::new());
    }

    let table_id = storage_table_id(table);
    view.read_tracker().record_key_ranges(
        table_id,
        encoded_keys.iter().map(|key| {
            let key_hash = primary_key_hash(key);
            (key_hash, key_hash)
        }),
    );

    let tablet = table.tablet();
    let mut resolved = HashMap::new();
    let mut shadowed = HashSet::new();
    if let Some(overlay) = TxnOverlayReader::for_tablet(&tablet, view)? {
        for key in encoded_keys {
            if let Some(entry) = overlay.primary_key_entry(key) {
                shadowed.insert(key.clone());
                if let Some(row_id) = entry.row_id {
                    resolved.insert(key.clone(), u64::from(row_id));
                }
            }
        }
    }

    let target_set: HashSet<Vec<u8>> = encoded_keys
        .iter()
        .filter(|key| !shadowed.contains(*key))
        .cloned()
        .collect();
    if target_set.is_empty() {
        return Ok(resolved);
    }

    let rows = visible_primary_key_rows_without_read_tracking(view, table, Some(&target_set))?;
    resolved.extend(rows.into_iter().map(|row| (row.key, row.row_id)));
    Ok(resolved)
}

#[derive(Debug)]
struct VisiblePrimaryKeyRow {
    key: Vec<u8>,
    row_id: u64,
    location: PhysicalRowRef,
}

fn visible_primary_key_rows(
    view: &TransactionView,
    table: &TableHandle,
    target_keys: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<VisiblePrimaryKeyRow>> {
    if let Some(target_keys) = target_keys {
        let table_id = storage_table_id(table);
        view.read_tracker().record_key_ranges(
            table_id,
            target_keys.iter().map(|key| {
                let key_hash = primary_key_hash(key);
                (key_hash, key_hash)
            }),
        );
    }
    visible_primary_key_rows_without_read_tracking(view, table, target_keys)
}

fn visible_primary_key_rows_without_read_tracking(
    view: &TransactionView,
    table: &TableHandle,
    target_keys: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<VisiblePrimaryKeyRow>> {
    let tablet = table.tablet();
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("tablet schema missing"))?;
    if schema.keys_type() != KeysType::PrimaryKeys {
        return Err(paro_error::invalid_input(
            "visible_primary_key_rows requires PRIMARY_KEYS tablet",
        ));
    }

    let serializer = PrimaryKeySerializer::from_schema_ref(&schema)?;
    let overlay = TxnOverlayReader::for_tablet(&tablet, view)?;
    let overlay_delete_vectors = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors);

    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    if let Some(overlay) = overlay.as_ref() {
        for (key, entry) in overlay.primary_key_entries() {
            if target_keys.is_some_and(|target| !target.contains(key)) {
                continue;
            }
            seen.insert(key.clone());
            if let (Some(row_id), Some(location)) = (entry.row_id, entry.row_ref) {
                rows.push(VisiblePrimaryKeyRow {
                    key: key.clone(),
                    row_id: u64::from(row_id),
                    location,
                });
            }
        }
        if target_keys.is_some_and(|target| target.iter().all(|key| seen.contains(key))) {
            return Ok(rows);
        }
    }

    let mut params = TabletReaderParams::with_version(view.visible_version_i64())
        .with_columns((0..schema.num_key_columns()).collect())
        .with_emit_row_id(true);
    if let Some(delete_vectors) = overlay_delete_vectors {
        params = params.with_overlay_delete_vectors(delete_vectors);
    }
    let mut reader = table.create_reader(params)?;
    reader.prepare()?;

    while let Some(chunk) = reader.get_next_chunk()? {
        let encoded = serializer.encode_chunk(&chunk)?;
        let row_id_col = chunk
            .column(schema.num_key_columns())
            .ok_or_else(|| paro_error::internal("missing row_id column in primary-key scan"))?;
        for (row_idx, key) in encoded.into_iter().enumerate() {
            if target_keys.is_some_and(|target| !target.contains(&key)) || !seen.insert(key.clone())
            {
                continue;
            }
            let row_id = read_row_id_value(row_id_col.get_value(row_idx))?;
            let location = tablet.decode_row_id(RowID::from_raw(row_id))?;
            rows.push(VisiblePrimaryKeyRow {
                key,
                row_id,
                location,
            });
            if target_keys.is_some_and(|target| rows.len() == target.len()) {
                return Ok(rows);
            }
        }
    }

    Ok(rows)
}

fn read_row_id_value(value: Value) -> Result<u64> {
    match value {
        Value::BigInt(row_id) if row_id >= 0 => Ok(row_id as u64),
        Value::UBigInt(row_id) => Ok(row_id),
        Value::Integer(row_id) if row_id >= 0 => Ok(row_id as u64),
        other => Err(paro_error::internal(format!(
            "invalid row_id value {:?} in primary-key scan",
            other
        ))),
    }
}

fn immediate_commit_ts(view: &TransactionView) -> Result<CommitTs> {
    let from_read_ts =
        view.read_ts().into_raw().checked_add(1).ok_or_else(|| {
            paro_error::invalid_input("read_ts overflow while assigning commit_ts")
        })?;
    Ok(CommitTs::new(from_read_ts))
}

fn storage_table_id(table: &TableHandle) -> TableId {
    TableId::new(table.table_id())
}
