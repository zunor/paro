// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::tablet_runtime::{PhysicalRowRef, PrimaryIndexUpdate, Tablet};
use crate::codec::vector_decoder;
use crate::compaction::publish::record::PkPublishDelta;
use crate::primary_key::{
    DeleteVector, PersistentIndex, PrimaryIndex, PrimaryIndexVersion, PrimaryKeyWriteConflict,
    RowID,
};
use crate::rowset::column::ColumnBatch;
use crate::rowset::{Rowset, RowsetSharedPtr};
use crate::tablet::tablet_schema::KeysType;
use crate::tablet::ColumnId;
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_transaction::CommitTs;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug)]
pub(crate) struct PreparedPrimaryIndexPublish {
    pairs: Vec<(Vec<u8>, RowID)>,
    tombstones: Vec<Vec<u8>>,
    pending_delete_vectors: HashMap<(u64, u32), DeleteVector>,
}

fn primary_key_write_conflict_error(
    tablet_id: u64,
    read_ts: u64,
    conflict: &PrimaryKeyWriteConflict,
) -> paro_common::error::ParoError {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    conflict.key.hash(&mut hasher);
    let write_kind = if conflict.is_tombstone() {
        "delete"
    } else {
        "upsert"
    };
    paro_error::serialization_failure(format!(
        "write-write conflict on tablet {} primary key: key_hash={} was modified after read timestamp {} at commit_ts {} ({})",
        tablet_id,
        hasher.finish(),
        read_ts,
        conflict.commit_ts(),
        write_kind
    ))
}

fn select_earlier_primary_key_conflict(
    slot: &mut Option<PrimaryKeyWriteConflict>,
    candidate: PrimaryKeyWriteConflict,
) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.commit_ts() < current.commit_ts()
                || (candidate.commit_ts() == current.commit_ts() && candidate.key < current.key)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

impl Tablet {
    fn primary_index_handle(&self) -> Arc<PrimaryIndex> {
        self.primary_index.read().unwrap().clone()
    }

    fn persistent_index_dir(&self) -> PathBuf {
        self.data_dir().join("primary_index")
    }

    fn persistent_index(&self) -> Result<PersistentIndex> {
        PersistentIndex::new(self.persistent_index_dir())
    }

    pub fn lookup_primary_key(&self, key: &[u8]) -> Result<Option<RowID>> {
        let idx = self.primary_index_handle();
        if let Some(version) = idx.latest_version(key) {
            return Ok(version.visible_row_id());
        }
        self.persistent_index()?.get(key)
    }

    pub fn lookup_primary_key_at(&self, key: &[u8], read_ts: u64) -> Result<Option<RowID>> {
        Ok(self
            .lookup_primary_key_version_at(key, read_ts)?
            .and_then(PrimaryIndexVersion::visible_row_id))
    }

    pub fn lookup_primary_key_version_at(
        &self,
        key: &[u8],
        read_ts: u64,
    ) -> Result<Option<PrimaryIndexVersion>> {
        let idx = self.primary_index_handle();
        if let Some(version) = idx.get_version_at(key, read_ts) {
            return Ok(Some(version));
        }
        self.persistent_index()?.get_version_at(key, read_ts)
    }

    pub fn lookup_primary_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<RowID>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let idx = self.primary_index_handle();
        let mut resolved = vec![None; keys.len()];
        let mut missing_positions = Vec::new();
        let mut missing_keys = Vec::new();

        for (idx_pos, key) in keys.iter().enumerate() {
            if let Some(version) = idx.latest_version(key) {
                resolved[idx_pos] = version.visible_row_id();
            } else {
                missing_positions.push(idx_pos);
                missing_keys.push(key.clone());
            }
        }

        if missing_keys.is_empty() {
            return Ok(resolved);
        }

        let persistent = self.persistent_index()?;
        let persisted = persistent.lookup_keys(&missing_keys)?;
        for (idx_pos, row_id) in missing_positions.into_iter().zip(persisted.into_iter()) {
            resolved[idx_pos] = row_id;
        }

        Ok(resolved)
    }

    pub fn lookup_primary_keys_at(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
    ) -> Result<Vec<Option<RowID>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let idx = self.primary_index_handle();
        let mut resolved = vec![None; keys.len()];
        let mut missing_positions = Vec::new();
        let mut missing_keys = Vec::new();

        for (idx_pos, key) in keys.iter().enumerate() {
            if let Some(version) = idx.get_version_at(key, read_ts) {
                resolved[idx_pos] = version.visible_row_id();
            } else {
                missing_positions.push(idx_pos);
                missing_keys.push(key.clone());
            }
        }

        if missing_keys.is_empty() {
            return Ok(resolved);
        }

        let persistent = self.persistent_index()?;
        let persisted = persistent.lookup_keys_at(&missing_keys, read_ts)?;
        for (idx_pos, row_id) in missing_positions.into_iter().zip(persisted.into_iter()) {
            resolved[idx_pos] = row_id;
        }

        Ok(resolved)
    }

    pub fn validate_primary_key_no_committed_after(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
    ) -> Result<()> {
        self.validate_primary_key_no_write_in_range(keys, read_ts, u64::MAX)
    }

    pub fn validate_primary_key_no_write_in_range(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        if let Some(conflict) = self.first_primary_key_write_conflict(keys, read_ts, commit_ts)? {
            return Err(primary_key_write_conflict_error(
                self.tablet_id(),
                read_ts,
                &conflict,
            ));
        }
        Ok(())
    }

    pub fn first_primary_key_write_conflict(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        if keys.is_empty() {
            return Ok(None);
        }

        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_slice()) {
                unique_keys.push(key.clone());
            }
        }

        let idx = self.primary_index_handle();
        if let Some(conflict) = idx.first_write_for_keys_in_range(&unique_keys, read_ts, commit_ts)
        {
            return Ok(Some(conflict));
        }

        let persistent = self.persistent_index()?;
        persistent.first_write_for_keys_in_range(&unique_keys, read_ts, commit_ts)
    }

    pub fn first_primary_key_range_write_conflict(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        let mut best = self
            .primary_index_handle()
            .first_key_range_write_in_range(lower, upper, read_ts, commit_ts);
        if let Some(conflict) = self
            .persistent_index()?
            .first_key_range_write_in_range(lower, upper, read_ts, commit_ts)?
        {
            select_earlier_primary_key_conflict(&mut best, conflict);
        }
        Ok(best)
    }

    pub fn snapshot_primary_index_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(Vec::new()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(Vec::new());
        }

        if self.primary_index_full.load(Ordering::Acquire) {
            return Ok(self.primary_index_handle().snapshot());
        }

        Ok(self.persistent_index()?.load()?.snapshot())
    }

    #[doc(hidden)]
    pub fn remove_primary_index_entry_for_test(&self, key: &[u8]) {
        self.primary_index_handle().remove(key);
    }

    /// Reconcile primary index cardinality with effective rows recorded in RowsetMeta.
    /// Only applies to PRIMARY_KEYS tablets.
    pub fn reconcile_primary_index_row_count(&self) -> Result<()> {
        let schema = match self.schema() {
            Some(s) => s,
            None => return Ok(()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(());
        }
        if self.has_pending_delete_locks() {
            return Ok(());
        }
        if !self.primary_index_full.load(Ordering::Acquire) {
            return Ok(());
        }

        let live_rows: u64 = self
            .rs_version_map
            .read()
            .unwrap()
            .values()
            .filter(|rs| rs.is_readable())
            .map(|rs| rs.rowset_meta().effective_rows())
            .sum();
        let index_len = self.primary_index_handle().len() as u64;
        if live_rows != index_len {
            return Err(paro_error::internal(format!(
                "PrimaryIndex cardinality mismatch: index={} vs effective_rows={}",
                index_len, live_rows
            )));
        }
        Ok(())
    }

    pub fn validate_primary_index_consistency_after_compaction(
        &self,
        output: &Rowset,
    ) -> Result<()> {
        let validate_once = || -> Result<()> {
            self.reconcile_primary_index_row_count()?;
            self.validate_primary_index_rowid_samples(output, output.end_version())
        };

        if let Err(first_error) = validate_once() {
            self.rebuild_primary_index_from_visible_rowsets()?;
            self.persist_primary_index_snapshot()?;
            validate_once().map_err(|second_error| {
                paro_error::internal(format!(
                    "failed to validate/rebuild primary index after compaction: first={}, second={}",
                    first_error, second_error
                ))
            })?;
        }
        Ok(())
    }

    pub(crate) fn apply_compaction_publish_delta(
        &self,
        output_rowset_id: u64,
        output_version: i64,
        pk_delta: &PkPublishDelta,
    ) -> Result<()> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(());
        }

        let mut pending_delete_vectors =
            HashMap::with_capacity(pk_delta.internal_delete_vectors.len());
        for delta in &pk_delta.internal_delete_vectors {
            let entry = pending_delete_vectors
                .entry((output_rowset_id, delta.segment_id))
                .or_insert_with(DeleteVector::new);
            for row_id in delta.delete_vector.iter() {
                entry.mark_deleted(row_id);
            }
        }

        let mut pairs = Vec::with_capacity(pk_delta.upsert_candidates.len());
        for candidate in &pk_delta.upsert_candidates {
            let output_entry = pending_delete_vectors
                .entry((
                    candidate.output_location.rowset_id,
                    candidate.output_location.segment_id,
                ))
                .or_insert_with(DeleteVector::new);
            let source_row_id = self.encode_row_location(candidate.source_location)?;
            let Some(current_row_id) = self.lookup_primary_key(&candidate.key)? else {
                output_entry.mark_deleted(candidate.output_location.row_offset.get());
                continue;
            };
            if current_row_id != source_row_id {
                output_entry.mark_deleted(candidate.output_location.row_offset.get());
                continue;
            }
            pairs.push((
                candidate.key.clone(),
                self.encode_row_location(candidate.output_location)?,
            ));
        }

        if !pairs.is_empty() {
            let commit_ts = u64::try_from(output_version)
                .map_err(|_| paro_error::invalid_input("negative primary index version"))?;
            self.persist_primary_index_upserts_at(&pairs, commit_ts)?;
            self.primary_index_handle()
                .batch_upsert_at(pairs, commit_ts);
        }

        if !pending_delete_vectors.is_empty() {
            // Compaction can publish after concurrent writes/deletes have already advanced the
            // tablet's visible version. Stamp output delete vectors at the current visible
            // version so they remain live after delete-vector version GC.
            let delete_vector_version = self.max_version().max(output_version);
            self.persist_delete_vectors(delete_vector_version, pending_delete_vectors)?;
        }
        Ok(())
    }

    fn validate_primary_index_rowid_samples(&self, rowset: &Rowset, version: i64) -> Result<()> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys || rowset.num_rows() == 0 {
            return Ok(());
        }

        let serializer = crate::primary_key::PrimaryKeySerializer::from_schema_ref(&schema)?;
        let key_projection: Vec<crate::tablet::ColumnId> = schema
            .columns()
            .iter()
            .filter(|column| column.is_key)
            .map(|column| column.id)
            .collect();
        let key_types: Vec<LogicalType> = schema
            .columns()
            .iter()
            .filter(|column| column.is_key)
            .map(|column| column.logical_type.clone())
            .collect();
        let allocator = Arc::new(default_allocator());
        let sample_budget = 32usize;
        let stride = ((rowset.num_rows() as usize).max(1)).div_ceil(sample_budget);

        let mut ordinal = 0usize;
        for segment in rowset.segments() {
            let delete_vector = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                segment.segment_id(),
                version,
            )?;
            let mut iter = crate::rowset::SegmentIterator::new_with_delete_vector(
                &segment,
                key_projection.clone(),
                delete_vector,
            )?;

            while iter.has_next() {
                let (row_ids, batch) = iter.next_batch(4096)?;
                if row_ids.is_empty() || batch.is_empty() {
                    continue;
                }
                let rows =
                    infer_row_count_for_keys(&key_projection, &key_types, &batch, row_ids.len())?;
                if rows == 0 {
                    continue;
                }
                let chunk =
                    build_key_chunk(&key_projection, &key_types, &batch, rows, allocator.clone())?;
                for (row_idx, &row_id) in row_ids.iter().enumerate().take(chunk.size()) {
                    let should_sample = ordinal == 0
                        || ordinal + 1 == rowset.num_rows() as usize
                        || ordinal % stride == 0;
                    ordinal += 1;
                    if !should_sample {
                        continue;
                    }

                    let key = serializer.encode_row(&chunk, row_idx)?;
                    let expected = self.encode_row_location(PhysicalRowRef::new(
                        rowset.rowset_id(),
                        segment.segment_id(),
                        row_id,
                    ))?;
                    let actual = self.lookup_primary_key(&key)?.ok_or_else(|| {
                        paro_error::internal(format!(
                            "primary index missing sampled compaction key in rowset {} segment {} row {}",
                            rowset.rowset_id(),
                            segment.segment_id(),
                            row_id
                        ))
                    })?;
                    if actual != expected {
                        return Err(paro_error::internal(format!(
                            "primary index sampled mismatch for rowset {} segment {} row {}: expected {:?}, got {:?}",
                            rowset.rowset_id(),
                            segment.segment_id(),
                            row_id,
                            expected,
                            actual
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn persist_primary_index_upserts_at(
        &self,
        pairs: &[(Vec<u8>, RowID)],
        commit_ts: u64,
    ) -> Result<()> {
        self.persistent_index()?.apply_upserts_at(pairs, commit_ts)
    }

    pub(super) fn prepare_primary_index_publish(
        &self,
        rowset: &Rowset,
        mut update: PrimaryIndexUpdate,
    ) -> Result<PreparedPrimaryIndexPublish> {
        let row_ids = self.row_ids_for_rowset(rowset)?;
        if row_ids.len() != update.written.len() {
            return Err(paro_error::internal(format!(
                "row id count {} does not match written keys {}",
                row_ids.len(),
                update.written.len()
            )));
        }

        let mut latest: HashMap<Vec<u8>, RowID> = HashMap::new();
        let mut tombstones: HashSet<Vec<u8>> = HashSet::new();
        let current = self.lookup_primary_keys(
            &update
                .written
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
        )?;

        for (((key, _old), row_id), current_row_id) in update
            .written
            .into_iter()
            .zip(row_ids.into_iter())
            .zip(current.into_iter())
        {
            let row_location = self.decode_row_id(row_id)?;
            let output_deleted = update
                .pending_delete_vectors
                .get(&row_location.segment_key())
                .is_some_and(|delete_vector| {
                    delete_vector.is_deleted(row_location.row_offset.get())
                });
            if output_deleted {
                if let Some(prev) = latest.remove(&key) {
                    let prev = self.decode_row_id(prev)?;
                    let entry = update
                        .pending_delete_vectors
                        .entry(prev.segment_key())
                        .or_insert_with(DeleteVector::new);
                    entry.mark_deleted(prev.row_offset.get());
                }
                if let Some(current_row_id) = current_row_id {
                    let old_loc = self.decode_row_id(current_row_id)?;
                    let entry = update
                        .pending_delete_vectors
                        .entry(old_loc.segment_key())
                        .or_insert_with(DeleteVector::new);
                    entry.mark_deleted(old_loc.row_offset.get());
                }
                tombstones.insert(key);
                continue;
            }

            tombstones.remove(&key);
            if let Some(prev) = latest.insert(key.clone(), row_id) {
                let prev = self.decode_row_id(prev)?;
                let entry = update
                    .pending_delete_vectors
                    .entry(prev.segment_key())
                    .or_insert_with(DeleteVector::new);
                entry.mark_deleted(prev.row_offset.get());
            }

            if let Some(current_row_id) = current_row_id {
                let old_loc = self.decode_row_id(current_row_id)?;
                let entry = update
                    .pending_delete_vectors
                    .entry(old_loc.segment_key())
                    .or_insert_with(DeleteVector::new);
                entry.mark_deleted(old_loc.row_offset.get());
            }
        }

        Ok(PreparedPrimaryIndexPublish {
            pairs: latest.into_iter().collect(),
            tombstones: tombstones.into_iter().collect(),
            pending_delete_vectors: update.pending_delete_vectors,
        })
    }

    pub(super) fn apply_prepared_primary_index_publish(
        &self,
        version: i64,
        rowset: &RowsetSharedPtr,
        prepared: PreparedPrimaryIndexPublish,
    ) -> Result<()> {
        if !prepared.tombstones.is_empty() {
            let commit_ts = u64::try_from(version)
                .map_err(|_| paro_error::invalid_input("negative primary index version"))?;
            let idx = self.primary_index_handle();
            for key in &prepared.tombstones {
                idx.remove_at(key, commit_ts);
            }
            self.persistent_index()?
                .apply_deletes_at(&prepared.tombstones, commit_ts)?;
        }

        if !prepared.pairs.is_empty() {
            let commit_ts = u64::try_from(version)
                .map_err(|_| paro_error::invalid_input("negative primary index version"))?;
            self.primary_index_handle()
                .batch_upsert_at(prepared.pairs.clone(), commit_ts);
            self.persist_primary_index_upserts_at(&prepared.pairs, commit_ts)?;
        }

        self.persist_delete_vectors_for_rowset_publish(
            version,
            rowset,
            prepared.pending_delete_vectors,
        )?;
        Ok(())
    }

    fn apply_primary_delete_internal(
        &self,
        keys: Vec<Vec<u8>>,
        delete_version: Option<i64>,
        ignore_missing: bool,
        advance_publish_version: bool,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let _meta_lock = self.meta_lock.write().unwrap();

        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.clone()) {
                unique_keys.push(key);
            }
        }

        let resolved = self.lookup_primary_keys(&unique_keys)?;
        let idx = self.primary_index_handle();
        let persistent = self.persistent_index()?;

        let mut resolved_locations = Vec::with_capacity(unique_keys.len());
        let mut existing_keys = Vec::with_capacity(unique_keys.len());
        for (key, current) in unique_keys.into_iter().zip(resolved.into_iter()) {
            match current {
                Some(current) => {
                    resolved_locations.push(self.decode_row_id(current)?);
                    existing_keys.push(key);
                }
                None if ignore_missing => {}
                None => {
                    return Err(paro_error::serialization_failure(format!(
                        "write-write conflict on tablet {} primary key delete: key no longer exists",
                        self.tablet_id()
                    )));
                }
            }
        }

        let version = delete_version.unwrap_or_else(|| self.max_version().saturating_add(1));
        let commit_ts = u64::try_from(version)
            .map_err(|_| paro_error::invalid_input("negative primary delete version"))?;
        for key in &existing_keys {
            idx.remove_at(key, commit_ts);
        }
        persistent.apply_deletes_at(&existing_keys, commit_ts)?;

        let mut pending: HashMap<(u64, u32), DeleteVector> = HashMap::new();
        for loc in resolved_locations {
            let entry = pending
                .entry(loc.segment_key())
                .or_insert_with(DeleteVector::new);
            entry.mark_deleted(loc.row_offset.get());
        }
        self.persist_delete_vectors_with_publish_advance(
            version,
            pending,
            advance_publish_version,
        )?;

        self.reconcile_primary_index_row_count()?;
        self.maybe_flush_primary_index()?;
        Ok(())
    }

    pub fn apply_primary_delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        self.apply_primary_delete_internal(keys, None, false, true)
    }

    /// Apply a primary-key delete at the given commit timestamp.
    /// Frontend commit paths must make the database journal durable before
    /// calling this; storage-local helpers use it only after a commit timestamp
    /// has been assigned by the transaction layer.
    pub(crate) fn apply_primary_delete_at_version(
        &self,
        keys: Vec<Vec<u8>>,
        commit_ts: CommitTs,
    ) -> Result<()> {
        let version = i64::try_from(commit_ts.into_raw())
            .map_err(|_| paro_error::invalid_input("commit_ts exceeds i64"))?;
        self.apply_primary_delete_internal(keys, Some(version), false, true)
    }

    pub(crate) fn apply_primary_delete_at_version_without_publish_advance(
        &self,
        keys: Vec<Vec<u8>>,
        commit_ts: CommitTs,
    ) -> Result<()> {
        let version = i64::try_from(commit_ts.into_raw())
            .map_err(|_| paro_error::invalid_input("commit_ts exceeds i64"))?;
        self.apply_primary_delete_internal(keys, Some(version), false, false)
    }

    pub(crate) fn replay_primary_delete_idempotent(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let _meta_lock = self.meta_lock.write().unwrap();

        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.clone()) {
                unique_keys.push(key);
            }
        }

        let idx = self.primary_index_handle();
        let persistent = self.persistent_index()?;
        let version = self.max_version();
        let mut pending: HashMap<(u64, u32), DeleteVector> = HashMap::new();
        let mut tombstones = Vec::new();

        for key in unique_keys {
            let occurrences = self.primary_key_occurrences(&key, version)?;
            let latest_live = occurrences.iter().find(|(_, is_deleted)| !*is_deleted);
            let should_preserve_latest_live = latest_live.is_some()
                && occurrences
                    .iter()
                    .skip(1)
                    .any(|(_, is_deleted)| *is_deleted);

            if should_preserve_latest_live {
                continue;
            }

            if let Some((location, _)) = latest_live {
                let entry = pending
                    .entry(location.segment_key())
                    .or_insert_with(DeleteVector::new);
                entry.mark_deleted(location.row_offset.get());
            }

            tombstones.push(key);
        }

        for key in &tombstones {
            idx.remove_at(key, u64::try_from(version).unwrap_or(0));
        }
        persistent.apply_deletes_at(&tombstones, u64::try_from(version).unwrap_or(0))?;
        if !pending.is_empty() {
            self.persist_delete_vectors(version, pending)?;
        }

        self.reconcile_primary_index_row_count()?;
        self.maybe_flush_primary_index()?;
        Ok(())
    }

    pub(crate) fn replay_primary_delete_idempotent_at_version(
        &self,
        keys: Vec<Vec<u8>>,
        delete_version: i64,
    ) -> Result<()> {
        self.apply_primary_delete_internal(keys, Some(delete_version), true, true)
    }

    fn primary_key_occurrences(
        &self,
        key: &[u8],
        version: i64,
    ) -> Result<Vec<(PhysicalRowRef, bool)>> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(Vec::new()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(Vec::new());
        }

        let serializer = crate::primary_key::PrimaryKeySerializer::from_schema_ref(&schema)?;
        let key_projection: Vec<crate::tablet::ColumnId> = schema
            .columns()
            .iter()
            .filter(|column| column.is_key)
            .map(|column| column.id)
            .collect();
        let key_types: Vec<LogicalType> = schema
            .columns()
            .iter()
            .filter(|column| column.is_key)
            .map(|column| column.logical_type.clone())
            .collect();
        let allocator = Arc::new(default_allocator());
        let rowsets = self.capture_consistent_rowsets(version)?;

        let mut occurrences = Vec::new();
        for rowset in rowsets.into_iter().rev() {
            rowset.load()?;
            for segment in rowset.segments() {
                let delete_vector = DeleteVector::load_from_dir_at_version(
                    rowset.rowset_path(),
                    segment.segment_id(),
                    version,
                )?;
                let mut iter = crate::rowset::SegmentIterator::new_with_delete_vector(
                    &segment,
                    key_projection.clone(),
                    None,
                )?;
                while iter.has_next() {
                    let (row_ids, batch) = iter.next_batch(4096)?;
                    if row_ids.is_empty() || batch.is_empty() {
                        continue;
                    }
                    let rows = infer_row_count_for_keys(
                        &key_projection,
                        &key_types,
                        &batch,
                        row_ids.len(),
                    )?;
                    if rows == 0 {
                        continue;
                    }
                    let chunk = build_key_chunk(
                        &key_projection,
                        &key_types,
                        &batch,
                        rows,
                        allocator.clone(),
                    )?;

                    for (row_idx, &row_id) in row_ids.iter().enumerate().take(chunk.size()) {
                        if serializer.encode_row(&chunk, row_idx)? != key {
                            continue;
                        }
                        let is_deleted = delete_vector
                            .as_ref()
                            .map(|dv| dv.is_deleted(row_id.get()))
                            .unwrap_or(false);
                        occurrences.push((
                            PhysicalRowRef::new(rowset.rowset_id(), segment.segment_id(), row_id),
                            is_deleted,
                        ));
                    }
                }
            }
        }

        Ok(occurrences)
    }

    pub(super) fn rebuild_primary_index_from_persistent(&self) -> Result<bool> {
        let persistent = self.persistent_index()?;
        match persistent.load() {
            Ok(index) => {
                let index = Arc::new(index);
                self.register_primary_index_callbacks(&index);
                *self.primary_index.write().unwrap() = index;
                self.primary_index_full.store(true, Ordering::Release);
                if self.reconcile_primary_index_row_count().is_ok() {
                    return Ok(false);
                }

                warn!(
                    tablet_id = self.tablet_id(),
                    persistent_index_dir = %self.persistent_index_dir().display(),
                    "persistent primary index is inconsistent with visible rowsets; rebuilding"
                );
                self.rebuild_primary_index_from_visible_rowsets()?;
                self.persist_primary_index_snapshot()?;
                Ok(true)
            }
            Err(error) => {
                warn!(
                    tablet_id = self.tablet_id(),
                    persistent_index_dir = %self.persistent_index_dir().display(),
                    error = %error,
                    "failed to load persistent index in current format; rebuilding from visible rowsets"
                );
                self.rebuild_primary_index_from_visible_rowsets()?;
                self.persist_primary_index_snapshot()?;
                Ok(true)
            }
        }
    }

    fn register_primary_index_callbacks(&self, index: &PrimaryIndex) {
        let flush_flag = self.primary_index_flush_requested.clone();
        index.register_mem_exceed_callback(move |_| {
            flush_flag.store(true, Ordering::Release);
        });
    }

    pub fn maybe_flush_primary_index(&self) -> Result<()> {
        if !self
            .primary_index_flush_requested
            .swap(false, Ordering::AcqRel)
        {
            return Ok(());
        }

        let idx = self.primary_index_handle();
        if idx.is_empty() {
            return Ok(());
        }

        let mut persistent = self.persistent_index()?;
        if let Err(err) = persistent.flush_l0(&idx, true) {
            self.primary_index_flush_requested
                .store(true, Ordering::Release);
            return Err(err);
        }
        idx.clear();
        self.primary_index_full.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn repair_primary_index_after_replay(&self) -> Result<()> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(());
        }

        if self.reconcile_primary_index_row_count().is_ok() {
            return Ok(());
        }

        self.rebuild_primary_index_from_visible_rowsets()?;
        self.reconcile_primary_index_row_count()?;
        self.persist_primary_index_snapshot()?;
        Ok(())
    }

    fn rebuild_primary_index_from_visible_rowsets(&self) -> Result<()> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(()),
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(());
        }

        let visible_rowsets = self.capture_consistent_rowsets(self.max_version())?;
        if visible_rowsets.is_empty() {
            return Ok(());
        }

        let serializer = crate::primary_key::PrimaryKeySerializer::from_schema_ref(&schema)?;
        let key_projection: Vec<crate::tablet::ColumnId> = schema
            .columns()
            .iter()
            .filter(|column| column.is_key)
            .map(|column| column.id)
            .collect();
        let key_types: Vec<LogicalType> = schema
            .columns()
            .iter()
            .filter(|column| column.is_key)
            .map(|column| column.logical_type.clone())
            .collect();
        let allocator = Arc::new(default_allocator());

        let repaired = PrimaryIndex::new();
        let snapshot = self.primary_index_handle().snapshot();
        if !snapshot.is_empty() {
            repaired.batch_upsert(snapshot);
        }

        for rowset in visible_rowsets {
            rowset.load()?;
            let segments = rowset.segments();
            for segment in segments {
                let delete_vector = DeleteVector::load_from_dir_at_version(
                    rowset.rowset_path(),
                    segment.segment_id(),
                    self.max_version(),
                )?;
                let mut iter = crate::rowset::SegmentIterator::new_with_delete_vector(
                    &segment,
                    key_projection.clone(),
                    delete_vector,
                )?;

                while iter.has_next() {
                    let (row_ids, batch) = iter.next_batch(4096)?;
                    if row_ids.is_empty() || batch.is_empty() {
                        continue;
                    }
                    let rows = infer_row_count_for_keys(
                        &key_projection,
                        &key_types,
                        &batch,
                        row_ids.len(),
                    )?;
                    if rows == 0 {
                        continue;
                    }
                    let chunk = build_key_chunk(
                        &key_projection,
                        &key_types,
                        &batch,
                        rows,
                        allocator.clone(),
                    )?;
                    let mut pairs = Vec::with_capacity(chunk.size());
                    for (row_idx, &row_id) in row_ids.iter().enumerate().take(chunk.size()) {
                        let key = serializer.encode_row(&chunk, row_idx)?;
                        let row_id = self.encode_row_location(PhysicalRowRef::new(
                            rowset.rowset_id(),
                            segment.segment_id(),
                            row_id,
                        ))?;
                        pairs.push((key, row_id));
                    }
                    repaired
                        .batch_upsert_at(pairs, u64::try_from(rowset.end_version()).unwrap_or(0));
                }
            }
        }

        let repaired = Arc::new(repaired);
        self.register_primary_index_callbacks(&repaired);
        *self.primary_index.write().unwrap() = repaired;
        self.primary_index_full.store(true, Ordering::Release);
        Ok(())
    }

    pub(super) fn persist_primary_index_snapshot(&self) -> Result<()> {
        let snapshot = self.primary_index_handle().snapshot();
        let persistent = self.persistent_index()?;
        persistent.reset()?;
        let mut persistent = self.persistent_index()?;
        if !snapshot.is_empty() {
            let commit_ts = u64::try_from(self.max_version()).unwrap_or(0);
            persistent.apply_upserts_at(&snapshot, commit_ts)?;
            let idx = self.primary_index_handle();
            persistent.flush_l0(&idx, true)?;
        }
        Ok(())
    }
}

fn infer_row_count_for_keys(
    projection: &[ColumnId],
    output_types: &[LogicalType],
    batch: &[(ColumnId, ColumnBatch)],
    expected: usize,
) -> Result<usize> {
    if batch.is_empty() || expected == 0 {
        return Ok(0);
    }

    let (col_id, batch) = &batch[0];
    let col_index = projection
        .iter()
        .position(|candidate| candidate == col_id)
        .ok_or_else(|| paro_error::internal("batch column not in primary key projection"))?;
    let ty = &output_types[col_index];
    if let Some(storage_dictionary) = &batch.storage_dictionary {
        let code_width = std::mem::size_of::<u32>();
        if storage_dictionary.codes.len() % code_width != 0 {
            return Err(paro_error::data_corrupted(
                "Storage dictionary code count is not u32-aligned",
            ));
        }
        Ok(storage_dictionary.codes.len() / code_width)
    } else {
        vector_decoder::infer_batch_row_count(ty, &batch.data, expected)
    }
}

fn build_key_chunk(
    projection: &[ColumnId],
    output_types: &[LogicalType],
    batch: &[(ColumnId, ColumnBatch)],
    rows: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    if rows == 0 {
        return Chunk::try_new(allocator);
    }

    let mut data_map: HashMap<ColumnId, &ColumnBatch> = HashMap::new();
    for (column_id, column_batch) in batch {
        data_map.insert(*column_id, column_batch);
    }

    let mut vectors: Vec<Arc<Vector>> = Vec::with_capacity(projection.len());
    for (idx, column_id) in projection.iter().enumerate() {
        let ty = &output_types[idx];
        let column_batch = data_map.get(column_id).ok_or_else(|| {
            paro_error::data_corrupted(format!("missing primary-key column {} in batch", column_id))
        })?;
        let vector =
            vector_decoder::decode_column_batch(ty, column_batch, rows, allocator.clone(), None)?;
        vectors.push(Arc::new(vector));
    }

    Ok(Chunk::from_arc_vectors(vectors, allocator))
}
