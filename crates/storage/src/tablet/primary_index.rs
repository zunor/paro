// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::tablet_runtime::{PrimaryIndexMemoryState, PrimaryIndexUpdate, Tablet};
use crate::codec::vector_decoder;
use crate::compaction::publish::record::PkPublishDelta;
use crate::primary_key::{
    DeleteVector, PersistentIndex, PrimaryIndex, PrimaryIndexProvenance, PrimaryIndexRowsetRoot,
    PrimaryIndexVersion, PrimaryKeyWriteConflict, RowID,
};
use crate::rowset::column::ColumnBatch;
use crate::rowset::PhysicalRowRef;
use crate::rowset::{Rowset, RowsetSharedPtr};
use crate::tablet::tablet_schema::KeysType;
use crate::tablet::ColumnId;
use parking_lot::Mutex as ParkingMutex;
use paro_common::allocator::{default_allocator, Allocator};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::{ProducerToken, Task, TaskExecutionMode, TaskExecutionResult};
use paro_transaction::CommitTs;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard, Weak};
use tracing::warn;

const PRIMARY_INDEX_COMPACTION_PRIORITY: i32 = -15;

struct PrimaryIndexMaintenanceState {
    producer: ProducerToken,
    tablet: Weak<Tablet>,
    pending: AtomicBool,
}

/// Table-scoped admission point for primary-index immutable compaction.
///
/// The scheduler owns at most one task per tablet. Each turn performs one
/// snapshot/merge/CAS-publish quantum, allowing the shared instance scheduler
/// to remain fair across tables while foreground commits continue publishing
/// newer L1 runs.
#[derive(Clone)]
pub(super) struct PrimaryIndexMaintenanceScheduler {
    state: Arc<PrimaryIndexMaintenanceState>,
}

impl std::fmt::Debug for PrimaryIndexMaintenanceScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimaryIndexMaintenanceScheduler")
            .field("pending", &self.state.pending.load(Ordering::Acquire))
            .finish()
    }
}

impl PrimaryIndexMaintenanceScheduler {
    fn new(scheduler: Arc<TaskScheduler>, tablet: Weak<Tablet>) -> Self {
        Self {
            state: Arc::new(PrimaryIndexMaintenanceState {
                producer: scheduler
                    .create_producer_with_priority(PRIMARY_INDEX_COMPACTION_PRIORITY),
                tablet,
                pending: AtomicBool::new(false),
            }),
        }
    }

    fn schedule(&self) {
        if self
            .state
            .pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let task: Arc<ParkingMutex<dyn Task>> =
            Arc::new(ParkingMutex::new(PrimaryIndexCompactionTask {
                state: Arc::clone(&self.state),
            }));
        self.state.producer.schedule_task(task);
    }
}

struct PrimaryIndexCompactionTask {
    state: Arc<PrimaryIndexMaintenanceState>,
}

impl PrimaryIndexCompactionTask {
    fn finish(&self) -> TaskExecutionResult {
        self.state.pending.store(false, Ordering::Release);
        TaskExecutionResult::Finished
    }

    /// Clear ownership, then re-read the level-triggered predicate. A flush
    /// racing with the task's final `compaction_needed` observation either
    /// schedules a new task after seeing `pending = false`, or is observed
    /// here and keeps this task alive. No trailing L1 publication can be lost
    /// in the edge between the last merge and task completion.
    fn finish_or_continue(&self, tablet: &Tablet) -> Result<TaskExecutionResult> {
        self.state.pending.store(false, Ordering::Release);
        let compaction_needed = tablet.persistent_index()?.compaction_needed();
        if compaction_needed
            && self
                .state
                .pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Ok(TaskExecutionResult::NotFinished);
        }
        Ok(TaskExecutionResult::Finished)
    }
}

impl Task for PrimaryIndexCompactionTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let Some(tablet) = self.state.tablet.upgrade() else {
            return Ok(self.finish());
        };
        match tablet.compact_primary_index_once() {
            Ok(true) => Ok(TaskExecutionResult::NotFinished),
            Ok(false) => self.finish_or_continue(&tablet),
            Err(error) => {
                warn!(
                    tablet_id = tablet.tablet_id(),
                    error = %error,
                    "background primary-index compaction failed"
                );
                Ok(self.finish())
            }
        }
    }

    fn task_type(&self) -> &str {
        "PrimaryIndexCompactionTask"
    }
}

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
    pub fn bind_primary_index_task_scheduler(
        self: &Arc<Self>,
        scheduler: Option<Arc<TaskScheduler>>,
    ) {
        let maintenance = scheduler.map(|scheduler| {
            PrimaryIndexMaintenanceScheduler::new(scheduler, Arc::downgrade(self))
        });
        let should_schedule = maintenance.is_some()
            && self
                .persistent_index()
                .map(|index| index.compaction_needed())
                .unwrap_or(false);
        *self.primary_index_maintenance_scheduler.write().unwrap() = maintenance;
        if should_schedule {
            self.schedule_primary_index_compaction();
        }
    }

    fn schedule_primary_index_compaction(&self) {
        if let Some(scheduler) = self
            .primary_index_maintenance_scheduler
            .read()
            .unwrap()
            .as_ref()
            .cloned()
        {
            scheduler.schedule();
        }
    }

    /// Execute one immutable primary-index merge quantum. Preparation and
    /// publication briefly own the tablet lock; the O(base) merge itself runs
    /// lock-free against pinned immutable readers.
    fn compact_primary_index_once(&self) -> Result<bool> {
        let plan = {
            let mut persistent = self.persistent_index_mut()?;
            persistent.prepare_compaction()?
        };
        let Some(plan) = plan else {
            return Ok(false);
        };

        let output = match PersistentIndex::execute_compaction(&plan) {
            Ok(output) => output,
            Err(error) => {
                self.persistent_index_mut()?.abort_compaction(&plan);
                return Err(error);
            }
        };
        let mut persistent = self.persistent_index_mut()?;
        let published = persistent.publish_compaction(plan, output)?;
        Ok(published && persistent.compaction_needed())
    }

    fn primary_index_handle(&self) -> Arc<PrimaryIndex> {
        self.primary_index.read().unwrap().index().clone()
    }

    fn primary_index_is_complete(&self) -> bool {
        self.primary_index.read().unwrap().is_complete()
    }

    fn new_primary_index_overlay(&self) -> Arc<PrimaryIndex> {
        let index = Arc::new(PrimaryIndex::new());
        self.register_primary_index_callbacks(index.as_ref());
        index
    }

    /// Hold the semantic-state read lock across every mutation. A flush owns
    /// the write lock while publishing the captured layer and swapping in a
    /// new overlay, so no writer can retain an old Arc and append after that
    /// layer has become immutable.
    fn mutate_primary_index<T>(&self, mutation: impl FnOnce(&PrimaryIndex) -> T) -> T {
        let state = self.primary_index.read().unwrap();
        mutation(state.index().as_ref())
    }

    fn persistent_index_dir(&self) -> PathBuf {
        self.data_dir().join("primary_index")
    }

    fn persistent_index(&self) -> Result<RwLockReadGuard<'_, PersistentIndex>> {
        self.persistent_primary_index
            .read()
            .map_err(|_| paro_error::internal("lock persistent primary index for read"))
    }

    fn persistent_index_mut(&self) -> Result<RwLockWriteGuard<'_, PersistentIndex>> {
        self.persistent_primary_index
            .write()
            .map_err(|_| paro_error::internal("lock persistent primary index for write"))
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

        Ok(self.materialize_complete_primary_index()?.snapshot())
    }

    #[doc(hidden)]
    pub fn remove_primary_index_entry_for_test(&self, key: &[u8]) -> Result<()> {
        // Tests that exercise consistency repair need to corrupt the complete
        // logical view. Removing a key only from the mutable overlay is not a
        // corruption when the immutable base still contains that key.
        let complete = Arc::new(self.materialize_complete_primary_index()?);
        complete.remove(key);
        self.register_primary_index_callbacks(&complete);
        *self.primary_index.write().unwrap() = PrimaryIndexMemoryState::Complete(complete);
        Ok(())
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
        if !self.primary_index_is_complete() {
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
            self.rebuild_primary_index_after_compaction(output)?;
            validate_once().map_err(|second_error| {
                paro_error::internal(format!(
                    "failed to validate/rebuild primary index after compaction: first={}, second={}",
                    first_error, second_error
                ))
            })?;
        }
        Ok(())
    }

    /// Rebuild the primary index from the post-publication durable rowset
    /// graph. Compaction WAL deliberately does not retain an unbounded key
    /// delta, so replay and large compactions use this as the authoritative
    /// publication path rather than trying to infer correctness from index
    /// cardinality or samples.
    pub(crate) fn rebuild_primary_index_after_compaction(&self, output: &Rowset) -> Result<()> {
        self.rebuild_primary_index_from_visible_rowsets()?;
        self.persist_primary_index_snapshot()?;
        self.reconcile_primary_index_row_count()?;
        self.validate_primary_index_rowid_samples(output, output.end_version())
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
                output_entry.mark_deleted(candidate.output_location.row_offset);
                continue;
            };
            if current_row_id != source_row_id {
                output_entry.mark_deleted(candidate.output_location.row_offset);
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
            self.mutate_primary_index(|index| index.batch_upsert_at(pairs, commit_ts));
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
                .is_some_and(|delete_vector| delete_vector.is_deleted(row_location.row_offset));
            if output_deleted {
                if let Some(prev) = latest.remove(&key) {
                    let prev = self.decode_row_id(prev)?;
                    let entry = update
                        .pending_delete_vectors
                        .entry(prev.segment_key())
                        .or_insert_with(DeleteVector::new);
                    entry.mark_deleted(prev.row_offset);
                }
                if let Some(current_row_id) = current_row_id {
                    let old_loc = self.decode_row_id(current_row_id)?;
                    let entry = update
                        .pending_delete_vectors
                        .entry(old_loc.segment_key())
                        .or_insert_with(DeleteVector::new);
                    entry.mark_deleted(old_loc.row_offset);
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
                entry.mark_deleted(prev.row_offset);
            }

            if let Some(current_row_id) = current_row_id {
                let old_loc = self.decode_row_id(current_row_id)?;
                let entry = update
                    .pending_delete_vectors
                    .entry(old_loc.segment_key())
                    .or_insert_with(DeleteVector::new);
                entry.mark_deleted(old_loc.row_offset);
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
        // The database journal plus immutable rowsets are the durable truth.
        // Keep the mutable primary index in the transaction's in-memory
        // publication boundary; its persistent levels are a derived cache
        // flushed only when the memory watermark requests it. Writing a
        // second WAL here would serialize every group-committed transaction
        // and still require rowset reconciliation after a crash.
        if !prepared.tombstones.is_empty() || !prepared.pairs.is_empty() {
            let commit_ts = u64::try_from(version)
                .map_err(|_| paro_error::invalid_input("negative primary index version"))?;
            self.mutate_primary_index(|index| {
                for key in &prepared.tombstones {
                    index.remove_at(key, commit_ts);
                }
                if !prepared.pairs.is_empty() {
                    index.batch_upsert_at(prepared.pairs.clone(), commit_ts);
                }
            });
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
        self.mutate_primary_index(|index| {
            for key in &existing_keys {
                index.remove_at(key, commit_ts);
            }
        });

        let mut pending: HashMap<(u64, u32), DeleteVector> = HashMap::new();
        for loc in resolved_locations {
            let entry = pending
                .entry(loc.segment_key())
                .or_insert_with(DeleteVector::new);
            entry.mark_deleted(loc.row_offset);
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
                entry.mark_deleted(location.row_offset);
            }

            tombstones.push(key);
        }

        self.mutate_primary_index(|index| {
            for key in &tombstones {
                index.remove_at(key, u64::try_from(version).unwrap_or(0));
            }
        });
        let persistent = self.persistent_index()?;
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
                            .map(|dv| dv.is_deleted(row_id))
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
        let expected_provenance = self.primary_index_provenance()?;
        let validated = (|| {
            let persistent = self.persistent_index()?;
            if persistent.provenance() != Some(&expected_provenance) {
                return Err(paro_error::data_corrupted(format!(
                    "persistent primary-index provenance does not match tablet {} durable state",
                    self.tablet_id()
                )));
            }
            Ok(())
        })();
        match validated {
            Ok(()) => {
                // Provenance is the structural proof. Keep the immutable base
                // in its mmap-backed LSM and start with a bounded empty L0;
                // loading every key into a second hash table makes recovery
                // O(table cardinality) and causes the first tiny post-restart
                // write to rewrite the complete base again.
                *self.primary_index.write().unwrap() =
                    PrimaryIndexMemoryState::Overlay(self.new_primary_index_overlay());
                Ok(false)
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

        if let Err(err) = self.flush_primary_index_memory_state(false) {
            self.primary_index_flush_requested
                .store(true, Ordering::Release);
            return Err(err);
        }
        Ok(())
    }

    /// Publish one stable mutable layer and atomically replace it with an
    /// empty overlay. The state write lock is deliberately held through the
    /// immutable-file publication: primary-index mutations hold the matching
    /// read lock, so no writer can append to an Arc after it has been sealed.
    fn flush_primary_index_memory_state(&self, force: bool) -> Result<()> {
        let mut state = self.primary_index.write().unwrap();
        let index = state.index().clone();
        if index.is_empty() && !state.is_complete() {
            return Ok(());
        }
        if !force && index.is_empty() {
            return Ok(());
        }

        let provenance = self.primary_index_provenance()?;
        let mut persistent = self.persistent_index_mut()?;
        if state.is_complete() {
            persistent.reset()?;
        }
        persistent.flush_l0_with_provenance(index.as_ref(), true, Some(provenance))?;
        let needs_compaction = persistent.compaction_needed();
        *state = PrimaryIndexMemoryState::Overlay(self.new_primary_index_overlay());
        drop(persistent);
        drop(state);
        self.primary_index_flush_requested
            .store(false, Ordering::Release);
        if needs_compaction {
            self.schedule_primary_index_compaction();
        }
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

        // Recovery starts from either a provenance-matched base or a strict
        // rowset rebuild.  Replayed rowsets/deletes then advance that base.
        // Cardinality remains a diagnostic only; it is never used to accept an
        // unproven persistent cache.
        if self.primary_index_is_complete() {
            if let Err(error) = self.reconcile_primary_index_row_count() {
                warn!(
                    tablet_id = self.tablet_id(),
                    error = %error,
                    "replayed primary index failed diagnostics; rebuilding from durable rowsets"
                );
                self.rebuild_primary_index_from_visible_rowsets()?;
                self.reconcile_primary_index_row_count()?;
            }
        }
        self.flush_primary_index_memory_state(true)?;
        Ok(())
    }

    pub(crate) fn apply_replayed_rowset_to_primary_index(&self, rowset_id: u64) -> Result<()> {
        let Some(schema) = self.schema() else {
            return Ok(());
        };
        if schema.keys_type() != KeysType::PrimaryKeys {
            return Ok(());
        }
        let Some(rowset) = self.find_rowset_by_id(rowset_id) else {
            return Ok(());
        };
        self.mutate_primary_index(|index| {
            self.upsert_visible_rowset_into_primary_index(
                index,
                &rowset,
                &schema,
                self.max_version(),
            )
        })
    }

    fn upsert_visible_rowset_into_primary_index(
        &self,
        target: &PrimaryIndex,
        rowset: &RowsetSharedPtr,
        schema: &crate::tablet::TabletSchemaRef,
        visible_version: i64,
    ) -> Result<()> {
        let serializer = crate::primary_key::PrimaryKeySerializer::from_schema_ref(schema)?;
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

        rowset.load()?;
        for segment in rowset.segments() {
            let delete_vector = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                segment.segment_id(),
                visible_version,
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
                target.batch_upsert_at(pairs, u64::try_from(rowset.end_version()).unwrap_or(0));
            }
        }
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
        let repaired = PrimaryIndex::new();
        for rowset in visible_rowsets {
            self.upsert_visible_rowset_into_primary_index(
                &repaired,
                &rowset,
                &schema,
                self.max_version(),
            )?;
        }

        let repaired = Arc::new(repaired);
        self.register_primary_index_callbacks(&repaired);
        *self.primary_index.write().unwrap() = PrimaryIndexMemoryState::Complete(repaired);
        Ok(())
    }

    pub(super) fn persist_primary_index_snapshot(&self) -> Result<()> {
        self.flush_primary_index_memory_state(true)
    }

    fn materialize_complete_primary_index(&self) -> Result<PrimaryIndex> {
        let state = self.primary_index.read().unwrap();
        let in_memory = state.index().clone();
        if state.is_complete() {
            let complete = PrimaryIndex::new();
            complete.batch_apply_versions(in_memory.snapshot_versions());
            return Ok(complete);
        }
        drop(state);
        let complete = self.persistent_index()?.load()?;
        complete.batch_apply_versions(in_memory.snapshot_versions());
        Ok(complete)
    }

    fn primary_index_provenance(&self) -> Result<PrimaryIndexProvenance> {
        let indexed_through_version = self.max_version();
        let mut rowset_root = self
            .capture_consistent_rowsets(indexed_through_version)?
            .into_iter()
            .map(|rowset| {
                let meta = rowset.rowset_meta();
                PrimaryIndexRowsetRoot {
                    rowset_id: rowset.rowset_id(),
                    start_version: rowset.start_version(),
                    end_version: rowset.end_version(),
                    num_segments: rowset.num_segments(),
                    effective_rows: meta.effective_rows(),
                }
            })
            .collect::<Vec<_>>();
        rowset_root.sort_by_key(|rowset| (rowset.start_version, rowset.rowset_id));
        Ok(PrimaryIndexProvenance {
            tablet_id: self.tablet_id(),
            indexed_through_version,
            layout_epoch: self.layout_epoch(),
            schema_epoch: self.schema_epoch(),
            schema_hash: self.schema_hash(),
            rowset_root,
        })
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
