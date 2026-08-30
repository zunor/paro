// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage-owned transaction write buffer.
//!
//! The transaction core only sees this object as an opaque participant state.
//! Storage keeps the typed pending rowsets, delete patches, writers, and future
//! overlay readers on this side of the boundary.

use crate::primary_key::RowID;
use crate::rowset::RowsetSharedPtr;
use crate::search::write_path::SearchWriteContext;
use crate::table::runtime_indexes::RuntimeIndexes;
use crate::tablet::{
    LayoutMaintenanceLease, PhysicalRowRef, PrimaryIndexUpdate, SearchIngestAdmissionLease,
    TabletRef, TabletState,
};
use crate::transaction::spill::{
    StagedDeleteVectorArtifact, StagedRowsetArtifact, TxnSpillMark, TxnSpillState,
};
use crate::write::{DeltaWriter, DeltaWriterSavepoint};
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{
    CommandId, DatabaseId, ParticipantId, ParticipantKind, ReadTs, TransactionView,
    TxnParticipantState, TxnResourceKey,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_TXN_WRITE_BUFFER_MEMORY_BUDGET: u64 = 256 * 1024 * 1024;
const MIN_TXN_WRITE_BUFFER_MEMORY_BUDGET: u64 = 64 * 1024 * 1024;
const MAX_TXN_WRITE_BUFFER_MEMORY_BUDGET: u64 = 2 * 1024 * 1024 * 1024;
const TXN_WRITE_BUFFER_MEMORY_FRACTION: u64 = 8;
const SPILLED_WRITER_HANDLE_BYTES: u64 = 64 * 1024;

/// Derive one transaction's write working-set waterline from the session
/// memory contract.
///
/// A fixed process-wide default fragments large inline search artifacts even
/// when a session has been admitted with substantially more memory. The
/// transaction waterline remains only a local spill/flush threshold: global
/// buffer reservations and provider build admission still arbitrate actual
/// concurrent memory. Keeping the derivation here gives every protocol and
/// embedded session the same storage-owned policy.
pub fn transaction_write_buffer_memory_budget(session_memory_limit: usize) -> u64 {
    if session_memory_limit == 0 {
        return DEFAULT_TXN_WRITE_BUFFER_MEMORY_BUDGET;
    }
    let session_memory_limit = u64::try_from(session_memory_limit).unwrap_or(u64::MAX);
    session_memory_limit
        .div_ceil(TXN_WRITE_BUFFER_MEMORY_FRACTION)
        .clamp(
            MIN_TXN_WRITE_BUFFER_MEMORY_BUDGET,
            MAX_TXN_WRITE_BUFFER_MEMORY_BUDGET,
        )
        .min(session_memory_limit)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphTableDmlDelta {
    pub inserted: u64,
    pub deleted: u64,
    pub updated: u64,
    pub updated_columns: BTreeSet<u32>,
}

fn estimated_primary_update_memory_bytes(update: &PrimaryIndexUpdate) -> u64 {
    let written = u64::try_from(update.written.capacity())
        .unwrap_or(u64::MAX)
        .saturating_mul(
            u64::try_from(std::mem::size_of::<(Vec<u8>, Option<RowID>)>()).unwrap_or(u64::MAX),
        )
        .saturating_add(update.written.iter().fold(0_u64, |total, (key, _)| {
            total.saturating_add(u64::try_from(key.capacity()).unwrap_or(u64::MAX))
        }));
    let delete_vectors = u64::try_from(update.pending_delete_vectors.capacity())
        .unwrap_or(u64::MAX)
        .saturating_mul(
            u64::try_from(std::mem::size_of::<(
                (u64, u32),
                crate::primary_key::DeleteVector,
            )>())
            .unwrap_or(u64::MAX),
        )
        .saturating_add(update.pending_delete_vectors.values().fold(
            0_u64,
            |total, delete_vector| {
                total.saturating_add(
                    u64::try_from(delete_vector.bitmap().serialized_size()).unwrap_or(u64::MAX),
                )
            },
        ));
    u64::try_from(std::mem::size_of::<PrimaryIndexUpdate>())
        .unwrap_or(u64::MAX)
        .saturating_add(written)
        .saturating_add(delete_vectors)
}

/// A storage mutation staged by a transaction.
#[derive(Debug)]
pub enum PendingMutation {
    Rowset(PendingRowset),
    PrimaryDelete(PendingPrimaryDelete),
    RowIdDelete(PendingRowIdDelete),
}

impl PendingMutation {
    #[inline]
    pub fn command_id(&self) -> CommandId {
        match self {
            Self::Rowset(pending) => pending.created_at_command_id,
            Self::PrimaryDelete(pending) => pending.deleted_at_command_id,
            Self::RowIdDelete(pending) => pending.deleted_at_command_id,
        }
    }

    fn estimated_memory_bytes(&self) -> u64 {
        match self {
            Self::Rowset(pending) => {
                128_u64
                    .saturating_add(pending.rowset_path.to_string_lossy().len() as u64)
                    .saturating_add(pending.primary_key_overlay.iter().fold(
                        0_u64,
                        |acc, (key, _)| {
                            acc.saturating_add(key.len() as u64)
                        .saturating_add(std::mem::size_of::<PendingPrimaryKeyEntry>() as u64)
                        },
                    ))
                    .saturating_add(pending.rowset.retained_memory_bytes())
                    .saturating_add(
                        pending
                            .staged_artifact
                            .as_ref()
                            .map_or(0, StagedRowsetArtifact::estimated_handle_bytes),
                    )
                    .saturating_add(
                        pending
                            .primary_update
                            .as_ref()
                            .map_or(0, estimated_primary_update_memory_bytes),
                    )
            }
            Self::PrimaryDelete(pending) => pending
                .keys
                .iter()
                .fold(128_u64, |acc, key| acc.saturating_add(key.len() as u64))
                .saturating_add(pending.locations_memory_bytes()),
            Self::RowIdDelete(pending) => 128_u64.saturating_add(pending.locations_memory_bytes()),
        }
    }

    fn minimum_memory_bytes_after_delete_spill(&self) -> u64 {
        match self {
            Self::PrimaryDelete(pending) => pending
                .keys
                .iter()
                .fold(128_u64, |acc, key| acc.saturating_add(key.len() as u64))
                .saturating_add(if pending.locations.is_empty() {
                    pending.locations_memory_bytes()
                } else {
                    StagedDeleteVectorArtifact::minimum_estimated_handle_bytes()
                }),
            Self::RowIdDelete(pending) => 128_u64.saturating_add(if pending.locations.is_empty() {
                pending.locations_memory_bytes()
            } else {
                StagedDeleteVectorArtifact::minimum_estimated_handle_bytes()
            }),
            Self::Rowset(_) => self.estimated_memory_bytes(),
        }
    }
}

/// Pending rowset + primary key update metadata.
#[derive(Debug)]
pub struct PendingRowset {
    pub(crate) tablet: TabletRef,
    pub(crate) rowset: RowsetSharedPtr,
    pub(crate) primary_update: Option<PrimaryIndexUpdate>,
    pub(crate) primary_key_overlay: Vec<(Vec<u8>, PendingPrimaryKeyEntry)>,
    pub(crate) rowset_path: PathBuf,
    pub(crate) created_at_command_id: CommandId,
    /// Recovery descriptor for an evicted immutable rowset. This metadata is
    /// orthogonal to overlay execution: readers always use the same lazy
    /// `Rowset` handle regardless of whether its segment state is resident.
    pub(crate) staged_artifact: Option<StagedRowsetArtifact>,
}

impl PendingRowset {
    #[inline]
    pub fn created_at_command_id(&self) -> CommandId {
        self.created_at_command_id
    }
}

/// Transaction-private primary-key entry visible after its command boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingPrimaryKeyEntry {
    pub(crate) row_ref: Option<PhysicalRowRef>,
    pub(crate) row_id: Option<RowID>,
    pub(crate) created_at_command_id: CommandId,
}

impl PendingPrimaryKeyEntry {
    fn live(row_ref: PhysicalRowRef, row_id: RowID, command_id: CommandId) -> Self {
        Self {
            row_ref: Some(row_ref),
            row_id: Some(row_id),
            created_at_command_id: command_id,
        }
    }

    fn tombstone(command_id: CommandId) -> Self {
        Self {
            row_ref: None,
            row_id: None,
            created_at_command_id: command_id,
        }
    }
}

/// Pending primary key delete.
#[derive(Debug)]
pub struct PendingPrimaryDelete {
    pub(crate) tablet: TabletRef,
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) locations: Vec<PhysicalRowRef>,
    pub(crate) spilled_delete_vector: Option<StagedDeleteVectorArtifact>,
    pub(crate) deleted_at_command_id: CommandId,
    pub(crate) durable: bool,
}

impl PendingPrimaryDelete {
    #[inline]
    pub fn deleted_at_command_id(&self) -> CommandId {
        self.deleted_at_command_id
    }

    fn locations_memory_bytes(&self) -> u64 {
        self.spilled_delete_vector
            .as_ref()
            .map(StagedDeleteVectorArtifact::estimated_handle_bytes)
            .unwrap_or_else(|| {
                (self.locations.len() as u64)
                    .saturating_mul(std::mem::size_of::<PhysicalRowRef>() as u64)
            })
    }

    pub(crate) fn overlay_locations(&self) -> Result<Vec<PhysicalRowRef>> {
        if let Some(artifact) = &self.spilled_delete_vector {
            artifact.load_row_refs()
        } else {
            Ok(self.locations.clone())
        }
    }

    pub(crate) fn mark_spill_committed(&self) {
        if let Some(artifact) = &self.spilled_delete_vector {
            artifact.mark_committed_descriptor_written();
        }
    }

    pub(crate) fn abandon_spill(&self) {
        if let Some(artifact) = &self.spilled_delete_vector {
            artifact.abandon_and_remove();
        }
    }
}

/// Pending row-id delete.
#[derive(Debug)]
pub struct PendingRowIdDelete {
    pub(crate) tablet: TabletRef,
    pub(crate) locations: Vec<PhysicalRowRef>,
    pub(crate) spilled_delete_vector: Option<StagedDeleteVectorArtifact>,
    pub(crate) deleted_at_command_id: CommandId,
}

impl PendingRowIdDelete {
    #[inline]
    pub fn deleted_at_command_id(&self) -> CommandId {
        self.deleted_at_command_id
    }

    fn locations_memory_bytes(&self) -> u64 {
        self.spilled_delete_vector
            .as_ref()
            .map(StagedDeleteVectorArtifact::estimated_handle_bytes)
            .unwrap_or_else(|| {
                (self.locations.len() as u64)
                    .saturating_mul(std::mem::size_of::<PhysicalRowRef>() as u64)
            })
    }

    pub(crate) fn locations_for_commit(&self) -> Result<Vec<PhysicalRowRef>> {
        if let Some(artifact) = &self.spilled_delete_vector {
            artifact.load_row_refs()
        } else {
            Ok(self.locations.clone())
        }
    }

    pub(crate) fn overlay_locations(&self) -> Result<Vec<PhysicalRowRef>> {
        self.locations_for_commit()
    }

    pub(crate) fn mark_spill_committed(&self) {
        if let Some(artifact) = &self.spilled_delete_vector {
            artifact.mark_committed_descriptor_written();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PreparedStorageState {
    pub(crate) rowsets: Vec<PendingRowset>,
    pub(crate) primary_deletes: Vec<PendingPrimaryDelete>,
    pub(crate) row_id_deletes: Vec<PendingRowIdDelete>,
    /// Shared physical-layout leases acquired before SQL transaction locks are
    /// released and retained through required tablet publication.
    pub(crate) _layout_leases: Vec<LayoutMaintenanceLease>,
    /// Capacity reservations acquired before durable append and released by
    /// required apply or abort. This closes the check-then-publish race among
    /// concurrently prepared transactions targeting the same HNSW tail.
    pub(crate) _search_ingest_admissions: Vec<SearchIngestAdmissionLease>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct WriterKey {
    command_id: u32,
    tablet_id: u64,
}

impl WriterKey {
    #[inline]
    fn new(command_id: CommandId, tablet_id: u64) -> Self {
        Self {
            command_id: command_id.into_raw(),
            tablet_id,
        }
    }

    #[inline]
    fn command_id(self) -> CommandId {
        CommandId::new(self.command_id)
    }
}

#[derive(Debug, Clone)]
pub struct TxnWriteBufferMark {
    mutations_len: usize,
    dml_tables: BTreeSet<u64>,
    art_columns: HashMap<u64, BTreeSet<u32>>,
    graph_dml: HashMap<u64, GraphTableDmlDelta>,
    writer_marks: BTreeMap<WriterKey, DeltaWriterSavepoint>,
    writer_bytes: BTreeMap<WriterKey, u64>,
    spill_mark: TxnSpillMark,
    published_command_id: u32,
}

impl Default for TxnWriteBufferMark {
    fn default() -> Self {
        Self {
            mutations_len: 0,
            dml_tables: BTreeSet::new(),
            art_columns: HashMap::new(),
            graph_dml: HashMap::new(),
            writer_marks: BTreeMap::new(),
            writer_bytes: BTreeMap::new(),
            spill_mark: TxnSpillMark { next_sequence: 0 },
            published_command_id: 0,
        }
    }
}

#[derive(Debug)]
struct TxnWriteBufferInner {
    mutations: Vec<PendingMutation>,
    writers: BTreeMap<WriterKey, DeltaWriter>,
    writer_bytes: BTreeMap<WriterKey, u64>,
    dml_tables: BTreeSet<u64>,
    art_columns: HashMap<u64, BTreeSet<u32>>,
    graph_dml: HashMap<u64, GraphTableDmlDelta>,
    prepared: Option<PreparedStorageState>,
    spill: TxnSpillState,
}

impl TxnWriteBufferInner {
    fn new(database_id: DatabaseId) -> Self {
        Self {
            mutations: Vec::new(),
            writers: BTreeMap::new(),
            writer_bytes: BTreeMap::new(),
            dml_tables: BTreeSet::new(),
            art_columns: HashMap::new(),
            graph_dml: HashMap::new(),
            prepared: None,
            spill: TxnSpillState::new(database_id),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct TxnOverlaySnapshot {
    pub(crate) rowsets: Vec<RowsetSharedPtr>,
    pub(crate) row_id_deletes: Vec<PhysicalRowRef>,
    pub(crate) primary_keys: HashMap<Vec<u8>, PendingPrimaryKeyEntry>,
}

#[derive(Debug)]
pub struct TxnWriteBuffer {
    inner: Mutex<TxnWriteBufferInner>,
    materialize_lock: Mutex<()>,
    memory_budget_bytes: AtomicU64,
    memory_usage_bytes: AtomicU64,
    mutation_count: AtomicU64,
    published_command_id: AtomicU32,
    frozen: AtomicBool,
}

impl Default for TxnWriteBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_TXN_WRITE_BUFFER_MEMORY_BUDGET)
    }
}

impl TxnWriteBuffer {
    pub fn new(memory_budget_bytes: u64) -> Self {
        Self::new_for_database(DatabaseId::new(0), memory_budget_bytes)
    }

    pub fn new_for_database(database_id: DatabaseId, memory_budget_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(TxnWriteBufferInner::new(database_id)),
            materialize_lock: Mutex::new(()),
            memory_budget_bytes: AtomicU64::new(memory_budget_bytes),
            memory_usage_bytes: AtomicU64::new(0),
            mutation_count: AtomicU64::new(0),
            published_command_id: AtomicU32::new(0),
            frozen: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn memory_budget_bytes(&self) -> u64 {
        self.memory_budget_bytes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_memory_budget_bytes(&self, bytes: u64) {
        self.memory_budget_bytes.store(bytes, Ordering::Release);
    }

    #[inline]
    pub fn memory_usage_bytes(&self) -> u64 {
        self.memory_usage_bytes.load(Ordering::Acquire)
    }

    #[inline]
    pub fn mutation_count(&self) -> u64 {
        self.mutation_count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn published_command_id(&self) -> CommandId {
        CommandId::new(self.published_command_id.load(Ordering::Acquire))
    }

    #[inline]
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    pub fn publish_command_boundary(&self, command_id: CommandId) {
        let raw = command_id.into_raw();
        let mut current = self.published_command_id.load(Ordering::Acquire);
        while raw > current {
            match self.published_command_id.compare_exchange_weak(
                current,
                raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn freeze(&self, command_id: CommandId) {
        self.publish_command_boundary(command_id);
        self.frozen.store(true, Ordering::Release);
    }

    pub fn mark_savepoint(&self) -> Result<TxnWriteBufferMark> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        let mut writer_marks = BTreeMap::new();
        for (key, writer) in inner.writers.iter_mut() {
            writer_marks.insert(*key, writer.mark_savepoint()?);
        }
        Ok(TxnWriteBufferMark {
            mutations_len: inner.mutations.len(),
            dml_tables: inner.dml_tables.clone(),
            art_columns: inner.art_columns.clone(),
            graph_dml: inner.graph_dml.clone(),
            writer_marks,
            writer_bytes: inner.writer_bytes.clone(),
            spill_mark: inner.spill.mark(),
            published_command_id: self.published_command_id.load(Ordering::Acquire),
        })
    }

    pub fn rollback_to_savepoint(&self, mark: &TxnWriteBufferMark) -> Result<()> {
        let (cancelled_writers, removed_mutations) = {
            let mut inner = self.inner.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock txn write buffer: {e}"))
            })?;

            let writer_keys: Vec<WriterKey> = inner.writers.keys().copied().collect();
            let mut cancelled = Vec::new();
            for key in &writer_keys {
                if let Some(writer_mark) = mark.writer_marks.get(key) {
                    let writer = inner
                        .writers
                        .get_mut(key)
                        .ok_or_else(|| paro_error::internal("failed to get pending writer"))?;
                    writer.rollback_to_savepoint(writer_mark)?;
                }
            }
            for key in writer_keys {
                if mark.writer_marks.contains_key(&key) {
                    continue;
                }
                inner.writer_bytes.remove(&key);
                if let Some(writer) = inner.writers.remove(&key) {
                    cancelled.push(writer);
                }
            }
            inner.writer_bytes = mark.writer_bytes.clone();
            debug_assert!(inner.spill.mark().next_sequence >= mark.spill_mark.next_sequence);
            inner.spill.rollback_to_mark(mark.spill_mark);

            let removed = if mark.mutations_len >= inner.mutations.len() {
                Vec::new()
            } else {
                inner.mutations.split_off(mark.mutations_len)
            };
            inner.dml_tables = mark.dml_tables.clone();
            inner.art_columns = mark.art_columns.clone();
            inner.graph_dml = mark.graph_dml.clone();
            self.republish_stats_locked(&inner);
            (cancelled, removed)
        };

        for writer in cancelled_writers {
            writer.cancel()?;
        }
        for mutation in removed_mutations.into_iter().rev() {
            Self::rollback_mutation(mutation);
        }
        self.published_command_id
            .store(mark.published_command_id, Ordering::Release);
        Ok(())
    }

    pub fn record_dml_table(&self, table_oid: u64) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        inner.dml_tables.insert(table_oid);
        self.republish_stats_locked(&inner);
        Ok(())
    }

    pub fn has_dml_on_table(&self, table_oid: u64) -> bool {
        self.inner
            .lock()
            .map(|inner| inner.dml_tables.contains(&table_oid))
            .unwrap_or(true)
    }

    pub fn has_dml_on_any_table<I>(&self, table_oids: I) -> bool
    where
        I: IntoIterator<Item = u64>,
    {
        let Ok(inner) = self.inner.lock() else {
            return true;
        };
        table_oids
            .into_iter()
            .any(|table_oid| inner.dml_tables.contains(&table_oid))
    }

    pub fn add_rowset(
        &self,
        txn_id: u64,
        command_id: CommandId,
        tablet: TabletRef,
        rowset: RowsetSharedPtr,
        primary_update: Option<PrimaryIndexUpdate>,
    ) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        self.ensure_mutable()?;
        let mutation = self.rowset_mutation_with_governance(
            txn_id,
            command_id,
            tablet,
            rowset,
            primary_update,
            &mut inner,
            false,
        )?;
        inner.mutations.push(mutation);
        self.republish_stats_locked(&inner);
        Ok(())
    }

    pub fn add_primary_delete(
        &self,
        txn_id: u64,
        command_id: CommandId,
        tablet: TabletRef,
        keys: Vec<Vec<u8>>,
        locations: Vec<PhysicalRowRef>,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        if keys.len() != locations.len() {
            return Err(paro_error::invalid_input(format!(
                "primary delete key/location count mismatch: {} keys vs {} locations",
                keys.len(),
                locations.len()
            )));
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        self.ensure_mutable()?;
        let pending_rowset_ids = inner
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                PendingMutation::Rowset(pending)
                    if pending.tablet.tablet_id() == tablet.tablet_id() =>
                {
                    Some(pending.rowset.rowset_id())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        let mut durable_keys = Vec::new();
        let mut durable_locations = Vec::new();
        let mut overlay_keys = Vec::new();
        let mut overlay_locations = Vec::new();
        for (key, location) in keys.into_iter().zip(locations.into_iter()) {
            if pending_rowset_ids.contains(&location.rowset_id) {
                overlay_keys.push(key);
                overlay_locations.push(location);
            } else {
                durable_keys.push(key);
                durable_locations.push(location);
            }
        }

        if !durable_keys.is_empty() {
            let mutation = PendingMutation::PrimaryDelete(PendingPrimaryDelete {
                tablet: tablet.clone(),
                keys: durable_keys,
                locations: durable_locations,
                spilled_delete_vector: None,
                deleted_at_command_id: command_id,
                durable: true,
            });
            let mutation = self.spill_delete_mutation_if_needed(txn_id, mutation, &mut inner)?;
            inner.mutations.push(mutation);
        }
        if !overlay_keys.is_empty() {
            let mutation = PendingMutation::PrimaryDelete(PendingPrimaryDelete {
                tablet,
                keys: overlay_keys,
                locations: overlay_locations,
                spilled_delete_vector: None,
                deleted_at_command_id: command_id,
                durable: false,
            });
            let mutation = self.spill_delete_mutation_if_needed(txn_id, mutation, &mut inner)?;
            inner.mutations.push(mutation);
        }
        self.republish_stats_locked(&inner);
        Ok(())
    }

    pub fn add_row_id_delete(
        &self,
        txn_id: u64,
        command_id: CommandId,
        tablet: TabletRef,
        locations: Vec<PhysicalRowRef>,
    ) -> Result<()> {
        if locations.is_empty() {
            return Ok(());
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        self.ensure_mutable()?;
        let mutation = PendingMutation::RowIdDelete(PendingRowIdDelete {
            tablet,
            locations,
            spilled_delete_vector: None,
            deleted_at_command_id: command_id,
        });
        let mutation = self.spill_delete_mutation_if_needed(txn_id, mutation, &mut inner)?;
        inner.mutations.push(mutation);
        self.republish_stats_locked(&inner);
        Ok(())
    }

    pub fn register_art_columns(&self, tablet_id: u64, columns: Vec<u32>) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        self.ensure_mutable()?;
        inner
            .art_columns
            .entry(tablet_id)
            .or_default()
            .extend(columns);
        self.republish_stats_locked(&inner);
        Ok(())
    }

    pub(crate) fn with_tablet_writer<F, R>(
        &self,
        txn_id: u64,
        command_id: CommandId,
        read_ts: ReadTs,
        tablet: TabletRef,
        allocator: Arc<dyn Allocator>,
        search_write_context: SearchWriteContext,
        estimated_new_bytes: u64,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut DeltaWriter) -> Result<R>,
    {
        let tablet_id = tablet.tablet_id();
        let requested_key = WriterKey::new(command_id, tablet_id);

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        self.ensure_mutable()?;
        let key = inner
            .writers
            .keys()
            .copied()
            .find(|key| key.tablet_id == tablet_id)
            .unwrap_or(requested_key);

        if inner
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, PendingMutation::Rowset(p) if p.tablet.tablet_id() == tablet_id && p.created_at_command_id == command_id))
        {
            return Err(paro_error::not_supported(format!(
                "pending rowset already exists for tablet {} command {}",
                tablet_id,
                command_id.into_raw()
            )));
        }

        let estimated_before = self.estimated_memory_bytes_locked(&inner);
        let prior_writer_bytes = inner.writer_bytes.get(&key).copied().unwrap_or(0);
        let retained_elsewhere = estimated_before.saturating_sub(prior_writer_bytes);
        let estimated_projected = estimated_before.saturating_add(estimated_new_bytes);
        if self.is_over_budget(estimated_projected) {
            let current_spilled_bytes = Self::spilled_bytes_locked(&inner);
            inner
                .spill
                .preflight_foreground_spill(current_spilled_bytes, estimated_new_bytes)?;
        }

        let primary_key_overlay = if inner.writers.contains_key(&key) {
            None
        } else {
            Some(Self::primary_key_overlay_locked(
                &inner, tablet_id, command_id,
            ))
        };

        if let std::collections::btree_map::Entry::Vacant(entry) = inner.writers.entry(key) {
            let writer = DeltaWriter::open_transactional_with_allocator_and_search_context(
                tablet,
                txn_id,
                read_ts,
                allocator,
                search_write_context.clone(),
                primary_key_overlay.unwrap_or_default(),
            )?;
            entry.insert(writer);
        }

        let (result, retained_writer_bytes) = {
            let writer = inner
                .writers
                .get_mut(&key)
                .ok_or_else(|| paro_error::internal("failed to get pending writer"))?;
            writer.ensure_search_write_context(&search_write_context)?;
            let result = f(writer)?;
            let mut retained = writer.retained_memory_bytes();
            let actual_projected = retained_elsewhere.saturating_add(retained);
            if self.is_over_budget(actual_projected) {
                writer.relieve_memory_pressure()?;
                retained = writer
                    .retained_memory_bytes()
                    .max(SPILLED_WRITER_HANDLE_BYTES);
            }
            (result, retained)
        };
        inner.writer_bytes.insert(key, retained_writer_bytes);
        self.republish_stats_locked(&inner);
        Ok(result)
    }

    pub fn materialize_writers(&self) -> Result<()> {
        let _guard = self.materialize_lock.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock txn write buffer materializer: {e}"))
        })?;
        let keys = {
            let inner = self.inner.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock txn write buffer: {e}"))
            })?;
            inner.writers.keys().copied().collect::<Vec<_>>()
        };
        self.materialize_writer_keys_locked(keys)
    }

    pub(crate) fn immutable_overlay_snapshot_for_tablet(
        &self,
        tablet_id: u64,
        command_id: CommandId,
    ) -> Result<TxnOverlaySnapshot> {
        let _guard = self.materialize_lock.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock txn write buffer materializer: {e}"))
        })?;
        let visible_command = command_id.into_raw();
        let keys = {
            let inner = self.inner.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock txn write buffer: {e}"))
            })?;
            inner
                .writers
                .keys()
                .copied()
                .filter(|key| key.tablet_id == tablet_id && key.command_id < visible_command)
                .collect::<Vec<_>>()
        };
        self.materialize_writer_keys_locked(keys)?;
        self.overlay_snapshot_for_tablet_locked(tablet_id, command_id)
    }

    fn materialize_writer_keys_locked(&self, keys: Vec<WriterKey>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let pending = {
            let mut inner = self.inner.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock txn write buffer: {e}"))
            })?;
            let mut pending = BTreeMap::new();
            for key in keys {
                if let Some(writer) = inner.writers.remove(&key) {
                    pending.insert(key, writer);
                }
            }
            pending
        };

        for (key, writer) in pending {
            let txn_id = writer.txn_id();
            let (tablet, rowset, primary_update) = writer.finalize_for_transaction()?;
            let art_columns = self.art_columns_for(tablet.tablet_id())?;

            if tablet.state() == TabletState::Shutdown {
                let _ = std::fs::remove_dir_all(rowset.rowset_path());
                continue;
            }
            tablet.ensure_rowset_rssids(&rowset);

            if !art_columns.is_empty() {
                if let Err(err) =
                    RuntimeIndexes::rebuild_art_indexes_for_rowset(&rowset, &art_columns)
                {
                    tracing::warn!(
                        error = %err,
                        tablet_id = tablet.tablet_id(),
                        "ART index backfill failed for transaction rowset; queries will fallback to scan"
                    );
                }
            }

            let mut inner = self.inner.lock().map_err(|e| {
                paro_error::internal(format!("failed to lock txn write buffer: {e}"))
            })?;
            inner.writer_bytes.remove(&key);
            let mutation = self.rowset_mutation_with_governance(
                txn_id,
                key.command_id(),
                tablet,
                rowset.clone(),
                primary_update,
                &mut inner,
                true,
            )?;
            // Finalization converts already-admitted writer bytes into a rowset.
            // Rejecting here would consume the writer and lose staged rows.
            inner.mutations.push(mutation);
            self.republish_stats_locked(&inner);
        }
        Ok(())
    }

    fn overlay_snapshot_for_tablet_locked(
        &self,
        tablet_id: u64,
        command_id: CommandId,
    ) -> Result<TxnOverlaySnapshot> {
        let visible_command = command_id.into_raw();
        let inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        let mut snapshot = TxnOverlaySnapshot::default();
        for mutation in &inner.mutations {
            match mutation {
                PendingMutation::Rowset(pending)
                    if pending.tablet.tablet_id() == tablet_id
                        && pending.created_at_command_id.into_raw() < visible_command =>
                {
                    for (key, entry) in &pending.primary_key_overlay {
                        snapshot.primary_keys.insert(key.clone(), *entry);
                    }
                    snapshot.rowsets.push(pending.rowset.clone());
                }
                PendingMutation::PrimaryDelete(pending)
                    if pending.tablet.tablet_id() == tablet_id
                        && pending.deleted_at_command_id.into_raw() < visible_command =>
                {
                    for key in &pending.keys {
                        snapshot.primary_keys.insert(
                            key.clone(),
                            PendingPrimaryKeyEntry::tombstone(pending.deleted_at_command_id),
                        );
                    }
                    snapshot.row_id_deletes.extend(pending.overlay_locations()?);
                }
                PendingMutation::RowIdDelete(pending)
                    if pending.tablet.tablet_id() == tablet_id
                        && pending.deleted_at_command_id.into_raw() < visible_command =>
                {
                    snapshot.row_id_deletes.extend(pending.overlay_locations()?);
                }
                _ => {}
            }
        }
        Ok(snapshot)
    }

    pub fn take_graph_dml_deltas(&self) -> Result<HashMap<u64, GraphTableDmlDelta>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        let deltas = std::mem::take(&mut inner.graph_dml);
        self.republish_stats_locked(&inner);
        Ok(deltas)
    }

    pub fn record_graph_insert(&self, table_oid: u64, rows: usize) {
        if rows == 0 {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            let entry = inner.graph_dml.entry(table_oid).or_default();
            entry.inserted = entry.inserted.saturating_add(rows as u64);
            self.republish_stats_locked(&inner);
        }
    }

    pub fn record_graph_delete(&self, table_oid: u64, rows: usize) {
        if rows == 0 {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            let entry = inner.graph_dml.entry(table_oid).or_default();
            entry.deleted = entry.deleted.saturating_add(rows as u64);
            self.republish_stats_locked(&inner);
        }
    }

    pub fn record_graph_update(&self, table_oid: u64, rows: usize, updated_columns: &[u32]) {
        if rows == 0 {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            let entry = inner.graph_dml.entry(table_oid).or_default();
            entry.updated = entry.updated.saturating_add(rows as u64);
            entry
                .updated_columns
                .extend(updated_columns.iter().copied());
            self.republish_stats_locked(&inner);
        }
    }

    pub fn take_mutations(&self) -> Result<Vec<PendingMutation>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        let mutations = std::mem::take(&mut inner.mutations);
        self.republish_stats_locked(&inner);
        Ok(mutations)
    }

    /// Return the physical rowset volume that will be published per tablet.
    ///
    /// Commit admission runs before the durable journal append. Keeping this
    /// read-only snapshot on the write buffer avoids moving pending artifacts
    /// out of rollback ownership while a level-triggered search-maintenance
    /// gate may wait or reject the transaction.
    pub(crate) fn pending_rowset_volume_by_tablet(&self) -> Result<Vec<(TabletRef, u64, u64)>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        let mut volume_by_tablet = BTreeMap::<u64, (TabletRef, u64, u64)>::new();
        for mutation in &inner.mutations {
            let PendingMutation::Rowset(pending) = mutation else {
                continue;
            };
            let entry = volume_by_tablet
                .entry(pending.tablet.tablet_id())
                .or_insert_with(|| (Arc::clone(&pending.tablet), 0, 0));
            let rows = pending.rowset.num_rows();
            let on_disk = pending.rowset.total_disk_size();
            let bytes = if on_disk > 0 {
                on_disk
            } else {
                rows.saturating_mul(64)
            };
            entry.1 = entry.1.saturating_add(rows);
            entry.2 = entry.2.saturating_add(bytes);
        }
        Ok(volume_by_tablet.into_values().collect())
    }

    pub(crate) fn set_prepared(&self, prepared: PreparedStorageState) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        inner.prepared = Some(prepared);
        self.republish_stats_locked(&inner);
        Ok(())
    }

    pub(crate) fn take_prepared(&self) -> Result<Option<PreparedStorageState>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        let prepared = inner.prepared.take();
        self.republish_stats_locked(&inner);
        Ok(prepared)
    }

    pub fn rollback_writers(&self) {
        let pending = match self.inner.lock() {
            Ok(mut inner) => {
                inner.writer_bytes.clear();
                std::mem::take(&mut inner.writers)
            }
            Err(_) => return,
        };
        for (_key, writer) in pending {
            let _ = writer.cancel();
        }
        self.republish_stats();
    }

    pub fn rollback_mutations(&self) {
        let pending = match self.inner.lock() {
            Ok(mut inner) => {
                let mutations = std::mem::take(&mut inner.mutations);
                inner.art_columns.clear();
                inner.dml_tables.clear();
                inner.graph_dml.clear();
                inner.prepared.take();
                self.republish_stats_locked(&inner);
                mutations
            }
            Err(_) => Vec::new(),
        };
        for mutation in pending {
            Self::rollback_mutation(mutation);
        }
    }

    pub fn rollback_prepared(&self) {
        let prepared = match self.inner.lock() {
            Ok(mut inner) => {
                let prepared = inner.prepared.take();
                self.republish_stats_locked(&inner);
                prepared
            }
            Err(_) => None,
        };
        let Some(prepared) = prepared else {
            return;
        };
        for rowset in prepared.rowsets.into_iter().rev() {
            if let Some(artifact) = &rowset.staged_artifact {
                artifact.abandon_and_remove();
                continue;
            }
            let _ = std::fs::remove_dir_all(&rowset.rowset_path);
        }
        for delete in prepared.primary_deletes {
            if let Some(artifact) = &delete.spilled_delete_vector {
                artifact.abandon_and_remove();
            }
        }
        for delete in prepared.row_id_deletes {
            if let Some(artifact) = &delete.spilled_delete_vector {
                artifact.abandon_and_remove();
            }
        }
    }

    pub fn clear_after_commit(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.art_columns.clear();
            inner.dml_tables.clear();
            inner.graph_dml.clear();
            inner.writer_bytes.clear();
            self.republish_stats_locked(&inner);
        }
    }

    pub fn has_pending_storage_work(&self) -> bool {
        self.inner
            .lock()
            .map(|inner| {
                !inner.mutations.is_empty()
                    || !inner.writers.is_empty()
                    || !inner.graph_dml.is_empty()
                    || inner.prepared.is_some()
            })
            .unwrap_or(true)
    }

    fn ensure_mutable(&self) -> Result<()> {
        if self.is_frozen() {
            return Err(paro_error::invalid_transaction_state(
                "transaction write buffer is frozen",
            ));
        }
        Ok(())
    }

    fn rowset_mutation_with_governance(
        &self,
        txn_id: u64,
        command_id: CommandId,
        tablet: TabletRef,
        rowset: RowsetSharedPtr,
        primary_update: Option<PrimaryIndexUpdate>,
        inner: &mut TxnWriteBufferInner,
        preserve_on_stage_error: bool,
    ) -> Result<PendingMutation> {
        let mut mutation =
            Self::rowset_mutation(command_id, tablet.clone(), rowset, primary_update)?;
        let projected = self
            .estimated_memory_bytes_locked(inner)
            .saturating_add(mutation.estimated_memory_bytes());
        if self.is_over_budget(projected) {
            if let PendingMutation::Rowset(pending) = &mut mutation {
                let current_spilled_bytes = Self::spilled_bytes_locked(inner);
                match inner.spill.stage_rowset(
                    txn_id,
                    command_id,
                    &tablet,
                    &pending.rowset,
                    current_spilled_bytes,
                ) {
                    Ok(artifact) => {
                        pending.rowset.close()?;
                        pending.staged_artifact = Some(artifact);
                    }
                    Err(err) => {
                        if !preserve_on_stage_error {
                            Self::rollback_mutation(mutation);
                            return Err(err);
                        }
                        tracing::warn!(
                            error = %err,
                            tablet_id = tablet.tablet_id(),
                            command_id = command_id.into_raw(),
                            "transaction rowset recovery staging failed; retaining loaded rowset state"
                        );
                    }
                }
            }
            let projected_after_eviction = self
                .estimated_memory_bytes_locked(inner)
                .saturating_add(mutation.estimated_memory_bytes());
            if self.is_over_budget(projected_after_eviction) {
                if !preserve_on_stage_error {
                    Self::rollback_mutation(mutation);
                    return Err(paro_error::out_of_memory(format!(
                        "transaction write buffer budget exceeded after rowset eviction: projected={} bytes budget={} bytes",
                        projected_after_eviction,
                        self.memory_budget_bytes()
                    )));
                }
                tracing::warn!(
                    projected_bytes = projected_after_eviction,
                    budget_bytes = self.memory_budget_bytes(),
                    tablet_id = tablet.tablet_id(),
                    command_id = command_id.into_raw(),
                    "transaction write buffer retains irreducible rowset commit metadata above its waterline"
                );
            }
            return Ok(mutation);
        }
        Ok(mutation)
    }

    fn spill_delete_mutation_if_needed(
        &self,
        txn_id: u64,
        mut mutation: PendingMutation,
        inner: &mut TxnWriteBufferInner,
    ) -> Result<PendingMutation> {
        let current_memory_bytes = self.estimated_memory_bytes_locked(inner);
        let projected = current_memory_bytes.saturating_add(mutation.estimated_memory_bytes());
        if self.is_over_budget(projected) {
            let minimum_projected_after_spill = current_memory_bytes
                .saturating_add(mutation.minimum_memory_bytes_after_delete_spill());
            self.ensure_within_budget(minimum_projected_after_spill)?;

            let current_spilled_bytes = Self::spilled_bytes_locked(inner);
            match &mut mutation {
                PendingMutation::PrimaryDelete(pending) if !pending.locations.is_empty() => {
                    let artifact = inner.spill.stage_delete_vectors(
                        txn_id,
                        pending.deleted_at_command_id,
                        &pending.tablet,
                        &pending.locations,
                        current_spilled_bytes,
                    )?;
                    pending.locations.clear();
                    pending.spilled_delete_vector = Some(artifact);
                }
                PendingMutation::RowIdDelete(pending) if !pending.locations.is_empty() => {
                    let artifact = inner.spill.stage_delete_vectors(
                        txn_id,
                        pending.deleted_at_command_id,
                        &pending.tablet,
                        &pending.locations,
                        current_spilled_bytes,
                    )?;
                    pending.locations.clear();
                    pending.spilled_delete_vector = Some(artifact);
                }
                _ => {}
            }
        }
        if let Err(err) = self.ensure_within_budget(
            self.estimated_memory_bytes_locked(inner)
                .saturating_add(mutation.estimated_memory_bytes()),
        ) {
            Self::rollback_mutation(mutation);
            return Err(err);
        }
        Ok(mutation)
    }

    fn rowset_mutation(
        command_id: CommandId,
        tablet: TabletRef,
        rowset: RowsetSharedPtr,
        primary_update: Option<PrimaryIndexUpdate>,
    ) -> Result<PendingMutation> {
        let primary_key_overlay = if let Some(update) = primary_update.as_ref() {
            let row_ids = tablet.row_ids_for_rowset(&rowset)?;
            if row_ids.len() != update.written.len() {
                return Err(paro_error::internal(format!(
                    "row id count {} does not match written primary keys {}",
                    row_ids.len(),
                    update.written.len()
                )));
            }
            update
                .written
                .iter()
                .zip(row_ids.into_iter())
                .map(|((key, _), row_id)| {
                    let row_ref = tablet.decode_row_id(row_id)?;
                    Ok((
                        key.clone(),
                        PendingPrimaryKeyEntry::live(row_ref, row_id, command_id),
                    ))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };

        Ok(PendingMutation::Rowset(PendingRowset {
            tablet,
            rowset_path: rowset.rowset_path().to_path_buf(),
            rowset,
            primary_update,
            primary_key_overlay,
            created_at_command_id: command_id,
            staged_artifact: None,
        }))
    }

    fn primary_key_overlay_locked(
        inner: &TxnWriteBufferInner,
        tablet_id: u64,
        command_id: CommandId,
    ) -> HashMap<Vec<u8>, Option<RowID>> {
        let visible_command = command_id.into_raw();
        let mut overlay = HashMap::new();
        for mutation in &inner.mutations {
            match mutation {
                PendingMutation::Rowset(pending)
                    if pending.tablet.tablet_id() == tablet_id
                        && pending.created_at_command_id.into_raw() < visible_command =>
                {
                    for (key, entry) in &pending.primary_key_overlay {
                        overlay.insert(key.clone(), entry.row_id);
                    }
                }
                PendingMutation::PrimaryDelete(pending)
                    if pending.tablet.tablet_id() == tablet_id
                        && pending.deleted_at_command_id.into_raw() < visible_command =>
                {
                    for key in &pending.keys {
                        overlay.insert(key.clone(), None);
                    }
                }
                _ => {}
            }
        }
        overlay
    }

    fn art_columns_for(&self, tablet_id: u64) -> Result<Vec<u32>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| paro_error::internal(format!("failed to lock txn write buffer: {e}")))?;
        Ok(inner
            .art_columns
            .get(&tablet_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect())
    }

    fn rollback_mutation(mutation: PendingMutation) {
        match mutation {
            PendingMutation::Rowset(rowset) => {
                if let Some(artifact) = &rowset.staged_artifact {
                    artifact.abandon_and_remove();
                    return;
                }
                let _ = std::fs::remove_dir_all(&rowset.rowset_path);
            }
            PendingMutation::PrimaryDelete(delete) => {
                if let Some(artifact) = &delete.spilled_delete_vector {
                    artifact.abandon_and_remove();
                }
            }
            PendingMutation::RowIdDelete(delete) => {
                if let Some(artifact) = &delete.spilled_delete_vector {
                    artifact.abandon_and_remove();
                }
            }
        }
    }

    fn estimated_memory_bytes_locked(&self, inner: &TxnWriteBufferInner) -> u64 {
        let mutation_bytes = inner.mutations.iter().fold(0_u64, |acc, mutation| {
            acc.saturating_add(mutation.estimated_memory_bytes())
        });
        let writer_bytes = inner
            .writer_bytes
            .values()
            .fold(0_u64, |acc, bytes| acc.saturating_add(*bytes));
        let art_bytes = inner
            .art_columns
            .iter()
            .fold(0_u64, |acc, (_tablet, cols)| {
                acc.saturating_add(32 + (cols.len() as u64).saturating_mul(4))
            });
        let graph_bytes = inner.graph_dml.len() as u64 * 96;
        mutation_bytes
            .saturating_add(writer_bytes)
            .saturating_add(art_bytes)
            .saturating_add(graph_bytes)
    }

    fn mutation_count_locked(&self, inner: &TxnWriteBufferInner) -> u64 {
        (inner.mutations.len() + inner.writers.len()) as u64
    }

    fn ensure_within_budget(&self, projected_bytes: u64) -> Result<()> {
        let budget = self.memory_budget_bytes();
        if budget > 0 && projected_bytes > budget {
            return Err(paro_error::out_of_memory(format!(
                "transaction write buffer budget exceeded: projected={} bytes budget={} bytes",
                projected_bytes, budget
            )));
        }
        Ok(())
    }

    fn is_over_budget(&self, projected_bytes: u64) -> bool {
        let budget = self.memory_budget_bytes();
        budget > 0 && projected_bytes > budget
    }

    fn spilled_bytes_locked(inner: &TxnWriteBufferInner) -> u64 {
        let mutation_bytes = inner.mutations.iter().fold(0_u64, |acc, mutation| {
            acc.saturating_add(match mutation {
                PendingMutation::Rowset(pending) => pending
                    .staged_artifact
                    .as_ref()
                    .map(StagedRowsetArtifact::admitted_bytes)
                    .unwrap_or(0),
                PendingMutation::PrimaryDelete(pending) => pending
                    .spilled_delete_vector
                    .as_ref()
                    .map(StagedDeleteVectorArtifact::admitted_bytes)
                    .unwrap_or(0),
                PendingMutation::RowIdDelete(pending) => pending
                    .spilled_delete_vector
                    .as_ref()
                    .map(StagedDeleteVectorArtifact::admitted_bytes)
                    .unwrap_or(0),
            })
        });
        let prepared_bytes = inner.prepared.as_ref().map_or(0_u64, |prepared| {
            let rowset_bytes = prepared.rowsets.iter().fold(0_u64, |acc, pending| {
                acc.saturating_add(
                    pending
                        .staged_artifact
                        .as_ref()
                        .map(StagedRowsetArtifact::admitted_bytes)
                        .unwrap_or(0),
                )
            });
            let primary_bytes = prepared.primary_deletes.iter().fold(0_u64, |acc, pending| {
                acc.saturating_add(
                    pending
                        .spilled_delete_vector
                        .as_ref()
                        .map(StagedDeleteVectorArtifact::admitted_bytes)
                        .unwrap_or(0),
                )
            });
            let row_id_bytes = prepared.row_id_deletes.iter().fold(0_u64, |acc, pending| {
                acc.saturating_add(
                    pending
                        .spilled_delete_vector
                        .as_ref()
                        .map(StagedDeleteVectorArtifact::admitted_bytes)
                        .unwrap_or(0),
                )
            });
            rowset_bytes
                .saturating_add(primary_bytes)
                .saturating_add(row_id_bytes)
        });
        mutation_bytes.saturating_add(prepared_bytes)
    }

    fn republish_stats(&self) {
        if let Ok(inner) = self.inner.lock() {
            self.republish_stats_locked(&inner);
        }
    }

    fn republish_stats_locked(&self, inner: &TxnWriteBufferInner) {
        self.memory_usage_bytes
            .store(self.estimated_memory_bytes_locked(inner), Ordering::Release);
        self.mutation_count
            .store(self.mutation_count_locked(inner), Ordering::Release);
    }
}

#[derive(Debug)]
pub struct StorageTxnState {
    participant_id: ParticipantId,
    resource_key: TxnResourceKey,
    write_buffer: Arc<TxnWriteBuffer>,
}

impl StorageTxnState {
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            participant_id: ParticipantId::new(1),
            resource_key: TxnResourceKey::database(ParticipantKind::Storage, database_id),
            write_buffer: Arc::new(TxnWriteBuffer::new_for_database(
                database_id,
                DEFAULT_TXN_WRITE_BUFFER_MEMORY_BUDGET,
            )),
        }
    }

    #[inline]
    pub fn write_buffer(&self) -> Arc<TxnWriteBuffer> {
        Arc::clone(&self.write_buffer)
    }

    pub fn write_buffer_from_view(view: &TransactionView) -> Option<Arc<TxnWriteBuffer>> {
        view.participant_states().iter().find_map(|state| {
            state
                .as_any()
                .downcast_ref::<StorageTxnState>()
                .map(StorageTxnState::write_buffer)
        })
    }
}

impl TxnParticipantState for StorageTxnState {
    fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    fn participant_kind(&self) -> ParticipantKind {
        ParticipantKind::Storage
    }

    fn resource_key(&self) -> TxnResourceKey {
        self.resource_key
    }

    fn estimated_bytes(&self) -> usize {
        self.write_buffer.memory_usage_bytes() as usize
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_budget_is_derived_from_session_contract() {
        assert_eq!(
            transaction_write_buffer_memory_budget(0),
            DEFAULT_TXN_WRITE_BUFFER_MEMORY_BUDGET
        );
        assert_eq!(
            transaction_write_buffer_memory_budget(32 * 1024 * 1024),
            32 * 1024 * 1024
        );
        assert_eq!(
            transaction_write_buffer_memory_budget(8 * 1024 * 1024 * 1024),
            1024 * 1024 * 1024
        );
        assert_eq!(
            transaction_write_buffer_memory_budget(usize::MAX),
            MAX_TXN_WRITE_BUFFER_MEMORY_BUDGET
        );
    }
}
