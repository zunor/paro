// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bridges storage-level WAL replay with catalog and runtime recovery.

use super::apply_engine::ApplyEngine;
use super::consistency_report::{
    build_recovery_consistency_report, log_recovery_consistency_report,
};
use super::ddl::{catalog_apply_phase, route_registry_table_keys, CatalogApplyPhase};
use super::index_restore::{reconcile_fulltext_index_coverage, restore_runtime_art_indexes};
use super::registry::RouteRegistry;
use crate::database::wal_observability::WalReplayCounters;
use paro_catalog::collection::{CatalogReplaySummary, InstallMode};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CreateSchemaInfo, OnCreateConflict};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::mvcc::REPLAY_WRITER_ID;
use paro_common::ddl::{DdlChange, DdlChangeRecord};
#[cfg(test)]
use paro_common::effect::{
    encode_delete_patch_artifact_bytes, DeletePatchEncoding, DeletePatchGroup, DeletePatchInline,
    DeletePatchRef, DeletePatchSegment,
};
use paro_common::effect::{
    ApplyDescriptor, ArtifactNamespace, ArtifactRef, DeferredTask, StorageCommitOp, TabletApplyOp,
    TabletMutation, VersionSpan,
};
use paro_common::error as paro_error;
use paro_common::journal::{CheckpointFence, CommitRecord, MaintenanceRecord, RecoverySummary};
use paro_common::logging::targets;
use paro_storage::index::graph::GraphProjectionIndexManager;
use paro_storage::meta::TabletMetaManager;
use paro_storage::wal::recovery::{ReplayHandler, WalRecovery};
use paro_storage::wal::replay_state::ReplayResult;
use paro_storage::wal::wal_entry::WalHeaderMetadata;
use paro_storage::wal::write_ahead_log::WriteAheadLog;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Replay handler that applies WAL entries to a Catalog.
///
/// This handler is used during database startup to replay WAL entries
/// and restore the catalog to a consistent state.
pub trait RuntimeCatalogApplyBatch {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn record(&self, index: usize) -> &DdlChangeRecord;
    fn apply(&mut self, index: usize, commit_id: u64) -> paro_common::error::Result<()>;
}

#[cfg(test)]
struct RowsetCommitReplay<'a> {
    tablet_id: u64,
    rowset_id: u64,
    version_span: VersionSpan,
    rowset_path: &'a str,
    replaced_locations: &'a [(u64, u32, u32)],
    lsn: u64,
}

pub struct CatalogReplayHandler<'a> {
    /// The catalog to apply entries to
    pub(super) catalog: &'a Arc<ParoCatalog>,
    /// Transaction for replay operations
    pub(super) transaction: CatalogSnapshot,
    /// Database root used for staged-artifact publish and cleanup descriptors.
    pub(super) database_root: PathBuf,
    /// Persistent tablet metadata state used to hide shutdown tablets from startup manifest.
    pub(super) tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    /// Highest object id observed in replayed WAL create payloads.
    pub(super) max_seen_object_id: u64,
    /// Highest committed catalog timestamp installed during replay.
    pub(super) max_catalog_commit_id: u64,
    /// Fast routing registry used by runtime apply and recovery replay.
    pub(super) registry: RouteRegistry,
    /// Optional graph runtime registry used by live apply paths.
    pub(super) graph_registry: Option<Arc<GraphProjectionIndexManager>>,
    /// Deferred tasks recovered from durable journal records for startup redelivery.
    pub(super) replayed_deferred_tasks: Vec<DeferredTask>,
    /// Durable replay summary used to bootstrap allocators and frontiers.
    pub(super) recovery_summary: RecoverySummary,
    /// Optional startup replay counters for observability export.
    pub(super) replay_counters: Option<Arc<WalReplayCounters>>,
}

impl<'a> CatalogReplayHandler<'a> {
    /// Create a new catalog replay handler.
    pub fn new(catalog: &'a Arc<ParoCatalog>, txn_id: u64, commit_ts: u64) -> Self {
        let transaction = if txn_id >= REPLAY_WRITER_ID {
            CatalogSnapshot::writer(txn_id, commit_ts)
        } else {
            CatalogSnapshot::replay_writer(commit_ts)
        };
        let registry = RouteRegistry::from_catalog(catalog).unwrap_or_else(|error| {
            tracing::warn!(
                target: targets::INSTANCE,
                error = %error,
                "failed to bootstrap replay route registry from catalog; starting empty"
            );
            RouteRegistry::default()
        });
        Self {
            catalog,
            transaction,
            database_root: PathBuf::new(),
            tablet_meta_manager: None,
            max_seen_object_id: 0,
            max_catalog_commit_id: 0,
            registry,
            graph_registry: None,
            replayed_deferred_tasks: Vec::new(),
            recovery_summary: RecoverySummary::default(),
            replay_counters: None,
        }
    }

    pub fn with_database_root(mut self, database_root: PathBuf) -> Self {
        self.database_root = database_root;
        self
    }

    pub fn with_tablet_meta_manager(
        mut self,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Self {
        self.tablet_meta_manager = tablet_meta_manager;
        self
    }

    pub fn with_registry(mut self, registry: RouteRegistry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_graph_registry(mut self, graph_registry: Arc<GraphProjectionIndexManager>) -> Self {
        self.graph_registry = Some(graph_registry);
        self
    }

    pub(crate) fn with_replay_counters(mut self, replay_counters: Arc<WalReplayCounters>) -> Self {
        self.replay_counters = Some(replay_counters);
        self
    }

    pub(super) fn observe_object_id(&mut self, object_id: u64) {
        self.max_seen_object_id = self.max_seen_object_id.max(object_id);
    }

    pub(super) fn observe_catalog_commit_id(
        &mut self,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        if commit_id == 0 || commit_id >= paro_storage::transaction::manager::TRANSACTION_ID_START {
            return Err(paro_error::serialization_error(format!(
                "replayed catalog commit timestamp must be in committed range, got {}",
                commit_id
            )));
        }
        self.max_catalog_commit_id = self.max_catalog_commit_id.max(commit_id);
        Ok(())
    }

    pub(super) fn replay_storage_op(
        &mut self,
        op: &StorageCommitOp,
        lsn: u64,
    ) -> paro_common::error::Result<()> {
        self.apply_effects(std::slice::from_ref(op), &[], lsn, 0)
    }

    #[cfg(test)]
    pub(super) fn replay_primary_delete(
        &mut self,
        tablet_id: u64,
        keys: &[Vec<u8>],
        lsn: u64,
    ) -> paro_common::error::Result<()> {
        let route = self
            .registry
            .route_tablet(tablet_id)
            .cloned()
            .ok_or_else(|| {
                paro_error::serialization_error(format!(
                    "tablet {} missing from recovery registry",
                    tablet_id
                ))
            })?;
        let resolved = route.storage.tablet().lookup_primary_keys(keys)?;
        let mut locations = Vec::new();
        for row_id in resolved.into_iter().flatten() {
            locations.push(route.storage.tablet().decode_row_id(row_id)?);
        }
        let patch = Self::inline_delete_patch(&locations);
        let mutation = TabletMutation::ApplyDeletePatch {
            deleted_row_count: patch.row_count(),
            patch,
        };
        // Test-only helper: model a committed delete patch by reusing the durable lsn
        // as visibility when no full CommitRecord wrapper is needed.
        self.apply_effects(
            &[StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id,
                mutations: vec![mutation],
            })],
            &[],
            lsn,
            lsn.max(1),
        )
    }

    #[cfg(test)]
    fn replay_rowset_commit(
        &mut self,
        replay: RowsetCommitReplay<'_>,
    ) -> paro_common::error::Result<()> {
        let mut mutations = Vec::new();
        if !replay.replaced_locations.is_empty() {
            let patch = Self::inline_delete_patch(
                &replay
                    .replaced_locations
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect::<Vec<_>>(),
            );
            mutations.push(TabletMutation::ApplyDeletePatch {
                deleted_row_count: patch.row_count(),
                patch,
            });
        }
        mutations.push(TabletMutation::PublishRowset {
            rowset_id: replay.rowset_id,
            version_span: replay.version_span,
            rowset_ref: self
                .artifact_ref_for_tablet_path(replay.tablet_id, Path::new(replay.rowset_path))?,
        });
        self.replay_storage_op(
            &StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: replay.tablet_id,
                mutations,
            }),
            replay.lsn,
        )
    }

    pub(super) fn replay_tablet_mutation(
        &mut self,
        tablet_id: u64,
        mutation: &TabletMutation,
        lsn: u64,
    ) -> paro_common::error::Result<()> {
        self.replay_storage_op(
            &StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id,
                mutations: vec![mutation.clone()],
            }),
            lsn,
        )
    }

    #[cfg(test)]
    fn inline_delete_patch(locations: &[paro_storage::tablet::PhysicalRowRef]) -> DeletePatchRef {
        let mut grouped =
            std::collections::BTreeMap::<u64, std::collections::BTreeMap<u32, Vec<u32>>>::new();
        for location in locations {
            grouped
                .entry(location.rowset_id)
                .or_default()
                .entry(location.segment_id)
                .or_default()
                .push(location.row_offset);
        }
        let groups = grouped
            .into_iter()
            .map(|(rowset_id, segments)| DeletePatchGroup {
                rowset_id,
                segments: segments
                    .into_iter()
                    .map(|(segment_id, offsets)| {
                        let mut previous = 0u32;
                        let mut encoded = Vec::with_capacity(offsets.len());
                        for (index, row_offset) in offsets.into_iter().enumerate() {
                            if index == 0 {
                                encoded.push(row_offset);
                            } else {
                                encoded.push(row_offset - previous);
                            }
                            previous = row_offset;
                        }
                        DeletePatchSegment {
                            segment_id,
                            row_offsets_delta: encoded,
                        }
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        DeletePatchRef::Inline(DeletePatchInline {
            encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
            row_count: locations.len() as u32,
            groups,
        })
    }

    pub(super) fn install_replayed_entry(
        &mut self,
        collection: &paro_catalog::collection::CatalogCollection,
        commit_id: u64,
        entry: Arc<CatalogEntryEnum>,
        mode: InstallMode,
    ) -> paro_common::error::Result<()> {
        collection.install_replayed(commit_id, entry, mode)?;
        self.observe_catalog_commit_id(commit_id)
    }

    pub(super) fn publish_catalog_handle(
        &mut self,
        handle: paro_catalog::collection::StagedCatalogMutation,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        handle.publish(commit_id)?;
        self.observe_catalog_commit_id(commit_id)
    }

    pub fn summary(&self) -> CatalogReplaySummary {
        CatalogReplaySummary {
            max_catalog_commit_id: self.max_catalog_commit_id,
            max_seen_object_id: self.max_seen_object_id,
        }
    }

    pub fn replayed_deferred_tasks(&self) -> &[DeferredTask] {
        &self.replayed_deferred_tasks
    }

    pub fn recovery_summary(&self) -> RecoverySummary {
        RecoverySummary {
            max_lsn: self.recovery_summary.max_lsn,
            max_commit_id: self.recovery_summary.max_commit_id,
            max_maintenance_id: self.recovery_summary.max_maintenance_id,
            max_catalog_commit_id: self.max_catalog_commit_id,
            max_seen_object_id: self.max_seen_object_id,
        }
    }

    fn observe_replayed_lsn(&mut self, lsn: u64) {
        self.recovery_summary.max_lsn = self.recovery_summary.max_lsn.max(lsn);
    }

    fn finalize_object_id_allocator(&self) -> paro_common::error::Result<()> {
        if self.max_seen_object_id == 0 {
            return Ok(());
        }
        let next_object_id = self.max_seen_object_id.checked_add(1).ok_or_else(|| {
            paro_error::serialization_error(format!(
                "replayed object id {} overflowed allocator watermark",
                self.max_seen_object_id
            ))
        })?;
        self.catalog.bump_object_id_allocator(next_object_id);
        Ok(())
    }

    pub(super) fn ensure_schema(
        &mut self,
        schema_name: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<Arc<paro_catalog::entry::SchemaEntry>> {
        match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => Ok(schema),
            Err(_) => {
                let info = CreateSchemaInfo {
                    catalog: self.catalog.name().to_string(),
                    name: schema_name.to_string(),
                    internal: false,
                    on_conflict: OnCreateConflict::IgnoreOnConflict,
                };
                let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
                    paro_catalog::entry::SchemaEntry::from_info(
                        &info,
                        self.catalog.gc_epoch_handle(),
                        0,
                    ),
                )));
                self.install_replayed_entry(
                    self.catalog.get_schema_collection(),
                    commit_id,
                    entry,
                    InstallMode::RejectExisting,
                )?;
                self.catalog.get_schema(&self.transaction, schema_name)
            }
        }
    }

    fn apply_runtime_catalog_phase<T: RuntimeCatalogApplyBatch>(
        &mut self,
        batch: &mut T,
        commit_id: u64,
        phase: CatalogApplyPhase,
    ) -> paro_common::error::Result<()> {
        for index in 0..batch.len() {
            if catalog_apply_phase(batch.record(index)) != phase {
                continue;
            }
            batch.apply(index, commit_id)?;
            self.observe_catalog_commit_id(commit_id)?;
        }
        Ok(())
    }

    fn apply_runtime_catalog_non_drop<T: RuntimeCatalogApplyBatch>(
        &mut self,
        batch: &mut T,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        self.apply_runtime_catalog_phase(batch, commit_id, CatalogApplyPhase::Create)?;
        self.apply_runtime_catalog_phase(batch, commit_id, CatalogApplyPhase::Alter)
    }

    fn apply_runtime_catalog_drop<T: RuntimeCatalogApplyBatch>(
        &mut self,
        batch: &mut T,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        self.apply_runtime_catalog_phase(batch, commit_id, CatalogApplyPhase::Drop)
    }

    fn sync_route_registry_for_catalog_ops(
        &mut self,
        ops: &[paro_common::effect::CatalogTxnOp],
        phase: CatalogApplyPhase,
    ) -> paro_common::error::Result<()> {
        let mut targets = HashSet::new();
        for op in ops {
            if catalog_apply_phase(&op.change) != phase {
                continue;
            }
            let route_keys = match &op.change.change {
                DdlChange::DropSchema(_) if phase == CatalogApplyPhase::Drop => self
                    .registry
                    .table_keys_in_schema(&op.change.key.database, &op.change.key.name),
                _ => route_registry_table_keys(&op.change, self.catalog.name())?,
            };
            for key in route_keys {
                targets.insert(key);
            }
        }
        for target in targets {
            self.registry
                .sync_table_from_catalog(self.catalog, &target)?;
        }
        Ok(())
    }

    fn apply_effects(
        &mut self,
        storage_ops: &[StorageCommitOp],
        descriptors: &[ApplyDescriptor],
        lsn: u64,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        if storage_ops.is_empty() && descriptors.is_empty() {
            return Ok(());
        }
        ApplyEngine::new(self).apply_effects(storage_ops, descriptors, lsn, commit_id)
    }

    pub fn apply_runtime_commit_record<T: RuntimeCatalogApplyBatch>(
        &mut self,
        record: &CommitRecord,
        lsn: u64,
        catalog_batch: &mut T,
    ) -> paro_common::error::Result<()> {
        self.apply_runtime_catalog_non_drop(catalog_batch, record.commit_id)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Create)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Alter)?;
        self.apply_effects(
            &record.storage_ops,
            &record.apply_descriptors,
            lsn,
            record.commit_id,
        )?;
        self.apply_runtime_catalog_drop(catalog_batch, record.commit_id)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Drop)?;
        Ok(())
    }

    fn apply_recovered_commit_record(
        &mut self,
        record: &CommitRecord,
        lsn: u64,
    ) -> paro_common::error::Result<()> {
        self.replay_catalog_non_drop_ops(&record.catalog_ops, record.commit_id)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Create)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Alter)?;
        self.apply_effects(
            &record.storage_ops,
            &record.apply_descriptors,
            lsn,
            record.commit_id,
        )?;
        self.replay_catalog_drop_ops(&record.catalog_ops, record.commit_id)?;
        self.replayed_deferred_tasks
            .extend(record.deferred_tasks.iter().cloned());
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Drop)?;
        Ok(())
    }

    fn apply_recovered_maintenance_record(
        &mut self,
        record: &MaintenanceRecord,
        lsn: u64,
    ) -> paro_common::error::Result<()> {
        self.replay_catalog_non_drop_ops(&record.catalog_ops, 0)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Create)?;
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Alter)?;
        self.apply_effects(&record.storage_ops, &record.apply_descriptors, lsn, 0)?;
        self.replay_catalog_drop_ops(&record.catalog_ops, 0)?;
        self.replayed_deferred_tasks
            .extend(record.deferred_tasks.iter().cloned());
        self.sync_route_registry_for_catalog_ops(&record.catalog_ops, CatalogApplyPhase::Drop)?;
        Ok(())
    }

    fn artifact_ref_for_tablet_path(
        &self,
        tablet_id: u64,
        path: &Path,
    ) -> paro_common::error::Result<ArtifactRef> {
        let route = self.registry.route_tablet(tablet_id).ok_or_else(|| {
            paro_error::internal(format!(
                "tablet {} not mapped in route registry during replay",
                tablet_id
            ))
        })?;
        ArtifactRef::from_tablet_path(route.storage.tablet().data_dir(), path)
    }
}

impl<'a> ReplayHandler for CatalogReplayHandler<'a> {
    fn replay_commit_record(
        &mut self,
        lsn: u64,
        record: &CommitRecord,
    ) -> paro_common::error::Result<()> {
        self.observe_replayed_lsn(lsn);
        self.recovery_summary.max_commit_id =
            self.recovery_summary.max_commit_id.max(record.commit_id);
        self.apply_recovered_commit_record(record, lsn)
    }

    fn replay_maintenance_record(
        &mut self,
        lsn: u64,
        record: &MaintenanceRecord,
    ) -> paro_common::error::Result<()> {
        self.observe_replayed_lsn(lsn);
        self.recovery_summary.max_maintenance_id = self
            .recovery_summary
            .max_maintenance_id
            .max(record.maintenance_id);
        self.apply_recovered_maintenance_record(record, lsn)
    }

    fn replay_checkpoint_fence(
        &mut self,
        lsn: u64,
        fence: &CheckpointFence,
    ) -> paro_common::error::Result<()> {
        self.observe_replayed_lsn(lsn);
        tracing::info!(
            target: targets::INSTANCE,
            lsn,
            checkpoint_marker = fence.checkpoint_marker,
            "Checkpoint fence found during replay"
        );
        Ok(())
    }

    fn on_checkpoint(&mut self, checkpoint_marker: u64) -> paro_common::error::Result<()> {
        tracing::info!(
            target: targets::INSTANCE,
            checkpoint_marker = checkpoint_marker,
            "Checkpoint marker found during replay"
        );
        Ok(())
    }

    fn replay_compaction_publish(
        &mut self,
        tablet_id: u64,
        plan_id: u64,
        job_id: u64,
        output_rowset_id: u64,
        output_start_version: i64,
        output_end_version: i64,
        cumulative_point_action: paro_storage::compaction::plan::types::CumulativePointAction,
        output_rowset_path: &str,
        replaced_inputs: &[u64],
    ) -> paro_common::error::Result<()> {
        self.replay_tablet_mutation(
            tablet_id,
            &TabletMutation::PublishCompaction {
                plan_id,
                job_id,
                output_rowset_id,
                output_version: VersionSpan {
                    start: output_start_version,
                    end: output_end_version,
                },
                staged_ref: ArtifactRef {
                    namespace: ArtifactNamespace::Staged,
                    locator: Vec::new(),
                },
                output_ref: self.artifact_ref_for_tablet_path(
                    tablet_id,
                    Path::new(output_rowset_path),
                )?,
                replaced_inputs: replaced_inputs.to_vec(),
                retired_inputs: Vec::new(),
                cumulative_point_action: match cumulative_point_action {
                    paro_storage::compaction::plan::types::CumulativePointAction::Preserve => {
                        paro_common::effect::CompactionCumulativePointAction::Preserve
                    }
                    paro_storage::compaction::plan::types::CumulativePointAction::AdvanceToOutputEndExclusive => {
                        paro_common::effect::CompactionCumulativePointAction::AdvanceToOutputEndExclusive
                    }
                },
            },
            0,
        )
    }
}

/// Recover a database from its WAL.
///
/// This function:
/// 1. Opens the WAL file for the database
/// 2. Replays all entries to restore the catalog
/// 3. Returns the WAL for continued use
///
/// # Arguments
/// * `wal_path` - Path to the WAL file
/// * `catalog` - The catalog to restore
///
/// # Returns
/// * `Ok((wal, result, summary))` - Recovery completed successfully
/// * `Err(...)` - Fatal error during recovery
pub fn recover_database(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
) -> paro_common::error::Result<(
    WriteAheadLog,
    ReplayResult,
    CatalogReplaySummary,
    RecoverySummary,
    Vec<DeferredTask>,
)> {
    recover_database_observed(wal_path, catalog, tablet_meta_manager, None)
}

pub(crate) fn recover_database_observed(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    replay_counters: Option<Arc<WalReplayCounters>>,
) -> paro_common::error::Result<(
    WriteAheadLog,
    ReplayResult,
    CatalogReplaySummary,
    RecoverySummary,
    Vec<DeferredTask>,
)> {
    let recovery = WalRecovery::new(wal_path);

    // Use a dedicated replay writer identity and a maximally-open snapshot so
    // replay can stage mutations while still honoring committed visibility
    // boundaries when publishing.
    let mut handler = CatalogReplayHandler::new(catalog, 0, u64::MAX)
        .with_database_root(
            wal_path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_path_buf(),
        )
        .with_tablet_meta_manager(tablet_meta_manager)
        .with_registry(RouteRegistry::from_catalog(catalog)?);
    if let Some(replay_counters) = replay_counters {
        handler = handler.with_replay_counters(replay_counters);
    }
    let recovered = recovery.recover(&mut handler)?;
    let summary = handler.summary();
    let recovery_summary = handler.recovery_summary();
    let deferred_tasks = handler.replayed_deferred_tasks().to_vec();
    handler.finalize_object_id_allocator()?;
    catalog.rebuild_dependency_graph()?;
    restore_runtime_art_indexes(catalog);
    reconcile_fulltext_index_coverage(catalog);
    let report = build_recovery_consistency_report(catalog);
    log_recovery_consistency_report(&report);
    Ok((
        recovered.0,
        recovered.1,
        summary,
        recovery_summary,
        deferred_tasks,
    ))
}

/// Recover a database from its WAL with checkpoint coordination.
///
/// This function:
/// 1. Checks if checkpoint marker matches WAL checkpoint marker
/// 2. If they match, skips WAL replay (checkpoint was successful)
/// 3. Otherwise, replays WAL entries to restore the catalog
///
/// # Arguments
/// * `wal_path` - Path to the WAL file
/// * `catalog` - The catalog to restore
/// * `checkpoint_marker` - Optional checkpoint marker from metadata store
///
/// # Returns
/// * `Ok((wal, result))` - Recovery completed successfully
/// * `Err(...)` - Fatal error during recovery
pub fn recover_database_with_checkpoint(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    checkpoint_marker: Option<u64>,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: Option<u64>,
) -> paro_common::error::Result<(
    WriteAheadLog,
    ReplayResult,
    CatalogReplaySummary,
    RecoverySummary,
    Vec<DeferredTask>,
)> {
    recover_database_with_checkpoint_observed(
        wal_path,
        catalog,
        tablet_meta_manager,
        checkpoint_marker,
        wal_header_metadata,
        wal_keep_from,
        None,
    )
}

pub(crate) fn recover_database_with_checkpoint_observed(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    checkpoint_marker: Option<u64>,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: Option<u64>,
    replay_counters: Option<Arc<WalReplayCounters>>,
) -> paro_common::error::Result<(
    WriteAheadLog,
    ReplayResult,
    CatalogReplaySummary,
    RecoverySummary,
    Vec<DeferredTask>,
)> {
    let mut recovery = WalRecovery::new(wal_path);

    // If we have a checkpoint marker, use it for verification.
    if let Some(marker) = checkpoint_marker {
        recovery = recovery.with_checkpoint_marker(marker);
    }

    if let Some(metadata) = wal_header_metadata {
        recovery = recovery
            .with_wal_header_metadata(metadata.db_identifier, metadata.checkpoint_iteration);
    }

    if let Some(keep_from) = wal_keep_from {
        recovery = recovery.with_wal_keep_from(keep_from);
    }

    // Use a dedicated replay writer identity and a maximally-open snapshot.
    let mut handler = CatalogReplayHandler::new(catalog, 0, u64::MAX)
        .with_database_root(
            wal_path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_path_buf(),
        )
        .with_tablet_meta_manager(tablet_meta_manager)
        .with_registry(RouteRegistry::from_catalog(catalog)?);
    if let Some(replay_counters) = replay_counters {
        handler = handler.with_replay_counters(replay_counters);
    }
    let recovered = recovery.recover(&mut handler)?;
    let summary = handler.summary();
    let recovery_summary = handler.recovery_summary();
    let deferred_tasks = handler.replayed_deferred_tasks().to_vec();
    handler.finalize_object_id_allocator()?;
    catalog.rebuild_dependency_graph()?;
    restore_runtime_art_indexes(catalog);
    reconcile_fulltext_index_coverage(catalog);
    let report = build_recovery_consistency_report(catalog);
    log_recovery_consistency_report(&report);
    Ok((
        recovered.0,
        recovered.1,
        summary,
        recovery_summary,
        deferred_tasks,
    ))
}

/// Check if a WAL file exists and needs recovery.
pub fn needs_recovery(wal_path: &Path) -> bool {
    let report = paro_storage::wal::recovery::wal_health_check_read_only(wal_path);

    // Recover whenever any WAL stream exists so startup can consume checkpoint/recovery
    // artifacts and clean up stale files, even when main WAL is empty or absent.
    if report.main_wal.exists && report.main_wal.size_bytes > 0 {
        return true;
    }
    if report.checkpoint_wal.exists || report.recovery_wal.exists {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::database_catalog::ParoCatalog;
    use paro_catalog::entry::CatalogObjectId;
    use paro_catalog::entry::{
        CatalogEntryEnum, CatalogType, ColumnDefinition, CreateIndexInfo, CreateSchemaInfo,
        CreateSequenceInfo, CreateTableInfo, IndexBuildState, IndexCatalogEntry, IndexType,
        LogicalIndex, OnCreateConflict, SequenceCatalogEntry, TableCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::chunk::Chunk;
    use paro_common::ddl::{
        CreateIndexPayload, CreatePropertyGraphPayload, CreateSchemaPayload, CreateSequencePayload,
        CreateTablePayload, CreateViewPayload, DdlChange, DdlChangeRecord, DdlDependencyObjectRef,
        DdlDependencyRef, DdlObjectKey, DdlObjectKind, DdlStorageDescriptor, DdlWalColumnInfo,
        PropertyGraphVertexPayload,
    };
    use paro_common::effect::CatalogTxnOp;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_storage::primary_key::PrimaryKeySerializer;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use paro_storage::tablet::tablet_schema::KeysType;
    use paro_storage::wal::wal_entry::{ColumnInfo, WalEntry};
    use paro_storage::wal::wal_type::WalType;
    use paro_storage::wal::wal_writer::{WalInitState, WalWriter};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_primary_key_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default()
            .create_table_with_keys(types, KeysType::PrimaryKeys)
            .unwrap()
    }

    fn find_first_segment_dir(root: &Path) -> Option<PathBuf> {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };

            let mut has_segment = false;
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) == Some("dat") {
                    has_segment = true;
                }
            }

            if has_segment {
                return Some(dir);
            }
        }
        None
    }

    fn copy_dir_all(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let target = to.join(entry.file_name());
            if path.is_dir() {
                copy_dir_all(&path, &target);
            } else {
                fs::copy(&path, &target).unwrap();
            }
        }
    }

    fn ensure_main_schema(catalog: &Arc<ParoCatalog>) {
        let info = CreateSchemaInfo {
            catalog: catalog.name().to_string(),
            name: "main".to_string(),
            internal: false,
            on_conflict: OnCreateConflict::IgnoreOnConflict,
        };
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(&info, catalog.gc_epoch_handle(), 0),
        )));
        catalog
            .get_schema_collection()
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();
    }

    fn install_committed_table(
        catalog: &Arc<ParoCatalog>,
        schema_name: &str,
        table_name: &str,
        columns: Vec<ColumnDefinition>,
        storage: Arc<TableHandle>,
    ) {
        let info = CreateTableInfo::new(
            catalog.name().to_string(),
            schema_name.to_string(),
            table_name.to_string(),
            columns,
        );
        let entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            TableCatalogEntry::from_info(info, storage, 0),
        )));
        let schema = catalog
            .get_schema(&CatalogSnapshot::read_only(u64::MAX), schema_name)
            .unwrap();
        schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();
    }

    fn write_flushed_catalog_txn(
        writer: &WalWriter,
        txn_id: u64,
        commit_id: u64,
        changes: Vec<DdlChangeRecord>,
    ) {
        let record = paro_common::journal::CommitRecord {
            txn_id,
            start_time: 0,
            commit_id,
            catalog_ops: changes
                .into_iter()
                .map(|change| CatalogTxnOp { change })
                .collect(),
            storage_ops: vec![],
            apply_descriptors: vec![],
            deferred_tasks: vec![],
        };
        let commit = WalEntry::JournalRecord {
            lsn: commit_id,
            record: paro_common::journal::JournalRecord::Commit(record),
        };
        writer
            .write_entry(WalType::JournalRecord, &commit.serialize_data())
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_flushed_commit_record(writer: &WalWriter, lsn: u64, record: CommitRecord) {
        let commit = WalEntry::JournalRecord {
            lsn,
            record: paro_common::journal::JournalRecord::Commit(record),
        };
        writer
            .write_entry(WalType::JournalRecord, &commit.serialize_data())
            .unwrap();
        writer.flush().unwrap();
    }

    #[derive(Debug)]
    struct RecordingCatalogBatch {
        records: Vec<DdlChangeRecord>,
        applied: Vec<String>,
    }

    impl RuntimeCatalogApplyBatch for RecordingCatalogBatch {
        fn len(&self) -> usize {
            self.records.len()
        }

        fn record(&self, index: usize) -> &DdlChangeRecord {
            &self.records[index]
        }

        fn apply(&mut self, index: usize, _commit_id: u64) -> paro_common::error::Result<()> {
            self.applied.push(self.records[index].key.name.clone());
            Ok(())
        }
    }

    #[test]
    fn test_catalog_replay_create_schema() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);

        let payload = CreateSchemaPayload {
            object_id: 42,
            if_not_exists: false,
        };

        handler
            .replay_create_schema("test_schema", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "test_schema").unwrap();
        assert_eq!(schema.base.object_id.raw(), 42);
    }

    #[test]
    fn test_catalog_replay_create_table() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);

        let columns = [
            ColumnInfo::new("id".to_string(), LogicalType::Integer, false),
            ColumnInfo::new("name".to_string(), LogicalType::Varchar, true),
        ];
        let seed_storage = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
        let descriptor = seed_storage.to_descriptor().unwrap();
        let payload = CreateTablePayload {
            object_id: 99,
            columns: columns
                .iter()
                .map(|column| DdlWalColumnInfo {
                    name: column.name.clone(),
                    logical_type: column.logical_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
            constraints: Vec::new(),
            if_not_exists: false,
            storage: Some(DdlStorageDescriptor {
                format_version: descriptor.format_version,
                tablet_id: descriptor.tablet_id,
                table_id: descriptor.table_id,
                partition_id: descriptor.partition_id,
                schema_id: descriptor.schema_id,
                schema_version: descriptor.schema_version,
                schema_hash: descriptor.schema_hash,
                data_dir: descriptor.data_dir.clone(),
                keys_type: descriptor.keys_type,
            }),
        };

        handler
            .replay_create_table("main", "users", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let table = catalog.get_table(&txn, "main", "users").unwrap();
        let CatalogEntryEnum::Table(table) = table.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(table.get_storage_descriptor(), Some(&descriptor));
        assert_eq!(table.base.base.object_id.raw(), 99);
    }

    #[test]
    fn test_catalog_replay_create_index_metadata_marks_art_ready() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        let payload = CreateIndexPayload {
            object_id: 42,
            table_name: "users".to_string(),
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            index_type: "ART".to_string(),
            is_unique: false,
            if_not_exists: false,
            fulltext_config: None,
        };
        handler
            .replay_create_index("main", "idx_users_id", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema.get_index(0, u64::MAX, "idx_users_id").unwrap();
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        assert_eq!(index.base.base.object_id.raw(), 42);
        assert_eq!(index.failure_reason(), None);
    }

    #[test]
    fn test_catalog_replay_create_index_metadata_only_ready() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        let payload = CreateIndexPayload {
            object_id: 43,
            table_name: "users".to_string(),
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            index_type: "HNSW".to_string(),
            is_unique: false,
            if_not_exists: false,
            fulltext_config: None,
        };
        handler
            .replay_create_index("main", "idx_users_hnsw", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema.get_index(0, u64::MAX, "idx_users_hnsw").unwrap();
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        assert_eq!(index.base.base.object_id.raw(), 43);
        assert_eq!(index.failure_reason(), None);
    }

    #[test]
    fn test_reconcile_fulltext_index_coverage_marks_failed_on_incomplete() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Varchar]));
        let columns = vec![ColumnDefinition::new(
            "content".to_string(),
            LogicalType::Varchar,
        )];
        install_committed_table(&catalog, "main", "docs", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_strings(&["vector db"])]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "docs".to_string(),
            "idx_docs_fts".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::FullText)
        .with_fulltext_options(LogicalIndex::new(0), "simple")
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        reconcile_fulltext_index_coverage(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_docs_fts")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Failed);
        assert!(
            index
                .failure_reason()
                .unwrap_or_default()
                .contains("coverage incomplete"),
            "unexpected failure reason: {:?}",
            index.failure_reason()
        );
    }

    #[test]
    fn test_reconcile_fulltext_index_coverage_marks_ready_when_complete() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Varchar]));
        let columns = vec![ColumnDefinition::new(
            "content".to_string(),
            LogicalType::Varchar,
        )];
        install_committed_table(&catalog, "main", "docs", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_strings(&["vector db"])]);
        storage.append(&insert).unwrap();
        storage.build_runtime_fulltext_index(0).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "docs".to_string(),
            "idx_docs_fts".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Varchar],
        )
        .with_index_type(IndexType::FullText)
        .with_fulltext_options(LogicalIndex::new(0), "simple")
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        reconcile_fulltext_index_coverage(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_docs_fts")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        let coverage = index.coverage().expect("coverage should be populated");
        assert!(coverage.is_complete());
        assert!(storage.has_fulltext_index_with_config(0, "simple"));
    }

    #[test]
    fn test_restore_runtime_art_indexes_marks_ready_when_complete() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "users")
            .expect("users table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "users".to_string(),
            "idx_users_art".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Integer],
        )
        .with_index_type(IndexType::ART)
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        restore_runtime_art_indexes(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_users_art")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        let coverage = index.coverage().expect("coverage should be populated");
        assert!(coverage.is_complete());
        assert_eq!(storage.tablet().declared_art_columns(), vec![0]);
        assert!(storage
            .collect_segments(storage.max_version())
            .unwrap()
            .iter()
            .all(|(_, segment)| segment.art_index(0).is_some()));

        let report = build_recovery_consistency_report(&catalog);
        assert!(
            report.all_consistent,
            "report should be consistent: {report:?}"
        );
    }

    #[test]
    fn test_restore_runtime_art_indexes_marks_failed_on_missing_column() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, Arc::clone(&storage));

        let insert = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "users")
            .expect("users table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "users".to_string(),
            "idx_users_art_missing".to_string(),
            vec![LogicalIndex::new(99)],
            vec![LogicalType::Integer],
        )
        .with_index_type(IndexType::ART)
        .with_build_state(IndexBuildState::Building);
        let entry = Arc::new(CatalogEntryEnum::Index(Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            0,
            catalog.name().to_string(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        restore_runtime_art_indexes(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_users_art_missing")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Failed);
        assert!(
            index
                .failure_reason()
                .unwrap_or_default()
                .contains("column 99"),
            "unexpected failure reason: {:?}",
            index.failure_reason()
        );
        assert!(storage.tablet().declared_art_columns().is_empty());
        assert!(storage
            .collect_segments(storage.max_version())
            .unwrap()
            .iter()
            .all(|(_, segment)| segment.art_index(99).is_none()));
    }

    #[test]
    fn test_catalog_replay_create_sequence_applies_payload() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let payload = CreateSequencePayload {
            object_id: 123,
            if_not_exists: false,
            increment: 3,
            min_value: 5,
            max_value: 99,
            start_value: 7,
            cycle: true,
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_create_sequence("main", "seq_replayed", &payload, 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_sequence(txn.transaction_id, txn.start_time, "seq_replayed")
            .expect("sequence should exist after replay");
        let CatalogEntryEnum::Sequence(sequence) = entry.as_ref() else {
            panic!("expected sequence entry");
        };
        let data = sequence.get_data();
        assert_eq!(sequence.base.base.object_id.raw(), 123);
        assert_eq!(data.start_value, 7);
        assert_eq!(data.increment, 3);
        assert_eq!(data.min_value, 5);
        assert_eq!(data.max_value, 99);
        assert!(data.cycle);
    }

    #[test]
    fn test_catalog_replay_drop_schema_is_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(
                &CreateSchemaInfo {
                    catalog: catalog.name().to_string(),
                    name: "drop_me".to_string(),
                    internal: false,
                    on_conflict: OnCreateConflict::IgnoreOnConflict,
                },
                catalog.gc_epoch_handle(),
                0,
            ),
        )));
        catalog
            .get_schema_collection()
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler.replay_drop_schema("drop_me", 42).unwrap();
        handler.replay_drop_schema("drop_me", 42).unwrap();

        assert!(catalog
            .get_schema(&CatalogSnapshot::read_only(u64::MAX), "drop_me")
            .is_err());
    }

    #[test]
    fn test_catalog_replay_drop_sequence_is_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let schema = catalog
            .get_schema(&CatalogSnapshot::read_only(u64::MAX), "main")
            .unwrap();
        let entry = Arc::new(CatalogEntryEnum::Sequence(Arc::new(
            SequenceCatalogEntry::new(
                CreateSequenceInfo::new("main".to_string(), "seq_to_drop".to_string())
                    .with_catalog(catalog.name().to_string()),
                0,
                catalog.name().to_string(),
            )
            .unwrap(),
        )));
        schema
            .collection(CatalogType::Sequence)
            .expect("sequence collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_drop_sequence("main", "seq_to_drop", 42)
            .unwrap();
        handler
            .replay_drop_sequence("main", "seq_to_drop", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        assert!(schema
            .get_sequence(txn.transaction_id, txn.start_time, "seq_to_drop")
            .is_none());
    }

    #[test]
    fn test_catalog_replay_alter_entry_updates_table_comment() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("COMMENT ON TABLE main.docs IS 'replayed comment'", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(
            table.base.base.comment(),
            Some("replayed comment".to_string())
        );
    }

    #[test]
    fn test_catalog_replay_alter_entry_updates_column_comment() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Varchar]));
        let columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("note".to_string(), LogicalType::Varchar),
        ];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry(
                "COMMENT ON COLUMN main.docs.note IS 'replayed column comment'",
                42,
            )
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(
            table
                .get_column("note")
                .and_then(|column| column.comment.clone()),
            Some("replayed column comment".to_string())
        );
    }

    #[test]
    fn test_catalog_replay_alter_entry_renames_table() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME TO docs_v2", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        assert!(schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .is_none());
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs_v2")
            .expect("renamed table should exist");
        assert_eq!(entry.name(), "docs_v2");
    }

    #[test]
    fn test_catalog_replay_rename_uses_commit_id_visibility_boundary() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let commit_id = 77;
        let mut handler = CatalogReplayHandler::new(&catalog, 0, commit_id);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME TO docs_v2", commit_id)
            .unwrap();

        let at_commit = CatalogSnapshot::read_only(commit_id);
        let schema_at_commit = catalog.get_schema(&at_commit, "main").unwrap();
        assert!(schema_at_commit
            .get_table(at_commit.transaction_id, at_commit.start_time, "docs_v2")
            .is_none());
        assert!(schema_at_commit
            .get_table(at_commit.transaction_id, at_commit.start_time, "docs")
            .is_some());

        let after_commit = CatalogSnapshot::read_only(commit_id + 1);
        let schema_after_commit = catalog.get_schema(&after_commit, "main").unwrap();
        assert!(schema_after_commit
            .get_table(after_commit.transaction_id, after_commit.start_time, "docs")
            .is_none());
        assert!(schema_after_commit
            .get_table(
                after_commit.transaction_id,
                after_commit.start_time,
                "docs_v2"
            )
            .is_some());
    }

    #[test]
    fn test_catalog_replay_rename_table_across_schema() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let info = CreateSchemaInfo {
            catalog: catalog.name().to_string(),
            name: "archive".to_string(),
            internal: false,
            on_conflict: OnCreateConflict::IgnoreOnConflict,
        };
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(&info, catalog.gc_epoch_handle(), 0),
        )));
        catalog
            .get_schema_collection()
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("RENAME TABLE main.docs TO archive.docs_v2", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let main_schema = catalog.get_schema(&txn, "main").unwrap();
        let archive_schema = catalog.get_schema(&txn, "archive").unwrap();
        assert!(main_schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .is_none());
        let entry = archive_schema
            .get_table(txn.transaction_id, txn.start_time, "docs_v2")
            .expect("moved table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(table.base.schema_name, "archive");
        assert_eq!(table.base.base.name, "docs_v2");
    }

    #[test]
    fn test_catalog_replay_rename_table_commit_timestamp_baseline() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let replay_writer_id = 7;
        let replay_commit_ts = 42;
        let mut handler = CatalogReplayHandler::new(&catalog, replay_writer_id, replay_commit_ts);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME TO docs_v2", replay_commit_ts)
            .unwrap();

        let snapshot_at_commit = CatalogSnapshot::read_only(replay_commit_ts);
        let schema = catalog.get_schema(&snapshot_at_commit, "main").unwrap();
        assert!(
            schema
                .get_table(
                    snapshot_at_commit.transaction_id,
                    snapshot_at_commit.start_time,
                    "docs_v2",
                )
                .is_none(),
            "replay rename became visible at commit_ts, which means replay writer id is still leaking into publish visibility"
        );
        assert!(schema
            .get_table(
                snapshot_at_commit.transaction_id,
                snapshot_at_commit.start_time,
                "docs",
            )
            .is_some());

        let snapshot_after_commit = CatalogSnapshot::read_only(replay_commit_ts + 1);
        let schema_after_commit = catalog.get_schema(&snapshot_after_commit, "main").unwrap();
        assert!(schema_after_commit
            .get_table(
                snapshot_after_commit.transaction_id,
                snapshot_after_commit.start_time,
                "docs_v2",
            )
            .is_some());
    }

    #[test]
    fn test_catalog_replay_alter_entry_renames_column() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "docs", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_alter_entry("ALTER TABLE main.docs RENAME COLUMN id TO doc_id", 42)
            .unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let entry = schema
            .get_table(txn.transaction_id, txn.start_time, "docs")
            .expect("docs table should exist");
        let CatalogEntryEnum::Table(table) = entry.as_ref() else {
            panic!("expected table entry");
        };
        assert_eq!(table.columns[0].name, "doc_id");
    }

    #[test]
    fn test_recover_database_no_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("nonexistent.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let (wal, result, _summary, _recovery, _tasks) =
            recover_database(&wal_path, &catalog, None).unwrap();

        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert_eq!(result.entries_replayed, 0);
        assert!(!wal.is_initialized());
    }

    #[test]
    fn test_recover_database_with_entries() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let replayed_schema_oid = catalog.current_object_id().saturating_add(1_000);

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            paro_storage::wal::test_support::write_flushed_create_schema_txn_with_object_id(
                &writer,
                "test",
                "test_schema",
                replayed_schema_oid,
                1,
                100,
            )
            .unwrap();
        }

        let (_wal, result, _summary, _recovery, _tasks) =
            recover_database(&wal_path, &catalog, None).unwrap();

        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert!(result.entries_replayed > 0);

        // Verify catalog was restored
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "test_schema").unwrap();
        assert_eq!(schema.base.object_id.raw(), replayed_schema_oid);
        let replay_watermark = catalog.current_object_id();
        assert!(replay_watermark > replayed_schema_oid);

        let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        catalog
            .create_schema_with_snapshot(&write_txn, "post_recovery_schema")
            .unwrap();
        let created = catalog.get_schema(&txn, "post_recovery_schema").unwrap();
        assert_eq!(created.base.object_id.raw(), replay_watermark);
    }

    #[test]
    fn test_recover_database_restores_schema_table_view_index_and_property_graph() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("combo.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let seed_storage = create_table(&[LogicalType::Integer]);
        let descriptor = seed_storage.to_descriptor().unwrap();
        let schema_oid = 7_001;
        let table_oid = 7_002;
        let view_oid = 7_003;
        let index_oid = 7_004;
        let graph_oid = 7_005;

        let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
        write_flushed_catalog_txn(
            &writer,
            1,
            100,
            vec![
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        None::<String>,
                        "replay_combo",
                        DdlObjectKind::Schema,
                    ),
                    change: DdlChange::CreateSchema(CreateSchemaPayload {
                        object_id: schema_oid,
                        if_not_exists: false,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "items",
                        DdlObjectKind::Table,
                    ),
                    change: DdlChange::CreateTable(CreateTablePayload {
                        object_id: table_oid,
                        columns: vec![DdlWalColumnInfo {
                            name: "id".to_string(),
                            logical_type: LogicalType::Integer,
                            nullable: false,
                        }],
                        constraints: Vec::new(),
                        if_not_exists: false,
                        storage: Some(DdlStorageDescriptor {
                            format_version: descriptor.format_version,
                            tablet_id: descriptor.tablet_id,
                            table_id: descriptor.table_id,
                            partition_id: descriptor.partition_id,
                            schema_id: descriptor.schema_id,
                            schema_version: descriptor.schema_version,
                            schema_hash: descriptor.schema_hash,
                            data_dir: descriptor.data_dir.clone(),
                            keys_type: descriptor.keys_type,
                        }),
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "items_view",
                        DdlObjectKind::View,
                    ),
                    change: DdlChange::CreateView(CreateViewPayload {
                        object_id: view_oid,
                        sql: "CREATE VIEW replay_combo.items_view AS SELECT id FROM replay_combo.items"
                            .to_string(),
                        column_aliases: vec![],
                        dependencies: vec![DdlDependencyRef {
                            object: DdlDependencyObjectRef {
                                object_id: table_oid,
                                kind: "TABLE".to_string(),
                                catalog_name: "test".to_string(),
                                schema_id: Some(schema_oid),
                                schema_name: Some("replay_combo".to_string()),
                                name: "items".to_string(),
                            },
                            dependency_type: "regular".to_string(),
                        }],
                        if_not_exists: false,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "idx_items_id",
                        DdlObjectKind::Index,
                    ),
                    change: DdlChange::CreateIndex(CreateIndexPayload {
                        object_id: index_oid,
                        table_name: "items".to_string(),
                        column_ids: vec![0],
                        column_types: vec![LogicalType::Integer],
                        index_type: "ART".to_string(),
                        is_unique: false,
                        if_not_exists: false,
                        fulltext_config: None,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        Some("replay_combo"),
                        "items_graph",
                        DdlObjectKind::PropertyGraph,
                    ),
                    change: DdlChange::CreatePropertyGraph(CreatePropertyGraphPayload {
                        object_id: graph_oid,
                        schema: "replay_combo".to_string(),
                        graph_name: "items_graph".to_string(),
                        if_not_exists: false,
                        vertex_tables: vec![PropertyGraphVertexPayload {
                            table_name: "items".to_string(),
                            table_oid,
                            key_column_ids: vec![0],
                            label: "Item".to_string(),
                            property_column_ids: vec![],
                        }],
                        edge_tables: vec![],
                    }),
                },
            ],
        );

        let (_wal, result, _summary, _recovery, _tasks) =
            recover_database(&wal_path, &catalog, None).unwrap();
        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert!(result.entries_replayed > 0);

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "replay_combo").unwrap();
        assert_eq!(schema.base.object_id.raw(), schema_oid);
        assert!(schema
            .get_table(txn.transaction_id, txn.start_time, "items")
            .is_some());
        assert!(schema
            .get_view(txn.transaction_id, txn.start_time, "items_view")
            .is_some());
        assert!(schema
            .get_index(txn.transaction_id, txn.start_time, "idx_items_id")
            .is_some());
        assert!(schema.get_property_graph(&txn, "items_graph").is_ok());

        let dependency_error = catalog
            .dependency_graph()
            .plan_drop(CatalogObjectId::from_raw(table_oid), false)
            .unwrap_err();
        assert!(dependency_error.to_string().contains("items_view"));
    }

    #[test]
    fn test_catalog_replay_finalize_allocator_tracks_dropped_objects() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let replayed_object_id = catalog.current_object_id().saturating_add(1_000);

        let payload = CreateSchemaPayload {
            object_id: replayed_object_id,
            if_not_exists: false,
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_create_schema("ephemeral_schema", &payload, 42)
            .unwrap();
        handler.replay_drop_schema("ephemeral_schema", 43).unwrap();
        handler.finalize_object_id_allocator().unwrap();

        let read_txn = CatalogSnapshot::read_only(u64::MAX);
        assert!(catalog.get_schema(&read_txn, "ephemeral_schema").is_err());
        let next_object_id = catalog.current_object_id();
        assert!(next_object_id > replayed_object_id);

        let write_txn = CatalogSnapshot::permanent_writer(u64::MAX);
        catalog
            .create_schema_with_snapshot(&write_txn, "after_drop_replay")
            .unwrap();
        let created = catalog.get_schema(&read_txn, "after_drop_replay").unwrap();
        assert_eq!(created.base.object_id.raw(), next_object_id);
    }

    #[test]
    fn test_runtime_catalog_apply_orders_non_drop_before_drop() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let mut batch = RecordingCatalogBatch {
            records: vec![
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        None::<String>,
                        "drop_first",
                        DdlObjectKind::Schema,
                    ),
                    change: DdlChange::DropSchema(paro_common::ddl::DropSchemaPayload {
                        cascade: false,
                        if_exists: false,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        None::<String>,
                        "create_schema",
                        DdlObjectKind::Schema,
                    ),
                    change: DdlChange::CreateSchema(CreateSchemaPayload {
                        object_id: 91_001,
                        if_not_exists: false,
                    }),
                },
                DdlChangeRecord {
                    key: DdlObjectKey::new(
                        "test",
                        None::<String>,
                        "alter_last",
                        DdlObjectKind::Schema,
                    ),
                    change: DdlChange::AlterEntry(paro_common::ddl::AlterEntryPayload {
                        sql: "ALTER SCHEMA create_schema RENAME TO alter_last".to_string(),
                    }),
                },
            ],
            applied: Vec::new(),
        };

        let record = CommitRecord {
            txn_id: 7,
            start_time: 3,
            commit_id: 42,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .apply_runtime_commit_record(&record, 11, &mut batch)
            .unwrap();

        assert_eq!(
            batch.applied,
            vec![
                "create_schema".to_string(),
                "alter_last".to_string(),
                "drop_first".to_string(),
            ]
        );
        assert_eq!(handler.summary().max_catalog_commit_id, 42);
    }

    #[test]
    fn test_recovery_accumulates_deferred_tasks_from_commit_and_maintenance_records() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);

        let build_index = DeferredTask::BuildIndexRuntime {
            index: DdlObjectKey::new("test", Some("main"), "idx_runtime", DdlObjectKind::Index),
            table_name: "items".to_string(),
            index_type: "ART".to_string(),
            column_ids: vec![0],
            fulltext_config: None,
        };
        let graph_maintenance = DeferredTask::GraphDmlMaintenance {
            deltas: vec![paro_common::effect::GraphDmlTableDelta::from_parts(
                7,
                1,
                0,
                0,
                &std::collections::BTreeSet::new(),
            )],
        };

        let commit = CommitRecord {
            txn_id: 7,
            start_time: 3,
            commit_id: 42,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: vec![build_index.clone()],
        };
        let maintenance = MaintenanceRecord {
            maintenance_id: 9,
            kind: paro_common::journal::MaintenanceKind::Compaction,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: vec![graph_maintenance.clone()],
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler.replay_commit_record(42, &commit).unwrap();
        handler.replay_maintenance_record(43, &maintenance).unwrap();

        assert_eq!(
            handler.replayed_deferred_tasks(),
            &[build_index, graph_maintenance]
        );
    }

    #[test]
    fn test_catalog_replay_drop_table_idempotent() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "to_drop", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler.replay_drop_table("main", "to_drop", 42).unwrap();
        handler.replay_drop_table("main", "to_drop", 42).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        assert!(schema.get_table(0, u64::MAX, "to_drop").is_none());
    }

    #[test]
    fn test_catalog_replay_rowset_commit_applies_when_table_mapped() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_table(&[LogicalType::Integer]));
        let target_columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            target_columns,
            Arc::clone(&target_storage),
        );
        assert_eq!(target_storage.rowset_count(), 0);

        let source_storage = create_table(&[LogicalType::Integer]);
        let source_chunk = Chunk::from_vectors(vec![Vector::from_i32(&[1, 2, 3])]);
        source_storage.append(&source_chunk).unwrap();

        let source_descriptor = source_storage.to_descriptor().unwrap();
        let rowset_dir = find_first_segment_dir(Path::new(&source_descriptor.data_dir))
            .expect("expected source rowset directory with segment files");

        let target_descriptor = target_storage.to_descriptor().unwrap();
        let staged_rowset_dir = Path::new(&target_descriptor.data_dir)
            .join("_staged")
            .join("replay_rowset_commit")
            .join("rowset_9999");
        copy_dir_all(&rowset_dir, &staged_rowset_dir);
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_rowset_commit(RowsetCommitReplay {
                tablet_id: target_descriptor.tablet_id,
                rowset_id: 9_999,
                version_span: VersionSpan { start: 1, end: 1 },
                rowset_path: staged_rowset_dir.to_string_lossy().as_ref(),
                replaced_locations: &[],
                lsn: 1,
            })
            .unwrap();

        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows(), 3);

        // Rowset commit replay is idempotent for the same rowset_id.
        handler
            .replay_rowset_commit(RowsetCommitReplay {
                tablet_id: target_descriptor.tablet_id,
                rowset_id: 9_999,
                version_span: VersionSpan { start: 1, end: 1 },
                rowset_path: staged_rowset_dir.to_string_lossy().as_ref(),
                replaced_locations: &[],
                lsn: 2,
            })
            .unwrap();
        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows(), 3);
    }

    #[test]
    fn test_replay_commit_record_restores_tablet_applied_lsn_from_durable_lsn() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_table(&[LogicalType::Integer]));
        let target_columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            target_columns,
            Arc::clone(&target_storage),
        );

        let source_storage = create_table(&[LogicalType::Integer]);
        let source_chunk = Chunk::from_vectors(vec![Vector::from_i32(&[7, 8, 9])]);
        source_storage.append(&source_chunk).unwrap();
        let source_descriptor = source_storage.to_descriptor().unwrap();
        let rowset_dir = find_first_segment_dir(Path::new(&source_descriptor.data_dir))
            .expect("expected source rowset directory with segment files");
        let staged_rowset_dir = target_storage.tablet().staged_rowset_path(77, 4_242);
        copy_dir_all(&rowset_dir, &staged_rowset_dir);

        let tablet_id = target_storage.tablet_id();
        let record = CommitRecord {
            txn_id: 11,
            start_time: 5,
            commit_id: 19,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id,
                mutations: vec![TabletMutation::PublishRowset {
                    rowset_id: 4_242,
                    version_span: VersionSpan { start: 1, end: 1 },
                    rowset_ref: ArtifactRef::from_tablet_path(
                        target_storage.tablet().data_dir(),
                        &staged_rowset_dir,
                    )
                    .unwrap(),
                }],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        };

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX)
            .with_registry(RouteRegistry::from_catalog(&catalog).unwrap());
        handler.replay_commit_record(41, &record).unwrap();

        assert_eq!(target_storage.total_rows(), 3);
        assert_eq!(target_storage.tablet().applied_lsn(), 41);
        assert_eq!(handler.registry.tablet_applied_lsn(tablet_id), Some(41));
        assert!(!staged_rowset_dir.exists());
        assert!(target_storage
            .tablet()
            .canonical_rowset_path(4_242)
            .exists());
    }

    #[test]
    fn test_recover_database_replays_durable_rowset_publish_without_live_apply() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("durable_rowset_publish.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_table(&[LogicalType::Integer]));
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            Arc::clone(&target_storage),
        );
        assert_eq!(target_storage.rowset_count(), 0);

        let source_storage = create_table(&[LogicalType::Integer]);
        let source_chunk = Chunk::from_vectors(vec![Vector::from_i32(&[7, 8, 9])]);
        source_storage.append(&source_chunk).unwrap();
        let source_descriptor = source_storage.to_descriptor().unwrap();
        let rowset_dir = find_first_segment_dir(Path::new(&source_descriptor.data_dir))
            .expect("expected source rowset directory with segment files");
        let staged_rowset_dir = target_storage.tablet().staged_rowset_path(88, 4_242);
        copy_dir_all(&rowset_dir, &staged_rowset_dir);

        let commit_id = 41;
        let record = CommitRecord {
            txn_id: 11,
            start_time: 5,
            commit_id,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: target_storage.tablet_id(),
                mutations: vec![TabletMutation::PublishRowset {
                    rowset_id: 4_242,
                    version_span: VersionSpan {
                        start: commit_id as i64,
                        end: commit_id as i64,
                    },
                    rowset_ref: ArtifactRef::from_tablet_path(
                        target_storage.tablet().data_dir(),
                        &staged_rowset_dir,
                    )
                    .unwrap(),
                }],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        };

        let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
        write_flushed_commit_record(&writer, commit_id, record);

        let (_wal, result, _summary, recovery_summary, _tasks) =
            recover_database(&wal_path, &catalog, None).unwrap();

        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert!(result.entries_replayed > 0);
        assert_eq!(recovery_summary.max_lsn, commit_id);
        assert_eq!(recovery_summary.max_commit_id, commit_id);
        assert_eq!(target_storage.total_rows(), 3);
        assert_eq!(target_storage.tablet().applied_lsn(), commit_id);
        assert!(!staged_rowset_dir.exists());
        assert!(target_storage
            .tablet()
            .canonical_rowset_path(4_242)
            .exists());
        let replayed_rowset = target_storage
            .tablet()
            .get_rowset_by_version(commit_id as i64)
            .expect("rowset should be visible at commit version");
        assert_eq!(replayed_rowset.start_version(), commit_id as i64);
        assert_eq!(replayed_rowset.end_version(), commit_id as i64);
    }

    #[test]
    fn test_recover_database_replays_durable_delete_patch_without_live_apply() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("durable_delete_patch.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_primary_key_table(&[
            LogicalType::Integer,
            LogicalType::Integer,
        ]));
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            vec![
                ColumnDefinition::new("id".to_string(), LogicalType::Integer),
                ColumnDefinition::new("value".to_string(), LogicalType::Integer),
            ],
            Arc::clone(&target_storage),
        );

        let chunk = Chunk::from_vectors(vec![
            Vector::from_i32(&[1, 2, 3]),
            Vector::from_i32(&[10, 20, 30]),
        ]);
        target_storage.append(&chunk).unwrap();

        let serializer =
            PrimaryKeySerializer::from_schema_ref(&target_storage.tablet().schema().unwrap())
                .unwrap();
        let delete_key = serializer.encode_row(&chunk, 1).unwrap();
        let delete_row_id = target_storage
            .tablet()
            .lookup_primary_key(&delete_key)
            .unwrap()
            .expect("row should exist before replay");
        let delete_location = target_storage
            .tablet()
            .decode_row_id(delete_row_id)
            .expect("decode row id");
        let patch = CatalogReplayHandler::inline_delete_patch(&[delete_location]);

        let commit_id = 52;
        let record = CommitRecord {
            txn_id: 12,
            start_time: 6,
            commit_id,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: target_storage.tablet_id(),
                mutations: vec![TabletMutation::ApplyDeletePatch {
                    deleted_row_count: 1,
                    patch,
                }],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        };

        let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
        write_flushed_commit_record(&writer, commit_id, record);

        let (_wal, result, _summary, recovery_summary, _tasks) =
            recover_database(&wal_path, &catalog, None).unwrap();

        assert!(result.all_succeeded, "replay error: {:?}", result.error);
        assert_eq!(recovery_summary.max_commit_id, commit_id);
        assert_eq!(target_storage.tablet().applied_lsn(), commit_id);
        assert!(target_storage
            .tablet()
            .lookup_primary_key(&delete_key)
            .unwrap()
            .is_none());

        let rowset = target_storage
            .tablet()
            .find_rowset_by_id(delete_location.rowset_id)
            .expect("rowset should remain visible");
        let segment = rowset
            .get_segment(delete_location.segment_id)
            .expect("segment should exist");
        let delete_vector = segment
            .load_delete_vector_at_version(commit_id as i64)
            .unwrap()
            .expect("delete vector should exist at commit version");
        assert_eq!(delete_vector.version(), commit_id as i64);
        assert!(delete_vector.is_deleted(delete_location.row_offset));
    }

    #[test]
    fn test_recovery_skips_delete_patch_replay_when_tablet_applied_lsn_already_covers_record() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_primary_key_table(&[
            LogicalType::Integer,
            LogicalType::Integer,
        ]));
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            vec![
                ColumnDefinition::new("id".to_string(), LogicalType::Integer),
                ColumnDefinition::new("value".to_string(), LogicalType::Integer),
            ],
            Arc::clone(&target_storage),
        );

        let chunk = Chunk::from_vectors(vec![
            Vector::from_i32(&[1, 2, 3]),
            Vector::from_i32(&[10, 20, 30]),
        ]);
        target_storage.append(&chunk).unwrap();

        let serializer =
            PrimaryKeySerializer::from_schema_ref(&target_storage.tablet().schema().unwrap())
                .unwrap();
        let delete_key = serializer.encode_row(&chunk, 1).unwrap();
        let delete_row_id = target_storage
            .tablet()
            .lookup_primary_key(&delete_key)
            .unwrap()
            .expect("row should exist before replay");
        let delete_location = target_storage
            .tablet()
            .decode_row_id(delete_row_id)
            .expect("decode row id");
        let DeletePatchRef::Inline(patch) =
            CatalogReplayHandler::inline_delete_patch(&[delete_location])
        else {
            panic!("inline helper should return inline patch");
        };
        let artifact_path = target_storage
            .tablet()
            .data_dir()
            .join("_delete_patch")
            .join("txn_7")
            .join("patch_0.bin");
        std::fs::create_dir_all(artifact_path.parent().unwrap()).unwrap();
        std::fs::write(
            &artifact_path,
            encode_delete_patch_artifact_bytes(&patch).unwrap(),
        )
        .unwrap();

        let commit_id = 63;
        let patch_ref = DeletePatchRef::Artifact(
            ArtifactRef::from_tablet_path(target_storage.tablet().data_dir(), &artifact_path)
                .unwrap(),
        );
        let record = CommitRecord {
            txn_id: 12,
            start_time: 6,
            commit_id,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: target_storage.tablet_id(),
                mutations: vec![TabletMutation::ApplyDeletePatch {
                    deleted_row_count: 1,
                    patch: patch_ref.clone(),
                }],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        };

        let mut first = CatalogReplayHandler::new(&catalog, 0, u64::MAX)
            .with_registry(RouteRegistry::from_catalog(&catalog).unwrap());
        first.replay_commit_record(commit_id, &record).unwrap();
        assert_eq!(target_storage.tablet().applied_lsn(), commit_id);
        assert!(target_storage
            .tablet()
            .lookup_primary_key(&delete_key)
            .unwrap()
            .is_none());

        std::fs::remove_file(&artifact_path).unwrap();

        let mut second = CatalogReplayHandler::new(&catalog, 0, u64::MAX)
            .with_registry(RouteRegistry::from_catalog(&catalog).unwrap());
        second.replay_commit_record(commit_id, &record).unwrap();
        assert_eq!(
            second
                .registry
                .tablet_applied_lsn(target_storage.tablet_id()),
            Some(commit_id)
        );
        assert!(target_storage
            .tablet()
            .lookup_primary_key(&delete_key)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_drop_schema_replay_removes_schema_owned_routes_from_registry() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);
        let schema_info = CreateSchemaInfo {
            catalog: catalog.name().to_string(),
            name: "drop_registry".to_string(),
            internal: false,
            on_conflict: OnCreateConflict::IgnoreOnConflict,
        };
        let schema_entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(&schema_info, catalog.gc_epoch_handle(), 0),
        )));
        catalog
            .get_schema_collection()
            .install_committed(schema_entry, InstallMode::RejectExisting)
            .expect("install schema for replay test");

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        install_committed_table(
            &catalog,
            "drop_registry",
            "items",
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            Arc::clone(&storage),
        );
        let table_key = paro_common::ddl::DdlObjectKey::new(
            "test",
            Some("drop_registry"),
            "items",
            paro_common::ddl::DdlObjectKind::Table,
        );

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX)
            .with_registry(RouteRegistry::from_catalog(&catalog).unwrap());
        assert!(handler.registry.route_table_key(&table_key).is_some());

        handler
            .replay_commit_record(
                91,
                &CommitRecord {
                    txn_id: 5,
                    start_time: 1,
                    commit_id: 12,
                    catalog_ops: vec![paro_common::effect::CatalogTxnOp {
                        change: DdlChangeRecord {
                            key: paro_common::ddl::DdlObjectKey::new(
                                "test",
                                None::<String>,
                                "drop_registry",
                                paro_common::ddl::DdlObjectKind::Schema,
                            ),
                            change: paro_common::ddl::DdlChange::DropSchema(
                                paro_common::ddl::DropSchemaPayload {
                                    cascade: true,
                                    if_exists: false,
                                },
                            ),
                        },
                    }],
                    storage_ops: Vec::new(),
                    apply_descriptors: Vec::new(),
                    deferred_tasks: Vec::new(),
                },
            )
            .unwrap();

        assert!(handler.registry.route_table_key(&table_key).is_none());
    }

    #[test]
    fn test_catalog_replay_primary_delete_applies_when_table_mapped() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let target_storage = Arc::new(create_primary_key_table(&[
            LogicalType::Integer,
            LogicalType::Integer,
        ]));
        let target_columns = vec![
            ColumnDefinition::new("id".to_string(), LogicalType::Integer),
            ColumnDefinition::new("value".to_string(), LogicalType::Integer),
        ];
        install_committed_table(
            &catalog,
            "main",
            "target_table",
            target_columns,
            Arc::clone(&target_storage),
        );

        let chunk = Chunk::from_vectors(vec![
            Vector::from_i32(&[1, 2, 3]),
            Vector::from_i32(&[10, 20, 30]),
        ]);
        target_storage.append(&chunk).unwrap();

        let serializer =
            PrimaryKeySerializer::from_schema_ref(&target_storage.tablet().schema().unwrap())
                .unwrap();
        let delete_key = serializer.encode_row(&chunk, 1).unwrap();
        let tablet_id = target_storage.tablet_id();

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_primary_delete(tablet_id, std::slice::from_ref(&delete_key), 1)
            .unwrap();

        assert!(target_storage
            .tablet()
            .lookup_primary_key(&delete_key)
            .unwrap()
            .is_none());
        assert_eq!(
            target_storage
                .tablet()
                .snapshot_primary_index_entries()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn test_recovery_consistency_report_marks_healthy_table() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, Arc::clone(&storage));

        let report = build_recovery_consistency_report(&catalog);
        assert!(report.all_consistent);
        assert!(report.schema_count >= 1);
        assert!(report.table_count >= 1);

        let table_report = report
            .tables
            .iter()
            .find(|entry| entry.schema_name == "main" && entry.table_name == "users")
            .expect("expected report entry for main.users");
        assert!(table_report.has_storage);
        assert!(table_report.version_graph_ok);
        assert!(table_report.primary_index_reconciled);
        assert!(table_report.errors.is_empty());
    }

    #[test]
    fn test_recovery_consistency_report_detects_catalog_runtime_index_mismatch() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "id".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "users", columns, storage);

        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        let payload = CreateIndexPayload {
            object_id: 77,
            table_name: "users".to_string(),
            column_ids: vec![0],
            column_types: vec![LogicalType::Integer],
            index_type: "ART".to_string(),
            is_unique: false,
            if_not_exists: false,
            fulltext_config: None,
        };
        handler
            .replay_create_index("main", "idx_users_id", &payload, 42)
            .unwrap();

        let report = build_recovery_consistency_report(&catalog);
        assert!(!report.all_consistent);

        let table_report = report
            .tables
            .iter()
            .find(|entry| entry.schema_name == "main" && entry.table_name == "users")
            .expect("expected report entry for main.users");
        assert_eq!(table_report.catalog_index_count, 1);
        assert_eq!(table_report.runtime_index_count, Some(0));
        assert!(table_report
            .errors
            .iter()
            .any(|error| error.contains("index count mismatch")));
    }

    #[test]
    fn test_needs_recovery() {
        let dir = tempdir().unwrap();

        // Non-existent file
        let path = dir.path().join("nonexistent.wal");
        assert!(!needs_recovery(&path));

        // Empty file
        let empty_path = dir.path().join("empty.wal");
        std::fs::write(&empty_path, &[]).unwrap();
        assert!(!needs_recovery(&empty_path));

        // File with content
        let content_path = dir.path().join("content.wal");
        std::fs::write(&content_path, b"some content").unwrap();
        assert!(needs_recovery(&content_path));

        // Checkpoint WAL without main WAL should still trigger recovery.
        let checkpoint_only_main = dir.path().join("checkpoint_only.wal");
        let checkpoint_only_cp = dir.path().join("checkpoint_only.checkpoint.wal");
        std::fs::write(&checkpoint_only_cp, b"checkpoint content").unwrap();
        assert!(needs_recovery(&checkpoint_only_main));

        // Recovery WAL artifact should also trigger recovery for cleanup.
        let recovery_only_main = dir.path().join("recovery_only.wal");
        let recovery_only_rc = dir.path().join("recovery_only.recovery.wal");
        std::fs::write(&recovery_only_rc, b"recovery content").unwrap();
        assert!(needs_recovery(&recovery_only_main));
    }
}
