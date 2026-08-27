// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bridges storage-level WAL replay with catalog and runtime recovery.

use super::consistency_report::{
    build_recovery_consistency_report, log_recovery_consistency_report,
};
use super::index_restore::{
    restore_runtime_art_indexes, restore_search_registry_definitions,
    sweep_orphan_search_generation_workspaces,
};
use crate::checkpoint::runtime::RecordWatermarks;
use crate::commit::recovery_publish::{
    build_recovery_required_publish_plan, RecoveryPublishPlanInput,
};
use crate::search_registry::unregister_search_definition_by_name;
use paro_catalog::collection::{InstallMode, StagedCatalogMutation};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, CreateSchemaInfo, IndexType, OnCreateConflict,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_catalog::mvcc::REPLAY_WRITER_ID;
use paro_common::checkpoint::{JournalTailRef, RecoverySummary};
use paro_common::effect::{
    ApplyDescriptor, CatalogTxnOp, CleanupDescriptor, DeferredTask, PostCommitHookDescriptor,
    PreparedDataOp, RuntimeTransitionDescriptor, StagedArtifactDescriptor, StorageCommitOp,
    TabletMutation,
};
use paro_common::error as paro_error;
use paro_common::journal::JournalRecord;
use paro_common::logging::targets;
use paro_journal::segments::{ReplayCursor, SegmentCatalogStore};
use paro_journal::wal::recovery::{ReplayHandler, WalRecovery};
use paro_journal::wal::replay_state::ReplayResult;
use paro_journal::wal::wal_entry::WalHeaderMetadata;
use paro_journal::wal::write_ahead_log::WriteAheadLog;
use paro_journal::{
    encoded_journal_record_size_upper_bound, mutation_identity_for_tablet, JournalApplyRuntime,
    RecoveryPlaceholderRecordKind, TabletApplyPart,
};
use paro_storage::meta::TabletMetaManager;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::transaction::descriptor_cleanup::apply_cleanup_descriptor as run_cleanup_descriptor;
use paro_transaction::{
    CommitDurableBatch, CommitRuntime, CommitTs, RecoveryReplayCommit, RecoveryReplayEvent,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Replay handler that applies WAL entries to a Catalog.
///
/// This handler is used during database startup to replay WAL entries
/// and restore the catalog to a consistent state.
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
    /// Folded recovery summary for the replayed prefix.
    pub(super) recovery_summary: RecoverySummary,
    /// Deferred post-commit work recovered from the replayed durable tail.
    pub(super) replayed_deferred_tasks: Vec<DeferredTask>,
    /// Runtime used to order recovered tablet apply work by journal LSN.
    apply_runtime: Option<Arc<JournalApplyRuntime>>,
    /// Commit runtime used to replay recovered commit records through the ordered event stream.
    commit_runtime: Option<Arc<CommitRuntime>>,
}

impl<'a> CatalogReplayHandler<'a> {
    /// Create a new catalog replay handler.
    pub fn new(catalog: &'a Arc<ParoCatalog>, txn_id: u64, commit_ts: u64) -> Self {
        Self::new_with_bootstrap(catalog, txn_id, commit_ts, RecoverySummary::default())
    }

    pub fn new_with_bootstrap(
        catalog: &'a Arc<ParoCatalog>,
        txn_id: u64,
        commit_ts: u64,
        bootstrap: RecoverySummary,
    ) -> Self {
        let transaction = if txn_id >= REPLAY_WRITER_ID {
            CatalogSnapshot::writer(txn_id, commit_ts)
        } else {
            CatalogSnapshot::replay_writer(commit_ts)
        };
        Self {
            catalog,
            transaction,
            database_root: PathBuf::new(),
            tablet_meta_manager: None,
            max_seen_object_id: bootstrap.max_seen_object_id,
            max_catalog_commit_id: bootstrap.max_catalog_commit_id,
            recovery_summary: bootstrap,
            replayed_deferred_tasks: Vec::new(),
            apply_runtime: None,
            commit_runtime: None,
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

    pub fn with_apply_runtime(mut self, runtime: Option<Arc<JournalApplyRuntime>>) -> Self {
        self.apply_runtime = runtime;
        self
    }

    pub fn with_commit_runtime(mut self, runtime: Option<Arc<CommitRuntime>>) -> Self {
        self.commit_runtime = runtime;
        self
    }

    fn table_storage_for_tablet(&self, tablet_id: u64) -> Option<Arc<TableHandle>> {
        let txn = CatalogSnapshot::read_only(u64::MAX);
        for schema_entry in self
            .catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            for table_entry in schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .scan(txn.transaction_id, txn.start_time)
            {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };
                let Some(storage) = table.get_storage() else {
                    continue;
                };
                if storage.tablet_id() == tablet_id {
                    return Some(Arc::clone(storage));
                }
            }
        }
        None
    }

    fn finalize_replayed_derived_state(&self) -> paro_common::error::Result<()> {
        let txn = CatalogSnapshot::read_only(u64::MAX);
        for schema_entry in self
            .catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };
            let Some(tables) = schema.collection(CatalogType::Table) else {
                continue;
            };
            for table_entry in tables.scan(txn.transaction_id, txn.start_time) {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };
                if let Some(storage) = table.get_storage() {
                    storage.finalize_replayed_derived_state()?;
                }
            }
        }
        Ok(())
    }

    fn apply_cleanup_descriptor(
        &self,
        cleanup: &CleanupDescriptor,
    ) -> paro_common::error::Result<()> {
        run_cleanup_descriptor(cleanup, self.tablet_meta_manager.as_deref())
    }

    fn record_replayed_deferred_tasks(&mut self, tasks: &[DeferredTask]) {
        self.replayed_deferred_tasks.extend(tasks.iter().cloned());
    }

    fn record_replayed_post_commit_hooks(&mut self, hooks: &[PostCommitHookDescriptor]) {
        self.replayed_deferred_tasks
            .extend(hooks.iter().map(|hook| match hook {
                PostCommitHookDescriptor::GraphDmlMaintenance { deltas } => {
                    DeferredTask::GraphDmlMaintenance {
                        deltas: deltas.clone(),
                    }
                }
            }));
    }

    pub(crate) fn drain_replayed_deferred_tasks(&mut self) -> Vec<DeferredTask> {
        std::mem::take(&mut self.replayed_deferred_tasks)
    }

    fn path_from_components(components: &[String]) -> PathBuf {
        let mut iter = components.iter();
        let Some(first) = iter.next() else {
            return PathBuf::new();
        };
        let mut path = PathBuf::from(first);
        for component in iter {
            path.push(component);
        }
        path
    }

    fn apply_staged_artifact_descriptor(
        &self,
        descriptor: &StagedArtifactDescriptor,
    ) -> paro_common::error::Result<()> {
        match descriptor {
            StagedArtifactDescriptor::PropertyGraphBuild {
                object, staging, ..
            } => {
                let staging_path = Self::path_from_components(&staging.path_components);
                let final_path = self.database_root.join("graph").join(&object.name);

                if !staging_path.exists() {
                    if final_path.exists() {
                        return Ok(());
                    }
                    return Err(paro_error::internal(format!(
                        "missing staged property graph artifact during recovery: {}",
                        staging_path.display()
                    )));
                }

                if let Some(parent) = final_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|err| {
                        paro_error::internal(format!(
                            "recovery create property graph parent dir {}: {}",
                            parent.display(),
                            err
                        ))
                    })?;
                }

                if final_path.exists() {
                    std::fs::remove_dir_all(&final_path).map_err(|err| {
                        paro_error::internal(format!(
                            "recovery remove stale property graph dir {}: {}",
                            final_path.display(),
                            err
                        ))
                    })?;
                }

                std::fs::rename(&staging_path, &final_path).map_err(|err| {
                    paro_error::internal(format!(
                        "recovery publish property graph staging {} -> {}: {}",
                        staging_path.display(),
                        final_path.display(),
                        err
                    ))
                })
            }
            StagedArtifactDescriptor::BulkLoadRowset(_artifact) => {
                // Bulk-load rowsets are published by the tablet StorageCommitOp so
                // replay preserves mutation identity and commit ordering. The
                // descriptor remains in the durable record for CDC/follower
                // metadata and COPY-specific recovery diagnostics.
                Ok(())
            }
            StagedArtifactDescriptor::SearchGenerationBuild(_artifact) => Ok(()),
        }
    }

    fn unmark_declared_runtime_indexes(
        storage: &TableHandle,
        index_name: &str,
        index_type: &str,
        column_ids: &[u32],
    ) -> paro_common::error::Result<()> {
        match IndexType::from_str(index_type) {
            IndexType::ART => {
                for column_id in column_ids {
                    storage.release_art_index(index_name, *column_id)?;
                }
            }
            IndexType::HNSW | IndexType::Sparse | IndexType::FullText => {
                unregister_search_definition_by_name(storage, index_type, index_name)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_runtime_transition(
        &mut self,
        transition: &RuntimeTransitionDescriptor,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        match transition {
            RuntimeTransitionDescriptor::AttachIndexState { .. } => {
                // Recovery replays every storage mutation before one bootstrap
                // pass restores index runtimes from their final tablet heads.
                // Never schedule or materialize an index from an intermediate
                // record state while WAL replay is still in progress.
                let _ = commit_id;
                Ok(())
            }
            RuntimeTransitionDescriptor::DetachIndexState {
                index,
                table_name,
                index_type,
                column_ids,
                ..
            } => {
                let schema_name = index.schema.as_deref().ok_or_else(|| {
                    paro_error::serialization_error(
                        "DROP INDEX runtime transition missing schema name",
                    )
                })?;
                let schema = self.ensure_schema(schema_name, commit_id)?;
                if let Some(table_entry) = schema.get_table(
                    self.transaction.transaction_id,
                    self.transaction.start_time,
                    table_name,
                ) {
                    if let Some(table) = table_entry.as_ref().as_table() {
                        if let Some(storage) = table.get_storage() {
                            let _ = storage.remove_index(&index.name);
                            Self::unmark_declared_runtime_indexes(
                                storage.as_ref(),
                                &index.name,
                                index_type,
                                column_ids,
                            )?;
                        }
                    }
                }
                Ok(())
            }
            RuntimeTransitionDescriptor::RegisterGraphRuntime { .. }
            | RuntimeTransitionDescriptor::UnregisterGraphRuntime { .. } => Ok(()),
        }
    }

    fn apply_descriptors(
        &mut self,
        descriptors: &[ApplyDescriptor],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        for descriptor in descriptors {
            match descriptor {
                ApplyDescriptor::PublishStagedArtifact(artifact) => {
                    self.apply_staged_artifact_descriptor(artifact)?;
                }
                ApplyDescriptor::RuntimeTransition(transition) => {
                    self.apply_runtime_transition(transition, commit_id)?;
                }
                ApplyDescriptor::Cleanup(cleanup) => {
                    self.apply_cleanup_descriptor(cleanup)?;
                }
            }
        }
        Ok(())
    }

    fn build_recovery_descriptor_phase(
        &self,
        descriptors: Vec<ApplyDescriptor>,
        commit_id: u64,
    ) -> Box<dyn FnOnce() -> paro_common::error::Result<()> + Send + 'static> {
        let catalog = Arc::clone(self.catalog);
        let transaction = self.transaction;
        let database_root = self.database_root.clone();
        let tablet_meta_manager = self.tablet_meta_manager.clone();
        Box::new(move || {
            Self::apply_recovery_descriptors(
                catalog,
                transaction,
                database_root,
                tablet_meta_manager,
                descriptors,
                commit_id,
            )
        })
    }

    fn apply_recovery_descriptors(
        catalog: Arc<ParoCatalog>,
        transaction: CatalogSnapshot,
        database_root: PathBuf,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        descriptors: Vec<ApplyDescriptor>,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        for descriptor in descriptors {
            match descriptor {
                ApplyDescriptor::PublishStagedArtifact(artifact) => {
                    Self::apply_recovery_staged_artifact_descriptor(&database_root, &artifact)?;
                }
                ApplyDescriptor::RuntimeTransition(transition) => {
                    Self::apply_recovery_runtime_transition(
                        catalog.as_ref(),
                        &transaction,
                        &transition,
                        commit_id,
                    )?;
                }
                ApplyDescriptor::Cleanup(cleanup) => {
                    run_cleanup_descriptor(&cleanup, tablet_meta_manager.as_deref())?;
                }
            }
        }
        Ok(())
    }

    fn apply_recovery_staged_artifact_descriptor(
        database_root: &Path,
        descriptor: &StagedArtifactDescriptor,
    ) -> paro_common::error::Result<()> {
        match descriptor {
            StagedArtifactDescriptor::PropertyGraphBuild {
                object, staging, ..
            } => {
                let staging_path = Self::path_from_components(&staging.path_components);
                let final_path = database_root.join("graph").join(&object.name);

                if !staging_path.exists() {
                    if final_path.exists() {
                        return Ok(());
                    }
                    return Err(paro_error::internal(format!(
                        "missing staged property graph artifact during recovery publish: {}",
                        staging_path.display()
                    )));
                }

                if let Some(parent) = final_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|err| {
                        paro_error::internal(format!(
                            "recovery create property graph parent dir {}: {}",
                            parent.display(),
                            err
                        ))
                    })?;
                }

                if final_path.exists() {
                    std::fs::remove_dir_all(&final_path).map_err(|err| {
                        paro_error::internal(format!(
                            "recovery remove stale property graph dir {}: {}",
                            final_path.display(),
                            err
                        ))
                    })?;
                }

                std::fs::rename(&staging_path, &final_path).map_err(|err| {
                    paro_error::internal(format!(
                        "recovery publish property graph staging {} -> {}: {}",
                        staging_path.display(),
                        final_path.display(),
                        err
                    ))
                })
            }
            StagedArtifactDescriptor::BulkLoadRowset(_artifact) => Ok(()),
            StagedArtifactDescriptor::SearchGenerationBuild(_artifact) => Ok(()),
        }
    }

    fn apply_recovery_runtime_transition(
        catalog: &ParoCatalog,
        transaction: &CatalogSnapshot,
        transition: &RuntimeTransitionDescriptor,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        match transition {
            RuntimeTransitionDescriptor::AttachIndexState { .. } => {
                // See the synchronous replay path above. The post-replay
                // bootstrap observes the final head exactly once.
                let _ = (catalog, transaction, commit_id);
                Ok(())
            }
            RuntimeTransitionDescriptor::DetachIndexState {
                index,
                table_name,
                index_type,
                column_ids,
                ..
            } => {
                let Some(storage) =
                    Self::recovery_table_storage(catalog, transaction, index, table_name)?
                else {
                    return Ok(());
                };
                let _ = storage.remove_index(&index.name);
                Self::unmark_declared_runtime_indexes(
                    storage.as_ref(),
                    &index.name,
                    index_type,
                    column_ids,
                )
            }
            RuntimeTransitionDescriptor::RegisterGraphRuntime { .. }
            | RuntimeTransitionDescriptor::UnregisterGraphRuntime { .. } => {
                let _ = commit_id;
                Ok(())
            }
        }
    }

    fn recovery_table_storage(
        catalog: &ParoCatalog,
        transaction: &CatalogSnapshot,
        index: &paro_common::ddl::DdlObjectKey,
        table_name: &str,
    ) -> paro_common::error::Result<Option<Arc<TableHandle>>> {
        let schema_name = index.schema.as_deref().ok_or_else(|| {
            paro_error::serialization_error("runtime transition missing schema name")
        })?;
        let schema = match catalog.get_schema(transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => return Ok(None),
        };
        let Some(table_entry) = schema.get_table(
            transaction.transaction_id,
            transaction.start_time,
            table_name,
        ) else {
            return Ok(None);
        };
        let Some(table) = table_entry.as_ref().as_table() else {
            return Ok(None);
        };
        Ok(table.get_storage().cloned())
    }

    fn apply_tablet_mutation(
        &mut self,
        tablet_id: u64,
        mutation: &TabletMutation,
        lsn: u64,
        commit_visibility: Option<i64>,
    ) -> paro_common::error::Result<()> {
        let Some(storage) = self.table_storage_for_tablet(tablet_id) else {
            return Ok(());
        };
        apply_recovered_tablet_mutation(&storage, tablet_id, mutation, lsn, commit_visibility)?;
        Ok(())
    }

    fn apply_storage_ops(
        &mut self,
        storage_ops: &[StorageCommitOp],
        lsn: u64,
        commit_visibility: Option<i64>,
    ) -> paro_common::error::Result<()> {
        for op in storage_ops {
            match op {
                StorageCommitOp::Tablet(tablet) => {
                    for mutation in &tablet.mutations {
                        self.apply_tablet_mutation(
                            tablet.tablet_id,
                            mutation,
                            lsn,
                            commit_visibility,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn build_recovery_tablet_parts(
        &mut self,
        storage_ops: &[StorageCommitOp],
        lsn: u64,
        commit_visibility: Option<i64>,
    ) -> paro_common::error::Result<Vec<TabletApplyPart>> {
        let mut tablet_parts = Vec::new();
        for op in storage_ops {
            match op {
                StorageCommitOp::Tablet(tablet) => {
                    let Some(storage) = self.table_storage_for_tablet(tablet.tablet_id) else {
                        continue;
                    };
                    if storage.tablet().applied_lsn() >= lsn
                        && tablet_mutations_are_applied(
                            &storage,
                            tablet.tablet_id,
                            &tablet.mutations,
                            commit_visibility,
                        )
                    {
                        tracing::debug!(
                            target: targets::INSTANCE,
                            tablet_id = tablet.tablet_id,
                            lsn,
                            "tablet storage op skipped during recovery runtime because applied_lsn already covers record"
                        );
                        continue;
                    }
                    let tablet_id = tablet.tablet_id;
                    let mutations = tablet.mutations.clone();
                    tablet_parts.push(TabletApplyPart {
                        tablet_id,
                        apply: Box::new(move || {
                            for mutation in &mutations {
                                apply_recovered_tablet_mutation(
                                    &storage,
                                    tablet_id,
                                    mutation,
                                    lsn,
                                    commit_visibility,
                                )?;
                            }
                            Ok(())
                        }),
                    });
                }
            }
        }

        Ok(tablet_parts)
    }

    fn replay_gap_placeholders_before(&self, lsn: u64) -> paro_common::error::Result<()> {
        let Some(commit_runtime) = self.commit_runtime.as_ref() else {
            return Ok(());
        };
        let Some(apply_runtime) = self.apply_runtime.as_ref() else {
            return Ok(());
        };

        loop {
            let next = apply_runtime.next_dispatch_lsn();
            if next >= lsn {
                return Ok(());
            }
            commit_runtime
                .recovery_replay([RecoveryReplayEvent::Placeholder {
                    lsn: next,
                    record_kind: RecoveryPlaceholderRecordKind::Other,
                }])
                .map_err(|error| paro_error::internal(error.to_string()))?;
        }
    }

    fn replay_placeholder(
        &self,
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
    ) -> paro_common::error::Result<()> {
        if let Some(commit_runtime) = self.commit_runtime.as_ref() {
            commit_runtime
                .recovery_replay([RecoveryReplayEvent::Placeholder { lsn, record_kind }])
                .map_err(|error| paro_error::internal(error.to_string()))?;
        } else if let Some(runtime) = self.apply_runtime.as_ref() {
            runtime.advance_dispatch_past_placeholder(lsn, record_kind)?;
        }
        Ok(())
    }

    fn replay_commit_event(
        &mut self,
        lsn: u64,
        record: &paro_common::journal::CommitRecord,
        commit_visibility: i64,
    ) -> paro_common::error::Result<()> {
        let Some(commit_runtime) = self.commit_runtime.as_ref().cloned() else {
            self.apply_storage_ops(&record.storage_ops, lsn, Some(commit_visibility))?;
            return self.apply_descriptors(&record.apply_descriptors, record.commit_id);
        };

        let tablet_parts =
            self.build_recovery_tablet_parts(&record.storage_ops, lsn, Some(commit_visibility))?;
        let descriptor_phase = self
            .build_recovery_descriptor_phase(record.apply_descriptors.clone(), record.commit_id);
        let record_bytes =
            encoded_journal_record_size_upper_bound(&JournalRecord::Commit(record.clone()))
                .map_err(|error| {
                    paro_error::internal(format!(
                        "recovery commit record byte estimate overflow at lsn {lsn}: {error}"
                    ))
                })?;
        let commit_ts = CommitTs::new(record.commit_id);
        let batch = Arc::new(
            CommitDurableBatch::new(
                lsn,
                lsn,
                1,
                u64::from(record_bytes),
                Arc::from([record_bytes]),
                0,
                commit_ts,
                commit_ts,
            )
            .map_err(|error| paro_error::internal(error.to_string()))?,
        );
        let handle = batch
            .handle_at(0)
            .map_err(|error| paro_error::internal(error.to_string()))?;
        let required_publish = build_recovery_required_publish_plan(RecoveryPublishPlanInput {
            frontier: Arc::clone(commit_runtime.frontier()),
            apply_targets: Arc::from([]),
            catalog_serial: false,
            catalog_pre: Box::new(|| Ok(())),
            tablet_parts,
            descriptor_phase,
            catalog_post: Box::new(|| Ok(())),
        });
        commit_runtime
            .recovery_replay([RecoveryReplayEvent::Commit(RecoveryReplayCommit::new(
                handle,
                required_publish,
            ))])
            .map_err(|error| paro_error::internal(error.to_string()))?;
        Ok(())
    }

    fn replay_primary_delete_for_tablet(
        &mut self,
        tablet_id: u64,
        keys: &[Vec<u8>],
    ) -> paro_common::error::Result<()> {
        let Some(storage) = self.table_storage_for_tablet(tablet_id) else {
            return Ok(());
        };
        storage.replay_primary_delete(keys)
    }

    fn replay_row_id_delete_for_tablet(
        &mut self,
        tablet_id: u64,
        locations: &[(u64, u32, u32)],
    ) -> paro_common::error::Result<()> {
        let Some(storage) = self.table_storage_for_tablet(tablet_id) else {
            return Ok(());
        };
        storage.replay_row_id_delete(locations)
    }

    fn observe_journal_record(
        &mut self,
        lsn: u64,
        commit_id: u64,
        maintenance_id: u64,
        before_catalog_commit_id: u64,
        before_object_id: u64,
    ) {
        self.recovery_summary.max_lsn = self.recovery_summary.max_lsn.max(lsn);
        self.recovery_summary.max_commit_id = self.recovery_summary.max_commit_id.max(commit_id);
        self.recovery_summary.max_maintenance_id =
            self.recovery_summary.max_maintenance_id.max(maintenance_id);
        if self.max_catalog_commit_id > before_catalog_commit_id {
            self.recovery_summary.max_catalog_commit_id = self
                .recovery_summary
                .max_catalog_commit_id
                .max(self.max_catalog_commit_id);
        }
        if self.max_seen_object_id > before_object_id {
            self.recovery_summary.max_seen_object_id = self
                .recovery_summary
                .max_seen_object_id
                .max(self.max_seen_object_id);
        }
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
        handle: StagedCatalogMutation,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        handle.publish(commit_id)?;
        self.observe_catalog_commit_id(commit_id)
    }

    pub(super) fn observe_record(&mut self, watermarks: RecordWatermarks) {
        let next_lsn = self.recovery_summary.max_lsn.saturating_add(1);
        self.recovery_summary.max_lsn = next_lsn;
        self.recovery_summary.max_commit_id = self
            .recovery_summary
            .max_commit_id
            .max(watermarks.commit_id);
        self.recovery_summary.max_maintenance_id = self
            .recovery_summary
            .max_maintenance_id
            .max(watermarks.maintenance_id);
        self.recovery_summary.max_catalog_commit_id = self
            .recovery_summary
            .max_catalog_commit_id
            .max(watermarks.catalog_commit_id);
        self.recovery_summary.max_seen_object_id = self
            .recovery_summary
            .max_seen_object_id
            .max(watermarks.max_seen_object_id);
    }

    pub fn summary(&self) -> RecoverySummary {
        self.recovery_summary.clone()
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
                        Arc::clone(self.catalog.object_id_allocator()),
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

    pub(in crate::recovery) fn replay_rowset_commit(
        &mut self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> paro_common::error::Result<()> {
        let Some(storage) = self.table_storage_for_tablet(tablet_id) else {
            return Ok(());
        };
        storage.replay_rowset_commit(rowset_id, start_version, end_version, rowset_path)
    }
}

fn tablet_mutations_are_applied(
    storage: &TableHandle,
    tablet_id: u64,
    mutations: &[TabletMutation],
    commit_visibility: Option<i64>,
) -> bool {
    let commit_ts = commit_visibility.unwrap_or_default() as u64;
    mutations.iter().all(|mutation| {
        storage
            .tablet()
            .has_applied_mutation_identity(mutation_identity_for_tablet(
                commit_ts, tablet_id, mutation,
            ))
    })
}

fn apply_recovered_tablet_mutation(
    storage: &TableHandle,
    tablet_id: u64,
    mutation: &TabletMutation,
    lsn: u64,
    commit_visibility: Option<i64>,
) -> paro_common::error::Result<()> {
    let identity = mutation_identity_for_tablet(
        commit_visibility.unwrap_or_default() as u64,
        tablet_id,
        mutation,
    );
    if storage.tablet().has_applied_mutation_identity(identity) {
        tracing::debug!(
            target: targets::INSTANCE,
            tablet_id,
            lsn,
            mutation_kind = ?identity.mutation_kind,
            artifact_id = identity.artifact_id,
            "tablet mutation skipped during recovery because mutation identity was already applied"
        );
        return Ok(());
    }

    match mutation {
        TabletMutation::PublishRowset {
            rowset_id,
            version_span,
            rowset_ref,
        } => {
            let rowset_path = rowset_ref.resolve_for_tablet(storage.tablet().data_dir())?;
            storage.replay_rowset_commit(
                *rowset_id,
                version_span.start,
                version_span.end,
                rowset_path.to_string_lossy().as_ref(),
            )?;
        }
        TabletMutation::ApplyPrimaryDelete { keys } => {
            let delete_version = commit_visibility.ok_or_else(|| {
                paro_error::internal(
                    "maintenance record cannot carry ApplyPrimaryDelete without commit visibility",
                )
            })?;
            storage.replay_primary_delete_at_version(keys, delete_version)?;
        }
        TabletMutation::ApplyDeletePatch {
            patch,
            deleted_row_count: _,
        } => {
            let delete_version = commit_visibility.ok_or_else(|| {
                paro_error::internal(
                    "maintenance record cannot carry ApplyDeletePatch without commit visibility",
                )
            })?;
            let locations = patch.decode_row_refs_for_tablet(storage.tablet().data_dir())?;
            storage.replay_row_id_delete_at_version(&locations, delete_version)?;
        }
        TabletMutation::PublishCompaction { .. } => {
            storage.apply_compaction_publish(mutation)?;
        }
        TabletMutation::PublishSearchGeneration { .. } => {
            storage.replay_search_generation_publish(mutation)?;
        }
        TabletMutation::RetireSearchGeneration { .. } => {
            storage.apply_search_generation_retirement(mutation)?;
        }
    }

    storage.tablet().note_applied_mutation_identity(identity)?;
    storage.tablet().note_applied_lsn(lsn)?;
    Ok(())
}

impl<'a> ReplayHandler for CatalogReplayHandler<'a> {
    fn replay_transaction(
        &mut self,
        catalog_ops: &[CatalogTxnOp],
        data_ops: &[PreparedDataOp],
        _post_commit_hooks: &[PostCommitHookDescriptor],
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let before_catalog_commit_id = self.max_catalog_commit_id;
        let before_object_id = self.max_seen_object_id;
        self.replay_catalog_non_drop_ops(catalog_ops, commit_id)?;
        for op in data_ops {
            match op {
                PreparedDataOp::RowsetCommit {
                    locator,
                    start_version,
                    end_version,
                } => {
                    let rowset_path = locator.path_components.join("/");
                    self.replay_rowset_commit(
                        locator.tablet_id,
                        locator.rowset_id,
                        *start_version,
                        *end_version,
                        &rowset_path,
                    )?;
                }
                PreparedDataOp::PrimaryDelete { tablet_id, keys } => {
                    self.replay_primary_delete_for_tablet(*tablet_id, keys)?;
                }
                PreparedDataOp::RowIdDelete {
                    tablet_id,
                    locations,
                } => {
                    self.replay_row_id_delete_for_tablet(*tablet_id, locations)?;
                }
            }
        }
        self.replay_catalog_drop_ops(catalog_ops, commit_id)?;
        self.record_replayed_post_commit_hooks(_post_commit_hooks);
        self.observe_record(RecordWatermarks {
            commit_id,
            maintenance_id: 0,
            catalog_commit_id: if self.max_catalog_commit_id > before_catalog_commit_id {
                self.max_catalog_commit_id
            } else {
                0
            },
            max_seen_object_id: if self.max_seen_object_id > before_object_id {
                self.max_seen_object_id
            } else {
                0
            },
        });
        Ok(())
    }

    fn replay_journal_record(
        &mut self,
        lsn: u64,
        record: &JournalRecord,
    ) -> paro_common::error::Result<()> {
        self.replay_gap_placeholders_before(lsn)?;
        let before_catalog_commit_id = self.max_catalog_commit_id;
        let before_object_id = self.max_seen_object_id;
        match record {
            JournalRecord::Commit(record) => {
                let commit_visibility = i64::try_from(record.commit_id).map_err(|_| {
                    paro_error::invalid_input("commit_id exceeds supported version range")
                })?;
                self.replay_catalog_non_drop_ops(&record.catalog_ops, record.commit_id)?;
                self.replay_commit_event(lsn, record, commit_visibility)?;
                self.replay_catalog_drop_ops(&record.catalog_ops, record.commit_id)?;
                self.record_replayed_deferred_tasks(&record.deferred_tasks);
                self.observe_journal_record(
                    lsn,
                    record.commit_id,
                    0,
                    before_catalog_commit_id,
                    before_object_id,
                );
            }
            JournalRecord::Maintenance(record) => {
                self.apply_storage_ops(&record.storage_ops, lsn, None)?;
                self.replay_placeholder(lsn, RecoveryPlaceholderRecordKind::Maintenance)?;
                self.apply_descriptors(&record.apply_descriptors, 0)?;
                self.record_replayed_deferred_tasks(&record.deferred_tasks);
                self.observe_journal_record(
                    lsn,
                    0,
                    record.maintenance_id,
                    before_catalog_commit_id,
                    before_object_id,
                );
            }
            JournalRecord::CheckpointFence(_) => {
                self.replay_placeholder(lsn, RecoveryPlaceholderRecordKind::CheckpointFence)?;
                self.observe_journal_record(lsn, 0, 0, before_catalog_commit_id, before_object_id);
            }
        }
        Ok(())
    }
}

pub(crate) struct RecoveryReplayOutcome {
    pub wal: WriteAheadLog,
    pub replay_result: ReplayResult,
    pub summary: RecoverySummary,
    pub replayed_deferred_tasks: Vec<DeferredTask>,
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
) -> paro_common::error::Result<(WriteAheadLog, ReplayResult, RecoverySummary)> {
    let outcome = recover_database_with_bootstrap(
        wal_path,
        catalog,
        tablet_meta_manager,
        RecoverySummary::default(),
    )?;
    Ok((outcome.wal, outcome.replay_result, outcome.summary))
}

pub(crate) fn recover_database_with_bootstrap(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    bootstrap: RecoverySummary,
) -> paro_common::error::Result<RecoveryReplayOutcome> {
    recover_database_with_checkpoint_bootstrap(
        wal_path,
        catalog,
        tablet_meta_manager,
        None,
        None,
        None,
        None,
        None,
        bootstrap,
    )
}

/// Recover a database from committed checkpoint bootstrap plus WAL tail replay.
///
/// # Arguments
/// * `wal_path` - Path to the WAL file
/// * `catalog` - The catalog to restore
///
/// # Returns
/// * `Ok((wal, result))` - Recovery completed successfully
/// * `Err(...)` - Fatal error during recovery
pub fn recover_database_with_checkpoint(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    journal_tail: Option<JournalTailRef>,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: Option<u64>,
) -> paro_common::error::Result<(WriteAheadLog, ReplayResult, RecoverySummary)> {
    let outcome = recover_database_with_checkpoint_bootstrap(
        wal_path,
        catalog,
        tablet_meta_manager,
        journal_tail,
        wal_header_metadata,
        wal_keep_from,
        None,
        None,
        RecoverySummary::default(),
    )?;
    Ok((outcome.wal, outcome.replay_result, outcome.summary))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn recover_database_with_checkpoint_bootstrap(
    wal_path: &Path,
    catalog: &Arc<ParoCatalog>,
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    journal_tail: Option<JournalTailRef>,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: Option<u64>,
    apply_runtime: Option<Arc<JournalApplyRuntime>>,
    commit_runtime: Option<Arc<CommitRuntime>>,
    bootstrap: RecoverySummary,
) -> paro_common::error::Result<RecoveryReplayOutcome> {
    let catalog_store = SegmentCatalogStore::from_seed_path(wal_path);
    let handler = CatalogReplayHandler::new_with_bootstrap(catalog, 0, u64::MAX, bootstrap)
        .with_database_root(
            wal_path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_path_buf(),
        )
        .with_tablet_meta_manager(tablet_meta_manager);
    let mut handler = handler
        .with_apply_runtime(apply_runtime)
        .with_commit_runtime(commit_runtime);

    let replay_cursor = if let Some(journal_tail) = journal_tail {
        ReplayCursor::from_catalog(
            &catalog_store,
            journal_tail.replay_from_segment_id,
            journal_tail.replay_from_lsn,
        )?
    } else {
        ReplayCursor::from_catalog(&catalog_store, 0, 1)?
    };

    let mut replay_result = ReplayResult::success(0, 0);
    for entry in replay_cursor.entries() {
        let mut recovery = WalRecovery::new(&entry.path)
            .with_replay_lsn_bounds(entry.starting_lsn, entry.replay_from_lsn);
        if let Some(metadata) = wal_header_metadata {
            recovery = recovery
                .with_wal_header_metadata(metadata.db_identifier, metadata.checkpoint_iteration);
        }
        if let Some(keep_from) = wal_keep_from {
            recovery = recovery.with_wal_keep_from(keep_from);
        }

        let segment_result = recovery.replay_only(&mut handler)?;
        replay_result.entries_replayed += segment_result.entries_replayed;
        replay_result.last_successful_offset = segment_result.last_successful_offset;
        if !segment_result.all_succeeded {
            replay_result.all_succeeded = false;
            if replay_result.error.is_none() {
                replay_result.error = segment_result.error.clone();
            }
            break;
        }
    }

    if replay_result.all_succeeded {
        handler.finalize_replayed_derived_state()?;
    }
    let summary = handler.summary();
    let recovered_wal = WriteAheadLog::with_state_and_start_lsn(
        wal_path,
        if replay_result.all_succeeded {
            paro_journal::wal::wal_writer::WalInitState::Uninitialized
        } else {
            paro_journal::wal::wal_writer::WalInitState::UninitializedRequiresTruncate
        },
        wal_header_metadata.unwrap_or_default(),
        summary.max_lsn.saturating_add(1),
    )?;
    handler.finalize_object_id_allocator()?;
    let replayed_deferred_tasks = handler.drain_replayed_deferred_tasks();
    catalog.rebuild_dependency_graph()?;
    restore_runtime_art_indexes(catalog);
    if replay_result.all_succeeded {
        sweep_orphan_search_generation_workspaces(catalog);
    }
    restore_search_registry_definitions(catalog);
    let report = build_recovery_consistency_report(catalog);
    log_recovery_consistency_report(&report);
    Ok(RecoveryReplayOutcome {
        wal: recovered_wal,
        replay_result,
        summary,
        replayed_deferred_tasks,
    })
}

/// Check if a WAL file exists and needs recovery.
pub fn needs_recovery(wal_path: &Path) -> bool {
    let catalog_store = SegmentCatalogStore::from_seed_path(wal_path);
    let Ok(Some(catalog)) = catalog_store.load() else {
        return false;
    };
    for segment in catalog.segments {
        let path = catalog_store.layout().segment_path(segment.segment_id);
        if path
            .metadata()
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false)
        {
            return true;
        }
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
    use paro_common::ddl::{
        CreateIndexPayload, CreatePropertyGraphPayload, CreateSchemaPayload, CreateSequencePayload,
        CreateTablePayload, CreateViewPayload, DdlChange, DdlChangeRecord, DdlDependencyObjectRef,
        DdlDependencyRef, DdlObjectKey, DdlObjectKind, DdlStorageDescriptor, DdlWalColumnInfo,
        PropertyGraphVertexPayload,
    };
    use paro_common::effect::{CatalogTxnOp, PreparedDataOp, RowsetLocator};
    use paro_common::types::LogicalType;
    use paro_journal::wal::wal_entry::{ColumnInfo, WalEntry};
    use paro_journal::wal::wal_type::WalType;
    use paro_journal::wal::wal_writer::WalWriter;
    use paro_journal::wal::write_ahead_log::WriteAheadLog;
    use paro_storage::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    fn create_table(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn create_test_meta_manager(temp_dir: &TempDir) -> Arc<TabletMetaManager> {
        let store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(temp_dir.path().join("meta")).unwrap());
        Arc::new(TabletMetaManager::with_store_and_data_root(
            store,
            temp_dir.path(),
        ))
    }

    fn create_table_with_meta_manager(
        types: &[LogicalType],
        meta_manager: Arc<TabletMetaManager>,
    ) -> TableHandle {
        TableFactory::new(Some(meta_manager))
            .create_table(types)
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

    fn ensure_main_schema(catalog: &Arc<ParoCatalog>) {
        let info = CreateSchemaInfo {
            catalog: catalog.name().to_string(),
            name: "main".to_string(),
            internal: false,
            on_conflict: OnCreateConflict::IgnoreOnConflict,
        };
        let entry = Arc::new(CatalogEntryEnum::Schema(Arc::new(
            paro_catalog::entry::SchemaEntry::from_info(
                &info,
                Arc::clone(catalog.object_id_allocator()),
                catalog.gc_epoch_handle(),
                0,
            ),
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
            TableCatalogEntry::from_info(
                info,
                storage,
                catalog.object_id_allocator().allocate(),
                0,
            )
            .unwrap(),
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
        let begin = WalEntry::TxnBegin {
            txn_id,
            start_time: 0,
        };
        writer
            .write_entry(WalType::TxnBegin, &begin.serialize_data())
            .unwrap();
        for (seq, change) in changes.into_iter().enumerate() {
            let op = CatalogTxnOp { change };
            let entry = WalEntry::TxnCatalogOp {
                seq: seq as u32,
                op,
            };
            writer
                .write_entry(WalType::TxnCatalogOp, &entry.serialize_data())
                .unwrap();
        }
        let commit = WalEntry::TxnCommit { txn_id, commit_id };
        writer
            .write_entry(WalType::TxnCommit, &commit.serialize_data())
            .unwrap();
        writer.flush().unwrap();
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
        let temp_dir = tempdir().unwrap();
        let meta_manager = create_test_meta_manager(&temp_dir);
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX)
            .with_tablet_meta_manager(Some(meta_manager.clone()));

        let columns = [
            ColumnInfo::new("id".to_string(), LogicalType::Integer, false),
            ColumnInfo::new("name".to_string(), LogicalType::Varchar, true),
        ];
        let seed_storage = create_table_with_meta_manager(
            &[LogicalType::Integer, LogicalType::Varchar],
            meta_manager,
        );
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
            provider_config_json: "{}".to_string(),
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
    fn test_catalog_replay_create_index_preserves_provider_config() {
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
            // Replay is an opaque transport boundary. Provider validation is
            // performed when the search definition is registered.
            provider_config_json: r#"{"marker":"preserved"}"#.to_string(),
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
        assert_eq!(
            index.provider_config,
            serde_json::json!({"marker": "preserved"})
        );
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

        let insert = paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_i32_vector(&[1, 2, 3]),
        ]);
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
            catalog.object_id_allocator().allocate(),
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
    fn test_restore_runtime_art_indexes_accepts_complete_bitmap_representation() {
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        ensure_main_schema(&catalog);

        let storage = Arc::new(create_table(&[LogicalType::Integer]));
        let columns = vec![ColumnDefinition::new(
            "bucket".to_string(),
            LogicalType::Integer,
        )];
        install_committed_table(&catalog, "main", "events", columns, Arc::clone(&storage));

        let values = (0..128).map(|value| value % 2).collect::<Vec<i32>>();
        let insert = paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_i32_vector(&values),
        ]);
        storage.append(&insert).unwrap();

        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "main").unwrap();
        let table_entry = schema
            .get_table(txn.transaction_id, txn.start_time, "events")
            .expect("events table should exist");
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            panic!("expected table entry");
        };
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "events".to_string(),
            "idx_events_bucket".to_string(),
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
            catalog.object_id_allocator().allocate(),
        ))));
        schema
            .collection(CatalogType::Index)
            .expect("index collection")
            .install_committed(entry, InstallMode::RejectExisting)
            .unwrap();

        restore_runtime_art_indexes(&catalog);

        let entry = schema
            .get_index(txn.transaction_id, txn.start_time, "idx_events_bucket")
            .expect("index should exist");
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            panic!("expected index entry");
        };
        assert_eq!(index.build_state(), IndexBuildState::Ready);
        assert!(index
            .coverage()
            .is_some_and(|coverage| coverage.is_complete()));
        assert!(storage
            .collect_segments(storage.max_version())
            .unwrap()
            .iter()
            .all(|(_, segment)| segment.art_index(0).is_none()
                && segment.bitmap_index(0).is_some()
                && segment.has_complete_scalar_index(0)));
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

        let insert = paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_i32_vector(&[1, 2, 3]),
        ]);
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
            catalog.object_id_allocator().allocate(),
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
                Arc::clone(catalog.object_id_allocator()),
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
                catalog.object_id_allocator().allocate(),
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
            paro_catalog::entry::SchemaEntry::from_info(
                &info,
                Arc::clone(catalog.object_id_allocator()),
                catalog.gc_epoch_handle(),
                0,
            ),
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

        let (wal, result, _summary) = recover_database(&wal_path, &catalog, None).unwrap();

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
            let wal = WriteAheadLog::new(&wal_path).unwrap();
            let write_state = wal.begin_write();
            let writer = write_state.wal();
            paro_journal::wal::test_support::write_flushed_create_schema_txn_with_object_id(
                writer.as_ref(),
                "test",
                "test_schema",
                replayed_schema_oid,
                1,
                100,
            )
            .unwrap();
        }

        let (_wal, result, _summary) = recover_database(&wal_path, &catalog, None).unwrap();

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
        // The allocator is shared process-wide, so concurrent catalog work may consume IDs after
        // the watermark is observed. Recovery guarantees monotonicity past the replayed maximum,
        // not that this catalog receives the immediately adjacent ID.
        assert!(created.base.object_id.raw() >= replay_watermark);
        assert!(created.base.object_id.raw() > replayed_schema_oid);
    }

    #[test]
    fn test_recover_database_restores_schema_table_view_index_and_property_graph() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("combo.wal");
        let catalog = Arc::new(ParoCatalog::new("test".to_string()));
        catalog.initialize(false);
        let meta_manager = create_test_meta_manager(&dir);

        let seed_storage =
            create_table_with_meta_manager(&[LogicalType::Integer], meta_manager.clone());
        let descriptor = seed_storage.to_descriptor().unwrap();
        let schema_oid = 7_001;
        let table_oid = 7_002;
        let view_oid = 7_003;
        let index_oid = 7_004;
        let graph_oid = 7_005;

        let wal = WriteAheadLog::new(&wal_path).unwrap();
        let write_state = wal.begin_write();
        let writer = write_state.wal();
        write_flushed_catalog_txn(
            writer.as_ref(),
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
                        provider_config_json: "{}".to_string(),
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

        let (_wal, result, _summary) =
            recover_database(&wal_path, &catalog, Some(meta_manager)).unwrap();
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
        assert!(created.base.object_id.raw() >= next_object_id);
        assert!(created.base.object_id.raw() > replayed_object_id);
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
        let source_chunk = paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_i32_vector(&[1, 2, 3]),
        ]);
        source_storage.append(&source_chunk).unwrap();

        let source_descriptor = source_storage.to_descriptor().unwrap();
        let rowset_dir = find_first_segment_dir(Path::new(&source_descriptor.data_dir))
            .expect("expected source rowset directory with segment files");

        let target_descriptor = target_storage.to_descriptor().unwrap();
        let mut handler = CatalogReplayHandler::new(&catalog, 0, u64::MAX);
        handler
            .replay_rowset_commit(
                target_descriptor.tablet_id,
                9_999,
                1,
                1,
                rowset_dir.to_string_lossy().as_ref(),
            )
            .unwrap();

        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows().unwrap(), 3);

        // Rowset commit replay is idempotent for the same rowset_id.
        handler
            .replay_rowset_commit(
                target_descriptor.tablet_id,
                9_999,
                1,
                1,
                rowset_dir.to_string_lossy().as_ref(),
            )
            .unwrap();
        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows().unwrap(), 3);
    }

    #[test]
    fn test_replay_transaction_counts_rowset_commit_once_in_summary() {
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
        let source_chunk = paro_common::test_utils::test_chunk_from_vectors(vec![
            paro_common::test_utils::test_i32_vector(&[1, 2, 3]),
        ]);
        source_storage.append(&source_chunk).unwrap();
        let source_descriptor = source_storage.to_descriptor().unwrap();
        let rowset_dir = find_first_segment_dir(Path::new(&source_descriptor.data_dir))
            .expect("expected source rowset directory with segment files");

        let target_descriptor = target_storage.to_descriptor().unwrap();
        let mut handler = CatalogReplayHandler::new_with_bootstrap(
            &catalog,
            0,
            u64::MAX,
            RecoverySummary {
                max_lsn: 7,
                max_commit_id: 10,
                max_maintenance_id: 0,
                max_catalog_commit_id: 0,
                max_seen_object_id: 0,
            },
        );
        handler
            .replay_transaction(
                &[],
                &[PreparedDataOp::RowsetCommit {
                    locator: RowsetLocator {
                        tablet_id: target_descriptor.tablet_id,
                        rowset_id: 9_999,
                        path_components: vec![rowset_dir.to_string_lossy().to_string()],
                    },
                    start_version: 1,
                    end_version: 11,
                }],
                &[],
                11,
            )
            .unwrap();

        let summary = handler.summary();
        assert_eq!(summary.max_lsn, 8);
        assert_eq!(summary.max_commit_id, 11);
        assert_eq!(target_storage.rowset_count(), 1);
        assert_eq!(target_storage.total_rows().unwrap(), 3);
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
            provider_config_json: "{}".to_string(),
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

        // Seed path without a segment catalog
        let empty_path = dir.path().join("empty.wal");
        std::fs::write(&empty_path, &[]).unwrap();
        assert!(!needs_recovery(&empty_path));

        // Segment-backed WAL with a flushed transaction
        let content_path = dir.path().join("content.wal");
        let wal = WriteAheadLog::new(&content_path).unwrap();
        let write_state = wal.begin_write();
        let writer = write_state.wal();
        paro_journal::wal::test_support::write_flushed_create_schema_txn(
            writer.as_ref(),
            "test",
            "content_schema",
            1,
            10,
        )
        .unwrap();
        assert!(needs_recovery(&content_path));

        // Legacy sidecars no longer participate in recovery probing.
        let legacy_sidecar_seed = dir.path().join("legacy-sidecar.wal");
        std::fs::write(
            dir.path().join("legacy-sidecar.checkpoint.wal"),
            b"checkpoint content",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("legacy-sidecar.recovery.wal"),
            b"recovery content",
        )
        .unwrap();
        assert!(
            !needs_recovery(&legacy_sidecar_seed),
            "sidecars alone should not trigger recovery without a segment catalog"
        );
    }
}
