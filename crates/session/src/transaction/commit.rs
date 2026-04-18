// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::ddl_changes::PreparedCatalogOp;
use super::session_transaction::FrozenTransaction;
use crate::session::Session;
use paro_catalog::database_catalog::ParoCatalog;
use paro_common::ddl::{DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind};
use paro_common::durability::{PreparedCommitPlan, PreparedTabletPlan};
use paro_common::effect::{
    ApplyDescriptor, DeferredTask, PostCommitHookDescriptor, RuntimeTransitionDescriptor,
    StorageCommitOp, TabletMutation,
};
use paro_common::error::{ParoError, Result};
use paro_common::logging::targets;
use paro_instance::{CatalogReplayHandler, DatabaseHandle, RouteRegistry};
use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::transaction::txn::{PreparedStorageCommit, Transaction};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug)]
pub struct CommitOutcome {
    pub commit_id: u64,
    pub lsn: u64,
    pub durable_batch_lsn: u64,
    pub durable_batch_size: u64,
    pub durable_batch_bytes: u64,
    pub sync_latency_micros: u64,
    pub publish_wait_micros: u64,
    pub published_at: Instant,
    pub active_txn: Arc<Transaction>,
    pub catalog_ops: Vec<PreparedCatalogOp>,
    pub deferred_tasks: Vec<DeferredTask>,
}

#[derive(Debug)]
pub struct CommitFailure {
    pub error: ParoError,
    pub rollback_succeeded: bool,
}

struct RuntimeApplyBuildContext {
    database: Arc<DatabaseHandle>,
    catalog: Arc<ParoCatalog>,
    database_root: PathBuf,
    tablet_meta_manager: Option<Arc<paro_storage::meta::TabletMetaManager>>,
    graph_registry: Arc<paro_storage::index::graph::GraphProjectionIndexManager>,
    manager: Arc<paro_storage::transaction::manager::TransactionManager>,
    apply_state: Arc<Mutex<Option<CommitApplyState>>>,
}

pub struct CommitPipeline<'a> {
    session: &'a Session,
    frozen: FrozenTransaction,
}

impl CommitOutcome {
    fn observe_published(&mut self, wait_micros: u64) {
        self.publish_wait_micros = wait_micros;
        self.published_at = Instant::now();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogApplyPhase {
    Create,
    Alter,
    Drop,
}

fn catalog_apply_phase(record: &DdlChangeRecord) -> CatalogApplyPhase {
    match &record.change {
        DdlChange::CreateSchema(_)
        | DdlChange::CreateTable(_)
        | DdlChange::CreateView(_)
        | DdlChange::CreateIndex(_)
        | DdlChange::CreatePropertyGraph(_)
        | DdlChange::CreateSequence(_) => CatalogApplyPhase::Create,
        DdlChange::AlterEntry(_) => CatalogApplyPhase::Alter,
        DdlChange::DropSchema(_)
        | DdlChange::DropTable(_)
        | DdlChange::DropView(_)
        | DdlChange::DropIndex(_)
        | DdlChange::DropPropertyGraph(_)
        | DdlChange::DropSequence(_) => CatalogApplyPhase::Drop,
    }
}

impl<'a> CommitPipeline<'a> {
    pub fn new(session: &'a Session, frozen: FrozenTransaction) -> Self {
        Self { session, frozen }
    }

    pub fn execute(self) -> std::result::Result<CommitOutcome, CommitFailure> {
        let session = self.session;
        let FrozenTransaction {
            active,
            mut ddl_changes,
        } = self.frozen;

        let prepared_storage = match active.prepare_commit() {
            Ok(prepared) => prepared,
            Err(error) => {
                let rollback_succeeded = Self::rollback_catalog_changes(&mut ddl_changes).is_ok()
                    && session
                        .current_database
                        .transaction_manager()
                        .rollback_transaction(Arc::clone(&active))
                        .is_ok();
                return Err(CommitFailure {
                    error,
                    rollback_succeeded,
                });
            }
        };

        if ddl_changes.is_empty() && prepared_storage.is_empty() {
            if let Err(error) = session
                .current_database
                .transaction_manager()
                .complete_read_only_transaction(Arc::clone(&active))
            {
                return Err(CommitFailure {
                    error,
                    rollback_succeeded: false,
                });
            }

            return Ok(CommitOutcome {
                commit_id: 0,
                lsn: 0,
                durable_batch_lsn: 0,
                durable_batch_size: 0,
                durable_batch_bytes: 0,
                sync_latency_micros: 0,
                publish_wait_micros: 0,
                published_at: Instant::now(),
                active_txn: active,
                catalog_ops: ddl_changes,
                deferred_tasks: Vec::new(),
            });
        }

        let Some(coordinator) = session.current_database.journal_coordinator() else {
            return Err(CommitFailure {
                error: paro_common::error::internal("database journal coordinator missing"),
                rollback_succeeded: false,
            });
        };
        let Some(apply_runtime) = session.current_database.journal_apply_runtime() else {
            return Err(CommitFailure {
                error: paro_common::error::internal("database apply runtime missing"),
                rollback_succeeded: false,
            });
        };
        coordinator.sync_commit_id_with(
            session
                .current_database
                .transaction_manager()
                .durable_commit_id(),
        );

        let manager = Arc::clone(session.current_database.transaction_manager());
        let catalog = session.current_database.catalog().clone();
        let database_root = PathBuf::from(session.current_database.path());
        let tablet_meta_manager = session.current_database.tablet_meta_manager();
        let graph_registry = Arc::clone(session.instance.graph_manager());

        let mut prepared_storage = prepared_storage;
        let mut remaining_reprepare_attempts = 2usize;
        let submit_result = loop {
            let prepared_plan =
                match Self::build_prepared_commit_plan(&active, &ddl_changes, &prepared_storage) {
                    Ok(plan) => plan,
                    Err(error) => {
                        let rollback_succeeded = Self::rollback_catalog_changes(&mut ddl_changes)
                            .is_ok()
                            && session
                                .current_database
                                .transaction_manager()
                                .rollback_transaction(Arc::clone(&active))
                                .is_ok();
                        return Err(CommitFailure {
                            error,
                            rollback_succeeded,
                        });
                    }
                };
            let prepared_tablets = prepared_storage.tablets.clone();
            match coordinator.submit_commit_context(prepared_plan, move |_| {
                for tablet in &prepared_tablets {
                    tablet.tablet.validate_prepare_token(&tablet.token)?;
                }
                Ok(())
            }) {
                Ok(ctx) => break Ok(ctx),
                Err(error) if error.is_retryable() && remaining_reprepare_attempts > 0 => {
                    remaining_reprepare_attempts = remaining_reprepare_attempts.saturating_sub(1);
                    tracing::info!(
                        target: targets::TRANSACTION,
                        txn_id = active.id,
                        error = %error,
                        attempts_remaining = remaining_reprepare_attempts,
                        "commit prepare token went stale before durable append; repreparing commit plan"
                    );
                    match active.reprepare_commit(&prepared_storage.post_commit_hooks) {
                        Ok(next_prepared_storage) => {
                            prepared_storage = next_prepared_storage;
                        }
                        Err(reprepare_error) => {
                            let rollback_succeeded =
                                Self::rollback_catalog_changes(&mut ddl_changes).is_ok()
                                    && session
                                        .current_database
                                        .transaction_manager()
                                        .rollback_transaction(Arc::clone(&active))
                                        .is_ok();
                            return Err(CommitFailure {
                                error: reprepare_error,
                                rollback_succeeded,
                            });
                        }
                    }
                }
                Err(error) => break Err(error),
            }
        };

        match submit_result {
            Ok(ctx) => {
                let apply_state = Arc::new(Mutex::new(Some(CommitApplyState {
                    active: Arc::clone(&active),
                    deferred_tasks: Self::collect_deferred_tasks(
                        &prepared_storage.post_commit_hooks,
                        &ddl_changes,
                    ),
                    catalog_ops: ddl_changes,
                })));
                manager.mark_durable_commit(ctx.commit_id);
                let request = match Self::build_runtime_apply_request(
                    ctx,
                    RuntimeApplyBuildContext {
                        database: Arc::clone(&session.current_database),
                        catalog: Arc::clone(&catalog),
                        database_root,
                        tablet_meta_manager,
                        graph_registry,
                        manager: Arc::clone(&manager),
                        apply_state: Arc::clone(&apply_state),
                    },
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        return Err(CommitFailure {
                            error,
                            rollback_succeeded: false,
                        });
                    }
                };

                apply_runtime
                    .submit_observed(request)
                    .map(|mut observed| {
                        observed.value.observe_published(observed.wait_micros);
                        tracing::info!(
                            target: targets::WAL,
                            commit_id = observed.value.commit_id,
                            lsn = observed.value.lsn,
                            durable_batch_lsn = observed.value.durable_batch_lsn,
                            group_size = observed.value.durable_batch_size,
                            batch_bytes = observed.value.durable_batch_bytes,
                            sync_latency_micros = observed.value.sync_latency_micros,
                            publish_wait_micros = observed.value.publish_wait_micros,
                            "Commit published through WAL"
                        );
                        observed.value
                    })
                    .map_err(|error| CommitFailure {
                        error,
                        rollback_succeeded: false,
                    })
            }
            Err(error) => {
                let rollback_succeeded = Self::rollback_catalog_changes(&mut ddl_changes).is_ok()
                    && session
                        .current_database
                        .transaction_manager()
                        .rollback_transaction(Arc::clone(&active))
                        .is_ok();
                Err(CommitFailure {
                    error,
                    rollback_succeeded,
                })
            }
        }
    }

    fn rollback_catalog_changes(ddl_changes: &mut [PreparedCatalogOp]) -> Result<()> {
        for op in ddl_changes.iter_mut().rev() {
            if let Some(delta) = op.dependencies.take() {
                delta.discard();
            }
            if let Some(handle) = op.catalog.take() {
                handle.discard()?;
            }
        }
        Ok(())
    }

    fn build_prepared_commit_plan(
        active_txn: &Arc<Transaction>,
        ddl_changes: &[PreparedCatalogOp],
        prepared_storage: &PreparedStorageCommit,
    ) -> Result<PreparedCommitPlan> {
        Ok(PreparedCommitPlan {
            txn_id: active_txn.id,
            start_time: active_txn.start_time,
            catalog_ops: ddl_changes
                .iter()
                .map(|op| paro_common::effect::CatalogTxnOp {
                    change: op.record.clone(),
                })
                .collect(),
            storage_ops: Self::journal_storage_ops(prepared_storage)?,
            apply_descriptors: Self::collect_apply_descriptors(ddl_changes),
            deferred_tasks: Self::collect_deferred_tasks(
                &prepared_storage.post_commit_hooks,
                ddl_changes,
            ),
            tablets: prepared_storage
                .tablets
                .iter()
                .map(|tablet| PreparedTabletPlan::new(tablet.tablet.tablet_id(), tablet.token))
                .collect(),
        })
    }

    fn build_runtime_apply_request(
        ctx: paro_journal::CommitExecutionContext,
        runtime: RuntimeApplyBuildContext,
    ) -> Result<ApplyRequest<CommitOutcome>> {
        let RuntimeApplyBuildContext {
            database,
            catalog,
            database_root,
            tablet_meta_manager,
            graph_registry,
            manager,
            apply_state,
        } = runtime;

        let publish_active = apply_state
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| Arc::clone(&state.active))
            .ok_or_else(|| paro_common::error::internal("commit apply state missing"))?;
        let registry_slot = Arc::new(Mutex::new(database.route_registry_snapshot()));
        let commit_id = ctx.commit_id;
        let has_catalog_lane = !ctx.record.catalog_ops.is_empty();

        let catalog_pre = {
            let catalog = Arc::clone(&catalog);
            let registry_slot = Arc::clone(&registry_slot);
            let apply_state = Arc::clone(&apply_state);
            Box::new(move || {
                let mut guard = apply_state.lock().unwrap();
                let state = guard
                    .as_mut()
                    .ok_or_else(|| paro_common::error::internal("commit apply state missing"))?;
                Self::apply_catalog_phase(
                    &catalog,
                    &mut state.catalog_ops,
                    commit_id,
                    CatalogApplyPhase::Create,
                )?;
                Self::apply_catalog_phase(
                    &catalog,
                    &mut state.catalog_ops,
                    commit_id,
                    CatalogApplyPhase::Alter,
                )?;
                if has_catalog_lane {
                    let mut registry = registry_slot.lock().unwrap();
                    Self::sync_route_registry_for_runtime_ops(
                        &catalog,
                        &mut registry,
                        &state.catalog_ops,
                        CatalogApplyPhase::Create,
                    )?;
                    Self::sync_route_registry_for_runtime_ops(
                        &catalog,
                        &mut registry,
                        &state.catalog_ops,
                        CatalogApplyPhase::Alter,
                    )?;
                }
                Ok(())
            }) as Box<dyn FnOnce() -> Result<()> + Send>
        };

        let tablet_parts = ctx
            .record
            .storage_ops
            .iter()
            .map(|storage_op| {
                let storage_op = storage_op.clone();
                let registry_slot = Arc::clone(&registry_slot);
                TabletApplyPart {
                    tablet_id: storage_op.tablet_id(),
                    apply: Box::new(move || {
                        let storage = {
                            let registry = registry_slot.lock().unwrap().clone();
                            registry
                                .route_tablet(storage_op.tablet_id())
                                .cloned()
                                .map(|route| route.storage)
                                .ok_or_else(|| {
                                    paro_common::error::internal(format!(
                                        "runtime apply route missing for tablet {}",
                                        storage_op.tablet_id()
                                    ))
                                })?
                        };
                        Self::apply_storage_op(
                            storage.as_ref(),
                            &storage_op,
                            ctx.lsn,
                            i64::try_from(commit_id).map_err(|_| {
                                paro_common::error::invalid_input(
                                    "commit_id exceeds supported version range",
                                )
                            })?,
                            &registry_slot.lock().unwrap().clone(),
                        )
                    }),
                }
            })
            .collect::<Vec<_>>();

        let descriptor_phase = {
            let catalog = Arc::clone(&catalog);
            let registry_slot = Arc::clone(&registry_slot);
            let descriptors = ctx.record.apply_descriptors.clone();
            Box::new(move || {
                if descriptors.is_empty() {
                    return Ok(());
                }
                let registry = registry_slot.lock().unwrap().clone();
                let mut applier = CatalogReplayHandler::new(&catalog, 0, u64::MAX)
                    .with_database_root(database_root)
                    .with_tablet_meta_manager(tablet_meta_manager)
                    .with_registry(registry)
                    .with_graph_registry(graph_registry);
                applier.apply_descriptors(&descriptors, commit_id)
            }) as Box<dyn FnOnce() -> Result<()> + Send>
        };

        let catalog_post = {
            let catalog = Arc::clone(&catalog);
            let database = Arc::clone(&database);
            let apply_state = Arc::clone(&apply_state);
            Box::new(move || {
                let mut state =
                    apply_state.lock().unwrap().take().ok_or_else(|| {
                        paro_common::error::internal("commit apply state missing")
                    })?;
                Self::apply_catalog_phase(
                    &catalog,
                    &mut state.catalog_ops,
                    commit_id,
                    CatalogApplyPhase::Drop,
                )?;
                if has_catalog_lane {
                    let mut registry = registry_slot.lock().unwrap();
                    Self::sync_route_registry_for_runtime_ops(
                        &catalog,
                        &mut registry,
                        &state.catalog_ops,
                        CatalogApplyPhase::Drop,
                    )?;
                    let previous_registry = database.replace_route_registry(registry.clone());
                    let runtime_table_keys = Self::runtime_table_keys_for_catalog_ops(
                        &previous_registry,
                        &state.catalog_ops,
                    );
                    database
                        .sync_runtime_table_keys_incremental(
                            &previous_registry,
                            &registry,
                            &runtime_table_keys,
                        )
                        .map_err(|err| {
                            paro_common::error::internal(format!(
                                "incremental runtime table sync failed after catalog apply: {}",
                                err
                            ))
                        })?;
                }
                state.active.finalize_commit_after_apply(commit_id)?;
                Ok(CommitOutcome {
                    commit_id,
                    lsn: ctx.lsn,
                    durable_batch_lsn: ctx.durable_batch_lsn,
                    durable_batch_size: ctx.durable_batch_size,
                    durable_batch_bytes: ctx.durable_batch_bytes,
                    sync_latency_micros: ctx.sync_latency_micros,
                    publish_wait_micros: 0,
                    published_at: Instant::now(),
                    active_txn: state.active,
                    catalog_ops: state.catalog_ops,
                    deferred_tasks: state.deferred_tasks,
                })
            }) as Box<dyn FnOnce() -> Result<CommitOutcome> + Send>
        };

        let on_published = Box::new(move || {
            manager.publish_committed_transaction(Arc::clone(&publish_active), commit_id)?;
            Ok(())
        });

        Ok(ApplyRequest {
            lsn: ctx.lsn,
            durable_batch_lsn: ctx.durable_batch_lsn,
            commit_id: Some(commit_id),
            wait_mode: WaitMode::Published,
            catalog_serial: has_catalog_lane,
            catalog_pre,
            tablet_parts,
            descriptor_phase,
            catalog_post,
            on_published,
        })
    }

    fn apply_catalog_phase(
        catalog: &Arc<ParoCatalog>,
        ops: &mut [PreparedCatalogOp],
        commit_id: u64,
        phase: CatalogApplyPhase,
    ) -> Result<()> {
        for op in ops.iter_mut() {
            if catalog_apply_phase(&op.record) != phase {
                continue;
            }
            if let Some(handle) = op.catalog.take() {
                handle.publish(commit_id)?;
            }
            if let Some(delta) = op.dependencies.take() {
                delta.publish(catalog.dependency_graph())?;
            }
        }
        Ok(())
    }

    fn apply_storage_op(
        storage: &TableHandle,
        op: &StorageCommitOp,
        lsn: u64,
        commit_visibility: i64,
        registry: &RouteRegistry,
    ) -> Result<()> {
        match op {
            StorageCommitOp::Tablet(tablet) => {
                for mutation in &tablet.mutations {
                    match mutation {
                        TabletMutation::PublishRowset {
                            rowset_id,
                            version_span,
                            rowset_ref,
                        } => {
                            storage.replay_rowset_commit(
                                *rowset_id,
                                version_span.start,
                                version_span.end,
                                &rowset_ref
                                    .resolve_for_tablet(storage.tablet().data_dir())
                                    .to_string_lossy(),
                            )?;
                            registry.note_rowset_owner(*rowset_id, tablet.tablet_id);
                        }
                        TabletMutation::ApplyDeletePatch { patch, .. } => {
                            let locations =
                                patch.decode_row_refs_for_tablet(storage.tablet().data_dir())?;
                            storage
                                .replay_row_id_delete_at_version(&locations, commit_visibility)?;
                        }
                        TabletMutation::PublishCompaction { .. } => {
                            storage.apply_compaction_publish(mutation)?;
                            if let TabletMutation::PublishCompaction {
                                output_rowset_id,
                                replaced_inputs,
                                retired_inputs,
                                ..
                            } = mutation
                            {
                                registry.note_rowset_owner(*output_rowset_id, tablet.tablet_id);
                                for rowset_id in replaced_inputs {
                                    registry.forget_rowset_owner(*rowset_id);
                                }
                                for input in retired_inputs {
                                    registry.forget_rowset_owner(input.rowset_id);
                                }
                            }
                        }
                    }
                }
                registry.note_tablet_applied_lsn(
                    tablet.tablet_id,
                    storage.tablet().applied_lsn().max(lsn),
                );
            }
        }
        storage.tablet().note_applied_lsn(lsn)?;
        Ok(())
    }

    fn sync_route_registry_for_runtime_ops(
        catalog: &Arc<ParoCatalog>,
        registry: &mut RouteRegistry,
        ops: &[PreparedCatalogOp],
        phase: CatalogApplyPhase,
    ) -> Result<()> {
        for target in Self::route_registry_targets_for_phase(registry, ops, phase) {
            registry.sync_table_from_catalog(catalog, &target)?;
        }
        Ok(())
    }

    fn route_registry_targets_for_phase(
        registry: &RouteRegistry,
        ops: &[PreparedCatalogOp],
        phase: CatalogApplyPhase,
    ) -> Vec<DdlObjectKey> {
        let mut targets = HashSet::new();
        for op in ops {
            if catalog_apply_phase(&op.record) != phase {
                continue;
            }
            match &op.record.change {
                DdlChange::DropSchema(_) if phase == CatalogApplyPhase::Drop => {
                    for target in
                        registry.table_keys_in_schema(&op.record.key.database, &op.record.key.name)
                    {
                        targets.insert(target);
                    }
                }
                _ => {
                    for target in &op.dml_targets {
                        if target.kind == DdlObjectKind::Table {
                            targets.insert(target.clone());
                        }
                    }
                }
            }
        }
        targets.into_iter().collect()
    }

    fn runtime_table_keys_for_catalog_ops(
        previous_registry: &RouteRegistry,
        ops: &[PreparedCatalogOp],
    ) -> Vec<DdlObjectKey> {
        let mut targets = HashSet::new();
        for op in ops {
            match &op.record.change {
                DdlChange::DropSchema(_) => {
                    for target in previous_registry
                        .table_keys_in_schema(&op.record.key.database, &op.record.key.name)
                    {
                        targets.insert(target);
                    }
                }
                _ => {
                    for target in &op.dml_targets {
                        if target.kind == DdlObjectKind::Table {
                            targets.insert(target.clone());
                        }
                    }
                }
            }
        }
        targets.into_iter().collect()
    }

    fn collect_apply_descriptors(ddl_changes: &[PreparedCatalogOp]) -> Vec<ApplyDescriptor> {
        let mut descriptors = Vec::new();
        for op in ddl_changes {
            descriptors.extend(
                op.staged_artifacts
                    .iter()
                    .cloned()
                    .map(ApplyDescriptor::PublishStagedArtifact),
            );
            descriptors.extend(
                op.runtime_transitions
                    .iter()
                    .cloned()
                    .map(ApplyDescriptor::RuntimeTransition),
            );
            descriptors.extend(op.cleanups.iter().cloned().map(ApplyDescriptor::Cleanup));
        }
        descriptors
    }

    fn collect_deferred_tasks(
        prepared_hooks: &[PostCommitHookDescriptor],
        ddl_changes: &[PreparedCatalogOp],
    ) -> Vec<DeferredTask> {
        let mut tasks = prepared_hooks
            .iter()
            .cloned()
            .map(Self::hook_to_deferred_task)
            .collect::<Vec<_>>();
        for op in ddl_changes {
            tasks.extend(
                op.post_commit_hooks
                    .iter()
                    .cloned()
                    .map(Self::hook_to_deferred_task),
            );
            tasks.extend(
                op.runtime_transitions
                    .iter()
                    .filter_map(Self::runtime_to_deferred_task),
            );
        }
        tasks
    }

    fn journal_storage_ops(
        prepared_storage: &PreparedStorageCommit,
    ) -> Result<Vec<StorageCommitOp>> {
        Ok(prepared_storage.storage_ops.to_vec())
    }

    fn hook_to_deferred_task(hook: PostCommitHookDescriptor) -> DeferredTask {
        match hook {
            PostCommitHookDescriptor::GraphDmlMaintenance { deltas } => {
                DeferredTask::GraphDmlMaintenance { deltas }
            }
        }
    }

    fn runtime_to_deferred_task(transition: &RuntimeTransitionDescriptor) -> Option<DeferredTask> {
        match transition {
            RuntimeTransitionDescriptor::AttachIndexRuntime {
                index,
                table_name,
                index_type,
                column_ids,
                fulltext_config,
            } => Some(DeferredTask::BuildIndexRuntime {
                index: index.clone(),
                table_name: table_name.clone(),
                index_type: index_type.clone(),
                column_ids: column_ids.clone(),
                fulltext_config: fulltext_config.clone(),
            }),
            _ => None,
        }
    }
}

struct CommitApplyState {
    active: Arc<Transaction>,
    catalog_ops: Vec<PreparedCatalogOp>,
    deferred_tasks: Vec<DeferredTask>,
}

impl Session {
    pub(crate) fn commit_via_pipeline(&mut self) -> Result<()> {
        let frozen = self.transaction.freeze()?;
        let pipeline = CommitPipeline::new(self, frozen);
        match pipeline.execute() {
            Ok(outcome) => {
                super::post_commit::PostCommitActions::execute(self, outcome)?;
                Ok(())
            }
            Err(failure) => {
                if failure.rollback_succeeded {
                    self.on_transaction_rollback_prepared();
                    self.notify_transaction_rollback(None);
                    crate::utility::settings::reconcile_effective_settings(self)?;
                    self.refresh_session_metadata();
                } else {
                    tracing::warn!(
                        target: targets::TRANSACTION,
                        error = %failure.error,
                        "commit failed after durable rollback cleanup was unavailable"
                    );
                }
                Err(failure.error)
            }
        }
    }

    pub(crate) fn commit_auto_transaction(&mut self) -> Result<()> {
        self.commit_via_pipeline()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestSessionBuilder;
    use crate::transaction::ddl_changes::PreparedCatalogOp;
    use paro_catalog::dependency::DependencyDelta;
    use paro_catalog::entry::{CatalogObjectId, CatalogObjectRef, CatalogType, DependencyType};
    use paro_common::ddl::{
        CreateSchemaPayload, DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind,
    };
    use paro_context::DdlExecutionProfile;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex as StdMutex};
    use std::thread;
    use std::time::{Duration, Instant};

    fn staged_dependency_change(catalog_name: &str) -> PreparedCatalogOp {
        let schema_id = CatalogObjectId::from_raw(90_001);
        let view_id = CatalogObjectId::from_raw(90_002);
        let mut dependencies = DependencyDelta::new();
        dependencies.add_object(CatalogObjectRef::schema(
            schema_id,
            catalog_name.to_string(),
            "dep_schema".to_string(),
        ));
        dependencies.add_object(CatalogObjectRef::in_schema(
            view_id,
            CatalogType::View,
            catalog_name.to_string(),
            Some(schema_id),
            "dep_schema".to_string(),
            "dep_view".to_string(),
        ));
        dependencies.add_dependency(view_id, schema_id, DependencyType::OwnedBy);

        PreparedCatalogOp {
            record: DdlChangeRecord {
                key: DdlObjectKey::new(
                    catalog_name,
                    None::<String>,
                    "dep_schema",
                    DdlObjectKind::Schema,
                ),
                change: DdlChange::CreateSchema(CreateSchemaPayload {
                    object_id: schema_id.raw(),
                    if_not_exists: false,
                }),
            },
            profile: DdlExecutionProfile::metadata_only(),
            catalog: None,
            dependencies: Some(dependencies),
            dml_targets: Vec::new(),
            staged_artifacts: Vec::new(),
            runtime_transitions: Vec::new(),
            cleanups: Vec::new(),
            post_commit_hooks: Vec::new(),
            transient_runtime: None,
        }
    }

    #[test]
    fn commit_pipeline_commits_frozen_transaction() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();
        let catalog_name = session.current_database.catalog().name().to_string();
        session
            .transaction
            .ddl_changes()
            .lock()
            .unwrap()
            .record(staged_dependency_change(&catalog_name));

        let frozen = session.transaction.freeze().unwrap();
        let outcome = CommitPipeline::new(&session, frozen).execute().unwrap();

        assert!(outcome.commit_id > 0);
    }

    #[test]
    fn commit_transaction_publishes_dependency_delta() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();

        let catalog_name = session.current_database.catalog().name().to_string();
        session
            .transaction
            .ddl_changes()
            .lock()
            .unwrap()
            .record(staged_dependency_change(&catalog_name));

        assert!(!session
            .current_database
            .catalog()
            .dependency_graph()
            .contains_object(CatalogObjectId::from_raw(90_002)));

        session.commit_transaction().unwrap();

        let graph = session.current_database.catalog().dependency_graph();
        assert!(graph.contains_object(CatalogObjectId::from_raw(90_002)));
        let error = graph
            .plan_drop(CatalogObjectId::from_raw(90_001), false)
            .unwrap_err();
        assert!(error.to_string().contains("dep_view"));
    }

    #[test]
    fn read_only_commit_short_circuits_without_advancing_visibility_frontier() {
        let mut session = TestSessionBuilder::minimal().build();
        let before_metrics = session.current_database.wal_lifecycle_metrics();
        session.begin_explicit_transaction().unwrap();

        assert_eq!(
            session.current_database.transaction_manager().last_commit(),
            0
        );
        session.commit_transaction().unwrap();
        assert_eq!(
            session.current_database.transaction_manager().last_commit(),
            0
        );
        assert_eq!(
            session
                .current_database
                .transaction_manager()
                .durable_commit_id(),
            0
        );
        let after_metrics = session.current_database.wal_lifecycle_metrics();
        assert_eq!(
            before_metrics.journal_group_count,
            after_metrics.journal_group_count
        );
        assert_eq!(
            before_metrics.journal_commit_bytes_total,
            after_metrics.journal_commit_bytes_total
        );
    }

    #[test]
    fn commit_pipeline_returns_after_publish_frontier_advances() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();
        let catalog_name = session.current_database.catalog().name().to_string();
        session
            .transaction
            .ddl_changes()
            .lock()
            .unwrap()
            .record(staged_dependency_change(&catalog_name));

        let frozen = session.transaction.freeze().unwrap();
        let outcome = CommitPipeline::new(&session, frozen).execute().unwrap();

        assert_eq!(
            session
                .current_database
                .transaction_manager()
                .published_commit_id(),
            outcome.commit_id
        );
    }

    #[test]
    fn sql_commit_waits_for_published_frontier_when_earlier_durable_record_is_still_applying() {
        let mut session = TestSessionBuilder::minimal().build();
        let coordinator = session
            .current_database
            .journal_coordinator()
            .expect("journal coordinator");
        let apply_runtime = session
            .current_database
            .journal_apply_runtime()
            .expect("apply runtime");

        coordinator.sync_commit_id_with(
            session
                .current_database
                .transaction_manager()
                .durable_commit_id(),
        );

        let slow_part_started = Arc::new(AtomicBool::new(false));
        let release_slow_part = Arc::new((StdMutex::new(false), Condvar::new()));
        let release_slow_part_worker = Arc::clone(&release_slow_part);
        let slow_part_started_worker = Arc::clone(&slow_part_started);
        let apply_runtime_worker = Arc::clone(&apply_runtime);
        let coordinator_worker = Arc::clone(&coordinator);

        let earlier = thread::spawn(move || {
            coordinator_worker
                .submit_commit(
                    PreparedCommitPlan {
                        txn_id: 77,
                        start_time: 77,
                        catalog_ops: Vec::new(),
                        storage_ops: Vec::new(),
                        apply_descriptors: Vec::new(),
                        deferred_tasks: Vec::new(),
                        tablets: Vec::new(),
                    },
                    |_| Ok(()),
                    move |ctx| {
                        apply_runtime_worker.submit(ApplyRequest {
                            lsn: ctx.lsn,
                            durable_batch_lsn: ctx.lsn,
                            commit_id: Some(ctx.commit_id),
                            wait_mode: WaitMode::Published,
                            catalog_serial: false,
                            catalog_pre: Box::new(|| Ok(())),
                            tablet_parts: vec![TabletApplyPart {
                                tablet_id: 9_001,
                                apply: Box::new(move || {
                                    slow_part_started_worker.store(true, Ordering::Release);
                                    let (lock, wake) = &*release_slow_part_worker;
                                    let mut released = lock.lock().unwrap();
                                    while !*released {
                                        released = wake.wait(released).unwrap();
                                    }
                                    Ok(())
                                }),
                            }],
                            descriptor_phase: Box::new(|| Ok(())),
                            catalog_post: Box::new(|| Ok(())),
                            on_published: Box::new(|| Ok(())),
                        })?;
                        Ok(())
                    },
                )
                .unwrap();
        });

        for _ in 0..20 {
            if slow_part_started.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(slow_part_started.load(Ordering::Acquire));

        let catalog_name = session.current_database.catalog().name().to_string();
        session.begin_explicit_transaction().unwrap();
        session
            .transaction
            .ddl_changes()
            .lock()
            .unwrap()
            .record(staged_dependency_change(&catalog_name));

        let release_after_delay = Arc::clone(&release_slow_part);
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(120));
            let (lock, wake) = &*release_after_delay;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        });

        let started_at = Instant::now();
        session
            .commit_transaction()
            .expect("commit should wait for published frontier");
        let elapsed = started_at.elapsed();

        releaser.join().unwrap();
        earlier.join().unwrap();

        assert!(
            elapsed >= Duration::from_millis(80),
            "commit returned before published frontier cleared the earlier durable record: {elapsed:?}"
        );
        assert_eq!(
            session
                .current_database
                .transaction_manager()
                .published_commit_id(),
            session
                .current_database
                .transaction_manager()
                .durable_commit_id()
        );
    }
}
