// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::super::ddl_changes::{PreparedCatalogOp, TransientCatalogRuntime};
use super::super::post_commit::PostCommitActions;
use super::super::session_transaction::FrozenTransaction;
use super::ddl_publish::{
    build_apply_descriptor_phase, IndexBackfillPublishTask, SearchGenerationRetirementTask,
    StagedGenerationPublishTask,
};
use super::errors::{
    commit_runtime_error_is_definitely_nondurable, commit_runtime_error_to_paro, CommitFailure,
};
use super::job_builder;
use crate::session::Session;
use paro_catalog::transaction::{
    CatalogCommitParticipant, CatalogCommittedRecordApplier, CatalogPreparedChange,
};
use paro_common::ddl::DdlChange;
use paro_common::durability::PreparedCommitPlan;
use paro_common::effect::{
    ApplyDescriptor, DeferredTask, PostCommitHookDescriptor, RuntimeTransitionDescriptor,
    StorageCommitOp,
};
use paro_common::error::Result;
use paro_common::logging::targets;
use paro_instance::commit::live_publish::{build_required_publish_plan, LivePublishPlanInput};
use paro_journal::{encoded_size_upper_bound_for_plan, TabletApplyPart};
use paro_storage::search::StagedSearchGeneration;
use paro_storage::transaction::lifecycle_action;
use paro_storage::transaction::participant::{
    StorageCommitParticipant, StorageCommittedRecordApplier,
};
use paro_storage::transaction::txn::{PreparedStorageCommit, Transaction};
use paro_transaction::{
    AbortReason, AppendFailureRollbackPlan, ApplyTargetDescriptor, CommitFinalizeReservationInput,
    CommitParticipant, CommitRequest, CommitRuntimeAck, CommitSequencingPlan,
    CommittedRecordApplier, DatabaseId, ParticipantDescriptor, ParticipantId, ParticipantKind,
    PreparedCommitJob, PreparedCommitPart, TransactionView, TxnResourceKey,
    WriteConflictPlacementInput,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct CommitOutcome {
    pub commit_id: u64,
    pub active_txn: Arc<Transaction>,
    pub catalog_ops: Vec<PreparedCatalogOp>,
    pub deferred_tasks: Vec<DeferredTask>,
    pub published_at: Instant,
    pub post_commit_hooks: Vec<PostCommitHookDescriptor>,
}

struct CatalogPublishPrepare {
    descriptor: ParticipantDescriptor,
    catalog_ops: Vec<paro_common::effect::CatalogTxnOp>,
    applier: Arc<CatalogCommittedRecordApplier>,
    prepared_bytes: u64,
}

pub struct CommitPipeline<'a> {
    session: &'a Session,
    frozen: FrozenTransaction,
}

impl<'a> CommitPipeline<'a> {
    pub fn new(session: &'a Session, frozen: FrozenTransaction) -> Self {
        Self { session, frozen }
    }

    pub fn execute(self) -> std::result::Result<CommitOutcome, CommitFailure> {
        let session = self.session;
        let FrozenTransaction {
            active,
            ddl_changes,
            transaction_view,
        } = self.frozen;

        let request = Self::build_commit_request(session, &active, &ddl_changes, transaction_view);
        Self::execute_prepared(session, active, ddl_changes, request)
    }

    pub(crate) fn build_commit_request(
        session: &Session,
        active: &Arc<Transaction>,
        ddl_changes: &[PreparedCatalogOp],
        transaction_view: TransactionView,
    ) -> CommitRequest {
        job_builder::build_commit_request(session, active, ddl_changes, transaction_view)
    }

    fn execute_prepared(
        session: &Session,
        active: Arc<Transaction>,
        mut ddl_changes: Vec<PreparedCatalogOp>,
        mut request: CommitRequest,
    ) -> std::result::Result<CommitOutcome, CommitFailure> {
        debug_assert_eq!(request.txn_id, active.txn_id());

        let database_id = request.database_id;
        let manager = Arc::clone(session.current_database.transaction_manager());
        let total_started_at = Instant::now();
        let mut prepare_latency = std::time::Duration::ZERO;
        let mut validate_latency = std::time::Duration::ZERO;
        manager.record_commit_ack_mode(request.ack_policy);
        let plan = request.commit_plan();
        let ctx = request.validation_context();
        let storage_participant = StorageCommitParticipant::new(database_id, Arc::clone(&active));

        // Storage prepare snapshots pending writes before validation; catalog validation runs
        // before consuming staged catalog handles because its cheap plan checks are non-destructive.
        let stage_started_at = Instant::now();
        let prepared_storage_part = match storage_participant.prepare(&request.transaction_view) {
            Ok(prepared) => prepared,
            Err(error) => {
                let rollback_succeeded = Self::rollback_catalog_changes(&mut ddl_changes).is_ok()
                    && manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error,
                    rollback_succeeded,
                });
            }
        };
        prepare_latency += stage_started_at.elapsed();
        let stage_started_at = Instant::now();
        if let Err(error) = storage_participant.validate(&plan, &ctx) {
            let _ = storage_participant.abort(AbortReason::ValidationFailed);
            let rollback_succeeded = Self::rollback_catalog_changes(&mut ddl_changes).is_ok()
                && manager.rollback_transaction(Arc::clone(&active)).is_ok();
            return Err(CommitFailure {
                error,
                rollback_succeeded,
            });
        }
        validate_latency += stage_started_at.elapsed();

        let stage_started_at = Instant::now();
        let prepared_storage = prepared_storage_part.commit().clone();
        let storage_descriptor =
            match CommitParticipant::descriptor(&storage_participant, &prepared_storage_part) {
                Ok(descriptor) => descriptor,
                Err(error) => {
                    let _ = storage_participant.abort(AbortReason::ValidationFailed);
                    let rollback_succeeded = Self::rollback_catalog_changes(&mut ddl_changes)
                        .is_ok()
                        && manager.rollback_transaction(Arc::clone(&active)).is_ok();
                    return Err(CommitFailure {
                        error,
                        rollback_succeeded,
                    });
                }
            };
        prepare_latency += stage_started_at.elapsed();

        let catalog_prepare = if ddl_changes.is_empty() {
            None
        } else {
            let catalog_changes = Self::take_catalog_participant_changes(&mut ddl_changes);
            let catalog_participant = CatalogCommitParticipant::new(database_id, catalog_changes);
            let stage_started_at = Instant::now();
            if let Err(error) = catalog_participant.validate(&plan, &ctx) {
                let _ = catalog_participant.abort(AbortReason::ValidationFailed);
                let _ = storage_participant.abort(AbortReason::ValidationFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error,
                    rollback_succeeded,
                });
            }
            validate_latency += stage_started_at.elapsed();

            let stage_started_at = Instant::now();
            let prepared = match catalog_participant.prepare(&request.transaction_view) {
                Ok(prepared) => prepared,
                Err(error) => {
                    let _ = catalog_participant.abort(AbortReason::ValidationFailed);
                    let _ = storage_participant.abort(AbortReason::ValidationFailed);
                    let rollback_succeeded =
                        manager.rollback_transaction(Arc::clone(&active)).is_ok();
                    return Err(CommitFailure {
                        error,
                        rollback_succeeded,
                    });
                }
            };
            let descriptor = match CommitParticipant::descriptor(&catalog_participant, &prepared) {
                Ok(descriptor) => descriptor,
                Err(error) => {
                    let _ = catalog_participant.abort(AbortReason::ValidationFailed);
                    let _ = storage_participant.abort(AbortReason::ValidationFailed);
                    let rollback_succeeded =
                        manager.rollback_transaction(Arc::clone(&active)).is_ok();
                    return Err(CommitFailure {
                        error,
                        rollback_succeeded,
                    });
                }
            };
            prepare_latency += stage_started_at.elapsed();
            let prepared_bytes = prepared.prepared_bytes() as u64;
            let catalog_ops = prepared.catalog_ops().to_vec();
            let applier = Arc::new(CatalogCommittedRecordApplier::new(
                database_id,
                Arc::clone(session.current_database.catalog()),
                prepared,
            ));
            Some(CatalogPublishPrepare {
                descriptor,
                catalog_ops,
                applier,
                prepared_bytes,
            })
        };

        let catalog_ops = catalog_prepare
            .as_ref()
            .map(|prepare| prepare.catalog_ops.as_slice())
            .unwrap_or(&[]);

        let apply_descriptors = Self::collect_apply_descriptors(&ddl_changes);
        let publish_apply_descriptors = apply_descriptors.clone();
        let ddl_storage_ops = Self::collect_ddl_storage_ops(&ddl_changes);
        let durable_deferred_tasks = Self::collect_deferred_tasks(&prepared_storage, &ddl_changes);
        let post_commit_deferred_tasks = Self::post_commit_deferred_tasks(&durable_deferred_tasks);
        let index_publish_tasks = Self::collect_index_backfill_publish_tasks(&ddl_changes);
        let staged_generation_owners = index_publish_tasks
            .iter()
            .filter_map(IndexBackfillPublishTask::staged_search_generation)
            .collect::<Vec<_>>();
        let staged_generation_publish_tasks =
            match Self::collect_staged_generation_publish_tasks(&index_publish_tasks) {
                Ok(tasks) => tasks,
                Err(error) => {
                    if let Some(prepare) = &catalog_prepare {
                        let _ = prepare.applier.abort_prepared();
                    }
                    let _ = storage_participant.abort(AbortReason::ValidationFailed);
                    let rollback_succeeded =
                        manager.rollback_transaction(Arc::clone(&active)).is_ok();
                    return Err(CommitFailure {
                        error,
                        rollback_succeeded,
                    });
                }
            };
        let search_generation_retirement_tasks = ddl_changes
            .iter()
            .filter_map(|change| match change.transient_runtime.as_ref() {
                Some(TransientCatalogRuntime::RetireSearchGeneration(action)) => {
                    Some(SearchGenerationRetirementTask::from_action(action))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if !ddl_storage_ops.is_empty() {
            request.add_participants(vec![storage_descriptor.clone()]);
        }
        request.add_participants(Self::participants_for_durable_tasks(
            database_id,
            &durable_deferred_tasks,
        ));
        let mut durable_storage_ops = prepared_storage.storage_ops.clone();
        durable_storage_ops.extend(ddl_storage_ops);
        let durable_plan = PreparedCommitPlan {
            txn_id: active.id,
            start_time: active.start_time,
            catalog_ops: catalog_ops.to_vec(),
            storage_ops: durable_storage_ops,
            apply_descriptors,
            deferred_tasks: durable_deferred_tasks.clone(),
            tablets: Vec::new(),
        };
        let estimated_record_bytes = match encoded_size_upper_bound_for_plan(&durable_plan) {
            Ok(bytes) => bytes,
            Err(error) => {
                if let Some(prepare) = &catalog_prepare {
                    let _ = prepare.applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::DurableAppendFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error: paro_common::error::internal(format!(
                        "commit record size estimate overflow: {error}"
                    )),
                    rollback_succeeded,
                });
            }
        };
        let mut sequencing_plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());
        if let Err(error) =
            request.validate_single_database(database_id, Some(&sequencing_plan.plan))
        {
            if let Some(prepare) = &catalog_prepare {
                let _ = prepare.applier.abort_prepared();
            }
            let _ = storage_participant.abort(AbortReason::ValidationFailed);
            let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
            return Err(CommitFailure {
                error: paro_common::error::invalid_transaction_state(error.to_string()),
                rollback_succeeded,
            });
        }
        let stage_started_at = Instant::now();
        let ssi_outcome = match manager
            .validate_serializable_commit(&sequencing_plan.plan, &sequencing_plan.write_set)
        {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some(prepare) = &catalog_prepare {
                    let _ = prepare.applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::ValidationFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error,
                    rollback_succeeded,
                });
            }
        };
        validate_latency += stage_started_at.elapsed();
        sequencing_plan = sequencing_plan
            .with_validation_epoch(ssi_outcome.validation_epoch)
            .with_ssi_effect_epoch(ssi_outcome.ssi_effect_epoch)
            .with_estimated_bytes(estimated_record_bytes as usize);
        manager.record_commit_latency(
            paro_storage::transaction::manager::TransactionLatencyStage::CommitPrepare,
            prepare_latency,
        );
        manager.record_commit_latency(
            paro_storage::transaction::manager::TransactionLatencyStage::CommitValidate,
            validate_latency,
        );
        let write_set = sequencing_plan.write_set.clone();
        manager.sync_commit_backpressure_frontiers(
            paro_transaction::CommitTs::new(manager.durable_commit_id()),
            paro_transaction::CommitTs::new(manager.published_commit_id()),
        );
        let backpressure = manager.commit_backpressure_controller();
        if let Err(error) = backpressure.admit(&sequencing_plan.plan) {
            if let Some(prepare) = &catalog_prepare {
                let _ = prepare.applier.abort_prepared();
            }
            let _ = storage_participant.abort(AbortReason::Backpressure);
            let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
            return Err(CommitFailure {
                error: paro_common::error::invalid_transaction_state(error.to_string()),
                rollback_succeeded,
            });
        }
        for owner in &staged_generation_owners {
            if let Err(error) = owner.prepare_durable_handoff() {
                for staged in &staged_generation_owners {
                    let _ = staged.discard_before_durable_append();
                }
                if let Some(prepare) = &catalog_prepare {
                    let _ = prepare.applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::ValidationFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error,
                    rollback_succeeded,
                });
            }
        }
        let storage_applier =
            StorageCommittedRecordApplier::new(Arc::clone(&manager), Arc::clone(&active));
        let database = Arc::clone(&session.current_database);
        let max_seen_catalog_object_id = Self::max_seen_catalog_object_id(&ddl_changes);
        let ack_policy = request.ack_policy;
        let has_storage_apply = !prepared_storage.storage_ops.is_empty();
        let storage_apply_tablet_id = Self::storage_apply_tablet_id(&prepared_storage);
        let publish_manager = Arc::clone(&manager);
        let publish_transaction = Arc::clone(&active);
        let publish_participants: Arc<[ParticipantDescriptor]> =
            Arc::from(request.participants.clone().into_boxed_slice());
        let catalog_publish = catalog_prepare
            .as_ref()
            .map(|prepare| (prepare.descriptor.clone(), Arc::clone(&prepare.applier)));
        let has_catalog_publish = catalog_publish.is_some();
        let catalog_serial = has_catalog_publish;
        let storage_apply_descriptor = storage_descriptor.clone();
        let publish_commit_id = Arc::new(AtomicU64::new(0));
        let request_for_tablets = request.clone();
        let mut tablet_parts = if has_storage_apply {
            let publish_commit_id = Arc::clone(&publish_commit_id);
            vec![TabletApplyPart {
                tablet_id: storage_apply_tablet_id,
                apply: Box::new(move || {
                    let commit_id = publish_commit_id.load(Ordering::Acquire);
                    if commit_id == 0 {
                        return Err(paro_common::error::internal(
                            "storage apply started before commit id assignment",
                        ));
                    }
                    let committed_record = request_for_tablets
                        .committed_record(paro_transaction::CommitTs::new(commit_id));
                    storage_applier.apply_required(&committed_record, &storage_apply_descriptor)?;
                    Ok(())
                }),
            }]
        } else {
            Vec::new()
        };
        for task in staged_generation_publish_tasks {
            let tablet_id = task.tablet_id();
            tablet_parts.push(TabletApplyPart {
                tablet_id,
                apply: Box::new(move || task.apply()),
            });
        }
        for task in search_generation_retirement_tasks {
            let tablet_id = task.tablet_id();
            tablet_parts.push(TabletApplyPart {
                tablet_id,
                apply: Box::new(move || task.apply()),
            });
        }
        let catalog_post_request = request.clone();
        let publish_database = Arc::clone(&database);
        let catalog_post = Box::new(move |commit_id, journal_lsn| {
            let committed_record =
                catalog_post_request.committed_record(paro_transaction::CommitTs::new(commit_id));
            if let Some((catalog_descriptor, catalog_applier)) = catalog_publish {
                catalog_applier.apply_required(&committed_record, &catalog_descriptor)?;
            }
            for task in index_publish_tasks {
                task.execute(commit_id)?;
            }
            if let Some(wal) = publish_database.wal() {
                if let Err(error) = wal.note_flushed_lsn(journal_lsn) {
                    tracing::warn!(
                        target: targets::WAL,
                        db = %publish_database.name(),
                        lsn = journal_lsn,
                        error = %error,
                        "failed to advance journal segment catalog after transaction flush"
                    );
                }
            }
            Ok(())
        });
        let descriptor_phase = build_apply_descriptor_phase(
            Arc::clone(&session.instance),
            Arc::clone(&database),
            publish_apply_descriptors,
        );
        let publish_plan = build_required_publish_plan(LivePublishPlanInput {
            post_apply_finalize: lifecycle_action::post_apply_finalize_plan(
                Arc::clone(&publish_manager),
                Arc::clone(&publish_transaction),
            ),
            frontier: publish_manager.commit_frontier(),
            backpressure: Some(backpressure),
            participants: publish_participants,
            apply_targets: Arc::<[ApplyTargetDescriptor]>::from([]),
            catalog_serial,
            has_catalog_publish,
            max_seen_object_id: max_seen_catalog_object_id,
            catalog_pre: Box::new(|| Ok(())),
            on_commit_id_assigned: Box::new(move |commit_id| {
                publish_commit_id.store(commit_id, Ordering::Release);
            }),
            tablet_parts,
            descriptor_phase,
            catalog_post,
        });
        let retained_bytes = prepared_storage_part.prepared_bytes() as u64
            + catalog_prepare
                .as_ref()
                .map(|prepare| prepare.prepared_bytes)
                .unwrap_or(0);
        let job = PreparedCommitJob {
            sequencing_plan,
            durable_plan,
            reservation_input: CommitFinalizeReservationInput {
                txn_id: request.txn_id,
                read_ts: request.read_ts,
                write_set,
                wci_placement_input: WriteConflictPlacementInput::default(),
                frozen_read_set: request.frozen_read_set.clone(),
            },
            lock_release_plan: lifecycle_action::lock_release_plan(Arc::clone(&active)),
            pre_publish_release_plan: lifecycle_action::pre_publish_release_plan(
                Arc::clone(&manager),
                Arc::clone(&active),
            ),
            append_failure_rollback_plan: Self::append_failure_rollback_plan(
                Arc::clone(&active),
                staged_generation_owners.clone(),
            ),
            required_publish: publish_plan,
            deferred_publish: Vec::new(),
            ack_policy,
            estimated_record_bytes,
            retained_bytes,
            created_at: Instant::now(),
        };
        let publish_started_at = Instant::now();
        let runtime_outcome = match database.commit_runtime().commit_blocking(job) {
            Ok(outcome) => outcome,
            Err(error) => {
                if commit_runtime_error_is_definitely_nondurable(&error) {
                    for owner in &staged_generation_owners {
                        if let Err(cleanup_error) = owner.discard_before_durable_append() {
                            tracing::warn!(
                                target: targets::TRANSACTION,
                                error = %cleanup_error,
                                "failed to discard non-durable staged search generation"
                            );
                        }
                    }
                }
                return Err(CommitFailure {
                    error: commit_runtime_error_to_paro(error),
                    rollback_succeeded: false,
                });
            }
        };
        manager.record_commit_latency(
            paro_storage::transaction::manager::TransactionLatencyStage::CommitPublish,
            publish_started_at.elapsed(),
        );
        let commit_id = runtime_outcome.commit_ts.into_raw();
        if matches!(runtime_outcome.ack, CommitRuntimeAck::DurableOnly) {
            session.record_async_commit_floor(commit_id);
        }
        let mut post_commit_hooks = prepared_storage.post_commit_hooks;
        for op in &ddl_changes {
            post_commit_hooks.extend(op.post_commit_hooks.clone());
        }
        manager.record_commit_latency(
            paro_storage::transaction::manager::TransactionLatencyStage::CommitTotal,
            total_started_at.elapsed(),
        );

        Ok(CommitOutcome {
            commit_id,
            active_txn: active,
            catalog_ops: ddl_changes,
            deferred_tasks: post_commit_deferred_tasks,
            published_at: Instant::now(),
            post_commit_hooks,
        })
    }

    fn take_catalog_participant_changes(
        ddl_changes: &mut [PreparedCatalogOp],
    ) -> Vec<CatalogPreparedChange> {
        ddl_changes
            .iter_mut()
            .map(|op| {
                CatalogPreparedChange::new(
                    op.record.clone(),
                    op.catalog.take(),
                    op.dependencies.take(),
                )
            })
            .collect()
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

    fn max_seen_catalog_object_id(ddl_changes: &[PreparedCatalogOp]) -> u64 {
        if ddl_changes.is_empty() {
            return 0;
        }

        ddl_changes
            .iter()
            .flat_map(|op| match &op.record.change {
                DdlChange::CreateSchema(payload) => vec![payload.object_id],
                DdlChange::CreateTable(payload) => vec![payload.object_id],
                DdlChange::CreateView(payload) => vec![payload.object_id],
                DdlChange::CreateIndex(payload) => vec![payload.object_id],
                DdlChange::CreatePropertyGraph(payload) => vec![payload.object_id],
                DdlChange::CreateSequence(payload) => vec![payload.object_id],
                DdlChange::CreateRoutine(payload) => vec![payload.object_id, payload.routine_id],
                DdlChange::DropSchema(_)
                | DdlChange::DropTable(_)
                | DdlChange::DropView(_)
                | DdlChange::DropIndex(_)
                | DdlChange::DropPropertyGraph(_)
                | DdlChange::DropSequence(_)
                | DdlChange::DropRoutine(_)
                | DdlChange::AlterEntry(_) => Vec::new(),
            })
            .max()
            .unwrap_or(0)
    }

    fn collect_deferred_tasks(
        prepared_storage: &PreparedStorageCommit,
        ddl_changes: &[PreparedCatalogOp],
    ) -> Vec<DeferredTask> {
        let mut tasks = Vec::new();

        for hook in &prepared_storage.post_commit_hooks {
            let PostCommitHookDescriptor::GraphDmlMaintenance { deltas } = hook;
            tasks.push(DeferredTask::GraphDmlMaintenance {
                deltas: deltas.clone(),
            });
        }

        for op in ddl_changes {
            for transition in &op.runtime_transitions {
                if let RuntimeTransitionDescriptor::AttachIndexState {
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config,
                } = transition
                {
                    tasks.push(DeferredTask::FinalizeIndexState {
                        index: index.clone(),
                        table_name: table_name.clone(),
                        index_type: index_type.clone(),
                        column_ids: column_ids.clone(),
                        fulltext_config: fulltext_config.clone(),
                    });
                }
            }
        }

        tasks
    }

    fn post_commit_deferred_tasks(tasks: &[DeferredTask]) -> Vec<DeferredTask> {
        tasks
            .iter()
            .filter(|task| matches!(task, DeferredTask::GraphDmlMaintenance { .. }))
            .cloned()
            .collect()
    }

    fn collect_index_backfill_publish_tasks(
        ddl_changes: &[PreparedCatalogOp],
    ) -> Vec<IndexBackfillPublishTask> {
        ddl_changes
            .iter()
            .filter_map(|op| match op.transient_runtime.as_ref() {
                Some(TransientCatalogRuntime::CreateIndex(action)) => {
                    Some(IndexBackfillPublishTask::from_action(action))
                }
                _ => None,
            })
            .collect()
    }

    fn collect_staged_generation_publish_tasks(
        index_tasks: &[IndexBackfillPublishTask],
    ) -> Result<Vec<StagedGenerationPublishTask>> {
        index_tasks
            .iter()
            .filter_map(|task| task.staged_generation_publish_task().transpose())
            .collect()
    }

    fn append_failure_rollback_plan(
        transaction: Arc<Transaction>,
        staged_generations: Vec<Arc<StagedSearchGeneration>>,
    ) -> AppendFailureRollbackPlan {
        AppendFailureRollbackPlan::new(move || {
            transaction.rollback_prepared_storage_only();
            let mut first_error = None;
            for staged in staged_generations {
                if let Err(error) = staged.discard_before_durable_append() {
                    first_error.get_or_insert(error);
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        })
    }

    fn storage_apply_tablet_id(prepared_storage: &PreparedStorageCommit) -> u64 {
        prepared_storage
            .storage_ops
            .iter()
            .map(|op| match op {
                StorageCommitOp::Tablet(tablet) => tablet.tablet_id,
            })
            .next()
            .unwrap_or(0)
    }

    fn participants_for_durable_tasks(
        database_id: DatabaseId,
        tasks: &[DeferredTask],
    ) -> Vec<ParticipantDescriptor> {
        const SEARCH_DERIVED_PARTICIPANT_ID: ParticipantId = ParticipantId::new(3);
        const GRAPH_DERIVED_PARTICIPANT_ID: ParticipantId = ParticipantId::new(4);

        let mut participants = Vec::new();
        let mut push_unique = |participant: ParticipantDescriptor| {
            if !participants.contains(&participant) {
                participants.push(participant);
            }
        };

        for task in tasks {
            match task {
                DeferredTask::FinalizeIndexState { index_type, .. } => {
                    let index_type = paro_catalog::entry::IndexType::from_str(index_type);
                    if matches!(
                        index_type,
                        paro_catalog::entry::IndexType::HNSW
                            | paro_catalog::entry::IndexType::Sparse
                            | paro_catalog::entry::IndexType::FullText
                    ) {
                        push_unique(
                            ParticipantDescriptor::new(
                                SEARCH_DERIVED_PARTICIPANT_ID,
                                ParticipantKind::Search,
                                TxnResourceKey::database(ParticipantKind::Search, database_id),
                            )
                            .required(),
                        );
                    }
                }
                DeferredTask::GraphDmlMaintenance { .. } => {
                    push_unique(
                        ParticipantDescriptor::new(
                            GRAPH_DERIVED_PARTICIPANT_ID,
                            ParticipantKind::Graph,
                            TxnResourceKey::database(ParticipantKind::Graph, database_id),
                        )
                        .deferred(),
                    );
                }
            }
        }

        participants
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

    fn collect_ddl_storage_ops(ddl_changes: &[PreparedCatalogOp]) -> Vec<StorageCommitOp> {
        ddl_changes
            .iter()
            .flat_map(|change| change.storage_ops.iter().cloned())
            .collect()
    }
}

impl Session {
    pub(crate) fn commit_via_pipeline(&mut self) -> Result<()> {
        if let Some(read_only) = self.transaction.take_read_only_noop_for_commit() {
            self.current_database
                .transaction_manager()
                .rollback_transaction(read_only)?;
            self.on_transaction_commit_prepared();
            self.notify_transaction_commit();
            self.current_database.maybe_gc_catalog();
            crate::utility::settings::reconcile_effective_settings(self)?;
            self.refresh_session_metadata();
            return Ok(());
        }

        let frozen = self.transaction.freeze()?;
        let pipeline = CommitPipeline::new(self, frozen);
        match pipeline.execute() {
            Ok(outcome) => {
                self.current_database.schedule_auto_checkpoint_if_needed();
                PostCommitActions::execute(self, outcome)?;
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
    use paro_transaction::{
        CommitAckPolicy, LockMode, LockRequest, LockResource, ParticipantKind, TableId,
    };

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
            storage_ops: Vec::new(),
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

        let frozen = session.transaction.freeze().unwrap();
        let outcome = CommitPipeline::new(&session, frozen).execute().unwrap();

        assert!(outcome.commit_id > 0);
    }

    #[test]
    fn commit_request_freezes_view_locks_and_participants() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();
        session
            .transaction
            .prepare_statement_read_tracking(
                session.current_database.transaction_manager(),
                paro_transaction::ReadTrackingPolicy::RangeCritical,
            )
            .unwrap();
        session.transaction.command_counter_increment();

        let active = session.transaction.active_transaction().unwrap();
        active
            .acquire_lock_requests([LockRequest::new(
                LockResource::Table {
                    namespace: active.lock_namespace(),
                    table_id: TableId::new(99),
                },
                LockMode::IX,
            )])
            .unwrap();

        let catalog_name = session.current_database.catalog().name().to_string();
        session
            .transaction
            .ddl_changes()
            .lock()
            .unwrap()
            .record(staged_dependency_change(&catalog_name));

        let frozen = session.transaction.freeze().unwrap();
        let request = CommitPipeline::build_commit_request(
            &session,
            &frozen.active,
            &frozen.ddl_changes,
            frozen.transaction_view,
        );

        assert_eq!(request.command_id.into_raw(), 1);
        assert_eq!(request.read_ts, request.transaction_view.read_ts());
        assert_eq!(request.lock_set.held_lock_count(), 1);
        assert!(request
            .participants
            .iter()
            .any(|participant| participant.kind == ParticipantKind::Storage));
        assert!(request
            .participants
            .iter()
            .any(|participant| participant.kind == ParticipantKind::Catalog));
    }

    #[test]
    fn commit_request_can_use_durable_only_async_policy() {
        let mut session = TestSessionBuilder::minimal().build();
        session.set_commit_ack_policy_for_tests(CommitAckPolicy::DurableOnlyAsync);
        session.begin_explicit_transaction().unwrap();

        let frozen = session.transaction.freeze().unwrap();
        let request = CommitPipeline::build_commit_request(
            &session,
            &frozen.active,
            &frozen.ddl_changes,
            frozen.transaction_view,
        );

        assert_eq!(request.ack_policy, CommitAckPolicy::DurableOnlyAsync);
    }

    #[test]
    fn read_only_commit_releases_without_durable_timestamp() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();
        let manager = Arc::clone(session.current_database.transaction_manager());
        let published_before = manager.published_commit_id();

        session.commit_transaction().unwrap();

        assert!(!session.has_active_transaction());
        assert_eq!(manager.published_commit_id(), published_before);
    }

    #[test]
    fn durable_tasks_add_required_search_and_deferred_graph_participants() {
        let database_id = DatabaseId::new(7);
        let tasks = vec![
            DeferredTask::FinalizeIndexState {
                index: DdlObjectKey::new(
                    "memory",
                    Some("main"),
                    "idx_search",
                    DdlObjectKind::Index,
                ),
                table_name: "t".to_string(),
                index_type: "fulltext".to_string(),
                column_ids: vec![1],
                fulltext_config: Some("simple".to_string()),
            },
            DeferredTask::GraphDmlMaintenance { deltas: Vec::new() },
        ];

        let participants = CommitPipeline::participants_for_durable_tasks(database_id, &tasks);

        assert!(participants.iter().any(|participant| {
            participant.kind == ParticipantKind::Search && participant.is_required()
        }));
        assert!(participants.iter().any(|participant| {
            participant.kind == ParticipantKind::Graph && participant.is_deferred()
        }));
    }

    #[test]
    fn commit_transaction_publishes_dependency_delta() {
        let mut session = TestSessionBuilder::minimal().build();
        session.begin_explicit_transaction().unwrap();
        session
            .transaction
            .prepare_statement_read_tracking(
                session.current_database.transaction_manager(),
                paro_transaction::ReadTrackingPolicy::RangeCritical,
            )
            .unwrap();

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
}
