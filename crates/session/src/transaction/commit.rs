// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::ddl_changes::{IndexPostCommitAction, PreparedCatalogOp, TransientCatalogRuntime};
use super::index_backfill::IndexBackfillPlan;
use super::session_transaction::FrozenTransaction;
use crate::session::Session;
use paro_catalog::entry::{
    IndexCatalogEntry, IndexCoverage, IndexType as CatalogIndexType, TableCatalogEntry,
};
use paro_catalog::transaction::{
    CatalogCommitParticipant, CatalogCommittedRecordApplier, CatalogPreparedChange,
};
use paro_common::ddl::DdlChange;
use paro_common::durability::PreparedCommitPlan;
use paro_common::effect::{
    ApplyDescriptor, DeferredTask, PostCommitHookDescriptor, RuntimeTransitionDescriptor,
};
use paro_common::error::{ParoError, Result};
use paro_common::journal::JournalRecord;
use paro_common::logging::targets;
use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
use paro_storage::transaction::participant::{
    StorageCommitParticipant, StorageCommittedRecordApplier,
};
use paro_storage::transaction::txn::{PreparedStorageCommit, Transaction};
use paro_storage::{
    index::BoundIndex,
    search::{SearchIndexDefinition, SearchIndexKind},
    table::table_handle::TableHandle,
};
use paro_transaction::{
    AbortReason, CommitAckPolicy, CommitCoordinatorError, CommitParticipant, CommitRequest,
    CommitSequencingPlan, CommitTicket, CommittedRecordApplier, DatabaseId, ParticipantDescriptor,
    ParticipantId, ParticipantKind, RequiredPublishOutcome, TransactionView, TxnResourceKey,
};
use serde_json::{json, Value};
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

#[derive(Debug)]
pub struct CommitFailure {
    pub error: ParoError,
    pub rollback_succeeded: bool,
}

#[derive(Clone)]
struct IndexBackfillPublishTask {
    entry: Arc<IndexCatalogEntry>,
    table: Arc<TableCatalogEntry>,
    info: paro_catalog::entry::CreateIndexInfo,
    built_index: Option<Arc<dyn BoundIndex>>,
    coverage: Option<IndexCoverage>,
    backfill: Option<IndexBackfillPlan>,
}

impl IndexBackfillPublishTask {
    fn from_action(action: &IndexPostCommitAction) -> Self {
        Self {
            entry: Arc::clone(&action.entry),
            table: Arc::clone(&action.table),
            info: action.info.clone(),
            built_index: action.built_index.clone(),
            coverage: action.coverage.clone(),
            backfill: action.backfill.clone(),
        }
    }

    fn execute(&self, publish_ts: u64) -> Result<()> {
        if let Some(backfill) = &self.backfill {
            let report = backfill.bounded_final_catch_up(
                publish_ts,
                self.table.as_ref(),
                self.entry.as_ref(),
            )?;
            tracing::debug!(
                target: targets::TRANSACTION,
                index = %self.info.name,
                table = %self.info.table_name,
                from_ts = report.from_ts,
                to_ts = report.to_ts,
                consumed_commits = report.consumed_commits,
                "CREATE INDEX bounded final catch-up completed"
            );
        }

        let Some(storage) = self.table.get_storage() else {
            if matches!(
                self.info.index_type,
                CatalogIndexType::ART
                    | CatalogIndexType::HNSW
                    | CatalogIndexType::Sparse
                    | CatalogIndexType::FullText
            ) {
                return Err(paro_common::error::internal(format!(
                    "table '{}' has no storage for CREATE INDEX publish",
                    self.table.base.base.name
                )));
            }
            self.entry.mark_ready_with_coverage(self.coverage.clone());
            return Ok(());
        };

        if let Some(built_index) = self.built_index.as_ref() {
            if storage.has_index(&self.info.name) {
                let _ = storage.remove_index(&self.info.name);
            }
            storage.add_index(Arc::clone(built_index))?;
        } else if self.info.index_type == CatalogIndexType::ART {
            let [column_id] = self.info.column_ids.as_slice() else {
                return Err(paro_common::error::not_supported(
                    "ART indexes currently require exactly one column",
                ));
            };
            storage.rebuild_art_index(column_id.index)?;
        }

        Self::register_search_definition(storage.as_ref(), self.entry.as_ref())?;
        let coverage = self.recompute_coverage(storage.as_ref())?;
        self.entry.mark_ready_with_coverage(coverage);
        Ok(())
    }

    fn recompute_coverage(&self, storage: &TableHandle) -> Result<Option<IndexCoverage>> {
        match self.info.index_type {
            CatalogIndexType::HNSW | CatalogIndexType::Sparse | CatalogIndexType::FullText => {
                let definition_id = self.entry.base.base.object_id.raw();
                let Some(coverage) = storage.search_generation_coverage(definition_id)? else {
                    return Ok(self.coverage.clone());
                };
                Ok(Some(IndexCoverage::from_counts(
                    coverage.visible_version,
                    coverage.visible_segment_count,
                    coverage.indexed_segment_count,
                )))
            }
            _ => Ok(self.coverage.clone()),
        }
    }

    fn register_search_definition(storage: &TableHandle, entry: &IndexCatalogEntry) -> Result<()> {
        let Some(kind) = Self::search_kind(entry.index_type) else {
            return Ok(());
        };
        let definition = SearchIndexDefinition {
            definition_id: entry.base.base.object_id.raw(),
            table_id: storage.tablet().table_id(),
            name: entry.base.base.name.clone(),
            kind,
            column_ids: entry
                .get_column_ids()
                .iter()
                .map(|column| column.index)
                .collect(),
            expression: Self::search_expression(entry),
            provider_config: Self::search_provider_config(storage, entry)?,
            config_fingerprint: 0,
        };
        let expression = definition.expression.clone();
        let provider_config = definition.provider_config.clone();
        let column_ids = definition.column_ids.clone();
        let definition = SearchIndexDefinition {
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                kind,
                &column_ids,
                expression.as_deref(),
                &provider_config,
            ),
            ..definition
        };
        storage.register_search_definition(definition)
    }

    fn search_kind(index_type: CatalogIndexType) -> Option<SearchIndexKind> {
        match index_type {
            CatalogIndexType::HNSW => Some(SearchIndexKind::Hnsw),
            CatalogIndexType::Sparse => Some(SearchIndexKind::Sparse),
            CatalogIndexType::FullText => Some(SearchIndexKind::FullText),
            _ => None,
        }
    }

    fn search_expression(entry: &IndexCatalogEntry) -> Option<String> {
        if entry.index_type != CatalogIndexType::FullText {
            return None;
        }
        let binding = entry.fulltext_binding()?;
        Some(format!(
            "to_tsvector('{}', col_{})",
            binding.config, binding.column_id.index
        ))
    }

    fn search_provider_config(storage: &TableHandle, entry: &IndexCatalogEntry) -> Result<Value> {
        match entry.index_type {
            CatalogIndexType::HNSW => {
                let [column] = entry.get_column_ids() else {
                    return Err(paro_common::error::not_supported(
                        "HNSW search definition requires exactly one indexed column",
                    ));
                };
                let schema = storage.tablet().schema().ok_or_else(|| {
                    paro_common::error::internal("table schema missing for HNSW config")
                })?;
                let column = schema.column_by_id(column.index).ok_or_else(|| {
                    paro_common::error::column_not_found(format!(
                        "HNSW index column {} not found in schema",
                        column.index
                    ))
                })?;
                Ok(json!({
                    "m": column.hnsw_m,
                    "ef_construct": column.hnsw_ef_construct,
                    "distance": column.hnsw_distance,
                }))
            }
            CatalogIndexType::Sparse => Ok(json!({})),
            CatalogIndexType::FullText => {
                let config = entry
                    .fulltext_binding()
                    .map(|binding| binding.config.clone())
                    .unwrap_or_else(|| "simple".to_string());
                Ok(json!({ "config": config }))
            }
            _ => Ok(json!({})),
        }
    }
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

    fn build_commit_request(
        session: &Session,
        active: &Arc<Transaction>,
        ddl_changes: &[PreparedCatalogOp],
        transaction_view: TransactionView,
    ) -> CommitRequest {
        let database_id = DatabaseId::new(session.current_database.id());
        let mut participants = active.participant_descriptors();
        if !ddl_changes.is_empty() {
            participants.push(CatalogCommitParticipant::participant_descriptor(
                database_id,
            ));
        }
        CommitRequest::new(
            database_id,
            active.txn_id(),
            transaction_view,
            session.commit_ack_policy(),
            active.frozen_lock_set(),
            participants,
        )
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
            let catalog_ops = prepared.catalog_ops().to_vec();
            let applier = Arc::new(CatalogCommittedRecordApplier::new(
                database_id,
                Arc::clone(session.current_database.catalog()),
                prepared,
            ));
            Some((descriptor, catalog_ops, applier))
        };

        let catalog_ops = catalog_prepare
            .as_ref()
            .map(|(_, ops, _)| ops.as_slice())
            .unwrap_or(&[]);

        let apply_descriptors = Self::collect_apply_descriptors(&ddl_changes);
        let durable_deferred_tasks = Self::collect_deferred_tasks(&prepared_storage, &ddl_changes);
        let post_commit_deferred_tasks = Self::post_commit_deferred_tasks(&durable_deferred_tasks);
        let index_publish_tasks = Self::collect_index_backfill_publish_tasks(&ddl_changes);
        request.add_participants(Self::participants_for_durable_tasks(
            database_id,
            &durable_deferred_tasks,
        ));
        let durable_plan = PreparedCommitPlan {
            txn_id: active.id,
            start_time: active.start_time,
            catalog_ops: catalog_ops.to_vec(),
            storage_ops: prepared_storage.storage_ops.clone(),
            apply_descriptors,
            deferred_tasks: durable_deferred_tasks.clone(),
            tablets: Vec::new(),
        };
        let mut sequencing_plan = CommitSequencingPlan::from_commit_plan(request.commit_plan());
        let stage_started_at = Instant::now();
        let ssi_outcome = match manager
            .validate_serializable_commit(&sequencing_plan.plan, &sequencing_plan.write_set)
        {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some((_, _, applier)) = &catalog_prepare {
                    let _ = applier.abort_prepared();
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
            .with_ssi_effect_epoch(ssi_outcome.ssi_effect_epoch);
        manager.record_commit_latency(
            paro_storage::transaction::manager::TransactionLatencyStage::CommitPrepare,
            prepare_latency,
        );
        manager.record_commit_latency(
            paro_storage::transaction::manager::TransactionLatencyStage::CommitValidate,
            validate_latency,
        );
        let write_set = sequencing_plan.write_set.clone();
        let coordinator = Arc::clone(session.current_database.commit_coordinator());
        let journal = session.current_database.journal_coordinator();
        let frontiers = journal.frontiers();
        coordinator.sync_backpressure_frontiers(
            paro_transaction::CommitTs::new(
                frontiers.durable_commit_id.max(manager.durable_commit_id()),
            ),
            paro_transaction::CommitTs::new(
                frontiers
                    .published_commit_id
                    .max(manager.published_commit_id()),
            ),
        );
        let storage_applier =
            StorageCommittedRecordApplier::new(Arc::clone(&manager), Arc::clone(&active));
        let database = Arc::clone(&session.current_database);
        let journal_apply_runtime = database.journal_apply_runtime();
        let max_seen_catalog_object_id = Self::max_seen_catalog_object_id(&ddl_changes);
        let mut published_at = None;
        let ack_policy = request.ack_policy;
        let final_fence_manager = Arc::clone(&manager);
        let durable_metrics_manager = Arc::clone(&manager);
        let publish_metrics_manager = Arc::clone(&manager);

        let ticket = match coordinator.execute_transaction(
            &request,
            sequencing_plan,
            move |plan, _in_flight| final_fence_manager.ssi_final_fence_reason(plan),
            {
                let durable_plan = durable_plan.clone();
                move |commit_ts| {
                    let durable_started_at = Instant::now();
                    let record = durable_plan.clone().into_record(commit_ts.into_raw());
                    let results = journal.append_records(&[JournalRecord::Commit(record)])?;
                    durable_metrics_manager.record_commit_latency(
                        paro_storage::transaction::manager::TransactionLatencyStage::CommitDurable,
                        durable_started_at.elapsed(),
                    );
                    let first = results.first().copied().ok_or_else(|| {
                        paro_common::error::internal(
                            "journal coordinator returned no append result for commit record",
                        )
                    })?;
                    let last = results.last().copied().unwrap_or(first);
                    Ok(CommitTicket::new(
                        commit_ts,
                        first.lsn,
                        last.durable_batch_lsn,
                    ))
                }
            },
            |ticket| {
                manager.register_committed_transaction_summary(
                    ticket.commit_ts,
                    request.txn_id,
                    request.read_ts,
                    &write_set,
                    &request.frozen_read_set,
                )?;
                Ok(())
            },
            |ticket| {
                let publish_started_at = Instant::now();
                let commit_id = ticket.commit_ts.into_raw();
                let committed_record = request.committed_record(ticket.commit_ts);
                let storage_apply_record = committed_record.clone();
                let storage_apply_descriptor = storage_descriptor.clone();
                let storage_apply = storage_applier.clone();
                let catalog_publish = catalog_prepare.as_ref().map(
                    |(catalog_descriptor, _, catalog_applier)| {
                        (catalog_descriptor.clone(), Arc::clone(catalog_applier))
                    },
                );
                let index_publish_tasks = index_publish_tasks.clone();
                let has_catalog_publish = catalog_publish.is_some();
                let catalog_serial = has_catalog_publish;
                let publish_database = Arc::clone(&database);
                let publish_manager = Arc::clone(&manager);
                let on_published: Box<dyn FnOnce() -> Result<()> + Send + 'static> =
                    if ack_policy == CommitAckPolicy::DurableOnlyAsync {
                        let coordinator = Arc::clone(&coordinator);
                        let participants = request.participants.clone();
                        let commit_ts = ticket.commit_ts;
                        Box::new(move || {
                            coordinator.mark_required_published(commit_ts, &participants);
                            Ok(())
                        })
                    } else {
                        Box::new(|| Ok(()))
                    };
                let apply_request = ApplyRequest {
                    lsn: ticket.durable_lsn,
                    durable_batch_lsn: ticket.durable_batch_lsn,
                    commit_id: Some(commit_id),
                    wait_mode: WaitMode::Published,
                    catalog_serial,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: Vec::<TabletApplyPart>::new(),
                    descriptor_phase: Box::new(move || {
                        storage_apply
                            .apply_required(&storage_apply_record, &storage_apply_descriptor)?;
                        Ok(())
                    }),
                    catalog_post: Box::new(move || {
                        if let Some((catalog_descriptor, catalog_applier)) = catalog_publish {
                            catalog_applier.apply_required(&committed_record, &catalog_descriptor)?;
                        }
                        for task in index_publish_tasks {
                            task.execute(commit_id)?;
                        }
                        let catalog_commit_id = if has_catalog_publish { commit_id } else { 0 };
                        let (_summary, journal_lsn) = publish_database
                            .publish_checkpoint_transaction(
                                commit_id,
                                catalog_commit_id,
                                max_seen_catalog_object_id,
                            );
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
                        if publish_manager.published_commit_id() < commit_id {
                            return Err(paro_common::error::internal(format!(
                                "required publish completed below commit floor: published={} commit={}",
                                publish_manager.published_commit_id(),
                                commit_id
                            )));
                        }
                        Ok(())
                    }),
                    on_published,
                };
                let outcome = if ack_policy == CommitAckPolicy::DurableOnlyAsync {
                    journal_apply_runtime.submit_async(apply_request)?;
                    RequiredPublishOutcome::Queued
                } else {
                    let observed = journal_apply_runtime.submit_observed(apply_request)?;
                    publish_metrics_manager.record_commit_latency(
                        paro_storage::transaction::manager::TransactionLatencyStage::CommitRequiredPublishWait,
                        std::time::Duration::from_micros(observed.wait_micros),
                    );
                    RequiredPublishOutcome::Completed
                };
                published_at = Some(Instant::now());
                publish_metrics_manager.record_commit_latency(
                    paro_storage::transaction::manager::TransactionLatencyStage::CommitPublish,
                    publish_started_at.elapsed(),
                );
                Ok(outcome)
            },
        ) {
            Ok(ticket) => ticket,
            Err(CommitCoordinatorError::InvalidRequest { error }) => {
                if let Some((_, _, applier)) = &catalog_prepare {
                    let _ = applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::ValidationFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error: paro_common::error::invalid_transaction_state(error.to_string()),
                    rollback_succeeded,
                });
            }
            Err(CommitCoordinatorError::Backpressure { error }) => {
                if let Some((_, _, applier)) = &catalog_prepare {
                    let _ = applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::Backpressure);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error: paro_common::error::invalid_transaction_state(error.to_string()),
                    rollback_succeeded,
                });
            }
            Err(CommitCoordinatorError::Rejected { rejected, .. }) => {
                if let Some((_, _, applier)) = &catalog_prepare {
                    let _ = applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::ValidationFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error: paro_common::error::serialization_failure(format!(
                        "commit rejected at ordered final fence: {:?}",
                        rejected.reason
                    )),
                    rollback_succeeded,
                });
            }
            Err(CommitCoordinatorError::DurableAppend { error, .. }) => {
                if let Some((_, _, applier)) = &catalog_prepare {
                    let _ = applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::DurableAppendFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error,
                    rollback_succeeded,
                });
            }
            Err(CommitCoordinatorError::MissingTicket { commit_ts }) => {
                if let Some((_, _, applier)) = &catalog_prepare {
                    let _ = applier.abort_prepared();
                }
                let _ = storage_participant.abort(AbortReason::DurableAppendFailed);
                let rollback_succeeded = manager.rollback_transaction(Arc::clone(&active)).is_ok();
                return Err(CommitFailure {
                    error: paro_common::error::internal(format!(
                        "commit coordinator accepted commit {} without a durable ticket",
                        commit_ts
                    )),
                    rollback_succeeded,
                });
            }
            Err(CommitCoordinatorError::PostDurable { error, .. }) => {
                return Err(CommitFailure {
                    error,
                    rollback_succeeded: false,
                });
            }
            Err(CommitCoordinatorError::Publish { error, .. }) => {
                return Err(CommitFailure {
                    error,
                    rollback_succeeded: false,
                });
            }
        };

        let commit_id = ticket.commit_ts.into_raw();
        if ack_policy == CommitAckPolicy::DurableOnlyAsync {
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
            published_at: published_at.unwrap_or_else(Instant::now),
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
    use paro_transaction::{LockMode, LockRequest, LockResource, ParticipantKind, TableId};

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
