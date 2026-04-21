// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::ddl_changes::PreparedCatalogOp;
use super::session_transaction::FrozenTransaction;
use crate::session::Session;
use paro_common::ddl::DdlChange;
use paro_common::effect::{DeferredTask, PostCommitHookDescriptor, RuntimeTransitionDescriptor};
use paro_common::error::{ParoError, Result};
use paro_common::logging::targets;
use paro_storage::transaction::txn::{PreparedStorageCommit, Transaction};
use paro_storage::wal::txn_record::TxnRecord;
use paro_storage::wal::wal_writer::WalWriter;
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

        let manager = session.current_database.transaction_manager();
        let commit_id = manager.allocate_commit_id();

        if let Err(error) =
            Self::write_txn_journal(session, &active, &ddl_changes, commit_id, &prepared_storage)
        {
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

        if let Err(error) =
            manager.commit_transaction_with_commit_id(Arc::clone(&active), commit_id)
        {
            return Err(CommitFailure {
                error,
                rollback_succeeded: false,
            });
        }

        if let Err(error) = Self::publish_catalog_changes(session, &mut ddl_changes, commit_id) {
            return Err(CommitFailure {
                error,
                rollback_succeeded: false,
            });
        }
        let (catalog_commit_id, max_seen_object_id) =
            Self::published_catalog_watermarks(&ddl_changes, commit_id);
        let (_summary, journal_lsn) = session.current_database.publish_checkpoint_transaction(
            commit_id,
            catalog_commit_id,
            max_seen_object_id,
        );
        if let Some(wal) = session.current_database.wal() {
            if let Err(error) = wal.note_flushed_lsn(journal_lsn) {
                tracing::warn!(
                    target: targets::WAL,
                    db = %session.current_database.name(),
                    lsn = journal_lsn,
                    error = %error,
                    "failed to advance journal segment catalog after transaction flush"
                );
            }
        }

        let deferred_tasks = Self::collect_deferred_tasks(&prepared_storage, &ddl_changes);
        let mut post_commit_hooks = prepared_storage.post_commit_hooks;
        for op in &ddl_changes {
            post_commit_hooks.extend(op.post_commit_hooks.clone());
        }

        Ok(CommitOutcome {
            commit_id,
            active_txn: active,
            catalog_ops: ddl_changes,
            deferred_tasks,
            published_at: Instant::now(),
            post_commit_hooks,
        })
    }

    fn publish_catalog_changes(
        session: &Session,
        ddl_changes: &mut [PreparedCatalogOp],
        commit_id: u64,
    ) -> Result<()> {
        for op in ddl_changes.iter_mut() {
            if let Some(handle) = op.catalog.take() {
                handle.publish(commit_id)?;
            }
            if let Some(delta) = op.dependencies.take() {
                delta.publish(session.current_database.catalog().dependency_graph())?;
            }
        }
        Ok(())
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

    fn write_txn_journal(
        session: &Session,
        active_txn: &Arc<Transaction>,
        ddl_changes: &[PreparedCatalogOp],
        commit_id: u64,
        prepared_storage: &PreparedStorageCommit,
    ) -> Result<()> {
        let Some(wal) = session.current_database.wal() else {
            return Ok(());
        };
        let write_state = wal.begin_write();
        let writer: &Arc<WalWriter> = write_state.wal();

        writer.write_entry(
            TxnRecord::Begin {
                txn_id: active_txn.id,
                start_time: active_txn.start_time,
            }
            .wal_type(),
            &TxnRecord::Begin {
                txn_id: active_txn.id,
                start_time: active_txn.start_time,
            }
            .serialize_data()?,
        )?;

        for (seq, op) in ddl_changes.iter().enumerate() {
            let record = TxnRecord::CatalogOp {
                seq: seq as u32,
                op: paro_common::effect::CatalogTxnOp {
                    change: op.record.clone(),
                },
            };
            writer.write_entry(record.wal_type(), &record.serialize_data()?)?;
        }

        let base_seq = ddl_changes.len() as u32;
        for (idx, op) in prepared_storage.data_ops.iter().enumerate() {
            let record = TxnRecord::DataOp {
                seq: base_seq + idx as u32,
                op: op.clone(),
            };
            writer.write_entry(record.wal_type(), &record.serialize_data()?)?;
        }

        let hook_base_seq = base_seq + prepared_storage.data_ops.len() as u32;
        for (idx, hook) in prepared_storage.post_commit_hooks.iter().enumerate() {
            let record = TxnRecord::PostCommitHook {
                seq: hook_base_seq + idx as u32,
                hook: hook.clone(),
            };
            writer.write_entry(record.wal_type(), &record.serialize_data()?)?;
        }

        let commit = TxnRecord::Commit {
            txn_id: active_txn.id,
            commit_id,
        };
        writer.write_entry(commit.wal_type(), &commit.serialize_data()?)?;
        write_state.flush()?;
        Ok(())
    }

    fn published_catalog_watermarks(
        ddl_changes: &[PreparedCatalogOp],
        commit_id: u64,
    ) -> (u64, u64) {
        if ddl_changes.is_empty() {
            return (0, 0);
        }

        let max_seen_object_id = ddl_changes
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
            .unwrap_or(0);

        (commit_id, max_seen_object_id)
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
}

impl Session {
    pub(crate) fn commit_via_pipeline(&mut self) -> Result<()> {
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
}
