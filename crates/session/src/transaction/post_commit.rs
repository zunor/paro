// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::commit::CommitOutcome;
use super::ddl_changes::TransientCatalogRuntime;
use crate::session::Session;
use paro_catalog::entry::{
    CatalogEntryEnum, IndexCatalogEntry, IndexCoverage, IndexType as CatalogIndexType,
};
use paro_common::effect::{DeferredTask, PostCommitHookDescriptor, RuntimeTransitionDescriptor};
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_storage::metrics::storage_metrics;
use paro_storage::search::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};
use serde_json::{json, Value};

fn vector_dimension(logical_type: &LogicalType) -> u64 {
    match logical_type {
        LogicalType::Array(_, dimension) => *dimension as u64,
        _ => 0,
    }
}

pub struct PostCommitActions;

impl PostCommitActions {
    const DEFERRED_TASK_ATTEMPTS: usize = 2;

    pub fn execute(session: &mut Session, outcome: CommitOutcome) -> Result<()> {
        session.on_transaction_commit_prepared();
        if !outcome.catalog_ops.is_empty() {
            if let Err(err) = session.current_database.sync_compaction_tablets() {
                tracing::warn!(
                    target: targets::TRANSACTION,
                    commit_id = outcome.commit_id,
                    error = %err,
                    "compaction tablet registry sync failed after durable catalog commit"
                );
            }
        }
        Self::execute_deferred_tasks(session, &outcome)?;
        session.notify_transaction_commit();
        session.current_database.maybe_gc_catalog();
        crate::utility::settings::reconcile_effective_settings(session)?;
        session.refresh_session_metadata();
        Ok(())
    }

    fn execute_deferred_tasks(session: &Session, outcome: &CommitOutcome) -> Result<()> {
        for task in &outcome.deferred_tasks {
            let deferred_task_lag_micros = outcome
                .published_at
                .elapsed()
                .as_micros()
                .min(u64::MAX as u128) as u64;
            match task {
                DeferredTask::GraphDmlMaintenance { deltas } => {
                    tracing::info!(
                        target: targets::WAL,
                        commit_id = outcome.commit_id,
                        deferred_task_lag_micros,
                        task_kind = "graph_dml_maintenance",
                        "Dispatching deferred task after durable commit"
                    );
                    let hooks = [PostCommitHookDescriptor::GraphDmlMaintenance {
                        deltas: deltas.clone(),
                    }];
                    if let Err(err) = Self::run_deferred_with_retry(
                        "graph_dml_maintenance",
                        outcome.commit_id,
                        || session.apply_post_commit_hooks(&hooks, outcome.commit_id),
                    ) {
                        storage_metrics().record_derived_index_lag_ts(outcome.commit_id);
                        tracing::warn!(
                            target: targets::TRANSACTION,
                            commit_id = outcome.commit_id,
                            error = %err,
                            "deferred graph publish failed after retry; keeping storage/catalog visibility published"
                        );
                    }
                }
                DeferredTask::FinalizeIndexState {
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config,
                } => {
                    tracing::info!(
                        target: targets::WAL,
                        commit_id = outcome.commit_id,
                        deferred_task_lag_micros,
                        task_kind = "finalize_index_state",
                        index = %index.name,
                        table = %table_name,
                        "Dispatching deferred task after durable commit"
                    );
                    if let Err(err) = Self::run_deferred_with_retry(
                        "finalize_index_state",
                        outcome.commit_id,
                        || {
                            Self::finalize_index_state_task(
                                session,
                                outcome,
                                &index,
                                &table_name,
                                &index_type,
                                &column_ids,
                                fulltext_config.as_deref(),
                            )
                        },
                    ) {
                        storage_metrics().record_derived_index_lag_ts(outcome.commit_id);
                        tracing::warn!(
                            target: targets::TRANSACTION,
                            commit_id = outcome.commit_id,
                            index = %index.name,
                            table = %table_name,
                            index_type = %index_type,
                            error = %err,
                            "deferred index runtime task failed after retry; keeping storage/catalog visibility published"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn run_deferred_with_retry(
        task_kind: &'static str,
        commit_id: u64,
        mut task: impl FnMut() -> Result<()>,
    ) -> Result<()> {
        let mut last_error = None;
        for attempt in 1..=Self::DEFERRED_TASK_ATTEMPTS {
            match task() {
                Ok(()) => return Ok(()),
                Err(err) => {
                    tracing::warn!(
                        target: targets::TRANSACTION,
                        commit_id,
                        task_kind,
                        attempt,
                        max_attempts = Self::DEFERRED_TASK_ATTEMPTS,
                        error = %err,
                        "deferred derived publish attempt failed"
                    );
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| paro_error::internal("deferred derived publish failed")))
    }

    fn finalize_index_state_task(
        session: &Session,
        outcome: &CommitOutcome,
        index: &paro_common::ddl::DdlObjectKey,
        table_name: &str,
        index_type: &str,
        column_ids: &[u32],
        fulltext_config: Option<&str>,
    ) -> Result<()> {
        for op in &outcome.catalog_ops {
            let matches_transition = op.runtime_transitions.iter().any(|transition| {
                matches!(
                    transition,
                    RuntimeTransitionDescriptor::AttachIndexState {
                        index: transition_index,
                        ..
                    } if transition_index == index
                )
            });
            if !matches_transition {
                continue;
            }

            let Some(TransientCatalogRuntime::CreateIndex(action)) = op.transient_runtime.as_ref()
            else {
                return Self::finalize_index_state_from_catalog(
                    session,
                    index,
                    table_name,
                    index_type,
                    column_ids,
                    fulltext_config,
                );
            };

            let apply_result = (|| -> Result<()> {
                let storage = action.table.get_storage().ok_or_else(|| {
                    paro_error::internal(format!(
                        "table '{}' has no storage for CREATE INDEX deferred build",
                        action.table.base.base.name
                    ))
                })?;

                if let Some(built_index) = action.built_index.as_ref() {
                    if storage.has_index(&action.info.name) {
                        let _ = storage.remove_index(&action.info.name);
                    }
                    storage.add_index(built_index.clone())?;
                } else {
                    Self::finalize_index_state_from_action(session, action)?;
                }

                let coverage = Self::recompute_index_coverage(action)?;
                action.entry.mark_ready_with_coverage(coverage);
                Ok(())
            })();

            if let Err(err) = apply_result {
                Self::cleanup_failed_index_state_from_action(action);
                action.entry.mark_failed(Some(err.to_string()));
                return Err(err);
            }

            return Ok(());
        }

        Self::finalize_index_state_from_catalog(
            session,
            index,
            table_name,
            index_type,
            column_ids,
            fulltext_config,
        )
    }

    fn finalize_index_state_from_action(
        _session: &Session,
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) -> Result<()> {
        let Some(storage) = action.table.get_storage() else {
            if action.info.index_type == CatalogIndexType::ART
                || action.info.index_type == CatalogIndexType::HNSW
                || action.info.index_type == CatalogIndexType::FullText
            {
                return Err(paro_error::internal(format!(
                    "table '{}' has no storage. cannot finalize {} index",
                    action.table.base.base.name,
                    action.info.index_type.as_str()
                )));
            }
            return Ok(());
        };

        if action.info.index_type == CatalogIndexType::ART {
            let [column_id] = action.info.column_ids.as_slice() else {
                return Err(paro_error::not_supported(
                    "ART indexes currently require exactly one column",
                ));
            };
            storage.rebuild_art_index(column_id.index)?;
        }

        Self::register_search_definition_from_entry(storage.as_ref(), action.entry.as_ref())?;

        Ok(())
    }

    fn finalize_index_state_from_catalog(
        session: &Session,
        index: &paro_common::ddl::DdlObjectKey,
        table_name: &str,
        index_type: &str,
        column_ids: &[u32],
        _fulltext_config: Option<&str>,
    ) -> Result<()> {
        let txn = session.catalog_txn_view();
        let schema_name = index.schema.as_deref().ok_or_else(|| {
            paro_error::serialization_error("CREATE INDEX runtime transition missing schema name")
        })?;
        let schema = session
            .current_database
            .catalog()
            .get_schema(&txn, schema_name)?;
        let Some(table_entry) = schema.get_table(txn.transaction_id, txn.start_time, table_name)
        else {
            tracing::debug!(
                target: targets::TRANSACTION,
                table = %table_name,
                index = %index.name,
                "index runtime transition skipped: table disappeared before post-commit"
            );
            return Ok(());
        };
        let Some(table) = table_entry.as_ref().as_table() else {
            return Ok(());
        };
        let Some(storage) = table.get_storage() else {
            return Ok(());
        };

        let apply_result = (|| -> Result<()> {
            if CatalogIndexType::from_str(index_type) == CatalogIndexType::ART {
                let [column_id] = column_ids else {
                    return Err(paro_error::not_supported(
                        "ART indexes currently require exactly one column",
                    ));
                };
                storage.declare_art_index(*column_id);
                if let Err(err) = storage.rebuild_art_index(*column_id) {
                    storage.forget_art_index(*column_id);
                    let _ = storage.drop_art_index(*column_id);
                    return Err(err);
                }
                return Ok(());
            }

            let Some(entry) = Self::resolve_index_entry(session, index) else {
                return Err(paro_error::internal(format!(
                    "index '{}' disappeared before search registry registration",
                    index.name
                )));
            };
            Self::register_search_definition_from_entry(storage.as_ref(), entry.as_ref())?;
            Ok(())
        })();

        if let Err(err) = apply_result {
            Self::cleanup_failed_index_state_from_catalog(
                storage.as_ref(),
                &index.name,
                index_type,
                column_ids,
            );
            if let Some(entry) = Self::resolve_index_entry(session, index) {
                entry.mark_failed(Some(err.to_string()));
            }
            return Err(err);
        }

        Ok(())
    }

    fn resolve_index_entry(
        session: &Session,
        index: &paro_common::ddl::DdlObjectKey,
    ) -> Option<std::sync::Arc<IndexCatalogEntry>> {
        let txn = session.catalog_txn_view();
        let schema_name = index.schema.as_deref()?;
        let schema = session
            .current_database
            .catalog()
            .get_schema(&txn, schema_name)
            .ok()?;
        let entry = schema.get_index(txn.transaction_id, txn.start_time, &index.name)?;
        match &*entry {
            CatalogEntryEnum::Index(index_entry) => Some(index_entry.clone()),
            _ => None,
        }
    }

    fn cleanup_failed_index_state_from_action(
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) {
        let Some(storage) = action.table.get_storage() else {
            return;
        };
        let column_ids = action
            .info
            .column_ids
            .iter()
            .map(|column| column.index)
            .collect::<Vec<_>>();
        Self::cleanup_failed_index_state_from_catalog(
            storage.as_ref(),
            &action.info.name,
            action.info.index_type.as_str(),
            &column_ids,
        );
    }

    fn cleanup_failed_index_state_from_catalog(
        storage: &paro_storage::table::table_handle::TableHandle,
        index_name: &str,
        index_type: &str,
        column_ids: &[u32],
    ) {
        match CatalogIndexType::from_str(index_type) {
            CatalogIndexType::ART => {
                for column_id in column_ids {
                    storage.forget_art_index(*column_id);
                    let _ = storage.drop_art_index(*column_id);
                }
            }
            CatalogIndexType::HNSW => {
                let _ = storage.unregister_search_definition_by_name(index_name);
            }
            CatalogIndexType::Sparse => {
                let _ = storage.unregister_search_definition_by_name(index_name);
            }
            CatalogIndexType::FullText => {
                let _ = storage.unregister_search_definition_by_name(index_name);
            }
            _ => {}
        }
    }

    fn recompute_index_coverage(
        action: &crate::transaction::ddl_changes::IndexPostCommitAction,
    ) -> Result<Option<IndexCoverage>> {
        let Some(storage) = action.table.get_storage() else {
            return Ok(action.coverage.clone());
        };

        match action.info.index_type {
            CatalogIndexType::HNSW | CatalogIndexType::Sparse | CatalogIndexType::FullText => {
                let definition_id = action.entry.base.base.object_id.raw();
                let Some(coverage) = storage.search_generation_coverage(definition_id)? else {
                    return Ok(action.coverage.clone());
                };
                Ok(Some(IndexCoverage::from_counts(
                    coverage.visible_version,
                    coverage.visible_segment_count,
                    coverage.indexed_segment_count,
                )))
            }
            _ => Ok(action.coverage.clone()),
        }
    }

    fn register_search_definition_from_entry(
        storage: &paro_storage::table::table_handle::TableHandle,
        entry: &IndexCatalogEntry,
    ) -> Result<()> {
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
            freshness_policy: SearchFreshnessPolicy::default_for_kind(kind),
            provider_config: Self::search_provider_config(storage, entry)?,
            config_fingerprint: 0,
        };
        let expression = definition.expression.clone();
        let provider_config = definition.provider_config.clone();
        let column_ids = definition.column_ids.clone();
        let kind = definition.kind;
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

    fn search_provider_config(
        storage: &paro_storage::table::table_handle::TableHandle,
        entry: &IndexCatalogEntry,
    ) -> Result<Value> {
        match entry.index_type {
            CatalogIndexType::HNSW => {
                let [column] = entry.get_column_ids() else {
                    return Err(paro_error::not_supported(
                        "HNSW search definition requires exactly one indexed column",
                    ));
                };
                let schema = storage
                    .tablet()
                    .schema()
                    .ok_or_else(|| paro_error::internal("table schema missing for HNSW config"))?;
                let column = schema.column_by_id(column.index).ok_or_else(|| {
                    paro_error::column_not_found(format!(
                        "HNSW index column {} not found in schema",
                        column.index
                    ))
                })?;
                Ok(json!({
                    "m": column.hnsw_m,
                    "ef_construct": column.hnsw_ef_construct,
                    "distance": column.hnsw_distance,
                    "dimension": vector_dimension(&column.logical_type),
                }))
            }
            CatalogIndexType::Sparse => Ok(json!({ "physical_encoding": "binary-v1" })),
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
