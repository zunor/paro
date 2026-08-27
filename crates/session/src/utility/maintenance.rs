// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::completion::StatementCompletion;
use crate::result::sink::ResultSink;
use crate::Session;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_parser::ast::{CompactTarget, OptimizeTableAction, OptimizeTableStmt};
use paro_storage::table::table_handle::TableHandle;
use std::sync::Arc;

pub(crate) async fn execute_checkpoint<S: ResultSink>(
    session: &mut Session,
    sink: &mut S,
) -> Result<()> {
    session
        .current_database
        .force_checkpoint()
        .map_err(|e| paro_error::internal(e.to_string()))?;
    tracing::info!(
        target: targets::CHECKPOINT,
        session_id = session.id,
        database = %session.current_database.name(),
        force = true,
        "Checkpoint completed"
    );

    sink.finish_result(&StatementCompletion::Checkpoint).await?;
    Ok(())
}

pub(crate) async fn execute_optimize_table<S: ResultSink>(
    session: &mut Session,
    stmt: &OptimizeTableStmt,
    sink: &mut S,
) -> Result<()> {
    if session.has_active_transaction() && !session.is_auto_commit() {
        return Err(paro_error::invalid_transaction_state(
            "OPTIMIZE TABLE cannot run inside a transaction block",
        ));
    }
    match &stmt.action {
        OptimizeTableAction::Compact {
            target: CompactTarget::Block,
        } => {}
        OptimizeTableAction::Compact {
            target: CompactTarget::Segment,
        } => {
            return Err(paro_error::not_supported(
                "OPTIMIZE TABLE COMPACT SEGMENT has no meaning for Paro rowset storage",
            ));
        }
        OptimizeTableAction::All | OptimizeTableAction::Purge { .. } => {
            return Err(paro_error::not_supported(
                "OPTIMIZE TABLE currently requires the explicit COMPACT action",
            ));
        }
    }

    let table = resolve_table(session, stmt)?;
    let max_compactions = stmt
        .limit
        .map(usize::try_from)
        .transpose()
        .map_err(|_| paro_error::out_of_range("OPTIMIZE TABLE LIMIT exceeds usize"))?;
    let table_name = stmt.table.name.clone();
    let completed = tokio::task::spawn_blocking(move || table.optimize_all(max_compactions))
        .await
        .map_err(|error| {
            paro_error::internal(format!("OPTIMIZE TABLE worker failed: {error}"))
        })??;
    tracing::info!(
        target: targets::STORAGE,
        session_id = session.id,
        table = %table_name,
        compactions = completed,
        "OPTIMIZE TABLE completed"
    );
    sink.finish_result(&StatementCompletion::Custom("OPTIMIZE TABLE".to_string()))
        .await?;
    Ok(())
}

fn resolve_table(session: &Session, stmt: &OptimizeTableStmt) -> Result<Arc<TableHandle>> {
    if let Some(database) = stmt.database.as_ref() {
        if !database
            .name
            .eq_ignore_ascii_case(session.current_database.name())
        {
            return Err(paro_error::not_supported(
                "OPTIMIZE TABLE does not support cross-database maintenance",
            ));
        }
    }

    let snapshot = session.catalog_txn_view();
    let table_name = &stmt.table.name;
    let entry = if let Some(schema) = stmt.schema.as_ref() {
        session
            .current_database
            .catalog()
            .get_table(&snapshot, &schema.name, table_name)?
    } else {
        let mut found = None;
        for search_entry in session.search_path().get() {
            let catalog_name = if search_entry.catalog.is_empty() {
                session.current_database.name()
            } else {
                search_entry.catalog.as_str()
            };
            if !catalog_name.eq_ignore_ascii_case(session.current_database.name()) {
                continue;
            }
            if let Ok(entry) = session.current_database.catalog().get_table(
                &snapshot,
                &search_entry.schema,
                table_name,
            ) {
                found = Some(entry);
                break;
            }
        }
        found.ok_or_else(|| paro_error::table_not_found(table_name))?
    };
    let table = entry
        .as_table()
        .ok_or_else(|| paro_error::wrong_object_type("table", table_name))?;
    table
        .get_storage()
        .cloned()
        .ok_or_else(|| paro_error::wrong_object_type("stored table", table_name))
}
