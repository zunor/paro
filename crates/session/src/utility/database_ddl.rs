// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::completion::StatementCompletion;
use crate::result::sink::ResultSink;
use crate::Session;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_parser::ast::{CreateDatabaseStmt, DropDatabaseStmt};

pub(crate) async fn execute_create_database<S: ResultSink>(
    session: &mut Session,
    stmt: &CreateDatabaseStmt,
    sink: &mut S,
) -> Result<()> {
    ensure_database_utility_allowed(session, "CREATE DATABASE/DROP DATABASE")?;

    if session
        .instance
        .database_registry()
        .get_database(&stmt.database_name)
        .is_some()
    {
        if !stmt.if_not_exists {
            return Err(paro_error::catalog(format!(
                "database \"{}\" already exists",
                stmt.database_name
            )));
        }
    } else {
        session
            .instance
            .create_database(&stmt.database_name)
            .map_err(|e| paro_error::catalog(e.to_string()))?;
        tracing::info!(
            target: targets::CATALOG,
            session_id = session.id,
            database = %stmt.database_name,
            "Database created"
        );
    }

    sink.finish_result(&StatementCompletion::CreateDatabase)
        .await?;
    Ok(())
}

pub(crate) async fn execute_drop_database<S: ResultSink>(
    session: &mut Session,
    stmt: &DropDatabaseStmt,
    sink: &mut S,
) -> Result<()> {
    ensure_database_utility_allowed(session, "CREATE DATABASE/DROP DATABASE")?;

    let name = &stmt.database.name;
    if session.current_database.name() == name {
        return Err(paro_error::catalog(format!(
            "cannot drop the currently open database \"{name}\"",
        )));
    }

    match session.instance.drop_database(name) {
        Ok(_) => {
            tracing::info!(
                target: targets::CATALOG,
                session_id = session.id,
                database = %name,
                "Database dropped"
            );
        }
        Err(err) if !stmt.if_exists => return Err(paro_error::catalog(err.to_string())),
        Err(_) => {}
    }

    sink.finish_result(&StatementCompletion::DropDatabase)
        .await?;
    Ok(())
}

fn ensure_database_utility_allowed(session: &Session, action: &str) -> Result<()> {
    if session.has_active_transaction() && !session.is_auto_commit() {
        return Err(paro_error::invalid_transaction_state(format!(
            "{action} cannot run inside a transaction block",
        )));
    }
    Ok(())
}
