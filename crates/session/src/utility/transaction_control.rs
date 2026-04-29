// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::completion::StatementCompletion;
use crate::result::sink::ResultSink;
use crate::utility::settings::isolation_level_to_setting_value;
use crate::Session;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_parser::ast::{
    TransactionIsolationLevel, TransactionKind, TransactionOptions, TransactionReadMode,
};
use paro_transaction::IsolationLevel;

fn isolation_level_from_ast(level: TransactionIsolationLevel) -> IsolationLevel {
    match level {
        TransactionIsolationLevel::Serializable => IsolationLevel::Serializable,
        TransactionIsolationLevel::Snapshot => IsolationLevel::Snapshot,
    }
}

fn read_only_from_ast(mode: TransactionReadMode) -> bool {
    match mode {
        TransactionReadMode::ReadOnly => true,
        TransactionReadMode::ReadWrite => false,
    }
}

fn transaction_characteristics(
    options: TransactionOptions,
) -> (Option<IsolationLevel>, Option<bool>) {
    (
        options.isolation_level.map(isolation_level_from_ast),
        options.read_mode.map(read_only_from_ast),
    )
}

pub(crate) async fn execute_transaction_control<S: ResultSink>(
    session: &mut Session,
    kind: &TransactionKind,
    sink: &mut S,
) -> Result<()> {
    let completion = match kind {
        TransactionKind::Begin(options) => {
            let (isolation_level, read_only) = transaction_characteristics(*options);
            session.begin_explicit_transaction_with_characteristics(isolation_level, read_only)?;
            StatementCompletion::Begin
        }
        TransactionKind::Start(options) => {
            let (isolation_level, read_only) = transaction_characteristics(*options);
            session.begin_explicit_transaction_with_characteristics(isolation_level, read_only)?;
            StatementCompletion::StartTransaction
        }
        TransactionKind::Commit => {
            session.commit_transaction()?;
            StatementCompletion::Commit
        }
        TransactionKind::Rollback => {
            session.rollback_transaction()?;
            StatementCompletion::Rollback
        }
        TransactionKind::SetTransaction(options) => {
            let (isolation_level, read_only) = transaction_characteristics(*options);
            session.set_transaction_characteristics(isolation_level, read_only)?;
            StatementCompletion::Set
        }
        TransactionKind::SetSessionCharacteristics(options) => {
            let (isolation_level, read_only) = transaction_characteristics(*options);
            if let Some(isolation_level) = isolation_level {
                session.set_session_setting(
                    "default_transaction_isolation",
                    Value::Varchar(isolation_level_to_setting_value(isolation_level).to_string()),
                )?;
            }
            if let Some(read_only) = read_only {
                session.set_default_transaction_read_only(read_only);
            }
            StatementCompletion::Set
        }
        TransactionKind::Savepoint(name) => {
            if !session.is_in_explicit_block() {
                return Err(paro_error::invalid_transaction_state(
                    "SAVEPOINT can only be used in transaction blocks".to_string(),
                ));
            }
            let storage_mark = session.transaction.mark_savepoint()?;
            let portal_mark = session.current_portal_mark();
            session
                .transaction
                .define_savepoint(name.name.clone(), portal_mark, storage_mark);
            StatementCompletion::Savepoint
        }
        TransactionKind::ReleaseSavepoint(name) => {
            if !session.is_in_explicit_block() {
                return Err(paro_error::invalid_transaction_state(
                    "RELEASE SAVEPOINT can only be used in transaction blocks".to_string(),
                ));
            }
            session.transaction.release_savepoint(&name.name)?;
            StatementCompletion::Release
        }
        TransactionKind::RollbackToSavepoint(name) => {
            if !session.has_active_transaction() {
                return Err(paro_error::invalid_transaction_state(format!(
                    "savepoint \"{}\" does not exist",
                    name.name
                )));
            }
            let portal_mark = session
                .transaction
                .rollback_to_savepoint(&name.name)?
                .portal_mark;
            session.on_savepoint_rollback_prepared(portal_mark);
            crate::utility::settings::reconcile_effective_settings(session)?;
            session.refresh_session_metadata();
            StatementCompletion::RollbackTo
        }
        TransactionKind::PrepareTransaction(_) => {
            return Err(paro_error::not_implemented(
                "PREPARE TRANSACTION is not implemented yet",
            ))
        }
        TransactionKind::CommitPrepared(_) => {
            return Err(paro_error::not_implemented(
                "COMMIT PREPARED is not implemented yet",
            ))
        }
        TransactionKind::RollbackPrepared(_) => {
            return Err(paro_error::not_implemented(
                "ROLLBACK PREPARED is not implemented yet",
            ))
        }
    };

    sink.finish_result(&completion).await?;
    Ok(())
}
