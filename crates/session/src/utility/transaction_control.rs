use crate::completion::StatementCompletion;
use crate::result::sink::ResultSink;
use crate::Session;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::TransactionKind;

pub(crate) async fn execute_transaction_control<S: ResultSink>(
    session: &mut Session,
    kind: &TransactionKind,
    sink: &mut S,
) -> Result<()> {
    let completion = match kind {
        TransactionKind::Begin => {
            session.begin_explicit_transaction()?;
            StatementCompletion::Begin
        }
        TransactionKind::Start => {
            session.begin_explicit_transaction()?;
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
        TransactionKind::Savepoint(name) => {
            if !session.is_in_explicit_block() {
                return Err(paro_error::invalid_transaction_state(
                    "SAVEPOINT can only be used in transaction blocks".to_string(),
                ));
            }
            let storage_mark = session.transaction.active_transaction()?.mark_savepoint()?;
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
