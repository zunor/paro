use crate::completion::{DiscardCommand, StatementCompletion};
use crate::result::sink::ResultSink;
use crate::Session;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::DiscardTarget;

pub(crate) async fn execute_discard<S: ResultSink>(
    session: &mut Session,
    target: DiscardTarget,
    sink: &mut S,
) -> Result<()> {
    if matches!(target, DiscardTarget::All) && session.has_active_transaction() {
        return Err(paro_error::invalid_transaction_state(
            "DISCARD ALL cannot run inside a transaction block".to_string(),
        ));
    }

    match target {
        DiscardTarget::All => {
            session.reset_session_state();
        }
        DiscardTarget::Plans => {
            session.state.clear_prepared_statements();
            session.refresh_session_metadata();
        }
        DiscardTarget::Temp | DiscardTarget::Sequences => {
            session.refresh_session_metadata();
        }
    }

    sink.finish_result(&StatementCompletion::Discard(DiscardCommand::from(target)))
        .await?;
    Ok(())
}
