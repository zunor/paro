// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Utility front-end execution helpers.

mod database_ddl;
mod discard;
mod maintenance;
pub(crate) mod settings;
mod transaction_control;

use crate::dispatch::UtilityCommand;
use crate::result::sink::ResultSink;
use crate::Session;
use paro_common::error::Result;

pub(crate) async fn execute_utility_command<S: ResultSink>(
    session: &mut Session,
    cmd: UtilityCommand,
    sink: &mut S,
) -> Result<()> {
    match cmd {
        UtilityCommand::Transaction(stmt) => {
            transaction_control::execute_transaction_control(session, &stmt.kind, sink).await
        }
        UtilityCommand::VariableSet(stmt) => {
            settings::execute_variable_set(session, &stmt, sink).await
        }
        UtilityCommand::VariableShow(stmt) => {
            settings::execute_variable_show(session, &stmt, sink).await
        }
        UtilityCommand::Discard(stmt) => discard::execute_discard(session, stmt.target, sink).await,
        UtilityCommand::Checkpoint(_) => maintenance::execute_checkpoint(session, sink).await,
        UtilityCommand::CreateDatabase(stmt) => {
            database_ddl::execute_create_database(session, &stmt, sink).await
        }
        UtilityCommand::DropDatabase(stmt) => {
            database_ddl::execute_drop_database(session, &stmt, sink).await
        }
    }
}
