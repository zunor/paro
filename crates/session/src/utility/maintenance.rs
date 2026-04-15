// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::completion::StatementCompletion;
use crate::result::sink::ResultSink;
use crate::Session;
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;

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
