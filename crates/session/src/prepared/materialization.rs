// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::error::Result;
use paro_context::StatementContext;
use paro_execution::query_executor::compiled::CompiledStatement;
use paro_execution::query_executor::executor::Executor;

use crate::prepared::portal::MaterializedPortalData;
use crate::result::retained_store::SessionRetainedResultStore;
use crate::Session;

/// Materializes a compiled statement into in-memory chunks for portal/cursor consumption.
///
/// # Preconditions
///
/// Callers must have already invoked `session.begin_statement_scope()` so the
/// session owns an active statement context. This helper initializes the executor
/// through `session.set_executor()` and runs it through `session.get_executor()`,
/// both of which panic when no active statement is present.
pub(crate) async fn materialize_compiled_statement(
    session: &mut Session,
    ctx: Arc<StatementContext>,
    compiled: CompiledStatement,
) -> Result<MaterializedPortalData> {
    let executor = Executor::new(ctx);
    session.set_executor(executor);

    let mut stream = session.get_executor().execute(compiled)?;
    let store = SessionRetainedResultStore::new(session.session_memory_budget());
    while let Some(chunk) = stream.fetch()? {
        store.append(chunk.try_deep_copy(chunk.allocator().clone())?)?;
    }

    Ok(MaterializedPortalData::new(store))
}
