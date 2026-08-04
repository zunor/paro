// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_context::StatementCancellation;
use paro_storage::search::{
    OpenedSearchCursor, ResourceBudget, SearchBatchConfig, SearchBatchState, SearchReadSnapshot,
};
use paro_storage::table::table_handle::TableHandle;

pub(crate) struct SearchOperatorDriver {
    table: Arc<TableHandle>,
    snapshot: SearchReadSnapshot,
    cursor: Box<dyn paro_storage::search::SearchCursor>,
    batch_config: SearchBatchConfig,
    budget: ResourceBudget,
    projected_columns: Vec<usize>,
    emit_score: bool,
}

impl std::fmt::Debug for SearchOperatorDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchOperatorDriver")
            .field("snapshot", &self.snapshot)
            .field("batch_config", &self.batch_config)
            .field("budget", &self.budget)
            .field("projected_columns", &self.projected_columns)
            .field("emit_score", &self.emit_score)
            .finish()
    }
}

impl SearchOperatorDriver {
    pub(crate) fn new(
        table: Arc<TableHandle>,
        opened: OpenedSearchCursor,
        batch_config: SearchBatchConfig,
        budget: ResourceBudget,
        projected_columns: Vec<usize>,
        emit_score: bool,
    ) -> Self {
        Self {
            table,
            snapshot: opened.snapshot,
            cursor: opened.cursor,
            batch_config,
            budget,
            projected_columns,
            emit_score,
        }
    }

    pub(crate) fn next_chunk(
        &mut self,
        cancellation: &StatementCancellation,
    ) -> Result<Option<Chunk>> {
        loop {
            cancellation.check()?;
            match self
                .cursor
                .next_batch(&self.batch_config, &mut self.budget)?
            {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => {
                    return self
                        .table
                        .materialize_search_batch(
                            &self.snapshot,
                            batch,
                            &self.projected_columns,
                            self.emit_score,
                        )
                        .map(Some)
                }
                SearchBatchState::Exhausted => return Ok(None),
            }
        }
    }
}
