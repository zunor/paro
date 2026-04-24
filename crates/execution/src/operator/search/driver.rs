// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_storage::search::{
    OpenedSearchCursor, ResourceBudget, SearchBatchConfig, SearchBatchState, SearchReadSnapshot,
};
use paro_storage::table::table_handle::TableHandle;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::result_type::SourceResultType;

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

    fn next_chunk(&mut self, ctx: &ExecutionContext) -> Result<Option<Chunk>> {
        loop {
            ctx.check_cancelled()?;
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

#[derive(Debug)]
pub(crate) struct SearchOperatorGlobalState {
    driver: Mutex<SearchOperatorDriver>,
}

impl SearchOperatorGlobalState {
    pub(crate) fn new(driver: SearchOperatorDriver) -> Self {
        Self {
            driver: Mutex::new(driver),
        }
    }
}

impl GlobalSourceState for SearchOperatorGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn max_threads(&self) -> usize {
        1
    }
}

#[derive(Debug, Default)]
pub(crate) struct SearchOperatorLocalState;

impl LocalSourceState for SearchOperatorLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub(crate) fn build_search_batch_config(row_limit_hint: usize) -> SearchBatchConfig {
    SearchBatchConfig {
        row_limit: row_limit_hint.max(1).min(1024),
        preferred_bytes: 1 << 20,
    }
}

pub(crate) fn build_search_resource_budget(
    ctx: &ExecutionContext,
    heap_budget_items: usize,
) -> ResourceBudget {
    ResourceBudget {
        memory_limit_bytes: ctx.session.limits.max_memory.max(1),
        heap_budget_items: heap_budget_items.max(1),
        parallelism_slots: ctx.num_threads().max(1),
        cpu_step_budget: None,
        context: None,
    }
}

pub(crate) fn search_get_data(
    ctx: &ExecutionContext,
    chunk: &mut Chunk,
    input: &mut OperatorSourceInput,
    output_types: &[LogicalType],
) -> Result<SourceResultType> {
    let gstate = input
        .global_state
        .as_any()
        .downcast_ref::<SearchOperatorGlobalState>()
        .expect("invalid search source global state");
    let mut driver = gstate.driver.lock().unwrap();
    match driver.next_chunk(ctx)? {
        Some(next_chunk) => {
            *chunk = next_chunk;
            Ok(SourceResultType::HaveMoreOutput)
        }
        None => {
            *chunk = Chunk::try_init_empty(output_types, chunk.allocator().clone())?;
            Ok(SourceResultType::Finished)
        }
    }
}
