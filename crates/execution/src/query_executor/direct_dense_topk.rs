// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Direct execution for compiled, unfiltered dense-vector Top-K queries.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_context::StatementContext;
use paro_storage::search::{ResourceBudget, SearchBatchConfig, SearchBatchState};
use paro_transaction::TableId;

use crate::operators::search::source::{
    open_planned_search_cursor, resolve_dense_query, search_storage_table,
};
use crate::query_executor::compiled::DirectDenseTopKExecutable;
use crate::runtime::ParameterBindings;

pub(crate) fn execute(
    session: &Arc<StatementContext>,
    executable: &DirectDenseTopKExecutable,
    params: &ParameterBindings,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    session.cancellation.check()?;
    let spec = &executable.spec;
    let table = search_storage_table(&spec.table, "direct dense Top-K")?;
    session
        .transaction_view()
        .read_tracker()
        .record_table_read(TableId::new(table.table_id()));
    let query = resolve_dense_query(&spec.query, params)?;
    let mut opened = open_planned_search_cursor(
        &spec.capability_token,
        "direct dense Top-K",
        |token| {
            table.open_vector_search_cursor_with_token_for_view(
                token,
                spec.column_id,
                query.as_ref(),
                spec.k,
                spec.params,
                None,
                session.transaction_view(),
            )
        },
        || {
            table
                .vector_capability(spec.column_id as u32)
                .map(|capability| capability.capability_token())
        },
    )?;

    let batch_config = SearchBatchConfig {
        row_limit: spec.k.max(1),
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget {
        memory_limit_bytes: session.limits.max_memory.max(1),
        heap_budget_items: spec.k.max(1),
        parallelism_slots: session.number_of_threads().max(1),
        cpu_step_budget: None,
        context: None,
    };
    let source = loop {
        session.cancellation.check()?;
        match opened.cursor.next_batch(&batch_config, &mut budget)? {
            SearchBatchState::Ready(batch) if batch.is_empty() => continue,
            SearchBatchState::Ready(batch) => {
                break table.materialize_search_batch(
                    &opened.snapshot,
                    batch,
                    &spec.projected_columns,
                    false,
                )?;
            }
            SearchBatchState::Exhausted => {
                let types = executable
                    .output_projection
                    .iter()
                    .map(|&index| {
                        spec.output_types.get(index).cloned().ok_or_else(|| {
                            paro_error::internal("direct dense Top-K projection is out of range")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Chunk::try_init_empty(&types, allocator);
            }
        }
    };

    if executable.output_projection.len() == source.column_count()
        && executable
            .output_projection
            .iter()
            .copied()
            .eq(0..source.column_count())
    {
        return Ok(source);
    }

    let output_types = executable
        .output_projection
        .iter()
        .map(|&index| {
            source
                .column(index)
                .map(|column| column.logical_type().clone())
                .ok_or_else(|| {
                    paro_error::internal("direct dense Top-K projection is out of range")
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut output = Chunk::try_init_empty(&output_types, allocator)?;
    output.reference_columns(&source, &executable.output_projection);
    Ok(output)
}
