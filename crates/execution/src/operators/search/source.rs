// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
use paro_storage::index::fulltext::text_index::GlobalFullTextStats;
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};
use paro_storage::search::{ResourceBudget, SearchBatchConfig, SearchRequestMode};
use paro_storage::table::table_handle::TableHandle;
use paro_transaction::TableId;

use crate::execution_context::ExecutionContext;
use crate::operators::search::driver::SearchOperatorDriver;
use crate::physical::specs::{
    FullTextSearchSpec, SearchSourceSpec, SparseVectorSearchSpec, VectorSearchSpec,
};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SearchSourceGlobal, SearchSourceLocal, SourceGlobal, SourceLocal};

#[derive(Clone, Copy)]
pub(crate) enum SearchSourceSpecRef<'a> {
    Vector(&'a VectorSearchSpec),
    Sparse(&'a SparseVectorSearchSpec),
    FullText(&'a FullTextSearchSpec),
}

impl<'a> From<&'a SearchSourceSpec> for SearchSourceSpecRef<'a> {
    fn from(spec: &'a SearchSourceSpec) -> Self {
        match spec {
            SearchSourceSpec::Vector(spec) => Self::Vector(spec),
            SearchSourceSpec::Sparse(spec) => Self::Sparse(spec),
            SearchSourceSpec::FullText(spec) => Self::FullText(spec),
        }
    }
}

pub(crate) fn create_search_global(
    _ctx: &mut PipelineInitContext,
    spec: SearchSourceSpecRef<'_>,
) -> Result<SourceGlobal> {
    Ok(SourceGlobal::Search(Arc::new(SearchSourceGlobal {
        output_types: search_output_types(spec).into(),
    })))
}

pub(crate) fn create_search_local(
    ctx: &mut PipelineInitContext,
    global: &SourceGlobal,
    spec: SearchSourceSpecRef<'_>,
) -> Result<SourceLocal> {
    search_global(global)?;
    Ok(SourceLocal::Search(SearchSourceLocal {
        driver: Some(create_search_driver(ctx, spec)?),
    }))
}

fn search_output_types(spec: SearchSourceSpecRef<'_>) -> &[LogicalType] {
    match spec {
        SearchSourceSpecRef::Vector(spec) => &spec.output_types,
        SearchSourceSpecRef::Sparse(spec) => &spec.output_types,
        SearchSourceSpecRef::FullText(spec) => &spec.output_types,
    }
}

fn create_search_driver(
    ctx: &mut PipelineInitContext,
    spec: SearchSourceSpecRef<'_>,
) -> Result<SearchOperatorDriver> {
    let (table, opened, row_limit_hint, heap_budget_items, projected_columns, emit_score) =
        match spec {
            SearchSourceSpecRef::Vector(spec) => {
                let table = search_storage_table(&spec.table, "vector search")?;
                record_search_table_read(ctx, &table);
                let opened = table.open_vector_search_cursor_for_view(
                    spec.column_id,
                    &spec.query_vector,
                    spec.k,
                    spec.params,
                    spec.predicate.clone(),
                    &ctx.query.transaction,
                )?;
                (
                    table,
                    opened,
                    spec.k,
                    spec.k,
                    spec.projected_columns.to_vec(),
                    spec.emit_score,
                )
            }
            SearchSourceSpecRef::Sparse(spec) => {
                let table = search_storage_table(&spec.table, "sparse vector search")?;
                record_search_table_read(ctx, &table);
                let opened = table.open_sparse_vector_search_cursor_for_view(
                    spec.column_id,
                    &spec.query_vector,
                    spec.k,
                    spec.predicate.clone(),
                    &ctx.query.transaction,
                )?;
                (
                    table,
                    opened,
                    spec.k,
                    spec.k,
                    spec.projected_columns.to_vec(),
                    spec.emit_score,
                )
            }
            SearchSourceSpecRef::FullText(spec) => {
                let table = search_storage_table(&spec.table, "fulltext search")?;
                record_search_table_read(ctx, &table);
                let parsed = parse_fulltext_query(spec)?;
                let opened = match &spec.mode {
                    SearchRequestMode::Filter => table.open_fulltext_filter_cursor_for_view(
                        spec.column_id,
                        &parsed,
                        &spec.config,
                        spec.predicate.clone(),
                        &ctx.query.transaction,
                    )?,
                    SearchRequestMode::TopK { limit } => table
                        .open_fulltext_search_cursor_for_view(
                            spec.column_id,
                            &parsed,
                            *limit,
                            &spec.config,
                            spec.predicate.clone(),
                            collect_fulltext_stats(&table, spec),
                            spec.score_mode,
                            &ctx.query.transaction,
                        )?,
                };
                let row_limit_hint = match &spec.mode {
                    SearchRequestMode::Filter => 1024,
                    SearchRequestMode::TopK { limit } => *limit,
                };
                (
                    table,
                    opened,
                    row_limit_hint,
                    row_limit_hint,
                    spec.projected_columns.to_vec(),
                    spec.emit_score,
                )
            }
        };
    let batch_config = SearchBatchConfig {
        row_limit: row_limit_hint.max(1).min(1024),
        preferred_bytes: 1 << 20,
    };
    let budget = ResourceBudget {
        memory_limit_bytes: ctx.query.session.limits.max_memory.max(1),
        heap_budget_items: heap_budget_items.max(1),
        parallelism_slots: ctx.query.session.number_of_threads().max(1),
        cpu_step_budget: None,
        context: None,
    };
    Ok(SearchOperatorDriver::new(
        table,
        opened,
        batch_config,
        budget,
        projected_columns,
        emit_score,
    ))
}

fn search_storage_table(
    table: &Arc<paro_catalog::entry::TableCatalogEntry>,
    label: &str,
) -> Result<Arc<TableHandle>> {
    table.get_storage().cloned().ok_or_else(|| {
        paro_error::internal(format!(
            "table {} has no storage handle for {label}",
            table.base.base.name
        ))
    })
}

fn record_search_table_read(ctx: &PipelineInitContext, table: &TableHandle) {
    ctx.query
        .transaction
        .read_tracker()
        .record_table_read(TableId::new(table.table_id()));
}

const FULLTEXT_MIN_TOKEN_LEN: usize = 1;
const FULLTEXT_MAX_TOKEN_LEN: Option<usize> = None;

fn parse_fulltext_query(spec: &FullTextSearchSpec) -> Result<ParsedQuery> {
    let (_kind, tokenizer) = tokenizer_from_config(&spec.config)?;
    parse_fulltext_query_with_tokenizer(spec, tokenizer.as_ref())
}

fn parse_fulltext_query_with_tokenizer(
    spec: &FullTextSearchSpec,
    tokenizer: &dyn Tokenizer,
) -> Result<ParsedQuery> {
    let query = spec.query.as_str();
    match spec.query_kind {
        paro_storage::search::FullTextQueryKind::Legacy => parse_query(
            query,
            tokenizer,
            FULLTEXT_MIN_TOKEN_LEN,
            FULLTEXT_MAX_TOKEN_LEN,
        ),
        paro_storage::search::FullTextQueryKind::TsQuery => parse_to_tsquery(
            query,
            tokenizer,
            FULLTEXT_MIN_TOKEN_LEN,
            FULLTEXT_MAX_TOKEN_LEN,
        ),
        paro_storage::search::FullTextQueryKind::Plain => parse_plainto_tsquery(
            query,
            tokenizer,
            FULLTEXT_MIN_TOKEN_LEN,
            FULLTEXT_MAX_TOKEN_LEN,
        ),
        paro_storage::search::FullTextQueryKind::Phrase => parse_phraseto_tsquery(
            query,
            tokenizer,
            FULLTEXT_MIN_TOKEN_LEN,
            FULLTEXT_MAX_TOKEN_LEN,
        ),
        paro_storage::search::FullTextQueryKind::WebSearch => parse_websearch_to_tsquery(
            query,
            tokenizer,
            FULLTEXT_MIN_TOKEN_LEN,
            FULLTEXT_MAX_TOKEN_LEN,
        ),
    }
}

fn collect_fulltext_stats(
    table: &Arc<TableHandle>,
    spec: &FullTextSearchSpec,
) -> Option<GlobalFullTextStats> {
    table
        .fulltext_capability(spec.column_id as u32, &spec.config)?
        .generation_stats
        .fulltext_global_stats()
}

pub(crate) fn poll_search_next(
    ctx: &mut OperatorCallContext,
    global: &SourceGlobal,
    local: &mut SourceLocal,
    output: &mut Chunk,
) -> Result<SourcePoll> {
    let global = search_global(global)?;
    let local = search_local(local)?;
    let driver = local
        .driver
        .as_mut()
        .ok_or_else(|| paro_error::internal("search source local driver missing"))?;
    let exec_ctx = ExecutionContext::new(ctx.query.session.clone(), ctx.thread, None);
    match driver.next_chunk(&exec_ctx)? {
        Some(next) => {
            *output = next;
            Ok(SourcePoll::Output)
        }
        None => {
            *output = Chunk::try_init_empty(&global.output_types, output.allocator().clone())?;
            Ok(SourcePoll::Finished)
        }
    }
}

#[inline(always)]
fn search_global(global: &SourceGlobal) -> Result<&SearchSourceGlobal> {
    match global {
        SourceGlobal::Search(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal("search source global state mismatch")),
    }
}

#[inline(always)]
fn search_local(local: &mut SourceLocal) -> Result<&mut SearchSourceLocal> {
    match local {
        SourceLocal::Search(state) => Ok(state),
        _ => Err(paro_error::internal("search source local state mismatch")),
    }
}
