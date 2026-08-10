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
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};
use paro_storage::index::fulltext::ts_serde::parse_serialized_tsquery;
use paro_storage::search::{
    CapabilityToken, OpenSearchCursorResult, ResourceBudget, SearchBatchConfig, SearchRequestMode,
};
use paro_storage::table::table_handle::TableHandle;
use paro_transaction::TableId;

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
                let opened = open_planned_search_cursor(
                    &spec.capability_token,
                    "vector search",
                    |token| {
                        table.open_vector_search_cursor_with_token_for_view(
                            token,
                            spec.column_id,
                            &spec.query_vector,
                            spec.k,
                            spec.params,
                            spec.predicate.clone(),
                            &ctx.query.transaction,
                        )
                    },
                    || {
                        table
                            .vector_capability(spec.column_id as u32)
                            .map(|capability| capability.capability_token())
                    },
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
                let opened = open_planned_search_cursor(
                    &spec.capability_token,
                    "sparse vector search",
                    |token| {
                        table.open_sparse_vector_search_cursor_with_token_for_view(
                            token,
                            spec.column_id,
                            &spec.query_vector,
                            spec.k,
                            spec.predicate.clone(),
                            &ctx.query.transaction,
                        )
                    },
                    || {
                        table
                            .sparse_capability(spec.column_id as u32)
                            .map(|capability| capability.capability_token())
                    },
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
                    SearchRequestMode::Filter => open_planned_search_cursor(
                        &spec.capability_token,
                        "fulltext filter search",
                        |token| {
                            table.open_fulltext_filter_cursor_with_token_for_view(
                                token,
                                spec.column_id,
                                &parsed,
                                &spec.config,
                                spec.predicate.clone(),
                                &ctx.query.transaction,
                            )
                        },
                        || {
                            table
                                .fulltext_capability(spec.column_id as u32, &spec.config)
                                .map(|capability| capability.capability_token())
                        },
                    )?,
                    SearchRequestMode::TopK { limit } => open_planned_search_cursor(
                        &spec.capability_token,
                        "fulltext top-k search",
                        |token| {
                            table.open_fulltext_search_cursor_with_token_for_view(
                                token,
                                spec.column_id,
                                &parsed,
                                *limit,
                                &spec.config,
                                spec.predicate.clone(),
                                None,
                                spec.score_mode,
                                &ctx.query.transaction,
                            )
                        },
                        || {
                            table
                                .fulltext_capability(spec.column_id as u32, &spec.config)
                                .map(|capability| capability.capability_token())
                        },
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
        paro_storage::search::FullTextQueryKind::SerializedTsQuery => {
            parse_serialized_tsquery(query)
        }
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

fn open_planned_search_cursor<T>(
    planned_token: &CapabilityToken,
    context: &str,
    mut open_with_token: impl FnMut(&CapabilityToken) -> Result<OpenSearchCursorResult<T>>,
    resolve_current_token: impl FnOnce() -> Option<CapabilityToken>,
) -> Result<T> {
    match open_with_token(planned_token)? {
        OpenSearchCursorResult::Opened(opened) => Ok(opened),
        OpenSearchCursorResult::NotQueryable => search_not_queryable_error(context),
        OpenSearchCursorResult::CapabilityTokenStale => {
            let refreshed = resolve_current_token().ok_or_else(|| {
                paro_error::object_not_found(
                    "Search capability",
                    format!("{context} current capability not found"),
                )
            })?;
            match open_with_token(&refreshed)? {
                OpenSearchCursorResult::Opened(opened) => Ok(opened),
                OpenSearchCursorResult::CapabilityTokenStale => Err(paro_error::internal(format!(
                    "{context} refreshed capability token is stale"
                ))),
                OpenSearchCursorResult::NotQueryable => search_not_queryable_error(context),
            }
        }
    }
}

fn search_not_queryable_error<T>(context: &str) -> Result<T> {
    Err(paro_error::object_not_found(
        "Search generation",
        format!("{context} planned capability is not queryable"),
    ))
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
    match driver.next_chunk(ctx.cancel)? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use paro_storage::search::{SearchCapabilityState, SearchNotQueryableReason};

    fn queryable_token(generation_id: u64) -> CapabilityToken {
        CapabilityToken {
            definition_id: 7,
            generation_id,
            root_version: generation_id,
            capability_state: SearchCapabilityState::Queryable,
        }
    }

    #[test]
    fn planned_search_cursor_reresolves_stale_capability_token() {
        let planned = queryable_token(1);
        let refreshed = queryable_token(2);
        let mut opened_generations = Vec::new();

        let opened = open_planned_search_cursor(
            &planned,
            "fulltext top-k search",
            |token| {
                opened_generations.push(token.generation_id);
                if token.generation_id == planned.generation_id {
                    Ok(OpenSearchCursorResult::CapabilityTokenStale)
                } else {
                    Ok(OpenSearchCursorResult::Opened(42))
                }
            },
            || Some(refreshed),
        )
        .expect("stale token should be re-resolved");

        assert_eq!(opened, 42);
        assert_eq!(opened_generations, vec![1, 2]);
    }

    #[test]
    fn planned_search_cursor_reports_missing_capability_after_stale_token() {
        let planned = queryable_token(1);

        let err = open_planned_search_cursor::<i32>(
            &planned,
            "vector search",
            |_token| Ok(OpenSearchCursorResult::CapabilityTokenStale),
            || None,
        )
        .expect_err("missing refreshed capability should fail");

        assert!(
            err.to_string().contains("current capability not found"),
            "{err}"
        );
    }

    #[test]
    fn planned_search_cursor_preserves_not_queryable_after_refresh() {
        let planned = queryable_token(1);
        let mut refreshed = queryable_token(2);
        refreshed.capability_state = SearchCapabilityState::NotQueryable {
            reason: SearchNotQueryableReason::TailOverBudget,
        };

        let err = open_planned_search_cursor::<i32>(
            &planned,
            "sparse vector search",
            |token| {
                if token.generation_id == planned.generation_id {
                    Ok(OpenSearchCursorResult::CapabilityTokenStale)
                } else {
                    Ok(OpenSearchCursorResult::NotQueryable)
                }
            },
            || Some(refreshed),
        )
        .expect_err("not queryable refreshed token should fail");

        assert!(err.to_string().contains("not queryable"), "{err}");
    }
}
