// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingClass;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::cast::array_casts::parse_vector_literal;
use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};
use paro_storage::index::fulltext::ts_serde::parse_serialized_tsquery;
use paro_storage::index::{Predicate, PredicateComparison, PredicateTree};
use paro_storage::search::{
    CapabilityToken, DenseVectorQuery, OpenSearchCursorResult, ResourceBudget, SearchBatchConfig,
    SearchReadOptions, SearchRequestMode,
};
use paro_storage::table::table_handle::TableHandle;
use paro_transaction::TableId;

use crate::operators::search::driver::SearchOperatorDriver;
use crate::physical::specs::{
    FullTextSearchSpec, SearchPredicateTemplate, SearchSourceSpec, SparseVectorSearchSpec,
    VectorSearchSpec,
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
    let read_options = SearchReadOptions::with_page_cache(ctx.query.session.page_cache().clone());
    let (table, opened, row_limit_hint, heap_budget_items, projected_columns, emit_score) =
        match spec {
            SearchSourceSpecRef::Vector(spec) => {
                let table = search_storage_table(&spec.table, "vector search")?;
                record_search_table_read(ctx, &table);
                let query_vector = resolve_dense_query(&spec.query, ctx.query.params.as_ref())?;
                let predicate =
                    bind_search_predicate(spec.predicate.as_ref(), ctx.query.params.as_ref())?;
                let opened = open_planned_search_cursor(
                    &spec.capability_token,
                    "vector search",
                    |token| {
                        table.open_vector_search_cursor_with_token_for_view(
                            token,
                            spec.column_id,
                            query_vector.as_ref(),
                            spec.distance,
                            spec.k,
                            spec.params,
                            predicate.clone(),
                            &ctx.query.transaction,
                            &read_options,
                        )
                    },
                    || {
                        table
                            .vector_capability(spec.column_id as u32, spec.distance)
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
                let predicate =
                    bind_search_predicate(spec.predicate.as_ref(), ctx.query.params.as_ref())?;
                let opened = open_planned_search_cursor(
                    &spec.capability_token,
                    "sparse vector search",
                    |token| {
                        table.open_sparse_vector_search_cursor_with_token_for_view(
                            token,
                            spec.column_id,
                            &spec.query_vector,
                            spec.k,
                            predicate.clone(),
                            &ctx.query.transaction,
                            &read_options,
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
                let predicate =
                    bind_search_predicate(spec.predicate.as_ref(), ctx.query.params.as_ref())?;
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
                                predicate.clone(),
                                &ctx.query.transaction,
                                &read_options,
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
                                predicate.clone(),
                                None,
                                spec.score_mode,
                                &ctx.query.transaction,
                                &read_options,
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

fn bind_search_predicate(
    template: Option<&SearchPredicateTemplate>,
    params: &crate::runtime::ParameterBindings,
) -> Result<Option<PredicateTree>> {
    template
        .map(|template| bind_search_predicate_node(template, params))
        .transpose()
}

fn bind_search_predicate_node(
    template: &SearchPredicateTemplate,
    params: &crate::runtime::ParameterBindings,
) -> Result<PredicateTree> {
    match template {
        SearchPredicateTemplate::Bound(tree) => Ok(tree.clone()),
        SearchPredicateTemplate::ParameterComparison {
            column_id,
            comparison,
            slot,
            target_type,
        } => {
            let bound = params.value_for_slot(slot)?;
            if bound.is_null() {
                return Ok(PredicateTree::leaf(Predicate::In {
                    column_id: *column_id,
                    values: Vec::new(),
                }));
            }
            let value = if &bound.logical_type() == target_type {
                bound.clone()
            } else {
                bound.cast(target_type).map_err(|error| {
                    paro_error::type_mismatch(format!(
                        "search predicate parameter ${} cannot be cast to {target_type}: {error}",
                        slot.index.index() + 1
                    ))
                })?
            };
            Ok(PredicateTree::leaf(scalar_predicate(
                *column_id,
                *comparison,
                value,
            )))
        }
        SearchPredicateTemplate::And(children) => Ok(PredicateTree::And(
            children
                .iter()
                .map(|child| bind_search_predicate_node(child, params))
                .collect::<Result<Vec<_>>>()?,
        )),
        SearchPredicateTemplate::Or(children) => Ok(PredicateTree::Or(
            children
                .iter()
                .map(|child| bind_search_predicate_node(child, params))
                .collect::<Result<Vec<_>>>()?,
        )),
    }
}

fn scalar_predicate(column_id: u32, comparison: PredicateComparison, value: Value) -> Predicate {
    match comparison {
        PredicateComparison::Equal => Predicate::Eq { column_id, value },
        PredicateComparison::NotEqual => Predicate::NotEq { column_id, value },
        PredicateComparison::LessThan => Predicate::Lt { column_id, value },
        PredicateComparison::LessThanOrEqual => Predicate::Le { column_id, value },
        PredicateComparison::GreaterThan => Predicate::Gt { column_id, value },
        PredicateComparison::GreaterThanOrEqual => Predicate::Ge { column_id, value },
    }
}

pub(crate) fn resolve_dense_query<'a>(
    query: &'a DenseVectorQuery,
    params: &crate::runtime::ParameterBindings,
) -> Result<Cow<'a, [f32]>> {
    let DenseVectorQuery::RuntimeParameter { slot, dimension } = query else {
        let DenseVectorQuery::Literal(values) = query else {
            unreachable!("dense vector query variants are exhaustive")
        };
        return Ok(Cow::Borrowed(values));
    };

    let value = params.value_for_slot(slot)?;
    let values = match value {
        Value::Varchar(text) => parse_vector_literal(text)?,
        Value::Array(elements, _, _) | Value::List(elements, _) => elements
            .iter()
            .map(|element| match element {
                Value::Float(value) => Ok(*value),
                Value::Double(value) => Ok(*value as f32),
                Value::TinyInt(value) => Ok(*value as f32),
                Value::SmallInt(value) => Ok(*value as f32),
                Value::Integer(value) => Ok(*value as f32),
                Value::BigInt(value) => Ok(*value as f32),
                other => Err(paro_error::type_mismatch(format!(
                    "dense vector parameter contains {}, expected numeric values",
                    other.logical_type()
                ))),
            })
            .collect::<Result<Vec<_>>>()?,
        other => {
            return Err(paro_error::type_mismatch(format!(
                "dense vector parameter must be VARCHAR or FLOAT array, got {}",
                other.logical_type()
            )))
        }
    };
    if values.len() != *dimension {
        return Err(paro_error::invalid_value(
            format!("VECTOR({dimension})"),
            format!("{} dimensions", values.len()),
        ));
    }
    Ok(Cow::Owned(values))
}

pub(crate) fn search_storage_table(
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

pub(crate) fn open_planned_search_cursor<T>(
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
    let allocator = ctx
        .memory
        .accounted_allocator_for(MemoryTag::BaseTable, MemoryAccountingClass::NonRevocable);
    match driver.next_chunk(ctx.cancel, allocator)? {
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
    use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
    use paro_common::types::LogicalType;
    use paro_storage::search::{SearchCapabilityState, SearchNotQueryableReason};

    use crate::runtime::{ParameterBindingEpoch, ParameterBindings};

    fn queryable_token(generation_id: u64) -> CapabilityToken {
        CapabilityToken {
            definition_id: 7,
            generation_id,
            root_version: generation_id,
            capability_state: SearchCapabilityState::Queryable,
        }
    }

    #[test]
    fn search_predicate_parameter_binds_once_to_the_storage_type() {
        let slot = ParameterSlot::new(RuntimeParamId::new(0), LogicalType::SmallInt);
        let template = SearchPredicateTemplate::ParameterComparison {
            column_id: 3,
            comparison: PredicateComparison::Equal,
            slot,
            target_type: LogicalType::Integer,
        };
        let bindings = ParameterBindings::new(
            vec![Value::SmallInt(667)],
            vec![LogicalType::SmallInt],
            ParameterBindingEpoch::new(1),
        )
        .expect("parameter bindings");

        assert_eq!(
            bind_search_predicate(Some(&template), &bindings).expect("bind predicate"),
            Some(PredicateTree::leaf(Predicate::Eq {
                column_id: 3,
                value: Value::Integer(667),
            }))
        );
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
