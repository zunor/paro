// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryDomain, MemoryOwner};
use paro_common::runtime_value::{IntegralDomainPosition, Value};
use paro_common::types::LogicalType;
use paro_function::scalar::cast::array_casts::parse_vector_literal;
use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};
use paro_storage::index::fulltext::ts_serde::parse_serialized_tsquery;
use paro_storage::index::PredicateComparison;
use paro_storage::index::{Predicate, PredicateTree};
use paro_storage::search::{
    CapabilityToken, DenseVectorQuery, OpenSearchCursorResult, ResourceBudget, SearchBatchConfig,
    SearchMemoryAccountant, SearchReadOptions, SearchRequestMode,
};
use paro_storage::table::table_handle::TableHandle;
use paro_transaction::TableId;

use crate::memory_runtime::QueryMemoryPool;
use crate::operators::search::driver::SearchOperatorDriver;
use crate::physical::specs::{
    FullTextSearchSpec, SearchPredicateTemplate, SearchPredicateValue, SearchSourceSpec,
    SparseVectorSearchSpec, VectorSearchSpec,
};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{SearchSourceGlobal, SearchSourceLocal, SourceGlobal, SourceLocal};

impl SearchMemoryAccountant for QueryMemoryPool {
    fn try_reserve(&self, bytes: usize) -> Result<()> {
        MemoryOwner::acquire_capacity(self, MemoryDomain::Host, bytes)?;
        MemoryOwner::record_allocation(
            self,
            MemoryDomain::Host,
            MemoryTag::VectorIndex,
            MemoryAccountingClass::NonRevocable,
            bytes,
        );
        Ok(())
    }

    fn release(&self, bytes: usize) {
        MemoryOwner::release_allocation(
            self,
            MemoryDomain::Host,
            MemoryTag::VectorIndex,
            MemoryAccountingClass::NonRevocable,
            bytes,
        );
        MemoryOwner::release_capacity(self, MemoryDomain::Host, bytes);
    }
}

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
        memory_accountant: Some(ctx.query.memory.clone()),
        memory_tracker: Default::default(),
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
    let bound = template.tree().clone().try_map_values(&mut |value| {
        match value {
            SearchPredicateValue::Bound(value) => Ok(BoundSearchValue::Value(value)),
            SearchPredicateValue::RuntimeParameter { slot, target_type } => {
                let bound = params.value_for_slot(&slot)?;
                if bound.logical_type() == target_type {
                    Ok(BoundSearchValue::Value(bound.clone()))
                } else {
                    match bound.cast(&target_type) {
                        Ok(value) => Ok(BoundSearchValue::Value(value)),
                        Err(error) => match bound.integral_domain_position(&target_type) {
                            Some(IntegralDomainPosition::Below) => Ok(BoundSearchValue::BelowDomain),
                            Some(IntegralDomainPosition::Above) => Ok(BoundSearchValue::AboveDomain),
                            Some(IntegralDomainPosition::Within) | None => {
                                Err(paro_error::type_mismatch(format!(
                                    "search predicate parameter ${} cannot be cast to {target_type}: {error}",
                                    slot.index.index() + 1
                                )))
                            }
                        },
                    }
                }
            }
        }
    })?;
    let bound = bound.try_map_predicates(&mut bind_search_predicate_leaf)?;
    Ok(normalize_search_null_semantics(bound))
}

#[derive(Debug)]
enum BoundSearchValue {
    Value(Value),
    BelowDomain,
    AboveDomain,
}

fn bind_search_predicate_leaf(predicate: Predicate<BoundSearchValue>) -> Result<Predicate<Value>> {
    Ok(match predicate {
        Predicate::Eq { column_id, value } => {
            bind_ordered_comparison(column_id, PredicateComparison::Equal, value)
        }
        Predicate::NotEq { column_id, value } => {
            bind_ordered_comparison(column_id, PredicateComparison::NotEqual, value)
        }
        Predicate::Lt { column_id, value } => {
            bind_ordered_comparison(column_id, PredicateComparison::LessThan, value)
        }
        Predicate::Le { column_id, value } => {
            bind_ordered_comparison(column_id, PredicateComparison::LessThanOrEqual, value)
        }
        Predicate::Gt { column_id, value } => {
            bind_ordered_comparison(column_id, PredicateComparison::GreaterThan, value)
        }
        Predicate::Ge { column_id, value } => {
            bind_ordered_comparison(column_id, PredicateComparison::GreaterThanOrEqual, value)
        }
        Predicate::In { column_id, values } => Predicate::In {
            column_id,
            values: values
                .into_iter()
                .filter_map(|value| match value {
                    BoundSearchValue::Value(value) => Some(value),
                    BoundSearchValue::BelowDomain | BoundSearchValue::AboveDomain => None,
                })
                .collect(),
        },
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => bind_search_range(column_id, lower, upper),
        Predicate::FixedIn { column_id, values } => Predicate::FixedIn { column_id, values },
        Predicate::IsNull { column_id } => Predicate::IsNull { column_id },
        Predicate::IsNotNull { column_id } => Predicate::IsNotNull { column_id },
        Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        } => Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        },
        Predicate::StringPrefixIn {
            column_id,
            prefixes,
        } => Predicate::StringPrefixIn {
            column_id,
            prefixes,
        },
        Predicate::StringLike {
            column_id,
            pattern,
            negated,
        } => Predicate::StringLike {
            column_id,
            pattern,
            negated,
        },
        Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        } => Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        },
    })
}

fn bind_ordered_comparison(
    column_id: u32,
    comparison: PredicateComparison,
    value: BoundSearchValue,
) -> Predicate<Value> {
    match value {
        BoundSearchValue::Value(value) => comparison.with_value(column_id, value),
        BoundSearchValue::BelowDomain => match comparison {
            PredicateComparison::Equal
            | PredicateComparison::LessThan
            | PredicateComparison::LessThanOrEqual => empty_search_predicate(column_id),
            PredicateComparison::NotEqual
            | PredicateComparison::GreaterThan
            | PredicateComparison::GreaterThanOrEqual => Predicate::IsNotNull { column_id },
        },
        BoundSearchValue::AboveDomain => match comparison {
            PredicateComparison::Equal
            | PredicateComparison::GreaterThan
            | PredicateComparison::GreaterThanOrEqual => empty_search_predicate(column_id),
            PredicateComparison::NotEqual
            | PredicateComparison::LessThan
            | PredicateComparison::LessThanOrEqual => Predicate::IsNotNull { column_id },
        },
    }
}

fn bind_search_range(
    column_id: u32,
    lower: BoundSearchValue,
    upper: BoundSearchValue,
) -> Predicate<Value> {
    match (lower, upper) {
        (BoundSearchValue::AboveDomain, _) | (_, BoundSearchValue::BelowDomain) => {
            empty_search_predicate(column_id)
        }
        (BoundSearchValue::BelowDomain, BoundSearchValue::AboveDomain) => {
            Predicate::IsNotNull { column_id }
        }
        (BoundSearchValue::BelowDomain, BoundSearchValue::Value(upper)) => Predicate::Le {
            column_id,
            value: upper,
        },
        (BoundSearchValue::Value(lower), BoundSearchValue::AboveDomain) => Predicate::Ge {
            column_id,
            value: lower,
        },
        (BoundSearchValue::Value(lower), BoundSearchValue::Value(upper)) => Predicate::Range {
            column_id,
            lower,
            upper,
        },
    }
}

fn empty_search_predicate(column_id: u32) -> Predicate<Value> {
    Predicate::In {
        column_id,
        values: Vec::new(),
    }
}

/// WHERE rejects UNKNOWN, so comparisons to a NULL runtime value have an
/// empty exact result. Keep that SQL rule at the one-time binding boundary;
/// storage indexes never need a second parameter-specific predicate AST.
fn normalize_search_null_semantics(tree: PredicateTree) -> PredicateTree {
    match tree {
        PredicateTree::Leaf(predicate) => PredicateTree::Leaf(match predicate {
            Predicate::Eq { column_id, value }
            | Predicate::NotEq { column_id, value }
            | Predicate::Lt { column_id, value }
            | Predicate::Le { column_id, value }
            | Predicate::Gt { column_id, value }
            | Predicate::Ge { column_id, value }
                if value.is_null() =>
            {
                Predicate::In {
                    column_id,
                    values: Vec::new(),
                }
            }
            Predicate::Range {
                column_id,
                lower,
                upper,
            } if lower.is_null() || upper.is_null() => Predicate::In {
                column_id,
                values: Vec::new(),
            },
            Predicate::In { column_id, values } => Predicate::In {
                column_id,
                values: values
                    .into_iter()
                    .filter(|value| !value.is_null())
                    .collect(),
            },
            predicate => predicate,
        }),
        PredicateTree::And(children) => PredicateTree::And(
            children
                .into_iter()
                .map(normalize_search_null_semantics)
                .collect(),
        ),
        PredicateTree::Or(children) => PredicateTree::Or(
            children
                .into_iter()
                .map(normalize_search_null_semantics)
                .collect(),
        ),
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
        let template = SearchPredicateTemplate::parameter_comparison(
            3,
            PredicateComparison::Equal,
            slot,
            LogicalType::Integer,
        );
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
    fn out_of_domain_parameter_comparison_folds_without_cast_error() {
        let slot = ParameterSlot::new(RuntimeParamId::new(0), LogicalType::Integer);
        let bindings = ParameterBindings::new(
            vec![Value::Integer(100_000)],
            vec![LogicalType::Integer],
            ParameterBindingEpoch::new(1),
        )
        .expect("parameter bindings");

        let equal = SearchPredicateTemplate::parameter_comparison(
            3,
            PredicateComparison::Equal,
            slot.clone(),
            LogicalType::SmallInt,
        );
        assert_eq!(
            bind_search_predicate(Some(&equal), &bindings).expect("fold equality"),
            Some(PredicateTree::leaf(Predicate::In {
                column_id: 3,
                values: Vec::new(),
            }))
        );

        let less_than = SearchPredicateTemplate::parameter_comparison(
            3,
            PredicateComparison::LessThan,
            slot,
            LogicalType::SmallInt,
        );
        assert_eq!(
            bind_search_predicate(Some(&less_than), &bindings).expect("fold upper bound"),
            Some(PredicateTree::leaf(Predicate::IsNotNull { column_id: 3 }))
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
