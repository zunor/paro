// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical operators for search-path rewrites.

use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
pub use paro_storage::index::fulltext::scoring::FullTextScoreMode;
use paro_storage::index::fulltext::tokenizer::tokenizer_from_config;

use crate::expression::Expression;

use super::Get;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FullTextQueryStats {
    pub term_count: usize,
    pub positive_term_count: usize,
    pub phrase_count: usize,
    pub proximity_count: usize,
    pub prefix_count: usize,
    pub not_count: usize,
    pub or_branch_count: usize,
}

impl FullTextQueryStats {
    pub fn new(term_count: usize) -> Self {
        Self {
            term_count,
            positive_term_count: term_count,
            ..Self::default()
        }
    }

    pub fn effective_query_terms(&self) -> usize {
        self.positive_term_count.max(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullTextQueryStatsKind {
    Legacy,
    TsQuery,
    Plain,
    Phrase,
    WebSearch,
}

pub fn analyze_fulltext_query_stats(query: &ParsedQuery) -> FullTextQueryStats {
    fn walk(query: &ParsedQuery, negated: bool, stats: &mut FullTextQueryStats) {
        match query {
            ParsedQuery::Term(_) => {
                stats.term_count += 1;
                if negated {
                    stats.not_count += 1;
                } else {
                    stats.positive_term_count += 1;
                }
            }
            ParsedQuery::Prefix(_) => {
                stats.term_count += 1;
                stats.prefix_count += 1;
                if negated {
                    stats.not_count += 1;
                } else {
                    stats.positive_term_count += 1;
                }
            }
            ParsedQuery::Phrase(items) => {
                stats.phrase_count += 1;
                stats.term_count += items.len();
                if negated {
                    stats.not_count += items.len();
                } else {
                    stats.positive_term_count += items.len();
                }
            }
            ParsedQuery::FollowedBy(items, _) => {
                stats.proximity_count += 1;
                for item in items {
                    walk(item, negated, stats);
                }
            }
            ParsedQuery::Not(child) => walk(child, !negated, stats),
            ParsedQuery::And(items) => {
                for item in items {
                    walk(item, negated, stats);
                }
            }
            ParsedQuery::Or(items) => {
                stats.or_branch_count += items.len();
                for item in items {
                    walk(item, negated, stats);
                }
            }
        }
    }

    let mut stats = FullTextQueryStats::default();
    walk(query, false, &mut stats);
    if stats.term_count == 0 {
        stats.term_count = stats.positive_term_count;
    }
    if stats.positive_term_count == 0 && stats.term_count > 0 {
        stats.positive_term_count = 1;
    }
    stats
}

pub fn build_fulltext_query_stats(
    query_text: &str,
    config: &str,
    query_kind: FullTextQueryStatsKind,
) -> Result<FullTextQueryStats> {
    let (_kind, tokenizer) = tokenizer_from_config(config)?;
    let parsed = match query_kind {
        FullTextQueryStatsKind::Legacy => parse_query(query_text, tokenizer.as_ref(), 1, None)?,
        FullTextQueryStatsKind::TsQuery => {
            parse_to_tsquery(query_text, tokenizer.as_ref(), 1, None)?
        }
        FullTextQueryStatsKind::Plain => {
            parse_plainto_tsquery(query_text, tokenizer.as_ref(), 1, None)?
        }
        FullTextQueryStatsKind::Phrase => {
            parse_phraseto_tsquery(query_text, tokenizer.as_ref(), 1, None)?
        }
        FullTextQueryStatsKind::WebSearch => {
            parse_websearch_to_tsquery(query_text, tokenizer.as_ref(), 1, None)?
        }
    };
    Ok(analyze_fulltext_query_stats(&parsed))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchType {
    HnswVector {
        column_id: u32,
    },
    SparseVector {
        column_id: u32,
    },
    FullTextTopK {
        column_id: u32,
        score_mode: FullTextScoreMode,
        query_stats: FullTextQueryStats,
    },
    FullTextFilter {
        column_id: u32,
        query_stats: FullTextQueryStats,
    },
}

impl SearchType {
    pub fn fulltext_topk(
        column_id: u32,
        score_mode: FullTextScoreMode,
        query_stats: FullTextQueryStats,
    ) -> Self {
        Self::FullTextTopK {
            column_id,
            score_mode,
            query_stats,
        }
    }

    pub fn fulltext_filter(column_id: u32, query_stats: FullTextQueryStats) -> Self {
        Self::FullTextFilter {
            column_id,
            query_stats,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchCandidate {
    pub search_type: SearchType,
    pub estimated_cost: f64,
    pub threshold: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchDecision {
    IndexScan {
        search_type: SearchType,
        estimated_cost: f64,
        confidence: Confidence,
    },
    DeferToRuntime {
        candidates: Vec<SearchCandidate>,
        sequential_cost: f64,
    },
}

#[derive(Debug, Clone)]
pub struct SearchScan {
    pub get: Get,
    pub decision: SearchDecision,
    pub projections: Vec<Expression>,
    /// Preserves the absorbed Projection's aliases for EXPLAIN and execution.
    pub output_names: Vec<String>,
    pub projection_table_index: usize,
    pub absorbed_predicates: Vec<Expression>,
    pub residual_predicates: Vec<Expression>,
    /// Index of the score expression inside `projections`.
    pub score_projection_index: usize,
    pub score_expression: Expression,
    pub order_ascending: bool,
    pub limit: usize,
}

impl SearchScan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        get: Get,
        decision: SearchDecision,
        projections: Vec<Expression>,
        projection_table_index: usize,
        absorbed_predicates: Vec<Expression>,
        residual_predicates: Vec<Expression>,
        score_projection_index: usize,
        score_expression: Expression,
        order_ascending: bool,
        limit: usize,
    ) -> Self {
        let output_names = (0..projections.len())
            .map(|idx| format!("expr_{}", idx + 1))
            .collect();
        Self {
            get,
            decision,
            projections,
            output_names,
            projection_table_index,
            absorbed_predicates,
            residual_predicates,
            score_projection_index,
            score_expression,
            order_ascending,
            limit,
        }
    }

    pub fn with_output_names(mut self, output_names: Vec<String>) -> Self {
        self.output_names = output_names;
        self
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.projections
            .iter()
            .map(|expr| expr.return_type())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct FullTextFilterScan {
    pub get: Get,
    pub match_expression: Expression,
    pub other_predicates: Vec<Expression>,
    pub residual_predicates: Vec<Expression>,
    pub decision: SearchDecision,
}

impl FullTextFilterScan {
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.get.returned_types.clone()
    }
}
