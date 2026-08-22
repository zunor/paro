// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
pub use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::tokenizer::{tokenizer_from_config, TokenizerKind};
use crate::index::fulltext::ts_serde::parse_serialized_tsquery;
use crate::index::PredicateTree;
use crate::rowset::SparseVector;
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};
use paro_common::typed_parameters::ParameterSlot;

use super::stats::TableId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchRequestMode {
    TopK { limit: usize },
    Filter,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProjectionSpec {
    pub columns: Vec<ColumnId>,
    pub include_score: bool,
}

/// Dense query vector source retained in an immutable search plan.
///
/// Runtime parameters keep values out of compiled plan images while carrying
/// the fixed vector dimension required to validate each execution.
#[derive(Debug, Clone, PartialEq)]
pub enum DenseVectorQuery {
    Literal(Vec<f32>),
    RuntimeParameter {
        slot: ParameterSlot,
        dimension: usize,
    },
}

impl DenseVectorQuery {
    pub fn dimension(&self) -> usize {
        match self {
            Self::Literal(values) => values.len(),
            Self::RuntimeParameter { dimension, .. } => *dimension,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HnswIntent {
    pub column_id: ColumnId,
    pub query: DenseVectorQuery,
    pub ef: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SparseIntent {
    pub column_id: ColumnId,
    pub query_vector: SparseVector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullTextQueryKind {
    Legacy,
    TsQuery,
    /// Canonical TSQUERY text produced by a SQL tsquery function. Terms have
    /// already passed through the source text-search configuration and must
    /// not be normalized a second time.
    SerializedTsQuery,
    Plain,
    Phrase,
    WebSearch,
}

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

pub fn normalize_fulltext_config(config: &str) -> Option<String> {
    TokenizerKind::from_config(config)
        .ok()
        .map(|kind| kind.config_name().to_string())
}

pub fn build_fulltext_query_stats(
    query_text: &str,
    config: &str,
    query_kind: FullTextQueryKind,
) -> Result<FullTextQueryStats> {
    let normalized_config = normalize_fulltext_config(config).ok_or_else(|| {
        paro_error::invalid_input(format!("unsupported fulltext config: {config}"))
    })?;
    let (_kind, tokenizer) = tokenizer_from_config(&normalized_config)?;
    let parsed = match query_kind {
        FullTextQueryKind::Legacy => parse_query(query_text, tokenizer.as_ref(), 1, None)?,
        FullTextQueryKind::TsQuery => parse_to_tsquery(query_text, tokenizer.as_ref(), 1, None)?,
        FullTextQueryKind::SerializedTsQuery => parse_serialized_tsquery(query_text)?,
        FullTextQueryKind::Plain => parse_plainto_tsquery(query_text, tokenizer.as_ref(), 1, None)?,
        FullTextQueryKind::Phrase => {
            parse_phraseto_tsquery(query_text, tokenizer.as_ref(), 1, None)?
        }
        FullTextQueryKind::WebSearch => {
            parse_websearch_to_tsquery(query_text, tokenizer.as_ref(), 1, None)?
        }
    };
    Ok(analyze_fulltext_query_stats(&parsed))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextIntent {
    pub column_id: ColumnId,
    pub query: String,
    pub query_kind: FullTextQueryKind,
    pub query_stats: FullTextQueryStats,
    pub config: String,
    pub score_mode: FullTextScoreMode,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchIntent {
    Hnsw(HnswIntent),
    Sparse(SparseIntent),
    FullText(FullTextIntent),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FusionStrategy {
    ReciprocalRankFusion {
        window_size: usize,
        rank_constant: usize,
    },
    WeightedBlend {
        weights: Vec<f32>,
    },
}

/// Canonical search query intent shared across optimizer/planner/execution.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedSearchRequest {
    pub table_id: TableId,
    pub mode: SearchRequestMode,
    pub predicate: Option<PredicateTree>,
    pub projections: ProjectionSpec,
    pub intents: Vec<SearchIntent>,
    pub fusion: Option<FusionStrategy>,
}

impl NormalizedSearchRequest {
    pub fn validate(&self) -> Result<()> {
        if self.intents.is_empty() {
            return Err(paro_error::invalid_input(
                "NormalizedSearchRequest requires at least one search intent",
            ));
        }
        if let SearchRequestMode::TopK { limit } = self.mode {
            if limit == 0 {
                return Err(paro_error::invalid_input(
                    "SearchRequestMode::TopK requires limit > 0",
                ));
            }
        }
        if self.fusion.is_some() && self.intents.len() < 2 {
            return Err(paro_error::invalid_input(
                "FusionStrategy requires at least two search intents",
            ));
        }
        if self.fusion.is_some() {
            return Err(paro_error::not_supported(
                "FusionStrategy is modeled in NormalizedSearchRequest but not executable in v1",
            ));
        }
        Ok(())
    }

    pub fn has_multiple_intents(&self) -> bool {
        self.intents.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_fulltext_query_stats, normalize_fulltext_config, FullTextIntent, FullTextQueryKind,
        FullTextQueryStats, FullTextScoreMode, FusionStrategy, NormalizedSearchRequest,
        ProjectionSpec, SearchIntent, SearchRequestMode,
    };

    #[test]
    fn normalized_request_rejects_invalid_shapes() {
        let empty = NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::Filter,
            predicate: None,
            projections: ProjectionSpec::default(),
            intents: Vec::new(),
            fusion: None,
        };
        assert!(empty.validate().is_err());

        let zero_topk = NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::TopK { limit: 0 },
            predicate: None,
            projections: ProjectionSpec::default(),
            intents: vec![SearchIntent::FullText(FullTextIntent {
                column_id: 0,
                query: "graph".to_string(),
                query_kind: FullTextQueryKind::Legacy,
                query_stats: FullTextQueryStats::new(1),
                config: "simple".to_string(),
                score_mode: FullTextScoreMode::Bm25,
            })],
            fusion: None,
        };
        assert!(zero_topk.validate().is_err());

        let fusion_without_multiple_intents = NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::Filter,
            predicate: None,
            projections: ProjectionSpec::default(),
            intents: vec![SearchIntent::FullText(FullTextIntent {
                column_id: 0,
                query: "graph".to_string(),
                query_kind: FullTextQueryKind::Legacy,
                query_stats: FullTextQueryStats::new(1),
                config: "simple".to_string(),
                score_mode: FullTextScoreMode::Bm25,
            })],
            fusion: Some(FusionStrategy::ReciprocalRankFusion {
                window_size: 20,
                rank_constant: 60,
            }),
        };
        assert!(fusion_without_multiple_intents.validate().is_err());

        let fusion_with_multiple_intents = NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::TopK { limit: 10 },
            predicate: None,
            projections: ProjectionSpec::default(),
            intents: vec![
                SearchIntent::FullText(FullTextIntent {
                    column_id: 0,
                    query: "graph".to_string(),
                    query_kind: FullTextQueryKind::Legacy,
                    query_stats: FullTextQueryStats::new(1),
                    config: "simple".to_string(),
                    score_mode: FullTextScoreMode::Bm25,
                }),
                SearchIntent::FullText(FullTextIntent {
                    column_id: 1,
                    query: "vector".to_string(),
                    query_kind: FullTextQueryKind::Legacy,
                    query_stats: FullTextQueryStats::new(1),
                    config: "simple".to_string(),
                    score_mode: FullTextScoreMode::Bm25,
                }),
            ],
            fusion: Some(FusionStrategy::WeightedBlend {
                weights: vec![0.5, 0.5],
            }),
        };
        assert!(fusion_with_multiple_intents.validate().is_err());
    }

    #[test]
    fn normalized_request_accepts_v1_shape_without_fusion() {
        let request = NormalizedSearchRequest {
            table_id: 1,
            mode: SearchRequestMode::TopK { limit: 10 },
            predicate: None,
            projections: ProjectionSpec {
                columns: vec![0, 2],
                include_score: true,
            },
            intents: vec![SearchIntent::FullText(FullTextIntent {
                column_id: 0,
                query: "graph".to_string(),
                query_kind: FullTextQueryKind::Legacy,
                query_stats: FullTextQueryStats::new(1),
                config: "simple".to_string(),
                score_mode: FullTextScoreMode::Bm25,
            })],
            fusion: None,
        };

        assert!(request.validate().is_ok());
        assert!(!request.has_multiple_intents());
    }

    #[test]
    fn fulltext_config_is_normalized_to_registered_tokenizer_name() {
        assert_eq!(
            normalize_fulltext_config(" SIMPLE "),
            Some("simple".to_string())
        );
        assert_eq!(
            normalize_fulltext_config("english"),
            Some("english".to_string())
        );
        assert_eq!(normalize_fulltext_config(""), None);
    }

    #[test]
    fn fulltext_query_stats_are_built_from_shared_parser() {
        let stats =
            build_fulltext_query_stats("\"graph database\"", "simple", FullTextQueryKind::Phrase)
                .unwrap();
        assert_eq!(stats.term_count, 2);
        assert_eq!(stats.effective_query_terms(), 2);
        assert_eq!(stats.not_count, 0);
    }
}
