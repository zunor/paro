// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Names for optimizer passes that can be toggled or reported.

use std::fmt;
use std::str::FromStr;

use paro_common::error::{self as paro_error, ParoError, Result};

/// Types of optimizers that can be enabled or disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptimizerType {
    /// Rewrites expressions (constant folding, simplification).
    ExpressionRewriter,
    /// Pulls filters up through operators.
    FilterPullup,
    /// Pushes filters down towards table scans.
    FilterPushdown,
    /// Pulls up empty results to short-circuit execution.
    EmptyResultPullup,
    /// Pushes filters into CTEs.
    CteFilterPusher,
    /// Optimizes regex patterns to range scans.
    RegexRange,
    /// Optimizes IN clause expressions.
    InClause,
    /// Reorders joins for optimal execution.
    JoinOrder,
    /// Reuses grouped aggregates across redundant detail joins.
    AggregateJoinSubsumption,
    /// Pre-aggregates multiplicative nullable join sides by their join key.
    AggregateJoinPreaggregation,
    /// Derives scalar reductions from finalized alpha-equivalent grouped aggregates.
    AggregatePostReduction,
    /// Reuses a key-preserving detail stream for a correlated partition aggregate.
    CorrelatedPartitionAggregate,
    /// Reuses a filtered detail stream for an uncorrelated scalar aggregate.
    ScalarAggregateWindow,
    /// Eliminates redundant joins introduced by `DelimGet`.
    DelimJoinElimination,
    /// Rewrites UNNEST operations.
    UnnestRewriter,
    /// Removes unused columns from projections.
    UnusedColumns,
    /// Gathers node-level cardinality statistics onto plan nodes.
    StatisticsGathering,
    /// Propagates statistics through the plan.
    StatisticsPropagation,
    /// Eliminates common subexpressions.
    CommonSubexpressions,
    /// Combines common aggregates.
    CommonAggregate,
    /// Analyzes column lifetime for memory optimization.
    ColumnLifetime,
    /// Chooses build and probe sides for hash joins.
    BuildProbeSide,
    /// Pushes LIMIT down through operators.
    LimitPushdown,
    /// Rewrites eligible scan/filter/order patterns into search-aware scans.
    SearchOptimization,
    SegmentPruner,
    /// Converts ORDER BY + LIMIT to TopN.
    TopN,
    /// Eliminates redundant window functions with TopN.
    TopNWindowElimination,
    /// Eliminates duplicate GROUP BY groups.
    DuplicateGroups,
    /// Reorders filter conditions for efficiency.
    ReorderFilter,
    /// Pushes sampling down to table scans.
    SamplingPushdown,
    /// Pushes join filters to table scans.
    JoinFilterPushdown,
    /// Separates hash keys from residual predicates on inner joins.
    MixedJoinPredicate,
    /// Extension-provided optimizers.
    Extension,
    /// Materializes CTEs for reuse.
    MaterializedCte,
    /// Rewrites SUM expressions.
    SumRewriter,
    /// Delays materialization for efficiency.
    LateMaterialization,
    /// Inlines CTEs when beneficial.
    CteInlining,
    /// Eliminates common subplans.
    CommonSubplan,
    /// Eliminates redundant joins.
    JoinElimination,
    /// Eliminates redundant COUNT window functions.
    CountWindowElimination,
    /// Decomposes `GraphMatch` into scan and expand operators.
    GraphMatchDecompose,
    /// Selects the starting vertex for graph pattern traversal.
    GraphStartSelection,
    /// Pushes predicates into graph scan and expand operators.
    GraphPredicatePushdown,
}

impl OptimizerType {
    pub const ALL: [OptimizerType; 43] = [
        OptimizerType::ExpressionRewriter,
        OptimizerType::FilterPullup,
        OptimizerType::FilterPushdown,
        OptimizerType::EmptyResultPullup,
        OptimizerType::CteFilterPusher,
        OptimizerType::RegexRange,
        OptimizerType::InClause,
        OptimizerType::JoinOrder,
        OptimizerType::AggregateJoinSubsumption,
        OptimizerType::AggregateJoinPreaggregation,
        OptimizerType::AggregatePostReduction,
        OptimizerType::CorrelatedPartitionAggregate,
        OptimizerType::ScalarAggregateWindow,
        OptimizerType::DelimJoinElimination,
        OptimizerType::UnnestRewriter,
        OptimizerType::UnusedColumns,
        OptimizerType::StatisticsGathering,
        OptimizerType::StatisticsPropagation,
        OptimizerType::CommonSubexpressions,
        OptimizerType::CommonAggregate,
        OptimizerType::ColumnLifetime,
        OptimizerType::BuildProbeSide,
        OptimizerType::LimitPushdown,
        OptimizerType::SearchOptimization,
        OptimizerType::SegmentPruner,
        OptimizerType::TopN,
        OptimizerType::TopNWindowElimination,
        OptimizerType::DuplicateGroups,
        OptimizerType::ReorderFilter,
        OptimizerType::SamplingPushdown,
        OptimizerType::JoinFilterPushdown,
        OptimizerType::MixedJoinPredicate,
        OptimizerType::Extension,
        OptimizerType::MaterializedCte,
        OptimizerType::SumRewriter,
        OptimizerType::LateMaterialization,
        OptimizerType::CteInlining,
        OptimizerType::CommonSubplan,
        OptimizerType::JoinElimination,
        OptimizerType::CountWindowElimination,
        OptimizerType::GraphMatchDecompose,
        OptimizerType::GraphStartSelection,
        OptimizerType::GraphPredicatePushdown,
    ];

    pub fn all() -> Vec<OptimizerType> {
        Self::ALL.to_vec()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            OptimizerType::ExpressionRewriter => "expression_rewriter",
            OptimizerType::FilterPullup => "filter_pullup",
            OptimizerType::FilterPushdown => "filter_pushdown",
            OptimizerType::EmptyResultPullup => "empty_result_pullup",
            OptimizerType::CteFilterPusher => "cte_filter_pusher",
            OptimizerType::RegexRange => "regex_range",
            OptimizerType::InClause => "in_clause",
            OptimizerType::JoinOrder => "join_order",
            OptimizerType::AggregateJoinSubsumption => "aggregate_join_subsumption",
            OptimizerType::AggregateJoinPreaggregation => "aggregate_join_preaggregation",
            OptimizerType::AggregatePostReduction => "aggregate_post_reduction",
            OptimizerType::CorrelatedPartitionAggregate => "correlated_partition_aggregate",
            OptimizerType::ScalarAggregateWindow => "scalar_aggregate_window",
            OptimizerType::DelimJoinElimination => "delim_join_elimination",
            OptimizerType::UnnestRewriter => "unnest_rewriter",
            OptimizerType::UnusedColumns => "unused_columns",
            OptimizerType::StatisticsGathering => "statistics_gathering",
            OptimizerType::StatisticsPropagation => "statistics_propagation",
            OptimizerType::CommonSubexpressions => "common_subexpressions",
            OptimizerType::CommonAggregate => "common_aggregate",
            OptimizerType::ColumnLifetime => "column_lifetime",
            OptimizerType::BuildProbeSide => "build_probe_side",
            OptimizerType::LimitPushdown => "limit_pushdown",
            OptimizerType::SearchOptimization => "search_optimization",
            OptimizerType::SegmentPruner => "segment_pruner",
            OptimizerType::TopN => "top_n",
            OptimizerType::TopNWindowElimination => "top_n_window_elimination",
            OptimizerType::DuplicateGroups => "duplicate_groups",
            OptimizerType::ReorderFilter => "reorder_filter",
            OptimizerType::SamplingPushdown => "sampling_pushdown",
            OptimizerType::JoinFilterPushdown => "join_filter_pushdown",
            OptimizerType::MixedJoinPredicate => "mixed_join_predicate",
            OptimizerType::Extension => "extension",
            OptimizerType::MaterializedCte => "materialized_cte",
            OptimizerType::SumRewriter => "sum_rewriter",
            OptimizerType::LateMaterialization => "late_materialization",
            OptimizerType::CteInlining => "cte_inlining",
            OptimizerType::CommonSubplan => "common_subplan",
            OptimizerType::JoinElimination => "join_elimination",
            OptimizerType::CountWindowElimination => "count_window_elimination",
            OptimizerType::GraphMatchDecompose => "graph_match_decompose",
            OptimizerType::GraphStartSelection => "graph_start_selection",
            OptimizerType::GraphPredicatePushdown => "graph_predicate_pushdown",
        }
    }
}

impl fmt::Display for OptimizerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OptimizerType {
    type Err = ParoError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "expression_rewriter" => Ok(OptimizerType::ExpressionRewriter),
            "filter_pullup" => Ok(OptimizerType::FilterPullup),
            "filter_pushdown" => Ok(OptimizerType::FilterPushdown),
            "empty_result_pullup" => Ok(OptimizerType::EmptyResultPullup),
            "cte_filter_pusher" => Ok(OptimizerType::CteFilterPusher),
            "regex_range" => Ok(OptimizerType::RegexRange),
            "in_clause" => Ok(OptimizerType::InClause),
            "join_order" => Ok(OptimizerType::JoinOrder),
            "aggregate_join_subsumption" => Ok(OptimizerType::AggregateJoinSubsumption),
            "aggregate_join_preaggregation" => Ok(OptimizerType::AggregateJoinPreaggregation),
            "aggregate_post_reduction" => Ok(OptimizerType::AggregatePostReduction),
            "correlated_partition_aggregate" => Ok(OptimizerType::CorrelatedPartitionAggregate),
            "scalar_aggregate_window" => Ok(OptimizerType::ScalarAggregateWindow),
            "delim_join_elimination" => Ok(OptimizerType::DelimJoinElimination),
            "unnest_rewriter" => Ok(OptimizerType::UnnestRewriter),
            "unused_columns" => Ok(OptimizerType::UnusedColumns),
            "statistics_gathering" => Ok(OptimizerType::StatisticsGathering),
            "statistics_propagation" => Ok(OptimizerType::StatisticsPropagation),
            "common_subexpressions" => Ok(OptimizerType::CommonSubexpressions),
            "common_aggregate" => Ok(OptimizerType::CommonAggregate),
            "column_lifetime" => Ok(OptimizerType::ColumnLifetime),
            "build_probe_side" => Ok(OptimizerType::BuildProbeSide),
            "limit_pushdown" => Ok(OptimizerType::LimitPushdown),
            "search_optimization" => Ok(OptimizerType::SearchOptimization),
            "segment_pruner" => Ok(OptimizerType::SegmentPruner),
            "top_n" => Ok(OptimizerType::TopN),
            "top_n_window_elimination" => Ok(OptimizerType::TopNWindowElimination),
            "duplicate_groups" => Ok(OptimizerType::DuplicateGroups),
            "reorder_filter" => Ok(OptimizerType::ReorderFilter),
            "sampling_pushdown" => Ok(OptimizerType::SamplingPushdown),
            "join_filter_pushdown" => Ok(OptimizerType::JoinFilterPushdown),
            "mixed_join_predicate" => Ok(OptimizerType::MixedJoinPredicate),
            "extension" => Ok(OptimizerType::Extension),
            "materialized_cte" => Ok(OptimizerType::MaterializedCte),
            "sum_rewriter" => Ok(OptimizerType::SumRewriter),
            "late_materialization" => Ok(OptimizerType::LateMaterialization),
            "cte_inlining" => Ok(OptimizerType::CteInlining),
            "common_subplan" => Ok(OptimizerType::CommonSubplan),
            "join_elimination" => Ok(OptimizerType::JoinElimination),
            "count_window_elimination" => Ok(OptimizerType::CountWindowElimination),
            "graph_match_decompose" => Ok(OptimizerType::GraphMatchDecompose),
            "graph_start_selection" => Ok(OptimizerType::GraphStartSelection),
            "graph_predicate_pushdown" => Ok(OptimizerType::GraphPredicatePushdown),
            _ => Err(paro_error::invalid_input(format!(
                "Unknown optimizer type: {s}"
            ))),
        }
    }
}

/// Returns the registered optimizer names.
pub fn list_all_optimizers() -> Vec<&'static str> {
    OptimizerType::ALL
        .iter()
        .copied()
        .map(OptimizerType::as_str)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn optimizer_type_display_uses_registered_name() {
        assert_eq!(
            OptimizerType::ExpressionRewriter.to_string(),
            "expression_rewriter"
        );
        assert_eq!(OptimizerType::FilterPushdown.to_string(), "filter_pushdown");
        assert_eq!(OptimizerType::JoinOrder.to_string(), "join_order");
        assert_eq!(OptimizerType::TopN.to_string(), "top_n");
        assert_eq!(
            OptimizerType::StatisticsGathering.to_string(),
            "statistics_gathering"
        );
        assert_eq!(
            OptimizerType::SearchOptimization.to_string(),
            "search_optimization"
        );
        assert_eq!(
            OptimizerType::DelimJoinElimination.to_string(),
            "delim_join_elimination"
        );
    }

    #[test]
    fn optimizer_type_from_str_is_case_insensitive() {
        assert_eq!(
            OptimizerType::from_str("expression_rewriter").unwrap(),
            OptimizerType::ExpressionRewriter
        );
        assert_eq!(
            OptimizerType::from_str("FILTER_PUSHDOWN").unwrap(),
            OptimizerType::FilterPushdown
        );
        assert_eq!(
            OptimizerType::from_str("Join_Order").unwrap(),
            OptimizerType::JoinOrder
        );
        assert_eq!(
            OptimizerType::from_str("statistics_gathering").unwrap(),
            OptimizerType::StatisticsGathering
        );
        assert_eq!(
            OptimizerType::from_str("SEARCH_OPTIMIZATION").unwrap(),
            OptimizerType::SearchOptimization
        );
    }

    #[test]
    fn optimizer_type_round_trips_recent_renames() {
        for optimizer in [
            OptimizerType::DelimJoinElimination,
            OptimizerType::BuildProbeSide,
            OptimizerType::SegmentPruner,
        ] {
            assert_eq!(
                OptimizerType::from_str(optimizer.as_str()).unwrap(),
                optimizer
            );
        }
    }

    #[test]
    fn list_all_optimizers_matches_all_variants() {
        let listed = list_all_optimizers();
        let listed_set: HashSet<_> = listed.iter().copied().collect();
        let all_set: HashSet<_> = OptimizerType::ALL
            .iter()
            .copied()
            .map(OptimizerType::as_str)
            .collect();

        assert_eq!(listed.len(), OptimizerType::ALL.len());
        assert_eq!(listed_set.len(), OptimizerType::ALL.len());
        assert_eq!(listed_set, all_set);
    }

    #[test]
    fn optimizer_type_rejects_unknown_name() {
        assert!(OptimizerType::from_str("nonexistent_optimizer").is_err());
    }
}
