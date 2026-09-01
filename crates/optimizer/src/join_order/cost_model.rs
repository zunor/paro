// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join-order cost model based on cardinality estimates.

use crate::join::build_probe_side::{
    choose_join_build_side, estimate_hash_build_row_width, estimate_row_payload_width,
    estimate_row_width_from_payload, JoinBuildCandidate, JoinBuildSide,
};
use crate::join_order::cardinality::CardinalityEstimator;
use crate::join_order::query_graph::{JoinEdgeOrientation, JoinPredicateSet};
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::join_order::relation_manager::RelationStats;
use paro_planner::expression::Expression;
use std::sync::Arc;

/// A node in the dynamic programming join plan.
///
#[derive(Debug, Clone)]
pub(crate) struct DPJoinNode {
    /// The set of relations in this node.
    pub set: Arc<JoinRelationSet>,
    /// The selected query-graph edge that connects the left and right children.
    pub predicates: Option<JoinPredicateSet>,
    /// Whether this is a leaf node (single relation).
    pub is_leaf: bool,
    /// The left child set (for non-leaf nodes).
    pub left_set: Arc<JoinRelationSet>,
    /// The right child set (for non-leaf nodes).
    pub right_set: Arc<JoinRelationSet>,
    /// Immutable child alternatives selected for this frontier member.
    pub left_plan: Option<Arc<DPJoinNode>>,
    pub right_plan: Option<Arc<DPJoinNode>>,
    /// Physical build input in the original `left_set`/`right_set`
    /// coordinates. Reconstruction places this input on the executable
    /// join's right side; it must not infer orientation again from an
    /// arbitrary DP pair order.
    pub build_side: JoinBuildSide,
    /// The cost of this join node.
    pub cost: f64,
    /// The estimated cardinality of this node. Keep the fractional estimate
    /// throughout DP enumeration; logical plans quantize it only once when the
    /// chosen tree is reconstructed.
    pub cardinality: f64,
    /// Cardinality used for risk-adjusted work costing.
    pub risk_cardinality: f64,
    /// Schema-dependent bytes emitted by this node.
    ///
    /// This cannot be recovered from `set`: reduction joins retain filtering
    /// relations in the set for graph connectivity while emitting only their
    /// preserved child's columns.
    pub output_payload_width: usize,
    /// Largest retained build payload on this path. This resource dimension
    /// is kept separate from scalar work so grant-sensitive search can retain
    /// a lower-memory tree even when it is not the scalar-cost winner.
    pub peak_build_bytes: u64,
}

impl DPJoinNode {
    /// Create a leaf node (single relation).
    ///
    /// Leaf nodes have cost 0 since they represent base tables.
    pub fn leaf(
        set: Arc<JoinRelationSet>,
        output_payload_width: usize,
        cardinality: f64,
        risk_cardinality: f64,
    ) -> Self {
        Self {
            set: set.clone(),
            predicates: None,
            is_leaf: true,
            left_set: set.clone(),
            right_set: set,
            left_plan: None,
            right_plan: None,
            build_side: JoinBuildSide::Right,
            cost: 0.0,
            cardinality,
            risk_cardinality,
            output_payload_width,
            peak_build_bytes: 0,
        }
    }

    /// Create an intermediate node (join of two relations).
    fn intermediate(
        predicates: Option<JoinPredicateSet>,
        left: &DPJoinNode,
        right: &DPJoinNode,
        estimate: CostedJoin,
    ) -> Self {
        Self {
            set: estimate.combination,
            predicates,
            is_leaf: false,
            left_set: left.set.clone(),
            right_set: right.set.clone(),
            left_plan: Some(Arc::new(left.clone())),
            right_plan: Some(Arc::new(right.clone())),
            build_side: estimate.build_side,
            cost: estimate.breakdown.total(),
            cardinality: estimate.cardinality,
            risk_cardinality: estimate.risk_cardinality,
            output_payload_width: estimate.output_payload_width,
            peak_build_bytes: estimate.peak_build_bytes,
        }
    }
}

/// The CostModel computes the cost of join plans.
///
#[derive(Debug)]
pub(crate) struct CostModel {
    /// Cardinality estimator used to calculate cost.
    pub cardinality_estimator: CardinalityEstimator,
    risk_cardinality_estimator: CardinalityEstimator,
    materialization_cardinality_estimator: CardinalityEstimator,
    relation_materialization_cardinalities: Vec<usize>,
    relation_widths: Vec<usize>,
    relation_control_regions: Vec<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct JoinCostBreakdown {
    build: f64,
    probe: f64,
    match_output: f64,
    children: f64,
}

struct CostedJoin {
    combination: Arc<JoinRelationSet>,
    cardinality: f64,
    risk_cardinality: f64,
    output_payload_width: usize,
    breakdown: JoinCostBreakdown,
    build_side: JoinBuildSide,
    peak_build_bytes: u64,
}

struct JoinCostInputs<'a> {
    left: &'a DPJoinNode,
    right: &'a DPJoinNode,
    predicates: Option<&'a JoinPredicateSet>,
    join_rows: f64,
    output_payload_width: usize,
    left_materialization_rows: f64,
    right_materialization_rows: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct JoinConditionProfile {
    left_payload_width: usize,
    right_payload_width: usize,
    has_join_conditions: bool,
    has_hash_key: bool,
}

/// `ScanStructure` records every accepted hash match as a probe-row ordinal
/// plus a build-row pointer, then copies that identity into its emit buffers.
/// Model those two concrete passes without pretending that a fused probe also
/// copies the referenced column payload.
const HASH_MATCH_ROW_BYTES: usize = 2 * (std::mem::size_of::<u32>() + std::mem::size_of::<usize>());

/// A random hash bucket access transfers one cache line on both publication
/// and lookup. Build-row serialization is accounted separately so wide
/// payloads still affect orientation without making probe work artificially
/// dominate narrow builds.
const HASH_BUCKET_CACHE_LINE_BYTES: usize = 64;

/// A cross product emits dictionary vectors: the repeated probe ordinal is one
/// `u32` per output while the contiguous build range is implicit. This is
/// intentionally cheaper than `HASH_MATCH_ROW_BYTES`, whose scan structure
/// stages both an ordinal and a pointer twice.
const CROSS_PRODUCT_SELECTION_BYTES: usize = std::mem::size_of::<u32>();

/// A general nested-loop comparison advances one probe/build cursor pair for
/// every candidate before copying accepted values into flat output vectors.
const NESTED_LOOP_CURSOR_BYTES: usize = 2 * std::mem::size_of::<usize>();

fn estimated_payload_bytes(rows: f64, width: usize) -> u64 {
    if !rows.is_finite() || rows >= u64::MAX as f64 {
        u64::MAX
    } else {
        (rows.max(0.0) * width as f64).min(u64::MAX as f64) as u64
    }
}

fn estimate_hash_probe_row_width(condition_payload_width: usize) -> usize {
    const HASH_PROBE_RUNTIME_BYTES: usize = std::mem::size_of::<u64>()
        + HASH_BUCKET_CACHE_LINE_BYTES
        + std::mem::size_of::<usize>()
        + std::mem::size_of::<u32>();
    condition_payload_width.saturating_add(HASH_PROBE_RUNTIME_BYTES)
}

/// One candidate physical input expressed in byte-equivalent work units.
///
/// Build work uses the same serialized row-width model as the final
/// build/probe-side optimizer. Probe work reads and hashes only condition
/// values. A probe result is not charged as a materialized row here: adjacent
/// hash probes are fused into one execution pipeline. If that result later
/// becomes a build input, its actual payload is charged by that parent's build
/// work instead.
#[derive(Debug, Clone, Copy)]
struct HashInputEstimate {
    rows: f64,
    projected_payload_width: usize,
    condition_payload_width: usize,
    contains_control_region: bool,
}

impl HashInputEstimate {
    fn serialized_build_work(self) -> f64 {
        self.rows
            * estimate_hash_build_row_width(
                self.projected_payload_width,
                self.condition_payload_width,
            ) as f64
    }

    fn execution_build_work(self) -> f64 {
        self.serialized_build_work() + self.rows * HASH_BUCKET_CACHE_LINE_BYTES as f64
    }

    fn probe_work(self) -> f64 {
        self.rows * estimate_hash_probe_row_width(self.condition_payload_width) as f64
    }
}

impl JoinCostBreakdown {
    fn total(self) -> f64 {
        self.build + self.probe + self.match_output + self.children
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

impl CostModel {
    /// Create a new CostModel.
    pub fn new() -> Self {
        Self {
            cardinality_estimator: CardinalityEstimator::new(),
            risk_cardinality_estimator: CardinalityEstimator::new(),
            materialization_cardinality_estimator: CardinalityEstimator::new(),
            relation_materialization_cardinalities: Vec::new(),
            relation_widths: Vec::new(),
            relation_control_regions: Vec::new(),
        }
    }

    /// Clear query-local estimates.
    pub fn reset(&mut self) {
        self.cardinality_estimator = CardinalityEstimator::new();
        self.risk_cardinality_estimator = CardinalityEstimator::new();
        self.materialization_cardinality_estimator = CardinalityEstimator::new();
        self.relation_materialization_cardinalities.clear();
        self.relation_widths.clear();
        self.relation_control_regions.clear();
    }

    /// Initialize the cost model with relation statistics.
    ///
    /// This should be called after all relations have been added to the
    /// relation manager and before computing any costs.
    pub fn init_cost_model(
        &mut self,
        set_manager: &mut JoinRelationSetManager,
        relation_stats: &[RelationStats],
    ) {
        for (i, stats) in relation_stats.iter().enumerate() {
            let set = set_manager.get_relation(i);
            self.cardinality_estimator
                .init_cardinality_estimator_props(&set, stats);
            let mut risk_stats = stats.clone();
            risk_stats.cardinality = stats.risk_cardinality.max(stats.cardinality);
            self.risk_cardinality_estimator
                .init_cardinality_estimator_props(&set, &risk_stats);
            let mut materialization_stats = stats.clone();
            materialization_stats.cardinality = stats
                .materialization_cardinality
                .max(stats.risk_cardinality)
                .max(stats.cardinality);
            if !stats.materialization_distinct_count.is_empty() {
                materialization_stats.column_distinct_count =
                    stats.materialization_distinct_count.clone();
            }
            self.materialization_cardinality_estimator
                .init_cardinality_estimator_props(&set, &materialization_stats);
        }
        self.relation_widths = relation_stats
            .iter()
            .map(|stats| stats.estimated_payload_width)
            .collect();
        self.relation_materialization_cardinalities = relation_stats
            .iter()
            .map(|stats| {
                stats
                    .materialization_cardinality
                    .max(stats.risk_cardinality)
                    .max(stats.cardinality)
            })
            .collect();
        self.relation_control_regions = relation_stats
            .iter()
            .map(|stats| stats.contains_control_region)
            .collect();
    }

    pub(crate) fn init_equivalent_relations(
        &mut self,
        filters: &[Arc<crate::join_order::query_graph::FilterInfo>],
    ) {
        self.cardinality_estimator
            .init_equivalent_relations(filters);
        self.risk_cardinality_estimator
            .init_equivalent_relations(filters);
        self.materialization_cardinality_estimator
            .init_equivalent_relations(filters);
    }

    /// Compute the cost of joining two nodes.
    ///
    /// The cost is computed as:
    /// cost = build_rows * serialized_hash_build_row_bytes
    ///      + probe_rows * evaluated_probe_key_bytes
    ///      + output_rows * hash_match_identity_bytes
    ///      + cost(left) + cost(right)
    ///
    /// Hash-probe outputs remain in the current pipeline, so charging every
    /// logical join output as materialized work would double-count a fused
    /// probe chain. The serialized payload is charged exactly when a subtree
    /// becomes a build input to a parent join.
    #[cfg(test)]
    fn compute_cost(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        set_manager: &mut JoinRelationSetManager,
        predicates: Option<&JoinPredicateSet>,
    ) -> f64 {
        self.estimate_join(left, right, set_manager, predicates)
            .breakdown
            .total()
    }

    #[cfg(test)]
    fn compute_cost_breakdown(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        set_manager: &mut JoinRelationSetManager,
        predicates: Option<&JoinPredicateSet>,
    ) -> JoinCostBreakdown {
        self.estimate_join(left, right, set_manager, predicates)
            .breakdown
    }

    fn estimate_join(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        set_manager: &mut JoinRelationSetManager,
        predicates: Option<&JoinPredicateSet>,
    ) -> CostedJoin {
        let combination = set_manager.union(&left.set, &right.set);
        let join_rows = self
            .cardinality_estimator
            .estimate_cardinality(&combination);
        let risk_join_rows = self
            .risk_cardinality_estimator
            .estimate_cardinality(&combination)
            .max(join_rows);
        let output_payload_width = Self::output_payload_width(left, right, predicates);
        let left_materialization_rows = self
            .materialization_cardinality(&left.set)
            .max(left.risk_cardinality);
        let right_materialization_rows = self
            .materialization_cardinality(&right.set)
            .max(right.risk_cardinality);
        let (breakdown, build_side) = self.cost_breakdown_for_cardinality(JoinCostInputs {
            left,
            right,
            predicates,
            join_rows: risk_join_rows,
            output_payload_width,
            left_materialization_rows,
            right_materialization_rows,
        });
        CostedJoin {
            combination,
            cardinality: join_rows,
            risk_cardinality: risk_join_rows,
            output_payload_width,
            breakdown,
            build_side,
            peak_build_bytes: left.peak_build_bytes.max(right.peak_build_bytes).max(
                match build_side {
                    JoinBuildSide::Left => estimated_payload_bytes(
                        left_materialization_rows,
                        left.output_payload_width,
                    ),
                    JoinBuildSide::Right => estimated_payload_bytes(
                        right_materialization_rows,
                        right.output_payload_width,
                    ),
                },
            ),
        }
    }

    /// Conservative size of a subtree if it becomes an irreversible build.
    ///
    /// Equality selectivity can rank a join, but without a proof that every
    /// contributing relation is key-preserving it cannot reduce the memory
    /// envelope below the largest atomic input. Future constraint proofs can
    /// tighten this floor explicitly instead of relying on a point estimate.
    fn materialization_cardinality(&mut self, set: &JoinRelationSet) -> f64 {
        let estimated = self
            .materialization_cardinality_estimator
            .estimate_cardinality(set);
        let atomic_floor = set
            .relations()
            .iter()
            .filter_map(|relation| {
                self.relation_materialization_cardinalities
                    .get(*relation)
                    .copied()
            })
            .max()
            .unwrap_or(0) as f64;
        estimated.max(atomic_floor)
    }

    fn cost_breakdown_for_cardinality(
        &self,
        inputs: JoinCostInputs<'_>,
    ) -> (JoinCostBreakdown, JoinBuildSide) {
        let JoinCostInputs {
            left,
            right,
            predicates,
            join_rows,
            output_payload_width,
            left_materialization_rows,
            right_materialization_rows,
        } = inputs;
        let left_rows = left.risk_cardinality;
        let right_rows = right.risk_cardinality;
        let conditions = Self::condition_profile(predicates);
        let filtering_side = Self::reduction_filtering_side(predicates);
        if !conditions.has_hash_key {
            let left_row_width = estimate_row_width_from_payload(left.output_payload_width) as f64;
            let right_row_width =
                estimate_row_width_from_payload(right.output_payload_width) as f64;
            let left_work = left_rows * left_row_width;
            let right_work = right_rows * right_row_width;
            let build_side = choose_join_build_side(
                filtering_side,
                JoinBuildCandidate {
                    serialized_work: left_materialization_rows * left_row_width,
                    contains_control_region: filtering_side == Some(JoinBuildSide::Left)
                        && self.contains_control_region(&left.set),
                },
                JoinBuildCandidate {
                    serialized_work: right_materialization_rows * right_row_width,
                    contains_control_region: filtering_side == Some(JoinBuildSide::Right)
                        && self.contains_control_region(&right.set),
                },
            );
            let build = match build_side {
                JoinBuildSide::Left => left_work,
                JoinBuildSide::Right => right_work,
            };
            if !conditions.has_join_conditions {
                return (
                    JoinCostBreakdown {
                        build,
                        probe: left_rows * right_rows * CROSS_PRODUCT_SELECTION_BYTES as f64,
                        match_output: 0.0,
                        children: left.cost + right.cost,
                    },
                    build_side,
                );
            }
            let pair_width = conditions
                .left_payload_width
                .saturating_add(conditions.right_payload_width)
                .saturating_add(NESTED_LOOP_CURSOR_BYTES);
            return (
                JoinCostBreakdown {
                    build,
                    probe: left_rows * right_rows * pair_width as f64,
                    // General NLJ writes accepted values into flat vectors rather
                    // than returning dictionary references like cross/hash joins.
                    match_output: join_rows
                        * estimate_row_width_from_payload(output_payload_width) as f64,
                    children: left.cost + right.cost,
                },
                build_side,
            );
        }
        let left_input = HashInputEstimate {
            rows: left_rows,
            projected_payload_width: left.output_payload_width,
            condition_payload_width: conditions.left_payload_width,
            contains_control_region: filtering_side == Some(JoinBuildSide::Left)
                && self.contains_control_region(&left.set),
        };
        let right_input = HashInputEstimate {
            rows: right_rows,
            projected_payload_width: right.output_payload_width,
            condition_payload_width: conditions.right_payload_width,
            contains_control_region: filtering_side == Some(JoinBuildSide::Right)
                && self.contains_control_region(&right.set),
        };
        let left_materialization = HashInputEstimate {
            rows: left_materialization_rows,
            ..left_input
        };
        let right_materialization = HashInputEstimate {
            rows: right_materialization_rows,
            ..right_input
        };
        let build_side =
            Self::hash_build_side(left_materialization, right_materialization, filtering_side);
        let (build, probe) = match build_side {
            JoinBuildSide::Left => (left_input, right_input),
            JoinBuildSide::Right => (right_input, left_input),
        };
        (
            JoinCostBreakdown {
                build: build.execution_build_work(),
                probe: probe.probe_work(),
                // Hash comparison joins stage each accepted probe/build identity.
                // Non-hash joins returned through the nested-loop branch above and
                // therefore never pay this hash-match buffer cost.
                match_output: join_rows * HASH_MATCH_ROW_BYTES as f64,
                children: left.cost + right.cost,
            },
            build_side,
        )
    }

    fn reduction_filtering_side(predicates: Option<&JoinPredicateSet>) -> Option<JoinBuildSide> {
        match predicates.and_then(JoinPredicateSet::reduction_orientation) {
            Some(JoinEdgeOrientation::Forward) => Some(JoinBuildSide::Right),
            Some(JoinEdgeOrientation::Inverted) => Some(JoinBuildSide::Left),
            None => None,
        }
    }

    fn output_payload_width(
        left: &DPJoinNode,
        right: &DPJoinNode,
        predicates: Option<&JoinPredicateSet>,
    ) -> usize {
        let Some(orientation) = predicates.and_then(JoinPredicateSet::reduction_orientation) else {
            return left
                .output_payload_width
                .saturating_add(right.output_payload_width);
        };
        match orientation {
            JoinEdgeOrientation::Forward => left.output_payload_width,
            JoinEdgeOrientation::Inverted => right.output_payload_width,
        }
    }

    pub(crate) fn payload_width(&self, set: &JoinRelationSet) -> usize {
        set.relations()
            .iter()
            .map(|relation| self.relation_widths.get(*relation).copied().unwrap_or(0))
            .sum()
    }

    fn contains_control_region(&self, set: &JoinRelationSet) -> bool {
        set.relations().iter().any(|relation| {
            self.relation_control_regions
                .get(*relation)
                .copied()
                .unwrap_or(false)
        })
    }

    /// Estimate the condition values stored or evaluated on each side of a
    /// cut and report whether at least one equality supports a hash table.
    fn condition_profile(predicates: Option<&JoinPredicateSet>) -> JoinConditionProfile {
        let Some(predicates) = predicates else {
            return JoinConditionProfile::default();
        };
        let mut left_width = 0usize;
        let mut right_width = 0usize;
        let mut has_hash_key = false;
        for predicate in predicates.predicates() {
            let Some(orientation) = predicate.orientation() else {
                continue;
            };
            let filter = predicate.filter();
            let mut add_comparison =
                |comparison: &paro_planner::expression::ComparisonExpression| {
                    has_hash_key |= matches!(
                        comparison.comparison_type,
                        paro_planner::expression::ComparisonType::Equal
                            | paro_planner::expression::ComparisonType::NotDistinctFrom
                    );
                    let expression_width = |expression: &Expression| {
                        estimate_row_payload_width(&[expression.return_type()])
                    };
                    let (cut_left, cut_right) = match orientation {
                        JoinEdgeOrientation::Forward => {
                            (comparison.left.as_ref(), comparison.right.as_ref())
                        }
                        JoinEdgeOrientation::Inverted => {
                            (comparison.right.as_ref(), comparison.left.as_ref())
                        }
                    };
                    left_width = left_width.saturating_add(expression_width(cut_left));
                    right_width = right_width.saturating_add(expression_width(cut_right));
                };
            match &filter.filter {
                Expression::Comparison(comparison) => add_comparison(comparison),
                Expression::Conjunction(conjunction) => {
                    for child in &conjunction.children {
                        if let Expression::Comparison(comparison) = child {
                            add_comparison(comparison);
                        }
                    }
                }
                _ => {}
            }
        }
        JoinConditionProfile {
            left_payload_width: left_width,
            right_payload_width: right_width,
            has_join_conditions: predicates.has_join_conditions(),
            has_hash_key,
        }
    }

    /// Apply the same physical orientation policy as `BuildProbeSideOptimizer`.
    /// Comparison joins, including their SEMI/ANTI inverses, retain the cheaper
    /// serialized build side. A filtering control region is the one exception
    /// because moving it to the probe side would require a qualitatively
    /// different materialization.
    fn hash_build_side(
        left: HashInputEstimate,
        right: HashInputEstimate,
        filtering_side: Option<JoinBuildSide>,
    ) -> JoinBuildSide {
        choose_join_build_side(
            filtering_side,
            JoinBuildCandidate {
                serialized_work: left.serialized_build_work(),
                contains_control_region: left.contains_control_region,
            },
            JoinBuildCandidate {
                serialized_work: right.serialized_build_work(),
                contains_control_region: right.contains_control_region,
            },
        )
    }

    /// Compute the cost and create a new DPJoinNode.
    pub fn compute_cost_and_create_node(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        set_manager: &mut JoinRelationSetManager,
        predicates: Option<JoinPredicateSet>,
    ) -> DPJoinNode {
        let estimate = self.estimate_join(left, right, set_manager, predicates.as_ref());

        DPJoinNode::intermediate(predicates, left, right, estimate)
    }

    /// Get the estimated cardinality for a relation set.
    pub fn get_cardinality(&mut self, set: &JoinRelationSet) -> f64 {
        self.cardinality_estimator.estimate_cardinality(set)
    }

    pub fn get_risk_cardinality(&mut self, set: &JoinRelationSet) -> f64 {
        self.risk_cardinality_estimator
            .estimate_cardinality(set)
            .max(self.get_cardinality(set))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::join_order::query_graph::{CutPredicateResolution, FilterInfo, JoinPredicateSet};
    use crate::join_order::relation_manager::DistinctCount;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, OperatorExpression, OperatorType,
    };
    use paro_planner::operator::{AntiJoinMode, ColumnBinding, JoinType};

    fn column_distinct_counts(
        table_index: usize,
        counts: impl IntoIterator<Item = DistinctCount>,
    ) -> HashMap<ColumnBinding, DistinctCount> {
        counts
            .into_iter()
            .enumerate()
            .map(|(column_index, count)| (ColumnBinding::new(table_index, column_index), count))
            .collect()
    }

    fn create_column_ref(
        table_index: usize,
        column_index: usize,
    ) -> paro_planner::expression::Expression {
        paro_planner::expression::Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn create_equality_filter(
        set_manager: &mut JoinRelationSetManager,
        left_table: usize,
        left_col: usize,
        right_table: usize,
        right_col: usize,
        filter_index: usize,
    ) -> Arc<FilterInfo> {
        create_comparison_filter(
            set_manager,
            left_table,
            left_col,
            right_table,
            right_col,
            filter_index,
            ComparisonType::Equal,
        )
    }

    fn create_comparison_filter(
        set_manager: &mut JoinRelationSetManager,
        left_table: usize,
        left_col: usize,
        right_table: usize,
        right_col: usize,
        filter_index: usize,
        comparison_type: ComparisonType,
    ) -> Arc<FilterInfo> {
        let expr = paro_planner::expression::Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(left_table, left_col)),
            right: Box::new(create_column_ref(right_table, right_col)),
            comparison_type,
        });

        let set = set_manager.get_relation_from_vec(vec![left_table, right_table]);
        let left_set = set_manager.get_relation(left_table);
        let right_set = set_manager.get_relation(right_table);

        let mut filter = FilterInfo::new_inner(expr, set, filter_index);
        filter.set_left_set(left_set);
        filter.set_right_set(right_set);
        filter.set_left_binding(ColumnBinding::new(left_table, left_col), left_table);
        filter.set_right_binding(ColumnBinding::new(right_table, right_col), right_table);

        Arc::new(filter)
    }

    fn create_reduction_filter(
        set_manager: &mut JoinRelationSetManager,
        preserved_table: usize,
        preserved_col: usize,
        filtering_table: usize,
        filtering_col: usize,
        filter_index: usize,
        join_type: JoinType,
    ) -> Arc<FilterInfo> {
        assert!(matches!(join_type, JoinType::Semi | JoinType::Anti));
        let expr = paro_planner::expression::Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(preserved_table, preserved_col)),
            right: Box::new(create_column_ref(filtering_table, filtering_col)),
            comparison_type: ComparisonType::Equal,
        });
        let set = set_manager.get_relation_from_vec(vec![preserved_table, filtering_table]);
        let preserved_set = set_manager.get_relation(preserved_table);
        let filtering_set = set_manager.get_relation(filtering_table);
        let mut filter = FilterInfo::new(expr, set, filter_index, join_type, AntiJoinMode::Regular);
        filter.set_left_set(preserved_set);
        filter.set_right_set(filtering_set);
        filter.set_left_binding(
            ColumnBinding::new(preserved_table, preserved_col),
            preserved_table,
        );
        filter.set_right_binding(
            ColumnBinding::new(filtering_table, filtering_col),
            filtering_table,
        );
        Arc::new(filter)
    }

    fn predicate_set(filters: &[Arc<FilterInfo>]) -> JoinPredicateSet {
        let filter = filters.first().expect("non-empty join predicate set");
        let crate::join_order::query_graph::CutPredicateResolution::Resolved(Some(predicates)) =
            JoinPredicateSet::from_filters(
                filters.iter(),
                filter.left_set().map_or(filter.set.as_ref(), Arc::as_ref),
                filter.right_set().map_or(filter.set.as_ref(), Arc::as_ref),
            )
        else {
            panic!("expected a valid non-empty join predicate set")
        };
        predicates
    }

    fn leaf(model: &mut CostModel, set: Arc<JoinRelationSet>) -> DPJoinNode {
        let cardinality = model.get_cardinality(set.as_ref());
        let risk_cardinality = model.get_risk_cardinality(set.as_ref());
        DPJoinNode::leaf(
            set.clone(),
            model.payload_width(set.as_ref()),
            cardinality,
            risk_cardinality,
        )
    }

    #[test]
    fn test_cost_model_new() {
        let mut cost_model = CostModel::new();
        // Just verify it can be created
        assert_eq!(cost_model.get_cardinality(&JoinRelationSet::empty()), 1.0);
    }

    #[test]
    fn test_dp_join_node_leaf() {
        let mut set_manager = JoinRelationSetManager::new();
        let set = set_manager.get_relation(0);

        let node = DPJoinNode::leaf(set.clone(), 7, 0.0, 0.0);

        assert!(node.is_leaf);
        assert_eq!(node.cost, 0.0);
        assert_eq!(node.cardinality, 0.0);
        assert_eq!(node.output_payload_width, 7);
        assert!(Arc::ptr_eq(&node.set, &set));
    }

    #[test]
    fn test_dp_join_node_intermediate() {
        let mut set_manager = JoinRelationSetManager::new();
        let left = set_manager.get_relation(0);
        let right = set_manager.get_relation(1);
        let combined = set_manager.union(&left, &right);

        let node = DPJoinNode::intermediate(
            None,
            &DPJoinNode::leaf(left.clone(), 5, 10.0, 10.0),
            &DPJoinNode::leaf(right.clone(), 6, 5.0, 5.0),
            CostedJoin {
                combination: combined.clone(),
                cardinality: 50.0,
                risk_cardinality: 50.0,
                output_payload_width: 11,
                breakdown: JoinCostBreakdown {
                    build: 25.0,
                    probe: 25.0,
                    match_output: 25.0,
                    children: 25.0,
                },
                build_side: JoinBuildSide::Right,
                peak_build_bytes: 30,
            },
        );

        assert!(!node.is_leaf);
        assert!(node.predicates.is_none());
        assert_eq!(node.cost, 100.0);
        assert_eq!(node.cardinality, 50.0);
        assert_eq!(node.output_payload_width, 11);
        assert!(Arc::ptr_eq(&node.left_set, &left));
        assert!(Arc::ptr_eq(&node.right_set, &right));
    }

    #[test]
    fn test_init_cost_model() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        let stats = vec![
            RelationStats::with_cardinality(1000),
            RelationStats::with_cardinality(500),
        ];

        cost_model.init_cost_model(&mut set_manager, &stats);

        // Check that cardinalities were initialized
        let set0 = set_manager.get_relation(0);
        let card0 = cost_model.get_cardinality(&set0);
        assert_eq!(card0, 1000.0);

        let set1 = set_manager.get_relation(1);
        let card1 = cost_model.get_cardinality(&set1);
        assert_eq!(card1, 500.0);
    }

    #[test]
    fn leaf_risk_cardinality_does_not_replace_its_expected_estimate() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();
        let mut stats = RelationStats::with_cardinality(100);
        stats.risk_cardinality = 500;
        cost_model.init_cost_model(&mut set_manager, &[stats]);

        let set = set_manager.get_relation(0);
        assert_eq!(cost_model.get_cardinality(&set), 100.0);
        assert_eq!(cost_model.get_risk_cardinality(&set), 500.0);
    }

    #[test]
    fn hash_orientation_uses_materialization_risk_without_repricing_cpu_work() {
        let mut sets = JoinRelationSetManager::new();
        let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
        let predicates = predicate_set(std::slice::from_ref(&filter));
        let mut left_stats = RelationStats::with_cardinality(10);
        left_stats.risk_cardinality = 10;
        left_stats.materialization_cardinality = 10_000;
        left_stats.estimated_payload_width = 8;
        left_stats.column_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(10_000, true)]);
        let mut right_stats = RelationStats::with_cardinality(100);
        right_stats.risk_cardinality = 100;
        right_stats.materialization_cardinality = 100;
        right_stats.estimated_payload_width = 8;
        right_stats.column_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(10_000, true)]);

        let mut model = CostModel::new();
        model.init_equivalent_relations(std::slice::from_ref(&filter));
        model.init_cost_model(&mut sets, &[left_stats, right_stats]);
        let combined = sets.get_relation_from_vec(vec![0, 1]);
        assert_eq!(model.materialization_cardinality(&combined), 10_000.0);
        let left = leaf(&mut model, sets.get_relation(0));
        let right = leaf(&mut model, sets.get_relation(1));
        let node = model.compute_cost_and_create_node(&left, &right, &mut sets, Some(predicates));

        assert_eq!(node.build_side, JoinBuildSide::Right);
        let conditions = CostModel::condition_profile(node.predicates.as_ref());
        let expected_right_build = HashInputEstimate {
            rows: right.risk_cardinality,
            projected_payload_width: right.output_payload_width,
            condition_payload_width: conditions.right_payload_width,
            contains_control_region: false,
        }
        .execution_build_work();
        let breakdown =
            model.compute_cost_breakdown(&left, &right, &mut sets, node.predicates.as_ref());
        assert_eq!(breakdown.build, expected_right_build);
    }

    #[test]
    fn materialization_rows_keep_their_own_distinct_domains() {
        let mut sets = JoinRelationSetManager::new();
        let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
        let mut left_stats = RelationStats::with_cardinality(10);
        left_stats.materialization_cardinality = 1_000_000;
        left_stats.column_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(10, false)]);
        left_stats.materialization_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(1_000_000, false)]);
        let mut right_stats = RelationStats::with_cardinality(10);
        right_stats.materialization_cardinality = 1_000_000;
        right_stats.column_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(10, false)]);
        right_stats.materialization_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(1_000_000, false)]);

        let mut model = CostModel::new();
        model.init_equivalent_relations(std::slice::from_ref(&filter));
        model.init_cost_model(&mut sets, &[left_stats, right_stats]);

        let joined = sets.get_relation_from_vec(vec![0, 1]);
        assert_eq!(model.materialization_cardinality(&joined), 1_000_000.0);
    }

    #[test]
    fn test_compute_cost_leaf_nodes() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Initialize with join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model.init_equivalent_relations(&[filter]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        // Create leaf nodes
        let left = leaf(&mut cost_model, set_manager.get_relation(0));
        let right = leaf(&mut cost_model, set_manager.get_relation(1));

        // Compute cost
        let cost = cost_model.compute_cost(&left, &right, &mut set_manager, None);

        // Cost should be join cardinality + 0 + 0 (leaf costs are 0)
        // Join cardinality = (1000 * 500) / max(100, 50) = 5000
        assert!(cost > 0.0);
    }

    #[test]
    fn test_compute_cost_with_existing_costs() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Initialize with join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model.init_equivalent_relations(&[filter]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        // Create nodes with existing costs
        let left_set = set_manager.get_relation(0);
        let right_set = set_manager.get_relation(1);

        let left = DPJoinNode {
            set: left_set.clone(),
            predicates: None,
            is_leaf: false,
            left_set: left_set.clone(),
            right_set: left_set.clone(),
            left_plan: None,
            right_plan: None,
            build_side: JoinBuildSide::Right,
            cost: 100.0,
            cardinality: 1000.0,
            risk_cardinality: 1000.0,
            output_payload_width: cost_model.payload_width(&left_set),
            peak_build_bytes: 0,
        };

        let right = DPJoinNode {
            set: right_set.clone(),
            predicates: None,
            is_leaf: false,
            left_set: right_set.clone(),
            right_set: right_set.clone(),
            left_plan: None,
            right_plan: None,
            build_side: JoinBuildSide::Right,
            cost: 50.0,
            cardinality: 500.0,
            risk_cardinality: 500.0,
            output_payload_width: cost_model.payload_width(&right_set),
            peak_build_bytes: 0,
        };

        // Compute cost
        let cost = cost_model.compute_cost(&left, &right, &mut set_manager, None);

        // Cost should include left.cost + right.cost
        assert!(cost >= 150.0); // At least the sum of child costs
    }

    #[test]
    fn test_compute_cost_and_create_node() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Initialize with join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        cost_model.init_equivalent_relations(&[filter]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1]);

        // Create leaf nodes
        let left = leaf(&mut cost_model, set_manager.get_relation(0));
        let right = leaf(&mut cost_model, set_manager.get_relation(1));

        // Create join node
        let join_node =
            cost_model.compute_cost_and_create_node(&left, &right, &mut set_manager, None);

        assert!(!join_node.is_leaf);
        assert!(join_node.cost > 0.0);
        assert!(join_node.cardinality > 0.0);
        assert_eq!(join_node.set.count(), 2);
    }

    #[test]
    fn test_get_cardinality() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        let stats = vec![RelationStats::with_cardinality(1000)];
        cost_model.init_cost_model(&mut set_manager, &stats);

        let set = set_manager.get_relation(0);
        let card = cost_model.get_cardinality(&set);
        assert_eq!(card, 1000.0);
    }

    #[test]
    fn test_three_way_join_cost() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // Create filters for A-B and B-C joins
        let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        let filter_bc = create_equality_filter(&mut set_manager, 1, 1, 2, 0, 1);
        cost_model.init_equivalent_relations(&[filter_ab, filter_bc]);

        // Initialize relation stats
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);

        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(
            1,
            [DistinctCount::new(50, true), DistinctCount::new(25, true)],
        );

        let mut stats2 = RelationStats::with_cardinality(200);
        stats2.column_distinct_count = column_distinct_counts(2, [DistinctCount::new(20, true)]);

        cost_model.init_cost_model(&mut set_manager, &[stats0, stats1, stats2]);

        // Create leaf nodes
        let a = leaf(&mut cost_model, set_manager.get_relation(0));
        let b = leaf(&mut cost_model, set_manager.get_relation(1));
        let c = leaf(&mut cost_model, set_manager.get_relation(2));

        // Compare two join orders: (A ⋈ B) ⋈ C vs A ⋈ (B ⋈ C)
        let ab = cost_model.compute_cost_and_create_node(&a, &b, &mut set_manager, None);
        let ab_c = cost_model.compute_cost_and_create_node(&ab, &c, &mut set_manager, None);

        let bc = cost_model.compute_cost_and_create_node(&b, &c, &mut set_manager, None);
        let a_bc = cost_model.compute_cost_and_create_node(&a, &bc, &mut set_manager, None);

        // Both should have valid costs
        assert!(ab_c.cost > 0.0);
        assert!(a_bc.cost > 0.0);
    }

    #[test]
    fn weak_anti_reduction_follows_selective_inner_join() {
        // TPC-H Q16 shape: partsupp is first narrowed by a selective part
        // predicate; a tiny supplier exclusion then probes only those rows.
        // ANTI-first barely reduces the fact input and must not win merely on
        // the smaller cardinality of that first intermediate.
        let mut sets = JoinRelationSetManager::new();
        let part_filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
        let supplier_exclusion = create_reduction_filter(&mut sets, 0, 1, 2, 0, 1, JoinType::Anti);
        let filters = vec![part_filter.clone(), supplier_exclusion.clone()];

        let mut partsupp_stats = RelationStats::with_cardinality(800_000);
        partsupp_stats.estimated_payload_width = 16;
        partsupp_stats.column_distinct_count = column_distinct_counts(
            0,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        let mut part_stats = RelationStats::with_cardinality(30_000);
        part_stats.estimated_payload_width = 40;
        part_stats.column_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(30_000, true)]);
        let mut supplier_stats = RelationStats::with_cardinality(112);
        supplier_stats.estimated_payload_width = 8;
        supplier_stats.column_distinct_count =
            column_distinct_counts(2, [DistinctCount::new(112, true)]);

        let mut model = CostModel::new();
        model.init_equivalent_relations(&filters);
        model.init_cost_model(&mut sets, &[partsupp_stats, part_stats, supplier_stats]);

        let mut partsupp = leaf(&mut model, sets.get_relation(0));
        partsupp.cardinality = model.get_cardinality(&partsupp.set);
        let mut part = leaf(&mut model, sets.get_relation(1));
        part.cardinality = model.get_cardinality(&part.set);
        let mut supplier = leaf(&mut model, sets.get_relation(2));
        supplier.cardinality = model.get_cardinality(&supplier.set);

        let anti_first = model.compute_cost_and_create_node(
            &partsupp,
            &supplier,
            &mut sets,
            Some(predicate_set(&[supplier_exclusion.clone()])),
        );
        let anti_then_part = model.compute_cost_and_create_node(
            &anti_first,
            &part,
            &mut sets,
            Some(predicate_set(&[part_filter.clone()])),
        );

        let part_first = model.compute_cost_and_create_node(
            &partsupp,
            &part,
            &mut sets,
            Some(predicate_set(&[part_filter])),
        );
        let part_then_anti = model.compute_cost_and_create_node(
            &part_first,
            &supplier,
            &mut sets,
            Some(predicate_set(&[supplier_exclusion])),
        );

        assert!(
            part_then_anti.cost < anti_then_part.cost,
            "selective inner first: {}, weak anti first: {}",
            part_then_anti.cost,
            anti_then_part.cost
        );
    }

    #[test]
    fn hash_input_orientation_matches_width_aware_build_side_for_every_join() {
        let mut sets = JoinRelationSetManager::new();
        let inner_filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
        let reduction_filter = create_reduction_filter(&mut sets, 0, 0, 1, 0, 1, JoinType::Anti);
        let mut left_stats = RelationStats::with_cardinality(10);
        left_stats.estimated_payload_width = 1_024;
        let mut right_stats = RelationStats::with_cardinality(100);
        right_stats.estimated_payload_width = 8;
        let mut model = CostModel::new();
        model.init_cost_model(&mut sets, &[left_stats, right_stats]);
        let mut left = leaf(&mut model, sets.get_relation(0));
        left.cardinality = 10.0;
        let mut right = leaf(&mut model, sets.get_relation(1));
        right.cardinality = 100.0;

        let inner = predicate_set(&[inner_filter]);
        let inner_cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&inner));
        let integer_key_width = estimate_row_payload_width(&[LogicalType::Integer]);
        assert_eq!(
            inner_cost.build,
            100.0
                * (estimate_hash_build_row_width(8, integer_key_width)
                    + HASH_BUCKET_CACHE_LINE_BYTES) as f64,
            "the wider ten-row input costs more to serialize than the narrow hundred-row input"
        );
        assert_eq!(
            inner_cost.probe,
            10.0 * estimate_hash_probe_row_width(integer_key_width) as f64
        );

        let reduction = predicate_set(&[reduction_filter]);
        let reduction_cost =
            model.compute_cost_breakdown(&left, &right, &mut sets, Some(&reduction));
        assert_eq!(reduction_cost.build, inner_cost.build);
        assert_eq!(reduction_cost.probe, inner_cost.probe);
        assert_eq!(reduction_cost.total(), inner_cost.total());

        let reduction_node =
            model.compute_cost_and_create_node(&left, &right, &mut sets, Some(reduction));
        assert_eq!(reduction_node.output_payload_width, 1_024);
    }

    #[test]
    fn hash_probe_work_does_not_assume_runtime_filter_pushdown() {
        let mut sets = JoinRelationSetManager::new();
        let filter = create_equality_filter(&mut sets, 0, 0, 1, 0, 0);
        let mut build_stats = RelationStats::with_cardinality(10);
        build_stats.column_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(10, true)]);
        let mut probe_stats = RelationStats::with_cardinality(1_000);
        probe_stats.column_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(1_000, true)]);
        let mut model = CostModel::new();
        model.init_equivalent_relations(&[Arc::clone(&filter)]);
        model.init_cost_model(&mut sets, &[build_stats, probe_stats]);
        let build = leaf(&mut model, sets.get_relation(0));
        let probe = leaf(&mut model, sets.get_relation(1));
        let predicates = predicate_set(&[filter]);

        let cost = model.compute_cost_breakdown(&build, &probe, &mut sets, Some(&predicates));
        let integer_key_width = estimate_row_payload_width(&[LogicalType::Integer]);
        assert_eq!(
            cost.probe,
            1_000.0 * estimate_hash_probe_row_width(integer_key_width) as f64,
            "runtime-filter eligibility belongs to physical pushdown, not the join graph"
        );
    }

    #[test]
    fn range_join_does_not_claim_an_equality_runtime_filter() {
        let mut sets = JoinRelationSetManager::new();
        let filter = create_comparison_filter(&mut sets, 0, 0, 1, 0, 0, ComparisonType::LessThan);
        let left_stats = RelationStats::with_cardinality(10);
        let right_stats = RelationStats::with_cardinality(1_000);
        let mut model = CostModel::new();
        model.init_cost_model(&mut sets, &[left_stats, right_stats]);
        let left = leaf(&mut model, sets.get_relation(0));
        let right = leaf(&mut model, sets.get_relation(1));
        let predicates = predicate_set(&[filter]);
        let combination = sets.union(&left.set, &right.set);
        let join_rows = model.get_cardinality(&combination);

        let cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&predicates));
        let condition_width = estimate_row_payload_width(&[LogicalType::Integer]);
        assert_eq!(cost.build, 10.0 * estimate_row_width_from_payload(1) as f64);
        assert_eq!(
            cost.match_output,
            join_rows * estimate_row_width_from_payload(2) as f64
        );
        assert_eq!(
            cost.probe,
            10.0 * 1_000.0 * (2 * condition_width + NESTED_LOOP_CURSOR_BYTES) as f64,
            "range joins materialize one side, evaluate every pair, and copy accepted rows"
        );
    }

    #[test]
    fn reduction_control_region_remains_on_the_build_side() {
        let mut sets = JoinRelationSetManager::new();
        let reduction_filter = create_reduction_filter(&mut sets, 0, 0, 1, 0, 0, JoinType::Anti);
        let mut preserved_stats = RelationStats::with_cardinality(10);
        preserved_stats.estimated_payload_width = 8;
        let mut filtering_stats = RelationStats::with_cardinality(100);
        filtering_stats.estimated_payload_width = 1_024;
        filtering_stats.contains_control_region = true;
        let mut model = CostModel::new();
        model.init_cost_model(&mut sets, &[preserved_stats, filtering_stats]);
        let mut preserved = leaf(&mut model, sets.get_relation(0));
        preserved.cardinality = 10.0;
        let mut filtering = leaf(&mut model, sets.get_relation(1));
        filtering.cardinality = 100.0;

        let predicates = predicate_set(&[reduction_filter]);
        let cost =
            model.compute_cost_breakdown(&preserved, &filtering, &mut sets, Some(&predicates));
        let key_width = estimate_row_payload_width(&[LogicalType::Integer]);
        assert_eq!(
            cost.build,
            100.0
                * (estimate_hash_build_row_width(1_024, key_width) + HASH_BUCKET_CACHE_LINE_BYTES)
                    as f64
        );
    }

    #[test]
    fn selective_snowflake_path_beats_unfiltered_fact_dimension_path() {
        // Customer/order/lineitem/supplier/nation/region shape. The redundant
        // customer=nation edge is the transitive equality that makes the
        // selective dimension path representable without a Cartesian product.
        let mut sets = JoinRelationSetManager::new();
        let filters = vec![
            create_equality_filter(&mut sets, 0, 0, 1, 1, 0),
            create_equality_filter(&mut sets, 1, 0, 2, 0, 1),
            create_equality_filter(&mut sets, 2, 1, 3, 0, 2),
            create_equality_filter(&mut sets, 0, 1, 3, 1, 3),
            create_equality_filter(&mut sets, 3, 1, 4, 0, 4),
            create_equality_filter(&mut sets, 0, 1, 4, 0, 5),
            create_equality_filter(&mut sets, 4, 1, 5, 0, 6),
        ];
        let stats = vec![
            RelationStats {
                cardinality: 150_000,
                column_distinct_count: column_distinct_counts(
                    0,
                    [
                        DistinctCount::new(150_000, false),
                        DistinctCount::new(25, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 135_000,
                column_distinct_count: column_distinct_counts(
                    1,
                    [
                        DistinctCount::new(1_500_000, false),
                        DistinctCount::new(150_000, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 6_001_215,
                column_distinct_count: column_distinct_counts(
                    2,
                    [
                        DistinctCount::new(1_500_000, false),
                        DistinctCount::new(10_000, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 10_000,
                column_distinct_count: column_distinct_counts(
                    3,
                    [
                        DistinctCount::new(10_000, false),
                        DistinctCount::new(25, false),
                    ],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 25,
                column_distinct_count: column_distinct_counts(
                    4,
                    [DistinctCount::new(25, false), DistinctCount::new(5, false)],
                ),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 1,
                column_distinct_count: column_distinct_counts(5, [DistinctCount::new(5, false)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
        ];
        let mut model = CostModel::new();
        model.init_equivalent_relations(&filters);
        model.init_cost_model(&mut sets, &stats);

        let customer_nation_region = sets.get_relation_from_vec(vec![0, 4, 5]);
        let customer_orders = sets.get_relation_from_vec(vec![0, 1]);
        let customer_supplier = sets.get_relation_from_vec(vec![0, 3]);
        let filtered_orders = sets.get_relation_from_vec(vec![0, 1, 4, 5]);
        let customer_supplier_nation_region = sets.get_relation_from_vec(vec![0, 3, 4, 5]);
        let full_join = sets.get_relation_from_vec(vec![0, 1, 2, 3, 4, 5]);

        assert_eq!(model.get_cardinality(&customer_nation_region), 30_000.0);
        assert_eq!(model.get_cardinality(&customer_orders), 135_000.0);
        // The nation equality class is bounded by the 25-row nation domain,
        // even when nation itself is not yet part of this DP subset. Ignoring
        // that class-wide bound makes this explosive join look selective.
        assert_eq!(model.get_cardinality(&customer_supplier), 60_000_000.0);
        assert_eq!(model.get_cardinality(&filtered_orders), 27_000.0);
        assert_eq!(
            model.get_cardinality(&customer_supplier_nation_region),
            12_000_000.0
        );
        assert!((model.get_cardinality(&full_join) - 4_320.874_8).abs() < 1e-6);
    }

    #[test]
    fn equality_class_uses_each_observed_domain_once() {
        let mut sets = JoinRelationSetManager::new();
        let filters = vec![
            create_equality_filter(&mut sets, 0, 0, 1, 0, 0),
            create_equality_filter(&mut sets, 1, 0, 2, 0, 1),
        ];
        let stats = vec![
            RelationStats {
                cardinality: 10,
                column_distinct_count: column_distinct_counts(0, [DistinctCount::new(10, true)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 100,
                column_distinct_count: column_distinct_counts(1, [DistinctCount::new(100, true)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
            RelationStats {
                cardinality: 1_000,
                column_distinct_count: column_distinct_counts(2, [DistinctCount::new(1_000, true)]),
                stats_initialized: true,
                ..RelationStats::default()
            },
        ];
        let mut model = CostModel::new();
        model.init_equivalent_relations(&filters);
        model.init_cost_model(&mut sets, &stats);

        let full_join = sets.get_relation_from_vec(vec![0, 1, 2]);
        assert_eq!(model.get_cardinality(&full_join), 10.0);
    }

    #[test]
    fn test_cross_product_cost() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut cost_model = CostModel::new();

        // No join filters - this will be a cross product
        let stats = vec![
            RelationStats::with_cardinality(100),
            RelationStats::with_cardinality(50),
        ];
        cost_model.init_cost_model(&mut set_manager, &stats);

        let left = leaf(&mut cost_model, set_manager.get_relation(0));
        let right = leaf(&mut cost_model, set_manager.get_relation(1));

        let cost = cost_model.compute_cost(&left, &right, &mut set_manager, None);

        assert_eq!(
            cost,
            50.0 * estimate_row_width_from_payload(1) as f64
                + 100.0 * 50.0 * CROSS_PRODUCT_SELECTION_BYTES as f64,
            "cross product materializes the smaller input and emits one repeated-row ordinal per pair"
        );
    }

    #[test]
    fn unoriented_graph_predicate_uses_cross_product_cost_contract() {
        let mut sets = JoinRelationSetManager::new();
        let left_set = sets.get_relation(0);
        let right_set = sets.get_relation(1);
        let full_set = sets.union(&left_set, &right_set);
        let residual = Arc::new(FilterInfo::new_inner(
            Expression::Operator(OperatorExpression::new(
                OperatorType::Coalesce,
                vec![create_column_ref(0, 0), create_column_ref(1, 0)],
                LogicalType::Boolean,
            )),
            full_set,
            0,
        ));
        let CutPredicateResolution::Resolved(Some(predicates)) =
            JoinPredicateSet::from_filters([&residual], &left_set, &right_set)
        else {
            panic!("multi-relation residual should form a cut predicate set")
        };
        assert!(!predicates.has_join_conditions());

        let mut model = CostModel::new();
        model.init_cost_model(
            &mut sets,
            &[
                RelationStats::with_cardinality(100),
                RelationStats::with_cardinality(50),
            ],
        );
        let left = leaf(&mut model, left_set);
        let right = leaf(&mut model, right_set);
        let cost = model.compute_cost_breakdown(&left, &right, &mut sets, Some(&predicates));

        assert_eq!(cost.build, 50.0 * estimate_row_width_from_payload(1) as f64);
        assert_eq!(
            cost.probe,
            100.0 * 50.0 * CROSS_PRODUCT_SELECTION_BYTES as f64
        );
        assert_eq!(cost.match_output, 0.0);
    }
}
