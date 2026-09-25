// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join-order cost model based on cardinality estimates.

use crate::cost::join_layout::{
    choose_join_build_side, estimate_hash_build_row_width, estimate_row_payload_width,
    estimate_row_width_from_payload, JoinBuildCandidate, JoinBuildSide,
};
use crate::estimate::join::CardinalityEstimator;
use crate::estimate::selectivity::SelectivityDefaults;
use crate::region::join::candidate::DPJoinNode;
use crate::region::join::query_graph::{JoinEdgeOrientation, JoinPredicateSet};
use crate::region::join::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::region::join::relation_manager::RelationStats;
use paro_planner::expression::Expression;
use paro_planner::logical::plan::CardinalityProvenance;
use std::sync::Arc;

/// The RegionCostModel computes the cost of join plans.
///
#[derive(Debug)]
pub(crate) struct RegionCostModel {
    pub(crate) regional_pricing: Option<crate::cost::join::JoinWorkPricing>,
    pub(crate) residual_selectivities: Vec<(Arc<JoinRelationSet>, f64)>,
    pub(crate) selectivity_defaults: SelectivityDefaults,
    /// Cardinality estimator used to calculate cost.
    pub cardinality_estimator: CardinalityEstimator,
    risk_cardinality_estimator: CardinalityEstimator,
    materialization_cardinality_estimator: CardinalityEstimator,
    relation_materialization_cardinalities: Vec<usize>,
    relation_widths: Vec<usize>,
    relation_control_regions: Vec<bool>,
    relation_provenances: Vec<CardinalityProvenance>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct JoinCostBreakdown {
    build: f64,
    probe: f64,
    match_output: f64,
    children: f64,
}

pub(crate) struct CostedJoin {
    pub(crate) combination: Arc<JoinRelationSet>,
    pub(crate) cardinality: f64,
    pub(crate) risk_cardinality: f64,
    pub(crate) materialization_cardinality: f64,
    pub(crate) materialization_is_reduction_bound: bool,
    pub(crate) output_payload_width: usize,
    pub(crate) breakdown: JoinCostBreakdown,
    pub(crate) build_side: JoinBuildSide,
    pub(crate) peak_build_bytes: u64,
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
    hash_key_width: usize,
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
    pub(crate) fn total(self) -> f64 {
        self.build + self.probe + self.match_output + self.children
    }
}

impl RegionCostModel {
    /// Create a join cost model from the statement's shared priors.
    pub fn new(selectivity_defaults: SelectivityDefaults) -> Self {
        Self {
            regional_pricing: None,
            residual_selectivities: Vec::new(),
            cardinality_estimator: CardinalityEstimator::new(selectivity_defaults.clone()),
            risk_cardinality_estimator: CardinalityEstimator::new(selectivity_defaults.clone()),
            materialization_cardinality_estimator: CardinalityEstimator::new(
                selectivity_defaults.clone(),
            ),
            selectivity_defaults,
            relation_materialization_cardinalities: Vec::new(),
            relation_widths: Vec::new(),
            relation_control_regions: Vec::new(),
            relation_provenances: Vec::new(),
        }
    }

    /// Clear query-local estimates.
    pub fn reset(&mut self) {
        self.residual_selectivities.clear();
        self.cardinality_estimator = CardinalityEstimator::new(self.selectivity_defaults.clone());
        self.risk_cardinality_estimator =
            CardinalityEstimator::new(self.selectivity_defaults.clone());
        self.materialization_cardinality_estimator =
            CardinalityEstimator::new(self.selectivity_defaults.clone());
        self.relation_materialization_cardinalities.clear();
        self.relation_widths.clear();
        self.relation_control_regions.clear();
        self.relation_provenances.clear();
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
        self.relation_provenances = relation_stats
            .iter()
            .map(|stats| stats.cardinality_provenance)
            .collect();
    }

    pub(crate) fn relation_provenance(&self, relation: usize) -> CardinalityProvenance {
        self.relation_provenances[relation]
    }

    pub(crate) fn init_equivalent_relations(
        &mut self,
        filters: &[Arc<crate::region::join::query_graph::FilterInfo>],
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
        let reduction_orientation = predicates.and_then(JoinPredicateSet::reduction_orientation);
        let preserved_expected_rows = match reduction_orientation {
            Some(JoinEdgeOrientation::Forward) => Some(left.cardinality),
            Some(JoinEdgeOrientation::Inverted) => Some(right.cardinality),
            None => None,
        };
        let preserved_risk_rows = match reduction_orientation {
            Some(JoinEdgeOrientation::Forward) => Some(left.risk_cardinality),
            Some(JoinEdgeOrientation::Inverted) => Some(right.risk_cardinality),
            None => None,
        };
        let estimated_join_rows = self.get_cardinality(&combination);
        let join_rows = preserved_expected_rows.map_or(estimated_join_rows, |preserved| {
            estimated_join_rows.min(preserved)
        });
        let estimated_risk_join_rows = self
            .risk_cardinality_estimator
            .estimate_cardinality(&combination)
            .max(join_rows);
        let risk_join_rows = preserved_risk_rows.map_or(estimated_risk_join_rows, |preserved| {
            estimated_risk_join_rows.min(preserved.max(join_rows))
        });
        let output_payload_width = Self::output_payload_width(left, right, predicates);
        // A reduction's materialization bound belongs to the selected child
        // expression: its relation set also contains the filtering relation,
        // but those rows can never be emitted. Ordinary children still use
        // the relation-set estimator so alternate inner-join trees retain the
        // same conservative build contract.
        let child_materialization_rows = |model: &mut Self, child: &DPJoinNode| {
            if child.materialization_is_reduction_bound {
                child.materialization_cardinality
            } else {
                model.materialization_cardinality(&child.set)
            }
            .max(child.risk_cardinality)
        };
        let left_materialization_rows = child_materialization_rows(self, left);
        let right_materialization_rows = child_materialization_rows(self, right);
        let output_materialization_rows = match reduction_orientation {
            Some(JoinEdgeOrientation::Forward) => left_materialization_rows,
            Some(JoinEdgeOrientation::Inverted) => right_materialization_rows,
            None => self.materialization_cardinality(&combination),
        }
        .max(risk_join_rows);
        let priced_join_rows = if self.regional_pricing.is_some() {
            // Charge output before newly activated residuals, without
            // multiplying predicates inherited from child relations twice.
            self.cardinality_before_activation(&combination, &left.set, &right.set)
                .min(preserved_expected_rows.unwrap_or(f64::INFINITY))
        } else {
            risk_join_rows
        };
        let (breakdown, build_side) = self.cost_breakdown_for_cardinality(JoinCostInputs {
            left,
            right,
            predicates,
            join_rows: priced_join_rows,
            output_payload_width,
            left_materialization_rows,
            right_materialization_rows,
        });
        CostedJoin {
            combination,
            cardinality: join_rows,
            risk_cardinality: risk_join_rows,
            materialization_cardinality: output_materialization_rows,
            materialization_is_reduction_bound: reduction_orientation.is_some(),
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
        if let Some(pricing) = self.regional_pricing {
            use crate::cost::join::HashJoinWork;
            let hash_cost = |build: &DPJoinNode, probe: &DPJoinNode| {
                pricing.price(HashJoinWork {
                    build_rows: build.cardinality,
                    probe_rows: probe.cardinality,
                    output_rows: join_rows,
                    build_width: build.output_payload_width as f64,
                    probe_width: probe.output_payload_width as f64,
                    output_width: output_payload_width as f64,
                    key_width: conditions.hash_key_width as f64,
                })
            };
            let left_cost = if conditions.has_hash_key {
                hash_cost(left, right)
            } else {
                left.cardinality * left.output_payload_width as f64
            };
            let right_cost = if conditions.has_hash_key {
                hash_cost(right, left)
            } else {
                right.cardinality * right.output_payload_width as f64
            };
            let build_side = choose_join_build_side(
                filtering_side,
                JoinBuildCandidate {
                    serialized_work: left_cost,
                    contains_control_region: self.contains_control_region(&left.set),
                },
                JoinBuildCandidate {
                    serialized_work: right_cost,
                    contains_control_region: self.contains_control_region(&right.set),
                },
            );
            let work = if conditions.has_hash_key {
                match build_side {
                    JoinBuildSide::Left => left_cost,
                    JoinBuildSide::Right => right_cost,
                }
            } else {
                pricing.non_hash(
                    HashJoinWork {
                        build_rows: right.cardinality,
                        probe_rows: left.cardinality,
                        output_rows: join_rows,
                        build_width: right.output_payload_width as f64,
                        probe_width: left.output_payload_width as f64,
                        output_width: output_payload_width as f64,
                        key_width: 0.0,
                    },
                    conditions.has_join_conditions,
                )
            };
            return (
                JoinCostBreakdown {
                    build: work,
                    probe: 0.0,
                    match_output: 0.0,
                    children: left.cost + right.cost,
                },
                build_side,
            );
        }
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
        let mut hash_key_width = 0usize;
        for predicate in predicates.predicates() {
            let Some(orientation) = predicate.orientation() else {
                continue;
            };
            let filter = predicate.filter();
            let mut add_comparison =
                |comparison: &paro_planner::expression::ComparisonExpression| {
                    let is_hash_key = matches!(
                        comparison.comparison_type,
                        paro_planner::expression::ComparisonType::Equal
                            | paro_planner::expression::ComparisonType::NotDistinctFrom
                    );
                    has_hash_key |= is_hash_key;
                    if is_hash_key {
                        hash_key_width = hash_key_width.saturating_add(
                            paro_storage::rowset::scan_cost::ScanAccessCostModel::default()
                                .estimated_width(&comparison.right.return_type()),
                        );
                    }
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
            hash_key_width,
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
        // Estimate from the complete set, never by repeatedly multiplying a
        // child's already-filtered count. Each eligible predicate contributes
        // once regardless of the chosen tree or the graph's duplicate edges.
        let fraction: f64 = self
            .residual_selectivities
            .iter()
            .filter(|(required, _)| set.contains_all(required))
            .map(|(_, fraction)| fraction)
            .product();
        self.cardinality_estimator.estimate_cardinality(set) * fraction
    }

    pub(crate) fn cardinality_before_activation(
        &mut self,
        set: &JoinRelationSet,
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> f64 {
        let inherited: f64 = self
            .residual_selectivities
            .iter()
            .filter(|(required, _)| left.contains_all(required) || right.contains_all(required))
            .map(|(_, fraction)| fraction)
            .product();
        self.cardinality_estimator.estimate_cardinality(set) * inherited
    }

    pub fn get_risk_cardinality(&mut self, set: &JoinRelationSet) -> f64 {
        self.risk_cardinality_estimator
            .estimate_cardinality(set)
            .max(self.get_cardinality(set))
    }

    pub fn get_materialization_cardinality(&mut self, set: &JoinRelationSet) -> f64 {
        self.materialization_cardinality(set)
            .max(self.get_risk_cardinality(set))
    }
}

#[cfg(test)]
mod tests;
