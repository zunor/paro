// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::cost::join_layout::JoinBuildSide;
use crate::cost::region::CostedJoin;
use crate::region::join::{query_graph::JoinPredicateSet, relation::JoinRelationSet};
use paro_planner::logical::plan::CardinalityProvenance;
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
    pub cardinality_provenance: CardinalityProvenance,
    /// Cardinality used for risk-adjusted work costing.
    pub risk_cardinality: f64,
    /// Conservative cardinality used only if this subtree is selected as an
    /// irreversible build input. This keeps selectivity uncertainty from
    /// being mistaken for a physical materialization proof.
    pub materialization_cardinality: f64,
    /// Whether `materialization_cardinality` is an operator-local upper bound
    /// that is tighter than the relation-set estimate. Reduction joins own
    /// such a bound because they cannot emit more rows than their preserved
    /// child; ordinary joins must be re-estimated from the complete set.
    pub materialization_is_reduction_bound: bool,
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
    /// Stable tie-break shape, materialized once when the node is created.
    /// Frontier maintenance compares this value repeatedly; rebuilding a
    /// recursive String for every comparison made DP pricing pay an
    /// allocation proportional to the whole join tree.
    pub(crate) shape: Arc<str>,
}

impl DPJoinNode {
    pub(crate) fn compact_shape(&self) -> &str {
        &self.shape
    }

    /// Create a leaf node (single relation).
    ///
    /// Leaf nodes have cost 0 since they represent base tables.
    pub fn leaf(
        set: Arc<JoinRelationSet>,
        output_payload_width: usize,
        cardinality: f64,
        risk_cardinality: f64,
        materialization_cardinality: f64,
    ) -> Self {
        let shape = set
            .relations()
            .first()
            .map_or_else(|| "?".to_string(), usize::to_string);
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
            cardinality_provenance: CardinalityProvenance::Statistics,
            risk_cardinality,
            materialization_cardinality,
            materialization_is_reduction_bound: false,
            output_payload_width,
            peak_build_bytes: 0,
            shape: Arc::from(shape),
        }
    }

    /// Create an intermediate node (join of two relations).
    pub(crate) fn intermediate(
        predicates: Option<JoinPredicateSet>,
        left: &DPJoinNode,
        right: &DPJoinNode,
        estimate: CostedJoin,
    ) -> Self {
        let build = match estimate.build_side {
            JoinBuildSide::Left => "L",
            JoinBuildSide::Right => "R",
        };
        let shape = format!(
            "({} {build} {})",
            left.compact_shape(),
            right.compact_shape()
        );
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
            cardinality_provenance: if left.cardinality_provenance == CardinalityProvenance::Unknown
                || right.cardinality_provenance == CardinalityProvenance::Unknown
            {
                CardinalityProvenance::Unknown
            } else {
                CardinalityProvenance::JoinGraph
            },
            risk_cardinality: estimate.risk_cardinality,
            materialization_cardinality: estimate.materialization_cardinality,
            materialization_is_reduction_bound: estimate.materialization_is_reduction_bound,
            output_payload_width: estimate.output_payload_width,
            peak_build_bytes: estimate.peak_build_bytes,
            shape: Arc::from(shape),
        }
    }
}
