// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{AntiJoinMode, JoinCondition, JoinType, MarkJoinSemantics};
use std::sync::Arc;

use super::SpillExecutionPolicy;
use crate::physical::identity::Fingerprint;

/// Immutable bijection from the executor's natural join-output ordinals to
/// the SQL-facing output ordinals.
///
/// Both directions are materialized once when the physical plan is built. A
/// caller therefore cannot accidentally use a forward mapping as an inverse,
/// and row-production hot paths never scan the permutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputPermutation {
    representation: OutputPermutationRepresentation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputPermutationRepresentation {
    Identity(usize),
    Mapped(Arc<OutputPermutationMaps>),
}

#[derive(Debug, PartialEq, Eq)]
struct OutputPermutationMaps {
    forward: Box<[usize]>,
    inverse: Box<[usize]>,
}

impl OutputPermutation {
    pub fn identity(len: usize) -> Self {
        Self {
            representation: OutputPermutationRepresentation::Identity(len),
        }
    }

    pub fn from_forward(
        forward: impl IntoIterator<Item = usize>,
    ) -> paro_common::error::Result<Self> {
        let forward = forward.into_iter().collect::<Vec<_>>();
        if forward
            .iter()
            .copied()
            .enumerate()
            .all(|(natural, destination)| natural == destination)
        {
            return Ok(Self::identity(forward.len()));
        }
        let mut inverse = vec![usize::MAX; forward.len()];
        for (natural, &destination) in forward.iter().enumerate() {
            let Some(slot) = inverse.get_mut(destination) else {
                return Err(paro_common::error::internal(format!(
                    "join output permutation maps natural column {natural} to out-of-range column {destination}"
                )));
            };
            if *slot != usize::MAX {
                return Err(paro_common::error::internal(format!(
                    "join output permutation maps multiple natural columns to destination {destination}"
                )));
            }
            *slot = natural;
        }
        Ok(Self {
            representation: OutputPermutationRepresentation::Mapped(Arc::new(
                OutputPermutationMaps {
                    forward: forward.into_boxed_slice(),
                    inverse: inverse.into_boxed_slice(),
                },
            )),
        })
    }

    #[inline]
    pub fn destination_of(&self, natural: usize) -> Option<usize> {
        match &self.representation {
            OutputPermutationRepresentation::Identity(len) => (natural < *len).then_some(natural),
            OutputPermutationRepresentation::Mapped(maps) => maps.forward.get(natural).copied(),
        }
    }

    #[inline]
    pub fn natural_of(&self, destination: usize) -> Option<usize> {
        match &self.representation {
            OutputPermutationRepresentation::Identity(len) => {
                (destination < *len).then_some(destination)
            }
            OutputPermutationRepresentation::Mapped(maps) => maps.inverse.get(destination).copied(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.representation {
            OutputPermutationRepresentation::Identity(len) => *len,
            OutputPermutationRepresentation::Mapped(maps) => maps.forward.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_identity(&self) -> bool {
        matches!(
            &self.representation,
            OutputPermutationRepresentation::Identity(_)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterWaitPolicy {
    WaitComplete,
}

/// Explicit sideways-artifact contract selected by an AuxiliaryPlanRegion.
/// Absence is the mandatory no-filter fallback; execution may not infer this
/// contract from the presence of an eligible hash join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashJoinRuntimeFilterSpec {
    pub artifact: Fingerprint,
    pub wait_policy: RuntimeFilterWaitPolicy,
    pub resource: crate::physical::RuntimeFilterResourceContract,
}

#[cfg(test)]
mod output_permutation_tests {
    use super::OutputPermutation;

    #[test]
    fn stores_both_directions_of_a_valid_bijection() {
        let identity = OutputPermutation::identity(2);
        assert!(identity.is_identity());
        assert_eq!(identity.destination_of(1), Some(1));
        assert_eq!(identity.natural_of(2), None);

        let permutation = OutputPermutation::from_forward([2, 0, 1]).unwrap();

        assert_eq!(permutation.len(), 3);
        assert!(!permutation.is_identity());
        assert_eq!(permutation.destination_of(0), Some(2));
        assert_eq!(permutation.destination_of(2), Some(1));
        assert_eq!(permutation.natural_of(0), Some(1));
        assert_eq!(permutation.natural_of(2), Some(0));
    }

    #[test]
    fn rejects_non_bijective_forward_maps_at_construction() {
        assert!(OutputPermutation::from_forward([0, 0]).is_err());
        assert!(OutputPermutation::from_forward([0, 2]).is_err());
    }
}

#[derive(Debug, Clone)]
pub struct HashJoinSpec {
    pub join_type: JoinType,
    pub anti_join_mode: AntiJoinMode,
    /// Correctness proof that the complete build equality-key tuple is unique.
    /// Runtime index construction may use this to publish disjoint slots
    /// without duplicate-chain synchronization.
    pub build_keys_unique: bool,
    /// Pre-admitted direct domain for an integral build key. The build sink
    /// fills this artifact while retaining rows, so finish only publishes it
    /// instead of rescanning the row store. Unique keys use disjoint stores;
    /// repeated keys atomically link their build chains in the same pass.
    pub build_time_integer_index: Option<BuildTimeIntegerJoinIndexSpec>,
    /// Equality predicates used to locate a candidate hash chain.
    pub key_conditions: Box<[JoinCondition]>,
    /// Canonical ordered list of RHS expressions materialized after the visible
    /// build payload. Reduction predicates refer to this list by offset.
    pub build_residual_conditions: Box<[JoinCondition]>,
    /// Prefix of `build_residual_conditions` evaluated by the ordinary probe.
    /// Reduction-only joins keep their predicates out of the ordinary probe
    /// while sharing the same physical build layout.
    pub probe_residual_count: usize,
    pub left_projection: Box<[usize]>,
    /// Columns copied from the build input into the hash-table payload.
    /// Once materialized, the payload is already dense and is never projected
    /// by source-column indexes again.
    pub build_input_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    /// Visible build-side payload prefix in `build_payload_types`.
    pub build_output_count: usize,
    /// Visible build output followed by hidden residual-expression values.
    pub build_payload_types: Box<[LogicalType]>,
    /// Destination ordinal for every column in the executor's natural
    /// `[projected probe, visible build]` output. Keeping this permutation on
    /// the join makes physical orientation independent from its SQL-facing
    /// row contract without introducing a synthetic Project node.
    pub output_permutation: OutputPermutation,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub spill_policy: SpillExecutionPolicy,
    pub runtime_filter: Option<HashJoinRuntimeFilterSpec>,
    /// Multiple existential reductions over one preserved build relation and
    /// one equivalent filtering scan. Each step owns one match bit; the emit
    /// phase applies the required/forbidden masks after the shared probe.
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

#[derive(Debug, Clone)]
pub struct BuildTimeIntegerJoinIndexSpec {
    pub minimum: Value,
    pub maximum: Value,
    pub estimated_rows: usize,
}

#[derive(Debug, Clone)]
pub struct HashReductionCascadeSpec {
    pub predicates: Box<[HashReductionPredicateSpec]>,
    pub source_predicates: Box<[HashReductionSourcePredicateSpec]>,
    pub steps: Box<[HashReductionStepSpec]>,
    pub required_mask: u8,
    pub forbidden_mask: u8,
    /// Optional grouped summary for repeated `source_value <> build_value`
    /// reductions. Exact integer equality indexes can update one extrema state
    /// per key instead of walking every duplicate build row per source row.
    pub grouped_extrema: Option<HashReductionGroupedExtremaSpec>,
}

#[derive(Debug, Clone)]
pub struct HashReductionGroupedExtremaSpec {
    /// Column index in the merged filtering scan containing the summarized
    /// `BIGINT` value.
    pub source_value_index: usize,
    pub build_residual_offset: usize,
    pub channels: Box<[HashReductionExtremaChannelSpec]>,
    /// Maps an evaluated source-predicate mask to eligible extrema channels.
    /// This immutable table belongs to the physical plan and is shared by all
    /// workers instead of being reconstructed in every local operator state.
    pub channel_map: Arc<[u8; 256]>,
}

#[derive(Debug, Clone, Copy)]
pub struct HashReductionExtremaChannelSpec {
    /// Conjunction of source-local predicate bits guarding this summary.
    pub source_predicate_mask: u8,
    /// Reduction match bits established when the channel contains a value
    /// unequal to the current preserved-row value.
    pub match_mask: u8,
}

#[derive(Debug, Clone)]
pub struct HashReductionStepSpec {
    /// All predicate bits that must accept a candidate for this step to match.
    pub predicate_mask: u8,
    pub match_mask: u8,
}

#[derive(Debug, Clone)]
pub struct HashReductionPredicateSpec {
    /// Offset of this predicate's RHS value in the hidden build payload suffix.
    pub build_residual_offset: usize,
    pub predicate_mask: u8,
}

#[derive(Debug, Clone)]
pub struct HashReductionSourcePredicateSpec {
    pub expression: Expression,
    pub predicate_mask: u8,
}

#[derive(Debug, Clone)]
pub struct NestedLoopJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_semantics: MarkJoinSemantics,
    pub arbitrary_condition: Option<Expression>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SortRangeJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_semantics: MarkJoinSemantics,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ClassicIeJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_semantics: MarkJoinSemantics,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct CrossProductSpec {
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub spill_policy: SpillExecutionPolicy,
}

#[derive(Debug, Clone)]
pub struct DelimJoinSpec {
    pub side: DelimJoinSideSpec,
    pub duplicate_keys: Box<[Expression]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimJoinSideSpec {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct DelimScanSpec {
    pub target: DelimScanTarget,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimScanTarget {
    Values { table_index: usize },
    CachedOuter,
}
