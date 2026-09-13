// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded diagnostic evidence, never an input to rule eligibility or costing.
//! Counts are rejected bindings witnessing each guard, not disjoint outcomes:
//! one binding can fail several proof paths, but each guard counts at most once.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum TransformationRejectionGuard {
    ApplicationError,
    NoOutput,
    OutputContract,
    PublicationError,
    BoundaryUnavailable,
    SelectiveShape,
    SelectiveUnsafeOutput,
    SelectiveMissingCardinality,
    SelectiveInvalidColumn,
    SelectiveNoBaseSource,
    SelectiveRowIdPath,
    SelectiveJoinLocality,
    SelectiveMissingSourceRows,
    SelectiveNoReduction,
    SelectiveNoBenefit,
    AggregateShape,
    AggregateZeroLimit,
    AggregateProjectionShape,
    AggregateUnsafeOutput,
    AggregateOutputExpression,
    AggregateInputShape,
    AggregateGroupingDomain,
    AggregateOutputBinding,
    AggregateOrderBinding,
    AggregateNoDependency,
    AggregateDependencyInvalid,
    AggregateDependentColumn,
    AggregateRowIdPath,
    AggregateSource,
    AggregateStorage,
    AggregateMissingCardinality,
    AggregateNoBenefit,
    AggregateOrderUsesPayload,
    TopNShape,
    TopNZeroLimit,
    TopNProjectionShape,
    TopNUnsafeOutput,
    TopNOutputExpression,
    TopNOrderBinding,
    TopNProjectionMap,
    TopNSource,
    TopNColumnType,
    TopNMissingCardinality,
    TopNRowIdPath,
    TopNMissingSourceRows,
    TopNNoBenefit,
    TopNNoPayload,
    PrefixNoWitness,
}

impl TransformationRejectionGuard {
    pub const ALL: [Self; 48] = [
        Self::ApplicationError,
        Self::NoOutput,
        Self::OutputContract,
        Self::PublicationError,
        Self::BoundaryUnavailable,
        Self::SelectiveShape,
        Self::SelectiveUnsafeOutput,
        Self::SelectiveMissingCardinality,
        Self::SelectiveInvalidColumn,
        Self::SelectiveNoBaseSource,
        Self::SelectiveRowIdPath,
        Self::SelectiveJoinLocality,
        Self::SelectiveMissingSourceRows,
        Self::SelectiveNoReduction,
        Self::SelectiveNoBenefit,
        Self::AggregateShape,
        Self::AggregateZeroLimit,
        Self::AggregateProjectionShape,
        Self::AggregateUnsafeOutput,
        Self::AggregateOutputExpression,
        Self::AggregateInputShape,
        Self::AggregateGroupingDomain,
        Self::AggregateOutputBinding,
        Self::AggregateOrderBinding,
        Self::AggregateNoDependency,
        Self::AggregateDependencyInvalid,
        Self::AggregateDependentColumn,
        Self::AggregateRowIdPath,
        Self::AggregateSource,
        Self::AggregateStorage,
        Self::AggregateMissingCardinality,
        Self::AggregateNoBenefit,
        Self::AggregateOrderUsesPayload,
        Self::TopNShape,
        Self::TopNZeroLimit,
        Self::TopNProjectionShape,
        Self::TopNUnsafeOutput,
        Self::TopNOutputExpression,
        Self::TopNOrderBinding,
        Self::TopNProjectionMap,
        Self::TopNSource,
        Self::TopNColumnType,
        Self::TopNMissingCardinality,
        Self::TopNRowIdPath,
        Self::TopNMissingSourceRows,
        Self::TopNNoBenefit,
        Self::TopNNoPayload,
        Self::PrefixNoWitness,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::ApplicationError => "application_error",
            Self::NoOutput => "no_output",
            Self::OutputContract => "output_contract",
            Self::PublicationError => "publication_error",
            Self::BoundaryUnavailable => "boundary_unavailable",
            Self::SelectiveShape => "selective_shape",
            Self::SelectiveUnsafeOutput => "selective_unsafe_output",
            Self::SelectiveMissingCardinality => "selective_missing_cardinality",
            Self::SelectiveInvalidColumn => "selective_invalid_column",
            Self::SelectiveNoBaseSource => "selective_no_base_source",
            Self::SelectiveRowIdPath => "selective_row_id_path",
            Self::SelectiveJoinLocality => "selective_join_locality",
            Self::SelectiveMissingSourceRows => "selective_missing_source_rows",
            Self::SelectiveNoReduction => "selective_no_reduction",
            Self::SelectiveNoBenefit => "selective_no_benefit",
            Self::AggregateShape => "aggregate_shape",
            Self::AggregateZeroLimit => "aggregate_zero_limit",
            Self::AggregateProjectionShape => "aggregate_projection_shape",
            Self::AggregateUnsafeOutput => "aggregate_unsafe_output",
            Self::AggregateOutputExpression => "aggregate_output_expression",
            Self::AggregateInputShape => "aggregate_input_shape",
            Self::AggregateGroupingDomain => "aggregate_grouping_domain",
            Self::AggregateOutputBinding => "aggregate_output_binding",
            Self::AggregateOrderBinding => "aggregate_order_binding",
            Self::AggregateNoDependency => "aggregate_no_dependency",
            Self::AggregateDependencyInvalid => "aggregate_dependency_invalid",
            Self::AggregateDependentColumn => "aggregate_dependent_column",
            Self::AggregateRowIdPath => "aggregate_row_id_path",
            Self::AggregateSource => "aggregate_source",
            Self::AggregateStorage => "aggregate_storage",
            Self::AggregateMissingCardinality => "aggregate_missing_cardinality",
            Self::AggregateNoBenefit => "aggregate_no_benefit",
            Self::AggregateOrderUsesPayload => "aggregate_order_uses_payload",
            Self::TopNShape => "top_n_shape",
            Self::TopNZeroLimit => "top_n_zero_limit",
            Self::TopNProjectionShape => "top_n_projection_shape",
            Self::TopNUnsafeOutput => "top_n_unsafe_output",
            Self::TopNOutputExpression => "top_n_output_expression",
            Self::TopNOrderBinding => "top_n_order_binding",
            Self::TopNProjectionMap => "top_n_projection_map",
            Self::TopNSource => "top_n_source",
            Self::TopNColumnType => "top_n_column_type",
            Self::TopNMissingCardinality => "top_n_missing_cardinality",
            Self::TopNRowIdPath => "top_n_row_id_path",
            Self::TopNMissingSourceRows => "top_n_missing_source_rows",
            Self::TopNNoBenefit => "top_n_no_benefit",
            Self::TopNNoPayload => "top_n_no_payload",
            Self::PrefixNoWitness => "prefix_no_witness",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RejectionReasons(u64);

impl RejectionReasons {
    pub(crate) fn record(&mut self, guard: TransformationRejectionGuard) {
        self.0 |= 1u64 << guard as usize;
    }
}

pub(crate) fn reject<T>(
    reasons: &mut Option<RejectionReasons>,
    guard: TransformationRejectionGuard,
) -> Option<T> {
    if let Some(reasons) = reasons {
        reasons.record(guard);
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformationRejectionCounts([u64; 48]);

impl Default for TransformationRejectionCounts {
    fn default() -> Self {
        Self([0; 48])
    }
}

impl TransformationRejectionCounts {
    pub(crate) fn record(&mut self, reasons: RejectionReasons) {
        for guard in TransformationRejectionGuard::ALL {
            if reasons.0 & (1u64 << guard as usize) != 0 {
                self.0[guard as usize] = self.0[guard as usize].saturating_add(1);
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (TransformationRejectionGuard, u64)> + '_ {
        TransformationRejectionGuard::ALL
            .into_iter()
            .map(|guard| (guard, self.0[guard as usize]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_guards_deduplicate_per_binding_and_saturate() {
        assert!(TransformationRejectionGuard::ALL.len() <= u64::BITS as usize);
        let mut reasons = RejectionReasons::default();
        reasons.record(TransformationRejectionGuard::SelectiveRowIdPath);
        reasons.record(TransformationRejectionGuard::SelectiveRowIdPath);
        reasons.record(TransformationRejectionGuard::SelectiveNoBenefit);
        let mut counts = TransformationRejectionCounts::default();
        counts.record(reasons);
        assert_eq!(counts.iter().map(|(_, count)| count).sum::<u64>(), 2);
        counts.0[TransformationRejectionGuard::SelectiveRowIdPath as usize] = u64::MAX;
        counts.record(reasons);
        assert_eq!(
            counts.0[TransformationRejectionGuard::SelectiveRowIdPath as usize],
            u64::MAX
        );
    }

    #[test]
    fn disabled_guard_recording_preserves_option_result() {
        let mut disabled = None;
        assert_eq!(
            reject::<()>(&mut disabled, TransformationRejectionGuard::SelectiveShape),
            None
        );
        assert!(disabled.is_none());
    }
}
