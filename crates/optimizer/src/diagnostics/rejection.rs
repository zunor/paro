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
