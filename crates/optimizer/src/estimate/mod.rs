// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cardinality, column statistics and proven relational bounds.

pub(crate) mod aggregate_filter;
pub(crate) mod annotate;
pub(crate) mod cardinality_bound;
pub(crate) use annotate::annotate;

pub(crate) mod relation_proofs;
pub(crate) mod unique_keys;

/// Read-only statistics access. Regional costing borrows completed inputs;
/// it must not merge their column maps just to evaluate one cut.
pub trait ColumnStatisticsLookup {
    fn get(
        &self,
        binding: &paro_planner::logical::operator::ColumnBinding,
    ) -> Option<&std::sync::Arc<paro_storage::statistics::ColumnStatistics>>;
}

impl ColumnStatisticsLookup
    for std::collections::HashMap<
        paro_planner::logical::operator::ColumnBinding,
        std::sync::Arc<paro_storage::statistics::ColumnStatistics>,
    >
{
    fn get(
        &self,
        binding: &paro_planner::logical::operator::ColumnBinding,
    ) -> Option<&std::sync::Arc<paro_storage::statistics::ColumnStatistics>> {
        self.get(binding)
    }
}

impl<T: ColumnStatisticsLookup + ?Sized> ColumnStatisticsLookup for std::sync::Arc<T> {
    fn get(
        &self,
        binding: &paro_planner::logical::operator::ColumnBinding,
    ) -> Option<&std::sync::Arc<paro_storage::statistics::ColumnStatistics>> {
        self.as_ref().get(binding)
    }
}

pub mod selectivity;

/// A committed relation and its statement-local column evidence.
pub(crate) struct AnnotatedRelation {
    pub plan: paro_planner::logical::plan::OwnedLogicalPlan,
    pub column_stats: std::sync::Arc<
        std::collections::HashMap<
            paro_planner::logical::operator::ColumnBinding,
            std::sync::Arc<paro_storage::statistics::ColumnStatistics>,
        >,
    >,
}

pub(crate) mod join;

pub(crate) mod equality;
