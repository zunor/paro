// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opaque bound-relation boundary used while adapting Memo transformations.

use crate::plan::{CardinalityEstimate, UniqueKey};
use paro_common::types::LogicalType;
use paro_storage::statistics::{
    BaseStatistics, ColumnStatistics, DistinctProvenance, EstimatedNumericDistribution,
};
use std::sync::Arc;

use super::ColumnBinding;

/// The identity carried by a bound relation boundary is role-specific.  A
/// single integer previously served as an input ordinal, a Memo group-hole
/// token, and a plan-node occurrence; mixing those domains made an invalid
/// reference look plausible and allowed unchecked indexing.  Keeping the role
/// in the value makes transport contracts explicit and lets maps preserve it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BoundReferenceId {
    InputOrdinal(usize),
    GroupHole(u32),
    NodeOccurrence(u32),
    FrozenOutput,
}

impl BoundReferenceId {
    pub const fn group_hole(value: u32) -> Self {
        Self::GroupHole(value)
    }

    pub fn input_ordinal(value: usize) -> Self {
        Self::InputOrdinal(value)
    }

    pub const fn node_occurrence(value: u32) -> Self {
        Self::NodeOccurrence(value)
    }

    pub const fn frozen_output() -> Self {
        Self::FrozenOutput
    }

    pub fn input_ordinal_value(self) -> paro_common::error::Result<usize> {
        match self {
            Self::InputOrdinal(value) => Ok(value),
            _ => Err(paro_common::error::internal(
                "bound reference is not an input ordinal",
            )),
        }
    }

    pub fn group_hole_value(self) -> paro_common::error::Result<u32> {
        match self {
            Self::GroupHole(value) => Ok(value),
            _ => Err(paro_common::error::internal(
                "bound reference is not a Memo group hole",
            )),
        }
    }
}

/// Immutable value-domain evidence at a relational boundary. NDV estimates and
/// their proofs are separate; this snapshot contains validity, typed bounds,
/// and nested value domains, without carrying an HLL allocation into Memo.
#[derive(Debug, Clone)]
pub struct BoundColumnValues {
    statistics: Arc<BaseStatistics>,
    distribution: Option<EstimatedNumericDistribution>,
    encoding: Arc<[u8]>,
}

impl PartialEq for BoundColumnValues {
    fn eq(&self, other: &Self) -> bool {
        self.statistics.get_type() == other.statistics.get_type() && self.encoding == other.encoding
    }
}

impl Eq for BoundColumnValues {}

impl BoundColumnValues {
    pub fn new(statistics: BaseStatistics) -> paro_common::error::Result<Self> {
        Self::with_distribution(statistics, None)
    }

    pub fn from_column(column: &ColumnStatistics) -> paro_common::error::Result<Self> {
        Self::with_distribution(
            column.statistics().clone(),
            column.estimated_numeric_distribution(),
        )
    }

    fn with_distribution(
        mut statistics: BaseStatistics,
        distribution: Option<EstimatedNumericDistribution>,
    ) -> paro_common::error::Result<Self> {
        statistics.set_distinct_count(0);
        let mut encoding = statistics.to_bytes()?;
        encoding.push(u8::from(distribution.is_some()));
        if let Some(distribution) = distribution {
            encoding.extend_from_slice(&distribution.encoding());
        }
        Ok(Self {
            statistics: Arc::new(statistics),
            distribution,
            encoding: encoding.into(),
        })
    }

    pub fn statistics(&self) -> &BaseStatistics {
        &self.statistics
    }

    pub fn encoding(&self) -> &[u8] {
        &self.encoding
    }

    pub fn hull(&self, other: &Self) -> paro_common::error::Result<Self> {
        if self == other {
            return Ok(self.clone());
        }
        if self.statistics.get_type() != other.statistics.get_type() {
            return Err(paro_common::error::internal(
                "column value domains have different types",
            ));
        }
        // Pin the reduction order, including floating-point endpoint ties.
        let (left, right) = if self.encoding <= other.encoding {
            (self, other)
        } else {
            (other, self)
        };
        let mut statistics = left.statistics.as_ref().clone();
        statistics.merge(&right.statistics);
        Self::with_distribution(
            statistics,
            (self.distribution == other.distribution)
                .then_some(self.distribution)
                .flatten(),
        )
    }
}

/// A fact-backed relation reference whose implementation remains owned by an
/// external relational optimizer. It is legal only inside a transformation
/// transaction and must be consumed before physical planning.
#[derive(Debug, Clone)]
pub struct BoundReference {
    /// Stable identity of this reference occurrence. Unlike `PlanNodeId`, this
    /// survives optimizer passes that rebuild an operator shell.
    pub reference_id: BoundReferenceId,
    pub bindings: Vec<ColumnBinding>,
    pub types: Vec<LogicalType>,
    /// Immutable evidence resolved by the owning Memo, never by choosing or
    /// reconstructing a representative input tree.
    pub facts: Arc<BoundRelationFacts>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSourceColumn {
    pub source: usize,
    pub occurrence: usize,
    pub column: usize,
    pub rows: Option<CardinalityEstimate>,
    pub distinct: Option<u64>,
    pub unique: bool,
}

/// A source lineage is present only when every alternative supplies the same
/// complete source-column coverage. Unknown and partially covered paths must
/// not be confused with a covered path containing zero rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRelationFacts {
    /// A finite effect-free derivation proves that this equivalence class is
    /// safe to evaluate independently again on the same inputs. Splitting a
    /// sharing owner requires this proof; commuting evaluation requires more.
    pub can_replay: bool,
    /// Immutable row-domain estimate supplied by the referenced Memo group.
    /// A shell rewrite may clear NodeStats; it cannot clear this boundary fact.
    pub cardinality: Option<CardinalityEstimate>,
    /// A semantic row bound, separate from snapshot/cardinality estimates.
    pub maximum_cardinality: Option<u64>,
    /// Occurrence-independent domains from the referenced group. Statistical
    /// state accumulated while visiting another occurrence is not evidence
    /// for this boundary, even when its ColumnBindings are identical.
    pub column_domains: Vec<BoundColumnDomain>,
    pub column_values: Vec<Option<BoundColumnValues>>,
    pub unique_keys: Vec<UniqueKey>,
    /// Keys that remain unique when SQL grouping treats NULL values as equal.
    /// This is deliberately separate from ordinary/catalog uniqueness: a
    /// nullable UNIQUE constraint can admit several NULL tuples.
    pub grouping_unique_keys: Vec<UniqueKey>,
    pub source_lineage: Vec<Option<Vec<BoundSourceColumn>>>,
    pub contains_control_region: bool,
}

impl Default for BoundRelationFacts {
    fn default() -> Self {
        Self {
            can_replay: false,
            cardinality: None,
            maximum_cardinality: None,
            column_domains: Vec::new(),
            column_values: Vec::new(),
            unique_keys: Vec::new(),
            grouping_unique_keys: Vec::new(),
            source_lineage: Vec::new(),
            contains_control_region: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BoundColumnDomain {
    pub expected_distinct: Option<u64>,
    pub guaranteed_distinct_upper: Option<u64>,
    /// Provenance of the ranking point. Bound references remain planner-owned
    /// facts; consumers requiring a complete observed domain must check this
    /// explicitly instead of inferring it from `expected_distinct`.
    pub provenance: DistinctProvenance,
}

impl BoundReference {
    pub fn column_statistics(&self) -> Vec<Arc<ColumnStatistics>> {
        self.types
            .iter()
            .enumerate()
            .map(|(ordinal, ty)| {
                let domain = self
                    .facts
                    .column_domains
                    .get(ordinal)
                    .copied()
                    .unwrap_or_default();
                let mut base = self
                    .facts
                    .column_values
                    .get(ordinal)
                    .and_then(Option::as_ref)
                    .map(|value| value.statistics().clone())
                    .unwrap_or_else(|| BaseStatistics::create_unknown(ty.clone()));
                base.set_distinct_count(0);
                let mut column = ColumnStatistics::with_estimated_distinct_provenance(
                    base,
                    domain
                        .expected_distinct
                        .map(|distinct| usize::try_from(distinct).unwrap_or(usize::MAX)),
                    domain.provenance,
                )
                .with_estimated_numeric_distribution(
                    self.facts
                        .column_values
                        .get(ordinal)
                        .and_then(Option::as_ref)
                        .and_then(|value| value.distribution),
                );
                if let Some(upper) = domain.guaranteed_distinct_upper {
                    column = column.with_guaranteed_distinct_upper(upper);
                }
                Arc::new(column)
            })
            .collect()
    }
    pub fn new(
        reference_id: BoundReferenceId,
        bindings: Vec<ColumnBinding>,
        types: Vec<LogicalType>,
    ) -> Self {
        assert_eq!(bindings.len(), types.len());
        Self {
            reference_id,
            bindings,
            types,
            facts: Arc::new(BoundRelationFacts::default()),
        }
    }

    pub fn with_facts(mut self, facts: Arc<BoundRelationFacts>) -> Self {
        assert_eq!(self.bindings.len(), facts.source_lineage.len());
        self.facts = facts;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_transport_has_a_value_identity_and_never_becomes_a_bound() {
        let distribution = EstimatedNumericDistribution::normal(200.0, 30.0).unwrap();
        let column = ColumnStatistics::with_estimated_distinct(
            BaseStatistics::create_unknown(LogicalType::Double),
            None,
        )
        .with_estimated_numeric_distribution(Some(distribution));
        let values = BoundColumnValues::from_column(&column).unwrap();
        let unknown = BoundColumnValues::new(column.statistics().clone()).unwrap();
        assert_ne!(values, unknown);
        assert_eq!(values.hull(&values).unwrap(), values);
        assert_eq!(values.hull(&unknown).unwrap(), unknown);
        assert_eq!(unknown.hull(&values).unwrap(), unknown);
        let mut reference = BoundReference::new(
            BoundReferenceId::input_ordinal(0),
            vec![ColumnBinding::new(7, 0)],
            vec![LogicalType::Double],
        );
        reference.facts = Arc::new(BoundRelationFacts {
            column_values: vec![Some(values)],
            ..BoundRelationFacts::default()
        });
        let restored = reference.column_statistics().remove(0);
        assert_eq!(
            restored.estimated_numeric_distribution(),
            Some(distribution)
        );
        assert_eq!(restored.statistics().min_value(), None);
        assert_eq!(restored.statistics().max_value(), None);
        assert_eq!(restored.guaranteed_distinct_upper(), None);
    }
}
