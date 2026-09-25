// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Required/provided physical property lattice and canonical interning.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::identity::{
    BaseRelationId, CollationId, ColumnId, FactorizationSpecId, LocatorKindId, MutationBarrierId,
    QualityPolicyId, SnapshotId, StableReadProofId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NullOrder {
    First,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderingKey {
    pub column: ColumnId,
    pub direction: SortDirection,
    pub nulls: NullOrder,
    pub collation: Option<CollationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrderingScope {
    PartitionLocal,
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequiredOrdering {
    pub keys: Box<[OrderingKey]>,
    pub scope: OrderingScope,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrderingRequirement {
    Any,
    Ordered(RequiredOrdering),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvidedOrdering {
    Unordered,
    Ordered {
        keys: Box<[OrderingKey]>,
        scope: OrderingScope,
    },
}

impl ProvidedOrdering {
    pub fn satisfies(&self, required: &OrderingRequirement) -> bool {
        match required {
            OrderingRequirement::Any => true,
            OrderingRequirement::Ordered(required) => {
                let Self::Ordered { keys, scope } = self else {
                    return false;
                };
                let scope_satisfies = *scope == OrderingScope::Global
                    || required.scope == OrderingScope::PartitionLocal;
                scope_satisfies
                    && keys.len() >= required.keys.len()
                    && keys[..required.keys.len()] == *required.keys
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PartitioningRequirement {
    Any,
    Singleton,
    Hash {
        keys: Box<[ColumnId]>,
        partitions: Option<u16>,
    },
    Range {
        keys: Box<[ColumnId]>,
        partitions: Option<u16>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvidedPartitioning {
    Singleton,
    Hash {
        keys: Box<[ColumnId]>,
        partitions: u16,
    },
    Range {
        keys: Box<[ColumnId]>,
        partitions: u16,
    },
    RoundRobin {
        partitions: u16,
    },
    Unknown,
}

impl ProvidedPartitioning {
    pub fn satisfies(&self, required: &PartitioningRequirement) -> bool {
        match required {
            PartitioningRequirement::Any => true,
            PartitioningRequirement::Singleton => matches!(self, Self::Singleton),
            PartitioningRequirement::Hash { keys, partitions } => match self {
                Self::Hash {
                    keys: provided,
                    partitions: provided_count,
                } => {
                    provided == keys
                        && partitions
                            .map(|required_count| required_count == *provided_count)
                            .unwrap_or(true)
                }
                _ => false,
            },
            PartitioningRequirement::Range { keys, partitions } => match self {
                Self::Range {
                    keys: provided,
                    partitions: provided_count,
                } => {
                    provided == keys
                        && partitions
                            .map(|required_count| required_count == *provided_count)
                            .unwrap_or(true)
                }
                _ => false,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LocatorUse {
    ReadStable,
    WriteTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocatorRequirement {
    pub kind: Option<LocatorKindId>,
    pub use_kind: LocatorUse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocatorDescriptor {
    pub kind: LocatorKindId,
    pub snapshot: SnapshotId,
    pub write_target: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializationRequirement {
    pub values: BTreeSet<ColumnId>,
    pub locators: BTreeMap<BaseRelationId, LocatorRequirement>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProvidedMaterialization {
    pub values: BTreeSet<ColumnId>,
    pub locators: BTreeMap<BaseRelationId, LocatorDescriptor>,
}

impl ProvidedMaterialization {
    fn satisfies(&self, required: &MaterializationRequirement) -> bool {
        required.values.is_subset(&self.values)
            && required.locators.iter().all(|(relation, requirement)| {
                self.locators.get(relation).is_some_and(|provided| {
                    requirement
                        .kind
                        .map(|kind| kind == provided.kind)
                        .unwrap_or(true)
                        && (requirement.use_kind != LocatorUse::WriteTarget
                            || provided.write_target)
                })
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MutationSafetyRequirement {
    None,
    StableReadBeforeWrite {
        targets: BTreeSet<BaseRelationId>,
        snapshot: SnapshotId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvidedMutationSafety {
    NotApplicable,
    SnapshotStable {
        targets: BTreeSet<BaseRelationId>,
        snapshot: SnapshotId,
        proof: StableReadProofId,
    },
    MaterializedMutationInput {
        targets: BTreeSet<BaseRelationId>,
        snapshot: SnapshotId,
        barrier: MutationBarrierId,
    },
}

impl ProvidedMutationSafety {
    pub fn satisfies(&self, required: &MutationSafetyRequirement) -> bool {
        match required {
            MutationSafetyRequirement::None => true,
            MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } => match self {
                Self::SnapshotStable {
                    targets: provided,
                    snapshot: provided_snapshot,
                    ..
                }
                | Self::MaterializedMutationInput {
                    targets: provided,
                    snapshot: provided_snapshot,
                    ..
                } => provided_snapshot == snapshot && targets.is_subset(provided),
                Self::NotApplicable => false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RepresentationRequirement {
    Any,
    Flat,
    Factorized(FactorizationSpecId),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvidedRepresentation {
    Flat,
    Factorized(FactorizationSpecId),
}

impl ProvidedRepresentation {
    pub fn satisfies(&self, required: &RepresentationRequirement) -> bool {
        match required {
            RepresentationRequirement::Any => true,
            RepresentationRequirement::Flat => matches!(self, Self::Flat),
            RepresentationRequirement::Factorized(required) => {
                matches!(self, Self::Factorized(provided) if provided == required)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReplayabilityRequirement {
    Any,
    Rewindable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvidedReplayability {
    OnePass,
    Rewindable,
}

impl ProvidedReplayability {
    pub fn satisfies(self, required: ReplayabilityRequirement) -> bool {
        required == ReplayabilityRequirement::Any || self == Self::Rewindable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResultGuarantee {
    Exact,
    ApproximateAllowed(QualityPolicyId),
}

impl ResultGuarantee {
    pub fn satisfies(self, required: Self) -> bool {
        match (self, required) {
            (Self::Exact, _) => true,
            (Self::ApproximateAllowed(provided), Self::ApproximateAllowed(required)) => {
                provided == required
            }
            (Self::ApproximateAllowed(_), Self::Exact) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequiredProperties {
    pub ordering: OrderingRequirement,
    pub partitioning: PartitioningRequirement,
    pub materialization: MaterializationRequirement,
    pub mutation_safety: MutationSafetyRequirement,
    pub representation: RepresentationRequirement,
    pub replayability: ReplayabilityRequirement,
    pub result_guarantee: ResultGuarantee,
}

impl Default for RequiredProperties {
    fn default() -> Self {
        Self {
            ordering: OrderingRequirement::Any,
            partitioning: PartitioningRequirement::Any,
            materialization: MaterializationRequirement::default(),
            mutation_safety: MutationSafetyRequirement::None,
            representation: RepresentationRequirement::Any,
            replayability: ReplayabilityRequirement::Any,
            result_guarantee: ResultGuarantee::Exact,
        }
    }
}

impl RequiredProperties {
    pub fn validate(&self) -> Result<()> {
        if matches!(
            self.ordering,
            OrderingRequirement::Ordered(RequiredOrdering {
                scope: OrderingScope::Global,
                ..
            })
        ) && matches!(
            self.partitioning,
            PartitioningRequirement::Hash { .. } | PartitioningRequirement::Range { .. }
        ) {
            return Err(paro_error::internal(
                "global ordering and non-singleton partitioning cannot be required together",
            ));
        }
        if !matches!(self.ordering, OrderingRequirement::Any)
            && matches!(
                self.representation,
                RepresentationRequirement::Factorized(_)
            )
        {
            return Err(paro_error::internal(
                "ordered factorized output needs an explicit factor-aware property",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProvidedProperties {
    pub ordering: ProvidedOrdering,
    pub partitioning: ProvidedPartitioning,
    pub materialization: ProvidedMaterialization,
    pub mutation_safety: ProvidedMutationSafety,
    pub representation: ProvidedRepresentation,
    pub replayability: ProvidedReplayability,
    pub result_guarantee: ResultGuarantee,
}

impl ProvidedProperties {
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        self.ordering.satisfies(&required.ordering)
            && self.partitioning.satisfies(&required.partitioning)
            && self.materialization.satisfies(&required.materialization)
            && self.mutation_safety.satisfies(&required.mutation_safety)
            && self.representation.satisfies(&required.representation)
            && self.replayability.satisfies(required.replayability)
            && self.result_guarantee.satisfies(required.result_guarantee)
    }

    pub fn validate(&self) -> Result<()> {
        if matches!(
            self.ordering,
            ProvidedOrdering::Ordered {
                scope: OrderingScope::Global,
                ..
            }
        ) && matches!(
            self.partitioning,
            ProvidedPartitioning::Hash { .. }
                | ProvidedPartitioning::Range { .. }
                | ProvidedPartitioning::RoundRobin { .. }
                | ProvidedPartitioning::Unknown
        ) {
            return Err(paro_error::internal(
                "global ordering cannot be paired with non-singleton partitioning",
            ));
        }
        if matches!(self.representation, ProvidedRepresentation::Factorized(_))
            && !matches!(self.ordering, ProvidedOrdering::Unordered)
        {
            return Err(paro_error::internal(
                "factorized representation needs an explicit factor-aware ordering contract",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(column: u32) -> OrderingKey {
        OrderingKey {
            column: ColumnId(column),
            direction: SortDirection::Asc,
            nulls: NullOrder::Last,
            collation: None,
        }
    }

    #[test]
    fn ordering_uses_prefix_and_scope_semantics() {
        let provided = ProvidedOrdering::Ordered {
            keys: vec![order(1), order(2)].into_boxed_slice(),
            scope: OrderingScope::Global,
        };
        assert!(
            provided.satisfies(&OrderingRequirement::Ordered(RequiredOrdering {
                keys: vec![order(1)].into_boxed_slice(),
                scope: OrderingScope::Global,
            }))
        );
        assert!(
            !provided.satisfies(&OrderingRequirement::Ordered(RequiredOrdering {
                keys: vec![order(2)].into_boxed_slice(),
                scope: OrderingScope::Global,
            }))
        );
    }

    #[test]
    fn exact_can_satisfy_approximate_but_not_reverse() {
        assert!(ResultGuarantee::Exact
            .satisfies(ResultGuarantee::ApproximateAllowed(QualityPolicyId(1))));
        assert!(!ResultGuarantee::ApproximateAllowed(QualityPolicyId(1))
            .satisfies(ResultGuarantee::Exact));
    }

    #[test]
    fn mutation_safety_requires_same_snapshot_and_all_targets() {
        let provided = ProvidedMutationSafety::MaterializedMutationInput {
            targets: [BaseRelationId(1), BaseRelationId(2)].into_iter().collect(),
            snapshot: SnapshotId(7),
            barrier: MutationBarrierId(3),
        };
        assert!(
            provided.satisfies(&MutationSafetyRequirement::StableReadBeforeWrite {
                targets: [BaseRelationId(2)].into_iter().collect(),
                snapshot: SnapshotId(7),
            })
        );
        assert!(
            !provided.satisfies(&MutationSafetyRequirement::StableReadBeforeWrite {
                targets: [BaseRelationId(2)].into_iter().collect(),
                snapshot: SnapshotId(8),
            })
        );
    }

    #[test]
    fn invalid_cross_dimension_combination_is_rejected() {
        let properties = ProvidedProperties {
            ordering: ProvidedOrdering::Ordered {
                keys: vec![order(1)].into_boxed_slice(),
                scope: OrderingScope::Global,
            },
            partitioning: ProvidedPartitioning::Hash {
                keys: vec![ColumnId(1)].into_boxed_slice(),
                partitions: 4,
            },
            materialization: ProvidedMaterialization::default(),
            mutation_safety: ProvidedMutationSafety::NotApplicable,
            representation: ProvidedRepresentation::Flat,
            replayability: ProvidedReplayability::OnePass,
            result_guarantee: ResultGuarantee::Exact,
        };
        assert!(properties.validate().is_err());
    }
}
