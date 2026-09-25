// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical implementation admission and calibrated local costing.

use super::*;

mod facts;

pub(super) use facts::expression_cost_facts;

pub(super) use crate::physical::implementation::*;

pub(super) fn selected_implementation_flavor(
    id: ImplementationId,
    implementations: PlannerImplementationSet,
) -> Result<PhysicalImplementationFlavor> {
    match id {
        PLANNER_BASELINE_IMPLEMENTATION => Ok(implementations.baseline),
        PLANNER_PERFECT_HASH_AGGREGATE => Ok(PhysicalImplementationFlavor::PerfectHashAggregate),
        PLANNER_SORT_RANGE_JOIN => Ok(PhysicalImplementationFlavor::SortRangeJoin),
        PLANNER_CLASSIC_IE_JOIN => Ok(PhysicalImplementationFlavor::ClassicIeJoin),
        PLANNER_SEARCH_PROVIDER => Ok(PhysicalImplementationFlavor::SearchProvider),
        PLANNER_HASH_JOIN_RUNTIME_FILTER => Ok(PhysicalImplementationFlavor::HashJoinRuntimeFilter),
        PLANNER_HASH_JOIN_BUILD_LEFT => Ok(PhysicalImplementationFlavor::HashJoinBuildLeft),
        PLANNER_HASH_JOIN_BUILD_LEFT_RUNTIME_FILTER => {
            Ok(PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter)
        }
        PLANNER_PARTITION_AGGREGATE_WINDOW => {
            Ok(PhysicalImplementationFlavor::PartitionAggregateWindow)
        }
        PLANNER_SINGLETON_AGGREGATE_PROJECTION => {
            Ok(PhysicalImplementationFlavor::SingletonAggregateProjection)
        }
        PLANNER_EXTERNAL_CROSS_PRODUCT => Ok(PhysicalImplementationFlavor::CrossProductExternal),
        _ => Err(paro_error::internal(
            "winner references an implementation unavailable for its logical expression",
        )),
    }
}

pub(super) fn planner_region_contract(
    memo: &Memo,
    facet: Option<Fingerprint>,
) -> Result<Option<RegionCandidateContract>> {
    let Some(facet) = facet else {
        return Ok(None);
    };
    let region = memo.regions().region_for_facet(facet).ok_or_else(|| {
        paro_error::internal("physical implementation references an unowned planning facet")
    })?;
    Ok(Some(RegionCandidateContract {
        region,
        facets: vec![facet].into_boxed_slice(),
        artifacts: Box::new([]),
        artifact_dependencies: Box::new([]),
    }))
}

pub(super) fn planner_runtime_filter_region_contract(
    memo: &Memo,
    facet: Option<Fingerprint>,
    build: RegionBoundaryEndpoint,
    probe: RegionBoundaryEndpoint,
) -> Result<RegionCandidateContract> {
    let facet = facet.ok_or_else(|| {
        paro_error::internal("runtime-filter artifact has no owning planning facet")
    })?;
    let region = memo.regions().region_for_facet(facet).ok_or_else(|| {
        paro_error::internal("physical implementation references an unowned planning facet")
    })?;
    Ok(RegionCandidateContract {
        region,
        facets: vec![facet].into_boxed_slice(),
        artifacts: Box::new([RegionOwnedArtifact {
            fingerprint: facet,
            kind: RegionArtifactKind::RuntimeFilter,
        }]),
        artifact_dependencies: Box::new([RegionArtifactDependencyContract {
            artifact: facet,
            producer: build,
            consumer: probe,
            kind: RegionDependencyKind::ControlWaitComplete,
        }]),
    })
}

pub(super) use crate::cost::operator::*;

pub(super) fn is_contextual_operator(operator: &LogicalOperator) -> bool {
    matches!(
        operator,
        LogicalOperator::Join(_)
            | LogicalOperator::DependentJoin(_)
            | LogicalOperator::Aggregate(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::GraphMatch(_)
            | LogicalOperator::GraphExpand(_)
            | LogicalOperator::SearchScan(_)
            | LogicalOperator::FullTextFilterScan(_)
    )
}

pub(super) fn required_region_kind(operator: &LogicalOperator) -> Option<RegionFacetKind> {
    match operator {
        LogicalOperator::DependentJoin(_) => Some(RegionFacetKind::Parameterization),
        LogicalOperator::MaterializedCTE(_) => Some(RegionFacetKind::Sharing),
        LogicalOperator::RecursiveCTE(_) => Some(RegionFacetKind::Recursion),
        _ => None,
    }
}

pub(super) fn planner_region_facet(
    kind: RegionFacetKind,
    criticality: FacetCriticality,
    logical_identity: Fingerprint,
    operator: Fingerprint,
    owner: GroupId,
    scope: BTreeSet<GroupId>,
) -> RegionFacet {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_bytes(b"paro.planning-region.facet.v1");
    fingerprint.write_u64(kind as u64);
    fingerprint.write_u64(criticality as u64);
    // A facet belongs to a logical expression identity, not to the arena slot
    // that happened to receive it.  Transformations construct their root key
    // before the engine publishes a LogicalExprId; using the canonical key
    // fingerprint lets initial and transformed expressions participate in the
    // same physical-property search without an insertion-order dependency.
    fingerprint.write_fingerprint(logical_identity);
    fingerprint.write_fingerprint(operator);
    // RF is a capability of this join occurrence's relation, not of an
    // operator shape shared by unrelated contextual groups. Unioning those
    // anchors turns independent alternatives into one oversized region.
    // The owning group is already canonical at construction; later merges
    // recanonicalize the declaration without renaming archived artifacts.
    if kind == RegionFacetKind::RuntimeFilter {
        fingerprint.write_u64(owner.0 as u64);
    }
    RegionFacet {
        fingerprint: fingerprint.finish(),
        kind,
        criticality,
        priority: match criticality {
            FacetCriticality::Required => 100 + kind as u16,
            FacetCriticality::Optional => 1_000 + kind as u16,
        },
        scope_contract: if kind == RegionFacetKind::RuntimeFilter {
            crate::cascades::region::RegionScopeContract::OwnerWithImmediateInputs
        } else {
            crate::cascades::region::RegionScopeContract::Exact
        },
        scope,
    }
}
