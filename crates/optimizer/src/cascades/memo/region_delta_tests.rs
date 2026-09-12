// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility};
use crate::cascades::ids::ColumnId;
use crate::cascades::region::{FacetCriticality, RegionFacetKind, RegionScopeContract};
use paro_common::types::LogicalType;

#[test]
fn region_delta_matches_manual_reduction_across_three_orders() {
    for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
        let mut memo = Memo::new(SearchBudget::default());
        let groups: [GroupId; 3] = std::array::from_fn(|_| {
            memo.create_group(
                GroupSchema::new([ColumnDesc {
                    id: ColumnId(1),
                    logical_type: LogicalType::Integer,
                    nullable: false,
                    origin: ColumnOrigin::Derived {
                        key: Fingerprint(1),
                    },
                    visibility: ColumnVisibility::Visible,
                    name_hint: None,
                }])
                .unwrap(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            )
        });
        let facet = |priority, scope: &[GroupId]| RegionFacet {
            fingerprint: Fingerprint(100),
            kind: RegionFacetKind::RuntimeFilter,
            criticality: FacetCriticality::Optional,
            priority,
            scope_contract: RegionScopeContract::OwnerWithImmediateInputs,
            scope: scope.iter().copied().collect(),
        };
        let initial = facet(90, &groups[..1]);
        memo.upsert_region_facet(initial.clone()).unwrap();
        let updates = [
            initial.clone(),         // Exact duplicate.
            facet(90, &groups[1..]), // Scope-only delta.
            facet(10, &groups[..1]), // Priority-only delta.
        ];

        // Independent, manually reduced result: one unchanged contract,
        // scope {g0, g1, g2}, and min(90, 90, 10) priority. Only admission
        // uses the existing normalizer; no upsert/delta helper is the oracle.
        let expected = RegionForest::normalize(
            [facet(10, &groups)],
            usize::from(memo.budget().max_composite_region_groups),
            memo.budget().max_mandatory_region_groups as usize,
        )
        .unwrap();
        let dropped = memo
            .upsert_region_facets(order.map(|index| updates[index].clone()))
            .unwrap();
        assert_eq!(memo.regions(), &expected, "update order {order:?}");
        assert_eq!(dropped, expected.dropped_optional_facets);

        // Replaying equal/subset scopes with equal/weaker priorities must
        // remain a no-op, including a duplicate of the pre-update facet.
        let before = memo.regions.clone();
        let dropped = memo
            .upsert_region_facets([initial, facet(20, &groups[2..]), facet(10, &groups)])
            .unwrap();
        assert!(dropped.is_empty());
        assert_eq!(memo.regions(), &expected, "replay order {order:?}");
        assert!(std::sync::Arc::ptr_eq(&before, &memo.regions));
    }
}

fn memo_groups<const N: usize>() -> (Memo, [GroupId; N]) {
    memo_groups_with_budget(SearchBudget::default())
}

fn memo_groups_with_budget<const N: usize>(budget: SearchBudget) -> (Memo, [GroupId; N]) {
    let mut memo = Memo::new(budget);
    let groups = std::array::from_fn(|_| {
        memo.create_group(
            GroupSchema::new([ColumnDesc {
                id: ColumnId(1),
                logical_type: LogicalType::Integer,
                nullable: false,
                origin: ColumnOrigin::Derived {
                    key: Fingerprint(1),
                },
                visibility: ColumnVisibility::Visible,
                name_hint: None,
            }])
            .unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        )
    });
    (memo, groups)
}

fn rf(identity: u128, priority: u16, scope: &[GroupId]) -> RegionFacet {
    RegionFacet {
        fingerprint: Fingerprint(identity),
        kind: RegionFacetKind::RuntimeFilter,
        criticality: FacetCriticality::Optional,
        priority,
        scope_contract: RegionScopeContract::OwnerWithImmediateInputs,
        scope: scope.iter().copied().collect(),
    }
}

#[test]
fn conflicting_late_delta_cannot_publish_an_earlier_change() {
    let (mut memo, groups) = memo_groups::<2>();
    memo.upsert_region_facet(rf(1, 20, &groups[..1])).unwrap();
    let before = memo.regions.clone();
    for (kind, criticality, scope_contract) in [
        (
            RegionFacetKind::Sharing,
            FacetCriticality::Optional,
            RegionScopeContract::Exact,
        ),
        (
            RegionFacetKind::RuntimeFilter,
            FacetCriticality::Required,
            RegionScopeContract::OwnerWithImmediateInputs,
        ),
        (
            RegionFacetKind::RuntimeFilter,
            FacetCriticality::Optional,
            RegionScopeContract::Exact,
        ),
    ] {
        let mut conflicting = rf(1, 10, &groups);
        conflicting.kind = kind;
        conflicting.criticality = criticality;
        conflicting.scope_contract = scope_contract;
        assert!(memo
            .upsert_region_facets([rf(1, 10, &groups), conflicting])
            .is_err());
        assert!(Arc::ptr_eq(&before, &memo.regions));
    }
}

#[test]
fn region_delta_observes_rollback_and_group_canonicalization() {
    let (mut memo, [left, right, extra]) = memo_groups();
    memo.upsert_region_facet(rf(1, 20, &[right])).unwrap();
    let before = memo.regions.clone();
    let savepoint = memo.transformation_savepoint();
    memo.upsert_region_facet(rf(1, 10, &[right, extra]))
        .unwrap();
    assert!(!Arc::ptr_eq(&before, &memo.regions));
    memo.rollback_transformation(savepoint).unwrap();
    assert!(Arc::ptr_eq(&before, &memo.regions));
    let canonical = memo.merge_groups(left, right).unwrap();
    let merged = memo.regions.clone();
    memo.upsert_region_facet(rf(1, 30, &[right])).unwrap();
    assert!(Arc::ptr_eq(&merged, &memo.regions));
    memo.upsert_region_facet(rf(1, 10, &[right, extra]))
        .unwrap();
    let expected = RegionForest::normalize(
        [rf(1, 10, &[canonical, extra])],
        usize::from(memo.budget.max_composite_region_groups),
        memo.budget.max_mandatory_region_groups as usize,
    )
    .unwrap();
    assert_eq!(memo.regions(), &expected);
}

#[test]
fn no_change_and_new_delta_both_retain_previously_dropped_facets() {
    let (mut memo, groups) = memo_groups_with_budget::<3>(SearchBudget {
        max_composite_region_groups: 2,
        ..SearchBudget::default()
    });
    memo.upsert_region_facet(rf(1, 20, &groups[..1])).unwrap();
    let dropped = memo.upsert_region_facet(rf(2, 20, &groups)).unwrap();
    assert_eq!(dropped.as_ref(), &[Fingerprint(2)]);
    let before = memo.regions.clone();
    assert_eq!(
        memo.upsert_region_facets([rf(1, 30, &groups[..1])])
            .unwrap(),
        dropped
    );
    assert!(Arc::ptr_eq(&before, &memo.regions));
    assert_eq!(
        memo.upsert_region_facet(rf(1, 10, &groups[..1])).unwrap(),
        dropped
    );
    assert_eq!(memo.regions.dropped_optional_facets, dropped);
    assert_eq!(memo.regions.nodes[0].facets[0].priority, 10);
}
