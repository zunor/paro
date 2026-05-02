// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_external::abi::descriptor::ColumnDescriptor;
use paro_external::abi::encoding::{ColumnEncoding, ColumnPopulationMode};
use paro_external::abi::layout::{ColumnLayout, OffsetWidth, ScalarValueRef};
use paro_external::abi::lease::{ColumnBatchLease, LeaseOwnership, LeaseState};
use paro_external::abi::types::AbiLogicalType;

fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../runtimes/worker-common/conformance/abi")
        .join(name)
}

#[test]
fn descriptor_fixture_roundtrips() {
    let path = fixture_path("descriptor_roundtrip.json");
    let content = std::fs::read_to_string(&path).expect("read descriptor fixture");
    let lease: ColumnBatchLease = serde_json::from_str(&content).expect("decode descriptor lease");
    assert_eq!(lease.state, LeaseState::Committed);
    assert_eq!(lease.columns.len(), 3);
    assert_eq!(lease.columns[0].name, "numbers");
    assert_eq!(lease.columns[1].name, "labels");
    assert_eq!(lease.columns[2].encoding, ColumnEncoding::List);
    assert_eq!(
        lease.columns[2].logical_type,
        AbiLogicalType::List(Box::new(AbiLogicalType::Int32))
    );
    let encoded = serde_json::to_string_pretty(&lease).expect("encode descriptor lease");
    let reparsed: ColumnBatchLease = serde_json::from_str(&encoded).expect("reparse encoded lease");
    assert_eq!(reparsed, lease);
}

#[test]
fn lease_state_machine_is_explicit() {
    assert!(LeaseState::Allocated.can_transition_to(LeaseState::Writing));
    assert!(LeaseState::Writing.can_transition_to(LeaseState::Committed));
    assert!(LeaseState::Committed.can_transition_to(LeaseState::Released));
    assert!(!LeaseState::Released.can_transition_to(LeaseState::Committed));
}

#[test]
fn committed_lease_validates_descriptor_shapes() {
    let mut lease = ColumnBatchLease::new(
        42,
        4,
        LeaseOwnership {
            owner_worker_epoch: 9,
            owner_host_epoch: 7,
            owner_query_epoch: 3,
        },
    );
    lease.begin_write().expect("enter writing");
    lease
        .commit(
            99,
            Some(7),
            vec![
                ColumnDescriptor {
                    name: "scores".to_string(),
                    logical_type: AbiLogicalType::Float64,
                    encoding: ColumnEncoding::Flat,
                    population_mode: ColumnPopulationMode::Eager,
                    nullable: false,
                    validity: None,
                    layout: ColumnLayout::FixedWidth {
                        values: paro_external::abi::layout::BufferLease::host(0, 0, 32, 8),
                        stride: 8,
                    },
                    children: Vec::new(),
                },
                ColumnDescriptor {
                    name: "labels".to_string(),
                    logical_type: AbiLogicalType::Varchar,
                    encoding: ColumnEncoding::Flat,
                    population_mode: ColumnPopulationMode::Eager,
                    nullable: true,
                    validity: Some(paro_external::abi::layout::BufferLease::host(0, 32, 1, 1)),
                    layout: ColumnLayout::VarLen {
                        offsets: paro_external::abi::layout::BufferLease::host(0, 40, 20, 4),
                        data: paro_external::abi::layout::BufferLease::host(0, 64, 64, 1),
                        offset_width: OffsetWidth::U32,
                    },
                    children: Vec::new(),
                },
                ColumnDescriptor {
                    name: "step".to_string(),
                    logical_type: AbiLogicalType::Int64,
                    encoding: ColumnEncoding::Constant,
                    population_mode: ColumnPopulationMode::Eager,
                    nullable: false,
                    validity: None,
                    layout: ColumnLayout::Constant {
                        value: ScalarValueRef::Int64(10),
                    },
                    children: Vec::new(),
                },
            ],
        )
        .expect("commit lease");

    assert_eq!(lease.completion_fence, 99);
    assert!(lease.ensure_visible_to(7, 3).is_ok());
    assert!(lease.ensure_visible_to(7, 999).is_err());
}
