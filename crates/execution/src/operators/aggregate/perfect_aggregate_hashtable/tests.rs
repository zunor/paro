// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use super::{
    compute_slot_from_prepared, decode_group_batch, prepare_slot_encoding, PerfectHashKeyDomain,
    PerfectHashSlotLayout, PreparedDictionaryKey, PreparedSlotEncoding,
};

#[test]
fn mixed_radix_layout_is_dense_and_round_trips_every_component() {
    let layout = PerfectHashSlotLayout::try_new(vec![3, 5, 2]).expect("layout");
    assert_eq!(layout.strides, vec![10, 2, 1]);
    assert_eq!(layout.slot_count, 30);

    let mut slots = HashSet::new();
    for first in 0..3 {
        for second in 0..5 {
            for third in 0..2 {
                let mut slot = 0;
                for (group_idx, encoded) in [first, second, third].into_iter().enumerate() {
                    slot = layout
                        .add_component(slot, group_idx, encoded)
                        .expect("component");
                }
                assert!(slots.insert(slot));
                assert_eq!(layout.decode_component(slot, 0), first);
                assert_eq!(layout.decode_component(slot, 1), second);
                assert_eq!(layout.decode_component(slot, 2), third);
            }
        }
    }
    assert_eq!(slots.len(), layout.slot_count);
}

#[test]
fn mixed_radix_layout_rejects_invalid_domains_and_components() {
    assert!(PerfectHashSlotLayout::try_new(vec![1]).is_err());
    let layout = PerfectHashSlotLayout::try_new(vec![3]).expect("layout");
    assert!(layout.add_component(0, 0, 3).is_err());
}

#[test]
fn dictionary_key_codec_ignores_unreferenced_physical_values() {
    let child = Arc::new(paro_common::test_utils::test_string_vector(&[
        "A",
        "not-a-single-byte-key",
        "B",
    ]));
    let dictionary = paro_common::test_utils::test_dictionary(child, vec![0_u32, 2, 0, 2]);
    let decoded = dictionary.try_decode_ref(4).expect("dictionary decode");
    let domain = PerfectHashKeyDomain::try_new(LogicalType::Varchar).expect("domain");
    let mut prepared = PreparedDictionaryKey::try_new(&domain, &decoded, 4, 66, 3, 0)
        .expect("prepare key")
        .expect("small dictionary key");

    assert_eq!(prepared.encoded(0).unwrap(), 1);
    assert_eq!(prepared.encoded(1).unwrap(), 2);
}

#[test]
fn dictionary_key_codec_is_not_limited_by_q1_cardinality() {
    let values = (b'A'..=b'T')
        .map(|value| char::from(value).to_string())
        .collect::<Vec<_>>();
    let references = values.iter().map(String::as_str).collect::<Vec<_>>();
    let child = Arc::new(paro_common::test_utils::test_string_vector(&references));
    let selection = (0..40)
        .map(|row| if row % 2 == 0 { 0_u32 } else { 19_u32 })
        .collect::<Vec<_>>();
    let dictionary = paro_common::test_utils::test_dictionary(child, selection);
    let decoded = dictionary.try_decode_ref(40).expect("dictionary decode");
    let domain = PerfectHashKeyDomain::try_new(LogicalType::Varchar).expect("domain");
    let mut prepared = PreparedDictionaryKey::try_new(&domain, &decoded, 40, 66, 21, 0)
        .expect("prepare key")
        .expect("dictionary key");

    assert_eq!(prepared.encoded(0).unwrap(), 1);
    assert_eq!(prepared.encoded(1).unwrap(), 20);
}

#[test]
fn two_key_slot_encoding_roundtrips_dictionary_and_flat_values() {
    let first_values = ["R", "A", "N", "R"];
    let second_values = ["O", "F", "F", "O"];

    let first_child = Arc::new(paro_common::test_utils::test_string_vector(&[
        "R", "A", "N",
    ]));
    let second_child = Arc::new(paro_common::test_utils::test_string_vector(&["O", "F"]));
    let dictionary_groups = Chunk::from_arc_vectors(
        vec![
            Arc::new(paro_common::test_utils::test_dictionary(
                first_child,
                vec![0, 1, 2, 0],
            )),
            Arc::new(paro_common::test_utils::test_dictionary(
                second_child,
                vec![0, 1, 1, 0],
            )),
        ],
        paro_common::test_utils::test_allocator(),
    );
    assert_two_key_roundtrip(&dictionary_groups, &first_values, &second_values, true);

    let flat_groups = Chunk::from_arc_vectors(
        vec![
            Arc::new(paro_common::test_utils::test_string_vector(&first_values)),
            Arc::new(paro_common::test_utils::test_string_vector(&second_values)),
        ],
        paro_common::test_utils::test_allocator(),
    );
    assert_two_key_roundtrip(&flat_groups, &first_values, &second_values, false);
}

fn assert_two_key_roundtrip(
    groups: &Chunk,
    first_values: &[&str],
    second_values: &[&str],
    expect_dictionary_pair: bool,
) {
    let domains = vec![
        PerfectHashKeyDomain::try_new(LogicalType::Varchar).unwrap(),
        PerfectHashKeyDomain::try_new(LogicalType::Varchar).unwrap(),
    ];
    // Single-byte VARCHAR encodes byte + 1. These ranges cover A..R and
    // F..O respectively, with component zero reserved for NULL.
    let minima = vec![i128::from(b'A') + 1, i128::from(b'F') + 1];
    let layout = PerfectHashSlotLayout::try_new(vec![19, 11]).unwrap();
    let decoded = decode_group_batch(groups, 2).unwrap();
    let mut prepared =
        prepare_slot_encoding(&domains, &minima, &layout, &decoded, groups.size()).unwrap();
    assert_eq!(
        matches!(prepared, PreparedSlotEncoding::DictionaryPair { .. }),
        expect_dictionary_pair
    );

    for row in 0..groups.size() {
        let slot =
            compute_slot_from_prepared(&domains, &minima, &layout, &decoded, &mut prepared, row)
                .unwrap();
        for (group_idx, expected) in [first_values[row], second_values[row]]
            .into_iter()
            .enumerate()
        {
            let component = layout.decode_component(slot, group_idx);
            let encoded = minima[group_idx] + i128::try_from(component).unwrap() - 1;
            assert_eq!(
                domains[group_idx].value_from_encoded(encoded).unwrap(),
                Value::Varchar(expected.to_string())
            );
        }
    }
}
