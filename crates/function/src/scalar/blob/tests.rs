// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tests for BLOB scalar functions

#[cfg(test)]
mod tests {
    use crate::scalar::blob::{encode_sort_key, OrderModifiers};
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    #[test]
    fn test_order_modifiers_parse() {
        let mod1 = OrderModifiers::parse("ASC NULLS LAST").unwrap();
        assert!(mod1.ascending);
        assert!(!mod1.nulls_first);

        let mod2 = OrderModifiers::parse("DESC NULLS FIRST").unwrap();
        assert!(!mod2.ascending);
        assert!(mod2.nulls_first);

        let mod3 = OrderModifiers::parse("asc nulls first").unwrap();
        assert!(mod3.ascending);
        assert!(mod3.nulls_first);

        let mod4 = OrderModifiers::parse("desc nulls last").unwrap();
        assert!(!mod4.ascending);
        assert!(!mod4.nulls_first);
    }

    #[test]
    fn test_sort_key_encode_integers() {
        // Create a chunk with integers
        let mut vec = paro_common::test_utils::test_vector(LogicalType::Integer);
        vec.set_i32(0, 1);
        vec.set_i32(1, 2);
        vec.set_i32(2, 3);

        let chunk = paro_common::test_utils::test_chunk_from_arc_vectors(vec![vec.into()]);

        let modifiers = vec![OrderModifiers {
            ascending: true,
            nulls_first: false,
        }];

        // Encode sort keys
        let key0 = encode_sort_key(&chunk, 0, &[0], &modifiers).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0], &modifiers).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0], &modifiers).unwrap();

        // Keys should be in ascending order
        assert!(key0 < key1);
        assert!(key1 < key2);
    }

    #[test]
    fn test_sort_key_encode_integers_desc() {
        // Create a chunk with integers
        let mut vec = paro_common::test_utils::test_vector(LogicalType::Integer);
        vec.set_i32(0, 1);
        vec.set_i32(1, 2);
        vec.set_i32(2, 3);

        let chunk = paro_common::test_utils::test_chunk_from_arc_vectors(vec![vec.into()]);

        let modifiers = vec![OrderModifiers {
            ascending: false, // Descending
            nulls_first: false,
        }];

        // Encode sort keys
        let key0 = encode_sort_key(&chunk, 0, &[0], &modifiers).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0], &modifiers).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0], &modifiers).unwrap();

        // Keys should be in descending order
        assert!(key0 > key1);
        assert!(key1 > key2);
    }

    #[test]
    fn test_sort_key_encode_strings() {
        // Create a chunk with strings
        let mut vec = paro_common::test_utils::test_vector(LogicalType::Varchar);
        vec.set_string(0, "apple");
        vec.set_string(1, "banana");
        vec.set_string(2, "cherry");

        let chunk = paro_common::test_utils::test_chunk_from_arc_vectors(vec![vec.into()]);

        let modifiers = vec![OrderModifiers {
            ascending: true,
            nulls_first: false,
        }];

        // Encode sort keys
        let key0 = encode_sort_key(&chunk, 0, &[0], &modifiers).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0], &modifiers).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0], &modifiers).unwrap();

        // Keys should be in ascending order
        assert!(key0 < key1);
        assert!(key1 < key2);
    }

    #[test]
    fn test_sort_key_encode_nulls() {
        // Create a chunk with NULLs
        let mut vec = paro_common::test_utils::test_vector(LogicalType::Integer);
        vec.set_i32(0, 1);
        vec.set_null(1, true);
        vec.set_i32(2, 3);

        let chunk = paro_common::test_utils::test_chunk_from_arc_vectors(vec![vec.into()]);

        // Test NULLS LAST
        let modifiers_last = vec![OrderModifiers {
            ascending: true,
            nulls_first: false,
        }];

        let key0 = encode_sort_key(&chunk, 0, &[0], &modifiers_last).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0], &modifiers_last).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0], &modifiers_last).unwrap();

        // NULL should be last
        assert!(key0 < key2);
        assert!(key2 < key1);

        // Test NULLS FIRST
        let modifiers_first = vec![OrderModifiers {
            ascending: true,
            nulls_first: true,
        }];

        let key0 = encode_sort_key(&chunk, 0, &[0], &modifiers_first).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0], &modifiers_first).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0], &modifiers_first).unwrap();

        // NULL should be first
        assert!(key1 < key0);
        assert!(key0 < key2);
    }

    #[test]
    fn test_sort_key_encode_multi_column() {
        // Create a chunk with two columns
        let mut vec1 = paro_common::test_utils::test_vector(LogicalType::Integer);
        vec1.set_i32(0, 1);
        vec1.set_i32(1, 1);
        vec1.set_i32(2, 2);

        let mut vec2 = paro_common::test_utils::test_vector(LogicalType::Varchar);
        vec2.set_string(0, "b");
        vec2.set_string(1, "a");
        vec2.set_string(2, "c");

        let chunk = Chunk::from_arc_vectors(
            vec![vec1.into(), vec2.into()],
            paro_common::test_utils::test_allocator(),
        );

        let modifiers = vec![
            OrderModifiers {
                ascending: true,
                nulls_first: false,
            },
            OrderModifiers {
                ascending: true,
                nulls_first: false,
            },
        ];

        // Encode sort keys
        let key0 = encode_sort_key(&chunk, 0, &[0, 1], &modifiers).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0, 1], &modifiers).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0, 1], &modifiers).unwrap();

        // Row 1 (1, "a") should come before row 0 (1, "b")
        assert!(key1 < key0);
        // Row 0 (1, "b") should come before row 2 (2, "c")
        assert!(key0 < key2);
    }

    #[test]
    fn test_sort_key_encode_decimal_values() {
        let decimal_type = LogicalType::Decimal {
            precision: 6,
            scale: 2,
        };
        let mut vec = paro_common::test_utils::test_vector(decimal_type.clone());
        vec.set_value(0, &Value::Decimal(-125, 6, 2));
        vec.set_value(1, &Value::Decimal(250, 6, 2));
        vec.set_value(2, &Value::Decimal(850, 6, 2));

        let chunk = paro_common::test_utils::test_chunk_from_arc_vectors(vec![vec.into()]);
        let modifiers = vec![OrderModifiers {
            ascending: true,
            nulls_first: false,
        }];

        let key0 = encode_sort_key(&chunk, 0, &[0], &modifiers).unwrap();
        let key1 = encode_sort_key(&chunk, 1, &[0], &modifiers).unwrap();
        let key2 = encode_sort_key(&chunk, 2, &[0], &modifiers).unwrap();

        assert!(key0 < key1);
        assert!(key1 < key2);
    }
}
