// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Execution-time row storage.
//!
//! This module is the sealed row-buffer kernel used by operators while a query is
//! running. It is intentionally separate from `crate::rowset`, which is the
//! persistent Tablet Rowset/Segment storage format. A `RowStoreBuilder` owns
//! append-time regions; sealing it produces an immutable `RowStore` whose
//! `RowAddr` handles stay stable for the store lifetime.

mod addr;
mod block;
mod builder;
pub mod codec;
mod layout;
mod partition;
mod pin;
mod pinned;
mod raw;
mod region;
mod scan;
mod store;

pub use addr::RowAddr;
pub use builder::{RowAppender, RowStoreBuilder};
pub use layout::{RowLayout, RowValidityType};
pub use partition::{RadixPartitionedRows, RadixPartitionedRowsBuilder, RadixPartitioning};
pub use pinned::{PinnedRow, PinnedRows};
pub use scan::{RowScanCursor, RowScanState};
pub use store::{Ordering, PrefixReleasableRowStore, ReclaimableRowStore, RowStore};

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use paro_common::allocator::{Allocator, DefaultAllocator};
    use paro_common::chunk::Chunk;
    use paro_common::memory::{
        GrantAllocator, MemoryAccountingClass, MemoryAccumulator, MemoryDomain, MemoryGrant,
        MemoryOwner, MemoryResult,
    };
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::VECTOR_SIZE;

    use super::*;
    use crate::buffer::{BufferPool, MemoryTag};
    use crate::row::codec::{ColumnCodec, StructCodec, VarlenCodec};

    fn input_chunk() -> Chunk {
        let mut ids = test_vector_with_capacity(LogicalType::Integer, 4);
        ids.set_i32(0, 10);
        ids.set_i32(1, 20);
        ids.set_i32(2, 30);
        ids.set_i32(3, 40);
        ids.set_count(4);

        let mut names = test_vector_with_capacity(LogicalType::Varchar, 4);
        names.set_string(0, "ten");
        names.set_string(1, "twenty");
        names.set_string(2, "thirty");
        names.set_string(3, "forty");
        names.set_count(4);

        test_chunk_from_vectors(vec![ids, names])
    }

    fn single_int_chunk(value: i32) -> Chunk {
        let mut ids = test_vector_with_capacity(LogicalType::Integer, 1);
        ids.set_i32(0, value);
        ids.set_count(1);
        test_chunk_from_vectors(vec![ids])
    }

    fn build_store() -> RowStore {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let mut builder = RowStoreBuilder::from_types(
            pool,
            vec![LogicalType::Integer, LogicalType::Varchar],
            MemoryTag::HashTable,
        );
        builder.append(&input_chunk()).unwrap();
        builder.seal()
    }

    fn build_large_store(row_count: usize) -> RowStore {
        let pool = Arc::new(BufferPool::new(32 * 1024 * 1024));
        let mut builder =
            RowStoreBuilder::from_types(pool, vec![LogicalType::Integer], MemoryTag::HashTable);
        let mut ids = test_vector_with_capacity(LogicalType::Integer, row_count);
        for idx in 0..row_count {
            ids.set_i32(idx, idx as i32);
        }
        ids.set_count(row_count);
        builder.append(&test_chunk_from_vectors(vec![ids])).unwrap();
        builder.seal()
    }

    #[derive(Debug, Default)]
    struct TestMemoryOwner {
        issued: AtomicUsize,
        used: AtomicUsize,
        revocable: AtomicUsize,
    }

    impl MemoryOwner for TestMemoryOwner {
        fn acquire_capacity(&self, _domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
            self.issued.fetch_add(bytes, AtomicOrdering::SeqCst);
            Ok(())
        }

        fn release_capacity(&self, _domain: MemoryDomain, bytes: usize) {
            self.issued.fetch_sub(bytes, AtomicOrdering::SeqCst);
        }

        fn record_allocation(
            &self,
            _domain: MemoryDomain,
            _tag: MemoryTag,
            class: MemoryAccountingClass,
            bytes: usize,
        ) {
            self.used.fetch_add(bytes, AtomicOrdering::SeqCst);
            if class == MemoryAccountingClass::Revocable {
                self.revocable.fetch_add(bytes, AtomicOrdering::SeqCst);
            }
        }

        fn release_allocation(
            &self,
            _domain: MemoryDomain,
            _tag: MemoryTag,
            class: MemoryAccountingClass,
            bytes: usize,
        ) {
            self.used.fetch_sub(bytes, AtomicOrdering::SeqCst);
            if class == MemoryAccountingClass::Revocable {
                self.revocable.fetch_sub(bytes, AtomicOrdering::SeqCst);
            }
        }
    }

    #[test]
    fn row_store_builder_grant_allocator_accounts_physical_blocks() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let owner = Arc::new(TestMemoryOwner::default());
        let grant = MemoryGrant::new(0, MemoryDomain::Host, owner.clone()).unwrap();
        let accumulator = MemoryAccumulator::default();
        let allocator: Arc<dyn Allocator> = Arc::new(DefaultAllocator::new());
        let grant_allocator = GrantAllocator::new(
            &allocator,
            &grant,
            &accumulator,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );

        {
            let mut builder = RowStoreBuilder::from_types_with_grant_allocator(
                pool,
                vec![LogicalType::Integer],
                MemoryTag::HashTable,
                grant_allocator,
            );
            builder.append(&single_int_chunk(42)).unwrap();

            assert!(owner.issued.load(AtomicOrdering::SeqCst) > 0);
            assert_eq!(
                owner.used.load(AtomicOrdering::SeqCst),
                owner.issued.load(AtomicOrdering::SeqCst)
            );
            assert_eq!(
                owner.revocable.load(AtomicOrdering::SeqCst),
                owner.used.load(AtomicOrdering::SeqCst)
            );

            let store = builder.seal();
            assert_eq!(store.count(), 1);
            drop(store);
        }

        assert_eq!(owner.used.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(owner.issued.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(owner.revocable.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn sealed_store_pins_ordinals_in_caller_order() {
        let store = build_store();
        let pinned = store
            .pin_ordinals(&[2, 0, 3, 1], Ordering::Arbitrary)
            .unwrap();

        let mut output = test_chunk_with_capacity(store.layout().types(), 4);
        pinned.gather_columns(&[0, 1], &mut output, 0).unwrap();

        assert_eq!(output.get_value(0, 0), Some(Value::Integer(30)));
        assert_eq!(
            output.get_value(1, 0),
            Some(Value::Varchar("thirty".to_string()))
        );
        assert_eq!(output.get_value(0, 1), Some(Value::Integer(10)));
        assert_eq!(
            output.get_value(1, 1),
            Some(Value::Varchar("ten".to_string()))
        );
        assert_eq!(output.get_value(0, 2), Some(Value::Integer(40)));
        assert_eq!(output.get_value(0, 3), Some(Value::Integer(20)));
    }

    #[test]
    fn row_addrs_are_stable_in_addressable_store() {
        let store = build_store();
        let addr = store.addr_at_ordinal(1).unwrap();
        assert!(!addr.is_invalid());

        let pinned = store.pin_rows(&[addr]).unwrap();
        let row = pinned.row(0).unwrap();
        assert_eq!(row.addr(), addr);
        assert_eq!(row.read_value(0).unwrap(), Value::Integer(20));
    }

    #[test]
    fn prefix_releasable_waits_for_pins_before_physical_frontier_moves() {
        let store = build_large_store(70_000).into_prefix_releasable();
        let first_block_rows = store.scan_chunks()[0].row_count as u64;
        let pinned = store.pin_ordinal_range(0, 1).unwrap();
        store.advance_release_frontier(first_block_rows).unwrap();

        assert_eq!(store.logical_release_frontier(), first_block_rows);
        assert_eq!(store.physical_release_frontier(), 0);
        assert_eq!(store.outstanding_pins(), 1);

        drop(pinned);

        assert_eq!(store.physical_release_frontier(), first_block_rows);
        assert!(store.pin_ordinal_range(0, 1).is_err());
        assert!(store.pin_ordinal_range(first_block_rows as u32, 1).is_ok());
    }

    #[test]
    fn empty_prefix_pin_does_not_block_release() {
        let store = build_store().into_prefix_releasable();
        let empty = store.pin_ordinal_range(0, 0).unwrap();

        assert_eq!(empty.len(), 0);
        assert_eq!(store.outstanding_pins(), 0);

        store.advance_release_frontier(store.count()).unwrap();
        assert_eq!(store.physical_release_frontier(), store.count());
    }

    #[test]
    fn scanner_reads_sealed_store() {
        let store = build_store();
        let mut scanner = store.scanner();
        let mut chunk = test_chunk_with_capacity(store.layout().types(), 4);
        let count = scanner.next_chunk(&mut chunk).unwrap();

        assert_eq!(count, 4);
        assert_eq!(chunk.get_value(0, 3), Some(Value::Integer(40)));
        assert_eq!(scanner.next_chunk(&mut chunk).unwrap(), 0);
    }

    #[test]
    fn merge_builders_preserves_input_builder_order() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let mut left = RowStoreBuilder::from_types(
            Arc::clone(&pool),
            vec![LogicalType::Integer],
            MemoryTag::HashTable,
        );
        let mut right =
            RowStoreBuilder::from_types(pool, vec![LogicalType::Integer], MemoryTag::HashTable);

        left.append(&single_int_chunk(1)).unwrap();
        right.append(&single_int_chunk(2)).unwrap();

        let store = RowStoreBuilder::merge_builders(vec![left, right]).seal();
        let pinned = store.pin_ordinal_range(0, 2).unwrap();
        let mut output = test_chunk_with_capacity(store.layout().types(), 2);
        pinned.gather_columns(&[0], &mut output, 0).unwrap();

        assert_eq!(output.get_value(0, 0), Some(Value::Integer(1)));
        assert_eq!(output.get_value(0, 1), Some(Value::Integer(2)));
    }

    #[test]
    fn reclaimable_store_reclaims_after_full_scan_chunk() {
        let store = build_large_store(70_000).into_reclaimable();
        let first_block_rows = store.scan_chunks()[0].row_count as usize;
        let mut scanner = store.scanner();
        let mut chunk = test_chunk_with_capacity(store.layout().types(), VECTOR_SIZE);
        let mut consumed = 0usize;

        while consumed < first_block_rows {
            consumed += scanner.next_chunk(&mut chunk).unwrap();
        }

        assert_eq!(consumed, first_block_rows);
        assert_eq!(store.reclaimed_scan_chunk_prefix(), 1);
    }

    #[test]
    fn reclaim_tracker_waits_for_all_scanners() {
        let store = build_large_store(70_000).into_reclaimable();
        let first_block_rows = store.scan_chunks()[0].row_count as usize;
        let mut left = store.scanner();
        let mut right = store.scanner();
        let mut chunk = test_chunk_with_capacity(store.layout().types(), VECTOR_SIZE);

        let mut consumed = 0usize;
        while consumed < first_block_rows {
            consumed += left.next_chunk(&mut chunk).unwrap();
        }
        assert_eq!(store.reclaimed_scan_chunk_prefix(), 0);

        consumed = 0;
        while consumed < first_block_rows {
            consumed += right.next_chunk(&mut chunk).unwrap();
        }
        assert_eq!(store.reclaimed_scan_chunk_prefix(), 1);
    }

    #[test]
    fn codec_table_classifies_scalar_varlen_and_nested_types() {
        let layout = RowLayout::from_types(
            vec![
                LogicalType::Date,
                LogicalType::Varchar,
                LogicalType::List(Box::new(LogicalType::Varchar)),
                LogicalType::Array(Box::new(LogicalType::Integer), 2),
                LogicalType::Struct(vec![("x".to_string(), LogicalType::BigInt)]),
            ],
            RowValidityType::CanHaveNullValues,
        );

        assert!(matches!(
            layout.codecs().get(0),
            Some(ColumnCodec::Fixed { size: 4 })
        ));
        assert!(matches!(
            layout.codecs().get(1),
            Some(ColumnCodec::Varlen(VarlenCodec::InlineHeap16))
        ));
        assert!(matches!(layout.codecs().get(2), Some(ColumnCodec::List(_))));
        assert!(matches!(
            layout.codecs().get(3),
            Some(ColumnCodec::Array(_))
        ));
        assert!(matches!(
            layout.codecs().get(4),
            Some(ColumnCodec::Struct(_))
        ));
    }

    #[test]
    fn fixed_codec_scatter_handles_constant_and_dictionary_vectors() {
        let codec = ColumnCodec::Fixed { size: 8 };
        let constant = test_constant_vector(LogicalType::BigInt, 42_i64, 3);
        let mut constant_output = test_chunk_with_capacity(&[LogicalType::BigInt], 3);
        constant_output.set_cardinality(3);
        crate::row::codec::scatter_to_positions(
            &codec,
            0,
            &constant,
            &mut constant_output,
            &[2, 0, 1],
        )
        .unwrap();
        assert_eq!(constant_output.get_value(0, 0), Some(Value::BigInt(42)));
        assert_eq!(constant_output.get_value(0, 1), Some(Value::BigInt(42)));
        assert_eq!(constant_output.get_value(0, 2), Some(Value::BigInt(42)));

        let base = Arc::new(test_i64_vector(&[10, 20, 30]));
        let dictionary = paro_common::test_utils::test_dictionary(base, test_selection(vec![2, 0]));
        let mut dict_output = test_chunk_with_capacity(&[LogicalType::BigInt], 2);
        dict_output.set_cardinality(2);
        crate::row::codec::scatter_to_positions(&codec, 0, &dictionary, &mut dict_output, &[1, 0])
            .unwrap();
        assert_eq!(dict_output.get_value(0, 0), Some(Value::BigInt(10)));
        assert_eq!(dict_output.get_value(0, 1), Some(Value::BigInt(30)));
    }

    #[test]
    fn varlen_and_nested_codecs_round_trip() {
        let varchar_codec = ColumnCodec::Varlen(VarlenCodec::InlineHeap16);
        let mut strings = test_vector_with_capacity(LogicalType::Varchar, 2);
        strings.set_string(0, "alpha");
        strings.set_string(1, "omega");
        strings.set_count(2);
        let dictionary =
            paro_common::test_utils::test_dictionary(Arc::new(strings), test_selection(vec![1, 0]));
        let mut string_output = test_chunk_with_capacity(&[LogicalType::Varchar], 2);
        string_output.set_cardinality(2);
        crate::row::codec::scatter_to_positions(
            &varchar_codec,
            0,
            &dictionary,
            &mut string_output,
            &[0, 1],
        )
        .unwrap();
        assert_eq!(
            string_output.get_value(0, 0),
            Some(Value::Varchar("omega".to_string()))
        );
        assert_eq!(
            string_output.get_value(0, 1),
            Some(Value::Varchar("alpha".to_string()))
        );

        let struct_type = LogicalType::Struct(vec![
            ("name".to_string(), LogicalType::Varchar),
            ("score".to_string(), LogicalType::Integer),
        ]);
        let struct_codec = ColumnCodec::Struct(StructCodec::new(vec![
            (
                "name".to_string(),
                ColumnCodec::Varlen(VarlenCodec::InlineHeap16),
            ),
            ("score".to_string(), ColumnCodec::Fixed { size: 4 }),
        ]));
        let mut structs = test_vector_with_capacity(struct_type.clone(), 1);
        structs.set_value(
            0,
            &Value::Struct(
                vec![Value::Varchar("paro".to_string()), Value::Integer(7)],
                vec![
                    ("name".to_string(), LogicalType::Varchar),
                    ("score".to_string(), LogicalType::Integer),
                ],
            ),
        );
        structs.set_count(1);
        let mut struct_output = test_chunk_with_capacity(&[struct_type], 1);
        struct_output.set_cardinality(1);
        crate::row::codec::scatter_to_positions(
            &struct_codec,
            0,
            &structs,
            &mut struct_output,
            &[0],
        )
        .unwrap();
        assert_eq!(
            struct_output.get_value(0, 0),
            Some(Value::Struct(
                vec![Value::Varchar("paro".to_string()), Value::Integer(7)],
                vec![
                    ("name".to_string(), LogicalType::Varchar),
                    ("score".to_string(), LogicalType::Integer),
                ],
            ))
        );
    }
}
