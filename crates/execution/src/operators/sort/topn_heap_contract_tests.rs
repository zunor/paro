// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use crate::memory_runtime::{QueryMemoryPool, RetainedChunkVec};

use super::{accounted_metadata_vec, CombineCandidate, TopNEntry, TopNEntryHeap, TopNHeap};

fn memory_for(
    pool: &Arc<QueryMemoryPool>,
    domain: MemoryDomain,
    tag: MemoryTag,
    class: MemoryAccountingClass,
) -> MemoryAccountingContext {
    let owner: Arc<dyn MemoryOwner> = pool.clone();
    MemoryAccountingContext::from_owner(owner, domain, tag, class)
}

fn int_chunk(values: &[i32]) -> Chunk {
    paro_common::test_utils::test_chunk_from_vectors(vec![
        paro_common::test_utils::test_i32_vector(values),
    ])
}

fn heap_from_chunks(
    memory: MemoryAccountingContext,
    payload_types: Vec<LogicalType>,
    chunks: Vec<Chunk>,
    entries: impl IntoIterator<Item = (u32, usize)>,
    heap_size: usize,
) -> TopNHeap {
    let mut heap_data = RetainedChunkVec::new(memory.clone());
    for chunk in chunks {
        heap_data.push(chunk).expect("retain source chunk");
    }
    let mut heap = TopNEntryHeap::new(&memory);
    for (key, index) in entries {
        heap.try_push(
            TopNEntry::try_new(key.to_be_bytes().to_vec(), index, &memory)
                .expect("retain test key"),
        )
        .expect("retain test heap entry");
    }
    TopNHeap {
        heap,
        heap_data,
        memory,
        heap_size,
        offset: 0,
        modifiers: vec![],
        payload_types,
    }
}

fn extract_i32(heap: &mut TopNHeap) -> Vec<i32> {
    heap.extract_results()
        .expect("extract TopN")
        .into_iter()
        .flat_map(|chunk| {
            (0..chunk.size())
                .map(|row| chunk.column(0).expect("column").get_i32(row).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn combine_second_staging_batch_quota_failure_restores_accounting_and_sources() {
    let pool = Arc::new(QueryMemoryPool::unbounded());
    let memory = memory_for(
        &pool,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        MemoryAccountingClass::Revocable,
    );
    let row_count = VECTOR_SIZE + 1;
    let chunks = vec![
        int_chunk(
            &(0..VECTOR_SIZE)
                .map(|value| value as i32)
                .collect::<Vec<_>>(),
        ),
        int_chunk(&[VECTOR_SIZE as i32]),
    ];
    let mut left = heap_from_chunks(
        memory.clone(),
        vec![LogicalType::Integer],
        chunks,
        (0..row_count).map(|index| (index as u32, index)),
        row_count,
    );
    let mut right = heap_from_chunks(
        memory.clone(),
        vec![LogicalType::Integer],
        vec![int_chunk(&[100_000])],
        [(100_000, 0)],
        row_count,
    );
    let baseline = pool.issued_bytes();

    // Measure the exact first-batch peak using the same fresh staging store.
    // The real combine is then allowed precisely that much headroom, forcing
    // its second vector batch to fail after the first has been admitted.
    let mut calibration = RetainedChunkVec::new(memory);
    let first_batch = (0..VECTOR_SIZE).collect::<Vec<_>>();
    TopNHeap::for_each_gathered_rows(
        &left.memory,
        &[LogicalType::Integer],
        left.heap_data.as_slice(),
        first_batch.len(),
        |row| first_batch[row],
        |chunk| {
            calibration.push(chunk)?;
            Ok(())
        },
    )
    .expect("calibrate one staging batch");
    let first_batch_bytes = pool.issued_bytes() - baseline;
    assert!(first_batch_bytes > 0);
    drop(calibration);
    assert_eq!(pool.issued_bytes(), baseline);
    pool.set_capacity_bytes(baseline + first_batch_bytes);

    left.combine(&mut right)
        .expect_err("second staging batch must exceed the query quota");

    assert_eq!(pool.issued_bytes(), baseline);
    assert_eq!(left.heap.len(), row_count);
    assert_eq!(right.heap.len(), 1);
    // Extraction materializes an independently retained output and therefore
    // needs output headroom after the combine-failure assertions are complete.
    pool.set_capacity_bytes(usize::MAX / 4);
    assert_eq!(
        extract_i32(&mut left),
        (0..row_count as i32).collect::<Vec<_>>()
    );
    assert_eq!(extract_i32(&mut right), vec![100_000]);
    drop((left, right));
    assert_eq!(pool.issued_bytes(), 0, "stats={:?}", pool.runtime_stats());
}

fn complex_dictionary_chunk(prefix: &str) -> Chunk {
    let strings = std::array::from_fn::<_, 3, _>(|index| {
        format!("{prefix}-varchar-{index}-with-out-of-line-storage")
    });
    let string_refs = strings.iter().map(String::as_str).collect::<Vec<_>>();
    let string_child = Arc::new(paro_common::test_utils::test_string_vector(&string_refs));
    let string_dictionary =
        paro_common::test_utils::test_dictionary(string_child, vec![2_u32, 0, 1]);

    let list_type = LogicalType::List(Box::new(LogicalType::Varchar));
    let mut list_child = paro_common::test_utils::test_vector_with_capacity(list_type.clone(), 3);
    list_child.try_set_count(3).unwrap();
    for index in 0..3 {
        list_child.set_value(
            index,
            &Value::List(
                vec![Value::Varchar(format!(
                    "{prefix}-list-{index}-with-out-of-line-storage"
                ))],
                LogicalType::Varchar,
            ),
        );
    }
    let list_dictionary =
        paro_common::test_utils::test_dictionary(Arc::new(list_child), vec![2_u32, 0, 1]);
    paro_common::test_utils::test_chunk_from_vectors(vec![string_dictionary, list_dictionary])
}

#[test]
fn combine_gathers_cross_chunk_dictionary_varlen_and_list_payloads() {
    let memory =
        MemoryAccountingContext::detached(MemoryTag::OrderBy, MemoryAccountingClass::Revocable);
    let payload_types = vec![
        LogicalType::Varchar,
        LogicalType::List(Box::new(LogicalType::Varchar)),
    ];
    let mut left = heap_from_chunks(
        memory.clone(),
        payload_types.clone(),
        vec![
            complex_dictionary_chunk("left-a"),
            complex_dictionary_chunk("left-b"),
        ],
        [(0, 0), (2, 4)],
        4,
    );
    let mut right = heap_from_chunks(
        memory,
        payload_types,
        vec![
            complex_dictionary_chunk("right-a"),
            complex_dictionary_chunk("right-b"),
        ],
        [(1, 1), (3, 5)],
        4,
    );

    left.combine(&mut right).expect("combine complex payloads");
    let output = left.extract_results().expect("extract complex TopN");
    let actual = output
        .iter()
        .flat_map(|chunk| {
            (0..chunk.size())
                .map(|row| {
                    (
                        chunk.column(0).unwrap().get_value(row),
                        chunk.column(1).unwrap().get_value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected_prefixes = [("left-a", 2), ("right-a", 0), ("left-b", 0), ("right-b", 1)];
    let expected = expected_prefixes
        .into_iter()
        .map(|(prefix, source_index)| {
            (
                Value::Varchar(format!(
                    "{prefix}-varchar-{source_index}-with-out-of-line-storage"
                )),
                Value::List(
                    vec![Value::Varchar(format!(
                        "{prefix}-list-{source_index}-with-out-of-line-storage"
                    ))],
                    LogicalType::Varchar,
                ),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(right.heap.is_empty());
    assert!(right.heap_data.is_empty());
}

#[test]
fn combine_requires_identical_owner_domain_tag_and_class() {
    let pool = Arc::new(QueryMemoryPool::unbounded());
    let other_pool = Arc::new(QueryMemoryPool::unbounded());
    let base = memory_for(
        &pool,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        MemoryAccountingClass::Revocable,
    );
    let mismatches = [
        memory_for(
            &other_pool,
            MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        ),
        memory_for(
            &pool,
            MemoryDomain::PinnedHost,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        ),
        memory_for(
            &pool,
            MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        ),
        memory_for(
            &pool,
            MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::NonRevocable,
        ),
    ];
    for mismatch in mismatches {
        let mut left = heap_from_chunks(base.clone(), vec![], vec![], [], 0);
        let mut right = heap_from_chunks(mismatch, vec![], vec![], [], 0);
        left.combine(&mut right)
            .expect_err("mismatched accounting target must be rejected");
    }

    let mut left = heap_from_chunks(base.clone(), vec![], vec![], [], 0);
    let mut right = heap_from_chunks(base, vec![], vec![], [], 0);
    left.combine(&mut right)
        .expect("identical accounting target may combine");
}

#[test]
fn combine_metadata_admission_failure_is_atomic() {
    let pool = Arc::new(QueryMemoryPool::unbounded());
    let memory = memory_for(
        &pool,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        MemoryAccountingClass::Revocable,
    );
    let mut left = heap_from_chunks(
        memory.clone(),
        vec![LogicalType::Integer],
        vec![int_chunk(&[10, 20])],
        [(10, 0), (20, 1)],
        2,
    );
    let mut right = heap_from_chunks(
        memory,
        vec![LogicalType::Integer],
        vec![int_chunk(&[1, 2])],
        [(1, 0), (2, 1)],
        2,
    );
    let baseline = pool.issued_bytes();
    pool.set_capacity_bytes(baseline);

    left.combine(&mut right)
        .expect_err("candidate metadata must respect the hard query quota");

    assert_eq!(pool.issued_bytes(), baseline);
    assert_eq!(left.heap.len(), 2);
    assert_eq!(right.heap.len(), 2);
    pool.set_capacity_bytes(usize::MAX / 4);
    assert_eq!(extract_i32(&mut left), vec![10, 20]);
    assert_eq!(extract_i32(&mut right), vec![1, 2]);
    drop((left, right));
    assert_eq!(pool.issued_bytes(), 0);
}

#[test]
fn combine_final_heap_admission_failure_is_atomic() {
    let pool = Arc::new(QueryMemoryPool::unbounded());
    let memory = memory_for(
        &pool,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        MemoryAccountingClass::Revocable,
    );
    let mut left = heap_from_chunks(
        memory.clone(),
        vec![LogicalType::Integer],
        vec![int_chunk(&[10, 20])],
        [(10, 0), (20, 1)],
        2,
    );
    let mut right = heap_from_chunks(
        memory,
        vec![LogicalType::Integer],
        vec![int_chunk(&[1, 2])],
        [(1, 0), (2, 1)],
        2,
    );
    let baseline = pool.issued_bytes();

    let mut calibration = accounted_metadata_vec::<CombineCandidate>(&left.memory);
    calibration.try_reserve(4).expect("candidate calibration");
    let candidate_bytes = pool.issued_bytes() - baseline;
    drop(calibration);
    assert_eq!(pool.issued_bytes(), baseline);
    pool.set_capacity_bytes(baseline + candidate_bytes);

    left.combine(&mut right)
        .expect_err("final heap backing must be admitted before publication");

    assert_eq!(pool.issued_bytes(), baseline);
    assert_eq!(left.heap.len(), 2);
    assert_eq!(right.heap.len(), 2);
    pool.set_capacity_bytes(usize::MAX / 4);
    assert_eq!(extract_i32(&mut left), vec![10, 20]);
    assert_eq!(extract_i32(&mut right), vec![1, 2]);
    drop((left, right));
    assert_eq!(pool.issued_bytes(), 0);
}

#[test]
fn successful_combine_releases_heap_keys_scratch_and_payload_accounting() {
    let pool = Arc::new(QueryMemoryPool::unbounded());
    let memory = memory_for(
        &pool,
        MemoryDomain::Host,
        MemoryTag::OrderBy,
        MemoryAccountingClass::Revocable,
    );
    let mut left = heap_from_chunks(
        memory.clone(),
        vec![LogicalType::Integer],
        vec![int_chunk(&[10, 20])],
        [(10, 0), (20, 1)],
        2,
    );
    let mut right = heap_from_chunks(
        memory,
        vec![LogicalType::Integer],
        vec![int_chunk(&[1, 2])],
        [(1, 0), (2, 1)],
        2,
    );

    left.combine(&mut right)
        .expect("combine owner-backed heaps");
    assert!(pool.metadata_bytes() > 0);
    assert_eq!(extract_i32(&mut left), vec![1, 2]);
    drop((left, right));

    assert_eq!(pool.issued_bytes(), 0);
    assert_eq!(pool.metadata_bytes(), 0);
    assert_eq!(pool.revocable_bytes(), 0);
}
