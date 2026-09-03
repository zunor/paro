// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable lookup index for finalized full-partition aggregates.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::collections::AccountedVec;
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, ValidatedVectorSelection, Vector};

use crate::memory_runtime::RetainedMemoryHandle;
use crate::operators::aggregate::group_hash::hash_group_columns;
use crate::operators::aggregate::tuple_layout::group_vector_values_equal;

const EMPTY_DENSE_SLOT: u32 = u32::MAX;
const EMPTY_HASH_SLOT: u32 = 0;
const MIN_HASH_CAPACITY: usize = 8;
const HASH_LOAD_FACTOR_NUMERATOR: usize = 3;
const HASH_LOAD_FACTOR_DENOMINATOR: usize = 5;
const MAX_DENSE_INTEGER_SLOTS: usize = 16 * 1024 * 1024;
const MAX_DENSE_SLOTS_PER_GROUP: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AggregateRowRef(pub(crate) u32);

impl AggregateRowRef {
    fn try_new(row: usize) -> Result<Self> {
        let row = u32::try_from(row).map_err(|_| {
            paro_error::not_implemented(
                "partition aggregate finalized domain exceeds dictionary index width",
            )
        })?;
        if row == EMPTY_DENSE_SLOT {
            return Err(paro_error::internal(
                "partition aggregate row reference collides with empty sentinel",
            ));
        }
        Ok(Self(row))
    }
}

#[derive(Debug)]
enum PartitionKeyIndex {
    DenseInteger {
        key_type: LogicalType,
        minimum: i64,
        slots: Box<[u32]>,
        null_row: Option<AggregateRowRef>,
    },
    SparseInteger {
        key_type: LogicalType,
        /// Immutable, precisely-sized key domain. Sparse lookup is deliberately
        /// logarithmic: unlike `HashMap`, a boxed sorted slice has an exact
        /// publication footprint that can be admitted before allocation.
        rows: Box<[(i64, AggregateRowRef)]>,
        null_row: Option<AggregateRowRef>,
    },
    /// Frozen, read-only open-addressing index for generic or composite SQL
    /// grouping keys. Key columns remain vector-native, so lookup compares
    /// varlen values without allocating and carries no mutable aggregate state
    /// into the published snapshot.
    Hashed {
        key_types: Box<[LogicalType]>,
        key_columns: Box<[Arc<Vector>]>,
        /// Zero is empty; occupied entries store `aggregate_row + 1`.
        slots: Box<[u32]>,
    },
}

/// Group keys and finalized aggregate vectors published as one immutable unit.
#[derive(Debug)]
pub(crate) struct FinalizedPartitionIndex {
    key_count: usize,
    aggregate_columns: Box<[Arc<Vector>]>,
    keys: PartitionKeyIndex,
    _index_memory: RetainedMemoryHandle,
}

impl FinalizedPartitionIndex {
    pub(crate) fn try_new(
        key_types: Vec<LogicalType>,
        aggregate_count: usize,
        chunks: Vec<Chunk>,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        if key_types.is_empty() || aggregate_count == 0 {
            return Err(paro_error::internal(
                "partition aggregate index requires partition keys and aggregate values",
            ));
        }
        let key_count = key_types.len();
        let chunks: Arc<[Chunk]> = Arc::from(chunks.into_boxed_slice());
        for (index, chunk) in chunks.iter().enumerate() {
            if chunk.column_count() != key_count + aggregate_count {
                return Err(paro_error::internal(format!(
                    "partition aggregate result chunk {index} width mismatch: expected={}, actual={}",
                    key_count + aggregate_count,
                    chunk.column_count()
                )));
            }
        }
        if chunks.iter().any(|chunk| {
            chunk
                .types()
                .get(..key_count)
                .is_none_or(|types| types != key_types.as_slice())
        }) {
            return Err(paro_error::internal(
                "partition aggregate finalized key types disagree with its physical plan",
            ));
        }
        let (keys, index_memory) = if key_types.len() == 1
            && matches!(key_types[0], LogicalType::Integer | LogicalType::BigInt)
        {
            build_integer_index(&chunks, key_types[0].clone(), memory.clone())?
        } else {
            build_hash_index(&chunks, key_types, allocator, memory.clone())?
        };
        let aggregate_columns = flatten_aggregate_columns(&chunks, key_count, aggregate_count)?;
        Ok(Self {
            key_count,
            aggregate_columns,
            keys,
            _index_memory: index_memory,
        })
    }

    pub(crate) fn select_rows(&self, keys: &Chunk, selection: &mut SelectionVector) -> Result<()> {
        if keys.column_count() != self.key_count {
            return Err(paro_error::internal(
                "partition aggregate lookup key shape mismatch",
            ));
        }
        if selection.capacity() < keys.size() || !selection.is_uniquely_owned() {
            *selection =
                SelectionVector::try_with_capacity(keys.size().max(1), keys.allocator().clone())?;
        }
        selection.set_len(keys.size());
        if let PartitionKeyIndex::Hashed {
            key_types,
            key_columns,
            slots,
        } = &self.keys
        {
            if keys.types() != key_types.as_ref() {
                return Err(paro_error::internal(
                    "partition aggregate lookup key types disagree with finalized domain",
                ));
            }
            return select_hash_rows(key_types, key_columns, slots, keys, selection);
        }
        for row in 0..keys.size() {
            let result = match &self.keys {
                PartitionKeyIndex::DenseInteger {
                    key_type,
                    minimum,
                    slots,
                    null_row,
                } => {
                    let key = read_integer_key(
                        keys.column(0).expect("verified single partition key"),
                        row,
                        key_type,
                    )?;
                    match key {
                        None => *null_row,
                        Some(key) => i128::from(key)
                            .checked_sub(i128::from(*minimum))
                            .and_then(|offset| usize::try_from(offset).ok())
                            .and_then(|offset| slots.get(offset).copied())
                            .filter(|slot| *slot != EMPTY_DENSE_SLOT)
                            .map(AggregateRowRef),
                    }
                }
                PartitionKeyIndex::SparseInteger {
                    key_type,
                    rows,
                    null_row,
                } => {
                    let key = read_integer_key(
                        keys.column(0).expect("verified single partition key"),
                        row,
                        key_type,
                    )?;
                    match key {
                        None => *null_row,
                        Some(key) => rows
                            .binary_search_by_key(&key, |(candidate, _)| *candidate)
                            .ok()
                            .map(|index| rows[index].1),
                    }
                }
                PartitionKeyIndex::Hashed { .. } => unreachable!("hashed index returned above"),
            };
            let result = result.ok_or_else(|| {
                paro_error::internal(format!(
                    "partition aggregate detail row {row} has no finalized group"
                ))
            })?;
            selection.try_set(row, result.0 as usize)?;
        }
        Ok(())
    }

    /// Attach finalized aggregate columns as dictionary views over one shared
    /// immutable result domain. The selection is validated once and reused by
    /// every aggregate column, avoiding per-row materialization on analytical
    /// detail streams.
    pub(crate) fn attach_aggregates(
        &self,
        keys: &Chunk,
        selection: &mut SelectionVector,
        output: &mut Chunk,
        output_offset: usize,
    ) -> Result<()> {
        self.select_rows(keys, selection)?;
        let child_count = self
            .aggregate_columns
            .first()
            .map_or(0, |column| column.len());
        let validated = ValidatedVectorSelection::try_new(selection.clone(), child_count)?;
        for (aggregate_index, column) in self.aggregate_columns.iter().enumerate() {
            let target = output
                .data
                .get_mut(output_offset + aggregate_index)
                .ok_or_else(|| {
                    paro_error::internal("partition aggregate output column is missing")
                })?;
            *target = Arc::new(Vector::try_dictionary_from_validated(
                Arc::clone(column),
                validated.clone(),
            )?);
        }
        Ok(())
    }

    pub(crate) fn aggregate_columns(&self) -> &[Arc<Vector>] {
        &self.aggregate_columns
    }
}

fn build_hash_index(
    chunks: &[Chunk],
    key_types: Vec<LogicalType>,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
    memory: MemoryAccountingContext,
) -> Result<(PartitionKeyIndex, RetainedMemoryHandle)> {
    let group_count = chunks.iter().try_fold(0usize, |total, chunk| {
        total.checked_add(chunk.size()).ok_or_else(|| {
            paro_error::out_of_range("partition aggregate hash index row count overflow")
        })
    })?;
    if group_count >= EMPTY_DENSE_SLOT as usize {
        return Err(paro_error::not_implemented(
            "partition aggregate finalized domain exceeds dictionary index width",
        ));
    }
    let key_columns = flatten_columns(chunks, 0, key_types.len())?;
    let keys = Chunk::try_from_arc_vectors_with_cardinality(
        key_columns.iter().cloned().collect(),
        group_count,
        allocator,
    )?;
    let hashes = hash_group_columns(&keys)?;
    let hashes = hashes.as_slice::<u64>();
    let capacity = hash_capacity(group_count)?;
    let mut slots =
        AccountedVec::new_with_accounting(memory.grant()?, memory.tag(), memory.accounting_class());
    slots.try_resize_with(capacity, || EMPTY_HASH_SLOT)?;
    let mask = capacity - 1;
    for row in 0..group_count {
        let mut slot = hash_slot(hashes[row], mask);
        loop {
            let entry = slots[slot];
            if entry == EMPTY_HASH_SLOT {
                slots[slot] = u32::try_from(row + 1).map_err(|_| {
                    paro_error::not_implemented(
                        "partition aggregate finalized domain exceeds dictionary index width",
                    )
                })?;
                break;
            }
            let existing = entry as usize - 1;
            if group_rows_equal(&key_types, &key_columns, existing, &keys, row)? {
                return Err(duplicate_group_error());
            }
            slot = (slot + 1) & mask;
        }
    }

    let final_bytes = capacity
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| paro_error::out_of_range("partition aggregate hash index size overflow"))?;
    let final_memory = RetainedMemoryHandle::new(memory.retain(final_bytes)?);
    let slots = slots.as_slice().to_vec().into_boxed_slice();
    Ok((
        PartitionKeyIndex::Hashed {
            key_types: key_types.into_boxed_slice(),
            key_columns,
            slots,
        },
        final_memory,
    ))
}

fn select_hash_rows(
    key_types: &[LogicalType],
    key_columns: &[Arc<Vector>],
    slots: &[u32],
    keys: &Chunk,
    selection: &mut SelectionVector,
) -> Result<()> {
    if slots.is_empty() || !slots.len().is_power_of_two() {
        return Err(paro_error::internal(
            "partition aggregate hash index has invalid capacity",
        ));
    }
    let hashes = hash_group_columns(keys)?;
    let hashes = hashes.as_slice::<u64>();
    let mask = slots.len() - 1;
    for row in 0..keys.size() {
        let mut slot = hash_slot(hashes[row], mask);
        loop {
            let entry = slots[slot];
            if entry == EMPTY_HASH_SLOT {
                return Err(paro_error::internal(format!(
                    "partition aggregate detail row {row} has no finalized group"
                )));
            }
            let aggregate_row = entry as usize - 1;
            if group_rows_equal(key_types, key_columns, aggregate_row, keys, row)? {
                selection.try_set(row, aggregate_row)?;
                break;
            }
            slot = (slot + 1) & mask;
        }
    }
    Ok(())
}

fn group_rows_equal(
    key_types: &[LogicalType],
    stored: &[Arc<Vector>],
    stored_row: usize,
    incoming: &Chunk,
    incoming_row: usize,
) -> Result<bool> {
    for (column_index, (column, logical_type)) in stored.iter().zip(key_types).enumerate() {
        let incoming_column = incoming
            .column(column_index)
            .ok_or_else(|| paro_error::internal("partition aggregate lookup key is missing"))?;
        if !group_vector_values_equal(
            column,
            stored_row,
            incoming_column,
            incoming_row,
            logical_type,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn hash_capacity(group_count: usize) -> Result<usize> {
    let required = group_count
        .checked_mul(HASH_LOAD_FACTOR_DENOMINATOR)
        .and_then(|value| value.checked_add(HASH_LOAD_FACTOR_NUMERATOR - 1))
        .map(|value| value / HASH_LOAD_FACTOR_NUMERATOR)
        .ok_or_else(|| paro_error::out_of_range("partition aggregate hash capacity overflow"))?
        .max(MIN_HASH_CAPACITY);
    required
        .checked_next_power_of_two()
        .ok_or_else(|| paro_error::out_of_range("partition aggregate hash capacity overflow"))
}

#[inline]
fn hash_slot(hash: u64, mask: usize) -> usize {
    hash as usize & mask
}

fn flatten_columns(
    chunks: &[Chunk],
    source_offset: usize,
    column_count: usize,
) -> Result<Box<[Arc<Vector>]>> {
    let total_rows = chunks.iter().try_fold(0usize, |total, chunk| {
        total.checked_add(chunk.size()).ok_or_else(|| {
            paro_error::out_of_range("partition aggregate finalized row count overflow")
        })
    })?;
    if total_rows == 0 {
        return Ok(Box::new([]));
    }
    let first = chunks
        .first()
        .expect("non-empty finalized domain has a result chunk");
    if chunks.len() == 1 {
        return Ok((0..column_count)
            .map(|column| {
                Arc::clone(
                    first
                        .column(source_offset + column)
                        .expect("verified finalized result width"),
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice());
    }
    let mut columns = Vec::with_capacity(column_count);
    for column in 0..column_count {
        let source_index = source_offset + column;
        let source = first
            .column(source_index)
            .expect("verified finalized result width");
        let mut result = Vector::try_new(
            source.logical_type().clone(),
            total_rows,
            first.allocator().clone(),
        )?;
        result.try_set_count(total_rows)?;
        let mut offset = 0usize;
        for chunk in chunks {
            let source = chunk
                .column(source_index)
                .expect("verified finalized result width");
            result.try_copy_range(offset, source, 0, chunk.size())?;
            offset += chunk.size();
        }
        columns.push(Arc::new(result));
    }
    Ok(columns.into_boxed_slice())
}

fn flatten_aggregate_columns(
    chunks: &[Chunk],
    group_count: usize,
    aggregate_count: usize,
) -> Result<Box<[Arc<Vector>]>> {
    flatten_columns(chunks, group_count, aggregate_count)
}

fn build_integer_index(
    chunks: &[Chunk],
    key_type: LogicalType,
    memory: MemoryAccountingContext,
) -> Result<(PartitionKeyIndex, RetainedMemoryHandle)> {
    let mut minimum = i64::MAX;
    let mut maximum = i64::MIN;
    let mut non_null_count = 0usize;
    let mut group_count = 0usize;
    for chunk in chunks {
        let column = chunk.column(0).expect("verified integer group key");
        for row in 0..chunk.size() {
            group_count += 1;
            if let Some(key) = read_integer_key(column, row, &key_type)? {
                minimum = minimum.min(key);
                maximum = maximum.max(key);
                non_null_count += 1;
            }
        }
    }
    let domain = if non_null_count == 0 {
        0
    } else {
        i128::from(maximum)
            .checked_sub(i128::from(minimum))
            .and_then(|span| span.checked_add(1))
            .and_then(|span| usize::try_from(span).ok())
            .unwrap_or(usize::MAX)
    };
    if domain <= MAX_DENSE_INTEGER_SLOTS
        && domain <= group_count.saturating_mul(MAX_DENSE_SLOTS_PER_GROUP)
    {
        let mut slots = AccountedVec::new_with_accounting(
            memory.grant()?,
            memory.tag(),
            memory.accounting_class(),
        );
        slots.try_resize_with(domain, || EMPTY_DENSE_SLOT)?;
        let mut null_row = None;
        let mut result_row = 0usize;
        for chunk in chunks {
            let column = chunk.column(0).expect("verified integer group key");
            for row in 0..chunk.size() {
                let reference = AggregateRowRef::try_new(result_row)?;
                result_row += 1;
                match read_integer_key(column, row, &key_type)? {
                    None if null_row.replace(reference).is_some() => {
                        return Err(duplicate_group_error());
                    }
                    None => {}
                    Some(key) => {
                        let offset = usize::try_from(i128::from(key) - i128::from(minimum))
                            .expect("dense integer domain validated");
                        if std::mem::replace(&mut slots[offset], reference.0) != EMPTY_DENSE_SLOT {
                            return Err(duplicate_group_error());
                        }
                    }
                }
            }
        }
        let final_bytes = domain
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| {
                paro_error::out_of_range("partition aggregate dense index size overflow")
            })?;
        let final_memory = RetainedMemoryHandle::new(memory.retain(final_bytes)?);
        let slots = slots.as_slice().to_vec().into_boxed_slice();
        return Ok((
            PartitionKeyIndex::DenseInteger {
                key_type,
                minimum,
                slots,
                null_row,
            },
            final_memory,
        ));
    }

    let mut rows =
        AccountedVec::new_with_accounting(memory.grant()?, memory.tag(), memory.accounting_class());
    rows.try_reserve(group_count)?;
    let mut null_row = None;
    let mut result_row = 0usize;
    for chunk in chunks {
        let column = chunk.column(0).expect("verified integer group key");
        for row in 0..chunk.size() {
            let key = read_integer_key(column, row, &key_type)?;
            let reference = AggregateRowRef::try_new(result_row)?;
            match key {
                None if null_row.replace(reference).is_some() => {
                    return Err(duplicate_group_error());
                }
                None => {}
                Some(key) => rows.try_push((key, reference))?,
            }
            result_row += 1;
        }
    }
    rows.sort_unstable_by_key(|(key, _)| *key);
    if rows.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(duplicate_group_error());
    }
    let final_bytes = rows
        .len()
        .checked_mul(std::mem::size_of::<(i64, AggregateRowRef)>())
        .ok_or_else(|| {
            paro_error::out_of_range("partition aggregate sparse index size overflow")
        })?;
    let final_memory = RetainedMemoryHandle::new(memory.retain(final_bytes)?);
    let rows = rows.as_slice().to_vec().into_boxed_slice();
    Ok((
        PartitionKeyIndex::SparseInteger {
            key_type,
            rows,
            null_row,
        },
        final_memory,
    ))
}

fn read_integer_key(
    column: &paro_common::vector::Vector,
    row: usize,
    key_type: &LogicalType,
) -> Result<Option<i64>> {
    if column.is_null(row) {
        return Ok(None);
    }
    let value = match key_type {
        LogicalType::Integer => column.get_i32(row).map(i64::from),
        LogicalType::BigInt => column.get_i64(row),
        _ => None,
    };
    value.map(Some).ok_or_else(|| {
        paro_error::internal(format!(
            "partition aggregate {key_type} key at row {row} has invalid physical storage"
        ))
    })
}

fn duplicate_group_error() -> paro_common::error::ParoError {
    paro_error::internal("partition aggregate finalized index contains a duplicate group")
}

#[cfg(test)]
mod tests {
    use paro_common::allocator::MemoryTag;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::test_utils::{
        test_allocator, test_i32_vector_with_allocator, test_i64_vector_with_allocator,
        test_string_vector_with_allocator,
    };
    use paro_common::types::LogicalType;
    use paro_common::vector::{SelectionVector, Vector, VectorType};

    use super::FinalizedPartitionIndex;

    fn detached_memory() -> MemoryAccountingContext {
        MemoryAccountingContext::detached(MemoryTag::Window, MemoryAccountingClass::NonRevocable)
    }

    #[test]
    fn null_partition_and_tied_details_share_finalized_result() {
        let allocator = test_allocator();
        let mut groups = test_i32_vector_with_allocator(&[1, 2, 0], allocator.clone());
        groups.try_set_null(2, true).expect("null partition key");
        let mut sums = Vector::try_from_i64(&[10, 0, 30], allocator.clone()).expect("sums");
        sums.try_set_null(1, true).expect("null aggregate");
        let results =
            paro_common::chunk::Chunk::from_vectors(vec![groups, sums], allocator.clone());
        let index = FinalizedPartitionIndex::try_new(
            vec![LogicalType::Integer],
            1,
            vec![results],
            allocator.clone(),
            detached_memory(),
        )
        .expect("finalized index");

        let mut detail_keys = test_i32_vector_with_allocator(&[1, 0, 2, 1], allocator.clone());
        detail_keys.try_set_null(1, true).expect("null detail key");
        let keys = paro_common::chunk::Chunk::from_vectors(vec![detail_keys], allocator.clone());
        let mut output = paro_common::chunk::Chunk::try_initialize(
            &[LogicalType::BigInt],
            keys.size(),
            allocator.clone(),
        )
        .expect("output");
        output
            .try_set_cardinality(keys.size())
            .expect("output cardinality");
        let mut selection = SelectionVector::try_with_capacity(0, allocator).expect("selection");
        index
            .attach_aggregates(&keys, &mut selection, &mut output, 0)
            .expect("attach results");

        let values = output.column(0).expect("aggregate column");
        assert_eq!(values.vector_type(), VectorType::Dictionary);
        assert_eq!(values.get_i64(0), Some(10));
        assert_eq!(values.get_i64(1), Some(30));
        assert!(values.is_null(2));
        assert_eq!(values.get_i64(3), Some(10));
    }

    #[test]
    fn empty_finalized_domain_is_publishable_without_a_lookup() {
        let allocator = test_allocator();
        let index = FinalizedPartitionIndex::try_new(
            vec![LogicalType::Integer],
            1,
            Vec::new(),
            allocator,
            detached_memory(),
        )
        .expect("empty snapshot index");
        assert!(index.aggregate_columns.is_empty());
    }

    #[test]
    fn bigint_key_domain_uses_typed_dense_lookup() {
        let allocator = test_allocator();
        let results = paro_common::chunk::Chunk::from_vectors(
            vec![
                test_i64_vector_with_allocator(&[i64::MAX - 1, i64::MAX], allocator.clone()),
                test_i64_vector_with_allocator(&[7, 9], allocator.clone()),
            ],
            allocator.clone(),
        );
        let index = FinalizedPartitionIndex::try_new(
            vec![LogicalType::BigInt],
            1,
            vec![results],
            allocator.clone(),
            detached_memory(),
        )
        .expect("BIGINT finalized index");
        let keys = paro_common::chunk::Chunk::from_vectors(
            vec![test_i64_vector_with_allocator(
                &[i64::MAX, i64::MAX - 1],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut output =
            paro_common::chunk::Chunk::try_initialize(&[LogicalType::BigInt], 2, allocator.clone())
                .expect("output");
        output.try_set_cardinality(2).expect("output cardinality");
        let mut selection = SelectionVector::try_with_capacity(0, allocator).expect("selection");
        index
            .attach_aggregates(&keys, &mut selection, &mut output, 0)
            .expect("attach BIGINT results");
        assert_eq!(output.column(0).unwrap().get_i64(0), Some(9));
        assert_eq!(output.column(0).unwrap().get_i64(1), Some(7));
    }

    #[test]
    fn bigint_sparse_key_domain_uses_typed_lookup() {
        let allocator = test_allocator();
        let results = paro_common::chunk::Chunk::from_vectors(
            vec![
                test_i64_vector_with_allocator(&[i64::MIN, i64::MAX], allocator.clone()),
                test_i64_vector_with_allocator(&[11, 13], allocator.clone()),
            ],
            allocator.clone(),
        );
        let index = FinalizedPartitionIndex::try_new(
            vec![LogicalType::BigInt],
            1,
            vec![results],
            allocator.clone(),
            detached_memory(),
        )
        .expect("sparse BIGINT finalized index");
        let keys = paro_common::chunk::Chunk::from_vectors(
            vec![test_i64_vector_with_allocator(
                &[i64::MAX, i64::MIN],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut output =
            paro_common::chunk::Chunk::try_initialize(&[LogicalType::BigInt], 2, allocator.clone())
                .expect("output");
        output.try_set_cardinality(2).expect("output cardinality");
        let mut selection = SelectionVector::try_with_capacity(0, allocator).expect("selection");
        index
            .attach_aggregates(&keys, &mut selection, &mut output, 0)
            .expect("attach sparse BIGINT results");
        assert_eq!(output.column(0).unwrap().get_i64(0), Some(13));
        assert_eq!(output.column(0).unwrap().get_i64(1), Some(11));
    }

    #[test]
    fn composite_varlen_domain_uses_frozen_hash_lookup_across_chunks() {
        let allocator = test_allocator();
        let first = paro_common::chunk::Chunk::from_vectors(
            vec![
                test_string_vector_with_allocator(
                    &["long-category-alpha", "long-category-beta"],
                    allocator.clone(),
                ),
                test_i32_vector_with_allocator(&[1, 2], allocator.clone()),
                test_i64_vector_with_allocator(&[10, 20], allocator.clone()),
            ],
            allocator.clone(),
        );
        let mut null_string = test_string_vector_with_allocator(&["ignored"], allocator.clone());
        null_string.try_set_null(0, true).expect("null string key");
        let mut null_integer = test_i32_vector_with_allocator(&[0], allocator.clone());
        null_integer
            .try_set_null(0, true)
            .expect("null integer key");
        let second = paro_common::chunk::Chunk::from_vectors(
            vec![
                null_string,
                null_integer,
                test_i64_vector_with_allocator(&[30], allocator.clone()),
            ],
            allocator.clone(),
        );
        let index = FinalizedPartitionIndex::try_new(
            vec![LogicalType::Varchar, LogicalType::Integer],
            1,
            vec![first, second],
            allocator.clone(),
            detached_memory(),
        )
        .expect("composite finalized index");

        let mut detail_strings = test_string_vector_with_allocator(
            &["long-category-beta", "ignored", "long-category-alpha"],
            allocator.clone(),
        );
        detail_strings
            .try_set_null(1, true)
            .expect("null detail string");
        let mut detail_integers = test_i32_vector_with_allocator(&[2, 0, 1], allocator.clone());
        detail_integers
            .try_set_null(1, true)
            .expect("null detail integer");
        let keys = paro_common::chunk::Chunk::from_vectors(
            vec![detail_strings, detail_integers],
            allocator.clone(),
        );
        let mut output = paro_common::chunk::Chunk::try_initialize(
            &[LogicalType::BigInt],
            keys.size(),
            allocator.clone(),
        )
        .expect("output");
        output
            .try_set_cardinality(keys.size())
            .expect("output cardinality");
        let mut selection = SelectionVector::try_with_capacity(0, allocator).expect("selection");
        index
            .attach_aggregates(&keys, &mut selection, &mut output, 0)
            .expect("attach composite results");

        let values = output.column(0).expect("aggregate column");
        assert_eq!(values.get_i64(0), Some(20));
        assert_eq!(values.get_i64(1), Some(30));
        assert_eq!(values.get_i64(2), Some(10));
    }
}
