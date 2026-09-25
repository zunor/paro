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

/// Occupancy encoding for the immutable hash index.
///
/// Keeping the `row + 1` representation behind this type prevents callers
/// from mixing it with the dense index's raw row/sentinel representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct HashSlot(u32);

impl HashSlot {
    const EMPTY: Self = Self(0);

    fn try_from_row(row: AggregateRowRef) -> Result<Self> {
        row.0.checked_add(1).map(Self).ok_or_else(|| {
            paro_error::not_implemented(
                "partition aggregate finalized domain exceeds hash-slot width",
            )
        })
    }

    fn row(self) -> Option<AggregateRowRef> {
        self.0.checked_sub(1).map(AggregateRowRef)
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
    /// Frozen, read-only open-addressing index for generic or composite SQL
    /// grouping keys. Key columns remain vector-native, so lookup compares
    /// varlen values without allocating and carries no mutable aggregate state
    /// into the published snapshot.
    Hashed {
        key_types: Box<[LogicalType]>,
        key_columns: Box<[Arc<Vector>]>,
        slots: Box<[HashSlot]>,
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
        aggregate_types: Vec<LogicalType>,
        chunks: Vec<Chunk>,
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        if key_types.is_empty() || aggregate_types.is_empty() {
            return Err(paro_error::internal(
                "partition aggregate index requires partition keys and aggregate values",
            ));
        }
        if key_types
            .iter()
            .any(|logical_type| !logical_type.supports_flat_group_key())
        {
            return Err(paro_error::internal(
                "partition aggregate index received an unsupported group key type",
            ));
        }
        let key_count = key_types.len();
        let aggregate_count = aggregate_types.len();
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
            chunk.types().get(..key_count) != Some(key_types.as_slice())
                || chunk.types().get(key_count..) != Some(aggregate_types.as_slice())
        }) {
            return Err(paro_error::internal(
                "partition aggregate finalized types disagree with its physical plan",
            ));
        }
        let (keys, index_memory) = if key_types.len() == 1
            && matches!(key_types[0], LogicalType::Integer | LogicalType::BigInt)
        {
            build_integer_index(
                &chunks,
                key_types[0].clone(),
                allocator.clone(),
                memory.clone(),
            )?
        } else {
            build_hash_index(&chunks, key_types, allocator.clone(), memory.clone())?
        };
        let aggregate_columns = flatten_columns(&chunks, key_count, &aggregate_types, allocator)?;
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
    if group_count >= u32::MAX as usize {
        return Err(paro_error::not_implemented(
            "partition aggregate finalized domain exceeds dictionary index width",
        ));
    }
    let key_columns = flatten_columns(chunks, 0, &key_types, allocator.clone())?;
    if key_columns.len() != key_types.len() {
        return Err(paro_error::internal(
            "partition aggregate hash key columns do not match key types",
        ));
    }
    let keys =
        Chunk::try_from_arc_vectors_with_cardinality(key_columns.to_vec(), group_count, allocator)?;
    let hashes = hash_group_columns(&keys)?;
    let hashes = hashes.as_slice::<u64>();
    let capacity = hash_capacity(group_count)?;
    let mut slots =
        AccountedVec::new_with_accounting(memory.grant()?, memory.tag(), memory.accounting_class());
    slots.try_resize_with(capacity, || HashSlot::EMPTY)?;
    let mask = capacity - 1;
    for row in 0..group_count {
        let mut slot = hash_slot(hashes[row], mask);
        loop {
            let entry = slots[slot];
            if entry == HashSlot::EMPTY {
                slots[slot] = HashSlot::try_from_row(AggregateRowRef::try_new(row)?)?;
                break;
            }
            let existing = entry
                .row()
                .expect("non-empty partition aggregate hash slot")
                .0 as usize;
            if group_rows_equal(&key_types, &key_columns, existing, &keys, row)? {
                return Err(duplicate_group_error());
            }
            slot = (slot + 1) & mask;
        }
    }

    let final_bytes = capacity
        .checked_mul(std::mem::size_of::<HashSlot>())
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
    slots: &[HashSlot],
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
            if entry == HashSlot::EMPTY {
                return Err(paro_error::internal(format!(
                    "partition aggregate detail row {row} has no finalized group"
                )));
            }
            let aggregate_row = entry
                .row()
                .expect("non-empty partition aggregate hash slot")
                .0 as usize;
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
    if stored.len() != key_types.len() || incoming.column_count() != key_types.len() {
        return Err(paro_error::internal(format!(
            "partition aggregate key arity mismatch: types={}, stored={}, incoming={}",
            key_types.len(),
            stored.len(),
            incoming.column_count()
        )));
    }
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
    column_types: &[LogicalType],
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Box<[Arc<Vector>]>> {
    let total_rows = chunks.iter().try_fold(0usize, |total, chunk| {
        total.checked_add(chunk.size()).ok_or_else(|| {
            paro_error::out_of_range("partition aggregate finalized row count overflow")
        })
    })?;
    let column_count = column_types.len();
    let Some(first) = chunks.first() else {
        return column_types
            .iter()
            .map(|logical_type| {
                let mut vector = Vector::try_new(logical_type.clone(), 0, allocator.clone())?;
                vector.try_set_count(0)?;
                Ok(Arc::new(vector))
            })
            .collect::<Result<Vec<_>>>()
            .map(Vec::into_boxed_slice);
    };
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
        let mut result =
            Vector::try_new(column_types[column].clone(), total_rows, allocator.clone())?;
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

fn build_integer_index(
    chunks: &[Chunk],
    key_type: LogicalType,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
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

    build_hash_index(chunks, vec![key_type], allocator, memory)
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
            vec![LogicalType::BigInt],
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
            vec![LogicalType::Varchar],
            vec![LogicalType::BigInt],
            Vec::new(),
            allocator.clone(),
            detached_memory(),
        )
        .expect("empty snapshot index");
        assert_eq!(index.aggregate_columns.len(), 1);
        assert_eq!(index.aggregate_columns[0].len(), 0);

        let keys = paro_common::chunk::Chunk::from_vectors(
            vec![test_string_vector_with_allocator(
                &["absent"],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut selection = SelectionVector::try_with_capacity(1, allocator).expect("selection");
        assert!(index.select_rows(&keys, &mut selection).is_err());
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
            vec![LogicalType::BigInt],
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
            vec![LogicalType::BigInt],
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
            vec![LogicalType::BigInt],
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
