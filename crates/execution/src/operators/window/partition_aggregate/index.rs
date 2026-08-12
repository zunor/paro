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

const EMPTY_DENSE_SLOT: u32 = u32::MAX;
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
}

/// Group keys and finalized aggregate vectors published as one immutable unit.
#[derive(Debug)]
pub(crate) struct FinalizedPartitionIndex {
    group_count: usize,
    aggregate_columns: Box<[Arc<Vector>]>,
    keys: PartitionKeyIndex,
    _index_memory: RetainedMemoryHandle,
}

impl FinalizedPartitionIndex {
    pub(crate) fn try_new(
        key_type: LogicalType,
        aggregate_count: usize,
        chunks: Vec<Chunk>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        if !matches!(key_type, LogicalType::Integer | LogicalType::BigInt) || aggregate_count == 0 {
            return Err(paro_error::internal(
                "partition aggregate index requires an INTEGER/BIGINT key and aggregate values",
            ));
        }
        let group_count = 1;
        let chunks: Arc<[Chunk]> = Arc::from(chunks.into_boxed_slice());
        for (index, chunk) in chunks.iter().enumerate() {
            if chunk.column_count() != group_count + aggregate_count {
                return Err(paro_error::internal(format!(
                    "partition aggregate result chunk {index} width mismatch: expected={}, actual={}",
                    group_count + aggregate_count,
                    chunk.column_count()
                )));
            }
        }
        if chunks
            .iter()
            .any(|chunk| chunk.types().first() != Some(&key_type))
        {
            return Err(paro_error::internal(
                "partition aggregate finalized key type disagrees with its physical plan",
            ));
        }
        let (keys, index_memory) = build_integer_index(&chunks, key_type, memory.clone())?;
        let aggregate_columns = flatten_aggregate_columns(&chunks, group_count, aggregate_count)?;
        Ok(Self {
            group_count,
            aggregate_columns,
            keys,
            _index_memory: index_memory,
        })
    }

    pub(crate) fn lookup(&self, keys: &Chunk, row: usize) -> Result<AggregateRowRef> {
        if keys.column_count() != self.group_count || row >= keys.size() {
            return Err(paro_error::internal(
                "partition aggregate lookup key shape mismatch",
            ));
        }
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
        };
        result.ok_or_else(|| {
            paro_error::internal(format!(
                "partition aggregate detail row {row} has no finalized group"
            ))
        })
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
        if selection.capacity() < keys.size() || !selection.is_uniquely_owned() {
            *selection =
                SelectionVector::try_with_capacity(keys.size().max(1), output.allocator().clone())?;
        }
        selection.set_len(keys.size());
        for row in 0..keys.size() {
            let reference = self.lookup(keys, row)?;
            selection.try_set(row, reference.0 as usize)?;
        }
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

fn flatten_aggregate_columns(
    chunks: &[Chunk],
    group_count: usize,
    aggregate_count: usize,
) -> Result<Box<[Arc<Vector>]>> {
    let total_rows = chunks.iter().try_fold(0usize, |total, chunk| {
        total.checked_add(chunk.size()).ok_or_else(|| {
            paro_error::out_of_range("partition aggregate finalized row count overflow")
        })
    })?;
    if total_rows == 0 {
        return Ok(Box::new([]));
    }
    if total_rows >= EMPTY_DENSE_SLOT as usize {
        return Err(paro_error::not_implemented(
            "partition aggregate finalized domain exceeds dictionary index width",
        ));
    }
    let first = chunks
        .first()
        .expect("non-empty finalized domain has a result chunk");
    if chunks.len() == 1 {
        return Ok((0..aggregate_count)
            .map(|aggregate_idx| {
                Arc::clone(
                    first
                        .column(group_count + aggregate_idx)
                        .expect("verified finalized aggregate width"),
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice());
    }
    let mut columns = Vec::with_capacity(aggregate_count);
    for aggregate_idx in 0..aggregate_count {
        let source_index = group_count + aggregate_idx;
        let source = first
            .column(source_index)
            .expect("verified finalized aggregate width");
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
                .expect("verified finalized aggregate width");
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
            LogicalType::Integer,
            1,
            vec![results],
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
        let index = FinalizedPartitionIndex::try_new(
            LogicalType::Integer,
            1,
            Vec::new(),
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
            LogicalType::BigInt,
            1,
            vec![results],
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
            LogicalType::BigInt,
            1,
            vec![results],
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
}
