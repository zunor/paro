// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate-owned raw payload spill partitions.
//!
//! Build-phase external aggregate spill cannot safely use finalized aggregate
//! output as an intermediate representation. This buffer stores projected input
//! payload rows plus their grouping hash in radix partitions so a later
//! `AggregateBuildSpillReclaimer` can replay bounded partitions through the
//! normal aggregate update path.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::row::{
    RadixPartitionedRows, RadixPartitionedRowsBuilder, RowFormat, RowLayout, RowValidityType,
};

use super::radix_partitioned_aggregate_hashtable::{AggregateHTScanPosition, AggregateHashTable};
use super::row_format::{AggregatePayloadFormat, AggregateStateFormat};

#[derive(Debug)]
pub(crate) struct AggregatePayloadSpillBuffer {
    format: AggregatePayloadFormat,
    builder: RadixPartitionedRowsBuilder,
}

#[derive(Debug)]
pub(crate) struct AggregateStateSpillBuffer {
    format: AggregateStateFormat,
    encoding: AggregateStateEncoding,
    builder: RadixPartitionedRowsBuilder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateStateEncoding {
    RawBytes,
    FunctionSerialized,
}

impl AggregatePayloadSpillBuffer {
    pub(crate) fn new(
        buffer_pool: Arc<BufferPool>,
        payload_types: impl IntoIterator<Item = LogicalType>,
        radix_bits: usize,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        let format = AggregatePayloadFormat::new(payload_types);
        let layout = Arc::new(RowLayout::from_types(
            format.logical_types().to_vec(),
            RowValidityType::CanHaveNullValues,
        ));
        let builder = RadixPartitionedRowsBuilder::new_with_memory(
            buffer_pool,
            layout,
            MemoryTag::HashTable,
            radix_bits,
            AggregatePayloadFormat::HASH_COL_IDX,
            memory,
        )?;
        Ok(Self { format, builder })
    }

    pub(crate) fn append_payload(&mut self, payload: &Chunk, hashes: &Vector) -> Result<()> {
        if hashes.logical_type() != &LogicalType::UBigInt {
            return Err(paro_error::internal(format!(
                "aggregate payload spill hash vector must be UBigInt, found {:?}",
                hashes.logical_type()
            )));
        }
        if hashes.len() != payload.size() {
            return Err(paro_error::internal(format!(
                "aggregate payload spill hash row mismatch: hashes={} payload_rows={}",
                hashes.len(),
                payload.size()
            )));
        }
        if payload.column_count() != self.format.payload_width()
            || payload.types() != self.format.payload_types()
        {
            return Err(paro_error::internal(format!(
                "aggregate payload spill schema mismatch: expected {:?}, got {:?}",
                self.format.payload_types(),
                payload.types()
            )));
        }

        let mut vectors = Vec::with_capacity(payload.column_count() + 1);
        vectors.push(Arc::new(hashes.clone()));
        for col_idx in 0..payload.column_count() {
            vectors.push(Arc::clone(payload.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!("missing aggregate payload spill column {col_idx}"))
            })?));
        }
        let spill_chunk = Chunk::from_arc_vectors(vectors, payload.allocator().clone());
        self.builder.append(&spill_chunk)
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn size_in_bytes(&self) -> usize {
        self.builder.size_in_bytes()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn count(&self) -> u64 {
        self.builder.count()
    }

    pub(crate) fn seal(self) -> AggregateSpilledPayload {
        AggregateSpilledPayload {
            format: self.format,
            rows: self.builder.seal(),
        }
    }
}

impl AggregateStateSpillBuffer {
    pub(crate) fn new(
        buffer_pool: Arc<BufferPool>,
        group_types: impl IntoIterator<Item = LogicalType>,
        state_width: usize,
        encoding: AggregateStateEncoding,
        radix_bits: usize,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        let format = AggregateStateFormat::new(group_types, state_width);
        let layout = Arc::new(RowLayout::from_types(
            format.logical_types().to_vec(),
            RowValidityType::CanHaveNullValues,
        ));
        let builder = RadixPartitionedRowsBuilder::new_with_memory(
            buffer_pool,
            layout,
            MemoryTag::HashTable,
            radix_bits,
            AggregateStateFormat::HASH_COL_IDX,
            memory,
        )?;
        Ok(Self {
            format,
            encoding,
            builder,
        })
    }

    pub(crate) fn append_table(&mut self, table: &AggregateHashTable) -> Result<()> {
        let mut state_chunk = Chunk::try_initialize(
            self.format.logical_types(),
            paro_common::vector::VECTOR_SIZE,
            table.allocator(),
        )?;
        let mut position = AggregateHTScanPosition::default();
        match self.encoding {
            AggregateStateEncoding::RawBytes => {
                while table.scan_state_rows(&mut position, &mut state_chunk)? {
                    self.builder.append(&state_chunk)?;
                }
            }
            AggregateStateEncoding::FunctionSerialized => {
                while table.scan_serialized_state_rows(&mut position, &mut state_chunk)? {
                    self.builder.append(&state_chunk)?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> usize {
        self.builder.size_in_bytes()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn count(&self) -> u64 {
        self.builder.count()
    }

    pub(crate) fn seal(self) -> AggregateSpilledState {
        AggregateSpilledState {
            format: self.format,
            encoding: self.encoding,
            rows: self.builder.seal(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct AggregateSpilledPayload {
    format: AggregatePayloadFormat,
    rows: RadixPartitionedRows,
}

#[derive(Debug)]
pub(crate) struct AggregateSpilledState {
    format: AggregateStateFormat,
    encoding: AggregateStateEncoding,
    rows: RadixPartitionedRows,
}

impl AggregateSpilledPayload {
    #[inline]
    pub(crate) fn partition_count(&self) -> usize {
        self.rows.partition_count()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn count(&self) -> u64 {
        self.rows.count()
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> usize {
        self.rows.size_in_bytes()
    }

    pub(crate) fn replay_partition_payloads(
        &self,
        partition_idx: usize,
        allocator: Arc<dyn Allocator>,
        mut replay: impl FnMut(&Chunk) -> Result<()>,
    ) -> Result<()> {
        if partition_idx >= self.partition_count() {
            return Err(paro_error::internal(format!(
                "aggregate spilled payload partition out of bounds: partition_idx={} partition_count={}",
                partition_idx,
                self.partition_count()
            )));
        }

        let mut spill_chunk = Chunk::try_initialize(
            self.format.logical_types(),
            paro_common::vector::VECTOR_SIZE,
            allocator.clone(),
        )?;
        let mut scanner = self.rows.partition(partition_idx).scanner();
        loop {
            let scanned = scanner.next_chunk(&mut spill_chunk)?;
            if scanned == 0 {
                break;
            }
            let payload = payload_view_without_hash(&spill_chunk, &self.format, allocator.clone())?;
            replay(&payload)?;
        }
        Ok(())
    }
}

impl AggregateSpilledState {
    #[inline]
    pub(crate) fn partition_count(&self) -> usize {
        self.rows.partition_count()
    }

    #[inline]
    pub(crate) fn size_in_bytes(&self) -> usize {
        self.rows.size_in_bytes()
    }

    #[inline]
    pub(crate) fn state_width(&self) -> usize {
        self.format.state_width()
    }

    #[inline]
    pub(crate) fn encoding(&self) -> AggregateStateEncoding {
        self.encoding
    }

    pub(crate) fn replay_partition_state_rows(
        &self,
        partition_idx: usize,
        allocator: Arc<dyn Allocator>,
        mut replay: impl FnMut(&Vector, &Chunk, &Vector) -> Result<()>,
    ) -> Result<()> {
        if partition_idx >= self.partition_count() {
            return Err(paro_error::internal(format!(
                "aggregate spilled state partition out of bounds: partition_idx={} partition_count={}",
                partition_idx,
                self.partition_count()
            )));
        }

        let mut spill_chunk = Chunk::try_initialize(
            self.format.logical_types(),
            paro_common::vector::VECTOR_SIZE,
            allocator.clone(),
        )?;
        let mut scanner = self.rows.partition(partition_idx).scanner();
        loop {
            let scanned = scanner.next_chunk(&mut spill_chunk)?;
            if scanned == 0 {
                break;
            }
            let hashes = spill_chunk
                .column(AggregateStateFormat::HASH_COL_IDX)
                .ok_or_else(|| {
                    paro_error::internal("missing spilled aggregate state hash column")
                })?;
            let state_blobs = spill_chunk
                .column(self.format.state_col_idx())
                .ok_or_else(|| {
                    paro_error::internal("missing spilled aggregate state blob column")
                })?;
            let groups = state_group_view(&spill_chunk, &self.format, allocator.clone())?;
            replay(hashes, &groups, state_blobs)?;
        }
        Ok(())
    }
}

fn payload_view_without_hash(
    spill_chunk: &Chunk,
    format: &AggregatePayloadFormat,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    if spill_chunk.column_count() != format.logical_types().len() {
        return Err(paro_error::internal(format!(
            "aggregate spilled payload chunk width mismatch: expected={} actual={}",
            format.logical_types().len(),
            spill_chunk.column_count()
        )));
    }

    let mut vectors = Vec::with_capacity(format.payload_width());
    for col_idx in 0..format.payload_width() {
        vectors.push(Arc::clone(spill_chunk.column(col_idx + 1).ok_or_else(
            || {
                paro_error::internal(format!(
                    "missing spilled aggregate payload column {}",
                    col_idx + 1
                ))
            },
        )?));
    }
    if vectors.is_empty() {
        let mut payload = Chunk::try_initialize(&[], spill_chunk.size(), allocator)?;
        payload.try_set_cardinality(spill_chunk.size())?;
        return Ok(payload);
    }
    Ok(Chunk::from_arc_vectors(vectors, allocator))
}

fn state_group_view(
    spill_chunk: &Chunk,
    format: &AggregateStateFormat,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    if spill_chunk.column_count() != format.logical_types().len() {
        return Err(paro_error::internal(format!(
            "aggregate spilled state chunk width mismatch: expected={} actual={}",
            format.logical_types().len(),
            spill_chunk.column_count()
        )));
    }

    let mut vectors = Vec::with_capacity(format.group_width());
    for col_idx in 0..format.group_width() {
        vectors.push(Arc::clone(spill_chunk.column(col_idx + 1).ok_or_else(
            || {
                paro_error::internal(format!(
                    "missing spilled aggregate state group column {}",
                    col_idx + 1
                ))
            },
        )?));
    }
    if vectors.is_empty() {
        let mut groups = Chunk::try_initialize(&[], spill_chunk.size(), allocator)?;
        groups.try_set_cardinality(spill_chunk.size())?;
        return Ok(groups);
    }
    Ok(Chunk::from_arc_vectors(vectors, allocator))
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::runtime_value::Value;
    use paro_common::vector::{Vector, VECTOR_SIZE};
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
    use paro_planner::expression::{AggregateExpression, Expression, ReferenceExpression};

    use crate::operators::aggregate::aggregate_kernel::combine_states;
    use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
    use crate::operators::aggregate::build_helpers::{
        aggregate_objects, build_groups_chunk, create_hash_aggregate_tables, group_payload_refs,
        group_types, normalized_grouping_sets, update_hash_aggregate_tables,
    };
    use crate::operators::aggregate::group_hash::hash_group_columns;
    use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHTScanPosition;
    use crate::physical::specs::{AggregateSpec, GroupKeyEncoding};

    fn reference(index: usize, ty: LogicalType) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, ty))
    }

    fn count_star_expression() -> Expression {
        Expression::Aggregate(AggregateExpression::new(
            get_count_star_function(),
            vec![],
            LogicalType::BigInt,
        ))
    }

    fn grouped_count_spec() -> AggregateSpec {
        AggregateSpec {
            grouping_key_count: 1,
            estimated_input_rows: None,
            projection_exprs: Box::new([]),
            payload_types: Box::new([LogicalType::Integer]),
            groups: Box::new([reference(0, LogicalType::Integer)]),
            group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([count_star_expression()]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([])]),
            aggregate_filters: Box::new([None]),
            aggregate_orders: Box::new([Box::new([])]),
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["k".to_string(), "count".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
        }
    }

    fn grouped_varchar_count_spec() -> AggregateSpec {
        AggregateSpec {
            grouping_key_count: 1,
            estimated_input_rows: None,
            projection_exprs: Box::new([]),
            payload_types: Box::new([LogicalType::Varchar]),
            groups: Box::new([reference(0, LogicalType::Varchar)]),
            group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([count_star_expression()]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([])]),
            aggregate_filters: Box::new([None]),
            aggregate_orders: Box::new([Box::new([])]),
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["k".to_string(), "count".to_string()]),
            output_types: Box::new([LogicalType::Varchar, LogicalType::BigInt]),
        }
    }

    #[test]
    fn aggregate_payload_spill_replays_radix_partitions_into_hash_table() {
        let allocator = paro_common::test_utils::test_allocator();
        let buffer_pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let spec = grouped_count_spec();
        let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
        let group_refs = group_payload_refs(&spec).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let mut payload =
            Chunk::try_initialize(&[LogicalType::Integer], 5, allocator.clone()).expect("payload");
        payload.set_cardinality(5);
        for (row_idx, value) in [1, 2, 1, 3, 2].into_iter().enumerate() {
            payload
                .column_mut(0)
                .expect("group column")
                .set_value(row_idx, &Value::Integer(value));
        }

        let mut tables = create_hash_aggregate_tables(&spec, allocator.clone(), memory.clone(), 1)
            .expect("tables");
        let groups = build_groups_chunk(&payload, &group_refs).expect("groups");
        let hashes = tables[0].hash_groups(&groups).expect("hash groups");

        let mut spill =
            AggregatePayloadSpillBuffer::new(Arc::clone(&buffer_pool), payload.types(), 1, memory)
                .expect("spill");
        spill
            .append_payload(&payload, &hashes)
            .expect("append spill");
        assert_eq!(spill.count(), 5);
        assert!(spill.size_in_bytes() > 0);

        let spilled = spill.seal();
        assert_eq!(spilled.partition_count(), 2);
        assert_eq!(spilled.count(), 5);
        assert!(spilled.size_in_bytes() > 0);

        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 5);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(5);
        for partition_idx in 0..spilled.partition_count() {
            spilled
                .replay_partition_payloads(partition_idx, allocator.clone(), |payload_batch| {
                    let groups = build_groups_chunk(payload_batch, &group_refs)?;
                    update_hash_aggregate_tables(
                        &spec,
                        &aggregate_objects,
                        payload_batch,
                        &groups,
                        &grouping_sets,
                        &mut tables,
                        &mut addresses,
                        &mut new_groups,
                    )
                })
                .expect("replay partition");
        }

        let mut output =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::BigInt], 8, allocator)
                .expect("output");
        let mut position = AggregateHTScanPosition::default();
        assert!(tables[0].scan(&mut position, &mut output).expect("scan"));
        let mut actual = (0..output.size())
            .map(|row| {
                (
                    output
                        .column(0)
                        .expect("group")
                        .get_i32(row)
                        .expect("group"),
                    output
                        .column(1)
                        .expect("count")
                        .get_i64(row)
                        .expect("count"),
                )
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, vec![(1, 2), (2, 2), (3, 1)]);
    }

    #[test]
    fn aggregate_payload_spill_preserves_varchar_group_keys_across_batches() {
        let allocator = paro_common::test_utils::test_allocator();
        let buffer_pool = Arc::new(BufferPool::new(64 * 1024 * 1024));
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let spec = grouped_varchar_count_spec();
        let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
        let group_refs = group_payload_refs(&spec).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>();
        let values = [
            "PERU",
            "UNITED KINGDOM",
            "A deliberately long grouping key that lives outside the inline string payload",
        ];
        let row_count = VECTOR_SIZE * 3 + 17;
        let mut spill = AggregatePayloadSpillBuffer::new(
            Arc::clone(&buffer_pool),
            [LogicalType::Varchar],
            3,
            memory.clone(),
        )
        .expect("spill");
        for batch_start in (0..row_count).step_by(VECTOR_SIZE) {
            let batch_size = (row_count - batch_start).min(VECTOR_SIZE);
            let mut payload =
                Chunk::try_initialize(&[LogicalType::Varchar], batch_size, allocator.clone())
                    .expect("payload");
            payload.set_cardinality(batch_size);
            for row_idx in 0..batch_size {
                let value = values[(batch_start + row_idx) % values.len()];
                payload
                    .column_mut(0)
                    .expect("group column")
                    .set_value(row_idx, &Value::Varchar(value.to_string()));
            }
            let groups = build_groups_chunk(&payload, &group_refs).expect("groups");
            let hashes = hash_group_columns(&groups).expect("hashes");
            spill
                .append_payload(&payload, &hashes)
                .expect("append spill");
        }

        let spilled = spill.seal();
        let mut tables =
            create_hash_aggregate_tables(&spec, allocator.clone(), memory, 1).expect("tables");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
        for partition_idx in 0..spilled.partition_count() {
            spilled
                .replay_partition_payloads(partition_idx, allocator.clone(), |payload_batch| {
                    let groups = build_groups_chunk(payload_batch, &group_refs)?;
                    update_hash_aggregate_tables(
                        &spec,
                        &aggregate_objects,
                        payload_batch,
                        &groups,
                        &grouping_sets,
                        &mut tables,
                        &mut addresses,
                        &mut new_groups,
                    )
                })
                .expect("replay partition");
        }

        let mut output = Chunk::try_initialize(
            &[LogicalType::Varchar, LogicalType::BigInt],
            values.len(),
            allocator,
        )
        .expect("output");
        let mut position = AggregateHTScanPosition::default();
        let mut actual = Vec::new();
        while tables[0].scan(&mut position, &mut output).expect("scan") {
            actual.extend((0..output.size()).map(|row| {
                (
                    output
                        .column(0)
                        .unwrap()
                        .get_string(row)
                        .unwrap()
                        .to_string(),
                    output.column(1).unwrap().get_i64(row).unwrap(),
                )
            }));
        }
        actual.sort_unstable();
        let mut expected = values
            .iter()
            .enumerate()
            .map(|(value_idx, value)| {
                let count = (value_idx..row_count).step_by(values.len()).count() as i64;
                ((*value).to_string(), count)
            })
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn aggregate_state_spill_replays_partial_states_into_hash_table() {
        let allocator = paro_common::test_utils::test_allocator();
        let buffer_pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let spec = grouped_count_spec();
        let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
        let group_refs = group_payload_refs(&spec).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let mut payload =
            Chunk::try_initialize(&[LogicalType::Integer], 5, allocator.clone()).expect("payload");
        payload.set_cardinality(5);
        for (row_idx, value) in [1, 2, 1, 3, 2].into_iter().enumerate() {
            payload
                .column_mut(0)
                .expect("group column")
                .set_value(row_idx, &Value::Integer(value));
        }

        let mut source_tables =
            create_hash_aggregate_tables(&spec, allocator.clone(), memory.clone(), 1)
                .expect("source tables");
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, 5);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(5);
        let groups = build_groups_chunk(&payload, &group_refs).expect("groups");
        update_hash_aggregate_tables(
            &spec,
            &aggregate_objects,
            &payload,
            &groups,
            &grouping_sets,
            &mut source_tables,
            &mut addresses,
            &mut new_groups,
        )
        .expect("build source aggregate table");

        let state_width = AggregateStateLayout::new(&aggregate_objects)
            .expect("state layout")
            .total_size();
        let mut spill = AggregateStateSpillBuffer::new(
            Arc::clone(&buffer_pool),
            group_types(&spec).expect("group types"),
            state_width,
            AggregateStateEncoding::RawBytes,
            1,
            memory.clone(),
        )
        .expect("state spill");
        spill
            .append_table(&source_tables[0])
            .expect("append state spill");
        assert_eq!(spill.count(), 3);
        assert!(spill.size_in_bytes() > 0);
        let spilled = spill.seal();
        assert_eq!(spilled.partition_count(), 2);
        assert!(spilled.size_in_bytes() > 0);

        let mut target_tables = create_hash_aggregate_tables(&spec, allocator.clone(), memory, 1)
            .expect("target tables");
        let mut target_addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
        let mut target_new_groups =
            paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
        for partition_idx in 0..spilled.partition_count() {
            spilled
                .replay_partition_state_rows(
                    partition_idx,
                    allocator.clone(),
                    |hashes, groups, state_blobs| {
                        target_tables[0].find_or_create_groups(
                            groups,
                            hashes,
                            &mut target_addresses,
                            &mut target_new_groups,
                        )?;
                        let (mut state_words, source_addresses) =
                            materialize_state_addresses_for_test(
                                allocator.clone(),
                                state_blobs,
                                groups.size(),
                                state_width,
                            )?;
                        let mut arena =
                            paro_common::allocator::ArenaAllocator::new(allocator.clone());
                        let mut input_data = AggregateInputData::new(
                            None,
                            &mut arena,
                            AggregateCombineType::PreserveInput,
                        );
                        combine_states(
                            &aggregate_objects,
                            &mut input_data,
                            &source_addresses,
                            &target_addresses,
                            groups.size(),
                        )?;
                        state_words.clear();
                        Ok(())
                    },
                )
                .expect("replay state partition");
        }

        let mut output =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::BigInt], 8, allocator)
                .expect("output");
        let mut position = AggregateHTScanPosition::default();
        assert!(target_tables[0]
            .scan(&mut position, &mut output)
            .expect("scan"));
        let mut actual = (0..output.size())
            .map(|row| {
                (
                    output
                        .column(0)
                        .expect("group")
                        .get_i32(row)
                        .expect("group"),
                    output
                        .column(1)
                        .expect("count")
                        .get_i64(row)
                        .expect("count"),
                )
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, vec![(1, 2), (2, 2), (3, 1)]);
    }

    fn materialize_state_addresses_for_test(
        allocator: Arc<dyn paro_common::allocator::Allocator>,
        state_blobs: &Vector,
        row_count: usize,
        state_width: usize,
    ) -> Result<(Vec<u64>, Vector)> {
        let words_per_state = state_width.div_ceil(std::mem::size_of::<u64>());
        let mut state_words = vec![0u64; row_count * words_per_state];
        for row_idx in 0..row_count {
            let blob = state_blobs.get_blob(row_idx).expect("spilled state blob");
            assert_eq!(blob.len(), state_width);
            let dest = unsafe {
                std::slice::from_raw_parts_mut(
                    state_words
                        .as_mut_ptr()
                        .add(row_idx * words_per_state)
                        .cast::<u8>(),
                    state_width,
                )
            };
            dest.copy_from_slice(blob);
        }
        let mut addresses = Vector::try_new(LogicalType::BigInt, row_count, allocator)?;
        addresses.try_set_count(row_count)?;
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        for row_idx in 0..row_count {
            unsafe {
                *address_data.add(row_idx) = state_words
                    .as_mut_ptr()
                    .add(row_idx * words_per_state)
                    .cast::<u8>();
            }
        }
        Ok((state_words, addresses))
    }
}
