// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime handle for SQL set operations.
//!
//! Producers append task-local chunks during merge. Independent radix finish
//! tasks resolve multiplicities; publication restores first-seen order with
//! dictionary views over the input, without copying boxed values to output.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedHashMap, AccountedVec, MemoryAccountingClass, MemoryAccountingContext,
};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_context::StatementCancellation;
use paro_function::table::TableFunctionRuntimeContext;
use paro_planner::logical::operator::SetOpType;

use crate::physical::properties::MemoryClass;
use crate::physical::specs::{SetOperationInputSide, SetOperationSpec};
use crate::runtime::context::{FinishTaskId, OperatorCleanupContext, OperatorFinishContext};
use crate::runtime::sink::{
    FinishCoordinatorParticipation, FinishTaskGroup, FinishTaskPoll, FinishWork, NextFinishTask,
    ParallelFinishDriver,
};

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::radix::{finish_routing, radix_work, RadixChunk, RadixRows};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct SetOperationHandle {
    metadata: BreakerHandleMetadata,
    left_chunks: Mutex<Vec<RadixChunk>>,
    right_chunks: Mutex<Vec<RadixChunk>>,
    sealed_chunks: OnceLock<Arc<[Chunk]>>,
    sealed: AtomicBool,
    cleanup: CleanupState,
}

impl SetOperationHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            left_chunks: Mutex::new(Vec::new()),
            right_chunks: Mutex::new(Vec::new()),
            sealed_chunks: OnceLock::new(),
            sealed: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn routing_bytes(&self) -> usize {
        self.left_chunks
            .lock()
            .iter()
            .map(|c| c.routing.retained_bytes())
            .sum::<usize>()
            + self
                .right_chunks
                .lock()
                .iter()
                .map(|c| c.routing.retained_bytes())
                .sum::<usize>()
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub(crate) fn append_chunks(
        &self,
        side: SetOperationInputSide,
        chunks: &mut Vec<RadixChunk>,
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append to a sealed set-operation handle",
            ));
        }
        match side {
            SetOperationInputSide::Left => self.left_chunks.lock().extend(chunks.drain(..)),
            SetOperationInputSide::Right => self.right_chunks.lock().extend(chunks.drain(..)),
        }
        Ok(())
    }

    pub(crate) fn finish_work(
        self: &Arc<Self>,
        spec: &SetOperationSpec,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishWork> {
        ctx.cancel.check()?;
        let left = std::mem::take(&mut *self.left_chunks.lock());
        let right = std::mem::take(&mut *self.right_chunks.lock());
        let left_count = left.len();
        let mut input = left;
        input.extend(right);
        if !(spec.op == SetOpType::Union && spec.all) {
            finish_routing(
                &mut input,
                &(0..spec.output_types.len()).collect::<Vec<_>>(),
                ctx.query.max_parallel_tasks().max(1).next_power_of_two(),
                || {
                    ctx.memory.accounted_allocator_for(
                        MemoryTag::HashTable,
                        MemoryAccountingClass::NonRevocable,
                    )
                },
                ctx.cancel,
            )?;
        }
        let (chunks, routing): (Vec<_>, Vec<_>) =
            input.into_iter().map(|c| (c.input, c.routing)).unzip();
        if spec.op == SetOpType::Union && spec.all {
            self.publish(chunks)?;
            return Ok(FinishWork::None);
        }
        let memory = ctx
            .query
            .memory_accounting_context(MemoryTag::HashTable, MemoryAccountingClass::NonRevocable);
        let work = radix_work(&chunks, &routing);
        let task_count = work.len();
        if task_count == 1 {
            let rows = partition_rows(&chunks, &routing, work[0].clone(), &memory, ctx.cancel)?;
            let mut output =
                evaluate_partition(spec, &chunks, left_count, &rows, &memory, ctx.cancel)?;
            output.sort_unstable_by_key(|row| row.first);
            let allocator = ctx
                .memory
                .accounted_allocator_for(MemoryTag::HashTable, MemoryAccountingClass::NonRevocable);
            self.publish(rows_to_chunks(&output, &chunks, allocator, ctx.cancel)?)?;
            return Ok(FinishWork::None);
        }
        Ok(FinishWork::Parallel(FinishTaskGroup {
            task_count,
            driver: Arc::new(SetFinalizeDriver {
                handle: self.clone(),
                spec: spec.clone(),
                chunks,
                left_count,
                memory,
                routing,
                work,
                results: Mutex::new((0..task_count).map(|_| None).collect()),
                next_task: AtomicUsize::new(0),
            }),
            memory_class: MemoryClass::Blocking,
            coordinator_participation: FinishCoordinatorParticipation::DrainAvailable,
        }))
    }

    fn publish(&self, chunks: Vec<Chunk>) -> Result<()> {
        self.sealed_chunks
            .set(Arc::from(chunks.into_boxed_slice()))
            .map_err(|_| paro_error::internal("set-operation handle was sealed twice"))?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    pub fn sealed_chunks(&self) -> Result<Arc<[Chunk]>> {
        self.sealed_chunks.get().map(Arc::clone).ok_or_else(|| {
            paro_error::internal("set-operation emit source polled before handle was sealed")
        })
    }

    #[inline]
    pub fn pending_chunk_count(&self, side: SetOperationInputSide) -> usize {
        match side {
            SetOperationInputSide::Left => self.left_chunks.lock().len(),
            SetOperationInputSide::Right => self.right_chunks.lock().len(),
        }
    }

    #[inline]
    pub fn sealed_chunk_count(&self) -> usize {
        self.sealed_chunks
            .get()
            .map(|chunks| chunks.len())
            .unwrap_or(0)
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for SetOperationHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.left_chunks.lock().clear();
        self.right_chunks.lock().clear();
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SetRow {
    chunk: usize,
    row: usize,
}

#[derive(Debug)]
struct SetCounts {
    first: SetRow,
    left_count: usize,
    right_count: usize,
}

/// Inputs remain alive throughout finalization, so keys can borrow their rows.
/// In particular, varlen keys never need a second owned copy of their payload.
#[derive(Debug, Clone, Copy)]
struct SetKey<'a> {
    chunk: &'a Chunk,
    row: usize,
}

impl Hash for SetKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.chunk.data.len().hash(state);
        for column in &self.chunk.data {
            if column.logical_type().is_utf8_varlen() && !column.is_null(self.row) {
                column
                    .get_string(self.row)
                    .expect("non-null string")
                    .hash(state);
            } else if column.logical_type() == &LogicalType::Blob && !column.is_null(self.row) {
                column
                    .get_blob(self.row)
                    .expect("non-null blob")
                    .hash(state);
            } else {
                // Preserve Value's bitwise floating-point and nested semantics.
                column.get_value(self.row).hash(state);
            }
        }
    }
}

impl PartialEq for SetKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.chunk.data.len() == other.chunk.data.len()
            && self
                .chunk
                .data
                .iter()
                .zip(&other.chunk.data)
                .all(|(left, right)| {
                    if left.logical_type().is_utf8_varlen()
                        && right.logical_type().is_utf8_varlen()
                        && !left.is_null(self.row)
                        && !right.is_null(other.row)
                    {
                        left.get_string(self.row) == right.get_string(other.row)
                    } else if left.logical_type() == &LogicalType::Blob
                        && right.logical_type() == &LogicalType::Blob
                        && !left.is_null(self.row)
                        && !right.is_null(other.row)
                    {
                        left.get_blob(self.row) == right.get_blob(other.row)
                    } else {
                        left.get_value(self.row) == right.get_value(other.row)
                    }
                })
    }
}

impl Eq for SetKey<'_> {}

/// A canonical NULL payload keeps equality and array hashing consistent across
/// vector encodings. The validity mask distinguishes NULL from integer zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NullableIntegerSetKey<T, const N: usize> {
    values: [T; N],
    valid_bits: u8,
}

impl<T: Copy + Default, const N: usize> NullableIntegerSetKey<T, N> {
    fn from_row(chunk: &Chunk, row: usize, read: impl Fn(&Vector, usize) -> Option<T>) -> Self {
        debug_assert!((1..=3).contains(&N));
        let mut key = Self {
            values: [T::default(); N],
            valid_bits: 0,
        };
        for (column, slot) in key.values.iter_mut().enumerate() {
            if let Some(value) = read(&chunk.data[column], row) {
                *slot = value;
                key.valid_bits |= 1 << column;
            }
        }
        key
    }
}

#[derive(Debug, Clone, Copy)]
struct SetOutputRow {
    first: SetRow,
    repeats: usize,
}

fn metadata_vec<T>(memory: &MemoryAccountingContext) -> Result<AccountedVec<T>> {
    Ok(AccountedVec::new_with_accounting(
        memory.grant()?,
        memory.tag(),
        memory.accounting_class(),
    ))
}

fn push_row<T>(rows: &mut AccountedVec<T>, row: T) -> Result<()> {
    if rows.len() == rows.capacity() {
        rows.try_reserve(rows.capacity().max(VECTOR_SIZE))?;
    }
    rows.try_push(row)?;
    Ok(())
}

fn partition_rows(
    chunks: &[Chunk],
    routing: &[RadixRows],
    partitions: std::ops::Range<usize>,
    memory: &MemoryAccountingContext,
    cancel: &StatementCancellation,
) -> Result<AccountedVec<SetRow>> {
    let mut rows = metadata_vec(memory)?;
    rows.try_reserve(
        chunks
            .iter()
            .zip(routing)
            .map(|(c, r)| {
                partitions
                    .clone()
                    .map(|p| r.count(p, c.size()))
                    .sum::<usize>()
            })
            .sum(),
    )?;
    for (chunk_idx, (chunk, route)) in chunks.iter().zip(routing).enumerate() {
        cancel.check()?;
        for row in partitions.clone().flat_map(|p| route.rows(p, chunk.size())) {
            rows.try_push(SetRow {
                chunk: chunk_idx,
                row,
            })?;
        }
    }
    Ok(rows)
}

fn evaluate_partition(
    spec: &SetOperationSpec,
    chunks: &[Chunk],
    left_count: usize,
    rows: &[SetRow],
    memory: &MemoryAccountingContext,
    cancel: &StatementCancellation,
) -> Result<AccountedVec<SetOutputRow>> {
    // Small integer keys are often repeated many times (for example dimension
    // domains). Decode each candidate once instead of revisiting both vectors
    // during hash-chain equality. Wider/varlen rows keep the borrowed key path.
    match spec.output_types.as_ref() {
        [LogicalType::Integer] => {
            evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| {
                NullableIntegerSetKey::<i32, 1>::from_row(
                    &chunks[row.chunk],
                    row.row,
                    Vector::get_i32,
                )
            })
        }
        [LogicalType::Integer, LogicalType::Integer] => {
            evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| {
                NullableIntegerSetKey::<i32, 2>::from_row(
                    &chunks[row.chunk],
                    row.row,
                    Vector::get_i32,
                )
            })
        }
        [LogicalType::Integer, LogicalType::Integer, LogicalType::Integer] => {
            evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| {
                NullableIntegerSetKey::<i32, 3>::from_row(
                    &chunks[row.chunk],
                    row.row,
                    Vector::get_i32,
                )
            })
        }
        [LogicalType::BigInt] => {
            evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| {
                NullableIntegerSetKey::<i64, 1>::from_row(
                    &chunks[row.chunk],
                    row.row,
                    Vector::get_i64,
                )
            })
        }
        [LogicalType::BigInt, LogicalType::BigInt] => {
            evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| {
                NullableIntegerSetKey::<i64, 2>::from_row(
                    &chunks[row.chunk],
                    row.row,
                    Vector::get_i64,
                )
            })
        }
        [LogicalType::BigInt, LogicalType::BigInt, LogicalType::BigInt] => {
            evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| {
                NullableIntegerSetKey::<i64, 3>::from_row(
                    &chunks[row.chunk],
                    row.row,
                    Vector::get_i64,
                )
            })
        }
        _ => evaluate_partition_keys(spec, left_count, rows, memory, cancel, |row| SetKey {
            chunk: &chunks[row.chunk],
            row: row.row,
        }),
    }
}

fn evaluate_partition_keys<K: Eq + Hash>(
    spec: &SetOperationSpec,
    left_count: usize,
    rows: &[SetRow],
    memory: &MemoryAccountingContext,
    cancel: &StatementCancellation,
    key_for_row: impl Fn(SetRow) -> K,
) -> Result<AccountedVec<SetOutputRow>> {
    let mut index = AccountedHashMap::<K, usize>::new_with_accounting(
        memory.grant()?,
        memory.tag(),
        memory.accounting_class(),
    );
    let mut counts = metadata_vec::<SetCounts>(memory)?;
    for (position, &row) in rows.iter().enumerate() {
        if position % VECTOR_SIZE == 0 {
            cancel.check()?;
        }
        let key = key_for_row(row);
        let idx = if let Some(&idx) = index.get(&key) {
            idx
        } else {
            let idx = counts.len();
            push_row(
                &mut counts,
                SetCounts {
                    first: row,
                    left_count: 0,
                    right_count: 0,
                },
            )?;
            index.try_insert(key, idx)?;
            idx
        };
        let c = &mut counts[idx];
        if row.chunk < left_count {
            c.left_count += 1;
        } else {
            c.right_count += 1;
        }
    }
    let mut output = metadata_vec(memory)?;
    output.try_reserve(counts.len())?;
    for c in counts.iter() {
        let repeats = output_repeats(spec, c.left_count, c.right_count);
        if repeats > 0 {
            output.try_push(SetOutputRow {
                first: c.first,
                repeats,
            })?;
        }
    }
    Ok(output)
}

#[derive(Debug)]
struct SetFinalizeDriver {
    handle: Arc<SetOperationHandle>,
    spec: SetOperationSpec,
    chunks: Vec<Chunk>,
    left_count: usize,
    memory: MemoryAccountingContext,
    routing: Vec<RadixRows>,
    work: Vec<std::ops::Range<usize>>,
    results: Mutex<Vec<Option<AccountedVec<SetOutputRow>>>>,
    next_task: AtomicUsize,
}

impl ParallelFinishDriver for SetFinalizeDriver {
    fn next_task(&self, ctx: &mut OperatorFinishContext) -> Result<NextFinishTask> {
        ctx.cancel.check()?;
        let idx = self.next_task.fetch_add(1, Ordering::Relaxed);
        if idx >= self.results.lock().len() {
            return Ok(NextFinishTask::Drained);
        }
        Ok(NextFinishTask::Task(FinishTaskId(
            u32::try_from(idx)
                .map_err(|_| paro_error::internal("set-operation task id overflow"))?,
        )))
    }

    fn run_task(
        &self,
        task: FinishTaskId,
        ctx: &mut OperatorFinishContext,
    ) -> Result<FinishTaskPoll> {
        ctx.cancel.check()?;
        let idx = task.0 as usize;
        let rows = partition_rows(
            &self.chunks,
            &self.routing,
            self.work[idx].clone(),
            &self.memory,
            ctx.cancel,
        )?;
        let result = evaluate_partition(
            &self.spec,
            &self.chunks,
            self.left_count,
            &rows,
            &self.memory,
            ctx.cancel,
        )?;
        self.results.lock()[idx] = Some(result);
        Ok(FinishTaskPoll::Done)
    }

    fn finish_group(&self, ctx: &mut OperatorFinishContext) -> Result<()> {
        ctx.cancel.check()?;
        let mut output = metadata_vec(&self.memory)?;
        for slot in self.results.lock().iter_mut() {
            let mut rows = slot
                .take()
                .ok_or_else(|| paro_error::internal("set-operation finish result missing"))?;
            output.try_extend(rows.drain())?;
        }
        output.sort_unstable_by_key(|row| row.first);
        let allocator = ctx
            .memory
            .accounted_allocator_for(MemoryTag::HashTable, MemoryAccountingClass::NonRevocable);
        self.handle.publish(rows_to_chunks(
            &output,
            &self.chunks,
            allocator,
            ctx.cancel,
        )?)
    }
}

#[cfg(test)]
fn evaluate_set_operation(
    spec: &SetOperationSpec,
    mut left: Vec<Chunk>,
    right: Vec<Chunk>,
    allocator: Arc<dyn Allocator>,
) -> Result<Arc<[Chunk]>> {
    let left_count = left.len();
    left.extend(right);
    if spec.op == SetOpType::Union && spec.all {
        return Ok(Arc::from(left));
    }
    let memory = MemoryAccountingContext::detached(
        MemoryTag::HashTable,
        MemoryAccountingClass::NonRevocable,
    );
    let cancel = StatementCancellation::new(tokio_util::sync::CancellationToken::new(), None);
    let mut router = super::radix::RadixRouter::default();
    let mut routing = Vec::new();
    for chunk in &mut left {
        let columns = (0..chunk.data.len()).collect::<Vec<_>>();
        let routed = router.route(chunk, &columns, 4, allocator.clone())?;
        *chunk = routed.input;
        routing.push(routed.routing);
    }
    let mut output = metadata_vec(&memory)?;
    for partition in 0..4 {
        let rows = partition_rows(&left, &routing, partition..partition + 1, &memory, &cancel)?;
        output.try_extend(
            evaluate_partition(spec, &left, left_count, &rows, &memory, &cancel)?.drain(),
        )?;
    }
    output.sort_unstable_by_key(|row| row.first);
    Ok(Arc::from(rows_to_chunks(
        &output, &left, allocator, &cancel,
    )?))
}

#[inline]
fn output_repeats(spec: &SetOperationSpec, left_count: usize, right_count: usize) -> usize {
    match (spec.op, spec.all) {
        (SetOpType::Union, false) => usize::from(left_count > 0 || right_count > 0),
        (SetOpType::Union, true) => left_count + right_count,
        (SetOpType::Intersect, false) => usize::from(left_count > 0 && right_count > 0),
        (SetOpType::Intersect, true) => left_count.min(right_count),
        (SetOpType::Except, false) => usize::from(left_count > 0 && right_count == 0),
        (SetOpType::Except, true) => left_count.saturating_sub(right_count),
    }
}

fn rows_to_chunks(
    rows: &[SetOutputRow],
    inputs: &[Chunk],
    allocator: Arc<dyn Allocator>,
    cancel: &StatementCancellation,
) -> Result<Vec<Chunk>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut chunks = Vec::new();
    let mut selection = SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?;
    selection.set_len(VECTOR_SIZE);
    let mut source: Option<usize> = None;
    let mut count = 0;
    for row in rows {
        for _ in 0..row.repeats {
            if count == VECTOR_SIZE || (count > 0 && source != Some(row.first.chunk)) {
                cancel.check()?;
                chunks.push(gather_output_chunk(
                    &inputs[source.expect("nonempty selection has a source")],
                    &mut selection,
                    count,
                    allocator.clone(),
                )?);
                selection = SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?;
                selection.set_len(VECTOR_SIZE);
                count = 0;
            }
            source = Some(row.first.chunk);
            selection.try_set(count, row.first.row)?;
            count += 1;
        }
    }
    if let Some(source) = source {
        cancel.check()?;
        chunks.push(gather_output_chunk(
            &inputs[source],
            &mut selection,
            count,
            allocator,
        )?);
    }
    Ok(chunks)
}

fn gather_output_chunk(
    input: &Chunk,
    selection: &mut SelectionVector,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<Chunk> {
    selection.set_len(count);
    let columns = input
        .data
        .iter()
        .map(|column| Vector::try_gather_ref(column.clone(), selection.clone()).map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    // ALL can repeat a row more times than its original chunk's capacity.
    // Construct a new logical batch instead of inheriting that capacity.
    Chunk::try_from_arc_vectors_with_cardinality(columns, count, allocator)
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::test_utils::{test_allocator, test_chunk_from_vectors};
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use super::*;

    fn spec(op: SetOpType, all: bool) -> SetOperationSpec {
        SetOperationSpec {
            table_index: 0,
            op,
            all,
            output_names: Box::new(["v".to_string()]),
            output_types: Box::new([LogicalType::Integer]),
        }
    }

    fn chunk(values: &[i32]) -> Chunk {
        test_chunk_from_vectors(vec![
            Vector::try_from_i32(values, test_allocator()).expect("vector")
        ])
    }

    fn values(chunks: Arc<[Chunk]>) -> Vec<Value> {
        chunks
            .iter()
            .flat_map(|chunk| (0..chunk.size()).map(|row| chunk.data[0].get_value(row)))
            .collect()
    }

    #[test]
    fn union_distinct_preserves_first_seen_order() {
        let chunks = evaluate_set_operation(
            &spec(SetOpType::Union, false),
            vec![chunk(&[2, 1, 2])],
            vec![chunk(&[1, 3])],
            test_allocator(),
        )
        .expect("set op");

        assert_eq!(
            values(chunks),
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(3)]
        );
    }

    #[test]
    fn union_all_concatenates_inputs() {
        let chunks = evaluate_set_operation(
            &spec(SetOpType::Union, true),
            vec![chunk(&[1, 1])],
            vec![chunk(&[2, 1])],
            test_allocator(),
        )
        .expect("set op");

        assert_eq!(
            values(chunks),
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1)
            ]
        );
    }

    #[test]
    fn intersect_all_uses_min_counts() {
        let chunks = evaluate_set_operation(
            &spec(SetOpType::Intersect, true),
            vec![chunk(&[1, 1, 2])],
            vec![chunk(&[1, 1, 1, 3])],
            test_allocator(),
        )
        .expect("set op");

        assert_eq!(values(chunks), vec![Value::Integer(1), Value::Integer(1)]);
    }

    #[test]
    fn except_distinct_preserves_left_first_order() {
        let chunks = evaluate_set_operation(
            &spec(SetOpType::Except, false),
            vec![chunk(&[3, 1, 3, 2])],
            vec![chunk(&[2])],
            test_allocator(),
        )
        .expect("set op");

        assert_eq!(values(chunks), vec![Value::Integer(3), Value::Integer(1)]);
    }

    #[test]
    fn except_all_subtracts_right_counts() {
        let chunks = evaluate_set_operation(
            &spec(SetOpType::Except, true),
            vec![chunk(&[1, 1, 1, 2])],
            vec![chunk(&[1, 2, 2])],
            test_allocator(),
        )
        .expect("set op");

        assert_eq!(values(chunks), vec![Value::Integer(1), Value::Integer(1)]);
    }

    #[test]
    fn all_repetitions_can_exceed_the_first_input_chunk_capacity() {
        let mut input =
            Chunk::try_initialize(&[LogicalType::Integer], 1, test_allocator()).unwrap();
        input.try_set_cardinality(1).unwrap();
        input.set_value(0, 0, &Value::Integer(7)).unwrap();
        let output = evaluate_set_operation(
            &spec(SetOpType::Except, true),
            vec![input; VECTOR_SIZE + 1],
            vec![],
            test_allocator(),
        )
        .unwrap();
        assert_eq!(values(output), vec![Value::Integer(7); VECTOR_SIZE + 1]);
    }

    #[test]
    fn oversized_input_and_repeated_output_cross_vector_boundaries() {
        let output = evaluate_set_operation(
            &spec(SetOpType::Except, true),
            vec![chunk(&vec![1; VECTOR_SIZE * 2 + 17])],
            vec![chunk(&vec![1; 17])],
            test_allocator(),
        )
        .expect("oversized set input");
        assert_eq!(values(output), vec![Value::Integer(1); VECTOR_SIZE * 2]);
    }

    fn value_chunk(types: &[LogicalType], rows: &[Vec<Value>]) -> Chunk {
        let mut chunk = Chunk::try_initialize(types, rows.len().max(1), test_allocator()).unwrap();
        chunk.try_set_cardinality(rows.len()).unwrap();
        for (row_idx, row) in rows.iter().enumerate() {
            for (column_idx, value) in row.iter().enumerate() {
                chunk.set_value(column_idx, row_idx, value).unwrap();
            }
        }
        chunk
    }

    #[test]
    fn borrowed_keys_match_owned_values_across_vector_encodings() {
        use std::collections::hash_map::DefaultHasher;

        let types = [LogicalType::Varchar, LogicalType::Blob, LogicalType::Double];
        let rows = vec![
            vec![
                Value::Varchar("a long string payload".into()),
                Value::Blob(vec![0, 255, 7]),
                Value::Double(0.0),
            ],
            vec![
                Value::Null(LogicalType::Varchar),
                Value::Null(LogicalType::Blob),
                Value::Double(-0.0),
            ],
            vec![
                Value::Varchar("a long string payload".into()),
                Value::Blob(vec![0, 255, 7]),
                Value::Double(f64::from_bits(0x7ff8_0000_0000_0001)),
            ],
        ];
        let flat = value_chunk(&types, &rows);
        let mut selection = SelectionVector::try_with_capacity(4, test_allocator()).unwrap();
        selection.set_len(4);
        for (idx, row) in [2, 0, 1, 2].into_iter().enumerate() {
            selection.try_set(idx, row).unwrap();
        }
        let dictionary = Chunk::try_from_arc_vectors_with_cardinality(
            flat.data
                .iter()
                .map(|column| {
                    Arc::new(Vector::try_gather_ref(column.clone(), selection.clone()).unwrap())
                })
                .collect(),
            4,
            test_allocator(),
        )
        .unwrap();
        let repeated = Chunk::try_from_arc_vectors_with_cardinality(
            flat.data
                .iter()
                .map(|column| Arc::new(Vector::try_broadcast_ref(column.clone(), 0, 4).unwrap()))
                .collect(),
            4,
            test_allocator(),
        )
        .unwrap();
        let constant = |row: &[Value]| {
            Chunk::try_from_arc_vectors_with_cardinality(
                types
                    .iter()
                    .zip(row)
                    .map(|(ty, value)| {
                        Arc::new(
                            Vector::try_constant_from_value(
                                ty.clone(),
                                value.clone(),
                                4,
                                test_allocator(),
                            )
                            .unwrap(),
                        )
                    })
                    .collect(),
                4,
                test_allocator(),
            )
            .unwrap()
        };
        let chunks = [
            flat,
            dictionary,
            repeated,
            constant(&rows[0]),
            constant(&rows[1]),
        ];
        let keys = chunks
            .iter()
            .flat_map(|chunk| (0..chunk.size()).map(move |row| SetKey { chunk, row }))
            .collect::<Vec<_>>();
        for left in &keys {
            for right in &keys {
                let owned = |key: &SetKey<'_>| {
                    key.chunk
                        .data
                        .iter()
                        .map(|column| column.get_value(key.row))
                        .collect::<Vec<_>>()
                };
                assert_eq!(*left == *right, owned(left) == owned(right));
                if left == right {
                    let hash = |key: &SetKey<'_>| {
                        let mut hasher = DefaultHasher::new();
                        key.hash(&mut hasher);
                        hasher.finish()
                    };
                    assert_eq!(hash(left), hash(right));
                }
            }
        }
    }

    #[test]
    fn borrowed_keys_preserve_mixed_type_set_multiplicities() {
        let types = [
            LogicalType::Varchar,
            LogicalType::Double,
            LogicalType::List(Box::new(LogicalType::Integer)),
        ];
        let row = |text: Option<&str>, value: f64, list: i32| {
            vec![
                text.map_or_else(
                    || Value::Null(LogicalType::Varchar),
                    |text| Value::Varchar(text.into()),
                ),
                Value::Double(value),
                Value::List(vec![Value::Integer(list)], LogicalType::Integer),
            ]
        };
        let a = row(Some("long shared string payload"), 0.0, 1);
        let b = row(Some("long shared string payload"), -0.0, 1);
        let c = row(None, f64::from_bits(0x7ff8_0000_0000_0001), 2);
        let d = row(None, f64::from_bits(0x7ff8_0000_0000_0002), 2);
        let left = vec![a.clone(), c.clone(), a.clone(), b.clone(), d.clone()];
        let right = vec![
            c.clone(),
            c.clone(),
            a.clone(),
            row(Some("right-only"), 1.0, 3),
        ];
        for op in [SetOpType::Union, SetOpType::Intersect, SetOpType::Except] {
            for all in [false, true] {
                let mut spec = spec(op, all);
                spec.output_types = types.clone().into();
                spec.output_names = ["text".into(), "value".into(), "list".into()].into();
                let output = evaluate_set_operation(
                    &spec,
                    vec![value_chunk(&types, &left)],
                    vec![value_chunk(&types, &right)],
                    test_allocator(),
                )
                .unwrap();
                let actual = output
                    .iter()
                    .flat_map(|chunk| {
                        (0..chunk.size()).map(|row| {
                            chunk
                                .data
                                .iter()
                                .map(|column| column.get_value(row))
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>();
                let mut expected = Vec::new();
                if op == SetOpType::Union && all {
                    expected.extend(left.iter().chain(&right).cloned());
                } else {
                    let mut seen = std::collections::HashSet::new();
                    for row in left.iter().chain(&right) {
                        if !seen.insert(row) {
                            continue;
                        }
                        let l = left.iter().filter(|value| *value == row).count();
                        let r = right.iter().filter(|value| *value == row).count();
                        let repeats = match (op, all) {
                            (SetOpType::Union, false) => 1,
                            (SetOpType::Intersect, false) => usize::from(l > 0 && r > 0),
                            (SetOpType::Intersect, true) => l.min(r),
                            (SetOpType::Except, false) => usize::from(l > 0 && r == 0),
                            (SetOpType::Except, true) => l.saturating_sub(r),
                            _ => unreachable!(),
                        };
                        expected.extend(std::iter::repeat_n(row.clone(), repeats));
                    }
                }
                assert_eq!(actual, expected, "{op:?} all={all}");
            }
        }
    }

    fn assert_nullable_integer_keys_match_scalar_values<
        T: Copy + Default + Eq + Hash,
        const N: usize,
    >(
        chunks: &[Chunk],
        read: impl Fn(&Vector, usize) -> Option<T>,
    ) {
        use std::collections::hash_map::DefaultHasher;

        let rows = chunks
            .iter()
            .enumerate()
            .flat_map(|(chunk, input)| (0..input.size()).map(move |row| SetRow { chunk, row }))
            .collect::<Vec<_>>();
        let keys = rows
            .iter()
            .map(|row| NullableIntegerSetKey::<T, N>::from_row(&chunks[row.chunk], row.row, &read))
            .collect::<Vec<_>>();
        let scalar = rows
            .iter()
            .map(|row| {
                chunks[row.chunk]
                    .data
                    .iter()
                    .map(|column| column.get_value(row.row))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        // The independent Value oracle covers NULL/zero distinctions and all
        // validity-mask combinations without rebuilding the packed key.
        for left in 0..keys.len() {
            for right in 0..keys.len() {
                assert_eq!(keys[left] == keys[right], scalar[left] == scalar[right]);
                if scalar[left] == scalar[right] {
                    let hash = |key: &NullableIntegerSetKey<T, N>| {
                        let mut hasher = DefaultHasher::new();
                        key.hash(&mut hasher);
                        hasher.finish()
                    };
                    assert_eq!(hash(&keys[left]), hash(&keys[right]));
                }
            }
        }
    }

    #[test]
    fn compact_integer_keys_preserve_high_duplicate_dictionary_null_multiplicities() {
        assert_eq!(std::mem::size_of::<NullableIntegerSetKey<i32, 3>>(), 16);
        let n = Value::Null(LogicalType::Integer);
        let base_rows = vec![
            vec![n.clone(), Value::Integer(1), Value::Integer(-1)],
            vec![Value::Integer(0), n.clone(), Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(2), n.clone()],
            vec![
                Value::Integer(i32::MIN),
                Value::Integer(i32::MAX),
                Value::Integer(3),
            ],
            vec![n.clone(), Value::Integer(2), Value::Integer(-1)],
            vec![Value::Integer(4), Value::Integer(5), Value::Integer(6)],
            vec![Value::Integer(7), Value::Integer(8), Value::Integer(9)],
            vec![n.clone(), n.clone(), n.clone()],
            vec![Value::Integer(0), Value::Integer(0), Value::Integer(0)],
            vec![n.clone(), Value::Integer(0), Value::Integer(0)],
            vec![Value::Integer(0), n.clone(), Value::Integer(0)],
            vec![Value::Integer(0), Value::Integer(0), n.clone()],
            vec![
                Value::Integer(i32::MIN),
                n.clone(),
                Value::Integer(i32::MAX),
            ],
            vec![Value::Integer(0), n.clone(), n.clone()],
            vec![n.clone(), Value::Integer(0), n.clone()],
            vec![n.clone(), n.clone(), Value::Integer(0)],
        ];
        check_integer_set_multiplicities(LogicalType::Integer, base_rows);
    }

    #[test]
    fn compact_bigint_keys_preserve_extremes_dictionary_null_multiplicities() {
        assert_eq!(std::mem::size_of::<NullableIntegerSetKey<i64, 3>>(), 32);
        let n = Value::Null(LogicalType::BigInt);
        let b = Value::BigInt;
        let base_rows = vec![
            vec![n.clone(), b(1), b(-1)],
            vec![b(0), n.clone(), b(2)],
            vec![b(1), b(2), n.clone()],
            vec![b(i64::MIN), b(i64::MAX), b(1 << 40)],
            vec![n.clone(), b(2), b(-1)],
            vec![b(9_007_199_254_740_993), b(1 << 40), b(-(1 << 40))],
            vec![b(i64::MAX), b(i64::MIN), b(-1)],
            vec![n.clone(), n.clone(), n.clone()],
            vec![b(0), b(0), b(0)],
            vec![n.clone(), b(0), b(0)],
            vec![b(0), n.clone(), b(0)],
            vec![b(0), b(0), n.clone()],
            vec![b(i64::MIN), n.clone(), b(i64::MAX)],
            vec![b(0), n.clone(), n.clone()],
            vec![n.clone(), b(0), n.clone()],
            vec![n.clone(), n.clone(), b(0)],
        ];
        check_integer_set_multiplicities(LogicalType::BigInt, base_rows);
    }

    #[test]
    fn compact_bigint_keys_resolve_nullable_sequence_without_narrowing() {
        use paro_common::vector::VectorType;

        let validity = [
            [false, false, false],
            [true, false, false],
            [false, true, false],
            [true, true, false],
            [false, false, true],
            [true, false, true],
            [false, true, true],
            [true, true, true],
        ];
        let starts = [-3, i64::MIN, i64::MAX - 7];
        let increments = [1, 0, 1];
        for width in 1..=3 {
            let types = vec![LogicalType::BigInt; width];
            let expected = validity
                .iter()
                .enumerate()
                .map(|(row, mask)| {
                    (0..width)
                        .map(|column| {
                            if mask[column] {
                                Value::BigInt(starts[column] + row as i64 * increments[column])
                            } else {
                                Value::Null(LogicalType::BigInt)
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let columns = (0..width)
                .map(|column| {
                    let mut vector = Vector::try_sequence(
                        starts[column],
                        increments[column],
                        8,
                        test_allocator(),
                    )
                    .unwrap();
                    for (row, mask) in validity.iter().enumerate() {
                        if !mask[column] {
                            vector.try_set_null(row, true).unwrap();
                        }
                    }
                    assert_eq!(vector.vector_type(), VectorType::Sequence);
                    Arc::new(vector)
                })
                .collect();
            let sequence =
                Chunk::try_from_arc_vectors_with_cardinality(columns, 8, test_allocator()).unwrap();
            assert_eq!(
                sequence
                    .data
                    .iter()
                    .map(|column| column.get_value(7))
                    .collect::<Vec<_>>(),
                expected[7]
            );
            let chunks = [sequence.clone(), value_chunk(&types, &expected)];
            match width {
                1 => assert_nullable_integer_keys_match_scalar_values::<i64, 1>(
                    &chunks,
                    Vector::get_i64,
                ),
                2 => assert_nullable_integer_keys_match_scalar_values::<i64, 2>(
                    &chunks,
                    Vector::get_i64,
                ),
                3 => assert_nullable_integer_keys_match_scalar_values::<i64, 3>(
                    &chunks,
                    Vector::get_i64,
                ),
                _ => unreachable!(),
            }
            for op in [SetOpType::Union, SetOpType::Intersect, SetOpType::Except] {
                for all in [false, true] {
                    let mut spec = spec(op, all);
                    spec.output_types = types.clone().into_boxed_slice();
                    spec.output_names = (0..width)
                        .map(|i| format!("v{i}"))
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    let result = evaluate_set_operation(
                        &spec,
                        vec![sequence.clone(); 3],
                        vec![sequence.clone(); 2],
                        test_allocator(),
                    )
                    .unwrap();
                    let actual = result
                        .iter()
                        .flat_map(|chunk| {
                            (0..chunk.size()).map(|row| {
                                chunk
                                    .data
                                    .iter()
                                    .map(|column| column.get_value(row))
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect::<Vec<_>>();
                    let mut oracle = Vec::new();
                    if op == SetOpType::Union && all {
                        for _ in 0..5 {
                            oracle.extend(expected.clone());
                        }
                    } else {
                        let mut seen = std::collections::HashSet::new();
                        for row in &expected {
                            if seen.insert(row) {
                                let per_side =
                                    expected.iter().filter(|value| *value == row).count();
                                let repeats = match (op, all) {
                                    (SetOpType::Union | SetOpType::Intersect, false) => 1,
                                    (SetOpType::Intersect, true) => 2 * per_side,
                                    (SetOpType::Except, false) => 0,
                                    (SetOpType::Except, true) => per_side,
                                    _ => unreachable!(),
                                };
                                oracle.extend(std::iter::repeat_n(row.clone(), repeats));
                            }
                        }
                    }
                    assert_eq!(actual, oracle, "sequence width={width} {op:?} all={all}");
                }
            }
        }
    }

    fn check_integer_set_multiplicities(ty: LogicalType, base_rows: Vec<Vec<Value>>) {
        let patterns = [
            vec![0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            vec![4, 0, 0, 1, 6, 7, 8, 9, 10, 11, 12, 12, 13, 14, 15],
        ];
        let repetitions = VECTOR_SIZE;
        for width in 1..=3 {
            let types = vec![ty.clone(); width];
            let base_rows = base_rows
                .iter()
                .map(|row| row[..width].to_vec())
                .collect::<Vec<_>>();
            let base = value_chunk(&types, &base_rows);
            let mut selection =
                SelectionVector::try_with_capacity(base_rows.len(), test_allocator()).unwrap();
            selection.set_len(base_rows.len());
            for row in 0..base_rows.len() {
                selection.try_set(row, base_rows.len() - 1 - row).unwrap();
            }
            let dictionary = Chunk::try_from_arc_vectors_with_cardinality(
                base.data
                    .iter()
                    .map(|column| {
                        Arc::new(Vector::try_gather_ref(column.clone(), selection.clone()).unwrap())
                    })
                    .collect(),
                base_rows.len(),
                test_allocator(),
            )
            .unwrap();
            let mut encoded = vec![base.clone(), dictionary];
            for row in 7..=15 {
                encoded.push(
                    Chunk::try_from_arc_vectors_with_cardinality(
                        base.data
                            .iter()
                            .map(|column| {
                                Arc::new(Vector::try_broadcast_ref(column.clone(), row, 2).unwrap())
                            })
                            .collect(),
                        2,
                        test_allocator(),
                    )
                    .unwrap(),
                );
                encoded.push(
                    Chunk::try_from_arc_vectors_with_cardinality(
                        types
                            .iter()
                            .zip(&base_rows[row])
                            .map(|(ty, value)| {
                                Arc::new(
                                    Vector::try_constant_from_value(
                                        ty.clone(),
                                        value.clone(),
                                        2,
                                        test_allocator(),
                                    )
                                    .unwrap(),
                                )
                            })
                            .collect(),
                        2,
                        test_allocator(),
                    )
                    .unwrap(),
                );
            }
            match (&ty, width) {
                (LogicalType::Integer, 1) => assert_nullable_integer_keys_match_scalar_values::<
                    i32,
                    1,
                >(&encoded, Vector::get_i32),
                (LogicalType::Integer, 2) => assert_nullable_integer_keys_match_scalar_values::<
                    i32,
                    2,
                >(&encoded, Vector::get_i32),
                (LogicalType::Integer, 3) => assert_nullable_integer_keys_match_scalar_values::<
                    i32,
                    3,
                >(&encoded, Vector::get_i32),
                (LogicalType::BigInt, 1) => assert_nullable_integer_keys_match_scalar_values::<
                    i64,
                    1,
                >(&encoded, Vector::get_i64),
                (LogicalType::BigInt, 2) => assert_nullable_integer_keys_match_scalar_values::<
                    i64,
                    2,
                >(&encoded, Vector::get_i64),
                (LogicalType::BigInt, 3) => assert_nullable_integer_keys_match_scalar_values::<
                    i64,
                    3,
                >(&encoded, Vector::get_i64),
                _ => unreachable!(),
            }
            let repeated = |pattern: &[usize]| {
                let count = pattern.len() * repetitions;
                let mut selection =
                    SelectionVector::try_with_capacity(count, test_allocator()).unwrap();
                selection.set_len(count);
                for row in 0..count {
                    selection
                        .try_set(row, pattern[row % pattern.len()])
                        .unwrap();
                }
                Chunk::try_from_arc_vectors_with_cardinality(
                    base.data
                        .iter()
                        .map(|column| {
                            Arc::new(
                                Vector::try_gather_ref(column.clone(), selection.clone()).unwrap(),
                            )
                        })
                        .collect(),
                    count,
                    test_allocator(),
                )
                .unwrap()
            };
            // Independent scalar oracle: count each pattern's rows, then scale
            // multiplicity without depending on the compact key representation.
            let mut counts = std::collections::HashMap::<Vec<Value>, [usize; 2]>::new();
            let mut first = Vec::new();
            for (side, pattern) in patterns.iter().enumerate() {
                for &row in pattern {
                    if !counts.contains_key(&base_rows[row]) {
                        first.push(base_rows[row].clone());
                    }
                    counts.entry(base_rows[row].clone()).or_default()[side] += repetitions;
                }
            }
            for op in [SetOpType::Union, SetOpType::Intersect, SetOpType::Except] {
                for all in [false, true] {
                    let mut spec = spec(op, all);
                    spec.output_types = types.clone().into_boxed_slice();
                    spec.output_names = (0..width)
                        .map(|col| format!("v{col}"))
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    let result = evaluate_set_operation(
                        &spec,
                        vec![repeated(&patterns[0])],
                        vec![repeated(&patterns[1])],
                        test_allocator(),
                    )
                    .unwrap();
                    let actual = result
                        .iter()
                        .flat_map(|chunk| {
                            (0..chunk.size()).map(|row| {
                                chunk
                                    .data
                                    .iter()
                                    .map(|column| column.get_value(row))
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect::<Vec<_>>();
                    let mut expected = Vec::new();
                    if op == SetOpType::Union && all {
                        for pattern in &patterns {
                            for _ in 0..repetitions {
                                expected.extend(pattern.iter().map(|&row| base_rows[row].clone()));
                            }
                        }
                    } else {
                        for row in &first {
                            let [l, r] = counts[row];
                            let count = match (op, all) {
                                (SetOpType::Union, false) => 1,
                                (SetOpType::Intersect, false) => usize::from(l > 0 && r > 0),
                                (SetOpType::Intersect, true) => l.min(r),
                                (SetOpType::Except, false) => usize::from(l > 0 && r == 0),
                                (SetOpType::Except, true) => l.saturating_sub(r),
                                _ => unreachable!(),
                            };
                            expected.extend(std::iter::repeat_n(row.clone(), count));
                        }
                    }
                    assert_eq!(actual, expected, "width={width} {op:?} all={all}");
                }
            }
        }
    }
}
