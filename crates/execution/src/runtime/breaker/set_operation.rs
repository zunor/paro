// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime handle for SQL set operations.
//!
//! Producers append task-local chunks during merge. The shared sink's single
//! finish pass seals the result into immutable chunks, so emit reads are
//! lock-free and deterministic for the lifetime of the source cursor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::VECTOR_SIZE;
use paro_planner::operator::SetOpType;

use crate::physical::specs::{SetOperationInputSide, SetOperationSpec};
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct SetOperationHandle {
    metadata: BreakerHandleMetadata,
    left_chunks: Mutex<Vec<Chunk>>,
    right_chunks: Mutex<Vec<Chunk>>,
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

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn append_chunks(
        &self,
        side: SetOperationInputSide,
        chunks: &mut Vec<Chunk>,
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

    pub fn seal(&self, spec: &SetOperationSpec, allocator: Arc<dyn Allocator>) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let left = {
            let mut chunks = self.left_chunks.lock();
            std::mem::take(&mut *chunks)
        };
        let right = {
            let mut chunks = self.right_chunks.lock();
            std::mem::take(&mut *chunks)
        };
        let chunks = evaluate_set_operation(spec, left, right, allocator)?;
        self.sealed_chunks
            .set(chunks)
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

#[derive(Debug, Default)]
struct SetCounts {
    left_count: usize,
    right_count: usize,
}

fn evaluate_set_operation(
    spec: &SetOperationSpec,
    mut left: Vec<Chunk>,
    right: Vec<Chunk>,
    allocator: Arc<dyn Allocator>,
) -> Result<Arc<[Chunk]>> {
    if spec.op == SetOpType::Union && spec.all {
        left.extend(right);
        return Ok(Arc::from(left.into_boxed_slice()));
    }

    let mut index = HashMap::<Box<[Value]>, usize>::new();
    let mut counts = Vec::<SetCounts>::new();
    collect_side(&left, true, &mut index, &mut counts);
    collect_side(&right, false, &mut index, &mut counts);

    // Consume the HashMap and sort by insertion index to preserve first-seen order.
    let mut keyed: Vec<_> = index.into_iter().collect();
    keyed.sort_unstable_by_key(|(_, idx)| *idx);

    let rows: Vec<_> = keyed
        .iter()
        .filter_map(|(key, idx)| {
            let c = &counts[*idx];
            let repeats = output_repeats(spec, c.left_count, c.right_count);
            (repeats > 0).then_some((key.as_ref(), repeats))
        })
        .collect();
    rows_to_chunks(&rows, &spec.output_types, allocator)
}

fn collect_side(
    chunks: &[Chunk],
    left: bool,
    index: &mut HashMap<Box<[Value]>, usize>,
    counts: &mut Vec<SetCounts>,
) {
    use std::collections::hash_map::Entry;
    for chunk in chunks {
        for row_idx in 0..chunk.size() {
            let key = row_key(chunk, row_idx);
            let entry_idx = match index.entry(key) {
                Entry::Occupied(e) => *e.get(),
                Entry::Vacant(e) => {
                    let idx = counts.len();
                    counts.push(SetCounts::default());
                    e.insert(idx);
                    idx
                }
            };
            let c = &mut counts[entry_idx];
            if left {
                c.left_count += 1;
            } else {
                c.right_count += 1;
            }
        }
    }
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
    rows: &[(&[Value], usize)],
    output_types: &[paro_common::types::LogicalType],
    allocator: Arc<dyn Allocator>,
) -> Result<Arc<[Chunk]>> {
    let total_rows = rows.iter().map(|(_, repeats)| repeats).sum::<usize>();
    if total_rows == 0 {
        return Ok(Arc::from(Vec::<Chunk>::new().into_boxed_slice()));
    }

    let chunk_count = total_rows.div_ceil(VECTOR_SIZE);
    let mut chunks = Vec::with_capacity(chunk_count);
    let first_cap = total_rows.min(VECTOR_SIZE).max(1);
    let mut chunk = Chunk::try_initialize(output_types, first_cap, allocator.clone())?;
    chunk.try_set_cardinality(first_cap)?;
    let mut row_in_chunk = 0usize;
    let mut emitted = 0usize;

    for (row, repeats) in rows {
        for _ in 0..*repeats {
            if row_in_chunk == chunk.capacity() {
                chunk.try_set_cardinality(row_in_chunk)?;
                chunks.push(chunk);
                let remaining = total_rows - emitted;
                let cap = remaining.min(VECTOR_SIZE).max(1);
                chunk = Chunk::try_initialize(output_types, cap, allocator.clone())?;
                chunk.try_set_cardinality(cap)?;
                row_in_chunk = 0;
            }
            for (col_idx, value) in row.iter().enumerate() {
                chunk
                    .set_value(col_idx, row_in_chunk, value)
                    .ok_or_else(|| paro_error::internal("failed to write set-operation row"))?;
            }
            row_in_chunk += 1;
            emitted += 1;
        }
    }

    if row_in_chunk > 0 {
        chunk.try_set_cardinality(row_in_chunk)?;
        chunks.push(chunk);
    }
    Ok(Arc::from(chunks.into_boxed_slice()))
}

fn row_key(chunk: &Chunk, row: usize) -> Box<[Value]> {
    chunk
        .data
        .iter()
        .map(|column| column.get_value(row))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[cfg(test)]
mod tests {
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
}
