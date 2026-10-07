// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Task-local vector hashing for blocking operators. Merge moves chunk metadata;
//! each finish task gathers only its own radix rows, in original input order.

use std::ops::Range;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VectorOperations, VECTOR_SIZE};
use paro_context::StatementCancellation;

#[derive(Debug, Default)]
pub(crate) struct RadixRouter {
    buffered_rows: usize,
    hashes: Option<Vector>,
    column_hashes: Option<Vector>,
}

#[derive(Debug)]
pub(crate) struct RadixChunk {
    pub input: Chunk,
    pub routing: RadixRows,
}

#[derive(Debug, Default)]
pub(crate) struct RadixRows {
    rows: Option<SelectionVector>,
    offsets: Option<SelectionVector>,
}

impl RadixRows {
    pub fn count(&self, partition: usize, count: usize) -> usize {
        self.offsets.as_ref().map_or_else(
            || if partition == 0 { count } else { 0 },
            |o| o.get(partition + 1) - o.get(partition),
        )
    }
    #[cfg(test)]
    pub fn retained_bytes(&self) -> usize {
        (self.rows.as_ref().map_or(0, |r| r.capacity())
            + self.offsets.as_ref().map_or(0, |o| o.capacity()))
            * size_of::<u32>()
    }

    pub fn rows(&self, partition: usize, count: usize) -> impl Iterator<Item = usize> + '_ {
        let range = if self.rows.is_some() {
            let offsets = self.offsets.as_ref().expect("radix offsets");
            offsets.get(partition)..offsets.get(partition + 1)
        } else if partition == 0 {
            0..count
        } else {
            0..0
        };
        range.map(|i| self.rows.as_ref().map_or(i, |rows| rows.get(i)))
    }

    pub fn partitions(&self) -> usize {
        self.offsets.as_ref().map_or(1, |o| o.len() - 1)
    }
}

/// Coalesce radix bins into at least a vector of useful work, and omit empty
/// bins. The batch-size rule applies to every blocking operator and input size.
pub(crate) fn radix_work(chunks: &[Chunk], routing: &[RadixRows]) -> Vec<Range<usize>> {
    let partitions = routing.iter().map(RadixRows::partitions).max().unwrap_or(1);
    let target = VECTOR_SIZE;
    let mut work = Vec::new();
    let mut start = 0;
    let mut rows = 0;
    for partition in 0..partitions {
        rows += chunks
            .iter()
            .zip(routing)
            .map(|(c, r)| r.count(partition, c.size()))
            .sum::<usize>();
        if rows >= target {
            work.push(start..partition + 1);
            start = partition + 1;
            rows = 0;
        }
    }
    if rows > 0 {
        work.push(start..partitions);
    }
    if work.is_empty() {
        work.push(0..partitions);
    }
    work
}

impl RadixRouter {
    /// Keep small local inputs as references. Promotion hashes the buffered
    /// prefix once; subsequent chunks use the same task-local scratch.
    pub fn push(
        &mut self,
        chunks: &mut Vec<RadixChunk>,
        input: &mut Chunk,
        columns: &[usize],
        partitions: usize,
        allocator: impl FnOnce() -> Arc<dyn Allocator>,
        cancel: &StatementCancellation,
    ) -> Result<()> {
        self.buffered_rows = self.buffered_rows.saturating_add(input.size());
        if partitions <= 1 || columns.is_empty() || self.buffered_rows < partitions * VECTOR_SIZE {
            chunks.push(RadixChunk {
                input: input.handoff_referencing_vectors(),
                routing: RadixRows::default(),
            });
            return Ok(());
        }
        let allocator = allocator();
        if self.hashes.is_none() {
            self.route_pending(chunks, columns, partitions, allocator.clone(), cancel)?;
        }
        chunks.push(self.route(input, columns, partitions, allocator)?);
        Ok(())
    }

    fn route_pending(
        &mut self,
        chunks: &mut [RadixChunk],
        columns: &[usize],
        partitions: usize,
        allocator: Arc<dyn Allocator>,
        cancel: &StatementCancellation,
    ) -> Result<()> {
        for chunk in chunks {
            if chunk.routing.partitions() == 1 {
                cancel.check()?;
                *chunk = self.route(&mut chunk.input, columns, partitions, allocator.clone())?;
            }
        }
        Ok(())
    }

    pub fn route(
        &mut self,
        input: &mut Chunk,
        columns: &[usize],
        partitions: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<RadixChunk> {
        debug_assert!(partitions.is_power_of_two());
        let count = input.size();
        if partitions == 1 || columns.is_empty() {
            return Ok(RadixChunk {
                input: input.handoff_referencing_vectors(),
                routing: RadixRows::default(),
            });
        }
        if self
            .hashes
            .as_ref()
            .is_none_or(|v| v.logical_capacity() < count)
        {
            self.hashes = Some(Vector::try_new(
                LogicalType::UBigInt,
                count,
                allocator.clone(),
            )?);
            self.column_hashes = Some(Vector::try_new(
                LogicalType::UBigInt,
                count,
                allocator.clone(),
            )?);
        }
        let hashes = self.hashes.as_mut().expect("hash scratch initialized");
        let column_hashes = self
            .column_hashes
            .as_mut()
            .expect("hash scratch initialized");
        for (i, &column) in columns.iter().enumerate() {
            if i == 0 {
                VectorOperations::hash(&input.data[column], hashes, count)?;
            } else {
                VectorOperations::hash(&input.data[column], column_hashes, count)?;
                for row in 0..count {
                    hashes.as_mut_slice::<u64>()[row] = paro_common::hash::combine_hash(
                        hashes.as_slice::<u64>()[row],
                        column_hashes.as_slice::<u64>()[row],
                    );
                }
            }
        }
        let mut offsets = vec![0usize; partitions + 1];
        for &hash in &hashes.as_slice::<u64>()[..count] {
            offsets[(hash as usize & (partitions - 1)) + 1] += 1;
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut cursors = offsets.to_vec();
        let mut rows = SelectionVector::try_with_capacity(count, allocator.clone())?;
        rows.set_len(count);
        for (row, &hash) in hashes.as_slice::<u64>()[..count].iter().enumerate() {
            let partition = hash as usize & (partitions - 1);
            rows.try_set(cursors[partition], row)?;
            cursors[partition] += 1;
        }
        let mut boundaries = SelectionVector::try_with_capacity(offsets.len(), allocator)?;
        boundaries.set_len(offsets.len());
        for (i, offset) in offsets.into_iter().enumerate() {
            boundaries.try_set(i, offset)?;
        }
        Ok(RadixChunk {
            input: input.handoff_referencing_vectors(),
            routing: RadixRows {
                rows: Some(rows),
                offsets: Some(boundaries),
            },
        })
    }
}

/// Locals may merge before promotion. Complete their routing only when the
/// combined input justifies parallel work, so equal keys never straddle tasks.
pub(crate) fn finish_routing(
    chunks: &mut [RadixChunk],
    columns: &[usize],
    partitions: usize,
    allocator: impl FnOnce() -> Arc<dyn Allocator>,
    cancel: &StatementCancellation,
) -> Result<()> {
    if partitions <= 1
        || columns.is_empty()
        || chunks.iter().map(|c| c.input.size()).sum::<usize>() < partitions * VECTOR_SIZE
    {
        return Ok(());
    }
    RadixRouter::default().route_pending(chunks, columns, partitions, allocator(), cancel)
}
