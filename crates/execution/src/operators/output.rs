// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

#[inline]
pub(crate) fn ensure_source_output(
    output: &mut Chunk,
    types: &[LogicalType],
    capacity: usize,
) -> Result<()> {
    ensure_chunk_output(output, types, capacity)
}

#[inline]
pub(crate) fn ensure_transform_output(
    output: &mut Chunk,
    types: &[LogicalType],
    capacity: usize,
) -> Result<()> {
    ensure_chunk_output(output, types, capacity)
}

#[inline]
fn ensure_chunk_output(output: &mut Chunk, types: &[LogicalType], capacity: usize) -> Result<()> {
    if output.column_count() != types.len()
        || output.capacity() < capacity.max(1)
        || output.types() != types
    {
        *output = Chunk::try_initialize(types, capacity.max(1), output.allocator().clone())?;
    } else {
        output.try_reset(output.allocator().clone())?;
    }
    Ok(())
}
