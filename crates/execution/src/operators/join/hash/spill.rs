// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-join probe spill chunk shaping.

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;

pub(crate) fn build_probe_spill_chunk_into(
    input: &Chunk,
    hashes: &Vector,
    slot: &mut Option<Chunk>,
) -> Result<()> {
    if hashes.len() != input.size() {
        return Err(paro_error::internal(format!(
            "hash join probe hash vector cardinality mismatch: hashes={} input={}",
            hashes.len(),
            input.size()
        )));
    }
    if slot.is_none() {
        *slot = Some(Chunk::try_new(input.allocator().clone())?);
    }
    let chunk = slot
        .as_mut()
        .expect("hash join probe spill chunk was initialized above");
    chunk.data.clear();
    chunk.data.reserve(input.column_count().saturating_add(1));
    for col_idx in 0..input.column_count() {
        chunk
            .data
            .push(Arc::clone(input.column(col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "missing input column while building hash join probe spill chunk: {col_idx}"
                ))
            })?));
    }
    chunk.data.push(Arc::new(hashes.clone()));
    chunk.set_capacity(input.size().max(1));
    chunk.try_set_cardinality(input.size())?;
    Ok(())
}

pub(crate) fn probe_input_from_spill_chunk_into(
    spill_chunk: &Chunk,
    probe_column_count: usize,
    slot: &mut Option<Chunk>,
) -> Result<()> {
    if spill_chunk.column_count() < probe_column_count {
        return Err(paro_error::internal(format!(
            "hash join probe spill chunk has {} columns but replay expects {probe_column_count}",
            spill_chunk.column_count()
        )));
    }
    if slot.is_none() {
        *slot = Some(Chunk::try_new(spill_chunk.allocator().clone())?);
    }
    let input = slot
        .as_mut()
        .expect("hash join replay input chunk was initialized above");
    input.data.clear();
    input.data.reserve(probe_column_count);
    input
        .data
        .extend(spill_chunk.data[..probe_column_count].iter().cloned());
    input.set_capacity(spill_chunk.size().max(1));
    input.try_set_cardinality(spill_chunk.size())?;
    Ok(())
}
