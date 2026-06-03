// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash vector staging for hash-join keys.

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::join_hashtable::JoinHashTable;

pub(crate) fn compute_hashes_for_keys_into(
    hash_table: &JoinHashTable,
    keys: &Chunk,
    slot: &mut Option<Vector>,
) -> Result<()> {
    let required_capacity = keys.size().max(1);
    let needs_new = slot.as_ref().map_or(true, |hashes| {
        hashes.logical_type() != &LogicalType::UBigInt || hashes.capacity() < required_capacity
    });
    if needs_new {
        *slot = Some(Vector::try_new(
            LogicalType::UBigInt,
            required_capacity,
            keys.allocator().clone(),
        )?);
    }
    let hashes = slot
        .as_mut()
        .expect("hash join hash vector was initialized above");
    hash_table.compute_key_hashes(keys, hashes)?;
    Ok(())
}
