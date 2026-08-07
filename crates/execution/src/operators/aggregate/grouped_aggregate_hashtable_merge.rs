// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Direct tuple-row merge for grouped aggregate hash tables.

use super::*;

impl GroupedAggregateHashTable {
    /// Combine another table without round-tripping serialized group keys
    /// through column vectors. Stored hashes are reused, fixed-width keys stay
    /// in row form, and only out-of-line varlen bytes move between heaps.
    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.ensure_compatible(other)?;
        if other.count == 0 {
            return Ok(());
        }

        // Combining can insert at most every source row. Reserve that upper bound once so
        // lookup and row storage remain stable throughout the merge. Besides avoiding
        // geometric reallocation, this keeps aggregate-state pointers produced for a batch
        // valid until the batch's combine callbacks have consumed them.
        self.reserve_for_insertions(other.count)?;
        let has_aggregates = !self.aggregate_objects.is_empty();
        let address_capacity = other.count.min(VECTOR_SIZE);
        let mut source_addresses = has_aggregates
            .then(|| Vector::try_new(LogicalType::BigInt, address_capacity, self.allocator()))
            .transpose()?;
        let mut target_addresses = has_aggregates
            .then(|| Vector::try_new(LogicalType::BigInt, address_capacity, self.allocator()))
            .transpose()?;
        let inline_layout = self.inline_key_layout.clone();
        let inline_key_data = inline_layout
            .as_ref()
            .map(|_| self.inline_key_storage_mut_ptr())
            .transpose()?;

        let mut row_offset = 0usize;
        while row_offset < other.count {
            let batch_size = (other.count - row_offset).min(VECTOR_SIZE);

            let (source_address_data, target_address_data) =
                match (&mut source_addresses, &mut target_addresses) {
                    (Some(source), Some(target)) => {
                        source.try_set_count(batch_size)?;
                        target.try_set_count(batch_size)?;
                        (
                            Some(unsafe { source.flat_data_mut::<*mut u8>() }),
                            Some(unsafe { target.flat_data_mut::<*mut u8>() }),
                        )
                    }
                    (None, None) => (None, None),
                    _ => {
                        return Err(paro_error::internal(
                            "aggregate merge address vectors were initialized inconsistently",
                        ));
                    }
                };
            let mut new_state_ptrs = Vec::with_capacity(batch_size);

            for batch_idx in 0..batch_size {
                let source_row_idx = row_offset + batch_idx;
                let source_row = other.row_ptr(source_row_idx);
                let hash = other.layout.load_hash(source_row);
                let inline_key = inline_layout
                    .as_ref()
                    .map(|layout| unsafe {
                        layout.encode_serialized_row(&other.layout, source_row)
                    })
                    .transpose()?;
                if let Some(addresses) = source_address_data {
                    unsafe {
                        *addresses.add(batch_idx) = other.state_ptr(source_row_idx);
                    }
                }

                let mut slot = self.slot_for_hash(hash);
                loop {
                    let entry = self.entries[slot];
                    if !entry.is_occupied() {
                        let target_row_idx =
                            self.append_serialized_group_row(other, source_row_idx, hash)?;
                        self.entries[slot] =
                            AggregateHTEntry::from_hash_and_row(hash, target_row_idx)?;
                        if let (Some(inline_key), Some(inline_key_data)) =
                            (inline_key, inline_key_data)
                        {
                            // SAFETY: both lookup arrays were reserved together
                            // before merging and remain stable for the whole merge.
                            unsafe {
                                *inline_key_data.add(slot) = inline_key;
                            }
                        }
                        self.count += 1;
                        let target_state = self.state_ptr(target_row_idx);
                        if let Some(addresses) = target_address_data {
                            unsafe {
                                *addresses.add(batch_idx) = target_state;
                            }
                            new_state_ptrs.push(target_state);
                        }
                        break;
                    }

                    let keys_match = if let Some(inline_key) = inline_key {
                        let inline_key_data = inline_key_data.ok_or_else(|| {
                            paro_error::internal("Aggregate inline-key sidecar disappeared")
                        })?;
                        // SAFETY: `slot` is within both equally-sized lookup arrays.
                        entry.matches_hash(hash)
                            && unsafe { *inline_key_data.add(slot) } == inline_key
                    } else {
                        entry.matches_hash(hash)
                            && unsafe {
                                self.layout.compare_serialized_groups(
                                    self.row_ptr(entry.row_idx()),
                                    &self.varlen_heap,
                                    source_row,
                                    &other.varlen_heap,
                                )?
                            }
                    };
                    if keys_match {
                        if let Some(addresses) = target_address_data {
                            unsafe {
                                *addresses.add(batch_idx) = self.state_ptr(entry.row_idx());
                            }
                        }
                        break;
                    }
                    slot = (slot + 1) & self.bitmask;
                }
            }

            if has_aggregates {
                if !new_state_ptrs.is_empty() {
                    let new_addresses =
                        pointer_vector_from_slice(&new_state_ptrs, self.allocator())?;
                    initialize_states(
                        &self.state_layout,
                        &self.aggregate_objects,
                        &new_addresses,
                        new_state_ptrs.len(),
                    )?;
                }
                let mut input_data = AggregateInputData::new(
                    None,
                    &mut self.aggregate_allocator,
                    AggregateCombineType::AllowDestructive,
                );
                combine_states(
                    &self.aggregate_objects,
                    &mut input_data,
                    source_addresses.as_ref().ok_or_else(|| {
                        paro_error::internal("aggregate merge source addresses are missing")
                    })?,
                    target_addresses.as_ref().ok_or_else(|| {
                        paro_error::internal("aggregate merge target addresses are missing")
                    })?,
                    batch_size,
                )?;
            }
            row_offset += batch_size;
        }
        Ok(())
    }

    fn append_serialized_group_row(
        &mut self,
        source: &Self,
        source_row_idx: usize,
        hash: u64,
    ) -> Result<usize> {
        let target_row_idx = self.count;
        let row_words = self.row_width_words();
        let old_len = self.data.len();
        let new_len = old_len.checked_add(row_words).ok_or_else(|| {
            paro_error::internal(format!(
                "Hash table row storage overflow: old_len={old_len}, row_width={}",
                self.layout.row_width
            ))
        })?;
        self.data.try_resize_with(new_len, || 0)?;

        let target_row =
            unsafe { (self.data.as_mut_ptr() as *mut u8).add(old_len * size_of::<u64>()) };
        let source_row = source.row_ptr(source_row_idx);
        if let Err(error) = unsafe {
            self.layout.copy_serialized_groups(
                source_row,
                &source.varlen_heap,
                target_row,
                &mut self.varlen_heap,
            )
        } {
            self.data.truncate(old_len);
            return Err(error);
        }
        self.layout.store_hash(target_row, hash);
        Ok(target_row_idx)
    }
}
