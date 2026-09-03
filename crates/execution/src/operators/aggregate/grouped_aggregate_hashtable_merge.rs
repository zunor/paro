// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Direct tuple-row merge for grouped aggregate hash tables.

use super::*;

impl GroupedAggregateHashTable {
    /// Combine another table without round-tripping serialized group keys
    /// through column vectors. Stored hashes are reused, fixed-width keys stay
    /// in row form, and only out-of-line varlen bytes move between heaps.
    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.combine_many(std::slice::from_mut(other))
    }

    /// Combine several completed tables as one ownership transfer.
    ///
    /// The upper bound is reserved once and all temporary address vectors are
    /// reused across sources. This matters for parallel aggregate finalization:
    /// a partition normally receives one fragment from every build worker, and
    /// reserving fragment-by-fragment would repeatedly rehash the same target.
    pub(crate) fn combine_many(&mut self, others: &mut [Self]) -> Result<()> {
        let mut incoming_rows = 0usize;
        let mut largest_source = 0usize;
        for other in others.iter() {
            self.ensure_compatible(other)?;
            incoming_rows = incoming_rows
                .checked_add(other.count)
                .ok_or_else(|| paro_error::internal("aggregate merge source row count overflow"))?;
            largest_source = largest_source.max(other.count);
        }
        if incoming_rows == 0 {
            for other in others.iter_mut() {
                self.hash_runtime_stats
                    .merge(other.take_hash_runtime_stats());
            }
            return Ok(());
        }

        // Combining can insert at most every source row. Besides avoiding
        // geometric reallocation, this keeps aggregate-state pointers stable
        // until the batch's combine callbacks have consumed them.
        self.reserve_for_insertions(incoming_rows)?;
        let has_aggregates = !self.aggregate_objects.is_empty();
        let direct_program = self
            .direct_update_program
            .clone()
            .filter(DirectGroupedAggregateProgram::supports_trivial_state_copy);
        let uses_generic_combine = has_aggregates && direct_program.is_none();
        let address_capacity = largest_source.min(VECTOR_SIZE);
        let mut source_addresses = uses_generic_combine
            .then(|| Vector::try_new(LogicalType::BigInt, address_capacity, self.allocator()))
            .transpose()?;
        let mut target_addresses = uses_generic_combine
            .then(|| Vector::try_new(LogicalType::BigInt, address_capacity, self.allocator()))
            .transpose()?;
        let mut new_addresses = uses_generic_combine
            .then(|| Vector::try_new(LogicalType::BigInt, address_capacity, self.allocator()))
            .transpose()?;
        let inline_layout = self.inline_key_layout.clone();

        for other in others.iter_mut() {
            let mut row_offset = 0usize;
            while row_offset < other.count {
                let batch_size = (other.count - row_offset).min(VECTOR_SIZE);
                let observe_prefix_probes = self.hash_contract.lookup_is_prefix();
                let mut max_prefix_probe_distance = 0usize;

                let (source_address_data, target_address_data, new_address_data) = match (
                    &mut source_addresses,
                    &mut target_addresses,
                    &mut new_addresses,
                ) {
                    (Some(source), Some(target), Some(new)) => {
                        source.try_set_count(batch_size)?;
                        target.try_set_count(batch_size)?;
                        new.try_set_count(batch_size)?;
                        (
                            Some(unsafe { source.flat_data_mut::<*mut u8>() }),
                            Some(unsafe { target.flat_data_mut::<*mut u8>() }),
                            Some(unsafe { new.flat_data_mut::<*mut u8>() }),
                        )
                    }
                    (None, None, None) => (None, None, None),
                    _ => {
                        return Err(paro_error::internal(
                            "aggregate merge address vectors were initialized inconsistently",
                        ));
                    }
                };
                let mut new_state_count = 0usize;
                // This raw sidecar pointer is deliberately scoped to one data
                // batch. A strategy transition may rebuild the lookup index
                // after the scope, before the next batch reacquires it.
                let inline_key_data = inline_layout
                    .as_ref()
                    .map(|_| self.inline_key_storage_mut_ptr())
                    .transpose()?;
                for batch_idx in 0..batch_size {
                    let source_row_idx = row_offset + batch_idx;
                    let source_row = other.row_ptr(source_row_idx);
                    let source_state = other.state_ptr(source_row_idx);
                    // Runtime fallback is local to each table. Derive the
                    // source row's hash under the target's active contract
                    // instead of assuming independently built workers made
                    // the same adaptive decision.
                    let hash = other.serialized_hash_for_lookup_contract(
                        source_row_idx,
                        self.lookup_hash_contract(),
                    )?;
                    let inline_key = inline_layout
                        .as_ref()
                        .map(|layout| unsafe {
                            layout.encode_serialized_row(&other.layout, source_row)
                        })
                        .transpose()?;
                    if let Some(addresses) = source_address_data {
                        unsafe {
                            *addresses.add(batch_idx) = source_state;
                        }
                    }

                    let mut slot = self.slot_for_hash(hash);
                    let mut probe_distance = 0usize;
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
                            if direct_program.is_some() {
                                // The admitted direct program proves every
                                // state field is self-contained and trivially
                                // copyable. A new group can therefore inherit
                                // the complete source state without initialize
                                // plus vectorized combine round-trips.
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        source_state,
                                        target_state,
                                        self.state_layout.total_size(),
                                    );
                                }
                            } else if let (Some(addresses), Some(new_addresses)) =
                                (target_address_data, new_address_data)
                            {
                                unsafe {
                                    *addresses.add(batch_idx) = target_state;
                                    *new_addresses.add(new_state_count) = target_state;
                                }
                                new_state_count += 1;
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
                            let target_state = self.state_ptr(entry.row_idx());
                            if let Some(program) = direct_program.as_ref() {
                                // SAFETY: source and target belong to distinct
                                // compatible tables and the program was
                                // compiled for this exact state layout.
                                let combined = unsafe {
                                    program.combine_direct_rows(source_state, target_state)
                                };
                                debug_assert!(combined);
                            } else if let Some(addresses) = target_address_data {
                                unsafe {
                                    *addresses.add(batch_idx) = target_state;
                                }
                            }
                            break;
                        }
                        probe_distance += 1;
                        if observe_prefix_probes {
                            max_prefix_probe_distance =
                                max_prefix_probe_distance.max(probe_distance);
                        }
                        slot = (slot + 1) & self.bitmask;
                    }
                }

                if uses_generic_combine {
                    if new_state_count > 0 {
                        let new_addresses = new_addresses.as_mut().ok_or_else(|| {
                            paro_error::internal("aggregate merge new-state addresses are missing")
                        })?;
                        new_addresses.try_set_count(new_state_count)?;
                        initialize_states(
                            &self.state_layout,
                            &self.aggregate_objects,
                            new_addresses,
                            new_state_count,
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
                self.finish_prefix_probe_batch(max_prefix_probe_distance)?;
                row_offset += batch_size;
            }
        }
        // A completed source owns both its tuples and the observations made
        // while constructing them. Transfer the latter with the former so a
        // worker-local fallback is still visible when only the final target
        // is drained into EXPLAIN ANALYZE.
        for other in others.iter_mut() {
            self.hash_runtime_stats
                .merge(other.take_hash_runtime_stats());
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
