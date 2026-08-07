// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Serialized group-key projection between aggregate hash tables.
//!
//! A DISTINCT key table stores `(group keys..., aggregate inputs...)`. The
//! regular aggregate table only needs the group-key prefix. These helpers hash,
//! probe, compare, and copy that prefix in row form so finalization only
//! materializes the aggregate inputs that its update function consumes.

use super::*;

#[derive(Clone, Copy)]
pub(crate) struct SerializedSourceRows<'a> {
    start: usize,
    rows: &'a [u32],
}

impl<'a> SerializedSourceRows<'a> {
    pub(crate) fn new(start: usize, rows: &'a [u32]) -> Self {
        Self { start, rows }
    }

    pub(crate) fn len(self) -> usize {
        self.rows.len()
    }

    pub(crate) fn start(self) -> usize {
        self.start
    }

    pub(crate) fn relative_row(self, row_idx: usize) -> Result<usize> {
        self.rows
            .get(row_idx)
            .copied()
            .map(|row| row as usize)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Serialized source selection index out of bounds: row={row_idx}, count={}",
                    self.rows.len()
                ))
            })
    }

    pub(crate) fn source_row(self, row_idx: usize) -> Result<usize> {
        let start = self.start();
        let relative = self.relative_row(row_idx)?;
        start.checked_add(relative).ok_or_else(|| {
            paro_error::internal(format!(
                "Serialized source row offset overflow: start={start}, relative={relative}"
            ))
        })
    }
}

impl InlineKeyLayout {
    unsafe fn encode_serialized_prefix(
        &self,
        source_layout: &TupleLayout,
        source_row: *const u8,
    ) -> Result<InlineKey> {
        if source_layout.group_count() < self.group_types.len()
            || source_layout.group_types[..self.group_types.len()] != self.group_types
        {
            return Err(paro_error::internal(format!(
                "Inline serialized prefix mismatch: target={:?}, source={:?}",
                self.group_types, source_layout.group_types
            )));
        }

        let mut key_bytes = [0u8; INLINE_KEY_MAX_BYTES];
        let mut null_mask = 0u64;
        for group_idx in 0..self.group_types.len() {
            if !unsafe { source_layout.serialized_group_is_valid(source_row, group_idx) } {
                null_mask |= 1u64 << group_idx;
                continue;
            }
            let source = unsafe { source_row.add(source_layout.group_offsets[group_idx]) };
            write_serialized_inline_component(
                &mut key_bytes,
                self.byte_offsets[group_idx],
                source,
                &self.group_types[group_idx],
            )?;
        }
        Ok(InlineKey {
            bits: u64::from_le_bytes(key_bytes),
            null_mask,
        })
    }
}

impl GroupedAggregateHashTable {
    /// Project maximal adjacent runs that share the same serialized group prefix.
    ///
    /// `run_starts` contains offsets relative to `start`; `hashes` contains one
    /// group-prefix hash per run. Rows are never reordered, so callers may
    /// expand one lookup result across each run without changing update order.
    pub(crate) fn project_serialized_group_prefix_runs(
        &self,
        start: usize,
        count: usize,
        prefix_count: usize,
        run_starts: &mut SelectionVector,
        hashes: &mut Vector,
    ) -> Result<usize> {
        self.validate_serialized_range(start, count)?;
        if prefix_count > self.layout.group_count() {
            return Err(paro_error::internal(format!(
                "Serialized group run prefix exceeds layout: prefix={prefix_count}, groups={}",
                self.layout.group_count()
            )));
        }
        if run_starts.capacity() < count {
            return Err(paro_error::internal(format!(
                "Serialized group run output too small: rows={count}, capacity={}",
                run_starts.capacity()
            )));
        }
        if hashes.logical_type() != &LogicalType::UBigInt || hashes.capacity() < count {
            return Err(paro_error::internal(format!(
                "Serialized group run hash output mismatch: type={:?}, capacity={}, rows={count}",
                hashes.logical_type(),
                hashes.capacity()
            )));
        }
        if count == 0 {
            run_starts.set_len(0);
            hashes.try_set_count(0)?;
            return Ok(0);
        }

        run_starts.set_len(count);
        hashes.try_set_count(count)?;
        let starts = run_starts.as_mut_slice();
        let output = unsafe { hashes.flat_data_mut::<u64>() };
        starts[0] = 0;
        let mut previous = self.row_ptr(start);
        let mut previous_hash = unsafe {
            self.layout
                .hash_serialized_group_prefix(previous, prefix_count, &self.varlen_heap)?
        };
        unsafe { *output = previous_hash };
        let mut run_count = 1usize;
        for relative_row in 1..count {
            let current = self.row_ptr(start + relative_row);
            let same_prefix = unsafe {
                self.layout.compare_serialized_group_prefixes(
                    previous,
                    current,
                    prefix_count,
                    &self.varlen_heap,
                )?
            };
            if !same_prefix {
                starts[run_count] = u32::try_from(relative_row).map_err(|_| {
                    paro_error::internal(format!(
                        "Serialized group run offset exceeds u32: offset={relative_row}"
                    ))
                })?;
                previous_hash = unsafe {
                    self.layout.hash_serialized_group_prefix(
                        current,
                        prefix_count,
                        &self.varlen_heap,
                    )?
                };
                unsafe { *output.add(run_count) = previous_hash };
                run_count += 1;
            }
            previous = current;
        }
        run_starts.set_len(run_count);
        hashes.try_set_count(run_count)?;
        Ok(run_count)
    }

    pub(crate) fn gather_serialized_group_columns(
        &self,
        start: usize,
        count: usize,
        source_columns: &[usize],
        result: &mut Chunk,
    ) -> Result<()> {
        self.validate_serialized_range(start, count)?;
        if source_columns.len() != result.column_count() {
            return Err(paro_error::internal(format!(
                "Serialized group gather width mismatch: columns={}, output={}",
                source_columns.len(),
                result.column_count()
            )));
        }
        if result.capacity() < count {
            return Err(paro_error::internal(format!(
                "Serialized group gather output too small: rows={count}, capacity={}",
                result.capacity()
            )));
        }
        result.try_set_cardinality(count)?;
        let row_base = self.row_ptr(start);
        for (output_idx, &source_idx) in source_columns.iter().enumerate() {
            let output = result.column_mut(output_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Serialized group gather output column missing: index={output_idx}"
                ))
            })?;
            unsafe {
                self.layout.gather_group_column(
                    row_base,
                    self.layout.row_width,
                    count,
                    source_idx,
                    &self.varlen_heap,
                    output,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn find_or_create_serialized_group_prefix(
        &mut self,
        source: &GroupedAggregateHashTable,
        source_rows: SerializedSourceRows<'_>,
        hashes: &[u64],
        addresses: &mut Vector,
    ) -> Result<()> {
        self.layout.validate_serialized_prefix(&source.layout)?;
        let row_count = source_rows.len();
        if hashes.len() != row_count {
            return Err(paro_error::internal(format!(
                "Serialized group hash count mismatch: hashes={}, rows={row_count}",
                hashes.len()
            )));
        }
        validate_addresses_vector(addresses, row_count)?;
        if row_count == 0 {
            addresses.try_set_count(0)?;
            return Ok(());
        }
        self.ensure_lookup_storage_available()?;
        self.ensure_capacity_for(row_count)?;
        self.ensure_row_storage_capacity(row_count)?;

        addresses.try_set_count(row_count)?;
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        let inline_layout = self.inline_key_layout.clone();
        let inline_key_data = inline_layout
            .as_ref()
            .map(|_| self.inline_key_storage_mut_ptr())
            .transpose()?;
        let mut new_state_ptrs = Vec::new();
        for (row_idx, &hash) in hashes.iter().enumerate() {
            let source_row_idx = source_rows.source_row(row_idx)?;
            if source_row_idx >= source.count {
                return Err(paro_error::internal(format!(
                    "Serialized source row out of bounds: row={source_row_idx}, count={}",
                    source.count
                )));
            }
            let source_row = source.row_ptr(source_row_idx);
            let inline_key = inline_layout
                .as_ref()
                .map(|layout| unsafe {
                    layout.encode_serialized_prefix(&source.layout, source_row)
                })
                .transpose()?;
            let mut slot = self.slot_for_hash(hash);
            loop {
                let entry = self.entries[slot];
                if !entry.is_occupied() {
                    let new_row_idx =
                        self.append_serialized_group_prefix_row(source, source_row_idx, hash)?;
                    self.entries[slot] = AggregateHTEntry::from_hash_and_row(hash, new_row_idx)?;
                    if let (Some(inline_key), Some(inline_key_data)) = (inline_key, inline_key_data)
                    {
                        // SAFETY: both lookup arrays were reserved together and
                        // are not resized while this batch is being inserted.
                        unsafe {
                            *inline_key_data.add(slot) = inline_key;
                        }
                    }
                    self.count += 1;
                    let state_ptr = self.state_ptr(new_row_idx);
                    unsafe {
                        *address_data.add(row_idx) = state_ptr;
                    }
                    new_state_ptrs.push(state_ptr);
                    break;
                }

                let keys_match = if let Some(inline_key) = inline_key {
                    let inline_key_data = inline_key_data.ok_or_else(|| {
                        paro_error::internal("Aggregate inline-key sidecar disappeared")
                    })?;
                    // SAFETY: `slot` is within both equally-sized lookup arrays.
                    entry.matches_hash(hash) && unsafe { *inline_key_data.add(slot) } == inline_key
                } else {
                    entry.matches_hash(hash)
                        && unsafe {
                            self.layout.compare_serialized_group_prefix(
                                self.row_ptr(entry.row_idx()),
                                &self.varlen_heap,
                                &source.layout,
                                source_row,
                                &source.varlen_heap,
                            )?
                        }
                };
                if keys_match {
                    unsafe {
                        *address_data.add(row_idx) = self.state_ptr(entry.row_idx());
                    }
                    break;
                }
                slot = (slot + 1) & self.bitmask;
            }
        }

        if !new_state_ptrs.is_empty() {
            let new_addresses = pointer_vector_from_slice(&new_state_ptrs, self.allocator())?;
            initialize_states(
                &self.state_layout,
                &self.aggregate_objects,
                &new_addresses,
                new_state_ptrs.len(),
            )?;
        }
        Ok(())
    }

    fn append_serialized_group_prefix_row(
        &mut self,
        source: &GroupedAggregateHashTable,
        source_row_idx: usize,
        hash: u64,
    ) -> Result<usize> {
        let target_row_idx = self.count;
        let row_words = self.row_width_words();
        let old_len = self.data.len();
        let new_len = old_len.checked_add(row_words).ok_or_else(|| {
            paro_error::internal(format!(
                "Projected aggregate row storage overflow: old_len={old_len}, row_width={}",
                self.layout.row_width
            ))
        })?;
        self.data.try_resize_with(new_len, || 0)?;
        let target_row =
            unsafe { (self.data.as_mut_ptr() as *mut u8).add(old_len * size_of::<u64>()) };
        let source_row = source.row_ptr(source_row_idx);
        if let Err(error) = unsafe {
            self.layout.copy_serialized_group_prefix(
                &source.layout,
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

    fn validate_serialized_range(&self, start: usize, count: usize) -> Result<()> {
        let end = start.checked_add(count).ok_or_else(|| {
            paro_error::internal(format!(
                "Serialized group range overflow: start={start}, count={count}"
            ))
        })?;
        if end > self.count {
            return Err(paro_error::internal(format!(
                "Serialized group range out of bounds: start={start}, count={count}, rows={}",
                self.count
            )));
        }
        Ok(())
    }
}
