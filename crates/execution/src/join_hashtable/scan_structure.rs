// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Iteration state for hash join probe results (pointer chains, outer/semi/mark paths).
//!
//! Tracks pointers into the hash table, collision chains, and match flags for outer joins.

use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_common::vector::{SelectionVector, VECTOR_SIZE};
use std::sync::Arc;

use crate::join_hashtable::join_hashtable::JoinHashTable;
use crate::operator::join::join_result_helpers::{
    construct_anti_join_result, construct_left_outer_result, construct_mark_join_result,
    construct_semi_join_result,
};

/// Scan structure for probing the hash table and iterating over results.
///
/// This structure maintains state between calls to `Next()` when a single
/// probe may return multiple batches of results.
#[derive(Debug)]
pub struct ScanStructure {
    /// Pointers to build-side rows for matching entries.
    pub pointers: Vec<usize>,
    /// Incremental selection vector for the pointers.
    pub sel_vector: SelectionVector,
    /// Selection vector for matches that passed equality checks.
    pub chain_match_sel: SelectionVector,
    /// Selection vector for matches that failed equality checks (and need chain advancement).
    pub chain_no_match_sel: SelectionVector,
    /// Number of active pointers (matches found and needs verifying).
    pub count: usize,
    /// Whether each probe-side row has found at least one match.
    pub found_match: Vec<bool>,
    /// Selection vector for probe-side indices of current matches.
    pub lhs_sel: SelectionVector,
    /// Pointers to build-side rows that passed all predicates.
    pub rhs_pointers: Vec<usize>,
    /// Number of matches to be returned in current batch.
    pub match_count: usize,
    /// Whether the scan is finished.
    pub finished: bool,
    /// Whether this belongs to a null probe.
    pub is_null: bool,
    /// Whether any null keys were filtered.
    pub has_null_value_filter: bool,
    /// Offset to next pointer in build row.
    pub pointer_offset: usize,
    /// Whether chains longer than one exist in HT.
    pub has_long_chains: bool,
}

impl ScanStructure {
    fn next_inner_join_impl<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
        mut residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            return Ok(0);
        }

        let mut base_count = 0;

        while self.count > 0 && base_count < paro_common::vector::VECTOR_SIZE {
            // Safety check: if adding matches from current chains could overflow the batch,
            // and we already have some results, break and let the next call handle these chains.
            // This is safe because res_count <= self.count.
            if base_count > 0 && base_count + self.count > paro_common::vector::VECTOR_SIZE {
                break;
            }

            // Resolve predicates for current candidates
            let res_count = self.resolve_predicates(keys, hash_table, base_count);

            if res_count > 0 {
                let rhs_offset = base_count;
                let mut residual_sel = SelectionVector::incremental(res_count);
                let accepted_count = residual_filter(
                    &self.chain_match_sel,
                    &self.rhs_pointers[rhs_offset..rhs_offset + res_count],
                    res_count,
                    &mut residual_sel,
                )?;

                for i in 0..accepted_count {
                    let match_idx = residual_sel.get(i);
                    self.chain_match_sel
                        .set(i, self.chain_match_sel.get(match_idx));
                    self.rhs_pointers[rhs_offset + i] = self.rhs_pointers[rhs_offset + match_idx];
                }

                // Found some matches
                let count_to_copy = std::cmp::min(
                    accepted_count,
                    paro_common::vector::VECTOR_SIZE - base_count,
                );

                // Copy LHS indices
                for i in 0..count_to_copy {
                    self.lhs_sel
                        .set(base_count + i, self.chain_match_sel.get(i));
                    if hash_table.has_found_flag() {
                        hash_table.set_build_side_found(self.rhs_pointers[base_count + i], true);
                    }
                }

                // Copy RHS pointers (we need to store these for gathering)
                // rhs_pointers already contains row_ptr for these matches from resolve_predicates

                base_count += count_to_copy;

                if count_to_copy < res_count {
                    // We don't have enough space in the output chunk
                    // We need to keep the remaining matches for next call
                    // For now, let's assume we fit or we'll need a way to buffer
                    // state.lhs_output.Reference(left);
                    // state.scan_structure.Next(state.lhs_join_keys, state.lhs_output, chunk);
                }
            }

            // Always advance pointers and try to find more matches for the same input chunk
            // if we haven't filled our output batch yet.
            // If we have long chains, we might find more matches for the same rows.
            self.advance_pointers();
        }

        if base_count > 0 {
            self.gather_result(
                left,
                result,
                base_count,
                hash_table,
                left_projection_map,
                right_projection_map,
            );
        } else {
            result.set_cardinality(0);
        }

        if self.count == 0 {
            self.finished = true;
        }

        Ok(base_count)
    }

    /// Create a new scan structure.
    pub fn new(pointer_offset: usize) -> Self {
        Self {
            pointers: vec![0; VECTOR_SIZE],
            sel_vector: SelectionVector::incremental(VECTOR_SIZE),
            chain_match_sel: SelectionVector::incremental(VECTOR_SIZE),
            chain_no_match_sel: SelectionVector::incremental(VECTOR_SIZE),
            count: 0,
            found_match: vec![false; VECTOR_SIZE],
            lhs_sel: SelectionVector::incremental(VECTOR_SIZE),
            rhs_pointers: vec![0; VECTOR_SIZE],
            match_count: 0,
            finished: false,
            is_null: true,
            has_null_value_filter: false,
            pointer_offset,
            has_long_chains: false,
        }
    }

    /// Ensure the scan structure can address all probe rows in the current batch.
    pub fn ensure_capacity(&mut self, capacity: usize) {
        if self.pointers.len() >= capacity {
            return;
        }

        self.pointers.resize(capacity, 0);
        self.found_match.resize(capacity, false);
        self.rhs_pointers.resize(capacity, 0);
        self.sel_vector = SelectionVector::incremental(capacity);
        self.chain_match_sel = SelectionVector::incremental(capacity);
        self.chain_no_match_sel = SelectionVector::incremental(capacity);
        self.lhs_sel = SelectionVector::incremental(capacity);
    }

    /// Reset the scan structure for a new probe.
    pub fn reset(&mut self) {
        self.count = 0;
        self.match_count = 0;
        self.finished = false;
        self.is_null = false;
        self.has_null_value_filter = false;
        for i in 0..self.pointers.len() {
            self.found_match[i] = false;
            self.pointers[i] = 0;
            self.rhs_pointers[i] = 0;
        }
    }

    /// Check if all pointers are exhausted.
    #[inline]
    pub fn pointers_exhausted(&self) -> bool {
        self.count == 0
    }

    /// Advance pointers to the next entry in their chains.
    ///
    /// For each active pointer, load the next pointer from the chain.
    /// Pointers that reach null are removed from the active set.
    pub fn advance_pointers(&mut self) {
        if !self.has_long_chains {
            // If no chains longer than one, all pointers are exhausted after first iteration
            self.count = 0;
            return;
        }

        let mut new_count = 0;

        for i in 0..self.count {
            let idx = self.sel_vector.get(i);
            let ptr = self.pointers[idx];

            if ptr != 0 {
                // Load next pointer from chain
                let next_ptr = unsafe {
                    let next_ptr_location = (ptr + self.pointer_offset) as *const *const u8;
                    std::ptr::read_unaligned(next_ptr_location)
                };

                self.pointers[idx] = next_ptr as usize;

                if next_ptr as usize != 0 {
                    self.sel_vector.set(new_count, idx);
                    new_count += 1;
                }
            }
        }

        self.count = new_count;
    }

    /// Advance pointers using a custom selection vector.
    pub fn advance_pointers_sel(&mut self, sel: &SelectionVector, sel_count: usize) {
        if !self.has_long_chains {
            self.count = 0;
            return;
        }

        let mut new_count = 0;

        for i in 0..sel_count {
            let idx = sel.get(i);
            let ptr = self.pointers[idx];

            if ptr != 0 {
                // Load next pointer from chain
                let next_ptr = unsafe {
                    let next_ptr_location = (ptr + self.pointer_offset) as *const *const u8;
                    std::ptr::read_unaligned(next_ptr_location)
                };

                self.pointers[idx] = next_ptr as usize;

                if next_ptr as usize != 0 {
                    self.sel_vector.set(new_count, idx);
                    new_count += 1;
                }
            }
        }

        self.count = new_count;
    }

    /// Mark rows as matched (for outer joins).
    pub fn mark_matches(&mut self, result_sel: &SelectionVector, result_count: usize) {
        for i in 0..result_count {
            let idx = result_sel.get(i);
            self.found_match[idx] = true;
        }
    }

    fn accept_all_matches(
        _lhs_sel: &SelectionVector,
        _rhs_ptrs: &[usize],
        match_count: usize,
        output_sel: &mut SelectionVector,
    ) -> Result<usize> {
        for i in 0..match_count {
            output_sel.set(i, i);
        }
        Ok(match_count)
    }

    fn projected_build_types(
        hash_table: &JoinHashTable,
        right_projection_map: &[usize],
    ) -> Vec<LogicalType> {
        if right_projection_map.is_empty() {
            hash_table.build_types.clone()
        } else {
            right_projection_map
                .iter()
                .filter_map(|&idx| hash_table.build_types.get(idx).cloned())
                .collect()
        }
    }

    fn probe_row_has_null(
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        row_idx: usize,
    ) -> bool {
        hash_table
            .conditions
            .iter()
            .enumerate()
            .any(|(col_idx, condition)| {
                !matches!(
                    condition.comparison,
                    paro_planner::operator::join::JoinComparisonType::NotDistinctFrom
                ) && keys.data[col_idx].get_value(row_idx).is_null()
            })
    }

    fn scan_key_matches_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        mut residual_filter: F,
    ) -> Result<()>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        while self.count > 0 {
            let match_count = self.resolve_predicates(keys, hash_table, 0);
            let mut accepted_sel = SelectionVector::incremental(match_count);
            let accepted_count = residual_filter(
                &self.chain_match_sel,
                &self.rhs_pointers[..match_count],
                match_count,
                &mut accepted_sel,
            )?;

            let mut accepted_flags = vec![false; self.found_match.len()];
            for i in 0..accepted_count {
                let match_idx = accepted_sel.get(i);
                let lhs_idx = self.chain_match_sel.get(match_idx);
                self.found_match[lhs_idx] = true;
                accepted_flags[lhs_idx] = true;
            }

            let mut continue_rows = Vec::with_capacity(self.count.saturating_sub(accepted_count));
            for i in 0..self.count {
                let lhs_idx = self.sel_vector.get(i);
                if !accepted_flags[lhs_idx] {
                    continue_rows.push(lhs_idx as u32);
                }
            }

            if continue_rows.is_empty() {
                self.count = 0;
                break;
            }

            let continue_sel = SelectionVector::from_indices(continue_rows);
            self.advance_pointers_sel(&continue_sel, continue_sel.len());
        }

        Ok(())
    }

    fn mark_right_matches_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        mut residual_filter: F,
    ) -> Result<()>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        while self.count > 0 {
            let match_count = self.resolve_predicates(keys, hash_table, 0);
            let mut accepted_sel = SelectionVector::incremental(match_count);
            let accepted_count = residual_filter(
                &self.chain_match_sel,
                &self.rhs_pointers[..match_count],
                match_count,
                &mut accepted_sel,
            )?;

            for i in 0..accepted_count {
                let match_idx = accepted_sel.get(i);
                let lhs_idx = self.chain_match_sel.get(match_idx);
                self.found_match[lhs_idx] = true;
                let rhs_ptr = self.rhs_pointers[match_idx];
                hash_table.set_build_side_found(rhs_ptr, true);
            }

            self.advance_pointers();
        }

        Ok(())
    }

    fn next_semi_or_anti_join_with_filter<const MATCH: bool, F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        self.scan_key_matches_with_filter(keys, hash_table, residual_filter)?;

        let selected_rows: Vec<u32> = (0..left.size())
            .filter(|&idx| self.found_match[idx] == MATCH)
            .map(|idx| idx as u32)
            .collect();

        if selected_rows.is_empty() {
            result.set_cardinality(0);
        } else {
            let sel = SelectionVector::from_indices(selected_rows);
            if MATCH {
                construct_semi_join_result(left, &sel, sel.len(), left_projection_map, result);
            } else {
                construct_anti_join_result(left, &sel, sel.len(), left_projection_map, result);
            }
        }

        self.finished = true;
        Ok(result.size())
    }

    /// Resolve predicates for the current set of matching candidates.
    ///
    /// This compares the probe-side keys with the keys stored in the build-side rows.
    pub fn resolve_predicates(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        base_offset: usize,
    ) -> usize {
        // For now, we only implement simple equality checks

        let mut match_count = 0;
        let equality_types = &hash_table.equality_types;

        for i in 0..self.count {
            let idx = self.sel_vector.get(i);
            let row_ptr = self.pointers[idx];

            if row_ptr == 0 {
                continue;
            }

            // Verify all equality conditions
            let mut matched = true;

            for (col_idx, _col_type) in equality_types.iter().enumerate() {
                let probe_val = keys.data[col_idx].get_value(idx);
                let build_val = hash_table.read_equality_value(row_ptr, col_idx);

                if !hash_table.equality_values_match(col_idx, &probe_val, &build_val) {
                    matched = false;
                    break;
                }
            }

            if matched {
                self.chain_match_sel.set(match_count, idx);
                self.rhs_pointers[base_offset + match_count] = row_ptr as usize;
                match_count += 1;
            }
        }

        match_count
    }

    /// Scan results for an inner join.
    pub fn next_inner_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
    ) -> Result<usize> {
        self.next_inner_join_impl(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            right_projection_map,
            |_, _, match_count, output_sel| {
                for i in 0..match_count {
                    output_sel.set(i, i);
                }
                Ok(match_count)
            },
        )
    }

    /// Scan results for an inner join with an additional residual filter.
    pub fn next_inner_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
        residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        self.next_inner_join_impl(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            right_projection_map,
            residual_filter,
        )
    }

    pub fn next_left_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
    ) -> Result<usize> {
        self.next_left_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            right_projection_map,
            Self::accept_all_matches,
        )
    }

    pub fn next_left_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
        mut residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        let mut base_count = 0;

        while self.count > 0 && base_count < VECTOR_SIZE {
            if base_count > 0 && base_count + self.count > VECTOR_SIZE {
                break;
            }

            let rhs_offset = base_count;
            let match_count = self.resolve_predicates(keys, hash_table, rhs_offset);
            if match_count > 0 {
                let mut accepted_sel = SelectionVector::incremental(match_count);
                let accepted_count = residual_filter(
                    &self.chain_match_sel,
                    &self.rhs_pointers[rhs_offset..rhs_offset + match_count],
                    match_count,
                    &mut accepted_sel,
                )?;

                for i in 0..accepted_count {
                    let match_idx = accepted_sel.get(i);
                    let lhs_idx = self.chain_match_sel.get(match_idx);
                    self.lhs_sel.set(base_count + i, lhs_idx);
                    self.rhs_pointers[rhs_offset + i] = self.rhs_pointers[rhs_offset + match_idx];
                    self.found_match[lhs_idx] = true;
                    if hash_table.has_found_flag() {
                        hash_table.set_build_side_found(self.rhs_pointers[rhs_offset + i], true);
                    }
                }

                base_count += accepted_count;
            }

            self.advance_pointers();
        }

        if base_count > 0 {
            self.gather_result(
                left,
                result,
                base_count,
                hash_table,
                left_projection_map,
                right_projection_map,
            );
            return Ok(base_count);
        }

        let unmatched_rows: Vec<u32> = (0..left.size())
            .filter(|&idx| !self.found_match[idx])
            .map(|idx| idx as u32)
            .collect();

        if unmatched_rows.is_empty() {
            result.set_cardinality(0);
        } else {
            let unmatched_sel = SelectionVector::from_indices(unmatched_rows);
            let projected_right_types =
                Self::projected_build_types(hash_table, right_projection_map);
            construct_left_outer_result(
                left,
                &unmatched_sel,
                unmatched_sel.len(),
                left_projection_map,
                &projected_right_types,
                result,
            );
        }

        self.finished = true;
        Ok(result.size())
    }

    pub fn next_semi_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
    ) -> Result<usize> {
        self.next_semi_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            Self::accept_all_matches,
        )
    }

    pub fn next_semi_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        self.next_semi_or_anti_join_with_filter::<true, F>(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            residual_filter,
        )
    }

    pub fn next_anti_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
    ) -> Result<usize> {
        self.next_anti_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            Self::accept_all_matches,
        )
    }

    pub fn next_anti_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        self.next_semi_or_anti_join_with_filter::<false, F>(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            residual_filter,
        )
    }

    pub fn next_mark_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
    ) -> Result<usize> {
        self.next_mark_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            Self::accept_all_matches,
        )
    }

    pub fn next_mark_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        self.scan_key_matches_with_filter(keys, hash_table, residual_filter)?;

        let markers: Vec<Option<bool>> = (0..left.size())
            .map(|idx| {
                if self.found_match[idx] {
                    Some(true)
                } else if Self::probe_row_has_null(keys, hash_table, idx)
                    || hash_table
                        .has_null
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    None
                } else {
                    Some(false)
                }
            })
            .collect();

        construct_mark_join_result(left, left_projection_map, &markers, result);
        self.finished = true;
        Ok(result.size())
    }

    pub fn next_single_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
    ) -> Result<usize> {
        self.next_single_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            right_projection_map,
            Self::accept_all_matches,
        )
    }

    pub fn next_single_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
        mut residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        let mut matched_ptrs = vec![None; left.size()];

        while self.count > 0 {
            let match_count = self.resolve_predicates(keys, hash_table, 0);
            let mut accepted_sel = SelectionVector::incremental(match_count);
            let accepted_count = residual_filter(
                &self.chain_match_sel,
                &self.rhs_pointers[..match_count],
                match_count,
                &mut accepted_sel,
            )?;

            for i in 0..accepted_count {
                let match_idx = accepted_sel.get(i);
                let lhs_idx = self.chain_match_sel.get(match_idx);
                if matched_ptrs[lhs_idx].is_some() {
                    return Err(paro_common::error::invalid_input(
                        "More than one row returned by a SINGLE join",
                    ));
                }
                self.found_match[lhs_idx] = true;
                matched_ptrs[lhs_idx] = Some(self.rhs_pointers[match_idx]);
            }

            self.advance_pointers();
        }

        let left_sel = SelectionVector::incremental(left.size());
        let left_indices: Vec<usize> = if left_projection_map.is_empty() {
            (0..left.column_count()).collect()
        } else {
            left_projection_map.to_vec()
        };
        let right_indices: Vec<usize> = if right_projection_map.is_empty() {
            (0..hash_table.build_types.len()).collect()
        } else {
            right_projection_map.to_vec()
        };

        for (out_idx, left_idx) in left_indices.iter().enumerate() {
            result.data[out_idx] = Arc::new(Vector::dictionary(
                Arc::clone(&left.data[*left_idx]),
                left_sel.clone(),
            ));
        }

        let right_offset = left_indices.len();
        for (out_idx, build_idx) in right_indices.iter().enumerate() {
            let vector = result
                .column_mut(right_offset + out_idx)
                .expect("single join output vector must exist");

            for (row_idx, row_ptr) in matched_ptrs.iter().enumerate() {
                if let Some(ptr) = row_ptr {
                    let value = hash_table.read_build_value(*ptr, *build_idx);
                    vector.set_value(row_idx, &value);
                    vector.set_null(row_idx, false);
                } else {
                    vector.set_null(row_idx, true);
                }
            }
        }

        result.set_cardinality(left.size());
        self.finished = true;
        Ok(result.size())
    }

    pub fn next_right_semi_or_anti_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
    ) -> Result<usize> {
        self.next_right_semi_or_anti_join_with_filter(keys, hash_table, Self::accept_all_matches)
    }

    pub fn next_right_semi_or_anti_join_with_filter<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            return Ok(0);
        }

        self.mark_right_matches_with_filter(keys, hash_table, residual_filter)?;
        self.finished = true;
        Ok(0)
    }

    /// Gather data from build-side hash table to fill result chunk.
    pub fn gather_result(
        &self,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        count: usize,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        right_projection_map: &[usize],
    ) {
        let left_indices: Vec<usize> = if left_projection_map.is_empty() {
            (0..left.column_count()).collect()
        } else {
            left_projection_map.to_vec()
        };
        let right_indices: Vec<usize> = if right_projection_map.is_empty() {
            (0..hash_table.build_types.len()).collect()
        } else {
            right_projection_map.to_vec()
        };
        let mut expected_types = left_indices
            .iter()
            .map(|left_idx| left.data[*left_idx].logical_type().clone())
            .collect::<Vec<_>>();
        expected_types.extend(
            right_indices
                .iter()
                .filter_map(|build_idx| hash_table.build_types.get(*build_idx).cloned()),
        );
        if result.column_count() != expected_types.len()
            || result.capacity() < count
            || result.types() != expected_types
        {
            *result =
                paro_common::chunk::Chunk::initialize(&expected_types, VECTOR_SIZE.max(count));
        } else {
            result.reset();
        }
        result.set_cardinality(count);
        let mut lhs_sel = self.lhs_sel.clone();
        lhs_sel.set_len(count);

        // 1. Copy projected LHS columns
        for (out_idx, left_idx) in left_indices.iter().enumerate() {
            // Reference the columns with selection vector
            result.data[out_idx] = Arc::new(Vector::dictionary(
                left.data[*left_idx].clone(),
                lhs_sel.clone(),
            ));
        }

        // 2. Gather projected RHS columns
        let right_result_offset = left_indices.len();
        for (out_idx, build_idx) in right_indices.iter().enumerate() {
            for row_idx in 0..count {
                let row_ptr = self.rhs_pointers[row_idx];
                let val = hash_table.read_build_value(row_ptr, *build_idx);
                result
                    .column_mut(right_result_offset + out_idx)
                    .unwrap()
                    .set_value(row_idx, &val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join_hashtable::join_hashtable::{JoinHashTable, JoinHashTableConfig};
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_planner::expression::{ConstantExpression, Expression};
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
    use paro_storage::buffer::BufferPool;
    use std::sync::Arc;

    fn create_test_buffer_pool() -> Arc<BufferPool> {
        BufferPool::new_arc(64 * 1024 * 1024)
    }

    fn equality_condition() -> JoinCondition {
        JoinCondition::new(
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            JoinComparisonType::Equal,
        )
    }

    fn not_distinct_condition() -> JoinCondition {
        JoinCondition::new(
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            Expression::Constant(ConstantExpression::new(
                Value::Integer(1),
                LogicalType::Integer,
            )),
            JoinComparisonType::NotDistinctFrom,
        )
    }

    fn build_hash_table(
        join_type: JoinType,
        build_keys: &[i32],
        build_payload: &[i32],
    ) -> JoinHashTable {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            join_type,
            JoinHashTableConfig::default(),
        );

        let keys = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(build_keys))]);
        let payload = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(build_payload))]);
        ht.build(&keys, &payload).unwrap();
        ht.finalize().unwrap();
        ht
    }

    fn chunk_from_optional_i32(values: &[Option<i32>]) -> Chunk {
        let mut chunk = Chunk::initialize(&[LogicalType::Integer], values.len());
        for (row_idx, value) in values.iter().enumerate() {
            let column = chunk.column_mut(0).expect("column must exist");
            match value {
                Some(value) => column.set_value(row_idx, &Value::Integer(*value)),
                None => column.set_value(row_idx, &Value::Null(LogicalType::Integer)),
            }
        }
        chunk.set_cardinality(values.len());
        chunk
    }

    fn build_hash_table_from_optional(
        join_type: JoinType,
        condition: JoinCondition,
        build_keys: &[Option<i32>],
        build_payload: &[i32],
    ) -> JoinHashTable {
        let ht = JoinHashTable::new(
            create_test_buffer_pool(),
            vec![condition],
            vec![LogicalType::Integer],
            join_type,
            JoinHashTableConfig::default(),
        );

        let keys = chunk_from_optional_i32(build_keys);
        let payload = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(build_payload))]);
        ht.build(&keys, &payload).unwrap();
        ht.finalize().unwrap();
        ht
    }

    fn prepare_probe(ht: &JoinHashTable, probe_keys: &[i32]) -> (ScanStructure, Chunk, Chunk) {
        let keys = Chunk::from_arc_vectors(vec![Arc::new(Vector::from_i32(probe_keys))]);
        let left = keys.clone();
        let mut scan = ht.create_scan_structure();
        ht.probe(&keys, &mut scan, None, keys.size());
        (scan, keys, left)
    }

    #[test]
    fn test_scan_structure_new() {
        let ss = ScanStructure::new(16);
        assert_eq!(ss.count, 0);
        assert!(ss.is_null);
        assert!(!ss.finished);
        assert_eq!(ss.pointer_offset, 16);
    }

    #[test]
    fn test_scan_structure_reset() {
        let mut ss = ScanStructure::new(16);
        ss.count = 5;
        ss.finished = true;
        ss.is_null = false;
        ss.found_match[0] = true;

        ss.reset();

        assert_eq!(ss.count, 0);
        assert!(!ss.is_null);
        assert!(!ss.finished);
        assert!(!ss.found_match[0]);
    }

    #[test]
    fn test_pointers_exhausted() {
        let mut ss = ScanStructure::new(16);
        assert!(ss.pointers_exhausted());

        ss.count = 1;
        assert!(!ss.pointers_exhausted());
    }

    #[test]
    fn test_next_left_join_emits_matches_then_unmatched_rows() {
        let ht = build_hash_table(JoinType::Left, &[1, 2], &[10, 20]);
        let (mut scan, keys, left) = prepare_probe(&ht, &[1, 3]);
        let mut result = Chunk::initialize(&[LogicalType::Integer, LogicalType::Integer], 2);

        let first = scan
            .next_left_join(&keys, &left, &mut result, &ht, &[], &[])
            .unwrap();
        assert_eq!(first, 1);
        assert_eq!(result.data[0].get_value(0).to_string(), "1");
        assert_eq!(result.data[1].get_value(0).to_string(), "10");

        let second = scan
            .next_left_join(&keys, &left, &mut result, &ht, &[], &[])
            .unwrap();
        assert_eq!(second, 1);
        assert_eq!(result.data[0].get_value(0).to_string(), "3");
        assert!(result.data[1].is_null(0));
    }

    #[test]
    fn test_next_semi_anti_and_mark_join() {
        let ht = build_hash_table(JoinType::Inner, &[1, 2], &[10, 20]);

        let (mut semi_scan, keys, left) = prepare_probe(&ht, &[1, 3]);
        let mut semi_result = Chunk::initialize(&[LogicalType::Integer], 2);
        let semi_count = semi_scan
            .next_semi_join(&keys, &left, &mut semi_result, &ht, &[])
            .unwrap();
        assert_eq!(semi_count, 1);
        assert_eq!(semi_result.data[0].get_value(0).to_string(), "1");

        let (mut anti_scan, keys, left) = prepare_probe(&ht, &[1, 3]);
        let mut anti_result = Chunk::initialize(&[LogicalType::Integer], 2);
        let anti_count = anti_scan
            .next_anti_join(&keys, &left, &mut anti_result, &ht, &[])
            .unwrap();
        assert_eq!(anti_count, 1);
        assert_eq!(anti_result.data[0].get_value(0).to_string(), "3");

        let (mut mark_scan, keys, left) = prepare_probe(&ht, &[1, 3]);
        let mut mark_result = Chunk::initialize(&[LogicalType::Integer, LogicalType::Boolean], 2);
        let mark_count = mark_scan
            .next_mark_join(&keys, &left, &mut mark_result, &ht, &[])
            .unwrap();
        assert_eq!(mark_count, 2);
        assert_eq!(mark_result.data[1].get_value(0).to_string(), "true");
        assert_eq!(mark_result.data[1].get_value(1).to_string(), "false");
    }

    #[test]
    fn test_not_distinct_from_semi_and_anti_join_respect_null_matches() {
        let ht = build_hash_table_from_optional(
            JoinType::Semi,
            not_distinct_condition(),
            &[None, Some(2)],
            &[10, 20],
        );

        let keys = chunk_from_optional_i32(&[None, Some(1), Some(2)]);
        let left = keys.clone();

        let mut semi_scan = ht.create_scan_structure();
        ht.probe(&keys, &mut semi_scan, None, keys.size());
        let mut semi_result = Chunk::initialize(&[LogicalType::Integer], 3);
        let semi_count = semi_scan
            .next_semi_join(&keys, &left, &mut semi_result, &ht, &[])
            .unwrap();
        assert_eq!(semi_count, 2);
        assert!(semi_result.data[0].is_null(0));
        assert_eq!(semi_result.data[0].get_value(1).to_string(), "2");

        let mut anti_scan = ht.create_scan_structure();
        ht.probe(&keys, &mut anti_scan, None, keys.size());
        let mut anti_result = Chunk::initialize(&[LogicalType::Integer], 3);
        let anti_count = anti_scan
            .next_anti_join(&keys, &left, &mut anti_result, &ht, &[])
            .unwrap();
        assert_eq!(anti_count, 1);
        assert_eq!(anti_result.data[0].get_value(0).to_string(), "1");
    }

    #[test]
    fn test_next_single_join_null_fills_unmatched_rows() {
        let ht = build_hash_table(JoinType::Single, &[1, 2], &[10, 20]);
        let (mut scan, keys, left) = prepare_probe(&ht, &[1, 3]);
        let mut result = Chunk::initialize(&[LogicalType::Integer, LogicalType::Integer], 2);

        let count = scan
            .next_single_join(&keys, &left, &mut result, &ht, &[], &[])
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(result.data[0].get_value(0).to_string(), "1");
        assert_eq!(result.data[1].get_value(0).to_string(), "10");
        assert_eq!(result.data[0].get_value(1).to_string(), "3");
        assert!(result.data[1].is_null(1));
    }

    #[test]
    fn test_next_single_join_errors_on_duplicates() {
        let ht = build_hash_table(JoinType::Single, &[1, 1], &[10, 11]);
        let (mut scan, keys, left) = prepare_probe(&ht, &[1]);
        let mut result = Chunk::initialize(&[LogicalType::Integer, LogicalType::Integer], 1);

        let err = scan
            .next_single_join(&keys, &left, &mut result, &ht, &[], &[])
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("More than one row returned by a SINGLE join"));
    }

    #[test]
    fn test_next_right_semi_or_anti_join_marks_build_rows() {
        let ht = build_hash_table(JoinType::RightSemi, &[1, 2], &[10, 20]);
        let (mut scan, keys, _left) = prepare_probe(&ht, &[1]);

        let count = scan.next_right_semi_or_anti_join(&keys, &ht).unwrap();
        assert_eq!(count, 0);

        let mut matched_state = ht.create_full_outer_scan_state();
        let mut matched = Chunk::new();
        let matched_count = ht
            .scan_full_outer(&mut matched_state, true, &mut matched)
            .unwrap();
        assert_eq!(matched_count, 1);
        assert_eq!(matched.data[0].get_value(0).to_string(), "10");

        let mut unmatched_state = ht.create_full_outer_scan_state();
        let mut unmatched = Chunk::new();
        let unmatched_count = ht
            .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
            .unwrap();
        assert_eq!(unmatched_count, 1);
        assert_eq!(unmatched.data[0].get_value(0).to_string(), "20");
    }

    #[test]
    fn test_next_right_semi_or_anti_join_marks_all_duplicate_build_rows() {
        let ht = build_hash_table(JoinType::RightSemi, &[3, 3], &[30, 31]);
        let (mut scan, keys, _left) = prepare_probe(&ht, &[3, 3]);

        let count = scan.next_right_semi_or_anti_join(&keys, &ht).unwrap();
        assert_eq!(count, 0);

        let mut matched_state = ht.create_full_outer_scan_state();
        let mut matched = Chunk::new();
        let matched_count = ht
            .scan_full_outer(&mut matched_state, true, &mut matched)
            .unwrap();
        assert_eq!(matched_count, 2);
        assert_eq!(matched.data[0].get_value(0).to_string(), "30");
        assert_eq!(matched.data[0].get_value(1).to_string(), "31");

        let mut unmatched_state = ht.create_full_outer_scan_state();
        let mut unmatched = Chunk::new();
        let unmatched_count = ht
            .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
            .unwrap();
        assert_eq!(unmatched_count, 0);
    }

    #[test]
    fn test_next_inner_join_marks_build_rows_for_right_join_source_scan() {
        let ht = build_hash_table(JoinType::Right, &[1, 2], &[10, 20]);
        let (mut scan, keys, left) = prepare_probe(&ht, &[1]);
        let mut result = Chunk::initialize(&[LogicalType::Integer, LogicalType::Integer], 1);

        let count = scan
            .next_inner_join(&keys, &left, &mut result, &ht, &[], &[])
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(result.data[0].get_value(0).to_string(), "1");
        assert_eq!(result.data[1].get_value(0).to_string(), "10");

        let mut unmatched_state = ht.create_full_outer_scan_state();
        let mut unmatched = Chunk::new();
        let unmatched_count = ht
            .scan_full_outer(&mut unmatched_state, false, &mut unmatched)
            .unwrap();
        assert_eq!(unmatched_count, 1);
        assert_eq!(unmatched.data[0].get_value(0).to_string(), "20");
    }
}
