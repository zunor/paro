// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Iteration state for hash join probe results (pointer chains, outer/semi/mark paths).
//!
//! Tracks pointers into the hash table, collision chains, and match flags for outer joins.

use paro_common::allocator::Allocator;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_common::vector::{SelectionVector, VECTOR_SIZE};
use std::sync::Arc;

use crate::join_hashtable::hash_kernel::PreparedProbeKeys;
use crate::join_hashtable::JoinHashTable;
use crate::operators::join::join_result_helpers::{
    construct_anti_join_result, construct_left_outer_result, construct_mark_join_result,
    construct_semi_join_result,
};

/// Scan structure for probing the hash table and iterating over results.
///
/// This structure maintains state between calls to `Next()` when a single
/// probe may return multiple batches of results.
pub struct ScanStructure {
    /// Allocator for selection-vector scratch state.
    allocator: Arc<dyn Allocator>,
    /// Pointers to build-side rows for matching entries.
    pub pointers: Vec<usize>,
    /// Incremental selection vector for the pointers.
    pub sel_vector: SelectionVector,
    /// Reusable selection vector for probe rows after NULL filtering.
    pub probe_sel: SelectionVector,
    /// Reusable selection vector for probe rows that need to continue a chain.
    continue_sel: SelectionVector,
    /// Selection vector for matches that passed equality checks.
    pub chain_match_sel: SelectionVector,
    /// General-purpose scratch selection for residual filters and output gathers.
    pub scratch_sel: SelectionVector,
    /// Number of active pointers (matches found and needs verifying).
    pub count: usize,
    /// Whether each probe-side row has found at least one match.
    pub found_match: Vec<bool>,
    /// Selection vector for probe-side indices of current matches.
    pub lhs_sel: SelectionVector,
    /// Pointers to build-side rows that passed all predicates.
    pub rhs_pointers: Vec<usize>,
    /// Adjacent-unique build pointers used to preserve one-to-many build
    /// matches as dictionary vectors instead of copying repeated payloads.
    rhs_unique_pointers: Vec<usize>,
    /// Logical output row to adjacent-unique build row mapping.
    rhs_dictionary_sel: SelectionVector,
    /// Build pointers retained while a match batch is drained across output vectors.
    pending_rhs_pointers: Vec<usize>,
    /// Accepted matches in the current chain step.
    pending_match_count: usize,
    /// Next accepted match to emit.
    pending_match_offset: usize,
    /// Matched build row per probe row for a SINGLE join (zero means unmatched).
    single_match_pointers: Vec<usize>,
    /// Next probe row (or candidate row) to emit for one-row-per-probe joins.
    probe_output_offset: usize,
    /// Whether all match chains have been collected for one-row-per-probe joins.
    probe_matches_ready: bool,
    /// Reusable probe hash buffer.
    pub hashes: Vec<u64>,
    /// Reusable flags for residual-filtered probe rows.
    accepted_flags: Vec<bool>,
    /// Per-candidate existential match bits for a fused reduction cascade.
    candidate_match_masks: Vec<u8>,
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
    /// Whether every published pointer came from an exact-key index lookup.
    ///
    /// Direct-address indexes map a typed key ordinal to a unique build row,
    /// so repeating the row-key comparison while draining the scan would only
    /// duplicate work. Chained hash-table probes leave this disabled because
    /// their salt match still requires an exact equality check.
    pub exact_key_matches: bool,
}

impl std::fmt::Debug for ScanStructure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanStructure")
            .field("allocator", &self.allocator.name())
            .field("pointers", &self.pointers)
            .field("sel_vector", &self.sel_vector)
            .field("probe_sel", &self.probe_sel)
            .field("continue_sel", &self.continue_sel)
            .field("chain_match_sel", &self.chain_match_sel)
            .field("scratch_sel", &self.scratch_sel)
            .field("count", &self.count)
            .field("match_count", &self.match_count)
            .field("finished", &self.finished)
            .finish()
    }
}

impl ScanStructure {
    fn next_inner_join_impl<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
        mut residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            return Ok(0);
        }

        let prepared_keys = self.prepare_probe_keys(keys, hash_table)?;
        let mut base_count = 0;

        while base_count < paro_common::vector::VECTOR_SIZE {
            if self.pending_match_offset < self.pending_match_count {
                let available = self.pending_match_count - self.pending_match_offset;
                let emit_count =
                    available.min(paro_common::vector::VECTOR_SIZE.saturating_sub(base_count));
                for output_idx in 0..emit_count {
                    let pending_idx = self.pending_match_offset + output_idx;
                    self.lhs_sel.set(
                        base_count + output_idx,
                        self.chain_match_sel.get(pending_idx),
                    );
                    let build_ptr = self.pending_rhs_pointers[pending_idx];
                    self.rhs_pointers[base_count + output_idx] = build_ptr;
                    if hash_table.has_found_flag() {
                        hash_table.set_build_side_found(build_ptr, true);
                    }
                }
                self.pending_match_offset += emit_count;
                base_count += emit_count;

                if self.pending_match_offset < self.pending_match_count {
                    break;
                }

                self.pending_match_count = 0;
                self.pending_match_offset = 0;
                self.advance_pointers();
                continue;
            }

            if self.count == 0 {
                break;
            }

            let match_count =
                self.resolve_predicates_into_pending(prepared_keys.as_ref(), hash_table);
            self.scratch_sel.set_len(match_count);
            let accepted_count = residual_filter(
                &self.chain_match_sel,
                &self.pending_rhs_pointers[..match_count],
                match_count,
                &mut self.scratch_sel,
            )?;
            for accepted_idx in 0..accepted_count {
                let match_idx = self.scratch_sel.get(accepted_idx);
                self.chain_match_sel
                    .set(accepted_idx, self.chain_match_sel.get(match_idx));
                self.pending_rhs_pointers[accepted_idx] = self.pending_rhs_pointers[match_idx];
            }
            self.pending_match_count = accepted_count;
            self.pending_match_offset = 0;

            if accepted_count == 0 {
                self.advance_pointers();
            }
        }

        if base_count > 0 {
            self.gather_result(left, result, base_count, hash_table, left_projection_map)?;
        } else {
            result.set_cardinality(0);
        }

        if self.count == 0 && self.pending_match_count == 0 {
            self.finished = true;
        }

        Ok(base_count)
    }

    /// Create a new scan structure.
    pub fn try_new(pointer_offset: usize, allocator: Arc<dyn Allocator>) -> Result<Self> {
        Ok(Self {
            allocator: allocator.clone(),
            pointers: vec![0; VECTOR_SIZE],
            sel_vector: SelectionVector::try_incremental(VECTOR_SIZE, allocator.clone())?,
            probe_sel: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?,
            continue_sel: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?,
            chain_match_sel: SelectionVector::try_incremental(VECTOR_SIZE, allocator.clone())?,
            scratch_sel: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?,
            count: 0,
            found_match: vec![false; VECTOR_SIZE],
            lhs_sel: SelectionVector::try_incremental(VECTOR_SIZE, allocator.clone())?,
            rhs_pointers: vec![0; VECTOR_SIZE],
            rhs_unique_pointers: Vec::with_capacity(VECTOR_SIZE),
            rhs_dictionary_sel: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator.clone())?,
            pending_rhs_pointers: vec![0; VECTOR_SIZE],
            pending_match_count: 0,
            pending_match_offset: 0,
            single_match_pointers: vec![0; VECTOR_SIZE],
            probe_output_offset: 0,
            probe_matches_ready: false,
            hashes: vec![0; VECTOR_SIZE],
            accepted_flags: vec![false; VECTOR_SIZE],
            candidate_match_masks: vec![0; VECTOR_SIZE],
            match_count: 0,
            finished: false,
            is_null: true,
            has_null_value_filter: false,
            pointer_offset,
            has_long_chains: false,
            exact_key_matches: false,
        })
    }

    /// Ensure the scan structure can address all probe rows in the current batch.
    pub fn ensure_capacity(&mut self, capacity: usize) -> Result<()> {
        if self.pointers.len() >= capacity {
            return Ok(());
        }

        self.pointers.resize(capacity, 0);
        self.found_match.resize(capacity, false);
        self.rhs_pointers.resize(capacity, 0);
        self.pending_rhs_pointers.resize(capacity, 0);
        self.single_match_pointers.resize(capacity, 0);
        self.hashes.resize(capacity, 0);
        self.accepted_flags.resize(capacity, false);
        self.candidate_match_masks.resize(capacity, 0);
        self.sel_vector = SelectionVector::try_incremental(capacity, self.allocator.clone())?;
        self.probe_sel = SelectionVector::try_with_capacity(capacity, self.allocator.clone())?;
        self.continue_sel = SelectionVector::try_with_capacity(capacity, self.allocator.clone())?;
        self.chain_match_sel = SelectionVector::try_incremental(capacity, self.allocator.clone())?;
        self.scratch_sel = SelectionVector::try_with_capacity(capacity, self.allocator.clone())?;
        self.lhs_sel = SelectionVector::try_incremental(capacity, self.allocator.clone())?;
        self.rhs_unique_pointers
            .reserve(capacity.saturating_sub(self.rhs_unique_pointers.len()));
        self.rhs_dictionary_sel =
            SelectionVector::try_with_capacity(capacity, self.allocator.clone())?;
        Ok(())
    }

    /// Reset the scan structure for a new probe.
    pub fn reset(&mut self) {
        self.count = 0;
        self.match_count = 0;
        self.finished = false;
        self.is_null = false;
        self.has_null_value_filter = false;
        self.exact_key_matches = false;
        self.pending_match_count = 0;
        self.pending_match_offset = 0;
        self.probe_output_offset = 0;
        self.probe_matches_ready = false;
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

    fn advance_pointers_continue_sel(&mut self, sel_count: usize) {
        if !self.has_long_chains {
            self.count = 0;
            return;
        }

        let mut new_count = 0;
        for i in 0..sel_count {
            let idx = self.continue_sel.get(i);
            let ptr = self.pointers[idx];
            if ptr == 0 {
                continue;
            }

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
                ) && keys.data[col_idx].is_null(row_idx)
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
        let prepared_keys = self.prepare_probe_keys(keys, hash_table)?;
        while self.count > 0 {
            let match_count = self.resolve_predicates(prepared_keys.as_ref(), hash_table, 0);
            self.scratch_sel.set_len(match_count);
            let accepted_count = residual_filter(
                &self.chain_match_sel,
                &self.rhs_pointers[..match_count],
                match_count,
                &mut self.scratch_sel,
            )?;

            // accepted_flags is only read for rows that remain in sel_vector in
            // this chain step, so clearing the active slice is enough and avoids
            // an O(VECTOR_SIZE) reset on every long-chain iteration.
            for i in 0..self.count {
                self.accepted_flags[self.sel_vector.get(i)] = false;
            }
            for i in 0..accepted_count {
                let match_idx = self.scratch_sel.get(i);
                let lhs_idx = self.chain_match_sel.get(match_idx);
                self.found_match[lhs_idx] = true;
                self.accepted_flags[lhs_idx] = true;
            }

            self.continue_sel.set_len(self.count);
            let mut continue_count = 0usize;
            let continue_rows = self.continue_sel.as_mut_slice();
            for i in 0..self.count {
                let lhs_idx = self.sel_vector.get(i);
                if !self.accepted_flags[lhs_idx] {
                    continue_rows[continue_count] = lhs_idx as u32;
                    continue_count += 1;
                }
            }

            if continue_count == 0 {
                self.count = 0;
                break;
            }

            self.advance_pointers_continue_sel(continue_count);
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
        let prepared_keys = self.prepare_probe_keys(keys, hash_table)?;
        while self.count > 0 {
            let match_count = self.resolve_predicates(prepared_keys.as_ref(), hash_table, 0);
            self.scratch_sel.set_len(match_count);
            let accepted_count = residual_filter(
                &self.chain_match_sel,
                &self.rhs_pointers[..match_count],
                match_count,
                &mut self.scratch_sel,
            )?;

            for i in 0..accepted_count {
                let match_idx = self.scratch_sel.get(i);
                let lhs_idx = self.chain_match_sel.get(match_idx);
                self.found_match[lhs_idx] = true;
                let rhs_ptr = self.rhs_pointers[match_idx];
                hash_table.set_build_side_found(rhs_ptr, true);
            }

            self.advance_pointers();
        }

        Ok(())
    }

    /// Visit each equality-key candidate once and atomically publish all
    /// existential reduction bits accepted by `classify_matches`.
    pub fn mark_right_matches_with_masks<F>(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        mut classify_matches: F,
    ) -> Result<()>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut [u8]) -> Result<()>,
    {
        let prepared_keys = self.prepare_probe_keys(keys, hash_table)?;
        while self.count > 0 {
            let match_count = self.resolve_predicates(prepared_keys.as_ref(), hash_table, 0);
            let masks = &mut self.candidate_match_masks[..match_count];
            masks.fill(0);
            classify_matches(
                &self.chain_match_sel,
                &self.rhs_pointers[..match_count],
                match_count,
                masks,
            )?;
            for (candidate_idx, &mask) in masks.iter().enumerate() {
                if mask != 0 {
                    hash_table.mark_build_side_match_mask(self.rhs_pointers[candidate_idx], mask);
                }
            }
            self.advance_pointers();
        }
        self.finished = true;
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

        if !self.probe_matches_ready {
            self.scan_key_matches_with_filter(keys, hash_table, residual_filter)?;
            self.probe_matches_ready = true;
        }

        let (selected_count, finished) = collect_existence_output::<MATCH>(
            &mut self.scratch_sel,
            &self.found_match,
            &mut self.probe_output_offset,
            None,
            left.size(),
            result.capacity(),
        )?;

        if selected_count == 0 {
            result.set_cardinality(0);
        } else if MATCH {
            construct_semi_join_result(
                left,
                &self.scratch_sel,
                selected_count,
                left_projection_map,
                result,
            )?;
        } else {
            construct_anti_join_result(
                left,
                &self.scratch_sel,
                selected_count,
                left_projection_map,
                result,
            )?;
        }

        self.finished = finished;
        Ok(result.size())
    }

    /// Emit SQL `NOT IN` semantics after an equality probe.
    ///
    /// The caller handles a NULL on the build side globally. For a non-empty,
    /// all-valid build side, only the non-NULL probe rows retained in
    /// `probe_sel` can produce output, and only when no equality match exists.
    pub fn next_null_aware_anti_join(
        &mut self,
        keys: &paro_common::chunk::Chunk,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
    ) -> Result<usize> {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        if !self.probe_matches_ready {
            self.scan_key_matches_with_filter(keys, hash_table, Self::accept_all_matches)?;
            self.probe_matches_ready = true;
        }

        let (selected_count, finished) = collect_existence_output::<false>(
            &mut self.scratch_sel,
            &self.found_match,
            &mut self.probe_output_offset,
            Some(&self.probe_sel),
            self.probe_sel.len(),
            result.capacity(),
        )?;

        construct_anti_join_result(
            left,
            &self.scratch_sel,
            selected_count,
            left_projection_map,
            result,
        )?;
        self.finished = finished;
        Ok(result.size())
    }

    /// Resolve predicates for the current set of matching candidates.
    ///
    /// This compares the probe-side keys with the keys stored in the build-side rows.
    fn prepare_probe_keys<'a>(
        &self,
        keys: &'a paro_common::chunk::Chunk,
        hash_table: &JoinHashTable,
    ) -> Result<Option<PreparedProbeKeys<'a>>> {
        (!self.exact_key_matches)
            .then(|| hash_table.prepare_probe_keys(keys))
            .transpose()
    }

    fn resolve_predicates(
        &mut self,
        prepared_keys: Option<&PreparedProbeKeys<'_>>,
        hash_table: &JoinHashTable,
        base_offset: usize,
    ) -> usize {
        let mut match_count = 0;

        for i in 0..self.count {
            let idx = self.sel_vector.get(i);
            let row_ptr = self.pointers[idx];

            if row_ptr == 0 {
                continue;
            }

            if self.exact_key_matches
                || hash_table.key_values_match_build_row(
                    prepared_keys.expect("non-exact join probe must prepare its keys"),
                    idx,
                    row_ptr,
                )
            {
                self.chain_match_sel.set(match_count, idx);
                self.rhs_pointers[base_offset + match_count] = row_ptr as usize;
                match_count += 1;
            }
        }

        match_count
    }

    fn resolve_predicates_into_pending(
        &mut self,
        prepared_keys: Option<&PreparedProbeKeys<'_>>,
        hash_table: &JoinHashTable,
    ) -> usize {
        let mut match_count = 0;
        for active_idx in 0..self.count {
            let probe_idx = self.sel_vector.get(active_idx);
            let row_ptr = self.pointers[probe_idx];
            if row_ptr != 0
                && (self.exact_key_matches
                    || hash_table.key_values_match_build_row(
                        prepared_keys.expect("non-exact join probe must prepare its keys"),
                        probe_idx,
                        row_ptr,
                    ))
            {
                self.chain_match_sel.set(match_count, probe_idx);
                self.pending_rhs_pointers[match_count] = row_ptr;
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
    ) -> Result<usize> {
        self.next_inner_join_impl(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
            |_, _, match_count, output_sel| {
                for i in 0..match_count {
                    output_sel.set(i, i);
                }
                Ok(match_count)
            },
        )
    }

    /// Emit a payload-free inner join directly from an exact unique-key probe.
    ///
    /// The integer index has already compared the complete key and published
    /// the matching probe ordinals in `sel_vector`. With no duplicate build
    /// rows, residual predicate, right-side output, or unmatched-row tracking,
    /// the ordinary inner-join drain would only copy those ordinals through
    /// the pending-match buffers and prepare an empty build dictionary.
    pub fn next_exact_unique_left_only_inner_join(
        &mut self,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        left_projection_map: &[usize],
    ) -> Result<usize> {
        debug_assert!(self.exact_key_matches);
        debug_assert!(!self.has_long_chains);
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        let count = self.count;
        let selection = &self.sel_vector;
        if result.column_count() != left_projection_map.len() {
            return Err(paro_common::error::internal(format!(
                "left-only join output has {} columns but its projection has {} entries",
                result.column_count(),
                left_projection_map.len()
            )));
        }
        let selection_is_identity = count == left.size()
            && selection
                .as_slice()
                .iter()
                .enumerate()
                .all(|(row_idx, &selected)| selected as usize == row_idx);
        let result_column_count = result.column_count();
        for (output_idx, &left_idx) in left_projection_map.iter().enumerate() {
            let left_column = left.data.get(left_idx).ok_or_else(|| {
                paro_common::error::internal(format!(
                    "join left projection index {left_idx} is out of range for {} columns",
                    left.column_count()
                ))
            })?;
            let output = result.data.get_mut(output_idx).ok_or_else(|| {
                paro_common::error::internal(format!(
                    "join left output index {output_idx} is out of range for {} columns",
                    result_column_count
                ))
            })?;
            *output = if selection_is_identity {
                Arc::clone(left_column)
            } else {
                Arc::new(Vector::try_dictionary(
                    Arc::clone(left_column),
                    selection.clone(),
                )?)
            };
        }
        result.try_set_cardinality(count)?;
        self.count = 0;
        self.finished = true;
        Ok(count)
    }

    /// Scan results for an inner join with an additional residual filter.
    pub fn next_inner_join_with_filter<F>(
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
        self.next_inner_join_impl(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
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
    ) -> Result<usize> {
        self.next_left_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
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
        mut residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        let prepared_keys = self.prepare_probe_keys(keys, hash_table)?;
        let mut base_count = 0;

        while self.count > 0 && base_count < VECTOR_SIZE {
            if base_count > 0 && base_count + self.count > VECTOR_SIZE {
                break;
            }

            let rhs_offset = base_count;
            let match_count =
                self.resolve_predicates(prepared_keys.as_ref(), hash_table, rhs_offset);
            if match_count > 0 {
                self.scratch_sel.set_len(match_count);
                let accepted_count = residual_filter(
                    &self.chain_match_sel,
                    &self.rhs_pointers[rhs_offset..rhs_offset + match_count],
                    match_count,
                    &mut self.scratch_sel,
                )?;

                for i in 0..accepted_count {
                    let match_idx = self.scratch_sel.get(i);
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
            self.gather_result(left, result, base_count, hash_table, left_projection_map)?;
            return Ok(base_count);
        }

        self.scratch_sel.set_len(left.size());
        let mut unmatched_count = 0usize;
        let unmatched_rows = self.scratch_sel.as_mut_slice();
        for idx in 0..left.size() {
            if !self.found_match[idx] {
                unmatched_rows[unmatched_count] = idx as u32;
                unmatched_count += 1;
            }
        }
        self.scratch_sel.set_len(unmatched_count);

        if unmatched_count == 0 {
            result.set_cardinality(0);
        } else {
            construct_left_outer_result(
                left,
                &self.scratch_sel,
                unmatched_count,
                left_projection_map,
                hash_table.build_output_types(),
                result,
            )?;
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

        construct_mark_join_result(left, left_projection_map, &markers, result)?;
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
    ) -> Result<usize> {
        self.next_single_join_with_filter(
            keys,
            left,
            result,
            hash_table,
            left_projection_map,
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
        mut residual_filter: F,
    ) -> Result<usize>
    where
        F: FnMut(&SelectionVector, &[usize], usize, &mut SelectionVector) -> Result<usize>,
    {
        if self.finished {
            result.set_cardinality(0);
            return Ok(0);
        }

        if !self.probe_matches_ready {
            let prepared_keys = self.prepare_probe_keys(keys, hash_table)?;
            self.single_match_pointers[..left.size()].fill(0);
            while self.count > 0 {
                let match_count = self.resolve_predicates(prepared_keys.as_ref(), hash_table, 0);
                self.scratch_sel.set_len(match_count);
                let accepted_count = residual_filter(
                    &self.chain_match_sel,
                    &self.rhs_pointers[..match_count],
                    match_count,
                    &mut self.scratch_sel,
                )?;

                for i in 0..accepted_count {
                    let match_idx = self.scratch_sel.get(i);
                    let lhs_idx = self.chain_match_sel.get(match_idx);
                    if self.single_match_pointers[lhs_idx] != 0 {
                        return Err(paro_common::error::invalid_input(
                            "More than one row returned by a SINGLE join",
                        ));
                    }
                    self.found_match[lhs_idx] = true;
                    self.single_match_pointers[lhs_idx] = self.rhs_pointers[match_idx];
                }

                self.advance_pointers();
            }
            self.probe_matches_ready = true;
        }

        let right_offset = left_projection_map.len();
        result.try_reset_writable_suffix(right_offset, result.allocator().clone())?;
        let output_capacity = result.capacity();
        if output_capacity == 0 {
            return Err(paro_common::error::internal(
                "single join output requires non-zero capacity",
            ));
        }
        let emit_count = (left.size() - self.probe_output_offset).min(output_capacity);
        self.scratch_sel.try_make_exclusive()?;
        self.scratch_sel.set_len(emit_count);
        for output_idx in 0..emit_count {
            self.scratch_sel
                .set(output_idx, self.probe_output_offset + output_idx);
        }
        let left_sel = self.scratch_sel.clone();
        for (out_idx, left_idx) in left_projection_map.iter().enumerate() {
            let left_column = left.data.get(*left_idx).ok_or_else(|| {
                paro_common::error::internal(format!(
                    "single join left projection index {left_idx} is out of range for {} columns",
                    left.column_count()
                ))
            })?;
            result.data[out_idx] = Arc::new(Vector::try_dictionary(
                Arc::clone(left_column),
                left_sel.clone(),
            )?);
        }

        for build_idx in 0..hash_table.build_output_count() {
            let vector = result.column_mut(right_offset + build_idx).ok_or_else(|| {
                paro_common::error::internal(format!(
                    "single join output column {} is missing",
                    right_offset + build_idx
                ))
            })?;
            let start = self.probe_output_offset;
            let row_ptrs = &self.single_match_pointers[start..start + emit_count];
            // SAFETY: match pointers are either zero for an unmatched probe row
            // or were obtained from `hash_table` while probing it.
            unsafe { hash_table.gather_build_column(row_ptrs, build_idx, vector)? };
        }

        result.set_cardinality(emit_count);
        self.probe_output_offset += emit_count;
        self.finished = self.probe_output_offset == left.size();
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
        &mut self,
        left: &paro_common::chunk::Chunk,
        result: &mut paro_common::chunk::Chunk,
        count: usize,
        hash_table: &JoinHashTable,
        left_projection_map: &[usize],
    ) -> Result<()> {
        let expected_column_count = left_projection_map.len() + hash_table.build_output_count();
        let left_layout_matches =
            left_projection_map
                .iter()
                .enumerate()
                .all(|(out_idx, &left_idx)| {
                    left.data
                        .get(left_idx)
                        .zip(result.data.get(out_idx))
                        .is_some_and(|(left_column, result_column)| {
                            left_column.logical_type() == result_column.logical_type()
                        })
                });
        let right_layout_matches =
            hash_table
                .build_output_types()
                .iter()
                .enumerate()
                .all(|(build_idx, build_type)| {
                    result
                        .data
                        .get(left_projection_map.len() + build_idx)
                        .is_some_and(|column| column.logical_type() == build_type)
                });
        if result.column_count() != expected_column_count
            || result.capacity() < count
            || !left_layout_matches
            || !right_layout_matches
        {
            let mut expected_types = Vec::with_capacity(expected_column_count);
            for &left_idx in left_projection_map {
                let left_column = left.data.get(left_idx).ok_or_else(|| {
                    paro_common::error::internal(format!(
                        "join left projection index {left_idx} is out of range for {} columns",
                        left.column_count()
                    ))
                })?;
                expected_types.push(left_column.logical_type().clone());
            }
            expected_types.extend(hash_table.build_output_types().iter().cloned());
            *result = paro_common::chunk::Chunk::try_initialize(
                &expected_types,
                VECTOR_SIZE.max(count),
                result.allocator().clone(),
            )?;
        } else {
            result.try_reset(result.allocator().clone())?;
        }
        result.set_cardinality(count);
        let mut lhs_sel = self.lhs_sel.clone();
        lhs_sel.set_len(count);
        let lhs_is_identity = count == left.size()
            && lhs_sel
                .as_slice()
                .iter()
                .take(count)
                .enumerate()
                .all(|(row_idx, &selected)| selected as usize == row_idx);

        // 1. Copy projected LHS columns
        for (out_idx, left_idx) in left_projection_map.iter().enumerate() {
            let left_column = left.data.get(*left_idx).ok_or_else(|| {
                paro_common::error::internal(format!(
                    "join left projection index {left_idx} is out of range for {} columns",
                    left.column_count()
                ))
            })?;
            result.data[out_idx] = if lhs_is_identity {
                // A total, order-preserving match needs no dictionary wrapper.
                // This is common for foreign-key joins after an exact runtime
                // filter and prevents downstream joins from composing chains
                // of identity selections batch after batch.
                Arc::clone(left_column)
            } else {
                Arc::new(Vector::try_dictionary(
                    Arc::clone(left_column),
                    lhs_sel.clone(),
                )?)
            };
        }

        // 2. Gather projected RHS columns
        let right_result_offset = left_projection_map.len();
        let unique_rhs_count = self.prepare_rhs_dictionary(count)?;
        for (build_idx, build_type) in hash_table.build_output_types().iter().enumerate() {
            let output_idx = right_result_offset + build_idx;
            let use_dictionary = unique_rhs_count < count
                && dictionary_gather_is_smaller(build_type, count, unique_rhs_count);
            let row_ptrs = if use_dictionary {
                &self.rhs_unique_pointers[..unique_rhs_count]
            } else {
                &self.rhs_pointers[..count]
            };
            let output = result.column_mut(output_idx).ok_or_else(|| {
                paro_common::error::internal(format!("join output column {output_idx} is missing"))
            })?;
            // SAFETY: these row pointers were obtained from `hash_table` while
            // resolving the current probe matches.
            unsafe { hash_table.gather_build_column(row_ptrs, build_idx, output)? };
            if use_dictionary {
                let child = Arc::clone(&result.data[output_idx]);
                result.data[output_idx] = Arc::new(Vector::try_dictionary(
                    child,
                    self.rhs_dictionary_sel.clone(),
                )?);
            }
        }
        Ok(())
    }

    fn prepare_rhs_dictionary(&mut self, count: usize) -> Result<usize> {
        self.rhs_unique_pointers.clear();
        self.rhs_dictionary_sel.try_make_exclusive()?;
        self.rhs_dictionary_sel.set_len(count);
        let mut previous = None;
        let mut dictionary_idx = 0usize;
        for row_idx in 0..count {
            let pointer = self.rhs_pointers[row_idx];
            if previous != Some(pointer) {
                self.rhs_unique_pointers.push(pointer);
                previous = Some(pointer);
                dictionary_idx = self.rhs_unique_pointers.len() - 1;
            }
            self.rhs_dictionary_sel.set(row_idx, dictionary_idx);
        }
        Ok(self.rhs_unique_pointers.len())
    }
}

fn collect_existence_output<const MATCH: bool>(
    output: &mut SelectionVector,
    found_match: &[bool],
    candidate_offset: &mut usize,
    candidates: Option<&SelectionVector>,
    all_row_count: usize,
    output_capacity: usize,
) -> Result<(usize, bool)> {
    if output_capacity == 0 {
        return Err(paro_common::error::internal(
            "existence join requires a non-zero output capacity",
        ));
    }
    let candidate_count = candidates.map_or(all_row_count, SelectionVector::len);
    output.set_len(output_capacity.min(candidate_count));
    let selected_rows = output.as_mut_slice();
    let mut selected_count = 0usize;
    let mut cursor = *candidate_offset;
    while cursor < candidate_count && selected_count < output_capacity {
        let row_idx = candidates.map_or(cursor, |selection| selection.get(cursor));
        if found_match[row_idx] == MATCH {
            selected_rows[selected_count] = row_idx as u32;
            selected_count += 1;
        }
        cursor += 1;
    }
    *candidate_offset = cursor;
    output.set_len(selected_count);
    Ok((selected_count, cursor == candidate_count))
}

fn dictionary_gather_is_smaller(
    logical_type: &LogicalType,
    row_count: usize,
    unique_count: usize,
) -> bool {
    let value_width = logical_type.type_size();
    let flat_bytes = row_count.saturating_mul(value_width);
    let dictionary_bytes = unique_count
        .saturating_mul(value_width)
        .saturating_add(row_count.saturating_mul(std::mem::size_of::<u32>()));
    dictionary_bytes < flat_bytes
}

#[cfg(test)]
#[path = "scan_structure_tests.rs"]
mod tests;
