// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Finalization and probe publication for the exact two-BIGINT join index.

use paro_common::error::{ErrorClass, Result};
use paro_common::types::LogicalType;
use paro_planner::operator::join::JoinComparisonType;

use super::super::pair_integer_index::ExactI64PairJoinIndex;
use super::JoinHashTable;

impl JoinHashTable {
    pub(super) fn try_build_pair_integer_index(&self) -> Result<bool> {
        if self.equality_types.as_slice() != [LogicalType::BigInt, LogicalType::BigInt]
            || self.equality_comparisons.as_slice()
                != [JoinComparisonType::Equal, JoinComparisonType::Equal]
        {
            return Ok(false);
        }
        let mut index = match ExactI64PairJoinIndex::try_new(
            self.count(),
            self.allocator.clone(),
            &self.pointer_memory,
        ) {
            Ok(Some(index)) => index,
            Ok(None) => return Ok(false),
            Err(error) if error.error_class() == ErrorClass::Resource => return Ok(false),
            Err(error) => return Err(error),
        };

        let store = self.build_store.lock().unwrap();
        let mut has_long_chains = false;
        for block in store.block_ranges() {
            for row_idx in 0..block.row_count() {
                let row_ptr = unsafe { block.row_ptr(row_idx) };
                if let Some(previous) = index.insert(self.build_row_layout.base(), row_ptr)? {
                    self.build_row_layout
                        .set_next(row_ptr as *mut u8, previous as *const u8);
                    has_long_chains = true;
                }
            }
        }
        drop(store);

        self.chains_longer_than_one
            .store(has_long_chains, std::sync::atomic::Ordering::Relaxed);
        let index = Box::new(index);
        let index_ptr = std::ptr::from_ref(index.as_ref()) as *mut ExactI64PairJoinIndex;
        *self.pair_integer_index.lock().unwrap() = Some(index);
        self.probe_pair_integer_index
            .store(index_ptr, std::sync::atomic::Ordering::Release);
        self.finalize_grouped_reduction_extrema()?;
        self.finalized
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(true)
    }

    pub(super) fn probe_pair_integer_index(
        &self,
        keys: &paro_common::chunk::Chunk,
        scan_structure: &mut super::super::scan_structure::ScanStructure,
        filtered_count: usize,
    ) -> Result<bool> {
        let index = self
            .probe_pair_integer_index
            .load(std::sync::atomic::Ordering::Acquire);
        if index.is_null() {
            return Ok(false);
        }
        let left = keys.column(0).expect("pair join key is missing column 0");
        let right = keys.column(1).expect("pair join key is missing column 1");
        let prepared_rows = scan_structure.probe_sel.as_slice();
        scan_structure.sel_vector.set_len(keys.size());
        let matched_rows = scan_structure.sel_vector.as_mut_slice();
        let matched_count = unsafe { &*index }.lookup_vector_rows(
            left,
            right,
            keys.size(),
            &prepared_rows[..filtered_count],
            &mut scan_structure.pointers,
            matched_rows,
            self.build_row_layout.base(),
        )?;
        scan_structure.count = matched_count;
        scan_structure.sel_vector.set_len(matched_count);
        scan_structure.exact_key_matches = true;
        Ok(true)
    }

    pub(super) fn pair_integer_index_size(&self) -> usize {
        self.pair_integer_index
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |index| index.size_in_bytes())
    }
}
