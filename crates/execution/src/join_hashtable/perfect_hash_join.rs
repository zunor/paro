// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::join_hashtable::join_hashtable::JoinHashTable;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PerfectHashJoinStats {
    pub build_min: Value,
    pub build_max: Value,
    pub is_build_small: bool,
    pub is_build_dense: bool,
    pub build_range: u64,
}

impl Default for PerfectHashJoinStats {
    fn default() -> Self {
        Self {
            build_min: Value::Null(LogicalType::Integer),
            build_max: Value::Null(LogicalType::Integer),
            is_build_small: false,
            is_build_dense: false,
            build_range: 0,
        }
    }
}

pub struct PerfectHashJoinExecutor {
    pub stats: PerfectHashJoinStats,
    /// Build side data stored in columnar format, indexed by (key - min)
    pub build_data: Option<Chunk>,
    /// Bitmap indicating which entries in build_data are valid
    pub build_validity: Vec<bool>,
}

impl PerfectHashJoinExecutor {
    pub fn new() -> Self {
        Self {
            stats: PerfectHashJoinStats::default(),
            build_data: None,
            build_validity: Vec::new(),
        }
    }

    pub fn can_do_perfect_hash_join(
        &mut self,
        join_type: JoinType,
        conditions: &[JoinCondition],
        ht: &JoinHashTable,
        min: Value,
        max: Value,
    ) -> bool {
        // Only inner joins with single equality condition
        if join_type != JoinType::Inner
            || conditions.len() != 1
            || conditions[0].comparison != JoinComparisonType::Equal
        {
            return false;
        }

        let key_type = conditions[0].left.return_type();
        if !key_type.is_integral() {
            return false;
        }

        if min.is_null() || max.is_null() {
            return false;
        }

        // Calculate range
        let min_val = self.extract_i128(&min);
        let max_val = self.extract_i128(&max);

        if max_val < min_val {
            return false; // Empty or invalid
        }

        let range = (max_val - min_val) as u64;

        const MAX_BUILD_SIZE: u64 = 1048576;
        if range > MAX_BUILD_SIZE {
            return false;
        }

        // Check if HT count is not excessively large compared to range (too many duplicates)
        if ht.count() as u64 > range + 1 {
            return false;
        }

        self.stats.build_min = min;
        self.stats.build_max = max;
        self.stats.build_range = range;
        self.stats.is_build_small = true;

        true
    }

    fn extract_i128(&self, val: &Value) -> i128 {
        match val {
            Value::TinyInt(v) => *v as i128,
            Value::SmallInt(v) => *v as i128,
            Value::Integer(v) => *v as i128,
            Value::BigInt(v) => *v as i128,
            Value::HugeInt(v) => *v,
            Value::UTinyInt(v) => *v as i128,
            Value::USmallInt(v) => *v as i128,
            Value::UInteger(v) => *v as i128,
            Value::UBigInt(v) => *v as i128,
            Value::UHugeInt(v) => *v as i128,
            _ => 0,
        }
    }

    /// Gather min/max statistics for the first equality key from the JoinHashTable.
    pub fn gather_statistics(&self, ht: &JoinHashTable) -> Result<(Option<Value>, Option<Value>)> {
        let mut min_val: Option<Value> = None;
        let mut max_val: Option<Value> = None;

        for row_ptr in ht.all_build_row_ptrs() {
            let val = ht.read_equality_value(row_ptr, 0);
            if val.is_null() {
                continue;
            }

            if min_val.is_none() || val < *min_val.as_ref().unwrap() {
                min_val = Some(val.clone());
            }
            if max_val.is_none() || val > *max_val.as_ref().unwrap() {
                max_val = Some(val.clone());
            }
        }

        Ok((min_val, max_val))
    }

    /// Build the columnar perfect hash table by scanning the JoinHashTable.
    pub fn build_perfect_hash_table(&mut self, ht: &JoinHashTable) -> Result<()> {
        let build_size = (self.stats.build_range + 1) as usize;

        // 1. Initialize result chunk and validity
        let build_types = &ht.build_types;
        let mut build_data =
            Chunk::try_initialize(build_types, build_size, ht.allocator().clone())?;
        build_data.set_cardinality(build_size);

        let mut build_validity = vec![false; build_size];
        let mut unique_keys = 0;
        let min_val_i128 = self.extract_i128(&self.stats.build_min);

        for row_ptr in ht.all_build_row_ptrs() {
            let key_val = ht.read_equality_value(row_ptr, 0);

            if key_val.is_null() {
                continue;
            }

            let key_i128 = self.extract_i128(&key_val);
            let idx = (key_i128 - min_val_i128) as usize;

            if idx >= build_size {
                continue;
            }

            if build_validity[idx] {
                return Err(paro_common::error::internal(
                    "Perfect Hash Join does not support duplicates in build side".to_string(),
                ));
            }

            build_validity[idx] = true;
            unique_keys += 1;

            for (i, _build_type) in build_types.iter().enumerate() {
                let val = ht.read_build_value(row_ptr, i);
                build_data.column_mut(i).unwrap().set_value(idx, &val);
            }
        }

        if unique_keys == build_size && !ht.has_null.load(std::sync::atomic::Ordering::Relaxed) {
            self.stats.is_build_dense = true;
        }

        self.build_data = Some(build_data);
        self.build_validity = build_validity;

        Ok(())
    }

    /// Probe the perfect hash table.
    pub fn probe(
        &self,
        keys: &Chunk,
        lhs_output: &Chunk,
        result: &mut Chunk,
        sel: Option<&SelectionVector>,
    ) -> Result<()> {
        let count = keys.size();
        if count == 0 {
            result.set_cardinality(0);
            return Ok(());
        }

        let mut build_sel = SelectionVector::try_incremental(count, result.allocator().clone())?;
        let mut probe_sel = SelectionVector::try_incremental(count, result.allocator().clone())?;
        let mut match_count = 0;

        let key_vector = &keys.data[0];
        let min_val_i128 = self.extract_i128(&self.stats.build_min);
        let max_val_i128 = self.extract_i128(&self.stats.build_max);
        let build_size = self.build_validity.len();

        let row_count = sel.map(|s| s.len()).unwrap_or(count);
        for i in 0..row_count {
            let actual_idx = sel.map(|s| s.get(i)).unwrap_or(i);
            let val = key_vector.get_value(actual_idx);
            if val.is_null() {
                continue;
            }

            let key_i128 = self.extract_i128(&val);
            if key_i128 >= min_val_i128 && key_i128 <= max_val_i128 {
                let idx = (key_i128 - min_val_i128) as usize;
                if idx < build_size && self.build_validity[idx] {
                    build_sel.set(match_count, idx);
                    probe_sel.set(match_count, actual_idx);
                    match_count += 1;
                }
            }
        }

        if match_count > 0 {
            build_sel.set_len(match_count);
            probe_sel.set_len(match_count);

            // 1. Slice LHS output
            result.set_cardinality(match_count);
            let left_col_count = lhs_output.column_count();
            for i in 0..left_col_count {
                result.data[i] = Arc::new(Vector::try_dictionary(
                    lhs_output.data[i].clone(),
                    probe_sel.clone(),
                )?);
            }

            // 2. Dictionary RHS output
            if let Some(build_data) = &self.build_data {
                for i in 0..build_data.column_count() {
                    result.data[left_col_count + i] = Arc::new(Vector::try_dictionary(
                        build_data.data[i].clone(),
                        build_sel.clone(),
                    )?);
                }
            }
        } else {
            result.set_cardinality(0);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perfect_hash_join_stats_extraction() {
        let perfect = PerfectHashJoinExecutor::new();
        assert_eq!(perfect.extract_i128(&Value::Integer(42)), 42);
        assert_eq!(perfect.extract_i128(&Value::BigInt(1000)), 1000);
        assert_eq!(perfect.extract_i128(&Value::HugeInt(123456789)), 123456789);
    }
}
