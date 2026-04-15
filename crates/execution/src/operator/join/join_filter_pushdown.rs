// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Join Filter Pushdown
//!
//! Filter pushdown for join operations. This allows pushing filters derived from the
//! build side (e.g., min/max, bloom filters) down to the probe side.
//!
//!

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::collections::BloomFilter;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

/// Information about a column that filters are pushed to.
#[derive(Debug, Clone)]
pub struct JoinFilterPushdownColumn {
    /// The index of the join condition this column corresponds to.
    pub filter_idx: usize,
    /// The index of the column in the probe side to apply the filter to.
    pub filter_col_idx: usize,
}

/// A filter that is pushed down to the probe side.
#[derive(Debug, Clone)]
pub struct JoinFilterPushdownFilter {
    /// The index of the join condition.
    pub join_condition_idx: usize,
    /// The column information for the probe side.
    pub probe_column: JoinFilterPushdownColumn,
}

/// Global state for join filter pushdown.
pub struct JoinFilterGlobalState {
    /// Global minimum values for each join key.
    pub min_values: Mutex<Vec<Value>>,
    /// Global maximum values for each join key.
    pub max_values: Mutex<Vec<Value>>,
    /// Global bloom filter (optional).
    pub bloom_filter: Mutex<Option<BloomFilter>>,
    /// Total number of probe-side rows observed by the runtime filter.
    pub observed_probe_rows: AtomicUsize,
    /// Total number of probe-side rows kept after runtime filtering.
    pub kept_probe_rows: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinFilterRuntimeStats {
    pub observed_probe_rows: u64,
    pub kept_probe_rows: u64,
}

impl JoinFilterRuntimeStats {
    pub fn pruned_probe_rows(&self) -> u64 {
        self.observed_probe_rows
            .saturating_sub(self.kept_probe_rows)
    }

    pub fn prune_ratio(&self) -> Option<f64> {
        (self.observed_probe_rows > 0)
            .then_some(self.pruned_probe_rows() as f64 / self.observed_probe_rows as f64)
    }
}

impl JoinFilterGlobalState {
    pub fn new(count: usize, types: &[LogicalType], use_bloom: bool) -> Self {
        let mut min = Vec::with_capacity(count);
        let mut max = Vec::with_capacity(count);
        for t in types {
            min.push(Value::Null(t.clone()));
            max.push(Value::Null(t.clone()));
        }
        Self {
            min_values: Mutex::new(min),
            max_values: Mutex::new(max),
            bloom_filter: Mutex::new(if use_bloom {
                Some(BloomFilter::new(1024, 0.01)) // Default size, should be tuned
            } else {
                None
            }),
            observed_probe_rows: AtomicUsize::new(0),
            kept_probe_rows: AtomicUsize::new(0),
        }
    }

    pub fn runtime_stats(&self) -> Option<JoinFilterRuntimeStats> {
        let observed_probe_rows = self.observed_probe_rows.load(Ordering::Relaxed) as u64;
        if observed_probe_rows == 0 {
            return None;
        }
        Some(JoinFilterRuntimeStats {
            observed_probe_rows,
            kept_probe_rows: self.kept_probe_rows.load(Ordering::Relaxed) as u64,
        })
    }
}

/// Local state for join filter pushdown (thread-local).
pub struct JoinFilterLocalState {
    /// Local minimum values for each join key.
    pub min_values: Vec<Value>,
    /// Local maximum values for each join key.
    pub max_values: Vec<Value>,
    /// Local bloom filter (optional).
    pub bloom_filter: Option<BloomFilter>,
}

impl JoinFilterLocalState {
    pub fn new(count: usize, types: &[LogicalType], use_bloom: bool) -> Self {
        let mut min = Vec::with_capacity(count);
        let mut max = Vec::with_capacity(count);
        for t in types {
            min.push(Value::Null(t.clone()));
            max.push(Value::Null(t.clone()));
        }
        Self {
            min_values: min,
            max_values: max,
            bloom_filter: if use_bloom {
                Some(BloomFilter::new(1024, 0.01))
            } else {
                None
            },
        }
    }
}

/// Main logic for managing join filter pushdown.
pub struct JoinFilterPushdownInfo {
    /// Indices of join conditions that we are pushing filters for.
    pub join_condition: Vec<usize>,
    /// Information about filters to push to the probe side.
    pub probe_info: Vec<JoinFilterPushdownFilter>,
    /// Logical types of the join keys.
    pub condition_types: Vec<LogicalType>,
    /// Whether to use a bloom filter.
    pub use_bloom: bool,
}

impl JoinFilterPushdownInfo {
    pub fn new(
        join_condition: Vec<usize>,
        probe_info: Vec<JoinFilterPushdownFilter>,
        condition_types: Vec<LogicalType>,
        use_bloom: bool,
    ) -> Self {
        Self {
            join_condition,
            probe_info,
            condition_types,
            use_bloom,
        }
    }

    pub fn filter_kind(&self) -> &'static str {
        if self.use_bloom {
            "MIN/MAX + BLOOM"
        } else {
            "MIN/MAX"
        }
    }

    /// Get initial global state.
    pub fn get_global_state(&self) -> JoinFilterGlobalState {
        JoinFilterGlobalState::new(
            self.join_condition.len(),
            &self.condition_types,
            self.use_bloom,
        )
    }

    /// Get initial local state for a thread.
    pub fn get_local_state(&self) -> JoinFilterLocalState {
        JoinFilterLocalState::new(
            self.join_condition.len(),
            &self.condition_types,
            self.use_bloom,
        )
    }

    /// Sink values from a chunk of join keys.
    pub fn sink(&self, key_chunk: &Chunk, lstate: &mut JoinFilterLocalState) {
        for (i, &cond_idx) in self.join_condition.iter().enumerate() {
            let vec = &key_chunk.data[cond_idx];
            for row_idx in 0..key_chunk.size() {
                if vec.is_null(row_idx) {
                    continue;
                }
                let val = vec.get_value(row_idx);

                // Update Min
                if lstate.min_values[i].is_null() || val < lstate.min_values[i] {
                    lstate.min_values[i] = val.clone();
                }
                // Update Max
                if lstate.max_values[i].is_null() || val > lstate.max_values[i] {
                    lstate.max_values[i] = val.clone();
                }

                // Update Bloom (if enabled)
                if let Some(ref mut bloom) = lstate.bloom_filter {
                    bloom.add(&val);
                }
            }
        }
    }

    /// Combine local state into global state.
    pub fn combine(&self, gstate: &JoinFilterGlobalState, lstate: JoinFilterLocalState) {
        let mut gmin = gstate.min_values.lock().unwrap();
        let mut gmax = gstate.max_values.lock().unwrap();

        for (i, lmin) in lstate.min_values.into_iter().enumerate() {
            if !lmin.is_null() {
                if gmin[i].is_null() || lmin < gmin[i] {
                    gmin[i] = lmin;
                }
            }
        }
        for (i, lmax) in lstate.max_values.into_iter().enumerate() {
            if !lmax.is_null() {
                if gmax[i].is_null() || lmax > gmax[i] {
                    gmax[i] = lmax;
                }
            }
        }

        // Bloom filter combine
        if let Some(l_bloom) = lstate.bloom_filter {
            let mut g_bloom_lock = gstate.bloom_filter.lock().unwrap();
            if let Some(ref mut g_bloom) = *g_bloom_lock {
                g_bloom.merge(&l_bloom);
            }
        }
    }

    /// Apply filters to a chunk of probe keys and return a selection vector.
    pub fn apply_filters(
        &self,
        gstate: &JoinFilterGlobalState,
        probe_keys: &Chunk,
        sel: &mut SelectionVector,
    ) -> usize {
        let gmin = gstate.min_values.lock().unwrap();
        let gmax = gstate.max_values.lock().unwrap();
        let gbloom = gstate.bloom_filter.lock().unwrap();

        let mut new_count = 0;
        let count = probe_keys.size();

        for i in 0..count {
            let mut matches = true;
            for filter in &self.probe_info {
                let col_idx = filter.probe_column.filter_col_idx;
                let val = probe_keys.data[col_idx].get_value(i);
                if val.is_null() {
                    matches = false;
                    break;
                }

                let condition_idx = filter.join_condition_idx;
                let min = &gmin[condition_idx];
                let max = &gmax[condition_idx];

                // Range filter
                if !min.is_null() && val < *min {
                    matches = false;
                    break;
                }
                if !max.is_null() && val > *max {
                    matches = false;
                    break;
                }

                // Bloom filter
                if let Some(ref bloom) = *gbloom {
                    if !bloom.contains(&val) {
                        matches = false;
                        break;
                    }
                }
            }
            if matches {
                sel.set(new_count, i);
                new_count += 1;
            }
        }
        gstate
            .observed_probe_rows
            .fetch_add(count, Ordering::Relaxed);
        gstate
            .kept_probe_rows
            .fetch_add(new_count, Ordering::Relaxed);
        new_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    #[test]
    fn test_join_filter_pushdown() {
        let types = vec![LogicalType::Integer];
        let mut key_chunk = Chunk::initialize(&types, 10);
        for i in 0..10 {
            key_chunk
                .column_mut(0)
                .unwrap()
                .set_value(i, &Value::Integer(i as i32 + 10));
        }
        key_chunk.set_cardinality(10);

        let info = JoinFilterPushdownInfo::new(
            vec![0],
            vec![JoinFilterPushdownFilter {
                join_condition_idx: 0,
                probe_column: JoinFilterPushdownColumn {
                    filter_idx: 0,
                    filter_col_idx: 0,
                },
            }],
            types.clone(),
            true, // use bloom
        );

        let gstate = info.get_global_state();
        let mut lstate = info.get_local_state();

        info.sink(&key_chunk, &mut lstate);
        info.combine(&gstate, lstate);

        // Check Min/Max
        {
            let gmin = gstate.min_values.lock().unwrap();
            let gmax = gstate.max_values.lock().unwrap();
            assert_eq!(gmin[0], Value::Integer(10));
            assert_eq!(gmax[0], Value::Integer(19));
        }

        // Check Bloom Filter
        {
            let g_bloom = gstate.bloom_filter.lock().unwrap();
            let bloom = g_bloom.as_ref().unwrap();
            assert!(bloom.contains(&Value::Integer(10)));
            assert!(bloom.contains(&Value::Integer(15)));
            assert!(bloom.contains(&Value::Integer(19)));
            assert!(!bloom.contains(&Value::Integer(5)));
            assert!(!bloom.contains(&Value::Integer(25)));
        }

        // Apply filters
        let mut probe_chunk = Chunk::initialize(&types, 10);
        for i in 0..10 {
            probe_chunk
                .column_mut(0)
                .unwrap()
                .set_value(i, &Value::Integer(i as i32 * 5)); // 0, 5, 10, 15, 20, 25, 30, 35, 40, 45
        }
        probe_chunk.set_cardinality(10);

        let mut sel = SelectionVector::incremental(10);
        let filtered_count = info.apply_filters(&gstate, &probe_chunk, &mut sel);

        // Values matching: 10, 15 (both in range [10, 19] AND in bloom filter likely)
        assert_eq!(filtered_count, 2);
        assert_eq!(sel.get(0), 2); // 10
        assert_eq!(sel.get(1), 3); // 15

        let stats = gstate.runtime_stats().unwrap();
        assert_eq!(stats.observed_probe_rows, 10);
        assert_eq!(stats.kept_probe_rows, 2);
        assert_eq!(stats.pruned_probe_rows(), 8);
        assert_eq!(stats.prune_ratio().unwrap(), 0.8);
    }
}
