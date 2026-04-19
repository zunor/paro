// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Table Statistics
//!
//! Table-level statistics management for all columns.
//!
//! ## Design Notes
//! TableStatistics manages statistics for all columns in a table.
//! It provides thread-safe access via a shared lock and supports:
//! - Initialization from types or existing statistics
//! - Merging statistics from multiple sources
//! - Column addition/removal for schema changes
//! - Serialization for persistence
//!
//! Note: Table sampling (BlockingSample) is not implemented in this MVP.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex, MutexGuard};

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::base_statistics::BaseStatistics;
use super::column_statistics::ColumnStatistics;

/// Lock guard for table statistics access.
///
/// This ensures exclusive access to table statistics during operations
/// that require consistency across multiple columns.
pub struct TableStatisticsLock<'a> {
    #[allow(dead_code)]
    guard: MutexGuard<'a, ()>,
}

impl<'a> TableStatisticsLock<'a> {
    /// Create a new lock from a mutex guard.
    fn new(guard: MutexGuard<'a, ()>) -> Self {
        Self { guard }
    }
}

/// Table-level statistics for all columns.
///
/// Manages statistics for each column in a table, providing thread-safe
/// access and operations for merging, copying, and schema changes.
#[derive(Debug)]
pub struct TableStatistics {
    /// Lock for thread-safe access
    stats_lock: Arc<Mutex<()>>,
    /// Statistics for each column
    column_stats: Vec<Arc<ColumnStatistics>>,
}

impl Clone for TableStatistics {
    fn clone(&self) -> Self {
        let _lock = self.stats_lock.lock().unwrap();
        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats: self.column_stats.clone(),
        }
    }
}

impl Default for TableStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl TableStatistics {
    /// Create a new empty TableStatistics.
    pub fn new() -> Self {
        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats: Vec::new(),
        }
    }

    /// Initialize empty statistics for the given column types.
    ///
    /// # Arguments
    /// * `types` - The logical types of each column
    pub fn initialize_empty(types: &[LogicalType]) -> Self {
        let column_stats = types
            .iter()
            .map(|ty| ColumnStatistics::create_empty(ty.clone()))
            .collect();

        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        }
    }

    pub fn from_column_statistics(column_stats: Vec<Arc<ColumnStatistics>>) -> Self {
        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        }
    }

    /// Initialize from another TableStatistics, creating empty stats with same structure.
    ///
    /// This copies the column types and distinct statistics configuration,
    /// but creates empty base statistics.
    pub fn initialize_empty_from(other: &TableStatistics) -> Self {
        let _lock = other.stats_lock.lock().unwrap();

        let column_stats = other
            .column_stats
            .iter()
            .map(|stats| {
                let ty = stats.get_type().clone();
                let mut new_stats = ColumnStatistics::new(BaseStatistics::create_empty(ty));

                // Copy distinct stats configuration if present
                if stats.has_distinct_stats() {
                    if let Some(distinct) = stats.distinct_stats() {
                        new_stats.set_distinct(Some(distinct.copy()));
                    }
                }

                Arc::new(new_stats)
            })
            .collect();

        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        }
    }

    /// Initialize for adding a new column.
    ///
    /// Copies all existing column statistics and adds a new empty column.
    ///
    /// # Arguments
    /// * `parent` - The parent table statistics
    /// * `new_column_type` - The type of the new column
    pub fn initialize_add_column(parent: &TableStatistics, new_column_type: LogicalType) -> Self {
        let _lock = parent.stats_lock.lock().unwrap();

        let mut column_stats = parent.column_stats.clone();
        column_stats.push(ColumnStatistics::create_empty(new_column_type));

        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        }
    }

    /// Initialize for removing a column.
    ///
    /// Copies all column statistics except the removed one.
    ///
    /// # Arguments
    /// * `parent` - The parent table statistics
    /// * `removed_column` - The index of the column to remove
    pub fn initialize_remove_column(parent: &TableStatistics, removed_column: usize) -> Self {
        let _lock = parent.stats_lock.lock().unwrap();

        let column_stats = parent
            .column_stats
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != removed_column)
            .map(|(_, stats)| stats.clone())
            .collect();

        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        }
    }

    /// Initialize for altering a column type.
    ///
    /// Copies all column statistics, replacing the altered column with empty stats.
    ///
    /// # Arguments
    /// * `parent` - The parent table statistics
    /// * `changed_idx` - The index of the column being altered
    /// * `new_type` - The new type for the column
    pub fn initialize_alter_type(
        parent: &TableStatistics,
        changed_idx: usize,
        new_type: LogicalType,
    ) -> Self {
        let _lock = parent.stats_lock.lock().unwrap();

        let column_stats = parent
            .column_stats
            .iter()
            .enumerate()
            .map(|(i, stats)| {
                if i == changed_idx {
                    ColumnStatistics::create_empty(new_type.clone())
                } else {
                    stats.clone()
                }
            })
            .collect();

        Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        }
    }

    /// Merge statistics from another TableStatistics.
    ///
    /// Both tables must have the same number of columns.
    pub fn merge_stats(&mut self, other: &TableStatistics) {
        let _self_lock = self.stats_lock.lock().unwrap();
        let _other_lock = other.stats_lock.lock().unwrap();

        debug_assert_eq!(
            self.column_stats.len(),
            other.column_stats.len(),
            "Column count mismatch in merge_stats"
        );

        for (i, other_stats) in other.column_stats.iter().enumerate() {
            if i < self.column_stats.len() {
                // Clone and merge
                let mut merged = (*self.column_stats[i]).clone();
                merged.merge(other_stats);
                self.column_stats[i] = Arc::new(merged);
            }
        }
    }

    /// Merge base statistics for a specific column.
    ///
    /// # Arguments
    /// * `idx` - The column index
    /// * `stats` - The base statistics to merge
    pub fn merge_stats_column(&mut self, idx: usize, stats: &BaseStatistics) {
        let _lock = self.stats_lock.lock().unwrap();
        if idx < self.column_stats.len() {
            let mut merged = (*self.column_stats[idx]).clone();
            merged.statistics_mut().merge(stats);
            self.column_stats[idx] = Arc::new(merged);
        }
    }

    /// Merge base statistics for a specific column (with lock held).
    ///
    /// # Arguments
    /// * `_lock` - Proof that the lock is held
    /// * `idx` - The column index
    /// * `stats` - The base statistics to merge
    pub fn merge_stats_column_locked(
        &mut self,
        _lock: &TableStatisticsLock,
        idx: usize,
        stats: &BaseStatistics,
    ) {
        if idx < self.column_stats.len() {
            let mut merged = (*self.column_stats[idx]).clone();
            merged.statistics_mut().merge(stats);
            self.column_stats[idx] = Arc::new(merged);
        }
    }

    /// Get a reference to column statistics (requires lock).
    ///
    /// # Arguments
    /// * `_lock` - Proof that the lock is held
    /// * `idx` - The column index
    ///
    /// # Returns
    /// Reference to the column statistics, or None if index is out of bounds.
    pub fn get_stats<'a>(
        &'a self,
        _lock: &'a TableStatisticsLock,
        idx: usize,
    ) -> Option<&'a Arc<ColumnStatistics>> {
        self.column_stats.get(idx)
    }

    /// Copy base statistics for a specific column.
    ///
    /// # Arguments
    /// * `idx` - The column index
    ///
    /// # Returns
    /// A copy of the base statistics, or None if index is out of bounds.
    pub fn copy_stats(&self, idx: usize) -> Option<BaseStatistics> {
        let _lock = self.stats_lock.lock().unwrap();

        self.column_stats.get(idx).map(|stats| {
            let mut result = stats.statistics().copy();
            if stats.has_distinct_stats() {
                if let Some(distinct) = stats.distinct_stats() {
                    result.set_distinct_count(distinct.get_count());
                }
            }
            result
        })
    }

    /// Copy all statistics to another TableStatistics.
    pub fn copy_to(&self, other: &mut TableStatistics) {
        let _self_lock = self.stats_lock.lock().unwrap();

        other.stats_lock = Arc::new(Mutex::new(()));
        other.column_stats = self
            .column_stats
            .iter()
            .map(|stats| Arc::new(stats.copy()))
            .collect();
    }

    /// Copy all statistics to another TableStatistics (with lock held).
    pub fn copy_to_locked(&self, _lock: &TableStatisticsLock, other: &mut TableStatistics) {
        other.stats_lock = Arc::new(Mutex::new(()));
        other.column_stats = self
            .column_stats
            .iter()
            .map(|stats| Arc::new(stats.copy()))
            .collect();
    }

    /// Set statistics from another TableStatistics (moves data).
    pub fn set_stats(&mut self, other: TableStatistics) {
        let _lock = self.stats_lock.lock().unwrap();
        self.column_stats = other.column_stats;
    }

    /// Get a lock for thread-safe access.
    pub fn get_lock(&self) -> TableStatisticsLock<'_> {
        TableStatisticsLock::new(self.stats_lock.lock().unwrap())
    }

    /// Check if the statistics are empty (no columns).
    pub fn is_empty(&self) -> bool {
        self.column_stats.is_empty()
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.column_stats.len()
    }

    /// Serialize the table statistics to a writer.
    pub fn serialize<W: Write>(&self, w: &mut W) -> Result<()> {
        let _lock = self.stats_lock.lock().unwrap();

        // Write column count
        let count = self.column_stats.len() as u32;
        w.write_all(&count.to_le_bytes())?;

        // Write each column's statistics
        for stats in &self.column_stats {
            stats.serialize(w)?;
        }

        Ok(())
    }

    /// Deserialize table statistics from a reader.
    ///
    /// # Arguments
    /// * `r` - The reader
    /// * `types` - The logical types of each column (must match serialized data)
    pub fn deserialize<R: Read>(r: &mut R, types: &[LogicalType]) -> Result<Self> {
        // Read column count
        let mut count_buf = [0u8; 4];
        r.read_exact(&mut count_buf)?;
        let count = u32::from_le_bytes(count_buf) as usize;

        if count != types.len() {
            return Err(paro_error::internal(format!(
                "Column count mismatch: expected {}, got {}",
                types.len(),
                count
            )));
        }

        // Read each column's statistics
        let mut column_stats = Vec::with_capacity(count);
        for ty in types {
            let stats = ColumnStatistics::deserialize(r, ty.clone())?;
            column_stats.push(Arc::new(stats));
        }

        Ok(Self {
            stats_lock: Arc::new(Mutex::new(())),
            column_stats,
        })
    }

    /// Serialize to a byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut buf)?;
        Ok(buf)
    }

    /// Deserialize from a byte slice.
    pub fn from_bytes(bytes: &[u8], types: &[LogicalType]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor, types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;

    #[test]
    fn test_new_empty() {
        let stats = TableStatistics::new();
        assert!(stats.is_empty());
        assert_eq!(stats.column_count(), 0);
    }

    #[test]
    fn test_initialize_empty() {
        let types = vec![
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Double,
        ];
        let stats = TableStatistics::initialize_empty(&types);

        assert!(!stats.is_empty());
        assert_eq!(stats.column_count(), 3);
    }

    #[test]
    fn test_initialize_empty_from() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let original = TableStatistics::initialize_empty(&types);
        let copy = TableStatistics::initialize_empty_from(&original);

        assert_eq!(copy.column_count(), original.column_count());
    }

    #[test]
    fn test_initialize_add_column() {
        let types = vec![LogicalType::Integer];
        let parent = TableStatistics::initialize_empty(&types);

        let new_stats = TableStatistics::initialize_add_column(&parent, LogicalType::Varchar);

        assert_eq!(new_stats.column_count(), 2);
    }

    #[test]
    fn test_initialize_remove_column() {
        let types = vec![
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Double,
        ];
        let parent = TableStatistics::initialize_empty(&types);

        let new_stats = TableStatistics::initialize_remove_column(&parent, 1);

        assert_eq!(new_stats.column_count(), 2);
    }

    #[test]
    fn test_initialize_alter_type() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let parent = TableStatistics::initialize_empty(&types);

        let new_stats = TableStatistics::initialize_alter_type(&parent, 0, LogicalType::BigInt);

        assert_eq!(new_stats.column_count(), 2);

        // Check that the altered column has the new type
        let lock = new_stats.get_lock();
        let col_stats = new_stats.get_stats(&lock, 0).unwrap();
        assert_eq!(col_stats.get_type(), &LogicalType::BigInt);
    }

    #[test]
    fn test_merge_stats() {
        let types = vec![LogicalType::Integer];
        let mut stats1 = TableStatistics::initialize_empty(&types);
        let mut stats2 = TableStatistics::initialize_empty(&types);

        // Update stats in stats2
        {
            let mut base = BaseStatistics::create_empty(LogicalType::Integer);
            base.observe_value(&Value::Integer(42));
            stats2.merge_stats_column(0, &base);
        }

        stats1.merge_stats(&stats2);

        // Check that stats1 now has the merged data
        let copied = stats1.copy_stats(0).unwrap();
        assert!(copied.can_have_null() || copied.max_value().is_some());
    }

    #[test]
    fn test_merge_stats_column() {
        let types = vec![LogicalType::Integer];
        let mut stats = TableStatistics::initialize_empty(&types);

        let mut base = BaseStatistics::create_empty(LogicalType::Integer);
        base.observe_value(&Value::Integer(100));

        stats.merge_stats_column(0, &base);

        let copied = stats.copy_stats(0).unwrap();
        assert_eq!(copied.max_value(), Some(Value::Integer(100)));
    }

    #[test]
    fn test_get_stats_with_lock() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let stats = TableStatistics::initialize_empty(&types);

        let lock = stats.get_lock();

        let col0 = stats.get_stats(&lock, 0);
        assert!(col0.is_some());
        assert_eq!(col0.unwrap().get_type(), &LogicalType::Integer);

        let col1 = stats.get_stats(&lock, 1);
        assert!(col1.is_some());
        assert_eq!(col1.unwrap().get_type(), &LogicalType::Varchar);

        let col2 = stats.get_stats(&lock, 2);
        assert!(col2.is_none());
    }

    #[test]
    fn test_copy_stats() {
        let types = vec![LogicalType::Integer];
        let mut stats = TableStatistics::initialize_empty(&types);

        // Update the stats
        {
            let mut base = BaseStatistics::create_empty(LogicalType::Integer);
            base.observe_value(&Value::Integer(50));
            stats.merge_stats_column(0, &base);
        }

        let copied = stats.copy_stats(0);
        assert!(copied.is_some());

        let copied = copied.unwrap();
        assert_eq!(copied.max_value(), Some(Value::Integer(50)));
    }

    #[test]
    fn test_copy_stats_out_of_bounds() {
        let types = vec![LogicalType::Integer];
        let stats = TableStatistics::initialize_empty(&types);

        let copied = stats.copy_stats(10);
        assert!(copied.is_none());
    }

    #[test]
    fn test_copy_to() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let stats1 = TableStatistics::initialize_empty(&types);
        let mut stats2 = TableStatistics::new();

        stats1.copy_to(&mut stats2);

        assert_eq!(stats2.column_count(), 2);
    }

    #[test]
    fn test_set_stats() {
        let types1 = vec![LogicalType::Integer];
        let types2 = vec![LogicalType::Varchar, LogicalType::Double];

        let mut stats1 = TableStatistics::initialize_empty(&types1);
        let stats2 = TableStatistics::initialize_empty(&types2);

        stats1.set_stats(stats2);

        assert_eq!(stats1.column_count(), 2);
    }

    #[test]
    fn test_clone() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let stats1 = TableStatistics::initialize_empty(&types);
        let stats2 = stats1.clone();

        assert_eq!(stats1.column_count(), stats2.column_count());
    }

    #[test]
    fn test_serialize_deserialize() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let mut stats = TableStatistics::initialize_empty(&types);

        // Update some stats
        {
            let mut base = BaseStatistics::create_empty(LogicalType::Integer);
            base.observe_value(&Value::Integer(42));
            stats.merge_stats_column(0, &base);
        }

        let bytes = stats.to_bytes().expect("Serialization failed");
        let restored = TableStatistics::from_bytes(&bytes, &types).expect("Deserialization failed");

        assert_eq!(restored.column_count(), 2);

        let copied = restored.copy_stats(0).unwrap();
        assert_eq!(copied.max_value(), Some(Value::Integer(42)));
    }

    #[test]
    fn test_serialize_deserialize_empty() {
        let types: Vec<LogicalType> = vec![];
        let stats = TableStatistics::initialize_empty(&types);

        let bytes = stats.to_bytes().expect("Serialization failed");
        let restored = TableStatistics::from_bytes(&bytes, &types).expect("Deserialization failed");

        assert!(restored.is_empty());
    }

    #[test]
    fn test_deserialize_column_mismatch() {
        let types = vec![LogicalType::Integer];
        let stats = TableStatistics::initialize_empty(&types);

        let bytes = stats.to_bytes().expect("Serialization failed");

        // Try to deserialize with wrong number of types
        let wrong_types = vec![LogicalType::Integer, LogicalType::Varchar];
        let result = TableStatistics::from_bytes(&bytes, &wrong_types);

        assert!(result.is_err());
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let types = vec![LogicalType::Integer];
        let stats = Arc::new(TableStatistics::initialize_empty(&types));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let stats_clone = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..10 {
                        let _ = stats_clone.copy_stats(0);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
