// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! TableHandle - adapter between Catalog and Tablet storage.
//!
//! Route table operations to Tablet/Rowset pipeline.

use crate::compaction::compaction_manager::CompactionManager;
use crate::table::index_runtime::IndexRuntime;
use crate::tablet::{Tablet, TabletRef};
use paro_common::types::LogicalType;
use std::sync::{Arc, Weak};

#[derive(Debug, Clone)]
pub enum InsertOnConflictAction {
    DoNothing,
    DoUpdate {
        target_columns: Vec<usize>,
        source_columns: Vec<usize>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullTextIndexCoverage {
    pub visible_version: i64,
    pub visible_segment_count: usize,
    pub indexed_segment_count: usize,
}

impl FullTextIndexCoverage {
    pub fn is_complete(&self) -> bool {
        self.visible_segment_count == self.indexed_segment_count
    }
}

/// Adapter layer visible to Catalog; internally owns a Tablet.
pub struct TableHandle {
    runtime_tablet: TabletRef,
    column_types: Vec<LogicalType>,
    pub(crate) index_runtime: IndexRuntime,
    compaction_manager: std::sync::RwLock<Option<Weak<CompactionManager>>>,
}

/// Column specification used when building a TabletSchema from catalog metadata.
#[derive(Debug, Clone)]
pub struct TableColumnSpec {
    pub name: String,
    pub logical_type: LogicalType,
    pub is_key: bool,
    pub not_null: bool,
}

impl TableHandle {
    pub(crate) fn from_runtime_tablet(tablet: Tablet, column_types: Vec<LogicalType>) -> Self {
        Self {
            runtime_tablet: Arc::new(tablet),
            column_types,
            index_runtime: IndexRuntime::new(),
            compaction_manager: std::sync::RwLock::new(None),
        }
    }

    pub fn bind_compaction_manager(&self, manager: &Arc<CompactionManager>) {
        *self.compaction_manager.write().unwrap() = Some(Arc::downgrade(manager));
    }

    pub fn bound_compaction_manager(&self) -> Option<Arc<CompactionManager>> {
        self.compaction_manager
            .read()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
    }

    /// Get column types.
    pub fn types(&self) -> &[LogicalType] {
        &self.column_types
    }

    /// Maximum committed version visible in the underlying tablet.
    pub fn max_version(&self) -> i64 {
        self.runtime_tablet.max_version()
    }

    /// Tablet identifier backing this table.
    pub fn tablet_id(&self) -> u64 {
        self.runtime_tablet.tablet_id()
    }

    /// Clone the runtime tablet handle backing this table.
    pub fn tablet(&self) -> TabletRef {
        Arc::clone(&self.runtime_tablet)
    }
}

impl std::fmt::Debug for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHandle")
            .field("runtime_tablet", &self.runtime_tablet)
            .field("column_types", &self.column_types)
            .field("index_runtime", &self.index_runtime)
            .field(
                "has_bound_compaction_manager",
                &self.bound_compaction_manager().is_some(),
            )
            .finish()
    }
}

#[cfg(test)]
#[path = "table_handle_tests.rs"]
mod tests;
