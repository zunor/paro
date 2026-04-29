// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! TableHandle - adapter between Catalog and Tablet storage.
//!
//! Route table operations to Tablet/Rowset pipeline.

use crate::compaction::compaction_manager::CompactionManager;
use crate::search::registry::SearchIndexRegistry;
use crate::table::runtime_indexes::RuntimeIndexes;
use crate::tablet::{Tablet, TabletRef};
use paro_common::types::LogicalType;
use paro_journal::wal::write_ahead_log::WriteAheadLog;
use paro_journal::{JournalApplyRuntime, JournalCoordinator};
use std::sync::{Arc, Weak};

#[derive(Debug, Clone)]
pub enum InsertOnConflictAction {
    DoNothing,
    DoUpdate {
        target_columns: Vec<usize>,
        source_columns: Vec<usize>,
    },
}

/// Adapter layer visible to Catalog; internally owns a Tablet.
pub struct TableHandle {
    runtime_tablet: TabletRef,
    column_types: Vec<LogicalType>,
    pub(crate) runtime_indexes: RuntimeIndexes,
    pub(crate) search_registry: SearchIndexRegistry,
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
        let runtime_tablet = Arc::new(tablet);
        Self {
            search_registry: SearchIndexRegistry::new(runtime_tablet.clone()),
            runtime_tablet,
            column_types,
            runtime_indexes: RuntimeIndexes::new(),
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

    /// Storage table identifier backing this handle.
    pub fn table_id(&self) -> u64 {
        self.runtime_tablet.table_id()
    }

    /// Clone the runtime tablet handle backing this table.
    pub fn tablet(&self) -> TabletRef {
        Arc::clone(&self.runtime_tablet)
    }

    pub fn bind_database_wal(&self, wal: Option<Arc<WriteAheadLog>>) {
        self.runtime_tablet.bind_database_wal(wal);
    }

    pub fn bind_journal_coordinator(&self, coordinator: Option<Arc<JournalCoordinator>>) {
        self.runtime_tablet.bind_journal_coordinator(coordinator);
    }

    pub fn bind_journal_apply_runtime(&self, runtime: Option<Arc<JournalApplyRuntime>>) {
        self.runtime_tablet.bind_journal_apply_runtime(runtime);
    }

    #[cfg(test)]
    pub(crate) fn search_registry(&self) -> &SearchIndexRegistry {
        &self.search_registry
    }
}

impl std::fmt::Debug for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHandle")
            .field("runtime_tablet", &self.runtime_tablet)
            .field("column_types", &self.column_types)
            .field("runtime_indexes", &self.runtime_indexes)
            .field("search_registry", &self.search_registry)
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
