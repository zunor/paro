// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::compaction::compaction_manager::allocate_compaction_job_id;
use crate::compaction::execution::job_orchestrator::run_job;
use crate::compaction::plan::CompactionPlanner;
use crate::compaction::publish::record::CompactionPublishRecord;
use crate::table::index_runtime::IndexRuntime;
use crate::table::storage_descriptor::TableStorageDescriptor;
use paro_common::allocator::default_allocator;
use paro_common::effect::TabletMutation;
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;
use std::time::Duration;

const TABLE_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

impl TableHandle {
    fn restore_declared_runtime_indexes_for_rowset(&self, rowset_id: u64) -> Result<()> {
        let Some(rowset) = self.tablet().find_rowset_by_id(rowset_id) else {
            return Ok(());
        };

        let fulltext_columns = self.declared_fulltext_columns_with_config();
        if !fulltext_columns.is_empty() {
            IndexRuntime::build_runtime_fulltext_indexes_for_rowset(&rowset, &fulltext_columns)?;
        }

        let art_columns = self.declared_art_columns();
        if !art_columns.is_empty() {
            if let Err(err) =
                IndexRuntime::build_runtime_art_indexes_for_rowset(&rowset, &art_columns)
            {
                tracing::warn!(
                    error = %err,
                    rowset_id,
                    "ART index backfill failed for replayed rowset; queries will fallback to scan"
                );
            }
        }

        Ok(())
    }

    /// Build a stable storage descriptor for catalog persistence.
    pub fn to_descriptor(&self) -> Result<TableStorageDescriptor> {
        let schema = self
            .tablet()
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema missing"))?;

        TableStorageDescriptor::from_keys_type(
            self.tablet().tablet_id(),
            self.tablet().table_id(),
            self.tablet().partition_id(),
            schema.schema_id(),
            schema.schema_version(),
            self.tablet().schema_hash(),
            self.tablet().data_dir().to_string_lossy().into_owned(),
            schema.keys_type(),
        )
    }

    /// Mark the underlying tablet as shutdown and enqueue drop cleanup.
    pub fn mark_shutdown_and_schedule_sweep(&self, move_to_trash: bool) -> Result<()> {
        if let Some(manager) = self.bound_compaction_manager() {
            manager.drain_tablet(
                self.tablet().tablet_id(),
                "table shutdown sweep",
                TABLE_SHUTDOWN_DRAIN_TIMEOUT,
            )?;
            self.tablet()
                .mark_shutdown_and_schedule_sweep(move_to_trash)?;
            manager.unregister_tablet(self.tablet().tablet_id())?;
            return Ok(());
        }
        self.tablet()
            .mark_shutdown_and_schedule_sweep(move_to_trash)
    }

    /// Replay a rowset publish entry against the underlying tablet.
    pub fn replay_rowset_commit(
        &self,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        self.tablet()
            .replay_rowset_commit(rowset_id, start_version, end_version, rowset_path)?;
        self.restore_declared_runtime_indexes_for_rowset(rowset_id)?;
        self.tablet().repair_primary_index_after_replay()?;
        Ok(())
    }

    /// Replay persisted row-id deletes against the underlying tablet.
    pub fn replay_row_id_delete(&self, locations: &[(u64, u32, u32)]) -> Result<()> {
        self.replay_row_id_delete_at_version(locations, self.tablet().max_version())
    }

    pub fn replay_row_id_delete_at_version(
        &self,
        locations: &[(u64, u32, u32)],
        delete_version: i64,
    ) -> Result<()> {
        self.tablet()
            .apply_row_id_delete_locations_idempotent_at_version(locations, delete_version)?;
        self.tablet().repair_primary_index_after_replay()?;
        Ok(())
    }

    pub fn replay_primary_delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        self.tablet()
            .replay_primary_delete_idempotent(keys.to_vec())?;
        self.tablet().repair_primary_index_after_replay()?;
        Ok(())
    }

    pub fn replay_compaction_publish(&self, record: &CompactionPublishRecord) -> Result<()> {
        self.tablet().replay_compaction_publish(record)?;
        self.restore_declared_runtime_indexes_for_rowset(record.output_rowset_id)?;
        self.tablet().repair_primary_index_after_replay()?;
        Ok(())
    }

    pub fn apply_compaction_publish(&self, op: &TabletMutation) -> Result<()> {
        self.tablet().apply_compaction_publish(op)?;
        if let TabletMutation::PublishCompaction {
            output_rowset_id, ..
        } = op
        {
            self.restore_declared_runtime_indexes_for_rowset(*output_rowset_id)?;
        }
        self.tablet().repair_primary_index_after_replay()?;
        Ok(())
    }

    /// Replay table-level DELETE row IDs from WAL.
    pub fn replay_delete_rows(&self, _row_ids: &[u64]) -> Result<usize> {
        Err(paro_error::not_supported(
            "WAL table-level DELETE replay without tablet row locations",
        ))
    }

    /// Run a synchronous compaction over the currently visible rowsets.
    ///
    /// Returns `true` when a compacted output rowset was produced and published.
    pub fn optimize_compact(&self) -> Result<bool> {
        let Some(plan) = CompactionPlanner::plan(&self.tablet())? else {
            return Ok(false);
        };
        run_job(
            &self.tablet(),
            Arc::new(plan),
            allocate_compaction_job_id(),
            Arc::new(default_allocator()),
        )
    }

    /// Validate that the committed rowset version graph is internally legal.
    pub fn validate_version_graph(&self) -> Result<()> {
        self.tablet().validate_version_graph()
    }

    /// Reconcile primary-index cardinality with rowset effective rows.
    pub fn reconcile_primary_index_row_count(&self) -> Result<()> {
        self.tablet().reconcile_primary_index_row_count()
    }
}
