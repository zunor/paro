use super::table_handle::TableHandle;
use crate::compaction::compaction_manager::allocate_compaction_job_id;
use crate::compaction::execution::job_orchestrator::run_job;
use crate::compaction::plan::CompactionPlanner;
use crate::table::storage_descriptor::TableStorageDescriptor;
use paro_common::allocator::default_allocator;
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;
use std::time::Duration;

const TABLE_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

impl TableHandle {
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
        self.tablet().repair_primary_index_after_replay()?;
        Ok(())
    }

    /// Replay persisted row-id deletes against the underlying tablet.
    pub fn replay_row_id_delete(&self, locations: &[(u64, u32, u32)]) -> Result<()> {
        self.tablet().apply_row_id_delete_locations(locations)?;
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
