// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::table_handle::TableHandle;
use crate::compaction::compaction_manager::allocate_compaction_job_id;
use crate::compaction::execution::job_orchestrator::run_job_with_search_inline_builders;
use crate::compaction::plan::CompactionPlanner;
use crate::compaction::publish::record::CompactionPublishRecord;
use crate::table::runtime_indexes::RuntimeIndexes;
use crate::table::storage_descriptor::TableStorageDescriptor;
use paro_common::allocator::default_allocator;
use paro_common::effect::TabletMutation;
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;
use std::time::Duration;

const TABLE_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const FOREGROUND_OPTIMIZE_DRAIN_TIMEOUT: Duration = Duration::from_secs(10 * 60);

struct ForegroundCompactionRegistration {
    manager: Arc<crate::compaction::compaction_manager::CompactionManager>,
    tablet: Arc<crate::tablet::Tablet>,
}

impl Drop for ForegroundCompactionRegistration {
    fn drop(&mut self) {
        // `drain_tablet` keeps the tablet out of background admission. Restore
        // that registration on every success/error exit from foreground
        // OPTIMIZE so subsequent level-triggered maintenance remains live.
        self.manager.register_tablet(Arc::clone(&self.tablet));
    }
}

impl TableHandle {
    fn restore_runtime_indexes_for_rowset(&self, rowset_id: u64) -> Result<()> {
        let Some(rowset) = self.tablet().find_rowset_by_id(rowset_id) else {
            return Ok(());
        };

        let art_columns = self.declared_art_columns();
        if !art_columns.is_empty() {
            if let Err(err) = RuntimeIndexes::rebuild_art_indexes_for_rowset(&rowset, &art_columns)
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
        self.restore_runtime_indexes_for_rowset(rowset_id)?;
        self.tablet()
            .apply_replayed_rowset_to_primary_index(rowset_id)?;
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
        Ok(())
    }

    pub fn replay_primary_delete(&self, keys: &[Vec<u8>]) -> Result<()> {
        self.tablet()
            .replay_primary_delete_idempotent(keys.to_vec())?;
        Ok(())
    }

    pub fn replay_primary_delete_at_version(
        &self,
        keys: &[Vec<u8>],
        delete_version: i64,
    ) -> Result<()> {
        self.tablet()
            .replay_primary_delete_idempotent_at_version(keys.to_vec(), delete_version)?;
        Ok(())
    }

    pub fn replay_compaction_publish(&self, record: &CompactionPublishRecord) -> Result<()> {
        self.tablet().replay_compaction_publish(record)?;
        self.restore_runtime_indexes_for_rowset(record.output_rowset_id)?;
        Ok(())
    }

    pub fn apply_compaction_publish(&self, op: &TabletMutation) -> Result<()> {
        self.tablet().apply_compaction_publish(op)?;
        if let TabletMutation::PublishCompaction {
            output_rowset_id, ..
        } = op
        {
            self.restore_runtime_indexes_for_rowset(*output_rowset_id)?;
        }
        Ok(())
    }

    /// Reconcile derived structures once after the complete durable replay
    /// prefix is installed.
    ///
    /// Per-record repair would repeatedly rescan every visible rowset when a
    /// reconstructible primary-index cache trails the journal, making crash
    /// recovery quadratic in the number of incremental commits.
    pub fn finalize_replayed_derived_state(&self) -> Result<()> {
        self.tablet().repair_primary_index_after_replay()
    }

    pub fn apply_search_generation_publish(&self, op: &TabletMutation) -> Result<()> {
        self.tablet().apply_search_generation_publish(op)
    }

    pub fn replay_search_generation_publish(&self, op: &TabletMutation) -> Result<()> {
        self.tablet().replay_search_generation_publish(op)
    }

    pub fn apply_search_generation_retirement(&self, op: &TabletMutation) -> Result<()> {
        self.tablet().apply_search_generation_retirement(op)
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
        let search_inline_builders = self.search_write_context()?.inline_builders;
        let compacted = run_job_with_search_inline_builders(
            &self.tablet(),
            Arc::new(plan),
            allocate_compaction_job_id(),
            Arc::new(default_allocator()),
            search_inline_builders,
        )?;
        if compacted {
            self.search_registry.refresh_after_rowset_replacement()?;
        }
        Ok(compacted)
    }

    /// Own compaction for this tablet until its current physical debt is
    /// drained, then reconcile provider-owned derived state.
    ///
    /// An explicit OPTIMIZE is a foreground maintenance boundary, not a hint
    /// for the periodic scheduler. It first removes the tablet from background
    /// admission and drains any already accepted job, preventing two plans
    /// from rebuilding the same immutable inputs. Planning is repeated after
    /// every publication because each result changes the version graph.
    pub fn optimize_all(&self, max_compactions: Option<usize>) -> Result<usize> {
        let _registration = if let Some(manager) = self.bound_compaction_manager() {
            manager.drain_tablet(
                self.tablet_id(),
                "foreground OPTIMIZE TABLE",
                FOREGROUND_OPTIMIZE_DRAIN_TIMEOUT,
            )?;
            Some(ForegroundCompactionRegistration {
                manager,
                tablet: self.tablet(),
            })
        } else {
            None
        };

        let limit = max_compactions.unwrap_or(usize::MAX);
        let mut completed = 0usize;
        while completed < limit && self.optimize_compact()? {
            completed = completed.saturating_add(1);
        }

        // Physical publication normally installs one directly usable search
        // artifact for the output rowset. Run provider maintenance to drain a
        // remaining tail or manifest delta before reporting the explicit
        // optimization complete; each sweep is one fair definition quantum.
        for _ in 0..64 {
            let report = self.search_derived_maintenance_sweep()?;
            if !report.has_pending_work() {
                return Ok(completed);
            }
        }
        Err(paro_error::artifact_not_ready(
            "OPTIMIZE TABLE exhausted its derived-search maintenance quanta",
        ))
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
