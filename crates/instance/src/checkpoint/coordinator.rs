// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::manifest_store::ManifestStore;
use super::retention::RetentionCoordinator;
use super::runtime::{frontier_from_summary, ExactPrefixTimeout, PublishedPrefixTracker};
use super::view::CheckpointView;
use super::writers::{CatalogWriter, DerivedProgressWriter, RouteRegistryWriter, TabletWriter};
use crate::config::CheckpointConfigOptions;
use crate::storage_manager::StorageManager;
use bincode::Options;
use parking_lot::{RwLock, RwLockReadGuard};
use paro_catalog::database_catalog::ParoCatalog;
use paro_common::checkpoint::{
    BundleKind, JournalTailRef, ARTIFACT_ROOTS_BUNDLE_FORMAT_VERSION,
    CATALOG_BUNDLE_FORMAT_VERSION, DERIVED_PROGRESS_BUNDLE_FORMAT_VERSION,
    ROUTE_REGISTRY_BUNDLE_FORMAT_VERSION, TABLET_SHARD_BUNDLE_FORMAT_VERSION,
};
use paro_common::logging::targets;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct CheckpointCoordinator {
    config: RwLock<CheckpointConfigOptions>,
    checkpoint_in_progress: AtomicBool,
    checkpoint_success_total: AtomicU64,
    checkpoint_failure_total: AtomicU64,
    checkpoint_drain_timeout_total: AtomicU64,
    last_checkpoint_finished_at_micros: AtomicU64,
    last_checkpoint_wal_size_bytes: AtomicU64,
    published_prefix: Arc<PublishedPrefixTracker>,
}

pub struct CheckpointInFlightGuard<'a> {
    flag: &'a AtomicBool,
}

pub struct CheckpointExecutionContext<'a> {
    _checkpoint_guard: CheckpointInFlightGuard<'a>,
    storage: RwLockReadGuard<'a, Option<Box<dyn StorageManager>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTriggerReason {
    BytesThreshold,
    IntervalElapsed,
}

#[derive(Debug)]
struct SerializedTabletShardBundle {
    shard_id: u32,
    payload: Vec<u8>,
}

impl Drop for CheckpointInFlightGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

fn checkpoint_bincode() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

impl<'a> CheckpointExecutionContext<'a> {
    pub fn acquire(
        coordinator: &'a CheckpointCoordinator,
        storage_lock: &'a RwLock<Option<Box<dyn StorageManager>>>,
    ) -> Option<Self> {
        let checkpoint_guard = coordinator.try_acquire_in_progress()?;
        let storage = storage_lock.read();

        Some(Self {
            _checkpoint_guard: checkpoint_guard,
            storage,
        })
    }

    pub fn storage(&self) -> Option<&dyn StorageManager> {
        self.storage.as_ref().map(|storage| storage.as_ref())
    }
}

impl CheckpointCoordinator {
    pub fn new() -> Self {
        Self {
            config: RwLock::new(CheckpointConfigOptions::default()),
            checkpoint_in_progress: AtomicBool::new(false),
            checkpoint_success_total: AtomicU64::new(0),
            checkpoint_failure_total: AtomicU64::new(0),
            checkpoint_drain_timeout_total: AtomicU64::new(0),
            last_checkpoint_finished_at_micros: AtomicU64::new(now_micros()),
            last_checkpoint_wal_size_bytes: AtomicU64::new(0),
            published_prefix: Arc::new(PublishedPrefixTracker::new()),
        }
    }

    pub fn configure(&self, config: CheckpointConfigOptions) {
        *self.config.write() = config;
        self.last_checkpoint_finished_at_micros
            .compare_exchange(0, now_micros(), Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    pub fn config(&self) -> CheckpointConfigOptions {
        *self.config.read()
    }

    pub fn auto_trigger_reason(
        &self,
        has_wal: bool,
        wal_size: u64,
        estimated_wal_bytes: u64,
    ) -> Option<CheckpointTriggerReason> {
        if self.checkpoint_in_progress.load(Ordering::Acquire) {
            return None;
        }

        if !has_wal {
            return None;
        }

        let config = self.config();
        let wal_growth =
            wal_size.saturating_sub(self.last_checkpoint_wal_size_bytes.load(Ordering::Acquire));
        if wal_growth.saturating_add(estimated_wal_bytes) >= config.trigger_bytes {
            return Some(CheckpointTriggerReason::BytesThreshold);
        }

        if wal_size == 0 {
            return None;
        }

        let last_finished = self
            .last_checkpoint_finished_at_micros
            .load(Ordering::Acquire);
        let elapsed = now_micros().saturating_sub(last_finished);
        if elapsed >= config.trigger_interval.as_micros() as u64 {
            Some(CheckpointTriggerReason::IntervalElapsed)
        } else {
            None
        }
    }

    pub fn should_checkpoint(
        &self,
        has_wal: bool,
        wal_size: u64,
        estimated_wal_bytes: u64,
    ) -> bool {
        self.auto_trigger_reason(has_wal, wal_size, estimated_wal_bytes)
            .is_some()
    }

    pub fn interval_wait_timeout(&self) -> Duration {
        let config = self.config();
        let last_finished = self
            .last_checkpoint_finished_at_micros
            .load(Ordering::Acquire);
        let elapsed = Duration::from_micros(now_micros().saturating_sub(last_finished));
        config
            .trigger_interval
            .checked_sub(elapsed)
            .unwrap_or(config.trigger_interval)
    }

    pub fn is_in_progress(&self) -> bool {
        self.checkpoint_in_progress.load(Ordering::Acquire)
    }

    pub fn checkpoint_success_total(&self) -> u64 {
        self.checkpoint_success_total.load(Ordering::Relaxed)
    }

    pub fn checkpoint_failure_total(&self) -> u64 {
        self.checkpoint_failure_total.load(Ordering::Relaxed)
    }

    pub fn checkpoint_drain_timeout_total(&self) -> u64 {
        self.checkpoint_drain_timeout_total.load(Ordering::Relaxed)
    }

    pub fn published_prefix(&self) -> Arc<PublishedPrefixTracker> {
        Arc::clone(&self.published_prefix)
    }

    pub fn bootstrap_runtime(&self, summary: paro_common::checkpoint::RecoverySummary) {
        self.published_prefix.bootstrap(summary);
    }

    pub fn try_acquire_in_progress(&self) -> Option<CheckpointInFlightGuard<'_>> {
        self.checkpoint_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| CheckpointInFlightGuard {
                flag: &self.checkpoint_in_progress,
            })
    }

    pub fn checkpoint_if_needed(
        &self,
        storage_lock: &RwLock<Option<Box<dyn StorageManager>>>,
        catalog: &ParoCatalog,
        db_name: &str,
        force: bool,
    ) -> anyhow::Result<bool> {
        {
            let storage = storage_lock.read();
            let has_wal = storage.as_ref().map(|sm| sm.has_wal()).unwrap_or(false);
            let wal_size = storage.as_ref().map(|sm| sm.wal_size()).unwrap_or(0);
            if !force && !self.should_checkpoint(has_wal, wal_size, 0) {
                return Ok(false);
            }
        }

        let Some(ctx) = CheckpointExecutionContext::acquire(self, storage_lock) else {
            tracing::debug!(
                target: targets::CHECKPOINT,
                db = %db_name,
                "Checkpoint already in progress, skipping"
            );
            return Ok(false);
        };

        let result = self.execute(ctx, catalog, db_name);
        let current_wal_size = storage_lock
            .read()
            .as_ref()
            .map(|storage| storage.wal_size())
            .unwrap_or(0);
        self.record_checkpoint_outcome(&result, current_wal_size);
        result.map(|_| true)
    }

    pub(crate) fn record_checkpoint_outcome(
        &self,
        result: &anyhow::Result<()>,
        current_wal_size: u64,
    ) {
        self.last_checkpoint_finished_at_micros
            .store(now_micros(), Ordering::Release);
        self.last_checkpoint_wal_size_bytes
            .store(current_wal_size, Ordering::Release);
        if result.is_ok() {
            self.checkpoint_success_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.checkpoint_failure_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn execute(
        &self,
        ctx: CheckpointExecutionContext<'_>,
        catalog: &ParoCatalog,
        db_name: &str,
    ) -> anyhow::Result<()> {
        let config = self.config();
        let sm = match ctx.storage() {
            Some(storage) => storage,
            None => return Ok(()),
        };

        let Some(manifest_store) = ManifestStore::open_for_storage(sm)? else {
            tracing::debug!(
                target: targets::CHECKPOINT,
                db = %db_name,
                "Skipping durable checkpoint for in-memory storage"
            );
            return Ok(());
        };
        let database_identity = ManifestStore::load_database_identity(sm)?;

        let checkpoint_cut = self.published_prefix.capture_durable_prefix();
        let bootstrap = self
            .published_prefix
            .wait_for_exact_prefix(checkpoint_cut.target_lsn, config.drain_timeout)
            .map_err(|timeout| self.drain_timeout_error(db_name, timeout))?;
        let frontier = frontier_from_summary(&bootstrap);
        let catalog_snapshot_ts = CheckpointView::catalog_snapshot_ts_for(&bootstrap);
        let view = CheckpointView::new(checkpoint_cut, frontier, bootstrap, catalog_snapshot_ts)?;

        tracing::info!(
            target: targets::CHECKPOINT,
            db = %db_name,
            target_lsn = checkpoint_cut.target_lsn,
            published_commit_id = view.frontier.checkpoint_commit_id,
            published_maintenance_id = view.frontier.checkpoint_maintenance_id,
            "Starting database checkpoint from exact published prefix"
        );

        let mut staged = manifest_store.begin_publish(database_identity)?;
        let catalog_bytes = CatalogWriter::serialize_view(catalog, &view)?;
        manifest_store.stage_raw_bundle(
            &mut staged,
            "catalog.bin",
            BundleKind::Catalog,
            CATALOG_BUNDLE_FORMAT_VERSION,
            &catalog_bytes,
            None,
        )?;
        tracing::info!(
            target: targets::CHECKPOINT,
            db = %db_name,
            bytes = catalog_bytes.len(),
            "Catalog snapshot serialized for checkpoint"
        );

        let route_registry = RouteRegistryWriter::serialize_view(catalog, &view)?;
        manifest_store.stage_bundle(
            &mut staged,
            "route-registry.bin",
            BundleKind::RouteRegistry,
            ROUTE_REGISTRY_BUNDLE_FORMAT_VERSION,
            &route_registry,
            None,
        )?;

        let tablet_shards = TabletWriter::serialize_view(catalog, &view)?;
        for shard in Self::serialize_tablet_shards(tablet_shards, config.max_concurrent_writers)? {
            manifest_store.stage_raw_bundle(
                &mut staged,
                &format!("tablet-shard-{:03}.bin", shard.shard_id),
                BundleKind::TabletShard {
                    shard_id: shard.shard_id,
                },
                TABLET_SHARD_BUNDLE_FORMAT_VERSION,
                &shard.payload,
                None,
            )?;
        }

        let derived_progress = DerivedProgressWriter::serialize_view(catalog, sm, &view)?;
        manifest_store.stage_bundle(
            &mut staged,
            "derived-progress.bin",
            BundleKind::DerivedProgress,
            DERIVED_PROGRESS_BUNDLE_FORMAT_VERSION,
            &derived_progress,
            None,
        )?;
        manifest_store.stage_bundle(
            &mut staged,
            "artifact-roots.bin",
            BundleKind::ArtifactRoots,
            ARTIFACT_ROOTS_BUNDLE_FORMAT_VERSION,
            &super::artifact_gc::ArtifactGc::checkpoint_roots(sm),
            None,
        )?;

        let replay_from_lsn = view.frontier.checkpoint_lsn.saturating_add(1);
        let replay_from_segment_id = sm
            .get_wal_arc()
            .map(|wal| wal.segment_id_for_lsn(replay_from_lsn))
            .transpose()
            .map_err(|e| anyhow::anyhow!(e))?
            .unwrap_or(0);

        let manifest = manifest_store.publish_manifest(
            staged,
            view.frontier.clone(),
            view.bootstrap.clone(),
            JournalTailRef {
                replay_from_segment_id,
                replay_from_lsn,
            },
            RetentionCoordinator::retention_floor(view.frontier.checkpoint_lsn),
        )?;
        let retention_report = RetentionCoordinator::advance_retention(
            &manifest_store,
            catalog,
            sm,
            &manifest,
            config,
        )?;

        if let Some(tablet_meta_manager) = sm.get_tablet_meta_manager() {
            tablet_meta_manager
                .rebuild_storage_manifest()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        tracing::info!(
            target: targets::CHECKPOINT,
            db = %db_name,
            checkpoint_id = manifest.checkpoint_id,
            checkpoint_lsn = view.frontier.checkpoint_lsn,
            replay_from_segment_id,
            bundle_count = manifest.bundle_refs.len(),
            deleted_checkpoints = retention_report.checkpoint_gc.deleted_checkpoints,
            pruned_segments = retention_report.segment_prune.deleted_segments,
            removed_artifact_graph_dirs = retention_report.artifact_gc.removed_graph_dirs,
            removed_artifact_staging_entries = retention_report.artifact_gc.removed_staging_entries,
            removed_artifact_compaction_dirs = retention_report.artifact_gc.removed_compaction_dirs,
            "Database checkpointed via committed snapshot manifest publish"
        );
        Ok(())
    }

    fn drain_timeout_error(&self, db_name: &str, timeout: ExactPrefixTimeout) -> anyhow::Error {
        let drain_timeout = self.config().drain_timeout;
        self.checkpoint_drain_timeout_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            target: targets::CHECKPOINT,
            db = %db_name,
            target_lsn = timeout.target_lsn,
            published_lsn = timeout.published_lsn,
            durable_lsn = timeout.durable_lsn,
            timeout_ms = drain_timeout.as_millis() as u64,
            reason = "published_prefix_timeout",
            "Checkpoint drain timed out before exact journal prefix became available"
        );
        anyhow::anyhow!(
            "checkpoint drain timed out waiting for published prefix {} (published={}, durable={})",
            timeout.target_lsn,
            timeout.published_lsn,
            timeout.durable_lsn
        )
    }

    fn serialize_tablet_shards(
        tablet_shards: Vec<paro_common::checkpoint::TabletShardBundle>,
        max_concurrent_writers: usize,
    ) -> anyhow::Result<Vec<SerializedTabletShardBundle>> {
        if tablet_shards.is_empty() {
            return Ok(Vec::new());
        }

        if max_concurrent_writers <= 1 || tablet_shards.len() == 1 {
            let mut bundles = Vec::with_capacity(tablet_shards.len());
            for shard in tablet_shards {
                bundles.push(SerializedTabletShardBundle {
                    shard_id: shard.shard_id,
                    payload: checkpoint_bincode().serialize(&shard)?,
                });
            }
            return Ok(bundles);
        }

        let work = std::sync::Mutex::new(tablet_shards.into_iter().enumerate());
        let results = std::sync::Mutex::new(Vec::new());
        let worker_count = max_concurrent_writers.max(1);

        thread::scope(|scope| -> anyhow::Result<()> {
            let mut handles = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let work = &work;
                let results = &results;
                handles.push(scope.spawn(move || -> anyhow::Result<()> {
                    loop {
                        let next = work.lock().unwrap().next();
                        let Some((index, shard)) = next else {
                            return Ok(());
                        };
                        let payload = checkpoint_bincode().serialize(&shard)?;
                        results.lock().unwrap().push((
                            index,
                            SerializedTabletShardBundle {
                                shard_id: shard.shard_id,
                                payload,
                            },
                        ));
                    }
                }));
            }

            for handle in handles {
                handle.join().map_err(|panic| {
                    if let Some(message) = panic.downcast_ref::<&str>() {
                        anyhow::anyhow!("checkpoint writer thread panicked: {message}")
                    } else if let Some(message) = panic.downcast_ref::<String>() {
                        anyhow::anyhow!("checkpoint writer thread panicked: {message}")
                    } else {
                        anyhow::anyhow!("checkpoint writer thread panicked")
                    }
                })??;
            }
            Ok(())
        })?;

        let mut results = results.into_inner().unwrap();
        results.sort_by_key(|(index, _)| *index);
        Ok(results.into_iter().map(|(_, bundle)| bundle).collect())
    }
}

impl Default for CheckpointCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_trigger_uses_growth_since_last_checkpoint_finish() {
        let coordinator = CheckpointCoordinator::new();
        coordinator.configure(CheckpointConfigOptions {
            trigger_bytes: 10,
            trigger_interval: Duration::from_secs(3600),
            ..CheckpointConfigOptions::default()
        });

        assert_eq!(
            coordinator.auto_trigger_reason(true, 9, 0),
            None,
            "bytes below threshold should not trigger"
        );
        assert_eq!(
            coordinator.auto_trigger_reason(true, 10, 0),
            Some(CheckpointTriggerReason::BytesThreshold)
        );

        let result: anyhow::Result<()> = Ok(());
        coordinator.record_checkpoint_outcome(&result, 10);
        assert_eq!(
            coordinator.auto_trigger_reason(true, 10, 0),
            None,
            "already-checkpointed bytes should not retrigger immediately"
        );
        assert_eq!(
            coordinator.auto_trigger_reason(true, 19, 0),
            None,
            "growth below threshold since last checkpoint should stay quiet"
        );
        assert_eq!(
            coordinator.auto_trigger_reason(true, 20, 0),
            Some(CheckpointTriggerReason::BytesThreshold)
        );
    }

    #[test]
    fn abort_resets_interval_anchor_for_background_retry() {
        let coordinator = CheckpointCoordinator::new();
        coordinator.configure(CheckpointConfigOptions {
            trigger_bytes: u64::MAX,
            trigger_interval: Duration::from_millis(25),
            ..CheckpointConfigOptions::default()
        });

        let result: anyhow::Result<()> = Err(anyhow::anyhow!("timeout"));
        coordinator.record_checkpoint_outcome(&result, 7);
        assert_eq!(
            coordinator.auto_trigger_reason(true, 7, 0),
            None,
            "abort should reset the interval anchor instead of retrying immediately"
        );
    }
}
