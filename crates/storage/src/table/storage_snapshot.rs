// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Lazy table storage snapshot.

use crate::rowset::{RowsetSharedPtr, SegmentOptions, SegmentSharedPtr};
use crate::table::table_handle::TableHandle;
use crate::tablet::{
    TabletReadGuard, TabletRef, TabletSchemaAdaptationPlan, TabletSnapshotMaterialization,
};
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{ReadSnapshotLease, ReadTs};
use std::sync::{Arc, Mutex, OnceLock};

pub struct StorageSnapshot {
    read_ts: ReadTs,
    visible_version: i64,
    tablet_id: u64,
    tablet: TabletRef,
    materialized: OnceLock<TabletSnapshotMaterialization>,
    materialize_lock: Mutex<()>,
    read_lease: Option<Arc<ReadSnapshotLease>>,
    _guard: TabletReadGuard,
}

impl StorageSnapshot {
    pub fn capture(
        tablet: TabletRef,
        read_ts: ReadTs,
        read_lease: Option<Arc<ReadSnapshotLease>>,
    ) -> Result<Self> {
        let visible_version = i64::try_from(read_ts.into_raw())
            .map_err(|_| paro_error::invalid_input("read_ts exceeds i64"))?;
        let guard = TabletReadGuard::pin(&tablet, visible_version)?;
        Ok(Self {
            read_ts,
            visible_version,
            tablet_id: tablet.tablet_id(),
            tablet,
            materialized: OnceLock::new(),
            materialize_lock: Mutex::new(()),
            read_lease,
            _guard: guard,
        })
    }

    #[inline]
    pub fn read_ts(&self) -> ReadTs {
        self.read_ts
    }

    #[inline]
    pub fn visible_version(&self) -> i64 {
        self.visible_version
    }

    #[inline]
    pub fn tablet_id(&self) -> u64 {
        self.tablet_id
    }

    #[inline]
    pub fn materialize(&self) -> Result<&TabletSnapshotMaterialization> {
        if let Some(materialized) = self.materialized.get() {
            return Ok(materialized);
        }

        let _guard = self.materialize_lock.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock storage snapshot materializer: {e}"))
        })?;
        if let Some(materialized) = self.materialized.get() {
            return Ok(materialized);
        }

        let materialized = self
            .tablet
            .materialize_storage_snapshot(self.visible_version)?;
        let _ = self.materialized.set(materialized);
        self.materialized
            .get()
            .ok_or_else(|| paro_error::internal("storage snapshot materialization was not cached"))
    }

    #[inline]
    pub fn layout_epoch_snapshot(&self) -> Result<u64> {
        Ok(self.materialize()?.layout_epoch_snapshot)
    }

    #[inline]
    pub fn schema_epoch_snapshot(&self) -> Result<Option<u64>> {
        Ok(self.materialize()?.schema_epoch_snapshot)
    }

    #[inline]
    pub fn physical_schema_token(&self) -> Result<Option<u64>> {
        Ok(self.materialize()?.physical_schema_token)
    }

    #[inline]
    pub fn schema_adaptation_plan(&self) -> Result<&TabletSchemaAdaptationPlan> {
        Ok(&self.materialize()?.schema_adaptation)
    }

    #[inline]
    pub fn schema_adaptation_required(&self) -> Result<bool> {
        Ok(self.schema_adaptation_plan()?.adaptation_required())
    }

    #[inline]
    pub fn rowsets(&self) -> Result<Vec<RowsetSharedPtr>> {
        Ok(self.materialize()?.rowsets.clone())
    }

    #[inline]
    pub fn rowset_count(&self) -> Result<usize> {
        Ok(self.materialize()?.rowsets.len())
    }

    #[inline]
    pub fn has_read_lease(&self) -> bool {
        self.read_lease.is_some()
    }

    pub fn segments_with_options(
        &self,
        options: SegmentOptions,
    ) -> Result<Vec<(RowsetSharedPtr, SegmentSharedPtr)>> {
        let mut segments = Vec::new();
        let materialized = self.materialize()?;
        for rowset in &materialized.rowsets {
            for segment in rowset.segments_with_options(options.clone())? {
                segments.push((rowset.clone(), segment));
            }
        }
        Ok(segments)
    }
}

impl std::fmt::Debug for StorageSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let materialized = self.materialized.get().is_some();
        f.debug_struct("StorageSnapshot")
            .field("read_ts", &self.read_ts)
            .field("visible_version", &self.visible_version)
            .field("tablet_id", &self.tablet_id)
            .field("materialized", &materialized)
            .field("has_read_lease", &self.has_read_lease())
            .finish()
    }
}

impl TableHandle {
    pub fn storage_snapshot(
        &self,
        read_ts: ReadTs,
        read_lease: Option<Arc<ReadSnapshotLease>>,
    ) -> Result<StorageSnapshot> {
        StorageSnapshot::capture(self.tablet(), read_ts, read_lease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::SegmentOptions;
    use crate::table::table_factory::TableFactory;
    use crate::test_utils::{test_chunk_from_vectors, test_i32_vector};
    use paro_common::types::LogicalType;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn captures_visible_rowsets_and_holds_tablet_pin() {
        let table = TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("create table");
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]))
            .expect("append first chunk");

        let visible = table.max_version();
        let snapshot = table
            .storage_snapshot(ReadTs::new(visible as u64), None)
            .expect("capture snapshot");

        assert_eq!(snapshot.visible_version(), visible);
        assert_eq!(snapshot.tablet_id(), table.tablet_id());
        assert_eq!(table.tablet().min_active_visible_version(), Some(visible));
        assert_eq!(snapshot.rowsets().expect("snapshot rowsets").len(), 1);
        assert!(snapshot.layout_epoch_snapshot().expect("layout epoch") > 0);
        assert_eq!(
            snapshot
                .segments_with_options(SegmentOptions::default())
                .expect("snapshot segments")
                .len(),
            table
                .collect_segments(visible)
                .expect("visible segments")
                .len()
        );

        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[4, 5])]))
            .expect("append later chunk");
        assert!(table.max_version() > visible);
        assert_eq!(snapshot.visible_version(), visible);
        assert_eq!(snapshot.rowsets().expect("snapshot rowsets").len(), 1);

        drop(snapshot);
        assert_eq!(table.tablet().min_active_visible_version(), None);
    }

    #[test]
    fn capture_defers_layout_materialization_until_rowsets_are_requested() {
        let table = TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("create table");
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[1])]))
            .expect("append first chunk");

        let visible = table.max_version();
        let snapshot = table
            .storage_snapshot(ReadTs::new(visible as u64), None)
            .expect("capture snapshot");

        assert_eq!(table.tablet().layout_epoch_lease_count(), 0);
        let rowsets = snapshot.rowsets().expect("materialize rowsets");
        assert_eq!(rowsets.len(), 1);
        assert_eq!(table.tablet().layout_epoch_lease_count(), 1);
        assert!(snapshot
            .schema_epoch_snapshot()
            .expect("schema epoch")
            .is_some());
        assert!(snapshot
            .physical_schema_token()
            .expect("physical token")
            .is_some());

        drop(snapshot);
        assert_eq!(table.tablet().layout_epoch_lease_count(), 0);
    }

    #[test]
    fn parallel_workers_share_one_materialized_storage_snapshot() {
        let table = TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("create table");
        for values in [&[1, 2, 3][..], &[4, 5, 6][..]] {
            table
                .append(&test_chunk_from_vectors(vec![test_i32_vector(values)]))
                .expect("append chunk");
        }

        let visible = table.max_version();
        let snapshot = Arc::new(
            table
                .storage_snapshot(ReadTs::new(visible as u64), None)
                .expect("capture snapshot"),
        );
        let barrier = Arc::new(Barrier::new(8));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let snapshot = Arc::clone(&snapshot);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                let materialized = snapshot.materialize().expect("materialize");
                (
                    materialized.layout_epoch_snapshot,
                    materialized.schema_epoch_snapshot,
                    materialized.rowsets.len(),
                )
            }));
        }

        let mut results = Vec::new();
        for worker in workers {
            results.push(worker.join().expect("worker"));
        }
        assert!(results.iter().all(|result| *result == results[0]));
        assert_eq!(results[0].2, 2);
        assert_eq!(table.tablet().layout_epoch_lease_count(), 1);
        drop(snapshot);
        assert_eq!(table.tablet().layout_epoch_lease_count(), 0);
    }

    #[test]
    #[ignore = "T036 explicit storage snapshot performance regression check"]
    fn storage_snapshot_segment_collection_perf_budget() {
        const ROWSETS: usize = 48;
        const SAMPLES: usize = 160;
        const BATCH: usize = 64;

        let table = TableFactory::default()
            .create_table(&[LogicalType::Integer])
            .expect("create table");
        for rowset in 0..ROWSETS {
            let start = (rowset * 16) as i32;
            let values: Vec<i32> = (start..start + 16).collect();
            table
                .append(&test_chunk_from_vectors(vec![test_i32_vector(&values)]))
                .expect("append rowset");
        }

        let visible = table.max_version();
        let read_ts = ReadTs::new(visible as u64);
        assert_eq!(
            table
                .collect_segments_with_options(visible, SegmentOptions::default())
                .expect("legacy warmup")
                .len(),
            ROWSETS
        );
        assert_eq!(
            table
                .storage_snapshot(read_ts, None)
                .expect("snapshot warmup")
                .segments_with_options(SegmentOptions::default())
                .expect("snapshot segments")
                .len(),
            ROWSETS
        );

        let mut legacy = Vec::with_capacity(SAMPLES);
        let mut snapshot = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            legacy.push(time_batch(BATCH, || {
                let segments = table
                    .collect_segments_with_options(visible, SegmentOptions::default())
                    .expect("legacy collect segments");
                assert_eq!(segments.len(), ROWSETS);
            }));
            snapshot.push(time_batch(BATCH, || {
                let segments = table
                    .storage_snapshot(read_ts, None)
                    .expect("capture snapshot")
                    .segments_with_options(SegmentOptions::default())
                    .expect("snapshot collect segments");
                assert_eq!(segments.len(), ROWSETS);
            }));
        }

        let legacy_p50 = percentile(&mut legacy.clone(), 0.50);
        let legacy_p99 = percentile(&mut legacy.clone(), 0.99);
        let snapshot_p50 = percentile(&mut snapshot.clone(), 0.50);
        let snapshot_p99 = percentile(&mut snapshot.clone(), 0.99);
        let p50_delta = ratio_delta(snapshot_p50, legacy_p50);
        let p99_delta = ratio_delta(snapshot_p99, legacy_p99);

        println!(
            "T036 storage_snapshot legacy_p50_ns={} snapshot_p50_ns={} p50_delta={:.2}% legacy_p99_ns={} snapshot_p99_ns={} p99_delta={:.2}%",
            legacy_p50.as_nanos(),
            snapshot_p50.as_nanos(),
            p50_delta * 100.0,
            legacy_p99.as_nanos(),
            snapshot_p99.as_nanos(),
            p99_delta * 100.0
        );

        assert!(
            p50_delta <= 0.02,
            "T036 p50 budget exceeded: {:.2}%",
            p50_delta * 100.0
        );
        assert!(
            p99_delta <= 0.03,
            "T036 p99 budget exceeded: {:.2}%",
            p99_delta * 100.0
        );
    }

    fn time_batch(iterations: usize, mut f: impl FnMut()) -> Duration {
        let start = Instant::now();
        for _ in 0..iterations {
            f();
        }
        start.elapsed() / iterations as u32
    }

    fn percentile(samples: &mut [Duration], pct: f64) -> Duration {
        samples.sort_unstable();
        let idx = ((samples.len() - 1) as f64 * pct).round() as usize;
        samples[idx.min(samples.len() - 1)]
    }

    fn ratio_delta(candidate: Duration, baseline: Duration) -> f64 {
        let baseline = baseline.as_nanos().max(1) as f64;
        candidate.as_nanos() as f64 / baseline - 1.0
    }
}
