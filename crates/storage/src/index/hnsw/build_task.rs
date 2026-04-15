// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{
    DistanceMetric, HnswBuildConcurrencyBudget, HnswBuildStopCheck, HnswBuilder, HnswConfig,
    MmapVectorStorage,
};
use crate::rowset::encoding::PLAIN_PAGE_HEADER_SIZE;
use crate::rowset::page::{
    BlockCompressionCodec, CompressionType, IndexPageFooter, IndexPageType, Lz4Codec, PageFooter,
    PageIO, PagePointer, ZstdCodec, DEFAULT_MIN_SPACE_SAVING,
};
use crate::rowset::segment::{SegmentFooter, SegmentSharedPtr};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::ColumnId;
use parking_lot::Mutex;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::types::LogicalType;
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use rayon::current_num_threads;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Build configuration for a single HNSW-indexed column.
#[derive(Debug, Clone)]
pub struct HnswColumnBuildConfig {
    pub column_id: ColumnId,
    pub config: HnswConfig,
    pub distance: DistanceMetric,
}

impl HnswColumnBuildConfig {
    pub fn new(column_id: ColumnId, config: HnswConfig, distance: DistanceMetric) -> Self {
        Self {
            column_id,
            config,
            distance,
        }
    }
}

/// Summary returned by scheduler-backed HNSW build.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HnswBuildSummary {
    /// Number of segment tasks scheduled.
    pub scheduled_segments: usize,
    /// Number of segments that actually wrote at least one HNSW index page.
    pub built_segments: usize,
    /// Number of HNSW column indexes written.
    pub built_columns: usize,
    /// Number of segments skipped because all target columns already had HNSW.
    pub skipped_segments: usize,
}

#[derive(Debug, Clone)]
struct SegmentColumnJob {
    column_id: ColumnId,
    dim: usize,
    config: HnswConfig,
    distance: DistanceMetric,
}

#[derive(Debug, Clone)]
struct HnswBuildJob {
    rowset_id: u64,
    segment_id: u32,
    segment_path: PathBuf,
    columns: Vec<SegmentColumnJob>,
}

#[derive(Debug, Default, Clone, Copy)]
struct HnswBuildResult {
    built_columns: usize,
}

#[derive(Debug)]
struct PendingHnswPage {
    column_id: ColumnId,
    compression: CompressionType,
    num_entries: u32,
    index_data: Vec<u8>,
}

struct SharedBuildState {
    total_tasks: usize,
    completed_tasks: AtomicUsize,
    built_segments: AtomicUsize,
    built_columns: AtomicUsize,
    first_error: Mutex<Option<ParoError>>,
    stop_requested: AtomicBool,
    stop_check: Option<HnswBuildStopCheck>,
    parallel_build_budget: Arc<HnswBuildConcurrencyBudget>,
}

impl SharedBuildState {
    fn new(
        total_tasks: usize,
        parallel_build_budget: Arc<HnswBuildConcurrencyBudget>,
        stop_check: Option<HnswBuildStopCheck>,
    ) -> Self {
        Self {
            total_tasks,
            completed_tasks: AtomicUsize::new(0),
            built_segments: AtomicUsize::new(0),
            built_columns: AtomicUsize::new(0),
            first_error: Mutex::new(None),
            stop_requested: AtomicBool::new(false),
            stop_check,
            parallel_build_budget,
        }
    }

    fn on_task_complete(&self, result: Result<HnswBuildResult>) {
        match result {
            Ok(result) => {
                if result.built_columns > 0 {
                    self.built_segments.fetch_add(1, Ordering::AcqRel);
                    self.built_columns
                        .fetch_add(result.built_columns, Ordering::AcqRel);
                }
            }
            Err(err) => {
                self.request_stop();
                let mut first_error = self.first_error.lock();
                if first_error.is_none() {
                    *first_error = Some(err);
                }
            }
        }
        self.completed_tasks.fetch_add(1, Ordering::AcqRel);
    }

    fn should_stop(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
            || self
                .stop_check
                .as_ref()
                .is_some_and(|check| check.should_stop())
    }

    fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
    }

    fn completed(&self) -> usize {
        self.completed_tasks.load(Ordering::Acquire)
    }

    fn summary(&self) -> HnswBuildSummary {
        let built_segments = self.built_segments.load(Ordering::Acquire);
        let built_columns = self.built_columns.load(Ordering::Acquire);
        let scheduled_segments = self.total_tasks;
        HnswBuildSummary {
            scheduled_segments,
            built_segments,
            built_columns,
            skipped_segments: scheduled_segments.saturating_sub(built_segments),
        }
    }

    fn take_first_error(&self) -> Option<ParoError> {
        self.first_error.lock().take()
    }

    fn create_builder(self: &Arc<Self>) -> HnswBuilder {
        let state = Arc::clone(self);
        HnswBuilder::new()
            .with_concurrency_budget(Arc::clone(&self.parallel_build_budget))
            .with_stop_check(HnswBuildStopCheck::new(move || state.should_stop()))
    }
}

/// Task that builds missing HNSW indexes for a single segment.
pub struct HnswBuildTask {
    job: Option<HnswBuildJob>,
    state: Arc<SharedBuildState>,
}

impl HnswBuildTask {
    fn new(job: HnswBuildJob, state: Arc<SharedBuildState>) -> Self {
        Self {
            job: Some(job),
            state,
        }
    }

    fn run(job: &HnswBuildJob, state: &Arc<SharedBuildState>) -> Result<HnswBuildResult> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&job.segment_path)
            .map_err(|err| {
                paro_error::io_error(format!(
                    "open segment file {} for HNSW build: {}",
                    job.segment_path.display(),
                    err
                ))
            })?;

        let original_len = file
            .metadata()
            .map_err(|err| {
                paro_error::io_error(format!(
                    "read segment file size {} before HNSW build: {}",
                    job.segment_path.display(),
                    err
                ))
            })?
            .len();

        let result = (|| {
            if state.should_stop() {
                return Err(paro_error::query_canceled());
            }

            let mut footer = read_segment_footer(&mut file)?;
            let hnsw_builder = state.create_builder();
            let mut pending_pages = Vec::new();

            // Build all HNSW payloads first, then mutate the segment file only after all pages are ready.
            for column in &job.columns {
                if state.should_stop() {
                    return Err(paro_error::query_canceled());
                }

                let Some(col_meta) = footer
                    .column_metas
                    .iter()
                    .find(|meta| meta.column_id == column.column_id)
                else {
                    return Err(paro_error::data_corrupted(format!(
                        "segment {} rowset {} missing column {} in footer",
                        job.segment_id, job.rowset_id, column.column_id
                    )));
                };

                if col_meta.hnsw_index_pointer.is_some() {
                    continue;
                }

                let Some(page) =
                    build_hnsw_page_data(&job.segment_path, col_meta, column, &hnsw_builder)?
                else {
                    continue;
                };
                pending_pages.push(page);
            }

            if pending_pages.is_empty() {
                return Ok(HnswBuildResult { built_columns: 0 });
            }
            if state.should_stop() {
                return Err(paro_error::query_canceled());
            }

            let mut built_columns = 0usize;
            for page in pending_pages {
                let ptr = append_hnsw_page(
                    &mut file,
                    page.compression,
                    &page.index_data,
                    page.num_entries,
                )?;

                let target = footer
                    .column_metas
                    .iter_mut()
                    .find(|meta| meta.column_id == page.column_id)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "segment {} rowset {} lost column {} during HNSW build",
                            job.segment_id, job.rowset_id, page.column_id
                        ))
                    })?;
                target.hnsw_index_pointer = Some(ptr);
                target.total_mem_footprint =
                    target.total_mem_footprint.saturating_add(ptr.size as u64);
                built_columns += 1;
            }

            file.seek(SeekFrom::End(0)).map_err(|err| {
                paro_error::io_error(format!(
                    "seek end for segment {} rowset {} footer append: {}",
                    job.segment_id, job.rowset_id, err
                ))
            })?;
            let footer_bytes = footer.serialize();
            file.write_all(&footer_bytes).map_err(|err| {
                paro_error::io_error(format!(
                    "append footer for segment {} rowset {}: {}",
                    job.segment_id, job.rowset_id, err
                ))
            })?;
            file.flush().map_err(|err| {
                paro_error::io_error(format!(
                    "flush segment {} rowset {} after HNSW build: {}",
                    job.segment_id, job.rowset_id, err
                ))
            })?;

            Ok(HnswBuildResult { built_columns })
        })();

        if result.is_err() {
            rollback_segment_file(&mut file, original_len);
        }
        result
    }
}

impl Task for HnswBuildTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let Some(job) = self.job.take() else {
            return Ok(TaskExecutionResult::Finished);
        };
        let result = if self.state.should_stop() {
            Err(paro_error::query_canceled())
        } else {
            Self::run(&job, &self.state)
        };
        self.state.on_task_complete(result);
        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "HnswBuildTask"
    }
}

fn read_segment_footer(file: &mut File) -> Result<SegmentFooter> {
    let file_size = file.metadata().map_err(paro_error::io)?.len();
    if file_size < 4 {
        return Err(paro_error::data_corrupted(format!(
            "segment file too small for footer: {} bytes",
            file_size
        )));
    }

    file.seek(SeekFrom::End(-4)).map_err(paro_error::io)?;
    let mut footer_size_buf = [0u8; 4];
    file.read_exact(&mut footer_size_buf)
        .map_err(paro_error::io)?;
    let footer_size = u32::from_le_bytes(footer_size_buf) as u64;

    if footer_size < 8 || footer_size > file_size {
        return Err(paro_error::data_corrupted(format!(
            "invalid segment footer size: {} (file size {})",
            footer_size, file_size
        )));
    }

    file.seek(SeekFrom::End(-(footer_size as i64)))
        .map_err(paro_error::io)?;
    let mut footer_bytes = vec![0u8; footer_size as usize - 4];
    file.read_exact(&mut footer_bytes).map_err(paro_error::io)?;
    SegmentFooter::deserialize(&footer_bytes)
}

fn rollback_segment_file(file: &mut File, original_len: u64) {
    let _ = file.set_len(original_len);
    let _ = file.seek(SeekFrom::Start(original_len));
    let _ = file.flush();
}

fn build_hnsw_page_data(
    segment_path: &PathBuf,
    col_meta: &crate::rowset::segment::ColumnMeta,
    job: &SegmentColumnJob,
    hnsw_builder: &HnswBuilder,
) -> Result<Option<PendingHnswPage>> {
    if col_meta.num_rows == 0 {
        return Ok(None);
    }

    let vector_storage = Arc::new(MmapVectorStorage::open_range(
        segment_path,
        col_meta.data_page_pointer.offset + PLAIN_PAGE_HEADER_SIZE as u64,
        col_meta.num_rows * job.dim as u64 * std::mem::size_of::<f32>() as u64,
        job.dim,
    )?);
    let index = hnsw_builder.build(vector_storage, job.config, job.distance)?;
    let index_data = index.serialize()?;
    Ok(Some(PendingHnswPage {
        column_id: job.column_id,
        compression: col_meta.compression,
        num_entries: index.graph.links.num_points() as u32,
        index_data,
    }))
}

fn append_hnsw_page(
    file: &mut File,
    compression: CompressionType,
    index_data: &[u8],
    num_entries: u32,
) -> Result<PagePointer> {
    let footer = PageFooter::Index(IndexPageFooter {
        num_entries,
        page_type: IndexPageType::Leaf,
    });
    file.seek(SeekFrom::End(0)).map_err(paro_error::io)?;
    let codec = compression_codec(compression);
    PageIO::compress_and_write_page(
        codec.as_deref(),
        DEFAULT_MIN_SPACE_SAVING,
        file,
        index_data,
        &footer,
    )
}

fn compression_codec(compression: CompressionType) -> Option<Box<dyn BlockCompressionCodec>> {
    match compression {
        CompressionType::None => None,
        CompressionType::Lz4 => Some(Box::new(Lz4Codec)),
        CompressionType::Zstd => Some(Box::new(ZstdCodec::default())),
    }
}

fn collect_segment_job(
    rowset_id: u64,
    segment: &SegmentSharedPtr,
    columns: &[HnswColumnBuildConfig],
) -> Result<Option<HnswBuildJob>> {
    let mut target_columns = Vec::new();

    for config in columns {
        let Some(meta) = segment.get_column_meta(config.column_id) else {
            continue;
        };
        if meta.hnsw_index_pointer.is_some() {
            continue;
        }

        let schema_col = segment
            .schema()
            .column_by_id(config.column_id)
            .ok_or_else(|| {
                paro_error::column_not_found(format!(
                    "column {} not found in segment schema",
                    config.column_id
                ))
            })?;

        let dim = match &schema_col.logical_type {
            LogicalType::Array(inner, dim) if matches!(**inner, LogicalType::Float) => *dim,
            other => {
                return Err(paro_error::not_supported(format!(
                    "HNSW build requires Array(Float, N), got {:?} for column {}",
                    other, config.column_id
                )));
            }
        };

        target_columns.push(SegmentColumnJob {
            column_id: config.column_id,
            dim,
            config: config.config,
            distance: config.distance,
        });
    }

    if target_columns.is_empty() {
        return Ok(None);
    }

    Ok(Some(HnswBuildJob {
        rowset_id,
        segment_id: segment.segment_id(),
        segment_path: segment.file_path().to_path_buf(),
        columns: target_columns,
    }))
}

fn collect_jobs(
    rowsets: &[RowsetSharedPtr],
    columns: &[HnswColumnBuildConfig],
) -> Result<Vec<HnswBuildJob>> {
    if rowsets.is_empty() || columns.is_empty() {
        return Ok(Vec::new());
    }

    let mut jobs = Vec::new();
    for rowset in rowsets {
        rowset.load()?;
        for segment in rowset.segments() {
            if let Some(job) = collect_segment_job(rowset.rowset_id(), &segment, columns)? {
                jobs.push(job);
            }
        }
    }

    Ok(jobs)
}

fn derive_parallel_build_budget(scheduler: &TaskScheduler) -> Arc<HnswBuildConcurrencyBudget> {
    let scheduler_workers = scheduler.number_of_threads().max(0) as usize;
    let scheduler_concurrency = scheduler_workers.saturating_add(1).max(1);
    let rayon_workers = current_num_threads().max(1);
    let parallel_slots = (rayon_workers / scheduler_concurrency).max(1);
    Arc::new(HnswBuildConcurrencyBudget::new(parallel_slots))
}

/// Build missing HNSW segment indexes in parallel via TaskScheduler.
///
/// This is used by `CREATE INDEX ... USING HNSW` metadata-only flow to materialize
/// per-segment HNSW bodies for already persisted rowsets.
pub fn build_missing_hnsw_indexes_with_scheduler(
    rowsets: &[RowsetSharedPtr],
    columns: &[HnswColumnBuildConfig],
    scheduler: Arc<TaskScheduler>,
) -> Result<HnswBuildSummary> {
    build_missing_hnsw_indexes_with_scheduler_and_stop_check(rowsets, columns, scheduler, None)
}

/// Same as [`build_missing_hnsw_indexes_with_scheduler`] but supports cooperative cancellation.
pub fn build_missing_hnsw_indexes_with_scheduler_and_stop_check(
    rowsets: &[RowsetSharedPtr],
    columns: &[HnswColumnBuildConfig],
    scheduler: Arc<TaskScheduler>,
    stop_check: Option<HnswBuildStopCheck>,
) -> Result<HnswBuildSummary> {
    let jobs = collect_jobs(rowsets, columns)?;
    if jobs.is_empty() {
        return Ok(HnswBuildSummary::default());
    }

    let state = Arc::new(SharedBuildState::new(
        jobs.len(),
        derive_parallel_build_budget(scheduler.as_ref()),
        stop_check,
    ));
    let producer = scheduler.create_producer();

    let tasks: Vec<Arc<Mutex<dyn Task>>> = jobs
        .into_iter()
        .map(|job| {
            Arc::new(Mutex::new(HnswBuildTask::new(job, state.clone()))) as Arc<Mutex<dyn Task>>
        })
        .collect();
    producer.schedule_tasks(tasks);

    let marker = AtomicBool::new(true);
    while state.completed() < state.total_tasks {
        let completed = scheduler.execute_tasks_for_producer(&producer, &marker, 1);
        if completed == 0
            && state.completed() < state.total_tasks
            && !scheduler.wait_for_task_for_producer(&producer)
        {
            std::thread::yield_now();
        }
    }

    if let Some(err) = state.take_first_error() {
        return Err(err);
    }

    for rowset in rowsets {
        rowset.reload()?;
    }

    Ok(state.summary())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{HnswIndex, InMemoryVectorStorage, SearchParams};
    use crate::rowset::segment::{
        ColumnData, Segment, SegmentOptions, SegmentWriter, SegmentWriterOptions,
    };
    use crate::rowset::{Rowset, RowsetMeta, RowsetState};
    use crate::tablet::{KeysType, TabletColumn, TabletSchema, Version};
    use paro_common::types::LogicalType;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn build_missing_hnsw_indexes_with_scheduler_materializes_segment_indexes() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("0.dat");

        let dim = 4;
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![
                    TabletColumn::new(0, "id", LogicalType::BigInt),
                    TabletColumn::new(
                        1,
                        "vec",
                        LogicalType::Array(Box::new(LogicalType::Float), dim),
                    ),
                ],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );

        let mut writer =
            SegmentWriter::create(schema.clone(), &segment_path, SegmentWriterOptions::new(0))
                .unwrap();

        let mut id_data = Vec::new();
        let mut vector_data = Vec::new();
        for i in 0..64 {
            id_data.extend_from_slice(&(i as i64).to_le_bytes());
            for _ in 0..dim {
                vector_data.extend_from_slice(&(i as f32).to_le_bytes());
            }
        }
        writer
            .append_chunk(&[
                ColumnData::new(id_data, 64),
                ColumnData::new(vector_data, 64),
            ])
            .unwrap();
        writer.finalize().unwrap();

        let segment = Arc::new(
            Segment::open(
                0,
                &segment_path,
                schema.clone(),
                SegmentOptions::default().with_verify_checksum(false),
                1,
                1,
                1,
            )
            .unwrap(),
        );
        assert!(segment.hnsw_index(1).is_none());

        let mut meta = RowsetMeta::new(1, 1, Version::singleton(0));
        meta.set_num_rows(64);
        meta.set_num_segments(1);
        meta.set_rowset_state(RowsetState::Visible);
        meta.set_rowset_path(temp_dir.path().to_string_lossy().to_string());
        let rowset = Arc::new(
            Rowset::create_with_segments(schema.clone(), meta, temp_dir.path(), vec![segment])
                .unwrap(),
        );

        let scheduler = Arc::new(TaskScheduler::new());
        scheduler.set_threads(2).unwrap();

        let summary = build_missing_hnsw_indexes_with_scheduler(
            &[rowset.clone()],
            &[HnswColumnBuildConfig::new(
                1,
                HnswConfig::new(8, 50),
                DistanceMetric::Euclidean,
            )],
            scheduler,
        )
        .unwrap();

        assert_eq!(summary.scheduled_segments, 1);
        assert_eq!(summary.built_segments, 1);
        assert_eq!(summary.built_columns, 1);

        rowset.reload().unwrap();
        let mut reloaded_segments = rowset.segments();
        let rebuilt_segment = reloaded_segments.remove(0);
        let index = rebuilt_segment
            .hnsw_index(1)
            .expect("missing built HNSW index");
        assert_eq!(index.graph.links.num_points(), 64);

        let result = index
            .search_one(
                &[32.0; 4],
                1,
                &SearchParams {
                    ef: Some(64),
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        assert_eq!(result[0].idx, 32);

        // Ensure scheduler/materialization path and direct build path behave consistently.
        let mut flat = Vec::with_capacity(64 * dim);
        for i in 0..64 {
            for _ in 0..dim {
                flat.push(i as f32);
            }
        }
        let direct = HnswIndex::build(
            Arc::new(InMemoryVectorStorage::new(flat, dim)),
            HnswConfig::new(8, 50),
            DistanceMetric::Euclidean,
        );
        let direct_result = direct
            .search_one(
                &[32.0; 4],
                1,
                &SearchParams {
                    ef: Some(64),
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        assert_eq!(direct_result[0].idx, result[0].idx);
    }

    #[test]
    fn build_with_stop_check_keeps_segment_file_consistent() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("0.dat");

        let dim = 4;
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![
                    TabletColumn::new(0, "id", LogicalType::BigInt),
                    TabletColumn::new(
                        1,
                        "vec",
                        LogicalType::Array(Box::new(LogicalType::Float), dim),
                    ),
                ],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );

        let mut writer =
            SegmentWriter::create(schema.clone(), &segment_path, SegmentWriterOptions::new(0))
                .unwrap();

        let mut id_data = Vec::new();
        let mut vector_data = Vec::new();
        for i in 0..1024 {
            id_data.extend_from_slice(&(i as i64).to_le_bytes());
            for _ in 0..dim {
                vector_data.extend_from_slice(&(i as f32).to_le_bytes());
            }
        }
        writer
            .append_chunk(&[
                ColumnData::new(id_data, 1024),
                ColumnData::new(vector_data, 1024),
            ])
            .unwrap();
        writer.finalize().unwrap();

        let segment = Arc::new(
            Segment::open(
                0,
                &segment_path,
                schema.clone(),
                SegmentOptions::default().with_verify_checksum(false),
                1,
                1,
                1,
            )
            .unwrap(),
        );
        let mut meta = RowsetMeta::new(1, 1, Version::singleton(0));
        meta.set_num_rows(1024);
        meta.set_num_segments(1);
        meta.set_rowset_state(RowsetState::Visible);
        meta.set_rowset_path(temp_dir.path().to_string_lossy().to_string());
        let rowset = Arc::new(
            Rowset::create_with_segments(schema.clone(), meta, temp_dir.path(), vec![segment])
                .unwrap(),
        );

        let scheduler = Arc::new(TaskScheduler::new());
        scheduler.set_threads(2).unwrap();

        let before_len = std::fs::metadata(&segment_path).unwrap().len();
        let checks = Arc::new(AtomicUsize::new(0));
        let stop_check = {
            let checks = Arc::clone(&checks);
            HnswBuildStopCheck::new(move || checks.fetch_add(1, AtomicOrdering::Relaxed) > 0)
        };

        let err = build_missing_hnsw_indexes_with_scheduler_and_stop_check(
            &[rowset.clone()],
            &[HnswColumnBuildConfig::new(
                1,
                HnswConfig::new(8, 50),
                DistanceMetric::Euclidean,
            )],
            scheduler,
            Some(stop_check),
        )
        .unwrap_err();
        assert!(err.is_query_canceled());

        let after_len = std::fs::metadata(&segment_path).unwrap().len();
        assert_eq!(before_len, after_len);

        rowset.reload().unwrap();
        let mut segments = rowset.segments();
        let reloaded = segments.remove(0);
        assert!(reloaded.hnsw_index(1).is_none());
    }
}
