// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prefetcher - async/batched page prefetch into PageCache.
//!
//! Prefetch is a best-effort optimization. It uses a prefetch budget lease
//! to size its in-flight queue and schedules I/O via TaskScheduler.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use paro_common::error::{self as paro_error, Result};
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use tracing::trace;

use crate::buffer::{PageCache, PageContentKind, PageKey, DEFAULT_BLOCK_ALLOC_SIZE};
use crate::metrics::storage_metrics;

const SLOW_PREFETCH_ITEM_THRESHOLD: Duration = Duration::from_millis(8);
const SLOW_PREFETCH_BATCH_THRESHOLD: Duration = Duration::from_millis(20);

/// Prefetch options.
#[derive(Debug, Clone)]
pub struct PrefetchOptions {
    /// How many pages ahead to prefetch for sequential scans.
    pub window_pages: usize,
    /// How many pages to group into a single prefetch task.
    pub batch_pages: usize,
    /// Maximum in-flight bytes (0 = use reservation).
    pub max_inflight_bytes: usize,
    /// Maximum concurrent prefetch tasks (0 = derive from reservation).
    pub max_concurrent_tasks: usize,
}

impl Default for PrefetchOptions {
    fn default() -> Self {
        Self {
            window_pages: 8,
            batch_pages: 4,
            max_inflight_bytes: 0,
            max_concurrent_tasks: 0,
        }
    }
}

/// Budget contract used by storage prefetch without depending on execution.
pub trait PrefetchBudget: Send + Sync + std::fmt::Debug {
    fn target_bytes(&self) -> usize;

    fn update_target_bytes(&self, bytes: usize);

    fn try_acquire(&self, bytes: usize) -> bool;

    fn release(&self, bytes: usize);
}

/// A page prefetch item (PageKey + raw page location).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrefetchItem {
    pub key: PageKey,
    pub offset: u64,
    pub size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefetchState {
    InFlight,
    Ready,
}

#[derive(Debug)]
struct PrefetchRegistry {
    entries: Mutex<HashMap<PageKey, PrefetchState>>,
}

impl PrefetchRegistry {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn try_mark_inflight(&self, key: PageKey) -> bool {
        let mut entries = self.entries.lock();
        if entries.contains_key(&key) {
            return false;
        }
        entries.insert(key, PrefetchState::InFlight);
        true
    }

    fn mark_ready(&self, key: PageKey) {
        let mut entries = self.entries.lock();
        if let Some(state) = entries.get_mut(&key) {
            *state = PrefetchState::Ready;
        }
    }

    fn remove(&self, key: &PageKey) {
        let mut entries = self.entries.lock();
        entries.remove(key);
    }

    fn consume(&self, key: &PageKey) {
        let mut entries = self.entries.lock();
        match entries.remove(key) {
            Some(PrefetchState::Ready) => storage_metrics().inc_prefetch_hit(),
            Some(PrefetchState::InFlight) => storage_metrics().inc_prefetch_wait(),
            None => {}
        }
    }

    fn drain_all(&self) -> usize {
        let mut entries = self.entries.lock();
        let count = entries.len();
        entries.clear();
        count
    }
}

struct PrefetchTaskReservation {
    lease: Arc<dyn PrefetchBudget>,
    inflight_bytes: Arc<AtomicUsize>,
    inflight_tasks: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for PrefetchTaskReservation {
    fn drop(&mut self) {
        self.lease.release(self.bytes);
        self.inflight_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.inflight_tasks.fetch_sub(1, Ordering::Relaxed);
    }
}

struct PrefetchTask {
    cache: Arc<PageCache>,
    registry: Arc<PrefetchRegistry>,
    items: Vec<PrefetchItem>,
    file_path: PathBuf,
    reservation: Option<PrefetchTaskReservation>,
}

impl PrefetchTask {
    fn read_page_bytes(file: &mut File, offset: u64, size: u32) -> Result<Vec<u8>> {
        let page_size = size as usize;
        if page_size < 8 {
            return Err(paro_error::data_corrupted(format!(
                "Bad page: too small ({})",
                page_size
            )));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut page_data = vec![0u8; page_size];
        file.read_exact(&mut page_data)?;
        Ok(page_data)
    }
}

impl Task for PrefetchTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let batch_bytes = self
            .reservation
            .as_ref()
            .map(|reservation| reservation.bytes)
            .unwrap_or(0);
        let _reservation = self.reservation.take();
        let batch_start = Instant::now();

        let mut file = match File::open(&self.file_path) {
            Ok(file) => file,
            Err(_) => {
                for item in &self.items {
                    self.registry.remove(&item.key);
                }
                return Ok(TaskExecutionResult::Finished);
            }
        };

        let mut ready_count = 0usize;
        for item in &self.items {
            let item_start = Instant::now();
            let key = item.key;
            let result = self
                .cache
                .get_or_load(key, PageContentKind::Compressed, || {
                    Self::read_page_bytes(&mut file, item.offset, item.size)
                });
            match result {
                Ok(_) => {
                    self.registry.mark_ready(key);
                    ready_count += 1;
                }
                Err(_) => self.registry.remove(&key),
            }
            let elapsed = item_start.elapsed();
            if elapsed >= SLOW_PREFETCH_ITEM_THRESHOLD {
                trace!(
                    file = %self.file_path.display(),
                    page_offset = item.offset,
                    page_size = item.size,
                    elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                    "slow prefetch item",
                );
            }
        }

        let batch_elapsed = batch_start.elapsed();
        if batch_elapsed >= SLOW_PREFETCH_BATCH_THRESHOLD {
            trace!(
                file = %self.file_path.display(),
                pages = self.items.len(),
                ready = ready_count,
                batch_bytes,
                elapsed_ms = batch_elapsed.as_secs_f64() * 1000.0,
                "slow prefetch batch",
            );
        }

        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "PrefetchTask"
    }
}

/// Prefetcher for page cache.
#[derive(Clone)]
pub struct Prefetcher {
    cache: Arc<PageCache>,
    scheduler: Arc<TaskScheduler>,
    lease: Arc<dyn PrefetchBudget>,
    options: PrefetchOptions,
    registry: Arc<PrefetchRegistry>,
    inflight_bytes: Arc<AtomicUsize>,
    inflight_tasks: Arc<AtomicUsize>,
}

impl Prefetcher {
    pub fn new(
        cache: Arc<PageCache>,
        scheduler: Arc<TaskScheduler>,
        lease: Arc<dyn PrefetchBudget>,
        options: PrefetchOptions,
    ) -> Self {
        let prefetcher = Self {
            cache,
            scheduler,
            lease,
            options,
            registry: Arc::new(PrefetchRegistry::new()),
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
            inflight_tasks: Arc::new(AtomicUsize::new(0)),
        };
        let default_target = prefetcher.options.window_pages * DEFAULT_BLOCK_ALLOC_SIZE;
        prefetcher.update_target_bytes(default_target);
        prefetcher
    }

    pub fn options(&self) -> &PrefetchOptions {
        &self.options
    }

    /// Update the target bytes for the temporary memory state.
    pub fn update_target_bytes(&self, bytes: usize) {
        self.lease.update_target_bytes(bytes);
    }

    /// Record a page consumption (hit/wait).
    pub fn record_consume(&self, key: &PageKey) {
        self.registry.consume(key);
    }

    /// Record waste for all prefetched but unused pages.
    pub fn record_waste(&self) {
        let wasted = self.registry.drain_all();
        if wasted > 0 {
            storage_metrics().add_prefetch_waste(wasted as u64);
        }
    }

    fn budget_bytes(&self) -> usize {
        let reservation = self.lease.target_bytes();
        if reservation == 0 {
            return 0;
        }
        if self.options.max_inflight_bytes == 0 {
            reservation
        } else {
            std::cmp::min(reservation, self.options.max_inflight_bytes)
        }
    }

    fn max_tasks(&self) -> usize {
        let reservation = self.lease.target_bytes();
        if reservation == 0 {
            return 0;
        }
        let derived = std::cmp::max(1, reservation / DEFAULT_BLOCK_ALLOC_SIZE);
        if self.options.max_concurrent_tasks == 0 {
            derived
        } else {
            std::cmp::min(derived, self.options.max_concurrent_tasks)
        }
    }

    fn try_reserve(&self, bytes: usize, budget: usize, max_tasks: usize) -> bool {
        if bytes == 0 || budget == 0 || max_tasks == 0 {
            return false;
        }

        let current_tasks = self.inflight_tasks.load(Ordering::Relaxed);
        if current_tasks >= max_tasks {
            return false;
        }

        let current_bytes = self.inflight_bytes.load(Ordering::Relaxed);
        if current_bytes + bytes > budget {
            return false;
        }

        if !self.lease.try_acquire(bytes) {
            return false;
        }
        self.inflight_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inflight_tasks.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Prefetch a sequential window of items (best-effort).
    pub fn prefetch_window(&self, file_path: &Path, items: Vec<PrefetchItem>) {
        if self.options.window_pages == 0 {
            return;
        }
        let items = items.into_iter().take(self.options.window_pages).collect();
        self.prefetch_batch(file_path, items);
    }

    /// Prefetch an explicit batch of items (best-effort).
    pub fn prefetch_batch(&self, file_path: &Path, items: Vec<PrefetchItem>) {
        if items.is_empty() || self.options.batch_pages == 0 {
            return;
        }

        let budget = self.budget_bytes();
        if budget == 0 {
            return;
        }
        let max_tasks = self.max_tasks();
        if max_tasks == 0 {
            return;
        }

        let mut filtered = Vec::with_capacity(items.len());
        for item in items {
            if self.registry.try_mark_inflight(item.key) {
                filtered.push(item);
            }
        }
        if filtered.is_empty() {
            return;
        }

        let batch_pages = std::cmp::max(1, self.options.batch_pages);
        let mut tasks = Vec::new();
        let mut idx = 0;
        while idx < filtered.len() {
            let end = std::cmp::min(idx + batch_pages, filtered.len());
            let batch: Vec<PrefetchItem> = filtered[idx..end].to_vec();
            let batch_bytes = batch.iter().map(|item| item.size as usize).sum::<usize>();

            if !self.try_reserve(batch_bytes, budget, max_tasks) {
                for item in batch {
                    self.registry.remove(&item.key);
                }
                break;
            }

            let task = PrefetchTask {
                cache: self.cache.clone(),
                registry: self.registry.clone(),
                items: batch,
                file_path: file_path.to_path_buf(),
                reservation: Some(PrefetchTaskReservation {
                    lease: self.lease.clone(),
                    inflight_bytes: self.inflight_bytes.clone(),
                    inflight_tasks: self.inflight_tasks.clone(),
                    bytes: batch_bytes,
                }),
            };
            let task: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(task));
            tasks.push(task);
            idx = end;
        }

        if !tasks.is_empty() {
            self.scheduler.schedule_tasks(tasks);
        }
    }
}

impl std::fmt::Debug for Prefetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prefetcher")
            .field("window_pages", &self.options.window_pages)
            .field("batch_pages", &self.options.batch_pages)
            .field("target_bytes", &self.lease.target_bytes())
            .field(
                "inflight_bytes",
                &self.inflight_bytes.load(Ordering::Relaxed),
            )
            .field(
                "inflight_tasks",
                &self.inflight_tasks.load(Ordering::Relaxed),
            )
            .finish()
    }
}
