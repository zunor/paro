// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit queue batching and backpressure policy.

use super::super::{
    CommitFenceRejectReason, CommitFrontierSnapshot, DEFAULT_MAX_GROUP_COMMIT_FENCE_US,
};
use std::fmt;
use std::time::Duration;

const DEFAULT_MAX_COMMIT_BATCH_SIZE: usize = 256;
const DEFAULT_TARGET_COMMIT_BATCH_SIZE: usize = 64;
const DEFAULT_MAX_COMMIT_BATCH_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_TARGET_COMMIT_BATCH_BYTES: u64 = 2 * 1024 * 1024;
const DEFAULT_MAX_COMMIT_QUEUE_DEPTH: usize = 8192;
const DEFAULT_MAX_COMMIT_QUEUE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_MAX_PENDING_FENCE_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_MAX_DURABLE_TO_PUBLISHED_BYTES_LAG: u64 = 512 * 1024 * 1024;
const DEFAULT_QUEUE_HEAD_WAIT_BUDGET_US: u64 = 1_000;
const DEFAULT_DRAIN_OWNER_COALESCE_BUDGET_US: u64 = 500;
const DEFAULT_SERIALIZABLE_BYPASS_BATCH_BUDGET: u32 = 8;
const DEFAULT_SERIALIZABLE_BYPASS_WAIT_BUDGET_US: u64 = 2_000;
const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_US: u64 = 30_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitBatchPolicy {
    pub max_commit_batch_size: usize,
    pub max_commit_batch_bytes: u64,
    pub max_commit_fence_us: u64,
    pub max_fence_rejects_per_drain: usize,
    pub queue_head_wait_budget_us: u64,
    pub drain_owner_coalesce_budget_us: u64,
    pub target_batch_size: usize,
    pub target_batch_bytes: u64,
    pub adaptive_batch_sizing: bool,
    pub max_commit_finalize_pipeline_depth: usize,
    pub max_commit_finalize_phase1_shards: usize,
    pub serializable_bypass_batch_budget: u32,
    pub serializable_bypass_wait_budget_us: u64,
    pub serializable_bypass_adaptive: bool,
    pub serializable_bypass_min_batch_budget: u32,
    pub serializable_bypass_max_batch_budget: u32,
    pub max_commit_queue_depth: usize,
    pub max_commit_queue_bytes: u64,
    pub max_publish_lag_us: u64,
    pub max_unpublished_commit_count: u64,
    pub max_commit_finalize_queue_depth_reject: usize,
    pub max_durable_to_published_bytes_lag: u64,
    pub max_pending_fence_bytes: u64,
    pub max_cleanup_queue_depth: usize,
    pub max_cleanup_queue_bytes: u64,
    pub min_cleanup_queue_reserved_slots: usize,
    pub graceful_shutdown_timeout_us: u64,
}

impl Default for CommitBatchPolicy {
    fn default() -> Self {
        Self {
            max_commit_batch_size: DEFAULT_MAX_COMMIT_BATCH_SIZE,
            max_commit_batch_bytes: DEFAULT_MAX_COMMIT_BATCH_BYTES,
            max_commit_fence_us: DEFAULT_MAX_GROUP_COMMIT_FENCE_US,
            max_fence_rejects_per_drain: DEFAULT_MAX_COMMIT_BATCH_SIZE,
            queue_head_wait_budget_us: DEFAULT_QUEUE_HEAD_WAIT_BUDGET_US,
            drain_owner_coalesce_budget_us: DEFAULT_DRAIN_OWNER_COALESCE_BUDGET_US,
            target_batch_size: DEFAULT_TARGET_COMMIT_BATCH_SIZE,
            target_batch_bytes: DEFAULT_TARGET_COMMIT_BATCH_BYTES,
            adaptive_batch_sizing: false,
            max_commit_finalize_pipeline_depth: 1024,
            max_commit_finalize_phase1_shards: 0,
            serializable_bypass_batch_budget: DEFAULT_SERIALIZABLE_BYPASS_BATCH_BUDGET,
            serializable_bypass_wait_budget_us: DEFAULT_SERIALIZABLE_BYPASS_WAIT_BUDGET_US,
            serializable_bypass_adaptive: false,
            serializable_bypass_min_batch_budget: 1,
            serializable_bypass_max_batch_budget: 64,
            max_commit_queue_depth: DEFAULT_MAX_COMMIT_QUEUE_DEPTH,
            max_commit_queue_bytes: DEFAULT_MAX_COMMIT_QUEUE_BYTES,
            max_publish_lag_us: 0,
            max_unpublished_commit_count: 1024,
            max_commit_finalize_queue_depth_reject: 0,
            max_durable_to_published_bytes_lag: DEFAULT_MAX_DURABLE_TO_PUBLISHED_BYTES_LAG,
            max_pending_fence_bytes: DEFAULT_MAX_PENDING_FENCE_BYTES,
            max_cleanup_queue_depth: 0,
            max_cleanup_queue_bytes: 0,
            min_cleanup_queue_reserved_slots: 0,
            graceful_shutdown_timeout_us: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_US,
        }
    }
}

impl CommitBatchPolicy {
    #[inline]
    pub fn effective_target_batch_size(self) -> usize {
        self.target_batch_size
            .clamp(1, self.max_commit_batch_size.max(1))
    }

    #[inline]
    pub fn effective_target_batch_bytes(self) -> u64 {
        self.target_batch_bytes
            .clamp(1, self.max_commit_batch_bytes.max(1))
    }

    #[inline]
    pub fn graceful_shutdown_timeout(self) -> Duration {
        Duration::from_micros(self.graceful_shutdown_timeout_us)
    }

    pub fn check_enqueue(
        self,
        queue_depth: usize,
        queue_retained_bytes: u64,
        job_retained_bytes: u64,
    ) -> Result<(), CommitQueueBackpressure> {
        if self.max_commit_queue_depth > 0 && queue_depth >= self.max_commit_queue_depth {
            return Err(CommitQueueBackpressure::QueueDepth {
                depth: queue_depth,
                limit: self.max_commit_queue_depth,
            });
        }
        let projected = queue_retained_bytes.saturating_add(job_retained_bytes);
        if self.max_commit_queue_bytes > 0 && projected > self.max_commit_queue_bytes {
            return Err(CommitQueueBackpressure::QueueRetainedBytes {
                bytes: projected,
                limit: self.max_commit_queue_bytes,
            });
        }
        Ok(())
    }

    pub fn check_drain(
        self,
        input: &CommitDrainBackpressureInput<'_>,
    ) -> Result<(), CommitDrainBackpressure> {
        let unpublished = input
            .frontier
            .durable_commit_id
            .into_raw()
            .saturating_sub(input.frontier.published_commit_id.into_raw());
        if self.max_unpublished_commit_count > 0 && unpublished >= self.max_unpublished_commit_count
        {
            return Err(CommitDrainBackpressure::UnpublishedCommitCount {
                count: unpublished,
                limit: self.max_unpublished_commit_count,
            });
        }

        if self.max_durable_to_published_bytes_lag > 0 {
            if let Some(bytes) = input.frontier.durable_to_published_bytes_lag {
                if bytes >= self.max_durable_to_published_bytes_lag {
                    return Err(CommitDrainBackpressure::DurableToPublishedBytes {
                        bytes,
                        limit: self.max_durable_to_published_bytes_lag,
                    });
                }
            }
        }

        if self.max_commit_finalize_queue_depth_reject > 0
            && input.commit_finalize_queue_depth >= self.max_commit_finalize_queue_depth_reject
        {
            return Err(CommitDrainBackpressure::CommitFinalizeQueueDepth {
                depth: input.commit_finalize_queue_depth,
                limit: self.max_commit_finalize_queue_depth_reject,
            });
        }

        if self.max_pending_fence_bytes > 0
            && input.pending_fence_retained_bytes >= self.max_pending_fence_bytes
        {
            return Err(CommitDrainBackpressure::PendingFenceBytes {
                bytes: input.pending_fence_retained_bytes,
                limit: self.max_pending_fence_bytes,
            });
        }

        if self.max_cleanup_queue_depth > 0 && input.cleanup.depth >= self.max_cleanup_queue_depth {
            return Err(CommitDrainBackpressure::CleanupQueueDepth {
                depth: input.cleanup.depth,
                limit: self.max_cleanup_queue_depth,
            });
        }

        if self.max_cleanup_queue_bytes > 0 && input.cleanup.bytes >= self.max_cleanup_queue_bytes {
            return Err(CommitDrainBackpressure::CleanupQueueBytes {
                bytes: input.cleanup.bytes,
                limit: self.max_cleanup_queue_bytes,
            });
        }

        if input.cleanup.reserved_slots_available < self.min_cleanup_queue_reserved_slots {
            return Err(CommitDrainBackpressure::CleanupReservedSlots {
                available: input.cleanup.reserved_slots_available,
                required: self.min_cleanup_queue_reserved_slots,
            });
        }

        Ok(())
    }

    pub fn fence_reject_reason_for_batch(
        self,
        accepted_len: usize,
        accepted_estimated_bytes: u64,
        fence_elapsed_us: u64,
    ) -> Option<CommitFenceRejectReason> {
        if accepted_len >= self.max_commit_batch_size.max(1) {
            return Some(CommitFenceRejectReason::BatchSizeLimit);
        }
        if accepted_estimated_bytes >= self.max_commit_batch_bytes {
            return Some(CommitFenceRejectReason::BatchSizeLimit);
        }
        if fence_elapsed_us >= self.max_commit_fence_us {
            return Some(CommitFenceRejectReason::FenceBudgetExceeded {
                elapsed_us: fence_elapsed_us,
                limit_us: self.max_commit_fence_us,
            });
        }
        None
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupBackpressureSnapshot {
    pub depth: usize,
    pub bytes: u64,
    pub reserved_slots_available: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitDrainBackpressureInput<'a> {
    pub frontier: &'a CommitFrontierSnapshot,
    pub commit_finalize_queue_depth: usize,
    pub pending_fence_retained_bytes: u64,
    pub cleanup: CleanupBackpressureSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitQueueBackpressure {
    QueueDepth { depth: usize, limit: usize },
    QueueRetainedBytes { bytes: u64, limit: u64 },
}

impl fmt::Display for CommitQueueBackpressure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueDepth { depth, limit } => {
                write!(f, "commit queue depth {depth} reached limit {limit}")
            }
            Self::QueueRetainedBytes { bytes, limit } => {
                write!(
                    f,
                    "commit queue retained bytes {bytes} reached limit {limit}"
                )
            }
        }
    }
}

impl std::error::Error for CommitQueueBackpressure {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitDrainBackpressure {
    UnpublishedCommitCount { count: u64, limit: u64 },
    DurableToPublishedBytes { bytes: u64, limit: u64 },
    CommitFinalizeQueueDepth { depth: usize, limit: usize },
    PendingFenceBytes { bytes: u64, limit: u64 },
    CleanupQueueDepth { depth: usize, limit: usize },
    CleanupQueueBytes { bytes: u64, limit: u64 },
    CleanupReservedSlots { available: usize, required: usize },
}

impl fmt::Display for CommitDrainBackpressure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnpublishedCommitCount { count, limit } => {
                write!(f, "unpublished commit count {count} reached limit {limit}")
            }
            Self::DurableToPublishedBytes { bytes, limit } => {
                write!(
                    f,
                    "durable-to-published byte lag {bytes} reached limit {limit}"
                )
            }
            Self::CommitFinalizeQueueDepth { depth, limit } => {
                write!(
                    f,
                    "commit-finalize queue depth {depth} reached limit {limit}"
                )
            }
            Self::PendingFenceBytes { bytes, limit } => {
                write!(
                    f,
                    "pending fence retained bytes {bytes} reached limit {limit}"
                )
            }
            Self::CleanupQueueDepth { depth, limit } => {
                write!(f, "cleanup queue depth {depth} reached limit {limit}")
            }
            Self::CleanupQueueBytes { bytes, limit } => {
                write!(f, "cleanup queue bytes {bytes} reached limit {limit}")
            }
            Self::CleanupReservedSlots {
                available,
                required,
            } => write!(
                f,
                "cleanup reserved slots {available} below required {required}"
            ),
        }
    }
}

impl std::error::Error for CommitDrainBackpressure {}
