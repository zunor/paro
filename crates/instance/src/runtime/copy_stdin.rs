// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyStdinMetricsSnapshot {
    /// Bytes retained by active COPY FROM STDIN commands in this instance.
    pub current_buffer_bytes: usize,
    /// High-water mark of [`Self::current_buffer_bytes`].
    pub peak_buffer_bytes: usize,
    pub rejected_total: u64,
    pub rejected_total_limit: u64,
    pub rejected_frame_limit: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStdinRejectReason {
    TotalLimit,
    FrameLimit,
}

/// Instance-owned observability for PostgreSQL COPY FROM STDIN ingestion.
#[derive(Debug, Default)]
pub struct CopyStdinMetrics {
    current_buffer_bytes: AtomicUsize,
    peak_buffer_bytes: AtomicUsize,
    rejected_total: AtomicU64,
    rejected_total_limit: AtomicU64,
    rejected_frame_limit: AtomicU64,
}

impl CopyStdinMetrics {
    pub fn observe_buffer_bytes(&self, previous_bytes: usize, bytes: usize) {
        let current_total = if bytes >= previous_bytes {
            let delta = bytes - previous_bytes;
            self.current_buffer_bytes
                .fetch_add(delta, Ordering::Relaxed)
                + delta
        } else {
            let delta = previous_bytes - bytes;
            self.current_buffer_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(delta))
                })
                .unwrap_or(0)
                .saturating_sub(delta)
        };
        self.peak_buffer_bytes
            .fetch_max(current_total, Ordering::Relaxed);
    }

    pub fn finish_buffering(&self, buffer_bytes: usize) {
        if buffer_bytes == 0 {
            return;
        }
        let _ = self.current_buffer_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(buffer_bytes)),
        );
    }

    pub fn record_rejection(&self, reason: CopyStdinRejectReason) {
        self.rejected_total.fetch_add(1, Ordering::Relaxed);
        match reason {
            CopyStdinRejectReason::TotalLimit => {
                self.rejected_total_limit.fetch_add(1, Ordering::Relaxed);
            }
            CopyStdinRejectReason::FrameLimit => {
                self.rejected_frame_limit.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> CopyStdinMetricsSnapshot {
        CopyStdinMetricsSnapshot {
            current_buffer_bytes: self.current_buffer_bytes.load(Ordering::Relaxed),
            peak_buffer_bytes: self.peak_buffer_bytes.load(Ordering::Relaxed),
            rejected_total: self.rejected_total.load(Ordering::Relaxed),
            rejected_total_limit: self.rejected_total_limit.load(Ordering::Relaxed),
            rejected_frame_limit: self.rejected_frame_limit.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_instances_do_not_share_state() {
        let first = CopyStdinMetrics::default();
        let second = CopyStdinMetrics::default();

        first.observe_buffer_bytes(0, 8);
        first.record_rejection(CopyStdinRejectReason::TotalLimit);

        assert_eq!(first.snapshot().current_buffer_bytes, 8);
        assert_eq!(first.snapshot().rejected_total, 1);
        assert_eq!(second.snapshot().current_buffer_bytes, 0);
        assert_eq!(second.snapshot().rejected_total, 0);
    }
}
