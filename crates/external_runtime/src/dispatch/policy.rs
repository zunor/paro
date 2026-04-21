// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalDispatchPolicy {
    pub target_batch_bytes: u64,
    pub max_accumulation_bytes: u64,
    pub min_batch_rows: usize,
    pub max_batch_rows: usize,
    pub max_queue_depth_per_shard: usize,
    pub local_spin_budget_us: u64,
    pub worker_acquire_timeout_ms: u64,
    pub transport_retry_budget: usize,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DispatchPolicyError {
    #[error("min_batch_rows cannot exceed max_batch_rows")]
    InvalidBatchBounds,
    #[error("target_batch_bytes and max_accumulation_bytes must be positive")]
    InvalidByteBudget,
}

impl Default for ExternalDispatchPolicy {
    fn default() -> Self {
        Self {
            target_batch_bytes: 256 * 1024,
            max_accumulation_bytes: 4 * 1024 * 1024,
            min_batch_rows: 1,
            max_batch_rows: 16_384,
            max_queue_depth_per_shard: 8,
            local_spin_budget_us: 50,
            worker_acquire_timeout_ms: 500,
            transport_retry_budget: 1,
        }
    }
}

impl ExternalDispatchPolicy {
    pub fn validate(&self) -> Result<(), DispatchPolicyError> {
        if self.min_batch_rows > self.max_batch_rows {
            return Err(DispatchPolicyError::InvalidBatchBounds);
        }
        if self.target_batch_bytes == 0 || self.max_accumulation_bytes == 0 {
            return Err(DispatchPolicyError::InvalidByteBudget);
        }
        Ok(())
    }

    pub fn suggest_batch_rows(&self, estimated_row_bytes: u64, buffered_bytes: u64) -> usize {
        let row_bytes = estimated_row_bytes.max(1);
        let budget = self.target_batch_bytes.min(
            self.max_accumulation_bytes
                .saturating_sub(buffered_bytes)
                .max(row_bytes),
        );
        let rows = (budget / row_bytes).max(self.min_batch_rows as u64);
        rows.clamp(self.min_batch_rows as u64, self.max_batch_rows as u64) as usize
    }
}
