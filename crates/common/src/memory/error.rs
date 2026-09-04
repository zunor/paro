// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Memory runtime error types.

use thiserror::Error;

use crate::error::{self as paro_error, ParoError};

use super::MemoryDomain;

/// Result alias for memory runtime operations.
pub type MemoryResult<T> = std::result::Result<T, MemoryError>;

/// Hard memory runtime errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MemoryError {
    /// Logical grant capacity is exhausted.
    #[error("memory quota exhausted in {domain:?}: requested {requested} bytes, available {available} bytes")]
    QuotaExhausted {
        domain: MemoryDomain,
        requested: usize,
        available: usize,
    },

    /// A best-effort non-spillable plan reached its admitted resident cap.
    #[error(
        "runtime-capped plan exhausted memory in {domain:?}: requested {requested} bytes, available {available} bytes; uncapped peak demand {uncapped_peak_memory_upper} bytes has no forward-progress proof"
    )]
    RuntimeCapExhausted {
        domain: MemoryDomain,
        requested: usize,
        available: usize,
        uncapped_peak_memory_upper: u64,
    },

    /// The physical allocator failed after a grant was consumed.
    #[error("physical allocation failed for {bytes} bytes")]
    PhysicalAllocationFailed { bytes: usize },

    /// Reclaim failed to release memory.
    #[error("memory reclaim failed: {message}")]
    ReclaimFailed { message: String },

    /// Progress is blocked on an asynchronous memory action.
    #[error("memory operation blocked: {message}")]
    Blocked { message: String },
}

impl MemoryError {
    pub fn quota_exhausted(domain: MemoryDomain, requested: usize, available: usize) -> Self {
        Self::QuotaExhausted {
            domain,
            requested,
            available,
        }
    }

    pub fn physical_allocation_failed(bytes: usize) -> Self {
        Self::PhysicalAllocationFailed { bytes }
    }

    pub fn reclaim_failed(message: impl Into<String>) -> Self {
        Self::ReclaimFailed {
            message: message.into(),
        }
    }

    pub fn blocked(message: impl Into<String>) -> Self {
        Self::Blocked {
            message: message.into(),
        }
    }
}

impl From<MemoryError> for ParoError {
    fn from(value: MemoryError) -> Self {
        match value {
            MemoryError::QuotaExhausted {
                domain,
                requested,
                available,
            } => paro_error::out_of_memory(format!(
                "memory quota exhausted in {domain:?}: requested {requested} bytes, available {available} bytes"
            )),
            MemoryError::RuntimeCapExhausted {
                domain,
                requested,
                available,
                uncapped_peak_memory_upper,
            } => paro_error::out_of_memory(format!(
                "runtime-capped plan exhausted memory in {domain:?}: requested {requested} bytes, available {available} bytes; uncapped peak demand {uncapped_peak_memory_upper} bytes has no forward-progress proof"
            )),
            MemoryError::PhysicalAllocationFailed { bytes } => {
                paro_error::out_of_memory(format!("physical allocation failed for {bytes} bytes"))
            }
            MemoryError::ReclaimFailed { message } => {
                paro_error::out_of_memory(format!("memory reclaim failed: {message}"))
            }
            MemoryError::Blocked { message } => paro_error::out_of_memory(format!(
                "memory operation is blocked waiting for reclaim: {message}"
            )),
        }
    }
}
