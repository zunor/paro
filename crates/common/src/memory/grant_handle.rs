// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Minimal grant interface for storage-side data structures.

use super::MemoryResult;

/// Minimal memory grant interface.
pub trait MemoryGrantHandle {
    fn try_consume(&self, bytes: usize) -> MemoryResult<()>;
    fn refund(&self, bytes: usize);
    fn available_bytes(&self) -> usize;
}

impl MemoryGrantHandle for super::MemoryGrant {
    fn try_consume(&self, bytes: usize) -> MemoryResult<()> {
        super::MemoryGrant::try_consume(self, bytes)
    }

    fn refund(&self, bytes: usize) {
        super::MemoryGrant::refund(self, bytes)
    }

    fn available_bytes(&self) -> usize {
        super::MemoryGrant::available_bytes(self)
    }
}
