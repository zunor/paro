// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Executable memory contracts shared by physical planning and admission.
//!
//! A spill capability is not itself a memory proof. Each implementation
//! declares the resident state required to make progress, task-local scratch
//! at its maximum admitted concurrency, and the revocable working-set target.

use paro_common::error::{self as paro_error, Result};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionMemoryContract {
    pub fixed_non_revocable_bytes: u64,
    pub fixed_scratch_bytes: u64,
    pub per_task_scratch_bytes: u64,
    pub max_concurrent_tasks: u16,
    pub revocable_minimum_bytes: u64,
    pub revocable_target_bytes: u64,
    pub spill_buffer_minimum_bytes: u64,
}

impl ExecutionMemoryContract {
    pub fn minimum_memory_bytes(self) -> Result<u64> {
        self.fixed_non_revocable_bytes
            .checked_add(self.fixed_scratch_bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.per_task_scratch_bytes
                        .checked_mul(u64::from(self.max_concurrent_tasks))?,
                )
            })
            .and_then(|bytes| bytes.checked_add(self.revocable_minimum_bytes))
            .and_then(|bytes| bytes.checked_add(self.spill_buffer_minimum_bytes))
            .ok_or_else(|| paro_error::internal("execution memory floor overflow"))
    }

    pub fn preferred_memory_bytes(self) -> Result<u64> {
        self.minimum_memory_bytes()?
            .checked_add(self.revocable_target_bytes)
            .ok_or_else(|| paro_error::internal("execution memory target overflow"))
    }

    pub fn validate(self) -> Result<()> {
        if self.max_concurrent_tasks == 0 && self.per_task_scratch_bytes != 0 {
            return Err(paro_error::internal(
                "execution memory contract has task scratch without task concurrency",
            ));
        }
        let _ = self.preferred_memory_bytes()?;
        Ok(())
    }
}

/// Resident floor for one external blocking task: two vector/encoding blocks,
/// one row-store spill page, and fixed writer/state metadata.
pub const BLOCKING_FIXED_SCRATCH_BYTES: u64 = 64 * 1024;
pub const BLOCKING_PER_TASK_SCRATCH_BYTES: u64 =
    (paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE as u64) * 2;
pub const SPILL_BUFFER_MINIMUM_BYTES: u64 = paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE as u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_blocking_task_declares_an_executable_floor() {
        let contract = ExecutionMemoryContract {
            fixed_non_revocable_bytes: 0,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: BLOCKING_PER_TASK_SCRATCH_BYTES,
            max_concurrent_tasks: 1,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: SPILL_BUFFER_MINIMUM_BYTES,
        };
        let floor = contract.minimum_memory_bytes().unwrap();

        assert!(floor > 512 * 1024);
        assert!(floor <= 1024 * 1024);
    }
}
