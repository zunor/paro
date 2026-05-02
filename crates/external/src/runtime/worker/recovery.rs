// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};

use crate::runtime::data_plane::arena::SharedArena;
use crate::runtime::worker::pool::WorkerPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerRecoveryAction {
    HardRetireAndReHandshake,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochMismatchRecovery {
    pub worker_id: u64,
    pub reclaimed_host_leases: usize,
    pub reclaimed_query_leases: usize,
    pub action: WorkerRecoveryAction,
}

pub fn recover_epoch_mismatch(
    arena: &mut SharedArena,
    pool: &mut WorkerPool,
    worker_id: u64,
    expected_host_epoch: u64,
    expected_query_epoch: u64,
) -> Result<EpochMismatchRecovery> {
    let reclaimed_host_leases = arena.reclaim_host_epoch(expected_host_epoch);
    let reclaimed_query_leases = arena.reclaim_query_epoch(expected_query_epoch);
    pool.retire(worker_id, true)
        .map_err(|error| paro_error::internal(error.to_string()))?;
    Ok(EpochMismatchRecovery {
        worker_id,
        reclaimed_host_leases,
        reclaimed_query_leases,
        action: WorkerRecoveryAction::HardRetireAndReHandshake,
    })
}
