// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::data_plane::arena::SharedArena;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReclaimResult {
    pub reclaimed_leases: usize,
    pub retired_workers: usize,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CrashReclaimer;

impl CrashReclaimer {
    pub fn reclaim_worker_epoch(
        &self,
        arena: &mut SharedArena,
        worker_epoch: u64,
    ) -> CrashReclaimResult {
        let reclaimed = arena.reclaim_worker_epoch(worker_epoch);
        CrashReclaimResult {
            reclaimed_leases: reclaimed,
            retired_workers: usize::from(reclaimed > 0),
        }
    }

    pub fn reclaim_host_epoch(
        &self,
        arena: &mut SharedArena,
        host_epoch: u64,
    ) -> CrashReclaimResult {
        let reclaimed = arena.reclaim_host_epoch(host_epoch);
        CrashReclaimResult {
            reclaimed_leases: reclaimed,
            retired_workers: usize::from(reclaimed > 0),
        }
    }
}
