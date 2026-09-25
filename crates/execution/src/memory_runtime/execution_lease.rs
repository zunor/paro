// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-lifetime ownership of every resource selected by portfolio admission.

use paro_common::error::{self as paro_error, Result};
use paro_external::runtime::host::{ExternalDispatchGate, ExternalWorkerLease};
use paro_planner::physical::ExecutionResourceContract;

/// The executable counterpart of an optimizer resource contract.
///
/// Memory ownership is retained by the query's coordinator registration; this
/// value owns the remaining capabilities and keeps the complete operating
/// point inspectable for the same lifetime.
#[derive(Debug)]
pub struct ExecutionLease {
    resources: ExecutionResourceContract,
    external_workers: Option<ExternalWorkerLease>,
}

impl ExecutionLease {
    pub fn new(
        resources: ExecutionResourceContract,
        external_workers: Option<ExternalWorkerLease>,
    ) -> Result<Self> {
        let leased_external_slots = external_workers
            .as_ref()
            .map_or(0, ExternalWorkerLease::slots);
        if resources.max_parallel_tasks == 0
            || resources.minimum_memory_bytes > resources.working_set_memory_bytes
            || resources.working_set_memory_bytes > resources.memory_ceiling_bytes
            || leased_external_slots != resources.external_worker_slots
        {
            return Err(paro_error::internal(
                "execution lease does not materialize its resource contract",
            ));
        }
        Ok(Self {
            resources,
            external_workers,
        })
    }

    pub fn resources(&self) -> ExecutionResourceContract {
        self.resources
    }

    pub fn external_dispatch_gate(&self) -> Option<ExternalDispatchGate> {
        self.external_workers
            .as_ref()
            .map(ExternalWorkerLease::dispatch_gate)
    }
}
