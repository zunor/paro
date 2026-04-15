// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::ddl_lock::InstanceDdlOwner;
use super::gate::{InstanceLifecycleGate, InstanceLifecycleGateState};
use crate::ValidChecker;
use paro_common::error::{self as paro_error, Result};

/// Joint admission check for shutdown state and fatal invalidation.
#[derive(Debug)]
pub struct AdmissionController {
    gate: InstanceLifecycleGate,
    valid_checker: ValidChecker,
}

impl AdmissionController {
    pub(crate) fn new(valid_checker: ValidChecker) -> Self {
        Self {
            gate: InstanceLifecycleGate::new(),
            valid_checker,
        }
    }

    pub fn check(&self, ddl_owner: Option<InstanceDdlOwner>) -> Result<()> {
        match self.gate.state() {
            InstanceLifecycleGateState::Running => {}
            InstanceLifecycleGateState::ShuttingDown
                if ddl_owner == Some(InstanceDdlOwner::Shutdown) =>
            {
                return Ok(());
            }
            InstanceLifecycleGateState::ShuttingDown => {
                return Err(paro_error::cannot_connect_now().detail("instance is shutting down"));
            }
            InstanceLifecycleGateState::ShutDown => {
                return Err(paro_error::admin_shutdown().detail("instance has been shut down"));
            }
        }

        if let Err(message) = self.valid_checker.check_valid() {
            return Err(paro_error::cannot_connect_now().detail(message));
        }

        Ok(())
    }

    pub fn request_shutdown(&self) -> Result<()> {
        self.gate.request_shutdown()
    }

    pub fn is_invalidated(&self) -> bool {
        self.valid_checker.is_invalidated()
    }

    pub fn invalidate(&self, message: String) {
        self.valid_checker.invalidate(message);
    }

    pub fn terminate(&self) {
        self.invalidate("instance has been shut down".to_string());
        self.gate.mark_shut_down();
    }
}
