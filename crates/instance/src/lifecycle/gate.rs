// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum InstanceLifecycleGateState {
    Running = 0,
    ShuttingDown = 1,
    ShutDown = 2,
}

#[derive(Debug)]
pub(crate) struct InstanceLifecycleGate {
    state: AtomicU8,
}

impl Default for InstanceLifecycleGate {
    fn default() -> Self {
        Self::new()
    }
}

impl InstanceLifecycleGate {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(InstanceLifecycleGateState::Running as u8),
        }
    }

    pub(crate) fn state(&self) -> InstanceLifecycleGateState {
        match self.state.load(Ordering::Acquire) {
            0 => InstanceLifecycleGateState::Running,
            1 => InstanceLifecycleGateState::ShuttingDown,
            2 => InstanceLifecycleGateState::ShutDown,
            _ => InstanceLifecycleGateState::ShutDown,
        }
    }

    pub(crate) fn request_shutdown(&self) -> Result<()> {
        loop {
            match self.state() {
                InstanceLifecycleGateState::Running => {
                    if self
                        .state
                        .compare_exchange(
                            InstanceLifecycleGateState::Running as u8,
                            InstanceLifecycleGateState::ShuttingDown as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                InstanceLifecycleGateState::ShuttingDown => return Ok(()),
                InstanceLifecycleGateState::ShutDown => {
                    return Err(
                        paro_error::cannot_connect_now().detail("instance has been shut down")
                    )
                }
            }
        }
    }

    pub(crate) fn mark_shut_down(&self) {
        self.state.store(
            InstanceLifecycleGateState::ShutDown as u8,
            Ordering::Release,
        );
    }
}
