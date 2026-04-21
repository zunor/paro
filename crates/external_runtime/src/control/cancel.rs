// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelStage {
    Requested,
    WorkerMarkedCancelled,
    PythonInterrupted,
    ForceTerminate,
    WorkerRetired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelAction {
    SendWorkerCancel,
    InterruptPythonSafepoint,
    ForceTerminateWorker,
    RetireWorker,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelEscalationPolicy {
    pub python_interrupt_grace_ms: u64,
    pub force_terminate_grace_ms: u64,
}

impl Default for CancelEscalationPolicy {
    fn default() -> Self {
        Self {
            python_interrupt_grace_ms: 100,
            force_terminate_grace_ms: 500,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CancelEscalationError {
    #[error("cancel escalation cannot move from {from:?} to {to:?}")]
    InvalidTransition { from: CancelStage, to: CancelStage },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelEscalation {
    pub batch_id: u64,
    pub stage: CancelStage,
    pub requested_at_ms: u64,
    pub last_progress_at_ms: u64,
}

impl CancelEscalation {
    pub fn new(batch_id: u64, requested_at_ms: u64) -> Self {
        Self {
            batch_id,
            stage: CancelStage::Requested,
            requested_at_ms,
            last_progress_at_ms: requested_at_ms,
        }
    }

    pub fn advance_to(
        &mut self,
        next: CancelStage,
        now_ms: u64,
    ) -> Result<(), CancelEscalationError> {
        let allowed = matches!(
            (self.stage, next),
            (CancelStage::Requested, CancelStage::WorkerMarkedCancelled)
                | (
                    CancelStage::WorkerMarkedCancelled,
                    CancelStage::PythonInterrupted
                )
                | (CancelStage::PythonInterrupted, CancelStage::ForceTerminate)
                | (CancelStage::ForceTerminate, CancelStage::WorkerRetired)
        );
        if !allowed {
            return Err(CancelEscalationError::InvalidTransition {
                from: self.stage,
                to: next,
            });
        }
        self.stage = next;
        self.last_progress_at_ms = now_ms;
        Ok(())
    }

    pub fn next_action(&self, now_ms: u64, policy: &CancelEscalationPolicy) -> CancelAction {
        match self.stage {
            CancelStage::Requested => CancelAction::SendWorkerCancel,
            CancelStage::WorkerMarkedCancelled => {
                if now_ms.saturating_sub(self.last_progress_at_ms)
                    >= policy.python_interrupt_grace_ms
                {
                    CancelAction::InterruptPythonSafepoint
                } else {
                    CancelAction::None
                }
            }
            CancelStage::PythonInterrupted => {
                if now_ms.saturating_sub(self.last_progress_at_ms)
                    >= policy.force_terminate_grace_ms
                {
                    CancelAction::ForceTerminateWorker
                } else {
                    CancelAction::None
                }
            }
            CancelStage::ForceTerminate => CancelAction::RetireWorker,
            CancelStage::WorkerRetired => CancelAction::None,
        }
    }
}
