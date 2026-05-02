// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::routine::spec::{RoutineSemantics, RoutineSideEffects, RoutineStability};

use crate::runtime::control::state_machine::{SubmissionLifecycle, SubmissionState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryFailureKind {
    DispatchUnavailable,
    WorkerCrash,
    ProtocolMismatch,
    QueryCancelled,
    StatementTimeout,
    PythonException,
    HostContractViolation,
    SandboxViolation,
    EpochMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryDecision {
    pub transparent: bool,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_transparent_retries: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_transparent_retries: 1,
        }
    }
}

impl RetryPolicy {
    pub fn decide(
        &self,
        lifecycle: &SubmissionLifecycle,
        semantics: &RoutineSemantics,
        failure: RetryFailureKind,
    ) -> RetryDecision {
        if lifecycle.state != SubmissionState::Submitted {
            return RetryDecision {
                transparent: false,
                reason: "transparent retry is only legal before worker execution starts",
            };
        }
        if lifecycle.retry_count >= self.max_transparent_retries {
            return RetryDecision {
                transparent: false,
                reason: "transparent retry budget exhausted",
            };
        }
        if semantics.stability == RoutineStability::Volatile
            || semantics.side_effects != RoutineSideEffects::None
        {
            return RetryDecision {
                transparent: false,
                reason: "volatile or side-effecting routines are never transparently retried",
            };
        }

        match failure {
            RetryFailureKind::DispatchUnavailable
            | RetryFailureKind::WorkerCrash
            | RetryFailureKind::ProtocolMismatch => RetryDecision {
                transparent: true,
                reason: "dispatch-level failure is eligible for transparent retry",
            },
            RetryFailureKind::QueryCancelled => RetryDecision {
                transparent: false,
                reason: "query cancellation is terminal",
            },
            RetryFailureKind::StatementTimeout => RetryDecision {
                transparent: false,
                reason: "statement timeout is terminal",
            },
            RetryFailureKind::PythonException => RetryDecision {
                transparent: false,
                reason: "user code exceptions are never transparently retried",
            },
            RetryFailureKind::HostContractViolation => RetryDecision {
                transparent: false,
                reason: "contract violations are terminal",
            },
            RetryFailureKind::SandboxViolation => RetryDecision {
                transparent: false,
                reason: "sandbox violations are terminal",
            },
            RetryFailureKind::EpochMismatch => RetryDecision {
                transparent: false,
                reason: "epoch mismatches require lease reclaim and worker retirement",
            },
        }
    }
}
