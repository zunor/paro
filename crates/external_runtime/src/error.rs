// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, ParoError};
use paro_external_abi::LeaseError;

use crate::protocol::messages::PythonExceptionPayload;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalRoutineErrorKind {
    QueryCancelled,
    StatementTimeout,
    HostContractViolation {
        message: String,
    },
    PythonException(PythonExceptionPayload),
    WorkerCrash {
        worker_id: Option<u64>,
        message: String,
    },
    ProtocolMismatch {
        message: String,
    },
    EpochMismatch {
        lease_id: u64,
        expected_host_epoch: u64,
        expected_query_epoch: u64,
        actual_host_epoch: u64,
        actual_query_epoch: u64,
    },
    SandboxViolation {
        message: String,
    },
}

impl ExternalRoutineErrorKind {
    pub fn to_paro_error(&self) -> ParoError {
        match self {
            ExternalRoutineErrorKind::QueryCancelled => paro_error::query_canceled(),
            ExternalRoutineErrorKind::StatementTimeout => paro_error::statement_timeout(),
            ExternalRoutineErrorKind::HostContractViolation { message } => {
                paro_error::contract_violation(message.clone())
            }
            ExternalRoutineErrorKind::PythonException(payload) => {
                let mut err = paro_error::python_exception(payload.message.clone())
                    .detail(payload.formatted_traceback.clone())
                    .context(format!(
                        "external routine {}::{} batch {}",
                        payload.module, payload.handler, payload.batch_id
                    ));
                if payload.truncated {
                    err = err.hint(
                        "Python traceback was truncated to fit the sideband error payload budget.",
                    );
                }
                err
            }
            ExternalRoutineErrorKind::WorkerCrash { worker_id, message } => {
                let mut err = paro_error::worker_failure(message.clone());
                if let Some(worker_id) = worker_id {
                    err = err.detail(format!("worker_id={worker_id}"));
                }
                err
            }
            ExternalRoutineErrorKind::ProtocolMismatch { message } => {
                paro_error::external_protocol_mismatch(message.clone())
            }
            ExternalRoutineErrorKind::EpochMismatch {
                lease_id,
                expected_host_epoch,
                expected_query_epoch,
                actual_host_epoch,
                actual_query_epoch,
            } => paro_error::epoch_mismatch(format!(
                "lease {lease_id} visible to host epoch {expected_host_epoch} / query epoch {expected_query_epoch}, found host {actual_host_epoch} / query {actual_query_epoch}"
            )),
            ExternalRoutineErrorKind::SandboxViolation { message } => {
                paro_error::sandbox_violation(message.clone())
            }
        }
    }
}

impl From<LeaseError> for ExternalRoutineErrorKind {
    fn from(value: LeaseError) -> Self {
        match value {
            LeaseError::InvalidTransition { lease_id, from, to } => {
                ExternalRoutineErrorKind::HostContractViolation {
                    message: format!("lease {lease_id} cannot transition from {from:?} to {to:?}"),
                }
            }
            LeaseError::InvalidDescriptor { lease_id, source } => {
                ExternalRoutineErrorKind::HostContractViolation {
                    message: format!("lease {lease_id} descriptor validation failed: {source}"),
                }
            }
            LeaseError::EpochMismatch {
                lease_id,
                expected_host_epoch,
                expected_query_epoch,
                actual_host_epoch,
                actual_query_epoch,
            } => ExternalRoutineErrorKind::EpochMismatch {
                lease_id,
                expected_host_epoch,
                expected_query_epoch,
                actual_host_epoch,
                actual_query_epoch,
            },
        }
    }
}
