// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::routine::artifact::{ArtifactValidationState, RuntimeContract};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactValidationReport {
    pub validated_handler: String,
    pub protocol_version: u16,
    pub abi_version: u16,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ArtifactValidationError {
    #[error(
        "runtime contract mismatch: worker protocol {worker_protocol_version}, abi {abi_version}"
    )]
    ContractMismatch {
        worker_protocol_version: u16,
        abi_version: u16,
    },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ArtifactValidator;

impl ArtifactValidator {
    pub fn validate(
        &self,
        handler: &str,
        artifact_contract: &RuntimeContract,
        expected_contract: &RuntimeContract,
    ) -> Result<ArtifactValidationReport, ArtifactValidationError> {
        if artifact_contract.worker_protocol_version != expected_contract.worker_protocol_version
            || artifact_contract.abi_version != expected_contract.abi_version
        {
            return Err(ArtifactValidationError::ContractMismatch {
                worker_protocol_version: artifact_contract.worker_protocol_version,
                abi_version: artifact_contract.abi_version,
            });
        }

        Ok(ArtifactValidationReport {
            validated_handler: handler.to_string(),
            protocol_version: artifact_contract.worker_protocol_version,
            abi_version: artifact_contract.abi_version,
        })
    }

    pub fn to_validation_state(
        &self,
        result: Result<ArtifactValidationReport, ArtifactValidationError>,
    ) -> ArtifactValidationState {
        match result {
            Ok(report) => ArtifactValidationState::Ready {
                validated_handler: report.validated_handler,
                protocol_version: report.protocol_version,
            },
            Err(error) => ArtifactValidationState::Failed {
                reason: error.to_string(),
            },
        }
    }
}
