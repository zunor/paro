// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::encoding::{ColumnEncoding, ColumnPopulationMode};
use super::layout::{BufferLease, ColumnLayout};
use super::types::AbiLogicalType;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ColumnDescriptorError {
    #[error("column `{name}` uses encoding {encoding:?} but layout {layout:?}")]
    EncodingLayoutMismatch {
        name: String,
        encoding: ColumnEncoding,
        layout: ColumnLayout,
    },
    #[error("column `{name}` expects a validity bitmap buffer lease for nullable data")]
    MissingValidity { name: String },
    #[error("column `{name}` has invalid child layout")]
    InvalidChildren { name: String },
    #[error("column `{name}` has stride {stride} that does not match logical type")]
    InvalidStride { name: String, stride: u32 },
    #[error("column `{name}` constant value type does not match logical type")]
    ConstantTypeMismatch { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub name: String,
    pub logical_type: AbiLogicalType,
    pub encoding: ColumnEncoding,
    pub population_mode: ColumnPopulationMode,
    pub nullable: bool,
    pub validity: Option<BufferLease>,
    pub layout: ColumnLayout,
    pub children: Vec<ColumnDescriptor>,
}

impl ColumnDescriptor {
    pub fn validate(&self) -> Result<(), ColumnDescriptorError> {
        if self.nullable && self.validity.is_none() {
            return Err(ColumnDescriptorError::MissingValidity {
                name: self.name.clone(),
            });
        }

        match (&self.encoding, &self.layout, &self.logical_type) {
            (ColumnEncoding::Flat, ColumnLayout::FixedWidth { stride, .. }, logical_type) => {
                if let Some(expected) = logical_type.fixed_width_bytes() {
                    if *stride != expected {
                        return Err(ColumnDescriptorError::InvalidStride {
                            name: self.name.clone(),
                            stride: *stride,
                        });
                    }
                }
            }
            (
                ColumnEncoding::Flat,
                ColumnLayout::VarLen { .. },
                AbiLogicalType::Varchar
                | AbiLogicalType::Blob
                | AbiLogicalType::Json
                | AbiLogicalType::Jsonb,
            ) => {}
            (ColumnEncoding::List, ColumnLayout::List { .. }, AbiLogicalType::List(_)) => {
                if self.children.len() != 1 {
                    return Err(ColumnDescriptorError::InvalidChildren {
                        name: self.name.clone(),
                    });
                }
            }
            (ColumnEncoding::Struct, ColumnLayout::Struct, AbiLogicalType::Struct(fields)) => {
                if self.children.len() != fields.len() {
                    return Err(ColumnDescriptorError::InvalidChildren {
                        name: self.name.clone(),
                    });
                }
            }
            (
                ColumnEncoding::Dictionary,
                ColumnLayout::Dictionary { dictionary, .. },
                _logical_type,
            ) => {
                dictionary.validate()?;
            }
            (ColumnEncoding::Sequence, ColumnLayout::Sequence { .. }, _) => {}
            (ColumnEncoding::Constant, ColumnLayout::Constant { value }, logical_type) => {
                if let Some(constant_type) = value.logical_type() {
                    if &constant_type != logical_type {
                        return Err(ColumnDescriptorError::ConstantTypeMismatch {
                            name: self.name.clone(),
                        });
                    }
                }
            }
            _ => {
                return Err(ColumnDescriptorError::EncodingLayoutMismatch {
                    name: self.name.clone(),
                    encoding: self.encoding,
                    layout: self.layout.clone(),
                });
            }
        }

        for child in &self.children {
            child.validate()?;
        }

        Ok(())
    }
}
