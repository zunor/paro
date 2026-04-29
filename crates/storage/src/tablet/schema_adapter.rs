// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepare-stage schema adaptation for retained rowsets.

use crate::rowset::RowsetSharedPtr;
use crate::tablet::{ColumnId, TabletColumn, TabletSchemaRef};
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum SchemaColumnFill {
    Null(LogicalType),
    Default {
        logical_type: LogicalType,
        value: Value,
    },
}

impl SchemaColumnFill {
    pub(super) fn vector(&self, rows: usize, allocator: Arc<dyn Allocator>) -> Result<Vector> {
        match self {
            SchemaColumnFill::Null(ty) => Vector::try_constant_null(ty.clone(), rows, allocator),
            SchemaColumnFill::Default {
                logical_type,
                value,
            } => Vector::try_constant_from_value(
                logical_type.clone(),
                value.clone(),
                rows,
                allocator,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct RowsetSchemaAdapter {
    pub rowset_id: u64,
    pub schema_version: u32,
    pub physical_schema_token: u64,
    missing_read_columns: HashMap<usize, SchemaColumnFill>,
}

impl RowsetSchemaAdapter {
    pub(super) fn fill_for_read_idx(&self, read_idx: usize) -> Option<&SchemaColumnFill> {
        self.missing_read_columns.get(&read_idx)
    }

    fn is_empty(&self) -> bool {
        self.missing_read_columns.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSchemaAdaptationPlan {
    pub rowset_count: usize,
    pub adapted_rowset_count: usize,
    pub mixed_schema_versions: bool,
    pub mixed_physical_schema_tokens: bool,
    pub schema_version: Option<u32>,
    pub physical_schema_token: Option<u64>,
}

impl TabletSchemaAdaptationPlan {
    pub fn identity(rowset_count: usize, physical_schema_token: Option<u64>) -> Self {
        Self {
            rowset_count,
            adapted_rowset_count: 0,
            mixed_schema_versions: false,
            mixed_physical_schema_tokens: false,
            schema_version: None,
            physical_schema_token,
        }
    }

    pub fn for_snapshot(
        rowsets: &[RowsetSharedPtr],
        schema_version: Option<u32>,
        physical_schema_token: Option<u64>,
        schema_version_consistent: bool,
        physical_schema_token_consistent: bool,
    ) -> Self {
        let mixed_schema_versions = !schema_version_consistent;
        let mixed_physical_schema_tokens = !physical_schema_token_consistent;
        let adapted_rowset_count = if mixed_schema_versions || mixed_physical_schema_tokens {
            rowsets.len()
        } else {
            0
        };

        Self {
            rowset_count: rowsets.len(),
            adapted_rowset_count,
            mixed_schema_versions,
            mixed_physical_schema_tokens,
            schema_version,
            physical_schema_token,
        }
    }

    pub fn adaptation_required(&self) -> bool {
        self.adapted_rowset_count > 0
            || self.mixed_schema_versions
            || self.mixed_physical_schema_tokens
    }
}

pub(super) fn build_reader_schema_adapters(
    target_schema: &TabletSchemaRef,
    rowsets: &[RowsetSharedPtr],
    projection: &[ColumnId],
) -> Result<HashMap<u64, RowsetSchemaAdapter>> {
    let mut adapters = HashMap::new();
    for rowset in rowsets {
        let rowset_schema = rowset.schema();

        let mut missing_read_columns = HashMap::new();
        for (read_idx, column_id) in projection.iter().copied().enumerate() {
            let target_column = target_schema.column_by_id(column_id).ok_or_else(|| {
                paro_error::internal(format!(
                    "projected column {column_id} missing from tablet schema"
                ))
            })?;

            if let Some(rowset_column) = rowset_schema.column_by_id(column_id) {
                if rowset_column.logical_type != target_column.logical_type {
                    return Err(paro_error::type_mismatch(format!(
                        "rowset {} column {} has type {}, tablet expects {}",
                        rowset.rowset_id(),
                        column_id,
                        rowset_column.logical_type,
                        target_column.logical_type
                    )));
                }
                continue;
            }

            missing_read_columns.insert(read_idx, fill_for_added_column(target_column)?);
        }

        let meta = rowset.rowset_meta();
        let adapter = RowsetSchemaAdapter {
            rowset_id: rowset.rowset_id(),
            schema_version: rowset_schema.schema_version(),
            physical_schema_token: meta.schema_hash() as u64,
            missing_read_columns,
        };
        if !adapter.is_empty() {
            adapters.insert(adapter.rowset_id, adapter);
        }
    }
    Ok(adapters)
}

fn fill_for_added_column(column: &TabletColumn) -> Result<SchemaColumnFill> {
    if column.has_default_value {
        let default_bytes = column.default_value.as_deref().ok_or_else(|| {
            paro_error::invalid_input(format!(
                "column {} declares a default value without default bytes",
                column.id
            ))
        })?;
        return Ok(SchemaColumnFill::Default {
            logical_type: column.logical_type.clone(),
            value: decode_default_value(&column.logical_type, default_bytes)?,
        });
    }

    if column.is_nullable {
        return Ok(SchemaColumnFill::Null(column.logical_type.clone()));
    }

    Err(paro_error::not_null_violation(format!(
        "retained rowset is missing non-null column {} without a default value",
        column.id
    )))
}

fn decode_default_value(logical_type: &LogicalType, bytes: &[u8]) -> Result<Value> {
    match logical_type {
        LogicalType::Boolean => Ok(Value::Boolean(match bytes {
            [0] => false,
            [1] => true,
            _ => {
                return Err(invalid_default_width(logical_type, bytes.len(), 1));
            }
        })),
        LogicalType::TinyInt => Ok(Value::TinyInt(i8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 1))?,
        ))),
        LogicalType::SmallInt => Ok(Value::SmallInt(i16::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 2))?,
        ))),
        LogicalType::Integer => Ok(Value::Integer(i32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 4))?,
        ))),
        LogicalType::BigInt => Ok(Value::BigInt(i64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 8))?,
        ))),
        LogicalType::HugeInt => Ok(Value::HugeInt(i128::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 16))?,
        ))),
        LogicalType::UTinyInt => Ok(Value::UTinyInt(u8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 1))?,
        ))),
        LogicalType::USmallInt => Ok(Value::USmallInt(u16::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 2))?,
        ))),
        LogicalType::UInteger => Ok(Value::UInteger(u32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 4))?,
        ))),
        LogicalType::UBigInt => Ok(Value::UBigInt(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 8))?,
        ))),
        LogicalType::UHugeInt => Ok(Value::UHugeInt(u128::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 16))?,
        ))),
        LogicalType::Float => {
            Ok(Value::Float(f32::from_le_bytes(bytes.try_into().map_err(
                |_| invalid_default_width(logical_type, bytes.len(), 4),
            )?)))
        }
        LogicalType::Double => Ok(Value::Double(f64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 8))?,
        ))),
        LogicalType::Decimal { precision, scale } => {
            let raw = match bytes.len() {
                8 => i64::from_le_bytes(bytes.try_into().unwrap()) as i128,
                16 => i128::from_le_bytes(bytes.try_into().unwrap()),
                len => return Err(invalid_default_width(logical_type, len, 16)),
            };
            Ok(Value::Decimal(raw, *precision, *scale))
        }
        LogicalType::Date => {
            Ok(Value::Date(i32::from_le_bytes(bytes.try_into().map_err(
                |_| invalid_default_width(logical_type, bytes.len(), 4),
            )?)))
        }
        LogicalType::Time => {
            Ok(Value::Time(i64::from_le_bytes(bytes.try_into().map_err(
                |_| invalid_default_width(logical_type, bytes.len(), 8),
            )?)))
        }
        LogicalType::Timestamp => Ok(Value::Timestamp(i64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 8))?,
        ))),
        LogicalType::TimestampTz => Ok(Value::TimestampTz(i64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| invalid_default_width(logical_type, bytes.len(), 8))?,
        ))),
        LogicalType::Interval => {
            if bytes.len() != 16 {
                return Err(invalid_default_width(logical_type, bytes.len(), 16));
            }
            let months = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let days = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
            let micros = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
            Ok(Value::Interval(months, days, micros))
        }
        LogicalType::Uuid => {
            Ok(Value::Uuid(u128::from_le_bytes(bytes.try_into().map_err(
                |_| invalid_default_width(logical_type, bytes.len(), 16),
            )?)))
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => Ok(Value::Varchar(String::from_utf8(bytes.to_vec()).map_err(
            |_| {
                paro_error::invalid_input(format!(
                    "default value for {logical_type} is not valid utf-8"
                ))
            },
        )?)),
        LogicalType::Blob => Ok(Value::Blob(bytes.to_vec())),
        unsupported => Err(paro_error::not_implemented(format!(
            "default value decoding for {unsupported} columns"
        ))),
    }
}

fn invalid_default_width(
    logical_type: &LogicalType,
    actual: usize,
    expected: usize,
) -> paro_error::ParoError {
    paro_error::invalid_input(format!(
        "default value for {logical_type} has {actual} bytes, expected {expected}"
    ))
}
