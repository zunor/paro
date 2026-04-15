// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

pub mod unsafe_api;

mod array;
mod fixed;
mod list;
mod struct_codec;
mod varlen;

pub use array::ArrayCodec;
pub use list::ListCodec;
pub use struct_codec::StructCodec;
pub use varlen::VarlenCodec;

/// Precompiled scatter/gather strategy for one logical column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnCodec {
    Fixed { size: usize },
    Varlen(VarlenCodec),
    List(ListCodec),
    Array(ArrayCodec),
    Struct(StructCodec),
}

impl ColumnCodec {
    pub fn from_logical_type(logical_type: &LogicalType) -> Self {
        match logical_type {
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
            | LogicalType::StringLiteral => Self::Varlen(VarlenCodec::InlineHeap16),
            LogicalType::List(child) => Self::List(ListCodec::new(Self::from_logical_type(child))),
            LogicalType::Array(child, width) => {
                Self::Array(ArrayCodec::new(Self::from_logical_type(child), *width))
            }
            LogicalType::Struct(fields) => Self::Struct(StructCodec::new(
                fields
                    .iter()
                    .map(|(name, ty)| (name.clone(), Self::from_logical_type(ty)))
                    .collect(),
            )),
            _ => Self::Fixed {
                size: crate::row::RowLayout::get_type_size(logical_type),
            },
        }
    }
}

/// Per-layout codec table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CodecTable {
    codecs: Vec<ColumnCodec>,
}

impl CodecTable {
    pub fn from_types(types: &[LogicalType]) -> Self {
        Self {
            codecs: types.iter().map(ColumnCodec::from_logical_type).collect(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.codecs.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.codecs.is_empty()
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&ColumnCodec> {
        self.codecs.get(index)
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &ColumnCodec> {
        self.codecs.iter()
    }
}

/// Scatter a gathered vector into arbitrary output row positions.
///
/// The vector-shape boundary is handled once here: fixed-width columns decode the
/// source vector before entering size-specialized raw copy, while varlen/nested
/// paths delegate to their deep-copy codecs.
pub(crate) fn scatter_to_positions(
    codec: &ColumnCodec,
    column_idx: usize,
    gathered: &Vector,
    output: &mut Chunk,
    output_positions: &[usize],
) -> Result<()> {
    if gathered.len() < output_positions.len() {
        return Err(paro_error::internal(format!(
            "gathered vector length {} smaller than scatter count {}",
            gathered.len(),
            output_positions.len()
        )));
    }

    let output_column_count = output.column_count();
    let output_vector = output.column_mut(column_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "output column {} out of range {}",
            column_idx, output_column_count
        ))
    })?;

    match codec {
        ColumnCodec::Fixed { size } => {
            fixed::scatter_fixed(*size, gathered, output_vector, output_positions)
        }
        ColumnCodec::Varlen(varlen_codec) => {
            varlen::scatter(varlen_codec, gathered, output_vector, output_positions)
        }
        ColumnCodec::List(list_codec) => {
            list::scatter(list_codec, gathered, output_vector, output_positions)
        }
        ColumnCodec::Array(array_codec) => {
            array::scatter(array_codec, gathered, output_vector, output_positions)
        }
        ColumnCodec::Struct(struct_codec) => {
            struct_codec::scatter(struct_codec, gathered, output_vector, output_positions)
        }
    }
}
