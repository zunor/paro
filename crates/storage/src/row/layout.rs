// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;

use crate::row::codec::CodecTable;
use crate::row::raw::{RawRowLayout, RawRowNestednessType, RawRowValidityType};

/// Whether row storage reserves validity bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowValidityType {
    /// Columns can have NULL values.
    #[default]
    CanHaveNullValues,
    /// The layout can skip the row-front validity mask.
    CannotHaveNullValues,
}

impl RowValidityType {
    pub(crate) fn to_tuple(self) -> RawRowValidityType {
        match self {
            RowValidityType::CanHaveNullValues => RawRowValidityType::CanHaveNullValues,
            RowValidityType::CannotHaveNullValues => RawRowValidityType::CannotHaveNullValues,
        }
    }
}

/// Validity mask placement for a row layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowValidityLayout {
    column_count: usize,
    flag_width: usize,
    validity_type: RowValidityType,
}

impl RowValidityLayout {
    fn new(column_count: usize, validity_type: RowValidityType) -> Self {
        let validity_count = if validity_type == RowValidityType::CannotHaveNullValues {
            0
        } else {
            column_count
        };
        Self {
            column_count,
            flag_width: RowLayout::validity_mask_size(validity_count),
            validity_type,
        }
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.column_count
    }

    #[inline]
    pub fn flag_width(&self) -> usize {
        self.flag_width
    }

    #[inline]
    pub fn validity_type(&self) -> RowValidityType {
        self.validity_type
    }
}

/// Execution-time row layout without sort-key state.
///
/// This is intentionally distinct from the persistent `rowset` layout and from the
/// legacy `RawRowLayout`: it has no sort-key payload slot or sort-key dispatch.
#[derive(Debug, Clone)]
pub struct RowLayout {
    types: Vec<LogicalType>,
    codecs: CodecTable,
    offsets: Vec<usize>,
    validity: RowValidityLayout,
    data_width: usize,
    row_width: usize,
    all_constant: bool,
    variable_columns: Vec<usize>,
    heap_size_offset: Option<usize>,
}

impl RowLayout {
    /// Create an empty row layout.
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
            codecs: CodecTable::default(),
            offsets: Vec::new(),
            validity: RowValidityLayout::new(0, RowValidityType::CanHaveNullValues),
            data_width: 0,
            row_width: 0,
            all_constant: true,
            variable_columns: Vec::new(),
            heap_size_offset: None,
        }
    }

    /// Build a top-level row layout.
    pub fn from_types(types: Vec<LogicalType>, validity_type: RowValidityType) -> Self {
        let mut layout = Self::new();
        layout.initialize(types, validity_type);
        layout
    }

    /// Initialize this layout.
    pub fn initialize(&mut self, types: Vec<LogicalType>, validity_type: RowValidityType) {
        self.types = types;
        self.codecs = CodecTable::from_types(&self.types);
        self.offsets.clear();
        self.variable_columns.clear();
        self.validity = RowValidityLayout::new(self.types.len(), validity_type);
        self.row_width = self.validity.flag_width();
        self.all_constant = true;

        for (col_idx, typ) in self.types.iter().enumerate() {
            if !Self::type_is_constant_size(typ) {
                self.all_constant = false;
                self.variable_columns.push(col_idx);
            }
        }

        if !self.all_constant {
            self.heap_size_offset = Some(self.row_width);
            self.row_width += std::mem::size_of::<u64>();
        } else {
            self.heap_size_offset = None;
        }

        for typ in &self.types {
            self.offsets.push(self.row_width);
            self.row_width += Self::get_type_size(typ);
        }
        self.data_width = self.row_width - self.validity.flag_width();
    }

    /// Build a nested struct layout.
    pub fn struct_layout(fields: &[(String, LogicalType)]) -> Self {
        let mut layout = Self::new();
        layout.types = fields.iter().map(|(_, ty)| ty.clone()).collect();
        layout.codecs = CodecTable::from_types(&layout.types);
        layout.validity =
            RowValidityLayout::new(layout.types.len(), RowValidityType::CanHaveNullValues);
        layout.row_width = layout.validity.flag_width();
        layout.all_constant = true;
        for typ in &layout.types {
            if !Self::type_is_constant_size(typ) {
                layout.all_constant = false;
                layout.variable_columns.push(layout.offsets.len());
            }
        }
        if !layout.all_constant {
            layout.heap_size_offset = Some(layout.row_width);
            layout.row_width += std::mem::size_of::<u64>();
        } else {
            layout.heap_size_offset = None;
        }
        for typ in &layout.types {
            layout.offsets.push(layout.row_width);
            layout.row_width += Self::get_type_size(typ);
        }
        layout.data_width = layout.row_width - layout.validity.flag_width();
        layout
    }

    pub(crate) fn to_raw_layout(&self) -> RawRowLayout {
        let mut raw_layout = RawRowLayout::new();
        raw_layout.initialize_with_nestedness(
            self.types.clone(),
            self.validity.validity_type().to_tuple(),
            RawRowNestednessType::TopLevelLayout,
        );
        raw_layout
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.types.len()
    }

    #[inline]
    pub fn types(&self) -> &[LogicalType] {
        &self.types
    }

    #[inline]
    pub fn offsets(&self) -> &[usize] {
        &self.offsets
    }

    #[inline]
    pub fn codecs(&self) -> &CodecTable {
        &self.codecs
    }

    #[inline]
    pub fn validity(&self) -> RowValidityLayout {
        self.validity
    }

    #[inline]
    pub fn row_width(&self) -> usize {
        self.row_width
    }

    #[inline]
    pub fn data_width(&self) -> usize {
        self.data_width
    }

    #[inline]
    pub fn all_constant(&self) -> bool {
        self.all_constant
    }

    #[inline]
    pub fn variable_columns(&self) -> &[usize] {
        &self.variable_columns
    }

    #[inline]
    pub fn heap_size_offset(&self) -> Option<usize> {
        self.heap_size_offset
    }

    #[inline]
    pub fn all_valid(&self) -> bool {
        self.validity.validity_type() == RowValidityType::CannotHaveNullValues
    }

    #[inline]
    pub fn validity_mask_size(column_count: usize) -> usize {
        if column_count == 0 {
            0
        } else {
            column_count.div_ceil(8)
        }
    }

    pub fn type_is_constant_size(typ: &LogicalType) -> bool {
        !matches!(
            typ,
            LogicalType::Varchar
                | LogicalType::VarcharCollation(_)
                | LogicalType::TsVector
                | LogicalType::TsQuery
                | LogicalType::Json
                | LogicalType::Jsonb
                | LogicalType::Blob
                | LogicalType::List(_)
                | LogicalType::Struct(_)
                | LogicalType::Array(_, _)
        )
    }

    /// Storage width of a row cell.
    ///
    /// This follows the vector physical width. In particular, `DATE` is 4 bytes
    /// and `Decimal(precision <= 18)` is 8 bytes.
    pub fn get_type_size(typ: &LogicalType) -> usize {
        match typ {
            LogicalType::Boolean | LogicalType::TinyInt | LogicalType::UTinyInt => 1,
            LogicalType::SmallInt | LogicalType::USmallInt => 2,
            LogicalType::Integer
            | LogicalType::UInteger
            | LogicalType::Float
            | LogicalType::Date => 4,
            LogicalType::BigInt
            | LogicalType::UBigInt
            | LogicalType::Double
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => 8,
            LogicalType::HugeInt
            | LogicalType::UHugeInt
            | LogicalType::Uuid
            | LogicalType::Interval => 16,
            LogicalType::Decimal { precision, .. } => {
                if *precision <= 18 {
                    8
                } else {
                    16
                }
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => 16,
            LogicalType::List(_) | LogicalType::Array(_, _) => std::mem::size_of::<usize>(),
            LogicalType::Struct(fields) => {
                let mut size = Self::validity_mask_size(fields.len());
                for (_, field_type) in fields {
                    size += Self::get_type_size(field_type);
                }
                size
            }
            LogicalType::Null | LogicalType::Unknown => 1,
            LogicalType::IntegerLiteral(_) => 8,
            LogicalType::StringLiteral => 16,
        }
    }
}

impl Default for RowLayout {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_layout_uses_physical_widths_without_sort_key_state() {
        let layout = RowLayout::from_types(
            vec![
                LogicalType::Date,
                LogicalType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                LogicalType::Decimal {
                    precision: 19,
                    scale: 2,
                },
                LogicalType::Varchar,
            ],
            RowValidityType::CanHaveNullValues,
        );

        assert_eq!(RowLayout::get_type_size(&LogicalType::Date), 4);
        assert_eq!(
            RowLayout::get_type_size(&LogicalType::Decimal {
                precision: 18,
                scale: 2
            }),
            8
        );
        assert_eq!(
            RowLayout::get_type_size(&LogicalType::Decimal {
                precision: 19,
                scale: 2
            }),
            16
        );
        assert_eq!(layout.validity().flag_width(), 1);
        assert_eq!(layout.heap_size_offset(), Some(1));
        assert!(!layout.all_constant());
    }
}
