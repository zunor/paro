//! Raw physical row layout used by the execution-time row substrate.

use paro_common::types::LogicalType;

/// Whether the layout can have NULL values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RawRowValidityType {
    /// Columns can have NULL values (need validity bytes)
    #[default]
    CanHaveNullValues,
    /// Columns cannot have NULL values (skip validity bytes)
    CannotHaveNullValues,
}

/// Whether this is a top-level or nested layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RawRowNestednessType {
    /// Top-level layout (includes heap_size_offset if needed)
    #[default]
    TopLevelLayout,
    /// Nested struct layout (no heap_size_offset)
    NestedStructLayout,
}

/// Row layout for raw row storage.
///
/// Calculates offsets and sizes for storing rows in a compact format.
/// Used by RawRowCollection for Hash Join, Hash Aggregate, etc.
///
/// # Row Memory Layout
/// ```text
/// ┌─────────────┬─────────────┬─────────────┬─────────────┐
/// │ Validity    │ Heap Size   │ Col0 Data   │ Col1 Data   │
/// │ (N bits)    │ (if needed) │ (aligned)   │ (aligned)   │
/// └─────────────┴─────────────┴─────────────┴─────────────┘
/// ```
#[derive(Debug, Clone)]
pub struct RawRowLayout {
    /// Column types
    types: Vec<LogicalType>,
    /// Offset of each column within the row
    offsets: Vec<usize>,
    /// Width of validity flags (in bytes)
    flag_width: usize,
    /// Width of data portion (excluding flags)
    data_width: usize,
    /// Total row width
    row_width: usize,
    /// Whether all columns are constant (fixed) size
    all_constant: bool,
    /// Indices of variable-size columns
    variable_columns: Vec<usize>,
    /// Offset to heap size field (only for variable-size data)
    heap_size_offset: Option<usize>,
    /// Validity type
    validity_type: RawRowValidityType,
}

impl Default for RawRowLayout {
    fn default() -> Self {
        Self::new()
    }
}

impl RawRowLayout {
    /// Create an empty layout.
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
            offsets: Vec::new(),
            flag_width: 0,
            data_width: 0,
            row_width: 0,
            all_constant: true,
            variable_columns: Vec::new(),
            heap_size_offset: None,
            validity_type: RawRowValidityType::CanHaveNullValues,
        }
    }

    /// Initialize the layout with the given column types.
    ///
    /// # Arguments
    /// * `types` - Column types
    /// * `validity_type` - Whether columns can have NULL values
    pub fn initialize(&mut self, types: Vec<LogicalType>, validity_type: RawRowValidityType) {
        self.initialize_internal(types, validity_type, RawRowNestednessType::TopLevelLayout);
    }

    /// Initialize with nestedness control (for struct fields).
    pub fn initialize_with_nestedness(
        &mut self,
        types: Vec<LogicalType>,
        validity_type: RawRowValidityType,
        nestedness_type: RawRowNestednessType,
    ) {
        self.initialize_internal(types, validity_type, nestedness_type);
    }

    /// Build a nested layout for a Struct's fields.
    ///
    /// This layout includes a validity mask for the struct fields but does not
    /// include a heap-size offset (nested layout).
    pub fn struct_layout(fields: &[(String, LogicalType)]) -> Self {
        let mut layout = RawRowLayout::new();
        let types = fields.iter().map(|(_, ty)| ty.clone()).collect();
        layout.initialize_with_nestedness(
            types,
            RawRowValidityType::CanHaveNullValues,
            RawRowNestednessType::NestedStructLayout,
        );
        layout
    }

    fn initialize_internal(
        &mut self,
        types: Vec<LogicalType>,
        validity_type: RawRowValidityType,
        nestedness_type: RawRowNestednessType,
    ) {
        self.types = types;
        self.validity_type = validity_type;
        self.offsets.clear();
        self.variable_columns.clear();
        self.all_constant = true;

        // Validity mask at the front - 1 bit per column
        let validity_count = if validity_type == RawRowValidityType::CannotHaveNullValues {
            0
        } else {
            self.types.len()
        };
        self.flag_width = Self::validity_mask_size(validity_count);
        self.row_width = self.flag_width;

        // Check which columns are variable-size
        for (col_idx, typ) in self.types.iter().enumerate() {
            if !Self::type_is_constant_size(typ) {
                self.all_constant = false;
                self.variable_columns.push(col_idx);
            }
        }

        // Heap size offset (only for top-level layouts with variable data)
        if nestedness_type == RawRowNestednessType::TopLevelLayout && !self.all_constant {
            self.heap_size_offset = Some(self.row_width);
            self.row_width += std::mem::size_of::<u64>(); // idx_t
        } else {
            self.heap_size_offset = None;
        }

        // Calculate column offsets
        for typ in &self.types {
            self.offsets.push(self.row_width);
            self.row_width += Self::get_type_size(typ);
        }

        self.data_width = self.row_width - self.flag_width;
    }

    /// Create a copy of this layout.
    pub fn copy(&self) -> Self {
        self.clone()
    }

    // === Accessor Methods ===

    /// Get the number of columns.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.types.len()
    }

    /// Get the column types.
    #[inline]
    pub fn get_types(&self) -> &[LogicalType] {
        &self.types
    }

    /// Get the column offsets.
    #[inline]
    pub fn get_offsets(&self) -> &[usize] {
        &self.offsets
    }

    /// Get the total row width.
    #[inline]
    pub fn get_row_width(&self) -> usize {
        self.row_width
    }

    /// Get the offset to the data portion (after validity flags).
    #[inline]
    pub fn get_data_offset(&self) -> usize {
        self.flag_width
    }

    /// Get the width of the data portion.
    #[inline]
    pub fn get_data_width(&self) -> usize {
        self.data_width
    }

    /// Get the width of the validity flags.
    #[inline]
    pub fn get_flag_width(&self) -> usize {
        self.flag_width
    }

    /// Check if all columns are constant (fixed) size.
    #[inline]
    pub fn all_constant(&self) -> bool {
        self.all_constant
    }

    /// Get indices of variable-size columns.
    #[inline]
    pub fn get_variable_columns(&self) -> &[usize] {
        &self.variable_columns
    }

    /// Get the heap size offset (if any).
    #[inline]
    pub fn get_heap_size_offset(&self) -> Option<usize> {
        self.heap_size_offset
    }

    /// Check if all values are valid (no NULLs possible).
    #[inline]
    pub fn all_valid(&self) -> bool {
        self.validity_type == RawRowValidityType::CannotHaveNullValues
    }

    // === Helper Methods ===

    /// Calculate the size of validity mask in bytes.
    ///
    /// Uses 1 bit per column, rounded up to bytes.
    #[inline]
    pub fn validity_mask_size(column_count: usize) -> usize {
        if column_count == 0 {
            0
        } else {
            column_count.div_ceil(8)
        }
    }

    /// Check if a type has constant (fixed) size.
    pub fn type_is_constant_size(typ: &LogicalType) -> bool {
        match typ {
            // Variable-size types
            LogicalType::Varchar
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => false,
            LogicalType::List(_) => false,
            // Struct depends on children (simplified: treat as variable for now)
            LogicalType::Struct(_) => false,
            // Array is treated as variable-size in row storage (stored in heap)
            LogicalType::Array(_, _) => false,
            // All other types are fixed-size
            _ => true,
        }
    }

    /// Get the storage size for a type in the row.
    ///
    /// For variable-size types, this returns the size of the pointer/offset.
    pub fn get_type_size(typ: &LogicalType) -> usize {
        match typ {
            // Fixed-size types use their natural size
            LogicalType::Boolean => 1,
            LogicalType::TinyInt | LogicalType::UTinyInt => 1,
            LogicalType::SmallInt | LogicalType::USmallInt => 2,
            LogicalType::Integer | LogicalType::UInteger | LogicalType::Float => 4,
            LogicalType::BigInt
            | LogicalType::UBigInt
            | LogicalType::Double
            | LogicalType::Date
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time => 8,
            LogicalType::HugeInt
            | LogicalType::UHugeInt
            | LogicalType::Uuid
            | LogicalType::Interval => 16,
            LogicalType::Decimal { .. } => 16,

            // VARCHAR uses string_t structure (16 bytes: 4 length + 4 prefix + 8 ptr/inline)
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob => 16,

            // Variable-size types use a pointer (8 bytes)
            LogicalType::List(_) => std::mem::size_of::<usize>(),

            // Struct: sum of child sizes (simplified)
            LogicalType::Struct(fields) => {
                let mut size = Self::validity_mask_size(fields.len());
                for (_, field_type) in fields.iter() {
                    size += Self::get_type_size(field_type);
                }
                size
            }

            // Array: behaves like a list (stored in heap), 8 bytes for pointer/offset in row
            LogicalType::Array(_, _) => 8,

            // Special types (should not appear in raw row storage)
            LogicalType::Null
            | LogicalType::Unknown
            | LogicalType::IntegerLiteral(_)
            | LogicalType::StringLiteral => 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_layout() {
        let layout = RawRowLayout::new();
        assert_eq!(layout.column_count(), 0);
        assert_eq!(layout.get_row_width(), 0);
        assert!(layout.all_constant());
    }

    #[test]
    fn test_fixed_size_layout() {
        let mut layout = RawRowLayout::new();
        layout.initialize(
            vec![
                LogicalType::Integer,
                LogicalType::BigInt,
                LogicalType::Double,
            ],
            RawRowValidityType::CanHaveNullValues,
        );

        assert_eq!(layout.column_count(), 3);
        assert!(layout.all_constant());
        assert!(layout.get_variable_columns().is_empty());
        assert!(layout.get_heap_size_offset().is_none());

        // Validity: 1 byte (3 bits rounded up)
        assert_eq!(layout.get_flag_width(), 1);

        // Offsets: validity(1) + col0(4) + col1(8) + col2(8)
        assert_eq!(layout.get_offsets(), &[1, 5, 13]);
        assert_eq!(layout.get_row_width(), 21);
    }

    #[test]
    fn test_varchar_layout() {
        let mut layout = RawRowLayout::new();
        layout.initialize(
            vec![LogicalType::Integer, LogicalType::Varchar],
            RawRowValidityType::CanHaveNullValues,
        );

        assert_eq!(layout.column_count(), 2);
        assert!(!layout.all_constant());
        assert_eq!(layout.get_variable_columns(), &[1]);
        assert!(layout.get_heap_size_offset().is_some());

        // Validity: 1 byte
        // Heap size: 8 bytes (for variable data)
        // Col0 (INT): 4 bytes
        // Col1 (VARCHAR): 16 bytes (string_t)
        let heap_offset = layout.get_heap_size_offset().unwrap();
        assert_eq!(heap_offset, 1); // After validity

        assert_eq!(layout.get_offsets(), &[9, 13]); // After validity + heap_size
        assert_eq!(layout.get_row_width(), 29); // 1 + 8 + 4 + 16
    }

    #[test]
    fn test_no_null_layout() {
        let mut layout = RawRowLayout::new();
        layout.initialize(
            vec![LogicalType::Integer, LogicalType::BigInt],
            RawRowValidityType::CannotHaveNullValues,
        );

        assert!(layout.all_valid());
        assert_eq!(layout.get_flag_width(), 0);
        assert_eq!(layout.get_offsets(), &[0, 4]);
        assert_eq!(layout.get_row_width(), 12); // 4 + 8
    }

    #[test]
    fn test_validity_mask_size() {
        assert_eq!(RawRowLayout::validity_mask_size(0), 0);
        assert_eq!(RawRowLayout::validity_mask_size(1), 1);
        assert_eq!(RawRowLayout::validity_mask_size(8), 1);
        assert_eq!(RawRowLayout::validity_mask_size(9), 2);
        assert_eq!(RawRowLayout::validity_mask_size(16), 2);
        assert_eq!(RawRowLayout::validity_mask_size(17), 3);
    }

    #[test]
    fn test_type_is_constant_size() {
        assert!(RawRowLayout::type_is_constant_size(&LogicalType::Integer));
        assert!(RawRowLayout::type_is_constant_size(&LogicalType::BigInt));
        assert!(RawRowLayout::type_is_constant_size(&LogicalType::Double));
        assert!(RawRowLayout::type_is_constant_size(&LogicalType::Boolean));

        assert!(!RawRowLayout::type_is_constant_size(&LogicalType::Varchar));
        assert!(!RawRowLayout::type_is_constant_size(&LogicalType::List(
            Box::new(LogicalType::Integer)
        )));

        // Array with fixed element is NOT constant because it's stored in heap
        assert!(!RawRowLayout::type_is_constant_size(&LogicalType::Array(
            Box::new(LogicalType::Float),
            1536
        )));
    }

    #[test]
    fn test_get_type_size() {
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::Boolean), 1);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::TinyInt), 1);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::SmallInt), 2);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::Integer), 4);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::BigInt), 8);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::Double), 8);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::HugeInt), 16);
        assert_eq!(RawRowLayout::get_type_size(&LogicalType::Varchar), 16);

        // Array: behaves like a list (stored in heap), 8 bytes for pointer/offset in row
        assert_eq!(
            RawRowLayout::get_type_size(&LogicalType::Array(Box::new(LogicalType::Float), 4)),
            8
        );
    }

    #[test]
    fn test_mixed_types_layout() {
        let mut layout = RawRowLayout::new();
        layout.initialize(
            vec![
                LogicalType::Boolean,
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Double,
            ],
            RawRowValidityType::CanHaveNullValues,
        );

        assert_eq!(layout.column_count(), 4);
        assert!(!layout.all_constant());
        assert_eq!(layout.get_variable_columns(), &[2]); // VARCHAR at index 2

        // Validity: 1 byte (4 columns)
        // Heap size: 8 bytes
        // Bool: 1, Int: 4, Varchar: 16, Double: 8
        assert_eq!(layout.get_flag_width(), 1);
        assert!(layout.get_heap_size_offset().is_some());
    }

    #[test]
    fn test_many_columns_validity() {
        let mut layout = RawRowLayout::new();
        let types: Vec<LogicalType> = (0..20).map(|_| LogicalType::Integer).collect();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);

        // 20 columns = 3 bytes for validity (20 bits = 3 bytes)
        assert_eq!(layout.get_flag_width(), 3);
        assert_eq!(layout.column_count(), 20);
    }

    #[test]
    fn test_layout_copy() {
        let mut layout = RawRowLayout::new();
        layout.initialize(
            vec![LogicalType::Integer, LogicalType::Varchar],
            RawRowValidityType::CanHaveNullValues,
        );

        let copied = layout.copy();
        assert_eq!(copied.column_count(), layout.column_count());
        assert_eq!(copied.get_row_width(), layout.get_row_width());
        assert_eq!(copied.get_offsets(), layout.get_offsets());
    }
}
