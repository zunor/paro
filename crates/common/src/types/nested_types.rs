use super::LogicalType;

// ============================================================================
// ArrayType Helper Struct
// ============================================================================

/// Helper struct for Array type operations.
///
/// Provides static methods to work with fixed-size array types such as
/// `FLOAT[1536]` embeddings.
pub struct ArrayType;

impl ArrayType {
    /// Maximum allowed array size.
    pub const MAX_ARRAY_SIZE: usize = 100000;

    /// Get the child (element) type of an Array type.
    ///
    /// # Panics
    /// Panics if the type is not an Array.
    #[inline]
    pub fn get_child_type(logical_type: &LogicalType) -> &LogicalType {
        match logical_type {
            LogicalType::Array(child, _) => child.as_ref(),
            _ => panic!(
                "ArrayType::get_child_type called on non-Array type: {:?}",
                logical_type
            ),
        }
    }

    /// Get the fixed size of an Array type.
    ///
    /// # Panics
    /// Panics if the type is not an Array.
    #[inline]
    pub fn get_size(logical_type: &LogicalType) -> usize {
        match logical_type {
            LogicalType::Array(_, size) => *size,
            _ => panic!(
                "ArrayType::get_size called on non-Array type: {:?}",
                logical_type
            ),
        }
    }

    /// Check if the Array type has "any size" (size == 0).
    ///
    /// An Array with size 0 is used during binding when the size is not yet known.
    ///
    /// # Panics
    /// Panics if the type is not an Array.
    #[inline]
    pub fn is_any_size(logical_type: &LogicalType) -> bool {
        match logical_type {
            LogicalType::Array(_, size) => *size == 0,
            _ => panic!(
                "ArrayType::is_any_size called on non-Array type: {:?}",
                logical_type
            ),
        }
    }

    /// Recursively convert all ARRAY types to LIST types within the given type.
    ///
    /// This is useful for operations that don't support fixed-size arrays but can
    /// work with variable-length lists.
    ///
    /// # Example
    /// ```ignore
    /// // FLOAT[1536] → FLOAT[]
    /// let array_type = LogicalType::Array(Box::new(LogicalType::Float), 1536);
    /// let list_type = ArrayType::convert_to_list(&array_type);
    /// assert_eq!(list_type, LogicalType::List(Box::new(LogicalType::Float)));
    /// ```
    pub fn convert_to_list(logical_type: &LogicalType) -> LogicalType {
        match logical_type {
            LogicalType::Array(child, _) => {
                LogicalType::List(Box::new(Self::convert_to_list(child)))
            }
            LogicalType::List(child) => LogicalType::List(Box::new(Self::convert_to_list(child))),
            LogicalType::Struct(fields) => {
                let converted_fields: Vec<(String, LogicalType)> = fields
                    .iter()
                    .map(|(name, typ)| (name.clone(), Self::convert_to_list(typ)))
                    .collect();
                LogicalType::Struct(converted_fields)
            }
            // For all other types, return as-is
            _ => logical_type.clone(),
        }
    }

    /// Create a new Array type with the given child type and size.
    ///
    /// # Arguments
    /// * `child` - The element type of the array
    /// * `size` - The fixed size of the array (use 0 for "any size" during binding)
    ///
    /// # Panics
    /// Panics if size exceeds `MAX_ARRAY_SIZE`.
    pub fn create_array(child: LogicalType, size: usize) -> LogicalType {
        if size > Self::MAX_ARRAY_SIZE {
            panic!(
                "Array size {} exceeds maximum allowed size {}",
                size,
                Self::MAX_ARRAY_SIZE
            );
        }
        LogicalType::Array(Box::new(child), size)
    }

    /// Create an Array type with "any size" (size == 0).
    ///
    /// Used during binding when the array size is not yet determined.
    pub fn any_size(child: LogicalType) -> LogicalType {
        LogicalType::Array(Box::new(child), 0)
    }
}

// ============================================================================
// ListType Helper Struct
// ============================================================================

/// Helper struct for List type operations.
///
/// Provides static methods to work with `List` types.
pub struct ListType;

impl ListType {
    /// Get the child (element) type of a List type.
    ///
    /// # Panics
    /// Panics if the type is not a List.
    #[inline]
    pub fn get_child_type(logical_type: &LogicalType) -> &LogicalType {
        match logical_type {
            LogicalType::List(child) => child.as_ref(),
            _ => panic!(
                "ListType::get_child_type called on non-List type: {:?}",
                logical_type
            ),
        }
    }
}

// ============================================================================
// StructType Helper Struct
// ============================================================================

/// Helper struct for Struct type operations.
///
/// Provides static methods to work with `Struct` types.
pub struct StructType;

impl StructType {
    /// Get the child types of a Struct type.
    ///
    /// # Panics
    /// Panics if the type is not a Struct.
    #[inline]
    pub fn get_child_types(logical_type: &LogicalType) -> &[(String, LogicalType)] {
        match logical_type {
            LogicalType::Struct(fields) => fields.as_slice(),
            _ => panic!(
                "StructType::get_child_types called on non-Struct type: {:?}",
                logical_type
            ),
        }
    }

    /// Get the child type at a specific index.
    ///
    /// # Panics
    /// Panics if the type is not a Struct or index is out of bounds.
    #[inline]
    pub fn get_child_type(logical_type: &LogicalType, index: usize) -> &LogicalType {
        match logical_type {
            LogicalType::Struct(fields) => &fields[index].1,
            _ => panic!(
                "StructType::get_child_type called on non-Struct type: {:?}",
                logical_type
            ),
        }
    }

    /// Get the child name at a specific index.
    ///
    /// # Panics
    /// Panics if the type is not a Struct or index is out of bounds.
    #[inline]
    pub fn get_child_name(logical_type: &LogicalType, index: usize) -> &str {
        match logical_type {
            LogicalType::Struct(fields) => &fields[index].0,
            _ => panic!(
                "StructType::get_child_name called on non-Struct type: {:?}",
                logical_type
            ),
        }
    }

    /// Get the number of child fields.
    ///
    /// # Panics
    /// Panics if the type is not a Struct.
    #[inline]
    pub fn get_child_count(logical_type: &LogicalType) -> usize {
        match logical_type {
            LogicalType::Struct(fields) => fields.len(),
            _ => panic!(
                "StructType::get_child_count called on non-Struct type: {:?}",
                logical_type
            ),
        }
    }
}
