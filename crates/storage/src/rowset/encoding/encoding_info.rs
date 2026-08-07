// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Encoding Info Registry
//!
//! Manages encoding type selection and page builder/decoder creation.

use crate::rowset::page::EncodingType;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Field types for encoding selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FieldType {
    Boolean = 0,
    TinyInt = 1,
    SmallInt = 2,
    Int = 3,
    BigInt = 4,
    LargeInt = 5,
    Float = 6,
    Double = 7,
    Char = 8,
    Varchar = 9,
    Date = 10,
    DateTime = 11,
    Decimal = 12,
    Json = 13,
    Binary = 14,
    Vector = 15,
}

impl FieldType {
    /// Convert from u8 tag for deserialization.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(FieldType::Boolean),
            1 => Some(FieldType::TinyInt),
            2 => Some(FieldType::SmallInt),
            3 => Some(FieldType::Int),
            4 => Some(FieldType::BigInt),
            5 => Some(FieldType::LargeInt),
            6 => Some(FieldType::Float),
            7 => Some(FieldType::Double),
            8 => Some(FieldType::Char),
            9 => Some(FieldType::Varchar),
            10 => Some(FieldType::Date),
            11 => Some(FieldType::DateTime),
            12 => Some(FieldType::Decimal),
            13 => Some(FieldType::Json),
            14 => Some(FieldType::Binary),
            15 => Some(FieldType::Vector),
            _ => None,
        }
    }

    /// Convert to u8 tag for serialization.
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Get the size in bytes for fixed-width types.
    pub fn size(&self) -> Option<usize> {
        match self {
            FieldType::Boolean => Some(1),
            FieldType::TinyInt => Some(1),
            FieldType::SmallInt => Some(2),
            FieldType::Int => Some(4),
            FieldType::BigInt => Some(8),
            FieldType::LargeInt => Some(16),
            FieldType::Float => Some(4),
            FieldType::Double => Some(8),
            FieldType::Date => Some(4),
            FieldType::DateTime => Some(8),
            FieldType::Decimal => Some(16),
            // Variable-length types
            FieldType::Char
            | FieldType::Varchar
            | FieldType::Json
            | FieldType::Binary
            | FieldType::Vector => None,
        }
    }

    /// Check if this type is variable-length.
    pub fn is_variable_length(&self) -> bool {
        self.size().is_none()
    }

    /// Whether persisted values must satisfy the UTF-8 storage invariant.
    pub fn requires_valid_utf8(self) -> bool {
        matches!(self, FieldType::Char | FieldType::Varchar | FieldType::Json)
    }

    /// Check if this type supports dictionary encoding.
    pub fn supports_dict_encoding(&self) -> bool {
        match self {
            FieldType::Varchar | FieldType::Char | FieldType::Json => true,
            // Numeric types can also use dict encoding for low cardinality
            FieldType::SmallInt
            | FieldType::Int
            | FieldType::BigInt
            | FieldType::LargeInt
            | FieldType::Float
            | FieldType::Double
            | FieldType::Date
            | FieldType::DateTime
            | FieldType::Decimal => true,
            // TINYINT has only 256 values, BitShuffle is better
            FieldType::TinyInt | FieldType::Boolean | FieldType::Binary | FieldType::Vector => {
                false
            }
        }
    }
}

/// Encoding information for a specific type and encoding combination.
#[derive(Debug, Clone)]
pub struct EncodingInfo {
    pub field_type: FieldType,
    pub encoding: EncodingType,
}

impl EncodingInfo {
    pub fn new(field_type: FieldType, encoding: EncodingType) -> Self {
        EncodingInfo {
            field_type,
            encoding,
        }
    }
}

/// Registry for encoding information.
pub struct EncodingRegistry {
    /// Default encoding for each field type
    default_encodings: HashMap<FieldType, EncodingType>,
    /// Encoding for value-seek optimization
    value_seek_encodings: HashMap<FieldType, EncodingType>,
    /// All supported (type, encoding) combinations
    supported: HashMap<(FieldType, EncodingType), EncodingInfo>,
}

impl EncodingRegistry {
    /// Create a new registry with default encodings.
    pub fn new() -> Self {
        let mut registry = EncodingRegistry {
            default_encodings: HashMap::new(),
            value_seek_encodings: HashMap::new(),
            supported: HashMap::new(),
        };
        registry.init();
        registry
    }

    fn init(&mut self) {
        // BOOLEAN: RLE is default, BitShuffle and Plain also supported
        self.add(FieldType::Boolean, EncodingType::Rle, false);
        self.add(FieldType::Boolean, EncodingType::BitShuffle, false);
        self.add(FieldType::Boolean, EncodingType::Plain, true);

        // TINYINT: BitShuffle default
        self.add(FieldType::TinyInt, EncodingType::BitShuffle, false);
        self.add(FieldType::TinyInt, EncodingType::FrameOfReference, true);
        self.add(FieldType::TinyInt, EncodingType::Plain, false);

        // SMALLINT: BitShuffle default, Dict for low cardinality
        self.add(FieldType::SmallInt, EncodingType::BitShuffle, false);
        self.add(FieldType::SmallInt, EncodingType::FrameOfReference, true);
        self.add(FieldType::SmallInt, EncodingType::Plain, false);
        self.add(FieldType::SmallInt, EncodingType::Dict, false);

        // INT: BitShuffle default
        self.add(FieldType::Int, EncodingType::BitShuffle, false);
        self.add(FieldType::Int, EncodingType::FrameOfReference, true);
        self.add(FieldType::Int, EncodingType::Plain, false);
        self.add(FieldType::Int, EncodingType::Dict, false);

        // BIGINT: BitShuffle default
        self.add(FieldType::BigInt, EncodingType::BitShuffle, false);
        self.add(FieldType::BigInt, EncodingType::FrameOfReference, true);
        self.add(FieldType::BigInt, EncodingType::Plain, false);
        self.add(FieldType::BigInt, EncodingType::Dict, false);

        // LARGEINT: BitShuffle default
        self.add(FieldType::LargeInt, EncodingType::BitShuffle, false);
        self.add(FieldType::LargeInt, EncodingType::FrameOfReference, true);
        self.add(FieldType::LargeInt, EncodingType::Plain, false);

        // FLOAT: BitShuffle default
        self.add(FieldType::Float, EncodingType::BitShuffle, false);
        self.add(FieldType::Float, EncodingType::Plain, false);
        self.add(FieldType::Float, EncodingType::Dict, false);

        // DOUBLE: BitShuffle default
        self.add(FieldType::Double, EncodingType::BitShuffle, false);
        self.add(FieldType::Double, EncodingType::Plain, false);
        self.add(FieldType::Double, EncodingType::Dict, false);

        // CHAR: Dict default, Plain and Prefix also supported
        self.add(FieldType::Char, EncodingType::Dict, false);
        self.add(FieldType::Char, EncodingType::Plain, false);
        self.add(FieldType::Char, EncodingType::Prefix, true);

        // VARCHAR: Dict default
        self.add(FieldType::Varchar, EncodingType::Dict, false);
        self.add(FieldType::Varchar, EncodingType::Plain, false);
        self.add(FieldType::Varchar, EncodingType::Prefix, true);

        // DATE: BitShuffle default
        self.add(FieldType::Date, EncodingType::BitShuffle, false);
        self.add(FieldType::Date, EncodingType::Plain, false);
        self.add(FieldType::Date, EncodingType::FrameOfReference, true);
        self.add(FieldType::Date, EncodingType::Dict, false);

        // DATETIME: BitShuffle default
        self.add(FieldType::DateTime, EncodingType::BitShuffle, false);
        self.add(FieldType::DateTime, EncodingType::Plain, false);
        self.add(FieldType::DateTime, EncodingType::FrameOfReference, true);
        self.add(FieldType::DateTime, EncodingType::Dict, false);

        // DECIMAL: BitShuffle default
        self.add(FieldType::Decimal, EncodingType::BitShuffle, true);
        self.add(FieldType::Decimal, EncodingType::Plain, false);
        self.add(FieldType::Decimal, EncodingType::Dict, false);

        // JSON: Plain default, Dict also supported
        self.add(FieldType::Json, EncodingType::Plain, false);
        self.add(FieldType::Json, EncodingType::Dict, false);

        // BINARY: Plain only
        self.add(FieldType::Binary, EncodingType::Plain, false);

        // VECTOR: Plain only (vectors are typically stored in raw format)
        self.add(FieldType::Vector, EncodingType::Plain, false);
    }

    fn add(&mut self, field_type: FieldType, encoding: EncodingType, optimize_value_seek: bool) {
        let key = (field_type, encoding);

        // First encoding added becomes the default
        self.default_encodings.entry(field_type).or_insert(encoding);

        // Track value-seek optimized encoding
        if optimize_value_seek && !self.value_seek_encodings.contains_key(&field_type) {
            self.value_seek_encodings.insert(field_type, encoding);
        }

        self.supported
            .insert(key, EncodingInfo::new(field_type, encoding));
    }

    /// Get the default encoding for a field type.
    pub fn get_default_encoding(
        &self,
        field_type: FieldType,
        optimize_value_seek: bool,
    ) -> EncodingType {
        if optimize_value_seek {
            if let Some(&enc) = self.value_seek_encodings.get(&field_type) {
                return enc;
            }
        }
        self.default_encodings
            .get(&field_type)
            .copied()
            .unwrap_or(EncodingType::Plain)
    }

    /// Get encoding info for a specific type and encoding.
    pub fn get(&self, field_type: FieldType, encoding: EncodingType) -> Option<&EncodingInfo> {
        let encoding = if encoding == EncodingType::Default {
            self.get_default_encoding(field_type, false)
        } else {
            encoding
        };
        self.supported.get(&(field_type, encoding))
    }

    /// Check if a (type, encoding) combination is supported.
    pub fn is_supported(&self, field_type: FieldType, encoding: EncodingType) -> bool {
        let encoding = if encoding == EncodingType::Default {
            self.get_default_encoding(field_type, false)
        } else {
            encoding
        };
        self.supported.contains_key(&(field_type, encoding))
    }
}

impl Default for EncodingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global encoding registry singleton.
static ENCODING_REGISTRY: OnceLock<EncodingRegistry> = OnceLock::new();

/// Get the global encoding registry.
pub fn get_encoding_registry() -> &'static EncodingRegistry {
    ENCODING_REGISTRY.get_or_init(EncodingRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_encodings() {
        let registry = EncodingRegistry::new();

        // Boolean defaults to RLE
        assert_eq!(
            registry.get_default_encoding(FieldType::Boolean, false),
            EncodingType::Rle
        );

        // Int defaults to BitShuffle
        assert_eq!(
            registry.get_default_encoding(FieldType::Int, false),
            EncodingType::BitShuffle
        );

        // Varchar defaults to Dict
        assert_eq!(
            registry.get_default_encoding(FieldType::Varchar, false),
            EncodingType::Dict
        );
    }

    #[test]
    fn test_value_seek_encodings() {
        let registry = EncodingRegistry::new();

        // Int with value seek uses FOR
        assert_eq!(
            registry.get_default_encoding(FieldType::Int, true),
            EncodingType::FrameOfReference
        );

        // Varchar with value seek uses Prefix
        assert_eq!(
            registry.get_default_encoding(FieldType::Varchar, true),
            EncodingType::Prefix
        );
    }

    #[test]
    fn test_is_supported() {
        let registry = EncodingRegistry::new();

        assert!(registry.is_supported(FieldType::Int, EncodingType::BitShuffle));
        assert!(registry.is_supported(FieldType::Int, EncodingType::Plain));
        assert!(registry.is_supported(FieldType::Varchar, EncodingType::Dict));

        // TINYINT doesn't support Dict
        assert!(!registry.is_supported(FieldType::TinyInt, EncodingType::Dict));
    }

    #[test]
    fn test_field_type_size() {
        assert_eq!(FieldType::Int.size(), Some(4));
        assert_eq!(FieldType::BigInt.size(), Some(8));
        assert_eq!(FieldType::Varchar.size(), None);
        assert!(FieldType::Varchar.is_variable_length());
    }
}
