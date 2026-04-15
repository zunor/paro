// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Index Constraint Type
//!
//! Defines the constraint types for indexes.

use std::fmt;

/// Index constraint type enumeration.
///
/// Defines what kind of constraint an index enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum IndexConstraintType {
    /// Index is not built to enforce any constraint
    #[default]
    None = 0,
    /// Index is built to enforce a UNIQUE constraint
    Unique = 1,
    /// Index is built to enforce a PRIMARY KEY constraint
    Primary = 2,
    /// Index is built to enforce a FOREIGN KEY constraint
    Foreign = 3,
}

impl IndexConstraintType {
    /// Returns true if this constraint type enforces uniqueness.
    #[inline]
    pub fn is_unique(&self) -> bool {
        matches!(
            self,
            IndexConstraintType::Unique | IndexConstraintType::Primary
        )
    }

    /// Returns true if this is a primary key constraint.
    #[inline]
    pub fn is_primary(&self) -> bool {
        matches!(self, IndexConstraintType::Primary)
    }

    /// Returns true if this is a foreign key constraint.
    #[inline]
    pub fn is_foreign(&self) -> bool {
        matches!(self, IndexConstraintType::Foreign)
    }

    /// Serialize to a single byte.
    #[inline]
    pub fn to_byte(&self) -> u8 {
        *self as u8
    }

    /// Deserialize from a single byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(IndexConstraintType::None),
            1 => Some(IndexConstraintType::Unique),
            2 => Some(IndexConstraintType::Primary),
            3 => Some(IndexConstraintType::Foreign),
            _ => None,
        }
    }
}

impl fmt::Display for IndexConstraintType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexConstraintType::None => write!(f, "NONE"),
            IndexConstraintType::Unique => write!(f, "UNIQUE"),
            IndexConstraintType::Primary => write!(f, "PRIMARY"),
            IndexConstraintType::Foreign => write!(f, "FOREIGN"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_type_properties() {
        assert!(!IndexConstraintType::None.is_unique());
        assert!(!IndexConstraintType::None.is_primary());
        assert!(!IndexConstraintType::None.is_foreign());

        assert!(IndexConstraintType::Unique.is_unique());
        assert!(!IndexConstraintType::Unique.is_primary());

        assert!(IndexConstraintType::Primary.is_unique());
        assert!(IndexConstraintType::Primary.is_primary());

        assert!(!IndexConstraintType::Foreign.is_unique());
        assert!(IndexConstraintType::Foreign.is_foreign());
    }

    #[test]
    fn test_serialization() {
        for constraint in [
            IndexConstraintType::None,
            IndexConstraintType::Unique,
            IndexConstraintType::Primary,
            IndexConstraintType::Foreign,
        ] {
            let byte = constraint.to_byte();
            let restored = IndexConstraintType::from_byte(byte);
            assert_eq!(restored, Some(constraint));
        }

        assert_eq!(IndexConstraintType::from_byte(255), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", IndexConstraintType::None), "NONE");
        assert_eq!(format!("{}", IndexConstraintType::Unique), "UNIQUE");
        assert_eq!(format!("{}", IndexConstraintType::Primary), "PRIMARY");
        assert_eq!(format!("{}", IndexConstraintType::Foreign), "FOREIGN");
    }

    #[test]
    fn test_default() {
        assert_eq!(IndexConstraintType::default(), IndexConstraintType::None);
    }
}
