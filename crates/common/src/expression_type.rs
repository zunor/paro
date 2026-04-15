//! # Expression Type
//!
//! Expression type enumeration for SQL expressions.
//!
//! ## Note
//! This is a minimal implementation focusing on comparison types needed for
//! zone map filtering. Additional expression types can be added as needed.

use std::fmt;

/// Expression type enumeration.
///
/// This enum represents different types of SQL expressions, with a focus on
/// comparison operators used for zone map filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[derive(Default)]
pub enum ExpressionType {
    /// Invalid expression type
    #[default]
    Invalid = 0,

    // -----------------------------
    // Comparison Operators
    // -----------------------------
    /// Equal operator (=)
    CompareEqual = 25,
    /// Not equal operator (<>, !=)
    CompareNotEqual = 26,
    /// Less than operator (<)
    CompareLessThan = 27,
    /// Greater than operator (>)
    CompareGreaterThan = 28,
    /// Less than or equal operator (<=)
    CompareLessThanOrEqualTo = 29,
    /// Greater than or equal operator (>=)
    CompareGreaterThanOrEqualTo = 30,
    /// IN operator
    CompareIn = 35,
    /// NOT IN operator
    CompareNotIn = 36,
    /// IS DISTINCT FROM operator
    CompareDistinctFrom = 37,
    /// BETWEEN operator
    CompareBetween = 38,
    /// NOT BETWEEN operator
    CompareNotBetween = 39,
    /// IS NOT DISTINCT FROM operator
    CompareNotDistinctFrom = 40,
}

impl ExpressionType {
    /// Check if this is a comparison expression type.
    #[inline]
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            ExpressionType::CompareEqual
                | ExpressionType::CompareNotEqual
                | ExpressionType::CompareLessThan
                | ExpressionType::CompareGreaterThan
                | ExpressionType::CompareLessThanOrEqualTo
                | ExpressionType::CompareGreaterThanOrEqualTo
                | ExpressionType::CompareIn
                | ExpressionType::CompareNotIn
                | ExpressionType::CompareDistinctFrom
                | ExpressionType::CompareBetween
                | ExpressionType::CompareNotBetween
                | ExpressionType::CompareNotDistinctFrom
        )
    }

    /// Negate a comparison expression.
    /// e.g., = becomes !=, < becomes >=
    pub fn negate(&self) -> Option<ExpressionType> {
        match self {
            ExpressionType::CompareEqual => Some(ExpressionType::CompareNotEqual),
            ExpressionType::CompareNotEqual => Some(ExpressionType::CompareEqual),
            ExpressionType::CompareLessThan => Some(ExpressionType::CompareGreaterThanOrEqualTo),
            ExpressionType::CompareGreaterThan => Some(ExpressionType::CompareLessThanOrEqualTo),
            ExpressionType::CompareLessThanOrEqualTo => Some(ExpressionType::CompareGreaterThan),
            ExpressionType::CompareGreaterThanOrEqualTo => Some(ExpressionType::CompareLessThan),
            ExpressionType::CompareDistinctFrom => Some(ExpressionType::CompareNotDistinctFrom),
            ExpressionType::CompareNotDistinctFrom => Some(ExpressionType::CompareDistinctFrom),
            ExpressionType::CompareBetween => Some(ExpressionType::CompareNotBetween),
            ExpressionType::CompareNotBetween => Some(ExpressionType::CompareBetween),
            _ => None,
        }
    }

    /// Flip a comparison expression.
    /// e.g., < becomes >, = stays =
    pub fn flip(&self) -> Option<ExpressionType> {
        match self {
            ExpressionType::CompareEqual => Some(ExpressionType::CompareEqual),
            ExpressionType::CompareNotEqual => Some(ExpressionType::CompareNotEqual),
            ExpressionType::CompareLessThan => Some(ExpressionType::CompareGreaterThan),
            ExpressionType::CompareGreaterThan => Some(ExpressionType::CompareLessThan),
            ExpressionType::CompareLessThanOrEqualTo => {
                Some(ExpressionType::CompareGreaterThanOrEqualTo)
            }
            ExpressionType::CompareGreaterThanOrEqualTo => {
                Some(ExpressionType::CompareLessThanOrEqualTo)
            }
            ExpressionType::CompareDistinctFrom => Some(ExpressionType::CompareDistinctFrom),
            ExpressionType::CompareNotDistinctFrom => Some(ExpressionType::CompareNotDistinctFrom),
            _ => None,
        }
    }

    /// Convert operator string to ExpressionType.
    pub fn from_operator(op: &str) -> Option<ExpressionType> {
        match op {
            "=" => Some(ExpressionType::CompareEqual),
            "!=" | "<>" => Some(ExpressionType::CompareNotEqual),
            "<" => Some(ExpressionType::CompareLessThan),
            ">" => Some(ExpressionType::CompareGreaterThan),
            "<=" => Some(ExpressionType::CompareLessThanOrEqualTo),
            ">=" => Some(ExpressionType::CompareGreaterThanOrEqualTo),
            _ => None,
        }
    }

    /// Convert ExpressionType to operator string.
    pub fn to_operator(&self) -> Option<&'static str> {
        match self {
            ExpressionType::CompareEqual => Some("="),
            ExpressionType::CompareNotEqual => Some("!="),
            ExpressionType::CompareLessThan => Some("<"),
            ExpressionType::CompareGreaterThan => Some(">"),
            ExpressionType::CompareLessThanOrEqualTo => Some("<="),
            ExpressionType::CompareGreaterThanOrEqualTo => Some(">="),
            ExpressionType::CompareDistinctFrom => Some("IS DISTINCT FROM"),
            ExpressionType::CompareNotDistinctFrom => Some("IS NOT DISTINCT FROM"),
            _ => None,
        }
    }
}

impl fmt::Display for ExpressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionType::Invalid => write!(f, "INVALID"),
            ExpressionType::CompareEqual => write!(f, "COMPARE_EQUAL"),
            ExpressionType::CompareNotEqual => write!(f, "COMPARE_NOTEQUAL"),
            ExpressionType::CompareLessThan => write!(f, "COMPARE_LESSTHAN"),
            ExpressionType::CompareGreaterThan => write!(f, "COMPARE_GREATERTHAN"),
            ExpressionType::CompareLessThanOrEqualTo => write!(f, "COMPARE_LESSTHANOREQUALTO"),
            ExpressionType::CompareGreaterThanOrEqualTo => {
                write!(f, "COMPARE_GREATERTHANOREQUALTO")
            }
            ExpressionType::CompareIn => write!(f, "COMPARE_IN"),
            ExpressionType::CompareNotIn => write!(f, "COMPARE_NOT_IN"),
            ExpressionType::CompareDistinctFrom => write!(f, "COMPARE_DISTINCT_FROM"),
            ExpressionType::CompareBetween => write!(f, "COMPARE_BETWEEN"),
            ExpressionType::CompareNotBetween => write!(f, "COMPARE_NOT_BETWEEN"),
            ExpressionType::CompareNotDistinctFrom => write!(f, "COMPARE_NOT_DISTINCT_FROM"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_comparison() {
        assert!(ExpressionType::CompareEqual.is_comparison());
        assert!(ExpressionType::CompareNotEqual.is_comparison());
        assert!(ExpressionType::CompareLessThan.is_comparison());
        assert!(ExpressionType::CompareGreaterThan.is_comparison());
        assert!(!ExpressionType::Invalid.is_comparison());
    }

    #[test]
    fn test_negate() {
        assert_eq!(
            ExpressionType::CompareEqual.negate(),
            Some(ExpressionType::CompareNotEqual)
        );
        assert_eq!(
            ExpressionType::CompareLessThan.negate(),
            Some(ExpressionType::CompareGreaterThanOrEqualTo)
        );
        assert_eq!(
            ExpressionType::CompareGreaterThan.negate(),
            Some(ExpressionType::CompareLessThanOrEqualTo)
        );
    }

    #[test]
    fn test_flip() {
        assert_eq!(
            ExpressionType::CompareEqual.flip(),
            Some(ExpressionType::CompareEqual)
        );
        assert_eq!(
            ExpressionType::CompareLessThan.flip(),
            Some(ExpressionType::CompareGreaterThan)
        );
        assert_eq!(
            ExpressionType::CompareGreaterThanOrEqualTo.flip(),
            Some(ExpressionType::CompareLessThanOrEqualTo)
        );
    }

    #[test]
    fn test_from_operator() {
        assert_eq!(
            ExpressionType::from_operator("="),
            Some(ExpressionType::CompareEqual)
        );
        assert_eq!(
            ExpressionType::from_operator("!="),
            Some(ExpressionType::CompareNotEqual)
        );
        assert_eq!(
            ExpressionType::from_operator("<>"),
            Some(ExpressionType::CompareNotEqual)
        );
        assert_eq!(
            ExpressionType::from_operator("<"),
            Some(ExpressionType::CompareLessThan)
        );
        assert_eq!(ExpressionType::from_operator("invalid"), None);
    }

    #[test]
    fn test_to_operator() {
        assert_eq!(ExpressionType::CompareEqual.to_operator(), Some("="));
        assert_eq!(ExpressionType::CompareNotEqual.to_operator(), Some("!="));
        assert_eq!(ExpressionType::CompareLessThan.to_operator(), Some("<"));
    }

    #[test]
    fn test_display() {
        assert_eq!(ExpressionType::CompareEqual.to_string(), "COMPARE_EQUAL");
        assert_eq!(
            ExpressionType::CompareLessThan.to_string(),
            "COMPARE_LESSTHAN"
        );
    }

    #[test]
    fn test_default() {
        assert_eq!(ExpressionType::default(), ExpressionType::Invalid);
    }
}
