//! Function Bind Data
//!
//!
//!
//! ## Purpose
//!
//! `FunctionData` allows functions to store bind-time information that is
//! needed during execution. Examples:
//! - LIKE function: stores the compiled regex pattern
//! - SUBSTRING function: stores constant offset/length if known at bind time
//! - Date functions: stores format string

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Trait for storing extra data during function binding.
///
/// # Usage
/// ```ignore
/// struct LikeBindData {
///     pattern: String,
///     case_insensitive: bool,
/// }
///
/// impl FunctionData for LikeBindData {
///     fn clone_box(&self) -> Box<dyn FunctionData> {
///         Box::new(self.clone())
///     }
///     fn equals(&self, other: &dyn FunctionData) -> bool {
///         other.as_any().downcast_ref::<Self>()
///.map_or(false, |o| self.pattern == o.pattern)
///     }
///     fn as_any(&self) -> &dyn Any { self }
/// }
/// ```
pub trait FunctionData: Debug + Send + Sync {
    /// Create a boxed clone of this data.
    fn clone_box(&self) -> Box<dyn FunctionData>;

    /// Check equality with another FunctionData.
    fn equals(&self, other: &dyn FunctionData) -> bool;

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;
}

impl Clone for Box<dyn FunctionData> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn FunctionData> {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other.as_ref())
    }
}

/// Helper function to compare two optional FunctionData.
pub fn function_data_equals(
    left: Option<&Arc<dyn FunctionData>>,
    right: Option<&Arc<dyn FunctionData>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(l), Some(r)) => l.equals(r.as_ref()),
        _ => false,
    }
}
