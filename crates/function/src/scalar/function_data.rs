// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

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
use std::hash::{Hash, Hasher};
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

    /// Stable semantic fingerprint used by expression caching and CSE.
    ///
    /// Equal bind data must return the same value. Pointer identity is not a
    /// semantic property: independently bound equivalent expressions need to
    /// share compiled programs and common subexpressions.
    fn fingerprint(&self) -> u64;

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;
}

/// Hash ordinary bind-data values through the same deterministic FNV-1a
/// stream used by expression fingerprints.
pub fn function_data_fingerprint<T: Hash>(value: &T) -> u64 {
    let mut hasher = FunctionDataHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

struct FunctionDataHasher(u64);

impl FunctionDataHasher {
    fn new() -> Self {
        Self(0xcbf29ce484222325)
    }
}

impl Hasher for FunctionDataHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
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
