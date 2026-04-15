// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Page Builder Trait
//!
//! Trait for building encoded pages from column data.

use bytes::Bytes;
use paro_common::error::Result;

/// Trait for building pages from column data.
///
/// PageBuilder is used to encode column values into pages. Different
/// implementations handle different encoding schemes (Plain, Dictionary,
/// BitShuffle, RLE, etc.).
///
/// ## Lifecycle
///
/// 1. Create builder with `new()` or factory method
/// 2. Optionally call `reserve_head()` for header space
/// 3. Call `add()` repeatedly to add values
/// 4. Check `is_page_full()` to know when to flush
/// 5. Call `finish()` to get encoded page data
/// 6. Call `reset()` to reuse for next page
///
/// ## Example
///
/// ```ignore
/// let mut builder = PlainPageBuilder::new(options);
/// builder.reserve_head(4); // Reserve space for custom header
///
/// while !builder.is_page_full() {
///     let added = builder.add(&values[offset..], remaining);
///     offset += added;
///     remaining -= added;
/// }
///
/// let page_data = builder.finish();
/// // Write page_data to file...
///
/// builder.reset();
/// // Continue with next page...
/// ```
pub trait PageBuilder: Send + Sync {
    /// Reserve space at the head of the page buffer.
    ///
    /// This allows external code to write a custom header after `finish()`.
    /// The reserved bytes are not written by the PageBuilder.
    ///
    /// Must be called on an empty page before any `add()` calls.
    /// The reserved size persists across `reset()` calls.
    fn reserve_head(&mut self, _head_size: u8) {
        // Default: not supported
        panic!("reserve_head() not supported by this PageBuilder");
    }

    /// Check if the page is full and should be flushed.
    fn is_page_full(&self) -> bool;

    /// Add values to the page.
    ///
    /// # Arguments
    /// * `vals` - Pointer to values (type depends on implementation)
    /// * `count` - Number of values to add
    ///
    /// # Returns
    /// Number of values actually added (may be less than `count` if page is full)
    fn add(&mut self, vals: &[u8], count: u32) -> u32;

    /// Finish building the page and return encoded data.
    ///
    /// The returned data is valid until `reset()` is called.
    fn finish(&mut self) -> Result<Bytes>;

    /// Get the dictionary page for dictionary encoding.
    ///
    /// Returns `None` for non-dictionary encodings.
    fn get_dictionary_page(&self) -> Option<Bytes> {
        None
    }

    /// Reset the builder for reuse.
    ///
    /// Clears all data but preserves configuration and reserved head size.
    fn reset(&mut self);

    /// Get the number of values added to the current page.
    fn count(&self) -> u32;

    /// Get the current size of the page data in bytes.
    fn size(&self) -> u64;

    /// Get the first value in the page.
    ///
    /// Returns `None` if no values have been added.
    fn get_first_value(&self) -> Option<Bytes>;

    /// Get the last value in the page.
    ///
    /// Returns `None` if no values have been added.
    fn get_last_value(&self) -> Option<Bytes>;

    /// Check if all pages so far used dictionary encoding.
    ///
    /// Only meaningful for dictionary-capable builders.
    fn all_dict_encoded(&self) -> bool {
        false
    }
}

/// Options for creating page builders.
#[derive(Debug, Clone)]
pub struct PageBuilderOptions {
    /// Target page size in bytes (default: 256KB)
    pub page_size: usize,
    /// Data type size in bytes (for fixed-width types)
    pub type_size: usize,
    /// Whether nulls are allowed
    pub is_nullable: bool,
}

impl Default for PageBuilderOptions {
    fn default() -> Self {
        PageBuilderOptions {
            page_size: 256 * 1024, // 256KB default
            type_size: 0,
            is_nullable: true,
        }
    }
}

impl PageBuilderOptions {
    pub fn new(page_size: usize) -> Self {
        PageBuilderOptions {
            page_size,
            ..Default::default()
        }
    }

    pub fn with_type_size(mut self, type_size: usize) -> Self {
        self.type_size = type_size;
        self
    }

    pub fn with_nullable(mut self, is_nullable: bool) -> Self {
        self.is_nullable = is_nullable;
        self
    }
}
