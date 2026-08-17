// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! ## Implementation Notes
//! - 16-byte string type with inline optimization
//! - Strings ≤12 bytes are stored inline (no heap allocation)
//! - Longer strings store 4-byte prefix for fast comparison + pointer
//! - Self-contained: can be compared without external heap reference

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Maximum length for inlined strings.
pub const INLINE_CAPACITY: usize = 12;

/// Prefix length for pointer strings.
pub const PREFIX_LEN: usize = 4;

/// String view type (16 bytes).
///
/// Layout:
/// ```text
/// Offset  Size  Field
/// 0       4     length (u32)
/// 4       4     prefix[4] / inlined[0..4]
/// 8       8     ptr / inlined[4..12]
/// ```
///
/// - Inlined (≤12 bytes): `[length: u32][inlined: [u8; 12]]`
/// - Pointer (>12 bytes): `[length: u32][prefix: [u8; 4]][ptr: *const u8]`
///
/// # Safety
/// For pointer strings, the caller must ensure the pointed data remains valid
/// for the lifetime of this `StringView`. Typically, the data lives in a `StringHeap`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StringView {
    /// String length in bytes
    length: u32,
    /// First 4 bytes: prefix (for pointer) or inlined[0..4]
    prefix: [u8; 4],
    /// Last 8 bytes: pointer (for long strings) or inlined[4..12]
    ptr_or_inlined: u64,
}

impl StringView {
    /// Create a new StringView from a string slice.
    ///
    /// For short strings (≤12 bytes), data is stored inline.
    /// For longer strings, only the pointer is stored - caller must ensure
    /// the data remains valid.
    #[inline]
    pub fn new(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }

    /// Create a StringView from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let len = bytes.len() as u32;

        if bytes.len() <= INLINE_CAPACITY {
            // Inlined: copy data directly
            let mut prefix = [0u8; 4];
            let mut suffix = [0u8; 8];

            let prefix_len = bytes.len().min(4);
            prefix[..prefix_len].copy_from_slice(&bytes[..prefix_len]);

            if bytes.len() > 4 {
                let suffix_len = bytes.len() - 4;
                suffix[..suffix_len].copy_from_slice(&bytes[4..]);
            }

            Self {
                length: len,
                prefix,
                ptr_or_inlined: u64::from_le_bytes(suffix),
            }
        } else {
            // Pointer: store prefix + pointer
            let mut prefix = [0u8; 4];
            prefix.copy_from_slice(&bytes[..PREFIX_LEN]);

            Self {
                length: len,
                prefix,
                ptr_or_inlined: bytes.as_ptr() as u64,
            }
        }
    }

    /// Create a StringView from a raw pointer and length.
    ///
    /// # Safety
    /// The pointer must point to valid UTF-8 data of at least `len` bytes,
    /// and must remain valid for the lifetime of this StringView.
    #[inline]
    pub unsafe fn from_ptr(ptr: *const u8, len: u32) -> Self {
        if (len as usize) <= INLINE_CAPACITY {
            // Inlined: copy data
            let mut prefix = [0u8; 4];
            let mut suffix = [0u8; 8];

            let prefix_len = (len as usize).min(4);
            std::ptr::copy_nonoverlapping(ptr, prefix.as_mut_ptr(), prefix_len);

            if len > 4 {
                let suffix_len = len as usize - 4;
                std::ptr::copy_nonoverlapping(ptr.add(4), suffix.as_mut_ptr(), suffix_len);
            }

            Self {
                length: len,
                prefix,
                ptr_or_inlined: u64::from_le_bytes(suffix),
            }
        } else {
            // Pointer: store prefix + pointer
            let mut prefix = [0u8; 4];
            std::ptr::copy_nonoverlapping(ptr, prefix.as_mut_ptr(), PREFIX_LEN);

            Self {
                length: len,
                prefix,
                ptr_or_inlined: ptr as u64,
            }
        }
    }

    /// Create an empty string.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            length: 0,
            prefix: [0u8; 4],
            ptr_or_inlined: 0,
        }
    }

    /// Returns true if the string is stored inline (≤12 bytes).
    #[inline]
    pub fn is_inlined(&self) -> bool {
        (self.length as usize) <= INLINE_CAPACITY
    }

    /// Returns the length of the string in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// Returns true if the string is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Get the raw data pointer.
    ///
    /// For inlined strings, returns pointer to internal buffer.
    /// For pointer strings, returns the stored pointer.
    #[inline]
    pub fn get_data(&self) -> *const u8 {
        if self.is_inlined() {
            self.prefix.as_ptr()
        } else {
            self.ptr_or_inlined as *const u8
        }
    }

    /// Get the string data as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        if self.is_inlined() {
            // For inlined strings, we need to reconstruct from prefix + suffix
            // SAFETY: We're reading from our own struct
            unsafe {
                let ptr = &self.prefix as *const [u8; 4] as *const u8;
                std::slice::from_raw_parts(ptr, self.len())
            }
        } else {
            // SAFETY: Pointer is valid (caller's responsibility)
            unsafe {
                let ptr = self.ptr_or_inlined as *const u8;
                std::slice::from_raw_parts(ptr, self.len())
            }
        }
    }

    /// Get the string data as a str slice.
    ///
    /// # Safety
    /// Assumes the data is valid UTF-8. This is guaranteed if the StringView
    /// was created from valid UTF-8 data.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: We assume valid UTF-8 (enforced at construction)
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Get the prefix bytes (first 4 bytes).
    ///
    /// Used for fast comparison of long strings.
    #[inline]
    pub fn get_prefix(&self) -> &[u8; 4] {
        &self.prefix
    }

    /// Update the pointer for a pointer string.
    ///
    /// Used when relocating string data (e.g., after StringHeap reallocation).
    ///
    /// # Safety
    /// - Must only be called on pointer strings (len > 12)
    /// - New pointer must point to valid data of the same length
    #[inline]
    pub unsafe fn set_ptr(&mut self, new_ptr: *const u8) {
        debug_assert!(!self.is_inlined(), "Cannot set_ptr on inlined string");
        self.ptr_or_inlined = new_ptr as u64;
    }

    /// Finalize the string by updating the prefix from the data.
    ///
    /// Call this after modifying the underlying data to ensure the prefix
    /// is consistent with the actual string content.
    #[inline]
    pub fn finalize(&mut self) {
        if self.is_inlined() {
            // For inlined strings, zero out unused bytes in suffix
            let len = self.len();
            if len < 4 {
                // Zero unused prefix bytes
                for item in self.prefix.iter_mut().take(4).skip(len) {
                    *item = 0;
                }
                self.ptr_or_inlined = 0;
            } else if len < 12 {
                // Zero unused suffix bytes
                let suffix_len = len - 4;
                let mut suffix = self.ptr_or_inlined.to_le_bytes();
                for item in suffix.iter_mut().take(8).skip(suffix_len) {
                    *item = 0;
                }
                self.ptr_or_inlined = u64::from_le_bytes(suffix);
            }
        } else {
            // For pointer strings, copy prefix from data
            // SAFETY: Pointer is valid
            unsafe {
                let data_ptr = self.ptr_or_inlined as *const u8;
                std::ptr::copy_nonoverlapping(data_ptr, self.prefix.as_mut_ptr(), PREFIX_LEN);
            }
        }
    }
}

// --- Comparison operations ---

impl StringView {
    /// Fast equality check using bulk comparison.
    ///
    /// Optimized path:
    /// 1. Compare first 8 bytes (length + prefix) as u64
    /// 2. Compare next 8 bytes as u64
    /// 3. For pointer strings with same prefix, compare full data
    #[inline]
    pub fn equals(&self, other: &Self) -> bool {
        // Quick length check
        if self.length != other.length {
            return false;
        }

        // Compare prefix
        if self.prefix != other.prefix {
            return false;
        }

        if self.is_inlined() {
            // For inlined strings, compare the suffix
            self.ptr_or_inlined == other.ptr_or_inlined
        } else {
            // For pointer strings, compare actual data
            // (prefix already matched, so compare full data)
            let len = self.len();
            let self_data = self.ptr_or_inlined as *const u8;
            let other_data = other.ptr_or_inlined as *const u8;

            // SAFETY: Pointers are valid
            unsafe {
                std::slice::from_raw_parts(self_data, len)
                    == std::slice::from_raw_parts(other_data, len)
            }
        }
    }

    /// Lexicographic greater-than comparison.
    ///
    /// Optimized path:
    /// 1. Compare prefixes first (fast path for different prefixes)
    /// 2. Fall back to full memcmp for equal prefixes
    #[inline]
    pub fn greater_than(&self, other: &Self) -> bool {
        let self_len = self.len();
        let other_len = other.len();
        let min_len = self_len.min(other_len);

        // Compare prefixes first (for strings >= 4 bytes)
        if min_len >= PREFIX_LEN {
            let self_prefix = u32::from_be_bytes(self.prefix);
            let other_prefix = u32::from_be_bytes(other.prefix);

            if self_prefix != other_prefix {
                return self_prefix > other_prefix;
            }
        }

        // Full comparison
        let self_data = self.as_bytes();
        let other_data = other.as_bytes();

        match self_data[..min_len].cmp(&other_data[..min_len]) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => self_len > other_len,
        }
    }

    /// Lexicographic less-than comparison.
    #[inline]
    pub fn less_than(&self, other: &Self) -> bool {
        other.greater_than(self)
    }

    /// Lexicographic greater-than-or-equal comparison.
    #[inline]
    pub fn greater_than_or_equal(&self, other: &Self) -> bool {
        !self.less_than(other)
    }

    /// Lexicographic less-than-or-equal comparison.
    #[inline]
    pub fn less_than_or_equal(&self, other: &Self) -> bool {
        !self.greater_than(other)
    }
}

// --- Trait Implementations ---

impl Default for StringView {
    fn default() -> Self {
        Self::empty()
    }
}

impl PartialEq for StringView {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl Eq for StringView {}

impl PartialOrd for StringView {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StringView {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        if self.equals(other) {
            Ordering::Equal
        } else if self.greater_than(other) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl Hash for StringView {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for StringView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StringView({:?})", self.as_str())
    }
}

impl fmt::Display for StringView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for StringView {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for StringView {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

impl From<&String> for StringView {
    fn from(s: &String) -> Self {
        Self::new(s)
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size() {
        assert_eq!(std::mem::size_of::<StringView>(), 16);
    }

    #[test]
    fn test_empty_string() {
        let s = StringView::empty();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert!(s.is_inlined());
        assert_eq!(s.as_str(), "");
    }

    #[test]
    fn test_short_inlined() {
        let s = StringView::new("hello");
        assert!(!s.is_empty());
        assert_eq!(s.len(), 5);
        assert!(s.is_inlined());
        assert_eq!(s.as_str(), "hello");
    }

    #[test]
    fn test_max_inlined_string() {
        let s = StringView::new("123456789012");
        assert_eq!(s.len(), 12);
        assert!(s.is_inlined());
        assert_eq!(s.as_str(), "123456789012");
    }

    #[test]
    fn test_long_string_pointer() {
        let data = "1234567890123";
        let s = StringView::new(data);
        assert_eq!(s.len(), 13);
        assert!(!s.is_inlined());
        assert_eq!(s.as_str(), data);
    }

    #[test]
    fn test_equality_inlined() {
        let s1 = StringView::new("hello");
        let s2 = StringView::new("hello");
        let s3 = StringView::new("world");

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_equality_pointer() {
        let data1 = "this is a long string";
        let data2 = "this is a long string";
        let data3 = "this is another long string";

        let s1 = StringView::new(data1);
        let s2 = StringView::new(data2);
        let s3 = StringView::new(data3);

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_ordering_inlined() {
        let s1 = StringView::new("apple");
        let s2 = StringView::new("banana");
        let s3 = StringView::new("apple");

        assert!(s1 < s2);
        assert!(s2 > s1);
        assert!(s1 <= s3);
        assert!(s1 >= s3);
    }

    #[test]
    fn test_ordering_pointer() {
        let s1 = StringView::new("apple is a fruit");
        let s2 = StringView::new("banana is a fruit");
        let s3 = StringView::new("apple is a fruit");

        assert!(s1 < s2);
        assert!(s2 > s1);
        assert!(s1 <= s3);
        assert!(s1 >= s3);
    }

    #[test]
    fn test_length_ordering() {
        let s1 = StringView::new("abc");
        let s2 = StringView::new("abcd");

        assert!(s1 < s2);
        assert!(s2 > s1);
    }

    #[test]
    fn test_prefix_optimization() {
        let s1 = StringView::new("aaaa_long_string_here");
        let s2 = StringView::new("bbbb_long_string_here");

        assert!(s1 < s2);
        assert!(s2 > s1);
    }

    #[test]
    fn test_from_bytes() {
        let bytes = b"hello world";
        let s = StringView::from_bytes(bytes);
        assert_eq!(s.as_bytes(), bytes);
    }

    #[test]
    fn test_from_ptr() {
        let data = "test string";
        let s = unsafe { StringView::from_ptr(data.as_ptr(), data.len() as u32) };
        assert_eq!(s.as_str(), data);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(StringView::new("hello"));
        set.insert(StringView::new("world"));
        set.insert(StringView::new("hello"));

        assert_eq!(set.len(), 2);
        assert!(set.contains(&StringView::new("hello")));
        assert!(set.contains(&StringView::new("world")));
    }

    #[test]
    fn test_finalize() {
        let mut s = StringView::new("hi");
        s.finalize();
        assert_eq!(s.as_str(), "hi");
    }

    #[test]
    fn test_default() {
        let s: StringView = Default::default();
        assert!(s.is_empty());
        assert_eq!(s.as_str(), "");
    }

    #[test]
    fn test_display() {
        let s = StringView::new("hello");
        assert_eq!(format!("{}", s), "hello");
    }

    #[test]
    fn test_very_short_strings() {
        let s1 = StringView::new("a");
        let s2 = StringView::new("ab");
        let s3 = StringView::new("abc");

        assert_eq!(s1.as_str(), "a");
        assert_eq!(s2.as_str(), "ab");
        assert_eq!(s3.as_str(), "abc");

        assert!(s1 < s2);
        assert!(s2 < s3);
    }

    #[test]
    fn test_boundary_strings() {
        let s4 = StringView::new("1234");
        let s8 = StringView::new("12345678");
        let s12 = StringView::new("123456789012");
        let s13 = StringView::new("1234567890123");

        assert!(s4.is_inlined());
        assert!(s8.is_inlined());
        assert!(s12.is_inlined());
        assert!(!s13.is_inlined());

        assert_eq!(s4.as_str(), "1234");
        assert_eq!(s8.as_str(), "12345678");
        assert_eq!(s12.as_str(), "123456789012");
        assert_eq!(s13.as_str(), "1234567890123");
    }

    // ==========================================================================
    #[test]
    fn test_comparison_equals_method() {
        // Test the equals() method directly
        let s1 = StringView::new("hello");
        let s2 = StringView::new("hello");
        let s3 = StringView::new("world");

        assert!(s1.equals(&s2));
        assert!(!s1.equals(&s3));

        // Long strings
        let long1 = StringView::new("this is a very long string");
        let long2 = StringView::new("this is a very long string");
        let long3 = StringView::new("this is a different long string");

        assert!(long1.equals(&long2));
        assert!(!long1.equals(&long3));
    }

    #[test]
    fn test_comparison_greater_than_method() {
        // Test the greater_than() method directly
        let apple = StringView::new("apple");
        let banana = StringView::new("banana");

        assert!(!apple.greater_than(&banana));
        assert!(banana.greater_than(&apple));
        assert!(!apple.greater_than(&apple));
    }

    #[test]
    fn test_comparison_less_than_method() {
        // Test the less_than() method directly
        let apple = StringView::new("apple");
        let banana = StringView::new("banana");

        assert!(apple.less_than(&banana));
        assert!(!banana.less_than(&apple));
        assert!(!apple.less_than(&apple));
    }

    #[test]
    fn test_comparison_gte_lte_methods() {
        let s1 = StringView::new("abc");
        let s2 = StringView::new("abc");
        let s3 = StringView::new("abd");

        assert!(s1.greater_than_or_equal(&s2));
        assert!(s1.less_than_or_equal(&s2));
        assert!(s1.less_than_or_equal(&s3));
        assert!(s3.greater_than_or_equal(&s1));
    }

    #[test]
    fn test_mixed_inlined_pointer_comparison() {
        // Compare short (inlined) with long (pointer) strings
        let short = StringView::new("abc");
        let long = StringView::new("abcdefghijklmnop");

        assert!(short < long);
        assert!(short.less_than(&long));
        assert!(long > short);
        assert!(long.greater_than(&short));
        assert_ne!(short, long);
    }

    #[test]
    fn test_prefix_same_different_suffix_pointer() {
        // Test strings with same prefix but different suffix (for pointer strings)
        let s1 = StringView::new("same_prefix_different_end_1");
        let s2 = StringView::new("same_prefix_different_end_2");

        assert!(!s1.equals(&s2));
        assert!(s1 < s2);
        assert!(s2 > s1);
    }

    #[test]
    fn test_unicode_strings() {
        // Test UTF-8 unicode strings
        let emoji = StringView::new("🎉");
        let chinese = StringView::new("你好");
        let japanese = StringView::new("こんにちは");

        // Emoji is 4 bytes, so it's inlined
        assert!(emoji.is_inlined());
        assert_eq!(emoji.len(), 4);
        assert_eq!(emoji.as_str(), "🎉");

        // Chinese "你好" is 6 bytes, so it's inlined
        assert!(chinese.is_inlined());
        assert_eq!(chinese.len(), 6);
        assert_eq!(chinese.as_str(), "你好");

        // Japanese "こんにちは" is 15 bytes, so it's a pointer
        assert!(!japanese.is_inlined());
        assert_eq!(japanese.len(), 15);
        assert_eq!(japanese.as_str(), "こんにちは");
    }

    #[test]
    fn test_special_characters() {
        let s1 = StringView::new("\t\n\r");
        let s2 = StringView::new("\0\0\0");
        let s3 = StringView::new("a\x00b");

        assert_eq!(s1.as_str(), "\t\n\r");
        assert_eq!(s2.as_bytes(), b"\0\0\0");
        assert_eq!(s3.as_bytes(), b"a\0b");
    }

    #[test]
    fn test_copy_semantics() {
        let s1 = StringView::new("hello");
        let s2 = s1; // Copy, not move
        let s3 = s1; // Still valid

        assert_eq!(s1, s2);
        assert_eq!(s2, s3);
    }

    #[test]
    fn test_get_prefix() {
        let short = StringView::new("hi");
        let exact_prefix = StringView::new("1234");
        let long = StringView::new("hello_world_this_is_long");

        // For inlined strings, prefix is first 4 bytes of data
        assert_eq!(short.get_prefix()[..2], *b"hi");
        assert_eq!(exact_prefix.get_prefix(), b"1234");
        assert_eq!(long.get_prefix(), b"hell");
    }

    #[test]
    fn test_get_data_pointer() {
        let inlined = StringView::new("hello");
        let long = StringView::new("this is a very long string");

        // Both should return valid pointers
        let ptr1 = inlined.get_data();
        let ptr2 = long.get_data();

        assert!(!ptr1.is_null());
        assert!(!ptr2.is_null());

        // Should be able to read the data
        unsafe {
            assert_eq!(*ptr1, b'h');
            assert_eq!(*ptr2, b't');
        }
    }

    #[test]
    fn test_ordering_edge_cases() {
        // Empty string should be less than any non-empty string
        let empty = StringView::empty();
        let non_empty = StringView::new("a");

        assert!(empty < non_empty);
        assert!(non_empty > empty);

        // Same length strings with different content
        let abc = StringView::new("abc");
        let abd = StringView::new("abd");
        assert!(abc < abd);

        // Prefix comparison for very long strings
        let long_a = StringView::new("aaaa_very_very_long_string_here_1");
        let long_b = StringView::new("bbbb_very_very_long_string_here_2");
        assert!(long_a < long_b);
    }

    #[test]
    fn test_from_string_owned() {
        let owned = String::from("hello world");
        let s = StringView::from(owned.clone());
        assert_eq!(s.as_str(), "hello world");

        let s2 = StringView::from(&owned);
        assert_eq!(s2, s);
    }

    #[test]
    fn test_to_string_conversion() {
        let s = StringView::new("hello");
        let owned = s.to_string();
        assert_eq!(owned, "hello");

        let long = StringView::new("this is a long string for testing");
        let owned_long = long.to_string();
        assert_eq!(owned_long, "this is a long string for testing");
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        let key = StringView::new("test_key");
        map.insert(key, 42);

        // Same content should hash to same value
        let lookup_key = StringView::new("test_key");
        assert_eq!(map.get(&lookup_key), Some(&42));

        // Different content should not be found
        let other_key = StringView::new("other_key");
        assert_eq!(map.get(&other_key), None);
    }

    #[test]
    fn test_ord_trait() {
        // Test Ord trait through sort
        let mut strings = [
            StringView::new("cherry"),
            StringView::new("apple"),
            StringView::new("banana"),
        ];
        strings.sort();

        assert_eq!(strings[0].as_str(), "apple");
        assert_eq!(strings[1].as_str(), "banana");
        assert_eq!(strings[2].as_str(), "cherry");
    }

    #[test]
    fn test_partial_ord_trait() {
        let a = StringView::new("abc");
        let b = StringView::new("def");

        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Less));
        assert_eq!(b.partial_cmp(&a), Some(std::cmp::Ordering::Greater));
        assert_eq!(a.partial_cmp(&a), Some(std::cmp::Ordering::Equal));
    }
}
