// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! A non-owning, 16-byte view over variable-length bytes.
//!
//! Values up to 12 bytes are stored directly in the view. Longer values retain
//! a four-byte comparison prefix and a pointer into storage owned elsewhere,
//! normally a vector [`StringHeap`](crate::vector::StringHeap) or a row heap.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

const LENGTH_LEN: usize = std::mem::size_of::<u32>();
const PREFIX_LEN: usize = 4;
const INLINE_CAPACITY: usize = 12;
const INLINE_SUFFIX_LEN: usize = INLINE_CAPACITY - PREFIX_LEN;
const PAYLOAD_OFFSET: usize = LENGTH_LEN + PREFIX_LEN;

#[repr(C)]
#[derive(Clone, Copy)]
union StringViewPayload {
    pointer: *const u8,
    inline: [u8; INLINE_SUFFIX_LEN],
}

/// Non-owning view over a variable-length byte sequence.
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
/// Inline values are canonical: every unused byte in the 12-byte payload is
/// zero. Equality relies on this invariant for its fixed-width inline fast path.
/// All constructors and row-cell writers owned by this type preserve it.
///
/// Out-of-line values do not own their data. Unsafe construction sites must
/// keep the backing allocation alive and immutable for every use of the view.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StringView {
    /// Viewed length in bytes
    length: u32,
    /// First 4 bytes: prefix (for pointer) or inlined[0..4]
    prefix: [u8; PREFIX_LEN],
    /// Last 8 bytes: pointer (for long strings) or inlined[4..12]
    payload: StringViewPayload,
}

const _: () = assert!(INLINE_CAPACITY == PREFIX_LEN + INLINE_SUFFIX_LEN);
const _: () = assert!(std::mem::size_of::<*const u8>() == INLINE_SUFFIX_LEN);
const _: () = assert!(StringView::SIZE == 16);
const _: () = assert!(std::mem::size_of::<StringView>() == StringView::SIZE);
const _: () = assert!(std::mem::align_of::<StringView>() >= std::mem::align_of::<u64>());
const _: () = assert!(std::mem::offset_of!(StringView, length) == 0);
const _: () = assert!(std::mem::offset_of!(StringView, prefix) == LENGTH_LEN);
const _: () = assert!(std::mem::offset_of!(StringView, payload) == PAYLOAD_OFFSET);

// SAFETY: an out-of-line view points to immutable bytes. Moving or sharing the
// view between threads is sound when its unsafe construction contract is upheld:
// the backing owner must outlive every access and must not mutate the bytes.
unsafe impl Send for StringView {}
// SAFETY: see the `Send` implementation above.
unsafe impl Sync for StringView {}

impl StringView {
    /// Fixed physical width of a view and of a compatible row cell.
    pub const SIZE: usize = LENGTH_LEN + INLINE_CAPACITY;

    /// Maximum byte length stored directly in the view.
    pub const INLINE_CAPACITY: usize = INLINE_CAPACITY;

    /// Number of leading bytes cached by an out-of-line view.
    pub const PREFIX_LEN: usize = PREFIX_LEN;

    /// Construct a self-contained inline view.
    ///
    /// Returns `None` when `bytes` exceeds [`Self::INLINE_CAPACITY`]. Longer
    /// values must be installed in an owning heap before a view is created.
    #[inline]
    pub fn try_inline(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > INLINE_CAPACITY {
            return None;
        }

        let mut prefix = [0u8; PREFIX_LEN];
        let mut suffix = [0u8; INLINE_SUFFIX_LEN];
        let prefix_len = bytes.len().min(PREFIX_LEN);
        prefix[..prefix_len].copy_from_slice(&bytes[..prefix_len]);

        if bytes.len() > PREFIX_LEN {
            let suffix_len = bytes.len() - PREFIX_LEN;
            suffix[..suffix_len].copy_from_slice(&bytes[PREFIX_LEN..]);
        }

        Some(Self {
            length: bytes.len() as u32,
            prefix,
            payload: StringViewPayload { inline: suffix },
        })
    }

    /// Construct a view from initialized raw bytes.
    ///
    /// # Safety
    /// - `ptr` must be readable for `len` initialized bytes.
    /// - For `len > Self::INLINE_CAPACITY`, the allocation must remain alive
    ///   and immutable for every subsequent access through the returned view.
    /// - The returned view must not escape that backing allocation's lifetime.
    #[inline]
    pub unsafe fn from_raw_parts(ptr: *const u8, len: u32) -> Self {
        if (len as usize) <= INLINE_CAPACITY {
            let mut prefix = [0u8; PREFIX_LEN];
            let mut suffix = [0u8; INLINE_SUFFIX_LEN];

            let prefix_len = (len as usize).min(PREFIX_LEN);
            if prefix_len != 0 {
                unsafe { std::ptr::copy_nonoverlapping(ptr, prefix.as_mut_ptr(), prefix_len) };
            }

            if len as usize > PREFIX_LEN {
                let suffix_len = len as usize - PREFIX_LEN;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        ptr.add(PREFIX_LEN),
                        suffix.as_mut_ptr(),
                        suffix_len,
                    )
                };
            }

            Self {
                length: len,
                prefix,
                payload: StringViewPayload { inline: suffix },
            }
        } else {
            let mut prefix = [0u8; PREFIX_LEN];
            unsafe { std::ptr::copy_nonoverlapping(ptr, prefix.as_mut_ptr(), PREFIX_LEN) };

            Self {
                length: len,
                prefix,
                payload: StringViewPayload { pointer: ptr },
            }
        }
    }

    /// Construct an out-of-line view after `ptr` has been installed in its
    /// owning allocation.
    ///
    /// The comparison prefix is copied from `prefix_source`, avoiding a second
    /// read through a freshly retained or relocated pointer.
    ///
    /// # Safety
    /// - `len` must exceed [`Self::INLINE_CAPACITY`].
    /// - `prefix_source` must contain at least [`Self::PREFIX_LEN`] bytes and
    ///   its first four bytes must match the bytes at `ptr`.
    /// - `ptr` must be readable for `len` initialized bytes, and its allocation
    ///   must remain alive and immutable for every use of the returned view.
    #[inline]
    pub unsafe fn from_out_of_line(prefix_source: &[u8], ptr: *const u8, len: u32) -> Self {
        debug_assert!((len as usize) > INLINE_CAPACITY);
        debug_assert!(prefix_source.len() >= PREFIX_LEN);
        debug_assert!(!ptr.is_null());

        let mut prefix = [0u8; PREFIX_LEN];
        // SAFETY: the caller guarantees that `prefix_source` contains four
        // bytes. The destination is a distinct, initialized local array.
        unsafe {
            std::ptr::copy_nonoverlapping(prefix_source.as_ptr(), prefix.as_mut_ptr(), PREFIX_LEN)
        };
        Self {
            length: len,
            prefix,
            payload: StringViewPayload { pointer: ptr },
        }
    }

    /// Create an empty view.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            length: 0,
            prefix: [0u8; PREFIX_LEN],
            payload: StringViewPayload {
                inline: [0u8; INLINE_SUFFIX_LEN],
            },
        }
    }

    /// Returns true if the string is stored inline (≤12 bytes).
    #[inline]
    pub fn is_inlined(&self) -> bool {
        (self.length as usize) <= Self::INLINE_CAPACITY
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

    /// Borrow the viewed bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        if self.is_inlined() {
            // SAFETY: the layout assertions above guarantee a contiguous,
            // initialized 12-byte inline payload immediately after `length`.
            unsafe {
                let inline = std::ptr::from_ref(self).cast::<u8>().add(LENGTH_LEN);
                std::slice::from_raw_parts(inline, self.len())
            }
        } else {
            // SAFETY: validity is part of the out-of-line view invariant
            // established by every unsafe construction boundary.
            unsafe {
                let ptr = self.payload.pointer;
                std::slice::from_raw_parts(ptr, self.len())
            }
        }
    }

    /// Interpret the viewed bytes as UTF-8.
    #[inline]
    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_bytes())
    }

    /// Interpret the viewed bytes as UTF-8 without validation.
    ///
    /// # Safety
    /// The bytes must be valid UTF-8. Callers normally establish this from the
    /// logical type of the owning vector (for example `LogicalType::Varchar`).
    #[inline]
    pub unsafe fn as_str_unchecked(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Return the cached leading bytes used by comparisons.
    #[inline]
    pub fn prefix(&self) -> &[u8; PREFIX_LEN] {
        &self.prefix
    }

    /// Return the backing pointer for an out-of-line value.
    ///
    /// # Safety
    /// The view must not be inline, and its backing allocation must still be
    /// alive under the view's construction contract.
    #[inline]
    pub unsafe fn heap_ptr(&self) -> *const u8 {
        debug_assert!(!self.is_inlined(), "inline values have no heap pointer");
        // SAFETY: the caller guarantees that the pointer representation is active.
        unsafe { self.payload.pointer }
    }

    /// Update the pointer for an out-of-line value.
    ///
    /// Used when relocating string data (e.g., after StringHeap reallocation).
    ///
    /// # Safety
    /// - Must only be called on out-of-line values (len > 12)
    /// - New pointer must point to the same byte sequence at a new location,
    ///   preserving the cached prefix and length
    #[inline]
    pub unsafe fn set_ptr(&mut self, new_ptr: *const u8) {
        debug_assert!(!self.is_inlined(), "Cannot set_ptr on inlined string");
        self.payload = StringViewPayload { pointer: new_ptr };
    }

    /// Read the byte length directly from an unaligned physical row cell.
    ///
    /// # Safety
    /// `src` must be readable for at least [`LENGTH_LEN`] bytes and contain the
    /// leading bytes of a valid `StringView` row cell.
    #[inline]
    pub unsafe fn cell_len(src: *const u8) -> usize {
        unsafe { std::ptr::read_unaligned(src.cast::<u32>()) as usize }
    }

    /// Read only the backing pointer from an out-of-line physical row cell.
    ///
    /// # Safety
    /// `src` must be readable for [`Self::SIZE`] bytes and contain a valid
    /// out-of-line `StringView` row cell whose pointer is still live.
    #[inline]
    pub unsafe fn cell_heap_ptr(src: *const u8) -> *const u8 {
        debug_assert!(unsafe { Self::cell_len(src) } > INLINE_CAPACITY);
        let pointer =
            unsafe { std::ptr::read_unaligned(src.add(PAYLOAD_OFFSET).cast::<*const u8>()) };
        debug_assert!(!pointer.is_null());
        pointer
    }

    /// Replace only the backing pointer in an out-of-line physical row cell.
    ///
    /// # Safety
    /// - `dst` must be writable for [`Self::SIZE`] bytes and contain a valid
    ///   out-of-line `StringView` row cell.
    /// - `new_ptr` must point to the same immutable byte sequence at its new
    ///   location and remain valid for every subsequent access through the cell.
    #[inline]
    pub unsafe fn set_cell_heap_ptr(dst: *mut u8, new_ptr: *const u8) {
        debug_assert!(unsafe { Self::cell_len(dst) } > INLINE_CAPACITY);
        debug_assert!(!new_ptr.is_null());
        unsafe { std::ptr::write_unaligned(dst.add(PAYLOAD_OFFSET).cast::<*const u8>(), new_ptr) };
    }

    /// Read a view from an unaligned physical row cell.
    ///
    /// # Safety
    /// `src` must be readable for [`Self::SIZE`] bytes and contain a canonical
    /// cell previously written by [`Self::write_cell`]. This function does not
    /// validate the cell contents in release builds. Any out-of-line pointer in
    /// the cell must remain valid and immutable for every use of the result.
    #[inline]
    pub unsafe fn from_cell(src: *const u8) -> Self {
        let value = unsafe { std::ptr::read_unaligned(src.cast::<Self>()) };
        debug_assert!(value.is_inlined() || !unsafe { value.payload.pointer }.is_null());
        value
    }

    /// Write this view to an unaligned physical row cell.
    ///
    /// # Safety
    /// `dst` must be writable for [`Self::SIZE`] bytes. For an out-of-line
    /// value, the caller must ensure that the backing owner outlives the cell.
    #[inline]
    pub unsafe fn write_cell(&self, dst: *mut u8) {
        unsafe { std::ptr::write_unaligned(dst.cast::<Self>(), *self) };
    }

    /// Read the fixed-width length and prefix head as one machine word.
    #[inline]
    fn head(&self) -> u64 {
        // SAFETY: the compile-time layout and alignment assertions guarantee
        // that the first eight bytes are initialized and aligned for `u64`.
        unsafe { std::ptr::read(std::ptr::from_ref(self).cast::<u64>()) }
    }

    #[inline]
    fn inline_suffix(&self) -> [u8; INLINE_SUFFIX_LEN] {
        debug_assert!(self.is_inlined());
        // SAFETY: the inline representation is active by the length invariant.
        unsafe { self.payload.inline }
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
        if self.head() != other.head() {
            return false;
        }

        if self.is_inlined() {
            u64::from_ne_bytes(self.inline_suffix()) == u64::from_ne_bytes(other.inline_suffix())
        } else {
            // Prefix and length already match, so compare only the remaining
            // bytes. This is the single variable-width equality read.
            self.as_bytes()[PREFIX_LEN..] == other.as_bytes()[PREFIX_LEN..]
        }
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
        let self_len = self.len();
        let other_len = other.len();
        let prefix_ordering =
            u32::from_be_bytes(self.prefix).cmp(&u32::from_be_bytes(other.prefix));
        if prefix_ordering != Ordering::Equal {
            return prefix_ordering;
        }

        if self.is_inlined() && other.is_inlined() {
            let suffix_ordering = u64::from_be_bytes(self.inline_suffix())
                .cmp(&u64::from_be_bytes(other.inline_suffix()));
            return suffix_ordering.then_with(|| self_len.cmp(&other_len));
        }

        let min_len = self_len.min(other_len);
        if min_len <= PREFIX_LEN {
            return self_len.cmp(&other_len);
        }
        let content_ordering =
            self.as_bytes()[PREFIX_LEN..min_len].cmp(&other.as_bytes()[PREFIX_LEN..min_len]);

        if content_ordering == Ordering::Equal {
            self_len.cmp(&other_len)
        } else {
            content_ordering
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
        match self.as_str() {
            Ok(value) => f.debug_tuple("StringView").field(&value).finish(),
            Err(_) => f.debug_tuple("StringView").field(&self.as_bytes()).finish(),
        }
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    fn view(value: &'static str) -> StringView {
        view_bytes(value.as_bytes())
    }

    fn view_bytes(value: &'static [u8]) -> StringView {
        StringView::try_inline(value).unwrap_or_else(|| {
            // SAFETY: byte and string literals have immutable static storage.
            unsafe { StringView::from_raw_parts(value.as_ptr(), value.len() as u32) }
        })
    }

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
        assert_eq!(s.as_str().unwrap(), "");
    }

    #[test]
    fn test_short_inlined() {
        let s = view("hello");
        assert!(!s.is_empty());
        assert_eq!(s.len(), 5);
        assert!(s.is_inlined());
        assert_eq!(s.as_str().unwrap(), "hello");
    }

    #[test]
    fn test_max_inlined_string() {
        let s = view("123456789012");
        assert_eq!(s.len(), 12);
        assert!(s.is_inlined());
        assert_eq!(s.as_str().unwrap(), "123456789012");
    }

    #[test]
    fn test_long_string_pointer() {
        let data = "1234567890123";
        let s = view(data);
        assert_eq!(s.len(), 13);
        assert!(!s.is_inlined());
        assert_eq!(s.as_str().unwrap(), data);
    }

    #[test]
    fn test_equality_inlined() {
        let s1 = view("hello");
        let s2 = view("hello");
        let s3 = view("world");

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_equality_pointer() {
        let data1 = "this is a long string";
        let data2 = "this is a long string";
        let data3 = "this is another long string";

        let s1 = view(data1);
        let s2 = view(data2);
        let s3 = view(data3);

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_ordering_inlined() {
        let s1 = view("apple");
        let s2 = view("banana");
        let s3 = view("apple");

        assert!(s1 < s2);
        assert!(s2 > s1);
        assert!(s1 <= s3);
        assert!(s1 >= s3);
    }

    #[test]
    fn test_ordering_pointer() {
        let s1 = view("apple is a fruit");
        let s2 = view("banana is a fruit");
        let s3 = view("apple is a fruit");

        assert!(s1 < s2);
        assert!(s2 > s1);
        assert!(s1 <= s3);
        assert!(s1 >= s3);
    }

    #[test]
    fn test_length_ordering() {
        let s1 = view("abc");
        let s2 = view("abcd");

        assert!(s1 < s2);
        assert!(s2 > s1);
    }

    #[test]
    fn test_prefix_optimization() {
        let s1 = view("aaaa_long_string_here");
        let s2 = view("bbbb_long_string_here");

        assert!(s1 < s2);
        assert!(s2 > s1);
    }

    #[test]
    fn test_try_inline() {
        let bytes = b"hello world";
        let s = StringView::try_inline(bytes).unwrap();
        assert_eq!(s.as_bytes(), bytes);
        assert!(StringView::try_inline(b"thirteen bytes").is_none());
    }

    #[test]
    fn test_from_raw_parts() {
        let data = "test string";
        let s = unsafe { StringView::from_raw_parts(data.as_ptr(), data.len() as u32) };
        assert_eq!(s.as_str().unwrap(), data);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(view("hello"));
        set.insert(view("world"));
        set.insert(view("hello"));

        assert_eq!(set.len(), 2);
        assert!(set.contains(&view("hello")));
        assert!(set.contains(&view("world")));
    }

    #[test]
    fn test_cell_roundtrip_preserves_canonical_layout() {
        for bytes in [b"hi".as_slice(), b"this value is out of line".as_slice()] {
            let value = view_bytes(bytes);
            let mut cell = [0xff; StringView::SIZE];
            // SAFETY: `cell` is writable for one complete view.
            unsafe { value.write_cell(cell.as_mut_ptr()) };
            if value.is_inlined() {
                assert!(cell[LENGTH_LEN + value.len()..]
                    .iter()
                    .all(|byte| *byte == 0));
            }

            // SAFETY: the cell was just initialized by `write_cell`; any
            // pointer refers to immutable static test data.
            let decoded = unsafe { StringView::from_cell(cell.as_ptr()) };
            assert_eq!(decoded, value);
            assert_eq!(decoded.as_bytes(), bytes);
        }
    }

    #[test]
    fn test_cell_pointer_relocation_updates_only_pointer() {
        let original = b"this value is out of line".to_vec();
        let relocated = original.clone();
        assert_ne!(original.as_ptr(), relocated.as_ptr());
        // SAFETY: `original` remains alive and immutable until all uses of the
        // view and row cell below have completed.
        let value = unsafe {
            StringView::from_raw_parts(original.as_ptr(), original.len().try_into().unwrap())
        };
        let mut cell = [0u8; StringView::SIZE];
        // SAFETY: the cell is writable and both vectors contain the same
        // immutable byte sequence for the full cell lifetime.
        unsafe {
            value.write_cell(cell.as_mut_ptr());
            assert_eq!(StringView::cell_len(cell.as_ptr()), original.len());
            assert_eq!(StringView::cell_heap_ptr(cell.as_ptr()), original.as_ptr());
            StringView::set_cell_heap_ptr(cell.as_mut_ptr(), relocated.as_ptr());
            assert_eq!(StringView::cell_heap_ptr(cell.as_ptr()), relocated.as_ptr());
        }
        let decoded = unsafe { StringView::from_cell(cell.as_ptr()) };
        assert_eq!(decoded.as_bytes(), relocated);
    }

    #[test]
    fn test_default() {
        let s: StringView = Default::default();
        assert!(s.is_empty());
        assert_eq!(s.as_str().unwrap(), "");
    }

    #[test]
    fn test_very_short_strings() {
        let s1 = view("a");
        let s2 = view("ab");
        let s3 = view("abc");

        assert_eq!(s1.as_str().unwrap(), "a");
        assert_eq!(s2.as_str().unwrap(), "ab");
        assert_eq!(s3.as_str().unwrap(), "abc");

        assert!(s1 < s2);
        assert!(s2 < s3);
    }

    #[test]
    fn test_boundary_strings() {
        let s4 = view("1234");
        let s8 = view("12345678");
        let s12 = view("123456789012");
        let s13 = view("1234567890123");

        assert!(s4.is_inlined());
        assert!(s8.is_inlined());
        assert!(s12.is_inlined());
        assert!(!s13.is_inlined());

        assert_eq!(s4.as_str().unwrap(), "1234");
        assert_eq!(s8.as_str().unwrap(), "12345678");
        assert_eq!(s12.as_str().unwrap(), "123456789012");
        assert_eq!(s13.as_str().unwrap(), "1234567890123");
    }

    #[test]
    fn test_equality_trait_fast_paths() {
        let s1 = view("hello");
        let s2 = view("hello");
        let s3 = view("world");

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);

        let long1 = view("this is a very long string");
        let long2 = view("this is a very long string");
        let long3 = view("this is a different long string");

        assert_eq!(long1, long2);
        assert_ne!(long1, long3);
    }

    #[test]
    fn test_mixed_inlined_pointer_comparison() {
        // Compare short (inlined) with long (pointer) strings
        let short = view("abc");
        let long = view("abcdefghijklmnop");

        assert!(short < long);
        assert!(long > short);
        assert_ne!(short, long);
    }

    #[test]
    fn test_prefix_same_different_suffix_pointer() {
        // Test strings with same prefix but different suffix (for pointer strings)
        let s1 = view("same_prefix_different_end_1");
        let s2 = view("same_prefix_different_end_2");

        assert_ne!(s1, s2);
        assert!(s1 < s2);
        assert!(s2 > s1);
    }

    #[test]
    fn test_unicode_strings() {
        // Test UTF-8 unicode strings
        let emoji = view("🎉");
        let chinese = view("你好");
        let japanese = view("こんにちは");

        // Emoji is 4 bytes, so it's inlined
        assert!(emoji.is_inlined());
        assert_eq!(emoji.len(), 4);
        assert_eq!(emoji.as_str().unwrap(), "🎉");

        // Chinese "你好" is 6 bytes, so it's inlined
        assert!(chinese.is_inlined());
        assert_eq!(chinese.len(), 6);
        assert_eq!(chinese.as_str().unwrap(), "你好");

        // Japanese "こんにちは" is 15 bytes, so it's a pointer
        assert!(!japanese.is_inlined());
        assert_eq!(japanese.len(), 15);
        assert_eq!(japanese.as_str().unwrap(), "こんにちは");
    }

    #[test]
    fn test_special_characters() {
        let s1 = view("\t\n\r");
        let s2 = view("\0\0\0");
        let s3 = view("a\x00b");

        assert_eq!(s1.as_str().unwrap(), "\t\n\r");
        assert_eq!(s2.as_bytes(), b"\0\0\0");
        assert_eq!(s3.as_bytes(), b"a\0b");
    }

    #[test]
    fn test_copy_semantics() {
        let s1 = view("hello");
        let s2 = s1; // Copy, not move
        let s3 = s1; // Still valid

        assert_eq!(s1, s2);
        assert_eq!(s2, s3);
    }

    #[test]
    fn test_prefix() {
        let short = view("hi");
        let exact_prefix = view("1234");
        let long = view("hello_world_this_is_long");

        // For inlined strings, prefix is first 4 bytes of data
        assert_eq!(short.prefix()[..2], *b"hi");
        assert_eq!(exact_prefix.prefix(), b"1234");
        assert_eq!(long.prefix(), b"hell");
    }

    #[test]
    fn test_blob_utf8_validation() {
        let value = StringView::try_inline(&[0xff]).unwrap();
        assert!(value.as_str().is_err());
    }

    #[test]
    fn test_ordering_edge_cases() {
        // Empty string should be less than any non-empty string
        let empty = StringView::empty();
        let non_empty = view("a");

        assert!(empty < non_empty);
        assert!(non_empty > empty);

        // Same length strings with different content
        let abc = view("abc");
        let abd = view("abd");
        assert!(abc < abd);

        // Prefix comparison for very long strings
        let long_a = view("aaaa_very_very_long_string_here_1");
        let long_b = view("bbbb_very_very_long_string_here_2");
        assert!(long_a < long_b);
    }

    #[test]
    fn test_ordering_matches_byte_slice_ordering() {
        const VALUES: &[&[u8]] = &[
            b"",
            b"a",
            b"a\0",
            b"a\0\0",
            b"ab",
            b"ab\0",
            b"abcd",
            b"abcd0",
            b"abcdefghijkl",
            b"abcdefghijklm",
            b"same_prefix_long_value_1",
            b"same_prefix_long_value_2",
            b"\xffbinary",
        ];

        for left in VALUES {
            for right in VALUES {
                let left_view = view_bytes(left);
                let right_view = view_bytes(right);
                assert_eq!(
                    left_view.cmp(&right_view),
                    left.cmp(right),
                    "left={left:?}, right={right:?}"
                );
                assert_eq!(
                    left_view == right_view,
                    left == right,
                    "left={left:?}, right={right:?}"
                );
            }
        }
    }

    #[test]
    fn test_send_sync_contract_is_explicit() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StringView>();
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        let key = view("test_key");
        map.insert(key, 42);

        // Same content should hash to same value
        let lookup_key = view("test_key");
        assert_eq!(map.get(&lookup_key), Some(&42));

        // Different content should not be found
        let other_key = view("other_key");
        assert_eq!(map.get(&other_key), None);
    }

    #[test]
    fn test_ord_trait() {
        // Test Ord trait through sort
        let mut strings = [view("cherry"), view("apple"), view("banana")];
        strings.sort();

        assert_eq!(strings[0].as_str().unwrap(), "apple");
        assert_eq!(strings[1].as_str().unwrap(), "banana");
        assert_eq!(strings[2].as_str().unwrap(), "cherry");
    }

    #[test]
    fn test_partial_ord_trait() {
        let a = view("abc");
        let b = view("def");

        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Less));
        assert_eq!(b.partial_cmp(&a), Some(std::cmp::Ordering::Greater));
        assert_eq!(a.partial_cmp(&a), Some(std::cmp::Ordering::Equal));
    }
}
