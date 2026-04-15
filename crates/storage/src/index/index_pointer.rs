//! # Index Pointer
//!
//! 64-bit pointer for index nodes with metadata, offset, and buffer ID.
//!
//! ## Bit Layout
//!
//! ```text
//! MSB                                                                        LSB
//! 63         56 55                      32 31                                  0
//! +------------+--------------------------+------------------------------------+
//! |  Metadata  |           Offset         |           Buffer ID                |
//! |   8 bits   |          24 bits         |            32 bits                 |
//! +------------+--------------------------+------------------------------------+
//! ```
//!
//! - Metadata (8 bits): Node type or other metadata
//! - Offset (24 bits): Offset within the buffer
//! - Buffer ID (32 bits): Identifier of the buffer

use std::fmt;

/// Bit-shifting constants
const SHIFT_OFFSET: u64 = 32;
const SHIFT_METADATA: u64 = 56;

/// AND mask constants
const AND_OFFSET: u64 = 0x0000_0000_00FF_FFFF;
const AND_BUFFER_ID: u64 = 0x0000_0000_FFFF_FFFF;
const AND_METADATA: u64 = 0xFF00_0000_0000_0000;

/// A 64-bit pointer for index nodes.
///
/// This compact representation stores:
/// - 8 bits of metadata (e.g., node type)
/// - 24 bits of offset within a buffer
/// - 32 bits of buffer ID
///
/// This design allows efficient storage and manipulation of index node references
/// while supporting up to 4 billion buffers with 16 million entries each.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct IndexPointer {
    data: u64,
}

impl IndexPointer {
    /// Maximum offset value (24 bits)
    pub const MAX_OFFSET: u32 = 0x00FF_FFFF;

    /// Maximum buffer ID value (32 bits)
    pub const MAX_BUFFER_ID: u32 = u32::MAX;

    /// Creates an empty (null) IndexPointer.
    #[inline]
    pub const fn new() -> Self {
        Self { data: 0 }
    }

    /// Creates an IndexPointer with the given buffer ID and offset.
    ///
    /// # Arguments
    /// * `buffer_id` - The buffer identifier (32 bits)
    /// * `offset` - The offset within the buffer (24 bits, will be masked)
    #[inline]
    pub const fn with_buffer_and_offset(buffer_id: u32, offset: u32) -> Self {
        let shifted_offset = ((offset as u64) & AND_OFFSET) << SHIFT_OFFSET;
        Self {
            data: shifted_offset | (buffer_id as u64),
        }
    }

    /// Creates an IndexPointer from raw 64-bit data.
    #[inline]
    pub const fn from_raw(data: u64) -> Self {
        Self { data }
    }

    /// Returns the raw 64-bit data.
    #[inline]
    pub const fn get(&self) -> u64 {
        self.data
    }

    /// Sets the raw 64-bit data.
    #[inline]
    pub fn set(&mut self, data: u64) {
        self.data = data;
    }

    /// Returns true if this pointer has metadata set.
    #[inline]
    pub const fn has_metadata(&self) -> bool {
        (self.data & AND_METADATA) != 0
    }

    /// Returns the metadata (bits 56-63).
    #[inline]
    pub const fn get_metadata(&self) -> u8 {
        (self.data >> SHIFT_METADATA) as u8
    }

    /// Sets the metadata (bits 56-63).
    #[inline]
    pub fn set_metadata(&mut self, metadata: u8) {
        self.data &= !AND_METADATA;
        self.data |= (metadata as u64) << SHIFT_METADATA;
    }

    /// Returns the offset (bits 32-55).
    #[inline]
    pub const fn get_offset(&self) -> u32 {
        ((self.data >> SHIFT_OFFSET) & AND_OFFSET) as u32
    }

    /// Sets the offset (bits 32-55).
    #[inline]
    pub fn set_offset(&mut self, offset: u32) {
        // Clear existing offset bits
        self.data &= !(AND_OFFSET << SHIFT_OFFSET);
        // Set new offset
        self.data |= ((offset as u64) & AND_OFFSET) << SHIFT_OFFSET;
    }

    /// Returns the buffer ID (bits 0-31).
    #[inline]
    pub const fn get_buffer_id(&self) -> u32 {
        (self.data & AND_BUFFER_ID) as u32
    }

    /// Sets the buffer ID (bits 0-31).
    #[inline]
    pub fn set_buffer_id(&mut self, buffer_id: u32) {
        self.data &= !AND_BUFFER_ID;
        self.data |= buffer_id as u64;
    }

    /// Resets the IndexPointer to empty (null).
    #[inline]
    pub fn clear(&mut self) {
        self.data = 0;
    }

    /// Returns true if this pointer is empty (null).
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.data == 0
    }

    /// Returns true if this pointer is valid (non-empty).
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.data != 0
    }

    /// Adds a value to the buffer ID.
    ///
    /// This is useful for adjusting buffer IDs during index merging.
    #[inline]
    pub fn increase_buffer_id(&mut self, summand: u32) {
        // Only add to the lower 32 bits
        let current_buffer_id = self.get_buffer_id();
        self.set_buffer_id(current_buffer_id.wrapping_add(summand));
    }

    /// Serialize to bytes (little-endian).
    #[inline]
    pub fn to_bytes(&self) -> [u8; 8] {
        self.data.to_le_bytes()
    }

    /// Deserialize from bytes (little-endian).
    #[inline]
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self {
            data: u64::from_le_bytes(bytes),
        }
    }
}

impl fmt::Debug for IndexPointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexPointer")
            .field("metadata", &self.get_metadata())
            .field("offset", &self.get_offset())
            .field("buffer_id", &self.get_buffer_id())
            .field("raw", &format!("0x{:016X}", self.data))
            .finish()
    }
}

impl fmt::Display for IndexPointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            write!(f, "IndexPointer(null)")
        } else {
            write!(
                f,
                "IndexPointer(buf={}, off={}, meta={})",
                self.get_buffer_id(),
                self.get_offset(),
                self.get_metadata()
            )
        }
    }
}

// Ensure IndexPointer is exactly 8 bytes
const _: () = assert!(std::mem::size_of::<IndexPointer>() == 8);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_pointer() {
        let ptr = IndexPointer::new();
        assert!(ptr.is_empty());
        assert!(!ptr.is_valid());
        assert_eq!(ptr.get(), 0);
        assert_eq!(ptr.get_metadata(), 0);
        assert_eq!(ptr.get_offset(), 0);
        assert_eq!(ptr.get_buffer_id(), 0);
    }

    #[test]
    fn test_with_buffer_and_offset() {
        let ptr = IndexPointer::with_buffer_and_offset(42, 100);
        assert!(!ptr.is_empty());
        assert!(ptr.is_valid());
        assert_eq!(ptr.get_buffer_id(), 42);
        assert_eq!(ptr.get_offset(), 100);
        assert_eq!(ptr.get_metadata(), 0);
    }

    #[test]
    fn test_metadata() {
        let mut ptr = IndexPointer::with_buffer_and_offset(1, 2);
        assert!(!ptr.has_metadata());

        ptr.set_metadata(0xAB);
        assert!(ptr.has_metadata());
        assert_eq!(ptr.get_metadata(), 0xAB);

        // Verify other fields are unchanged
        assert_eq!(ptr.get_buffer_id(), 1);
        assert_eq!(ptr.get_offset(), 2);
    }

    #[test]
    fn test_offset_masking() {
        // Test that offset is properly masked to 24 bits
        let mut ptr = IndexPointer::new();
        ptr.set_offset(0xFFFF_FFFF); // Try to set more than 24 bits
        assert_eq!(ptr.get_offset(), 0x00FF_FFFF); // Should be masked
    }

    #[test]
    fn test_buffer_id() {
        let mut ptr = IndexPointer::new();
        ptr.set_buffer_id(0xDEAD_BEEF);
        assert_eq!(ptr.get_buffer_id(), 0xDEAD_BEEF);
    }

    #[test]
    fn test_increase_buffer_id() {
        let mut ptr = IndexPointer::with_buffer_and_offset(100, 50);
        ptr.set_metadata(5);

        ptr.increase_buffer_id(10);

        assert_eq!(ptr.get_buffer_id(), 110);
        assert_eq!(ptr.get_offset(), 50);
        assert_eq!(ptr.get_metadata(), 5);
    }

    #[test]
    fn test_clear() {
        let mut ptr = IndexPointer::with_buffer_and_offset(42, 100);
        ptr.set_metadata(0xFF);
        assert!(ptr.is_valid());

        ptr.clear();
        assert!(ptr.is_empty());
        assert_eq!(ptr.get(), 0);
    }

    #[test]
    fn test_serialization() {
        let ptr = IndexPointer::with_buffer_and_offset(0x1234_5678, 0x00AB_CDEF);
        let mut ptr2 = ptr;
        ptr2.set_metadata(0x42);

        let bytes = ptr2.to_bytes();
        let restored = IndexPointer::from_bytes(bytes);

        assert_eq!(restored.get_buffer_id(), ptr2.get_buffer_id());
        assert_eq!(restored.get_offset(), ptr2.get_offset());
        assert_eq!(restored.get_metadata(), ptr2.get_metadata());
        assert_eq!(restored, ptr2);
    }

    #[test]
    fn test_from_raw() {
        let raw: u64 = 0x42AB_CDEF_1234_5678;
        let ptr = IndexPointer::from_raw(raw);

        assert_eq!(ptr.get(), raw);
        assert_eq!(ptr.get_metadata(), 0x42);
        assert_eq!(ptr.get_offset(), 0xABCDEF);
        assert_eq!(ptr.get_buffer_id(), 0x12345678);
    }

    #[test]
    fn test_equality() {
        let ptr1 = IndexPointer::with_buffer_and_offset(1, 2);
        let ptr2 = IndexPointer::with_buffer_and_offset(1, 2);
        let ptr3 = IndexPointer::with_buffer_and_offset(1, 3);

        assert_eq!(ptr1, ptr2);
        assert_ne!(ptr1, ptr3);
    }

    #[test]
    fn test_display() {
        let ptr = IndexPointer::new();
        assert_eq!(format!("{}", ptr), "IndexPointer(null)");

        let ptr2 = IndexPointer::with_buffer_and_offset(42, 100);
        assert_eq!(format!("{}", ptr2), "IndexPointer(buf=42, off=100, meta=0)");
    }

    #[test]
    fn test_max_values() {
        let mut ptr = IndexPointer::new();
        ptr.set_buffer_id(IndexPointer::MAX_BUFFER_ID);
        ptr.set_offset(IndexPointer::MAX_OFFSET);
        ptr.set_metadata(0xFF);

        assert_eq!(ptr.get_buffer_id(), IndexPointer::MAX_BUFFER_ID);
        assert_eq!(ptr.get_offset(), IndexPointer::MAX_OFFSET);
        assert_eq!(ptr.get_metadata(), 0xFF);
    }
}
